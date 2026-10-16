# Research notes: state-of-the-art low-latency Java (2026)

Sources consulted (2026-10): Agrona project, Aeron Q&A 2025, Chronicle Software
tuning/off-heap blogs (2022–2026), LMAX Disruptor, "Low-Latency Java Programming" (2025),
JEP 471 (VarHandle/FFM successors to Unsafe), OpenJDK GC documentation.

## The libraries that matter

| Library | What it gives us | Why we use it here |
|---|---|---|
| **Agrona** (real-logic) | off-heap buffers, `ManyToOneRingBuffer` (MPSC), `BroadcastBuffer`, primitive maps, `UnsafeBuffer`, cache-line padding, nano clocks | lock-free, allocation-free hand-off between I/O threads and the matching thread; the data layer under Aeron |
| **Aeron** | IPC/UDP message transport, `LogBuffer`, zero-copy, single-writer | optional next step: process separation (gateway vs engine) over shared memory |
| **Chronicle Queue** | memory-mapped, persisted, near-zero-GC event log | *evaluated, then replaced*: cannot `mmap` on JDK 27 (see below); we now use the JDK **FFM API** (`java.lang.foreign`) for the durable journal |
| **JDK Foreign Function & Memory** (JEP 454) | supported off-heap memory / file mapping with deterministic deallocation | the durable, replayable event log (`TradeJournal`): `Arena` + mapped `MemorySegment`, no `Unsafe`, no reflection |
| **HdrHistogram** | allocation-free-ish latency histograms, correct percentiles | measure p50/p99/p99.9/max without coordinated omission |
| **LMAX Disruptor** | ring-buffer pub/sub | alternative to Agrona ring buffers; Agrona's is a simpler fit for a single consumer |
| **JCTools** | lock-free queues | alternative MPSC queues |
| **JMH** | microbenchmark harness | measure the matching kernel in isolation, with warm-up + statistics |
| **JOL** (optional) | object layout inspector | verify padding / false-sharing avoidance |

## The rules of the game (what "state of the art" actually means)

1. **Off-heap / never allocate on the hot path.** Allocation churn causes GC, GC causes
   jitter, jitter is the enemy. Pre-allocate everything at start-up; reuse via pools and
   ring buffers. Chronicle/Aeron/Agrona designs are effectively GC-free.
2. **Single writer per data structure, lock-free communication.** One thread owns the
   order book. Everyone else hands work to it through an MPSC ring buffer and reads results
   from an SPSC/broadcast buffer. No locks on the hot path.
3. **Busy-spin, don't block.** `Thread.onSpinWait()` on the consumer; avoid park/unpark and
   syscalls. Keep the thread hot in cache.
4. **Cache-line awareness.** 64-byte lines; pad shared counters to avoid false sharing;
   keep the hot working set small so it stays in L1/L2.
5. **Binary protocols.** Protobuf for the external interface (portable, versioned); SBE /
   hand-rolled fixed-layout structs for the *internal* hot path. Never JSON on the hot path.
6. **GC choice & tuning.** For a near-zero-allocation engine even the GC choice matters less,
   but the state of the art is: **ZGC (generational, Java 21+)** or **Shenandoah** for
   low pause, or **Epsilon** to prove the engine allocates nothing. Always measure with
   `-Xlog:gc*` and JFR.
7. **Pin & isolate.** `taskset`/`isolcpus`/`nohz_full` to pin the matching thread to a core,
   disable C-states, keep the CPU frequency high. (Documented; needs root for some knobs.)
8. **Measure like the pros.** Nanosecond timestamps (`System.nanoTime` / TSC), latency
   histograms, **coordinated omission** avoidance in the load generator (open-loop rate
   pacing, not closed-loop request/response), and tail latency (p99.9), not averages.

## Concrete plan for this project

* **Transport:** TCP + length-prefixed protobuf for the *interface* (easy to test with
  ordinary clients). Internally, decode once into a fixed-layout struct and hand off.
* **Hand-off:** Agrona `ManyToOneRingBuffer` for inbound (many gateway connections → one
  engine thread). Broadcast/SPSC ring for outbound (engine → many gateway threads).
* **Engine:** single thread, primitive-array order book, price-ladder + bitmap for O(1)-ish
  best-price maintenance.
* **Journal:** memory-mapped fixed-record log on the JDK FFM API (optional, toggle for
  benchmarks). Chronicle Queue was the original choice but is unusable on JDK 27 — see
  "Chronicle Queue on JDK 27" below.
* **Load gen:** custom Java client (pipelined, open-loop pacing, HdrHistogram). This is
  "better than k6" for a binary TCP protocol because k6 cannot express nanosecond
  round-trip latency over a length-prefixed protobuf stream; we keep a k6 script for an
  HTTP smoke/throughput test.
* **GC:** start with default G1, measure, then compare ZGC generational vs. Epsilon.

## Chronicle Queue on JDK 27 (investigation)

Chronicle Queue was the intended journal, but it fails to `mmap` on JDK 27:

```
java.lang.AssertionError: java.lang.IllegalAccessException:
  class is not public: sun.nio.ch.UnixFileDispatcherImpl.map0, from class net.openhft.chronicle.core.OS
```

`chronicle-core` resolves `sun.nio.ch.UnixFileDispatcherImpl.map0` with
`MethodHandles.lookup().unreflect(method)`. On JDK 27 that throws because the dispatcher class is
package-private; `--add-opens java.base/sun.nio.ch=ALL-UNNAMED` does **not** grant access, whereas
`MethodHandles.privateLookupIn(...)` does. A standalone probe confirmed exactly this. Patching
Chronicle's cached handle from outside also fails: `MAP0_MH` is `static final` and HotSpot
constant-folds reads of it, so an `Unsafe`-written replacement is ignored.

Conclusion: keep the JDK, replace the library. The journal now uses the **FFM API**
(`FileChannel.map(..., Arena)` + `MemorySegment`), which is the JDK's supported, future-proof
off-heap path (JEP 471 retires `Unsafe` memory access in favour of exactly this).
