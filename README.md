# lowlat — a low-latency electricity intraday exchange

A learning/engineering project: build a **continuous, price–time-priority matching engine for
electricity intraday trading** (in the spirit of the EEX/EPEX market headquartered in Leipzig),
then push it toward **nanosecond-class reaction latency** using the state of the art in modern
Java (Java 27), lock-free data structures (Agrona), memory-mapped persistence (the FFM API) and
best-practice measurement.

> This README is the **restart document**. It is updated after every step so the work can be
> resumed from scratch at any time. Read "How to restart" first.

---

## How to restart (TL;DR)

```bash
cd /home/preich/hgit/lowlat
source tools/env.sh          # puts local Java 27 + Maven on PATH
mvn -B test                  # build + run the acceptance tests
mvn -B package               # build the shaded, runnable jar
```

Everything is installed **locally under `$HOME`** — no root/sudo required.

---

## Step 0 — Toolchain (done)

Installed without root:

| Tool | Version | Location |
|---|---|---|
| `zip` | 3.0 | `$HOME/.local/opt/zip` (symlink in `$HOME/.local/bin`) |
| SDKMAN | 5.23.2 | `$HOME/.sdkman` |
| Java (Temurin) | **27** | `$HOME/.sdkman/candidates/java/current` |
| Maven | 3.10.0 | `$HOME/.sdkman/candidates/maven/current` |

Why these steps were needed:
* SDKMAN's installer requires `zip`, which was not installed and could not be `apt-get install`ed
  (no passwordless sudo). We downloaded the `zip` `.deb`, extracted it with `dpkg-deb -x` into
  `$HOME/.local/opt/zip` and symlinked the binary — a general trick for installing tools without root.
* `SDKMAN_DIR` and the `sdkman-init.sh` source line live in `~/.bashrc`; the project also ships
  `tools/env.sh` so non-interactive shells (CI, scripts, this agent) get the same environment.
* Java 27 is the latest GA release available via SDKMAN (`27.0.0.0+35-tem`). LTS alternatives are
  25 and 21; the code targets `--release 27`.

Reproduce the toolchain from scratch:

```bash
# 1. zip (no root)
apt-get download zip && mkdir -p "$HOME/.local/opt/zip" && dpkg-deb -x zip_*.deb "$HOME/.local/opt/zip"
ln -sf "$HOME/.local/opt/zip/usr/bin/zip" "$HOME/.local/bin/zip"

# 2. SDKMAN + Java + Maven
export PATH="$HOME/.local/bin:$PATH"; export SDKMAN_DIR="$HOME/.sdkman"
curl -sSL https://get.sdkman.io | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install java 27.0.0.0+35-tem
sdk install maven
```

## Step 1 — Domain & technology research (done)

* Energy markets: [`docs/01-research-electricity-markets.md`](docs/01-research-electricity-markets.md)
  — EEX/EPEX product model, continuous matching rules, IOC/FOK/GTC, negative prices, XBID/SIDC.
* Low-latency Java: [`docs/02-low-latency-java.md`](docs/02-low-latency-java.md)
  — Agrona / Aeron / Chronicle / HdrHistogram / JMH, and the "rules of the game".

## Step 2 — Matching engine core (done, tested)

Implemented in `src/main/java/exchange/engine/`:

* `EngineConfig` — fixed price-ladder geometry, immutable after start-up.
* `OrderStore` — struct-of-arrays order table with id recycling (no per-order objects).
* `TickBitmap` — two-level bitmap for O(1)-ish best-price lookup (no trees, no allocation).
* `OrderBook` — per-product price-ladder FIFOs with cached best bid/ask.
* `MatchingEngine` — continuous matching: price–time priority, trade at resting price,
  LIMIT/MARKET, GTC/IOC/FOK, negative prices, per-product isolation.

Fixed-point conventions (`Scales`): price = `long` in **0.01 EUR/MWh**, quantity = `long` in
**0.1 MW**. Never floating point on the hot path.

Domain rules implemented are listed in `docs/01-research-electricity-markets.md`; out of scope for
v1 (documented as future work): cross-border capacity (CMM/routing), batch matching, icebergs,
auctions.

**Acceptance tests** (`src/test/java/exchange/engine/MatchingEngineTest.java`) — 11 tests, all green:

```bash
mvn -B test
```

Covered: trade at resting price, price–time priority (FIFO), partial fill + rest, IOC, FOK,
cancel + best-price fallback, market order sweep, negative prices, product isolation,
off-tick / out-of-range rejections, monotonic trade ids.

---

## Step 3 — Wire protocol & transport (done, tested)

* `src/main/proto/exchange.proto` — the protobuf interface (`NewOrder`, `CancelOrder`,
  `ClientMessage`, `OrderAck`, `ExecutionReport`, `Fill`, `Trade`, `BookUpdate`, `ServerMessage`).
* `exchange.wire.FastWire` — a hand-rolled **protobuf-wire-compatible** codec used on the hot
  path (the protobuf-java runtime is kept only for the `.proto` definition and byte-compatibility
  tests). Each frame on the wire is `int32 length | protobuf message`.
* `exchange.gateway.GatewayServer` — a single NIO reactor thread: one `Selector`, per-session
  reusable direct buffers, batched flushes of "dirty" sessions, `selectNow()` busy-spin guarded so
  the selector iterator is only touched when `ready > 0`.
* `exchange.engine.EngineService` + `RingSink` — one matching thread fed by an Agrona MPSC
  `ManyToOneRingBuffer`; egress via an SPSC `OneToOneRingBuffer`; the gateway encodes on its own
  thread.

**Acceptance tests**: `exchange.wire.FastWireRoundTripTest` (6) and
`exchange.gateway.GatewayIntegrationTest` (3, real sockets end-to-end).

## Step 4 — Load generator (done)

`exchange.loadgen.LoadGen` — an open-loop generator (a *better* fit than k6 for a raw,
length-prefixed protobuf TCP protocol: k6 cannot express per-order nanosecond round-trip latency).
One non-blocking socket per connection, single thread that paces sends against a target rate
(avoiding coordinated omission) and drains responses; per-order round-trip latency is recorded in
an `HdrHistogram`. CLI: `--host --port --conns --rate --duration --products --inflight --tifmix`.

> A k6 script remains optional as a *secondary* smoke test; the Java generator is the primary
> instrument because only it can measure true round-trip latency.

## Step 5 — Persistence: memory-mapped event journal (done, tested)

`exchange.journal.TradeJournal` is the durable, replayable ledger (append-only, fixed 64-byte
records); `JournalingSink` decorates the egress sink so the matching thread journals accepted
orders, fills and status changes; `JournalDump` replays a journal; `--journal <path>` enables it.

**Design change — why we dropped Chronicle Queue.** We started with Chronicle Queue
(`net.openhft:chronicle-queue` 2026.7), the classic near-zero-GC persisted queue. On **JDK 27** it
can no longer memory-map a file:

```
java.lang.AssertionError: java.lang.IllegalAccessException:
  class is not public: sun.nio.ch.UnixFileDispatcherImpl.map0, from class net.openhft.chronicle.core.OS
```

Chronicle resolves `map0` with `MethodHandles.lookup().unreflect(...)`; that now rejects a
package-private class, and `--add-opens java.base/sun.nio.ch=ALL-UNNAMED` does **not** help (only
`privateLookupIn` does, which Chronicle does not use). We proved the diagnosis with a standalone
probe (`unreflect` fails, `privateLookupIn` succeeds) and confirmed the field cannot be patched
from outside: HotSpot constant-folds Chronicle's `static final MAP0_MH`. Pinning an older JVM was
not acceptable.

So we implemented the journal directly on the modern **Foreign Function & Memory** API
(JEP 454, stable since Java 22) — the supported successor to `Unsafe`/`MappedByteBuffer`, and for
a fixed-record append-only log genuinely "something better": a deterministic on-disk layout, a
64-byte header (`magic/version/recordSize/count/capacity`), geometric growth, deterministic
unmapping via `Arena.close()`, and one fewer heavyweight dependency. `append()` is a handful of
stores into an already-faulted-in page — no syscall, no allocation — so it is safe on the matching
thread.

**Tests**: `exchange.journal.TradeJournalTest` — write → close → reopen → replay → field
round-trip; geometric growth past the initial capacity; reopen-and-continue-appending; and
rejection of a non-journal file.

## Step 6 — Performance measurement & GC tuning (done)

### Matching kernel (no sockets, no rings) — `tools/run.sh bench`

| scenario | throughput | p50 | notes |
|---|---|---|---|
| `rest`  (insert-heavy) | ~16.6 M ops/s | 36 ns | build depth |
| `cross` (match-heavy)  | ~11.9 M ops/s | 59 ns | IOC crossing |
| `mixed` (50/50)        | ~8.6 M ops/s  | 82 ns | |

### End-to-end over TCP

* Sustained ~**198k orders/s** at target, peak ~**410k orders/s** (engine ~480k/s) with
  8 connections / inflight 2048 (FastWire codec).
* True round-trip latency (inflight = 1): **p50 ≈ 16 µs**, p99 ≈ 110 µs. This is dominated by
  syscalls + thread hand-off, not the matcher (whose p50 is tens of ns).

> Latency must be measured with **low** inflight (1–16). High inflight (≥256) inflates numbers
> with client-side pipelining delay and is only valid for *throughput*.

### GC comparison (engine, steady state)

G1 vs `-XX:+UseZGC -Xmx4g` vs Epsilon (`-XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC
-Xmx3g`). Epsilon ran clean at steady state, showing the engine is **nearly allocation-free** on
the hot path. GC choice did not move the p50; only the tail.

### Environment caveat

8 cores (i7-6700), 62 GB RAM, Ubuntu 24.04, **shared/loaded host**: expect multi-millisecond
OS-scheduling tail spikes (no CPU pinning/`isolcpus` available). Kernel p50 is 36–82 ns while E2E
p50 is ~16 µs — the gap is syscall/thread-handoff bound.

## Step 7 — Launcher (done)

`exchange.launcher.Launcher` is the shaded jar's `Main-Class`:

```bash
java -jar target/lowlat-exchange.jar engine  [--port 9001] [--products 1024] [--orders N] [--journal path]
java -jar target/lowlat-exchange.jar loadgen [--host 127.0.0.1 --port 9001 --conns 4 --rate 200000 --duration 10000 --inflight 16]
java -jar target/lowlat-exchange.jar bench   [--ops 20000000 --warmup 5000000 --scenario mixed]
java -jar target/lowlat-exchange.jar journal-dump <path>
```

Convenience wrapper `tools/run.sh <role> [args]` adds the JVM module flags
(`--add-exports java.base/jdk.internal.misc=ALL-UNNAMED`, etc.). Extra JVM flags via `JVM_FLAGS`:

```bash
JVM_FLAGS="-XX:+UseZGC -Xmx4g" tools/run.sh engine
```

## Step 8 — Logging with SLF4J + logback (done)

All diagnostics go through **SLF4J** with **logback** as the backend; there is no `System.out`
on the code path any more.

* `pom.xml` adds `org.slf4j:slf4j-api` and `ch.qos.logback:logback-classic`.
* `src/main/resources/logback.xml` — console + size/time-rotated file (`logs/lowlat.log`), each
  behind its **own `AsyncAppender`** with `neverBlock=true` so logging can never apply
  back-pressure to the reactor or the matching thread. (`AsyncAppender` accepts exactly one child,
  hence two wrappers.) A `NopStatusListener` keeps logback's own config chatter off stdout.
* `src/test/resources/logback-test.xml` — synchronous console-only config so `mvn test` output is
  deterministic.
* The **matching hot path never logs**: logging is confined to start-up/shutdown, the 5 s periodic
  engine stats, load-generator progress/results and error paths.

Overrides without editing files:

```bash
-Dlowlat.log.level=DEBUG            # root level (default INFO)
-Dlowlat.log.dir=/var/log/lowlat    # rolling-file directory (default logs/)
-Dlogback.configurationFile=/path/to/logback.xml
```

---

## How to restart from scratch (current, all green)

```bash
cd /home/preich/hgit/lowlat
source tools/env.sh
mvn -B test          # 24 acceptance tests: engine 11, wire 6, gateway 3, journal 4
mvn -B package       # shaded runnable jar at target/lowlat-exchange.jar

tools/run.sh bench --ops 20000000 --scenario mixed
tools/run.sh engine --port 9001 --journal /tmp/opencode/journal &
tools/run.sh loadgen --port 9001 --conns 4 --rate 200000 --duration 10000 --inflight 16
tools/run.sh journal-dump /tmp/opencode/journal
```

## Known issues / limitations

* **Chronicle Queue cannot mmap on JDK 27** (see Step 5) — replaced by the FFM journal.
* No CPU pinning / `isolcpus` (no root), so tail latency includes OS-scheduling noise.
* Journal growth (`append` when full) remaps the file and can stall the caller briefly; size the
  journal generously for latency runs.
* A stray `src/main/java/exchange/engine/.EngineMain.java.swp` (editor swap file) is present on
  disk and is **not** part of the build.

## Roadmap (future)

* k6 smoke script as a secondary load test; JMH harness for the kernel.
* Crash-recovery book rebuild by replaying `TradeJournal` into a fresh `MatchingEngine`.
* Optional CPU-affinity launcher and an `isolcpus` note for bare-metal tails.
* Aeron (real transport) instead of the in-process NIO gateway for multi-host fan-out.

