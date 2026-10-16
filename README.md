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
mvn -B test                  # build + run the 35 acceptance tests
mvn -B package               # build the shaded, runnable jar
tools/run.sh demo            # watch a realistic session: the trade TAPE + BOOK, live
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

**Acceptance tests**: `exchange.wire.FastWireRoundTripTest` (6), `exchange.gateway.GatewayIntegrationTest`
(3, real sockets end-to-end) and `exchange.gateway.MarketDataIntegrationTest` (2, public tape + book
broadcast — see Step 9).

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
| `rest`  (insert-heavy) | ~17.6 M ops/s | 28 ns | build depth |
| `cross` (match-heavy)  | ~13.6 M ops/s | 46 ns | IOC crossing |
| `mixed` (50/50)        | ~9.0 M ops/s  | 74 ns | |

> These are after the two quick wins below (allocation-free reactor; multiply-shift price→tick).
> Before them the same benchmark read 16.6 M / 36 ns (`rest`), 11.9 M / 59 ns (`cross`),
> 8.6 M / 82 ns (`mixed`). Treat the deltas as indicative — the host is shared — except for the
> isolated mapping A/B, which is a controlled same-process comparison.

### End-to-end over TCP

* True round-trip latency, **inflight = 1** (no client pipelining): **p50 ≈ 19 µs**, p90 ≈ 30 µs,
  p99 ≈ 97 µs. Dominated by syscalls + thread hand-off, not the matcher (whose p50 is tens of ns).
  Re-measured after the two quick wins below: **p50 ≈ 16 µs, p90 ≈ 21 µs, p99 ≈ 31 µs** (the tail is
  very host-load sensitive).
* At **inflight = 16** (low, but allows a little pipelining): p50 ≈ 33 µs.
* Throughput: sustained ~**200k orders/s** at target; peak engine-side ~**500k orders/s** with
  8 connections / inflight 2048 (FastWire codec).

> Latency must be measured with **low** inflight (1–16). High inflight (≥256) inflates numbers
> with client-side pipelining delay and is only valid for *throughput*.

### GC — measured (engine process under load, ~450k orders/s)

Method: start the engine with `-Xlog:gc:<file>`, drive it with the load generator, then split the
GC log by timestamp (start-up vs. steady state).

| collector | GC pauses during the load | longest pause | notes |
|---|---|---|---|
| G1 (default) | all 8 sit in a ~60 ms window at load onset (t≈3.44–3.49 s), then **zero** | 8.5 ms | **humongous** allocs: `OrderStore` + per-product ladders |
| ZGC (`-XX:+UseZGC -Xmx4g`) | **0** | — | concurrent; no stop-the-world |
| Epsilon (`-XX:+UseEpsilonGC -Xms2g -Xmx2g`) | **never collects** | — | 6.7 M orders / 20 s; used heap was 133→141 MB, **now flat at 132 MB** (alloc fix below) |

Takeaways:

* **The matching kernel allocates nothing on its hot path.** The 20 M-op `bench` GC log has no line
  after the benchmark's explicit `System.gc()`.
* In the whole engine process the only GC work is **start-up**: the struct-of-arrays `OrderStore`
  (~64 MB) and each traded product's price ladder (~3.5 MB/product) are single humongous
  allocations that G1 reclaims in a handful of young pauses. After load onset the log is silent.
* Epsilon survives the whole run. **After the reactor fix (see below) the engine's used heap is
  dead flat** — 132 MB across 3.3 M orders under load, versus 133→141 MB before — so the only
  steady-state allocation (the reactor's per-`select()` `Iterator`) is now gone, not merely small.

Conclusion: **GC is already off the critical path.** Collector choice does not move p50 (36–82 ns
kernel); it only bounds the tail. Use a low-pause collector (ZGC) plus pinning for bare-metal tails;
the remaining latency is syscall/thread-hand-off, not the collector.

### Optimisation log

Two quick wins from the backlog, each measured:

* **Allocation-free reactor** — `SelectedSelectionKeySet` replaces the JDK selector's ready-key set
  (Netty-style reflection on `sun.nio.ch.SelectorImpl`, with an automatic fallback), and the loop
  iterates the backing array by index. Engine used heap under sustained load went from growing
  (~0.5 MB/s) to **flat at 132 MB**, so even an Epsilon run no longer accumulates garbage.
* **Exact multiply-shift price→tick** — `EngineConfig` precomputes the divide in its constructor.
  Isolated same-process A/B: **1.8 ns vs 4.2 ns** per mapping (~57% cheaper, no `idiv`). Kernel
  `bench` p50 moved `rest` 36→28 ns, `cross` 59→46 ns, `mixed` 82→74 ns (indicative; shared host).

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
java -jar target/lowlat-exchange.jar demo
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

## Step 9 — Public market data tape & realistic tests (done, tested)

The `Trade` message existed in the `.proto` but was never emitted. It is now a first-class part of
the wire, so a **market-data subscriber can literally see the matched price**.

* `Sink.onTrade(...)` — an anonymous **public tape** print, emitted once per match (price + qty +
  aggressor side), separate from the per-participant `onFill`.
* `FastWire.KIND_TRADE` + `ServerEncoder.trade(...)` / `ServerParser` — protobuf-wire-compatible
  `ServerMessage.trade` (field 3), so the public tape rides the same framed codec.
* `GatewayServer` **broadcasts** each `Trade` to every connected session, alongside `BookUpdate`.
* **Book-update fix**: updates are now emitted when best price **or size** changes (previously only
  on a price change, so size-only changes were silently dropped).
* `MatchingEngine` gained top-of-book accessors (`bestBidPrice/Qty`, `bestAskPrice/Qty`) used by the
  tests and the demo.

### Tests — 35 total, all green

| test | what it proves |
|---|---|
| `MatchingEngineTest` (11) | order types, TIF, negative prices, priority |
| `DifferentialMarketTest` (3) | the optimised engine matches a **naive textbook reference** exactly, trade-for-trade and on the final book, over **60k randomized orders** (wide/tight/choppy books) |
| `MarketScenarioTest` (2) | a whole mini-session: prints at the resting price, cancels, book never crossed, trade ids monotonic, quantity conserved |
| `MarketDataIntegrationTest` (2) | over real TCP: a **passive subscriber** sees the book and the tape print at the matched price; a market sweep prints a rising ladder of prices |
| `EngineConfigTest` (3) | the multiply-shift price→tick map equals the naive div/mod reference everywhere, across tick sizes and the division fallback |
| `FastSelectorTest` (1) | the gateway installs the allocation-free ready-key set (fast-path is live, not silently falling back) |
| `FastWireRoundTripTest` (6) | protobuf-java byte compatibility |
| `GatewayIntegrationTest` (3) | end-to-end order → ack → fill over sockets |
| `TradeJournalTest` (4) | journal write/replay/growth/reopen |

### See it yourself

```bash
tools/run.sh demo    # replays a scripted session and logs the TAPE and BOOK live
```

```
ORDER  DE 15:00-15:15 (#1)  SELL 2.5 MW @ 50.10
BOOK   DE 15:00-15:15 (#1)  bid   -   x 0.0   |   ask 50.10 x 2.5
ORDER  DE 15:00-15:15 (#1)  BUY  1.0 MW @ 50.10
TAPE   DE 15:00-15:15 (#1)  50.10 EUR/MWh x 1.0 MW   (buy aggressor)
```

### Is it plausible / realistic?

**What is faithful to a real continuous intraday market (EPEX/EEX-style):** continuous
price–time priority, trades printing at the **resting** order's price ("regular matching"),
limit/market orders, GTC/IOC/FOK, cancel, negative prices, a single-threaded deterministic matcher,
and an anonymous public tape + top-of-book broadcast to all participants.

**What is simplified or missing — be honest about it:**

* **Only the aggressor is notified of a fill.** A real exchange sends an execution report to *both*
  counterparties; here the maker's fill is only visible on the tape. (Biggest realism gap.)
* Book data is **top-of-book only**, emitted once per command (a multi-level sweep broadcasts only
  the final book, not each intermediate level).
* No order **amend**, no **self-trade prevention**, no trading **halts/circuit breakers**, no
  delivery-period / market-coupling (XBID) semantics, no per-session **risk limits/fees**.
* Persistence is append-only; **crash-recovery replay into the book** is not wired up yet.
* Single process, in-memory; the transport is loopback NIO, not kernel-bypass.

**Latency realism:** the matcher itself does tens of nanoseconds per order in memory (that is the
"nanosecond reaction"), and a single core can do ~8–17 M orders/s. Real-network round trips are
~19 µs p50 here, entirely limited by syscalls/thread hand-off on a shared host — not by the engine.
On a tuned box (isolation, busy-poll, kernel bypass) single-digit-µs is the realistic target;
millisecond tails observed here are OS scheduling on a shared machine, not the matcher.

## Code conventions (readability on the hot path)

Being fast and being readable are not in conflict here, so the code follows a few rules:

* **No cryptic abbreviations in public/event APIs.** Hot-path parameters carry their meaning and
  unit: `price`, `qty`, `tradeId`, `tsNs`, `leavesQty`, `aggressorSide`, `makerOrderId` — never
  `p`, `q`, `t`, `s`. (`tsNs`/`recvNs` are suffixed `Ns` for nanosecond timestamps.)
* **Every egress method documents its contract** (see `exchange.engine.Sink`): that it runs on the
  matching thread, must not allocate or block, and what each field means — including that prices are
  fixed-point `long`s in 0.01 EUR/MWh and quantities in 0.1 MW.
* **Wire codec internals name the protobuf concepts** they mirror
  (`buf`, `offset`, `length`, `fieldNumber`, `wireType`, `value`), so the hand-rolled codec reads
  like the spec it implements.
* **Performance choices are commented where they are made** (e.g. the reusable scratch buffer and
  fixed native-endian layout in `RingSink`, why the trade tape is not double-journaled in
  `JournalingSink`, why the matcher never reads the clock).

---

## How to restart from scratch (current, all green)

```bash
cd /home/preich/hgit/lowlat
source tools/env.sh
mvn -B test          # 35 tests: engine 19 (11+2+3+3 config), wire 6, gateway 6 (3+2+1), journal 4
mvn -B package       # shaded runnable jar at target/lowlat-exchange.jar

tools/run.sh bench --ops 20000000 --scenario mixed
tools/run.sh demo
tools/run.sh engine --port 9001 --journal /tmp/opencode/journal &
tools/run.sh loadgen --port 9001 --conns 4 --rate 200000 --duration 10000 --inflight 16
tools/run.sh journal-dump /tmp/opencode/journal
```

## Optimisation backlog (low latency)

Ranked by expected effect on p99.9 on this shared, unpinned host. Items marked **[done]** are already
in the code; the rest are candidates.

**A. Application / JVM (cheap, high ROI)**

1. **[done] Kill the last hot-path allocation.** The reactor used to call
   `selector.selectedKeys().iterator()` on every non-empty `selectNow()`. `SelectedSelectionKeySet`
   now replaces the JDK selector's ready-key sets (the Netty-style reflection on
   `sun.nio.ch.SelectorImpl`) and the loop iterates the backing array by index — zero allocation,
   with an automatic fallback to the standard iterator if the install ever fails. Effect: the
   engine's used heap is now **flat (132 MB)** under sustained load where it previously grew ~0.5 MB/s.
2. **[done] `EngineConfig.tickFor` price→tick.** `EngineConfig` (now a final class, so it can cache
   precomputed state) builds an **exact** multiply-shift divide in its constructor and confirms
   divisibility with one multiply — no `idiv` per order. Isolated same-process A/B: **1.8 ns vs
   4.2 ns** per mapping (~57% cheaper); the `rest` kernel scenario moved p50 36→28 ns.
3. **Per-product book memory.** Each `OrderBook` eagerly allocates 4×`int[110 001]` + 2×`long[110
   001]` ≈ **3.5 MB/product** on first use (×1024 products ≈ 3.6 GB, plus humongous GC churn). Use a
   bounded/sparse window per product, or narrower types, to cut TLB/cache pressure and start-up GC.
4. **One fewer copy in `RingSink`.** It stages into a scratch `byte[128]` and `write()` copies it
   into the ring; Agrona `tryClaim`/`commit` would encode straight into the ring.
5. **JVM flags for the tail:** `-Xms=-Xmx`, `-XX:+AlwaysPreTouch`, `-XX:+UseTransparentHugePages`,
   `-XX:+UseNUMA`, and a low-pause collector (ZGC) or Parallel for throughput.
6. **CPU isolation:** `isolcpus`/`nohz_full` + `taskset`, NIC IRQ affinity (`irqbalance` off),
   `SO_BUSY_POLL`. Needs root/hardware.
7. **Matcher micro-opts:** `@Contended` padding on ring cursors, replace bounds checks with
   `Unsafe`/`VarHandle` access, software prefetch (`Unsafe.prefetchRead`) of the next book level,
   and fewer branches in the sweep loop.

**B. Transport (largest absolute win)**

8. **Native epoll / `io_uring`** instead of NIO (Netty `EpollEventLoop`, or Agrona/Aeron) — removes
   `Selector` overhead and enables multicast UDP market-data fan-out.
9. **Kernel-bypass** (`ef_vi`/OpenOnload, DPDK) for wire-to-wire sub-µs; out of scope on this host.

**C. Measurement correctness**

10. Port the kernel benchmark to **JMH** (JIT/DCE-safe) and add **coordinated-omission** correction
    to the HdrHistogram reports; keep k6 as a secondary smoke test.

## Known issues / limitations

* **Realism gaps (see Step 9):** only the aggressor gets an execution report (the maker's fill is
  only on the tape); book data is top-of-book and emitted once per command; no amend, self-trade
  prevention, halts, session risk limits, or delivery-period/market-coupling semantics.
* **Chronicle Queue cannot mmap on JDK 27** (see Step 5) — replaced by the FFM journal.
* No CPU pinning / `isolcpus` (no root), so tail latency includes OS-scheduling noise.
* Journal growth (`append` when full) remaps the file and can stall the caller briefly; size the
  journal generously for latency runs.
* A stray `src/main/java/exchange/engine/.EngineMain.java.swp` (editor swap file) is present on
  disk and is **not** part of the build.

## Roadmap (future)

* Maker-side execution reports (notify both counterparties of a fill).
* Per-fill book updates and a depth (N-level) market-data feed.
* Crash-recovery book rebuild by replaying `TradeJournal` into a fresh `MatchingEngine`.
* Order amend/cancel-replace and self-trade prevention.
* k6 smoke script as a secondary load test; JMH harness for the kernel.
* Optional CPU-affinity launcher and an `isolcpus` note for bare-metal tails.
* Aeron (real transport) instead of the in-process NIO gateway for multi-host fan-out.

