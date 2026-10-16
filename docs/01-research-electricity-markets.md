# Research notes: electricity intraday markets

Sources consulted (2026-10): EEX Trading Conditions (2026-09-15), EEX glossary,
EPEX SPOT "Continuous Trading Matching Algorithm" (2026-06), EPEX SPOT XBID launch
information package, Springer chapter on Single Intraday Coupling (SIDC), EEX/energie-chronik
history of the Leipzig intraday launch.

## The venue

* **EEX (European Energy Exchange) is headquartered in Leipzig**, Germany. It is the
  largest European energy exchange (power, gas, emissions, freight).
* Power trading at EEX is largely operated by **EPEX SPOT** (the power spot exchange).
* The German **intraday** market launched **25 September 2006** on an EEX platform
  (EEX/RWE Transportnetz Strom). It complemented the existing day-ahead spot market.
* Products deliver *physical* electricity for a specific **delivery period** in a
  specific **bidding zone / delivery area**.

## Product model (what we must model)

* A **contract / product** is identified by:
  * delivery start (timestamp, UTC)
  * delivery duration (e.g. 15 min, 60 min)
  * delivery area / bidding zone (DE-LU, FR, NL, BE, AT, ...)
  * optionally a "smallest tradable unit" (e.g. 0.1 MW)
* Prices can be **negative** (over-supply from renewables: curtailment / paying to off-take).
  A price grid must therefore cover e.g. `-500.00 .. +4000.00 EUR/MWh`.
* Quantity is power (MW) or energy (MWh) with a minimum tradable unit and a step.

## Order & matching semantics (the rules we implement)

From EEX Trading Conditions and the EPEX/XBID continuous matching description:

1. **Continuous trading, price–time priority.**
   * Better price wins; at equal price the *earlier time of receipt* wins.
   * Incoming SELL matches the highest bid; incoming BUY matches the lowest ask.
2. **Trade price = the price of the order already resting in the book**
   ("regular matching"). The incoming/aggressor order does not set the price.
3. **Order types:** limit and market (unlimited). Market orders execute before limit
   orders and are constrained by a *Market Order Matching Range*.
4. **Execution restrictions / time-in-force:**
   * `IOC` – execute what you can immediately, cancel the rest, never rest in the book.
   * `FOK` – execute the whole quantity immediately against one or more resting orders,
     otherwise cancel entirely. Never rests in the book.
   * `GTC` – remainder rests in the book.
   * `AON` (all-or-none), `NON` (partial allowed) also exist.
5. **Iceberg orders:** only a slice is shown; the visible slice renews with a new time
   stamp when consumed.
6. **Amend = new time stamp** (price/qty change loses time priority).
7. **Batch matching** is triggered by a *capacity update* (cross-border capacity), not by
   an order. It matches the largest price-difference pairs first, at a single clearing price.
8. **Cross-border coupling:** a shared order book (SOB) + capacity management module
   (CMM). Orders only cross-match if transmission capacity between their delivery areas is
   available. This is SIDC / XBID (M7 XBID).

## Scope decisions for our engine (v1)

We model the **single-zone continuous intraday book** with the core EPEX semantics:

* price–time priority, trade at resting price
* LIMIT + MARKET, TIF GTC / IOC / FOK, partial fills
* negative prices supported
* one matching engine, many products (order book per product), many participants

Out of scope for v1 (documented as future work): cross-border capacity (CMM/routing),
batch matching on capacity updates, icebergs, auctions.

## Why this makes a good latency problem

* **Millions of actors**: every generator (producer), industrial consumer, aggregator and
  algorithmic trader is a market participant. Burst-driven order flow (gate-closure,
  forecast revisions, weather events) produces huge same-second bursts.
* **Nanosecond goal**: reaction time is the competitive edge; matching must be
  deterministic and allocation-free on the hot path.
