# rar-chat — POC chunk 02 (upgraded)

Minimal terminal chat over any OpenAI-compatible streaming chat-completions
endpoint. This revision upgrades the Gemini-produced chunk-02 draft: env-file
configuration by argument, correct multi-turn history, finish_reason-gated
completion semantics, timer-enforced stall detection, a bounded SSE buffer,
and inline INV-/DEC- instrumentation throughout (registry in `src/lib.rs`).

## Build

Pinned to build on cargo/rustc 1.75 (Ubuntu 24.04 apt toolchain); the
committed `Cargo.lock` reproduces the verified dependency graph. On a rustup
toolchain you may loosen the `=` pins in `Cargo.toml`.

    cargo build --release
    cargo test          # 21 unit tests: env parsing, SSE framing, completion semantics

## Run

    rar-chat [ENV_FILE]

`ENV_FILE` is a KEY=VALUE file (comments `#`, optional `export`, optional
matching quotes). Keys found in the file override the process environment;
with no argument, the process environment alone is used. Copy an example and
fill in your key — and protect it: `chmod 600 nvidia.env`, never commit it
(`.gitignore` already excludes `*.env`).

| key                    | required | default                     |
|------------------------|----------|-----------------------------|
| `OPENAI_API_KEY`       | yes      | —                           |
| `OPENAI_MODEL`         | yes      | — (no default: DEC-CHAT-10) |
| `OPENAI_BASE_URL`      | no       | `https://api.openai.com/v1` |
| `OPENAI_SYSTEM`        | no       | (none)                      |
| `OPENAI_INCLUDE_USAGE` | no       | `1`                         |
| `OPENAI_STALL_SECS`    | no       | `30`                        |

### NVIDIA NIM

    cp nvidia.env.example nvidia.env    # edit: paste your nvapi- key
    chmod 600 nvidia.env
    ./target/release/rar-chat nvidia.env

The wire contract this driver speaks (POST `{base}/chat/completions`, bearer
auth, `stream: true`, SSE `data:` frames, `data: [DONE]` sentinel) is
verified against NVIDIA's NIM hosted catalog, which documents its LLM APIs
as OpenAI-compatible at base URL `https://integrate.api.nvidia.com/v1` with
keys from build.nvidia.com. `stream_options.include_usage` is an OpenAI
extension with per-model support variance on compatible endpoints; a missing
usage frame is never treated as an error, and `OPENAI_INCLUDE_USAGE=0` drops
the request field entirely if a model rejects it.

## Behavior contracts (summary — full registry in src/lib.rs)

- A turn commits to history only from a completed `ChatOutcome`; a
  `finish_reason` from the server — not `[DONE]`, not end-of-stream — is the
  completion criterion. Truncated, aborted, and stalled streams are distinct
  typed faults, and none of them commits anything.
- The API key is read from the env file or process environment, attached to
  the request, and appears in no log line or error message.
- The SSE reassembly buffer is bounded (1 MiB); UTF-8 is decoded only on
  complete frames.

## References

1. NVIDIA, "LLM APIs" — NIM API reference (base URL, endpoint, model table).
   https://docs.api.nvidia.com/nim/reference/llm-apis
2. NVIDIA, API Catalog and key generation.
   https://build.nvidia.com/ and https://build.nvidia.com/settings
3. OpenAI, "Create chat completion" — API reference (the compatibility
   target, including stream_options.include_usage semantics).
   https://platform.openai.com/docs/api-reference/chat/create
4. WHATWG, HTML Living Standard §9.2 "Server-sent events" (frame grammar,
   data-line joining, comment frames).
   https://html.spec.whatwg.org/multipage/server-sent-events.html

## Architecture (chunk 02c — Driver / Event-Handler split)

The ratified low-level distribution:

- **Driver** (`openai.rs`) — wire custody only. Builds the outbound request,
  fields the incoming byte stream, distinguishes wire-level facts (valid
  installments on the clean path; malformed SSE and residue on the dirty
  path, forwarded as records, never silently eaten; timeouts; transport
  errors). Owns the wire-detectable clocks: the mid-stream stall window and
  a loose first-byte backstop (a mechanical ceiling so a wedged strategy
  cannot hang a socket forever). Retries are mechanical here, spent against
  an allowance minted by the Handler. The Driver holds no pending-knowledge
  and no alternatives; a failover verdict passes through it opaque.
- **Event Handler** (`event_handler.rs`) — the swappable component. Refines
  installments into events and messages (normalization + completion
  machine), owns Wire Strategy (retry allowance, Send Bucket pool sizing,
  whether alternatives exist), and fans every fact out to observer ports.
  Only the strategy surface returns directives; observer ports have no
  control authority (camera / gate separation).
- **Emission records** (`emission.rs`) — one typed record per observable
  fact, five-rung vocabulary (read / installment / event / message /
  outcome). Ports: `TerminalPort` (display), `CapturePort` (JSONL).

Seam litmus: a decision needing pending-knowledge is the Handler's; a fact
detectable from the wire alone is the Driver's.

### Back pressure

Back pressure is outbound admission control (Send Buckets: Handler sizes the
pool, Driver spends before dialing), not read-pausing — so in-flight streams
always drain at wire speed and the stall clock never blames the peer for our
own throttling. The wire-level tools below remain the involuntary backstop:
kernel receive buffer, then the TCP (or HTTP/2 per-stream) flow-control
window. When the Driver's own pacing engages those, it annotates the stream
(`ReadPacingEngaged`) so the stall clock stands down.

### Capture files

`OPENAI_CAPTURE_DIR=<dir>` records one JSONL file per run: reads (timing
facts, stamped at read time), installments (with the read span they drew
from and assembly status), normalized events (linked to their installment),
normalization errors (written before the error propagates), residue, ledger
entries, and a per-turn trailer with verdict and counts. Statuses are
capture-time testimony; the read and installment layers are the facts a
replay re-derives against. **These files contain conversation content in the
clear** — treat them like an env file (`chmod 600`, never commit;
`.gitignore` excludes `captures/`). Verified: the API key appears in no
record.

### Failover

Set a `FAILOVER_OPENAI_*` block to give the caller a second endpoint. The
Handler learns only *that* an alternative exists; when the primary's retry
allowance is exhausted it returns the failover verdict; the caller — the one
component that constructs drivers — executes it. Decide-not-do, by
construction.

## Chunk 02d — to-spec: concurrency-ready, instrumented, hardened

Built to finished spec (chat is the smoke test, not a scope reduction).

### Concurrency model
`execute_turn` is `async` and every shared value is `Arc`/atomic; **no lock
guard is ever held across an `.await`**. Consequences:
- Many turns run concurrently via `tokio::spawn` (see `examples/stress.rs`) —
  N live streams on one thread, each parked at its own await.
- The runtime is pinned `current_thread` for clean single-thread aggregate
  time trials. Flipping to multi-core is **one line** in `src/main.rs`
  (`flavor = "current_thread"` → `"multi_thread"`); it compiles without a
  re-audit *because* the no-guard-across-await rule is held throughout. The
  build verifies this — the multi_thread variant compiles Send-clean.
- Per-stream `TurnState` (not a shared mutex) so concurrent turns never
  serialize on turn state — the first contention point the multi_thread flip
  would otherwise expose.

Cost note: `Arc`/`Mutex`/atomics carry a small per-op overhead vs their
single-thread cousins. Uncontended on one thread it is far below network
timing resolution; it is accepted deliberately as the price of the one-line
multi-core trial axis.

### The select! timer trio (Driver-owned, wire-detectable clocks)
Each read races three clocks in one `tokio::select!`:
- **stall** — per-read gap (loose first-byte backstop before byte one),
- **throughput floor** — EMA of *content* bytes/sec, evaluated per second,
  started at first byte (warm-up exempt); sustained starvation escalates.
  Keep-alive comments count as liveness, not throughput, so a comments-only
  stream fails as starved rather than passing as traffic.
- **overall deadline** — absolute turn ceiling (anti-slowloris backstop).

### Instrumentation (for bottleneck attribution)
Capture records now carry a `stream` id (de-interleave a multiplexed file),
plus scheduler samples: content throughput with an active-stream gauge,
select! wake causes, and capture-drop counts. Together these separate a
network-bound ceiling from a thread-cycle-bound or sink-bound one.

### Non-blocking emission bus
Observers (`TerminalSink`, `CaptureSink`) run on a dedicated bus task behind
a bounded channel. Emitting is a non-awaiting `try_send`; a full channel
drops records and counts them (surfaced as `capture_drop`). A slow or wedged
sink degrades capture — it can never back-pressure the wire. This is a
contract, enforced structurally.

### Strict decode & dirty-path completeness
- Frames are decoded strictly; an undecodable frame becomes a distinguished
  `undecodable_installment` record (byte count, error, hex prefix) — never a
  silent replacement-character substitution.
- The response is validated as `text/event-stream` before framing; a
  200-with-JSON-error or a proxy HTML page routes to the dirty path with its
  body intact (the vendor's stated error survives).
- `closed_with_no_content` is distinct from mid-answer truncation.
- Multiple choices (n>1) are rejected loudly rather than silently merged.
- A non-content channel on an installment (e.g. `tool_calls`) is surfaced as
  an `unhandled_channel` wire note, never dropped (tool calls have their own
  endpoint per design; this conduit only guarantees they are not lost).

### Cancellation
`execute_turn` takes a `CancellationToken`; a caller can abort an in-flight
turn cooperatively (console interrupt, or a harness).

### Explicit scope exclusions (ratified, not oversights)
- SSE reconnect / `Last-Event-ID` resumption — retry re-issues whole turns.
- Multiple choices per response — rejected, not handled.
- Tool-call *interpretation* — separate endpoint(s), separate Driver/Handler
  (treated as implied sub-agents); this conduit only refuses to lose them.
- Non-idempotent replay safety (double-execution of side-effecting tool
  calls) — a higher layer's concern, above this com stack.

## Review round (2026-09-06) — corrections to the 02d build

A fresh code review of the shipped 02d found and fixed:

- **Stall timer was dead after first byte** (regression from the `select!`
  restructure). The stall arm used a *relative* `timeout()` rebuilt every
  wakeup; the 1 Hz throughput tick `continue`d and thereby reset the stall
  clock every second, so it could never fire. Now an *absolute*
  `sleep_until(last_read + window)` deadline, which is rebuild-safe.
  Proven: 3 s stall trips at 3 s; previously ran unbounded.
- **Unbounded header wait.** `connect_timeout` covers only the dial; a peer
  that accepts TCP and never answers hung forever. The header wait is now
  bounded by the first-byte backstop; error-body reads by the stall window.
- **Records lost at exit.** `main` returned before the bus writer drained its
  channel, so a turn's final `stream_end` / `turn_trailer` could be missing
  from its own capture. `main` now drops all senders and awaits the writer.
- Added unit tests: strict-decode `Undecodable` at the parser; `EmptyStream`
  distinct from `Truncated` in the machine. 29 tests.

Behavior notes clarified by the review (not bugs, but worth knowing):

- **Throughput starvation time-to-trip** is *EMA decay time + sustained
  window*, not the sustained window alone. From a healthy rate, the EMA
  (α=0.4) takes ~8–10 s to fall below a 1 B/s floor, then the sustained
  clock starts. Default settings trip a silent stream at roughly 30 s.
  This is by design (a slow starter must not false-trip) — set
  `OPENAI_THROUGHPUT_SUSTAINED_SECS` with the decay in mind.
- **Retry allowance is per endpoint-series, not per user turn.** A failover
  re-issues on the alternative with a fresh allowance. The "single budget"
  guarantee holds within one endpoint's attempt series.
- **No endpoint-health memory yet.** After a failover, the next turn tries
  the primary again. Sticky failover / circuit breaking is a Handler
  strategy decision (pending-knowledge territory) and is deliberately not
  implemented until ruled.
