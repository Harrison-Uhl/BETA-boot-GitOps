//! # rar-chat — OpenAI-compatible streaming conduit (Driver + Event Handler)
//!
//! POC of the RAR low-level conversation stack. First exercised as terminal
//! chat (smoke test), but built to finished spec: the Driver/Event-Handler
//! split, concurrency model, and hardening are intended to carry unchanged
//! to an agent-harness local end, with only the Event Handler swapped.
//!
//! ## Module graph (wire → policy)
//! ```text
//!   network bytes
//!       │  sse.rs        installment reassembly, bounded, strict-decode
//!       ▼
//!   installments ─────► openai.rs   Driver: wire custody, select! clocks,
//!       │                            retries, failover pass-through
//!       ▼
//!   (forwarded per installment)
//!       │  event_handler.rs  swappable: normalize → machine, Wire Strategy,
//!       ▼                     per-stream state, fan-out to the bus
//!   normalized events / outcome
//!       │  machine.rs    completion semantics (pure, tested)
//!       ▼
//!   emission.rs   typed records + non-blocking bus  ──► ports.rs  sinks
//!   env_file.rs   config source (file → process env)
//!   driver.rs     shared vocabulary: events, outcome, faults
//! ```
//!
//! ## Invariant registry (session-minted DRAFT series `CHAT`; renumber at
//! ## registry merge per the standing ADR/INDEX allocation law)
//!
//! Configuration & secrets
//! - INV-CHAT-01  No secret (API key) in any log, error, telemetry, or
//!                capture record. Verified by leak-grep in the trials.
//! - INV-CHAT-02  Config lookup precedence is exactly env-file then process
//!                environment; no other source.
//!
//! Completion semantics
//! - INV-CHAT-03  Stream events and call outcome are distinct types; no
//!                consumer treats an event as a verdict.
//! - INV-CHAT-04  A ChatOutcome exists only if a finish_reason was received;
//!                [DONE] and EOF carry no completion authority.
//! - INV-CHAT-05  Outcome content is the byte-exact, in-order concatenation
//!                of TextDelta events; the machine never edits testimony.
//!
//! Wire framing & decode
//! - INV-CHAT-06  The SSE reassembly buffer is bounded; overflow is a typed
//!                fault, never unbounded growth.
//! - INV-CHAT-07  UTF-8 is evaluated only on complete installments, never on
//!                raw reads.
//! - INV-CHAT-12  Decode is strict: an undecodable installment becomes a
//!                distinguished record, never a lossy substitution. (02d)
//! - INV-CHAT-13  The response is validated as text/event-stream before
//!                framing; a non-stream body routes to the dirty path with
//!                its content intact. (02d)
//! - INV-CHAT-14  Nothing on the wire is silently eaten: malformed
//!                installments, residue, and unhandled channels (e.g.
//!                tool_calls) are all surfaced as distinguished records. (02d)
//!
//! Liveness (Driver-owned, wire-detectable — the seam litmus)
//! - INV-CHAT-08  Every exit passes StreamMachine::finalize.
//! - INV-CHAT-09  Liveness is timer-enforced: stall (per-read gap), first-
//!                byte backstop, throughput floor (content-byte EMA from
//!                first byte, warm-up exempt), and overall deadline. (02d)
//!
//! Component boundary & authority
//! - INV-CHAT-10  Handlers refine and decide; observation sinks receive
//!                records and return nothing (camera/gate split). Only the
//!                strategy surface returns directives.
//! - INV-CHAT-11  Retry authority is a single per-turn allowance minted by
//!                the Handler and spent mechanically by the Driver; the
//!                failover verdict is opaque to the Driver (escalate-by-
//!                return; only the caller constructs drivers).
//!
//! Concurrency (02d)
//! - INV-CHAT-15  execute_turn is async; all shared state is Arc/atomic and
//!                NO lock guard is held across an .await. The multi-thread
//!                runtime flip is therefore one line. Verified: the
//!                multi_thread build compiles Send-clean.
//! - INV-CHAT-16  Per-stream TurnState is owned, never shared — concurrent
//!                turns do not serialize on turn state.
//! - INV-CHAT-17  The emission bus is non-blocking by contract: emit is a
//!                non-awaiting try_send; a full bus drops-and-counts. A slow
//!                sink degrades capture, never the wire.
//!
//! ## Key decisions (DEC-CHAT; abbreviated — see module headers)
//! 01 no dotenv dep · 02 env-file syntax · 03 faults as hand-written enum ·
//! 04 [DONE] carries no completion authority · 05 tolerate-and-flag late
//! text · 06 dual frame terminators · 07 ignore non-data SSE fields · 08
//! usage optional per vendor · 09 driver named for the contract not a vendor
//! · 10 model has no default · 11 pop-and-retype on fault (uncommitted user
//! line) · 12 (02d) unbiased select — no false-starvation bias on the
//! anti-slowloris guard · 13 (02d) multiple choices rejected loudly, not
//! merged · 14 (02d) tool-call interpretation is a separate endpoint; this
//! conduit only guarantees non-loss.
//!
//! ## Ratified scope exclusions (not oversights)
//! SSE reconnect / Last-Event-ID (retry re-issues whole turns); multi-choice
//! responses (rejected); tool-call interpretation (separate Driver/Handler,
//! implied sub-agents); non-idempotent replay safety (higher layer).

pub mod driver;
pub mod emission;
pub mod env_file;
pub mod event_handler;
pub mod machine;
pub mod openai;
pub mod ports;
pub mod sse;
