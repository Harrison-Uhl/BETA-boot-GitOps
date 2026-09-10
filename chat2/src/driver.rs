//! # driver — vendor-neutral domain types for the LLM streaming seam
//!
//! ## Role in the system
//! This module is the *vocabulary* of the Driver boundary: the typed events a
//! streaming chat call emits, the outcome a completed call resolves to, and
//! the typed faults an incomplete call resolves to. The transport that
//! produces these lives in `openai.rs`; the policy that consumes them lives
//! in `machine.rs` and `main.rs`. Nothing here does I/O.
//!
//! ## Invariants
//! - INV-CHAT-03: Stream *events* and call *outcome* are distinct types.
//!   Events are testimony arriving mid-flight; the outcome is the settled
//!   verdict of the whole call. No consumer may treat an event as a verdict.
//! - INV-CHAT-04: A `ChatOutcome` exists only if a `finish_reason` was
//!   received. finish_reason — not `[DONE]`, not end-of-stream — is the
//!   completion criterion (ruling from the chunk-01 review cycle).
//!
//! ## Decisions
//! - DEC-CHAT-03: Faults are a hand-written enum implementing
//!   `std::error::Error` (no `thiserror` dependency), carried under `anyhow`
//!   and recoverable by downcast. This is a POC stand-in for the full
//!   membrane status enum; it deliberately distinguishes handler-directed
//!   abort from truncation from transport stall — conflating those was a
//!   defect in the reviewed Gemini draft.

use serde::Serialize;
use std::fmt;

/// One normalized event from the vendor stream, in arrival order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "ev", content = "v", rename_all = "snake_case")]
pub enum DriverEvent {
    /// A fragment of assistant text.
    TextDelta(String),
    /// The vendor's stated reason the generation stopped ("stop", "length",
    /// "content_filter", ...). INV-CHAT-04: receipt of this event is what
    /// makes a call completable.
    Finish(String),
    /// Token accounting, when the endpoint supplies it. Optional by design:
    /// `stream_options.include_usage` is an OpenAI extension whose support
    /// varies per model on OpenAI-compatible endpoints (verified for NVIDIA
    /// NIM 2026-09-05); absence is never an error.
    Usage {
        prompt_tokens: u32,
        completion_tokens: u32,
        total_tokens: u32,
    },
    /// The `data: [DONE]` sentinel. Protocol bookkeeping only — carries no
    /// completion authority (INV-CHAT-04).
    Done,
}

/// The settled result of one successful streaming chat call.
///
/// `content` is the full assistant reply assembled from every `TextDelta`.
/// The caller — not the event handler — appends this to conversation history,
/// and only on `Ok` (fix for the reviewed draft's lost-history defect).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatOutcome {
    pub content: String,
    pub finish_reason: String,
    pub usage: Option<(u32, u32, u32)>, // (prompt, completion, total)
}

/// Typed faults for calls that did not settle into a `ChatOutcome`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriverFault {
    /// The byte stream ended before any `finish_reason` arrived. The partial
    /// text length is reported for diagnostics; the partial text itself is
    /// NOT committed anywhere (INV-CHAT-03/04).
    Truncated { partial_chars: usize },
    /// The registered event handler returned `AbortStream`. Operator intent,
    /// not a transport failure — kept distinct from `Truncated`.
    Aborted { partial_chars: usize },
    /// No bytes arrived within the stall window. A silent connection is
    /// indistinguishable from a dead one; the timer, not the peer, must say so.
    Stalled { after_secs: u64 },
    /// The SSE reassembly buffer exceeded its bound (defense against a
    /// broken or hostile peer that never terminates a frame).
    BufferOverflow { limit_bytes: usize },
    /// Stream accepted but closed without producing any content frame.
    EmptyStream,
    /// The peer sent something outside the OpenAI-compatible contract.
    Protocol(String),
}

impl fmt::Display for DriverFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DriverFault::Truncated { partial_chars } => write!(
                f,
                "stream truncated: connection ended before finish_reason ({partial_chars} chars of uncommitted partial output discarded)"
            ),
            DriverFault::Aborted { partial_chars } => write!(
                f,
                "stream aborted by handler directive ({partial_chars} chars of uncommitted partial output discarded)"
            ),
            DriverFault::Stalled { after_secs } => {
                write!(f, "stream stalled: no bytes for {after_secs}s")
            }
            DriverFault::BufferOverflow { limit_bytes } => {
                write!(f, "SSE buffer exceeded {limit_bytes} bytes without a frame terminator")
            }
            DriverFault::EmptyStream => write!(f, "stream accepted but produced no content before closing"),
            DriverFault::Protocol(msg) => write!(f, "protocol violation: {msg}"),
        }
    }
}

impl std::error::Error for DriverFault {}
