//! # machine — pure completion-semantics state machine
//!
//! ## Role in the system
//! The one place where "did this call actually complete?" is decided. The
//! transport (`openai.rs`) feeds every `DriverEvent` through here and, at
//! end of stream, asks for the verdict. Because this type does no I/O, the
//! completion rules are unit-testable without a network — the test suite
//! drives it with hand-built event sequences, including the truncation
//! cases a live endpoint won't produce on demand.
//!
//! ## Invariants
//! - INV-CHAT-04 (enforced here): a `ChatOutcome` is issued only when a
//!   `finish_reason` was observed. Every other ending is a typed fault.
//! - INV-CHAT-05: `content` returned in the outcome is byte-for-byte the
//!   concatenation of the `TextDelta` events, in order. The machine never
//!   edits testimony.
//!
//! ## Decisions
//! - DEC-CHAT-04: `[DONE]` without a prior finish_reason resolves to
//!   `Truncated`, not success — the sentinel proves the peer chose to stop,
//!   not that generation completed. finish_reason followed by EOF *without*
//!   `[DONE]` resolves to success — the criterion was met; the missing
//!   sentinel is noted by the transport as telemetry, not failure.
//! - DEC-CHAT-05: Text arriving *after* finish_reason is appended and the
//!   anomaly recorded as a protocol note rather than a fault: tolerate on
//!   receive, but keep the evidence.

use crate::driver::{ChatOutcome, DriverEvent, DriverFault};

/// Why the transport stopped feeding events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndCause {
    /// The byte stream ended (peer closed, or `[DONE]` was consumed).
    StreamEnded,
    /// The handler returned `AbortStream`.
    HandlerAbort,
}

#[derive(Debug, Default)]
pub struct StreamMachine {
    content: String,
    finish_reason: Option<String>,
    usage: Option<(u32, u32, u32)>,
    saw_done: bool,
    /// Non-fatal contract oddities observed (DEC-CHAT-05); surfaced to the
    /// transport for logging.
    pub anomalies: Vec<String>,
}

impl StreamMachine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Accumulated character count — used for fault diagnostics.
    pub fn partial_chars(&self) -> usize {
        self.content.chars().count()
    }

    /// True once the completion criterion (INV-CHAT-04) has been met.
    pub fn is_completable(&self) -> bool {
        self.finish_reason.is_some()
    }

    pub fn saw_done(&self) -> bool {
        self.saw_done
    }

    /// Fold one event into the pending verdict.
    pub fn feed(&mut self, ev: &DriverEvent) {
        match ev {
            DriverEvent::TextDelta(t) => {
                if self.finish_reason.is_some() {
                    self.anomalies
                        .push("text delta arrived after finish_reason".to_string());
                }
                self.content.push_str(t); // INV-CHAT-05
            }
            DriverEvent::Finish(r) => {
                if let Some(prev) = &self.finish_reason {
                    self.anomalies.push(format!(
                        "second finish_reason '{r}' after '{prev}' — keeping the first"
                    ));
                } else {
                    self.finish_reason = Some(r.clone());
                }
            }
            DriverEvent::Usage {
                prompt_tokens,
                completion_tokens,
                total_tokens,
            } => {
                self.usage = Some((*prompt_tokens, *completion_tokens, *total_tokens));
            }
            DriverEvent::Done => {
                self.saw_done = true;
            }
        }
    }

    /// The verdict. Consumes the machine: events after the verdict are a
    /// caller bug the type system now prevents.
    pub fn finalize(self, cause: EndCause) -> Result<ChatOutcome, DriverFault> {
        let partial_chars = self.content.chars().count();
        match (cause, self.finish_reason) {
            (EndCause::HandlerAbort, _) => Err(DriverFault::Aborted { partial_chars }),
            (EndCause::StreamEnded, Some(finish_reason)) => Ok(ChatOutcome {
                content: self.content,
                finish_reason,
                usage: self.usage,
            }),
            // DEC-CHAT-04: no finish_reason means no completion. Distinguish
            // "produced nothing" from "cut off mid-answer" for honest display.
            (EndCause::StreamEnded, None) => {
                if partial_chars == 0 {
                    Err(DriverFault::EmptyStream)
                } else {
                    Err(DriverFault::Truncated { partial_chars })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::DriverEvent as E;

    fn deltas(words: &[&str]) -> Vec<E> {
        words.iter().map(|w| E::TextDelta(w.to_string())).collect()
    }

    #[test]
    fn sunny_path_finish_then_done() {
        let mut m = StreamMachine::new();
        for e in deltas(&["Hello", " world"]) {
            m.feed(&e);
        }
        m.feed(&E::Finish("stop".into()));
        m.feed(&E::Usage { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 });
        m.feed(&E::Done);
        let out = m.finalize(EndCause::StreamEnded).unwrap();
        assert_eq!(out.content, "Hello world");
        assert_eq!(out.finish_reason, "stop");
        assert_eq!(out.usage, Some((3, 2, 5)));
    }

    #[test]
    fn eof_before_finish_reason_is_truncated() {
        // The chunk-01 P0, now pinned by a test: connection dies mid-answer.
        let mut m = StreamMachine::new();
        for e in deltas(&["partial", " answer"]) {
            m.feed(&e);
        }
        let err = m.finalize(EndCause::StreamEnded).unwrap_err();
        assert_eq!(err, DriverFault::Truncated { partial_chars: 14 });
    }

    #[test]
    fn eof_with_no_content_is_empty_stream_not_truncated() {
        // "Accepted but produced nothing" is a distinct verdict from a
        // mid-answer cut (02d review fix).
        let m = StreamMachine::new();
        let err = m.finalize(EndCause::StreamEnded).unwrap_err();
        assert_eq!(err, DriverFault::EmptyStream);
    }

    #[test]
    fn done_without_finish_reason_is_truncated() {
        // DEC-CHAT-04: [DONE] carries no completion authority.
        let mut m = StreamMachine::new();
        m.feed(&E::TextDelta("half".into()));
        m.feed(&E::Done);
        let err = m.finalize(EndCause::StreamEnded).unwrap_err();
        assert_eq!(err, DriverFault::Truncated { partial_chars: 4 });
    }

    #[test]
    fn finish_reason_without_done_is_success() {
        // DEC-CHAT-04, other half: criterion met, sentinel missing.
        let mut m = StreamMachine::new();
        m.feed(&E::TextDelta("full".into()));
        m.feed(&E::Finish("stop".into()));
        let out = m.finalize(EndCause::StreamEnded).unwrap();
        assert_eq!(out.content, "full");
        assert!(out.usage.is_none(), "usage must be optional");
    }

    #[test]
    fn handler_abort_is_aborted_not_truncated() {
        let mut m = StreamMachine::new();
        m.feed(&E::TextDelta("cut".into()));
        let err = m.finalize(EndCause::HandlerAbort).unwrap_err();
        assert_eq!(err, DriverFault::Aborted { partial_chars: 3 });
    }

    #[test]
    fn late_text_is_kept_and_flagged() {
        // DEC-CHAT-05.
        let mut m = StreamMachine::new();
        m.feed(&E::Finish("stop".into()));
        m.feed(&E::TextDelta("straggler".into()));
        assert_eq!(m.anomalies.len(), 1);
        let out = m.finalize(EndCause::StreamEnded).unwrap();
        assert_eq!(out.content, "straggler");
    }
}
