//! # event_handler — swappable refinement + strategy, per-stream state
//!
//! Finished-spec changes from the chunk-02c shape:
//!  - Per-stream `TurnState` (no shared `Mutex<TurnState>` singleton): each
//!    concurrent stream carries its own state object, created by
//!    `begin_stream`. N concurrent turns never serialize through one lock,
//!    which is also the throughput fix under the aggregate metric — a shared
//!    turn-state mutex would be the first contention point on the
//!    multi_thread flip.
//!  - Strategy counters (`send_buckets`, retry allowance) remain shared and
//!    atomic — that sharing is intentional (one admission pool across
//!    streams) and lock-free.
//!  - `Send + Sync` throughout; the bus handle is cloneable and non-blocking,
//!    so nothing here holds a guard across an await (there are no awaits in
//!    this module at all — it is pure synchronous refinement/decision).
//!
//! Swappability: this concrete `EventHandler` is the troubleshooting/chat
//! recipient — it emits maximum wire+message detail. A harness recipient is
//! a different type implementing the same forwarding entry points, wanting
//! whole outcomes and reading finish_reason per its own policy. The Driver
//! is unchanged across the swap.

use crate::driver::{ChatOutcome, DriverEvent, DriverFault};
use crate::emission::{
    EmissionBus, EmissionRecord, InstallmentKind, InstallmentStatus, LedgerEntry, StreamId,
};
use crate::machine::{EndCause, StreamMachine};
use serde::Deserialize;
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowDirective { Continue, StopComplete, Abort }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultDirective { RetrySameEndpoint, Failover, GiveUp }

// ---- OpenAI-compatible normalization (Handler-side refinement) -----------

#[derive(Deserialize)]
struct Chunk {
    #[serde(default)]
    choices: Vec<Choice>,
    usage: Option<Usage>,
    error: Option<serde_json::Value>,
}
#[derive(Deserialize)]
struct Choice {
    #[serde(default)]
    delta: Delta,
    finish_reason: Option<String>,
}
#[derive(Deserialize, Default)]
struct Delta {
    content: Option<String>,
    /// Present but not interpreted here (tool calls have their own endpoint
    /// per ruling). Detected so it can be surfaced, never silently dropped.
    tool_calls: Option<serde_json::Value>,
}
#[derive(Deserialize)]
struct Usage {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
}

/// Outcome of normalizing one data installment.
pub struct Normalized {
    pub events: Vec<DriverEvent>,
    /// Non-content channels seen (e.g. "tool_calls") — surfaced as Wire
    /// notes by the caller, never dropped.
    pub unhandled_channels: Vec<String>,
    /// n>1 multi-choice: loud scope-out signal (not silent choice[0]).
    pub multiple_choices: Option<usize>,
}

pub fn normalize(data: &str) -> Result<Normalized, String> {
    if data.trim() == "[DONE]" {
        return Ok(Normalized { events: vec![DriverEvent::Done], unhandled_channels: vec![], multiple_choices: None });
    }
    let chunk: Chunk = serde_json::from_str(data).map_err(|e| format!("unparseable chunk: {e}"))?;
    if let Some(err) = chunk.error {
        return Err(format!("server error in stream: {err}"));
    }
    let multiple_choices = (chunk.choices.len() > 1).then_some(chunk.choices.len());
    let mut events = Vec::new();
    let mut unhandled = Vec::new();
    for c in chunk.choices {
        if c.delta.tool_calls.is_some() {
            unhandled.push("tool_calls".to_string());
        }
        if let Some(t) = c.delta.content {
            if !t.is_empty() {
                events.push(DriverEvent::TextDelta(t));
            }
        }
        if let Some(r) = c.finish_reason {
            events.push(DriverEvent::Finish(r));
        }
    }
    if let Some(u) = chunk.usage {
        events.push(DriverEvent::Usage {
            prompt_tokens: u.prompt_tokens,
            completion_tokens: u.completion_tokens,
            total_tokens: u.total_tokens,
        });
    }
    Ok(Normalized { events, unhandled_channels: unhandled, multiple_choices })
}

/// Per-stream refinement state. Owned by exactly one stream; never shared,
/// so no lock is needed around it.
pub struct StreamState {
    pub stream_id: StreamId,
    pub attempt: u32,
    machine: StreamMachine,
    pub installments: u64,
    pub events: u64,
    pub norm_errors: u64,
    pub reads: u64,
    pub content_bytes: u64,
    pub saw_any_content: bool,
}

impl StreamState {
    fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id, attempt: 0, machine: StreamMachine::new(),
            installments: 0, events: 0, norm_errors: 0, reads: 0,
            content_bytes: 0, saw_any_content: false,
        }
    }
    pub fn begin_attempt(&mut self) -> u32 {
        self.attempt += 1;
        self.machine = StreamMachine::new();
        self.attempt
    }
}

pub enum TurnResolution {
    Committed(ChatOutcome),
    Fault(DriverFault),
}

/// The component. Long-lived; shared strategy counters are atomic. Per-turn
/// state is handed out via `begin_stream`, not held here.
pub struct EventHandler {
    bus: EmissionBus,
    send_buckets: AtomicU32,
    retry_allowance_per_turn: u32,
    knows_failover_exists: bool,
}

impl EventHandler {
    pub fn new(bus: EmissionBus, send_buckets: u32, retry_allowance_per_turn: u32, knows_failover_exists: bool) -> Self {
        Self { bus, send_buckets: AtomicU32::new(send_buckets), retry_allowance_per_turn, knows_failover_exists }
    }

    pub fn bus(&self) -> &EmissionBus {
        &self.bus
    }
    pub fn knows_failover_exists(&self) -> bool {
        self.knows_failover_exists
    }
    pub fn retry_allowance_per_turn(&self) -> u32 {
        self.retry_allowance_per_turn
    }

    /// Create per-stream state and mint this turn's retry allowance ledger.
    pub fn begin_stream(&self, stream_id: StreamId) -> StreamState {
        self.bus.emit(EmissionRecord::Ledger {
            stream: stream_id, attempt: 0,
            entry: LedgerEntry::RetryAllowanceGranted { total: self.retry_allowance_per_turn },
        });
        StreamState::new(stream_id)
    }

    /// Driver spends one Send Bucket before dialing. Zero-bucket rule: fail
    /// fast (recorded), never a silent wait.
    pub fn acquire_send_bucket(&self, stream: StreamId, attempt: u32) -> bool {
        match self.send_buckets.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_sub(1)) {
            Ok(v) => {
                self.bus.emit(EmissionRecord::Ledger { stream, attempt, entry: LedgerEntry::SendBucketSpent { available_after: v - 1 } });
                true
            }
            Err(_) => {
                self.bus.emit(EmissionRecord::Ledger { stream, attempt, entry: LedgerEntry::SendBucketDenied });
                false
            }
        }
    }

    pub fn return_send_bucket(&self, stream: StreamId, attempt: u32) {
        let v = self.send_buckets.fetch_add(1, Ordering::SeqCst);
        self.bus.emit(EmissionRecord::Ledger { stream, attempt, entry: LedgerEntry::SendBucketReturned { available_after: v + 1 } });
    }

    /// Wire-fault strategy: draw against this stream's remaining allowance,
    /// then failover if alternatives are known, else give up. Allowance is
    /// tracked on the StreamState by the caller passing remaining count.
    pub fn on_wire_fault(&self, stream: StreamId, attempt: u32, remaining_allowance: &mut u32) -> FaultDirective {
        if *remaining_allowance > 0 {
            *remaining_allowance -= 1;
            self.bus.emit(EmissionRecord::Ledger { stream, attempt, entry: LedgerEntry::RetryAllowanceSpent { remaining: *remaining_allowance } });
            return FaultDirective::RetrySameEndpoint;
        }
        if self.knows_failover_exists { FaultDirective::Failover } else { FaultDirective::GiveUp }
    }

    /// Refine one valid data/comment installment into events; feed the
    /// machine; emit records; return a flow directive. Pure/sync — no await,
    /// no lock held anywhere.
    pub fn on_installment(
        &self,
        st: &mut StreamState,
        installment_seq: u64,
        kind: InstallmentKind,
        payload: &str,
        _status: InstallmentStatus,
    ) -> Result<FlowDirective, DriverFault> {
        let stream = st.stream_id;
        let attempt = st.attempt;
        if kind == InstallmentKind::Comment {
            return Ok(FlowDirective::Continue);
        }
        let norm = match normalize(payload) {
            Ok(n) => n,
            Err(detail) => {
                st.norm_errors += 1;
                self.bus.emit(EmissionRecord::NormalizationError { stream, attempt, installment_seq, detail: detail.clone() });
                return Err(DriverFault::Protocol(detail));
            }
        };
        if let Some(n) = norm.multiple_choices {
            // Loud scope-out: reject rather than silently merging choices.
            let detail = format!("multiple choices (n={n}) not supported");
            self.bus.emit(EmissionRecord::NormalizationError { stream, attempt, installment_seq, detail: detail.clone() });
            return Err(DriverFault::Protocol(detail));
        }
        for ch in &norm.unhandled_channels {
            self.bus.emit(EmissionRecord::Wire { stream, attempt, note: crate::emission::WireNote::UnhandledChannel { channel: ch.clone() } });
        }
        let mut saw_done = false;
        st.installments += 1;
        st.events += norm.events.len() as u64;
        for ev in &norm.events {
            st.machine.feed(ev);
            if let DriverEvent::TextDelta(t) = ev {
                st.content_bytes += t.len() as u64;
                st.saw_any_content = true;
            }
            if matches!(ev, DriverEvent::Done) {
                saw_done = true;
            }
        }
        for ev in &norm.events {
            self.bus.emit(EmissionRecord::NormalizedEvent { stream, attempt, installment_seq, event: ev.clone() });
        }
        if saw_done {
            return Ok(FlowDirective::StopComplete);
        }
        Ok(FlowDirective::Continue)
    }

    /// Drain machine anomalies and resolve this attempt.
    pub fn finalize_attempt(&self, st: &mut StreamState, cause: EndCause) -> TurnResolution {
        let stream = st.stream_id;
        let attempt = st.attempt;
        let machine = std::mem::replace(&mut st.machine, StreamMachine::new());
        for note in &machine.anomalies {
            self.bus.emit(EmissionRecord::Anomaly { stream, attempt, note: note.clone() });
        }
        match machine.finalize(cause) {
            Ok(outcome) => TurnResolution::Committed(outcome),
            Err(fault) => TurnResolution::Fault(fault),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn silent_handler() -> EventHandler {
        // A bus with a no-op sink for pure-logic tests.
        let (bus, _w) = EmissionBus::spawn(vec![], 64);
        EventHandler::new(bus, 4, 2, true)
    }

    #[test]
    fn done_maps_to_done() {
        assert_eq!(normalize("[DONE]").unwrap().events, vec![DriverEvent::Done]);
    }
    #[test]
    fn delta_and_finish_in_one_chunk() {
        let n = normalize(r#"{"choices":[{"delta":{"content":"hi"},"finish_reason":"stop"}]}"#).unwrap();
        assert_eq!(n.events, vec![DriverEvent::TextDelta("hi".into()), DriverEvent::Finish("stop".into())]);
    }
    #[test]
    fn tool_calls_channel_is_surfaced_not_dropped() {
        let n = normalize(r#"{"choices":[{"delta":{"tool_calls":[{"index":0}]}}]}"#).unwrap();
        assert_eq!(n.unhandled_channels, vec!["tool_calls".to_string()]);
    }
    #[test]
    fn multiple_choices_flagged() {
        let n = normalize(r#"{"choices":[{"delta":{"content":"a"}},{"delta":{"content":"b"}}]}"#).unwrap();
        assert_eq!(n.multiple_choices, Some(2));
    }

    #[tokio::test]
    async fn allowance_single_budget_then_failover() {
        let h = silent_handler();
        let mut rem = 2u32;
        assert_eq!(h.on_wire_fault(1, 1, &mut rem), FaultDirective::RetrySameEndpoint);
        assert_eq!(h.on_wire_fault(1, 2, &mut rem), FaultDirective::RetrySameEndpoint);
        assert_eq!(h.on_wire_fault(1, 3, &mut rem), FaultDirective::Failover);
    }

    #[tokio::test]
    async fn zero_buckets_fail_fast() {
        let (bus, _w) = EmissionBus::spawn(vec![], 64);
        let h = EventHandler::new(bus, 1, 0, false);
        assert!(h.acquire_send_bucket(1, 1));
        assert!(!h.acquire_send_bucket(1, 1));
        h.return_send_bucket(1, 1);
        assert!(h.acquire_send_bucket(1, 1));
    }
}
