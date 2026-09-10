//! # emission — typed records + async, non-blocking emission bus
//!
//! Two finished-spec properties this module now guarantees:
//!  1. Every observable fact is one typed `EmissionRecord` (five-rung
//!     vocabulary: read / installment / event / message / outcome).
//!  2. The bus is **non-blocking by contract**: an observer physically
//!     cannot back-pressure the data plane. Records are handed to a bounded
//!     channel; if a slow sink fills it, records are dropped and the drop is
//!     itself counted and reported — the camera degrades, the wire does not.
//!
//! Instrumentation: records carry enough to locate a bottleneck as
//! network-bound vs thread-cycle-bound vs lock/sink-bound — active-stream
//! gauge, scheduler poll ticks, per-record enqueue latency, drop counts.

use crate::driver::DriverEvent;
// SchedNote used in emit latency reporting
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

pub const CAPTURE_FORMAT_VERSION: u32 = 2;

/// Stable identifier for one concurrent stream (conversation/turn attempt),
/// so a multiplexed capture file can be de-interleaved per stream.
pub type StreamId = u64;

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "rec", rename_all = "snake_case")]
pub enum EmissionRecord {
    Read { stream: StreamId, attempt: u32, seq: u64, at_ms: u64, wall_unix_ms: u64, bytes: usize, gap_ms: u64 },
    Installment {
        stream: StreamId, attempt: u32, seq: u64, first_read: u64, last_read: u64,
        bytes: usize, kind: InstallmentKind, payload: String, status: InstallmentStatus,
    },
    NormalizedEvent { stream: StreamId, attempt: u32, installment_seq: u64, event: DriverEvent },
    NormalizationError { stream: StreamId, attempt: u32, installment_seq: u64, detail: String },
    /// A complete frame that would not decode as UTF-8 — carried as evidence
    /// rather than silently substituted (strict-decode rule).
    UndecodableInstallment { stream: StreamId, attempt: u32, seq: u64, bytes: usize, error: String, hex_prefix: String },
    Residue { stream: StreamId, attempt: u32, bytes: usize, lossy_text: String },
    StreamEnd { stream: StreamId, attempt: u32, cause: StreamEndCause, unconsumed_buffer_bytes: usize },
    Wire { stream: StreamId, attempt: u32, note: WireNote },
    Ledger { stream: StreamId, attempt: u32, entry: LedgerEntry },
    Anomaly { stream: StreamId, attempt: u32, note: String },
    /// Scheduler/throughput instrumentation, sampled in the select! loop.
    Scheduler { stream: StreamId, attempt: u32, note: SchedNote },
    TurnTrailer {
        stream: StreamId, format_version: u32, attempts: u32, verdict: TurnVerdictRecord,
        reads: u64, installments: u64, events: u64, normalization_errors: u64,
        select_polls: u64, capture_policy: &'static str,
    },
    /// Emitted by the bus writer itself when records were dropped under load.
    CaptureDrop { dropped: u64 },
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstallmentKind { Data, Comment }

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstallmentStatus { Complete, RecoveredTail }

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StreamEndCause {
    DoneSentinelConsumed,
    PeerClosed,
    ClosedWithNoContent,
    StallTimeout { window_secs: u64 },
    ThroughputStarved { floor_bps: u64, sustained_secs: u64 },
    OverallDeadline { limit_secs: u64 },
    FirstByteBackstop { window_secs: u64 },
    TransportError { detail: String },
    HandlerDirected { directive: String },
    BufferOverflow { limit_bytes: usize },
    HttpError { status: u16, body: String },
    NotAnEventStream { content_type: String, body_prefix: String },
    ConnectError { detail: String },
    MultipleChoicesUnsupported { n: usize },
    Cancelled,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WireNote {
    Ttft { ms: u64 },
    InterReadGap { ms: u64, bytes: usize },
    KeepAliveComment,
    EofWithoutDoneSentinel,
    ReadPacingEngaged,
    /// Non-content channel present on an installment (e.g. tool_calls) that
    /// this conduit does not interpret — surfaced, never dropped.
    UnhandledChannel { channel: String },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchedNote {
    /// Sampled throughput of the content channel over the EMA window.
    ContentThroughput { bytes_per_sec: u64, active_streams: u64 },
    /// One select! wakeup and which arm won (for bottleneck attribution).
    SelectWake { arm: &'static str },
    /// Record enqueue latency onto the bus (sink-bound detector).
    EmitLatencyUs { us: u64 },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LedgerEntry {
    SendBucketSpent { available_after: u32 },
    SendBucketReturned { available_after: u32 },
    SendBucketDenied,
    RetryAllowanceGranted { total: u32 },
    RetryAllowanceSpent { remaining: u32 },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TurnVerdictRecord {
    Committed { finish_reason: String, chars: usize, usage: Option<(u32, u32, u32)> },
    Failed { fault: String },
    FailoverDirected { after_attempts: u32 },
}

/// Sink side: a real observer. Runs on the bus writer task, never in the
/// read loop — so a slow sink cannot stall the wire.
pub trait ObserverSink: Send {
    fn write(&mut self, rec: &EmissionRecord);
}

/// The non-blocking handle held by producers (Driver/Handler). Cloneable and
/// `Send`; emitting is a bounded try_send that never awaits and never blocks.
#[derive(Clone)]
pub struct EmissionBus {
    tx: mpsc::Sender<EmissionRecord>,
    dropped: Arc<AtomicU64>,
    capacity: usize,
    /// Rolling max enqueue latency (µs) since last report, and a counter to
    /// pace reporting. These measure the bus itself — the one shared
    /// synchronization point every stream funnels through, hence the most
    /// likely internal bottleneck under concurrency.
    emit_max_us: Arc<AtomicU64>,
    emit_count: Arc<AtomicU64>,
}

impl EmissionBus {
    /// Spawn the writer task fanning records to sinks. Returns the producer
    /// handle plus a JoinHandle for shutdown.
    pub fn spawn(mut sinks: Vec<Box<dyn ObserverSink>>, capacity: usize) -> (Self, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<EmissionRecord>(capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let dropped_w = dropped.clone();
        let writer = tokio::spawn(async move {
            let mut last_drop_report = 0u64;
            while let Some(rec) = rx.recv().await {
                for s in sinks.iter_mut() {
                    s.write(&rec);
                }
                // Periodically surface accumulated drops as a real record.
                let d = dropped_w.load(Ordering::Relaxed);
                if d != last_drop_report {
                    last_drop_report = d;
                    let drop_rec = EmissionRecord::CaptureDrop { dropped: d };
                    for s in sinks.iter_mut() {
                        s.write(&drop_rec);
                    }
                }
            }
        });
        (Self { tx, dropped, capacity, emit_max_us: Arc::new(AtomicU64::new(0)), emit_count: Arc::new(AtomicU64::new(0)) }, writer)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Non-blocking emit. On a full channel the record is dropped and
    /// counted — the wire is never back-pressured by a slow sink.
    pub fn emit(&self, rec: EmissionRecord) {
        let t0 = std::time::Instant::now();
        if self.tx.try_send(rec).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        let us = t0.elapsed().as_micros() as u64;
        self.emit_max_us.fetch_max(us, Ordering::Relaxed);
        // Every 256 emits, surface the rolling max enqueue latency and reset.
        let n = self.emit_count.fetch_add(1, Ordering::Relaxed) + 1;
        if n % 256 == 0 {
            let peak = self.emit_max_us.swap(0, Ordering::Relaxed);
            // Best-effort, non-recursive: a full channel here just drops the
            // latency sample (it is itself diagnostic, never load-bearing).
            let _ = self.tx.try_send(EmissionRecord::Scheduler {
                stream: 0, attempt: 0, note: SchedNote::EmitLatencyUs { us: peak },
            });
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}
