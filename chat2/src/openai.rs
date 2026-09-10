//! # openai — the Driver: wire custody, select!-based liveness, spawn-ready
//!
//! Finished-spec changes:
//!  - Read loop restructured around `tokio::select!` racing three clocks
//!    against the next read: stall (per-read gap), throughput floor (EMA of
//!    content bytes/sec from first byte), and an overall turn deadline. This
//!    is intra-stream concurrency; `execute_turn` is `async` and takes
//!    `Arc<self>`-friendly `&self`, so the caller may `tokio::spawn` many
//!    turns for inter-stream concurrency.
//!  - `Send + Sync`; NO lock guard is ever held across an `.await` (the
//!    Handler is lock-free and synchronous; per-stream state is a local
//!    owned value, not shared). This is what keeps the multi_thread flip a
//!    single-line change.
//!  - Response validated as an event stream (Content-Type) before framing;
//!    a 200-with-JSON-error or proxy HTML page routes to the dirty path with
//!    its body intact, never fed to the SSE parser as if it were frames.
//!  - Undecodable installments and residue forwarded as distinguished
//!    records (strict-decode rule; nothing silently eaten).
//!
//! Cancellation: `execute_turn` honors a `CancellationToken` so a caller
//! (console Ctrl-C, or a harness) can abort an in-flight turn cooperatively.

use crate::driver::{ChatOutcome, DriverFault};
use crate::emission::{
    EmissionRecord, InstallmentKind, InstallmentStatus, SchedNote, StreamEndCause, StreamId,
    TurnVerdictRecord, WireNote, CAPTURE_FORMAT_VERSION,
};
use crate::env_file::EnvSource;
use crate::event_handler::{EventHandler, FaultDirective, FlowDirective, StreamState, TurnResolution};
use crate::machine::EndCause;
use crate::sse::{SseParser, SsePayload, DEFAULT_BUF_LIMIT};
use anyhow::{Context, Result};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct OpenAiConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub request_usage: bool,
    pub stall_timeout: Duration,
    pub first_byte_backstop: Duration,
    pub overall_deadline: Duration,
    pub throughput_floor_bps: u64,
    pub throughput_sustained: Duration,
    pub system_prompt: Option<String>,
}

impl OpenAiConfig {
    pub fn from_source_prefixed(src: &EnvSource, prefix: &str) -> Result<Self> {
        let k = |name: &str| format!("{prefix}{name}");
        let api_key = src.get(&k("OPENAI_API_KEY")).or_else(|| src.get("OPENAI_API_KEY"))
            .context("OPENAI_API_KEY is not set")?;
        let model = src.get(&k("OPENAI_MODEL")).context("OPENAI_MODEL is not set — required, no default")?;
        let base_url = src.get(&k("OPENAI_BASE_URL")).unwrap_or_else(|| "https://api.openai.com/v1".to_string());
        let request_usage = !matches!(src.get("OPENAI_INCLUDE_USAGE").as_deref(), Some("0") | Some("false") | Some("no"));
        let secs = |name: &str, dflt: u64| -> Result<u64> {
            match src.get(name) { Some(s) => s.parse().with_context(|| format!("{name} must be an integer")), None => Ok(dflt) }
        };
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key, model, request_usage,
            stall_timeout: Duration::from_secs(secs("OPENAI_STALL_SECS", 30)?),
            first_byte_backstop: Duration::from_secs(secs("OPENAI_FIRST_BYTE_BACKSTOP_SECS", 120)?),
            overall_deadline: Duration::from_secs(secs("OPENAI_OVERALL_DEADLINE_SECS", 600)?),
            throughput_floor_bps: secs("OPENAI_THROUGHPUT_FLOOR_BPS", 1)?,
            throughput_sustained: Duration::from_secs(secs("OPENAI_THROUGHPUT_SUSTAINED_SECS", 20)?),
            system_prompt: src.get("OPENAI_SYSTEM"),
        })
    }
    pub fn from_source(src: &EnvSource) -> Result<Self> {
        Self::from_source_prefixed(src, "")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: String,
}
impl Message {
    pub fn new(role: &str, content: &str) -> Self {
        Self { role: role.to_string(), content: content.to_string() }
    }
}

#[derive(Serialize)]
struct StreamOptions { include_usage: bool }
#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: &'a [Message],
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_options: Option<StreamOptions>,
}

#[derive(Debug)]
pub enum TurnVerdict {
    Committed(ChatOutcome),
    Failover { attempts_here: u32 },
    Failed(DriverFault),
}

pub struct OpenAiCompatDriver {
    client: reqwest::Client,
    cfg: OpenAiConfig,
}

impl OpenAiCompatDriver {
    pub fn new(cfg: OpenAiConfig) -> Self {
        Self {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(15))
                .build()
                .expect("reqwest client construction cannot fail"),
            cfg,
        }
    }
    pub fn config(&self) -> &OpenAiConfig {
        &self.cfg
    }

    /// One user turn on this endpoint. `stream_id` labels every emission so a
    /// multiplexed capture can be de-interleaved; `cancel` aborts in flight.
    pub async fn execute_turn(
        &self,
        stream_id: StreamId,
        messages: &[Message],
        handler: &EventHandler,
        cancel: &CancellationToken,
    ) -> TurnVerdict {
        let mut st = handler.begin_stream(stream_id);
        let mut remaining_allowance = handler.retry_allowance_per_turn();
        let mut select_polls: u64 = 0;

        let verdict = loop {
            let attempt = st.begin_attempt();
            if !handler.acquire_send_bucket(stream_id, attempt) {
                break TurnVerdict::Failed(DriverFault::Protocol("no send bucket available (admission denied)".into()));
            }
            let end = self.run_attempt(&mut st, messages, handler, cancel, &mut select_polls).await;
            handler.return_send_bucket(stream_id, attempt);
            match end {
                AttemptEnd::Complete(TurnResolution::Committed(o)) => break TurnVerdict::Committed(o),
                AttemptEnd::Complete(TurnResolution::Fault(f)) => {
                    match handler.on_wire_fault(stream_id, attempt, &mut remaining_allowance) {
                        FaultDirective::RetrySameEndpoint => continue,
                        FaultDirective::Failover => break TurnVerdict::Failover { attempts_here: attempt },
                        FaultDirective::GiveUp => break TurnVerdict::Failed(f),
                    }
                }
                AttemptEnd::Wire(cause) => {
                    if matches!(cause, StreamEndCause::Cancelled) {
                        break TurnVerdict::Failed(DriverFault::Protocol("cancelled".into()));
                    }
                    match handler.on_wire_fault(stream_id, attempt, &mut remaining_allowance) {
                        FaultDirective::RetrySameEndpoint => continue,
                        FaultDirective::Failover => break TurnVerdict::Failover { attempts_here: attempt },
                        FaultDirective::GiveUp => break TurnVerdict::Failed(wire_fault(cause)),
                    }
                }
            }
        };

        handler.bus().emit(EmissionRecord::TurnTrailer {
            stream: stream_id,
            format_version: CAPTURE_FORMAT_VERSION,
            attempts: st.attempt,
            verdict: match &verdict {
                TurnVerdict::Committed(o) => TurnVerdictRecord::Committed {
                    finish_reason: o.finish_reason.clone(), chars: o.content.chars().count(), usage: o.usage,
                },
                TurnVerdict::Failover { attempts_here } => TurnVerdictRecord::FailoverDirected { after_attempts: *attempts_here },
                TurnVerdict::Failed(f) => TurnVerdictRecord::Failed { fault: f.to_string() },
            },
            reads: st.reads, installments: st.installments, events: st.events,
            normalization_errors: st.norm_errors, select_polls, capture_policy: "full",
        });
        verdict
    }

    async fn run_attempt(
        &self,
        st: &mut StreamState,
        messages: &[Message],
        handler: &EventHandler,
        cancel: &CancellationToken,
        select_polls: &mut u64,
    ) -> AttemptEnd {
        let stream_id = st.stream_id;
        let attempt = st.attempt;
        let url = format!("{}/chat/completions", self.cfg.base_url);
        let body = ChatRequest {
            model: &self.cfg.model,
            messages,
            stream: true,
            stream_options: self.cfg.request_usage.then_some(StreamOptions { include_usage: true }),
        };

        // Bound the header wait: connect_timeout covers only the dial. A peer
        // that accepts TCP and never answers would otherwise hang here forever.
        let send_fut = tokio::time::timeout(
            self.cfg.first_byte_backstop,
            self.client.post(&url).bearer_auth(&self.cfg.api_key).json(&body).send(),
        );
        let resp = tokio::select! {
            _ = cancel.cancelled() => {
                let c = StreamEndCause::Cancelled;
                handler.bus().emit(EmissionRecord::StreamEnd { stream: stream_id, attempt, cause: c.clone(), unconsumed_buffer_bytes: 0 });
                return AttemptEnd::Wire(c);
            }
            r = send_fut => match r {
                Ok(Ok(r)) => r,
                Err(_elapsed) => {
                    let c = StreamEndCause::FirstByteBackstop { window_secs: self.cfg.first_byte_backstop.as_secs() };
                    handler.bus().emit(EmissionRecord::StreamEnd { stream: stream_id, attempt, cause: c.clone(), unconsumed_buffer_bytes: 0 });
                    return AttemptEnd::Wire(c);
                }
                Ok(Err(e)) => {
                    let c = StreamEndCause::ConnectError { detail: e.to_string() };
                    handler.bus().emit(EmissionRecord::StreamEnd { stream: stream_id, attempt, cause: c.clone(), unconsumed_buffer_bytes: 0 });
                    return AttemptEnd::Wire(c);
                }
            }
        };

        let status = resp.status();
        if !status.is_success() {
            let body = tokio::time::timeout(self.cfg.stall_timeout, resp.text()).await
                .map(|r| r.unwrap_or_default()).unwrap_or_else(|_| "(body read timed out)".into());
            let c = StreamEndCause::HttpError { status: status.as_u16(), body };
            handler.bus().emit(EmissionRecord::StreamEnd { stream: stream_id, attempt, cause: c.clone(), unconsumed_buffer_bytes: 0 });
            return AttemptEnd::Wire(c);
        }

        // Finished-spec: verify this is actually an event stream before we
        // trust the byte stream to be SSE. A 200 + application/json error, or
        // a proxy's text/html, routes dirty with its body — never framed.
        let ctype = resp.headers().get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
        if !ctype.contains("text/event-stream") {
            let body = tokio::time::timeout(self.cfg.stall_timeout, resp.text()).await
                .map(|r| r.unwrap_or_default()).unwrap_or_else(|_| "(body read timed out)".into());
            let prefix: String = body.chars().take(400).collect();
            let c = StreamEndCause::NotAnEventStream { content_type: ctype, body_prefix: prefix };
            handler.bus().emit(EmissionRecord::StreamEnd { stream: stream_id, attempt, cause: c.clone(), unconsumed_buffer_bytes: 0 });
            return AttemptEnd::Wire(c);
        }

        let mut parser = SseParser::with_limit(DEFAULT_BUF_LIMIT);
        let mut stream = resp.bytes_stream();
        let turn_start = Instant::now();
        let mut last_read_at = Instant::now();
        let mut first_byte_at: Option<Instant> = None;
        let mut read_seq: u64 = 0;
        let mut inst_seq: u64 = 0;
        let mut span_start: u64 = 1;
        let mut ttft_emitted = false;
        // Throughput EMA (content bytes/sec), evaluated once per second.
        let mut ema_bps: f64 = 0.0;
        let mut starved_dur: Option<std::time::Duration> = None;
        let mut throughput_tick = tokio::time::interval(Duration::from_secs(1));
        throughput_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut content_bytes_this_sec: u64 = 0;

        loop {
            let stall_window = if read_seq == 0 { self.cfg.first_byte_backstop } else { self.cfg.stall_timeout };
            // ABSOLUTE deadline anchored to the last read. The select! below
            // rebuilds its futures on every wakeup (including throughput
            // ticks that `continue`); a *relative* timeout here would reset
            // the stall clock each tick and never fire — the 02d regression
            // caught in review. sleep_until(absolute) is rebuild-safe.
            let stall_deadline = tokio::time::Instant::from_std(last_read_at + stall_window);
            let elapsed_overall = turn_start.elapsed();
            let overall_remaining = self.cfg.overall_deadline.saturating_sub(elapsed_overall);

            *select_polls += 1;
            // Fair (unbiased) polling: the throughput tick must not be
            // evaluated ahead of crediting the bytes of the read that would
            // clear a starvation condition. Arms are mutually exclusive per
            // wakeup, so fairness here costs nothing and removes a
            // false-starvation bias on the anti-slowloris guard.
            let item = tokio::select! {
                _ = cancel.cancelled() => {
                    handler.bus().emit(EmissionRecord::Scheduler { stream: stream_id, attempt, note: SchedNote::SelectWake { arm: "cancel" } });
                    let c = StreamEndCause::Cancelled;
                    handler.bus().emit(EmissionRecord::StreamEnd { stream: stream_id, attempt, cause: c.clone(), unconsumed_buffer_bytes: parser.buffered() });
                    return AttemptEnd::Wire(c);
                }
                _ = tokio::time::sleep(overall_remaining) => {
                    handler.bus().emit(EmissionRecord::Scheduler { stream: stream_id, attempt, note: SchedNote::SelectWake { arm: "overall_deadline" } });
                    let c = StreamEndCause::OverallDeadline { limit_secs: self.cfg.overall_deadline.as_secs() };
                    handler.bus().emit(EmissionRecord::StreamEnd { stream: stream_id, attempt, cause: c.clone(), unconsumed_buffer_bytes: parser.buffered() });
                    return AttemptEnd::Wire(c);
                }
                _ = throughput_tick.tick() => {
                    // Only meaningful after first byte (warm-up exemption).
                    if let Some(fb) = first_byte_at {
                        let warmed = fb.elapsed() >= Duration::from_secs(1);
                        let (new_ema, starved) = eval_throughput(
                            ema_bps, content_bytes_this_sec, self.cfg.throughput_floor_bps,
                            warmed, &mut starved_dur, turn_start.elapsed(), self.cfg.throughput_sustained,
                        );
                        ema_bps = new_ema;
                        content_bytes_this_sec = 0;
                        handler.bus().emit(EmissionRecord::Scheduler {
                            stream: stream_id, attempt,
                            note: SchedNote::ContentThroughput { bytes_per_sec: ema_bps as u64, active_streams: ACTIVE_STREAMS.load(std::sync::atomic::Ordering::Relaxed) },
                        });
                        if starved {
                            let c = StreamEndCause::ThroughputStarved {
                                floor_bps: self.cfg.throughput_floor_bps,
                                sustained_secs: self.cfg.throughput_sustained.as_secs(),
                            };
                            handler.bus().emit(EmissionRecord::StreamEnd { stream: stream_id, attempt, cause: c.clone(), unconsumed_buffer_bytes: parser.buffered() });
                            return AttemptEnd::Wire(c);
                        }
                    }
                    continue;
                }
                _ = tokio::time::sleep_until(stall_deadline) => {
                    handler.bus().emit(EmissionRecord::Scheduler { stream: stream_id, attempt, note: SchedNote::SelectWake { arm: "stall" } });
                    let c = if read_seq == 0 {
                        StreamEndCause::FirstByteBackstop { window_secs: stall_window.as_secs() }
                    } else {
                        StreamEndCause::StallTimeout { window_secs: stall_window.as_secs() }
                    };
                    handler.bus().emit(EmissionRecord::StreamEnd { stream: stream_id, attempt, cause: c.clone(), unconsumed_buffer_bytes: parser.buffered() });
                    return AttemptEnd::Wire(c);
                }
                r = stream.next() => match r {
                    None => break,
                    Some(Ok(b)) => b,
                    Some(Err(e)) => {
                        let c = StreamEndCause::TransportError { detail: e.to_string() };
                        handler.bus().emit(EmissionRecord::StreamEnd { stream: stream_id, attempt, cause: c.clone(), unconsumed_buffer_bytes: parser.buffered() });
                        return AttemptEnd::Wire(c);
                    }
                }
            };

            read_seq += 1;
            st.reads += 1;
            let now = Instant::now();
            if first_byte_at.is_none() {
                first_byte_at = Some(now);
            }
            let gap = now.duration_since(last_read_at);
            last_read_at = now;
            handler.bus().emit(EmissionRecord::Read {
                stream: stream_id, attempt, seq: read_seq,
                at_ms: now.duration_since(turn_start).as_millis() as u64,
                wall_unix_ms: SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0),
                bytes: item.len(), gap_ms: gap.as_millis() as u64,
            });

            let frames = match parser.push(&item) {
                Ok(f) => f,
                Err(o) => {
                    let c = StreamEndCause::BufferOverflow { limit_bytes: o.limit_bytes };
                    handler.bus().emit(EmissionRecord::StreamEnd { stream: stream_id, attempt, cause: c.clone(), unconsumed_buffer_bytes: parser.buffered() });
                    return AttemptEnd::Wire(c);
                }
            };

            for ev in frames {
                inst_seq += 1;
                match &ev.payload {
                    SsePayload::Undecodable { raw, error } => {
                        let hex: String = raw.iter().take(16).map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(" ");
                        handler.bus().emit(EmissionRecord::UndecodableInstallment {
                            stream: stream_id, attempt, seq: inst_seq, bytes: raw.len(), error: error.clone(), hex_prefix: hex,
                        });
                        span_start = read_seq;
                        continue;
                    }
                    _ => {}
                }
                let (kind, payload) = match &ev.payload {
                    SsePayload::Comment(c) => (InstallmentKind::Comment, c.clone()),
                    SsePayload::Data(d) => (InstallmentKind::Data, d.clone()),
                    SsePayload::Undecodable { .. } => unreachable!(),
                };
                handler.bus().emit(EmissionRecord::Installment {
                    stream: stream_id, attempt, seq: inst_seq, first_read: span_start, last_read: read_seq,
                    bytes: ev.raw_byte_len, kind, payload: payload.clone(), status: InstallmentStatus::Complete,
                });
                span_start = read_seq;
                if kind == InstallmentKind::Comment {
                    handler.bus().emit(EmissionRecord::Wire { stream: stream_id, attempt, note: WireNote::KeepAliveComment });
                    continue;
                }
                if !ttft_emitted {
                    ttft_emitted = true;
                    handler.bus().emit(EmissionRecord::Wire { stream: stream_id, attempt, note: WireNote::Ttft { ms: now.duration_since(turn_start).as_millis() as u64 } });
                }
                handler.bus().emit(EmissionRecord::Wire { stream: stream_id, attempt, note: WireNote::InterReadGap { ms: gap.as_millis() as u64, bytes: ev.raw_byte_len } });
                let before_content = st.content_bytes;
                match handler.on_installment(st, inst_seq, kind, &payload, InstallmentStatus::Complete) {
                    Ok(FlowDirective::Continue) => {
                        content_bytes_this_sec += st.content_bytes - before_content;
                    }
                    Ok(FlowDirective::StopComplete) => {
                        handler.bus().emit(EmissionRecord::StreamEnd { stream: stream_id, attempt, cause: StreamEndCause::DoneSentinelConsumed, unconsumed_buffer_bytes: parser.buffered() });
                        return AttemptEnd::Complete(handler.finalize_attempt(st, EndCause::StreamEnded));
                    }
                    Ok(FlowDirective::Abort) => {
                        handler.bus().emit(EmissionRecord::StreamEnd { stream: stream_id, attempt, cause: StreamEndCause::HandlerDirected { directive: "abort".into() }, unconsumed_buffer_bytes: parser.buffered() });
                        return AttemptEnd::Complete(handler.finalize_attempt(st, EndCause::HandlerAbort));
                    }
                    Err(_fault) => {
                        handler.bus().emit(EmissionRecord::StreamEnd { stream: stream_id, attempt, cause: StreamEndCause::HandlerDirected { directive: "protocol_error".into() }, unconsumed_buffer_bytes: parser.buffered() });
                        return AttemptEnd::Complete(handler.finalize_attempt(st, EndCause::HandlerAbort));
                    }
                }
            }
        }

        // Peer closed. Dirty path: recovered tail + residue as records.
        let (tail, residue) = parser.finish_with_residue();
        for ev in tail {
            inst_seq += 1;
            let (kind, payload) = match &ev.payload {
                SsePayload::Comment(c) => (InstallmentKind::Comment, c.clone()),
                SsePayload::Data(d) => (InstallmentKind::Data, d.clone()),
                SsePayload::Undecodable { raw, error } => {
                    let hex: String = raw.iter().take(16).map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(" ");
                    handler.bus().emit(EmissionRecord::UndecodableInstallment { stream: stream_id, attempt, seq: inst_seq, bytes: raw.len(), error: error.clone(), hex_prefix: hex });
                    continue;
                }
            };
            handler.bus().emit(EmissionRecord::Installment {
                stream: stream_id, attempt, seq: inst_seq, first_read: span_start, last_read: read_seq,
                bytes: ev.raw_byte_len, kind, payload: payload.clone(), status: InstallmentStatus::RecoveredTail,
            });
            let _ = handler.on_installment(st, inst_seq, kind, &payload, InstallmentStatus::RecoveredTail);
        }
        if !residue.is_empty() {
            handler.bus().emit(EmissionRecord::Residue { stream: stream_id, attempt, bytes: residue.len(), lossy_text: String::from_utf8_lossy(&residue).to_string() });
        }
        // Distinguish "accepted but produced nothing" from a mid-answer cut.
        let cause = if !st.saw_any_content { StreamEndCause::ClosedWithNoContent } else { StreamEndCause::PeerClosed };
        handler.bus().emit(EmissionRecord::StreamEnd { stream: stream_id, attempt, cause, unconsumed_buffer_bytes: 0 });
        let res = handler.finalize_attempt(st, EndCause::StreamEnded);
        if let TurnResolution::Committed(_) = &res {
            handler.bus().emit(EmissionRecord::Wire { stream: stream_id, attempt, note: WireNote::EofWithoutDoneSentinel });
        }
        AttemptEnd::Complete(res)
    }
}

/// Active concurrent-stream gauge, incremented/decremented by the caller
/// around each spawned turn — the aggregate-metric instrument.
pub static ACTIVE_STREAMS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub struct ActiveGuard;
impl ActiveGuard {
    pub fn enter() -> Self {
        ACTIVE_STREAMS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ActiveGuard
    }
}
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        ACTIVE_STREAMS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

enum AttemptEnd {
    Complete(TurnResolution),
    Wire(StreamEndCause),
}

fn wire_fault(c: StreamEndCause) -> DriverFault {
    match c {
        StreamEndCause::StallTimeout { window_secs } | StreamEndCause::FirstByteBackstop { window_secs } => DriverFault::Stalled { after_secs: window_secs },
        StreamEndCause::ThroughputStarved { sustained_secs, .. } => DriverFault::Stalled { after_secs: sustained_secs },
        StreamEndCause::OverallDeadline { limit_secs } => DriverFault::Stalled { after_secs: limit_secs },
        StreamEndCause::BufferOverflow { limit_bytes } => DriverFault::BufferOverflow { limit_bytes },
        StreamEndCause::HttpError { status, body } => DriverFault::Protocol(format!("HTTP {status}: {body}")),
        StreamEndCause::NotAnEventStream { content_type, body_prefix } => DriverFault::Protocol(format!("not an event stream (Content-Type: {content_type}): {body_prefix}")),
        StreamEndCause::ConnectError { detail } | StreamEndCause::TransportError { detail } => DriverFault::Protocol(detail),
        other => DriverFault::Protocol(format!("{other:?}")),
    }
}

/// Pure throughput-starvation evaluator, extracted so the anti-slowloris
/// guard is unit-testable without a live stream (fix for the "least-tested
/// safety guard" concern). Returns the updated EMA and whether a sustained
/// starvation condition has now been met.
pub fn eval_throughput(
    prev_ema: f64,
    bytes_this_window: u64,
    floor_bps: u64,
    warmed: bool,
    starved_since: &mut Option<std::time::Duration>,
    now_since_start: std::time::Duration,
    sustained: std::time::Duration,
) -> (f64, bool) {
    let ema = 0.6 * prev_ema + 0.4 * bytes_this_window as f64;
    if warmed && ema < floor_bps as f64 {
        let since = *starved_since.get_or_insert(now_since_start);
        let elapsed = now_since_start.saturating_sub(since);
        (ema, elapsed >= sustained)
    } else {
        *starved_since = None;
        (ema, false)
    }
}

#[cfg(test)]
mod throughput_tests {
    use super::eval_throughput;
    use std::time::Duration;

    #[test]
    fn healthy_stream_never_starves() {
        let mut ss = None;
        let mut ema = 0.0;
        for sec in 1..=30u64 {
            let (e, starved) = eval_throughput(ema, 50, 1, true, &mut ss, Duration::from_secs(sec), Duration::from_secs(20));
            ema = e;
            assert!(!starved, "healthy 50 B/s must never starve (sec {sec})");
        }
    }

    #[test]
    fn sustained_silence_trips_after_window_not_before() {
        let mut ss = None;
        let mut ema = 100.0; // start healthy, then go silent
        let mut tripped_at = None;
        for sec in 1..=40u64 {
            let (e, starved) = eval_throughput(ema, 0, 1, true, &mut ss, Duration::from_secs(sec), Duration::from_secs(20));
            ema = e;
            if starved && tripped_at.is_none() { tripped_at = Some(sec); }
        }
        let t = tripped_at.expect("silence must eventually trip");
        // EMA must decay below floor (1 B/s) before the 20s clock even starts,
        // so the trip is strictly later than 20s from the first silent second.
        assert!(t >= 20, "must not trip before the sustained window (tripped at {t}s)");
    }

    #[test]
    fn warmup_exempts_first_second() {
        let mut ss = None;
        // Not warmed: even zero bytes cannot trip, and cannot arm the clock.
        let (_e, starved) = eval_throughput(0.0, 0, 1, false, &mut ss, Duration::from_secs(0), Duration::from_secs(20));
        assert!(!starved);
        assert!(ss.is_none(), "starvation clock must not arm during warm-up");
    }

    #[test]
    fn recovery_resets_the_clock() {
        let mut ss = None;
        let mut ema = 0.0;
        // go silent for 10s (arms clock), then recover
        for sec in 1..=10u64 {
            let (e, _) = eval_throughput(ema, 0, 1, true, &mut ss, Duration::from_secs(sec), Duration::from_secs(20));
            ema = e;
        }
        assert!(ss.is_some(), "clock should be armed after sustained silence");
        // one healthy second clears it
        let (_e, starved) = eval_throughput(ema, 100, 1, true, &mut ss, Duration::from_secs(11), Duration::from_secs(20));
        assert!(!starved);
        assert!(ss.is_none(), "recovery must disarm the starvation clock");
    }
}
