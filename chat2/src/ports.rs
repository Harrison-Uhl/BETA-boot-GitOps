//! # ports — observer sinks (run on the bus writer task, never the read loop)
//!
//! Sinks implement `ObserverSink` and are owned by the `EmissionBus` writer
//! task. They can be arbitrarily slow without back-pressuring the wire: a
//! full bus drops records and counts them (surfaced as `CaptureDrop`). This
//! is the contract that #3 (blocking observer) demanded — enforced here by
//! construction, since sinks no longer sit in the Driver's hot path.

use crate::driver::DriverEvent;
use crate::emission::{EmissionRecord, ObserverSink, WireNote};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Live terminal rendering, driven off normalized-event records (recovered
/// tail included — single path to the screen, no display/history asymmetry).
pub struct TerminalSink;

impl ObserverSink for TerminalSink {
    fn write(&mut self, rec: &EmissionRecord) {
        match rec {
            EmissionRecord::NormalizedEvent { event, .. } => match event {
                DriverEvent::TextDelta(t) => { print!("{t}"); let _ = std::io::stdout().flush(); }
                DriverEvent::Finish(reason) => eprintln!("\n[SYSTEM] Stream completed. Exit Reason: {reason}"),
                DriverEvent::Usage { prompt_tokens, completion_tokens, total_tokens } =>
                    eprintln!("[ACCOUNTING] Prompt: {prompt_tokens}, Completion: {completion_tokens}, Total: {total_tokens}"),
                DriverEvent::Done => {}
            },
            EmissionRecord::Wire { note: WireNote::Ttft { ms }, .. } => eprintln!("\n[METRIC] TTFT: {ms}ms"),
            EmissionRecord::Wire { note: WireNote::UnhandledChannel { channel }, .. } => eprintln!("\n[WIRE] unhandled channel present: {channel} (surfaced, not interpreted)"),
            EmissionRecord::Anomaly { note, .. } => eprintln!("\n[ANOMALY] {note}"),
            EmissionRecord::NormalizationError { detail, .. } => eprintln!("\n[WIRE-ERROR] normalization failed: {detail}"),
            EmissionRecord::UndecodableInstallment { bytes, error, hex_prefix, .. } => eprintln!("\n[WIRE-ERROR] undecodable frame ({bytes}b): {error} [{hex_prefix}]"),
            EmissionRecord::Residue { bytes, .. } => eprintln!("\n[WIRE-ERROR] {bytes}b unframed residue at stream end"),
            EmissionRecord::CaptureDrop { dropped } => eprintln!("\n[CAPTURE] {dropped} record(s) dropped under load"),
            _ => {}
        }
    }
}

/// JSONL capture. Buffered writer; still cannot stall the wire because it
/// runs on the bus task, decoupled by the bounded channel.
pub struct CaptureSink {
    w: BufWriter<File>,
    since_flush: u32,
}
impl CaptureSink {
    pub fn create(dir: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let name = format!("capture-{}.jsonl", SystemTimeSecs());
        let path = dir.join(name);
        eprintln!("[CAPTURE] recording to {}", path.display());
        Ok(Self { w: BufWriter::new(File::create(&path)?), since_flush: 0 })
    }
}
#[allow(non_snake_case)]
fn SystemTimeSecs() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}
impl Drop for CaptureSink {
    fn drop(&mut self) {
        let _ = self.w.flush();
    }
}

impl ObserverSink for CaptureSink {
    fn write(&mut self, rec: &EmissionRecord) {
        match serde_json::to_string(rec) {
            Ok(line) => { let _ = writeln!(self.w, "{line}"); }
            Err(e) => { let _ = writeln!(self.w, "{{\"rec\":\"capture_error\",\"detail\":\"{e}\"}}"); }
        }
        // Flush periodically and on turn boundaries — bounds data loss on an
        // abrupt exit without an fsync-adjacent syscall per record (which
        // could saturate the bus task and cause silent CaptureDrops).
        self.since_flush += 1;
        let boundary = matches!(rec, EmissionRecord::TurnTrailer { .. });
        if boundary || self.since_flush >= 64 {
            let _ = self.w.flush();
            self.since_flush = 0;
        }
    }
}
