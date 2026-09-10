//! # rar-chat binary — terminal loop, endpoint roster, cancellation, roster
//!
//! Single-thread runtime for clean aggregate time trials. The types are all
//! `Send`-clean, so flipping `flavor = "current_thread"` to `"multi_thread"`
//! below is the ONE pinpoint change that moves trials onto multiple physical
//! cores — no other code changes, because no lock guard is ever held across
//! an await and all shared state is Arc/atomic.
//!
//! History-commit policy unchanged: commit only from a Committed outcome.

use anyhow::Result;
use rar_chat::emission::{EmissionBus, ObserverSink};
use rar_chat::env_file::EnvSource;
use rar_chat::event_handler::EventHandler;
use rar_chat::openai::{ActiveGuard, Message, OpenAiCompatDriver, OpenAiConfig, TurnVerdict};
use rar_chat::ports::{CaptureSink, TerminalSink};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio_util::sync::CancellationToken;

// === THE PINPOINT CHANGE FOR MULTI-CORE TRIALS: current_thread <-> multi_thread ===
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let source = match args.next() {
        Some(f) if f == "-h" || f == "--help" => { print_help(); return Ok(()); }
        Some(path) => EnvSource::from_file(&PathBuf::from(path))?,
        None => EnvSource::process_only(),
    };

    let primary = OpenAiConfig::from_source(&source)?;
    let failover: Option<OpenAiConfig> = source.get("FAILOVER_OPENAI_BASE_URL")
        .map(|_| OpenAiConfig::from_source_prefixed(&source, "FAILOVER_")).transpose()?;
    let retry_allowance: u32 = source.get("OPENAI_RETRY_ALLOWANCE").map(|s| s.parse()).transpose()?.unwrap_or(1);
    let send_buckets: u32 = source.get("OPENAI_SEND_BUCKETS").map(|s| s.parse()).transpose()?.unwrap_or(1);
    let bus_capacity: usize = source.get("OPENAI_BUS_CAPACITY").map(|s| s.parse()).transpose()?.unwrap_or(4096);

    let mut sinks: Vec<Box<dyn ObserverSink>> = vec![Box::new(TerminalSink)];
    if let Some(dir) = source.get("OPENAI_CAPTURE_DIR") {
        sinks.push(Box::new(CaptureSink::create(&PathBuf::from(dir))?));
    }
    let (bus, writer) = EmissionBus::spawn(sinks, bus_capacity);
    let handler = Arc::new(EventHandler::new(bus, send_buckets, retry_allowance, failover.is_some()));

    let system_prompt = primary.system_prompt.clone();
    eprintln!("rar-chat — endpoint {} | model {} | failover {} | buckets {} | config {}",
        primary.base_url, primary.model,
        failover.as_ref().map(|f| f.base_url.as_str()).unwrap_or("(none)"),
        send_buckets, source.origin.as_deref().unwrap_or("(process environment)"));
    eprintln!("commands: /quit  /reset (Ctrl-D also quits)");

    let driver = Arc::new(OpenAiCompatDriver::new(primary));
    let failover_driver = failover.map(|f| Arc::new(OpenAiCompatDriver::new(f)));

    let mut history: Vec<Message> = Vec::new();
    if let Some(sys) = system_prompt {
        history.push(Message::new("system", &sys));
    }

    let mut stream_id: u64 = 0;
    let mut lines = BufReader::new(io::stdin()).lines();
    loop {
        print!("you> ");
        std::io::stdout().flush()?;
        let Some(line) = lines.next_line().await? else { break };
        let line = line.trim();
        if line.is_empty() { continue; }
        match line {
            "/quit" | "/exit" => break,
            "/reset" => { history.retain(|m| m.role == "system"); eprintln!("(history cleared)"); continue; }
            _ => {}
        }

        history.push(Message::new("user", line));
        print!("ai> ");
        std::io::stdout().flush()?;

        stream_id += 1;
        let cancel = CancellationToken::new();
        let _active = ActiveGuard::enter();
        let mut verdict = driver.execute_turn(stream_id, &history, &handler, &cancel).await;

        if let TurnVerdict::Failover { attempts_here } = verdict {
            match &failover_driver {
                Some(fd) => {
                    eprintln!("\n[FAILOVER] primary exhausted after {attempts_here} attempt(s); re-issuing on {}", fd.config().base_url);
                    print!("ai> "); std::io::stdout().flush()?;
                    stream_id += 1;
                    verdict = fd.execute_turn(stream_id, &history, &handler, &cancel).await;
                }
                None => verdict = TurnVerdict::Failed(rar_chat::driver::DriverFault::Protocol("failover directed but no alternative configured".into())),
            }
        }

        match verdict {
            TurnVerdict::Committed(outcome) => history.push(Message::new("assistant", &outcome.content)),
            TurnVerdict::Failed(fault) => { history.pop(); eprintln!("\n[TURN FAILED] {fault} — turn not committed to history"); }
            TurnVerdict::Failover { .. } => unreachable!("resolved above"),
        }
        println!();
    }

    // Drain the emission bus before exit: drop every sender so the writer's
    // recv loop ends, then wait for it to finish flushing queued records.
    // Without this, records still in the channel at /quit are discarded with
    // the runtime — the review found a stalled turn's StreamEnd and
    // TurnTrailer missing from its own capture file for exactly this reason.
    drop(driver);
    drop(failover_driver);
    drop(handler);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), writer).await;
    Ok(())
}

fn print_help() {
    eprintln!("usage: rar-chat [ENV_FILE]");
    eprintln!("  wire:     OPENAI_API_KEY (req), OPENAI_MODEL (req), OPENAI_BASE_URL, OPENAI_SYSTEM, OPENAI_INCLUDE_USAGE");
    eprintln!("  clocks:   OPENAI_STALL_SECS, OPENAI_FIRST_BYTE_BACKSTOP_SECS, OPENAI_OVERALL_DEADLINE_SECS,");
    eprintln!("            OPENAI_THROUGHPUT_FLOOR_BPS, OPENAI_THROUGHPUT_SUSTAINED_SECS");
    eprintln!("  strategy: OPENAI_RETRY_ALLOWANCE, OPENAI_SEND_BUCKETS, OPENAI_BUS_CAPACITY");
    eprintln!("  capture:  OPENAI_CAPTURE_DIR");
    eprintln!("  failover: FAILOVER_OPENAI_BASE_URL / _MODEL / _API_KEY");
}
