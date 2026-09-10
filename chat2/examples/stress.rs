//! Concurrent-stream stress harness. cargo run --example stress -- <base> <model> <N>
use rar_chat::emission::{EmissionBus, ObserverSink, EmissionRecord, SchedNote};
use rar_chat::event_handler::EventHandler;
use rar_chat::openai::{ActiveGuard, Message, OpenAiCompatDriver, OpenAiConfig, TurnVerdict};
use rar_chat::env_file::EnvSource;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio_util::sync::CancellationToken;

struct CountingSink {
    reads: Arc<AtomicU64>, events: Arc<AtomicU64>, trailers: Arc<AtomicU64>,
    max_active: Arc<AtomicU64>, drops: Arc<AtomicU64>,
}
impl ObserverSink for CountingSink {
    fn write(&mut self, rec: &EmissionRecord) {
        match rec {
            EmissionRecord::Read { .. } => { self.reads.fetch_add(1, Ordering::Relaxed); }
            EmissionRecord::NormalizedEvent { .. } => { self.events.fetch_add(1, Ordering::Relaxed); }
            EmissionRecord::TurnTrailer { .. } => { self.trailers.fetch_add(1, Ordering::Relaxed); }
            EmissionRecord::Scheduler { note: SchedNote::ContentThroughput { active_streams, .. }, .. } => {
                self.max_active.fetch_max(*active_streams, Ordering::Relaxed);
            }
            EmissionRecord::CaptureDrop { dropped } => { self.drops.store(*dropped, Ordering::Relaxed); }
            _ => {}
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let base = a.get(1).cloned().unwrap_or("http://127.0.0.1:8092".into());
    let model = a.get(2).cloned().unwrap_or("mock-normal".into());
    let n: u64 = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(50);
    std::env::set_var("OPENAI_API_KEY", "stress-key");
    std::env::set_var("OPENAI_BASE_URL", &base);
    std::env::set_var("OPENAI_MODEL", &model);
    std::env::set_var("OPENAI_STALL_SECS", "10");
    let cfg = OpenAiConfig::from_source(&EnvSource::process_only())?;
    let reads = Arc::new(AtomicU64::new(0)); let events = Arc::new(AtomicU64::new(0));
    let trailers = Arc::new(AtomicU64::new(0)); let max_active = Arc::new(AtomicU64::new(0));
    let drops = Arc::new(AtomicU64::new(0));
    let sink = CountingSink { reads: reads.clone(), events: events.clone(), trailers: trailers.clone(), max_active: max_active.clone(), drops: drops.clone() };
    let (bus, writer) = EmissionBus::spawn(vec![Box::new(sink)], 8192);
    let handler = Arc::new(EventHandler::new(bus, n as u32, 0, false));
    let driver = Arc::new(OpenAiCompatDriver::new(cfg));
    let msgs = Arc::new(vec![Message::new("user", "hello")]);
    let t0 = std::time::Instant::now();
    let mut handles = Vec::new();
    for i in 1..=n {
        let d = driver.clone(); let h = handler.clone(); let m = msgs.clone();
        handles.push(tokio::spawn(async move {
            let _g = ActiveGuard::enter();
            d.execute_turn(i, &m, &h, &CancellationToken::new()).await
        }));
    }
    let mut committed = 0u64;
    for hd in handles { if let Ok(TurnVerdict::Committed(_)) = hd.await { committed += 1; } }
    let elapsed = t0.elapsed();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    drop(handler);
    let _ = tokio::time::timeout(std::time::Duration::from_millis(500), writer).await;
    println!("=== aggregate ({n} concurrent turns, current_thread) ===");
    println!("committed:     {committed}/{n}");
    println!("wall time:     {elapsed:?}");
    println!("throughput:    {:.0} turns/sec", n as f64 / elapsed.as_secs_f64());
    println!("total reads:   {}", reads.load(Ordering::Relaxed));
    println!("total events:  {}", events.load(Ordering::Relaxed));
    println!("trailers:      {}", trailers.load(Ordering::Relaxed));
    println!("peak active:   {}", max_active.load(Ordering::Relaxed));
    println!("capture drops: {}", drops.load(Ordering::Relaxed));
    Ok(())
}
