//! Benchmark logging and success tracking.

use std::io::Write;
use std::path::Path;
use std::sync::{Mutex, Once, OnceLock};

pub static INIT_LOGGING: Once = Once::new();
static BENCH_SUCCESS_LOG: OnceLock<Mutex<std::fs::File>> = OnceLock::new();

pub fn record_bench_success(group: &str, function: &str, success: bool) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("criterion_bench_success.log");
    let file = BENCH_SUCCESS_LOG.get_or_init(|| {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open criterion_bench_success.log");
        Mutex::new(f)
    });
    if let Ok(mut guard) = file.lock() {
        let _ = writeln!(
            guard,
            "{}\t{}\t{}",
            group,
            function,
            if success { 1 } else { 0 }
        );
        let _ = guard.flush();
    }
}

pub fn init_bench_logging() {
    INIT_LOGGING.call_once(|| {
        let filter = tracing_subscriber::EnvFilter::from_default_env()
            .add_directive("info".parse().unwrap());
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .with_thread_ids(false)
            .compact()
            .init();
        eprintln!("[bench] logging initialized (RUST_LOG=debug for more)");
    });
}
