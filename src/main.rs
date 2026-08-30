use my_remote_speaker::clock::AccurateClock;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn,my_remote_speaker=info"));
    fmt()
        .with_env_filter(env_filter)
        .with_file(true)
        .with_line_number(true)
        .init();
    test_clock().await;
}

async fn test_clock() {
    let clock = AccurateClock::new();
    clock.wait_for_sync().await;
    let now = clock.now().await;
    let now_local = chrono::Utc::now();
    info!("now = {}, now_local = {}", now, now_local);
}
