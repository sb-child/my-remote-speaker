use std::{error::Error, net::Ipv6Addr, sync::Arc};

use chrono::{DateTime, TimeDelta, Utc};
use tokio::{
    sync::RwLock,
    task::{JoinHandle, JoinSet},
    time::{Duration, Instant},
};

const NTP_SERVERS: &[&str] = &[
    "ntp.aliyun.com:123",
    "ntp.tencent.com:123",
    "cn.pool.ntp.org:123",
    "0.cn.pool.ntp.org",
    "1.cn.pool.ntp.org",
    "2.cn.pool.ntp.org",
    "3.cn.pool.ntp.org",
    "pool.ntp.org:123",
    "time.aws.com:123",
    "time.apple.com:123",
    "time.windows.com:123",
    "time.cloudflare.com:123",
    "time.android.com:123",
    "time1.google.com:123",
    "time2.google.com:123",
    "time3.google.com:123",
    "time4.google.com:123",
    "time.nist.gov:123",
    "time-a-wwv.nist.gov:123",
];

const MAX_ACCEPTABLE_RTT: TimeDelta = TimeDelta::milliseconds(150);

pub async fn get_ntp_time(
    client: Arc<rsntp::AsyncSntpClient>,
) -> Result<Option<DateTime<Utc>>, Box<dyn Error + Send + Sync>> {
    #[derive(Debug, Clone)]
    struct NtpSample {
        // server: String,
        offset: chrono::Duration,
        rtt: TimeDelta,
    }
    let mut set = JoinSet::new();
    for &server in NTP_SERVERS {
        let c = Arc::clone(&client);
        let server_str = server.to_string();
        set.spawn(async move {
            let res =
                tokio::time::timeout(Duration::from_secs(3), c.synchronize(&server_str)).await;
            match res {
                Ok(Ok(sntp_time)) => {
                    let receive_local_time = Utc::now();
                    let sntp_utc: DateTime<Utc> = sntp_time
                        .datetime()
                        .into_chrono_datetime()
                        .unwrap_or_default();
                    let rtt = sntp_time
                        .round_trip_delay()
                        .into_chrono_duration()
                        .unwrap_or_default();
                    let offset = sntp_utc - receive_local_time;
                    tracing::info!(
                        "NTP Server {}: Offset {} ms, RTT {} ms",
                        server_str,
                        offset.num_milliseconds(),
                        rtt.num_milliseconds()
                    );
                    Ok(NtpSample {
                        // server: server_str,
                        offset,
                        rtt,
                    })
                }
                Ok(Err(e)) => {
                    tracing::warn!("NTP Server {}: Sync error: {}", server_str, e);
                    Err(Box::new(e) as Box<dyn Error + Send + Sync>)
                }
                Err(_) => {
                    tracing::warn!("NTP Server {}: Timeout", server_str);
                    Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "NTP request timed out",
                    )) as Box<dyn Error + Send + Sync>)
                }
            }
        });
    }
    let mut samples = Vec::new();
    let mut errors = Vec::new();
    while let Some(join_result) = set.join_next().await {
        match join_result {
            Ok(Ok(sample)) => samples.push(sample),
            Ok(Err(err)) => errors.push(err),
            Err(join_err) => errors.push(Box::new(join_err) as Box<dyn Error + Send + Sync>),
        }
    }
    if samples.is_empty() && !errors.is_empty() {
        return Err(errors.remove(0));
    }
    let mut valid_samples: Vec<NtpSample> = samples
        .iter()
        .cloned()
        .filter(|s| s.rtt <= MAX_ACCEPTABLE_RTT)
        .collect();
    if valid_samples.len() < 2 {
        samples.sort_by_key(|s| s.rtt);
        valid_samples = samples.into_iter().take(3).collect();
    }
    if valid_samples.len() < 2 {
        return Ok(None);
    }
    valid_samples.sort_by_key(|s| s.offset);
    let median_index = valid_samples.len() / 2;
    let median_offset = valid_samples[median_index].offset;
    let accurate_now = Utc::now() + median_offset;
    tracing::info!(
        "NTP Synchronization Complete: Median offset {} ms (from {} samples)",
        median_offset.num_milliseconds(),
        valid_samples.len()
    );
    Ok(Some(accurate_now))
}

#[derive(Clone, Copy)]
struct ClockAnchor {
    instant: Instant,
    utc_time: DateTime<Utc>,
}

#[derive(Clone)]
pub struct AccurateClock {
    anchor: Arc<RwLock<Option<ClockAnchor>>>,
    task_handle: Arc<JoinHandle<()>>,
}

impl AccurateClock {
    pub fn new() -> Self {
        let anchor = Arc::new(RwLock::new(None));
        let task_handle = start_sync_task(anchor.clone());
        Self {
            anchor,
            task_handle: task_handle.into(),
        }
    }

    pub async fn now(&self) -> DateTime<Utc> {
        let guard = self.anchor.read().await;
        if let Some(anchor) = *guard {
            let elapsed = Instant::now().saturating_duration_since(anchor.instant);
            let chrono_elapsed = chrono::Duration::from_std(elapsed).unwrap_or_default();
            anchor.utc_time + chrono_elapsed
        } else {
            Utc::now()
        }
    }

    pub async fn wait_for_sync(&self) {
        loop {
            let guard = self.anchor.read().await;
            if guard.is_some() {
                break;
            }
            drop(guard);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn close(&self) {
        self.task_handle.abort();
        while !self.task_handle.is_finished() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

fn start_sync_task(anchor: Arc<RwLock<Option<ClockAnchor>>>) -> JoinHandle<()> {
    tokio::spawn(async move {
        // let config = rsntp::Config::default().bind_address((Ipv6Addr::UNSPECIFIED, 0).into()); // ipv6
        let config = rsntp::Config::default(); // ipv4
        let client = Arc::new(rsntp::AsyncSntpClient::with_config(config));
        loop {
            tracing::info!("NTP Synchronizing...");
            match get_ntp_time(client.clone()).await {
                Ok(Some(ntp_utc)) => {
                    let now_instant = Instant::now();
                    let new_anchor = ClockAnchor {
                        instant: now_instant,
                        utc_time: ntp_utc,
                    };
                    {
                        let mut guard = anchor.write().await;
                        *guard = Some(new_anchor);
                    }
                    tracing::info!("NTP Synchronization Completed.");
                }
                Ok(None) => {
                    tracing::error!("NTP server partially unavailable, will retry again.");
                    continue;
                }
                Err(e) => {
                    tracing::error!("NTP Synchronize Failed: {}", e);
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    continue;
                }
            }
            tokio::time::sleep(Duration::from_hours(1)).await;
        }
    })
}
