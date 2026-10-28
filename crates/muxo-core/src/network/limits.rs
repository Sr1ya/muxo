//! Speed and data limits, for every download together. Every byte Muxo receives passes through
//! take(), on the network it came in on, which says how long to stop reading for. Stopping reading
//! is what slows the sender: TCP's window fills, and it waits.

use crate::schedule::data_period_key;
use crate::types::{AppSettings, DataLimitPeriod, DEFAULT_SLOW_MODE_SPEED};
use crate::util::{now_ms, read_json, update_json};
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// A rate as a bucket of tokens (bytes) that refills at `rate` per second. Taking more than it
/// holds leaves it in debt, and the debt is the wait. A second's worth is all it ever holds.
struct Bucket {
    rate: f64,
    tokens: f64,
    at: f64,
}

impl Bucket {
    fn new(rate: u64) -> Self {
        Bucket { rate: rate as f64, tokens: 0.0, at: now_ms() }
    }

    fn take(&mut self, bytes: u64, now: f64) -> f64 {
        self.tokens = (self.tokens + (now - self.at) / 1000.0 * self.rate).min(self.rate);
        self.at = now;
        self.tokens -= bytes as f64;
        if self.tokens >= 0.0 { 0.0 } else { -self.tokens / self.rate * 1000.0 }
    }
}

/// The bucket for `rate` bytes a second: `current` changed in place, so a debt carries over.
fn adjust(current: Option<Bucket>, rate: Option<u64>) -> Option<Bucket> {
    let rate = rate.filter(|rate| *rate > 0)?;
    Some(match current {
        Some(mut bucket) => {
            bucket.rate = rate as f64;
            bucket
        }
        None => Bucket::new(rate),
    })
}

const PERIODS: [DataLimitPeriod; 3] = [DataLimitPeriod::Day, DataLimitPeriod::Week, DataLimitPeriod::Month];
const USAGE_SAVE: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct Usage {
    key: String,
    bytes: BTreeMap<String, u64>,
}

fn fresh_usage(now: f64) -> HashMap<DataLimitPeriod, Usage> {
    PERIODS
        .iter()
        .map(|period| (*period, Usage { key: data_period_key(*period, now), bytes: BTreeMap::new() }))
        .collect()
}

fn period_name(period: DataLimitPeriod) -> &'static str {
    match period {
        DataLimitPeriod::Day => "day",
        DataLimitPeriod::Week => "week",
        DataLimitPeriod::Month => "month",
    }
}

struct Inner {
    total: Option<Bucket>,
    per_network: HashMap<String, Bucket>,
    data_limits: HashMap<String, u64>,
    periods: HashMap<String, DataLimitPeriod>,
    usage: HashMap<DataLimitPeriod, Usage>,
    save_armed: bool,
}

pub struct Limits {
    inner: Mutex<Inner>,
    path: PathBuf,
    on_limit_reached: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl Limits {
    pub async fn load(data_dir: &std::path::Path) -> Arc<Self> {
        let path = data_dir.join("network-usage.json");
        let mut usage = fresh_usage(now_ms());
        if let Ok(Some(saved)) = read_json(&path).await {
            // Older releases saved only monthly usage. Preserve it without attributing it to a
            // day or week.
            let entries = if saved.get("version") == Some(&json!(2)) {
                saved.get("periods").cloned()
            } else {
                Some(json!({ "month": { "key": saved.get("month"), "bytes": saved.get("bytes") } }))
            };
            for period in PERIODS {
                let Some(entry) = entries.as_ref().and_then(|entries| entries.get(period_name(period)))
                else {
                    continue;
                };
                let current = usage.get_mut(&period).expect("every period");
                if entry.get("key").and_then(Value::as_str) != Some(current.key.as_str()) {
                    continue;
                }
                if let Some(bytes) = entry.get("bytes").and_then(Value::as_object) {
                    for (id, used) in bytes {
                        if let Some(used) = used.as_u64() {
                            current.bytes.insert(id.clone(), used);
                        }
                    }
                }
            }
        }
        Arc::new(Limits {
            inner: Mutex::new(Inner {
                total: None,
                per_network: HashMap::new(),
                data_limits: HashMap::new(),
                periods: HashMap::new(),
                usage,
                save_armed: false,
            }),
            path,
            on_limit_reached: Mutex::new(None),
        })
    }

    pub fn set_on_limit_reached(&self, listener: Arc<dyn Fn() + Send + Sync>) {
        *self.on_limit_reached.lock() = Some(listener);
    }

    /// Takes up the limits in `settings`. Slow mode, when on, stands in for the total limit.
    pub fn configure(&self, settings: &AppSettings) {
        let mut inner = self.inner.lock();
        let total = if settings.slow_mode == Some(true) {
            Some(settings.slow_mode_speed.unwrap_or(DEFAULT_SLOW_MODE_SPEED))
        } else {
            settings.speed_limit
        };
        let current = inner.total.take();
        inner.total = adjust(current, total);
        let preferences = settings.network_preferences.clone().unwrap_or_default();
        let ids: Vec<String> =
            inner.per_network.keys().cloned().chain(preferences.keys().cloned()).collect();
        for id in ids {
            let current = inner.per_network.remove(&id);
            if let Some(bucket) = adjust(current, preferences.get(&id).and_then(|p| p.speed_limit)) {
                inner.per_network.insert(id, bucket);
            }
        }
        inner.data_limits.clear();
        inner.periods.clear();
        for (id, preference) in &preferences {
            inner
                .periods
                .insert(id.clone(), preference.data_limit_period.unwrap_or(DataLimitPeriod::Month));
            if let Some(limit) = preference.data_limit {
                inner.data_limits.insert(id.clone(), limit);
            }
        }
    }

    fn roll_periods(inner: &mut Inner, now: f64) -> bool {
        let mut changed = false;
        for period in PERIODS {
            let key = data_period_key(period, now);
            let usage = inner.usage.get_mut(&period).expect("every period");
            if usage.key != key {
                *usage = Usage { key, bytes: BTreeMap::new() };
                changed = true;
            }
        }
        changed
    }

    fn arm_save(self: &Arc<Self>, inner: &mut Inner) {
        if inner.save_armed {
            return;
        }
        inner.save_armed = true;
        let limits = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(USAGE_SAVE).await;
            limits.save().await;
        });
    }

    /// Counts `bytes` just received on `network_id`, and says how many ms to stop reading for.
    pub fn take(self: &Arc<Self>, network_id: &str, bytes: u64) -> f64 {
        let now = now_ms();
        let mut inner = self.inner.lock();
        Self::roll_periods(&mut inner, now);
        let period = inner.periods.get(network_id).copied().unwrap_or(DataLimitPeriod::Month);
        let before = inner.usage[&period].bytes.get(network_id).copied().unwrap_or(0);
        for usage in inner.usage.values_mut() {
            *usage.bytes.entry(network_id.to_owned()).or_insert(0) += bytes;
        }
        self.arm_save(&mut inner);
        if let Some(limit) = inner.data_limits.get(network_id).copied() {
            // Told once the caller is done with its bytes: what it does may stop the very
            // connection they came in on.
            if before < limit && before + bytes >= limit {
                if let Some(listener) = self.on_limit_reached.lock().clone() {
                    tokio::spawn(async move { listener() });
                }
            }
        }
        let total = inner.total.as_mut().map_or(0.0, |bucket| bucket.take(bytes, now));
        let network =
            inner.per_network.get_mut(network_id).map_or(0.0, |bucket| bucket.take(bytes, now));
        total.max(network)
    }

    /// The download-speed cap, in bytes a second, that applies to `network_id` (for torrents,
    /// whose engine throttles itself).
    pub fn rate_for(&self, network_id: &str) -> Option<u64> {
        let inner = self.inner.lock();
        let total = inner.total.as_ref().map(|bucket| bucket.rate as u64);
        let network = inner.per_network.get(network_id).map(|bucket| bucket.rate as u64);
        match (total, network) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Whether the network has used up its data for the selected period.
    pub fn limit_reached(&self, network_id: &str) -> bool {
        let mut inner = self.inner.lock();
        Self::roll_periods(&mut inner, now_ms());
        let Some(limit) = inner.data_limits.get(network_id).copied() else { return false };
        let period = inner.periods.get(network_id).copied().unwrap_or(DataLimitPeriod::Month);
        inner.usage[&period].bytes.get(network_id).copied().unwrap_or(0) >= limit
    }

    /// Each network's usage for its selected calendar period.
    pub fn used_by_period(&self) -> BTreeMap<String, u64> {
        let mut inner = self.inner.lock();
        Self::roll_periods(&mut inner, now_ms());
        let ids: Vec<String> =
            inner.usage.values().flat_map(|usage| usage.bytes.keys().cloned()).collect();
        ids.into_iter()
            .map(|id| {
                let period = inner.periods.get(&id).copied().unwrap_or(DataLimitPeriod::Month);
                let used = inner.usage[&period].bytes.get(&id).copied().unwrap_or(0);
                (id, used)
            })
            .collect()
    }

    fn snapshot(inner: &Inner) -> Value {
        let periods: serde_json::Map<String, Value> = PERIODS
            .iter()
            .map(|period| {
                let usage = &inner.usage[period];
                (period_name(*period).to_owned(), json!({ "key": usage.key, "bytes": usage.bytes }))
            })
            .collect();
        json!({ "version": 2, "periods": periods })
    }

    /// Reset only the selected network's current period; other periods retain their usage.
    pub async fn reset_usage(&self, network_id: &str) -> std::io::Result<()> {
        let (snapshot, period, previous) = {
            let mut inner = self.inner.lock();
            Self::roll_periods(&mut inner, now_ms());
            let period = inner.periods.get(network_id).copied().unwrap_or(DataLimitPeriod::Month);
            let usage = inner.usage.get_mut(&period).expect("every period");
            let previous = usage.bytes.insert(network_id.to_owned(), 0).unwrap_or(0);
            (Self::snapshot(&inner), period, previous)
        };
        if let Err(error) = update_json(&self.path, move |_| snapshot).await {
            let mut inner = self.inner.lock();
            *inner.usage.get_mut(&period).unwrap().bytes.entry(network_id.to_owned()).or_insert(0) +=
                previous;
            return Err(error);
        }
        Ok(())
    }

    /// Writes every period's usage now (on quit; otherwise it's saved every USAGE_SAVE).
    pub async fn save(&self) {
        let snapshot = {
            let mut inner = self.inner.lock();
            inner.save_armed = false;
            Self::roll_periods(&mut inner, now_ms());
            Self::snapshot(&inner)
        };
        let _ = update_json(&self.path, move |_| snapshot).await;
    }
}
