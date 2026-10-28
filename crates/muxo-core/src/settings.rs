//! app-settings.json: what the user set, checked field by field on every read and write.

use crate::schedule::{default_schedule, valid_schedule};
use crate::types::{
    AppSettings, DataLimitPeriod, NetworkPreference, NetworkPreferences, SpeedUnit, ThemeSource,
    DOWNLOADS_AT_ONCE_MAX, DOWNLOADS_AT_ONCE_MIN,
};
use crate::util::{byte_count, read_json, update_json};
use serde_json::{Map, Value};
use std::io;
use std::path::{Path, PathBuf};

pub struct Settings {
    path: PathBuf,
}

/// Keeps only fields of the right type, entry by entry, rather than discarding every network over
/// one bad entry. An entry left with no field is dropped.
fn sanitize_network_preferences(parsed: &Value) -> NetworkPreferences {
    let mut result = NetworkPreferences::new();
    let Some(entries) = parsed.as_object() else { return result };
    for (id, value) in entries {
        let Some(value) = value.as_object() else { continue };
        let preference = NetworkPreference {
            custom_name: value.get("customName").and_then(Value::as_str).map(str::to_owned),
            color_id: value.get("colorId").and_then(Value::as_str).map(str::to_owned),
            off: (value.get("off") == Some(&Value::Bool(true))).then_some(true),
            speed_limit: byte_count(value.get("speedLimit")),
            data_limit: byte_count(value.get("dataLimit")),
            data_limit_period: value
                .get("dataLimitPeriod")
                .and_then(|period| serde_json::from_value::<DataLimitPeriod>(period.clone()).ok()),
        };
        if preference != NetworkPreference::default() {
            result.insert(id.clone(), preference);
        }
    }
    result
}

/// Trusts nothing past "this is valid JSON": the file may be hand-edited or from an older
/// version, and a patch comes from the renderer. One bad value only loses that value.
fn sanitize(parsed: &Value) -> AppSettings {
    let Some(record) = parsed.as_object() else { return AppSettings::default() };
    let get = |key: &str| record.get(key).filter(|value| !value.is_null());
    AppSettings {
        download_schedule: get("downloadSchedule").map(|value| {
            valid_schedule(value)
                .unwrap_or_else(|| crate::types::DownloadSchedule {
                    enabled: true,
                    days: vec![],
                    ..default_schedule()
                })
        }),
        theme_source: get("themeSource")
            .and_then(|value| serde_json::from_value::<ThemeSource>(value.clone()).ok()),
        auto_update: (get("autoUpdate") == Some(&Value::Bool(false))).then_some(false),
        destination_dir: get("destinationDir")
            .and_then(Value::as_str)
            .filter(|dir| Path::new(dir).is_absolute())
            .map(str::to_owned),
        // Clamped rather than dropped: a count saved under an older, higher limit keeps the most.
        downloads_at_once: get("downloadsAtOnce")
            .and_then(Value::as_u64)
            .filter(|count| *count >= DOWNLOADS_AT_ONCE_MIN as u64)
            .map(|count| count.min(DOWNLOADS_AT_ONCE_MAX as u64) as u32),
        network_preferences: get("networkPreferences").map(sanitize_network_preferences),
        speed_limit: byte_count(get("speedLimit")),
        slow_mode_speed: byte_count(get("slowModeSpeed")),
        slow_mode: (get("slowMode") == Some(&Value::Bool(true))).then_some(true),
        speed_unit: get("speedUnit")
            .and_then(|value| serde_json::from_value::<SpeedUnit>(value.clone()).ok()),
        browser_extension_used: (get("browserExtensionUsed") == Some(&Value::Bool(true)))
            .then_some(true),
    }
}

impl Settings {
    pub fn new(data_dir: &Path) -> Self {
        Settings { path: data_dir.join("app-settings.json") }
    }

    pub async fn load(&self) -> AppSettings {
        match read_json(&self.path).await {
            Ok(Some(value)) => sanitize(&value),
            _ => AppSettings::default(),
        }
    }

    /// Merges `patch` over what's saved. A field set to null in the patch is cleared (the window
    /// sends `undefined` as null, since JSON can't carry it).
    pub async fn save(&self, patch: Value) -> io::Result<()> {
        if let Some(schedule) = patch.get("downloadSchedule").filter(|value| !value.is_null()) {
            if valid_schedule(schedule).is_none() {
                return Err(io::Error::other(
                    "Choose different start and stop times and at least one day.",
                ));
            }
        }
        update_json(&self.path, move |current| {
            let current = current.map(|value| sanitize(&value)).unwrap_or_default();
            let mut merged: Map<String, Value> = match serde_json::to_value(current) {
                Ok(Value::Object(map)) => map,
                _ => Map::new(),
            };
            if let Value::Object(patch) = patch {
                for (key, value) in patch {
                    if value.is_null() {
                        merged.remove(&key);
                    } else {
                        merged.insert(key, value);
                    }
                }
            }
            serde_json::to_value(sanitize(&Value::Object(merged))).unwrap_or(Value::Null)
        })
        .await
    }
}
