//! The download schedule's windows and the data limits' calendar periods, both in the computer's
//! local time zone (see src/shared/downloadSchedule.ts and dataLimits.ts).

use crate::types::{DataLimitPeriod, DownloadSchedule};
use chrono::{DateTime, Datelike, Duration, Local, LocalResult, NaiveDate, TimeZone};
use serde_json::Value;

pub fn default_schedule() -> DownloadSchedule {
    DownloadSchedule { enabled: false, start_minute: 0, end_minute: 7 * 60, days: (0..7).collect(), end_date: None }
}

fn local_date(date: NaiveDate) -> String {
    date.format("%Y-%m-%d").to_string()
}

/// Whether `value` is a schedule the window could have saved: valid times, at least one day, and
/// an end date that is a real day.
pub fn valid_schedule(value: &Value) -> Option<DownloadSchedule> {
    let schedule: DownloadSchedule = serde_json::from_value(value.clone()).ok()?;
    let minutes_ok = [schedule.start_minute, schedule.end_minute].iter().all(|minute| *minute < 1440);
    if !minutes_ok
        || schedule.start_minute == schedule.end_minute
        || schedule.days.is_empty()
        || schedule.days.iter().any(|day| *day > 6)
    {
        return None;
    }
    if let Some(end) = &schedule.end_date {
        let parsed = NaiveDate::parse_from_str(end, "%Y-%m-%d").ok()?;
        if local_date(parsed) != *end {
            return None;
        }
    }
    Some(schedule)
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ScheduleWindow {
    pub allowed: bool,
    pub next_start: Option<f64>,
    pub ends_at: Option<f64>,
    pub expired: bool,
}

/// `date` at `minute` past local midnight. A time in the hour skipped when clocks spring forward
/// doesn't exist: the clock passes it the moment it jumps, so that instant is used instead.
fn at_local_minute(date: NaiveDate, minute: u32) -> DateTime<Local> {
    let naive = date.and_hms_opt(minute / 60, minute % 60, 0).expect("minute < 1440");
    match Local.from_local_datetime(&naive) {
        LocalResult::Single(at) => at,
        LocalResult::Ambiguous(earliest, _) => earliest,
        LocalResult::None => {
            let mut probe = naive;
            loop {
                probe += Duration::minutes(1);
                match Local.from_local_datetime(&probe) {
                    LocalResult::Single(at) => return at,
                    LocalResult::Ambiguous(earliest, _) => return earliest,
                    LocalResult::None => continue,
                }
            }
        }
    }
}

fn millis(at: DateTime<Local>) -> f64 {
    at.timestamp_millis() as f64
}

/// Overnight windows belong to the day they start; the end date includes that whole window.
pub fn schedule_window(schedule: Option<&DownloadSchedule>, now: f64) -> ScheduleWindow {
    let Some(schedule) = schedule.filter(|schedule| schedule.enabled) else {
        return ScheduleWindow { allowed: true, ..Default::default() };
    };
    let valid = serde_json::to_value(schedule).ok().and_then(|value| valid_schedule(&value));
    if valid.is_none() {
        return ScheduleWindow::default();
    }
    let today = Local.timestamp_millis_opt(now as i64).single().unwrap_or_else(Local::now).date_naive();
    let mut next_start = None;
    for offset in -1..=7i64 {
        let start_day = today + Duration::days(offset);
        if !schedule.days.contains(&start_day.weekday().num_days_from_sunday()) {
            continue;
        }
        if schedule.end_date.as_deref().is_some_and(|end| local_date(start_day).as_str() > end) {
            continue;
        }
        let end_day = if schedule.end_minute < schedule.start_minute {
            start_day + Duration::days(1)
        } else {
            start_day
        };
        let end = millis(at_local_minute(end_day, schedule.end_minute));
        let start = millis(at_local_minute(start_day, schedule.start_minute));
        if now >= start && now < end {
            return ScheduleWindow { allowed: true, ends_at: Some(end), ..Default::default() };
        }
        if start > now && next_start.is_none() {
            next_start = Some(start);
        }
    }
    ScheduleWindow { allowed: false, next_start, ends_at: None, expired: next_start.is_none() }
}

/// Calendar boundaries in local time. Weeks start Monday.
pub fn data_period_key(period: DataLimitPeriod, now: f64) -> String {
    let mut date =
        Local.timestamp_millis_opt(now as i64).single().unwrap_or_else(Local::now).date_naive();
    if period == DataLimitPeriod::Week {
        date -= Duration::days(date.weekday().num_days_from_monday() as i64);
    }
    match period {
        DataLimitPeriod::Month => date.format("%Y-%m").to_string(),
        _ => date.format("%Y-%m-%d").to_string(),
    }
}
