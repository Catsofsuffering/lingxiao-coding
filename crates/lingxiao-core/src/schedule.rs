pub struct Scheduler;

impl Scheduler {
    pub fn new() -> Self {
        Self
    }

    pub fn next_run_after(cron: &str, after_ms: u64) -> Option<u64> {
        next_cron_run_after(cron, after_ms).or_else(|| {
            let interval_ms = schedule_interval_ms(cron)?;
            Some(after_ms.saturating_add(interval_ms))
        })
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

fn schedule_interval_ms(spec: &str) -> Option<u64> {
    let normalized = spec.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "@once" | "once" => None,
        "@hourly" => Some(60 * 60 * 1000),
        "@daily" => Some(24 * 60 * 60 * 1000),
        "@weekly" => Some(7 * 24 * 60 * 60 * 1000),
        _ => normalized
            .strip_prefix("every ")
            .or_else(|| normalized.strip_prefix("every:"))
            .and_then(parse_duration_ms),
    }
}

fn parse_duration_ms(value: &str) -> Option<u64> {
    let value = value.trim();
    let unit = value.chars().last()?;
    let number = value[..value.len().saturating_sub(unit.len_utf8())]
        .trim()
        .parse::<u64>()
        .ok()?;
    match unit {
        's' => Some(number.saturating_mul(1000)),
        'm' => Some(number.saturating_mul(60 * 1000)),
        'h' => Some(number.saturating_mul(60 * 60 * 1000)),
        'd' => Some(number.saturating_mul(24 * 60 * 60 * 1000)),
        _ => None,
    }
}

fn next_cron_run_after(spec: &str, after_ms: u64) -> Option<u64> {
    let fields = spec.split_whitespace().collect::<Vec<_>>();
    if fields.len() != 5 {
        return None;
    }
    let minute = CronField::parse(fields[0], 0, 59)?;
    let hour = CronField::parse(fields[1], 0, 23)?;
    let day_of_month = CronField::parse(fields[2], 1, 31)?;
    let month = CronField::parse(fields[3], 1, 12)?;
    let day_of_week = CronField::parse(fields[4], 0, 7)?;

    let mut candidate_minute = after_ms / 60_000 + 1;
    let max_candidate = candidate_minute.saturating_add(366 * 24 * 60 * 5);
    while candidate_minute <= max_candidate {
        let date = UtcMinute::from_unix_minute(candidate_minute as i64);
        if minute.matches(date.minute)
            && hour.matches(date.hour)
            && day_of_month.matches(date.day)
            && month.matches(date.month)
            && day_of_week.matches(date.day_of_week)
        {
            return Some(candidate_minute.saturating_mul(60_000));
        }
        candidate_minute = candidate_minute.saturating_add(1);
    }
    None
}

struct CronField {
    allowed: Vec<u32>,
}

impl CronField {
    fn parse(spec: &str, min: u32, max: u32) -> Option<Self> {
        let mut allowed = Vec::new();
        for part in spec.split(',') {
            let (range, step) = match part.split_once('/') {
                Some((range, step)) => (range, step.parse::<u32>().ok()?),
                None => (part, 1),
            };
            if step == 0 {
                return None;
            }
            let (start, end) = if range == "*" {
                (min, max)
            } else if let Some((start, end)) = range.split_once('-') {
                (start.parse::<u32>().ok()?, end.parse::<u32>().ok()?)
            } else {
                let value = range.parse::<u32>().ok()?;
                (value, value)
            };
            if start < min || end > max || start > end {
                return None;
            }
            let mut value = start;
            while value <= end {
                let normalized = if max == 7 && value == 7 { 0 } else { value };
                if !allowed.contains(&normalized) {
                    allowed.push(normalized);
                }
                value = value.saturating_add(step);
                if step == 0 {
                    break;
                }
            }
        }
        Some(Self { allowed })
    }

    fn matches(&self, value: u32) -> bool {
        self.allowed.contains(&value)
    }
}

struct UtcMinute {
    minute: u32,
    hour: u32,
    day: u32,
    month: u32,
    day_of_week: u32,
}

impl UtcMinute {
    fn from_unix_minute(unix_minute: i64) -> Self {
        let days = unix_minute.div_euclid(24 * 60);
        let minute_of_day = unix_minute.rem_euclid(24 * 60) as u32;
        let (year, month, day) = civil_from_days(days);
        let _ = year;
        Self {
            minute: minute_of_day % 60,
            hour: minute_of_day / 60,
            day,
            month,
            day_of_week: day_of_week_from_days(days),
        }
    }
}

fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year as i32, month as u32, day as u32)
}

fn day_of_week_from_days(days_since_epoch: i64) -> u32 {
    // Unix epoch 1970-01-01 was Thursday. Cron uses 0/7 = Sunday.
    ((days_since_epoch + 4).rem_euclid(7)) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scheduler_creation() {
        let _s = Scheduler::new();
    }

    #[test]
    fn test_scheduler_next_run_common_specs() {
        assert_eq!(Scheduler::next_run_after("@hourly", 1_000), Some(3_601_000));
        assert_eq!(Scheduler::next_run_after("every 5m", 0), Some(300_000));
        assert_eq!(Scheduler::next_run_after("*/15 * * * *", 0), Some(900_000));
        assert_eq!(Scheduler::next_run_after("@once", 0), None);
    }

    #[test]
    fn test_scheduler_next_run_standard_cron_fields() {
        assert_eq!(Scheduler::next_run_after("5 * * * *", 0), Some(300_000));
        assert_eq!(
            Scheduler::next_run_after("0 1 * * *", 0),
            Some(60 * 60 * 1000)
        );
        assert_eq!(
            Scheduler::next_run_after("10,20 2-3 * * *", 0),
            Some((2 * 60 + 10) * 60 * 1000)
        );
    }

    #[test]
    fn test_scheduler_next_run_weekday_cron() {
        // 1970-01-01 was Thursday; next Friday midnight is 1970-01-02.
        assert_eq!(
            Scheduler::next_run_after("0 0 * * 5", 0),
            Some(24 * 60 * 60 * 1000)
        );
        assert_eq!(
            Scheduler::next_run_after("0 0 * * 7", 0),
            Some(3 * 24 * 60 * 60 * 1000)
        );
    }
}
