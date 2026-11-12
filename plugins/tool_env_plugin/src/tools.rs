//! Tool definitions and the guest-side clock.
//!
//! The current time is computed inside the component: the host links WASI
//! clocks (`wasi:clocks/wall-clock`) backed by the real system clock, so
//! `SystemTime` works here and no host call is needed. Environment variables and
//! system information are NOT reachable from the guest and go through
//! `host-control` (permission-checked on the host side).

use crate::ToolDefinition;

/// Static tool definitions, published on every discovery request.
pub fn definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "env:time".to_string(),
            description: "Returns the current UTC date and time in ISO-8601 format \
                          (YYYY-MM-DDTHH:MM:SSZ). Use it whenever the user asks what \
                          time or date it is, or when a timestamp is needed. \
                          The value comes from the host clock."
                .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDefinition {
            name: "env:get".to_string(),
            description: "Returns the value of a host environment variable by exact name. \
                          Only variables explicitly allowed by the host policy are readable; \
                          any other name is rejected. Use it when the user asks about an \
                          environment setting (for example PATH or HOME), not for guessing."
                .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Exact environment variable name, e.g. PATH or HOME."
                    }
                },
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "env:system".to_string(),
            description: "Returns host system information as JSON: os, arch, family, cpus, \
                          loadavg and mem_total_kb. Use it for questions about the machine \
                          the agent runs on (platform, CPU count, memory, load). \
                          loadavg and memory are null on non-Linux hosts."
                .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {}
            }),
        },
    ]
}

/// Current UTC time in ISO-8601 (`YYYY-MM-DDTHH:MM:SSZ`).
pub fn now_iso_utc() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = (secs / 86_400) as i64;
    let rem = (secs % 86_400) as u32;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year,
        month,
        day,
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Days since 1970-01-01 -> (year, month, day).
///
/// Howard Hinnant's `civil_from_days`: no timezone database, no dependencies,
/// correct for the whole proleptic Gregorian calendar.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}
