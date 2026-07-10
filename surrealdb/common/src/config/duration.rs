// --------------------------------------------------
// Duration
// --------------------------------------------------

use std::time::Duration;

/// Parse a duration string in the form `<number><unit>`.
///
/// Supported units:
///   - `µs` / `us` (microseconds)
///   - `ms` (milliseconds)
///   - `s` (seconds)
///   - `m` (minutes)
///   - `h` (hours)
///   - `d` (days)
pub fn parse_duration(s: &str) -> std::result::Result<Duration, String> {
	let s = s.trim();
	if s.is_empty() {
		return Err("Empty duration string".into());
	}
	// Plain numeric value (seconds)
	if let Ok(secs) = s.parse::<u64>() {
		return Ok(Duration::from_secs(secs));
	}
	// Split into numeric prefix and unit suffix
	let num_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
	let (num_str, unit) = s.split_at(num_end);
	if num_str.is_empty() {
		return Err(format!("Invalid duration string: '{s}'"));
	}
	let n: u64 = num_str.parse().map_err(|_| format!("Invalid duration number in: '{s}'"))?;
	match unit {
		"µs" | "us" => Ok(Duration::from_micros(n)),
		"ms" => Ok(Duration::from_millis(n)),
		"s" => Ok(Duration::from_secs(n)),
		"m" => Ok(Duration::from_secs(n * 60)),
		"h" => Ok(Duration::from_secs(n * 3600)),
		"d" => Ok(Duration::from_secs(n * 86400)),
		_ => Err(format!(
			"Unknown duration unit '{unit}' in: '{s}'. Expected µs, us, ms, s, m, h, or d"
		)),
	}
}

/// Format a duration as a compact string for query parameters.
///
/// Picks the largest unit that divides evenly, falling back to seconds.
pub fn format_duration(d: Duration) -> String {
	let micros = d.as_micros() as u64;
	if micros == 0 {
		return "0".to_string();
	}
	let secs = d.as_secs();
	// Try largest unit first
	if secs > 0 && secs.is_multiple_of(86400) && d.subsec_nanos() == 0 {
		return format!("{}d", secs / 86400);
	}
	if secs > 0 && secs.is_multiple_of(3600) && d.subsec_nanos() == 0 {
		return format!("{}h", secs / 3600);
	}
	if secs > 0 && secs.is_multiple_of(60) && d.subsec_nanos() == 0 {
		return format!("{}m", secs / 60);
	}
	if d.subsec_nanos() == 0 {
		return format!("{secs}s");
	}
	if micros.is_multiple_of(1000) {
		return format!("{}ms", micros / 1000);
	}
	format!("{micros}us")
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn test_parse_duration_plain_seconds() {
		assert_eq!(parse_duration("0").unwrap(), Duration::from_secs(0));
		assert_eq!(parse_duration("60").unwrap(), Duration::from_secs(60));
		assert_eq!(parse_duration("2592000").unwrap(), Duration::from_secs(2592000));
	}

	#[test]
	fn test_parse_duration_with_units() {
		assert_eq!(parse_duration("100µs").unwrap(), Duration::from_micros(100));
		assert_eq!(parse_duration("100us").unwrap(), Duration::from_micros(100));
		assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
		assert_eq!(parse_duration("60s").unwrap(), Duration::from_secs(60));
		assert_eq!(parse_duration("30m").unwrap(), Duration::from_secs(30 * 60));
		assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(24 * 3600));
		assert_eq!(parse_duration("30d").unwrap(), Duration::from_secs(30 * 86400));
	}

	#[test]
	fn test_parse_duration_invalid() {
		assert!(parse_duration("").is_err());
		assert!(parse_duration("abc").is_err());
		assert!(parse_duration("30x").is_err());
		assert!(parse_duration("30d8h").is_err());
	}

	#[test]
	fn test_format_duration() {
		assert_eq!(format_duration(Duration::from_secs(0)), "0");
		assert_eq!(format_duration(Duration::from_micros(500)), "500us");
		assert_eq!(format_duration(Duration::from_millis(100)), "100ms");
		assert_eq!(format_duration(Duration::from_secs(30)), "30s");
		assert_eq!(format_duration(Duration::from_secs(60)), "1m");
		assert_eq!(format_duration(Duration::from_secs(3600)), "1h");
		assert_eq!(format_duration(Duration::from_secs(86400)), "1d");
		// Non-round durations fall back to seconds
		assert_eq!(format_duration(Duration::from_secs(90)), "90s");
		assert_eq!(format_duration(Duration::from_secs(90061)), "90061s");
	}
}
