//! Protocols for communicating with the server

#[cfg(feature = "protocol-http")]
#[cfg_attr(docsrs, doc(cfg(feature = "protocol-http")))]
pub mod http;

#[cfg(feature = "protocol-ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "protocol-ws")))]
pub mod ws;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::time::Duration;

const NANOS_PER_SEC: i64 = 1_000_000_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const NANOS_PER_MICRO: i64 = 1_000;

pub struct WsNotification {}

// Converts a debug representation of `std::time::Duration` back
fn duration_from_str(duration: &str) -> Option<std::time::Duration> {
	let nanos = if let Some(duration) = duration.strip_suffix("ns") {
		duration.parse().ok()?
	} else if let Some(duration) = duration.strip_suffix("µs") {
		let micros = duration.parse::<Decimal>().ok()?;
		let multiplier = Decimal::try_new(NANOS_PER_MICRO, 0).ok()?;
		micros.checked_mul(multiplier)?.to_u128()?
	} else if let Some(duration) = duration.strip_suffix("ms") {
		let millis = duration.parse::<Decimal>().ok()?;
		let multiplier = Decimal::try_new(NANOS_PER_MILLI, 0).ok()?;
		millis.checked_mul(multiplier)?.to_u128()?
	} else {
		let duration = duration.strip_suffix('s')?;
		let secs = duration.parse::<Decimal>().ok()?;
		let multiplier = Decimal::try_new(NANOS_PER_SEC, 0).ok()?;
		secs.checked_mul(multiplier)?.to_u128()?
	};
	let secs = nanos.checked_div(NANOS_PER_SEC as u128)?;
	let nanos = nanos % (NANOS_PER_SEC as u128);
	Some(Duration::new(secs.try_into().ok()?, nanos.try_into().ok()?))
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	#[test]
	fn duration_from_str() {
		let durations = vec![
			Duration::ZERO,
			Duration::from_nanos(1),
			Duration::from_nanos(u64::MAX),
			Duration::from_micros(1),
			Duration::from_micros(u64::MAX),
			Duration::from_millis(1),
			Duration::from_millis(u64::MAX),
			Duration::from_secs(1),
			Duration::from_secs(u64::MAX),
			Duration::MAX,
		];

		for duration in durations {
			let string = format!("{duration:?}");
			let parsed = super::duration_from_str(&string)
				.unwrap_or_else(|| panic!("Duration {string} failed to parse"));
			assert_eq!(duration, parsed, "Duration {string} not parsed correctly");
		}
	}
}
