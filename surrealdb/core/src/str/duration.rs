/// Parse a string into a duration value.
pub trait ParseDuration {
	/// Parse a string into a duration value in nanoseconds.
	fn parse_duration<T>(self) -> Result<T, &'static str>
	where
		T: TryFrom<u128>;
}

impl ParseDuration for &str {
	/// Parse a string into a duration value in nanoseconds.
	fn parse_duration<T>(self) -> Result<T, &'static str>
	where
		T: TryFrom<u128>,
	{
		// Trim the input of any spaces
		let input = self.trim();
		// Check if this contains a suffix
		let split = input.find(|c: char| !c.is_ascii_digit());
		// Split the value into number and suffix
		let (number, suffix) = match split {
			Some(index) => input.split_at(index),
			None => (input, ""),
		};
		// Parse the number as a positive number
		let number = number.trim().parse::<u128>().map_err(|_| "Invalid number")?;
		// Parse the supplied suffix as a multiplier (converting to nanoseconds)
		let suffix: u128 = match suffix.trim().to_lowercase().as_str() {
			"ns" => 1,
			"µs" | "us" => 1_000,
			"ms" => 1_000_000,
			"s" => 1_000_000_000,
			"m" => 60 * 1_000_000_000,
			"h" => 60 * 60 * 1_000_000_000,
			_ => return Err("Unknown suffix"),
		};
		// Multiply the input by the suffix
		let total = number.checked_mul(suffix).ok_or("Overflow during multiplication")?;
		// Return the parsed duration in nanoseconds
		T::try_from(total).map_err(|_| "Failed to convert to target type")
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_parse_duration_nanoseconds() {
		assert_eq!("100ns".parse_duration::<u64>().unwrap(), 100);
	}

	#[test]
	fn test_parse_duration_microseconds() {
		assert_eq!("100us".parse_duration::<u64>().unwrap(), 100_000);
		assert_eq!("100µs".parse_duration::<u64>().unwrap(), 100_000);
	}

	#[test]
	fn test_parse_duration_milliseconds() {
		assert_eq!("100ms".parse_duration::<u64>().unwrap(), 100_000_000);
	}

	#[test]
	fn test_parse_duration_seconds() {
		assert_eq!("5s".parse_duration::<u64>().unwrap(), 5_000_000_000);
	}

	#[test]
	fn test_parse_duration_minutes() {
		assert_eq!("2m".parse_duration::<u64>().unwrap(), 120_000_000_000);
	}

	#[test]
	fn test_parse_duration_hours() {
		assert_eq!("1h".parse_duration::<u64>().unwrap(), 3_600_000_000_000);
	}

	#[test]
	fn test_parse_duration_with_whitespace() {
		assert_eq!("  100ms  ".parse_duration::<u64>().unwrap(), 100_000_000);
	}

	#[test]
	fn test_parse_duration_overflow() {
		let result = "18446744073709551616".parse_duration::<u64>();
		assert!(result.is_err());
	}

	#[test]
	fn test_parse_duration_nosuffix() {
		let result = "18471".parse_duration::<u64>();
		assert!(result.is_err());
	}

	#[test]
	fn test_parse_duration_invalid() {
		assert!("abc".parse_duration::<u64>().is_err());
		assert!("100xyz".parse_duration::<u64>().is_err());
	}
}
