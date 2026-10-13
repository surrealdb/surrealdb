/// Parse a string into a byte value.
pub trait ParseBytes {
	/// Parse a string into a byte value.
	fn parse_bytes<T>(self) -> Result<T, &'static str>
	where
		T: TryFrom<u128>;
}

impl ParseBytes for &str {
	/// Parse a string into a byte value.
	fn parse_bytes<T>(self) -> Result<T, &'static str>
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
		// Parse the supplied suffix as a multiplier
		let suffix = match suffix.trim().to_lowercase().as_str() {
			"" | "b" => 1,
			"k" | "kb" | "kib" => 1024,
			"m" | "mb" | "mib" => 1024 * 1024,
			"g" | "gb" | "gib" => 1024 * 1024 * 1024,
			_ => return Err("Unknown suffix"),
		};
		// Multiply the input by the suffix
		let total = number.checked_mul(suffix).ok_or("Overflow during multiplication")?;
		// Return the parsed byte total
		T::try_from(total).map_err(|_| "Failed to convert to target type")
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_parse_bytes_plain() {
		assert_eq!("100".parse_bytes::<u64>().unwrap(), 100);
		assert_eq!("100b".parse_bytes::<u64>().unwrap(), 100);
	}

	#[test]
	fn test_parse_bytes_kilobytes() {
		assert_eq!("1k".parse_bytes::<u64>().unwrap(), 1024);
		assert_eq!("1kb".parse_bytes::<u64>().unwrap(), 1024);
		assert_eq!("1kib".parse_bytes::<u64>().unwrap(), 1024);
		assert_eq!("10k".parse_bytes::<u64>().unwrap(), 10 * 1024);
	}

	#[test]
	fn test_parse_bytes_megabytes() {
		assert_eq!("1m".parse_bytes::<u64>().unwrap(), 1024 * 1024);
		assert_eq!("1mb".parse_bytes::<u64>().unwrap(), 1024 * 1024);
		assert_eq!("1mib".parse_bytes::<u64>().unwrap(), 1024 * 1024);
		assert_eq!("64m".parse_bytes::<u64>().unwrap(), 64 * 1024 * 1024);
	}

	#[test]
	fn test_parse_bytes_gigabytes() {
		assert_eq!("1g".parse_bytes::<u64>().unwrap(), 1024 * 1024 * 1024);
		assert_eq!("1gb".parse_bytes::<u64>().unwrap(), 1024 * 1024 * 1024);
		assert_eq!("1gib".parse_bytes::<u64>().unwrap(), 1024 * 1024 * 1024);
		assert_eq!("2g".parse_bytes::<u64>().unwrap(), 2 * 1024 * 1024 * 1024);
	}

	#[test]
	fn test_parse_bytes_case_insensitive() {
		assert_eq!("1K".parse_bytes::<u64>().unwrap(), 1024);
		assert_eq!("1KB".parse_bytes::<u64>().unwrap(), 1024);
		assert_eq!("1M".parse_bytes::<u64>().unwrap(), 1024 * 1024);
		assert_eq!("1MB".parse_bytes::<u64>().unwrap(), 1024 * 1024);
		assert_eq!("1G".parse_bytes::<u64>().unwrap(), 1024 * 1024 * 1024);
		assert_eq!("1GB".parse_bytes::<u64>().unwrap(), 1024 * 1024 * 1024);
	}

	#[test]
	fn test_parse_bytes_with_whitespace() {
		assert_eq!("  100  ".parse_bytes::<u64>().unwrap(), 100);
		assert_eq!("  64m  ".parse_bytes::<u64>().unwrap(), 64 * 1024 * 1024);
	}

	#[test]
	fn test_parse_bytes_overflow() {
		// Test a value that exceeds u64::MAX
		let result = "18446744073709551616".parse_bytes::<u64>();
		assert!(result.is_err());
	}

	#[test]
	fn test_parse_bytes_invalid() {
		assert!("abc".parse_bytes::<u64>().is_err());
		assert!("100xyz".parse_bytes::<u64>().is_err());
		assert!("100tb".parse_bytes::<u64>().is_err()); // Unsupported suffix
	}

	#[test]
	fn test_parse_bytes_zero() {
		assert_eq!("0".parse_bytes::<u64>().unwrap(), 0);
		assert_eq!("0kb".parse_bytes::<u64>().unwrap(), 0);
	}
}
