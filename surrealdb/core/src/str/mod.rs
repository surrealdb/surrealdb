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
