use std::cmp::max;
use std::path::PathBuf;

pub(crate) fn parse_generation_alloc_limit(s: &str) -> Result<usize, String> {
	let n = s.parse::<u32>().map_err(|e| e.to_string())?;
	Ok(2usize.pow(n.min(28)))
}

pub(crate) fn parse_path_list(s: &str) -> Result<Vec<PathBuf>, String> {
	let delimiter = if cfg!(target_os = "windows") {
		";"
	} else {
		":"
	};
	Ok(s.split(delimiter)
		.filter_map(|part| {
			let trimmed = part.trim();
			if trimmed.is_empty() {
				None
			} else {
				Some(PathBuf::from(trimmed))
			}
		})
		.collect())
}

pub(crate) fn parse_bytes<T: TryFrom<u128>>(s: &str) -> Result<T, String> {
	let s = s.trim();
	if let Ok(n) = s.parse::<u128>() {
		return T::try_from(n).map_err(|_| format!("Value {s} out of range"));
	}
	let num_end = s
		.find(|c: char| !c.is_ascii_digit())
		.ok_or_else(|| format!("Invalid byte string: '{s}'"))?;
	let (num_str, unit) = s.split_at(num_end);
	let n: u128 = num_str.parse().map_err(|_| format!("Invalid number in: '{s}'"))?;
	let multiplier: u128 = match unit.trim() {
		"B" | "b" => 1,
		"KiB" | "kib" | "KB" | "kb" | "K" | "k" => 1024,
		"MiB" | "mib" | "MB" | "mb" | "M" | "m" => 1024 * 1024,
		"GiB" | "gib" | "GB" | "gb" | "G" | "g" => 1024 * 1024 * 1024,
		"TiB" | "tib" | "TB" | "tb" | "T" | "t" => 1024 * 1024 * 1024 * 1024,
		u => return Err(format!("Unknown byte unit '{u}' in: '{s}'")),
	};
	let total = n.checked_mul(multiplier).ok_or_else(|| format!("Overflow in: '{s}'"))?;
	T::try_from(total).map_err(|_| format!("Value {total} out of range for target type"))
}

pub(crate) fn parse_duration_nanos(s: &str) -> Result<u64, String> {
	let s = s.trim();
	if let Ok(n) = s.parse::<u64>() {
		return Ok(n);
	}
	let num_end = s
		.find(|c: char| !c.is_ascii_digit())
		.ok_or_else(|| format!("Invalid duration string: '{s}'"))?;
	let (num_str, unit) = s.split_at(num_end);
	let n: u64 = num_str.parse().map_err(|_| format!("Invalid number in: '{s}'"))?;
	match unit.trim() {
		"ns" => Ok(n),
		"us" | "µs" => n.checked_mul(1_000).ok_or_else(|| format!("Overflow in: '{s}'")),
		"ms" => n.checked_mul(1_000_000).ok_or_else(|| format!("Overflow in: '{s}'")),
		"s" => n.checked_mul(1_000_000_000).ok_or_else(|| format!("Overflow in: '{s}'")),
		u => Err(format!("Unknown duration unit '{u}' in: '{s}'")),
	}
}

pub(crate) fn parse_bytes_usize(s: &str) -> Result<usize, String> {
	parse_bytes::<usize>(s)
}

pub(crate) fn parse_bytes_u64(s: &str) -> Result<u64, String> {
	parse_bytes::<u64>(s)
}

pub(crate) fn parse_memory_threshold(s: &str) -> Result<usize, String> {
	let n = parse_bytes::<usize>(s)?;
	Ok(match n {
		0 => 0,
		v => max(v, 1024 * 1024),
	})
}
