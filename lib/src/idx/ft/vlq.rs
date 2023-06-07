pub(super) fn delta_vlq_encode(input: Vec<u32>) -> Vec<u8> {
	let mut deltas: Vec<u32> = Vec::new();

	let mut last = 0;
	let mut iter = input.iter();
	// Keep the first number as is.
	if let Some(first) = iter.next() {
		last = *first;
		deltas.push(last);
	}

	// Calculate deltas for the rest
	while let Some(next) = iter.next() {
		let value = *next;
		let delta = value.wrapping_sub(last);
		deltas.push(delta);
		last = value;
	}

	// Encode deltas with VLQ
	deltas.into_iter().flat_map(|delta| vlq_encode(delta)).collect()
}

fn vlq_encode(mut value: u32) -> Vec<u8> {
	let mut result = Vec::new();
	loop {
		let mut byte = (value & 0x7F) as u8;
		value = value.wrapping_shr(7);
		if value != 0 {
			byte |= 0x80; // Set the continuation bit
		}
		result.push(byte);
		if value == 0 {
			break;
		}
	}
	result
}

pub(super) fn delta_vlq_decode(input: Vec<u8>) -> Vec<u32> {
	// Decode VLQ to deltas
	let deltas = vlq_decode(&input);

	// Calculate original values from deltas
	let mut value: u32 = 0;
	deltas
		.into_iter()
		.map(|delta| {
			value = value.wrapping_add(delta);
			value
		})
		.collect()
}

fn vlq_decode(input: &[u8]) -> Vec<u32> {
	let mut result = Vec::new();
	let mut value: u32 = 0;
	let mut shift = 0;
	for &byte in input {
		value |= ((byte & 0x7F) as u32) << shift;
		if byte & 0x80 == 0 {
			result.push(value);
			value = 0;
			shift = 0;
		} else {
			shift += 7;
		}
	}
	result
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::vlq::{delta_vlq_decode, delta_vlq_encode};

	fn test_vlq(v1: Vec<u32>) {
		let buf = delta_vlq_encode(v1.clone());
		// Did we really compress?
		if !v1.is_empty() {
			assert!(buf.len() < v1.len() * 8, "{} vs {}", buf.len(), v1.len() * 8);
		}
		println!("{} vs {}", buf.len(), v1.len() * 8);

		let v2 = delta_vlq_decode(buf);
		assert_eq!(v1, v2);
	}

	#[test]
	fn test_vlq_ascending_small_diff() {
		test_vlq(vec![0, 2, 5]);
	}

	#[test]
	fn test_vlq_descending_small_diff() {
		test_vlq(vec![5, 2, 0]);
	}

	#[test]
	fn test_vlq_random_small_diff() {
		test_vlq(vec![2, 5, 0]);
	}

	#[test]
	fn test_vlq_ascending_large_diff() {
		test_vlq(vec![0u32, i32::MAX as u32, u32::MAX]);
	}

	#[test]
	fn test_vlq_descending_large_diff() {
		test_vlq(vec![u32::MAX, i32::MAX as u32, 0u32]);
	}

	#[test]
	fn test_vlq_random_large_diff() {
		test_vlq(vec![u32::MAX, 0u32, i32::MAX as u32]);
	}

	#[test]
	fn test_vlq_empty() {
		test_vlq(vec![]);
	}
}
