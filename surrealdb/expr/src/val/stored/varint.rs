//! Canonical unsigned LEB128 for `u64`.
//!
//! A value is written in groups of seven bits, least significant group first.
//! The high bit of each byte says another byte follows. Each value has exactly
//! one encoding, at most [`MAX_LEN`] bytes: a reader refuses an encoding that
//! is longer than its value needs, that does not fit in 64 bits, or that ends
//! before its last byte, so two byte strings never name the same number.

/// The longest encoding of a `u64`.
pub const MAX_LEN: usize = 10;

/// Why a varint could not be read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum VarintError {
	#[error("a varint ends before its last byte")]
	Truncated,
	#[error("a varint is longer than its value needs")]
	Overlong,
	#[error("a varint does not fit in 64 bits")]
	Overflow,
}

/// Reads one canonical varint from the front of `b`: the value, and the number
/// of bytes it took.
#[inline]
pub fn read_u64(b: &[u8]) -> Result<(u64, usize), VarintError> {
	match b.first() {
		None => return Err(VarintError::Truncated),
		Some(&b0) if b0 < 0x80 => return Ok((u64::from(b0), 1)),
		Some(_) => {}
	}
	let mut v = 0u64;
	// `i <= 9`, so `7 * i <= 63` and no shift reaches the operand's width.
	for (i, &byte) in b.iter().take(MAX_LEN).enumerate() {
		// The tenth byte holds bit 63 only; a larger byte, or a continuation, overflows.
		if i == MAX_LEN - 1 && byte > 0x01 {
			return Err(VarintError::Overflow);
		}
		v |= u64::from(byte & 0x7F) << (7 * i);
		if byte & 0x80 == 0 {
			// `i > 0` here: the one-byte case returned above. A final group of
			// zero adds nothing, so a shorter encoding exists.
			return if byte == 0 {
				Err(VarintError::Overlong)
			} else {
				Ok((v, i + 1))
			};
		}
	}
	Err(VarintError::Truncated)
}

/// Writes `v` into `buf` and returns the bytes written, `1..=MAX_LEN` of them.
#[inline]
pub fn write_u64(v: u64, buf: &mut [u8; MAX_LEN]) -> &[u8] {
	// `len` is in `1..=MAX_LEN` and `i < len`, so every shift is at most 63 and
	// the split is inside the buffer.
	let len = len_u64(v);
	for (i, slot) in buf.iter_mut().take(len).enumerate() {
		let group = ((v >> (7 * i)) & 0x7F) as u8;
		*slot = if i + 1 < len {
			group | 0x80
		} else {
			group
		};
	}
	buf.split_at(len).0
}

/// The number of bytes [`write_u64`] writes for `v`.
#[inline]
pub const fn len_u64(v: u64) -> usize {
	(64 - (v | 1).leading_zeros() as usize).div_ceil(7)
}
