//! Lucene-style SmallFloat encoding for document lengths.
//!
//! Encodes a u32 value into a single byte using 3 bits for mantissa
//! and 5 bits for exponent. This provides ~12.5% maximum error but
//! reduces storage from 8 bytes to 1 byte per document.
//!
//! Reference: https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/util/SmallFloat.java

/// SmallFloat encoder/decoder for document lengths.
///
/// Uses Lucene's format: 3-bit mantissa (0-7) + 5-bit exponent (0-31).
/// Maximum representable value: 7 << 31 = ~15 billion.
pub struct SmallFloat;

/// Maximum exponent that can be decoded without overflowing u32.
/// With mantissa | 0x08 = 15 max, shift of 28 gives 15 << 28 = 4026531840 < u32::MAX.
/// Shift of 29 gives 15 << 29 = 8053063680 > u32::MAX (overflow).
const MAX_EXPONENT: u32 = 29;

/// Maximum encoded byte value (exponent=29, mantissa=7).
const MAX_ENCODED: u8 = ((MAX_EXPONENT as u8) << 3) | 0x07; // 239

/// Precomputed lookup table for length normalization.
///
/// Stores 256 precomputed values of:
/// `1 / (1 - b + b * decoded_length / avg_doc_length)`
///
/// This eliminates repeated computation during scoring.
pub struct NormCache {
	values: [f64; 256],
}

impl NormCache {
	/// Creates a new norm cache with precomputed values.
	///
	/// # Arguments
	/// * `k1` - BM25 k1 parameter (unused but kept for API consistency)
	/// * `b` - BM25 b parameter for length normalization
	/// * `avg_doc_len` - Average document length in the index
	pub fn new(_k1: f64, b: f64, avg_doc_len: f64) -> Self {
		let mut values = [0.0f64; 256];
		let one_minus_b = 1.0 - b;
		let b_over_avg = if avg_doc_len > 0.0 {
			b / avg_doc_len
		} else {
			0.0
		};

		for (i, value) in values.iter_mut().enumerate() {
			let decoded_len = SmallFloat::decode(i as u8) as f64;
			let norm = one_minus_b + b_over_avg * decoded_len;
			// Store inverse to avoid division during scoring
			*value = if norm > 0.0 {
				1.0 / norm
			} else {
				1.0
			};
		}

		Self {
			values,
		}
	}

	/// Gets the precomputed norm inverse for an encoded length.
	#[inline]
	pub fn get(&self, encoded_length: u8) -> f64 {
		self.values[encoded_length as usize]
	}

	/// Returns the number of cached values (always 256).
	#[cfg(test)]
	pub fn len(&self) -> usize {
		self.values.len()
	}
}

impl SmallFloat {
	/// Encodes a u32 value into a single byte.
	///
	/// Values 0-7 are stored exactly. Larger values lose precision
	/// (max ~12.5% error due to 3-bit mantissa).
	/// Values larger than ~4 billion saturate to the maximum encoded value.
	pub fn encode(value: u32) -> u8 {
		if value == 0 {
			return 0;
		}

		// Find the position of the highest set bit (0-indexed)
		let highest_bit = 31 - value.leading_zeros();

		// For values 0-7, we can store exactly (exponent = 0)
		if highest_bit < 3 {
			return value as u8;
		}

		// Exponent: how many bits above the lowest 3
		let exponent = highest_bit - 2;

		// Check for overflow - saturate at max representable value
		if exponent > MAX_EXPONENT {
			return MAX_ENCODED;
		}

		// Mantissa: top 3 bits of the value (excluding the implicit leading 1)
		let mantissa = (value >> (exponent - 1)) & 0x07;

		// Combine: 5-bit exponent in high bits, 3-bit mantissa in low bits
		((exponent as u8) << 3) | (mantissa as u8)
	}

	/// Decodes a byte back to a u32 value.
	///
	/// The decoded value may differ from the original by up to 12.5%
	/// due to mantissa quantization.
	/// Encoded values above MAX_ENCODED saturate to u32::MAX.
	pub fn decode(encoded: u8) -> u32 {
		if encoded == 0 {
			return 0;
		}

		let exponent = (encoded >> 3) as u32;
		let mantissa = (encoded & 0x07) as u32;

		if exponent == 0 {
			// Small values (0-7) stored exactly
			return mantissa;
		}

		// Handle overflow for encoded values beyond our range
		if exponent > MAX_EXPONENT {
			return u32::MAX;
		}

		// Reconstruct: mantissa has implicit high bit set
		// Value = mantissa << (exponent - 1), with the leading 1 restored
		(mantissa | 0x08) << (exponent - 1)
	}
}

#[cfg(test)]
mod tests {
	use super::{NormCache, SmallFloat};

	#[test]
	fn test_norm_cache_generation() {
		let k1 = 1.2f64;
		let b = 0.75f64;
		let avg_doc_len = 100.0f64;

		let cache = NormCache::new(k1, b, avg_doc_len);

		// Check cache size
		assert_eq!(cache.len(), 256);

		// Verify formula: norm_inv = 1 / (1 - b + b * decoded_len / avg_len)
		// For encoded=0 (decoded=0): norm_inv = 1 / (1 - 0.75 + 0) = 1 / 0.25 = 4.0
		assert!((cache.get(0) - 4.0).abs() < 0.001);

		// For a mid-range value, verify it's reasonable
		let mid = cache.get(128);
		assert!(mid > 0.0 && mid < 10.0, "Mid value should be reasonable: {}", mid);
	}

	#[test]
	fn test_encode_decode_roundtrip() {
		// Small values
		assert_eq!(SmallFloat::decode(SmallFloat::encode(0)), 0);
		assert_eq!(SmallFloat::decode(SmallFloat::encode(1)), 1);
		assert_eq!(SmallFloat::decode(SmallFloat::encode(7)), 7);

		// Medium values (some precision loss expected)
		let encoded = SmallFloat::encode(100);
		let decoded = SmallFloat::decode(encoded);
		assert!((decoded as i32 - 100).abs() <= 13, "100 -> {} (max 12.5% error)", decoded);

		// Large values
		let encoded = SmallFloat::encode(10000);
		let decoded = SmallFloat::decode(encoded);
		assert!((decoded as i32 - 10000).abs() <= 1250, "10000 -> {} (max 12.5% error)", decoded);
	}

	#[test]
	fn test_monotonicity() {
		// Larger inputs should never produce smaller outputs after decode
		let mut prev_decoded = 0u32;
		for i in 0..=255u8 {
			let decoded = SmallFloat::decode(i);
			assert!(
				decoded >= prev_decoded,
				"Monotonicity violated at {}: {} < {}",
				i,
				decoded,
				prev_decoded
			);
			prev_decoded = decoded;
		}
	}

	#[test]
	fn test_encode_saturates_at_max() {
		// Values beyond max representable should encode to MAX_ENCODED (239)
		// MAX_ENCODED = (29 << 3) | 7 = 239, which decodes to (7|8) << 28 = 4026531840
		assert_eq!(SmallFloat::encode(u32::MAX), 239);

		// 1 << 24 = 16777216, which is within range (highest_bit=24, exponent=22)
		// Let's verify it encodes correctly
		let encoded = SmallFloat::encode(1 << 24);
		let decoded = SmallFloat::decode(encoded);
		// Should be close to 16777216
		assert!(decoded >= (1 << 24) - (1 << 21), "1<<24 should encode/decode reasonably");
	}

	#[test]
	fn test_encode_boundary_values() {
		// Test exact boundary between exact and lossy encoding
		// Values 0-7 are stored exactly
		for i in 0..=7u32 {
			assert_eq!(SmallFloat::decode(SmallFloat::encode(i)), i, "Value {} should be exact", i);
		}

		// Value 8 starts losing precision (3-bit mantissa can't represent 1000 exactly)
		let encoded_8 = SmallFloat::encode(8);
		let decoded_8 = SmallFloat::decode(encoded_8);
		assert!((8..=9).contains(&decoded_8), "8 should decode to 8 or 9, got {}", decoded_8);
	}

	#[test]
	fn test_decode_boundary_values() {
		// Test decoding edge cases
		assert_eq!(SmallFloat::decode(0), 0);
		assert_eq!(SmallFloat::decode(1), 1);
		assert_eq!(SmallFloat::decode(7), 7);

		// Encoded 8 = exponent 1, mantissa 0 = (0|8) << 0 = 8
		assert_eq!(SmallFloat::decode(8), 8);

		// MAX_ENCODED (239) should decode to max representable
		let max_decoded = SmallFloat::decode(239);
		assert!(max_decoded > 0, "MAX_ENCODED should decode to positive value");

		// Values above MAX_ENCODED should saturate to u32::MAX
		assert_eq!(SmallFloat::decode(240), u32::MAX);
		assert_eq!(SmallFloat::decode(255), u32::MAX);
	}

	#[test]
	fn test_precision_error_bounds() {
		// Verify that precision error is within 12.5% for various ranges
		let test_values = [10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 1000000];

		for &value in &test_values {
			let encoded = SmallFloat::encode(value);
			let decoded = SmallFloat::decode(encoded);

			// Calculate relative error
			let error = (decoded as f64 - value as f64).abs() / value as f64;

			// Error should be <= 12.5% (0.125)
			assert!(
				error <= 0.125,
				"Value {} encoded to {}, decoded to {} - error {:.2}% exceeds 12.5%",
				value,
				encoded,
				decoded,
				error * 100.0
			);
		}
	}

	#[test]
	fn test_norm_cache_zero_avg_doc_len() {
		// Edge case: zero average doc length (empty index)
		let cache = NormCache::new(1.2, 0.75, 0.0);

		// With avg_doc_len=0, b_over_avg=0, so norm = 1 - b = 0.25
		// norm_inv = 1 / 0.25 = 4.0 for all values
		assert!((cache.get(0) - 4.0).abs() < 0.001);
		assert!((cache.get(100) - 4.0).abs() < 0.001);
		assert!((cache.get(255) - 4.0).abs() < 0.001);
	}

	#[test]
	fn test_norm_cache_zero_b_parameter() {
		// With b=0, length normalization is disabled
		let cache = NormCache::new(1.2, 0.0, 100.0);

		// norm = 1 - 0 + 0 = 1, so norm_inv = 1.0 for all values
		for i in 0..=255u8 {
			assert!(
				(cache.get(i) - 1.0).abs() < 0.001,
				"With b=0, norm_inv should be 1.0, got {} for encoded={}",
				cache.get(i),
				i
			);
		}
	}

	#[test]
	fn test_norm_cache_full_b_parameter() {
		// With b=1.0, full length normalization
		let cache = NormCache::new(1.2, 1.0, 100.0);

		// For decoded_len = 100 (avg), norm = 0 + 1.0 * 100/100 = 1.0
		// norm_inv = 1.0
		let encoded_100 = SmallFloat::encode(100);
		let norm_at_avg = cache.get(encoded_100);
		assert!(
			(norm_at_avg - 1.0).abs() < 0.15, // Allow for SmallFloat precision loss
			"At avg doc length, norm_inv should be ~1.0, got {}",
			norm_at_avg
		);

		// Shorter docs should have higher norm_inv (bonus)
		let encoded_50 = SmallFloat::encode(50);
		assert!(cache.get(encoded_50) > norm_at_avg, "Shorter docs should have higher norm_inv");

		// Longer docs should have lower norm_inv (penalty)
		let encoded_200 = SmallFloat::encode(200);
		assert!(cache.get(encoded_200) < norm_at_avg, "Longer docs should have lower norm_inv");
	}

	#[test]
	fn test_encode_powers_of_two() {
		// Powers of two are interesting because they hit exponent boundaries
		for exp in 0..31 {
			let value = 1u32 << exp;
			let encoded = SmallFloat::encode(value);
			let decoded = SmallFloat::decode(encoded);

			// Should be within 12.5% error
			let error = (decoded as f64 - value as f64).abs() / value.max(1) as f64;
			assert!(
				error <= 0.125,
				"Power of 2: 2^{} = {} encoded to {}, decoded to {} - error {:.2}%",
				exp,
				value,
				encoded,
				decoded,
				error * 100.0
			);
		}
	}

	#[test]
	fn test_typical_document_lengths() {
		// Test document lengths typical in full-text search
		let typical_lengths = [
			5,     // very short doc (title)
			50,    // short paragraph
			200,   // medium paragraph
			500,   // long paragraph
			1000,  // short article
			5000,  // medium article
			10000, // long article
			50000, // book chapter
		];

		for &len in &typical_lengths {
			let encoded = SmallFloat::encode(len);
			let decoded = SmallFloat::decode(encoded);

			// For BM25, the precision loss is acceptable
			let error_pct = ((decoded as i64 - len as i64).abs() as f64 / len as f64) * 100.0;
			assert!(
				error_pct <= 12.5,
				"Doc length {} has {:.1}% error (max 12.5%)",
				len,
				error_pct
			);
		}
	}
}
