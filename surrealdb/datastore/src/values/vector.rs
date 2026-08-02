//! How a vector is stored, and how it is spelled inside a key.
//!
//! An ANN index stores the vector itself twice over: as the value under an
//! element key, and - when vectors are deduplicated by content - as a hash that
//! addresses the bucket holding every element sharing that vector. Both are
//! keyspace bytes, so the payload, its two encodings and the hash that derives a
//! key from it are all declared here.
//!
//! Values use ordinary revisioned encoding. Keys use a separate, explicitly
//! versioned wire form with a fixed discriminant per variant, so that adding a
//! variant cannot renumber an existing one and orphan the keys already written
//! under it. That is why the discriminants are consts rather than the enum's own
//! ordering, and why they are pinned by tests in this file.
//!
//! The in-memory representation search actually operates on, and the conversion
//! from a query value, stay with the index engines above.

use std::io::Write;

use anyhow::Result;
use blake3::Hasher as Blake3Hasher;
use revision::{DeserializeRevisioned, SerializeRevisioned, revisioned};
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, BorrowReader, DecodeError, Encode, EncodeError, Writer};
use surrealdb_kvs::value::KVValue;

/// The identifier a stored vector element is addressed by.
///
/// Shared by both graph families: `!he`/`!de` map one of these to a vector
/// payload, and the proximity graphs are built over them.
pub type ElementId = u64;

const SERIALIZED_VECTOR_KEY_REVISION: u16 = 1;
const SERIALIZED_VECTOR_F64_KEY_DISCRIMINANT: u32 = 0;
const SERIALIZED_VECTOR_F32_KEY_DISCRIMINANT: u32 = 1;
const SERIALIZED_VECTOR_I64_KEY_DISCRIMINANT: u32 = 2;
const SERIALIZED_VECTOR_I32_KEY_DISCRIMINANT: u32 = 3;
const SERIALIZED_VECTOR_I16_KEY_DISCRIMINANT: u32 = 4;
const SERIALIZED_VECTOR_F16_KEY_DISCRIMINANT: u32 = 5;
const SERIALIZED_VECTOR_I8_KEY_DISCRIMINANT: u32 = 6;
const SERIALIZED_VECTOR_U8_KEY_DISCRIMINANT: u32 = 7;

/// Vector payload stored in ANN keys and values.
#[revisioned(revision = 2)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SerializedVector {
	/// 64-bit floating-point vector.
	F64(Vec<f64>),
	/// 32-bit floating-point vector.
	F32(Vec<f32>),
	/// 64-bit signed integer vector.
	I64(Vec<i64>),
	/// 32-bit signed integer vector.
	I32(Vec<i32>),
	/// 16-bit signed integer vector.
	I16(Vec<i16>),
	/// 16-bit floating-point vector encoded as IEEE-754 half bits.
	#[revision(start = 2)]
	F16(Vec<u16>),
	/// 8-bit signed integer vector.
	#[revision(start = 2)]
	I8(Vec<i8>),
	/// 8-bit unsigned integer vector.
	#[revision(start = 2)]
	U8(Vec<u8>),
}

impl KVValue for SerializedVector {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(mut val: &[u8], _: ()) -> Result<Self> {
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut val)?)
	}
}

impl<F> Encode<F> for SerializedVector {
	#[inline]
	fn encode<W: Write>(&self, w: &mut Writer<W>) -> std::result::Result<(), EncodeError> {
		// Capacity hint: payload bytes + small overhead for key revision/header/length.
		let cap = match self {
			SerializedVector::F64(v) => v.len() * 8 + 16,
			SerializedVector::F16(v) => v.len() * 2 + 16,
			SerializedVector::F32(v) => v.len() * 4 + 16,
			SerializedVector::I64(v) => v.len() * 8 + 16,
			SerializedVector::I32(v) => v.len() * 4 + 16,
			SerializedVector::I16(v) => v.len() * 2 + 16,
			SerializedVector::I8(v) => v.len() + 16,
			SerializedVector::U8(v) => v.len() + 16,
		};
		let mut buf = Vec::with_capacity(cap);
		self.serialize_key_wire(&mut buf).map_err(EncodeError::custom)?;
		w.write_slice(&buf)?;
		Ok(())
	}
}

impl<'de, F> BorrowDecode<'de, F> for SerializedVector {
	fn borrow_decode(r: &mut BorrowReader<'de>) -> std::result::Result<Self, DecodeError> {
		let slice = r.read_cow()?;
		let bytes: &[u8] = slice.as_ref();
		Self::deserialize_key_wire(bytes).map_err(DecodeError::custom)
	}
}

impl SerializedVector {
	fn serialize_key_wire<W: Write>(
		&self,
		writer: &mut W,
	) -> std::result::Result<(), revision::Error> {
		SerializeRevisioned::serialize_revisioned(&SERIALIZED_VECTOR_KEY_REVISION, writer)?;
		let discriminant = match self {
			Self::F64(_) => SERIALIZED_VECTOR_F64_KEY_DISCRIMINANT,
			Self::F32(_) => SERIALIZED_VECTOR_F32_KEY_DISCRIMINANT,
			Self::I64(_) => SERIALIZED_VECTOR_I64_KEY_DISCRIMINANT,
			Self::I32(_) => SERIALIZED_VECTOR_I32_KEY_DISCRIMINANT,
			Self::I16(_) => SERIALIZED_VECTOR_I16_KEY_DISCRIMINANT,
			Self::F16(_) => SERIALIZED_VECTOR_F16_KEY_DISCRIMINANT,
			Self::I8(_) => SERIALIZED_VECTOR_I8_KEY_DISCRIMINANT,
			Self::U8(_) => SERIALIZED_VECTOR_U8_KEY_DISCRIMINANT,
		};
		SerializeRevisioned::serialize_revisioned(&discriminant, writer)?;
		match self {
			Self::F64(values) => SerializeRevisioned::serialize_revisioned(values, writer),
			Self::F32(values) => SerializeRevisioned::serialize_revisioned(values, writer),
			Self::I64(values) => SerializeRevisioned::serialize_revisioned(values, writer),
			Self::I32(values) => SerializeRevisioned::serialize_revisioned(values, writer),
			Self::I16(values) => SerializeRevisioned::serialize_revisioned(values, writer),
			Self::F16(values) => SerializeRevisioned::serialize_revisioned(values, writer),
			Self::I8(values) => SerializeRevisioned::serialize_revisioned(values, writer),
			Self::U8(values) => SerializeRevisioned::serialize_revisioned(values, writer),
		}
	}

	fn deserialize_key_wire(mut bytes: &[u8]) -> std::result::Result<Self, revision::Error> {
		let key_revision = u16::deserialize_revisioned(&mut bytes)?;
		if key_revision != SERIALIZED_VECTOR_KEY_REVISION {
			return Err(revision::Error::Deserialize(format!(
				"Invalid key revision `{key_revision}` for type `SerializedVector`"
			)));
		}
		let discriminant = u32::deserialize_revisioned(&mut bytes)?;
		match discriminant {
			SERIALIZED_VECTOR_F64_KEY_DISCRIMINANT => {
				Ok(Self::F64(Vec::<f64>::deserialize_revisioned(&mut bytes)?))
			}
			SERIALIZED_VECTOR_F32_KEY_DISCRIMINANT => {
				Ok(Self::F32(Vec::<f32>::deserialize_revisioned(&mut bytes)?))
			}
			SERIALIZED_VECTOR_I64_KEY_DISCRIMINANT => {
				Ok(Self::I64(Vec::<i64>::deserialize_revisioned(&mut bytes)?))
			}
			SERIALIZED_VECTOR_I32_KEY_DISCRIMINANT => {
				Ok(Self::I32(Vec::<i32>::deserialize_revisioned(&mut bytes)?))
			}
			SERIALIZED_VECTOR_I16_KEY_DISCRIMINANT => {
				Ok(Self::I16(Vec::<i16>::deserialize_revisioned(&mut bytes)?))
			}
			SERIALIZED_VECTOR_F16_KEY_DISCRIMINANT => {
				Ok(Self::F16(Vec::<u16>::deserialize_revisioned(&mut bytes)?))
			}
			SERIALIZED_VECTOR_I8_KEY_DISCRIMINANT => {
				Ok(Self::I8(Vec::<i8>::deserialize_revisioned(&mut bytes)?))
			}
			SERIALIZED_VECTOR_U8_KEY_DISCRIMINANT => {
				Ok(Self::U8(Vec::<u8>::deserialize_revisioned(&mut bytes)?))
			}
			_ => Err(revision::Error::Deserialize(format!(
				"Invalid key discriminant `{discriminant}` for type `SerializedVector`"
			))),
		}
	}

	pub fn dimension(&self) -> usize {
		match self {
			Self::F64(v) => v.len(),
			Self::F16(v) => v.len(),
			Self::F32(v) => v.len(),
			Self::I64(v) => v.len(),
			Self::I32(v) => v.len(),
			Self::I16(v) => v.len(),
			Self::I8(v) => v.len(),
			Self::U8(v) => v.len(),
		}
	}

	/// Computes a BLAKE3 hash of the vector's bytes.
	///
	/// This is used for deduplicating vectors in the HNSW index when `HASHED_VECTOR` is enabled.
	/// The hash is calculated by iterating over the vector elements and updating the hasher
	/// with their little-endian byte representation.
	pub fn compute_hash(&self) -> [u8; 32] {
		let mut hasher = Blake3Hasher::new();
		match self {
			Self::F64(v) => {
				for &val in v {
					hasher.update(&val.to_le_bytes());
				}
			}
			Self::F16(v) => {
				for &val in v {
					hasher.update(&val.to_le_bytes());
				}
			}
			Self::F32(v) => {
				for &val in v {
					hasher.update(&val.to_le_bytes());
				}
			}
			Self::I64(v) => {
				for &val in v {
					hasher.update(&val.to_le_bytes());
				}
			}
			Self::I32(v) => {
				for &val in v {
					hasher.update(&val.to_le_bytes());
				}
			}
			Self::I16(v) => {
				for &val in v {
					hasher.update(&val.to_le_bytes());
				}
			}
			Self::I8(v) => {
				for &val in v {
					hasher.update(&val.to_le_bytes());
				}
			}
			Self::U8(v) => {
				for &val in v {
					hasher.update(&val.to_le_bytes());
				}
			}
		}
		*hasher.finalize().as_bytes()
	}
}

#[cfg(test)]
mod tests {
	use revision::{DeserializeRevisioned, SerializeRevisioned, revisioned};

	use super::*;

	#[revisioned(revision = 1)]
	#[derive(Clone, Debug, PartialEq)]
	enum OldSerializedVector {
		F64(Vec<f64>),
		F32(Vec<f32>),
		I64(Vec<i64>),
		I32(Vec<i32>),
		I16(Vec<i16>),
	}

	fn old_serialized_vector_cases() -> Vec<(OldSerializedVector, SerializedVector)> {
		vec![
			(
				OldSerializedVector::F64(vec![1.0, 2.0, 3.0]),
				SerializedVector::F64(vec![1.0, 2.0, 3.0]),
			),
			(
				OldSerializedVector::F32(vec![1.0, 2.0, 3.0]),
				SerializedVector::F32(vec![1.0, 2.0, 3.0]),
			),
			(OldSerializedVector::I64(vec![1, 2, 3]), SerializedVector::I64(vec![1, 2, 3])),
			(OldSerializedVector::I32(vec![1, 2, 3]), SerializedVector::I32(vec![1, 2, 3])),
			(OldSerializedVector::I16(vec![-1, 0, 1]), SerializedVector::I16(vec![-1, 0, 1])),
		]
	}

	fn serialize_revisioned<T: SerializeRevisioned>(value: &T) -> Vec<u8> {
		let mut bytes = Vec::new();
		SerializeRevisioned::serialize_revisioned(value, &mut bytes).unwrap();
		bytes
	}

	fn serialize_key_wire(vector: &SerializedVector) -> Vec<u8> {
		let mut bytes = Vec::new();
		vector.serialize_key_wire(&mut bytes).unwrap();
		bytes
	}

	#[test]
	fn test_serialized_vector_revision_1_variants_keep_their_main_discriminants() {
		for (old, expected) in old_serialized_vector_cases() {
			let bytes = serialize_revisioned(&old);
			let vector = SerializedVector::deserialize_revisioned(&mut bytes.as_slice()).unwrap();
			assert_eq!(vector, expected);
		}
	}

	#[test]
	fn test_serialized_vector_key_wire_keeps_revision_1_bytes_for_existing_variants() {
		for (old, current) in old_serialized_vector_cases() {
			assert_eq!(serialize_key_wire(&current), serialize_revisioned(&old));
		}
	}

	#[test]
	fn test_serialized_vector_key_wire_roundtrips_all_variants() {
		for vector in [
			SerializedVector::F64(vec![1.0, 2.0, 3.0]),
			SerializedVector::F32(vec![1.0, 2.0, 3.0]),
			SerializedVector::I64(vec![1, 2, 3]),
			SerializedVector::I32(vec![1, 2, 3]),
			SerializedVector::I16(vec![1, 2, 3]),
			SerializedVector::F16(vec![1, 2, 3]),
			SerializedVector::I8(vec![1, 2, 3]),
			SerializedVector::U8(vec![1, 2, 3]),
		] {
			let bytes = serialize_key_wire(&vector);
			let decoded = SerializedVector::deserialize_key_wire(&bytes).unwrap();
			assert_eq!(decoded, vector);
		}
	}
}
