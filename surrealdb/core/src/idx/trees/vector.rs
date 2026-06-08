//! Shared vector representations and distance helpers used by ANN index implementations.
//!
//! [`Vector`] is the in-memory ndarray representation used during search, while
//! [`SerializedVector`] is the persisted representation used in ANN keys and payloads. Values use
//! normal revisioned encoding; keys use a stable key-wire encoding so existing vector-document keys
//! remain reachable after new vector variants are added.

use std::cmp::PartialEq;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::ops::{Deref, Sub};
use std::sync::Arc;

use ahash::{AHasher, HashSet};
use anyhow::{Result, ensure};
use blake3::Hasher as Blake3Hasher;
use half::f16;
use ndarray::{Array1, Zip};
use ndarray_stats::DeviationExt;
use revision::{DeserializeRevisioned, SerializeRevisioned, revisioned};
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, BorrowReader, DecodeError, Encode, EncodeError, Writer};

use crate::catalog::{Distance, VectorType};
use crate::err::Error;
use crate::fnc::util::math::ToFloat;
use crate::kvs::KVValue;
use crate::val::{Number, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum Vector {
	/// 64-bit floating-point vector.
	F64(Array1<f64>),
	/// 16-bit floating-point vector.
	F16(Array1<f16>),
	/// 32-bit floating-point vector.
	F32(Array1<f32>),
	/// 64-bit signed integer vector.
	I64(Array1<i64>),
	/// 32-bit signed integer vector.
	I32(Array1<i32>),
	/// 16-bit signed integer vector.
	I16(Array1<i16>),
	/// 8-bit signed integer vector.
	I8(Array1<i8>),
	/// 8-bit unsigned integer vector.
	U8(Array1<u8>),
}

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

impl From<&Vector> for SerializedVector {
	fn from(value: &Vector) -> Self {
		match value {
			Vector::F64(v) => Self::F64(v.to_vec()),
			Vector::F16(v) => Self::F16(v.iter().map(|v| v.to_bits()).collect()),
			Vector::F32(v) => Self::F32(v.to_vec()),
			Vector::I64(v) => Self::I64(v.to_vec()),
			Vector::I32(v) => Self::I32(v.to_vec()),
			Vector::I16(v) => Self::I16(v.to_vec()),
			Vector::I8(v) => Self::I8(v.to_vec()),
			Vector::U8(v) => Self::U8(v.to_vec()),
		}
	}
}

impl From<SerializedVector> for Vector {
	fn from(value: SerializedVector) -> Self {
		match value {
			SerializedVector::F64(v) => Self::F64(Array1::from_vec(v)),
			SerializedVector::F16(v) => {
				Self::F16(Array1::from_vec(v.into_iter().map(f16::from_bits).collect()))
			}
			SerializedVector::F32(v) => Self::F32(Array1::from_vec(v)),
			SerializedVector::I64(v) => Self::I64(Array1::from_vec(v)),
			SerializedVector::I32(v) => Self::I32(Array1::from_vec(v)),
			SerializedVector::I16(v) => Self::I16(Array1::from_vec(v)),
			SerializedVector::I8(v) => Self::I8(Array1::from_vec(v)),
			SerializedVector::U8(v) => Self::U8(Array1::from_vec(v)),
		}
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

	pub(super) fn try_from_value(t: VectorType, d: usize, v: Value) -> Result<Self> {
		let res = match t {
			VectorType::F64 => {
				let mut vec = Vec::with_capacity(d);
				Self::check_vector_value(v, &mut vec)?;
				Self::F64(vec)
			}
			VectorType::F16 => {
				let mut vec = Vec::with_capacity(d);
				Self::check_vector_value_f16(v, &mut vec)?;
				Self::F16(vec)
			}
			VectorType::F32 => {
				let mut vec = Vec::with_capacity(d);
				Self::check_vector_value(v, &mut vec)?;
				Self::F32(vec)
			}
			VectorType::I64 => {
				let mut vec = Vec::with_capacity(d);
				Self::check_vector_value(v, &mut vec)?;
				Self::I64(vec)
			}
			VectorType::I32 => {
				let mut vec = Vec::with_capacity(d);
				Self::check_vector_value(v, &mut vec)?;
				Self::I32(vec)
			}
			VectorType::I16 => {
				let mut vec = Vec::with_capacity(d);
				Self::check_vector_value(v, &mut vec)?;
				Self::I16(vec)
			}
			VectorType::I8 => {
				let mut vec = Vec::with_capacity(d);
				Self::check_vector_value(v, &mut vec)?;
				Self::I8(vec)
			}
			VectorType::U8 => {
				let mut vec = Vec::with_capacity(d);
				Self::check_vector_value(v, &mut vec)?;
				Self::U8(vec)
			}
		};
		Ok(res)
	}

	fn check_vector_value_f16(value: Value, vec: &mut Vec<u16>) -> Result<()> {
		match value {
			Value::Array(a) => {
				for v in a.0 {
					Self::check_vector_value_f16(v, vec)?;
				}
				Ok(())
			}
			Value::Number(n) => {
				let n: f32 = n.try_into()?;
				vec.push(f16::from_f32(n).to_bits());
				Ok(())
			}
			_ => Err(anyhow::Error::new(Error::InvalidVectorValue(value.to_raw_string()))),
		}
	}

	fn check_vector_value<T>(value: Value, vec: &mut Vec<T>) -> Result<()>
	where
		T: TryFrom<Number, Error = Error>,
	{
		match value {
			Value::Array(a) => {
				for v in a.0 {
					Self::check_vector_value(v, vec)?;
				}
				Ok(())
			}
			Value::Number(n) => {
				vec.push(n.try_into()?);
				Ok(())
			}
			_ => Err(anyhow::Error::new(Error::InvalidVectorValue(value.to_raw_string()))),
		}
	}

	pub(super) fn dimension(&self) -> usize {
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
	pub(crate) fn compute_hash(&self) -> [u8; 32] {
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

impl Vector {
	#[inline]
	fn dot_product<T>(a: &Array1<T>, b: &Array1<T>) -> f64
	where
		T: ToFloat,
	{
		a.iter().zip(b.iter()).map(|(a, b)| a.to_float() * b.to_float()).sum()
	}

	#[inline]
	fn magnitude<T>(a: &Array1<T>) -> f64
	where
		T: ToFloat,
	{
		a.iter().map(|v| v.to_float().powi(2)).sum::<f64>().sqrt()
	}

	#[inline]
	fn chebyshev<T>(a: &Array1<T>, b: &Array1<T>) -> f64
	where
		T: ToFloat,
	{
		a.iter()
			.zip(b.iter())
			.map(|(a, b)| (a.to_float() - b.to_float()).abs())
			.fold(0.0_f64, f64::max)
	}

	fn chebyshev_distance(&self, other: &Self) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => a.linf_dist(b).unwrap_or(f64::INFINITY),
			(Self::F16(a), Self::F16(b)) => Self::chebyshev(a, b),
			(Self::F32(a), Self::F32(b)) => {
				a.linf_dist(b).map(|r| r as f64).unwrap_or(f64::INFINITY)
			}
			(Self::I64(a), Self::I64(b)) => {
				a.linf_dist(b).map(|r| r as f64).unwrap_or(f64::INFINITY)
			}
			(Self::I32(a), Self::I32(b)) => {
				a.linf_dist(b).map(|r| r as f64).unwrap_or(f64::INFINITY)
			}
			(Self::I16(a), Self::I16(b)) => Self::chebyshev(a, b),
			(Self::I8(a), Self::I8(b)) => Self::chebyshev(a, b),
			(Self::U8(a), Self::U8(b)) => Self::chebyshev(a, b),
			_ => f64::NAN,
		}
	}

	#[inline]
	fn cosine_distance_f64(a: &Array1<f64>, b: &Array1<f64>) -> f64 {
		let dot_product = a.dot(b);
		let norm_a = (a * a).sum().sqrt();
		let norm_b = (b * b).sum().sqrt();
		1.0 - dot_product / (norm_a * norm_b)
	}

	#[inline]
	fn cosine_distance_f32(a: &Array1<f32>, b: &Array1<f32>) -> f64 {
		let dot_product = a.dot(b) as f64;
		let norm_a = ((a * a).sum() as f64).sqrt();
		let norm_b = ((b * b).sum() as f64).sqrt();
		1.0 - dot_product / (norm_a * norm_b)
	}

	#[inline]
	fn cosine_dist<T>(a: &Array1<T>, b: &Array1<T>) -> f64
	where
		T: ToFloat,
	{
		let dot_product = Self::dot_product(a, b);
		let norm_a = Self::magnitude(a);
		let norm_b = Self::magnitude(b);
		1.0 - dot_product / (norm_a * norm_b)
	}

	fn cosine_distance(&self, other: &Self) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => Self::cosine_distance_f64(a, b),
			(Self::F16(a), Self::F16(b)) => Self::cosine_dist(a, b),
			(Self::F32(a), Self::F32(b)) => Self::cosine_distance_f32(a, b),
			(Self::I64(a), Self::I64(b)) => Self::cosine_dist(a, b),
			(Self::I32(a), Self::I32(b)) => Self::cosine_dist(a, b),
			(Self::I16(a), Self::I16(b)) => Self::cosine_dist(a, b),
			(Self::I8(a), Self::I8(b)) => Self::cosine_dist(a, b),
			(Self::U8(a), Self::U8(b)) => Self::cosine_dist(a, b),
			_ => f64::INFINITY,
		}
	}

	fn cosine_normalized_distance(&self, other: &Self) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => 1.0 - Self::dot_product(a, b),
			(Self::F16(a), Self::F16(b)) => 1.0 - Self::dot_product(a, b),
			(Self::F32(a), Self::F32(b)) => 1.0 - Self::dot_product(a, b),
			(Self::I64(a), Self::I64(b)) => 1.0 - Self::dot_product(a, b),
			(Self::I32(a), Self::I32(b)) => 1.0 - Self::dot_product(a, b),
			(Self::I16(a), Self::I16(b)) => 1.0 - Self::dot_product(a, b),
			(Self::I8(a), Self::I8(b)) => 1.0 - Self::dot_product(a, b),
			(Self::U8(a), Self::U8(b)) => 1.0 - Self::dot_product(a, b),
			_ => f64::INFINITY,
		}
	}

	#[inline]
	fn euclidean<T>(a: &Array1<T>, b: &Array1<T>) -> f64
	where
		T: ToFloat,
	{
		Zip::from(a).and(b).map_collect(|x, y| (x.to_float() - y.to_float()).powi(2)).sum().sqrt()
	}
	fn euclidean_distance(&self, other: &Self) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => a.l2_dist(b).unwrap_or(f64::INFINITY),
			(Self::F16(a), Self::F16(b)) => Self::euclidean(a, b),
			(Self::F32(a), Self::F32(b)) => a.l2_dist(b).unwrap_or(f64::INFINITY),
			(Self::I64(a), Self::I64(b)) => a.l2_dist(b).unwrap_or(f64::INFINITY),
			(Self::I32(a), Self::I32(b)) => a.l2_dist(b).unwrap_or(f64::INFINITY),
			(Self::I16(a), Self::I16(b)) => Self::euclidean(a, b),
			(Self::I8(a), Self::I8(b)) => Self::euclidean(a, b),
			(Self::U8(a), Self::U8(b)) => Self::euclidean(a, b),
			_ => f64::INFINITY,
		}
	}

	#[inline]
	fn hamming<T>(a: &Array1<T>, b: &Array1<T>) -> f64
	where
		T: PartialEq,
	{
		Zip::from(a).and(b).fold(0, |acc, a, b| {
			if a != b {
				acc + 1
			} else {
				acc
			}
		}) as f64
	}

	fn hamming_distance(&self, other: &Self) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => Self::hamming(a, b),
			(Self::F16(a), Self::F16(b)) => Self::hamming(a, b),
			(Self::F32(a), Self::F32(b)) => Self::hamming(a, b),
			(Self::I64(a), Self::I64(b)) => Self::hamming(a, b),
			(Self::I32(a), Self::I32(b)) => Self::hamming(a, b),
			(Self::I16(a), Self::I16(b)) => Self::hamming(a, b),
			(Self::I8(a), Self::I8(b)) => Self::hamming(a, b),
			(Self::U8(a), Self::U8(b)) => Self::hamming(a, b),
			_ => f64::INFINITY,
		}
	}

	#[inline]
	fn jaccard_f64(a: &Array1<f64>, b: &Array1<f64>) -> f64 {
		let mut union: HashSet<u64> = a.iter().map(|f| f.to_bits()).collect();
		let intersection_size = b.iter().fold(0, |acc, n| {
			if !union.insert(n.to_bits()) {
				acc + 1
			} else {
				acc
			}
		}) as f64;
		1.0 - intersection_size / union.len() as f64
	}

	#[inline]
	fn jaccard_f32(a: &Array1<f32>, b: &Array1<f32>) -> f64 {
		let mut union: HashSet<u32> = a.iter().map(|f| f.to_bits()).collect();
		let intersection_size = b.iter().fold(0, |acc, n| {
			if !union.insert(n.to_bits()) {
				acc + 1
			} else {
				acc
			}
		}) as f64;
		intersection_size / union.len() as f64
	}

	#[inline]
	fn jaccard_f16(a: &Array1<f16>, b: &Array1<f16>) -> f64 {
		let mut union: HashSet<u16> = a.iter().map(|f| f.to_bits()).collect();
		let intersection_size = b.iter().fold(0, |acc, n| {
			if !union.insert(n.to_bits()) {
				acc + 1
			} else {
				acc
			}
		}) as f64;
		intersection_size / union.len() as f64
	}

	#[inline]
	fn jaccard_integers<T>(a: &Array1<T>, b: &Array1<T>) -> f64
	where
		T: Eq + Hash + Clone,
	{
		let mut union: HashSet<T> = a.iter().cloned().collect();
		let intersection_size = b.iter().cloned().fold(0, |acc, n| {
			if !union.insert(n) {
				acc + 1
			} else {
				acc
			}
		}) as f64;
		intersection_size / union.len() as f64
	}

	pub(super) fn jaccard_similarity(&self, other: &Self) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => Self::jaccard_f64(a, b),
			(Self::F16(a), Self::F16(b)) => Self::jaccard_f16(a, b),
			(Self::F32(a), Self::F32(b)) => Self::jaccard_f32(a, b),
			(Self::I64(a), Self::I64(b)) => Self::jaccard_integers(a, b),
			(Self::I32(a), Self::I32(b)) => Self::jaccard_integers(a, b),
			(Self::I16(a), Self::I16(b)) => Self::jaccard_integers(a, b),
			(Self::I8(a), Self::I8(b)) => Self::jaccard_integers(a, b),
			(Self::U8(a), Self::U8(b)) => Self::jaccard_integers(a, b),
			_ => f64::NAN,
		}
	}

	#[inline]
	fn manhattan<T>(a: &Array1<T>, b: &Array1<T>) -> f64
	where
		T: Sub<Output = T> + ToFloat + Copy,
	{
		a.iter().zip(b.iter()).map(|(&a, &b)| (a - b).to_float().abs()).sum()
	}

	#[inline]
	fn manhattan_float<T>(a: &Array1<T>, b: &Array1<T>) -> f64
	where
		T: ToFloat,
	{
		a.iter().zip(b.iter()).map(|(a, b)| (a.to_float() - b.to_float()).abs()).sum()
	}

	pub(super) fn manhattan_distance(&self, other: &Self) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => a.l1_dist(b).unwrap_or(f64::INFINITY),
			(Self::F16(a), Self::F16(b)) => Self::manhattan_float(a, b),
			(Self::F32(a), Self::F32(b)) => a.l1_dist(b).map(|r| r as f64).unwrap_or(f64::INFINITY),
			(Self::I64(a), Self::I64(b)) => a.l1_dist(b).map(|r| r as f64).unwrap_or(f64::INFINITY),
			(Self::I32(a), Self::I32(b)) => a.l1_dist(b).map(|r| r as f64).unwrap_or(f64::INFINITY),
			(Self::I16(a), Self::I16(b)) => Self::manhattan(a, b),
			(Self::I8(a), Self::I8(b)) => Self::manhattan(a, b),
			(Self::U8(a), Self::U8(b)) => Self::manhattan_float(a, b),
			_ => f64::NAN,
		}
	}

	#[inline]
	fn minkowski<T>(a: &Array1<T>, b: &Array1<T>, order: f64) -> f64
	where
		T: ToFloat,
	{
		let dist: f64 = a
			.iter()
			.zip(b.iter())
			.map(|(a, b)| (a.to_float() - b.to_float()).abs().powf(order))
			.sum();
		dist.powf(1.0 / order)
	}

	pub(super) fn minkowski_distance(&self, other: &Self, order: f64) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => Self::minkowski(a, b, order),
			(Self::F16(a), Self::F16(b)) => Self::minkowski(a, b, order),
			(Self::F32(a), Self::F32(b)) => Self::minkowski(a, b, order),
			(Self::I64(a), Self::I64(b)) => Self::minkowski(a, b, order),
			(Self::I32(a), Self::I32(b)) => Self::minkowski(a, b, order),
			(Self::I16(a), Self::I16(b)) => Self::minkowski(a, b, order),
			(Self::I8(a), Self::I8(b)) => Self::minkowski(a, b, order),
			(Self::U8(a), Self::U8(b)) => Self::minkowski(a, b, order),
			_ => f64::NAN,
		}
	}

	#[inline]
	fn pearson<T>(x: &Array1<T>, y: &Array1<T>) -> f64
	where
		T: ToFloat,
	{
		let mean_x = x.iter().map(ToFloat::to_float).sum::<f64>() / x.len() as f64;
		let mean_y = y.iter().map(ToFloat::to_float).sum::<f64>() / y.len() as f64;

		let mut sum_xy = 0.0;
		let mut sum_x2 = 0.0;
		let mut sum_y2 = 0.0;

		for (xi, yi) in x.iter().zip(y.iter()) {
			let diff_x = xi.to_float() - mean_x;
			let diff_y = yi.to_float() - mean_y;
			sum_xy += diff_x * diff_y;
			sum_x2 += diff_x.powi(2);
			sum_y2 += diff_y.powi(2);
		}

		let numerator = sum_xy;
		let denominator = (sum_x2 * sum_y2).sqrt();

		if denominator == 0.0 {
			return 0.0; // Return 0 if the denominator is 0
		}

		numerator / denominator
	}

	fn pearson_similarity(&self, other: &Self) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => Self::pearson(a, b),
			(Self::F16(a), Self::F16(b)) => Self::pearson(a, b),
			(Self::F32(a), Self::F32(b)) => Self::pearson(a, b),
			(Self::I64(a), Self::I64(b)) => Self::pearson(a, b),
			(Self::I32(a), Self::I32(b)) => Self::pearson(a, b),
			(Self::I16(a), Self::I16(b)) => Self::pearson(a, b),
			(Self::I8(a), Self::I8(b)) => Self::pearson(a, b),
			(Self::U8(a), Self::U8(b)) => Self::pearson(a, b),
			_ => f64::NAN,
		}
	}

	fn inner_product_distance(&self, other: &Self) -> f64 {
		match (self, other) {
			(Self::F64(a), Self::F64(b)) => -Self::dot_product(a, b),
			(Self::F16(a), Self::F16(b)) => -Self::dot_product(a, b),
			(Self::F32(a), Self::F32(b)) => -Self::dot_product(a, b),
			(Self::I64(a), Self::I64(b)) => -Self::dot_product(a, b),
			(Self::I32(a), Self::I32(b)) => -Self::dot_product(a, b),
			(Self::I16(a), Self::I16(b)) => -Self::dot_product(a, b),
			(Self::I8(a), Self::I8(b)) => -Self::dot_product(a, b),
			(Self::U8(a), Self::U8(b)) => -Self::dot_product(a, b),
			_ => f64::INFINITY,
		}
	}

	fn mem_size(&self) -> usize {
		let s = match self {
			Self::F64(arr) => arr.len() * std::mem::size_of::<f64>(),
			Self::F16(arr) => arr.len() * std::mem::size_of::<f16>(),
			Self::F32(arr) => arr.len() * std::mem::size_of::<f32>(),
			Self::I64(arr) => arr.len() * std::mem::size_of::<i64>(),
			Self::I32(arr) => arr.len() * std::mem::size_of::<i32>(),
			Self::I16(arr) => arr.len() * std::mem::size_of::<i16>(),
			Self::I8(arr) => arr.len() * std::mem::size_of::<i8>(),
			Self::U8(arr) => arr.len() * std::mem::size_of::<u8>(),
		};
		// Array1 overhead (approximately 24 bytes for ndarray metadata)
		s + 24
	}
}

/// For vectors, as we want to support very large vectors, we want to avoid copy
/// or clone. So the requirement is multiple ownership but not thread safety.
/// However, because we are running in an async context, and because we are
/// using cache structures that use the Arc as a key, the cached objects has to
/// be Sent, which then requires the use of Arc (rather than just Rc).
/// As computing the hash for a large vector is costly, this structures also
/// caches the hashcode to avoid recomputing it.
#[derive(Debug, Clone)]
pub struct SharedVector(Arc<Vector>, u64);
impl From<Vector> for SharedVector {
	fn from(v: Vector) -> Self {
		let mut h = AHasher::default();
		v.hash(&mut h);
		Self(Arc::new(v), h.finish())
	}
}

impl Deref for SharedVector {
	type Target = Vector;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Hash for SharedVector {
	fn hash<H: Hasher>(&self, state: &mut H) {
		state.write_u64(self.1);
	}
}

impl PartialEq for SharedVector {
	fn eq(&self, other: &Self) -> bool {
		self.1 == other.1 && self.0 == other.0
	}
}
impl Eq for SharedVector {}

impl SharedVector {
	pub(super) fn mem_size(&self) -> usize {
		// SharedVector stack size + Vector heap size + Arc heap overhead
		std::mem::size_of::<Self>() + self.0.mem_size() + 16
	}
}

impl Hash for Vector {
	fn hash<H: Hasher>(&self, state: &mut H) {
		match self {
			Vector::F64(v) => {
				let h = v.iter().fold(0, |acc, &x| acc ^ x.to_bits());
				state.write_u64(h);
			}
			Vector::F16(v) => {
				let h = v.iter().fold(0, |acc, &x| acc ^ x.to_bits());
				state.write_u16(h);
			}
			Vector::F32(v) => {
				let h = v.iter().fold(0, |acc, &x| acc ^ x.to_bits());
				state.write_u32(h);
			}
			Vector::I64(v) => {
				let h = v.iter().fold(0, |acc, &x| acc ^ x);
				state.write_i64(h);
			}
			Vector::I32(v) => {
				let h = v.iter().fold(0, |acc, &x| acc ^ x);
				state.write_i32(h);
			}
			Vector::I16(v) => {
				let h = v.iter().fold(0, |acc, &x| acc ^ x);
				state.write_i16(h);
			}
			Vector::I8(v) => {
				let h = v.iter().fold(0, |acc, &x| acc ^ x);
				state.write_i8(h);
			}
			Vector::U8(v) => {
				let h = v.iter().fold(0, |acc, &x| acc ^ x);
				state.write_u8(h);
			}
		}
	}
}

#[cfg(test)]
impl SharedVector {
	pub(crate) fn clone_vector(&self) -> Vector {
		self.0.as_ref().clone()
	}
}

#[cfg(test)]
impl From<&Vector> for Value {
	fn from(v: &Vector) -> Self {
		let vec: Vec<_> = match v {
			Vector::F64(a) => a.iter().map(|i| Number::Float(*i)).map(Value::from).collect(),
			Vector::F16(a) => {
				a.iter().map(|i| Number::Float(i.to_f64())).map(Value::from).collect()
			}
			Vector::F32(a) => a.iter().map(|i| Number::Float(*i as f64)).map(Value::from).collect(),
			Vector::I64(a) => a.iter().map(|i| Number::Int(*i)).map(Value::from).collect(),
			Vector::I32(a) => a.iter().map(|i| Number::Int(*i as i64)).map(Value::from).collect(),
			Vector::I16(a) => a.iter().map(|i| Number::Int(*i as i64)).map(Value::from).collect(),
			Vector::I8(a) => a.iter().map(|i| Number::Int(*i as i64)).map(Value::from).collect(),
			Vector::U8(a) => a.iter().map(|i| Number::Int(*i as i64)).map(Value::from).collect(),
		};
		Value::from(vec)
	}
}

impl Vector {
	#[cfg(test)]
	pub(super) fn try_from_value(t: VectorType, d: usize, v: Value) -> Result<Self> {
		let res = match t {
			VectorType::F64 => {
				let mut vec = Vec::with_capacity(d);
				SerializedVector::check_vector_value(v, &mut vec)?;
				Vector::F64(Array1::from_vec(vec))
			}
			VectorType::F16 => {
				let mut vec = Vec::with_capacity(d);
				SerializedVector::check_vector_value_f16(v, &mut vec)?;
				Vector::F16(Array1::from_vec(vec.into_iter().map(f16::from_bits).collect()))
			}
			VectorType::F32 => {
				let mut vec = Vec::with_capacity(d);
				SerializedVector::check_vector_value(v, &mut vec)?;
				Vector::F32(Array1::from_vec(vec))
			}
			VectorType::I64 => {
				let mut vec = Vec::with_capacity(d);
				SerializedVector::check_vector_value(v, &mut vec)?;
				Vector::I64(Array1::from_vec(vec))
			}
			VectorType::I32 => {
				let mut vec = Vec::with_capacity(d);
				SerializedVector::check_vector_value(v, &mut vec)?;
				Vector::I32(Array1::from_vec(vec))
			}
			VectorType::I16 => {
				let mut vec = Vec::with_capacity(d);
				SerializedVector::check_vector_value(v, &mut vec)?;
				Vector::I16(Array1::from_vec(vec))
			}
			VectorType::I8 => {
				let mut vec = Vec::with_capacity(d);
				SerializedVector::check_vector_value(v, &mut vec)?;
				Vector::I8(Array1::from_vec(vec))
			}
			VectorType::U8 => {
				let mut vec = Vec::with_capacity(d);
				SerializedVector::check_vector_value(v, &mut vec)?;
				Vector::U8(Array1::from_vec(vec))
			}
		};
		Ok(res)
	}

	pub(super) fn try_from_vector(t: VectorType, v: &[Number]) -> Result<Self> {
		let res = match t {
			VectorType::F64 => {
				let mut vec = Vec::with_capacity(v.len());
				Self::check_vector_number(v, &mut vec)?;
				Vector::F64(Array1::from_vec(vec))
			}
			VectorType::F16 => {
				let mut vec = Vec::with_capacity(v.len());
				Self::check_vector_number_f16(v, &mut vec)?;
				Vector::F16(Array1::from_vec(vec))
			}
			VectorType::F32 => {
				let mut vec = Vec::with_capacity(v.len());
				Self::check_vector_number(v, &mut vec)?;
				Vector::F32(Array1::from_vec(vec))
			}
			VectorType::I64 => {
				let mut vec = Vec::with_capacity(v.len());
				Self::check_vector_number(v, &mut vec)?;
				Vector::I64(Array1::from_vec(vec))
			}
			VectorType::I32 => {
				let mut vec = Vec::with_capacity(v.len());
				Self::check_vector_number(v, &mut vec)?;
				Vector::I32(Array1::from_vec(vec))
			}
			VectorType::I16 => {
				let mut vec = Vec::with_capacity(v.len());
				Self::check_vector_number(v, &mut vec)?;
				Vector::I16(Array1::from_vec(vec))
			}
			VectorType::I8 => {
				let mut vec = Vec::with_capacity(v.len());
				Self::check_vector_number(v, &mut vec)?;
				Vector::I8(Array1::from_vec(vec))
			}
			VectorType::U8 => {
				let mut vec = Vec::with_capacity(v.len());
				Self::check_vector_number(v, &mut vec)?;
				Vector::U8(Array1::from_vec(vec))
			}
		};
		Ok(res)
	}

	fn check_vector_number_f16(v: &[Number], vec: &mut Vec<f16>) -> Result<()> {
		for n in v {
			let n: f32 = (*n).try_into()?;
			vec.push(f16::from_f32(n));
		}
		Ok(())
	}

	fn check_vector_number<T>(v: &[Number], vec: &mut Vec<T>) -> Result<()>
	where
		T: TryFrom<Number, Error = Error>,
	{
		for n in v {
			vec.push((*n).try_into()?);
		}
		Ok(())
	}

	pub(super) fn len(&self) -> usize {
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

	pub(super) fn check_expected_dimension(current: usize, expected: usize) -> Result<()> {
		ensure!(
			current == expected,
			Error::InvalidVectorDimension {
				current,
				expected,
			}
		);
		Ok(())
	}

	pub(super) fn check_dimension(&self, expected_dim: usize) -> Result<()> {
		Self::check_expected_dimension(self.len(), expected_dim)
	}
}

impl Distance {
	pub(super) fn calculate(&self, a: &Vector, b: &Vector) -> f64 {
		match self {
			Distance::Chebyshev => a.chebyshev_distance(b),
			Distance::Cosine => a.cosine_distance(b),
			Distance::CosineNormalized => a.cosine_normalized_distance(b),
			Distance::Euclidean => a.euclidean_distance(b),
			Distance::Hamming => a.hamming_distance(b),
			Distance::InnerProduct => a.inner_product_distance(b),
			Distance::Jaccard => a.jaccard_similarity(b),
			Distance::Manhattan => a.manhattan_distance(b),
			Distance::Minkowski(order) => a.minkowski_distance(b, order.to_float()),
			Distance::Pearson => a.pearson_similarity(b),
		}
	}
}

#[cfg(test)]
mod tests {
	use revision::{DeserializeRevisioned, SerializeRevisioned, revisioned};

	use crate::catalog::{Distance, VectorType};
	use crate::idx::trees::knn::tests::{RandomItemGenerator, get_seed_rnd, new_random_vec};
	use crate::idx::trees::vector::{SerializedVector, SharedVector, Vector};
	use crate::val::{Array, Number, Value};

	#[revisioned(revision = 1)]
	#[derive(Clone, Debug, PartialEq)]
	enum OldSerializedVector {
		F64(Vec<f64>),
		F32(Vec<f32>),
		I64(Vec<i64>),
		I32(Vec<i32>),
		I16(Vec<i16>),
	}

	fn value_array(values: Vec<Value>) -> Value {
		Value::Array(Array(values))
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

	fn test_distance(dist: &Distance, a1: &[f64], a2: &[f64], res: f64) {
		// Convert the arrays to Vec<Number>
		let mut v1 = vec![];
		a1.iter().for_each(|&n| v1.push(n.into()));
		let mut v2 = vec![];
		a2.iter().for_each(|&n| v2.push(n.into()));

		// Check the generic distance implementation
		assert_eq!(dist.compute(&v1, &v2).unwrap(), res.into());

		// Check the "Vector" optimised implementations
		let t = VectorType::F64;
		let v1: SharedVector = Vector::try_from_vector(t, &v1).unwrap().into();
		let v2: SharedVector = Vector::try_from_vector(t, &v2).unwrap().into();
		assert_eq!(dist.calculate(&v1, &v2), res);
	}

	fn test_distance_collection(dist: &Distance, size: usize, dim: usize) {
		let mut rng = get_seed_rnd();
		for vt in [
			VectorType::F64,
			VectorType::F32,
			VectorType::I64,
			VectorType::I32,
			VectorType::I16,
			VectorType::F16,
			VectorType::I8,
			VectorType::U8,
		] {
			let r#gen = RandomItemGenerator::new(dist, dim);
			let mut num_zero = 0;
			for i in 0..size {
				let v1 = new_random_vec(&mut rng, vt, dim, &r#gen);
				let v2 = new_random_vec(&mut rng, vt, dim, &r#gen);
				let d = dist.calculate(&v1, &v2);
				assert!(
					d.is_finite() && !d.is_nan(),
					"i: {i} - vt: {vt} - v1: {v1:?} - v2: {v2:?}"
				);
				assert_ne!(d, f64::NAN, "i: {i} - vt: {vt} - v1: {v1:?} - v2: {v2:?}");
				assert_ne!(d, f64::INFINITY, "i: {i} - vt: {vt} - v1: {v1:?} - v2: {v2:?}");
				if d == 0.0 {
					num_zero += 1;
				}
			}
			let zero_rate = num_zero as f64 / size as f64;
			assert!(zero_rate < 0.1, "vt: {vt} - zero_rate: {zero_rate}");
		}
	}

	#[test]
	fn test_distance_chebyshev() {
		test_distance_collection(&Distance::Chebyshev, 100, 1536);
		test_distance(&Distance::Chebyshev, &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], 1.0);
	}

	#[test]
	fn test_distance_cosine() {
		test_distance_collection(&Distance::Cosine, 100, 1536);
		test_distance(&Distance::Cosine, &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], 0.007416666029069652);
	}

	#[test]
	fn test_distance_cosine_normalized() {
		test_distance_collection(&Distance::CosineNormalized, 100, 1536);
		test_distance(&Distance::CosineNormalized, &[1.0, 0.0, 0.0], &[0.5, 0.5, 0.0], 0.5);
	}

	#[test]
	fn test_distance_euclidean() {
		test_distance_collection(&Distance::Euclidean, 100, 1536);
		test_distance(&Distance::Euclidean, &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], 1.7320508075688772);
	}

	#[test]
	fn test_distance_hamming() {
		test_distance_collection(&Distance::Hamming, 100, 1536);
		test_distance(&Distance::Hamming, &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], 3.0);
	}

	#[test]
	fn test_distance_inner_product() {
		test_distance_collection(&Distance::InnerProduct, 100, 1536);
		test_distance(&Distance::InnerProduct, &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], -20.0);
	}

	#[test]
	fn test_distance_jaccard() {
		test_distance_collection(&Distance::Jaccard, 100, 768);
		test_distance(&Distance::Jaccard, &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], 0.5);
	}
	#[test]
	fn test_distance_manhattan() {
		test_distance_collection(&Distance::Manhattan, 100, 1536);
		test_distance(&Distance::Manhattan, &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], 3.0);
	}
	#[test]
	fn test_distance_minkowski() {
		test_distance_collection(&Distance::Minkowski(3.into()), 100, 1536);
		test_distance(
			&Distance::Minkowski(3.into()),
			&[1.0, 2.0, 3.0],
			&[2.0, 3.0, 4.0],
			1.4422495703074083,
		);
	}

	#[test]
	fn test_distance_pearson() {
		test_distance_collection(&Distance::Pearson, 100, 1536);
		test_distance(&Distance::Pearson, &[1.0, 2.0, 3.0], &[2.0, 3.0, 4.0], 1.0);
	}

	#[test]
	fn test_serialized_vector_f16_roundtrip() {
		let vector = SerializedVector::try_from_value(
			VectorType::F16,
			2,
			value_array(vec![
				Value::Number(Number::Float(1.5)),
				Value::Number(Number::Float(-2.25)),
			]),
		)
		.unwrap();
		let SerializedVector::F16(bits) = &vector else {
			panic!("expected F16 serialized vector");
		};
		assert_eq!(bits.len(), 2);
		let Vector::F16(roundtrip) = Vector::from(vector) else {
			panic!("expected F16 vector");
		};
		assert_eq!(roundtrip[0].to_f32(), 1.5);
		assert_eq!(roundtrip[1].to_f32(), -2.25);
	}

	#[test]
	fn test_serialized_vector_i8_u8_range_validation() {
		assert!(
			SerializedVector::try_from_value(
				VectorType::U8,
				1,
				value_array(vec![Value::Number(Number::Int(-1))])
			)
			.is_err()
		);
		assert!(
			SerializedVector::try_from_value(
				VectorType::I8,
				1,
				value_array(vec![Value::Number(Number::Int(128))])
			)
			.is_err()
		);
		assert!(
			SerializedVector::try_from_value(
				VectorType::U8,
				1,
				value_array(vec![Value::Number(Number::Int(255))])
			)
			.is_ok()
		);
		assert!(
			SerializedVector::try_from_value(
				VectorType::I8,
				1,
				value_array(vec![Value::Number(Number::Int(-128))])
			)
			.is_ok()
		);
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
