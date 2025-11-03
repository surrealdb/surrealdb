//! Core serialization traits and binary wire format implementations.
//!
//! This module defines the [`Serializable`] trait and implements it for common Rust types,
//! providing a language-agnostic binary protocol for WASM guest-host communication.
//!
//! # Wire Format
//!
//! The serialization format is designed to be:
//! - **Compact**: Minimal overhead for small types
//! - **Deterministic**: Same input always produces same output
//! - **Language-agnostic**: Can be implemented in any language
//! - **Little-endian**: All multi-byte integers use little-endian byte order
//!
//! ## Transfer Layout
//!
//! When transferred across WASM boundaries, all serialized data is prefixed with a length:
//! ```text
//! [4-byte length (u32, LE)][serialized data]
//! ```
//!
//! ## Type Formats
//!
//! - **Primitives**: Direct byte encoding (String: UTF-8, numbers: LE bytes, bool: 0/1)
//! - **Enums**: Tag byte + optional payload (Option, Result, Bound)
//! - **Collections**: Length-prefixed elements (Vec, tuples)
//! - **Complex**: FlatBuffers protocol (Value, Kind)
//!
//! See individual type implementations for detailed format specifications.

use std::ops::Bound;

use anyhow::Result;
#[cfg(feature = "host")]
use async_trait::async_trait;
use surrealdb_types::SurrealValue;

use crate::arg::SerializableArg;
#[cfg(feature = "host")]
use crate::controller::AsyncMemoryController;
use crate::controller::MemoryController;
#[cfg(feature = "host")]
use crate::transfer::AsyncTransfer;
use crate::transfer::{Ptr, Transfer};

/// A wrapper around serialized binary data.
///
/// This type holds the raw bytes of a serialized value and can be transferred
/// across WASM boundaries using the [`Transfer`] trait.
///
/// # Memory Layout
///
/// When transferred, `Serialized` data uses this layout:
/// ```text
/// [4-byte length (u32, LE)][data bytes]
/// ```
pub struct Serialized(pub bytes::Bytes);

// Guest side implementation (sync)
impl Transfer for Serialized {
	fn transfer(self, controller: &mut dyn MemoryController) -> Result<Ptr> {
		let len = 4 + self.0.len();
		let ptr = controller.alloc(len as u32)?;
		let mem = controller.mut_mem(ptr, len as u32);
		mem[0..4].copy_from_slice(&(self.0.len() as u32).to_le_bytes());
		mem[4..len].copy_from_slice(&self.0);
		Ok(ptr.into())
	}

	fn receive(ptr: Ptr, controller: &mut dyn MemoryController) -> Result<Self> {
		let mem = controller.mut_mem(*ptr, 4);
		let len = u32::from_le_bytes(mem[0..4].try_into()?);
		let data = controller.mut_mem(*ptr + 4, len).to_vec();
		#[allow(clippy::unnecessary_cast)]
		controller.free(*ptr, 4 + len as u32)?;
		Ok(Serialized(data.into()))
	}
}

// Host side implementation (async)
#[cfg(feature = "host")]
#[async_trait]
impl AsyncTransfer for Serialized {
	async fn transfer(self, controller: &mut dyn AsyncMemoryController) -> Result<Ptr> {
		let len = 4 + self.0.len();
		let ptr = controller.alloc(len as u32).await?;
		let mem = controller.mut_mem(ptr, len as u32)?;
		let len_bytes = (self.0.len() as u32).to_le_bytes();
		mem[0..4].copy_from_slice(len_bytes.as_slice());
		mem[4..len].copy_from_slice(&self.0);
		Ok(ptr.into())
	}

	async fn receive(ptr: Ptr, controller: &mut dyn AsyncMemoryController) -> Result<Self> {
		let mem = controller.mut_mem(*ptr, 4)?;
		let len = u32::from_le_bytes(mem[0..4].try_into()?);
		let data = controller.mut_mem(*ptr + 4, len)?.to_vec();
		#[allow(clippy::unnecessary_cast)]
		controller.free(*ptr, 4 + len as u32).await?;
		Ok(Serialized(data.into()))
	}
}

// Guest side implementation for Serializable (sync)
impl<T: Serializable> Transfer for T {
	fn transfer(self, controller: &mut dyn MemoryController) -> Result<Ptr> {
		Transfer::transfer(self.serialize()?, controller)
	}

	fn receive(ptr: Ptr, controller: &mut dyn MemoryController) -> Result<Self> {
		Self::deserialize(Transfer::receive(ptr, controller)?)
	}
}

// Host side implementation for Serializable (async)
#[cfg(feature = "host")]
#[async_trait]
impl<T: Serializable + Send> AsyncTransfer for T {
	async fn transfer(self, controller: &mut dyn AsyncMemoryController) -> Result<Ptr> {
		AsyncTransfer::transfer(self.serialize()?, controller).await
	}

	async fn receive(ptr: Ptr, controller: &mut dyn AsyncMemoryController) -> Result<Self> {
		Self::deserialize(AsyncTransfer::receive(ptr, controller).await?)
	}
}

/// A trait for types that can be serialized to and deserialized from a binary format.
///
/// This trait defines the core serialization protocol used for cross-language communication
/// in the Surrealism ecosystem. Unlike Serde, this provides complete control over the wire
/// format, ensuring compatibility across different programming languages.
///
/// # Implementation Requirements
///
/// Implementers must ensure:
/// - **Deterministic**: Same input always produces same output
/// - **Round-trip safe**: `deserialize(serialize(x))` == `x`
/// - **Little-endian**: All multi-byte integers use little-endian byte order
/// - **Documented**: Wire format must be clearly documented
///
/// # Example
///
/// ```rust,ignore
/// use surrealism_types::serialize::{Serializable, Serialized};
///
/// impl Serializable for MyType {
///     fn serialize(self) -> Result<Serialized> {
///         // Convert to bytes...
///         Ok(Serialized(bytes))
///     }
///
///     fn deserialize(serialized: Serialized) -> Result<Self> {
///         // Convert from bytes...
///         Ok(my_value)
///     }
/// }
/// ```
pub trait Serializable: Sized {
	/// Serialize this value into binary format.
	///
	/// # Errors
	///
	/// Returns an error if serialization fails (e.g., invalid UTF-8, allocation failure).
	fn serialize(self) -> Result<Serialized>;

	/// Deserialize a value from binary format.
	///
	/// # Errors
	///
	/// Returns an error if:
	/// - The data is malformed or truncated
	/// - The data doesn't match the expected format
	/// - Type-specific validation fails
	fn deserialize(serialized: Serialized) -> Result<Self>;
}

impl<T: SurrealValue> Serializable for SerializableArg<T> {
	fn serialize(self) -> Result<Serialized> {
		self.0.into_value().serialize()
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		Ok(SerializableArg(T::from_value(surrealdb_types::Value::deserialize(serialized)?)?))
	}
}

// ============================================================================
// Primitive Type Implementations
// ============================================================================

/// String serialization.
///
/// Wire format: Raw UTF-8 bytes (no null terminator)
/// ```text
/// [UTF-8 bytes...]
/// ```
impl Serializable for String {
	fn serialize(self) -> Result<Serialized> {
		Ok(Serialized(self.as_bytes().to_vec().into()))
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		String::from_utf8(serialized.0.to_vec())
			.map_err(|e| anyhow::anyhow!("Invalid UTF-8 string: {}", e))
	}
}

/// f64 (64-bit floating point) serialization.
///
/// Wire format: 8 bytes, little-endian IEEE 754
/// ```text
/// [8 bytes: f64 LE]
/// ```
impl Serializable for f64 {
	fn serialize(self) -> Result<Serialized> {
		Ok(Serialized(self.to_le_bytes().to_vec().into()))
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		if serialized.0.len() != 8 {
			return Err(anyhow::anyhow!("Expected 8 bytes for f64, got {}", serialized.0.len()));
		}

		Ok(f64::from_le_bytes(serialized.0[..8].try_into()?))
	}
}

/// u64 (64-bit unsigned integer) serialization.
///
/// Wire format: 8 bytes, little-endian
/// ```text
/// [8 bytes: u64 LE]
/// ```
impl Serializable for u64 {
	fn serialize(self) -> Result<Serialized> {
		Ok(Serialized(self.to_le_bytes().to_vec().into()))
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		if serialized.0.len() != 8 {
			return Err(anyhow::anyhow!("Expected 8 bytes for u64, got {}", serialized.0.len()));
		}

		Ok(u64::from_le_bytes(serialized.0[..8].try_into()?))
	}
}

/// i64 (64-bit signed integer) serialization.
///
/// Wire format: 8 bytes, little-endian two's complement
/// ```text
/// [8 bytes: i64 LE]
/// ```
impl Serializable for i64 {
	fn serialize(self) -> Result<Serialized> {
		Ok(Serialized(self.to_le_bytes().to_vec().into()))
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		if serialized.0.len() != 8 {
			return Err(anyhow::anyhow!("Expected 8 bytes for i64, got {}", serialized.0.len()));
		}

		Ok(i64::from_le_bytes(serialized.0[..8].try_into()?))
	}
}

/// bool (boolean) serialization.
///
/// Wire format: 1 byte (0 = false, 1 = true, other values accepted as true)
/// ```text
/// [1 byte: 0x00 or 0x01]
/// ```
impl Serializable for bool {
	fn serialize(self) -> Result<Serialized> {
		Ok(Serialized(vec![self as u8].into()))
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		Ok(serialized.0[0] != 0)
	}
}

// ============================================================================
// SurrealDB Type Implementations (FlatBuffers-based)
// ============================================================================

/// [`surrealdb_types::Kind`] serialization using FlatBuffers.
///
/// Wire format: FlatBuffers-encoded Kind schema
/// ```text
/// [FlatBuffers data...]
/// ```
impl Serializable for surrealdb_types::Kind {
	fn serialize(self) -> Result<Serialized> {
		let x = surrealdb_types::encode_kind(&self)?;
		Ok(Serialized(x.into()))
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		surrealdb_types::decode_kind(&serialized.0)
	}
}

/// [`surrealdb_types::Value`] serialization using FlatBuffers.
///
/// Wire format: FlatBuffers-encoded Value schema
/// ```text
/// [FlatBuffers data...]
/// ```
///
/// This leverages the `surrealdb-protocol` FlatBuffers schema, which supports
/// all SurrealDB types including records, geometries, durations, etc.
impl Serializable for surrealdb_types::Value {
	fn serialize(self) -> Result<Serialized> {
		let x = surrealdb_types::encode(&self)?;
		Ok(Serialized(x.into()))
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		surrealdb_types::decode(&serialized.0)
	}
}

// ============================================================================
// Standard Library Type Implementations
// ============================================================================

/// [`Result<T, E>`] serialization with tag-based discrimination.
///
/// Wire format:
/// ```text
/// Ok(T):  [0x00][serialized T]
/// Err(E): [0x01][serialized E]
/// ```
impl<T: Serializable, E: Serializable> Serializable for Result<T, E> {
	fn serialize(self) -> Result<Serialized> {
		match self {
			Ok(value) => {
				let serialized = value.serialize()?;
				let mut result = Vec::with_capacity(1 + serialized.0.len());
				result.push(0); // First byte is 0 for Ok
				result.extend_from_slice(&serialized.0);
				Ok(Serialized(result.into()))
			}
			Err(error) => {
				let serialized = error.serialize()?;
				let mut result = Vec::with_capacity(1 + serialized.0.len());
				result.push(1); // First byte is 1 for Err
				result.extend_from_slice(&serialized.0);
				Ok(Serialized(result.into()))
			}
		}
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		if serialized.0.is_empty() {
			return Err(anyhow::anyhow!("Empty serialized data"));
		}

		match serialized.0[0] {
			0 => {
				// Ok variant - deserialize the value from remaining bytes
				let value_bytes = Serialized(serialized.0.slice(1..));
				let value = T::deserialize(value_bytes)?;
				Ok(Ok(value))
			}
			1 => {
				// Err variant - extract error string from remaining bytes
				let error_bytes = &serialized.0[1..];
				let error = E::deserialize(Serialized(error_bytes.to_vec().into()))?;
				Ok(Err(error))
			}
			_ => Err(anyhow::anyhow!("Invalid Result variant byte")),
		}
	}
}

/// [`anyhow::Result<T>`] serialization.
///
/// Wire format: Same as `Result<T, String>` (error converted to string)
/// ```text
/// Ok(T):        [0x00][serialized T]
/// Err(String):  [0x01][error message as UTF-8]
/// ```
impl<T: Serializable> Serializable for anyhow::Result<T> {
	fn serialize(self) -> Result<Serialized> {
		self.map_err(|e| e.to_string()).serialize()
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		Ok(Result::<T, String>::deserialize(serialized)?.map_err(|e| anyhow::anyhow!(e)))
	}
}

/// [`Option<T>`] serialization with tag-based discrimination.
///
/// Wire format:
/// ```text
/// Some(T): [0x00][serialized T]
/// None:    [0x01]
/// ```
impl<T: Serializable> Serializable for Option<T> {
	fn serialize(self) -> Result<Serialized> {
		match self {
			Some(value) => {
				let serialized = value.serialize()?;
				let mut result = Vec::with_capacity(1 + serialized.0.len());
				result.push(0); // First byte is 0 for Some
				result.extend_from_slice(&serialized.0);
				Ok(Serialized(result.into()))
			}
			None => Ok(Serialized(vec![1].into())),
		}
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		if serialized.0.is_empty() {
			return Err(anyhow::anyhow!("Empty serialized data"));
		}

		match serialized.0[0] {
			0 => {
				// Some variant - deserialize the value from remaining bytes
				let value_bytes = Serialized(serialized.0.slice(1..));
				let value = T::deserialize(value_bytes)?;
				Ok(Some(value))
			}
			1 => {
				// None variant
				Ok(None)
			}
			_ => Err(anyhow::anyhow!("Invalid Option variant byte")),
		}
	}
}

/// [`Vec<T>`] serialization with length-prefixed elements.
///
/// Wire format:
/// ```text
/// [4-byte count (u32 LE)]
/// [4-byte len1 (u32 LE)][element1 data]
/// [4-byte len2 (u32 LE)][element2 data]
/// ...
/// ```
impl<T: Serializable> Serializable for Vec<T> {
	fn serialize(self) -> Result<Serialized> {
		let mut result = (self.len() as u32).to_le_bytes().to_vec();
		for value in self {
			let serialized = value.serialize()?;
			result.extend_from_slice(&(serialized.0.len() as u32).to_le_bytes());
			result.extend_from_slice(&serialized.0);
		}
		Ok(Serialized(result.into()))
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		if serialized.0.is_empty() {
			return Err(anyhow::anyhow!("Empty serialized data"));
		}

		let len = u32::from_le_bytes(serialized.0[0..4].try_into()?) as usize;
		let mut result = Vec::with_capacity(len);
		let mut pos = 4;
		for _ in 0..len {
			let value_len = u32::from_le_bytes(serialized.0[pos..pos + 4].try_into()?) as usize;
			let value_bytes = Serialized(serialized.0.slice(pos + 4..pos + 4 + value_len));
			let value = T::deserialize(value_bytes)?;
			result.push(value);
			pos += 4 + value_len;
		}
		Ok(result)
	}
}

/// [`std::ops::Bound<T>`] serialization for range bounds.
///
/// Wire format:
/// ```text
/// Unbounded:    [0x00]
/// Included(T):  [0x01][serialized T]
/// Excluded(T):  [0x02][serialized T]
/// ```
impl<T: Serializable> Serializable for Bound<T> {
	fn serialize(self) -> Result<Serialized> {
		let bytes = match self {
			Bound::Unbounded => {
				vec![0]
			}
			Bound::Included(value) => {
				let serialized = value.serialize()?;
				let mut result = Vec::with_capacity(1 + serialized.0.len());
				result.push(1);
				result.extend_from_slice(&serialized.0);
				result
			}
			Bound::Excluded(value) => {
				let serialized = value.serialize()?;
				let mut result = Vec::with_capacity(1 + serialized.0.len());
				result.push(2);
				result.extend_from_slice(&serialized.0);
				result
			}
		};

		Ok(Serialized(bytes.into()))
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		let bytes = serialized.0;
		match bytes[0] {
			0 => Ok(Bound::Unbounded),
			1 => {
				let value = T::deserialize(Serialized(bytes.slice(1..)))?;
				Ok(Bound::Included(value))
			}
			2 => {
				let value = T::deserialize(Serialized(bytes.slice(1..)))?;
				Ok(Bound::Excluded(value))
			}
			_ => Err(anyhow::anyhow!("Invalid Bound variant byte")),
		}
	}
}

/// A serializable range type that can represent any Rust range.
///
/// Wire format:
/// ```text
/// [4-byte beg_len (u32 LE)]
/// [4-byte end_len (u32 LE)]
/// [beg_bound data (beg_len bytes)]
/// [end_bound data (end_len bytes)]
/// ```
///
/// This allows representing `..`, `start..`, `..end`, `start..end`,
/// `start..=end`, etc. by encoding the start and end bounds.
#[derive(Debug)]
pub struct SerializableRange<T: Serializable> {
	/// The start bound of the range.
	pub beg: Bound<T>,
	/// The end bound of the range.
	pub end: Bound<T>,
}

impl<T: Serializable + Clone> SerializableRange<T> {
	/// Convert any range type into a `SerializableRange`.
	///
	/// # Note
	///
	/// This function clones the bounds because Rust's `IntoBounds` trait is unstable.
	/// Once stabilized, this could avoid the clone.
	pub fn from_range_bounds(range: impl std::ops::RangeBounds<T>) -> Result<Self> {
		Ok(SerializableRange {
			beg: range.start_bound().cloned(),
			end: range.end_bound().cloned(),
		})
	}
}

impl<T: Serializable> Serializable for SerializableRange<T> {
	fn serialize(self) -> Result<Serialized> {
		let beg = self.beg.serialize()?;
		let end = self.end.serialize()?;
		let mut result = Vec::with_capacity(8 + beg.0.len() + end.0.len());
		result.extend_from_slice(&(beg.0.len() as u32).to_le_bytes());
		result.extend_from_slice(&(end.0.len() as u32).to_le_bytes());
		result.extend_from_slice(&beg.0);
		result.extend_from_slice(&end.0);
		Ok(Serialized(result.into()))
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		let bytes = serialized.0;
		let beg_len = u32::from_le_bytes(bytes[0..4].try_into()?) as usize;
		let end_len = u32::from_le_bytes(bytes[4..8].try_into()?) as usize;
		let beg = Bound::deserialize(Serialized(bytes.slice(8..8 + beg_len)))?;
		let end = Bound::deserialize(Serialized(bytes.slice(8 + beg_len..8 + beg_len + end_len)))?;
		let range = SerializableRange {
			beg,
			end,
		};
		Ok(range)
	}
}

impl<T: Serializable> std::ops::RangeBounds<T> for SerializableRange<T> {
	fn start_bound(&self) -> Bound<&T> {
		self.beg.as_ref()
	}

	fn end_bound(&self) -> Bound<&T> {
		self.end.as_ref()
	}
}

/// Tuple serialization with length-prefixed elements.
///
/// Wire format for tuple `(A, B, ...)`:
/// ```text
/// [4-byte lenA (u32 LE)][element A data]
/// [4-byte lenB (u32 LE)][element B data]
/// ...
/// ```
///
/// Tuples from 1 to 10 elements are supported.
macro_rules! impl_tuple {
    ($(($($name:ident),+)),+ $(,)?) => {
        $(impl<$($name: Serializable),+> Serializable for ($($name,)+) {
            fn serialize(self) -> Result<Serialized> {
                #[allow(non_snake_case)]
                let ($($name,)+) = self;
                $(#[allow(non_snake_case)] let $name = $name.serialize()?;)+
                let mut result = Vec::with_capacity(0 $(+ $name.0.len())+);
                $(result.extend_from_slice(&($name.0.len() as u32).to_le_bytes());
                result.extend_from_slice(&$name.0);)+
                Ok(Serialized(result.into()))
            }

            fn deserialize(serialized: Serialized) -> Result<Self> {
                let mut pos = 0;
                $(
                    let len = u32::from_le_bytes(serialized.0[pos..pos + 4].try_into()?) as usize;
                    #[allow(non_snake_case)]
                    let $name = $name::deserialize(Serialized(serialized.0.slice(pos + 4..pos + 4 + len)))?;
                    pos += 4 + len;
                )+
                let _ = pos;
                Ok(($($name,)+))
            }
        })+
    }
}

impl_tuple! {
	(A),
	(A, B),
	(A, B, C),
	(A, B, C, D),
	(A, B, C, D, E),
	(A, B, C, D, E, F),
	(A, B, C, D, E, F, G),
	(A, B, C, D, E, F, G, H),
	(A, B, C, D, E, F, G, H, I),
	(A, B, C, D, E, F, G, H, I, J),
}

/// Unit type `()` serialization.
///
/// Wire format: Empty (0 bytes)
/// ```text
/// []
/// ```
impl Serializable for () {
	fn serialize(self) -> Result<Serialized> {
		Ok(Serialized(vec![].into()))
	}

	fn deserialize(_: Serialized) -> Result<Self> {
		Ok(())
	}
}
