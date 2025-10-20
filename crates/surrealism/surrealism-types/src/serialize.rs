use std::ops::Bound;

use anyhow::Result;
use surrealdb_protocol::fb::v1 as proto_fb;
use surrealdb_types::{FromFlatbuffers, SurrealValue, ToFlatbuffers};

use crate::arg::SerializableArg;
use crate::controller::MemoryController;
use crate::transfer::{Ptr, Transfer};

pub struct Serialized(pub bytes::Bytes);

impl Transfer for Serialized {
	fn transfer(self, controller: &mut dyn MemoryController) -> Result<Ptr> {
		let len = 4 + self.0.len();
		let ptr = controller.alloc(len as u32, 8)?;
		let mem = controller.mut_mem(ptr, len as u32);
		mem[0..4].copy_from_slice(&(self.0.len() as u32).to_le_bytes());
		mem[4..len].copy_from_slice(&self.0);
		Ok(ptr.into())
	}

	fn receive(ptr: Ptr, controller: &mut dyn MemoryController) -> Result<Self> {
		let mem = controller.mut_mem(*ptr, 4);
		let len = u32::from_le_bytes(mem[0..4].try_into()?);
		let data = controller.mut_mem(*ptr + 4, len).to_vec();
		controller.free(*ptr, 4 + len as u32)?;
		Ok(Serialized(data.into()))
	}
}

impl<T: Serializable> Transfer for T {
	fn transfer(self, controller: &mut dyn MemoryController) -> Result<Ptr> {
		self.serialize()?.transfer(controller)
	}

	fn receive(ptr: Ptr, controller: &mut dyn MemoryController) -> Result<Self> {
		Self::deserialize(Serialized::receive(ptr, controller)?)
	}
}

pub trait Serializable: Sized {
	fn serialize(self) -> Result<Serialized>;
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

// string
// first byte indicates the length of the string
// followed by the string bytes
impl Serializable for String {
	fn serialize(self) -> Result<Serialized> {
		Ok(Serialized(self.as_bytes().to_vec().into()))
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		String::from_utf8(serialized.0.to_vec())
			.map_err(|e| anyhow::anyhow!("Invalid UTF-8 string: {}", e))
	}
}

// f64
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

//u64
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

// i64
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

//bool
impl Serializable for bool {
	fn serialize(self) -> Result<Serialized> {
		Ok(Serialized(vec![self as u8].into()))
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		Ok(serialized.0[0] != 0)
	}
}

impl Serializable for surrealdb_types::Kind {
	fn serialize(self) -> Result<Serialized> {
		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let root = self.to_fb(&mut builder)?;
		builder.finish(root, None);
		let data = builder.finished_data().to_vec();
		Ok(Serialized(data.into()))
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		let value = flatbuffers::root::<proto_fb::Kind>(&serialized.0)?;
		Ok(surrealdb_types::Kind::from_fb(value)?)
	}
}

impl Serializable for surrealdb_types::Value {
	fn serialize(self) -> Result<Serialized> {
		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let root = self.to_fb(&mut builder)?;
		builder.finish(root, None);
		Ok(Serialized(builder.finished_data().to_vec().into()))
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		let value = flatbuffers::root::<proto_fb::Value>(&serialized.0)?;
		Ok(surrealdb_types::Value::from_fb(value)?)
	}
}

// First byte to be 0 for Ok, followed by the serialized value.
// First byte to be 1 for Err, followed by the serialized error string.
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

// anyhow::Result
// depends on rust's standard result type
impl<T: Serializable> Serializable for anyhow::Result<T> {
	fn serialize(self) -> Result<Serialized> {
		self.map_err(|e| e.to_string()).serialize()
	}

	fn deserialize(serialized: Serialized) -> Result<Self> {
		Ok(Result::<T, String>::deserialize(serialized)?.map_err(|e| anyhow::anyhow!(e)))
	}
}

// Option
// first byte to be 0 for Some, followed by the serialized value.
// first byte to be 1 for None.
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

// Vec
// first byte indicates the length of the vector
// followed by the serialized values, all prefixed with the length of the value
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

// Bound
// First byte indicates the bound type (0 for Unbounded, 1 for Included, 2 for Excluded)
// followed by the serialized value
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

// Range
// Starts with two u32 values, which indicate how many bytes the start and end are
// followed by the serialized start and end, if any
#[derive(Debug)]
pub struct SerializableRange<T: Serializable> {
	pub beg: Bound<T>,
	pub end: Bound<T>,
}

impl<T: Serializable + Clone> SerializableRange<T> {
	/// FYI: IntoBounds is unstable, so we're left with RangeBounds which causes this function to
	/// clone.
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

// Tuples
// Each element is prefixed with the length of the element
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

impl Serializable for () {
	fn serialize(self) -> Result<Serialized> {
		Ok(Serialized(vec![].into()))
	}

	fn deserialize(_: Serialized) -> Result<Self> {
		Ok(())
	}
}
