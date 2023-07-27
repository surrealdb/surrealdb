use crate::err::Error;
use crate::kvs::Val;
use crate::sql::index::VectorType;
use crate::sql::{Number, Value};

pub(crate) mod balltree;
mod points;
mod store;

enum Vector {
	I64(Vec<i64>),
	F64(Vec<f64>),
	U32(Vec<u32>),
	I32(Vec<i32>),
	F32(Vec<f32>),
	U16(Vec<u16>),
	I16(Vec<i16>),
}

impl Vector {
	fn new(a: &[Value], vt: &VectorType, d: usize) -> Result<Self, Error> {
		Self::check_dim(a, d)?;
		match vt {
			VectorType::I64 => Self::new_i64(a, d),
			VectorType::F64 => Self::new_f64(a, d),
			VectorType::U32 => Self::new_u32(a, d),
			VectorType::I32 => Self::new_i32(a, d),
			VectorType::F32 => Self::new_f32(a, d),
			VectorType::U16 => Self::new_u16(a, d),
			VectorType::I16 => Self::new_i16(a, d),
		}
	}

	fn new_i64(a: &[Value], d: usize) -> Result<Self, Error> {
		let mut r = Vec::with_capacity(d);
		for v in a.iter() {
			r.push(Self::check_number(v, "64 bits signed integer")?.to_int());
		}
		Ok(Self::I64(r))
	}

	pub(super) fn new_f64(a: &[Value], d: usize) -> Result<Self, Error> {
		let mut r = Vec::with_capacity(d);
		for v in a.iter() {
			r.push(Self::check_number(v, "64 bits float number")?.to_float());
		}
		Ok(Self::F64(r))
	}

	pub(super) fn new_u32(a: &[Value], d: usize) -> Result<Self, Error> {
		let mut r = Vec::with_capacity(d);
		for v in a.iter() {
			let n = Self::check_number(v, "32 bits unsigned number")?.to_int();
			if n > u32::MAX as i64 || n < u32::MIN as i64 {
				return Err(Error::InvalidVectorType {
					current: v.to_raw_string(),
					expected: "32 bits unsigned number",
				});
			}
			r.push(n as u32);
		}
		Ok(Self::U32(r))
	}

	fn new_i32(a: &[Value], d: usize) -> Result<Self, Error> {
		let mut r = Vec::with_capacity(d);
		for v in a.iter() {
			let n = Self::check_number(v, "32 bits signed number")?.to_int();
			if n > i32::MAX as i64 || n < i32::MIN as i64 {
				return Err(Error::InvalidVectorType {
					current: v.to_raw_string(),
					expected: "32 bits signed number",
				});
			}
			r.push(n as i32);
		}
		Ok(Self::I32(r))
	}

	fn new_f32(a: &[Value], d: usize) -> Result<Self, Error> {
		let mut r = Vec::with_capacity(d);
		for v in a.iter() {
			let n = Self::check_number(v, "32 bits float number")?.to_float();
			if n > f32::MAX as f64 || n < f32::MIN as f64 {
				return Err(Error::InvalidVectorType {
					current: v.to_raw_string(),
					expected: "32 bits float number",
				});
			}
			r.push(n as f32);
		}
		Ok(Self::F32(r))
	}

	fn new_u16(a: &[Value], d: usize) -> Result<Self, Error> {
		let mut r = Vec::with_capacity(d);
		for v in a.iter() {
			let n = Self::check_number(v, "16 bits unsigned number")?.to_int();
			if n > u16::MAX as i64 || n < u16::MIN as i64 {
				return Err(Error::InvalidVectorType {
					current: v.to_raw_string(),
					expected: "16 bits unsigned number",
				});
			}
			r.push(n as u16);
		}
		Ok(Self::U16(r))
	}

	fn new_i16(a: &[Value], d: usize) -> Result<Self, Error> {
		let mut r = Vec::with_capacity(d);
		for v in a.iter() {
			let n = Self::check_number(v, "16 bits signed number")?.to_int();
			if n > i16::MAX as i64 || n < i16::MIN as i64 {
				return Err(Error::InvalidVectorType {
					current: v.to_raw_string(),
					expected: "16 bits signed number",
				});
			}
			r.push(n as i16);
		}
		Ok(Self::I16(r))
	}

	fn check_dim(a: &[Value], s: usize) -> Result<(), Error> {
		if s != a.len() {
			return Err(Error::InvalidVectorDimension {
				current: a.len(),
				expected: s,
			});
		}
		Ok(())
	}

	fn check_number<'a>(v: &'a Value, expected: &'static str) -> Result<&'a Number, Error> {
		if let Value::Number(n) = v {
			Ok(n)
		} else {
			Err(Error::InvalidVectorType {
				current: v.to_raw_string(),
				expected,
			})
		}
	}

	fn _try_from(val: &[u8], vt: &VectorType) -> Result<Self, Error> {
		Ok(match vt {
			VectorType::I64 => Self::I64(bincode::deserialize(val)?),
			VectorType::F64 => Self::F64(bincode::deserialize(val)?),
			VectorType::U32 => Self::U32(bincode::deserialize(val)?),
			VectorType::I32 => Self::I32(bincode::deserialize(val)?),
			VectorType::F32 => Self::F32(bincode::deserialize(val)?),
			VectorType::U16 => Self::U16(bincode::deserialize(val)?),
			VectorType::I16 => Self::I16(bincode::deserialize(val)?),
		})
	}
}

impl TryFrom<&Vector> for Val {
	type Error = bincode::Error;

	fn try_from(v: &Vector) -> Result<Self, Self::Error> {
		match v {
			Vector::I64(v) => bincode::serialize(v),
			Vector::F64(v) => bincode::serialize(v),
			Vector::U32(v) => bincode::serialize(v),
			Vector::I32(v) => bincode::serialize(v),
			Vector::F32(v) => bincode::serialize(v),
			Vector::U16(v) => bincode::serialize(v),
			Vector::I16(v) => bincode::serialize(v),
		}
	}
}
