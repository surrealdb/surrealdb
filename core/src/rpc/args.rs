use crate::sql::Array;
use crate::sql::Value;

use super::rpc_error::RpcError;

pub trait Take {
	fn needs_one(self) -> Result<Value, RpcError>;
	fn needs_two(self) -> Result<(Value, Value), RpcError>;
	fn needs_three(self) -> Result<(Value, Value, Value), RpcError>;
	fn needs_zero_one_or_two(self) -> Result<(Value, Value), RpcError>;
	fn needs_one_or_two(self) -> Result<(Value, Value), RpcError>;
	fn needs_one_two_or_three(self) -> Result<(Value, Value, Value), RpcError>;
	fn needs_three_or_four(self) -> Result<(Value, Value, Value, Value), RpcError>;
}

impl Take for Array {
	/// Convert the array to one argument
	fn needs_one(self) -> Result<Value, RpcError> {
		if self.len() != 1 {
			return Err(RpcError::InvalidParams);
		}
		let mut x = self.into_iter();
		match x.next() {
			Some(a) => Ok(a),
			None => Ok(Value::None),
		}
	}
	/// Convert the array to two arguments
	fn needs_two(self) -> Result<(Value, Value), RpcError> {
		if self.len() != 2 {
			return Err(RpcError::InvalidParams);
		}
		let mut x = self.into_iter();
		match (x.next(), x.next()) {
			(Some(a), Some(b)) => Ok((a, b)),
			(Some(a), None) => Ok((a, Value::None)),
			(_, _) => Ok((Value::None, Value::None)),
		}
	}
	/// Convert the array to three arguments
	fn needs_three(self) -> Result<(Value, Value, Value), RpcError> {
		if self.len() != 3 {
			return Err(RpcError::InvalidParams);
		}
		let mut x = self.into_iter();
		match (x.next(), x.next(), x.next()) {
			(Some(a), Some(b), Some(c)) => Ok((a, b, c)),
			_ => Err(RpcError::InvalidParams),
		}
	}
	/// Convert the array to two arguments
	fn needs_zero_one_or_two(self) -> Result<(Value, Value), RpcError> {
		if self.len() > 2 {
			return Err(RpcError::InvalidParams);
		}
		let mut x = self.into_iter();
		match (x.next(), x.next()) {
			(Some(a), Some(b)) => Ok((a, b)),
			(Some(a), None) => Ok((a, Value::None)),
			(_, _) => Ok((Value::None, Value::None)),
		}
	}
	/// Convert the array to two arguments
	fn needs_one_or_two(self) -> Result<(Value, Value), RpcError> {
		if self.is_empty() || self.len() > 2 {
			return Err(RpcError::InvalidParams);
		}
		let mut x = self.into_iter();
		match (x.next(), x.next()) {
			(Some(a), Some(b)) => Ok((a, b)),
			(Some(a), None) => Ok((a, Value::None)),
			(_, _) => Ok((Value::None, Value::None)),
		}
	}
	/// Convert the array to three arguments
	fn needs_one_two_or_three(self) -> Result<(Value, Value, Value), RpcError> {
		if self.is_empty() || self.len() > 3 {
			return Err(RpcError::InvalidParams);
		}
		let mut x = self.into_iter();
		match (x.next(), x.next(), x.next()) {
			(Some(a), Some(b), Some(c)) => Ok((a, b, c)),
			(Some(a), Some(b), None) => Ok((a, b, Value::None)),
			(Some(a), None, None) => Ok((a, Value::None, Value::None)),
			(_, _, _) => Ok((Value::None, Value::None, Value::None)),
		}
	}
	/// Convert the array to four arguments
	fn needs_three_or_four(self) -> Result<(Value, Value, Value, Value), RpcError> {
		if self.len() < 3 || self.len() > 4 {
			return Err(RpcError::InvalidParams);
		}
		let mut x = self.into_iter();
		match (x.next(), x.next(), x.next(), x.next()) {
			(Some(a), Some(b), Some(c), Some(d)) => Ok((a, b, c, d)),
			(Some(a), Some(b), Some(c), None) => Ok((a, b, c, Value::None)),
			(_, _, _, _) => Ok((Value::None, Value::None, Value::None, Value::None)),
		}
	}
}
