use crate::rpc::protocol::v1::types::V1Array;
use crate::rpc::protocol::v1::types::V1Value;

use super::error::RpcError;

pub trait Take {
	fn needs_one(self) -> Result<V1Value, RpcError>;
	fn needs_two(self) -> Result<(V1Value, V1Value), RpcError>;
	fn needs_one_or_two(self) -> Result<(V1Value, V1Value), RpcError>;
	fn needs_two_or_three(self) -> Result<(V1Value, V1Value, V1Value), RpcError>;
	fn needs_one_two_or_three(self) -> Result<(V1Value, V1Value, V1Value), RpcError>;
	fn needs_three_or_four(self) -> Result<(V1Value, V1Value, V1Value, V1Value), RpcError>;
	fn needs_three_four_or_five(
		self,
	) -> Result<(V1Value, V1Value, V1Value, V1Value, V1Value), RpcError>;
}

impl Take for V1Array {
	/// Convert the array to one argument
	fn needs_one(self) -> Result<V1Value, RpcError> {
		if self.len() != 1 {
			return Err(RpcError::InvalidParams);
		}
		let mut x = self.0.into_iter();
		match x.next() {
			Some(a) => Ok(a),
			None => Ok(V1Value::None),
		}
	}
	/// Convert the array to two arguments
	fn needs_two(self) -> Result<(V1Value, V1Value), RpcError> {
		if self.len() != 2 {
			return Err(RpcError::InvalidParams);
		}
		let mut x = self.0.into_iter();
		match (x.next(), x.next()) {
			(Some(a), Some(b)) => Ok((a, b)),
			(Some(a), None) => Ok((a, V1Value::None)),
			(_, _) => Ok((V1Value::None, V1Value::None)),
		}
	}
	/// Convert the array to two arguments
	fn needs_one_or_two(self) -> Result<(V1Value, V1Value), RpcError> {
		if self.is_empty() || self.len() > 2 {
			return Err(RpcError::InvalidParams);
		}
		let mut x = self.0.into_iter();
		match (x.next(), x.next()) {
			(Some(a), Some(b)) => Ok((a, b)),
			(Some(a), None) => Ok((a, V1Value::None)),
			(_, _) => Ok((V1Value::None, V1Value::None)),
		}
	}
	/// Convert the array to three arguments
	fn needs_two_or_three(self) -> Result<(V1Value, V1Value, V1Value), RpcError> {
		if self.len() < 2 || self.len() > 3 {
			return Err(RpcError::InvalidParams);
		}
		let mut x = self.0.into_iter();
		match (x.next(), x.next(), x.next()) {
			(Some(a), Some(b), Some(c)) => Ok((a, b, c)),
			(Some(a), Some(b), None) => Ok((a, b, V1Value::None)),
			(_, _, _) => Ok((V1Value::None, V1Value::None, V1Value::None)),
		}
	}
	/// Convert the array to three arguments
	fn needs_one_two_or_three(self) -> Result<(V1Value, V1Value, V1Value), RpcError> {
		if self.is_empty() || self.len() > 3 {
			return Err(RpcError::InvalidParams);
		}
		let mut x = self.0.into_iter();
		match (x.next(), x.next(), x.next()) {
			(Some(a), Some(b), Some(c)) => Ok((a, b, c)),
			(Some(a), Some(b), None) => Ok((a, b, V1Value::None)),
			(Some(a), None, None) => Ok((a, V1Value::None, V1Value::None)),
			(_, _, _) => Ok((V1Value::None, V1Value::None, V1Value::None)),
		}
	}
	/// Convert the array to four arguments
	fn needs_three_or_four(self) -> Result<(V1Value, V1Value, V1Value, V1Value), RpcError> {
		if self.len() < 3 || self.len() > 4 {
			return Err(RpcError::InvalidParams);
		}
		let mut x = self.0.into_iter();
		match (x.next(), x.next(), x.next(), x.next()) {
			(Some(a), Some(b), Some(c), Some(d)) => Ok((a, b, c, d)),
			(Some(a), Some(b), Some(c), None) => Ok((a, b, c, V1Value::None)),
			(_, _, _, _) => Ok((V1Value::None, V1Value::None, V1Value::None, V1Value::None)),
		}
	}
	/// Convert the array to four arguments
	fn needs_three_four_or_five(
		self,
	) -> Result<(V1Value, V1Value, V1Value, V1Value, V1Value), RpcError> {
		if self.len() < 3 || self.len() > 5 {
			return Err(RpcError::InvalidParams);
		}
		let mut x = self.0.into_iter();
		match (x.next(), x.next(), x.next(), x.next(), x.next()) {
			(Some(a), Some(b), Some(c), Some(d), Some(e)) => Ok((a, b, c, d, e)),
			(Some(a), Some(b), Some(c), Some(d), None) => Ok((a, b, c, d, V1Value::None)),
			(Some(a), Some(b), Some(c), None, None) => Ok((a, b, c, V1Value::None, V1Value::None)),
			(_, _, _, _, _) => {
				Ok((V1Value::None, V1Value::None, V1Value::None, V1Value::None, V1Value::None))
			}
		}
	}
}
