use surrealdb::sql::Array;
use surrealdb::sql::Value;

pub trait Take {
	fn needs_one(self) -> Result<Value, ()>;
	fn needs_two(self) -> Result<(Value, Value), ()>;
	fn needs_one_or_two(self) -> Result<(Value, Value), ()>;
}

impl Take for Array {
	/// Convert the array to one argument
	fn needs_one(self) -> Result<Value, ()> {
		if self.len() < 1 {
			return Err(());
		}
		let mut x = self.into_iter();
		match x.next() {
			Some(a) => Ok(a),
			None => Ok(Value::None),
		}
	}
	/// Convert the array to two arguments
	fn needs_two(self) -> Result<(Value, Value), ()> {
		if self.len() < 2 {
			return Err(());
		}
		let mut x = self.into_iter();
		match (x.next(), x.next()) {
			(Some(a), Some(b)) => Ok((a, b)),
			(Some(a), None) => Ok((a, Value::None)),
			(_, _) => Ok((Value::None, Value::None)),
		}
	}
	/// Convert the array to two arguments
	fn needs_one_or_two(self) -> Result<(Value, Value), ()> {
		if self.len() < 1 {
			return Err(());
		}
		let mut x = self.into_iter();
		match (x.next(), x.next()) {
			(Some(a), Some(b)) => Ok((a, b)),
			(Some(a), None) => Ok((a, Value::None)),
			(_, _) => Ok((Value::None, Value::None)),
		}
	}
}
