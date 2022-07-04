use surrealdb::sql::Array;
use surrealdb::sql::Value;

pub trait Take {
	fn take_one(self) -> Value;
	fn take_two(self) -> (Value, Value);
	fn take_three(self) -> (Value, Value, Value);
}

impl Take for Array {
	// Convert the array to one argument
	fn take_one(self) -> Value {
		let mut x = self.into_iter();
		match x.next() {
			Some(a) => a,
			None => Value::None,
		}
	}
	// Convert the array to two arguments
	fn take_two(self) -> (Value, Value) {
		let mut x = self.into_iter();
		match (x.next(), x.next()) {
			(Some(a), Some(b)) => (a, b),
			(Some(a), None) => (a, Value::None),
			(_, _) => (Value::None, Value::None),
		}
	}
	// Convert the array to three arguments
	fn take_three(self) -> (Value, Value, Value) {
		let mut x = self.into_iter();
		match (x.next(), x.next(), x.next()) {
			(Some(a), Some(b), Some(c)) => (a, b, c),
			(Some(a), Some(b), None) => (a, b, Value::None),
			(Some(a), None, None) => (a, Value::None, Value::None),
			(_, _, _) => (Value::None, Value::None, Value::None),
		}
	}
}
