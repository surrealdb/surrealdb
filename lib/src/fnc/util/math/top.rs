use crate::sql::number::Number;

pub trait Top {
	fn top(self, _c: i64) -> Number;
}

impl Top for Vec<Number> {
	fn top(self, _c: i64) -> Number {
		todo!()
	}
}
