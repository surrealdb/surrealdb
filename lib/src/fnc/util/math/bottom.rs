use crate::sql::number::Number;

pub trait Bottom {
	fn bottom(self, _c: i64) -> Number;
}

impl Bottom for Vec<Number> {
	fn bottom(self, _c: i64) -> Number {
		todo!()
	}
}
