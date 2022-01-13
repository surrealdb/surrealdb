use crate::sql::number::Number;

pub trait Percentile {
	fn percentile(self, _: Number) -> Number;
}

impl Percentile for Vec<Number> {
	fn percentile(self, _: Number) -> Number {
		todo!()
	}
}
