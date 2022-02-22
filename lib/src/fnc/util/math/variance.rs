use crate::sql::number::Number;

pub trait Variance {
	fn variance(self) -> Number;
}

impl Variance for Vec<Number> {
	fn variance(self) -> Number {
		todo!()
	}
}
