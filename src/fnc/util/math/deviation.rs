use crate::sql::number::Number;

pub trait Deviation {
	fn deviation(self) -> Number;
}

impl Deviation for Vec<Number> {
	fn deviation(self) -> Number {
		todo!()
	}
}
