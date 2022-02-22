use crate::sql::number::Number;

pub trait Quartile {
	fn quartile(self) -> Number;
}

impl Quartile for Vec<Number> {
	fn quartile(self) -> Number {
		todo!()
	}
}
