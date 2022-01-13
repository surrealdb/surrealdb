use crate::sql::number::Number;

pub trait Midhinge {
	fn midhinge(self) -> Number;
}

impl Midhinge for Vec<Number> {
	fn midhinge(self) -> Number {
		todo!()
	}
}
