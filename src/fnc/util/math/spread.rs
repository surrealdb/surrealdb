use crate::sql::number::Number;

pub trait Spread {
	fn spread(self) -> Number;
}

impl Spread for Vec<Number> {
	fn spread(self) -> Number {
		todo!()
	}
}
