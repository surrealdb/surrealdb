use crate::sql::number::Number;

pub trait Interquartile {
	fn interquartile(self) -> Number;
}

impl Interquartile for Vec<Number> {
	fn interquartile(self) -> Number {
		todo!()
	}
}
