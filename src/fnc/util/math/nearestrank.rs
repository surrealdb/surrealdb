use crate::sql::number::Number;

pub trait Nearestrank {
	fn nearestrank(self, _: Number) -> Number;
}

impl Nearestrank for Vec<Number> {
	fn nearestrank(self, _: Number) -> Number {
		todo!()
	}
}
