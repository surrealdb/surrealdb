use crate::sql::number::Number;

pub trait Mode {
	fn mode(self) -> Number;
}

impl Mode for Vec<Number> {
	fn mode(self) -> Number {
		todo!()
	}
}
