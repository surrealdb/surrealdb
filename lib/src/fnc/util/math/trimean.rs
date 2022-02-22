use crate::sql::number::Number;

pub trait Trimean {
	fn trimean(self) -> Number;
}

impl Trimean for Vec<Number> {
	fn trimean(self) -> Number {
		todo!()
	}
}
