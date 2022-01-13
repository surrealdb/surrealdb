use crate::sql::number::Number;

pub trait Median {
	fn median(&mut self) -> Number;
}

impl Median for Vec<Number> {
	fn median(&mut self) -> Number {
		self.sort();
		self.remove(self.len() / 2)
	}
}
