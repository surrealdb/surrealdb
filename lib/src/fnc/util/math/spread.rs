use crate::sql::number::Number;

pub trait Spread {
	/// (Implementing range since var and IQR are implemented)
	/// O(n) time complex
	fn spread(self) -> Number;
}

impl Spread for Vec<Number> {
	fn spread(self) -> Number {
		let init = self.get(0);

		let min_max = self.iter().fold((init, init), |(mut min, mut max), val| {
			min = std::cmp::min(min, Some(val));
			max = std::cmp::max(max, Some(val));

			(min, max)
		});

		match min_max {
			(Some(min), Some(max)) => max - min,
			_ => Number::Float(f64::NAN),
		}
	}
}
