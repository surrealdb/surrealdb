use crate::val::Number;

pub trait Spread {
	/// Gets the extent to which a distribution is stretched
	/// O(n) time complex
	fn spread(self) -> Number;
}

impl Spread for Vec<Number> {
	fn spread(self) -> Number {
		// Get the initial number
		let init = self.first();
		// Get the minimum and the maximum
		let min_max = self.iter().fold((init, init), |(mut min, mut max), val| {
			min = std::cmp::min(min, Some(val));
			max = std::cmp::max(max, Some(val));
			(min, max)
		});
		// Return the maximum - minimum or NaN
		match min_max {
			(Some(min), Some(max)) => max - min,
			_ => Number::NAN,
		}
	}
}
