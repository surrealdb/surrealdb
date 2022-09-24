use crate::sql::number::Number;
use std::collections::HashMap;

pub trait Mode {
	/// Most Frequent Number in dataset
	/// O(n*w) time complex s.t. w = distinct count
	fn mode(self) -> Number;
}

impl Mode for Vec<Number> {
	fn mode(self) -> Number {
		let frequencies = self.iter().fold(HashMap::new(), |mut freqs, value| {
			let entry = freqs.entry(value).or_insert(Number::from(0));

			*freqs.entry(value).or_insert(Number::from(0)) = Number::from(1) + entry.clone();
			freqs
		});

		let mode = frequencies
			.into_iter()
			.max_by_key(|(_, count)| count.clone())
			.map(|(value, _)| value.clone());

		mode.unwrap_or(Number::Float(f64::NAN))
	}
}
