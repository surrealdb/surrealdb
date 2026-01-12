use std::collections::BTreeMap;

use crate::val::Number;

pub trait Mode {
	/// Find the most frequent number in collection
	/// O(n*w) time complex s.t. w = distinct count
	fn mode(self) -> Number;
}

impl Mode for Vec<Number> {
	fn mode(self) -> Number {
		// Iterate over all numbers, and get their frequency
		let frequencies = self.into_iter().fold(BTreeMap::new(), |mut freqs, value| {
			let entry = freqs.entry(value).or_insert_with(|| 0u32);
			*entry += 1;
			freqs
		});
		// Get the maximum number by frequency
		frequencies.into_iter().max_by_key(|(_, n)| *n).map(|(v, _)| v).unwrap_or(Number::NAN)
	}
}
