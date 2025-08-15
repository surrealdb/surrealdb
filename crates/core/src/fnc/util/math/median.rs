use crate::val::Number;
use crate::val::number::Sorted;

pub trait Median {
	fn median(self) -> f64;
}

impl Median for Sorted<&Vec<Number>> {
	fn median(self) -> f64 {
		if self.0.is_empty() {
			f64::NAN
		} else if self.0.len() % 2 == 1 {
			// return the middle: _ _ X _ _
			self.0[self.0.len() / 2].to_float()
		} else {
			// return the average of the middles: _ _ X Y _ _
			(self.0[self.0.len() / 2 - 1].to_float() + self.0[self.0.len() / 2].to_float()) / 2.0
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::number::Sort;

	#[test]
	fn test_median() {
		let tests: Vec<(Vec<Number>, f64)> = vec![
			(vec![], f64::NAN),
			(vec![1.into()], 1.0),
			(vec![1.into(), 2.into()], 1.5),
			(vec![1.into(), 2.into(), 3.into()], 2.0),
			(vec![1.into(), 2.into(), 3.into(), 4.into()], 2.5),
		];

		for (mut data, expected) in tests {
			let sorted = data.sorted();

			let actual = sorted.median();
			if f64::is_nan(expected) {
				assert!(f64::is_nan(actual));
			} else {
				assert_eq!(expected, actual);
			}
		}
	}
}
