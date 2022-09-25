use crate::sql::number::{Number, Sorted};

pub trait Nearestrank {
	/// Pull the closest extant record from the dataset at the %-th percentile
	fn nearestrank(self, perc: Number) -> Number;
}

impl Nearestrank for Sorted<&Vec<Number>> {
	fn nearestrank(self, perc: Number) -> Number {
		const NAN: Number = Number::Float(f64::NAN);

		if self.0.len() == 0 {
			return NAN;
		}
		if (perc <= Number::from(0)) | (perc > Number::from(100)) {
			return NAN;
		}

		if perc == Number::from(100) {
			return self.0.get(self.0.len()).unwrap_or(&NAN).clone();
		}

		let n_percent_idx =
			(Number::from(self.0.len()) * perc / Number::from(100)).as_float().ceil() as usize;

		match n_percent_idx {
			0 => self.0.get(0).unwrap_or(&NAN),
			idx => self.0.get(idx - 1).unwrap_or(&NAN),
		}
		.clone()
	}
}
