use crate::sql::number::{Number, Sorted};

pub trait Nearestrank {
	/// Pull the closest extant record from the dataset at the %-th percentile
	fn nearestrank(self, perc: Number) -> Number;
}

impl Nearestrank for Sorted<&Vec<Number>> {
	fn nearestrank(self, perc: Number) -> Number {
		// If an empty set, then return NaN
		if self.0.is_empty() {
			return Number::NAN;
		}
		// If an invalid percentile, then return NaN
		if (perc <= Number::from(0)) | (perc > Number::from(100)) {
			return Number::NAN;
		}
		// If 100%, then get the last value in the set
		if perc == Number::from(100) {
			return self.0.get(self.0.len()).unwrap_or(&Number::NAN).clone();
		}
		// Get the index of the specified percentile
		let n_percent_idx = Number::from(self.0.len()) * perc / Number::from(100);
		// Return the closest extant record for the index
		match n_percent_idx.as_float().ceil() as usize {
			0 => self.0.get(0).unwrap_or(&Number::NAN).clone(),
			idx => self.0.get(idx - 1).unwrap_or(&Number::NAN).clone(),
		}
	}
}
