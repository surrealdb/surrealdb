use crate::sql::number::{Number, Sorted};

pub trait Percentile {
	/// Gets the N percentile, averaging neighboring records if non-exact
	fn percentile(&self, perc: Number) -> Number;
}

impl Percentile for Sorted<&Vec<Number>> {
	fn percentile(&self, perc: Number) -> Number {
		// If an empty set, then return NaN
		if self.0.is_empty() {
			return Number::NAN;
		}
		// If an invalid percentile, then return NaN
		if (perc <= Number::from(0)) | (perc > Number::from(100)) {
			return Number::NAN;
		}
		// Get the index of the specified percentile
		let n_percent_idx = Number::from(self.0.len()) * perc / Number::from(100);
		// Calculate the N percentile for the index
		if n_percent_idx.to_float().fract().abs() < 1e-10 {
			let idx = n_percent_idx.as_usize();
			let val = self.0.get(idx - 1).unwrap_or(&Number::NAN).clone();
			val
		} else if n_percent_idx > Number::from(1) {
			let idx = n_percent_idx.as_usize();
			let val = self.0.get(idx - 1).unwrap_or(&Number::NAN);
			let val = val + self.0.get(idx).unwrap_or(&Number::NAN);
			val / Number::from(2)
		} else {
			Number::NAN
		}
	}
}
