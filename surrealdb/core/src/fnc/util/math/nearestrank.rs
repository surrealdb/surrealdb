use crate::val::Number;
use crate::val::number::Sorted;

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
		let perc = perc.as_float();
		if !(0.0..=100.0).contains(&perc) {
			return Number::NAN;
		}
		let idx = self.0.len() as f64 * (perc * (1.0 / 100.0));
		self.0[(idx as usize).min(self.0.len() - 1)]
	}
}
