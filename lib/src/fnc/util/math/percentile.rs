use crate::sql::number::{Number,Sorted};

pub trait Percentile {
	/// Gets the N percentile where N is an number (0-100)
	fn percentile(&self, _: Number) -> Number;
}

impl Percentile for Sorted<&Vec<Number>> {
	fn percentile(&self, perc: Number) -> Number {
		let n_percent_idx = (Number::from(self.0.len())*perc/Number::from(100)).round();
		self.0.get(n_percent_idx.to_usize()).unwrap_or(&Number::Float(f64::NAN)).clone()
	}
}
