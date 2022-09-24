use crate::sql::number::{Number, Sorted};

pub trait Percentile {
	/// Gets the N percentile where N is an number (0-100) will average neighboring records if non-exact
	fn percentile(&self, _: Number) -> Number;
}

impl Percentile for Sorted<&Vec<Number>> {
	fn percentile(&self, perc: Number) -> Number {
		const NAN: Number = Number::Float(f64::NAN);

		if self.0.len() == 0 {
			return NAN;
		}
		if (perc <= Number::from(0)) | (perc > Number::from(100)) {
			return NAN;
		}

		let n_percent_idx = Number::from(self.0.len()) * perc / Number::from(100);

		if n_percent_idx.clone().as_float().fract().abs() < 1e-10 {
			let idx = n_percent_idx.as_int() as usize;
			self.0.get(idx - 1).unwrap_or(&NAN).clone()
		} else if n_percent_idx > Number::from(1) {
			let idx = n_percent_idx.as_int() as usize;
			(self.0.get(idx - 1).unwrap_or(&NAN) + self.0.get(idx).unwrap_or(&NAN))
				/ Number::from(2)
		} else {
			NAN
		}
	}
}
