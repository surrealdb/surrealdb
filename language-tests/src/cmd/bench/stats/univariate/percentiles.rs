/// A "view" into the percentiles of a sample
pub struct Percentiles(Box<[f64]>);

// TODO(rust-lang/rfcs#735) move this `impl` into a private percentiles module
impl Percentiles {
	/// Returns the percentile at `p`%
	///
	/// Safety:
	///
	/// - Make sure that `p` is in the range `[0, 100]`
	unsafe fn at_unchecked(&self, p: f64) -> f64 {
		debug_assert!((0.0..=100.0).contains(&p));
		debug_assert!(!self.0.is_empty());
		let len = self.0.len() - 1;

		if p == 100.0 {
			self.0[len]
		} else {
			let rank = (p / 100.0) * len as f64;
			let integer = rank.floor();
			let fraction = rank - integer;
			let n = integer as usize;
			unsafe {
				let &floor = self.0.get_unchecked(n);
				let &ceiling = self.0.get_unchecked(n + 1);

				floor + (ceiling - floor) * fraction
			}
		}
	}

	/// Returns the percentile at `p`%
	///
	/// # Panics
	///
	/// Panics if `p` is outside the closed `[0, 100]` range
	pub fn at(&self, p: f64) -> f64 {
		assert!((0.0..=100.0).contains(&p));
		assert!(!self.0.is_empty());

		unsafe { self.at_unchecked(p) }
	}

	/// Returns the interquartile range
	pub fn iqr(&self) -> f64 {
		let q1 = self.at(25.0);
		let q3 = self.at(75.0);

		q3 - q1
	}

	/// Returns the 50th percentile
	pub fn median(&self) -> f64 {
		self.at(50.0)
	}

	/// Returns the 25th, 50th and 75th percentiles
	pub fn quartiles(&self) -> (f64, f64, f64) {
		(self.at(25.0), self.at(50.0), self.at(75.0))
	}
}
