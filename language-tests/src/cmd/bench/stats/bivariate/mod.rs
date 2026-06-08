//! Bivariate analysis

use crate::cmd::bench::stats::{
	bivariate::resamples::Resamples,
	tuple::{Tuple, TupledDistributionsBuilder},
	univariate::Sample,
};

pub mod regression;
mod resamples;

/// Bivariate `(X, Y)` data
///
/// Invariants:
///
/// - No `NaN`s in the data
/// - At least two data points in the set
#[derive(Clone, Copy)]
pub struct Data<'a>(&'a [f64], &'a [f64]);

impl<'a> Data<'a> {
	/// Returns the length of the data set
	pub fn len(&self) -> usize {
		self.0.len()
	}

	/// Iterate over the data set
	pub fn iter(&self) -> Pairs<'a> {
		Pairs {
			data: *self,
			state: 0,
		}
	}
}

impl<'a> Data<'a> {
	/// Creates a new data set from two existing slices
	pub fn new(xs: &'a [f64], ys: &'a [f64]) -> Data<'a> {
		assert!(
			xs.len() == ys.len()
				&& xs.len() > 1
				&& xs.iter().all(|x| !x.is_nan())
				&& ys.iter().all(|y| !y.is_nan())
		);

		Data(xs, ys)
	}

	/// Returns the bootstrap distributions of the parameters estimated by the `statistic`
	pub fn bootstrap<T, S>(&self, nresamples: usize, statistic: S) -> T::Distributions
	where
		S: Fn(Data) -> T + Sync,
		T: Tuple + Send,
		T::Distributions: Send,
		T::Builder: Send,
	{
		let mut resamples = Resamples::new(*self);
		(0..nresamples)
			.map(|_| statistic(resamples.next()))
			.fold(T::Builder::new(0), |mut sub_distributions, sample| {
				sub_distributions.push(sample);
				sub_distributions
			})
			.complete()
	}

	/// Returns a view into the `X` data
	pub fn x(&self) -> &'a Sample {
		Sample::new(self.0)
	}

	/// Returns a view into the `Y` data
	pub fn y(&self) -> &'a Sample {
		Sample::new(self.1)
	}
}

/// Iterator over `Data`
pub struct Pairs<'a> {
	data: Data<'a>,
	state: usize,
}

impl<'a> Iterator for Pairs<'a> {
	type Item = (&'a f64, &'a f64);

	fn next(&mut self) -> Option<(&'a f64, &'a f64)> {
		if self.state < self.data.len() {
			let i = self.state;
			self.state += 1;

			// This is safe because i will always be < self.data.{0,1}.len()
			debug_assert!(i < self.data.0.len());
			debug_assert!(i < self.data.1.len());
			unsafe { Some((self.data.0.get_unchecked(i), self.data.1.get_unchecked(i))) }
		} else {
			None
		}
	}
}
