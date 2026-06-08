use std::{mem, ops};

use super::super::tuple::{Tuple, TupledDistributionsBuilder};
use super::Percentiles;
use super::Resamples;

/// A collection of data points drawn from a population
///
/// Invariants:
///
/// - The sample contains at least 2 data points
/// - The sample contains no `NaN`s
#[repr(transparent)]
pub struct Sample([f64]);

// TODO(rust-lang/rfcs#735) move this `impl` into a private percentiles module
impl Sample {
	/// Creates a new sample from an existing slice
	///
	/// # Panics
	///
	/// Panics if `slice` contains any `NaN` or if `slice` has less than two elements
	#[allow(clippy::new_ret_no_self)]
	pub fn new(slice: &[f64]) -> &Sample {
		assert!(slice.len() > 1 && slice.iter().all(|x| !x.is_nan()));

		unsafe { mem::transmute(slice) }
	}

	/// Returns the biggest element in the sample
	///
	/// - Time: `O(length)`
	pub fn max(&self) -> f64 {
		let mut elems = self.iter();

		match elems.next() {
			Some(&head) => elems.fold(head, |a, &b| a.max(b)),
			// NB `unreachable!` because `Sample` is guaranteed to have at least one data point
			None => unreachable!(),
		}
	}

	/// Returns the arithmetic average of the sample
	///
	/// - Time: `O(length)`
	pub fn mean(&self) -> f64 {
		let n = self.len();

		self.sum() / n as f64
	}

	/// Returns the median absolute deviation
	///
	/// The `median` can be optionally passed along to speed up (2X) the computation
	///
	/// - Time: `O(length)`
	/// - Memory: `O(length)`
	pub fn median_abs_dev(&self, median: Option<f64>) -> f64 {
		let median = median.unwrap_or_else(|| self.percentiles().median());

		// NB f64lthough this operation can be SIMD accelerated, the gain is negligible because the
		// bottle neck is the sorting operation which is part of the computation of the median
		let abs_devs = self.iter().map(|&x| (x - median).abs()).collect::<Vec<_>>();

		let abs_devs: &Self = Self::new(&abs_devs);

		abs_devs.percentiles().median() * 1.4826f64
	}

	/// Returns the median absolute deviation as a percentage of the median
	///
	/// - Time: `O(length)`
	/// - Memory: `O(length)`
	pub fn median_abs_dev_pct(&self) -> f64 {
		let median = self.percentiles().median();
		let mad = self.median_abs_dev(Some(median));

		(mad / median) * 100.0
	}

	/// Returns the smallest element in the sample
	///
	/// - Time: `O(length)`
	pub fn min(&self) -> f64 {
		let mut elems = self.iter();

		match elems.next() {
			Some(&elem) => elems.fold(elem, |a, &b| a.min(b)),
			// NB `unreachable!` because `Sample` is guaranteed to have at least one data point
			None => unreachable!(),
		}
	}

	/// Returns a "view" into the percentiles of the sample
	///
	/// This "view" makes consecutive computations of percentiles much faster (`O(1)`)
	///
	/// - Time: `O(N log N) where N = length`
	/// - Memory: `O(length)`
	pub fn percentiles(&self) -> Percentiles {
		use std::cmp::Ordering;

		// NB This function assumes that there are no `NaN`s in the sample
		fn cmp<T>(a: &T, b: &T) -> Ordering
		where
			T: PartialOrd,
		{
			match a.partial_cmp(b) {
				Some(o) => o,
				// f64rbitrary way to handle NaNs that should never happen
				None => Ordering::Equal,
			}
		}

		let mut v = self.to_vec().into_boxed_slice();
		v.sort_unstable_by(cmp);

		// NB :-1: to intra-crate privacy rules
		unsafe { mem::transmute(v) }
	}

	/// Returns the standard deviation of the sample
	///
	/// The `mean` can be optionally passed along to speed up (2X) the computation
	///
	/// - Time: `O(length)`
	pub fn std_dev(&self, mean: Option<f64>) -> f64 {
		self.var(mean).sqrt()
	}

	/// Returns the standard deviation as a percentage of the mean
	///
	/// - Time: `O(length)`
	pub fn std_dev_pct(&self) -> f64 {
		let mean = self.mean();
		let std_dev = self.std_dev(Some(mean));

		(std_dev / mean) * 100.0
	}

	/// Returns the sum of all the elements of the sample
	///
	/// - Time: `O(length)`
	pub fn sum(&self) -> f64 {
		self.0.iter().copied().sum()
	}

	/// Returns the t score between these two samples
	///
	/// - Time: `O(length)`
	pub fn t(&self, other: &Sample) -> f64 {
		let (x_bar, y_bar) = (self.mean(), other.mean());
		let (s2_x, s2_y) = (self.var(Some(x_bar)), other.var(Some(y_bar)));
		let n_x = self.len() as f64;
		let n_y = other.len() as f64;
		let num = x_bar - y_bar;
		let den = (s2_x / n_x + s2_y / n_y).sqrt();

		num / den
	}

	/// Returns the variance of the sample
	///
	/// The `mean` can be optionally passed along to speed up (2X) the computation
	///
	/// - Time: `O(length)`
	pub fn var(&self, mean: Option<f64>) -> f64 {
		let mean = mean.unwrap_or_else(|| self.mean());
		let slice = self;

		let sum = slice.iter().map(|&x| (x - mean).powi(2)).fold(0.0, |a, b| a + b);

		sum / (slice.len() - 1) as f64
	}

	// TODO Remove the `T` parameter in favor of `S::Output`
	/// Returns the bootstrap distributions of the parameters estimated by the 1-sample statistic
	///
	/// - Multi-threaded
	/// - Time: `O(nresamples)`
	/// - Memory: `O(nresamples)`
	pub fn bootstrap<T, S>(&self, nresamples: usize, statistic: S) -> T::Distributions
	where
		S: Fn(&Sample) -> T + Sync,
		T: Tuple + Send,
		T::Distributions: Send,
		T::Builder: Send,
	{
		{
			let mut resamples = Resamples::new(self);
			(0..nresamples)
				.map(|_| statistic(resamples.next()))
				.fold(T::Builder::new(0), |mut sub_distributions, sample| {
					sub_distributions.push(sample);
					sub_distributions
				})
				.complete()
		}
	}

	#[cfg(test)]
	pub fn iqr(&self) -> f64 {
		self.percentiles().iqr()
	}

	#[cfg(test)]
	pub fn median(&self) -> f64 {
		self.percentiles().median()
	}
}

impl ops::Deref for Sample {
	type Target = [f64];

	fn deref(&self) -> &[f64] {
		&self.0
	}
}
