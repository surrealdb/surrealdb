//! Mixed bootstrap

use crate::cmd::bench::stats::{
	tuple::{Tuple, TupledDistributionsBuilder},
	univariate::{Sample, resamples::Resamples},
};

/// Performs a *mixed* two-sample bootstrap
pub fn bootstrap<T, S>(a: &Sample, b: &Sample, nresamples: usize, statistic: S) -> T::Distributions
where
	S: Fn(&Sample, &Sample) -> T + Sync,
	T: Tuple + Send,
	T::Distributions: Send,
	T::Builder: Send,
{
	let n_a = a.len();
	let n_b = b.len();
	let mut c = Vec::with_capacity(n_a + n_b);
	c.extend_from_slice(a);
	c.extend_from_slice(b);
	let c = Sample::new(&c);

	let mut resamples = Resamples::new(c);
	(0..nresamples)
		.map(|_| {
			let resample = resamples.next();
			let a: &Sample = Sample::new(&resample[..n_a]);
			let b: &Sample = Sample::new(&resample[n_a..]);

			statistic(a, b)
		})
		.fold(T::Builder::new(0), |mut sub_distributions, sample| {
			sub_distributions.push(sample);
			sub_distributions
		})
		.complete()
}
