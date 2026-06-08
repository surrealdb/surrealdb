//! Univariate analysis

mod percentiles;
mod resamples;
mod sample;

pub mod mixed;
pub mod outliers;

use std::cmp;

use crate::cmd::bench::stats::tuple::{Tuple, TupledDistributionsBuilder};

use self::resamples::Resamples;

pub use self::percentiles::Percentiles;
pub use self::sample::Sample;

/// Performs a two-sample bootstrap
#[allow(clippy::cast_lossless)]
pub fn bootstrap<T, S>(a: &Sample, b: &Sample, nresamples: usize, statistic: S) -> T::Distributions
where
	S: Fn(&Sample, &Sample) -> T + Sync,
	T: Tuple + Send,
	T::Distributions: Send,
	T::Builder: Send,
{
	let nresamples_sqrt = (nresamples as f64).sqrt().ceil() as usize;
	let per_chunk = nresamples.div_ceil(nresamples_sqrt);

	let mut a_resamples = Resamples::new(a);
	let mut b_resamples = Resamples::new(b);
	(0..nresamples_sqrt)
		.map(|i| {
			let start = i * per_chunk;
			let end = cmp::min((i + 1) * per_chunk, nresamples);
			let a_resample = a_resamples.next();

			let mut sub_distributions: T::Builder = TupledDistributionsBuilder::new(end - start);

			for _ in start..end {
				let b_resample = b_resamples.next();
				sub_distributions.push(statistic(a_resample, b_resample));
			}
			sub_distributions
		})
		.fold(T::Builder::new(0), |mut a, mut b| {
			a.extend(&mut b);
			a
		})
		.complete()
}
