use std::mem;

use crate::cmd::bench::stats::{
	rand_util::{Rng, new_rng},
	univariate::Sample,
};

pub struct Resamples<'a> {
	rng: Rng,
	sample: &'a [f64],
	stage: Option<Vec<f64>>,
}

#[allow(clippy::should_implement_trait)]
impl<'a> Resamples<'a> {
	pub fn new(sample: &'a Sample) -> Resamples<'a> {
		let slice = sample;

		Resamples {
			rng: new_rng(),
			sample: slice,
			stage: None,
		}
	}

	pub fn next(&mut self) -> &Sample {
		let n = self.sample.len();
		let rng = &mut self.rng;

		match self.stage {
			None => {
				let mut stage = Vec::with_capacity(n);

				for _ in 0..n {
					let idx = rng.rand_range(0u64..(self.sample.len() as u64));
					stage.push(self.sample[idx as usize]);
				}

				self.stage = Some(stage);
			}
			Some(ref mut stage) => {
				for elem in stage.iter_mut() {
					let idx = rng.rand_range(0u64..(self.sample.len() as u64));
					*elem = self.sample[idx as usize];
				}
			}
		}

		if let Some(ref v) = self.stage {
			unsafe { mem::transmute::<&[f64], &Sample>(v) }
		} else {
			unreachable!();
		}
	}
}

#[cfg(test)]
mod test {
	use crate::cmd::bench::stats::univariate::{Sample, resamples::Resamples};

	#[test]
	fn different_subsets() {
		let size = 1000;
		let v: Vec<_> = (0..size).map(|i| i as f64).collect();
		let sample = Sample::new(&v);
		let mut resamples = Resamples::new(sample);

		// Hypothetically, we might see one duplicate, but more than one is likely to be a bug.
		let mut num_duplicated = 0;
		for _ in 0..1000 {
			let sample_1 = resamples.next().iter().cloned().collect::<Vec<_>>();
			let sample_2 = resamples.next().iter().cloned().collect::<Vec<_>>();

			if sample_1 == sample_2 {
				num_duplicated += 1;
			}
		}

		if num_duplicated > 1 {
			panic!("Found {} duplicate samples", num_duplicated);
		}
	}
}
