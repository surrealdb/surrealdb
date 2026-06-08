use crate::cmd::bench::stats::{
	bivariate::Data,
	rand_util::{Rng, new_rng},
};

pub struct Resamples<'a> {
	rng: Rng,
	data: (&'a [f64], &'a [f64]),
	stage: Option<(Vec<f64>, Vec<f64>)>,
}

#[allow(clippy::should_implement_trait)]
impl<'a> Resamples<'a> {
	pub fn new(data: Data<'a>) -> Resamples<'a> {
		Resamples {
			rng: new_rng(),
			data: (data.x(), data.y()),
			stage: None,
		}
	}

	pub fn next(&mut self) -> Data<'_> {
		let n = self.data.0.len();

		match self.stage {
			None => {
				let mut stage = (Vec::with_capacity(n), Vec::with_capacity(n));

				for _ in 0..n {
					let i = self.rng.rand_range(0u64..(self.data.0.len() as u64)) as usize;

					stage.0.push(self.data.0[i]);
					stage.1.push(self.data.1[i]);
				}

				self.stage = Some(stage);
			}
			Some(ref mut stage) => {
				for i in 0..n {
					let j = self.rng.rand_range(0u64..(self.data.0.len() as u64)) as usize;

					stage.0[i] = self.data.0[j];
					stage.1[i] = self.data.1[j];
				}
			}
		}

		if let Some((ref x, ref y)) = self.stage {
			Data(x, y)
		} else {
			unreachable!();
		}
	}
}
