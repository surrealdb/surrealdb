//! Regression analysis

use crate::cmd::bench::stats::{bivariate::Data, dot};

/// A straight line that passes through the origin `y = m * x`
#[derive(Clone, Copy)]
pub struct Slope(pub f64);

impl Slope {
	/// Fits the data to a straight line that passes through the origin using ordinary least
	/// squares
	///
	/// - Time: `O(length)`
	pub fn fit(data: &Data<'_>) -> Slope {
		let xs = data.0;
		let ys = data.1;

		let xy = dot(xs, ys);
		let x2 = dot(xs, xs);

		Slope(xy / x2)
	}

	/// Computes the goodness of fit (coefficient of determination) for this data set
	///
	/// - Time: `O(length)`
	pub fn r_squared(&self, data: &Data<'_>) -> f64 {
		let m = self.0;
		let xs = data.0;
		let ys = data.1;

		let n = xs.len() as f64;
		let y_bar = ys.iter().copied().sum::<f64>() / n;

		let mut ss_res = 0.;
		let mut ss_tot = 0.;

		for (&x, &y) in data.iter() {
			ss_res = ss_res + (y - m * x).powi(2);
			ss_tot = ss_tot + (y - y_bar).powi(2);
		}

		1.0 - ss_res / ss_tot
	}
}
