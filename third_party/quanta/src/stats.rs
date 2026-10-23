/// Estimates the arithmetic mean (and the error) for a set of samples.
///
/// This type is written and maintained internally as it is trivial to implement and doesn't warrant
/// a separate dependency.  As well, we add some features like exposing the sample count,
/// calculating the mean + error value, etc, that existing crates don't do.
///
/// Based on [Welford's algorithm][welfords] which computes the mean incrementally, with constant
/// time and space complexity.
///
/// [welfords]: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford%27s_online_algorithm
#[derive(Default)]
pub(crate) struct Variance {
    mean: f64,
    mean2: f64,
    n: u64,
}

impl Variance {
    #[inline]
    pub fn add(&mut self, sample: f64) {
        self.n += 1;
        let n_f = self.n as f64;
        let delta_sq = (sample - self.mean).powi(2);
        self.mean2 += ((n_f - 1.0) * delta_sq) / n_f;
        self.mean += (sample - self.mean) / n_f;
    }

    #[inline]
    pub fn mean(&self) -> f64 {
        self.mean
    }

    #[inline]
    pub fn mean_error(&self) -> f64 {
        if self.n < 2 {
            return 0.0;
        }

        let n_f = self.n as f64;
        let sd = (self.mean2 / (n_f - 1.0)).sqrt();
        sd / n_f.sqrt()
    }

    #[inline]
    pub fn mean_with_error(&self) -> f64 {
        let mean = self.mean.abs();
        mean + self.mean_error().abs()
    }

    #[inline]
    pub fn has_significant_result(&self) -> bool {
        self.n >= 2
    }

    #[inline]
    pub fn samples(&self) -> u64 {
        self.n
    }
}

#[cfg(test)]
mod tests {
    use super::Variance;

    #[test]
    fn basic() {
        let inputs = &[5.0, 10.0, 12.0, 15.0, 20.0];
        let mut variance = Variance::default();
        for input in inputs {
            variance.add(*input);
        }

        assert_eq!(variance.mean(), 12.4);

        let expected_mean_error = 2.5019;
        assert!((variance.mean_error() - expected_mean_error).abs() < 0.001);
    }
}
