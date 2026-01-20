//! The functionality and parameters around a z-score normaliser.
use super::traits::Normaliser;

/// A z-score normaliser.
///
/// # Fields
/// * `mean` - The mean of the normaliser.
/// * `std_dev` - The standard deviation of the normaliser.
#[derive(Debug, PartialEq)]
pub struct ZScore {
    pub mean: f32,
    pub std_dev: f32,
}

impl Normaliser for ZScore {
    /// Normalises a value.
    ///
    /// # Arguments
    /// * `input` - The value to normalise.
    ///
    /// # Returns
    /// The normalised value.
    fn normalise(&self, input: f32) -> f32 {
        (input - self.mean) / self.std_dev
    }

    /// Applies the inverse of the value for the normaliser.
    ///
    /// # Arguments
    /// * `input` - The value to inverse normalise.
    ///
    /// # Returns
    /// The inverse normalised value.
    fn inverse_normalise(&self, input: f32) -> f32 {
        (input * self.std_dev) + self.mean
    }

    fn key() -> String {
        "z_score".to_string()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_normalise_with_both_bounds() {
        let normaliser = ZScore {
            mean: 0.0,
            std_dev: 1.0,
        };
        let input = 0.0;
        let expected = 0.0;
        let actual = normaliser.normalise(input);
        assert_eq!(expected, actual);
    }
}
