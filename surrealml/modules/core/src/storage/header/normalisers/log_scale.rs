//! The functionality and parameters around a log scaling normaliser.
use super::traits::Normaliser;

/// A log scaling normaliser.
///
/// # Fields
/// * `base` - The base of the logarithm.
/// * `min` - The minimum value to scale to.
#[derive(Debug, PartialEq)]
pub struct LogScaling {
    pub base: f32,
    pub min: f32,
}

impl Normaliser for LogScaling {
    /// Normalises a value.
    ///
    /// # Arguments
    /// * `input` - The value to normalise.
    ///
    /// # Returns
    /// The normalised value.
    fn normalise(&self, input: f32) -> f32 {
        (input + self.min).log(self.base)
    }

    /// Applies the inverse of the value for the normaliser.
    ///
    /// # Arguments
    /// * `input` - The value to inverse normalise.
    ///
    /// # Returns
    /// The inverse normalised value.
    fn inverse_normalise(&self, input: f32) -> f32 {
        (input.powf(self.base)) - self.min
    }

    /// The key of the normaliser.
    ///
    /// # Returns
    /// The key of the normaliser.
    fn key() -> String {
        "log_scaling".to_string()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_normalise_with_both_bounds() {
        let normaliser = LogScaling {
            base: 10.0,
            min: 0.0,
        };
        let input = 10.0;
        let expected = 1.0;
        let actual = normaliser.normalise(input);
        assert_eq!(expected, actual);
    }
}
