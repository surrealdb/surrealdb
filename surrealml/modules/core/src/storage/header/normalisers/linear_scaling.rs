//! The functionality and parameters around a linear scaling normaliser.
use super::traits::Normaliser;

/// A linear scaling normaliser.
///
/// # Fields
/// * `min` - The minimum value to scale to.
/// * `max` - The maximum value to scale to.
#[derive(Debug, PartialEq)]
pub struct LinearScaling {
    pub min: f32,
    pub max: f32,
}

impl Normaliser for LinearScaling {
    /// Normalises a value.
    ///
    /// # Arguments
    /// * `input` - The value to normalise.
    ///
    /// # Returns
    /// The normalised value.
    fn normalise(&self, input: f32) -> f32 {
        let range = self.max - self.min;
        (input - self.min) / range
    }

    /// Applies the inverse of the value for the normaliser.
    ///
    /// # Arguments
    /// * `input` - The value to inverse normalise.
    ///
    /// # Returns
    /// The inverse normalised value.
    fn inverse_normalise(&self, input: f32) -> f32 {
        let range = self.max - self.min;
        (input * range) + self.min
    }

    /// The key of the normaliser.
    ///
    /// # Returns
    /// The key of the normaliser.
    fn key() -> String {
        "linear_scaling".to_string()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_normalise_with_both_bounds() {
        let normaliser = LinearScaling {
            min: 0.0,
            max: 100.0,
        };
        let input = 50.0;
        let expected = 0.5;
        let actual = normaliser.normalise(input);
        assert_eq!(expected, actual);
    }
}
