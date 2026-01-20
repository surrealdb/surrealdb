//! The functionality and parameters around a clipping normaliser.
use super::traits::Normaliser;

/// A clipping normaliser.
///
/// # Fields
/// * `min` - The minimum value to clip to.
/// * `max` - The maximum value to clip to.
#[derive(Debug, PartialEq)]
pub struct Clipping {
    pub min: Option<f32>,
    pub max: Option<f32>,
}

impl Normaliser for Clipping {
    /// Normalises a value.
    ///
    /// # Arguments
    /// * `input` - The value to normalise.
    ///
    /// # Returns
    /// The normalised value.
    fn normalise(&self, input: f32) -> f32 {
        match (self.min, self.max) {
            (Some(min), Some(max)) => {
                if input < min {
                    min
                } else if input > max {
                    max
                } else {
                    input
                }
            }
            (Some(min), None) => {
                if input < min {
                    min
                } else {
                    input
                }
            }
            (None, Some(max)) => {
                if input > max {
                    max
                } else {
                    input
                }
            }
            (None, None) => input,
        }
    }

    fn key() -> String {
        "clipping".to_string()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_normalise_with_both_bounds() {
        let normaliser = Clipping {
            min: Some(0.0),
            max: Some(1.0),
        };
        let input = 0.5;
        let expected = 0.5;
        let actual = normaliser.normalise(input);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_normalise_with_min_bound() {
        let normaliser = Clipping {
            min: Some(0.0),
            max: None,
        };
        let input = -0.5;
        let expected = 0.0;
        let actual = normaliser.normalise(input);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_normalise_with_max_bound() {
        let normaliser = Clipping {
            min: None,
            max: Some(1.0),
        };
        let input = 1.5;
        let expected = 1.0;
        let actual = normaliser.normalise(input);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_normalise_with_no_bounds() {
        let normaliser = Clipping {
            min: None,
            max: None,
        };
        let input = 0.5;
        let expected = 0.5;
        let actual = normaliser.normalise(input);
        assert_eq!(expected, actual);
    }
}
