//! Defines the constructing and storing of normalisers.
use super::clipping;
use super::linear_scaling;
use super::log_scale;
use super::traits::Normaliser;
use super::utils::{extract_label, extract_two_numbers};
use super::z_score;
use std::fmt;

use crate::errors::error::{SurrealError, SurrealErrorStatus};
use crate::safe_eject_option;

/// A wrapper for all different types of normalisers.
///
/// # Arguments
/// * `LinearScaling` - A linear scaling normaliser.
/// * `Clipping` - A clipping normaliser.
/// * `LogScaling` - A log scaling normaliser.
/// * `ZScore` - A z-score normaliser.
#[derive(Debug, PartialEq)]
pub enum NormaliserType {
    LinearScaling(linear_scaling::LinearScaling),
    Clipping(clipping::Clipping),
    LogScaling(log_scale::LogScaling),
    ZScore(z_score::ZScore),
}

impl NormaliserType {
    /// Constructs a new normaliser.
    ///
    /// # Arguments
    /// * `label` - The label of the normaliser.
    /// * `one` - The first parameter of the normaliser.
    /// * `two` - The second parameter of the normaliser.
    ///
    /// # Returns
    /// A new normaliser.
    pub fn new(label: String, one: f32, two: f32) -> Self {
        match label.as_str() {
            "linear_scaling" => {
                NormaliserType::LinearScaling(linear_scaling::LinearScaling { min: one, max: two })
            }
            "clipping" => NormaliserType::Clipping(clipping::Clipping {
                min: Some(one),
                max: Some(two),
            }),
            "log_scaling" => NormaliserType::LogScaling(log_scale::LogScaling {
                base: one,
                min: two,
            }),
            "z_score" => NormaliserType::ZScore(z_score::ZScore {
                mean: one,
                std_dev: two,
            }),
            _ => panic!("Invalid normaliser label: {}", label),
        }
    }

    /// Unpacks a normaliser from a string.
    ///
    /// # Arguments
    /// * `normaliser_data` - A string containing the normaliser data.
    ///
    /// # Returns
    /// (type of normaliser, [normaliser parameters], column name)
    pub fn unpack_normaliser_data(
        normaliser_data: &str,
    ) -> Result<(String, [f32; 2], String), SurrealError> {
        let mut normaliser_buffer = normaliser_data.split("=>");

        let column_name = safe_eject_option!(normaliser_buffer.next());
        let normaliser_type = safe_eject_option!(normaliser_buffer.next()).to_string();

        let label = extract_label(&normaliser_type)?;
        let numbers = extract_two_numbers(&normaliser_type)?;
        Ok((label, numbers, column_name.to_string()))
    }

    /// Constructs a normaliser from a string.
    ///
    /// # Arguments
    /// * `data` - A string containing the normaliser data.
    ///
    /// # Returns
    /// (normaliser, column name)
    pub fn from_string(data: String) -> Result<(Self, String), SurrealError> {
        let (label, numbers, column_name) = Self::unpack_normaliser_data(&data)?;
        let normaliser = match label.as_str() {
            "linear_scaling" => {
                let min = numbers[0];
                let max = numbers[1];
                NormaliserType::LinearScaling(linear_scaling::LinearScaling { min, max })
            }
            "clipping" => {
                let min = numbers[0];
                let max = numbers[1];
                NormaliserType::Clipping(clipping::Clipping {
                    min: Some(min),
                    max: Some(max),
                })
            }
            "log_scaling" => {
                let base = numbers[0];
                let min = numbers[1];
                NormaliserType::LogScaling(log_scale::LogScaling { base, min })
            }
            "z_score" => {
                let mean = numbers[0];
                let std_dev = numbers[1];
                NormaliserType::ZScore(z_score::ZScore { mean, std_dev })
            }
            _ => {
                let error = SurrealError::new(
                    format!("Unknown normaliser type: {}", label).to_string(),
                    SurrealErrorStatus::Unknown,
                );
                return Err(error);
            }
        };
        Ok((normaliser, column_name))
    }

    /// Normalises a value.
    ///
    /// # Arguments
    /// * `value` - The value to normalise.
    ///
    /// # Returns
    /// The normalised value.
    pub fn normalise(&self, value: f32) -> f32 {
        match self {
            NormaliserType::LinearScaling(normaliser) => normaliser.normalise(value),
            NormaliserType::Clipping(normaliser) => normaliser.normalise(value),
            NormaliserType::LogScaling(normaliser) => normaliser.normalise(value),
            NormaliserType::ZScore(normaliser) => normaliser.normalise(value),
        }
    }

    /// Inverse normalises a value.
    ///
    /// # Arguments
    /// * `value` - The value to inverse normalise.
    ///
    /// # Returns
    /// The inverse normalised value.
    pub fn inverse_normalise(&self, value: f32) -> f32 {
        match self {
            NormaliserType::LinearScaling(normaliser) => normaliser.inverse_normalise(value),
            NormaliserType::Clipping(normaliser) => normaliser.inverse_normalise(value),
            NormaliserType::LogScaling(normaliser) => normaliser.inverse_normalise(value),
            NormaliserType::ZScore(normaliser) => normaliser.inverse_normalise(value),
        }
    }
}

impl fmt::Display for NormaliserType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NormaliserType::LinearScaling(normaliser) => {
                write!(f, "linear_scaling({},{})", normaliser.min, normaliser.max)
            }
            NormaliserType::Clipping(normaliser) => {
                let min = normaliser.min.unwrap_or_default();
                let max = normaliser.max.unwrap_or_default();
                write!(f, "clipping({},{})", min, max)
            }
            NormaliserType::LogScaling(normaliser) => {
                write!(f, "log_scaling({},{})", normaliser.base, normaliser.min)
            }
            NormaliserType::ZScore(normaliser) => {
                write!(f, "z_score({},{})", normaliser.mean, normaliser.std_dev)
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    pub fn generate_string() -> String {
        let normaliser =
            NormaliserType::LinearScaling(linear_scaling::LinearScaling { min: 0.0, max: 1.0 });
        let column_name = "column_name".to_string();
        format!("{}=>{}", column_name, normaliser)
    }

    #[test]
    fn test_normaliser_type_to_string() {
        let normaliser =
            NormaliserType::LinearScaling(linear_scaling::LinearScaling { min: 0.0, max: 1.0 });
        assert_eq!(normaliser.to_string(), "linear_scaling(0,1)");
    }

    #[test]
    fn test_normaliser_type_from_string() {
        let normaliser_string = generate_string();
        let (normaliser, column_name) = NormaliserType::from_string(normaliser_string).unwrap();
        assert_eq!(
            normaliser,
            NormaliserType::LinearScaling(linear_scaling::LinearScaling { min: 0.0, max: 1.0 })
        );
        assert_eq!(column_name, "column_name");
    }
}
