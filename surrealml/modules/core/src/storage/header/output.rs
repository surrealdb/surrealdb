//! Defines the struct housing data around the outputs of the model.
use super::normalisers::wrapper::NormaliserType;
use crate::{
    errors::error::{SurrealError, SurrealErrorStatus},
    safe_eject_option,
};
use std::fmt;

/// Houses data around the outputs of the model.
///
/// # Fields
/// * `name` - The name of the output.
/// * `normaliser` - The normaliser to be applied to the output if there is one.
#[derive(Debug, PartialEq)]
pub struct Output {
    pub name: Option<String>,
    pub normaliser: Option<NormaliserType>,
}

impl Output {
    /// Creates a new instance of the Output struct with no normaliser or name.
    ///
    /// # Returns
    /// A new instance of the Output struct with no normaliser or name.
    pub fn fresh() -> Self {
        Output {
            name: None,
            normaliser: None,
        }
    }

    /// Creates a new instance of the Output struct without a normaliser.
    ///
    /// # Arguments
    /// * `name` - The name of the output.
    pub fn new(name: String) -> Self {
        Output {
            name: Some(name),
            normaliser: None,
        }
    }

    /// Adds a normaliser to the output.
    ///
    /// # Arguments
    /// * `normaliser` - The normaliser to be applied to the output.
    pub fn add_normaliser(&mut self, normaliser: NormaliserType) {
        self.normaliser = Some(normaliser);
    }

    /// Converts a string to an instance of the Output struct.
    ///
    /// # Arguments
    /// * `data` - The string to be converted into an instance of the Output struct.
    ///
    /// # Returns
    /// * `Output` - The string as an instance of the Output struct.
    pub fn from_string(data: String) -> Result<Self, SurrealError> {
        if !data.contains("=>") {
            return Ok(Output::fresh());
        }
        let mut buffer = data.split("=>");

        let name = safe_eject_option!(buffer.next());
        let name = match name {
            "none" => None,
            _ => Some(name.to_string()),
        };

        let normaliser = safe_eject_option!(buffer.next());
        let normaliser = match normaliser {
            "none" => None,
            _ => Some(NormaliserType::from_string(data).unwrap().0),
        };
        Ok(Output { name, normaliser })
    }
}

impl fmt::Display for Output {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.name.is_none() && self.normaliser.is_none() {
            return write!(f, "");
        }

        let name = self.name.as_deref().unwrap_or("none");
        let normaliser = self
            .normaliser
            .as_ref()
            .map(|n| n.to_string())
            .unwrap_or_else(|| "none".to_string());

        write!(f, "{}=>{}", name, normaliser)
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;

    #[test]
    fn test_output_to_string() {
        // with no normaliser
        let mut output = Output::new("test".to_string());
        assert_eq!(output.to_string(), "test=>none");

        let normaliser_data = "a=>linear_scaling(0.0,1.0)".to_string();
        let normaliser = NormaliserType::from_string(normaliser_data).unwrap();

        output.add_normaliser(normaliser.0);
        assert_eq!(output.to_string(), "test=>linear_scaling(0,1)");
    }

    #[test]
    fn test_from_string() {
        let data = "test=>linear_scaling(0,1)".to_string();
        let output = Output::from_string(data).unwrap();

        assert_eq!(output.name.unwrap(), "test");
        assert_eq!(
            output.normaliser.unwrap().to_string(),
            "linear_scaling(0,1)"
        );
    }

    #[test]
    fn test_from_string_with_no_normaliser() {
        let data = "test=>none".to_string();
        let output = Output::from_string(data).unwrap();

        assert_eq!(output.name.unwrap(), "test");
        assert!(output.normaliser.is_none());
    }

    #[test]
    fn test_from_string_with_no_name() {
        let data = "none=>none".to_string();
        let output = Output::from_string(data).unwrap();

        assert!(output.name.is_none());
        assert!(output.normaliser.is_none());
    }

    #[test]
    fn test_from_string_with_empty_string() {
        let data = "".to_string();
        let output = Output::from_string(data).unwrap();

        assert!(output.name.is_none());
        assert!(output.normaliser.is_none());
    }

    #[test]
    fn test_to_string_with_no_data() {
        let output = Output::fresh();
        assert_eq!(output.to_string(), "");
    }
}
