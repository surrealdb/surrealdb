//! Utility functions for normalisers to reduce code duplication in areas that cannot be easily defined in a struct.
use crate::errors::error::{SurrealError, SurrealErrorStatus};
use crate::{safe_eject_internal, safe_eject_option};
use regex::{Captures, Regex};

/// Extracts the label from a normaliser string.
///
/// # Arguments
/// * `data` - A string containing the normaliser data.
pub fn extract_label(data: &str) -> Result<String, SurrealError> {
    let re: Regex = safe_eject_internal!(Regex::new(r"^(.*?)\("));
    let captures: Captures = safe_eject_option!(re.captures(data));
    Ok(safe_eject_option!(captures.get(1)).as_str().to_string())
}

/// Extracts two numbers from a string with brackets where the numbers are in the brackets seperated by comma.
///
/// # Arguments
/// * `data` - A string containing the normaliser data.
///
/// # Returns
/// [number1, number2] from `"(number1, number2)"`
pub fn extract_two_numbers(data: &str) -> Result<[f32; 2], SurrealError> {
    let re: Regex = safe_eject_internal!(Regex::new(r"[-+]?\d+(\.\d+)?"));
    let mut numbers = re.find_iter(data);
    let mut buffer: [f32; 2] = [0.0, 0.0];

    let num_one_str = safe_eject_option!(numbers.next()).as_str();
    let num_two_str = safe_eject_option!(numbers.next()).as_str();

    buffer[0] = safe_eject_internal!(num_one_str.parse::<f32>());
    buffer[1] = safe_eject_internal!(num_two_str.parse::<f32>());
    Ok(buffer)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_extract_two_numbers() {
        let data = "linear_scaling(0.0,1.0)".to_string();
        let numbers = extract_two_numbers(&data).unwrap();
        assert_eq!(numbers[0], 0.0);
        assert_eq!(numbers[1], 1.0);

        let data = "linear_scaling(0,1)".to_string();
        let numbers = extract_two_numbers(&data).unwrap();
        assert_eq!(numbers[0], 0.0);
        assert_eq!(numbers[1], 1.0);
    }

    #[test]
    fn test_extract_label() {
        let data = "linear_scaling(0.0,1.0)".to_string();
        let label = extract_label(&data).unwrap();
        assert_eq!(label, "linear_scaling");
    }
}
