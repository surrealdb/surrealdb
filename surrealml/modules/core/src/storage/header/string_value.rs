//! Defines a generic string value for the header.
use std::fmt;

/// Defines a generic string value for the header.
///
/// # Fields
/// * `value` - The value of the string.
#[derive(Debug, PartialEq)]
pub struct StringValue {
    pub value: Option<String>,
}

impl StringValue {
    /// Creates a new string value with no value.
    ///
    /// # Returns
    /// A new string value with no value.
    pub fn fresh() -> Self {
        StringValue { value: None }
    }

    /// Creates a new string value from a string.
    ///
    /// # Arguments
    /// * `value` - The value of the string.
    ///
    /// # Returns
    /// A new string value.
    pub fn from_string(value: String) -> Self {
        match value.as_str() {
            "" => StringValue { value: None },
            _ => StringValue { value: Some(value) },
        }
    }
}

impl fmt::Display for StringValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.value {
            Some(val) => write!(f, "{}", val),
            None => write!(f, ""),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fresh() {
        let string_value = StringValue::fresh();
        assert_eq!(string_value, StringValue { value: None });
    }

    #[test]
    fn test_from_string() {
        let string_value = StringValue::from_string(String::from("test"));
        assert_eq!(
            string_value,
            StringValue {
                value: Some(String::from("test")),
            }
        );
    }

    #[test]
    fn test_from_string_none() {
        let string_value = StringValue::from_string(String::from(""));
        assert_eq!(string_value, StringValue { value: None });
    }

    #[test]
    fn test_to_string() {
        let string_value = StringValue::from_string(String::from("test"));
        assert_eq!(string_value.to_string(), String::from("test"));
    }

    #[test]
    fn test_to_string_none() {
        let string_value = StringValue { value: None };
        assert_eq!(string_value.to_string(), String::from(""));
    }
}
