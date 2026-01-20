//! InputDims is a struct that holds the dimensions of the input tensors for the model.
use std::fmt;

/// InputDims is a struct that holds the dimensions of the input tensors for the model.
///
/// # Fields
/// * `dims` - The dimensions of the input tensors.
#[derive(Debug, PartialEq)]
pub struct InputDims {
    pub dims: [i32; 2],
}

impl InputDims {
    /// Creates a new `InputDims` struct with all zeros.
    ///
    /// # Returns
    /// A new `InputDims` struct with all zeros.
    pub fn fresh() -> Self {
        InputDims { dims: [0, 0] }
    }

    /// Creates a new `InputDims` struct from a string.
    ///
    /// # Arguments
    /// * `data` - The dimensions as a string.
    ///
    /// # Returns
    /// A new `InputDims` struct.
    pub fn from_string(data: String) -> InputDims {
        if data == *"" {
            return InputDims::fresh();
        }
        let dims: Vec<&str> = data.split(",").collect();
        let dims: Vec<i32> = dims.iter().map(|x| x.parse::<i32>().unwrap()).collect();
        InputDims {
            dims: [dims[0], dims[1]],
        }
    }
}

impl fmt::Display for InputDims {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.dims == [0, 0] {
            write!(f, "")
        } else {
            write!(f, "{},{}", self.dims[0], self.dims[1])
        }
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;

    #[test]
    fn test_fresh() {
        let input_dims = InputDims::fresh();
        assert_eq!(input_dims.dims[0], 0);
        assert_eq!(input_dims.dims[1], 0);
    }

    #[test]
    fn test_from_string() {
        let input_dims = InputDims::from_string("1,2".to_string());
        assert_eq!(input_dims.dims[0], 1);
        assert_eq!(input_dims.dims[1], 2);
    }

    #[test]
    fn test_to_string() {
        let input_dims = InputDims::from_string("1,2".to_string());
        assert_eq!(input_dims.to_string(), "1,2".to_string());
    }
}
