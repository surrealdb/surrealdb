//! InputDims is a struct that holds the dimensions of the input tensors for the model.
use std::fmt;

use crate::errors::error::{SurrealError, SurrealErrorStatus};
use crate::safe_eject;

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
		InputDims {
			dims: [0, 0],
		}
	}

	/// Creates a new `InputDims` struct from a string.
	///
	/// # Arguments
	/// * `data` - The dimensions as a string.
	///
	/// # Returns
	/// A new `InputDims` struct, or a `SurrealError` if the dimensions are malformed.
	pub fn from_string(data: String) -> Result<InputDims, SurrealError> {
		if data == *"" {
			return Ok(InputDims::fresh());
		}
		let parts: Vec<&str> = data.split(",").collect();
		// Reject input that does not contain exactly two dimensions so that the indexing
		// below cannot panic on attacker-controlled header data.
		if parts.len() != 2 {
			return Err(SurrealError::new(
				format!(
					"invalid input dimensions '{}': expected 2 comma-separated values, found {}",
					data,
					parts.len()
				),
				SurrealErrorStatus::BadRequest,
			));
		}
		Ok(InputDims {
			dims: [
				safe_eject!(parts[0].parse::<i32>(), SurrealErrorStatus::BadRequest),
				safe_eject!(parts[1].parse::<i32>(), SurrealErrorStatus::BadRequest),
			],
		})
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
		let input_dims = InputDims::from_string("1,2".to_string()).unwrap();
		assert_eq!(input_dims.dims[0], 1);
		assert_eq!(input_dims.dims[1], 2);
	}

	#[test]
	fn test_to_string() {
		let input_dims = InputDims::from_string("1,2".to_string()).unwrap();
		assert_eq!(input_dims.to_string(), "1,2".to_string());
	}

	#[test]
	fn test_from_string_empty_is_fresh() {
		let input_dims = InputDims::from_string("".to_string()).unwrap();
		assert_eq!(input_dims, InputDims::fresh());
	}

	// Regression tests for GHSA-jwr6-6444-28xv: malformed dimensions in a `.surml`
	// header must surface a structured error instead of panicking (the release
	// profile uses `panic = 'abort'`, so a panic here would crash the server).
	#[test]
	fn test_from_string_non_numeric_errors() {
		// The exact malformed value from the advisory proof-of-concept.
		let err = InputDims::from_string("bad".to_string()).unwrap_err();
		assert_eq!(err.status, SurrealErrorStatus::BadRequest);
	}

	#[test]
	fn test_from_string_too_few_dims_errors() {
		let err = InputDims::from_string("1".to_string()).unwrap_err();
		assert_eq!(err.status, SurrealErrorStatus::BadRequest);
	}

	#[test]
	fn test_from_string_too_many_dims_errors() {
		let err = InputDims::from_string("1,2,3".to_string()).unwrap_err();
		assert_eq!(err.status, SurrealErrorStatus::BadRequest);
	}
}
