//! Defines the operations around performing computations on a loaded model.
use std::collections::HashMap;

use ndarray::ArrayD;
use ort::session::Session;
use ort::value::ValueType;

use crate::errors::error::{SurrealError, SurrealErrorStatus};
use crate::execution::session::get_session;
use crate::safe_eject;
use crate::storage::surml_file::SurMlFile;

/// A wrapper for the loaded machine learning model so we can perform computations on the loaded
/// model.
///
/// # Attributes
/// * `surml_file` - The loaded machine learning model using interior mutability to allow mutable
///   access to the model
pub struct ModelComputation<'a> {
	pub surml_file: &'a mut SurMlFile,
}

impl ModelComputation<'_> {
	/// Creates a Tensor that can be used as input to the loaded model from a hashmap of keys and
	/// values.
	///
	/// # Arguments
	/// * `input_values` - A hashmap of keys and values that will be used to create the input
	///   tensor.
	///
	/// # Returns
	/// A Tensor that can be used as input to the loaded model.
	pub fn input_tensor_from_key_bindings(
		&self,
		input_values: HashMap<String, f32>,
	) -> Result<ArrayD<f32>, SurrealError> {
		let buffer = self.input_vector_from_key_bindings(input_values)?;
		Ok(ndarray::arr1::<f32>(&buffer).into_dyn())
	}

	/// Creates a vector of dimensions for the input tensor from the loaded model.
	///
	/// # Arguments
	/// * `session_ref` - A reference to the session to get the input shape
	///
	/// # Returns
	/// A vector of dimensions for the input tensor to be reshaped into from the loaded model.
	fn process_input_dims(session_ref: &Session) -> Result<Vec<usize>, SurrealError> {
		// In ort 2.0.0-rc.11, we access input metadata through session.inputs()
		let inputs = session_ref.inputs();
		if inputs.is_empty() {
			return Err(SurrealError {
				message: "No inputs found in session".into(),
				status: SurrealErrorStatus::Unknown,
			});
		}

		// Get the first input's dtype
		let dtype = inputs[0].dtype();

		// Extract dimensions from the ValueType
		let unwrapped_dims = match dtype {
			ValueType::Tensor {
				ty: _,
				shape,
				dimension_symbols: _,
			} => shape,
			_ => {
				return Err(SurrealError {
					message: "input dims not found".into(),
					status: SurrealErrorStatus::Unknown,
				});
			}
		};

		let mut dims_cache = Vec::new();
		for dim in unwrapped_dims.iter() {
			if dim < &0 {
				dims_cache.push((dim * -1) as usize);
			} else {
				dims_cache.push(*dim as usize);
			}
		}
		Ok(dims_cache)
	}

	/// Creates a Vector that can be used manipulated with other operations such as normalisation
	/// from a hashmap of keys and values.
	///
	/// # Arguments
	/// * `input_values` - A hashmap of keys and values that will be used to create the input
	///   vector.
	///
	/// # Returns
	/// A Vector that can be used manipulated with other operations such as normalisation.
	pub fn input_vector_from_key_bindings(
		&self,
		mut input_values: HashMap<String, f32>,
	) -> Result<Vec<f32>, SurrealError> {
		let mut buffer = Vec::with_capacity(self.surml_file.header.keys.store.len());

		for key in &self.surml_file.header.keys.store {
			let value = match input_values.get_mut(key) {
				Some(value) => value,
				None => {
					return Err(SurrealError::new(
						format!(
							"src/execution/compute.rs 67: Key {} not found in input values",
							key
						),
						SurrealErrorStatus::NotFound,
					));
				}
			};
			buffer.push(std::mem::take(value));
		}

		Ok(buffer)
	}

	/// Performs a raw computation on the loaded model.
	///
	/// # Arguments
	/// * `tensor` - The input tensor to the loaded model.
	///
	/// # Returns
	/// The computed output tensor from the loaded model.
	pub fn raw_compute(
		&self,
		tensor: ArrayD<f32>,
		_dims: Option<(i32, i32)>,
	) -> Result<Vec<f32>, SurrealError> {
		let mut session = get_session(self.surml_file.model.clone())?;
		let dims_cache = ModelComputation::process_input_dims(&session)?;
		let tensor = if dims_cache.is_empty() {
			// If we couldn't get dimensions from the session, use the tensor as-is
			tensor
		} else {
			match tensor.into_shape_with_order(dims_cache) {
				Ok(tensor) => tensor,
				Err(_) => {
					return Err(SurrealError::new(
						"Failed to reshape tensor to input dimensions".to_string(),
						SurrealErrorStatus::Unknown,
					));
				}
			}
		};
		let tensor = match ort::value::Tensor::from_array(tensor) {
			Ok(tensor) => tensor,
			Err(_) => {
				return Err(SurrealError::new(
					"Failed to convert tensor to ort tensor".to_string(),
					SurrealErrorStatus::Unknown,
				));
			}
		};
		let x = ort::inputs![tensor];
		let outputs = safe_eject!(session.run(x), SurrealErrorStatus::Unknown);

		let mut buffer: Vec<f32> = Vec::new();

		// extract the output tensor converting the values to f32 if they are i64
		match outputs[0].try_extract_tensor::<f32>() {
			Ok((_shape, data)) => {
				for i in data.iter() {
					buffer.push(*i);
				}
			}
			Err(_) => {
				let (_shape, data) = safe_eject!(
					outputs[0].try_extract_tensor::<i64>(),
					SurrealErrorStatus::Unknown
				);
				for i in data.iter() {
					buffer.push(*i as f32);
				}
			}
		};
		Ok(buffer)
	}

	/// Checks the header applying normalisers if present and then performs a raw computation on the
	/// loaded model. Will also apply inverse normalisers if present on the outputs.
	///
	/// # Notes
	/// This function is fairly coupled and will consider breaking out the functions later on if
	/// needed.
	///
	/// # Arguments
	/// * `input_values` - A hashmap of keys and values that will be used to create the input
	///   tensor.
	///
	/// # Returns
	/// The computed output tensor from the loaded model.
	pub fn buffered_compute(
		&self,
		input_values: &mut HashMap<String, f32>,
	) -> Result<Vec<f32>, SurrealError> {
		// applying normalisers if present
		for (key, value) in &mut *input_values {
			let value_ref = *value;
			if let Some(normaliser) = self.surml_file.header.get_normaliser(&key.to_string())? {
				*value = normaliser.normalise(value_ref);
			}
		}
		let tensor = self.input_tensor_from_key_bindings(input_values.clone())?;
		let output = self.raw_compute(tensor, None)?;

		// if no normaliser is present, return the output
		if self.surml_file.header.output.normaliser.is_none() {
			return Ok(output);
		}

		// apply the normaliser to the output
		let output_normaliser = match self.surml_file.header.output.normaliser.as_ref() {
			Some(normaliser) => normaliser,
			None => {
				return Err(SurrealError::new(
					String::from(
						"No normaliser present for output which shouldn't happen as passed initial check for",
					)
					.to_string(),
					SurrealErrorStatus::Unknown,
				));
			}
		};
		let mut buffer = Vec::with_capacity(output.len());

		for value in output {
			buffer.push(output_normaliser.inverse_normalise(value));
		}
		Ok(buffer)
	}
}

#[cfg(test)]
mod tests {

	/// A model of one `MatMul` node, `y = x · w`, taking `x` of shape `[1, 3]` and returning `y`
	/// of shape `[1, 3]`, encoded as an ONNX `ModelProto`.
	///
	/// Written out by hand so a test can run a model with several outputs without a training
	/// framework or a stored model file.
	fn multi_output_onnx(w: [f32; 9]) -> Vec<u8> {
		fn varint(mut v: u64, out: &mut Vec<u8>) {
			while v >= 0x80 {
				out.push((v as u8) | 0x80);
				v >>= 7;
			}
			out.push(v as u8);
		}
		fn int(field: u64, v: u64, out: &mut Vec<u8>) {
			varint(field << 3, out);
			varint(v, out);
		}
		fn bytes(field: u64, v: &[u8], out: &mut Vec<u8>) {
			varint((field << 3) | 2, out);
			varint(v.len() as u64, out);
			out.extend_from_slice(v);
		}
		// A float tensor value of shape `[1, 3]`.
		fn value_info(name: &str) -> Vec<u8> {
			let mut shape = Vec::new();
			for dim in [1, 3] {
				let mut d = Vec::new();
				int(1, dim, &mut d);
				bytes(1, &d, &mut shape);
			}
			let mut tensor = Vec::new();
			int(1, 1, &mut tensor);
			bytes(2, &shape, &mut tensor);
			let mut ty = Vec::new();
			bytes(1, &tensor, &mut ty);
			let mut info = Vec::new();
			bytes(1, name.as_bytes(), &mut info);
			bytes(2, &ty, &mut info);
			info
		}
		let mut node = Vec::new();
		bytes(1, b"x", &mut node);
		bytes(1, b"w", &mut node);
		bytes(2, b"y", &mut node);
		bytes(4, b"MatMul", &mut node);
		let mut weights = Vec::new();
		int(1, 3, &mut weights);
		int(1, 3, &mut weights);
		int(2, 1, &mut weights);
		bytes(8, b"w", &mut weights);
		let raw: Vec<u8> = w.iter().flat_map(|v| v.to_le_bytes()).collect();
		bytes(9, &raw, &mut weights);
		let mut graph = Vec::new();
		bytes(1, &node, &mut graph);
		bytes(2, b"multi_output", &mut graph);
		bytes(5, &weights, &mut graph);
		bytes(11, &value_info("x"), &mut graph);
		bytes(12, &value_info("y"), &mut graph);
		let mut opset = Vec::new();
		int(2, 13, &mut opset);
		let mut model = Vec::new();
		int(1, 8, &mut model);
		bytes(7, &graph, &mut model);
		bytes(8, &opset, &mut model);
		model
	}

	#[test]
	fn raw_compute_returns_every_output() {
		use super::*;
		let model = multi_output_onnx([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]);
		let mut file = SurMlFile::fresh(model);
		let computation = ModelComputation {
			surml_file: &mut file,
		};
		let input = ndarray::arr1::<f32>(&[1.0, 1.0, 1.0]).into_dyn();
		let output = computation.raw_compute(input, None).unwrap();
		assert_eq!(output, [12.0, 15.0, 18.0]);
	}

	#[cfg(any(
		feature = "sklearn-tests",
		feature = "onnx-tests",
		feature = "torch-tests",
		feature = "tensorflow-tests"
	))]
	use super::*;
	#[cfg(any(
		feature = "sklearn-tests",
		feature = "onnx-tests",
		feature = "torch-tests",
		feature = "tensorflow-tests"
	))]
	#[cfg(feature = "sklearn-tests")]
	#[test]
	fn test_raw_compute_linear_sklearn() {
		let mut file = SurMlFile::from_file("./model_stash/sklearn/surml/linear.surml").unwrap();
		let model_computation = ModelComputation {
			surml_file: &mut file,
		};

		let mut input_values = HashMap::new();
		input_values.insert(String::from("squarefoot"), 1000.0);
		input_values.insert(String::from("num_floors"), 2.0);

		let raw_input = model_computation.input_tensor_from_key_bindings(input_values).unwrap();

		let output = model_computation.raw_compute(raw_input, Some((1, 2))).unwrap();
		assert_eq!(output.len(), 1);
		assert_eq!(output[0], 985.57745);
	}

	#[cfg(feature = "sklearn-tests")]
	#[test]
	fn test_buffered_compute_linear_sklearn() {
		let mut file = SurMlFile::from_file("./model_stash/sklearn/surml/linear.surml").unwrap();
		let model_computation = ModelComputation {
			surml_file: &mut file,
		};

		let mut input_values = HashMap::new();
		input_values.insert(String::from("squarefoot"), 1000.0);
		input_values.insert(String::from("num_floors"), 2.0);

		let output = model_computation.buffered_compute(&mut input_values).unwrap();
		assert_eq!(output.len(), 1);
	}

	#[cfg(feature = "onnx-tests")]
	#[test]
	fn test_raw_compute_linear_onnx() {
		let mut file = SurMlFile::from_file("./model_stash/onnx/surml/linear.surml").unwrap();
		let model_computation = ModelComputation {
			surml_file: &mut file,
		};

		let mut input_values = HashMap::new();
		input_values.insert(String::from("squarefoot"), 1000.0);
		input_values.insert(String::from("num_floors"), 2.0);

		let raw_input = model_computation.input_tensor_from_key_bindings(input_values).unwrap();

		let output = model_computation.raw_compute(raw_input, Some((1, 2))).unwrap();
		assert_eq!(output.len(), 1);
		assert_eq!(output[0], 985.57745);
	}

	#[cfg(feature = "onnx-tests")]
	#[test]
	fn test_buffered_compute_linear_onnx() {
		let mut file = SurMlFile::from_file("./model_stash/onnx/surml/linear.surml").unwrap();
		let model_computation = ModelComputation {
			surml_file: &mut file,
		};

		let mut input_values = HashMap::new();
		input_values.insert(String::from("squarefoot"), 1000.0);
		input_values.insert(String::from("num_floors"), 2.0);

		let output = model_computation.buffered_compute(&mut input_values).unwrap();
		assert_eq!(output.len(), 1);
	}

	#[cfg(feature = "torch-tests")]
	#[test]
	fn test_raw_compute_linear_torch() {
		let mut file = SurMlFile::from_file("./model_stash/torch/surml/linear.surml").unwrap();
		let model_computation = ModelComputation {
			surml_file: &mut file,
		};

		let mut input_values = HashMap::new();
		input_values.insert(String::from("squarefoot"), 1000.0);
		input_values.insert(String::from("num_floors"), 2.0);

		let raw_input = model_computation.input_tensor_from_key_bindings(input_values).unwrap();

		let output = model_computation.raw_compute(raw_input, None).unwrap();
		assert_eq!(output.len(), 1);
	}

	#[cfg(feature = "torch-tests")]
	#[test]
	fn test_buffered_compute_linear_torch() {
		let mut file = SurMlFile::from_file("./model_stash/torch/surml/linear.surml").unwrap();
		let model_computation = ModelComputation {
			surml_file: &mut file,
		};

		let mut input_values = HashMap::new();
		input_values.insert(String::from("squarefoot"), 1000.0);
		input_values.insert(String::from("num_floors"), 2.0);

		let output = model_computation.buffered_compute(&mut input_values).unwrap();
		assert_eq!(output.len(), 1);
	}

	#[cfg(feature = "tensorflow-tests")]
	#[test]
	fn test_raw_compute_linear_tensorflow() {
		let mut file = SurMlFile::from_file("./model_stash/tensorflow/surml/linear.surml").unwrap();
		let model_computation = ModelComputation {
			surml_file: &mut file,
		};

		let mut input_values = HashMap::new();
		input_values.insert(String::from("squarefoot"), 1000.0);
		input_values.insert(String::from("num_floors"), 2.0);

		let raw_input = model_computation.input_tensor_from_key_bindings(input_values).unwrap();

		let output = model_computation.raw_compute(raw_input, None).unwrap();
		assert_eq!(output.len(), 1);
	}

	#[cfg(feature = "tensorflow-tests")]
	#[test]
	fn test_buffered_compute_linear_tensorflow() {
		let mut file = SurMlFile::from_file("./model_stash/tensorflow/surml/linear.surml").unwrap();
		let model_computation = ModelComputation {
			surml_file: &mut file,
		};

		let mut input_values = HashMap::new();
		input_values.insert(String::from("squarefoot"), 1000.0);
		input_values.insert(String::from("num_floors"), 2.0);

		let output = model_computation.buffered_compute(&mut input_values).unwrap();
		assert_eq!(output.len(), 1);
	}
}
