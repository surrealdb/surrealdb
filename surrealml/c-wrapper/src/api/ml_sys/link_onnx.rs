use crate::utils::EmptyReturn;

/// Links the onnx file to the environment.
///
/// This is now a no-op since `ort` automatically downloads and initializes
/// ONNX Runtime via the `download-binaries` feature (enabled by default).
///
/// # Returns
/// An EmptyReturn object containing the outcome of the operation.
#[no_mangle]
pub extern "C" fn link_onnx() -> EmptyReturn {
	// ort handles initialization automatically, so this is now a no-op
	EmptyReturn {
		is_error: 0,
		error_message: std::ptr::null_mut(),
	}
}
