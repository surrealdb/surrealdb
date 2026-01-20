//! Defines the session module for the execution module.
use crate::errors::error::{SurrealError, SurrealErrorStatus};
use crate::safe_eject;
use ort::session::Session;

#[cfg(feature = "gpu")]
use ort::execution_providers::CUDAExecutionProvider;
#[cfg(feature = "gpu")]
use ort::execution_providers::ExecutionProvider;

/// Creates a session for a model.
///
/// # Arguments
/// * `model_bytes` - The model bytes (usually extracted fromt the surml file)
///
/// # Returns
/// A session object.
pub fn get_session(model_bytes: Vec<u8>) -> Result<Session, SurrealError> {
	#[cfg(feature = "gpu")]
	let mut builder = safe_eject!(Session::builder(), SurrealErrorStatus::Unknown);

	#[cfg(not(feature = "gpu"))]
	let builder = safe_eject!(Session::builder(), SurrealErrorStatus::Unknown);

	#[cfg(feature = "gpu")]
	{
		let cuda = CUDAExecutionProvider::default();
		if let Err(e) = cuda.register(&mut builder) {
			eprintln!("Failed to register CUDA: {:?}. Falling back to CPU.", e);
		} else {
			println!("CUDA registered successfully");
		}
	}
	let session: Session =
		safe_eject!(builder.commit_from_memory(&model_bytes), SurrealErrorStatus::Unknown);
	Ok(session)
}
