//! Defines the session module for the execution module.
use std::sync::{Mutex, OnceLock};

use ort::environment::Environment;
#[cfg(feature = "gpu")]
use ort::ep::CUDA;
use ort::session::Session;

use crate::errors::error::{SurrealError, SurrealErrorStatus};
use crate::safe_eject;

/// The process-wide ONNX Runtime environment. Every session borrows this single
/// environment when it is built, so ONNX Runtime is initialised once for the life
/// of the process rather than per session.
static ENVIRONMENT: OnceLock<Environment> = OnceLock::new();

/// Serialises the environment build. ONNX Runtime allows only one environment
/// per process and rejects a second `build()` with an error, so the slow path
/// must call `build()` at most once even under concurrent first use.
static ENVIRONMENT_INIT: Mutex<()> = Mutex::new(());

/// Returns the shared ONNX Runtime environment, building it on first use.
///
/// # Returns
/// A reference to the process-wide environment.
fn environment() -> Result<&'static Environment, SurrealError> {
	if let Some(environment) = ENVIRONMENT.get() {
		return Ok(environment);
	}
	let _guard = ENVIRONMENT_INIT.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
	// A thread that waited on the lock may find the environment already built.
	if let Some(environment) = ENVIRONMENT.get() {
		return Ok(environment);
	}
	let builder = ort::init().with_name("surrealml");
	#[cfg(feature = "gpu")]
	let builder = builder.with_execution_providers([CUDA::default().build()]);
	let environment = safe_eject!(builder.build(), SurrealErrorStatus::Unknown);
	let _ = ENVIRONMENT.set(environment);
	Ok(ENVIRONMENT.get().expect("environment was just set"))
}

/// Creates a session for a model.
///
/// # Arguments
/// * `model_bytes` - The model bytes (usually extracted fromt the surml file)
///
/// # Returns
/// A session object.
pub fn get_session(model_bytes: Vec<u8>) -> Result<Session, SurrealError> {
	let environment = environment()?;
	let mut builder = safe_eject!(Session::builder(environment), SurrealErrorStatus::Unknown);
	let session: Session =
		safe_eject!(builder.commit_from_memory(&model_bytes), SurrealErrorStatus::Unknown);
	Ok(session)
}
