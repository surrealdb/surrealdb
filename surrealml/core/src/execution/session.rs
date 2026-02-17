//! Defines the session module for the execution module.
use std::sync::Once;

use ort::session::Session;

use crate::errors::error::{SurrealError, SurrealErrorStatus};
use crate::safe_eject;

static INIT_ORT_BACKEND: Once = Once::new();

/// Ensures the ort-tract (pure Rust) backend is set before any ort API is used.
fn ensure_ort_backend() {
	INIT_ORT_BACKEND.call_once(|| {
		ort::set_api(ort_tract::api());
	});
}

/// Creates a session for a model.
///
/// # Arguments
/// * `model_bytes` - The model bytes (usually extracted fromt the surml file)
///
/// # Returns
/// A session object.
pub fn get_session(model_bytes: Vec<u8>) -> Result<Session, SurrealError> {
	ensure_ort_backend();
	let builder = safe_eject!(Session::builder(), SurrealErrorStatus::Unknown);
	let session: Session =
		safe_eject!(builder.commit_from_memory(&model_bytes), SurrealErrorStatus::Unknown);
	Ok(session)
}
