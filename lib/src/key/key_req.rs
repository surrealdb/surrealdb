use crate::key::error::KeyError;

/// Key requirements are functions that we expect all keys to have
pub(crate) trait KeyRequirements {
	/// Returns the category of the key for error reporting
	fn key_category() -> KeyError;
}
