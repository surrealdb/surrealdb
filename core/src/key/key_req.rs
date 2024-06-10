use crate::key::category::Category;

/// Key requirements are functions that we expect all keys to have
pub(crate) trait KeyRequirements {
	/// Returns the category of the key for error reporting
	fn key_category(&self) -> Category;
}
