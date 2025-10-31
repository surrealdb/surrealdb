use thiserror::Error;

/// Defines custom error types for the crate.
///
/// This enum represents various error conditions that can occur during operations,
/// such as interactions with registries or other internal mechanisms. It uses
/// `thiserror::Error` to automatically derive implementations for `std::error::Error`
/// and `std::fmt::Display`, making it easier to handle and display errors.
#[derive(Debug, Error)]
pub enum Error {
	/// Indicates that an operation on the function registry failed because it was locked.
	///
	/// This error occurs when attempting to modify or access a registry that is currently
	/// locked, preventing concurrent or unauthorized modifications. It may suggest retrying
	/// the operation later or checking for proper synchronization mechanisms.
	#[error("Failed to operate on function registry")]
	RegistryLocked,
}
