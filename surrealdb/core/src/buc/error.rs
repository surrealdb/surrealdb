//! Failures raised by the bucket layer.
//!
//! These cover reaching an object-storage backend at all (URL parsing, backend
//! support, the global-bucket policy), the access rules a bucket carries
//! (read-only mode, the PERMISSIONS clause, the file allowlist), and the store
//! operations themselves.

// The mapper below is the only place this layer's failures become public.
// A new variant must make that decision explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use common::{LeafError, internal_todo};
use object_store::Error as ObjectStoreError;
use surrealdb_types::Error as TypesError;

use crate::buc::BucketOperation;

/// A failure in the bucket layer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
	/// The bucket's permissions do not allow this operation
	#[error("You don't have permission to {op} this file in the `{name}` bucket")]
	BucketPermissions {
		name: String,
		op: BucketOperation,
	},

	/// Represents an underlying error with the Object Store
	#[error("Object Store error: {0}")]
	#[expect(
		clippy::enum_variant_names,
		reason = "the variant name is the label the wire snapshot pins this row under"
	)]
	ObsError(#[from] ObjectStoreError),

	/// A path outside the configured allowlist was requested
	#[error("File access denied: {0}")]
	FileAccessDenied(String),

	/// A bucket without its own backend was used, but no global bucket exists
	#[error("No global bucket has been configured")]
	NoGlobalBucket,

	/// The bucket has no usable connection
	#[error("Bucket `{0}` is unavailable")]
	BucketUnavailable(String),

	/// Only the global bucket may be used, and this connection is not to it
	#[error("Bucket is unavailable")]
	GlobalBucketEnforced,

	/// The `BACKEND` url could not be parsed, or names an unusable location
	///
	/// Every construction site sits in a backend module that is compiled out on
	/// wasm, so the variant is unreachable there.
	#[error("Bucket url could not be processed: {0}")]
	#[cfg_attr(target_family = "wasm", expect(dead_code))]
	InvalidBucketUrl(String),

	/// The `BACKEND` url names a scheme no backend claims
	#[error("Bucket backend is not supported")]
	UnsupportedBackend,

	/// A write was attempted against a bucket defined as read-only
	#[error("Write operation is not supported, as bucket `{0}` is in read-only mode")]
	ReadonlyBucket(String),

	/// The backend rejected or failed an operation
	#[error("Operation for bucket `{0}` failed: {1}")]
	ObjectStoreFailure(String, String),
}

impl LeafError for Error {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			// Classified rather than unclassified: an `object_store` failure is
			// an engine-internal fault, and the underlying error travels with
			// it as the cause.
			Error::ObsError(_) => TypesError::internal(message),
			Error::BucketPermissions {
				..
			}
			| Error::FileAccessDenied(_)
			| Error::NoGlobalBucket
			| Error::BucketUnavailable(_)
			| Error::GlobalBucketEnforced
			| Error::InvalidBucketUrl(_)
			| Error::UnsupportedBackend
			| Error::ReadonlyBucket(_)
			| Error::ObjectStoreFailure(..) => internal_todo(message),
		}
	}
}
