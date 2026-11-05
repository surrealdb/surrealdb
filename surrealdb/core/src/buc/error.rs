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
	// Only the file backend constructs this, and that backend is not compiled
	// for wasm. The variant stays in the enum on every target so the error's
	// wire representation does not vary by target.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	FileAccessDenied(String),

	/// A presigned URL was asked for a lifetime past this node's ceiling.
	///
	/// Names both durations, because the caller cannot see the node's
	/// configuration and would otherwise have to guess what is allowed.
	#[error(
		"A presigned URL cannot last {requested:?}: this node allows at most {allowed:?}. Raise SURREAL_BUCKET_PRESIGN_MAX_TTL to change it"
	)]
	PresignTtlTooLong {
		requested: std::time::Duration,
		allowed: std::time::Duration,
	},

	/// A presigned upload was asked for a byte ceiling past this node's.
	#[error(
		"A presigned upload cannot allow {requested} bytes: this node allows at most {allowed}. Raise SURREAL_BUCKET_PRESIGN_MAX_SIZE to change it"
	)]
	PresignMaxSizeTooLarge {
		requested: u64,
		allowed: u64,
	},

	/// Presigning was attempted on a node with no `SURREAL_BUCKET_PRESIGN_SECRET`.
	///
	/// Fails closed rather than generating a per-node key: that would work on a
	/// single node and fail intermittently behind a load balancer, which is a
	/// far worse way to discover the misconfiguration.
	#[error(
		"Presigned URLs are unavailable: this node has no URL-signing secret. Set SURREAL_BUCKET_PRESIGN_SECRET to the same value of at least 32 bytes on every node"
	)]
	PresignNoSecret,

	/// A write named a media type the bucket's backend cannot store.
	///
	/// Refused rather than dropped, so a caller never believes a type was kept
	/// when it was not. Only the file backend lacks one.
	#[error(
		"Bucket `{0}` cannot store a content type: its backend keeps none. Write without one, or as `application/octet-stream`"
	)]
	ContentTypeUnsupported(String),

	/// A presigned URL could not be produced or checked.
	#[error("{0}")]
	Presign(String),

	/// A presigned URL did not verify.
	///
	/// Carries no detail on purpose: a mismatched tenant, a changed secret and
	/// an edited parameter are indistinguishable to the caller.
	#[error("The presigned URL is not valid for this request")]
	PresignBadSignature,

	/// A presigned URL was presented past its expiry.
	///
	/// Distinct from a bad signature so the route can answer `410 Gone` rather
	/// than `401`: the URL was genuine, and re-minting is the remedy.
	#[error("The presigned URL has expired")]
	PresignExpired,

	/// A bucket without its own backend was used, but no global bucket exists
	#[error("No global bucket has been configured")]
	NoGlobalBucket,

	/// The bucket has no usable connection
	#[error("Bucket `{0}` is unavailable")]
	BucketUnavailable(String),

	/// Only the global bucket may be used, and this connection is not to it
	#[error("Bucket is unavailable")]
	GlobalBucketEnforced,

	/// A `global://` backend was given by a principal below the root
	#[error("Only a root user can give a bucket a `global://` backend")]
	GlobalBackendNeedsRoot,

	/// The `BACKEND` url or bucket name could not be parsed, or names an unusable location
	#[error("Bucket url could not be processed: {0}")]
	InvalidBucketUrl(String),

	/// The `BACKEND` url names a scheme no backend claims
	#[error("Bucket backend is not supported")]
	UnsupportedBackend,

	/// The build left out the `files` feature, which buckets need
	#[cfg_attr(feature = "files", allow(dead_code))]
	#[error("Buckets are not available: this build was compiled without the `files` feature")]
	FeatureDisabled,

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
			// A caller asked for a lifetime the node does not allow: bad input,
			// not a fault, and the message names both durations.
			Error::PresignTtlTooLong {
				..
			}
			| Error::PresignMaxSizeTooLarge {
				..
			}
			| Error::PresignExpired => TypesError::validation(message, None),
			// The request asked for what this bucket cannot do: bad input
			// against this bucket, not a fault.
			Error::ContentTypeUnsupported(_) => TypesError::validation(message, None),
			// An unusable credential, not bad input.
			Error::PresignBadSignature => TypesError::not_allowed(message, None),
			// The principal may define buckets, but not over the whole global
			// bucket, which holds every namespace's objects.
			Error::GlobalBackendNeedsRoot => TypesError::not_allowed(message, None),
			// The node is missing a signing secret. Nothing the caller can fix, and
			// nothing broken — it was started that way.
			Error::PresignNoSecret => TypesError::configuration(message, None),
			// The signing step refused the request. Every other cause is
			// unreachable by the time this is constructed — HMAC accepts a key
			// of any length — so what remains is an expiry that cannot be represented, which is
			// caller input.
			Error::Presign(_) => TypesError::validation(message, None),
			// The build, not the request, is at fault: a configuration error,
			// as the expression and index layers classify theirs.
			Error::FeatureDisabled => TypesError::configuration(message, None),
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
