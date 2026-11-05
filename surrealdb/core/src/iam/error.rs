//! Failures raised by the engine's authentication flows.
//!
//! Signin, signup, token verification and access-grant issuance: the caller
//! could not prove who they are, or the access method they named cannot serve
//! the operation they asked for.
//!
//! Distinct from [`PolicyError`](surrealdb_iam::PolicyError), which is the
//! authorisation verdict once identity is established, and from the
//! `DEFINE`/`ALTER ACCESS` failures, which are catalog vocabulary.
//!
//! Several variants are deliberately vague. An authentication failure must not
//! tell a caller which half of a credential was wrong, nor whether a named
//! access method exists, so distinct internal causes collapse into one message.

// The mapper below is the only place this layer's failures become public.
// A new variant must make that decision explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use common::{LeafError, internal_todo};
use surrealdb_types::{AuthError, Error as TypesError};

/// A failure in an authentication flow.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
	/// A token could not be signed for an otherwise successful authentication
	#[error("There was an error creating the token")]
	TokenMakingFailed,

	/// The signin or signup query completed without yielding a record
	#[error("No record was returned")]
	NoRecordFound,

	/// A credential-based signin was attempted without both credentials
	#[error("Username or Password was not provided")]
	MissingUserOrPass,

	/// The signin parameters name no level or access method to sign in against
	#[error("No signin target to either SC or DB or NS or KV")]
	NoSigninTarget,

	/// The supplied password did not match the stored hash
	#[error("The password did not verify")]
	InvalidPass,

	/// There was an error with authentication
	///
	/// This error hides different kinds of errors directly related to
	/// authentication
	#[error("There was a problem with authentication")]
	InvalidAuth,

	/// There was an unexpected error while performing authentication
	///
	/// This error hides different kinds of unexpected errors that may affect
	/// authentication
	#[error("There was an unexpected error while performing authentication")]
	UnexpectedAuth,

	/// There was an error with signing up
	#[error("There was a problem with signing up")]
	InvalidSignup,

	/// The token has expired
	#[error("The token has expired")]
	ExpiredToken,

	/// The named access method exists but is of a kind this operation cannot use
	#[error("The access method cannot be used in the requested operation")]
	AccessMethodMismatch,

	/// No access method of the requested name exists at the requested level
	#[error("The access method does not exist")]
	AccessNotFound,

	/// The access method's configured duration cannot be represented
	#[error("This access method has an invalid duration")]
	AccessInvalidDuration,

	/// The access method's duration lands outside the representable range
	#[error("This access method results in an invalid expiration")]
	AccessInvalidExpiration,

	/// The record access `SIGNUP` clause failed to execute
	#[error("The record access signup query failed")]
	AccessRecordSignupQueryFailed,

	/// The record access `SIGNIN` clause failed to execute
	#[error("The record access signin query failed")]
	AccessRecordSigninQueryFailed,

	/// The record access method defines no `SIGNUP` clause
	#[error("This record access method does not allow signup")]
	AccessRecordNoSignup,

	/// The record access method defines no `SIGNIN` clause
	#[error("This record access method does not allow signin")]
	AccessRecordNoSignin,

	/// A bearer signin was attempted without the grant key
	#[error("This bearer access method requires a key to be provided")]
	AccessBearerMissingKey,

	/// The supplied bearer grant does not have the expected shape
	#[error("This bearer access grant has an invalid format")]
	AccessGrantBearerInvalid,
}

/// A token whose payload cannot be base64-decoded is an opaque authentication
/// failure, never a decoding error: the caller learns only that the token was
/// rejected.
impl From<base64::DecodeError> for Error {
	fn from(_: base64::DecodeError) -> Error {
		Error::InvalidAuth
	}
}

/// As with base64: a token that fails to decode, or whose signature or claims
/// fail validation, is reported as a plain authentication failure.
impl From<jsonwebtoken::errors::Error> for Error {
	fn from(_: jsonwebtoken::errors::Error) -> Error {
		Error::InvalidAuth
	}
}

impl LeafError for Error {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			Error::ExpiredToken => TypesError::not_allowed(message, AuthError::TokenExpired),
			Error::InvalidAuth => TypesError::not_allowed(message, AuthError::InvalidAuth),
			Error::UnexpectedAuth => TypesError::not_allowed(message, AuthError::UnexpectedAuth),
			Error::MissingUserOrPass => {
				TypesError::not_allowed(message, AuthError::MissingUserOrPass)
			}
			Error::NoSigninTarget => TypesError::not_allowed(message, AuthError::NoSigninTarget),
			Error::InvalidPass => TypesError::not_allowed(message, AuthError::InvalidPass),
			Error::TokenMakingFailed => {
				TypesError::not_allowed(message, AuthError::TokenMakingFailed)
			}
			Error::InvalidSignup => TypesError::not_allowed(message, AuthError::InvalidSignup),
			Error::AccessRecordNoSignup | Error::AccessRecordNoSignin => {
				TypesError::not_allowed(message, None)
			}
			// A signin or signup that yielded no row is a miss, not a refusal:
			// the credentials were accepted and the query simply found nothing.
			Error::NoRecordFound => TypesError::not_found(message, None),
			// The record access `SIGNIN`/`SIGNUP` clause is user-authored
			// SurrealQL, so its failure is a query failure rather than a
			// refusal to authenticate.
			Error::AccessRecordSignupQueryFailed | Error::AccessRecordSigninQueryFailed => {
				TypesError::query(message, None)
			}
			Error::AccessMethodMismatch
			| Error::AccessNotFound
			| Error::AccessInvalidDuration
			| Error::AccessInvalidExpiration
			| Error::AccessBearerMissingKey
			| Error::AccessGrantBearerInvalid => internal_todo(message),
		}
	}
}
