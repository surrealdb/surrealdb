use std::sync::Once;

pub use surrealdb_iam::*;
pub use token::Token;

pub mod access;
mod action_impls;
pub mod base;
pub mod check;
pub mod clear;
mod error;
pub(crate) mod file;
pub mod issue;
#[cfg(feature = "jwks")]
pub mod jwks;
pub mod reset;
pub mod signin;
pub mod signup;
pub mod token;
pub mod verify;

pub(crate) use error::Error;

use crate::catalog;

fn algorithm_to_jwt_algorithm(alg: catalog::Algorithm) -> jsonwebtoken::Algorithm {
	match alg {
		catalog::Algorithm::Hs256 => jsonwebtoken::Algorithm::HS256,
		catalog::Algorithm::Hs384 => jsonwebtoken::Algorithm::HS384,
		catalog::Algorithm::Hs512 => jsonwebtoken::Algorithm::HS512,
		catalog::Algorithm::EdDSA => jsonwebtoken::Algorithm::EdDSA,
		catalog::Algorithm::Es256 => jsonwebtoken::Algorithm::ES256,
		catalog::Algorithm::Es384 => jsonwebtoken::Algorithm::ES384,
		catalog::Algorithm::Es512 => {
			static ES512_WARN: Once = Once::new();
			ES512_WARN.call_once(|| {
				warn!("ES512 is not currently supported by the underlying cryptography library and will fall back to ES384. Please update your access definition to use ES384 or another supported algorithm.");
			});
			jsonwebtoken::Algorithm::ES384
		}
		catalog::Algorithm::Ps256 => jsonwebtoken::Algorithm::PS256,
		catalog::Algorithm::Ps384 => jsonwebtoken::Algorithm::PS384,
		catalog::Algorithm::Ps512 => jsonwebtoken::Algorithm::PS512,
		catalog::Algorithm::Rs256 => jsonwebtoken::Algorithm::RS256,
		catalog::Algorithm::Rs384 => jsonwebtoken::Algorithm::RS384,
		catalog::Algorithm::Rs512 => jsonwebtoken::Algorithm::RS512,
	}
}

/// Returns true if the error is an expired-token auth error (e.g. from `verify::token`).
///
/// Authentication failures are always raised bare into `anyhow` - no core
/// `Error` variant wraps them - so a single downcast sees every occurrence.
pub fn is_expired_token_error(e: &anyhow::Error) -> bool {
	matches!(e.downcast_ref::<Error>(), Some(Error::ExpiredToken))
}
