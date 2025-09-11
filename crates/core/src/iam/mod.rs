pub use entities::Level;
use thiserror::Error;

pub mod access;
pub mod auth;
pub mod base;
pub mod check;
pub mod clear;
pub mod entities;
pub(crate) mod file;
pub mod issue;
#[cfg(feature = "jwks")]
pub mod jwks;
pub mod reset;
pub mod signin;
pub mod signup;
pub mod token;
pub mod verify;

pub use self::auth::*;
pub use self::entities::*;
use crate::catalog;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
	#[error("Invalid role '{0}'")]
	InvalidRole(String),

	#[error("Not enough permissions to perform this action")]
	NotAllowed {
		actor: String,
		action: String,
		resource: String,
	},
}

fn algorithm_to_jwt_algorithm(alg: catalog::Algorithm) -> jsonwebtoken::Algorithm {
	match alg {
		catalog::Algorithm::Hs256 => jsonwebtoken::Algorithm::HS256,
		catalog::Algorithm::Hs384 => jsonwebtoken::Algorithm::HS384,
		catalog::Algorithm::Hs512 => jsonwebtoken::Algorithm::HS512,
		catalog::Algorithm::EdDSA => jsonwebtoken::Algorithm::EdDSA,
		catalog::Algorithm::Es256 => jsonwebtoken::Algorithm::ES256,
		catalog::Algorithm::Es384 => jsonwebtoken::Algorithm::ES384,
		catalog::Algorithm::Es512 => jsonwebtoken::Algorithm::ES384,
		catalog::Algorithm::Ps256 => jsonwebtoken::Algorithm::PS256,
		catalog::Algorithm::Ps384 => jsonwebtoken::Algorithm::PS384,
		catalog::Algorithm::Ps512 => jsonwebtoken::Algorithm::PS512,
		catalog::Algorithm::Rs256 => jsonwebtoken::Algorithm::RS256,
		catalog::Algorithm::Rs384 => jsonwebtoken::Algorithm::RS384,
		catalog::Algorithm::Rs512 => jsonwebtoken::Algorithm::RS512,
	}
}

pub fn is_allowed_check(actor: &Actor, action: &Action, resource: &Resource) -> bool {
	match action {
		Action::View => resource.level().sublevel_of(actor.level()),
		Action::Edit => {
			if actor.has_role(Role::Owner) {
				resource.level().sublevel_of(actor.level())
			} else if actor.has_role(Role::Editor) {
				matches!(
					resource.kind(),
					ResourceKind::Namespace
						| ResourceKind::Database
						| ResourceKind::Record
						| ResourceKind::Table
						| ResourceKind::Document
						| ResourceKind::Option
						| ResourceKind::Function
						| ResourceKind::Analyzer
						| ResourceKind::Parameter
						| ResourceKind::Event
						| ResourceKind::Field
						| ResourceKind::Index
				) && resource.level().sublevel_of(actor.level())
			} else {
				false
			}
		}
	}
}

pub fn is_allowed(actor: &Actor, action: &Action, resource: &Resource) -> Result<(), Error> {
	if !is_allowed_check(actor, action, resource) {
		let err = Error::NotAllowed {
			actor: actor.to_string(),
			action: action.to_string(),
			resource: format!("{}", resource),
		};

		trace!("{}", err);
		return Err(err);
	}

	Ok(())
}
