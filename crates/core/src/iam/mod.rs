use cedar_policy::Context;
pub use entities::Level;
use thiserror::Error;

pub mod access;
pub mod auth;
pub mod base;
pub mod check;
pub mod clear;
pub mod entities;
pub mod issue;
#[cfg(feature = "jwks")]
pub mod jwks;
pub mod policies;
pub mod signin;
pub mod signup;
pub mod token;
pub mod verify;

pub use self::auth::*;
pub use self::entities::*;

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

impl From<Error> for String {
	fn from(e: Error) -> String {
		e.to_string()
	}
}

pub fn is_allowed(
	actor: &Actor,
	action: &Action,
	resource: &Resource,
	ctx: Option<Context>,
) -> Result<(), Error> {
	match policies::is_allowed(actor, action, resource, ctx.unwrap_or(Context::empty())) {
		(allowed, _) if allowed => Ok(()),
		_ => {
			let err = Error::NotAllowed {
				actor: actor.to_string(),
				action: action.to_string(),
				resource: format!("{}", resource),
			};

			trace!("{}", err);
			Err(err)
		}
	}
}
