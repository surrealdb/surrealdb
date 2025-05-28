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
