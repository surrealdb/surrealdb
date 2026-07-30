//! Identity and access management: the authentication and authorisation
//! primitives shared across the SurrealDB codebase.
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to SemVer and its API is
//! free to change and break code even between patch versions. If you are looking for a stable
//! interface to the SurrealDB library please have a look at
//! <a href="https://crates.io/crates/surrealdb">the Rust SDK</a>.
//! </section>

mod auth;
pub mod entities;
pub mod scram;

pub use auth::*;
pub use entities::*;
pub use scram::{ScramCredential, ScramParseError};
use tracing::trace;

#[derive(thiserror::Error, Debug)]
pub enum PolicyError {
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

pub fn is_allowed(actor: &Actor, action: &Action, resource: &Resource) -> Result<(), PolicyError> {
	if !is_allowed_check(actor, action, resource) {
		let err = PolicyError::NotAllowed {
			actor: actor.to_string(),
			action: action.to_string(),
			resource: format!("{}", resource),
		};

		trace!("{}", err);
		return Err(err);
	}

	Ok(())
}

/// Derive an Argon2id hash of a plaintext password for storage.
///
/// Centralizes the hashing so every place that turns a `PASSWORD` clause into
/// stored credentials (DEFINE/ALTER USER conversions and the root-user
/// bootstrap) agrees on the algorithm, parameters, and salt policy.
pub fn hash_password(password: &str) -> String {
	use argon2::Argon2;
	use argon2::password_hash::{PasswordHasher, SaltString};
	use rand_core::OsRng;

	Argon2::default()
		.hash_password(password.as_bytes(), &SaltString::generate(&mut OsRng))
		.expect("password hashing should not fail")
		.to_string()
}
