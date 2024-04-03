use crate::sql::statements::{DefineTokenStatement, DefineUserStatement};
use revision::revisioned;
use serde::{Deserialize, Serialize};

use super::{is_allowed, Action, Actor, Error, Level, Resource, Role};

/// Specifies the current authentication for the datastore execution context.
#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
#[non_exhaustive]
pub struct Auth {
	actor: Actor,
}

impl Auth {
	pub fn new(actor: Actor) -> Self {
		Self {
			actor,
		}
	}

	pub fn id(&self) -> &str {
		self.actor.id()
	}

	/// Return current authentication level
	pub fn level(&self) -> &Level {
		self.actor.level()
	}

	/// Check if the current auth is anonymous
	pub fn is_anon(&self) -> bool {
		matches!(self.level(), Level::No)
	}

	/// Check if the current level is Root
	pub fn is_root(&self) -> bool {
		matches!(self.level(), Level::Root)
	}

	/// Check if the current level is Namespace
	pub fn is_ns(&self) -> bool {
		matches!(self.level(), Level::Namespace(_))
	}

	/// Check if the current level is Database
	pub fn is_db(&self) -> bool {
		matches!(self.level(), Level::Database(_, _))
	}

	/// Check if the current level is Scope
	pub fn is_scope(&self) -> bool {
		matches!(self.level(), Level::Scope(_, _, _))
	}

	/// System Auth helpers
	///
	/// These are not stored in the database and are used for internal operations
	/// Do not use for authentication
	pub fn for_root(role: Role) -> Self {
		Self::new(Actor::new("system_auth".into(), vec![role], Level::Root))
	}

	pub fn for_ns(role: Role, ns: &str) -> Self {
		Self::new(Actor::new("system_auth".into(), vec![role], (ns,).into()))
	}

	pub fn for_db(role: Role, ns: &str, db: &str) -> Self {
		Self::new(Actor::new("system_auth".into(), vec![role], (ns, db).into()))
	}

	pub fn for_sc(rid: String, ns: &str, db: &str, sc: &str) -> Self {
		Self::new(Actor::new(rid, vec![], (ns, db, sc).into()))
	}

	//
	// Permission checks
	//

	/// Checks if the current auth is allowed to perform an action on a given resource
	pub fn is_allowed(&self, action: Action, res: &Resource) -> Result<(), Error> {
		is_allowed(&self.actor, &action, res, None)
	}

	/// Checks if the current actor has a given role
	pub fn has_role(&self, role: &Role) -> bool {
		self.actor.has_role(role)
	}
}

impl std::convert::From<(&DefineUserStatement, Level)> for Auth {
	fn from(val: (&DefineUserStatement, Level)) -> Self {
		Self::new((val.0, val.1).into())
	}
}

impl std::convert::From<(&DefineTokenStatement, Level)> for Auth {
	fn from(val: (&DefineTokenStatement, Level)) -> Self {
		Self::new((val.0, val.1).into())
	}
}
