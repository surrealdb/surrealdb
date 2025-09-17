use anyhow::Result;
use revision::revisioned;
use serde::{Deserialize, Serialize};

use super::{Action, Actor, Level, Resource, Role, is_allowed};

/// Specifies the current authentication for the datastore execution context.
#[revisioned(revision = 1)]
#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

	/// Check if the current level is Record
	pub fn is_record(&self) -> bool {
		matches!(self.level(), Level::Record(_, _, _))
	}

	/// Check if the current level is Namespace, and the namespace matches
	pub fn is_ns_check(&self, ns: &str) -> bool {
		matches!(self.level(), Level::Namespace(n) if n.eq(ns))
	}

	/// Check if the current level is Database, and the namespace and database
	/// match
	pub fn is_db_check(&self, ns: &str, db: &str) -> bool {
		matches!(self.level(), Level::Database(n, d) if n.eq(ns) && d.eq(db))
	}

	/// System Auth helpers
	///
	/// These are not stored in the database and are used for internal
	/// operations Do not use for authentication
	pub fn for_root(role: Role) -> Self {
		Self::new(Actor::new("system_auth".into(), vec![role], Level::Root))
	}

	pub fn for_ns(role: Role, ns: &str) -> Self {
		Self::new(Actor::new("system_auth".into(), vec![role], Level::Namespace(ns.to_owned())))
	}

	pub fn for_db(role: Role, ns: &str, db: &str) -> Self {
		Self::new(Actor::new(
			"system_auth".into(),
			vec![role],
			Level::Database(ns.to_owned(), db.to_owned()),
		))
	}

	pub fn for_record(rid: String, ns: &str, db: &str, ac: &str) -> Self {
		Self::new(Actor::new(
			rid.to_string(),
			vec![],
			Level::Record(ns.to_owned(), db.to_owned(), ac.to_owned()),
		))
	}

	//
	// Permission checks
	//

	/// Checks if the current auth is allowed to perform an action on a given
	/// resource
	pub fn is_allowed(&self, action: Action, res: &Resource) -> Result<()> {
		is_allowed(&self.actor, &action, res)
			.map_err(crate::err::Error::from)
			.map_err(anyhow::Error::new)
	}

	/// Checks if the current actor has a given role
	pub fn has_role(&self, role: Role) -> bool {
		self.actor.has_role(role)
	}

	/// Checks if the current actor has a Owner role
	pub fn has_owner_role(&self) -> bool {
		self.actor.has_owner_role()
	}

	/// Checks if the current actor has a Editor role
	pub fn has_editor_role(&self) -> bool {
		self.actor.has_editor_role()
	}

	/// Checks if the current actor has a Viewer role
	pub fn has_viewer_role(&self) -> bool {
		self.actor.has_viewer_role()
	}
}
