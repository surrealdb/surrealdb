use std::ops::Deref;
use std::str::FromStr as _;

use anyhow::Result;
use revision::revisioned;
use serde::{Deserialize, Serialize};

use super::{Level, Resource, ResourceKind};
use crate::iam::Role;

//
// User
//
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Actor {
	res: Resource,
	roles: Vec<Role>,
}

impl Default for Actor {
	fn default() -> Self {
		Self {
			res: ResourceKind::Actor.on_level(Level::No),
			roles: Vec::new(),
		}
	}
}

impl std::fmt::Display for Actor {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if self.res.level() == &Level::No {
			return write!(f, "Actor::Anonymous");
		}

		write!(
			f,
			"{}{}::{}({})",
			self.res.level(),
			self.res.kind(),
			self.res.id(),
			self.roles.iter().map(|r| format!("{}", r)).collect::<Vec<String>>().join(", ")
		)
	}
}

impl Actor {
	pub(crate) fn new(id: String, roles: Vec<Role>, level: Level) -> Self {
		Self {
			res: Resource::new(id, super::ResourceKind::Actor, level),
			roles,
		}
	}

	pub fn from_role_names(id: String, roles: &[String], level: Level) -> Result<Self> {
		let roles = roles.iter().map(|x| Role::from_str(x)).collect::<Result<Vec<_>, _>>()?;
		Ok(Self::new(id, roles, level))
	}

	/// Checks if the actor has the given role.
	pub(crate) fn has_role(&self, role: Role) -> bool {
		self.roles.contains(&role)
	}

	/// Checks if the actor has the Owner role.
	pub(crate) fn has_owner_role(&self) -> bool {
		self.roles.iter().any(|r| r.eq(&Role::Owner))
	}

	/// Checks if the actor has the Editor role.
	pub(crate) fn has_editor_role(&self) -> bool {
		self.roles.iter().any(|r| r.eq(&Role::Owner) || r.eq(&Role::Editor))
	}

	/// Checks if the actor has the Viewer role.
	pub(crate) fn has_viewer_role(&self) -> bool {
		self.roles.iter().any(|r| r.eq(&Role::Owner) || r.eq(&Role::Editor) || r.eq(&Role::Viewer))
	}
}

impl Deref for Actor {
	type Target = Resource;
	fn deref(&self) -> &Self::Target {
		&self.res
	}
}
