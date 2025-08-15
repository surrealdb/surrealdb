use std::ops::Deref;
use std::str::FromStr;

use revision::revisioned;
use serde::{Deserialize, Serialize};

use super::{Level, Resource, ResourceKind};
use crate::expr::statements::{DefineAccessStatement, DefineUserStatement};
use crate::iam::{Error, Role};

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

impl std::convert::TryFrom<(&DefineUserStatement, Level)> for Actor {
	type Error = Error;
	fn try_from(val: (&DefineUserStatement, Level)) -> Result<Self, Self::Error> {
		let roles = val.0.roles.iter().map(|e| Role::from_str(e)).collect::<Result<_, _>>()?;
		Ok(Self::new(val.0.name.to_string(), roles, val.1))
	}
}

impl std::convert::From<(&DefineAccessStatement, Level)> for Actor {
	fn from(val: (&DefineAccessStatement, Level)) -> Self {
		Self::new(val.0.name.to_string(), Vec::default(), val.1)
	}
}
