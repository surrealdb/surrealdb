use revision::revisioned;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::str::FromStr;

use cedar_policy::{Entity, EntityId, EntityTypeName, EntityUid, RestrictedExpression};
use serde::{Deserialize, Serialize};

use super::{Level, Resource, ResourceKind};
use crate::iam::Role;
use crate::sql::statements::{DefineTokenStatement, DefineUserStatement};

//
// User
//
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
#[non_exhaustive]
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
	pub fn new(id: String, roles: Vec<Role>, level: Level) -> Self {
		Self {
			res: Resource::new(id, super::ResourceKind::Actor, level),
			roles,
		}
	}

	/// Checks if the actor has the given role.
	pub fn has_role(&self, role: &Role) -> bool {
		self.roles.contains(role)
	}

	// Cedar policy helpers
	pub fn cedar_attrs(&self) -> HashMap<String, RestrictedExpression> {
		[
			("type", self.kind().into()),
			("level", self.level().into()),
			("roles", RestrictedExpression::new_set(self.roles.iter().map(|r| r.into()))),
		]
		.into_iter()
		.map(|(x, v)| (x.into(), v))
		.collect()
	}

	pub fn cedar_parents(&self) -> HashSet<EntityUid> {
		let mut parents = HashSet::with_capacity(1);
		parents.insert(self.res.level().into());
		parents
	}

	pub fn cedar_entities(&self) -> Vec<Entity> {
		let mut entities = Vec::new();

		entities.push(self.into());

		for role in self.roles.iter() {
			entities.push(role.into());
		}

		for level in self.res.level().cedar_entities() {
			entities.push(level);
		}

		entities
	}
}

impl Deref for Actor {
	type Target = Resource;
	fn deref(&self) -> &Self::Target {
		&self.res
	}
}

impl std::convert::From<&Actor> for EntityUid {
	fn from(actor: &Actor) -> Self {
		EntityUid::from_type_name_and_id(
			EntityTypeName::from_str("Actor").unwrap(),
			EntityId::from_str(actor.id()).unwrap(),
		)
	}
}

impl std::convert::From<&Actor> for Entity {
	fn from(actor: &Actor) -> Self {
		Entity::new(actor.into(), actor.cedar_attrs(), actor.cedar_parents())
	}
}

impl std::convert::From<(&DefineUserStatement, Level)> for Actor {
	fn from(val: (&DefineUserStatement, Level)) -> Self {
		Self::new(val.0.name.to_string(), val.0.roles.iter().map(Role::from).collect(), val.1)
	}
}

impl std::convert::From<(&DefineTokenStatement, Level)> for Actor {
	fn from(val: (&DefineTokenStatement, Level)) -> Self {
		Self::new(val.0.name.to_string(), Vec::default(), val.1)
	}
}
