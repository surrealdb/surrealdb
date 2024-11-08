use crate::iam::Error;
use cedar_policy::{Entity, EntityTypeName, EntityUid, RestrictedExpression};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

// In the future, we will allow for custom roles. For now, provide predefined roles.
#[revisioned(revision = 1)]
#[derive(Hash, Clone, Default, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Role {
	#[default]
	Viewer,
	Editor,
	Owner,
}

impl Role {
	pub fn as_str(&self) -> &'static str {
		match self {
			Self::Viewer => "Viewer",
			Self::Editor => "Editor",
			Self::Owner => "Owner",
		}
	}
}

impl std::fmt::Display for Role {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		write!(f, "{}", self.as_str())
	}
}

impl FromStr for Role {
	type Err = Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s.to_ascii_lowercase().as_str() {
			"viewer" => Ok(Self::Viewer),
			"editor" => Ok(Self::Editor),
			"owner" => Ok(Self::Owner),
			_ => Err(Error::InvalidRole(s.to_string())),
		}
	}
}

impl std::convert::From<&Role> for EntityUid {
	fn from(role: &Role) -> Self {
		EntityUid::from_type_name_and_id(
			EntityTypeName::from_str("Role").unwrap(),
			format!("{}", role).parse().unwrap(),
		)
	}
}

impl std::convert::From<&Role> for Entity {
	fn from(role: &Role) -> Self {
		Entity::new(role.into(), Default::default(), Default::default())
	}
}

impl std::convert::From<&Role> for RestrictedExpression {
	fn from(role: &Role) -> Self {
		format!("{}", EntityUid::from(role)).parse().unwrap()
	}
}
