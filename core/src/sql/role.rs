use crate::iam;
use crate::sql::statements::info::InfoStructure;
use crate::sql::Value;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Role {
	#[default]
	Viewer,
	Editor,
	Owner,
}

impl From<iam::Role> for Role {
	fn from(val: iam::Role) -> Self {
		match val {
			iam::Role::Viewer => Role::Viewer,
			iam::Role::Editor => Role::Editor,
			iam::Role::Owner => Role::Owner,
		}
	}
}

impl From<&iam::Role> for Role {
	fn from(val: &iam::Role) -> Self {
		match val {
			iam::Role::Viewer => Role::Viewer,
			iam::Role::Editor => Role::Editor,
			iam::Role::Owner => Role::Owner,
		}
	}
}

impl From<Role> for iam::Role {
	fn from(val: Role) -> Self {
		match val {
			Role::Viewer => iam::Role::Viewer,
			Role::Editor => iam::Role::Editor,
			Role::Owner => iam::Role::Owner,
		}
	}
}

impl From<&Role> for iam::Role {
	fn from(val: &Role) -> Self {
		match val {
			Role::Viewer => iam::Role::Viewer,
			Role::Editor => iam::Role::Editor,
			Role::Owner => iam::Role::Owner,
		}
	}
}

impl Role {
	pub fn as_str(&self) -> &'static str {
		iam::Role::from(self).as_str()
	}
}

impl fmt::Display for Role {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", iam::Role::from(self))
	}
}

impl InfoStructure for Role {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}
