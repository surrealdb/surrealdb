use std::str::FromStr;

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::expr::Ident;
use crate::iam::Error;

// In the future, we will allow for custom roles. For now, provide predefined
// roles.
#[revisioned(revision = 1)]
#[derive(Hash, Copy, Clone, Default, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Role {
	#[default]
	Viewer,
	Editor,
	Owner,
}

impl std::fmt::Display for Role {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		match self {
			Self::Viewer => write!(f, "Viewer"),
			Self::Editor => write!(f, "Editor"),
			Self::Owner => write!(f, "Owner"),
		}
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

impl std::convert::From<Role> for Ident {
	fn from(role: Role) -> Self {
		match role {
			Role::Viewer => unsafe { Ident::new_unchecked("Viewer".to_owned()) },
			Role::Editor => unsafe { Ident::new_unchecked("Editor".to_owned()) },
			Role::Owner => unsafe { Ident::new_unchecked("Owner".to_owned()) },
		}
	}
}
