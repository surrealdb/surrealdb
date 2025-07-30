use std::time::Duration;


use serde::{Deserialize, Serialize};

use revision::{revisioned, Revisioned};

use crate::catalog::scope::Scope;


#[revisioned(revision = 4)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct UserDefinition {
	pub name: String,
	pub scope: Scope,
	pub hash: String,
	pub code: String,
	pub roles: Vec<UserRole>,
	pub duration: UserDuration,
	pub comment: Option<String>,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct UserRole(pub String);


#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
// Durations representing the expiration of different elements of user authentication
// In this context, the None variant represents that the element does not expire
pub struct UserDuration {
	// Duration after which the token obtained after authenticating with user credentials expires
	pub token: Option<Duration>,
	// Duration after which the session authenticated with user credentials or token expires
	pub session: Option<Duration>,
}
