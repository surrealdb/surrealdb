use crate::sql::Duration;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

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

impl Default for UserDuration {
	fn default() -> Self {
		Self {
			// By default, tokens for system users expire after one hour
			token: Some(Duration::from_hours(1)),
			// By default, sessions for system users do not expire
			session: None,
		}
	}
}
