use crate::val::Duration;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::str;

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
// Durations representing the expiration of different elements of the access
// method In this context, the None variant represents that the element does not
// expire
pub struct AccessDuration {
	// Duration after which the grants generated with the access method expire
	// For access methods whose grants are tokens, this value is irrelevant
	pub grant: Option<Duration>,
	// Duration after which the tokens obtained with the access method expire
	// For access methods that cannot issue tokens, this value is irrelevant
	pub token: Option<Duration>,
	// Duration after which the session authenticated with the access method expires
	pub session: Option<Duration>,
}

impl Default for AccessDuration {
	fn default() -> Self {
		Self {
			// By default, access grants expire in 30 days.
			grant: Some(Duration::from_days(30).expect("30 days should fit in a duration")),
			// By default, tokens expire after one hour
			token: Some(Duration::from_hours(1).expect("1 hour should fit in a duration")),
			// By default, sessions do not expire
			session: None,
		}
	}
}
