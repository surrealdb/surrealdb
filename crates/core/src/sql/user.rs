use crate::val::Duration;

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
// Durations representing the expiration of different elements of user
// authentication In this context, the None variant represents that the element
// does not expire
pub struct UserDuration {
	// Duration after which the token obtained after authenticating with user credentials expires
	pub token: Option<Duration>,
	// Duration after which the session authenticated with user credentials or token expires
	pub session: Option<Duration>,
}

/*
impl Default for UserDuration {
	fn default() -> Self {
		Self {
			// By default, tokens expire after one hour
			token: Some(Duration::from_hours(1).expect("1 hour should fit in a duration")),
			// By default, sessions do not expire
			session: None,
		}
	}
}
*/

impl From<UserDuration> for crate::expr::user::UserDuration {
	fn from(v: UserDuration) -> Self {
		crate::expr::user::UserDuration {
			token: v.token,
			session: v.session,
		}
	}
}
impl From<crate::expr::user::UserDuration> for UserDuration {
	fn from(v: crate::expr::user::UserDuration) -> Self {
		UserDuration {
			token: v.token,
			session: v.session,
		}
	}
}
