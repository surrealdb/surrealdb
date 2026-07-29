use crate::{Auth, Level, Role};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AuthLimit {
	pub level: Level,
	pub role: Option<Role>,
}

impl AuthLimit {
	pub fn new(level: Level, role: Option<Role>) -> Self {
		Self {
			level,
			role,
		}
	}

	pub fn new_from_auth(auth: &Auth) -> Self {
		Self {
			level: auth.level().clone(),
			role: auth.max_role(),
		}
	}
}
