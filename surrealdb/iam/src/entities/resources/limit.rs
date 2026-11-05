use crate::{Auth, Level, Role};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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

	/// Whether this limit grants nothing `other` does not: its level is a sublevel of `other`'s,
	/// and its role cap is no higher, where no cap is the highest. Every limit is within itself.
	pub fn is_within(&self, other: &AuthLimit) -> bool {
		let role = match (&self.role, &other.role) {
			(_, None) => true,
			(None, Some(_)) => false,
			(Some(own), Some(cap)) => own <= cap,
		};
		role && self.level.sublevel_of(&other.level)
	}
}

#[cfg(test)]
mod tests {
	use super::AuthLimit;
	use crate::{Level, Role};

	#[test]
	fn a_limit_is_within_itself_and_any_wider_one() {
		let db = AuthLimit::new(Level::Database("n".into(), "d".into()), Some(Role::Editor));
		let ns = AuthLimit::new(Level::Namespace("n".into()), Some(Role::Owner));
		assert!(db.is_within(&db));
		assert!(db.is_within(&ns));
		assert!(!ns.is_within(&db));
		let uncapped = AuthLimit::new(Level::Namespace("n".into()), None);
		assert!(ns.is_within(&uncapped));
		assert!(!uncapped.is_within(&ns));
		let elsewhere = AuthLimit::new(Level::Database("m".into(), "d".into()), Some(Role::Viewer));
		assert!(!elsewhere.is_within(&ns));
	}
}
