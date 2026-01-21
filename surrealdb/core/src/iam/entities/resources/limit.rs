use std::sync::Arc;

use crate::dbs::Options;
use crate::iam::{Auth, Level, Role};

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

	pub fn limit_opt(&self, opt: &Options) -> Options {
		let mut opt = opt.clone();
		opt.auth = Arc::new(opt.auth.as_ref().new_limited(self));
		opt
	}
}
