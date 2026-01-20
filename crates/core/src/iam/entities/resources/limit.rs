use std::sync::Arc;

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::dbs::Options;
use crate::iam::{Auth, Level, Role};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct AuthLimit {
	pub level: Level,
	pub role: Option<Role>,
}

impl Default for AuthLimit {
	fn default() -> Self {
		Self {
			level: Level::No,
			role: None,
		}
	}
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

	pub fn new_no_limit() -> Self {
		Self {
			level: Level::Root,
			role: Some(Role::Owner),
		}
	}

	pub fn limit_opt(&self, opt: &Options) -> Options {
		let mut opt = opt.clone();
		opt.auth = Arc::new(opt.auth.as_ref().new_limited(self));
		opt
	}
}
