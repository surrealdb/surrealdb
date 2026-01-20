use revision::revisioned;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum AuthLevel {
	No,
	Root,
	Namespace(String),
	Database(String, String),
	Record(String, String, String),
}

impl From<crate::iam::Level> for AuthLevel {
	fn from(value: crate::iam::Level) -> Self {
		match value {
			crate::iam::Level::No => Self::No,
			crate::iam::Level::Root => Self::Root,
			crate::iam::Level::Namespace(ns) => Self::Namespace(ns),
			crate::iam::Level::Database(ns, db) => Self::Database(ns, db),
			crate::iam::Level::Record(ns, db, id) => Self::Record(ns, db, id),
		}
	}
}

impl From<&AuthLevel> for crate::iam::Level {
	fn from(value: &AuthLevel) -> Self {
		match value {
			AuthLevel::No => crate::iam::Level::No,
			AuthLevel::Root => crate::iam::Level::Root,
			AuthLevel::Namespace(ns) => crate::iam::Level::Namespace(ns.clone()),
			AuthLevel::Database(ns, db) => crate::iam::Level::Database(ns.clone(), db.clone()),
			AuthLevel::Record(ns, db, id) => {
				crate::iam::Level::Record(ns.clone(), db.clone(), id.clone())
			}
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AuthLimit {
	pub level: AuthLevel,
	pub role: Option<String>,
}

impl Default for AuthLimit {
	fn default() -> Self {
		Self {
			level: AuthLevel::No,
			role: None,
		}
	}
}

impl AuthLimit {
	pub fn new(level: AuthLevel, role: Option<String>) -> Self {
		Self {
			level,
			role,
		}
	}

	pub fn new_no_limit() -> Self {
		Self {
			level: AuthLevel::Root,
			role: Some("Owner".to_string()),
		}
	}
}

impl From<crate::iam::AuthLimit> for AuthLimit {
	fn from(value: crate::iam::AuthLimit) -> Self {
		Self {
			level: value.level.into(),
			role: value.role.map(|r| r.to_string()),
		}
	}
}

impl TryFrom<&AuthLimit> for crate::iam::AuthLimit {
	type Error = anyhow::Error;

	fn try_from(value: &AuthLimit) -> anyhow::Result<Self> {
		Ok(Self {
			level: (&value.level).into(),
			role: value
				.role
				.as_ref()
				.map(|r| r.parse().map_err(|e| anyhow::anyhow!("Invalid role: {}", e)))
				.transpose()?,
		})
	}
}
