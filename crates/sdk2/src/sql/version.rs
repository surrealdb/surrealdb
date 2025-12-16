use surrealdb_types::Datetime;

use crate::sql::{BuildSql, BuildSqlContext};

#[derive(Clone, Default)]
pub struct Version(pub(crate) Option<Datetime>);

impl BuildSql for Version {
	fn build(self, ctx: &mut BuildSqlContext) {
		if let Some(version) = self.0 {
			ctx.push(format!(" VERSION {version}"));
		}
	}
}

pub trait IntoVersion {
	fn build(self, version: &mut Version);
}

impl IntoVersion for Datetime {
	fn build(self, version: &mut Version) {
		version.0 = Some(self);
	}
}

impl IntoVersion for chrono::DateTime<chrono::Utc> {
	fn build(self, version: &mut Version) {
		version.0 = Some(Datetime::from(self));
	}
}

impl IntoVersion for &Datetime {
	fn build(self, version: &mut Version) {
		version.0 = Some(self.clone());
	}
}

impl IntoVersion for &chrono::DateTime<chrono::Utc> {
	fn build(self, version: &mut Version) {
		version.0 = Some(Datetime::from(self.clone()));
	}
}