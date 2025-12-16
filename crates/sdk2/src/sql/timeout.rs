use surrealdb_types::Duration;

use crate::sql::{BuildSql, BuildSqlContext};

#[derive(Clone, Default)]
pub struct Timeout(pub(crate) Option<Duration>);

impl BuildSql for Timeout {
	fn build(self, ctx: &mut BuildSqlContext) {
		if let Some(timeout) = self.0 {
			ctx.push(format!(" TIMEOUT {timeout}"));
		}
	}
}

pub trait IntoTimeout {
	fn build(self, timeout: &mut Timeout);
}

impl IntoTimeout for Duration {
	fn build(self, timeout: &mut Timeout) {
		timeout.0 = Some(self);
	}
}

impl IntoTimeout for &Duration {
	fn build(self, timeout: &mut Timeout) {
		timeout.0 = Some(self.clone().into());
	}
}

impl IntoTimeout for std::time::Duration {
	fn build(self, timeout: &mut Timeout) {
		timeout.0 = Some(Duration::from(self));
	}
}

impl IntoTimeout for &std::time::Duration {
	fn build(self, timeout: &mut Timeout) {
		timeout.0 = Some(Duration::from(self.clone()));
	}
}