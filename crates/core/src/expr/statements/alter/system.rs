use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::statements::alter::AlterKind;
use crate::expr::{Base, Timeout};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct AlterSystemStatement {
	pub query_timeout: AlterKind<Timeout>,
	pub compact: bool,
}

impl AlterSystemStatement {
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> anyhow::Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Any, &Base::Root)?;
		// Are we doing compaction?
		if self.compact {
			ctx.tx().compact(None).await?;
		}
		Ok(Value::None)
	}
}

impl ToSql for AlterSystemStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterSystemStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
