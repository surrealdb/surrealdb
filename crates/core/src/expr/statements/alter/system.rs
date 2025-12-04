use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::statements::alter::AlterKind;
use crate::expr::{Base, Expr, FlowResultExt};
use crate::iam::{Action, ResourceKind};
use crate::kvs::Key;
use crate::val::{Duration, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct AlterSystemStatement {
	pub query_timeout: AlterKind<Expr>,
	pub compact: bool,
}

impl AlterSystemStatement {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> anyhow::Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Any, &Base::Root)?;
		// Are we doing compaction?
		if self.compact {
			ctx.tx().compact::<Key>(None).await?;
		}
		match &self.query_timeout {
			AlterKind::None => {}
			AlterKind::Set(timeout) => {
				let timeout = stk
					.run(|stk| timeout.compute(stk, ctx, opt, doc))
					.await
					.catch_return()?
					.cast_to::<Duration>()?;
				opt.dynamic_configuration().set_query_timeout(Some(timeout.0));
			}
			AlterKind::Drop => {
				opt.dynamic_configuration().set_query_timeout(None);
			}
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
