use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::statements::alter::AlterKind;
use crate::expr::{Base, Expr, FlowResultExt};
use crate::iam::{Action, ResourceKind};
use crate::kvs::Key;
use crate::val::{Duration, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, priority_lfu::DeepSizeOf)]
/// Alters system-wide settings and maintenance operations.
///
/// Supported operations:
/// - `query_timeout`: set/drop a global query timeout which is enforced across queries. The value
///   is evaluated as a `Duration` expression at runtime.
/// - `compact`: runs a storage compaction across the entire datastore.
pub(crate) struct AlterSystemStatement {
	/// Global query timeout alteration. `Set` evaluates an expression to a
	/// `Duration`; `Drop` clears the timeout; `None` leaves it unchanged.
	pub query_timeout: AlterKind<Expr>,
	/// When true, triggers a datastore-wide compaction.
	pub compact: bool,
}

impl AlterSystemStatement {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
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
