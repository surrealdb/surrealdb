use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, priority_lfu::DeepSizeOf)]
/// Executes `ALTER DATABASE` operations.
///
/// Supported options:
/// - `compact`: triggers a compaction of the current database keyspace.
pub(crate) struct AlterDatabaseStatement {
	pub compact: bool,
}

impl AlterDatabaseStatement {
	/// Computes the effect of the `ALTER DATABASE` statement.
	///
	/// Permissions: requires `Action::Edit` on `ResourceKind::Database`.
	///
	/// Side effects:
	/// - If `compact` is true, compacts the underlying storage for the current namespace+database.
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> anyhow::Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Database, &Base::Ns)?;
		// Extract ids
		let (namespace_id, database_id) = ctx.expect_ns_db_ids(opt).await?;
		// Do we request compacting?
		if self.compact {
			let database_root = crate::key::database::all::new(namespace_id, database_id);
			ctx.tx().compact(Some(database_root)).await?;
		}
		// Ok all good
		Ok(Value::None)
	}
}

impl ToSql for AlterDatabaseStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterDatabaseStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
