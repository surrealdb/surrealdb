use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::Base;
use crate::expr::statements::alter::database::AlterDatabaseStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::schema::DbRoot;
use crate::val::Value;

/// Computes the effect of the `ALTER DATABASE` statement.
///
/// Permissions: requires `Action::Edit` on `ResourceKind::Database`.
///
/// Side effects:
/// - If `compact` is true, compacts the underlying storage for the current namespace+database.
pub(crate) async fn alter_database_statement_compute(
	this: &AlterDatabaseStatement,
	ctx: &Context,
	opt: &Options,
) -> anyhow::Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Database, Base::Ns)?;
	// Extract ids
	let (namespace_id, database_id) = ctx.expect_ns_db_ids(opt).await?;
	// Do we request compacting?
	if this.compact {
		let database_root = DbRoot {
			ns: namespace_id,
			db: database_id,
		};
		ctx.tx().compact(&database_root).await?;
	}
	// Ok all good
	Ok(Value::None)
}
