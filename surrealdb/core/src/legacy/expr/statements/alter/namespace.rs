use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::Base;
use crate::expr::statements::alter::namespace::AlterNamespaceStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::schema::NsRoot;
use crate::val::Value;

/// Computes the effect of the `ALTER NAMESPACE` statement.
///
/// Permissions: requires `Action::Edit` on `ResourceKind::Namespace`.
///
/// Side effects:
/// - If `compact` is true, compacts the underlying storage for the current namespace.
pub(crate) async fn alter_namespace_statement_compute(
	this: &AlterNamespaceStatement,
	ctx: &Context,
	opt: &Options,
) -> anyhow::Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Namespace, Base::Root)?;
	// Extract ids
	let namespace_id = ctx.expect_ns_id(opt).await?;
	// Do we request compacting?
	if this.compact {
		let namespace_root = NsRoot {
			ns: namespace_id,
		};
		ctx.tx().compact(&namespace_root).await?;
	}
	// Ok all good
	Ok(Value::None)
}
