//! PermissionFilter operator: a table's `PERMISSIONS FOR select` predicate,
//! applied per row in the permission frame.
//!
//! The predicate is the definer's, so it is evaluated the way every stored
//! permission predicate is: reads inside it are not themselves
//! permission-checked (a predicate following a record link would otherwise
//! recurse into itself), an effect inside it is rejected (GHSA-66r2-5gwj-gxm2),
//! and it runs under the authority the definition recorded, not the caller's.
//! The same frame is what [`crate::exec::permission::check_permission_for_value`]
//! establishes for a permission resolved at scan time.

use surrealdb_iam::AuthLimit;

use crate::exec::ExecutionContext;
use crate::expr::logical::GuardKind;

/// The context a table's permission predicate is evaluated in: its reads are
/// not themselves permission-checked, a read or write guard refuses an effect
/// a schema clause must not have ([`crate::dbs::NoWriteFrame`]), and it
/// runs under the authority the definition recorded when there is one. The
/// predicate is definer code, so the frame is privileged
/// ([`ExecutionContext::privileged_frame`]): it holds none of the caller's
/// bindings. Every engine that applies a `PermissionFilter` evaluates its
/// predicate here.
pub(crate) fn permission_frame(
	ctx: &ExecutionContext,
	guard: GuardKind,
	auth: Option<&AuthLimit>,
) -> ExecutionContext {
	let frame = ctx.clone().privileged_frame().with_skip_fetch_perms(true);
	let frame = match guard {
		GuardKind::Read => frame.refusing_writes(crate::dbs::NoWriteFrame::PermissionPredicate),
		GuardKind::Write => frame.refusing_writes(crate::dbs::NoWriteFrame::WritePermission),
	};
	match auth {
		Some(limit) => frame.with_limited_auth(limit),
		None => frame,
	}
}
