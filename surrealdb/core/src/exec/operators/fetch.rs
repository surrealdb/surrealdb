// Runtime that compiled programs call to run FETCH and record dereferences.
#![allow(dead_code)]

use surrealdb_strand::Strand;

use crate::catalog::providers::TableProvider;
use crate::exec::ExecutionContext;
use crate::exec::fan_out::{RECORD_DEREFERENCE, evaluate_each};
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, should_check_perms, validate_record_user_access,
};
use crate::expr::ControlFlowExt;
use crate::expr::logical::LogicalExpr;
use crate::iam::Action;
use crate::val::{RecordId, TableName, Value};

/// A [`FetchStep`] before its computed index is compiled: what
/// [`fetch_parts`] reads off a FETCH path for the planner to compile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FetchPart<'a> {
	Field(&'a Strand),
	All,
	First,
	Last,
	Index(&'a LogicalExpr),
	Unsupported,
}

/// The steps of a FETCH path, root first, up to and including the first one
/// the fetch cannot walk.
///
/// A fetch path names a stored position, so it starts from a field of the row
/// and continues through field accesses, `.*`, first/last and computed
/// indices. A `Broadcast` is its base's steps followed by its body's, since
/// the body continues the same written path over each element. Any other
/// form — another root, or a step the fetch cannot walk — is an
/// [`FetchPart::Unsupported`] step, and nothing after it is listed because
/// the walk ends there.
pub(crate) fn fetch_parts(expr: &LogicalExpr) -> Vec<FetchPart<'_>> {
	fn collect<'a>(expr: &'a LogicalExpr, out: &mut Vec<FetchPart<'a>>) {
		match expr {
			LogicalExpr::Field(name) => out.push(FetchPart::Field(name)),
			LogicalExpr::Current => {}
			LogicalExpr::Access {
				base,
				field,
			} => {
				collect(base, out);
				out.push(FetchPart::Field(field));
			}
			LogicalExpr::Each(base) => {
				collect(base, out);
				out.push(FetchPart::All);
			}
			LogicalExpr::First(base) => {
				collect(base, out);
				out.push(FetchPart::First);
			}
			LogicalExpr::Last(base) => {
				collect(base, out);
				out.push(FetchPart::Last);
			}
			LogicalExpr::Index {
				base,
				index,
			} => {
				collect(base, out);
				out.push(FetchPart::Index(index));
			}
			LogicalExpr::Broadcast {
				base,
				body,
				..
			} => {
				collect(base, out);
				collect(body, out);
			}
			other => {
				if let Some(base) = other.step_base() {
					collect(base, out);
				}
				out.push(FetchPart::Unsupported);
			}
		}
	}
	let mut out = Vec::new();
	collect(expr, &mut out);
	if let Some(end) = out.iter().position(|part| *part == FetchPart::Unsupported) {
		out.truncate(end + 1);
	}
	out
}

/// A step of a `FETCH` path as the walk reads it, whatever form the path is
/// held in.
pub(crate) enum WalkStep<'a> {
	/// `.name`.
	Field(&'a Strand),
	/// `.*`.
	All,
	/// The first element of an array.
	First,
	/// `[$]`.
	Last,
	/// `[e]`, a computed index or key, evaluated by [`FetchIndex`].
	Index,
	/// A step the walk does not go through.
	Unsupported,
}

/// A step of a `FETCH` path: the walk reads which step it is.
pub(crate) trait FetchWalk: Sync {
	fn step(&self) -> WalkStep<'_>;
}

/// The value of the computed index of a step, evaluated in `ctx`.
pub(crate) trait FetchIndex<S>: Sync {
	fn index<'a>(
		&'a self,
		ctx: &'a ExecutionContext,
		step: &'a S,
	) -> crate::exec::BoxFut<'a, crate::expr::FlowResult<Value>>;
}

/// Fetch all specified fields for a value, over paths of steps held in any
/// form, their computed indexes evaluated by `index`.
pub(crate) async fn fetch_fields_with<S: FetchWalk, E: FetchIndex<S>>(
	ctx: &ExecutionContext,
	mut value: Value,
	fields: &[Vec<S>],
	index: &E,
) -> crate::expr::FlowResult<Value> {
	for field in fields {
		fetch_field_path(ctx, &mut value, field, index).await?;
	}
	Ok(value)
}

/// Traverse a field path through a value, fetching record IDs along the way.
///
/// Uses an iterative loop for linear path descent (Field on Object, First, Last)
/// and only recurses for fan-out cases (iterating over array/object elements).
/// This avoids per-step heap allocation and reduces stack depth compared to
/// fully recursive traversal.
fn fetch_field_path<'a, S: FetchWalk, E: FetchIndex<S>>(
	ctx: &'a ExecutionContext,
	value: &'a mut Value,
	path: &'a [S],
	index: &'a E,
) -> crate::exec::BoxFut<'a, crate::expr::FlowResult<()>> {
	Box::pin(async move {
		let mut current = value;
		let mut depth = 0usize;

		loop {
			// End of path: fetch the current value if it's a record
			if depth >= path.len() {
				return fetch_value_if_record(ctx, current).await;
			}

			// Mid-path RecordId: fetch in place and retry at the same depth
			if matches!(&*current, Value::RecordId(_)) {
				fetch_value_if_record(ctx, current).await?;
				continue;
			}

			match path[depth].step() {
				WalkStep::Field(name) => match current {
					Value::Object(obj) => {
						current = match obj.get_mut(name.as_str()) {
							Some(child) => child,
							None => return Ok(()),
						};
						depth += 1;
					}
					Value::Array(arr) => {
						// Fan-out: apply the same field path to each array element
						return fetch_each(ctx, arr.iter_mut(), &path[depth..], index).await;
					}
					_ => return Ok(()),
				},
				WalkStep::All => match current {
					Value::Array(arr) => {
						return fetch_each(ctx, arr.iter_mut(), &path[depth + 1..], index).await;
					}
					Value::Object(obj) => {
						return fetch_each(ctx, obj.values_mut(), &path[depth + 1..], index).await;
					}
					_ => return Ok(()),
				},
				WalkStep::First => {
					current = match current {
						Value::Array(arr) => match arr.first_mut() {
							Some(v) => v,
							None => return Ok(()),
						},
						_ => return Ok(()),
					};
					depth += 1;
				}
				WalkStep::Last => {
					current = match current {
						Value::Array(arr) => match arr.last_mut() {
							Some(v) => v,
							None => return Ok(()),
						},
						_ => return Ok(()),
					};
					depth += 1;
				}
				// A computed index/key part, e.g. `refs[0]`, `refs[-1]`, `refs[$i]`.
				// Evaluate it, then mutably index into an array (numeric,
				// negative/out-of-range skips silently) or key into an object
				// (string or numeric-as-string key); anything else is a no-op,
				// matching how other unsupported parts are handled.
				WalkStep::Index => {
					let index_value = index.index(ctx, &path[depth]).await?;
					let next = match (&mut *current, &index_value) {
						(Value::Array(arr), Value::Number(n)) => {
							n.as_array_index().and_then(|idx| arr.get_mut(idx))
						}
						(Value::Object(obj), Value::String(key)) => obj.get_mut(key.as_str()),
						(Value::Object(obj), Value::Number(n)) => obj.get_mut(&n.to_string()),
						_ => None,
					};
					current = match next {
						Some(v) => v,
						None => return Ok(()),
					};
					depth += 1;
				}
				// The fetch does not walk through any other step.
				WalkStep::Unsupported => return Ok(()),
			}
		}
	})
}

/// Fan-out helper: process each value with the remaining path.
///
/// Used when traversal encounters a collection (array or object) that
/// requires applying the remaining field path to each element.
async fn fetch_each<'a, S: FetchWalk, E: FetchIndex<S>>(
	ctx: &'a ExecutionContext,
	values: impl Iterator<Item = &'a mut Value>,
	remaining_path: &'a [S],
	index: &'a E,
) -> crate::expr::FlowResult<()> {
	for item in values {
		fetch_field_path(ctx, item, remaining_path, index).await?;
	}
	Ok(())
}

/// If the value is a RecordId, fetch it and replace the value.
/// If it's an array of RecordIds, fetch each one using batch fetching.
async fn fetch_value_if_record(
	ctx: &ExecutionContext,
	value: &mut Value,
) -> crate::expr::FlowResult<()> {
	match value {
		Value::RecordId(rid) => {
			let fetched = fetch_record(ctx, rid).await?;
			*value = fetched;
		}
		Value::Array(arr) => {
			// Use batch fetch for arrays - more efficient for larger arrays
			batch_fetch_in_place(ctx, arr.as_mut_slice()).await?;
		}
		_ => {}
	}
	Ok(())
}

/// Fetch a single raw record value from the datastore.
///
/// Returns `Ok(Some(val))` with the record's ID injected, or `Ok(None)` if
/// the record does not exist in a table that does.  A record id naming a
/// table that is not defined at `version` fails with `TbNotFound`, as a scan
/// of that table does, whichever position the id is read from — a `FROM`
/// source, a dereference, a `FETCH` target.  The table is only looked up once
/// the record is found missing, so a read that finds its record pays nothing
/// for the check.  This is the single point of record retrieval that all
/// higher-level helpers compose on top of.
pub(crate) async fn fetch_raw_record(
	ctx: &ExecutionContext,
	rid: &RecordId,
	version: Option<u64>,
) -> crate::expr::FlowResult<Option<Value>> {
	fetch_raw_record_in(ctx, rid, version, false).await
}

/// [`fetch_raw_record`] for a dereference — a path or `FETCH` reading fields
/// of the record the id names — where the signed-in record user's own record
/// (see [`is_auth_record`]) reads as absent when its table is not defined,
/// rather than failing with `TbNotFound`.
pub(crate) async fn fetch_raw_deref(
	ctx: &ExecutionContext,
	rid: &RecordId,
	version: Option<u64>,
) -> crate::expr::FlowResult<Option<Value>> {
	fetch_raw_record_in(ctx, rid, version, true).await
}

async fn fetch_raw_record_in(
	ctx: &ExecutionContext,
	rid: &RecordId,
	version: Option<u64>,
	auth_reads_none: bool,
) -> crate::expr::FlowResult<Option<Value>> {
	let db_ctx = ctx.database().context("fetch_raw_record requires database context")?;
	let txn = db_ctx.txn();
	let record = txn
		.get_record(
			db_ctx.ns_ctx.ns.namespace_id,
			db_ctx.db.database_id,
			&rid.table,
			&rid.key,
			version,
		)
		.await
		.context("Failed to fetch record")?;
	if record.data.is_none() {
		if !(auth_reads_none && is_auth_record(ctx, rid))
			&& db_ctx
				.table_is_undefined(&rid.table, version)
				.await
				.context("Failed to get table definition")?
		{
			return Err(crate::expr::ControlFlow::Err(anyhow::Error::new(
				crate::catalog::Error::TbNotFound {
					name: rid.table.clone(),
				},
			)));
		}
		return Ok(None);
	}

	Ok(Some(record.data.clone()))
}

/// Fetch a single raw record value as a locked read, registering the
/// record key for commit-time conflict detection.
///
/// Reads exactly what [`fetch_raw_record`] reads at the current version,
/// except that the read is always issued to the underlying datastore —
/// never served from the transaction read cache — so the conflict
/// registration reaches the storage engine even for a record that does
/// not exist. There is no versioned form: a locked read of a historical
/// snapshot could never be validated at commit.
pub(crate) async fn fetch_raw_record_for_update(
	ctx: &ExecutionContext,
	rid: &RecordId,
) -> crate::expr::FlowResult<Option<Value>> {
	let db_ctx = ctx.database().context("fetch_raw_record_for_update requires database context")?;
	let txn = db_ctx.txn();
	let record = txn
		.get_record_for_update(
			db_ctx.ns_ctx.ns.namespace_id,
			db_ctx.db.database_id,
			&rid.table,
			&rid.key,
		)
		.await
		.context("Failed to fetch record")?;
	if record.data.is_none() {
		return Ok(None);
	}

	Ok(Some(record.data.clone()))
}

/// Process a fetched record: check table-level permissions, evaluate computed
/// fields, and apply field-level permissions.
///
/// Returns `Ok(true)` if the record passes all permission checks, or
/// `Ok(false)` if the record should be hidden (Deny or failed Conditional).
/// Computed fields are always evaluated (even without permissions) using the
/// same `FieldState` + `compute_fields_for_value` path as the scan pipeline,
/// ensuring a single code path for computed field evaluation throughout the
/// executor.
pub(crate) async fn process_fetched_record(
	ctx: &ExecutionContext,
	rid: &RecordId,
	val: &mut Value,
) -> crate::expr::FlowResult<bool> {
	process_fetched_record_for(ctx, rid, val, None).await
}

/// [`process_fetched_record`] for a reader of the record's fields `needed`: the computed fields
/// derived are those `needed` names, with the ones they read and the ones a field permission reads
/// ([`super::scan::pipeline::filter_field_state_for_projection`]); every one where `needed` is
/// `None`.
pub(crate) async fn process_fetched_record_for(
	ctx: &ExecutionContext,
	rid: &RecordId,
	val: &mut Value,
	needed: Option<&std::collections::HashSet<String>>,
) -> crate::expr::FlowResult<bool> {
	let db_ctx = ctx.database().context("process_fetched_record requires database context")?;
	// SECURITY: confine record materialization to the caller's
	// tenant. This path serves record dereference and the FETCH clause, which
	// resolve records by id with no preceding scan, so it is the fetch-side
	// equivalent of the scan-operator tenant guard.
	validate_record_user_access(db_ctx)?;
	let check_perms =
		should_check_perms(db_ctx, Action::View).context("Failed to check permissions")?;

	// 1. Table-level permission check, before computing fields for the caller: the guard derives
	//    the computed fields it reads on a copy of its own (`ComputedFieldGuard`), so `val` is
	//    still as stored.
	if check_perms {
		let select_perm = table_select_permission(ctx, &rid.table).await?;

		match &select_perm {
			PhysicalPermission::Deny => return Ok(false),
			PhysicalPermission::Allow => {}
			PhysicalPermission::Conditional(_) => {
				let allowed = check_permission_for_value(&select_perm, val, None, ctx)
					.await
					.context("Permission check failed")?;
				if !allowed {
					return Ok(false);
				}
			}
		}
	}

	// 2. Build FieldState (computed fields + field permissions)
	let field_state =
		super::scan::pipeline::build_field_state(ctx, &rid.table, check_perms, needed).await?;
	let field_state = reading_own_record(ctx, rid, field_state);

	// 3. Evaluate computed fields and apply field-level permissions
	super::scan::pipeline::materialise_fields_with_permissions(
		ctx,
		&field_state,
		val,
		false,
		check_perms,
	)
	.await?;

	Ok(true)
}

/// The table `SELECT` permission a record of `table` read by id is checked
/// against: the table descriptor's, compiled once per table version
/// ([`crate::exec::vm::descriptor::select_permission`]), so every record of
/// the table fetched — a `FETCH` target, a record dereference — is checked
/// against the same compiled guard.
pub(crate) async fn table_select_permission(
	ctx: &ExecutionContext,
	table: &TableName,
) -> crate::expr::FlowResult<PhysicalPermission> {
	crate::exec::vm::descriptor::select_permission(ctx, table).await
}

/// Fetch a single record by its ID without permission checks, but with
/// computed fields evaluated.
///
/// Used during permission predicate evaluation to prevent reentrant
/// permission checks that would recurse infinitely on cyclic links.
/// Computed fields are still evaluated so that permission expressions
/// referencing computed fields on linked records work correctly.
pub(crate) async fn fetch_record_no_perms(
	ctx: &ExecutionContext,
	rid: &RecordId,
) -> crate::expr::FlowResult<Value> {
	// When the program has a unit for the record's table,
	// the record is read through that unit.
	if let Some(deref) = ctx.vm_deref()
		&& let Some(fetched) = deref.fetch(ctx, rid, false).await
	{
		return fetched;
	}
	let Some(mut val) = fetch_raw_deref(ctx, rid, ctx.version_stamp()).await? else {
		return Ok(Value::None);
	};
	let field_state =
		super::scan::pipeline::build_field_state(ctx, &rid.table, false, None).await?;
	let field_state = reading_own_record(ctx, rid, field_state);
	super::scan::pipeline::compute_fields_for_value(ctx, &field_state, &mut val, true).await?;
	Ok(val)
}

/// `state`, the field state of the table `rid` is a record of, as it applies
/// to a read of `rid` made from inside one of its own computed fields: the
/// record is read as stored, with no computed field derived, since deriving
/// them is what the read is part of. A read of any other record, or of this one
/// outside its computed fields, derives them all.
fn reading_own_record(
	ctx: &ExecutionContext,
	rid: &RecordId,
	mut state: super::scan::pipeline::FieldState,
) -> super::scan::pipeline::FieldState {
	if ctx.root().computing_record.as_ref().is_some_and(|computing| &computing.rid == rid) {
		state.computed_fields.clear();
	}
	state
}

/// Fetch a single record by its ID, evaluating computed fields and applying
/// table-level and field-level permission checks.
///
/// Uses the transaction record cache so repeated fetches of the same record
/// within a transaction only hit the datastore once.
pub(crate) async fn fetch_record(
	ctx: &ExecutionContext,
	rid: &RecordId,
) -> crate::expr::FlowResult<Value> {
	// When the program has a unit for the record's table,
	// the record is read through that unit.
	// A computed field whose value does not coerce to its kind fails the
	// dereference naming the field.
	if let Some(deref) = ctx.vm_deref()
		&& let Some(fetched) = deref.fetch(ctx, rid, true).await
	{
		return fetched.map_err(computed_coercion);
	}
	let Some(mut val) = fetch_raw_deref(ctx, rid, ctx.version_stamp()).await? else {
		require_table(ctx, rid).await?;
		return Ok(Value::None);
	};
	if !process_fetched_record(ctx, rid, &mut val).await? {
		return Ok(Value::None);
	}
	Ok(val)
}

/// `failure` raised dereferencing a record, a computed field's failed
/// coercion named as the dereference names it.
fn computed_coercion(failure: crate::expr::ControlFlow) -> crate::expr::ControlFlow {
	match failure {
		crate::expr::ControlFlow::Err(e) => match e.downcast_ref::<crate::doc::Error>() {
			Some(crate::doc::Error::FieldCoerce {
				field_name,
				..
			}) => crate::expr::ControlFlow::Err(anyhow::anyhow!(
				"Failed to coerce computed field '{field_name}'"
			)),
			_ => crate::expr::ControlFlow::Err(e),
		},
		other => other,
	}
}

/// Fetch several records by id, overlapping the fetches.
///
/// The transaction cache deduplicates repeated ids automatically. The only
/// expressions that run are each target's own, which is what
/// [`RECORD_DEREFERENCE`] names.
///
/// # Arguments
///
/// * `ctx` - The execution context containing the transaction
/// * `rids` - A slice of RecordIds to fetch
///
/// # Returns
///
/// A vector of Values in the same order as the input RecordIds.
/// Missing records are returned as `Value::None`.
pub(crate) async fn batch_fetch_records(
	ctx: &ExecutionContext,
	rids: &[RecordId],
) -> crate::expr::FlowResult<Vec<Value>> {
	evaluate_each(ctx, RECORD_DEREFERENCE, rids, |rid| fetch_record(ctx, rid)).await
}

/// Batch fetch records and replace RecordIds in an array in place.
///
/// This is a convenience function that modifies an array of values,
/// replacing any RecordIds with their fetched record data.
///
/// # Arguments
///
/// * `ctx` - The execution context containing the transaction
/// * `values` - A mutable slice of Values to process
///
/// # Note
///
/// This function collects all RecordIds first, fetches them in batch,
/// then replaces them in the original array. Non-RecordId values are
/// left unchanged.
pub(crate) async fn batch_fetch_in_place(
	ctx: &ExecutionContext,
	values: &mut [Value],
) -> crate::expr::FlowResult<()> {
	// Collect indices and RecordIds to fetch
	let to_fetch: Vec<(usize, RecordId)> = values
		.iter()
		.enumerate()
		.filter_map(|(i, v)| {
			if let Value::RecordId(rid) = v {
				Some((i, rid.clone()))
			} else {
				None
			}
		})
		.collect();

	if to_fetch.is_empty() {
		return Ok(());
	}

	// Extract just the RecordIds for batch fetching
	let rids: Vec<RecordId> = to_fetch.iter().map(|(_, rid)| rid.clone()).collect();

	// Batch fetch all records
	let fetched = batch_fetch_records(ctx, &rids).await?;

	// Replace RecordIds with fetched values
	for ((idx, _), fetched_value) in to_fetch.into_iter().zip(fetched) {
		values[idx] = fetched_value;
	}

	Ok(())
}

/// Whether `rid` is the record `$auth` holds for the signed-in record user.
///
/// A dereference reads this record's fields to judge the caller against its
/// own attributes (`PERMISSIONS ... WHERE author = $auth.id`), and neither the
/// record nor its table need exist: a dereference that finds no record yields
/// `NONE` for every field, and a comparison against `NONE` grants nothing a
/// comparison against a stored value would not. Only a dereference of this
/// record is exempt; every other record id naming a table that is not defined
/// fails with `TbNotFound`.
pub(crate) fn is_auth_record(ctx: &ExecutionContext, rid: &RecordId) -> bool {
	matches!(ctx.value("auth"), Some(Value::RecordId(auth)) if auth == rid)
}

/// Raise `TbNotFound` when the table of `rid` does not exist, unless `rid` is
/// the signed-in record user's own record ([`is_auth_record`]).
///
/// Reading a record through a table that does not exist is an error wherever
/// the read happens — a `FROM` source, a record link followed by a path, a
/// `FETCH`, a destructure — as it is for a point read (`RecordIdScan`) and a
/// table scan. A table that exists but holds no such record reads as `NONE`.
/// Asked only once the record is absent, since a record cannot be present in
/// a table that is not; a cached absence is re-read from the transaction
/// before it is believed (`DatabaseContext::table_is_undefined`).
pub(crate) async fn require_table(
	ctx: &ExecutionContext,
	rid: &RecordId,
) -> crate::expr::FlowResult<()> {
	let db_ctx = ctx.database().context("A record read requires a database context")?;
	if !is_auth_record(ctx, rid)
		&& db_ctx
			.table_is_undefined(&rid.table, ctx.version_stamp())
			.await
			.context("Failed to get table")?
	{
		return Err(crate::expr::ControlFlow::Err(anyhow::Error::new(
			crate::catalog::Error::TbNotFound {
				name: rid.table.clone(),
			},
		)));
	}
	Ok(())
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::exec::operators::test_util::{parse_expr, parse_path};

	#[test]
	fn a_fetch_path_is_read_root_first_through_its_broadcasts() {
		let path = parse_path("a.b[0].c.*[$i]");
		let parts = fetch_parts(&path);
		let index = match parts.as_slice() {
			[
				FetchPart::Field(a),
				FetchPart::Field(b),
				FetchPart::Index(index),
				FetchPart::Field(c),
				FetchPart::All,
				FetchPart::Index(i),
			] => {
				assert_eq!(a.as_str(), "a");
				assert_eq!(b.as_str(), "b");
				assert_eq!(c.as_str(), "c");
				assert_eq!(**i, parse_expr("$i"));
				index
			}
			other => panic!("unexpected steps {other:?}"),
		};
		assert_eq!(**index, LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::Integer(0)));
	}

	#[test]
	fn a_step_the_fetch_cannot_walk_ends_the_path() {
		assert_eq!(
			fetch_parts(&parse_path("a[WHERE x].b")),
			vec![FetchPart::Field(&Strand::new_static("a")), FetchPart::Unsupported]
		);
		assert_eq!(
			fetch_parts(&parse_path("a[$].b")),
			vec![
				FetchPart::Field(&Strand::new_static("a")),
				FetchPart::Last,
				FetchPart::Field(&Strand::new_static("b"))
			]
		);
		// A path not rooted at a field of the row fetches nothing.
		assert_eq!(fetch_parts(&parse_path("$p.a")), vec![FetchPart::Unsupported]);
	}

	/// The guard of a table is compiled once per execution: two records of
	/// the same table are checked against the same compiled predicate.
	#[tokio::test]
	async fn a_tables_guard_is_compiled_once_per_execution() {
		use crate::exec::operators::test_util::TestDb;
		let db = TestDb::new_with_auth(
			"DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE public = true;
			 DEFINE TABLE other SCHEMALESS PERMISSIONS FOR select WHERE public = true;",
		)
		.await;
		let anonymous = crate::dbs::Session::default().with_ns("test").with_db("test");
		let ctx = db.exec_ctx_as(&anonymous, crate::kvs::TransactionType::Read).await;
		let doc = TableName::from("doc");
		let first = table_select_permission(&ctx, &doc).await.expect("compiles");
		let second = table_select_permission(&ctx, &doc).await.expect("cached");
		let (PhysicalPermission::Conditional(first), PhysicalPermission::Conditional(second)) =
			(&first, &second)
		else {
			panic!("a conditional guard: {first:?} / {second:?}");
		};
		assert!(first.ptr_eq(second), "the second fetch reuses the compiled guard");
		// Another table compiles its own.
		let other =
			table_select_permission(&ctx, &TableName::from("other")).await.expect("compiles");
		let PhysicalPermission::Conditional(other) = &other else {
			panic!("a conditional guard: {other:?}");
		};
		assert!(!first.ptr_eq(other));
		// A context sharing the execution shares the cache.
		let shared = ctx.with_param("x", Value::from(1));
		let again = table_select_permission(&shared, &doc).await.expect("cached");
		let PhysicalPermission::Conditional(again) = &again else {
			panic!("a conditional guard: {again:?}");
		};
		assert!(first.ptr_eq(again));
	}
}
