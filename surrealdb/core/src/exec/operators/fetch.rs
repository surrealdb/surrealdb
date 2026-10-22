// Runtime that compiled programs call to run FETCH and record dereferences.
#![allow(dead_code)]

use std::future::Future;

use surrealdb_strand::Strand;

use crate::exec::ExecutionContext;
use crate::exec::fan_out::{RECORD_DEREFERENCE, evaluate_each};
use crate::exec::operators::link_window::{LinkPool, RowsWindow, Slot, Windows};
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
			other @ (LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Doc
			| LogicalExpr::Flatten {
				..
			}
			| LogicalExpr::Filter {
				..
			}
			| LogicalExpr::Method {
				..
			}
			| LogicalExpr::Destructure {
				..
			}
			| LogicalExpr::Optional {
				..
			}
			| LogicalExpr::Lookup {
				..
			}
			| LogicalExpr::Recurse(_)
			| LogicalExpr::RepeatRecurse(_)
			| LogicalExpr::SetField {
				..
			}
			| LogicalExpr::Prefix {
				..
			}
			| LogicalExpr::Postfix {
				..
			}
			| LogicalExpr::Invoke {
				..
			}
			| LogicalExpr::Binary {
				..
			}
			| LogicalExpr::Call(_)
			| LogicalExpr::Closure(_)
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_)) => {
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

/// How a `FETCH` reads the records its paths reach.
#[derive(Clone, Copy, Default)]
pub(crate) struct FetchLinks<'a> {
	/// The windows a collection of links is read through, where the physical plan admitted
	/// them; none reads each link for itself.
	pub(crate) pool: Option<&'a LinkPool>,
	/// The records of the links of a batch of source rows, read before the rows ran: a position
	/// that names one of them reads it from here.
	pub(crate) ahead: Option<&'a RowsWindow>,
}

/// Fetch all specified fields for a value, over paths of steps held in any
/// form, their computed indexes evaluated by `index`.
pub(crate) async fn fetch_fields_with<S: FetchWalk, E: FetchIndex<S>>(
	ctx: &ExecutionContext,
	mut value: Value,
	fields: &[Vec<S>],
	index: &E,
	links: FetchLinks<'_>,
) -> crate::expr::FlowResult<Value> {
	for field in fields {
		fetch_field_path(ctx, &mut value, field, index, links, Slot::Unread).await?;
	}
	Ok(value)
}

/// Traverse a field path through a value, fetching record IDs along the way.
///
/// Uses an iterative loop for linear path descent (Field on Object, First, Last)
/// and only recurses for fan-out cases (iterating over array/object elements).
/// This avoids per-step heap allocation and reduces stack depth compared to
/// fully recursive traversal.
///
/// `first` is what a window holds for `value`, read when `value` is a record id: it serves the
/// first record the walk fetches, and every later fetch of the walk reads for itself.
fn fetch_field_path<'a, S: FetchWalk, E: FetchIndex<S>>(
	ctx: &'a ExecutionContext,
	value: &'a mut Value,
	path: &'a [S],
	index: &'a E,
	links: FetchLinks<'a>,
	first: Slot<'a>,
) -> crate::exec::BoxFut<'a, crate::expr::FlowResult<()>> {
	Box::pin(async move {
		let mut current = value;
		let mut depth = 0usize;
		let mut first = first;

		loop {
			// End of path: fetch the current value if it's a record
			if depth >= path.len() {
				return fetch_value_if_record(ctx, current, links, first).await;
			}

			// Mid-path RecordId: fetch in place and retry at the same depth
			if matches!(&*current, Value::RecordId(_)) {
				fetch_value_if_record(
					ctx,
					current,
					links,
					std::mem::replace(&mut first, Slot::Unread),
				)
				.await?;
				continue;
			}

			match path[depth].step() {
				WalkStep::Field(name) => {
					if let Value::Object(obj) = current {
						current = match obj.get_mut(name.as_str()) {
							Some(child) => child,
							None => return Ok(()),
						};
						depth += 1;
					} else if let Value::Array(arr) = current {
						// Fan-out: apply the same field path to each array element
						return fetch_each_element(
							ctx,
							arr.as_mut_slice(),
							&path[depth..],
							index,
							links,
						)
						.await;
					} else {
						return Ok(());
					}
				}
				WalkStep::All => {
					if let Value::Array(arr) = current {
						return fetch_each_element(
							ctx,
							arr.as_mut_slice(),
							&path[depth + 1..],
							index,
							links,
						)
						.await;
					} else if let Value::Object(obj) = current {
						return fetch_each(ctx, obj.values_mut(), &path[depth + 1..], index, links)
							.await;
					} else {
						return Ok(());
					}
				}
				WalkStep::First => {
					let Value::Array(arr) = current else {
						return Ok(());
					};
					let Some(v) = arr.first_mut() else {
						return Ok(());
					};
					current = v;
					depth += 1;
				}
				WalkStep::Last => {
					let Value::Array(arr) = current else {
						return Ok(());
					};
					let Some(v) = arr.last_mut() else {
						return Ok(());
					};
					current = v;
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
/// Used when traversal encounters an object whose values each take the remaining field path.
async fn fetch_each<'a, S: FetchWalk, E: FetchIndex<S>>(
	ctx: &'a ExecutionContext,
	values: impl Iterator<Item = &'a mut Value>,
	remaining_path: &'a [S],
	index: &'a E,
	links: FetchLinks<'a>,
) -> crate::expr::FlowResult<()> {
	for item in values {
		fetch_field_path(ctx, item, remaining_path, index, links, Slot::Unread).await?;
	}
	Ok(())
}

/// Fan-out helper over the elements of an array, in order. A window reads the records of the
/// elements that are record ids a run at a time, where the plan admitted windows and the array
/// holds two or more; each element then takes the remaining path with the slot of its own record.
async fn fetch_each_element<'a, S: FetchWalk, E: FetchIndex<S>>(
	ctx: &'a ExecutionContext,
	elements: &'a mut [Value],
	remaining_path: &'a [S],
	index: &'a E,
	links: FetchLinks<'a>,
) -> crate::expr::FlowResult<()> {
	let include = |rid: &RecordId| links.ahead.is_none_or(|ahead| !ahead.holds(rid));
	let mut windows = Windows::of(links.pool, elements, &include);
	for at in 0..elements.len() {
		let slot = if let Value::RecordId(rid) = &elements[at] {
			match links.ahead.and_then(|ahead| ahead.slot(ctx, rid)) {
				Some(slot) => slot,
				None => match &mut windows {
					Some(windows) => {
						windows.prepare(ctx, elements, at, &include).await?;
						windows.slot(at)
					}
					None => Slot::Unread,
				},
			}
		} else {
			Slot::Unread
		};
		fetch_field_path(ctx, &mut elements[at], remaining_path, index, links, slot).await?;
	}
	Ok(())
}

/// If the value is a RecordId, fetch it and replace the value, reading it from `first` where the
/// slot holds it. If it's an array of RecordIds, fetch each one using batch fetching.
async fn fetch_value_if_record<'s>(
	ctx: &ExecutionContext,
	value: &mut Value,
	links: FetchLinks<'s>,
	first: Slot<'s>,
) -> crate::expr::FlowResult<()> {
	if let Value::RecordId(rid) = value {
		let slot = match first {
			Slot::Unread => {
				links.ahead.and_then(|ahead| ahead.slot(ctx, rid)).unwrap_or(Slot::Unread)
			}
			held @ (Slot::Cached(..) | Slot::Stored(_)) => held,
		};
		let fetched = fetch_record_slot(ctx, rid, slot).await?;
		*value = fetched;
	} else if let Value::Array(arr) = value {
		// Use batch fetch for arrays - more efficient for larger arrays
		batch_fetch_in_place(ctx, arr.as_mut_slice(), links).await?;
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
	let (record, generation) = crate::exec::migrate::stamped_record(
		&txn,
		db_ctx.ns_ctx.ns.namespace_id,
		db_ctx.db.database_id,
		&rid.table,
		&rid.key,
		version,
	)
	.await
	.context("Failed to fetch record")?;
	if record.data.is_none() {
		return absent_record(ctx, rid, version, auth_reads_none).await;
	}
	// The record as a read sees it: brought through the table's migration
	// steps it has not passed.
	let mut data = record.data.clone();
	crate::exec::migrate::replay_record(ctx, &rid.table, &mut data, generation).await?;
	Ok(Some(data))
}

/// The answer for a record id that names no record: `NONE` where its table is defined, and
/// `TbNotFound` where it is not, unless `auth_reads_none` and `rid` is the signed-in record
/// user's own record ([`is_auth_record`]).
async fn absent_record(
	ctx: &ExecutionContext,
	rid: &RecordId,
	version: Option<u64>,
	auth_reads_none: bool,
) -> crate::expr::FlowResult<Option<Value>> {
	let db_ctx = ctx.database().context("fetch_raw_record requires database context")?;
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
	Ok(None)
}

/// [`fetch_raw_deref`] for a record a window may hold: the cached record's value as
/// `get_record` answers a cache hit, the stored bytes decoded here, or the read of
/// [`fetch_raw_deref`] where the window holds nothing. A record decoded from stored bytes at the
/// current version is held in the transaction's cache as `get_record` holds the record it reads,
/// so a later read of it in the transaction, windowed or not, is a cache hit. A held record is
/// brought through the migration steps of its table it has not passed, from the generation its
/// stored bytes carry, as [`fetch_raw_deref`] brings the record it reads.
///
/// A read of a held record adds no state to the future beyond what [`fetch_raw_deref`] holds: it
/// awaits only the replay that read awaits, and a decode that has to read the table's layouts
/// runs out of line. Every dereference of a computed field's cycle of links goes through this
/// future, so its size is the stack each level of the cycle spends.
pub(crate) async fn fetch_raw_deref_slot(
	ctx: &ExecutionContext,
	rid: &RecordId,
	slot: Slot<'_>,
	version: Option<u64>,
) -> crate::expr::FlowResult<Option<Value>> {
	match slot {
		Slot::Unread => fetch_raw_deref(ctx, rid, version).await,
		Slot::Cached(record, generation) => {
			let mut data = record.data.clone();
			crate::exec::migrate::replay_record(ctx, &rid.table, &mut data, generation).await?;
			Ok(Some(data))
		}
		Slot::Stored(bytes) => {
			let db_ctx = ctx.database().context("fetch_raw_record requires database context")?;
			let txn = db_ctx.txn();
			let (ns, db) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
			let decoded = match txn.decode_stamped_resident(
				ns,
				db,
				&rid.table,
				bytes,
				rid.clone(),
				version,
			) {
				Some(decoded) => decoded,
				None => {
					Box::pin(txn.decode_stamped_record(
						ns,
						db,
						&rid.table,
						bytes,
						rid.clone(),
						version,
					))
					.await
				}
			};
			let (record, generation) = decoded.context("Failed to fetch record")?;
			if record.data.is_none() {
				return Box::pin(absent_record(ctx, rid, version, true)).await;
			}
			let mut data = match version {
				None => {
					let record = record.into_read_only();
					let data = record.data.clone();
					txn.cache_record(ns, db, &rid.table, &rid.key, record, generation);
					data
				}
				Some(_) => record.data,
			};
			crate::exec::migrate::replay_record(ctx, &rid.table, &mut data, generation).await?;
			Ok(Some(data))
		}
	}
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
	let (record, generation) = crate::exec::migrate::stamped_record_for_update(
		&txn,
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
	let mut data = record.data.clone();
	crate::exec::migrate::replay_record(ctx, &rid.table, &mut data, generation).await?;
	Ok(Some(data))
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
pub(crate) fn fetch_record_no_perms<'a>(
	ctx: &'a ExecutionContext,
	rid: &'a RecordId,
) -> impl Future<Output = crate::expr::FlowResult<Value>> + 'a {
	fetch_record_no_perms_slot(ctx, rid, Slot::Unread)
}

/// [`fetch_record_no_perms`] reading the record from `slot`.
pub(crate) async fn fetch_record_no_perms_slot(
	ctx: &ExecutionContext,
	rid: &RecordId,
	slot: Slot<'_>,
) -> crate::expr::FlowResult<Value> {
	if ctx.refuses_dereference() {
		return Err(anyhow::Error::new(crate::exec::migrate::MigrationError::Dereference).into());
	}
	// When the program has a unit for the record's table,
	// the record is read through that unit.
	let mut slot = slot;
	if let Some(deref) = ctx.vm_deref()
		&& let Some(fetched) = deref.fetch_slot(ctx, rid, false, &mut slot).await
	{
		return fetched;
	}
	let Some(val) = fetch_raw_deref_slot(ctx, rid, slot, ctx.version_stamp()).await? else {
		return Ok(Value::None);
	};
	// Deriving the computed fields may dereference a link and so reach this read again: it runs
	// as a nested evaluation of the statement's driver, as a read unit's body does
	// ([`crate::exec::vm::Deref::fetch_slot`]).
	let (ctx, rid) = (ctx.clone(), rid.clone());
	crate::exec::trampoline::nest(async move {
		let mut val = val;
		let field_state =
			super::scan::pipeline::build_field_state(&ctx, &rid.table, false, None).await?;
		let field_state = reading_own_record(&ctx, &rid, field_state);
		super::scan::pipeline::compute_fields_for_value(&ctx, &field_state, &mut val, true).await?;
		Ok(val)
	})
	.await
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
pub(crate) fn fetch_record<'a>(
	ctx: &'a ExecutionContext,
	rid: &'a RecordId,
) -> impl Future<Output = crate::expr::FlowResult<Value>> + 'a {
	fetch_record_slot(ctx, rid, Slot::Unread)
}

/// [`fetch_record`] reading the record from `slot`, which a window filled for `rid`: everything
/// after the read of the record's value runs as it does for a record read for itself.
pub(crate) async fn fetch_record_slot(
	ctx: &ExecutionContext,
	rid: &RecordId,
	slot: Slot<'_>,
) -> crate::expr::FlowResult<Value> {
	if ctx.refuses_dereference() {
		return Err(anyhow::Error::new(crate::exec::migrate::MigrationError::Dereference).into());
	}
	// When the program has a unit for the record's table,
	// the record is read through that unit.
	// A computed field whose value does not coerce to its kind fails the
	// dereference naming the field.
	let mut slot = slot;
	if let Some(deref) = ctx.vm_deref()
		&& let Some(fetched) = deref.fetch_slot(ctx, rid, true, &mut slot).await
	{
		return fetched.map_err(computed_coercion);
	}
	let Some(val) = fetch_raw_deref_slot(ctx, rid, slot, ctx.version_stamp()).await? else {
		require_table(ctx, rid).await?;
		return Ok(Value::None);
	};
	// Deriving the computed fields may dereference a link and so reach this read again: it runs
	// as a nested evaluation of the statement's driver, as a read unit's body does
	// ([`crate::exec::vm::Deref::fetch_slot`]).
	let (ctx, rid) = (ctx.clone(), rid.clone());
	crate::exec::trampoline::nest(async move {
		let mut val = val;
		Ok(match process_fetched_record(&ctx, &rid, &mut val).await? {
			true => val,
			false => Value::None,
		})
	})
	.await
}

/// Whether `rid` names a record the caller can see, for `record::exists` and
/// `.exists()`.
///
/// Answered as a read of the record by id is, under the same table permission,
/// without materialising the record's fields (only computed fields that a
/// conditional table `SELECT` permission itself reads are derived by its
/// [`ComputedFieldGuard`](crate::exec::permission::ComputedFieldGuard)):
/// existence depends on the record and its table permission, and a computed
/// field that is expensive or fails must not change the answer. `check_perms`
/// false reads the record without permissions, as a dereference made while a
/// permission is judged does. A table that is not defined raises `TbNotFound`,
/// as a read does.
pub(crate) async fn record_exists(
	ctx: &ExecutionContext,
	rid: &RecordId,
	check_perms: bool,
) -> crate::expr::FlowResult<bool> {
	if ctx.refuses_dereference() {
		return Err(anyhow::Error::new(crate::exec::migrate::MigrationError::Dereference).into());
	}
	let Some(val) = fetch_raw_deref(ctx, rid, ctx.version_stamp()).await? else {
		return Ok(false);
	};
	if !check_perms {
		return Ok(true);
	}
	let db_ctx = ctx.database().context("record_exists requires database context")?;
	validate_record_user_access(db_ctx)?;
	if !should_check_perms(db_ctx, Action::View).context("Failed to check permissions")? {
		return Ok(true);
	}
	let (ctx, rid) = (ctx.clone(), rid.clone());
	crate::exec::trampoline::nest(async move {
		let select_perm = table_select_permission(&ctx, &rid.table).await?;
		match &select_perm {
			PhysicalPermission::Deny => Ok(false),
			PhysicalPermission::Allow => Ok(true),
			PhysicalPermission::Conditional(_) => {
				check_permission_for_value(&select_perm, &val, None, &ctx)
					.await
					.context("Permission check failed")
			}
		}
	})
	.await
}

/// `failure` raised dereferencing a record, a computed field's failed
/// coercion named as the dereference names it.
fn computed_coercion(failure: crate::expr::ControlFlow) -> crate::expr::ControlFlow {
	if let crate::expr::ControlFlow::Err(e) = failure {
		if let Some(crate::doc::Error::FieldCoerce {
			field_name,
			..
		}) = e.downcast_ref::<crate::doc::Error>()
		{
			crate::expr::ControlFlow::Err(anyhow::anyhow!(
				"Failed to coerce computed field '{field_name}'"
			))
		} else {
			crate::expr::ControlFlow::Err(e)
		}
	} else {
		failure
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
	#[cfg(test)]
	super::link_window::tests::note_each();
	evaluate_each(ctx, RECORD_DEREFERENCE, rids, |rid| fetch_record(ctx, rid)).await
}

/// Batch fetch records and replace RecordIds in an array in place.
///
/// This is a convenience function that modifies an array of values,
/// replacing any RecordIds with their fetched record data. Non-RecordId
/// values are left unchanged.
///
/// With `links` holding windows, the records are read a window at a time (those the rows window
/// holds are read from it) and consumed one position at a time, in order, each result written in
/// place at its position; the first position whose fetch fails fails the call. Without, the ids
/// are collected and fetched through [`batch_fetch_records`].
pub(crate) async fn batch_fetch_in_place(
	ctx: &ExecutionContext,
	values: &mut [Value],
	links: FetchLinks<'_>,
) -> crate::expr::FlowResult<()> {
	if links.pool.is_some() {
		let include = |rid: &RecordId| links.ahead.is_none_or(|ahead| !ahead.holds(rid));
		let mut windows = Windows::of(links.pool, values, &include);
		for at in 0..values.len() {
			let Value::RecordId(rid) = &values[at] else {
				continue;
			};
			let slot = match links.ahead.and_then(|ahead| ahead.slot(ctx, rid)) {
				Some(slot) => slot,
				None => match &mut windows {
					Some(windows) => {
						windows.prepare(ctx, values, at, &include).await?;
						windows.slot(at)
					}
					None => Slot::Unread,
				},
			};
			let fetched = fetch_record_slot(ctx, rid, slot).await?;
			values[at] = fetched;
		}
		return Ok(());
	}
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
///
/// The session's own record ([`is_session_record`]) is this record wherever it
/// is read, including beneath a `FOR`, closure or function argument that
/// rebinds `$auth`.
pub(crate) fn is_auth_record(ctx: &ExecutionContext, rid: &RecordId) -> bool {
	matches!(ctx.value("auth"), Some(Value::RecordId(auth)) if auth == rid)
		|| is_session_record(ctx, rid)
}

/// Whether `rid` is the record the session is signed in as (`$session.rd`).
///
/// Unlike [`is_auth_record`], this reads the session itself rather than
/// whatever `$auth` is bound to where the read happens, so a `FOR`, closure or
/// function argument named `$auth` never stands in for it.
pub(crate) fn is_session_record(ctx: &ExecutionContext, rid: &RecordId) -> bool {
	matches!(
		ctx.session().and_then(|session| session.rd.as_ref()),
		Some(Value::RecordId(signed_in)) if signed_in == rid
	)
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
