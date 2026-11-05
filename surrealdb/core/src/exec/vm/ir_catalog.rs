//! The fill phase of the per-planning catalog memo.
//!
//! The memo answers synchronously from what it holds, so what the conversion of a container reads
//! is read first, through the planner's transaction. [`fill`] walks a bound container in program
//! order, reads the answers of every table read it names ([`TableAnswers`], one per
//! [`SourceKey`]) and the definitions those tables reach, and holds them in the memo. [`top_up`]
//! reads the answers a later step asked for that the first fill did not read. A step that can
//! introduce a table read leaves its names pending in the memo, and a top-up of every pending name
//! runs before the physical plan is made.
//!
//! The answers come from the same readers, in the same order, that the planner's access-path choice
//! reads through, so the decision made from them is the planner's. A read that fails
//! declines the plan-time access path for that table, as the planner does: the answers then hold no
//! index and an unresolved permission context.
//!
//! The module sits under `vm` because it shares the planner's readers with the planner's mapping.

use std::collections::HashSet;
use std::sync::Arc;

use crate::catalog::providers::TableProvider;
use crate::catalog::{FieldDefinition, Permission};
use crate::exec::function::FunctionRegistry;
use crate::exec::index::access_path::{ElementColumns, admissible_indexes};
use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::analysis::effects;
use crate::exec::ir::cache::planning::PlanningCatalog;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::Rewriter;
use crate::exec::ir::core::walk::{ancestors, ops_in};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::catalog::{EffectCx, SourceKey};
use crate::exec::ir::iface::frame::Snapshot;
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::iface::statements::{AlterObject, DdlChecks, DefineObject, RemoveObject};
use crate::exec::ir::logical::ddl::{
	field_definition_of, literal_operand, written_field, written_field_template,
};
use crate::exec::ir::logical::ops::{LogOp, LoopKind, StepAttrs};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::access::{
	InlineFields, ReadVersion, RestrictedSelect, TableAnswers, source_key,
};
use crate::exec::ir::phys::access_expr::{Reconstructed, predicate_of_region};
use crate::exec::ir::phys::seed_host::AnswerLoader;
use crate::exec::ir::unit::UnitKind;
use crate::exec::planner::Planner;
use crate::exec::vm::NotCovered;
use crate::exec::vm::payload::CountSelect;
use crate::exec::vm::program::FieldRefusal;
use crate::expr::logical::EffectSet;
use crate::expr::lookup::LookupKind;
use crate::expr::statements::define::DefineKind;
use crate::expr::with::With;
use crate::expr::{Function, Idiom};
use crate::kvs::index::filter_online_indexes;
use crate::val::{TableName, Value};

/// The outcome for a catalog read that failed.
fn unreadable(cause: &anyhow::Error) -> IrError {
	IrError::CatalogUnreadable {
		what: "ir_catalog",
		cause: format!("{cause:#}"),
	}
}

/// Reads every table read of `ir` into `memo`, every function `ir` calls, and the definitions
/// they reach. The edge tables of a graph hop with a `WHERE` are read as current table reads,
/// for the key condition the physical plan decides for the hop.
///
/// Tables are read in program order. A schema statement before a read means the definitions held
/// do not describe the catalog the read runs against, so the memo is told of the change
/// ([`PlanningCatalog::mark_catalog_change`]) and answers `Unknown` from then on: the statements
/// of the container are planned again when they are reached. A read that stands before every
/// schema statement is read as it would be without the others, and so is a read inside a schema
/// statement's own regions (a view's backfill), which changes no definition of the tables it
/// reads.
///
/// The container is bound and holds no physical op. A read error leaves what was read before it
/// in place.
#[allow(dead_code)]
pub(crate) async fn fill(
	ir: &Ir,
	planner: &Planner<'_>,
	memo: &mut PlanningCatalog,
) -> Result<(), IrError> {
	let mut keys: Vec<SourceKey> = Vec::new();
	let mut seen: HashSet<SourceKey> = HashSet::new();
	let mut schema_before_read = false;
	let mut schema_ops: Vec<OpId> = Vec::new();
	let mut failure: Option<IrError> = None;
	let mut functions: Vec<String> = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		let Some(data) = ir.op(op) else {
			return Walk::Continue;
		};
		if let OpKind::Expr(ExprOp::Call(call)) = &data.kind
			&& let Function::Custom(name) = &call.function
			&& !functions.contains(name)
		{
			functions.push(name.clone());
		}
		let OpKind::Logical(kind) = &data.kind else {
			return Walk::Continue;
		};
		match kind {
			LogOp::Define(..) | LogOp::Remove(..) | LogOp::Alter(..) | LogOp::Rebuild(..) => {
				schema_ops.push(op);
			}
			LogOp::Source(..) => {
				if after_schema(ir, op, &schema_ops) {
					schema_before_read = true;
				} else {
					match source_key(ir, op, &*memo, &*memo) {
						Ok(Some(key)) => {
							if seen.insert(key.clone()) {
								keys.push(key);
							}
						}
						Ok(None) => {}
						Err(error) => {
							failure.get_or_insert(error);
						}
					}
				}
			}
			LogOp::Loop {
				kind: LoopKind::GraphStep,
				step: Some(step),
				..
			} => {
				if let StepAttrs::Lookup(attrs) = step.as_ref()
					&& matches!(attrs.kind, LookupKind::Graph(_))
					&& attrs.cond && matches!(
					ir.snapshot(data.snapshot),
					Some(Snapshot::Current) | None
				) && !after_schema(ir, op, &schema_ops)
				{
					for subject in attrs.subjects.iter() {
						let key: SourceKey = (subject.table.clone(), None, ReadVersion::Current);
						if seen.insert(key.clone()) {
							keys.push(key);
						}
					}
				}
			}
			LogOp::Write(..) => {
				if after_schema(ir, op, &schema_ops) {
					schema_before_read = true;
				} else {
					for table in written_tables(ir, op) {
						let key: SourceKey = (table, None, ReadVersion::Current);
						if seen.insert(key.clone()) {
							keys.push(key);
						}
					}
				}
			}
			LogOp::Request {
				..
			}
			| LogOp::Txn {
				..
			}
			| LogOp::Stmt {
				..
			}
			| LogOp::Statement {
				..
			}
			| LogOp::Values {
				..
			}
			| LogOp::Match {
				..
			}
			| LogOp::MatchRead {
				..
			}
			| LogOp::MatchUpdate(_)
			| LogOp::MatchDelete {
				..
			}
			| LogOp::MatchInsert(_)
			| LogOp::MatchOutput(_)
			| LogOp::BindStep(_)
			| LogOp::Subquery
			| LogOp::Assign(_)
			| LogOp::Unset {
				..
			}
			| LogOp::Content {
				..
			}
			| LogOp::DdlGuard {
				..
			}
			| LogOp::UnionAll
			| LogOp::Join {
				..
			}
			| LogOp::Filter
			| LogOp::PermissionFilter {
				..
			}
			| LogOp::Split {
				..
			}
			| LogOp::Aggregate(_)
			| LogOp::Distinct
			| LogOp::Sort(_)
			| LogOp::Window(_)
			| LogOp::Limit {
				..
			}
			| LogOp::Project(_)
			| LogOp::Fetch
			| LogOp::Timeout
			| LogOp::VersionScope
			| LogOp::Only
			| LogOp::Explain {
				..
			}
			| LogOp::Eval
			| LogOp::Block
			| LogOp::Let(_)
			| LogOp::Return
			| LogOp::Loop {
				..
			}
			| LogOp::If
			| LogOp::Info {
				..
			}
			| LogOp::Sleep
			| LogOp::Use {
				..
			}
			| LogOp::Option {
				..
			}
			| LogOp::Access(_)
			| LogOp::Kill
			| LogOp::Live(_)
			| LogOp::Subscriber(_)
			| LogOp::Show(_)
			| LogOp::Choose(_)
			| LogOp::WriteStage {
				..
			}
			| LogOp::Events(_)
			| LogOp::Yield
			| LogOp::Next
			| LogOp::Break => {}
		}
		Walk::Continue
	});
	if let Some(error) = failure {
		return Err(error);
	}
	if schema_before_read {
		memo.mark_catalog_change();
	}
	for key in keys {
		let answers = read_answers(planner, &key).await?;
		memo.insert_answers(key.clone(), Arc::new(answers));
		// The table and the definitions it reaches are loaded with the answers.
		let _ = memo.table(&key.0);
	}
	// The functions the container calls, whose definitions the physical phase states on each
	// call, and the definitions they reach.
	for name in &functions {
		let _ = memo.function(name);
	}
	while memo.load_pending().await.map_err(|e| unreadable(&e))? {}
	Ok(())
}

/// Whether `op` runs after one of `schema_ops`, the schema statements met before it in program
/// order: one that does not hold `op` in its own regions.
fn after_schema(ir: &Ir, op: OpId, schema_ops: &[OpId]) -> bool {
	schema_ops.iter().any(|schema| !holds(ir, *schema, op))
}

/// Whether `op` stands in a region `owner` holds, at any depth.
fn holds(ir: &Ir, owner: OpId, op: OpId) -> bool {
	let mut at = op;
	loop {
		let region = ir.enclosing_region(at);
		match ir.parent_op(region) {
			Some(parent) if parent == owner => return true,
			Some(parent) => at = parent,
			None => return false,
		}
	}
}

/// Reads the answers of every table read the memo was asked for and does not hold, and of every
/// table read `ir` names that the memo does not hold.
///
/// A read of a table the first fill did not name (introduced by a rewrite pass) is pending after
/// its lookup, or stands in the container as a source the pass made; this reads it. Reading adds
/// no pending read.
pub(crate) async fn top_up(
	ir: &Ir,
	planner: &Planner<'_>,
	memo: &PlanningCatalog,
) -> Result<(), IrError> {
	let mut failure: Option<IrError> = None;
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Logical(LogOp::Source(..)))) {
			match source_key(ir, op, memo, memo) {
				// A lookup of a key the memo does not hold leaves it pending.
				Ok(Some(key)) => {
					let _ = memo.table_answers(&key);
				}
				Ok(None) => {}
				Err(error) => {
					failure.get_or_insert(error);
				}
			}
		}
		Walk::Continue
	});
	if let Some(error) = failure {
		return Err(error);
	}
	for key in memo.take_pending_sources() {
		if memo.holds_answers(&key) {
			continue;
		}
		let answers = read_answers(planner, &key).await?;
		memo.insert_answers(key.clone(), Arc::new(answers));
		let _ = memo.table(&key.0);
	}
	while memo.load_pending().await.map_err(|e| unreadable(&e))? {}
	Ok(())
}

/// Folds the row-independent record paths of the predicate of every filter of `ir`
/// ([`Planner::fold_constant_record_paths`]) and holds each conjunct the fold changed in the memo,
/// as the access-path decision reads it ([`PlanningCatalog::insert_record_folded`]).
///
/// Only a container of a request's own statements folds: the code of any other unit is compiled
/// for the requests that reach it, which the values of this transaction's records do not hold for.
/// A container that may change state, calls a stored function, or reads at a version folds
/// nothing: a change, or a function that may make one, could alter a record between the plan-time
/// read and the per-row read the fold stands for, and a versioned read observes a snapshot the walk
/// does not. A filter in stored code
/// folds nothing either, since stored code is compiled for every request that reaches it.
pub(crate) async fn fill_record_folds(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	planner: &Planner<'_>,
	memo: &PlanningCatalog,
	unit: &UnitKind,
) -> Result<(), IrError> {
	if !matches!(
		unit,
		UnitKind::Request | UnitKind::RequestStatement { .. } | UnitKind::ServedStatement { .. }
	) {
		return Ok(());
	}
	let top: Vec<OpId> = ops_in(ir, ir.top()).collect();
	for op in top {
		let set = effects::summary_with(ir, facts, cx, op).set;
		if set.mutates_state() {
			return Ok(());
		}
	}
	let mut unfoldable = false;
	let mut regions = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		let Some(data) = ir.op(op) else {
			return Walk::Continue;
		};
		match &data.kind {
			OpKind::Logical(LogOp::VersionScope) => {
				unfoldable = true;
				return Walk::Stop;
			}
			OpKind::Expr(ExprOp::Call(call)) if matches!(call.function, Function::Custom(_)) => {
				unfoldable = true;
				return Walk::Stop;
			}
			OpKind::Logical(LogOp::Filter) if !in_stored_clause(ir, op) => {
				regions.extend(data.regions.iter().copied().filter(|region| {
					ir.region(*region).is_some_and(|held| {
						matches!(held.kind, RegionKind::Clause(ClauseRole::Where))
					})
				}));
			}
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {}
		}
		Walk::Continue
	});
	if unfoldable {
		return Ok(());
	}
	for region in regions {
		// A predicate the rebuild cannot read is left to the decision, which reads it as it does
		// any other.
		let mut predicate = Reconstructed::default();
		if !matches!(predicate_of_region(ir, region, &mut predicate), Ok(true)) {
			continue;
		}
		for (_, conjunct) in predicate.conjuncts {
			let mut folded = conjunct.clone();
			if planner.fold_constant_record_paths(&mut folded).await && folded != conjunct {
				memo.insert_record_folded(conjunct, folded);
			}
		}
	}
	Ok(())
}

/// Whether `op` stands inside a stored clause.
fn in_stored_clause(ir: &Ir, op: OpId) -> bool {
	ancestors(ir, op).any(|(region, _)| {
		ir.region(region).is_some_and(|held| matches!(held.kind, RegionKind::StoredClause(_)))
	})
}

/// Loads the table answers a conversion asks for, through the planner and into the memo.
pub(crate) struct PlannerLoader<'a> {
	pub(crate) planner: &'a Planner<'a>,
	pub(crate) memo: &'a PlanningCatalog,
}

impl AnswerLoader for PlannerLoader<'_> {
	fn load<'a>(
		&'a self,
		keys: &'a [SourceKey],
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), IrError>> + Send + 'a>> {
		Box::pin(async move {
			for key in keys {
				if self.memo.holds_answers(key) {
					continue;
				}
				let answers = read_answers(self.planner, key).await?;
				self.memo.insert_answers(key.clone(), Arc::new(answers));
			}
			Ok(())
		})
	}
}

/// The tables a write names whole or by record id among its operands, when it names them as
/// literals.
fn written_tables(ir: &Ir, write: crate::exec::ir::iface::ids::OpId) -> Vec<TableName> {
	use crate::exec::ir::core::value::ValueDef;
	let Some(data) = ir.op(write) else {
		return Vec::new();
	};
	let mut tables = Vec::new();
	for operand in &data.operands {
		let Some(ValueDef::OpResult {
			op,
			..
		}) = ir.value(*operand).map(|value| value.def)
		else {
			continue;
		};
		let Some(OpKind::Expr(kind)) = ir.op(op).map(|held| &held.kind) else {
			continue;
		};
		match kind {
			ExprOp::RecordId(record) => tables.push(record.table.clone()),
			ExprOp::Literal(value) => {
				if let Value::Table(table) = &**value {
					tables.push(table.clone());
				} else if let Value::RecordId(record) = &**value {
					tables.push(record.table.clone());
				}
			}
			ExprOp::Array
			| ExprOp::Set
			| ExprOp::Object {
				..
			}
			| ExprOp::FreeParam {
				..
			}
			| ExprOp::SessionParam {
				..
			}
			| ExprOp::CatalogParam {
				..
			}
			| ExprOp::Table {
				..
			}
			| ExprOp::Mock(_)
			| ExprOp::Constant {
				..
			}
			| ExprOp::Field {
				..
			}
			| ExprOp::ComputedRead(_)
			| ExprOp::Doc
			| ExprOp::Access {
				..
			}
			| ExprOp::Deref {
				..
			}
			| ExprOp::Index
			| ExprOp::First
			| ExprOp::Last
			| ExprOp::Each
			| ExprOp::Flatten {
				..
			}
			| ExprOp::Dispatch(_)
			| ExprOp::Method {
				..
			}
			| ExprOp::Destructure {
				..
			}
			| ExprOp::Optional
			| ExprOp::RepeatRecurse
			| ExprOp::SetField {
				..
			}
			| ExprOp::TupleRow {
				..
			}
			| ExprOp::Prefix(_)
			| ExprOp::Convert(_)
			| ExprOp::Postfix {
				..
			}
			| ExprOp::Invoke
			| ExprOp::Binary {
				..
			}
			| ExprOp::ShortCircuit {
				..
			}
			| ExprOp::Call(_)
			| ExprOp::ClosureCreate(_)
			| ExprOp::If
			| ExprOp::Throw
			| ExprOp::Subquery(_)
			| ExprOp::Yield
			| ExprOp::Truthy
			| ExprOp::Agg {
				..
			}
			| ExprOp::Now {
				..
			}
			| ExprOp::Raise(_)
			| ExprOp::DepthCharge {
				..
			} => {}
		}
	}
	tables
}

/// The answers for a table read that declines the plan-time access path: `base` with an
/// unresolved permission context and no index, so that the read scans the table. `base` says
/// whether the read enforces.
fn declined(table: &TableName, base: TableAnswers) -> TableAnswers {
	TableAnswers {
		table: table.clone(),
		restricted: RestrictedSelect::Unresolved,
		..base
	}
}

/// What the `SELECT` permission of `table` does to a caller whose reads are checked, as a count of
/// its records reads it: a table that is not defined is not visible, and the count reports it
/// missing before any permission is judged.
fn count_select_of(table: Option<&crate::catalog::TableDefinition>) -> CountSelect {
	use crate::expr::logical::LogicalPermission;
	let Some(table) = table else {
		return CountSelect::Deny;
	};
	match table.select_permission_logical() {
		Ok(LogicalPermission::None) => CountSelect::Deny,
		Ok(LogicalPermission::Full) => CountSelect::Allow,
		Ok(LogicalPermission::Specific(_)) => CountSelect::Conditional,
		Err(_) => match &table.permissions.select {
			Permission::None => CountSelect::Deny,
			Permission::Full => CountSelect::Allow,
			Permission::Specific(_) => CountSelect::Conditional,
		},
	}
}

/// The `INLINE` fields of `table`, declared by `fields`, when it is a relation table that is not
/// lightweight: its inline generation and each field's raw name, in the order an adjacency entry
/// holds their values.
fn inline_of(
	table: &crate::catalog::TableDefinition,
	fields: &[FieldDefinition],
) -> Option<Arc<InlineFields>> {
	match &table.table_type {
		crate::catalog::TableType::Relation(relation) if !relation.lightweight => {}
		crate::catalog::TableType::Any
		| crate::catalog::TableType::Normal
		| crate::catalog::TableType::Relation(_) => return None,
	}
	let names = crate::doc::inline_fields(fields)
		.into_iter()
		.map(|field| match field.name.0.as_slice() {
			[crate::expr::Part::Field(name)] => name.as_str().to_owned(),
			_ => field.name.to_raw_string(),
		})
		.collect();
	Some(Arc::new(InlineFields {
		generation: table.graph_inline_gen,
		names,
	}))
}

/// What the access-path decision reads about the table read `key` names.
///
/// The reads are the planner's: the table definition, the lightweight relation test, the
/// enforcement the source owes, the restricted-field analysis, the online indexes at the version,
/// the declared fields, the element columns ([`Planner::with_array_columns_as_elements`]), the key
/// structure of the record ids and the caller's authority. The record-id structure is read
/// whatever the statement holds; the decision reads it only where the statement has a condition
/// or an ordering.
///
/// A table is defined for a read at a stamp when the catalog defined it at the stamp or at the
/// current version. A deferred read counts the table as defined, because the run decides whether
/// it exists: a table dropped after the stamp is scanned, not taken for absent. `enforces` says
/// the table declares something a read enforces; whether a node above the read already does is
/// the conversion's to see.
pub(super) async fn read_answers(
	planner: &Planner<'_>,
	key: &SourceKey,
) -> Result<TableAnswers, IrError> {
	let (table, with, version) = key;
	let plain = TableAnswers::plain(table.clone(), true);
	let Some(txn) = planner.txn() else {
		// Nothing is known without a transaction: the read enforces and scans, and a count judges
		// each record.
		return Ok(declined(
			table,
			TableAnswers {
				enforces: true,
				select: CountSelect::Conditional,
				..plain
			},
		));
	};
	let Some((ns, db)) = planner.ns_db_ids().await else {
		// A namespace or database that is not there reads no table: the scan raises the error the
		// read owes before it reads anything, and nothing is enforced.
		return Ok(plain);
	};
	let reads_versioned = *version != ReadVersion::Current;
	let current = txn.get_tb(ns, db, table, None).await.map_err(|e| unreadable(&e))?;
	let (stamped, stamped_unreadable) = match version {
		ReadVersion::Stamped(stamp) => match txn.get_tb(ns, db, table, Some(*stamp)).await {
			Ok(def) => (def, false),
			Err(e)
				if matches!(e.downcast_ref(), Some(crate::catalog::Error::TbNotFound { .. }))
					|| matches!(
						surrealdb_datastore::storage_error(&e),
						Some(surrealdb_kvs::Error::UnsupportedVersionedQueries)
					) =>
			{
				(None, true)
			}
			Err(e) => return Err(unreadable(&e)),
		},
		ReadVersion::Current | ReadVersion::Deferred => (None, false),
	};
	let effective = match version {
		ReadVersion::Current => current.as_deref(),
		ReadVersion::Stamped(_) => stamped.as_deref(),
		ReadVersion::Deferred => None,
	};
	let defined = match version {
		ReadVersion::Current => current.is_some(),
		ReadVersion::Stamped(_) => current.is_some() || stamped.is_some() || stamped_unreadable,
		ReadVersion::Deferred => true,
	};
	let lightweight =
		is_lightweight(planner, table).await.map_err(|e| unreadable(&anyhow::anyhow!(e.reason)))?;
	// A late-bound statement or a deferred version stamp meets the table's permission as it
	// stands when it runs, so a count judges each record rather than trusting the definition
	// read here. A literal version stamp reads the table definition at that stamp when the
	// backend supports versioned catalog reads.
	let select = match planner.is_late_bound()
		|| matches!(version, ReadVersion::Deferred)
		|| stamped_unreadable
	{
		true => CountSelect::Conditional,
		false => count_select_of(effective),
	};
	let field_version = match version {
		ReadVersion::Stamped(stamp) => Some(*stamp),
		ReadVersion::Current | ReadVersion::Deferred => None,
	};
	let fields = match txn.all_tb_fields(ns, db, table, field_version).await {
		Ok(fields) => fields,
		Err(_) => {
			return Ok(declined(
				table,
				TableAnswers {
					defined,
					lightweight,
					enforces: defined,
					permissioned: planner.reads_are_checked(),
					select,
					..plain
				},
			));
		}
	};
	let declares = fields.iter().any(|field| {
		field.computed.is_some()
			|| (planner.reads_are_checked() && !matches!(field.select_permission, Permission::Full))
	}) || (planner.reads_are_checked()
		&& !effective.is_some_and(|def| matches!(def.permissions.select, Permission::Full)));
	// A late-bound statement or a deferred version stamp reads the catalog as it stands when it
	// runs, so it enforces whatever that holds.
	let enforces = planner.is_late_bound()
		|| matches!(version, ReadVersion::Deferred)
		|| (defined && declares);
	let restricted: RestrictedSelect =
		planner.resolve_restricted_select_prefixes(table).await.into();
	let permissioned = planner.reads_are_checked()
		&& (matches!(version, ReadVersion::Deferred)
			|| !effective.is_some_and(|def| matches!(def.permissions.select, Permission::Full)));
	let inline = effective.and_then(|def| inline_of(def, &fields));
	let base = TableAnswers {
		table: table.clone(),
		defined,
		lightweight,
		enforces,
		restricted,
		version: *version,
		permissioned,
		allows_clock: planner.allows_clock_bound(),
		fields,
		select,
		inline,
		..plain
	};
	if matches!(base.restricted, RestrictedSelect::Unresolved) {
		return Ok(declined(table, base));
	}
	let key_structure = planner.record_key_structure(txn, ns, db, table).await;
	let id_order =
		!reads_versioned && key_structure.as_ref().is_none_or(|key| key.all_exactly_ordered());
	let base = TableAnswers {
		key: key_structure,
		id_order,
		..base
	};
	// Under `WITH NOINDEX` no index serves the read; the columns are read for the scan's
	// selectivity probes alone, and a read that fails leaves it with none.
	let no_index = matches!(with, Some(With::NoIndex));
	let failed = |base: TableAnswers| {
		if no_index {
			base
		} else {
			declined(table, base)
		}
	};
	let indexes = match txn.all_tb_indexes(ns, db, table, None).await {
		Ok(indexes) => indexes,
		Err(_) => return Ok(failed(base)),
	};
	let indexes = match version {
		ReadVersion::Stamped(stamp) => {
			let then = match txn.all_tb_indexes(ns, db, table, Some(*stamp)).await {
				Ok(then) => then,
				Err(_) => return Ok(failed(base)),
			};
			let kept: Vec<_> = indexes
				.iter()
				.filter(|now| then.iter().any(|was| was.index_id == now.index_id))
				.cloned()
				.collect();
			Arc::from(kept)
		}
		ReadVersion::Current | ReadVersion::Deferred => indexes,
	};
	let indexes = match filter_online_indexes(txn, ns, db, indexes).await {
		Ok(indexes) => indexes,
		Err(_) => return Ok(failed(base)),
	};
	let admissible =
		admissible_indexes(Arc::clone(&indexes), &base.restricted.prefixes(), reads_versioned);
	let columns = if admissible.is_empty() {
		Some(ElementColumns::unchanged(admissible))
	} else {
		planner.with_array_columns_as_elements(txn, ns, db, table, admissible).await
	};
	if no_index {
		return Ok(TableAnswers {
			probe_columns: columns,
			..base
		});
	}
	Ok(TableAnswers {
		indexes,
		columns,
		..base
	})
}

// ---------------------------------------------------------------------------------------------
// The catalog checks of field statements
// ---------------------------------------------------------------------------------------------

/// A field statement whose field path and table are written, with what its checks read of it.
enum FieldStatement {
	/// `DEFINE FIELD`: the definition its template forms under the written name and table.
	Define {
		name: Idiom,
		table: TableName,
		definition: Box<FieldDefinition>,
		kind: DefineKind,
	},
	/// `REMOVE FIELD`.
	Remove {
		name: Idiom,
		table: TableName,
	},
	/// `ALTER FIELD`.
	Alter {
		name: Idiom,
		table: TableName,
	},
}

/// Whether an op with effects `set` and kind `kind` may change what the checks of a later field
/// statement read: the tables, fields and indexes of the database, or the database the session
/// reaches.
fn may_change_checked_catalog(set: EffectSet, kind: &OpKind) -> bool {
	const CHANGES: EffectSet = EffectSet::CHANGES_CATALOG
		.union(EffectSet::WRITES_DATA)
		.union(EffectSet::WRITES_EXTERNAL)
		.union(EffectSet::WRITES_SESSION)
		.union(EffectSet::OPAQUE_BODY)
		.union(EffectSet::DEFERS);
	set.intersects(CHANGES) || matches!(kind, OpKind::Logical(LogOp::Use { .. }))
}

/// The field statement `op` is, when its path and table are written and its checks can be made.
///
/// A path or table that is evaluated, a path with a step beyond those a stored field path holds,
/// a comment that is evaluated and a template that does not decode are left to the program, which
/// resolves its names and checks the definition against the catalog it finds when it runs.
fn field_statement(ir: &Ir, op: OpId, kind: &LogOp) -> Result<Option<FieldStatement>, IrError> {
	let Some(data) = ir.op(op) else {
		return Ok(None);
	};
	let operands = &data.operands;
	match kind {
		LogOp::Define(attrs) => {
			let DefineObject::Field(field) = &attrs.object else {
				return Ok(None);
			};
			let comment = literal_operand(ir, operands, field.comment)?;
			if !matches!(comment, Some(Value::None | Value::String(_))) {
				return Ok(None);
			}
			let Some((name, table)) = written_field(ir, operands, field.name, field.what)? else {
				return Ok(None);
			};
			let template = written_field_template(
				&attrs.template.value,
				&name.to_raw_string(),
				table.as_str(),
			);
			let Ok(definition) = field_definition_of(&template) else {
				return Ok(None);
			};
			Ok(Some(FieldStatement::Define {
				name,
				table: TableName::new(table.as_str()),
				definition: Box::new(definition),
				kind: attrs.mode.clone(),
			}))
		}
		LogOp::Remove(attrs) => {
			let RemoveObject::Field {
				name,
				table,
			} = &attrs.object
			else {
				return Ok(None);
			};
			Ok(written_field(ir, operands, *name, *table)?.map(|(name, table)| {
				FieldStatement::Remove {
					name,
					table: TableName::new(table.as_str()),
				}
			}))
		}
		LogOp::Alter(attrs) => {
			let AlterObject::Field(field) = &attrs.object else {
				return Ok(None);
			};
			Ok(written_field(ir, operands, field.name, field.what)?.map(|(name, table)| {
				FieldStatement::Alter {
					name,
					table: TableName::new(table.as_str()),
				}
			}))
		}
		LogOp::Request {
			..
		}
		| LogOp::Txn {
			..
		}
		| LogOp::Stmt {
			..
		}
		| LogOp::Statement {
			..
		}
		| LogOp::Source(_)
		| LogOp::Values {
			..
		}
		| LogOp::Match {
			..
		}
		| LogOp::MatchRead {
			..
		}
		| LogOp::MatchUpdate(_)
		| LogOp::MatchDelete {
			..
		}
		| LogOp::MatchInsert(_)
		| LogOp::MatchOutput(_)
		| LogOp::BindStep(_)
		| LogOp::Subquery
		| LogOp::Write(_)
		| LogOp::Assign(_)
		| LogOp::Unset {
			..
		}
		| LogOp::Content {
			..
		}
		| LogOp::DdlGuard {
			..
		}
		| LogOp::Rebuild(_)
		| LogOp::UnionAll
		| LogOp::Join {
			..
		}
		| LogOp::Filter
		| LogOp::PermissionFilter {
			..
		}
		| LogOp::Split {
			..
		}
		| LogOp::Aggregate(_)
		| LogOp::Distinct
		| LogOp::Sort(_)
		| LogOp::Window(_)
		| LogOp::Limit {
			..
		}
		| LogOp::Project(_)
		| LogOp::Fetch
		| LogOp::Timeout
		| LogOp::VersionScope
		| LogOp::Only
		| LogOp::Explain {
			..
		}
		| LogOp::Eval
		| LogOp::Block
		| LogOp::Let(_)
		| LogOp::Return
		| LogOp::Loop {
			..
		}
		| LogOp::If
		| LogOp::Info {
			..
		}
		| LogOp::Sleep
		| LogOp::Use {
			..
		}
		| LogOp::Option {
			..
		}
		| LogOp::Access(_)
		| LogOp::Kill
		| LogOp::Live(_)
		| LogOp::Subscriber(_)
		| LogOp::Show(_)
		| LogOp::Choose(_)
		| LogOp::WriteStage {
			..
		}
		| LogOp::Events(_)
		| LogOp::Yield
		| LogOp::Next
		| LogOp::Break => Ok(None),
	}
}

/// The field statements of `ir` whose checks read the catalog the planner's transaction holds:
/// those that stand at statement position, with nothing before them in program order that may
/// change the tables, fields or indexes they read or the database the session reaches. The first
/// statement of a request that changes the catalog is the last to qualify.
fn checkable_field_statements(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
) -> Result<Vec<(OpId, FieldStatement)>, IrError> {
	let mut found = Vec::new();
	let mut changed = false;
	let mut failure = None;
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		let Some(data) = ir.op(op) else {
			return Walk::Continue;
		};
		if let OpKind::Logical(
			LogOp::Request {
				..
			}
			| LogOp::Txn {
				..
			}
			| LogOp::Stmt {
				..
			},
		) = &data.kind
		{
			return Walk::Continue;
		}
		if let OpKind::Logical(kind @ (LogOp::Define(..) | LogOp::Remove(..) | LogOp::Alter(..))) =
			&data.kind
		{
			if !changed {
				match field_statement(ir, op, kind) {
					Ok(Some(statement)) => found.push((op, statement)),
					Ok(None) => {}
					Err(error) => {
						failure.get_or_insert(error);
					}
				}
			}
			changed = true;
			return Walk::SkipRegions;
		}
		if may_change_checked_catalog(
			effects::summary_with(ir, facts, &structural_cx(registry, ir), op).set,
			&data.kind,
		) {
			changed = true;
		}
		Walk::SkipRegions
	});
	match failure {
		Some(error) => Err(error),
		None => Ok(found),
	}
}

/// The refusal a check raised, as the field checks carry it: `Some(None)` for a check that
/// passed and `None` for an error that is no refusal of a definition, which leaves the statement
/// to its program.
fn refusal_of(checked: anyhow::Result<()>) -> Option<Option<FieldRefusal>> {
	match checked {
		Ok(()) => Some(None),
		Err(error) => FieldRefusal::of(&error).map(Some),
	}
}

/// The checks `statement` makes against the catalog `planner`'s transaction holds, as the body
/// of the statement makes them.
///
/// `None` where the planner holds no transaction or its database is not there to read, where a
/// read fails, and where a check fails with an error that is no refusal of the definition: such a
/// statement is checked by its program against the catalog it finds when it runs.
async fn field_checks(planner: &Planner<'_>, statement: &FieldStatement) -> Option<DdlChecks> {
	let txn = planner.txn()?;
	let (ns, db) = planner.ns_db_ids().await?;
	match statement {
		FieldStatement::Define {
			name,
			table,
			definition,
			kind,
		} => {
			let existing_table = txn.get_tb(ns, db, table, None).await.ok()?;
			let existing_field = txn
				.get_tb_field(ns, db, table, &FieldDefinition::storage_name(name), None)
				.await
				.ok()?;
			let fields = txn.all_tb_fields(ns, db, table, None).await.ok();
			// One check while the definition is formed, the rest once the database is resolved.
			let indexed = refusal_of(
				crate::ddl::define::field::define_field_definition_check_indexed_computed(
					txn, ns, db, definition,
				)
				.await,
			)?;
			// The record id components an `id` names are judged when the statement runs; the
			// clash of any other field with one reads only the catalog.
			let refused = refusal_of(
				async {
					crate::ddl::define::field::define_field_definition_validate_clauses(
						txn, ns, db, definition,
					)
					.await?;
					if !definition.name.is_id() {
						crate::ddl::define_field_statement_validate_key_components(
							txn, ns, db, kind, definition,
						)
						.await?;
					}
					Ok(())
				}
				.await,
			)?;
			let flexible = refusal_of(
				crate::ddl::define_field_statement_validate_flexible_restrictions(
					txn, ns, db, definition,
				)
				.await,
			)?;
			Some(DdlChecks::DefineField {
				existing_table,
				existing_field,
				fields,
				indexed,
				refused,
				flexible,
			})
		}
		FieldStatement::Remove {
			name,
			table,
		}
		| FieldStatement::Alter {
			name,
			table,
		} => {
			let key_refusal = refusal_of(
				crate::ddl::key_scan::refuse_component_name(txn, ns, db, table, name).await,
			)?;
			Some(DdlChecks::Field {
				existing_table: txn.get_tb(ns, db, table, None).await.ok()?,
				existing_field: txn
					.get_tb_field(ns, db, table, &FieldDefinition::storage_name(name), None)
					.await
					.ok()?,
				fields: txn.all_tb_fields(ns, db, table, None).await.ok()?,
				key_refusal,
			})
		}
	}
}

/// Gives the field statements of `ir` the checks their bodies make against the catalog, read
/// through the planner's transaction, in place of the [`DdlChecks::None`] the binder bound them
/// with. Runs on a bound container, before any phase converts it, and after [`fill`].
///
/// A statement is checked only where the catalog it reads is the one it meets when it runs: it
/// stands at statement position with nothing before it in program order that may change a table,
/// a field, an index or the session's database ([`may_change_checked_catalog`]), so a request
/// that defines a table and then a field of it checks the first and leaves the second to its
/// program. A late-bound planner checks nothing. A statement left without checks lowers to the
/// program that makes the same checks when it runs, which raises the same errors at the same
/// position, so the result of a request does not depend on which statements were checked.
///
/// The container is bound and holds no physical op.
pub(crate) async fn fill_field_checks(
	ir: &mut Ir,
	facts: &mut FactCache,
	planner: &Planner<'_>,
) -> Result<(), IrError> {
	if planner.is_late_bound() {
		return Ok(());
	}
	let statements = checkable_field_statements(ir, facts, planner.function_registry())?;
	let mut made = Vec::with_capacity(statements.len());
	for (op, statement) in &statements {
		if let Some(checks) = field_checks(planner, statement).await {
			made.push((*op, checks));
		}
	}
	let mut rewriter = Rewriter::new(ir, facts);
	for (op, checks) in made {
		rewriter.update_kind(op, |kind| match kind {
			OpKind::Logical(LogOp::Define(attrs)) => attrs.checks = checks,
			OpKind::Logical(LogOp::Remove(attrs)) => attrs.checks = checks,
			OpKind::Logical(LogOp::Alter(attrs)) => attrs.checks = checks,
			OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_) => {}
		})?;
	}
	Ok(())
}

/// Whether `table` is a lightweight relation in the catalog the statement is planned against, which
/// stores no record range to scan. Without a transaction, or in a namespace or database that is not
/// there, no table is. A catalog read that fails is an error: the answer picks the scan's storage
/// source, and a guess would read a key range the relation never stores.
async fn is_lightweight(planner: &Planner<'_>, table: &TableName) -> Result<bool, NotCovered> {
	let (Some(txn), Some((ns, db))) = (planner.txn(), planner.ns_db_ids().await) else {
		return Ok(false);
	};
	let def = txn
		.get_tb(ns, db, table, None)
		.await
		.map_err(|_| NotCovered::internal("the table catalog could not be read"))?;
	Ok(def.is_some_and(|def| {
		crate::kvs::lightweight::lightweight_relation(&def.table_type).is_some()
	}))
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::*;
	use crate::dbs::Session;
	use crate::exec::operators::test_util::TestDb;
	use crate::kvs::TransactionType;

	const SETUP: &str = "
		DEFINE TABLE b SCHEMALESS PERMISSIONS FULL;
		DEFINE FIELD k ON b PERMISSIONS FOR select NONE;
		DEFINE INDEX b_k ON b FIELDS k;
		DEFINE INDEX b_j ON b FIELDS j;
	";

	/// The answers a fill holds for a current read of `b`, read as the principal of `session`.
	async fn answers_as(db: &TestDb, session: &Session) -> TableAnswers {
		let ctx = db.exec_ctx_as(session, TransactionType::Read).await;
		let txn = ctx.try_txn().expect("a transaction");
		let planner = Planner::with_txn(
			ctx.ctx(),
			ctx.function_registry(),
			txn,
			Some("test".to_owned()),
			Some("test".to_owned()),
		)
		.with_auth(Arc::clone(&ctx.root().auth));
		let key: SourceKey = ("b".into(), None, ReadVersion::Current);
		read_answers(&planner, &key).await.expect("the answers are read")
	}

	/// The answers a fill holds name the restricted columns of the principal the compile is for:
	/// the owner's carry none, a record user's name the column its permission hides, whichever
	/// principal planned before it on the same datastore.
	#[tokio::test]
	async fn answers_carry_the_restricted_columns_of_the_principal_they_are_read_for() {
		let db = TestDb::new(SETUP).await;
		let record = Session::for_record(
			"test",
			"test",
			"test",
			surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("users", "alice")),
		);
		let owner = TestDb::owner();
		for session in [&owner, &record, &owner, &record] {
			let answers = answers_as(&db, session).await;
			if std::ptr::eq(session, &owner) {
				assert_eq!(answers.restricted, RestrictedSelect::None);
			} else {
				assert!(
					matches!(&answers.restricted, RestrictedSelect::Fields(fields) if fields.len() == 1),
					"a record user's answers name the column its permission hides, got {:?}",
					answers.restricted
				);
			}
		}
	}

	#[tokio::test]
	async fn versioned_answers_do_not_trust_current_full_select_permission() {
		let db = TestDb::new("DEFINE TABLE b SCHEMALESS PERMISSIONS FULL;").await;
		let record = Session::for_record(
			"test",
			"test",
			"test",
			surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("users", "alice")),
		);
		let ctx = db.exec_ctx_as(&record, TransactionType::Read).await;
		let txn = ctx.try_txn().expect("a transaction");
		let planner = Planner::with_txn(
			ctx.ctx(),
			ctx.function_registry(),
			txn,
			Some("test".to_owned()),
			Some("test".to_owned()),
		)
		.with_auth(Arc::clone(&ctx.root().auth));

		let deferred: SourceKey = ("b".into(), None, ReadVersion::Deferred);
		let answers = read_answers(&planner, &deferred).await.expect("deferred answers");
		assert_eq!(answers.select, CountSelect::Conditional);
		assert!(answers.enforces);
		assert!(answers.permissioned);

		let stamped: SourceKey = ("b".into(), None, ReadVersion::Stamped(0));
		let answers = read_answers(&planner, &stamped).await.expect("stamped answers");
		assert_ne!(answers.select, CountSelect::Allow);
		assert!(answers.enforces);
		assert!(answers.permissioned);
	}
}
