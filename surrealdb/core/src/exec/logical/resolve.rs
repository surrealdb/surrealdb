//! Schema resolution: what the catalog declares, written into the plan.
//!
//! The pass runs first in [`super::bind`]. It gives every table source its
//! [`Schema`] and desugars the three schema-derived behaviours the
//! engine enforces on a read into plan nodes, exactly where the scan pipeline
//! applies them, and only for the callers it applies them to:
//!
//! | Declaration | Node | Condition |
//! |---|---|---|
//! | table `PERMISSIONS FOR select WHERE p` | `PermissionFilter[p]` directly above the `Source`, `p` judged with the computed fields it reads derived | the caller is permission-checked |
//! | table `PERMISSIONS FOR select NONE` | `PermissionFilter[false]` | the caller is permission-checked |
//! | `COMPUTED e` on field `c` | the column `e AS c : kind @computed` of a derivation layer | the statement reads `c`, or a guard the statement brings into the plan does, and the caller is unchecked or `c`'s select guard is not decided before the body |
//! | `COMPUTED e` on field `c` with a select guard decided before the body (`PERMISSIONS FOR select NONE`, or `WHERE p` reading only stored fields) | no derivation column; the guard-layer column `NONE AS c @permission`, or `(IF p THEN e ELSE NONE END) AS c : kind @permission` | the caller is permission-checked and the statement reads `c` |
//! | field `PERMISSIONS FOR select WHERE p` on `f` | the column `(IF p THEN f ELSE NONE END) AS f @permission` of the guard layer | the caller is permission-checked and `f` can reach the caller |
//! | field `PERMISSIONS FOR select NONE` on `f` | the column `NONE AS f @permission` of the guard layer | the caller is permission-checked and `f` can reach the caller |
//! | either on a nested or wildcard path `a.b`, `arr.*.f` | a guard-layer column on the root field, built from [`LogicalExpr::SetField`] and [`LogicalExpr::Broadcast`] | the caller is permission-checked |
//!
//! A *layer* is a `Project` of the shape `*, column, …` whose every column
//! carries a frame that is not the caller's ([`LogicalOp::is_layer`]): the
//! row passes through with each column set under the field's own name. The
//! row above the stack carries `c` as its computed value and `f` as the value
//! the caller may see, so every user expression reads plain field names and
//! nothing above the stack is rewritten: no read is renamed, no `*` is
//! expanded, no `OMIT` is added. A `MATCHES` on a field that is a column of
//! the stack is refused here, with the expression as the user wrote it, since
//! no index may answer it ([`Resolver::refuse_matches`]).
//!
//! # What the runtime does, and the stack that reproduces it
//!
//! The order of checks is the scan pipeline's
//! (`operators/scan/pipeline.rs::materialise_fields_with_permissions`). The
//! table permission is judged against the record as stored, with the computed
//! fields its predicate reads derived onto a copy in the predicate's own frame
//! — the `PermissionFilter` does that itself (the VM's `Lowering::permission`;
//! `exec::permission::ComputedFieldGuard` for a compiled guard), so the row
//! passes it as stored.
//! Computed fields are then derived from storage; for a checked caller, a computed field whose
//! select guard is decided before its body (`exec::permission::plan_table_derivations`) is not
//! derived here: a `NONE` guard never derives it, and a `WHERE` guard reading only stored fields
//! is judged against the record as stored, once per row, and derives the body only when it
//! admits. For a checked caller the stored fields' guards
//! then run, each judged against the *unreduced* record — raw stored fields
//! and the computed fields as first derived — with `$value` the field's
//! value, and a denied field is cut. A guard that reads a computed field is
//! judged against a copy of that record with the computed fields the guards
//! read derived onto it in their own frame, as the table permission is: a
//! field derived for the caller from a record the caller may not select is
//! `NONE`, which satisfies a clause written to deny. If anything was cut,
//! every computed field is re-derived against the reduced record: a body
//! reading a cut field sees `NONE`, and a self-dereference through `id`
//! reads the reduced view. Last the computed fields' own guards run, judged
//! against the unreduced record, or the copy, with `$value` the final
//! computed value. An unchecked caller gets the derivation alone.
//!
//! Let S be the restricted stored fields (a checked caller only), C the
//! computed fields, and Cr ⊆ C the fields that need re-derivation: a body
//! that reads some g ∈ S by name — `g`, `$this.g`, `$parent.g` — or
//! dereferences its own id, plus everything that transitively reads a field
//! in Cr. The field a named component of the record id derives (`city` of an
//! `id` declared `[city: string, ..]`) is a member of C whose body is `id[i]`:
//! its derivation column carries the key-part frame ([`EvalFrame::key_part`]),
//! which says the column reads that component of the id and nothing else, and
//! the body indexes the key of the id rather than dereferencing it. It reads
//! `id` alone, which DDL keeps out of S while it names components, and no
//! member of C, so it is not in Cr. What the
//! statement *needs* of C is what its stages read by name,
//! transitively through the bodies — a nested statement that inherits the
//! row ([`LogicalOp::inherits_row`]) reads with the stage that holds it —
//! or all of C when a stage reads the row as a whole: `*`, a row parameter,
//! a projection function, a dynamic index, a nested plan naming a row
//! parameter. The stack above the `PermissionFilter` (above the
//! schema-bearing `Source` when the caller is unchecked or the table
//! permission is `FULL`) is, bottom-up:
//!
//! 1. **Derivation layers A₁…Aₖ**, one per rank of the computed dependency order, so that a layer's
//!    columns never read one another and a field reading another computed field sits one layer
//!    higher. A column is `c := body_c` in the `Computed` frame with the field's declared kind.
//!    Emitted for checked and unchecked callers alike, for the needed fields outside Cr and for
//!    every field a judged guard (below) reads, with what those depend on, except the fields of a
//!    checked caller whose select guard is decided before the body (item 2). Nothing is emitted
//!    when nothing is needed.
//! 2. **The guard layer B**, checked callers only. A field's guard is a column only where the field
//!    can reach the caller: for g ∈ S when the statement reads g or the row as a whole, when a
//!    needed re-derivation reads g, or when a needed re-derived body dereferences its own id and so
//!    reads the record whole; for c ∈ C when c is needed. A judged guard that reads the row as a
//!    whole makes every field reach the caller.
//!    - g ∈ S: `g := IF p_g THEN g ELSE NONE END`, `$value` replaced by `g` and every other read
//!      left as written. `Permission` frame, no kind. `PERMISSIONS FOR select NONE` is `g := NONE`;
//!      `FULL` gets no column.
//!    - c ∈ C ∖ Cr with a `WHERE` select permission: `c := IF p_c THEN c ELSE NONE END`, `$value`
//!      left in `p_c` for the layer to bind to c's value. `Permission` frame, the field's kind.
//!      `NONE` is `c := NONE`; `FULL` gets no column.
//!    - c ∈ C ∖ Cr whose guard is decided before the body ([`Derivation::JudgeFirst`]): `c := IF
//!      p_c THEN body_c ELSE NONE END` with no derivation column, `p_c` reading stored fields only,
//!      so it is judged against the record as stored. `Permission` frame for the whole column, the
//!      field's kind, so a write the body reaches is refused as a permission clause refuses it.
//!      [`Derivation::Hidden`] (`NONE`) is `c := NONE` with no derivation column.
//!    - c ∈ Cr: `c := body_c` in the `Computed` frame when its select permission is `FULL`,
//!      otherwise `c := IF p_c THEN body_c ELSE NONE END` in the `Permission` frame, `$value` left
//!      in `p_c` as above — `IF false THEN body_c …` for `NONE` when another needed re-derivation
//!      reads c, `c := NONE` when none does. The field's kind.
//!
//!    The layer evaluates its columns on stored fields first, each against its input, so a guard
//!    is judged against the unreduced record; then its columns on computed fields in dependency
//!    order, each body in the `Computed` frame against the *reduced* record — the input with the
//!    stored columns landed, carrying each re-derivation landed before it — so a body reading a
//!    cut field reads `NONE` and a self-dereference reads the same view. A computed field's guard
//!    is judged against the unreduced record with `$value` the value its body produced. A guard
//!    that reads a computed field is judged against a copy of the unreduced record with the
//!    computed fields the layer's guards read derived onto it in the frame a permission predicate
//!    derives them in, which the layer states (`ProjectAttrs::judged`). The lowering of the layer
//!    builds each record (`vm::ir_lower::pipe::layer`).
//! 3. **Nested and wildcard guards** join B as a column on the root field: a guard on `a.b` is `a
//!    := set_field(a, 'b', IF p THEN a.b ELSE NONE END)` with `$value` replaced by `a.b`; where the
//!    position the segment applies to may hold an array, the `set_field` runs per element inside a
//!    `Broadcast`, as the runtime's `each` widens `a.b` over an array `a`; a `*` segment is a
//!    `Broadcast` over the position. Several guards under one root compose into one column, deeper
//!    guards inside shallower ones, each judging the raw value from the layer's input. Inside a
//!    `Broadcast` body the current value is the element, so a guard there may read only `$value`;
//!    one that reads the row but not `$value` is hoisted to `IF p THEN a ELSE <a with the path cut>
//!    END` over the whole column.
//!
//! No other node is emitted and no statement shape is refused. The rules
//! above are [`Resolver::plan_layers`] and [`Resolver::guard_column`].
//!
//! # Declarations left to runtime enforcement
//!
//! A table whose declarations the stack cannot express is left unresolved
//! for the caller in question — its source keeps `schema: None` and no node
//! is inserted — so the physical planner compiles it with the scan's own
//! enforcement, as it does for any source this pass does not reach. That is
//! the case for a checked caller when a guard's `$value` reaches a nested
//! plan, where no expression can stand for it; when a guard path ends in `*`,
//! whose denial removes elements rather than a key; when a `*` applies to a
//! position that may hold an object, whose values `set_field` cannot map
//! over; when a path would map over arrays at two positions, which a nested
//! `Broadcast` does not do; when a body in Cr reads the row as a whole, so
//! the reduced view it must read has no expression; when guards composed
//! into one column run under different `AUTH LIMIT`s; and, for every caller,
//! when the computed dependency order is cyclic. A clause with no logical
//! form leaves the table unresolved as well.
//!
//! # Permission gates
//!
//! The gates are mirrored, not approximated. A caller is checked when
//! `exec::permission::should_check_perms(View)` would say so for the
//! session's namespace and database; a namespace or database user outside its
//! scope is refused as `validate_record_user_access` refuses it. A `COMPUTED`
//! body runs as the definer, so a table source inside one is resolved for the
//! definer's authority narrowed by the field's `AUTH LIMIT`; a permission
//! predicate runs unchecked, so a table source inside one gets no permission
//! nodes of its own. A source of a table whose bodies are being expanded is
//! left unresolved rather than expanded again; see the module documentation
//! of [`super`] on why that needs no cycle guard. A multi-source `FROM` gets
//! the stack of each table over that table's input.
//!
//! # Catalog access
//!
//! One asynchronous gather reads every table the plan names — as a `FROM`
//! source, as the table of a record-id source, through a `FROM $param` whose
//! value is known at bind time, as the source of a statement inside a body or
//! a guard, and, when the plan fetches, as the target of a `record<t>` field
//! kind or of a `FETCH` target's kind — as of the `VERSION` the statement
//! names when it can be resolved at bind time. A `FETCH` is the one consumer
//! of the tables a field kind links to: a plan holding one, at any depth,
//! follows every field kind transitively, bounded by the tables of the
//! database, each loaded once, so the schema of every table its `FETCH` can
//! reach through declared kinds is in the snapshot; a plan holding none loads
//! only the tables it reads.
//!
//! A current-version table is read from the node-wide
//! [`CatalogSnapshot`](crate::kvs::cache::ds::CatalogSnapshot), one entry
//! per table keyed by `(namespace, database, table, catalog version)`. The
//! version is the table's own: a uuid under the table's `!cv` key, rewritten
//! by every provider method that writes the table, one of its fields or one
//! of its indexes, and the gather reads it once per table, through the
//! transaction, so that one read validates that table's entry: a DDL
//! committed elsewhere on the table moves its version and misses, while a
//! DDL on another table leaves it in place; the transaction's own uncommitted
//! DDL mints a version only it can read, so it misses too and loads through
//! itself, and the entries it publishes can never be served to another
//! transaction; a rolled-back version is never read again. A table no
//! catalog write has ever stamped has no version and is shared under the
//! content hash of its definition instead. A table the snapshot does not
//! hold is loaded through the transaction and published under its version
//! for the next bind on the node. A `VERSION`ed read has no shared form and
//! goes to the transaction. The definitions' clauses are compiled from their
//! stored text into logical form through the catalog's lazily compiled
//! bodies; nothing here reads the surface expression form of a
//! clause.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use surrealdb_strand::{Strand, TableName};
use surrealdb_types::ToSql;

use super::{BindContext, Caller, visit_binding, visit_binding_mut, visit_deep};
use crate::catalog::providers::TableProvider;
use crate::catalog::{FieldDefinition, PermissionKind, TableDefinition};
use crate::err::{EngineError, Error};
use crate::exec::function::FunctionRegistry;
use crate::exec::permission::{Derivation, plan_table_derivations};
use crate::exec::planner::util::{
	SELECT_ITERATION_PARAMS, evaluates_against_row, resolve_param_value, try_value_to_logical_expr,
};
use crate::expr::logical::{
	ComputedBody, EvalFrame, FieldPath, FieldSchema, FieldSegment, GuardKind, LogicalAggregate,
	LogicalDistinct, LogicalEval, LogicalExplain, LogicalExpr, LogicalFetch, LogicalField,
	LogicalFields, LogicalFilter, LogicalForeach, LogicalIf, LogicalLet, LogicalLimit,
	LogicalLiteral, LogicalOnly, LogicalOp, LogicalPermission, LogicalPermissionFilter,
	LogicalProject, LogicalRecursePath, LogicalReturn, LogicalSequence, LogicalSort, LogicalSource,
	LogicalSplit, LogicalSubquery, LogicalTimeout, LogicalUnionAll, LogicalVersionScope,
	LogicalWindow, LogicalWrite, LogicalWriteStatement, Schema, SourceRef,
};
use crate::expr::{BinaryOperator, Kind, KindLiteral};
use crate::kvs::Transaction;
use crate::kvs::cache::ds::{CatalogSnapshot as SharedCatalog, TableSnapshot};
use crate::val::Value;

/// Resolve every table source in `op` against the transaction's catalog and
/// desugar the declarations it finds. Without a principal, a transaction or a
/// database there is nothing to resolve against, and the plan is returned as
/// it came: every source keeps `schema: None`, which the physical planner
/// compiles with runtime enforcement.
pub(super) async fn resolve(
	op: LogicalOp,
	bx: &BindContext<'_>,
	caller: Option<&Caller>,
) -> Result<LogicalOp, Error> {
	let (Some(caller), Some(txn), Some((ns, db))) = (caller, &bx.txn, bx.ns_db) else {
		return Ok(op);
	};
	let snapshot = gather(&op, bx, txn, ns, db).await?;
	// The tenant check precedes every use the plan makes of a table's
	// permission clauses, so a caller outside its tenant learns nothing of a
	// table's clauses from the statement's outcome: a plan that reads or
	// writes a table is checked here, whether it names the table or the run
	// does.
	if snapshot.tables.is_empty() && !reaches_tables(&op) {
		return Ok(op);
	}
	caller.validate_access()?;
	if snapshot.tables.is_empty() {
		return Ok(op);
	}
	let mut resolver = Resolver {
		snapshot: &snapshot,
		caller,
		registry: bx.registry,
		row_scoped: bx.row_scoped,
	};
	let scope = Scope {
		checks: caller.checks_permissions(),
		clause: PermissionKind::Select,
		in_progress: Vec::new(),
		version: Version::Current,
	};
	resolver.resolve_op(op, &scope)
}

/// Whether `op` reads or writes a table: it holds a source, at any depth, or a
/// write, wherever the table it reaches is named.
fn reaches_tables(op: &LogicalOp) -> bool {
	op.visit_ops_deep(
		&mut |node| match matches!(node, LogicalOp::Source(_) | LogicalOp::Write(_)) {
			true => std::ops::ControlFlow::Break(()),
			false => std::ops::ControlFlow::Continue(()),
		},
	)
	.is_break()
}

// ---------------------------------------------------------------------------
// The gather
// ---------------------------------------------------------------------------

/// The catalog state one bind reads, keyed by table and version. A table is
/// `None` when its definitions hold a clause with no logical form; every
/// source of it is left unresolved.
struct CatalogSnapshot {
	tables: HashMap<(TableName, Option<u64>), Option<Arc<TableFacts>>>,
	/// Turns a `VERSION` datetime into the stamp the tables were loaded under.
	stamps: surrealdb_kvs::timestamp::BoxTimeStampImpl,
	/// `FROM $param` sources whose value named a table or record at bind
	/// time, by parameter name.
	param_sources: HashMap<String, SourceRef>,
	/// `VERSION $param` values that named a datetime at bind time, as version
	/// stamps, by parameter name.
	param_versions: HashMap<String, u64>,
	/// The types of the database that hold a union of labels, each by name
	/// with the name of the union it holds: the union itself, or an alias or
	/// domain over one. Read when a field of a gathered table names a type: a
	/// field of one orders by the declaration of the union's labels.
	unions: HashMap<String, String>,
}

/// What one bind knows about one table. A table that is not defined has an
/// open schema and permissions of `NONE`, as the scan resolves a missing
/// definition.
struct TableFacts {
	schema: Arc<Schema>,
	select: LogicalPermission,
	/// The clauses a mutation's subjects are gated by, in logical form: the
	/// `create`, `update` or `delete` clause, as `select` is the clause a read
	/// is gated by.
	///
	/// One entry per mutation kind this plan performs, and none for a plan
	/// that performs no mutation: a clause no source in this plan can be
	/// resolved under is not lowered, so a read pays nothing for the write
	/// side. A clause is therefore read through [`TableFacts::permission`],
	/// which answers `None` for a clause the gather did not lower rather than
	/// standing in another one.
	write: Vec<(PermissionKind, LogicalPermission)>,
	fields: Vec<FieldFacts>,
}

impl TableFacts {
	/// The clause gating a source resolved under `kind`, or `None` when the
	/// gather did not lower it.
	fn permission(&self, kind: PermissionKind) -> Option<&LogicalPermission> {
		if matches!(kind, PermissionKind::Select) {
			return Some(&self.select);
		}
		self.write.iter().find(|(clause, _)| *clause == kind).map(|(_, permission)| permission)
	}
}

/// The `PERMISSIONS` clause gating the subjects of one mutation, picked by
/// statement kind: `DELETE` is gated by `delete`, `CREATE` by `create`, and
/// every other mutation — `UPDATE`, `UPSERT`, `RELATE`, `INSERT` — by
/// `update`.
fn write_clause(statement: &LogicalWriteStatement) -> PermissionKind {
	match statement {
		LogicalWriteStatement::Create(_) => PermissionKind::Create,
		LogicalWriteStatement::Delete(_) => PermissionKind::Delete,
		LogicalWriteStatement::Update(_)
		| LogicalWriteStatement::Upsert(_)
		| LogicalWriteStatement::Insert(_)
		| LogicalWriteStatement::Relate(_) => PermissionKind::Update,
	}
}

/// The mutation clauses `op` performs, deduplicated: what the gather lowers
/// for every table the plan names. The walk is the one the gather collects
/// tables with, so a source it reaches has the clause it resolves under.
fn plan_write_clauses(op: &LogicalOp) -> Vec<PermissionKind> {
	let mut clauses: Vec<PermissionKind> = Vec::new();
	visit_deep(op, &mut |node| {
		if let LogicalOp::Write(LogicalWrite {
			statement,
			..
		}) = node
		{
			let clause = write_clause(statement);
			if !clauses.contains(&clause) {
				clauses.push(clause);
			}
		}
	});
	clauses
}

/// What one bind knows about one field, in logical form.
struct FieldFacts {
	/// The field's path as declared, as text: the key its clauses are held
	/// under, and the name diagnostics use.
	name: String,
	/// The field's path as declared.
	path: FieldPath,
	kind: Option<Kind>,
	computed: Option<LogicalExpr>,
	select: LogicalPermission,
	/// When a computed field is derived relative to its select guard for a
	/// checked caller, as [`plan_table_derivations`] decides it over the whole
	/// table; [`Derivation::AsMain`] for a stored field. An unchecked caller
	/// has no guard to defer to, and the layers read this only for a checked
	/// one.
	derivation: Derivation,
	/// The definer's authority, when it narrows the caller's; `None` for the
	/// root-Owner stamp, which narrows nothing.
	auth: Option<crate::iam::AuthLimit>,
	/// The position of the record id component the field names, for the
	/// virtual definition of one ([`FieldDefinition::key_part`]). Its
	/// `computed` body is `id[position]`.
	key_part: Option<u32>,
}

impl FieldFacts {
	fn restricted(&self) -> bool {
		!matches!(self.select, LogicalPermission::Full)
	}

	/// The frame of the column that derives this computed field from the row
	/// as the scan reads it: one that says the column reads a record id
	/// component for the field of a named component, and the frame of a
	/// computed body for every other computed field.
	fn derivation_frame(&self) -> EvalFrame {
		match self.key_part {
			Some(_) => EvalFrame::key_part(),
			None => EvalFrame::computed(self.auth.clone()),
		}
	}

	/// Whether `body`, this computed field's body, reaches the row through its
	/// own record id and so reads a record other than the row it is evaluated
	/// on. The body of a named component, `id[position]`, indexes the key of
	/// the id and follows no link.
	fn dereferences_own_id(&self, body: &LogicalExpr) -> bool {
		self.key_part.is_none() && dereferences_own_id(body)
	}

	/// The field the path starts from: the key of the row a column standing
	/// for this field is set under.
	fn root(&self) -> &Strand {
		self.path.root().expect("a declared path starts from a field")
	}
}

/// The `VERSION` a pipeline reads as of.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Version {
	/// No `VERSION`: the transaction's current state.
	Current,
	/// A `VERSION` resolved to a stamp at bind time.
	At(u64),
	/// A `VERSION` only the runtime can evaluate. Sources beneath it are left
	/// unresolved.
	Unknown,
}

impl Version {
	fn stamp(self) -> Option<Option<u64>> {
		match self {
			Version::Current => Some(None),
			Version::At(stamp) => Some(Some(stamp)),
			Version::Unknown => None,
		}
	}
}

/// A reference to a table the plan reads, before the catalog is consulted.
enum Named {
	Table(TableName),
	Param(String),
}

/// The error a failed catalog read raises while the catalog is gathered: the
/// refusal itself where `e` carries one, so the statement fails naming the
/// release it needs, and otherwise an internal error with the message `what`
/// builds.
fn catalog_read_error(e: &anyhow::Error, what: impl FnOnce() -> String) -> Error {
	match Error::entry_refusal(e) {
		Some(entry) => Error::Catalog(surrealdb_catalog::Error::Entry(entry)),
		None => EngineError::Internal(what()).into(),
	}
}

async fn gather(
	op: &LogicalOp,
	bx: &BindContext<'_>,
	txn: &Arc<Transaction>,
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
) -> Result<CatalogSnapshot, Error> {
	// The parameters the plan reads as a source or a version, resolved once.
	// A name the plan binds itself — a `LET` or a `FOR` variable — takes its
	// value where the statement runs, so the value the context holds under
	// that name at bind time is never consulted; so does a name a `DEFINE
	// PARAM` or `REMOVE PARAM` of the plan changes. A closure body is not
	// walked: its sources are resolved where the closure is invoked.
	let mut param_names = HashSet::new();
	let mut bound = HashSet::new();
	visit_deep(op, &mut |node| match node {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Param(param),
			..
		}) => {
			param_names.insert(param.as_str().to_owned());
		}
		LogicalOp::VersionScope(LogicalVersionScope {
			version: LogicalExpr::Param(param),
			..
		}) => {
			param_names.insert(param.as_str().to_owned());
		}
		LogicalOp::Let(LogicalLet {
			name,
			..
		}) => {
			bound.insert(name.clone());
		}
		LogicalOp::Foreach(LogicalForeach {
			param,
			..
		}) => {
			bound.insert(param.as_str().to_owned());
		}
		LogicalOp::Ddl(ddl) => {
			if let Some(name) = super::params::changed_param(&ddl.statement) {
				bound.insert(name.to_owned());
			}
		}
		LogicalOp::Source(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Write(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Filter(_)
		| LogicalOp::PermissionFilter(_)
		| LogicalOp::Split(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Window(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Project(_)
		| LogicalOp::Fetch(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::Only(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Eval(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Return(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_) => {}
	});
	let param_names: HashSet<String> = param_names.difference(&bound).cloned().collect();
	let mut param_sources = HashMap::new();
	let mut param_versions = HashMap::new();
	let stamps = txn.version_impl();
	for name in param_names {
		let ns_db = bx.ns_db.filter(|_| bx.inlines_definitions);
		let Some(value) =
			resolve_param_value(&name, bx.ctx, bx.txn.as_ref(), ns_db, bx.row_scoped).await
		else {
			continue;
		};
		if let Value::Datetime(datetime) = &value {
			if let Ok(stamp) = (*datetime).to_version_stamp(stamps.as_ref()) {
				param_versions.insert(name, stamp);
			}
		} else if matches!(value, Value::Table(_) | Value::RecordId(_)) {
			// A record id whose key holds a closure has no literal form: the
			// source stays a parameter and is resolved when the statement
			// runs.
			let Some(expr) = try_value_to_logical_expr(value) else {
				continue;
			};
			let source = SourceRef::classify(expr);
			if matches!(source, SourceRef::Table(_) | SourceRef::RecordId(_)) {
				param_sources.insert(name, source);
			}
		}
	}

	// The tables the plan names, each as of the version it is read under.
	let mut worklist: Vec<(TableName, Option<u64>)> = Vec::new();
	let mut collect = |named: Named, version: Version| {
		let Some(stamp) = version.stamp() else {
			return;
		};
		let table = match named {
			Named::Table(table) => table,
			Named::Param(name) => match param_sources.get(&name) {
				Some(SourceRef::Table(table)) => table.clone(),
				Some(SourceRef::RecordId(rid)) => rid.table.clone(),
				Some(SourceRef::Mock(_) | SourceRef::Param(_) | SourceRef::Dynamic(_)) | None => {
					return;
				}
			},
		};
		worklist.push((table, stamp));
	};
	collect_named(op, Version::Current, &param_versions, stamps.as_ref(), &mut collect);
	// The tables a field kind links to are read only by a `FETCH`, which
	// replaces a link with the record it names; a plan without one reads no
	// linked table through its kinds.
	let follows_kinds = plan_fetches(op);
	// The mutation clauses this plan can resolve a source under; empty for a
	// plan that performs no mutation.
	let write_clauses = plan_write_clauses(op);

	// Current-version tables come from the node-wide catalog snapshot, each
	// validated by one read of its own catalog version through the
	// transaction. A plan that names no current-version table reads no
	// version; a `VERSION`ed read has no shared form and goes to the
	// transaction. Tables discovered below through field kinds and bodies
	// carry the stamp of the table that named them, so every current-version
	// table the gather reaches has a snapshot to consult.
	let cache = bx.ctx.get_cache();
	let current: Vec<TableName> =
		worklist.iter().filter(|(_, stamp)| stamp.is_none()).map(|(t, _)| t.clone()).collect();
	let mut shared: Option<Arc<SharedCatalog>> = if current.is_empty() {
		None
	} else {
		Some(SharedCatalog::for_tables(cache.as_deref(), txn, ns, db, &current).await.map_err(
			|e| catalog_read_error(&e, || format!("reading the catalog snapshot: {e:#}")),
		)?)
	};

	// Load each table once, following what its definition names. A table
	// whose clauses have no logical form is recorded as unresolvable, so it
	// is neither loaded again nor followed: its sources stay unresolved and
	// the scan enforces its clauses at runtime.
	let mut tables: HashMap<(TableName, Option<u64>), Option<Arc<TableFacts>>> = HashMap::new();
	while let Some((table, stamp)) = worklist.pop() {
		if tables.contains_key(&(table.clone(), stamp)) {
			continue;
		}
		let (def, fields) = match (stamp, shared.as_mut()) {
			(None, Some(shared)) => {
				if shared.entry(&table).is_none() {
					// Every current-version table waiting in the worklist is
					// filled in the same round, so the snapshot grows once
					// per round rather than once per table.
					let missing: Vec<TableName> = std::iter::once(table.clone())
						.chain(
							worklist
								.iter()
								.filter(|(t, s)| s.is_none() && shared.entry(t).is_none())
								.map(|(t, _)| t.clone()),
						)
						.collect();
					*shared =
						shared.extended(cache.as_deref(), txn, &missing).await.map_err(|e| {
							catalog_read_error(&e, || {
								format!("reading the definition of table `{table}`: {e:#}")
							})
						})?;
				}
				let entry = shared.entry(&table).ok_or_else(|| {
					EngineError::Internal(format!(
						"table `{table}` was not loaded into the catalog snapshot"
					))
				})?;
				let Some(gathered) = shared_facts(entry, &table, &write_clauses)? else {
					tables.insert((table, stamp), None);
					continue;
				};
				gathered.push_named(&mut worklist, stamp, follows_kinds);
				tables.insert((table, stamp), Some(Arc::clone(&gathered.facts)));
				continue;
			}
			(stamp, _) => (
				txn.get_tb(ns, db, &table, stamp).await.map_err(|e| {
					catalog_read_error(&e, || {
						format!("reading the definition of table `{table}`: {e:#}")
					})
				})?,
				txn.all_tb_fields(ns, db, &table, stamp).await.map_err(|e| {
					catalog_read_error(&e, || {
						format!("reading the fields of table `{table}`: {e:#}")
					})
				})?,
			),
		};
		let Some(facts) = table_facts(&table, def.as_deref(), &fields, &write_clauses)? else {
			tables.insert((table, stamp), None);
			continue;
		};
		let gathered = Gathered::of(facts);
		gathered.push_named(&mut worklist, stamp, follows_kinds);
		tables.insert((table, stamp), Some(gathered.facts));
	}
	let names_types = tables.values().flatten().any(|facts| {
		facts.fields.iter().any(|field| field.kind.as_ref().is_some_and(Kind::has_named_type))
	});
	let unions = if names_types {
		use crate::catalog::providers::TypeProvider;
		let types = txn.all_db_types(ns, db, None).await.map_err(|e| {
			catalog_read_error(&e, || format!("reading the types of the database: {e:#}"))
		})?;
		let defined: HashMap<&str, &crate::catalog::TypeDefinition> =
			types.iter().map(|definition| (definition.name.as_str(), definition)).collect();
		let by_name = |name: &str| defined.get(name).copied();
		types
			.iter()
			.filter_map(|definition| {
				let union = definition.union_behind(&by_name)?;
				Some((definition.name.to_string(), union.name.to_string()))
			})
			.collect()
	} else {
		HashMap::new()
	};
	Ok(CatalogSnapshot {
		tables,
		stamps,
		param_sources,
		param_versions,
		unions,
	})
}

/// Whether `op` fetches anywhere — a `FETCH` clause, or a `RETURN … FETCH` —
/// its nested plans included.
fn plan_fetches(op: &LogicalOp) -> bool {
	let mut fetches = false;
	visit_deep(op, &mut |node| {
		fetches |= match node {
			LogicalOp::Fetch(_) => true,
			LogicalOp::Return(LogicalReturn {
				fetch,
				..
			}) => !fetch.is_empty(),
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Write(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Window(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Info(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::Sleep(_) => false,
		};
	});
	fetches
}

/// Reports every table and source parameter the plan reads, with the version
/// each is read under, descending through scalar subqueries.
fn collect_named(
	op: &LogicalOp,
	version: Version,
	param_versions: &HashMap<String, u64>,
	stamps: &dyn surrealdb_kvs::timestamp::TimeStampImpl,
	out: &mut impl FnMut(Named, Version),
) {
	let version = match op {
		LogicalOp::VersionScope(LogicalVersionScope {
			version: expr,
			..
		}) => resolve_version(expr, param_versions, stamps),
		LogicalOp::Source(LogicalSource {
			source,
			..
		}) => {
			match source {
				SourceRef::Table(table) => out(Named::Table(table.clone()), version),
				SourceRef::RecordId(rid) => out(Named::Table(rid.table.clone()), version),
				SourceRef::Param(param) => out(Named::Param(param.as_str().to_owned()), version),
				SourceRef::Mock(_) | SourceRef::Dynamic(_) => {}
			}
			version
		}
		LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Write(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Filter(_)
		| LogicalOp::PermissionFilter(_)
		| LogicalOp::Split(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Window(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Project(_)
		| LogicalOp::Fetch(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::Only(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Eval(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::Info(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::Sleep(_) => version,
	};
	// A `FETCH` reads the tables its targets link to. A target's kind against
	// the unbound input names them when the link is written into the
	// statement — a record id or a cast in the projection; a link through a
	// declared field is reached from the field's kind once its table is
	// loaded.
	if let LogicalOp::Fetch(LogicalFetch {
		input,
		fetches,
	}) = op
	{
		fetched_tables(fetches, &input.schema(), version, out);
	} else if let LogicalOp::Return(LogicalReturn {
		what,
		fetch,
	}) = op
	{
		fetched_tables(fetch, &what.schema(), version, out);
	}
	let Ok(()) = op.visit_scalars(&mut |scalar| {
		let _ = visit_binding(scalar, &mut |expr| {
			if let LogicalExpr::Subquery(plan) = expr {
				collect_named(plan, version, param_versions, stamps, out);
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
		Ok::<_, std::convert::Infallible>(())
	});
	for child in op.children() {
		collect_named(child, version, param_versions, stamps, out);
	}
}

/// The stamp a `VERSION` expression denotes at bind time: a datetime literal
/// or a parameter that resolved to one. Anything else is the runtime's.
fn resolve_version(
	expr: &LogicalExpr,
	param_versions: &HashMap<String, u64>,
	stamps: &dyn surrealdb_kvs::timestamp::TimeStampImpl,
) -> Version {
	if let LogicalExpr::Literal(LogicalLiteral::Datetime(datetime)) = expr {
		match (*datetime).to_version_stamp(stamps) {
			Ok(stamp) => Version::At(stamp),
			Err(_) => Version::Unknown,
		}
	} else if let LogicalExpr::Param(param) = expr {
		match param_versions.get(param.as_str()) {
			Some(stamp) => Version::At(*stamp),
			None => Version::Unknown,
		}
	} else {
		Version::Unknown
	}
}

/// Reports the tables the `FETCH` targets `fetches` can link to, typed against
/// `schema`, the rows they are fetched from. A kind that names no table — a
/// field of an open schema, a target that is not a field — reports nothing:
/// a link through a declared field is reached from the field's kind when its
/// table is loaded.
fn fetched_tables(
	fetches: &[LogicalExpr],
	schema: &Schema,
	version: Version,
	out: &mut impl FnMut(Named, Version),
) {
	let mut targets = HashSet::new();
	for fetch in fetches {
		fetch.kind(schema).collect_reference_target_tables(&mut targets);
	}
	for table in targets {
		out(Named::Table(table), version);
	}
}

/// The tables named by table and record-id sources inside the plans a scalar
/// holds, at any depth, a closure body excepted.
fn tables_in_expr(expr: &LogicalExpr) -> Vec<TableName> {
	let mut out = Vec::new();
	let _ = visit_binding(expr, &mut |e| {
		if let LogicalExpr::Subquery(plan) = e {
			visit_deep(plan, &mut |node| {
				if let LogicalOp::Source(LogicalSource {
					source,
					..
				}) = node
				{
					match source {
						SourceRef::Table(table) => out.push(table.clone()),
						SourceRef::RecordId(rid) => out.push(rid.table.clone()),
						SourceRef::Mock(_) | SourceRef::Param(_) | SourceRef::Dynamic(_) => {}
					}
				}
			});
		}
		std::ops::ControlFlow::<()>::Continue(())
	});
	out
}

/// Whether a clause has a logical form but holds a write no operator compiles.
///
/// Such a clause leaves its table to runtime enforcement, exactly as a clause
/// with no logical form does: both are clauses the engine cannot
/// evaluate, and a caller with no statement behind it to evaluate the clause —
/// a GQL plan — would otherwise fail on a read that never needed the write's
/// branch.
fn holds_an_unplannable_write(permission: &LogicalPermission) -> bool {
	match permission {
		LogicalPermission::Specific(guard) => super::unplannable_write_in_expr(guard).is_some(),
		LogicalPermission::None | LogicalPermission::Full => false,
	}
}

/// One table's facts with the tables they lead the gather to.
struct Gathered {
	facts: Arc<TableFacts>,
	/// The tables named by the computed fields and the field and table
	/// clauses, in the order the gather visits them.
	named: Vec<TableName>,
	/// The tables the fields' kinds reference, which the gather follows only
	/// when the plan follows kinds.
	kind_targets: Vec<TableName>,
}

impl Gathered {
	fn of(facts: TableFacts) -> Self {
		let mut named = Vec::new();
		let mut kind_targets = Vec::new();
		for field in &facts.fields {
			if let Some(kind) = &field.kind {
				let mut targets = HashSet::new();
				kind.collect_reference_target_tables(&mut targets);
				kind_targets.extend(targets);
			}
			for body in field.computed.iter().chain(match &field.select {
				LogicalPermission::Specific(guard) => Some(guard),
				LogicalPermission::None | LogicalPermission::Full => None,
			}) {
				named.extend(tables_in_expr(body));
			}
		}
		for permission in std::iter::once(&facts.select).chain(facts.write.iter().map(|(_, p)| p)) {
			if let LogicalPermission::Specific(guard) = permission {
				named.extend(tables_in_expr(guard));
			}
		}
		Self {
			facts: Arc::new(facts),
			named,
			kind_targets,
		}
	}

	fn push_named(
		&self,
		worklist: &mut Vec<(TableName, Option<u64>)>,
		stamp: Option<u64>,
		follows_kinds: bool,
	) {
		if follows_kinds {
			worklist.extend(self.kind_targets.iter().map(|t| (t.clone(), stamp)));
		}
		worklist.extend(self.named.iter().map(|t| (t.clone(), stamp)));
	}
}

/// The facts of a current-version table, derived once per catalog version.
///
/// [`table_facts`] reads the shared entry and the plan's mutation clauses and
/// nothing else, so its answer for one set of clauses is held in the entry's
/// derived slot and served to every later bind under the same version. A
/// malformed `AUTH LIMIT` is an error and is not held: each bind reports it.
fn shared_facts(
	entry: &TableSnapshot,
	table: &TableName,
	writes: &[PermissionKind],
) -> Result<Option<Arc<Gathered>>, Error> {
	let memo = entry.derived.get_or_init(|| Arc::new(SharedFacts::default()));
	let Some(memo) = memo.downcast_ref::<SharedFacts>() else {
		return Err(EngineError::Internal(format!(
			"the derived slot of table `{table}` holds another planner's value"
		))
		.into());
	};
	if let Ok(held) = memo.0.lock()
		&& let Some((_, gathered)) = held.iter().find(|(clauses, _)| clauses.as_slice() == writes)
	{
		return Ok(gathered.clone());
	}
	let gathered = table_facts(table, entry.definition.as_deref(), &entry.fields, writes)?
		.map(|facts| Arc::new(Gathered::of(facts)));
	if let Ok(mut held) = memo.0.lock()
		&& !held.iter().any(|(clauses, _)| clauses.as_slice() == writes)
	{
		held.push((writes.to_vec(), gathered.clone()));
	}
	Ok(gathered)
}

/// The facts [`shared_facts`] holds in one table's derived slot, one entry per
/// set of mutation clauses a plan lowered.
#[derive(Default)]
struct SharedFacts(std::sync::Mutex<Vec<FactsFor>>);

/// One set of mutation clauses and the facts gathered under it; `None` when
/// the table's clauses have no logical form.
type FactsFor = (Vec<PermissionKind>, Option<Arc<Gathered>>);

/// Builds what the pass knows about one table from its definitions, reading
/// each clause in the logical form the catalog compiled from its stored text.
///
/// `writes` names the mutation clauses to lower beside `select`: the kinds the
/// plan performs, and nothing for a plan that performs none.
///
/// `None` when a clause has no logical form, or a field is declared on a path
/// the schema cannot describe: the table's sources are then left unresolved,
/// and the scan evaluates the stored clauses itself at runtime, as it does for
/// any source the analyzer does not reach. A malformed `AUTH LIMIT` is a
/// catalog error and is reported as one.
fn table_facts(
	table: &TableName,
	def: Option<&TableDefinition>,
	defs: &[FieldDefinition],
	writes: &[PermissionKind],
) -> Result<Option<TableFacts>, Error> {
	let select = match def {
		Some(def) => match def.select_permission_logical() {
			Ok(select) => select,
			Err(_) => return Ok(None),
		},
		None => LogicalPermission::None,
	};
	if holds_an_unplannable_write(&select) {
		return Ok(None);
	}
	// A table with no definition denies its mutation clauses as it denies
	// `select`: the definition a write creates on the way carries the default
	// permissions, which grant a checked caller nothing.
	let mut write = Vec::with_capacity(writes.len());
	for clause in writes {
		let permission = match def {
			Some(def) => match def.permission_logical(*clause) {
				Ok(permission) => permission,
				Err(_) => return Ok(None),
			},
			None => LogicalPermission::None,
		};
		if holds_an_unplannable_write(&permission) {
			return Ok(None);
		}
		write.push((*clause, permission));
	}
	let mut fields = Vec::with_capacity(defs.len());
	let derivations = plan_table_derivations(defs, true);
	// Every record of the table is keyed by a record id of the table, so the
	// kind of `id` is checked; a declared field's kind is enforced on write
	// alone and is not.
	let mut schema_fields =
		vec![FieldSchema::checked(FieldPath::field("id"), Kind::Record(vec![table.clone()]))];
	for fd in defs {
		let name = fd.name.to_raw_string();
		let computed = match fd.computed_logical().transpose() {
			Ok(computed) => computed.cloned(),
			Err(_) => return Ok(None),
		};
		let Ok(select) = fd.select_permission_logical() else {
			return Ok(None);
		};
		if holds_an_unplannable_write(&select) {
			return Ok(None);
		}
		let auth = narrowing_auth_limit(&fd.auth_limit).map_err(|e| {
			EngineError::Internal(format!(
				"the AUTH LIMIT of field `{name}` on table `{table}`: {e:#}"
			))
		})?;
		// A name outside the schema's vocabulary (a position such as `a[0]`)
		// declares nothing an expression can be matched against, so the
		// whole table is left to runtime enforcement.
		let Some(path) = FieldPath::from_idiom(&fd.name) else {
			return Ok(None);
		};
		if path.root().is_none() {
			return Ok(None);
		}
		let declared = path.clone();
		schema_fields.retain(|f| f.path != path);
		schema_fields.push(FieldSchema {
			path,
			kind: fd.field_kind.clone().unwrap_or(Kind::Any),
			checked: false,
			flexible: fd.flexible,
			computed: computed.clone().map(|expr| ComputedBody {
				expr,
				kind: fd.field_kind.clone(),
				auth: auth.clone(),
			}),
			select_permission: select.clone(),
		});
		let derivation = derivations
			.iter()
			.find_map(|(key, plan)| (*key == name).then_some(*plan))
			.unwrap_or(Derivation::AsMain);
		fields.push(FieldFacts {
			name,
			path: declared,
			kind: fd.field_kind.clone(),
			computed,
			select,
			derivation,
			auth,
			key_part: fd.key_part,
		});
	}
	let schema = match &def {
		Some(def) if def.schemafull => Schema::closed(schema_fields),
		Some(_) | None => Schema {
			fields: schema_fields,
			openness: crate::expr::logical::Openness::Open,
		},
	};
	Ok(Some(TableFacts {
		schema: Arc::new(schema),
		select,
		write,
		fields,
	}))
}

/// The `AUTH LIMIT` a definition runs under when it narrows the caller. The
/// root-Owner stamp narrows nothing and is the default every definition
/// carries, so it maps to `None`.
fn narrowing_auth_limit(
	limit: &crate::catalog::auth::AuthLimit,
) -> anyhow::Result<Option<crate::iam::AuthLimit>> {
	if limit == &crate::catalog::auth::AuthLimit::new_no_limit() {
		return Ok(None);
	}
	Ok(Some(crate::iam::AuthLimit::try_from(limit)?))
}

// ---------------------------------------------------------------------------
// The rewrite
// ---------------------------------------------------------------------------

struct Resolver<'a> {
	snapshot: &'a CatalogSnapshot,
	caller: &'a Caller,
	registry: &'a FunctionRegistry,
	/// The parameters bound per row by a `SELECT`: `$this`, `$self`, `$parent`.
	row_scoped: &'a [&'a str],
}

/// Where in the plan a source is being resolved.
#[derive(Clone)]
struct Scope {
	/// Whether reads in this position are permission-checked. `false` inside
	/// a permission predicate, as `skip_fetch_perms` is at runtime.
	checks: bool,
	/// The `PERMISSIONS` clause a table source in this position is gated by.
	///
	/// `Select` everywhere a plan reads, which is everywhere but one: the
	/// table and record targets a mutation reaches directly, whose subjects
	/// the statement's own clause gates. A plan nested in a scalar reads, and
	/// so does an explicitly written nested `SELECT`, whatever encloses
	/// either, so the clause follows neither inwards.
	clause: PermissionKind,
	/// The tables whose bodies are being expanded on the way here.
	in_progress: Vec<TableName>,
	version: Version,
}

impl Scope {
	/// The scope a mutation's row source resolves in: its subjects are gated
	/// by `clause`, the clause the statement kind names.
	fn writing(&self, clause: PermissionKind) -> Scope {
		Scope {
			clause,
			..self.clone()
		}
	}

	/// The scope a plan nested in a scalar resolves in: a read, whether or not
	/// a mutation encloses the scalar.
	fn reading(&self) -> Scope {
		Scope {
			clause: PermissionKind::Select,
			..self.clone()
		}
	}
}

/// The parameters that denote the row. `$this` and `$self` name the current
/// row; `$parent` names the document root, which is this row inside a
/// definition's body and at the level of a statement no statement encloses,
/// and the enclosing row inside a nested statement. A read through `$parent`
/// is taken as a read of this row wherever it appears: inside a nested
/// statement that counts a read of the enclosing row against this row's
/// table, which adds a column and withholds nothing.
const ROW_PARAMS: &[&str] = SELECT_ITERATION_PARAMS;

/// The columns of the layer stack over one table source, bottom-up. Empty
/// vectors emit no layer.
#[derive(Default)]
struct Layers {
	/// The derivation layers, lowest rank first.
	derivations: Vec<Vec<LogicalField>>,
	/// The guard layer; empty for an unchecked caller.
	guards: Vec<LogicalField>,
}

impl Layers {
	/// The stack over `source`: each non-empty set of columns as one layer,
	/// the derivations first and the guard layer on top.
	fn over(self, source: LogicalOp) -> LogicalOp {
		let mut op = source;
		for columns in self.derivations.into_iter().chain(std::iter::once(self.guards)) {
			if columns.is_empty() {
				continue;
			}
			let mut fields = Vec::with_capacity(columns.len() + 1);
			fields.push(LogicalField::All);
			fields.extend(columns);
			op = LogicalOp::Project(LogicalProject {
				input: Box::new(op),
				fields: LogicalFields::Select(fields),
				omit: Vec::new(),
			});
		}
		op
	}
}

impl Resolver<'_> {
	fn resolve_op(&mut self, op: LogicalOp, scope: &Scope) -> Result<LogicalOp, Error> {
		match op {
			relational @ (LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Write(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Window(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)) => self.resolve_pipeline(relational, scope),
			// A join's inputs are pipelines of their own; its keys and filter
			// may hold plans.
			// A join lifted from a definer's clause was bound when it was
			// lifted, its right side in the clause's scope: resolving it again
			// here would read that side as the caller.
			LogicalOp::Join(join) if join.frame != crate::expr::logical::EvalFrame::caller() => {
				Ok(LogicalOp::Join(join))
			}
			join @ LogicalOp::Join(_) => {
				let mut op = join.map_children(|child| self.resolve_op(child, scope))?;
				self.resolve_scalars(&mut op, scope)?;
				Ok(op)
			}
			// A statement after one that changes the catalog reads the catalog
			// that change leaves, which this bind cannot see, and a pipeline
			// resolved here keeps its layers when bound again: it is left for
			// the bind of its own, where it is planned.
			LogicalOp::Sequence(LogicalSequence {
				stmts,
			}) => {
				let mut changed = false;
				let stmts = stmts
					.into_iter()
					.map(|stmt| {
						if changed {
							return Ok(stmt);
						}
						changed = crate::exec::planner::fold::changes_the_catalog(&stmt);
						self.resolve_op(stmt, scope)
					})
					.collect::<Result<_, Error>>()?;
				Ok(LogicalOp::Sequence(LogicalSequence {
					stmts,
				}))
			}
			// So is a loop body that changes the catalog, which a later
			// iteration reads after the change.
			LogicalOp::Foreach(foreach)
				if crate::exec::planner::fold::changes_the_catalog(&foreach.body) =>
			{
				let mut op = LogicalOp::Foreach(foreach);
				self.resolve_scalars(&mut op, scope)?;
				Ok(op)
			}
			// A statement: its inputs are statements or pipelines of their
			// own, and its scalars may hold plans.
			statement @ (LogicalOp::Eval(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Info(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::Sleep(_)) => {
				let mut op = statement.map_children(|child| self.resolve_op(child, scope))?;
				self.resolve_scalars(&mut op, scope)?;
				Ok(op)
			}
		}
	}

	/// Resolves the plans nested in the scalars `op` itself owns.
	fn resolve_scalars(&mut self, op: &mut LogicalOp, scope: &Scope) -> Result<(), Error> {
		op.visit_scalars_mut(&mut |scalar| self.resolve_expr(scalar, scope))
	}

	/// Resolves the plans nested in one scalar. A plan inside a closure body
	/// is left unresolved: the closure is invoked by later statements, in
	/// their transactions, and its body resolves its sources when it runs.
	fn resolve_expr(&mut self, expr: &mut LogicalExpr, scope: &Scope) -> Result<(), Error> {
		visit_binding_mut(expr, &mut |e| {
			if let LogicalExpr::Subquery(plan) = e {
				let taken = std::mem::replace(plan.as_mut(), placeholder());
				// A plan a scalar holds reads: a statement's own clause gates
				// the rows it writes, not the rows a subquery of its condition
				// or its data clause reads.
				**plan = self.resolve_op(taken, &scope.reading())?;
			}
			Ok(())
		})
	}

	/// Resolves one relational pipeline: the stages down to its leaf, then the
	/// leaf, then the stages again with the leaf's declarations in hand.
	fn resolve_pipeline(&mut self, op: LogicalOp, scope: &Scope) -> Result<LogicalOp, Error> {
		let (mut stages, leaf) = peel(op);
		// The innermost `VERSION` is the one the leaf reads under.
		let mut scope = scope.clone();
		for stage in &stages {
			if let LogicalOp::VersionScope(LogicalVersionScope {
				version,
				..
			}) = stage
			{
				scope.version = resolve_version(
					version,
					self.param_versions(&scope),
					self.snapshot.stamps.as_ref(),
				);
			}
		}
		for stage in &mut stages {
			// An analyzer node exists only once the pipeline is bound, and its
			// scalars were resolved when it was built, in the scope its clause
			// reads in: a permission predicate unchecked, a computed body as
			// its definer. Resolving them again in this scope would read them
			// as the caller.
			if matches!(stage, LogicalOp::PermissionFilter(_)) || stage.is_layer() {
				continue;
			}
			self.resolve_scalars(stage, &scope)?;
		}
		// A pipeline already carrying analyzer nodes was bound before; its
		// leaf is not resolved again.
		let already_bound = stages
			.iter()
			.any(|stage| matches!(stage, LogicalOp::PermissionFilter(_)) || stage.is_layer());
		let leaf = match leaf {
			// A mutation is its own scope, and nothing around it resolves what
			// it holds: `stage_input` does not name it, so `peel` stops here,
			// and a leaf is not one of the stages passed to `resolve_scalars`
			// above. Both of its sources are therefore resolved here — a
			// `Rows` input as the pipeline it is, and the scalars a `Targets`
			// list owns, either of which may hold a plan. `EXPLAIN CREATE …`
			// is what makes this a leaf under stages rather than always the
			// whole plan.
			//
			// Resolution binds sources and substitutes params; it does not
			// settle the write's own permission obligations. The write
			// operator's stage list carries them.
			LogicalOp::Write(write) => {
				// `map_children` reaches the `Rows` source alone: the rows the
				// statement writes, which its own `PERMISSIONS` clause gates
				// as far as the targets it names directly — an explicitly
				// written nested `SELECT` among them reads under `FOR select`.
				// A `Targets` list reads nothing, and the scalars below are
				// read wherever they are written.
				let rows = scope.writing(write_clause(&write.statement));
				let mut op =
					LogicalOp::Write(write).map_children(|child| self.resolve_op(child, &rows))?;
				// A leaf the caller may not write yields no row; a list drops it.
				if let LogicalOp::Write(LogicalWrite {
					source: crate::expr::logical::LogicalWriteSource::Rows(input),
					..
				}) = &mut op
				{
					drop_refused_leaves(input);
				}
				// The `WHERE` of an `UPDATE`, `DELETE` or `UPSERT` compares the enum fields of the
				// table it writes by declaration, as a `SELECT`'s does.
				if let LogicalOp::Write(write) = &mut op
					&& let Some(table) = written_table(&write.source)
					&& let Some(cond) = write.cond.as_mut()
					&& let Some(facts) = self.facts(&table, &scope)
				{
					super::enum_order::rewrite_condition(
						cond,
						&|name| {
							facts
								.fields
								.iter()
								.find(|f| f.name == name)
								.and_then(|f| f.kind.as_ref())
						},
						&|name| self.snapshot.unions.get(name).cloned(),
					);
				}
				self.resolve_scalars(&mut op, &scope)?;
				op
			}
			LogicalOp::Source(LogicalSource {
				source,
				schema,
				with,
				for_update,
			}) => {
				let source = match source {
					SourceRef::Param(param) => self.param_source(param, &scope),
					other @ (SourceRef::Table(_)
					| SourceRef::RecordId(_)
					| SourceRef::Mock(_)
					| SourceRef::Dynamic(_)) => other,
				};
				let table = match &source {
					SourceRef::Table(table) => Some(table.clone()),
					SourceRef::RecordId(rid) => Some(rid.table.clone()),
					SourceRef::Mock(_) | SourceRef::Param(_) | SourceRef::Dynamic(_) => None,
				};
				let facts = match (&table, already_bound) {
					(Some(table), false) => self.facts(table, &scope),
					(Some(_), true) | (None, _) => None,
				};
				let mut leaf = LogicalOp::Source(LogicalSource {
					source,
					schema,
					with,
					for_update,
				});
				self.resolve_scalars(&mut leaf, &scope)?;
				match (facts, table) {
					(Some(facts), Some(table)) => {
						super::enum_order::rewrite_stages(
							&mut stages,
							&|name| {
								facts
									.fields
									.iter()
									.find(|f| f.name == name)
									.and_then(|f| f.kind.as_ref())
							},
							&|name| self.snapshot.unions.get(name).cloned(),
						);
						self.table_stack(leaf, &table, &facts, &stages, &scope)?
					}
					(None, _) | (Some(_), None) => leaf,
				}
			}
			// An explicitly written nested `SELECT` reads under `FOR select`,
			// wherever it stands: as a mutation's target it names the rows the
			// statement then writes, and naming them is a read. The
			// statement's own clause gates those rows where the mutation
			// reaches them directly, as a table or record target.
			LogicalOp::Subquery(LogicalSubquery {
				input,
			}) => LogicalOp::Subquery(LogicalSubquery {
				input: Box::new(self.resolve_pipeline(*input, &scope.reading())?),
			}),
			LogicalOp::UnionAll(LogicalUnionAll {
				inputs,
			}) => {
				// The stages over a union read the rows of every input, so a field is an enum
				// there only when each input is a table that declares it as the same one.
				if !already_bound {
					let sources: Vec<Option<Arc<TableFacts>>> = inputs
						.iter()
						.map(|input| {
							if let LogicalOp::Source(LogicalSource {
								source: SourceRef::Table(table),
								..
							}) = input
							{
								self.facts(table, &scope)
							} else {
								None
							}
						})
						.collect();
					if !sources.is_empty() && sources.iter().all(Option::is_some) {
						let tables: Vec<&Arc<TableFacts>> = sources.iter().flatten().collect();
						fn declared<'f>(facts: &'f TableFacts, name: &str) -> Option<&'f Kind> {
							facts
								.fields
								.iter()
								.find(|f| f.name == name)
								.and_then(|f| f.kind.as_ref())
						}
						super::enum_order::rewrite_stages(
							&mut stages,
							&|name| {
								let first = declared(tables[0], name)?;
								tables[1..]
									.iter()
									.all(|facts| declared(facts, name) == Some(first))
									.then_some(first)
							},
							&|name| self.snapshot.unions.get(name).cloned(),
						);
					}
				}
				LogicalOp::UnionAll(LogicalUnionAll {
					inputs: inputs
						.into_iter()
						.map(|input| {
							self.resolve_union_input(input, &scope, already_bound, &stages)
						})
						.collect::<Result<_, Error>>()?,
				})
			}
			mut leaf @ (LogicalOp::Values(_) | LogicalOp::Match(_)) => {
				self.resolve_scalars(&mut leaf, &scope)?;
				leaf
			}
			// A join is a leaf to `peel`: each input is a pipeline of its own.
			join @ LogicalOp::Join(_) => self.resolve_op(join, &scope)?,
			// `peel` stops at a leaf; every stage is in `stages`.
			LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Window(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_) => unreachable!("peel leaves only a leaf"),
			// A statement beneath a stage (`EXPLAIN RETURN …`, `EXPLAIN { … }`):
			// its own inputs and scalars are resolved as a statement's are,
			// and the stage is put back over the result.
			statement @ (LogicalOp::Eval(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Info(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::Sleep(_)) => self.resolve_op(statement, &scope)?,
		};
		Ok(reassemble(stages, leaf))
	}

	/// One input of a multi-source `FROM`. The stages above the union see the
	/// rows of every input, so each table gets its own stack, built for those
	/// stages, over its own input.
	fn resolve_union_input(
		&mut self,
		input: LogicalOp,
		scope: &Scope,
		already_bound: bool,
		stages: &[LogicalOp],
	) -> Result<LogicalOp, Error> {
		match input {
			LogicalOp::Source(LogicalSource {
				source,
				schema,
				with,
				for_update,
			}) => {
				let source = match source {
					SourceRef::Param(param) => self.param_source(param, scope),
					other @ (SourceRef::Table(_)
					| SourceRef::RecordId(_)
					| SourceRef::Mock(_)
					| SourceRef::Dynamic(_)) => other,
				};
				let table = match &source {
					SourceRef::Table(table) => Some(table.clone()),
					SourceRef::RecordId(rid) => Some(rid.table.clone()),
					SourceRef::Mock(_) | SourceRef::Param(_) | SourceRef::Dynamic(_) => None,
				};
				let mut leaf = LogicalOp::Source(LogicalSource {
					source,
					schema,
					with,
					for_update,
				});
				self.resolve_scalars(&mut leaf, scope)?;
				let facts = match (&table, already_bound) {
					(Some(table), false) => self.facts(table, scope),
					(Some(_), true) | (None, _) => None,
				};
				match (facts, table) {
					(Some(facts), Some(table)) => {
						self.table_stack(leaf, &table, &facts, stages, scope)
					}
					(None, _) | (Some(_), None) => Ok(leaf),
				}
			}
			// A nested `SELECT` among several targets reads under `FOR select`,
			// as it does anywhere else; only the targets the mutation reaches
			// directly are gated by the statement's own clause.
			LogicalOp::Subquery(LogicalSubquery {
				input,
			}) => Ok(LogicalOp::Subquery(LogicalSubquery {
				input: Box::new(self.resolve_pipeline(*input, &scope.reading())?),
			})),
			other @ (LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Write(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Window(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Info(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::Sleep(_)) => {
				let mut other = self.order_branch_enums(other, scope);
				self.resolve_scalars(&mut other, scope)?;
				Ok(other)
			}
		}
	}

	/// `branch`, one query of a set operation, with the comparisons, sort keys and group
	/// extremes of its own stages over the enum fields of the table it reads ordering by
	/// declaration, as a pipeline's do ([`super::enum_order::rewrite_stages`]). A branch that is
	/// not a pipeline over a table, or one already bound, is returned as it is.
	fn order_branch_enums(&self, branch: LogicalOp, scope: &Scope) -> LogicalOp {
		let (mut stages, leaf) = peel(branch);
		let bound = stages
			.iter()
			.any(|stage| matches!(stage, LogicalOp::PermissionFilter(_)) || stage.is_layer());
		let table = if let LogicalOp::Source(LogicalSource {
			source,
			..
		}) = &leaf
		{
			match source {
				SourceRef::Table(table) => Some(table.clone()),
				SourceRef::RecordId(rid) => Some(rid.table.clone()),
				SourceRef::Mock(_) | SourceRef::Param(_) | SourceRef::Dynamic(_) => None,
			}
		} else {
			None
		};
		if !bound
			&& !stages.is_empty()
			&& let Some(table) = table
			&& let Some(facts) = self.facts(&table, scope)
		{
			super::enum_order::rewrite_stages(
				&mut stages,
				&|name| facts.fields.iter().find(|f| f.name == name).and_then(|f| f.kind.as_ref()),
				&|name| self.snapshot.unions.get(name).cloned(),
			);
		}
		reassemble(stages, leaf)
	}

	/// The facts for `table` in `scope`, or `None` when the leaf is to stay
	/// unresolved: the table is being expanded already, the gather did not
	/// load it under this version, or one of its clauses has no logical form.
	fn facts(&self, table: &TableName, scope: &Scope) -> Option<Arc<TableFacts>> {
		if scope.in_progress.contains(table) {
			return None;
		}
		let stamp = scope.version.stamp()?;
		self.snapshot.tables.get(&(table.clone(), stamp)).cloned().flatten()
	}

	/// The scope a permission predicate of `table` is resolved in.
	/// The source `FROM $param` reads in `scope`: the table or record the
	/// statement's value of the parameter names, or the parameter where that
	/// value is not known when the plan is bound. Inside a definition's body
	/// — a permission predicate or a `COMPUTED` body, definer code that holds
	/// none of the caller's bindings — the caller's value is never taken; the
	/// parameter is read where the body runs.
	fn param_source(&self, param: crate::expr::Param, scope: &Scope) -> SourceRef {
		if !scope.in_progress.is_empty() {
			return SourceRef::Param(param);
		}
		match self.snapshot.param_sources.get(param.as_str()) {
			Some(resolved) => resolved.clone(),
			None => SourceRef::Param(param),
		}
	}

	/// The version stamps of the parameters a `VERSION` reads in `scope`: none
	/// inside a definition's body, for the reason [`Self::param_source`] gives.
	fn param_versions(&self, scope: &Scope) -> &HashMap<String, u64> {
		static NONE: std::sync::LazyLock<HashMap<String, u64>> =
			std::sync::LazyLock::new(HashMap::new);
		if scope.in_progress.is_empty() {
			&self.snapshot.param_versions
		} else {
			&NONE
		}
	}

	fn permission_scope(&self, table: &TableName, scope: &Scope) -> Scope {
		let mut inner = scope.clone();
		inner.checks = false;
		inner.in_progress.push(table.clone());
		inner
	}

	/// The scope a `COMPUTED` body of `table` is resolved in: checked as the
	/// definer's authority, narrowed by the field's `AUTH LIMIT`, would be.
	fn computed_scope(&self, table: &TableName, field: &FieldFacts, scope: &Scope) -> Scope {
		let mut inner = scope.clone();
		inner.checks = match &field.auth {
			Some(limit) => {
				self.caller.checks_permissions_as(&self.caller.auth().new_limited(limit))
			}
			None => self.caller.checks_permissions(),
		};
		inner.in_progress.push(table.clone());
		inner
	}

	/// The source with its schema attached and its table permission above it,
	/// for a checked caller.
	///
	/// The clause is the one the scope names: `select` for a read, and the
	/// statement's own clause for the rows a mutation writes. A clause of
	/// `NONE` becomes a predicate of `false`, so the source yields nothing and
	/// the statement is not failed: a target the caller may not write is
	/// refused by yielding nothing rather than by raising.
	///
	/// A *conditional* write clause stops there: it is judged per record by
	/// the write's own gates, twice for each record the statement edits —
	/// once against the record as found and once against it as edited — and a
	/// filter here would be a third judgement of a predicate that may write.
	/// The line is drawn between a clause that is `None` and one that is
	/// `Specific`: only `None` refuses the target.
	/// So for a mutation's row source this node carries a refusal and never a
	/// guard, and [`GuardKind`] on it says which.
	fn filtered_source(
		&mut self,
		leaf: LogicalOp,
		facts: &TableFacts,
		table: &TableName,
		scope: &Scope,
	) -> Result<LogicalOp, Error> {
		let LogicalOp::Source(LogicalSource {
			source,
			with,
			for_update,
			..
		}) = leaf
		else {
			unreachable!("filtered_source takes a Source")
		};
		let source = LogicalOp::Source(LogicalSource {
			source,
			schema: Some(Arc::clone(&facts.schema)),
			with,
			for_update,
		});
		if !scope.checks {
			return Ok(source);
		}
		let permission = facts.permission(scope.clause).ok_or_else(|| {
			EngineError::Internal(format!(
				"the `{}` permission of table `{table}` was not lowered for this plan",
				scope.clause
			))
		})?;
		let guard = match scope.clause {
			PermissionKind::Select => GuardKind::Read,
			PermissionKind::Create | PermissionKind::Update | PermissionKind::Delete => {
				GuardKind::Write
			}
		};
		let predicate = match permission {
			LogicalPermission::Full => return Ok(source),
			LogicalPermission::None => LogicalExpr::Literal(LogicalLiteral::Bool(false)),
			LogicalPermission::Specific(_) if guard == GuardKind::Write => return Ok(source),
			LogicalPermission::Specific(guard) => {
				let mut predicate = guard.clone();
				self.resolve_expr(&mut predicate, &self.permission_scope(table, scope))?;
				predicate
			}
		};
		Ok(LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input: Box::new(source),
			predicate,
			auth: None,
			guard,
		}))
	}

	/// The table source `leaf` with its declarations written above it: the
	/// filtered source under the layer stack the stages need. The leaf is
	/// returned as it came — unresolved, for the scan to enforce — when the
	/// declarations have no stack (see the module documentation).
	fn table_stack(
		&mut self,
		leaf: LogicalOp,
		table: &TableName,
		facts: &TableFacts,
		stages: &[LogicalOp],
		scope: &Scope,
	) -> Result<LogicalOp, Error> {
		let full_table = matches!(
			leaf,
			LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(_),
				..
			})
		);
		let Some(layers) = self.plan_layers(table, facts, stages, scope)? else {
			return Ok(leaf);
		};
		// Every field the stack may set on the row: a computed field, or a
		// restricted field's root. A `MATCHES` on one has no index that may
		// serve it, whether or not the field ends up read.
		let columns: HashSet<Strand> = facts
			.fields
			.iter()
			.filter(|field| field.computed.is_some() || (scope.checks && field.restricted()))
			.map(|field| field.root().clone())
			.collect();
		self.refuse_matches(stages, &columns, full_table)?;
		let source = self.filtered_source(leaf, facts, table, scope)?;
		Ok(layers.over(source))
	}

	/// The layer stack for `table` beneath `stages`, or `None` when its
	/// declarations must be enforced by the scan.
	///
	/// This is the encoding the module documentation states: derivation
	/// layers for the computed fields the stages and the judged guards read,
	/// then, for a checked caller, the guard layer with a column per field
	/// that can reach the caller, a re-derived computed field's body being the
	/// value its column evaluates against the reduced record.
	fn plan_layers(
		&mut self,
		table: &TableName,
		facts: &TableFacts,
		stages: &[LogicalOp],
		scope: &Scope,
	) -> Result<Option<Layers>, Error> {
		let computed: Vec<&FieldFacts> =
			facts.fields.iter().filter(|field| field.computed.is_some()).collect();
		let restricted: Vec<&FieldFacts> = if scope.checks {
			facts.fields.iter().filter(|field| field.restricted()).collect()
		} else {
			Vec::new()
		};
		if computed.is_empty() && restricted.is_empty() {
			return Ok(Some(Layers::default()));
		}
		// A computed field is declared on a single name and no restricted
		// path descends through one; the catalog enforces both at definition
		// time, and a table that has neither has no layer form.
		if computed.iter().any(|field| field.path.as_field().is_none()) {
			return Ok(None);
		}
		let computed_names: HashSet<Strand> =
			computed.iter().map(|field| field.root().clone()).collect();
		if restricted
			.iter()
			.any(|field| field.path.as_field().is_none() && computed_names.contains(field.root()))
		{
			return Ok(None);
		}
		// `$value` is replaced by an expression of the row; inside a nested
		// plan that expression would read the nested plan's rows.
		for field in &restricted {
			if let LogicalPermission::Specific(guard) = &field.select
				&& nested_plan_mentions(guard, &["value"])
			{
				return Ok(None);
			}
		}

		// The computed dependency order. A body that reads the row as a whole
		// reads every other computed field, so it ranks after all of them;
		// several such bodies rank in definition order.
		let whole_row: HashSet<Strand> = computed
			.iter()
			.filter(|field| {
				let body = field.computed.as_ref().expect("a computed field");
				self.reads_whole_row(body, ROW_PARAMS, self.row_scoped)
			})
			.map(|field| field.root().clone())
			.collect();
		let mut deps: HashMap<Strand, HashSet<Strand>> = HashMap::new();
		for (index, field) in computed.iter().enumerate() {
			let body = field.computed.as_ref().expect("a computed field");
			let mut reads = reads_of(body, &computed_names, ROW_PARAMS);
			reads.remove(field.root());
			if whole_row.contains(field.root()) {
				reads.extend(
					computed[..index]
						.iter()
						.map(|other| other.root().clone())
						.filter(|other| whole_row.contains(other)),
				);
				reads.extend(
					computed_names
						.iter()
						.filter(|other| !whole_row.contains(*other) && *other != field.root())
						.cloned(),
				);
			}
			deps.insert(field.root().clone(), reads);
		}
		let Some(rank_of) = ranks(&computed, &deps) else {
			return Ok(None);
		};
		let closure = |seed: HashSet<Strand>| -> HashSet<Strand> {
			let mut out = HashSet::new();
			let mut worklist: Vec<Strand> = seed.into_iter().collect();
			while let Some(name) = worklist.pop() {
				if !out.insert(name.clone()) {
					continue;
				}
				if let Some(reads) = deps.get(&name) {
					worklist.extend(reads.iter().cloned());
				}
			}
			out
		};

		// What the stages read: the computed fields and the restricted stored
		// roots by name, or the whole row.
		let stored_roots: HashSet<Strand> = restricted
			.iter()
			.filter(|field| field.computed.is_none())
			.map(|field| field.root().clone())
			.collect();
		let named: HashSet<Strand> = computed_names.union(&stored_roots).cloned().collect();
		let (mut all, reads) = self.statement_reads(stages, &named);
		let read_computed: HashSet<Strand> =
			reads.iter().filter(|name| computed_names.contains(*name)).cloned().collect();
		let read_roots: HashSet<Strand> =
			reads.into_iter().filter(|name| stored_roots.contains(name)).collect();

		// Cr: the computed fields whose value depends on what the caller may
		// see of the stored fields, and everything reading one of them.
		let mut rederived: HashSet<Strand> = HashSet::new();
		if !stored_roots.is_empty() {
			for field in &computed {
				let body = field.computed.as_ref().expect("a computed field");
				if whole_row.contains(field.root())
					|| reads_any_of(body, &stored_roots, ROW_PARAMS)
					|| field.dereferences_own_id(body)
				{
					rederived.insert(field.root().clone());
				}
			}
			loop {
				let before = rederived.len();
				for field in &computed {
					if deps[field.root()].iter().any(|dep| rederived.contains(dep)) {
						rederived.insert(field.root().clone());
					}
				}
				if rederived.len() == before {
					break;
				}
			}
		}
		// A re-derivation reads the reduced record; a body reading it as a
		// whole has no expression for that view.
		if rederived.iter().any(|name| whole_row.contains(name)) {
			return Ok(None);
		}

		// The fields outside Cr whose select guard a checked caller is judged
		// by before the body runs, or denied by without it: neither has a
		// derivation column. A field in Cr keeps its re-derivation column,
		// which is already a guard over the body.
		let deferred: HashSet<Strand> = if scope.checks {
			computed
				.iter()
				.filter(|field| {
					field.derivation != Derivation::AsMain && !rederived.contains(field.root())
				})
				.map(|field| field.root().clone())
				.collect()
		} else {
			HashSet::new()
		};
		let judge_first = |field: &FieldFacts| {
			field.derivation == Derivation::JudgeFirst && deferred.contains(field.root())
		};

		// The guards in logical form, their nested plans resolved once, in the
		// permission scope.
		let mut guards: HashMap<String, LogicalExpr> = HashMap::new();
		for field in &restricted {
			if let LogicalPermission::Specific(guard) = &field.select {
				let mut predicate = guard.clone();
				self.resolve_expr(&mut predicate, &self.permission_scope(table, scope))?;
				guards.insert(field.name.clone(), predicate);
			}
		}
		// The bodies in logical form, resolved once each in the scope the body
		// runs in, as they are found to be evaluated.
		let mut bodies: HashMap<Strand, LogicalExpr> = HashMap::new();

		// What the caller receives, and the guards that judge it. A guard is a
		// column only where its field can reach the caller: a stored field's
		// where the statement reads the field or a re-derivation reads it off
		// the reduced record — a body that dereferences its own record reads
		// that record whole — and a computed field's where the statement reads
		// the field. A guard so judged that reads the row as a whole puts every
		// field on it, which is settled by starting over with everything read.
		let (needed, rederived_needed, guarded_roots, guard_reads) = loop {
			let needed: HashSet<Strand> = if all {
				computed_names.clone()
			} else {
				closure(read_computed.clone())
			};
			let mut rederived_needed: Vec<&FieldFacts> = computed
				.iter()
				.copied()
				.filter(|field| needed.contains(field.root()) && rederived.contains(field.root()))
				.collect();
			rederived_needed.sort_by_key(|field| rank_of[field.root()]);
			for field in &rederived_needed {
				if !bodies.contains_key(field.root()) {
					let mut body = field.computed.clone().expect("a computed field");
					self.resolve_expr(&mut body, &self.computed_scope(table, field, scope))?;
					bodies.insert(field.root().clone(), body);
				}
			}
			let guarded_roots: HashSet<Strand> = if all
				|| rederived_needed
					.iter()
					.any(|field| field.dereferences_own_id(&bodies[field.root()]))
			{
				stored_roots.clone()
			} else {
				let mut roots = read_roots.clone();
				for field in &rederived_needed {
					roots.extend(reads_of(&bodies[field.root()], &stored_roots, ROW_PARAMS));
				}
				roots
			};
			let mut guard_reads: HashSet<Strand> = HashSet::new();
			let mut whole = false;
			for field in &restricted {
				let LogicalPermission::Specific(guard) = &field.select else {
					continue;
				};
				let judged = if field.computed.is_some() {
					needed.contains(field.root())
				} else {
					guarded_roots.contains(field.root())
				};
				if !judged {
					continue;
				}
				whole |= self.reads_whole_row(guard, ROW_PARAMS, self.row_scoped);
				guard_reads.extend(reads_of(guard, &computed_names, ROW_PARAMS));
			}
			if whole && !all {
				all = true;
				continue;
			}
			break (needed, rederived_needed, guarded_roots, guard_reads);
		};
		let is_rederived =
			|name: &Strand| rederived_needed.iter().any(|field| field.root() == name);

		// The first derivations: what the caller receives that is not
		// re-derived, and what a judged guard reads with everything it depends
		// on — a guard is judged against the unreduced record, so of a
		// re-derived field it reads the first derivation.
		let derived: HashSet<Strand> = {
			let mut derived: HashSet<Strand> = needed
				.iter()
				.filter(|name| !rederived.contains(*name) && !deferred.contains(*name))
				.cloned()
				.collect();
			derived.extend(closure(guard_reads));
			derived
		};
		for field in &computed {
			let body_needed = derived.contains(field.root())
				|| (judge_first(field) && needed.contains(field.root()));
			if body_needed && !bodies.contains_key(field.root()) {
				let mut body = field.computed.clone().expect("a computed field");
				self.resolve_expr(&mut body, &self.computed_scope(table, field, scope))?;
				bodies.insert(field.root().clone(), body);
			}
		}

		// The derivation layers, one per rank, columns in the order the catalog lists the fields.
		let mut derivations: Vec<Vec<LogicalField>> = Vec::new();
		for field in &computed {
			if !derived.contains(field.root()) {
				continue;
			}
			let rank = rank_of[field.root()];
			if derivations.len() <= rank {
				derivations.resize_with(rank + 1, Vec::new);
			}
			derivations[rank].push(LogicalField::column(
				field.root().as_str(),
				bodies[field.root()].clone(),
				field.kind.clone(),
				field.derivation_frame(),
			));
		}
		if !scope.checks {
			return Ok(Some(Layers {
				derivations,
				guards: Vec::new(),
			}));
		}

		// The guard layer: one column per guarded stored root.
		let mut guard_columns = Vec::new();
		let mut roots: Vec<&Strand> = Vec::new();
		for field in restricted.iter().filter(|field| field.computed.is_none()) {
			if guarded_roots.contains(field.root()) && !roots.contains(&field.root()) {
				roots.push(field.root());
			}
		}
		for root in roots {
			let under: Vec<&FieldFacts> = restricted
				.iter()
				.copied()
				.filter(|field| field.computed.is_none() && field.root() == root)
				.collect();
			let Some(expr) = guard_column(root, &under, &facts.schema, &guards, self.registry)
			else {
				return Ok(None);
			};
			let auth = under[0].auth.clone();
			guard_columns.push(LogicalField::column(
				root.as_str(),
				expr,
				None,
				EvalFrame::permission(auth),
			));
		}
		// The columns on the computed fields the caller receives, in dependency
		// order: a re-derived field's value is its body, which the layer
		// evaluates against the reduced record and lands for every later body
		// to read, and its own guard is judged on that value as `$value`.
		let mut on_computed: Vec<&FieldFacts> =
			computed.iter().copied().filter(|field| needed.contains(field.root())).collect();
		on_computed.sort_by_key(|field| rank_of[field.root()]);
		for field in on_computed {
			let name = field.root();
			// The body a guard column evaluates: a re-derivation, or the body of
			// a field whose guard is judged before it runs.
			let rederivation =
				(is_rederived(name) || judge_first(field)).then(|| bodies[name].clone());
			let column = match (&field.select, rederivation) {
				(LogicalPermission::Full, None) => continue,
				(LogicalPermission::Full, Some(body)) => LogicalField::column(
					name.as_str(),
					body,
					field.kind.clone(),
					EvalFrame::computed(field.auth.clone()),
				),
				// A denied field's re-derivation is still what a later body
				// reads of it; one nothing reads is `NONE` outright.
				(LogicalPermission::None, Some(body))
					if rederived_needed.iter().any(|other| deps[other.root()].contains(name)) =>
				{
					LogicalField::column(
						name.as_str(),
						judge_column(LogicalExpr::Literal(LogicalLiteral::Bool(false)), body),
						field.kind.clone(),
						EvalFrame::permission(field.auth.clone()),
					)
				}
				(LogicalPermission::None, _) => LogicalField::column(
					name.as_str(),
					LogicalExpr::Literal(LogicalLiteral::None),
					None,
					EvalFrame::permission(field.auth.clone()),
				),
				(LogicalPermission::Specific(_), rederivation) => {
					let value = rederivation.unwrap_or_else(|| LogicalExpr::Field(name.clone()));
					LogicalField::column(
						name.as_str(),
						judge_column(guards[&field.name].clone(), value),
						field.kind.clone(),
						EvalFrame::permission(field.auth.clone()),
					)
				}
			};
			guard_columns.push(column);
		}
		Ok(Some(Layers {
			derivations,
			guards: guard_columns,
		}))
	}

	/// The fields in `names` the stages read off the row by name — a nested
	/// statement that inherits the row included — and whether some stage
	/// reads the row as a whole: a `*` projection, a row parameter, a call
	/// evaluated against the row, a dynamic index on the row, or a nested
	/// plan mentioning a row parameter, so that every field has to be on it.
	fn statement_reads(
		&self,
		stages: &[LogicalOp],
		names: &HashSet<Strand>,
	) -> (bool, HashSet<Strand>) {
		let mut all = false;
		let mut reads = HashSet::new();
		for stage in stages {
			if let LogicalOp::Project(LogicalProject {
				fields,
				..
			})
			| LogicalOp::Aggregate(LogicalAggregate {
				fields,
				..
			}) = stage && fields.has_all_selection()
			{
				all = true;
			}
			let Ok(()) = stage.visit_scalars(&mut |scalar| {
				if self.reads_whole_row(scalar, ROW_PARAMS, self.row_scoped) {
					all = true;
				}
				reads.extend(reads_of(scalar, names, ROW_PARAMS));
				Ok::<_, std::convert::Infallible>(())
			});
		}
		(all, reads)
	}

	/// Refuses a `MATCHES` in a user stage whose left operand reads a field in
	/// `columns`, with the expression as the user wrote it.
	///
	/// `MATCHES` is answered by a full-text index and nothing else. An index
	/// over a field the caller may not select is withheld from the caller, and
	/// a computed field is never indexed, so a `MATCHES` on either has no index
	/// that may serve it and the statement fails — the outcome the physical
	/// planner reaches for any `MATCHES` no index supports. A source of one
	/// record gets no full-text executor, and a `MATCHES` over it evaluates to
	/// `false` per row rather than failing, so nothing is refused unless the
	/// source is the whole table.
	fn refuse_matches(
		&self,
		stages: &[LogicalOp],
		columns: &HashSet<Strand>,
		full_table: bool,
	) -> Result<(), Error> {
		if !full_table || columns.is_empty() {
			return Ok(());
		}
		let check = |expr: &LogicalExpr| -> Result<(), Error> {
			let found = expr.visit(&mut |e| {
				if let LogicalExpr::Binary {
					left,
					op: BinaryOperator::Matches(_),
					..
				} = e && let Some((name, _)) = leading_field(left, ROW_PARAMS)
					&& columns.contains(name)
				{
					return std::ops::ControlFlow::Break(e.to_sql());
				}
				std::ops::ControlFlow::Continue(())
			});
			match found {
				std::ops::ControlFlow::Break(exp) => Err(crate::idx::Error::NoIndexFoundForMatch {
					exp,
				}
				.into()),
				std::ops::ControlFlow::Continue(()) => Ok(()),
			}
		};
		for stage in stages {
			match stage {
				LogicalOp::Filter(LogicalFilter {
					predicate,
					..
				}) => check(predicate)?,
				LogicalOp::Project(LogicalProject {
					fields,
					..
				}) => {
					let mut exprs = Vec::new();
					fields.exprs(&mut exprs);
					for expr in exprs {
						check(expr)?;
					}
				}
				LogicalOp::Source(_)
				| LogicalOp::Values(_)
				| LogicalOp::Match(_)
				| LogicalOp::Write(_)
				| LogicalOp::Subquery(_)
				| LogicalOp::UnionAll(_)
				| LogicalOp::Join(_)
				| LogicalOp::PermissionFilter(_)
				| LogicalOp::Split(_)
				| LogicalOp::Aggregate(_)
				| LogicalOp::Distinct(_)
				| LogicalOp::Sort(_)
				| LogicalOp::Window(_)
				| LogicalOp::Limit(_)
				| LogicalOp::Fetch(_)
				| LogicalOp::Timeout(_)
				| LogicalOp::VersionScope(_)
				| LogicalOp::Only(_)
				| LogicalOp::Explain(_)
				| LogicalOp::Eval(_)
				| LogicalOp::If(_)
				| LogicalOp::Throw(_)
				| LogicalOp::Break(_)
				| LogicalOp::Continue(_)
				| LogicalOp::Sequence(_)
				| LogicalOp::Let(_)
				| LogicalOp::Return(_)
				| LogicalOp::Foreach(_)
				| LogicalOp::Info(_)
				| LogicalOp::Ddl(_)
				| LogicalOp::Sleep(_) => {}
			}
		}
		Ok(())
	}

	/// Whether `expr`, evaluated against a row, reads the row as a whole or in
	/// a way no column stands for: a bare row parameter, a wildcard, a
	/// dynamic or method access on the row, a call evaluated against the row
	/// ([`evaluates_against_row`]), or a nested plan mentioning one of
	/// `nested_scoped`.
	fn reads_whole_row(
		&self,
		expr: &LogicalExpr,
		row_params: &[&str],
		nested_scoped: &[&str],
	) -> bool {
		match expr {
			// Invoking a value runs a body this expression does not hold —
			// see [`super::reads::invokes_an_unseen_body`]. What it reads
			// off the row cannot be told from the call, so it demands the
			// whole row and every field that can reach the caller keeps its
			// guard column. Tried before the path and catch-all arms, both
			// of which would otherwise take these shapes.
			expr if super::reads::invokes_an_unseen_body(expr, self.registry) => true,
			LogicalExpr::Param(param) => row_params.contains(&param.as_str()),
			LogicalExpr::Field(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Access {
				..
			}
			| LogicalExpr::Index {
				..
			}
			| LogicalExpr::First(_)
			| LogicalExpr::Last(_)
			| LogicalExpr::Each(_)
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
			| LogicalExpr::Broadcast {
				..
			} => self.path_reads_whole_row(expr, row_params, nested_scoped, false),
			LogicalExpr::Call(call) => {
				if evaluates_against_row(call, self.registry) {
					return true;
				}
				call.args.iter().any(|arg| self.reads_whole_row(arg, row_params, nested_scoped))
			}
			LogicalExpr::Subquery(plan) => {
				if plan_mentions_params(plan, nested_scoped) {
					return true;
				}
				let mut whole = false;
				row_scalars(plan, &mut |scalar| {
					whole |= self.reads_whole_row(scalar, row_params, nested_scoped);
				});
				whole
			}
			LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Constant(_)
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
			| LogicalExpr::Closure(_)
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue => expr
				.children()
				.into_iter()
				.any(|child| self.reads_whole_row(child, row_params, nested_scoped)),
		}
	}

	/// [`Self::reads_whole_row`] for a path. `body` is whether the path is the
	/// body a step applies to the value produced so far — a broadcast, an
	/// optional chain, an implicit recursion — so that its root is
	/// [`LogicalExpr::Current`] and its first step continues the enclosing
	/// path rather than starting one on the row.
	///
	/// A path reads the whole row when it starts from `@`; when the first
	/// step applied to the row is anything but a field read or a lookup, or
	/// the first step applied to a row parameter is anything but a field read
	/// (`$this.f`, `$this["f"]`); when the expression it starts from does; or
	/// when an operand evaluated in the row's context does — an index, a
	/// filter predicate, a method argument, a lookup's clauses — including
	/// those of the steps a broadcast, an optional chain or an implicit
	/// recursion continues with. A written recursion body and a destructure
	/// apply to the values they select, not to the row, and are not entered.
	fn path_reads_whole_row(
		&self,
		expr: &LogicalExpr,
		row_params: &[&str],
		nested_scoped: &[&str],
		body: bool,
	) -> bool {
		let first = first_step(expr);
		let root = expr.path_root();
		match root {
			LogicalExpr::Doc => return true,
			LogicalExpr::Field(_) => {}
			LogicalExpr::Current => {
				if !body && !matches!(first, Some(LogicalExpr::Lookup { .. })) {
					return true;
				}
			}
			LogicalExpr::Param(param) if row_params.contains(&param.as_str()) => {
				let names_field = match first {
					Some(LogicalExpr::Access {
						..
					}) => true,
					Some(LogicalExpr::Index {
						index,
						..
					}) => matches!(index.as_ref(), LogicalExpr::Literal(LogicalLiteral::String(_))),
					Some(_) | None => false,
				};
				if !names_field {
					return true;
				}
			}
			// The expression a `(e).rest` path starts from.
			LogicalExpr::Param(_)
			| LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Constant(_)
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
			| LogicalExpr::Subquery(_) => {
				if self.reads_whole_row(root, row_params, nested_scoped) {
					return true;
				}
			}
			// A step is never a root.
			LogicalExpr::Access {
				..
			}
			| LogicalExpr::Index {
				..
			}
			| LogicalExpr::First(_)
			| LogicalExpr::Last(_)
			| LogicalExpr::Each(_)
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
			| LogicalExpr::Broadcast {
				..
			} => {}
		}
		let mut current = expr;
		loop {
			match current {
				LogicalExpr::Index {
					base,
					index,
				} => {
					if self.reads_whole_row(index, row_params, nested_scoped) {
						return true;
					}
					current = base;
				}
				LogicalExpr::Filter {
					base,
					predicate,
				} => {
					if self.reads_whole_row(predicate, row_params, nested_scoped) {
						return true;
					}
					current = base;
				}
				LogicalExpr::Method {
					base,
					args,
					..
				} => {
					if args.iter().any(|arg| self.reads_whole_row(arg, row_params, nested_scoped)) {
						return true;
					}
					current = base;
				}
				LogicalExpr::Lookup {
					base,
					lookup,
				} => {
					if lookup
						.cond
						.iter()
						.chain(&lookup.limit)
						.chain(&lookup.start)
						.any(|e| self.reads_whole_row(e, row_params, nested_scoped))
					{
						return true;
					}
					current = base;
				}
				LogicalExpr::Broadcast {
					base,
					body,
					..
				} => {
					if self.path_reads_whole_row(body, row_params, nested_scoped, true) {
						return true;
					}
					current = base;
				}
				LogicalExpr::Optional {
					base,
					then,
				} => {
					if self.path_reads_whole_row(then, row_params, nested_scoped, true) {
						return true;
					}
					current = base;
				}
				LogicalExpr::Recurse(recurse) => {
					if let Some(LogicalRecursePath::Trailing(body)) = &recurse.path
						&& self.path_reads_whole_row(body, row_params, nested_scoped, true)
					{
						return true;
					}
					current = &recurse.base;
				}
				LogicalExpr::Access {
					base,
					..
				}
				| LogicalExpr::Flatten {
					base,
					..
				}
				| LogicalExpr::Destructure {
					base,
					..
				} => current = base,
				LogicalExpr::First(base)
				| LogicalExpr::Last(base)
				| LogicalExpr::Each(base)
				| LogicalExpr::RepeatRecurse(base) => current = base,
				LogicalExpr::Field(_)
				| LogicalExpr::Doc
				| LogicalExpr::Current
				| LogicalExpr::Literal(_)
				| LogicalExpr::Array(_)
				| LogicalExpr::Set(_)
				| LogicalExpr::Object(_)
				| LogicalExpr::RecordId(_)
				| LogicalExpr::Param(_)
				| LogicalExpr::Table(_)
				| LogicalExpr::Mock(_)
				| LogicalExpr::Constant(_)
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
				| LogicalExpr::Subquery(_) => return false,
			}
		}
	}
}

/// The rank of each computed field in the dependency order: `0` for a field
/// reading no other computed field, else one more than the highest rank it
/// reads. `None` when the reads are cyclic.
fn ranks(
	computed: &[&FieldFacts],
	deps: &HashMap<Strand, HashSet<Strand>>,
) -> Option<HashMap<Strand, usize>> {
	let mut ranks: HashMap<Strand, usize> = HashMap::new();
	while ranks.len() < computed.len() {
		let mut progressed = false;
		for field in computed {
			let name = field.root();
			if ranks.contains_key(name) {
				continue;
			}
			let reads = &deps[name];
			if reads.iter().all(|dep| ranks.contains_key(dep)) {
				let rank = reads.iter().map(|dep| ranks[dep] + 1).max().unwrap_or(0);
				ranks.insert(name.clone(), rank);
				progressed = true;
			}
		}
		if !progressed {
			return None;
		}
	}
	Some(ranks)
}

/// `IF p THEN value ELSE NONE END`: `guard` with `$value` replaced by `value`
/// as the condition, `value` as the kept value.
fn judge(guard: &LogicalExpr, value: LogicalExpr) -> LogicalExpr {
	let mut predicate = guard.clone();
	substitute_param(&mut predicate, "value", &value);
	LogicalExpr::If(Box::new(LogicalIf {
		branches: vec![(predicate, value)],
		otherwise: Some(LogicalExpr::Literal(LogicalLiteral::None)),
	}))
}

/// The column for a field guarded `WHERE guard` whose value the layer
/// evaluates apart from the guard: `IF guard THEN value ELSE NONE END` with
/// `$value` left in `guard` for the layer to bind to the value it evaluated,
/// so a re-derivation is evaluated once and judged as the caller receives it.
fn judge_column(guard: LogicalExpr, value: LogicalExpr) -> LogicalExpr {
	LogicalExpr::If(Box::new(LogicalIf {
		branches: vec![(guard, value)],
		otherwise: Some(LogicalExpr::Literal(LogicalLiteral::None)),
	}))
}

/// The guard applied to the raw value `raw` of a restricted field: the
/// judgement for a `WHERE` clause, `NONE` for `PERMISSIONS NONE`, `raw` itself
/// for `FULL`. `guards` holds each `WHERE` clause by field name.
fn apply_guard(
	field: &FieldFacts,
	raw: LogicalExpr,
	guards: &HashMap<String, LogicalExpr>,
) -> LogicalExpr {
	match &field.select {
		LogicalPermission::Full => raw,
		LogicalPermission::None => LogicalExpr::Literal(LogicalLiteral::None),
		LogicalPermission::Specific(_) => judge(&guards[&field.name], raw),
	}
}

/// The guard-layer column for the stored field `root`, composing every guard
/// in `under` — the root's own and those on paths beneath it — into one
/// expression over the raw value the layer's input holds under `root`.
/// `None` when no expression reproduces the cuts the runtime makes (see the
/// module documentation).
///
/// A guard on the root itself is `IF p THEN root ELSE NONE END`. A guard on a
/// path beneath it sets the key it names on the value the path reaches:
/// `set_field(v, 'k', IF p THEN v.k ELSE NONE END)`, `$value` being `v.k`;
/// where the position `v` may hold an array, the `set_field` runs per element
/// inside a `Broadcast`, and a `*` segment is a `Broadcast` over the position.
/// Guards on paths sharing a prefix compose through one `set_field` per key,
/// the deeper cuts applied to the value the shallower ones set. Each guard
/// judges the raw value: the tree of `set_field`s is built over the guarded
/// value, but every `$value` and every other read in a guard is of the
/// layer's input.
///
/// Inside a `Broadcast` body the current value is an element rather than the
/// row, so a guard placed there may read the row only through `$value`; a
/// guard that reads the row otherwise, and does not read `$value`, is judged
/// once over the whole column instead — `IF p THEN column ELSE column with
/// the path cut END` — and one that reads both has no place here.
///
/// Cuts commute, since every guard reads the raw value, so the order in which
/// guards under one root compose does not change the result.
fn guard_column(
	root: &Strand,
	under: &[&FieldFacts],
	schema: &Schema,
	guards: &HashMap<String, LogicalExpr>,
	registry: &FunctionRegistry,
) -> Option<LogicalExpr> {
	// One column runs under one authority.
	let mut auths: Vec<&Option<crate::iam::AuthLimit>> = Vec::new();
	for field in under {
		if !auths.contains(&&field.auth) {
			auths.push(&field.auth);
		}
	}
	if auths.len() > 1 {
		return None;
	}
	let raw = LogicalExpr::Field(root.clone());
	let mut acc = match under.iter().find(|field| field.path.as_field().is_some()) {
		Some(field) => apply_guard(field, raw.clone(), guards),
		None => raw.clone(),
	};
	let root_path = FieldPath::field(root.clone());
	let mut tree = GuardTree::default();
	let mut hoisted: Vec<&FieldFacts> = Vec::new();
	for field in under.iter().filter(|field| field.path.as_field().is_none()) {
		let segments = &field.path.segments()[1..];
		// A denial on `arr.*` removes elements; a key is all a `set_field`
		// removes.
		if matches!(segments.last(), Some(FieldSegment::All)) {
			return None;
		}
		// The positions the path maps over: an explicit `*`, or a field read
		// applied to a position that may hold an array. A `Broadcast` inside
		// a mapped body applies plainly, so a path maps at most once; a `*`
		// over a position that may hold an object would need to map over the
		// object's values, which no expression does.
		let mut mappings = 0;
		let mut prefix = root_path.clone();
		for segment in segments {
			let kind = kind_at(schema, &prefix);
			match segment {
				FieldSegment::Field(_) => {
					if may_hold_array(&kind) {
						mappings += 1;
					}
				}
				FieldSegment::All => {
					if may_hold_object(&kind) {
						return None;
					}
					mappings += 1;
				}
			}
			prefix = prefix.child(segment.clone());
		}
		if mappings > 1 {
			return None;
		}
		if mappings == 1
			&& let LogicalPermission::Specific(_) = &field.select
			&& reads_row(&guards[&field.name], registry)
		{
			if mentions_param(&guards[&field.name], "value") {
				return None;
			}
			hoisted.push(field);
			continue;
		}
		tree.insert(segments, field);
	}
	acc = tree.apply(acc, &raw, &root_path, schema, guards);
	for field in hoisted {
		let segments = &field.path.segments()[1..];
		let cut = cut_path(acc.clone(), segments, &root_path, schema);
		acc = LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![(guards[&field.name].clone(), acc)],
			otherwise: Some(cut),
		}));
	}
	Some(acc)
}

/// The guards declared beneath one root field, by the path segments beneath
/// it.
#[derive(Default)]
struct GuardTree<'a> {
	/// The guard declared at exactly this position.
	guard: Option<&'a FieldFacts>,
	children: Vec<(FieldSegment, GuardTree<'a>)>,
}

impl<'a> GuardTree<'a> {
	fn insert(&mut self, segments: &[FieldSegment], field: &'a FieldFacts) {
		let Some((first, rest)) = segments.split_first() else {
			self.guard = Some(field);
			return;
		};
		let child = match self.children.iter().position(|(segment, _)| segment == first) {
			Some(index) => &mut self.children[index].1,
			None => {
				self.children.push((first.clone(), GuardTree::default()));
				&mut self.children.last_mut().expect("just pushed").1
			}
		};
		child.insert(rest, field);
	}

	/// The value at this position with the guards of the subtree applied.
	/// `acc` is the value as guarded so far and `raw` the raw read of the
	/// position from the layer's input, or from the current element inside a
	/// `Broadcast` body; `prefix` is the declared path of the position.
	fn apply(
		&self,
		acc: LogicalExpr,
		raw: &LogicalExpr,
		prefix: &FieldPath,
		schema: &Schema,
		guards: &HashMap<String, LogicalExpr>,
	) -> LogicalExpr {
		let mut acc = match self.guard {
			Some(field) => apply_guard(field, raw.clone(), guards),
			None => acc,
		};
		for (segment, child) in &self.children {
			let kind = kind_at(schema, prefix);
			let child_prefix = prefix.clone().child(segment.clone());
			acc = match segment {
				FieldSegment::Field(key) => {
					let current = LogicalExpr::Current;
					if may_hold_array(&kind) {
						let element_raw = LogicalExpr::Access {
							base: Box::new(current.clone()),
							field: key.clone(),
						};
						LogicalExpr::Broadcast {
							base: Box::new(acc),
							body: Box::new(LogicalExpr::SetField {
								base: Box::new(current.clone()),
								field: key.clone(),
								value: Box::new(child.apply(
									element_raw.clone(),
									&element_raw,
									&child_prefix,
									schema,
									guards,
								)),
							}),
							flatten: false,
						}
					} else {
						let key_raw = LogicalExpr::Access {
							base: Box::new(raw.clone()),
							field: key.clone(),
						};
						LogicalExpr::SetField {
							base: Box::new(acc),
							field: key.clone(),
							value: Box::new(child.apply(
								key_raw.clone(),
								&key_raw,
								&child_prefix,
								schema,
								guards,
							)),
						}
					}
				}
				FieldSegment::All => {
					let current = LogicalExpr::Current;
					LogicalExpr::Broadcast {
						base: Box::new(acc),
						body: Box::new(child.apply(
							current.clone(),
							&current,
							&child_prefix,
							schema,
							guards,
						)),
						flatten: false,
					}
				}
			};
		}
		acc
	}
}

/// `base` with the path `segments` beneath it cut: the key the path ends in
/// removed, per element wherever the path maps.
fn cut_path(
	base: LogicalExpr,
	segments: &[FieldSegment],
	prefix: &FieldPath,
	schema: &Schema,
) -> LogicalExpr {
	let Some((first, rest)) = segments.split_first() else {
		return LogicalExpr::Literal(LogicalLiteral::None);
	};
	let kind = kind_at(schema, prefix);
	let child_prefix = prefix.clone().child(first.clone());
	match first {
		FieldSegment::Field(key) => {
			if may_hold_array(&kind) {
				let element_key = LogicalExpr::Access {
					base: Box::new(LogicalExpr::Current),
					field: key.clone(),
				};
				LogicalExpr::Broadcast {
					base: Box::new(base),
					body: Box::new(LogicalExpr::SetField {
						base: Box::new(LogicalExpr::Current),
						field: key.clone(),
						value: Box::new(cut_path(element_key, rest, &child_prefix, schema)),
					}),
					flatten: false,
				}
			} else {
				let key_value = LogicalExpr::Access {
					base: Box::new(base.clone()),
					field: key.clone(),
				};
				LogicalExpr::SetField {
					base: Box::new(base),
					field: key.clone(),
					value: Box::new(cut_path(key_value, rest, &child_prefix, schema)),
				}
			}
		}
		FieldSegment::All => LogicalExpr::Broadcast {
			base: Box::new(base),
			body: Box::new(cut_path(LogicalExpr::Current, rest, &child_prefix, schema)),
			flatten: false,
		},
	}
}

/// The kind of the value at the declared position `prefix`: the declaration's
/// kind where a value read there is known to be of it, else the element kind
/// of a declared array or set the position is `*` of, else anything.
///
/// Every caller reads the answer through [`may_hold_array`], to decide whether
/// a read at this position distributes over an array. That is treating the
/// value as being of its kind, which [`FieldSchema::checked`] is what permits:
/// a stored field's declaration is enforced on write alone, so a row written
/// before the declaration — or before it changed — holds what it held. An
/// unchecked declaration therefore answers `Any`, leaving the runtime value's
/// own shape to decide. A guard that took a stored `object` at its word over a
/// row still holding an array would cut nothing from that array's elements.
fn kind_at(schema: &Schema, prefix: &FieldPath) -> Kind {
	if let Some(field) = schema.field(prefix) {
		return if field.checked {
			field.kind.clone()
		} else {
			Kind::Any
		};
	}
	if let Some((FieldSegment::All, parent)) = prefix.segments().split_last()
		&& let Kind::Array(inner, _) | Kind::Set(inner, _) =
			kind_at(schema, &FieldPath::new(parent.to_vec()))
	{
		return *inner;
	}
	Kind::Any
}

/// Whether a value of `kind` may be an array, over which a field read
/// distributes. A set is not: the runtime's cut leaves a set as it is, and so
/// does a `set_field` applied to one.
fn may_hold_array(kind: &Kind) -> bool {
	match kind {
		Kind::Any | Kind::Named(_) | Kind::Array(..) => true,
		Kind::Either(kinds) => kinds.iter().any(may_hold_array),
		Kind::Literal(literal) => match literal {
			KindLiteral::Array(_) => true,
			KindLiteral::String(_)
			| KindLiteral::Integer(_)
			| KindLiteral::Float(_)
			| KindLiteral::Decimal(_)
			| KindLiteral::Duration(_)
			| KindLiteral::Object(_)
			| KindLiteral::Bool(_) => false,
		},
		Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Set(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::File(_)
		| Kind::Enum(_) => false,
	}
}

/// Whether a value of `kind` may be an object, whose values a `*` iterates.
fn may_hold_object(kind: &Kind) -> bool {
	match kind {
		Kind::Any | Kind::Named(_) | Kind::Object => true,
		Kind::Either(kinds) => kinds.iter().any(may_hold_object),
		Kind::Literal(literal) => match literal {
			KindLiteral::Object(_) => true,
			KindLiteral::String(_)
			| KindLiteral::Integer(_)
			| KindLiteral::Float(_)
			| KindLiteral::Decimal(_)
			| KindLiteral::Duration(_)
			| KindLiteral::Array(_)
			| KindLiteral::Bool(_) => false,
		},
		Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32
		| Kind::Number
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Set(..)
		| Kind::Array(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::File(_)
		| Kind::Enum(_) => false,
	}
}

/// Whether a guard reads the row other than through `$value`: a field read,
/// `@`, the current value, a row parameter, a call evaluated against the row,
/// or a nested plan mentioning a row parameter.
fn reads_row(guard: &LogicalExpr, registry: &FunctionRegistry) -> bool {
	guard.any(|e| match e {
		LogicalExpr::Field(_) | LogicalExpr::Doc | LogicalExpr::Current => true,
		LogicalExpr::Param(param) => ROW_PARAMS.contains(&param.as_str()),
		LogicalExpr::Subquery(plan) => plan_mentions_params(plan, ROW_PARAMS),
		LogicalExpr::Call(call) => evaluates_against_row(call, registry),
		// A call whose body is not in the guard: `$pred()`, `$o.check()`, a
		// method the registry does not define. What it reads off the row
		// cannot be told from the guard, so it counts as reading it — the
		// same judgement `reads::reads_row_opaquely` makes, and for the
		// reason recorded on `reads::invokes_an_unseen_body`.
		e if crate::exec::logical::reads::invokes_an_unseen_body(e, registry) => true,
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
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
		| LogicalExpr::Broadcast {
			..
		}
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
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue => false,
	})
}

/// Whether `$name` is mentioned anywhere in `expr`, nested plans included.
fn mentions_param(expr: &LogicalExpr, name: &str) -> bool {
	expr.any(|e| matches!(e, LogicalExpr::Param(param) if param.as_str() == name))
		|| nested_plan_mentions(expr, &[name])
}

/// Whether a plan nested in `expr` mentions any parameter in `names`.
fn nested_plan_mentions(expr: &LogicalExpr, names: &[&str]) -> bool {
	expr.any(|e| matches!(e, LogicalExpr::Subquery(plan) if plan_mentions_params(plan, names)))
}

/// Whether any parameter in `names` is mentioned anywhere in `plan`, in its
/// scalars, its sources, or any plan nested in them.
fn plan_mentions_params(plan: &LogicalOp, names: &[&str]) -> bool {
	let mut found = false;
	visit_deep(plan, &mut |node| {
		if found {
			return;
		}
		if let LogicalOp::Source(LogicalSource {
			source: SourceRef::Param(param),
			..
		}) = node && names.contains(&param.as_str())
		{
			found = true;
			return;
		}
		let Ok(()) = node.visit_scalars(&mut |scalar| {
			if scalar.any(|e| matches!(e, LogicalExpr::Param(p) if names.contains(&p.as_str()))) {
				found = true;
			}
			Ok::<_, std::convert::Infallible>(())
		});
	});
	found
}

/// The innermost step of a path — the one applied directly to the root — and
/// the number of steps between `expr` and the root, following `base` only. A
/// root, or an expression that is not a path, is its own answer with a count
/// of zero.
fn innermost_step(expr: &LogicalExpr) -> (&LogicalExpr, usize) {
	let mut current = expr;
	let mut steps = 0;
	while let Some(base) = current.step_base() {
		steps += 1;
		if base.step_base().is_none() {
			return (current, steps);
		}
		current = base;
	}
	(expr, 0)
}

/// The first step a path applies to its root. A broadcast applies nothing
/// itself: its first step is the one its body applies to the current value.
/// `None` for a root alone.
fn first_step(expr: &LogicalExpr) -> Option<&LogicalExpr> {
	let (inner, steps) = innermost_step(expr);
	if steps == 0 {
		return None;
	}
	if let LogicalExpr::Broadcast {
		body,
		..
	} = inner
	{
		return first_step(body).or(Some(inner));
	}
	Some(inner)
}

/// The field a path reads off the row, when it names one — `f`, `f.rest`,
/// `$this.f`, `$this["f"]` — and whether any step follows the read.
fn leading_field<'a>(expr: &'a LogicalExpr, row_params: &[&str]) -> Option<(&'a Strand, bool)> {
	let (inner, steps) = innermost_step(expr);
	if steps == 0 {
		if let LogicalExpr::Field(name) = inner {
			return Some((name, false));
		}
		return None;
	}
	let root = inner.step_base()?;
	if let LogicalExpr::Field(name) = root {
		return Some((name, true));
	}
	let LogicalExpr::Param(param) = root else {
		return None;
	};
	if !row_params.contains(&param.as_str()) {
		return None;
	}
	if let LogicalExpr::Access {
		field,
		..
	} = inner
	{
		return Some((field, steps > 1));
	}
	if let LogicalExpr::Index {
		index,
		..
	} = inner
		&& let LogicalExpr::Literal(LogicalLiteral::String(name)) = index.as_ref()
	{
		return Some((name, steps > 1));
	}
	// `$this.f.rest`: the read is the first step of the body, applied to the
	// row as the current value, and the rest follows it.
	if let LogicalExpr::Broadcast {
		body,
		..
	} = inner
		&& let LogicalExpr::Access {
			base,
			field,
		} = innermost_step(body).0
		&& matches!(base.as_ref(), LogicalExpr::Current)
	{
		return Some((field, true));
	}
	None
}

/// Whether a body reaches the row through its own record id — `id.f`,
/// `$this.id.f`, `id[$k]` — or reads the id somewhere a dereference could
/// follow. The bare `id` (`COMPUTED $this.id`) is the id itself and cannot be
/// followed.
fn dereferences_own_id(body: &LogicalExpr) -> bool {
	let id: HashSet<Strand> = std::iter::once(Strand::new("id")).collect();
	if !reads_any_of(body, &id, ROW_PARAMS) {
		return false;
	}
	!leading_field(body, ROW_PARAMS)
		.is_some_and(|(name, has_rest)| name.as_str() == "id" && !has_rest)
}

/// The fields in `names` that `expr` reads off the row, not entering nested
/// plans, whose rows are their own.
fn reads_of(expr: &LogicalExpr, names: &HashSet<Strand>, row_params: &[&str]) -> HashSet<Strand> {
	let mut out = HashSet::new();
	collect_reads(expr, names, row_params, &mut out);
	out
}

/// [`reads_of`], accumulating into `out`. A nested statement that inherits
/// the row ([`LogicalOp::inherits_row`]) evaluates its own scalars against
/// it, so those are read as well; a relational plan supplies its own rows and
/// is not entered.
fn collect_reads(
	expr: &LogicalExpr,
	names: &HashSet<Strand>,
	row_params: &[&str],
	out: &mut HashSet<Strand>,
) {
	let _ = expr.visit(&mut |e| {
		if let Some((name, _)) = leading_field(e, row_params)
			&& names.contains(name)
		{
			out.insert(name.clone());
		}
		if let LogicalExpr::Subquery(plan) = e {
			row_scalars(plan, &mut |scalar| collect_reads(scalar, names, row_params, out));
		}
		std::ops::ControlFlow::<()>::Continue(())
	});
}

/// Applies `f` to every scalar of `plan` that is evaluated against the row
/// of the statement holding `plan`: nothing for a relational plan, and for a
/// statement that inherits the row its own scalars and those of the
/// statements nested in it that inherit the row in turn.
fn row_scalars(plan: &LogicalOp, f: &mut impl FnMut(&LogicalExpr)) {
	if !plan.inherits_row() {
		return;
	}
	let Ok(()) = plan.visit_scalars(&mut |scalar| {
		f(scalar);
		Ok::<_, std::convert::Infallible>(())
	});
	for child in plan.children() {
		row_scalars(child, f);
	}
}

fn reads_any_of(expr: &LogicalExpr, names: &HashSet<Strand>, row_params: &[&str]) -> bool {
	!reads_of(expr, names, row_params).is_empty()
}

/// Replaces `$name` with `replacement` wherever it appears, the root of a path
/// (`$name.rest`) included, not entering nested plans or closure bodies; a
/// closure body reads the name where it is invoked, from the layer's binding.
fn substitute_param(expr: &mut LogicalExpr, name: &str, replacement: &LogicalExpr) {
	let Ok(()) = visit_binding_mut(expr, &mut |e| {
		if let LogicalExpr::Param(param) = e
			&& param.as_str() == name
		{
			*e = replacement.clone();
		}
		Ok::<_, std::convert::Infallible>(())
	});
}

// ---------------------------------------------------------------------------
// Pipeline surgery
// ---------------------------------------------------------------------------

fn placeholder() -> LogicalOp {
	LogicalOp::Eval(LogicalEval {
		expr: LogicalExpr::Break,
	})
}

/// The single input of a relational stage, or `None` for a leaf or a
/// statement.
fn stage_input(op: &mut LogicalOp) -> Option<&mut Box<LogicalOp>> {
	match op {
		LogicalOp::Filter(LogicalFilter {
			input,
			..
		})
		| LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input,
			..
		})
		| LogicalOp::Split(LogicalSplit {
			input,
			..
		})
		| LogicalOp::Aggregate(LogicalAggregate {
			input,
			..
		})
		| LogicalOp::Distinct(LogicalDistinct {
			input,
		})
		| LogicalOp::Window(LogicalWindow {
			input,
			..
		})
		| LogicalOp::Sort(LogicalSort {
			input,
			..
		})
		| LogicalOp::Limit(LogicalLimit {
			input,
			..
		})
		| LogicalOp::Project(LogicalProject {
			input,
			..
		})
		| LogicalOp::Fetch(LogicalFetch {
			input,
			..
		})
		| LogicalOp::Timeout(LogicalTimeout {
			input,
			..
		})
		| LogicalOp::VersionScope(LogicalVersionScope {
			input,
			..
		})
		| LogicalOp::Only(LogicalOnly {
			input,
		})
		| LogicalOp::Explain(LogicalExplain {
			input,
			..
		}) => Some(input),
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Write(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Eval(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::Info(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::Sleep(_) => None,
	}
}

/// Splits a pipeline into its stages, outermost first, each holding a
/// placeholder input, and its leaf.
fn peel(mut op: LogicalOp) -> (Vec<LogicalOp>, LogicalOp) {
	let mut stages = Vec::new();
	loop {
		match stage_input(&mut op) {
			Some(input) => {
				let inner = std::mem::replace(input.as_mut(), placeholder());
				stages.push(op);
				op = inner;
			}
			None => return (stages, op),
		}
	}
}

/// The inverse of [`peel`].
fn reassemble(stages: Vec<LogicalOp>, leaf: LogicalOp) -> LogicalOp {
	let mut current = leaf;
	for mut stage in stages.into_iter().rev() {
		**stage_input(&mut stage).expect("a peeled stage has an input") = current;
		current = stage;
	}
	current
}

/// Whether `op` is a leaf the binding refused: a table or record whose clause
/// for the statement is `NONE` for the caller, under a permission filter of
/// `false` ([`Resolver::filtered_source`]). It yields no row, so the write has
/// nothing to judge there.
fn is_refused_leaf(op: &LogicalOp) -> bool {
	matches!(
		op,
		LogicalOp::PermissionFilter(LogicalPermissionFilter {
			predicate: LogicalExpr::Literal(LogicalLiteral::Bool(false)),
			guard: GuardKind::Write,
			..
		})
	)
}

/// Remove from a mutation's list of row sources each leaf the binding refused
/// ([`is_refused_leaf`]).
///
/// A refused leaf yields no row, so removing it changes no write. It only
/// stops the leaf, which is no longer a plain table or record, from keeping
/// the rest of the list from being classified as write targets. A list left
/// with one leaf is that leaf; a list whose every leaf was refused keeps its
/// first, which yields nothing, as the list did. A single source is left as it
/// is.
fn drop_refused_leaves(op: &mut LogicalOp) {
	let LogicalOp::UnionAll(LogicalUnionAll {
		inputs,
	}) = op
	else {
		return;
	};
	inputs.iter_mut().for_each(drop_refused_leaves);
	if !inputs.iter().any(is_refused_leaf) {
		return;
	}
	if inputs.iter().all(is_refused_leaf) {
		inputs.truncate(1);
	} else {
		inputs.retain(|input| !is_refused_leaf(input));
	}
	if inputs.len() == 1
		&& let Some(only) = inputs.pop()
	{
		*op = only;
	}
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use surrealdb_types::ToSql;

	use super::super::test_fixture::{Fixture, db_viewer, owner, record_user};
	use super::*;

	/// The lines of the rendered plan that are layers, outermost first.
	fn layers(plan: &str) -> Vec<&str> {
		plan.lines()
			.map(str::trim)
			.filter(|line| line.starts_with("Project[*, ") && line.contains('@'))
			.collect()
	}

	/// The layer nodes of a bound plan, outermost first.
	fn layer_nodes(plan: &LogicalOp) -> Vec<LogicalOp> {
		let mut out = Vec::new();
		let _ = plan.visit(&mut |n| {
			if n.is_layer() {
				out.push(n.clone());
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
		out
	}

	/// The column named `name` of a layer.
	fn column(layer: &LogicalOp, name: &str) -> LogicalField {
		let LogicalOp::Project(LogicalProject {
			fields: LogicalFields::Select(fields),
			..
		}) = layer
		else {
			panic!("not a layer: {layer}");
		};
		fields
			.iter()
			.find(|field| field.output_path().is_some_and(|path| path.is_field(name)))
			.cloned()
			.unwrap_or_else(|| panic!("no column {name} in {layer}"))
	}

	/// Whether the table source of a bound plan is unresolved.
	fn source_is_unresolved(plan: &LogicalOp, table: &str) -> bool {
		plan.any(|n| {
			matches!(
				n,
				LogicalOp::Source(LogicalSource {
					source: SourceRef::Table(t),
					schema: None,
					..
				}) if t.as_str() == table
			)
		})
	}

	#[tokio::test]
	async fn a_closed_schema_carries_the_declared_kinds_and_id() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL;
			 DEFINE FIELD a ON t TYPE int;
			 DEFINE FIELD b ON t TYPE option<string>;
			 DEFINE FIELD c ON t TYPE object FLEXIBLE;",
		)
		.await;
		let bound = fixture.bound_as(&owner(), vec![]).await;
		let bx = fixture.bind_context(&bound);
		let plan = resolve(super::super::lower_test("SELECT * FROM t"), &bx, None)
			.await
			.expect("resolves");
		// Without a principal nothing is resolved.
		assert!(plan.any(|n| matches!(
			n,
			LogicalOp::Source(LogicalSource {
				schema: None,
				..
			})
		)));

		let caller = Caller::new(&bx).expect("a principal");
		let plan = resolve(super::super::lower_test("SELECT * FROM t"), &bx, Some(&caller))
			.await
			.expect("resolves");
		let mut schema = None;
		let _ = plan.visit(&mut |n| {
			if let LogicalOp::Source(LogicalSource {
				schema: Some(s),
				..
			}) = n
			{
				schema = Some(Arc::clone(s));
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
		let schema = schema.expect("the source is resolved");
		assert_eq!(schema.openness, crate::expr::logical::Openness::Closed);
		let kinds: Vec<(String, Kind)> =
			schema.fields.iter().map(|f| (f.path.to_sql(), f.kind.clone())).collect();
		assert_eq!(kinds[0], ("id".to_string(), Kind::Record(vec![TableName::from("t")])));
		assert!(kinds.contains(&("a".to_string(), Kind::Int)), "{kinds:?}");
		assert!(kinds.contains(&("b".to_string(), Kind::option(Kind::String))), "{kinds:?}");
		let c = schema.field(&FieldPath::field("c")).expect("c");
		assert!(c.flexible);
		// The root Owner bypasses every check, so no node is inserted.
		assert!(!plan.to_string().contains("PermissionFilter"), "{plan}");
	}

	#[tokio::test]
	async fn a_schemaless_table_is_open_and_gets_no_nodes() {
		let fixture = Fixture::new("DEFINE TABLE t SCHEMALESS;").await;
		let plan = fixture.bind_as(&owner(), "SELECT * FROM t WHERE a = 1").await;
		assert!(plan.contains("Source[t]"), "{plan}");
		assert!(!plan.contains("PermissionFilter"), "{plan}");
		assert!(layers(&plan).is_empty(), "{plan}");
	}

	#[tokio::test]
	async fn a_table_permission_is_a_filter_for_a_checked_caller_only() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FOR select WHERE published = true;
			 DEFINE FIELD published ON t TYPE bool;",
		)
		.await;
		let plan = fixture.bind_as(&record_user(), "SELECT * FROM t WHERE published").await;
		assert!(plan.contains("PermissionFilter[published = true]"), "{plan}");
		let filter = plan.find("Filter[published]").expect(&plan);
		let permission = plan.find("PermissionFilter[").expect(&plan);
		assert!(filter < permission, "the user's filter stays above the barrier:\n{plan}");

		for session in [owner(), db_viewer()] {
			let plan = fixture.bind_as(&session, "SELECT * FROM t").await;
			assert!(
				!plan.contains("PermissionFilter"),
				"a bypassing caller is not filtered:\n{plan}"
			);
		}
	}

	#[tokio::test]
	async fn permissions_none_is_an_always_false_filter() {
		let fixture = Fixture::new("DEFINE TABLE t SCHEMALESS PERMISSIONS NONE;").await;
		let plan = fixture.bind_as(&record_user(), "SELECT * FROM t").await;
		assert!(plan.contains("PermissionFilter[false]"), "{plan}");
		// So is a table that does not exist: a record user is denied rather
		// than allowed by default.
		let plan = fixture.bind_as(&record_user(), "SELECT * FROM missing").await;
		assert!(plan.contains("PermissionFilter[false]"), "{plan}");
	}

	#[tokio::test]
	async fn a_computed_field_is_a_derivation_layer_and_the_user_reads_it_by_name() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL;
			 DEFINE FIELD a ON t TYPE int;
			 DEFINE FIELD b ON t TYPE int;
			 DEFINE FIELD c ON t TYPE int COMPUTED a + b;",
		)
		.await;
		let plan = fixture.bind_as(&owner(), "SELECT c, a FROM t WHERE c > 1 ORDER BY a").await;
		assert_eq!(layers(&plan), ["Project[*, a + b AS c : int @computed]"], "{plan}");
		// Nothing above the layer is rewritten.
		assert!(plan.contains("Filter[c > 1]"), "{plan}");
		assert!(plan.contains("Project[c, a]"), "{plan}");
		assert!(plan.contains("Sort[ORDER BY a]"), "{plan}");
		// The layer sits directly above the source, below the user's filter.
		let layer = plan.find("Project[*, ").expect(&plan);
		let filter = plan.find("Filter[").expect(&plan);
		let source = plan.find("Source[t]").expect(&plan);
		assert!(filter < layer && layer < source, "{plan}");
	}

	#[tokio::test]
	async fn a_field_permission_is_a_guard_layer_for_a_checked_caller() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD owner ON t TYPE record<user>;
			 DEFINE FIELD secret ON t TYPE string PERMISSIONS FOR select WHERE $auth.id = owner;",
		)
		.await;
		let plan = fixture.bind_as(&record_user(), "SELECT secret FROM t WHERE secret = 'x'").await;
		assert_eq!(
			layers(&plan),
			["Project[*, (IF $auth.id = owner THEN secret ELSE NONE END) AS secret @permission]"],
			"{plan}"
		);
		assert!(plan.contains("Filter[secret = 'x']"), "{plan}");
		assert!(plan.contains("Project[secret]"), "{plan}");
		// The owner is not checked, so the field is read raw.
		let plan = fixture.bind_as(&owner(), "SELECT secret FROM t").await;
		assert!(layers(&plan).is_empty(), "{plan}");
		assert!(plan.contains("Project[secret]"), "{plan}");
	}

	#[tokio::test]
	async fn a_source_whose_computed_fields_are_all_unread_gets_no_layer() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD c ON t COMPUTED 1 + 1;",
		)
		.await;
		let plan = fixture.bind_as(&owner(), "SELECT id FROM t WHERE a IN []").await;
		assert!(layers(&plan).is_empty(), "{plan}");
		assert!(plan.contains("Filter[a INSIDE []]"), "{plan}");
		// Read, the computed field is derived and its read is not discarded.
		let plan = fixture.bind_as(&owner(), "SELECT id FROM t WHERE c IN []").await;
		assert_eq!(layers(&plan), ["Project[*, 1 + 1 AS c @computed]"], "{plan}");
		assert!(plan.contains("Filter[c INSIDE []]"), "{plan}");
		// A `GROUP ALL` count reads no field, so the count path keeps its
		// bare source.
		let plan = fixture.bind_as(&owner(), "SELECT count() FROM t GROUP ALL").await;
		assert!(layers(&plan).is_empty(), "{plan}");
	}

	#[tokio::test]
	async fn a_guard_of_a_field_the_statement_does_not_read_is_not_evaluated() {
		// A guard is a column only where its field can reach the caller: the
		// statement reads `a` alone, so neither `secret`'s guard nor the
		// computed field is evaluated.
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD a ON t TYPE int;
			 DEFINE FIELD secret ON t TYPE string PERMISSIONS FOR select NONE;
			 DEFINE FIELD c ON t TYPE int COMPUTED a * 2;",
		)
		.await;
		let plan = fixture.bind_as(&record_user(), "SELECT a FROM t").await;
		assert!(layers(&plan).is_empty(), "{plan}");
		// Reading the field is what brings its guard in.
		let plan = fixture.bind_as(&record_user(), "SELECT a, secret FROM t").await;
		assert_eq!(layers(&plan), ["Project[*, NONE AS secret @permission]"], "{plan}");
	}

	#[tokio::test]
	async fn a_whole_row_read_derives_every_computed_field() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD a ON t TYPE int;
			 DEFINE FIELD c ON t TYPE int COMPUTED a + 1;
			 DEFINE FIELD secret ON t TYPE string PERMISSIONS FOR select WHERE false;",
		)
		.await;
		// `*` copies the row, so every computed field is on it; the projection
		// is left as written, with no field added and nothing omitted.
		let plan = fixture.bind_as(&record_user(), "SELECT * FROM t").await;
		assert_eq!(
			layers(&plan),
			[
				// `PERMISSIONS FOR select WHERE false` denies the field outright. The
				// column keeps its `@permission` classification and its guard as written;
				// `SpecialiseGuards` specialises a guard that yields a literal, so no
				// conditional is evaluated per row for a field no caller can read.
				"Project[*, (IF false THEN secret ELSE NONE END) AS secret @permission]",
				"Project[*, a + 1 AS c : int @computed]",
			],
			"{plan}"
		);
		assert!(!plan.contains("OMIT"), "{plan}");
		assert!(!plan.contains("Project[*, c"), "{plan}");
		// So do `$this`, a projection function and a `*` in a later stage.
		for sql in [
			"SELECT id FROM t WHERE $this.a = 1 OR $this = NONE",
			"SELECT type::fields(['a']) FROM t",
			"SELECT a, * FROM t",
			"SELECT * FROM t ORDER BY c",
		] {
			let plan = fixture.bind_as(&owner(), sql).await;
			assert_eq!(
				layers(&plan),
				["Project[*, a + 1 AS c : int @computed]"],
				"`{sql}`:\n{plan}"
			);
		}
		// A nested `SELECT *` gets its own stack; the outer one has no source
		// of its own to resolve.
		let plan =
			fixture.bind_as(&record_user(), "SELECT * FROM (SELECT * FROM t) WHERE c > 1").await;
		assert_eq!(layers(&plan).len(), 2, "{plan}");
		assert_eq!(plan.matches("Source[t]").count(), 1, "{plan}");
	}

	#[tokio::test]
	async fn an_unchecked_caller_gets_the_derivation_layers_alone() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD secret ON t TYPE int PERMISSIONS FOR select WHERE $value > 0;
			 DEFINE FIELD c ON t TYPE int COMPUTED secret + 1 PERMISSIONS FOR select WHERE $value < 10;",
		)
		.await;
		for session in [owner(), db_viewer()] {
			let plan = fixture.bind_as(&session, "SELECT c FROM t WHERE secret = 1").await;
			assert_eq!(layers(&plan), ["Project[*, secret + 1 AS c : int @computed]"], "{plan}");
			assert!(!plan.contains("@permission"), "{plan}");
			assert!(plan.contains("Filter[secret = 1]"), "{plan}");
		}
	}

	#[tokio::test]
	async fn a_matches_on_a_layer_column_is_refused_as_the_user_wrote_it() {
		// No index may answer a `MATCHES` on a field the caller may not select,
		// so the statement is refused, naming the user's expression.
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			 DEFINE FIELD content ON t TYPE string PERMISSIONS FOR select NONE;
			 DEFINE FIELD c ON t TYPE string COMPUTED 'x';",
		)
		.await;
		let refused = |sql: &'static str| async {
			let bound = fixture.bound_as(&record_user(), vec![]).await;
			let bx = fixture.bind_context(&bound);
			super::super::bind(super::super::lower_test(sql), &bx).await.map(|_| ())
		};
		let err =
			refused("SELECT id FROM t WHERE content @@ 'hunter2'").await.expect_err("refused");
		assert_eq!(
			err.to_string(),
			"There was no suitable index supporting the expression: content @@ 'hunter2'"
		);
		let err = refused("SELECT id FROM t WHERE a = 1 AND $this.content @@ 'x'")
			.await
			.expect_err("refused");
		assert!(err.to_string().ends_with("$this.content @@ 'x'"), "{err}");
		// A computed field is never indexed either.
		let err = refused("SELECT c @@ 'x' AS m FROM t").await.expect_err("refused");
		assert!(err.to_string().ends_with("c @@ 'x'"), "{err}");
		// A single record gets no full-text executor, and a `MATCHES` over it is
		// `false` rather than an error, so nothing is refused.
		refused("SELECT id FROM t:one WHERE content @@ 'hunter2'").await.expect("binds");
		// A field the caller reads raw is not a column and not refused here.
		let bound = fixture.bound_as(&owner(), vec![]).await;
		let bx = fixture.bind_context(&bound);
		super::super::bind(super::super::lower_test("SELECT id FROM t WHERE content @@ 'x'"), &bx)
			.await
			.expect("binds");
	}

	#[tokio::test]
	async fn a_computed_guard_reads_stored_fields_raw_and_the_user_reads_the_guard() {
		// The guard on `gated` names `secret`, which the caller may not select.
		// It is judged against the record, so it reads `secret` from the guard
		// layer's input — never through the caller's guard, where it would be
		// `NONE` — and every read of `gated` above the layer is of the guarded
		// value. The guard reads stored fields only, so it is judged before the
		// body and `gated` has no derivation column. `secret` itself is not
		// read, so it gets no column.
		let fixture = Fixture::new(
			"DEFINE TABLE item SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD counter ON item TYPE option<number>;
			 DEFINE FIELD secret ON item TYPE option<string> PERMISSIONS FOR select NONE;
			 DEFINE FIELD gated ON item TYPE string COMPUTED 'G'
			 	PERMISSIONS FOR select WHERE secret = NONE;",
		)
		.await;
		let plan = fixture.bind_as(&record_user(), "SELECT gated AS alias FROM item").await;
		assert_eq!(
			layers(&plan),
			["Project[*, (IF secret = NONE THEN 'G' ELSE NONE END) AS gated : string @permission]"],
			"{plan}"
		);
		assert!(plan.contains("Project[gated AS alias]"), "{plan}");
		// So does a `WHERE` on it.
		let plan = fixture.bind_as(&record_user(), "SELECT id FROM item WHERE gated = 'G'").await;
		assert!(plan.contains("Filter[gated = 'G']"), "{plan}");
		assert_eq!(layers(&plan).len(), 1, "{plan}");
		// `*` over a table with a restricted field puts the denied field's
		// column in the same guard layer.
		let plan = fixture.bind_as(&record_user(), "SELECT * FROM item").await;
		assert_eq!(layers(&plan).len(), 1, "{plan}");
	}

	#[tokio::test]
	async fn a_denied_computed_field_is_not_derived() {
		// `hidden` denies every caller outright, so its body never runs for a
		// checked caller: no derivation layer holds it, and the guard layer
		// sets it to `NONE`. `shown` has no guard and is derived as before.
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD a ON t TYPE int;
			 DEFINE FIELD shown ON t TYPE int COMPUTED a + 1;
			 DEFINE FIELD hidden ON t TYPE int COMPUTED a + 2 PERMISSIONS FOR select NONE;",
		)
		.await;
		let plan = fixture.bind_as(&record_user(), "SELECT * FROM t").await;
		assert_eq!(
			layers(&plan),
			[
				"Project[*, NONE AS hidden @permission]",
				"Project[*, a + 1 AS shown : int @computed]",
			],
			"{plan}"
		);
		assert!(!plan.contains("a + 2"), "{plan}");
		// An unchecked caller is shown everything, so both derive, in the order the catalog
		// lists the fields.
		let plan = fixture.bind_as(&owner(), "SELECT * FROM t").await;
		assert_eq!(
			layers(&plan),
			["Project[*, a + 2 AS hidden : int @computed, a + 1 AS shown : int @computed]"],
			"{plan}"
		);
	}

	#[tokio::test]
	async fn a_judge_first_computed_field_is_derived_under_its_guard() {
		// The guard reads a stored field only, so it is judged before the body:
		// the guard layer's column is the guard over the body, and no
		// derivation layer holds the field.
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD owner ON t TYPE record<user>;
			 DEFINE FIELD a ON t TYPE int;
			 DEFINE FIELD c ON t TYPE int COMPUTED a * 2 PERMISSIONS FOR select WHERE $auth.id = owner;",
		)
		.await;
		let plan = fixture.bind_as(&record_user(), "SELECT c FROM t").await;
		assert_eq!(
			layers(&plan),
			["Project[*, (IF $auth.id = owner THEN a * 2 ELSE NONE END) AS c : int @permission]"],
			"{plan}"
		);
		assert!(!plan.contains("@computed"), "{plan}");
		// An unchecked caller derives the body with no guard.
		let plan = fixture.bind_as(&owner(), "SELECT c FROM t").await;
		assert_eq!(layers(&plan), ["Project[*, a * 2 AS c : int @computed]"], "{plan}");
	}

	#[tokio::test]
	async fn a_denied_computed_field_another_body_reads_is_derived() {
		// `visible` reads `hidden`, so `hidden`'s value is needed by a body the
		// caller may see: it keeps its derivation column, and the guard layer
		// masks it afterwards.
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD a ON t TYPE int;
			 DEFINE FIELD hidden ON t TYPE int COMPUTED a + 2 PERMISSIONS FOR select NONE;
			 DEFINE FIELD visible ON t TYPE int COMPUTED hidden * 2;",
		)
		.await;
		let plan = fixture.bind_as(&record_user(), "SELECT hidden, visible FROM t").await;
		assert!(plan.contains("a + 2 AS hidden : int @computed"), "{plan}");
		assert!(plan.contains("NONE AS hidden @permission"), "{plan}");
	}

	#[tokio::test]
	async fn a_body_reading_a_restricted_field_is_re_derived_through_its_guard() {
		// The computed field reads a restricted stored field, so its value for
		// the caller is the body over the record as the caller may read it —
		// the layer evaluates it against its stored guards' output — and its
		// own guard is judged with that value as `$value`. Both columns are
		// the guard layer's, the stored guard first.
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD secret ON t TYPE int PERMISSIONS FOR select WHERE $value > 0;
			 DEFINE FIELD c ON t TYPE int COMPUTED secret + 1 PERMISSIONS FOR select WHERE $value < 10;",
		)
		.await;
		let plan = fixture.bind_as(&record_user(), "SELECT c FROM t").await;
		assert_eq!(
			layers(&plan),
			[
				"Project[*, (IF secret > 0 THEN secret ELSE NONE END) AS secret @permission, (IF $value < 10 THEN secret + 1 ELSE NONE END) AS c : int @permission]"
			],
			"{plan}"
		);
	}

	#[tokio::test]
	async fn a_self_dereference_is_re_derived_in_the_guard_layer() {
		// A body reaching the row through its id reads the stored fields as
		// the caller may, so it is re-derived in the guard layer, where the
		// physical operator answers the dereference from the reduced view; a
		// body reading no restricted field is derived once, beneath it.
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD a ON t TYPE int;
			 DEFINE FIELD secret ON t TYPE int PERMISSIONS FOR select WHERE $value > 0;
			 DEFINE FIELD c ON t TYPE int COMPUTED id.secret;
			 DEFINE FIELD d ON t TYPE int COMPUTED a * 2;",
		)
		.await;
		let plan = fixture.bind_as(&record_user(), "SELECT c, d FROM t").await;
		assert_eq!(
			layers(&plan),
			[
				"Project[*, (IF secret > 0 THEN secret ELSE NONE END) AS secret @permission, id.secret AS c : int @computed]",
				"Project[*, a * 2 AS d : int @computed]",
			],
			"{plan}"
		);
		// The bare id is the id itself and cannot be followed, so the body
		// reads no restricted field: derived once, with no guard evaluated.
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD secret ON t TYPE int PERMISSIONS FOR select WHERE $value > 0;
			 DEFINE FIELD me ON t TYPE record<t> COMPUTED $this.id;",
		)
		.await;
		let plan = fixture.bind_as(&record_user(), "SELECT me FROM t").await;
		assert_eq!(layers(&plan), ["Project[*, $this.id AS me : record<t> @computed]"], "{plan}");
	}

	#[tokio::test]
	async fn a_guard_reading_a_re_derived_field_reads_its_first_derivation() {
		// The guard on `secret` reads `c`, and `c` reads `secret`: the runtime
		// judges the guard against the first derivation of `c` and hands out
		// the second. The first is a derivation layer beneath the guard layer;
		// the second is the body again in the guard layer, which the layer
		// evaluates against the record as its stored guards leave it.
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD secret ON t TYPE int PERMISSIONS FOR select WHERE c > 0;
			 DEFINE FIELD c ON t TYPE int COMPUTED secret + 1;",
		)
		.await;
		let plan = fixture.bind_as(&record_user(), "SELECT c FROM t").await;
		assert_eq!(
			layers(&plan),
			[
				"Project[*, (IF c > 0 THEN secret ELSE NONE END) AS secret @permission, secret + 1 AS c : int @computed]",
				"Project[*, secret + 1 AS c : int @computed]",
			],
			"{plan}"
		);
	}

	#[tokio::test]
	async fn computed_fields_are_derived_one_rank_per_layer() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL;
			 DEFINE FIELD a ON t TYPE int;
			 DEFINE FIELD c ON t TYPE int COMPUTED a + 1;
			 DEFINE FIELD d ON t TYPE int COMPUTED c * 2;
			 DEFINE FIELD e ON t TYPE int COMPUTED a - 1;",
		)
		.await;
		// `d` reads `c`, so it sits one layer higher; `e` reads nothing
		// computed and is not read, so it is not derived.
		let plan = fixture.bind_as(&owner(), "SELECT d FROM t").await;
		assert_eq!(
			layers(&plan),
			["Project[*, c * 2 AS d : int @computed]", "Project[*, a + 1 AS c : int @computed]"],
			"{plan}"
		);
		// Fields of one rank share a layer, in definition order.
		let plan = fixture.bind_as(&owner(), "SELECT c, e FROM t").await;
		assert_eq!(
			layers(&plan),
			["Project[*, a + 1 AS c : int @computed, a - 1 AS e : int @computed]"],
			"{plan}"
		);
	}

	#[tokio::test]
	async fn a_nested_guard_sets_the_key_on_its_root() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD owner ON t TYPE record<user>;
			 DEFINE FIELD a ON t TYPE object;
			 DEFINE FIELD a.b ON t TYPE string PERMISSIONS FOR select WHERE $value != 'x';
			 DEFINE FIELD a.c ON t TYPE string PERMISSIONS FOR select NONE;",
		)
		.await;
		// `a` is a stored field, so its declaration is enforced on write alone
		// and the cut runs per element of whatever the row holds.
		let plan = fixture.bind_as(&record_user(), "SELECT a FROM t").await;
		assert_eq!(
			layers(&plan),
			[
				"Project[*, aset_field(, 'b', (IF .b != 'x' THEN .b ELSE NONE END))set_field(, 'c', NONE) AS a @permission]"
			],
			"{plan}"
		);
		assert!(plan.contains("Project[a]"), "{plan}");
		// A guard on the root composes with the guards beneath it, outermost.
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD owner ON t TYPE record<user>;
			 DEFINE FIELD a ON t TYPE object PERMISSIONS FOR select WHERE $auth.id = owner;
			 DEFINE FIELD a.b ON t TYPE object;
			 DEFINE FIELD a.b.c ON t TYPE string PERMISSIONS FOR select WHERE $value != 'x';",
		)
		.await;
		// A path two positions deep would map twice, once for `a` and once for
		// `a.b`, and a `Broadcast` inside a mapped body applies plainly — the
		// second mapping would be dropped. Neither declaration is checked, so
		// neither position can be ruled out as holding an array, and no
		// expression reproduces the cut: the column is declined and the scan
		// enforces the guard at run time.
		let plan = fixture.bind_as(&record_user(), "SELECT a FROM t").await;
		assert!(layers(&plan).is_empty(), "{plan}");
	}

	#[tokio::test]
	async fn a_guard_beneath_a_stored_field_sets_the_key_per_element() {
		// `arr` is declared to hold an object, but a declaration binds writes
		// alone, so the row may hold an array of them and the cut runs per
		// element.
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD owner ON t TYPE record<user>;
			 DEFINE FIELD arr ON t TYPE object;
			 DEFINE FIELD arr.secret ON t TYPE string PERMISSIONS FOR select WHERE $value != 'x';",
		)
		.await;
		let bound = fixture.bound_as(&record_user(), vec![]).await;
		let bx = fixture.bind_context(&bound);
		let plan = super::super::bind(super::super::lower_test("SELECT arr FROM t"), &bx)
			.await
			.expect("binds");
		let layers = layer_nodes(&plan);
		assert_eq!(layers.len(), 1, "{plan}");
		let LogicalField::Single {
			expr,
			kind,
			frame,
			..
		} = column(&layers[0], "arr")
		else {
			unreachable!()
		};
		assert_eq!(kind, None);
		assert_eq!(frame, EvalFrame::permission(None));
		// A broadcast over the array whose body sets `secret` on the element
		// from the guard judged with the element's `secret` as `$value`.
		let current = LogicalExpr::Current;
		let raw = LogicalExpr::Access {
			base: Box::new(current.clone()),
			field: Strand::new("secret"),
		};
		let guarded = LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![(
				LogicalExpr::Binary {
					left: Box::new(raw.clone()),
					op: BinaryOperator::NotEqual,
					right: Box::new(LogicalExpr::Literal(LogicalLiteral::String(Strand::new("x")))),
				},
				raw,
			)],
			otherwise: Some(LogicalExpr::Literal(LogicalLiteral::None)),
		}));
		assert_eq!(
			expr,
			LogicalExpr::Broadcast {
				base: Box::new(LogicalExpr::Field(Strand::new("arr"))),
				body: Box::new(LogicalExpr::SetField {
					base: Box::new(current),
					field: Strand::new("secret"),
					value: Box::new(guarded),
				}),
				flatten: false,
			},
			"{plan}"
		);
		// A guard on `a.b` where `a` may hold an array sets the key per element
		// too, as the runtime widens `a.b` over an array. Only an import
		// defines a field whose parent is not defined.
		let fixture = Fixture::new(
			"OPTION IMPORT;
			 DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			 DEFINE FIELD a.b ON t PERMISSIONS FOR select NONE;",
		)
		.await;
		let bound = fixture.bound_as(&record_user(), vec![]).await;
		let bx = fixture.bind_context(&bound);
		let plan = super::super::bind(super::super::lower_test("SELECT a FROM t"), &bx)
			.await
			.expect("binds");
		let layers = layer_nodes(&plan);
		let LogicalField::Single {
			expr,
			..
		} = column(&layers[0], "a")
		else {
			unreachable!()
		};
		assert_eq!(
			expr,
			LogicalExpr::Broadcast {
				base: Box::new(LogicalExpr::Field(Strand::new("a"))),
				body: Box::new(LogicalExpr::SetField {
					base: Box::new(LogicalExpr::Current),
					field: Strand::new("b"),
					value: Box::new(LogicalExpr::Literal(LogicalLiteral::None)),
				}),
				flatten: false,
			},
			"{plan}"
		);
	}

	#[tokio::test]
	async fn a_guard_reading_the_row_is_judged_over_the_whole_column() {
		// Inside the broadcast the current value is the element, so a guard
		// reading `owner` is hoisted: the column as stored when it holds, else
		// the column with `secret` cut from every element.
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD owner ON t TYPE record<user>;
			 DEFINE FIELD arr ON t TYPE object;
			 DEFINE FIELD arr.secret ON t TYPE string PERMISSIONS FOR select WHERE $auth.id = owner;",
		)
		.await;
		let bound = fixture.bound_as(&record_user(), vec![]).await;
		let bx = fixture.bind_context(&bound);
		let plan = super::super::bind(super::super::lower_test("SELECT arr FROM t"), &bx)
			.await
			.expect("binds");
		let layers = layer_nodes(&plan);
		assert_eq!(layers.len(), 1, "{plan}");
		let LogicalField::Single {
			expr,
			..
		} = column(&layers[0], "arr")
		else {
			unreachable!()
		};
		let LogicalExpr::If(judged) = expr else {
			panic!("{plan}");
		};
		assert_eq!(judged.branches.len(), 1);
		assert_eq!(judged.branches[0].0.to_sql(), "$auth.id = owner");
		assert_eq!(judged.branches[0].1, LogicalExpr::Field(Strand::new("arr")));
		assert_eq!(
			judged.otherwise,
			Some(LogicalExpr::Broadcast {
				base: Box::new(LogicalExpr::Field(Strand::new("arr"))),
				body: Box::new(LogicalExpr::SetField {
					base: Box::new(LogicalExpr::Current),
					field: Strand::new("secret"),
					value: Box::new(LogicalExpr::Literal(LogicalLiteral::None)),
				}),
				flatten: false,
			}),
			"{plan}"
		);
	}

	#[tokio::test]
	async fn declarations_the_stack_cannot_express_leave_the_source_to_the_scan() {
		let cases = [
			// A denial on `arr.*` removes elements.
			(
				"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
				 DEFINE FIELD arr ON t TYPE array<string>;
				 DEFINE FIELD OVERWRITE arr.* ON t TYPE string PERMISSIONS FOR select WHERE $value != 'x';",
				"SELECT arr FROM t",
			),
			// A `*` over a position that may hold an object, which only an
			// import can define with no field over it.
			(
				"OPTION IMPORT;
				 DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
				 DEFINE FIELD arr.*.secret ON t PERMISSIONS FOR select NONE;",
				"SELECT arr FROM t",
			),
			// A `*` over a declared array: the declaration binds writes alone,
			// so the row may hold an object where an array was declared, and a
			// broadcast over an object sets the key on the object itself
			// instead of on the values beneath it.
			(
				"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
				 DEFINE FIELD arr ON t TYPE array<object>;
				 DEFINE FIELD arr.*.secret ON t TYPE string PERMISSIONS FOR select WHERE $value != 'x';",
				"SELECT arr FROM t",
			),
			// A guard whose `$value` reaches a nested plan.
			(
				"DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
				 DEFINE TABLE u SCHEMALESS;
				 DEFINE FIELD secret ON t PERMISSIONS FOR select WHERE $value IN (SELECT VALUE x FROM u WHERE y = $value);",
				"SELECT secret FROM t",
			),
			// A guard inside a broadcast reading both the row and `$value`.
			(
				"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
				 DEFINE FIELD owner ON t TYPE record<user>;
				 DEFINE FIELD arr ON t TYPE array<object>;
				 DEFINE FIELD arr.*.secret ON t TYPE string PERMISSIONS FOR select WHERE $value != owner;",
				"SELECT arr FROM t",
			),
			// A re-derived body reading the row as a whole.
			(
				"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
				 DEFINE FIELD secret ON t TYPE int PERMISSIONS FOR select WHERE $value > 0;
				 DEFINE FIELD c ON t TYPE object COMPUTED $this;",
				"SELECT c FROM t",
			),
		];
		for (setup, sql) in cases {
			let fixture = Fixture::new(setup).await;
			let bound = fixture.bound_as(&record_user(), vec![]).await;
			let bx = fixture.bind_context(&bound);
			let plan = super::super::bind(super::super::lower_test(sql), &bx).await.expect("binds");
			assert!(source_is_unresolved(&plan, "t"), "`{sql}` should be the scan's:\n{plan}");
			assert!(layer_nodes(&plan).is_empty(), "{plan}");
			// Nothing restricts an unchecked caller, so the same table is
			// resolved for the owner.
			let bound = fixture.bound_as(&owner(), vec![]).await;
			let bx = fixture.bind_context(&bound);
			let plan = super::super::bind(super::super::lower_test(sql), &bx).await.expect("binds");
			assert!(!source_is_unresolved(&plan, "t"), "`{sql}` for the owner:\n{plan}");
		}
	}

	#[tokio::test]
	async fn a_multi_source_from_gets_a_stack_per_input() {
		let fixture = Fixture::new(
			"DEFINE TABLE u SCHEMALESS PERMISSIONS FULL;
			 DEFINE FIELD c ON u COMPUTED 1;
			 DEFINE TABLE v SCHEMALESS PERMISSIONS FULL;
			 DEFINE FIELD secret ON v PERMISSIONS FOR select NONE;",
		)
		.await;
		let plan = fixture.bind_as(&record_user(), "SELECT * FROM u, v, u").await;
		assert_eq!(
			layers(&plan),
			[
				"Project[*, 1 AS c @computed]",
				"Project[*, NONE AS secret @permission]",
				"Project[*, 1 AS c @computed]",
			],
			"{plan}"
		);
		// The stages above the union decide what each input derives and
		// guards: `secret` is `v`'s alone, and `c` is not read.
		let plan = fixture.bind_as(&record_user(), "SELECT secret FROM u, v").await;
		assert_eq!(layers(&plan), ["Project[*, NONE AS secret @permission]"], "{plan}");
	}

	#[tokio::test]
	async fn a_statement_that_inherits_the_row_reads_its_fields() {
		// A block and an `IF` branch evaluate against the row that holds them,
		// so a computed field they read is derived; a user-defined function
		// body runs against the row and its reads cannot be seen from the
		// call, so every computed field is derived beneath one.
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL;
			 DEFINE FIELD a ON t TYPE int;
			 DEFINE FIELD c ON t TYPE int COMPUTED a + 1;
			 DEFINE FUNCTION fn::f() { RETURN c; };",
		)
		.await;
		let derived = ["Project[*, a + 1 AS c : int @computed]"];
		let plan = fixture.bind_as(&owner(), "SELECT { RETURN c } AS x FROM t").await;
		assert_eq!(layers(&plan), derived, "{plan}");
		let plan =
			fixture.bind_as(&owner(), "SELECT (IF a > 0 { c } ELSE { 0 }) AS x FROM t").await;
		assert_eq!(layers(&plan), derived, "{plan}");
		let plan = fixture.bind_as(&owner(), "SELECT id FROM t WHERE { RETURN c } > 1").await;
		assert_eq!(layers(&plan), derived, "{plan}");
		let plan = fixture.bind_as(&owner(), "SELECT fn::f() AS x FROM t").await;
		assert_eq!(layers(&plan), derived, "{plan}");
	}

	#[tokio::test]
	async fn this_dot_field_is_a_field_read() {
		let fixture = Fixture::new(
			"DEFINE TABLE u SCHEMALESS;
			 DEFINE FIELD c ON u COMPUTED 1;",
		)
		.await;
		let plan = fixture.bind_as(&owner(), "SELECT id FROM u WHERE $this.c = 1").await;
		assert_eq!(layers(&plan), ["Project[*, 1 AS c @computed]"], "{plan}");
		assert!(plan.contains("Filter[$this.c = 1]"), "{plan}");
	}

	#[tokio::test]
	async fn a_nested_source_is_resolved_and_a_self_reference_is_not_expanded_twice() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMALESS PERMISSIONS FOR select WHERE (SELECT count() FROM t GROUP ALL) != NONE;
			 DEFINE TABLE u SCHEMALESS PERMISSIONS FOR select WHERE x = 1;",
		)
		.await;
		let plan = fixture
			.bind_as(&record_user(), "SELECT * FROM t WHERE a IN (SELECT VALUE a FROM u)")
			.await;
		// The user's nested plan gets its own permission filter.
		assert!(plan.contains("PermissionFilter[x = 1]"), "{plan}");
		// The permission's own subquery names `t`, which is being expanded:
		// its source stays unresolved and no second filter appears.
		assert_eq!(plan.matches("PermissionFilter[(").count(), 1, "{plan}");
		let mut unresolved_t = 0;
		let plan_op = {
			let bound = fixture.bound_as(&record_user(), vec![]).await;
			let bx = fixture.bind_context(&bound);
			let caller = Caller::new(&bx).expect("principal");
			resolve(super::super::lower_test("SELECT * FROM t"), &bx, Some(&caller))
				.await
				.expect("resolves")
		};
		visit_deep(&plan_op, &mut |n| {
			if let LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(t),
				schema: None,
				..
			}) = n && t.as_str() == "t"
			{
				unresolved_t += 1;
			}
		});
		assert_eq!(unresolved_t, 1, "{plan_op}");
	}

	#[tokio::test]
	async fn a_param_source_known_at_bind_time_is_a_table() {
		let fixture =
			Fixture::new("DEFINE TABLE t SCHEMALESS PERMISSIONS FOR select WHERE p;").await;
		let bound = fixture
			.bound_as(&record_user(), vec![("tb", Value::Table(TableName::from("t")))])
			.await;
		let bx = fixture.bind_context(&bound);
		let plan = super::super::bind(super::super::lower_test("SELECT * FROM $tb"), &bx)
			.await
			.expect("binds")
			.to_string();
		assert!(plan.contains("Source[t]"), "{plan}");
		assert!(plan.contains("PermissionFilter[p]"), "{plan}");
	}

	#[tokio::test]
	async fn a_database_user_outside_its_database_is_refused() {
		let fixture = Fixture::new("DEFINE TABLE t SCHEMALESS;").await;
		let session = crate::dbs::Session::for_level(
			crate::iam::Level::Database("other".to_owned(), "other".to_owned()),
			crate::iam::Role::Owner,
		)
		.with_ns("test")
		.with_db("test");
		let bound = fixture.bound_as(&session, vec![]).await;
		let bx = fixture.bind_context(&bound);
		let err = super::super::bind(super::super::lower_test("SELECT * FROM t"), &bx)
			.await
			.expect_err("refused");
		assert!(err.to_string().contains("namespace"), "{err}");
	}

	#[tokio::test]
	async fn a_computed_body_is_resolved_for_the_principal_it_runs_as() {
		// A body defined by the root Owner carries no narrowing limit, so it
		// runs as the caller: the nested source is checked exactly when the
		// caller is.
		let fixture = Fixture::new(
			"DEFINE TABLE u SCHEMALESS PERMISSIONS FOR select WHERE x = 1;
			 DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD c ON t COMPUTED (SELECT VALUE x FROM u)[0];",
		)
		.await;
		let owner_plan = fixture.bind_as(&owner(), "SELECT c FROM t").await;
		assert!(owner_plan.contains("@computed"), "{owner_plan}");
		assert!(!owner_plan.contains("PermissionFilter"), "{owner_plan}");
		let user_plan = fixture.bind_as(&record_user(), "SELECT c FROM t").await;
		assert!(user_plan.contains("PermissionFilter[x = 1]"), "{user_plan}");
	}

	#[tokio::test]
	async fn layers_carry_the_kinds_the_schema_reads_them_by() {
		use crate::expr::logical::EvalMode;

		// A computed column coerces to the declared kind; a guard column
		// coerces nothing and reads as an option of the field's kind; and
		// every read above the stack is typed by the column.
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD name ON t TYPE string;
			 DEFINE FIELD owner ON t TYPE record<user>;
			 DEFINE FIELD len ON t TYPE int COMPUTED string::len(name);
			 DEFINE FIELD secret ON t TYPE string PERMISSIONS FOR select WHERE $auth.id = owner;
			 DEFINE FIELD score ON t TYPE int COMPUTED len * 2 PERMISSIONS FOR select WHERE $auth.id = owner;",
		)
		.await;
		let bound = fixture.bound_as(&record_user(), vec![]).await;
		let bx = fixture.bind_context(&bound);
		let plan = super::super::bind(
			super::super::lower_test(
				"SELECT len, secret, score FROM t WHERE len > 1 AND secret = 'x'",
			),
			&bx,
		)
		.await
		.expect("binds");
		// `score`'s guard reads a stored field only, so it is judged before the
		// body and `score` has no derivation column: `len` derives beneath the
		// guard layer, which holds `score`'s body.
		let layers = layer_nodes(&plan);
		let [guards, lower] = layers.as_slice() else {
			panic!("{plan}");
		};
		let (mut filter, mut project) = (None, None);
		let _ = plan.visit(&mut |n| {
			if matches!(n, LogicalOp::Filter(_)) {
				filter = Some(n.clone());
			} else if matches!(n, LogicalOp::Project(_)) && !n.is_layer() {
				project = Some(n.clone());
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
		let kind_and_mode = |field: LogicalField| {
			let LogicalField::Single {
				kind,
				frame,
				..
			} = field
			else {
				unreachable!()
			};
			(kind, frame.mode)
		};
		// What each column coerces to, and who evaluates it.
		assert_eq!(kind_and_mode(column(lower, "len")), (Some(Kind::Int), EvalMode::Computed));
		assert_eq!(kind_and_mode(column(guards, "secret")), (None, EvalMode::Permission));
		assert_eq!(kind_and_mode(column(guards, "score")), (Some(Kind::Int), EvalMode::Permission));
		// What the schema reads each column as: the body `string::len(name)`
		// alone would be `Any`, so the declared kind is what types `len`.
		let schema = guards.schema();
		let kind_of = |name: &str| schema.field(&FieldPath::field(name)).map(|f| f.kind.clone());
		assert_eq!(kind_of("len"), Some(Kind::Int));
		assert_eq!(kind_of("secret"), Some(Kind::option(Kind::String)));
		assert_eq!(kind_of("score"), Some(Kind::option(Kind::Int)));
		// The user's predicate reads the columns by name, and types by them.
		let LogicalOp::Filter(LogicalFilter {
			input,
			predicate,
		}) = filter.unwrap_or_else(|| panic!("{plan}"))
		else {
			unreachable!()
		};
		let input_schema = input.schema();
		let mut read_kinds = Vec::new();
		let _ = predicate.visit(&mut |e| {
			if let LogicalExpr::Field(_) = e {
				read_kinds.push(e.kind(&input_schema));
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
		assert_eq!(read_kinds, [Kind::Int, Kind::option(Kind::String)], "{plan}");
		// The projection's output fields are typed by the columns they read.
		let out = project.unwrap_or_else(|| panic!("{plan}")).schema();
		let kinds: Vec<(String, Kind)> =
			out.fields.iter().map(|f| (f.path.to_sql(), f.kind.clone())).collect();
		assert_eq!(
			kinds,
			[
				("len".to_string(), Kind::Int),
				("secret".to_string(), Kind::option(Kind::String)),
				("score".to_string(), Kind::option(Kind::Int)),
			],
			"{plan}"
		);
	}

	#[tokio::test]
	async fn the_gather_loads_every_table_a_fetch_can_reach() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL;
			 DEFINE FIELD owner ON t TYPE record<user>;
			 DEFINE FIELD label ON t COMPUTED (SELECT VALUE name FROM ONLY tag:1);
			 DEFINE TABLE user SCHEMAFULL;
			 DEFINE FIELD company ON user TYPE option<record<company>>;
			 DEFINE TABLE company SCHEMAFULL;
			 DEFINE TABLE org SCHEMAFULL;
			 DEFINE TABLE tag SCHEMAFULL;",
		)
		.await;
		let bound = fixture.bound_as(&owner(), vec![]).await;
		let bx = fixture.bind_context(&bound);
		let (ns, db) = bx.ns_db.expect("a database");
		let txn = bx.txn.clone().expect("a transaction");
		let cases = [
			// A link through a declared field, and the field's own links.
			("SELECT * FROM t FETCH owner", vec!["company", "t", "tag", "user"]),
			("SELECT * FROM t FETCH owner.company", vec!["company", "t", "tag", "user"]),
			// Without a FETCH nothing follows the field kinds: the statement
			// reads `t`, and the table its computed body names, and no more.
			("SELECT * FROM t", vec!["t", "tag"]),
			("SELECT owner FROM t WHERE owner.company != NONE", vec!["t", "tag"]),
			// A link written into the projection is reached only by its
			// being fetched.
			("SELECT org:1 AS o FROM t", vec!["t", "tag"]),
			("SELECT org:1 AS o FROM t FETCH o", vec!["company", "org", "t", "tag", "user"]),
			(
				"SELECT <record<org>> x AS o FROM t FETCH o",
				vec!["company", "org", "t", "tag", "user"],
			),
			(
				"RETURN (SELECT org:1 AS o FROM t) FETCH o",
				vec!["company", "org", "t", "tag", "user"],
			),
			// A FETCH inside a nested plan follows the kinds of every table.
			(
				"SELECT * FROM t WHERE (SELECT * FROM t FETCH owner) != NONE",
				vec!["company", "t", "tag", "user"],
			),
			// A target whose kind names no table adds nothing.
			("SELECT * FROM t FETCH missing", vec!["company", "t", "tag", "user"]),
			("SELECT * FROM t FETCH $paths", vec!["company", "t", "tag", "user"]),
		];
		for (sql, want) in cases {
			let snapshot =
				gather(&super::super::lower_test(sql), &bx, &txn, ns, db).await.expect("gathers");
			let mut got: Vec<String> =
				snapshot.tables.keys().map(|(table, _)| table.to_string()).collect();
			got.sort();
			assert_eq!(got, want, "tables gathered for `{sql}`");
		}
	}

	/// The plan a write's target scalar holds is resolved, not returned as it
	/// was parsed.
	///
	/// Nothing around a `Write` resolves what it holds: `stage_input` does not
	/// name it, so `peel` returns it as the leaf, and a leaf is not among the
	/// stages `resolve_pipeline` hands to `resolve_scalars`. A target computed
	/// by a `SELECT` would therefore keep unresolved schema and permission
	/// state, and reach the write operator in that state.
	///
	/// Checked through the structure rather than the rendered plan, which
	/// prints a write as `Write[CREATE]` and shows neither its targets nor
	/// what they contain. `EXPLAIN CREATE …` is covered too, because that is
	/// the shape where the write is a leaf beneath a stage rather than the
	/// whole plan.
	#[tokio::test]
	async fn a_write_target_has_its_nested_plan_resolved() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FOR select WHERE published = true;
			 DEFINE FIELD published ON t TYPE bool;",
		)
		.await;
		let bound = fixture.bound_as(&record_user(), vec![]).await;
		let bx = fixture.bind_context(&bound);

		for sql in ["CREATE (SELECT VALUE id FROM t)", "EXPLAIN CREATE (SELECT VALUE id FROM t)"] {
			// `bind` rather than `resolve`: it is what `bind_as` runs, and the
			// only difference here is that the plan is kept as a tree instead
			// of rendered.
			let plan = super::super::bind(super::super::lower_test(sql), &bx).await.expect("binds");

			// `visit` walks operator children, and a target list is not one —
			// the write is found by walking, its targets by asking it.
			let mut writes = Vec::new();
			let _ = plan.visit(&mut |n| {
				if matches!(n, LogicalOp::Write(_)) {
					writes.push(n.clone());
				}
				std::ops::ControlFlow::<()>::Continue(())
			});
			assert_eq!(writes.len(), 1, "expected one write in `{sql}`:\n{plan}");

			let mut nested = Vec::new();
			writes[0]
				.visit_scalars(&mut |scalar| {
					if let LogicalExpr::Subquery(inner) = scalar {
						nested.push((**inner).clone());
					}
					Ok::<(), std::convert::Infallible>(())
				})
				.expect("infallible");
			assert_eq!(nested.len(), 1, "expected one target subquery in `{sql}`:\n{plan}");

			assert!(
				!source_is_unresolved(&nested[0], "t"),
				"the target's source should be bound in `{sql}`:\n{}",
				nested[0]
			);
			assert!(
				nested[0].any(|n| matches!(n, LogicalOp::PermissionFilter(_))),
				"the target's plan should carry the caller's select permission in `{sql}`:\n{}",
				nested[0]
			);
		}
	}

	/// The gather lowers the clause a source can be resolved under and no
	/// other: a plan that performs no mutation names none, so a read carries
	/// the select clause alone.
	#[test]
	fn a_plan_names_the_clauses_its_mutations_are_gated_by() {
		let clauses = |sql| plan_write_clauses(&super::super::lower_test(sql));
		assert!(clauses("SELECT * FROM t WHERE a = 1").is_empty());
		assert!(clauses("RETURN (SELECT * FROM t)").is_empty());
		assert_eq!(clauses("CREATE t"), vec![PermissionKind::Create]);
		assert_eq!(
			clauses("SELECT id, (CREATE log:1) AS made FROM t"),
			vec![PermissionKind::Create],
			"a mutation in a scalar position names its clause too"
		);
		assert_eq!(
			clauses("{ CREATE a; CREATE b; }"),
			vec![PermissionKind::Create],
			"one entry per kind, however many mutations perform it"
		);
	}

	/// A `Write` over a named table's rows, built by taking a statement from
	/// its own lowering and giving it a source of known shape.
	///
	/// The reading mutations lower to this shape on their own; assembling it
	/// here keeps the table under test out of the statement's text, so a test
	/// states which table the gather should reach rather than depending on how
	/// a target lowers.
	fn write_over_rows(sql: &str, table: &str) -> LogicalOp {
		let LogicalOp::Write(crate::expr::logical::LogicalWrite {
			statement,
			data,
			duplicate_update,
			cond,
			..
		}) = super::super::lower_test(sql)
		else {
			panic!("`{sql}` does not lower to a write");
		};
		// The clauses are carried across with the statement: the source is
		// what this rebuilds, and a clause the statement wrote belongs to it
		// wherever its rows come from.
		LogicalOp::Write(crate::expr::logical::LogicalWrite {
			statement,
			data,
			duplicate_update,
			cond,
			source: crate::expr::logical::LogicalWriteSource::Rows(Box::new(LogicalOp::Source(
				LogicalSource {
					source: SourceRef::Table(TableName::from(table)),
					schema: None,
					with: None,
					for_update: false,
				},
			))),
			trace: crate::expr::logical::LookupTrace::None,
		})
	}

	/// The predicates of the plan's permission filters, outermost first.
	fn permission_predicates(plan: &LogicalOp) -> Vec<String> {
		let mut out = Vec::new();
		let _ = plan.visit(&mut |n| {
			if let LogicalOp::PermissionFilter(LogicalPermissionFilter {
				predicate,
				..
			}) = n
			{
				out.push(predicate.to_sql());
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
		out
	}

	/// A mutation's row source is gated by the clause its statement kind
	/// names, and a read of the same table by `select`: the two clauses of one
	/// table do not stand in for each other. The mutation's *conditional*
	/// clause is not judged here at all — the write's own gates judge it, once
	/// against each record as found and once as edited — so the source carries
	/// no predicate where the read carries one.
	#[tokio::test]
	async fn a_mutations_row_source_is_gated_by_its_own_clause() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMALESS
			 	PERMISSIONS FOR select WHERE published = true, FOR create WHERE owner = $auth.id;",
		)
		.await;
		let bound = fixture.bound_as(&record_user(), vec![]).await;
		let bx = fixture.bind_context(&bound);
		let caller = Caller::new(&bx).expect("a principal");

		let plan =
			resolve(write_over_rows("CREATE t", "t"), &bx, Some(&caller)).await.expect("resolves");
		assert!(permission_predicates(&plan).is_empty(), "{plan}");

		let plan = resolve(super::super::lower_test("SELECT * FROM t"), &bx, Some(&caller))
			.await
			.expect("resolves");
		assert_eq!(permission_predicates(&plan), vec!["published = true".to_owned()], "{plan}");
	}

	/// A mutation clause of `NONE` drops the target rather than failing the
	/// statement: the source keeps a predicate of `false` and yields nothing,
	/// which is how a target the caller may not write is refused.
	#[tokio::test]
	async fn a_denied_mutation_target_is_dropped_and_not_an_error() {
		let fixture =
			Fixture::new("DEFINE TABLE t SCHEMALESS PERMISSIONS FOR select FULL, FOR create NONE;")
				.await;
		let bound = fixture.bound_as(&record_user(), vec![]).await;
		let bx = fixture.bind_context(&bound);
		let caller = Caller::new(&bx).expect("a principal");

		let plan = resolve(write_over_rows("CREATE t", "t"), &bx, Some(&caller))
			.await
			.expect("a denied target resolves; it does not fail the statement");
		assert_eq!(permission_predicates(&plan), vec!["false".to_owned()], "{plan}");

		// The caller may read the table it may not write to.
		let plan = resolve(super::super::lower_test("SELECT * FROM t"), &bx, Some(&caller))
			.await
			.expect("resolves");
		assert!(permission_predicates(&plan).is_empty(), "{plan}");
	}
	/// A table whose `id` names two components, and an ordinary computed field
	/// that reads one of them.
	const READINGS: &str = "DEFINE TABLE reading SCHEMAFULL PERMISSIONS FULL;
		 DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
		 DEFINE FIELD temp ON reading TYPE int;
		 DEFINE FIELD label ON reading TYPE string COMPUTED string::concat(city, '-', temp);";

	/// The declared kind and the frame of a layer column.
	fn kind_and_frame(field: LogicalField) -> (Option<Kind>, EvalFrame) {
		let LogicalField::Single {
			kind,
			frame,
			..
		} = field
		else {
			unreachable!()
		};
		(kind, frame)
	}

	/// A component is derived as a computed column whose frame says it reads
	/// the record id, and a field that reads it derives one rank above.
	#[tokio::test]
	async fn a_component_is_a_key_column_beneath_the_fields_that_read_it() {
		use crate::expr::logical::EvalMode;

		let fixture = Fixture::new(READINGS).await;
		let bound = fixture.bound_as(&owner(), vec![]).await;
		let bx = fixture.bind_context(&bound);
		let plan = super::super::bind(
			super::super::lower_test("SELECT city, label FROM reading WHERE city = 'Paris'"),
			&bx,
		)
		.await
		.expect("binds");
		let layers = layer_nodes(&plan);
		let [upper, lower] = layers.as_slice() else {
			panic!("{plan}");
		};
		let (kind, frame) = kind_and_frame(column(lower, "city"));
		assert_eq!(kind, Some(Kind::String), "{plan}");
		assert_eq!(frame, EvalFrame::key_part(), "{plan}");
		assert_eq!(frame.mode, EvalMode::Computed);
		assert!(frame.auth.is_none(), "reading the id narrows nothing");
		// An ordinary computed field is not a key column, whatever it reads.
		let (kind, frame) = kind_and_frame(column(upper, "label"));
		assert_eq!(kind, Some(Kind::String), "{plan}");
		assert_eq!(frame.mode, EvalMode::Computed);
		assert!(!frame.key_part, "{plan}");
		// `time` is not read, so it is not derived.
		assert!(!plan.to_string().contains("AS time"), "{plan}");
		// The plan says which columns read the id.
		let text = plan.to_string();
		assert!(text.contains("@computed @key"), "{text}");
		assert_eq!(text.matches("@key").count(), 1, "{text}");

		// What the schema reads each column as: the component by the type the
		// id declares for it.
		let schema = upper.schema();
		let kind_of = |name: &str| schema.field(&FieldPath::field(name)).map(|f| f.kind.clone());
		assert_eq!(kind_of("city"), Some(Kind::String));
		assert_eq!(kind_of("label"), Some(Kind::String));
		let plan = super::super::bind(super::super::lower_test("SELECT time FROM reading"), &bx)
			.await
			.expect("binds");
		let layers = layer_nodes(&plan);
		let [only] = layers.as_slice() else {
			panic!("{plan}");
		};
		let schema = only.schema();
		assert_eq!(
			schema.field(&FieldPath::field("time")).map(|f| f.kind.clone()),
			Some(Kind::Datetime)
		);
	}

	/// A statement that reads no component derives none, and one that reads the
	/// whole row derives all of them with the fields that read them.
	#[tokio::test]
	async fn components_are_derived_where_they_are_read() {
		let fixture = Fixture::new(READINGS).await;
		for sql in [
			"SELECT temp FROM reading",
			"SELECT id, temp FROM reading WHERE temp > 1 ORDER BY temp",
			"SELECT count() FROM reading GROUP ALL",
		] {
			let plan = fixture.bind_as(&owner(), sql).await;
			assert!(layers(&plan).is_empty(), "{sql}\n{plan}");
		}

		let bound = fixture.bound_as(&owner(), vec![]).await;
		let bx = fixture.bind_context(&bound);
		let plan = super::super::bind(super::super::lower_test("SELECT * FROM reading"), &bx)
			.await
			.expect("binds");
		let layers = layer_nodes(&plan);
		let [upper, lower] = layers.as_slice() else {
			panic!("{plan}");
		};
		for name in ["city", "time"] {
			let (_, frame) = kind_and_frame(column(lower, name));
			assert_eq!(frame, EvalFrame::key_part(), "{name}\n{plan}");
		}
		let (_, frame) = kind_and_frame(column(upper, "label"));
		assert!(!frame.key_part, "{plan}");
	}

	/// A component depends on the record id alone, so a restricted stored field
	/// puts no guard beneath it, while an ordinary computed field that reaches
	/// the row through its own id is re-derived from the reduced record.
	#[tokio::test]
	async fn a_component_is_not_rederived_beneath_a_restricted_field() {
		let fixture = Fixture::new(
			"DEFINE TABLE reading SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
			 DEFINE FIELD secret ON reading TYPE string PERMISSIONS FOR select NONE;
			 DEFINE FIELD peek ON reading COMPUTED id.secret;",
		)
		.await;
		let plan = fixture.bind_as(&record_user(), "SELECT city, time FROM reading").await;
		assert!(!plan.contains("@permission"), "{plan}");
		assert!(plan.contains("@computed @key"), "{plan}");
		// A body that follows its own id is judged against what the
		// caller may see of the record, so the guard is in the plan.
		let plan = fixture.bind_as(&record_user(), "SELECT peek FROM reading").await;
		assert!(plan.contains("@permission"), "{plan}");
	}
}

/// The one table a mutation writes, where its subjects name one: the table its rows are read
/// from, or the one table or record id it names.
fn written_table(source: &crate::expr::logical::LogicalWriteSource) -> Option<TableName> {
	use crate::expr::logical::LogicalWriteSource;
	fn leaf(op: &LogicalOp) -> Option<TableName> {
		if let LogicalOp::Source(LogicalSource {
			source,
			..
		}) = op
		{
			return if let SourceRef::Table(table) = source {
				Some(table.clone())
			} else if let SourceRef::RecordId(rid) = source {
				Some(rid.table.clone())
			} else {
				None
			};
		}
		match op.children() {
			[child] => leaf(child),
			_ => None,
		}
	}
	match source {
		LogicalWriteSource::Rows(input) => leaf(input),
		LogicalWriteSource::Targets(targets) => match targets.as_slice() {
			[LogicalExpr::Table(table)] => Some(table.clone()),
			[LogicalExpr::RecordId(rid)] => Some(rid.table.clone()),
			_ => None,
		},
		LogicalWriteSource::Inserts(_) => None,
	}
}
