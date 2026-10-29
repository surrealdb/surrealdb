//! The items from [`Built`] through [`bind_body`] are the expression binder's interface, which
//! `build::expr` defines and this module re-exports so that the statement binder reads one path
//! for it. State that belongs to the statement binder lives in [`BindCx`], which implements
//! [`ExprHost`].
//!
//! [`BindCx`] is the host the expression binder calls back into. It holds the rewriter of the
//! container being built and the stack of regions around the point being bound. Every region the
//! binder or the expression binder enters is on that stack, so [`scope::return_target`] and
//! [`scope::loop_exit`] see the regions an expression opens between a `RETURN`, `BREAK` or
//! `CONTINUE` and the region it leaves.

#![deny(clippy::wildcard_enum_match_arm)]

use std::mem;
use std::sync::Arc;

use smallvec::SmallVec;
use surrealdb_strand::Strand;
use surrealdb_types::ToSql;

use super::ops::{LogOp, LoopKind};
use super::scope::{self, LoopExit};
use super::verify::timestamp_arg;
use crate::catalog::{FunctionDefinition, Permission};
use crate::exec::function::FunctionRegistry;
pub(crate) use crate::exec::ir::build::expr::{
	Built, Control, ExprHost, FieldRead, LoopSite, NameRef, bind_body, bind_expr, bind_fetch_path,
};
use crate::exec::ir::build::logical::{
	bind_closure, bind_loop_site, bind_value_statement, receives_row,
};
use crate::exec::ir::build::match_fold::MatchBinding;
use crate::exec::ir::build::stmt::SchemaBinding;
use crate::exec::ir::cache::deps::ValueDeps;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::location::{LocOrigin, Location};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::verify::{VerifyError, VerifyRule};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ConvertSite, ExprOp, RaiseKind};
use crate::exec::ir::iface::catalog::{CatalogAnswer, EffectCx, SemanticCatalog};
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::frame::{AuthorityFrame, Snapshot};
use crate::exec::ir::iface::ids::{BlockId, FrameId, OpId, RegionId, SnapshotId, ValueId};
use crate::exec::ir::iface::premise::CatalogObjectKey;
use crate::exec::ir::iface::region_kind::{RegionKind, RegionProperties, Visibility};
use crate::exec::ir::iface::types::{FactSource, TypeKnowledge};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::logical::footprint::CatalogKind;
use crate::exec::surface::Surface;
use crate::exec::vm::codec::Codec;
use crate::exec::vm::compile_cache::ObjectVersion;
use crate::exec::vm::payload::Refusal;
use crate::expr::logical::{
	EvalFrame, LogicalClosure, LogicalExpr, LogicalFields, LogicalOp, ROW_PARAMS,
};
use crate::expr::{Function, Kind};
use crate::kvs::cache::ds::FunctionSnapshot;
use crate::val::Value;

/// The error of an invariant the binder relies on and that no request can break: a request that
/// breaks one is an internal error, never a user error.
pub(crate) fn invariant(detail: &str) -> IrError {
	IrError::Verify(VerifyError {
		op: None,
		rule: VerifyRule::PerOp,
		detail: detail.to_owned(),
	})
}

/// One region on the stack: what the binder needs to know of it while it is the innermost.
struct Entry {
	region: RegionId,
	block: BlockId,
	/// The length of the environment when the region was entered; leaving the region drops every
	/// name bound after it.
	env_mark: usize,
	/// The index in the environment below which no name resolves: the length of the environment
	/// when the innermost region that does not inherit its enclosing bindings was entered.
	floor: usize,
	/// The statement timestamp this region introduces, when it is a statement region or isolated
	/// from above.
	timestamp: Option<ValueId>,
}

/// What the binder notes of the logical plans it binds, by the identity of the plans' nodes, so
/// that the legality the logical plan states of its pipelines can be read for the loops the
/// binder created.
///
/// A node is named by its address: the notes are read while the plans the request was bound from
/// are still borrowed, and never outlive them.
#[derive(Debug, Default)]
pub(crate) struct BindNotes {
	/// Each `logical.loop` that reads, row by row, the rows below a node of a logical plan, with
	/// the address of that node.
	pub(crate) loops: Vec<(OpId, usize)>,
	/// Each `logical.stmt` with the address of the logical plan of the statement it holds.
	pub(crate) statements: Vec<(OpId, usize)>,
	/// The statements bound as late-bound, as written, by the index their `logical.statement`
	/// names ([`LogOp::Statement`](super::ops::LogOp::Statement)'s `served`).
	pub(crate) served: Vec<Arc<LogicalOp>>,
}

/// The identity of the logical node `node`, as [`BindNotes`] names it.
pub(crate) fn node_identity(node: &LogicalOp) -> usize {
	std::ptr::from_ref(node) as usize
}

/// A name bound to an SSA value.
struct Binding {
	name: Strand,
	value: ValueId,
	kind: BindingKind,
}

/// What bound a name.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BindingKind {
	/// A variable: a `LET`, a `FOR` variable, a parameter or a closure's capture of one.
	Variable,
	/// A name the binder binds to a row of the frame it binds.
	Row,
	/// `$this` or `$parent` bound to the document of the frame a statement in value position is
	/// entered from ([`BindCx::declare_statement_row_names`]).
	Entered,
}

/// The context a request is bound in.
///
/// Holds the rewriter of the container, the effect context, the stack of enclosing regions
/// (outermost first, the container's top region at the bottom), the lexical environment and the
/// counters the expression binder draws from. Every op it creates carries the frame, the snapshot
/// and the statement position it was last given.
pub(crate) struct BindCx<'a> {
	rw: Rewriter<'a>,
	effects: EffectCx<'a>,
	stack: Vec<Entry>,
	kinds: Vec<RegionKind>,
	env: Vec<Binding>,
	current: Option<ValueId>,
	row: Option<ValueId>,
	/// The value a field read or `@` reads in place of `row`, inside a body over a value (an
	/// optional chain's `then`, a destructure alias, a broadcast); a step's argument still reads
	/// the document root.
	field_base: Option<ValueId>,
	/// The document root a step's argument reads where it is not `row`: set inside a closure body
	/// invoked over a row that is not its document root.
	step_root: Option<ValueId>,
	/// The row `step_root` stands beside: the row of the closure body that sets it. An element
	/// filter over that row binds `$parent` to the document root.
	step_root_row: Option<ValueId>,
	aggregating: bool,
	/// Whether the closure literal bound next is the closure argument a builtin runs in place over
	/// an array's elements ([`Self::mark_closure_in_place`]).
	closure_in_place: bool,
	/// The row of the closure body being bound: the row of the frame that invokes the closure,
	/// which the closure may be invoked without.
	closure_row: Option<ValueId>,
	/// The fields of the projection directly above the `ORDER BY` about to be bound, which the
	/// keys resolve their aliases against.
	order_fields: Option<LogicalFields>,
	/// The fields of that projection, by position, whose value is a key of the `ORDER BY` bound
	/// beneath it, each with the key's position ([`ProjectAttrs::sort_keys`]).
	///
	/// [`ProjectAttrs::sort_keys`]: super::ops::ProjectAttrs::sort_keys
	sort_keys: Vec<(u16, u16)>,
	convert_sites: u32,
	stmt: u32,
	frame: FrameId,
	snapshot: SnapshotId,
	/// The first refusal bound since it was last taken.
	refused: Option<Refusal>,
	/// Whether the statements bound now are planned together with the top-level statement that
	/// holds them, which is every statement up to and including the first one that may change the
	/// catalog; those after it are planned when their list reaches them.
	eager: bool,
	/// Whether a statement that is not planned eagerly and runs as a group is bound as one the
	/// program plans when it reaches it
	/// ([`RequestInput::serves_late`](crate::exec::ir::RequestInput)).
	serves_late: bool,
	/// Whether running the top-level statement being bound may change the catalog: it holds a
	/// schema statement anywhere, or reaches a body its text does not show. A write in such a
	/// statement may meet definitions the bind cannot read.
	catalog_may_change: bool,
	/// Whether the statements written in the top-level statement's expressions may meet a
	/// catalog it changes while it runs: it may change the catalog, and it holds a schema
	/// statement or a write anywhere in it. A statement that may change the catalog only through
	/// a body it calls, and writes nothing, reads in its nested statements the catalog it was
	/// bound against.
	nested_may_change: bool,
	/// Whether the container runs after a change to the catalog its planning cannot know
	/// ([`RequestInput::late_bound`](crate::exec::ir::RequestInput)).
	late_bound: bool,
	/// The statement families and the catalog codec of the entry the container is bound for, or
	/// `None` where the binder is given none: a `MATCH` or a schema statement is then refused.
	surface: Option<&'static Surface>,
	/// Whether the top-level statement being bound writes records, which defines a table the
	/// catalog does not hold.
	statement_writes: bool,
	/// Whether a write of the top-level statement bound so far may run code the catalog stores
	/// ([`Self::note_write`]).
	after_stored_code: bool,
	/// Where the code being bound was written, where it is not the request text: the definition a
	/// body is bound from.
	origin: Option<LocOrigin>,
	/// Whether something bound so far in the top-level statement may have changed the catalog: a
	/// schema statement, or a call of code the plan does not show (a script, a function whose body
	/// is not bound in place, a builtin that runs statements). A call of a function bound after
	/// it meets the definition that change leaves, not the one the bind read.
	catalog_touched: bool,
	/// The first refusal of the top-level statement being bound that is raised before any of the
	/// statement runs.
	hoisted: Option<Refusal>,
	/// Whether a `LET` written as an expression is raised where it stands, when a record reaches
	/// it, rather than as the refusal of the whole top-level statement: the subscripts and filters
	/// of a write's place ([`Self::deferring_let`]).
	defers_let: bool,
	/// Whether the text of each expression bound is recorded on the op that computes it: the
	/// statements under an `EXPLAIN`, which prints their clauses as written.
	writing: bool,
	/// The plan-time values the bind consumed.
	#[allow(dead_code)]
	deps: ValueDeps,
	/// The loops and statements noted for the logical plans they were bound from.
	notes: BindNotes,
	/// The table the next write bound writes to whatever its subjects name: the table of the
	/// stage bundle being bound ([`UnitKind::StageBundle`](crate::exec::ir::unit::UnitKind)), or
	/// of the view table whose order is being bound
	/// ([`UnitKind::ViewRows`](crate::exec::ir::unit::UnitKind)), which writes a view row.
	bundle_table: Option<BundleTable>,
	/// The table the next write bound writes to, which its statement names only through a value.
	named_table: Option<surrealdb_strand::TableName>,
	/// Whether a write of request code that names its one table runs its stages as the bundle the
	/// compile links for that table ([`Self::links_stages`]).
	links_stages: bool,
}

/// The table a stage bundle or a view table's order is bound for.
#[derive(Clone, Debug)]
pub(crate) struct BundleTable {
	pub(crate) table: surrealdb_strand::TableName,
	/// Whether the write writes a view row: the order of the view table's rows.
	pub(crate) view_row: bool,
}

impl<'a> BindCx<'a> {
	/// A context over `ir`, whose top region is the innermost region, under the default frame and
	/// snapshot of the container.
	///
	/// A top block that takes arguments takes the statement timestamp first: it is a unit planned
	/// as a body of its own ([`Ir::with_top_args`]), and no statement region around it supplies
	/// the clock.
	pub(crate) fn new(
		ir: &'a mut Ir,
		facts: &'a mut FactCache,
		effects: EffectCx<'a>,
	) -> Result<Self, IrError> {
		let top = ir.top();
		let kind = ir.region(top).map(|region| region.kind).ok_or(IrError::Stale("top region"))?;
		let block = ir
			.region(top)
			.and_then(|region| region.blocks.first().copied())
			.ok_or(IrError::Stale("top block"))?;
		let timestamp = ir.block(block).and_then(|data| data.args.first().copied());
		Ok(Self {
			rw: Rewriter::new(ir, facts),
			effects,
			stack: vec![Entry {
				region: top,
				block,
				env_mark: 0,
				floor: 0,
				timestamp,
			}],
			kinds: vec![kind],
			env: Vec::new(),
			current: None,
			row: None,
			field_base: None,
			step_root: None,
			step_root_row: None,
			aggregating: false,
			closure_in_place: false,
			closure_row: None,
			order_fields: None,
			sort_keys: Vec::new(),
			convert_sites: 0,
			stmt: 0,
			frame: FrameId::new(0),
			snapshot: SnapshotId::new(0),
			refused: None,
			eager: true,
			serves_late: false,
			catalog_may_change: false,
			nested_may_change: false,
			late_bound: false,
			surface: None,
			statement_writes: false,
			after_stored_code: false,
			origin: None,
			catalog_touched: false,
			hoisted: None,
			defers_let: false,
			writing: false,
			deps: ValueDeps::new(),
			notes: BindNotes::default(),
			bundle_table: None,
			named_table: None,
			links_stages: false,
		})
	}

	/// Names `table` as the table the next write bound writes to, which its statement names only
	/// through a value.
	pub(crate) fn name_next_write(&mut self, table: surrealdb_strand::TableName) {
		self.named_table = Some(table);
	}

	/// Takes the table [`Self::name_next_write`] named, if the write bound has not taken it.
	pub(crate) fn take_named_table(&mut self) -> Option<surrealdb_strand::TableName> {
		self.named_table.take()
	}

	/// Binds the next write as one to `table`, the table of the stage bundle being bound.
	pub(crate) fn bind_bundle_of(&mut self, table: surrealdb_strand::TableName) {
		self.bundle_table = Some(BundleTable {
			table,
			view_row: false,
		});
	}

	/// Binds the next write as the write of a view row of `table`, the view table whose order is
	/// being bound.
	pub(crate) fn bind_view_rows_of(&mut self, table: surrealdb_strand::TableName) {
		self.bundle_table = Some(BundleTable {
			table,
			view_row: true,
		});
	}

	/// The table of the stage bundle or view table's order being bound, taken by the one write the
	/// unit holds, so that a write nested in its clauses names its own.
	pub(crate) fn take_bundle_table(&mut self) -> Option<BundleTable> {
		self.bundle_table.take()
	}

	/// The kinds of the regions around the point being bound, outermost first.
	pub(crate) fn kinds(&self) -> &[RegionKind] {
		&self.kinds
	}

	/// Whether the point being bound stands directly in a statement list: the innermost region is
	/// the body of a block, an arm of an `IF`, the body of a `FOR` or a function body. A
	/// statement of such a list starts at an instant of its own; a point in a clause, a subquery
	/// or a closure body belongs to the statement that holds it.
	pub(crate) fn in_statement_list(&self) -> bool {
		matches!(
			self.kinds.last(),
			Some(
				RegionKind::Block
					| RegionKind::ValueBlock
					| RegionKind::StatementIf
					| RegionKind::ValueIf
					| RegionKind::StatementFor
					| RegionKind::ValueFor
					| RegionKind::FunctionBody
			)
		)
	}

	/// The block ops are created in.
	pub(crate) fn block(&self) -> Result<BlockId, IrError> {
		self.stack.last().map(|entry| entry.block).ok_or_else(|| invariant("no region is entered"))
	}

	/// The arguments of the block ops are created in: for the top region of a unit, the statement
	/// timestamp and then the values its kind documents.
	pub(crate) fn block_args(&self) -> Result<SmallVec<[ValueId; 4]>, IrError> {
		let block = self.block()?;
		self.rw
			.ir()
			.block(block)
			.map(|data| data.args.iter().copied().collect())
			.ok_or(IrError::Stale("block"))
	}

	/// The position in the request of the statement being bound.
	pub(crate) fn set_statement(&mut self, index: usize) -> Result<(), IrError> {
		self.stmt = u32::try_from(index).map_err(|_| IrError::Capacity("statement index"))?;
		Ok(())
	}

	/// The results of `op`.
	pub(crate) fn results(&self, op: OpId) -> Result<SmallVec<[ValueId; 2]>, IrError> {
		self.rw.ir().op(op).map(|data| data.results.clone()).ok_or(IrError::Stale("op"))
	}

	/// Creates `kind` at the end of the current block with the given operands and results.
	pub(crate) fn emit(
		&mut self,
		kind: OpKind,
		operands: &[ValueId],
		results: &[TypeKnowledge],
	) -> Result<OpId, IrError> {
		self.create(kind, operands.iter().copied().collect(), results.iter().cloned().collect())
	}

	/// Records `text` as the surface text of the expression the op `op` is built from. The text
	/// is a label that never enters identity.
	pub(crate) fn write_text_on(&mut self, op: OpId, text: String) -> Result<(), IrError> {
		let loc = self.rw.ir().op(op).ok_or(IrError::Stale("op"))?.loc.clone();
		self.rw.set_location(op, loc.with_text(Arc::new(text)))
	}

	/// Creates `kind` and answers its first result.
	pub(crate) fn emit_value(
		&mut self,
		kind: OpKind,
		operands: &[ValueId],
		result: TypeKnowledge,
	) -> Result<ValueId, IrError> {
		let op = self.emit(kind, operands, &[result])?;
		self.results(op)?.first().copied().ok_or(IrError::Stale("result"))
	}

	/// A `NONE` literal at the end of the current block.
	pub(crate) fn none_literal(&mut self) -> Result<ValueId, IrError> {
		self.emit_value(
			OpKind::Expr(ExprOp::Literal(Box::new(Value::None))),
			&[],
			TypeKnowledge::Fact {
				kind: Kind::None,
				source: FactSource::Literal,
			},
		)
	}

	/// Closes the current block with a `logical.yield` of `values`.
	pub(crate) fn yield_logical(&mut self, values: &[ValueId]) -> Result<(), IrError> {
		self.emit(OpKind::Logical(LogOp::Yield), values, &[]).map(drop)
	}

	/// Closes the current block with an `expr.yield` of `values`.
	pub(crate) fn yield_expr(&mut self, values: &[ValueId]) -> Result<(), IrError> {
		self.emit(OpKind::Expr(ExprOp::Yield), values, &[]).map(drop)
	}

	/// Erases `op` and records that it never runs.
	pub(crate) fn erase_unreachable(&mut self, op: OpId) -> Result<(), IrError> {
		self.rw.erase(op, EliminationReason::Unreachable)
	}

	/// Records `refusal` as the refusal bound, keeping an earlier one.
	pub(crate) fn note_refusal(&mut self, refusal: Refusal) {
		self.refused.get_or_insert(refusal);
	}

	/// Answers the refusal bound since it was last taken, and clears it.
	pub(crate) fn take_refusal(&mut self) -> Option<Refusal> {
		self.refused.take()
	}

	/// Notes that the loop `loop_op` reads, row by row, the rows below `base` in the logical plan.
	pub(crate) fn note_loop(&mut self, loop_op: OpId, base: &LogicalOp) {
		self.notes.loops.push((loop_op, node_identity(base)));
	}

	/// Notes that the `logical.stmt` `stmt` holds the logical plan `plan`.
	pub(crate) fn note_statement(&mut self, stmt: OpId, plan: &LogicalOp) {
		self.notes.statements.push((stmt, node_identity(plan)));
	}

	/// The notes made so far, which are cleared.
	pub(crate) fn take_notes(&mut self) -> BindNotes {
		mem::take(&mut self.notes)
	}

	/// Starts a top-level statement: its statements are planned eagerly and none has hoisted a
	/// refusal yet.
	pub(crate) fn begin_statement(&mut self) {
		self.eager = true;
		self.catalog_may_change = false;
		self.nested_may_change = false;
		self.statement_writes = false;
		self.after_stored_code = false;
		self.catalog_touched = false;
		self.hoisted = None;
	}

	/// States `text` as the expression the op that computes `value` stands for, whatever the op's
	/// origin, when the op is one of the current block's.
	pub(crate) fn state_text_of(
		&mut self,
		value: ValueId,
		text: Arc<String>,
	) -> Result<(), IrError> {
		let block = self.block()?;
		let ir = self.rw.ir();
		let Some(&ValueDef::OpResult {
			op,
			..
		}) = ir.value(value).map(|data| &data.def)
		else {
			return Ok(());
		};
		let Some(data) = ir.op(op).filter(|data| data.parent == block) else {
			return Ok(());
		};
		let loc = Location {
			origin: LocOrigin::Written(text),
			..data.loc.clone()
		};
		self.rw.set_location(op, loc)
	}

	/// Notes that what is bound next follows something that may have changed the catalog.
	pub(crate) fn note_catalog_touch(&mut self) {
		self.catalog_touched = true;
	}

	/// Notes whether running `op`, the top-level statement being bound, may change the catalog
	/// ([`Self::catalog_may_change`]): a schema statement written anywhere in it, or an effect that
	/// changes the catalog or runs a body the statement's text does not show (a user-defined
	/// function, a script, a statement given as text).
	pub(crate) fn note_catalog_change_of(&mut self, op: &LogicalOp) {
		use crate::expr::logical::EffectSet;
		let reached = crate::exec::logical::effects::effects_of_plan_with(
			op,
			&crate::exec::logical::effects::Oracle::new(self.effects.registry),
		)
		.set;
		let holds_schema = crate::exec::planner::fold::changes_the_catalog(op);
		self.catalog_may_change =
			holds_schema || reached.intersects(EffectSet::CHANGES_CATALOG | EffectSet::OPAQUE_BODY);
		self.nested_may_change = self.catalog_may_change
			&& (holds_schema
				|| op
					.visit_ops_deep(&mut |node| {
						if matches!(node, LogicalOp::Write(_)) {
							std::ops::ControlFlow::Break(())
						} else {
							std::ops::ControlFlow::Continue(())
						}
					})
					.is_break());
		self.statement_writes = reached.writes_data();
	}

	/// Whether `op`, a statement of a list, is a point after which the list's statements may meet a
	/// catalog it changed: it holds a schema statement, or, where the container serves the
	/// statements it does not plan eagerly, it reaches code whose effect on the catalog the plan
	/// cannot see (a body its text does not show, or an effect that changes the catalog).
	pub(crate) fn changes_the_catalog_for_later(&self, op: &LogicalOp) -> bool {
		use crate::expr::logical::EffectSet;
		if crate::exec::planner::fold::changes_the_catalog(op) {
			return true;
		}
		self.serves_late
			&& crate::exec::logical::effects::effects_of_plan_with(
				op,
				&crate::exec::logical::effects::Oracle::new(self.effects.registry),
			)
			.set
			.intersects(EffectSet::CHANGES_CATALOG | EffectSet::OPAQUE_BODY)
	}

	/// Notes `write`, bound now, when it may run code the catalog stores, which may change the
	/// catalog for the writes bound after it in the statement: a delete, whose reference actions
	/// write other tables, or a write to a table with events.
	pub(crate) fn note_write(
		&mut self,
		write: &crate::expr::logical::LogicalWrite,
		table: Option<&surrealdb_strand::TableName>,
	) {
		use crate::exec::ir::cache::answer::CatalogAnswer;
		use crate::expr::logical::LogicalWriteStatement;
		self.after_stored_code |= matches!(write.statement, LogicalWriteStatement::Delete(_))
			|| table.is_none_or(|table| match self.effects.catalog.events(table) {
				CatalogAnswer::Found(events) => !events.is_empty(),
				CatalogAnswer::Unknown => true,
				CatalogAnswer::NotLoaded | CatalogAnswer::Absent => false,
			});
	}

	/// Whether a write of the statement bound before this point may run code the catalog stores
	/// ([`Self::note_write`]).
	pub(crate) fn after_stored_code(&self) -> bool {
		self.after_stored_code
	}

	/// Sets whether the container runs after a change to the catalog its planning cannot know.
	pub(crate) fn set_late_bound(&mut self, late_bound: bool) {
		self.late_bound = late_bound;
	}

	/// Whether the definitions of a table the statement being bound writes may differ, when it
	/// writes, from those the bind reads: the container runs after a change its planning cannot
	/// know, or the statement may change the catalog itself ([`Self::note_catalog_change_of`]).
	pub(crate) fn catalog_may_change(&self) -> bool {
		self.late_bound || self.catalog_may_change
	}

	/// Whether a read bound now, nested in a statement, may meet definitions the bind cannot read:
	/// the top-level statement may change the catalog while it runs and holds a schema statement
	/// or a write ([`Self::note_catalog_change_of`]), the read follows a statement of its list
	/// that may have, or the container runs after a change its planning cannot know.
	pub(crate) fn nested_read_may_meet_change(&self) -> bool {
		(self.late_bound || self.nested_may_change || !self.eager)
			&& self.kinds.iter().any(|kind| matches!(kind, RegionKind::Subquery))
	}

	/// Whether a table the catalog the statement is bound against does not define may be defined
	/// when a read of the statement runs: the statement writes records or may change the catalog,
	/// the read follows a statement of its list that may have, or the container runs after a
	/// change its planning cannot know.
	pub(crate) fn tables_may_be_defined(&self) -> bool {
		self.catalog_may_change() || self.statement_writes || !self.eager
	}

	/// Whether the statement bound now is planned with its top-level statement.
	pub(crate) fn eager(&self) -> bool {
		self.eager
	}

	/// Sets whether a write of the container's request code that names its one table may run its
	/// stages as the bundle the compile links for that table.
	pub(crate) fn set_links_stages(&mut self, links: bool) {
		self.links_stages = links;
	}

	/// Whether the write bound now may run its stages as the bundle the compile links for its
	/// table: the container is request code that sets it, the write is not under an `EXPLAIN`,
	/// which prints the stages it runs, and the write is a statement of the request itself, under
	/// its `TIMEOUT` at most. A write in a block, a branch, a loop or an expression spends the
	/// computation-depth budget from where it stands and may run once per iteration, so it binds
	/// its stages as its own.
	pub(crate) fn links_stages(&self) -> bool {
		self.links_stages
			&& !self.writing
			&& self.kinds.iter().all(|kind| {
				matches!(
					kind,
					RegionKind::Request
						| RegionKind::Transaction(_)
						| RegionKind::Statement(_)
						| RegionKind::Deadline
				)
			})
	}

	/// Sets whether a statement that is not planned eagerly and runs as a group is bound as one
	/// the program plans when it reaches it.
	pub(crate) fn set_serves_late(&mut self, serves_late: bool) {
		self.serves_late = serves_late;
	}

	/// Sets the surface of the entry the container is bound for. Without it, a `MATCH`, a
	/// schema statement, `INFO`, `LIVE`, `KILL`, `USE`, `OPTION`, `ACCESS` and `SHOW` are refused.
	pub(crate) fn set_surface(&mut self, surface: Option<&'static Surface>) {
		self.surface = surface;
	}

	/// The binding of a `MATCH` statement, if the entry serves one.
	pub(crate) fn match_binding(&self) -> Option<&'static (dyn MatchBinding + Sync)> {
		self.surface.and_then(|surface| surface.match_binding)
	}

	/// The binding of the statements that are not data statements, if the entry serves them.
	pub(crate) fn schema_binding(&self) -> Option<&'static (dyn SchemaBinding + Sync)> {
		self.surface.and_then(|surface| surface.schema_binding)
	}

	/// The codec that reads the definitions of the schema statements bound for this entry.
	pub(crate) fn codec(&self) -> Result<&'static Codec, IrError> {
		self.surface.map(|surface| surface.codec).ok_or_else(|| {
			invariant("a schema statement bound for no entry, so no catalog codec reads it")
		})
	}

	/// Whether the statement bound now is one the program plans when it reaches it: one that is
	/// not planned eagerly, where the container serves such statements.
	pub(crate) fn binds_served(&self) -> bool {
		self.serves_late && !self.eager
	}

	/// Records `op`, a statement bound as late-bound, as written, and answers the index its
	/// `logical.statement` names.
	pub(crate) fn note_served(&mut self, op: &LogicalOp) -> Result<u32, IrError> {
		let at = u32::try_from(self.notes.served.len())
			.map_err(|_| IrError::Capacity("late-bound statements of a container"))?;
		self.notes.served.push(Arc::new(op.clone()));
		Ok(at)
	}

	/// Sets whether the statement bound now is planned eagerly, and answers the previous setting.
	pub(crate) fn set_eager(&mut self, eager: bool) -> bool {
		mem::replace(&mut self.eager, eager)
	}

	/// Records `refusal` as the refusal of the whole top-level statement, keeping an earlier one.
	pub(crate) fn hoist_refusal(&mut self, refusal: Refusal) {
		self.hoisted.get_or_insert(refusal);
	}

	/// Whether a `LET` written as an expression bound now is raised where it stands, when it is
	/// reached, and never hoisted to the top-level statement: in a write's place
	/// ([`Self::deferring_let`]), and in code the catalog stores (an event's `WHEN` and `THEN`, a
	/// field's clauses, a stored clause of a definition), which raises it as the clause runs.
	pub(crate) fn defers_let(&self) -> bool {
		self.defers_let || self.in_stored_code()
	}

	/// Whether the point being bound stands in code the catalog stores: an event's `WHEN` or
	/// `THEN`, a field's clause, or a stored clause of a definition.
	pub(crate) fn in_stored_code(&self) -> bool {
		self.kinds.iter().any(|kind| {
			matches!(
				kind,
				RegionKind::EventFrame | RegionKind::FieldFrame(_) | RegionKind::StoredClause(_)
			)
		})
	}

	/// Runs `body` with a `LET` written as an expression in it raised where it stands
	/// ([`Self::defers_let`]), and restores the previous setting whether `body` succeeded or not.
	pub(crate) fn deferring_let<R>(
		&mut self,
		body: impl FnOnce(&mut Self) -> Result<R, IrError>,
	) -> Result<R, IrError> {
		let outer = mem::replace(&mut self.defers_let, true);
		let outcome = body(self);
		self.defers_let = outer;
		outcome
	}

	/// Answers the refusal hoisted to the top-level statement, and clears it.
	pub(crate) fn take_hoisted(&mut self) -> Option<Refusal> {
		self.hoisted.take()
	}

	/// Restores the refusal an enclosing bind held, which was taken to scope a nested one.
	pub(crate) fn restore_refusal(&mut self, refusal: Option<Refusal>) {
		self.refused = refusal;
	}

	/// Runs `body` with the text of every expression it binds recorded on the op that computes
	/// it, and restores the previous setting whether `body` succeeded or not.
	pub(crate) fn with_writing<R>(
		&mut self,
		body: impl FnOnce(&mut Self) -> Result<R, IrError>,
	) -> Result<R, IrError> {
		let outer = mem::replace(&mut self.writing, true);
		let outcome = body(self);
		self.writing = outer;
		outcome
	}

	/// Binds `name` to `value` in the innermost region; the binding ends with that region. The
	/// value is named too, so a read of the name has a text to print: a plan's, and the refusal of
	/// a KNN query vector read from it.
	pub(crate) fn declare(&mut self, name: Strand, value: ValueId) {
		let _ = self.rw.set_hint_name(value, name.clone());
		self.env.push(Binding {
			name,
			value,
			kind: BindingKind::Row,
		});
	}

	/// [`Self::declare`] of a variable: a `LET`, a `FOR` variable, a parameter, or a closure's
	/// capture of one ([`Self::lookup_variable`]).
	pub(crate) fn declare_variable(&mut self, name: Strand, value: ValueId) {
		let _ = self.rw.set_hint_name(value, name.clone());
		self.env.push(Binding {
			name,
			value,
			kind: BindingKind::Variable,
		});
	}

	/// The value of the innermost variable named `name` ([`Self::declare_variable`]), passing
	/// over the bindings of that name to a row nested inside it; `None` where no variable holds it.
	pub(crate) fn lookup_variable(&self, name: &str) -> Option<ValueId> {
		self.lookup_kinds(name, &[BindingKind::Variable])
	}

	/// The value of the innermost variable named `name`, or of the innermost binding of `name` to
	/// the document a statement in value position is entered from, whichever is nearer, passing
	/// over the bindings of that name to a row nested inside it; `None` where neither holds it.
	pub(crate) fn lookup_variable_or_entered(&self, name: &str) -> Option<ValueId> {
		self.lookup_kinds(name, &[BindingKind::Variable, BindingKind::Entered])
	}

	fn lookup_kinds(&self, name: &str, kinds: &[BindingKind]) -> Option<ValueId> {
		let floor = self.stack.last().map_or(0, |entry| entry.floor);
		self.env[floor..]
			.iter()
			.rev()
			.find(|binding| kinds.contains(&binding.kind) && binding.name.as_str() == name)
			.map(|binding| binding.value)
	}

	/// Binds `name` to `value` in the innermost region as the document a statement is entered
	/// with, which [`Self::lookup_variable_or_entered`] finds and a row name nested inside it
	/// shadows. The binding ends with that region.
	pub(crate) fn declare_entered(&mut self, name: Strand, value: ValueId) {
		self.declare_unnamed_as(name, value, BindingKind::Entered);
	}

	/// Binds `name` to `value` in the innermost region without naming the value: a name the binder
	/// declares for a read of its own, which no text prints. The binding ends with that region.
	pub(crate) fn declare_unnamed(&mut self, name: Strand, value: ValueId) {
		self.declare_unnamed_as(name, value, BindingKind::Row);
	}

	fn declare_unnamed_as(&mut self, name: Strand, value: ValueId, kind: BindingKind) {
		self.env.push(Binding {
			name,
			value,
			kind,
		});
	}

	/// The argument at `index` of the top block of the container, which a unit binds the names it
	/// reads from.
	pub(crate) fn top_arg(&self, index: usize) -> Result<ValueId, IrError> {
		let entry = self.stack.first().ok_or(IrError::Stale("top region"))?;
		let block = self.rw.ir().block(entry.block).ok_or(IrError::Stale("top block"))?;
		block.args.get(index).copied().ok_or(IrError::Stale("top argument"))
	}

	/// The definitions readable at the catalog version the request is bound to.
	pub(crate) fn catalog(&self) -> &'a dyn SemanticCatalog {
		self.effects.catalog
	}

	/// The id of the frame that evaluates as `eval` under the principal of the current frame. The
	/// user's own frame is the current one.
	pub(crate) fn frame_for(&mut self, eval: &EvalFrame) -> Result<FrameId, IrError> {
		if eval.is_caller() {
			return Ok(self.frame);
		}
		let current = self.rw.ir().frame(self.frame).ok_or(IrError::Stale("frame"))?;
		if current.eval == *eval {
			return Ok(self.frame);
		}
		let frame = AuthorityFrame {
			eval: eval.clone(),
			unit: current.unit.clone(),
		};
		self.rw.intern_frame(frame)
	}

	/// Runs `body` with every op it creates under `frame`, and restores the frame whether `body`
	/// succeeded or not.
	pub(crate) fn with_frame<R>(
		&mut self,
		frame: FrameId,
		body: impl FnOnce(&mut Self) -> Result<R, IrError>,
	) -> Result<R, IrError> {
		let outer = mem::replace(&mut self.frame, frame);
		let outcome = body(self);
		self.frame = outer;
		outcome
	}

	/// Runs `body` with every op it creates reading the data as of `version`, and restores the
	/// snapshot whether `body` succeeded or not.
	pub(crate) fn with_version<R>(
		&mut self,
		version: ValueId,
		body: impl FnOnce(&mut Self) -> Result<R, IrError>,
	) -> Result<R, IrError> {
		let snapshot = self.rw.intern_snapshot(Snapshot::Version(version))?;
		let outer = mem::replace(&mut self.snapshot, snapshot);
		let outcome = body(self);
		self.snapshot = outer;
		outcome
	}

	/// Runs `body` with every op it creates reading the current state, under no `VERSION` of the
	/// code around it, and restores the snapshot whether `body` succeeded or not: the body of a
	/// closure, which reads at the stamp of the frame that invokes it.
	pub(crate) fn with_current_state<R>(
		&mut self,
		body: impl FnOnce(&mut Self) -> Result<R, IrError>,
	) -> Result<R, IrError> {
		let snapshot = self.rw.intern_snapshot(Snapshot::Current)?;
		let outer = mem::replace(&mut self.snapshot, snapshot);
		let outcome = body(self);
		self.snapshot = outer;
		outcome
	}

	/// Runs `body` with `row` as the row a field read or `@` reads, the document root a step's
	/// argument reads and the current value a path that starts with a lookup reads, with no field
	/// base around it, and restores all three whether `body` succeeded or not.
	pub(crate) fn with_row<R>(
		&mut self,
		row: ValueId,
		body: impl FnOnce(&mut Self) -> Result<R, IrError>,
	) -> Result<R, IrError> {
		let outer = self.row.replace(row);
		let base = self.field_base.take();
		let current = self.current.replace(row);
		let outcome = body(self);
		self.row = outer;
		self.field_base = base;
		self.current = current;
		outcome
	}

	/// The row a field read or `@` reads where a row frame is entered: the field base where one is
	/// set, otherwise the frame's row.
	pub(crate) fn frame_row(&self) -> Option<ValueId> {
		self.field_base.or(self.row)
	}

	/// Leaves `fields` as the projection the next `ORDER BY` bound resolves its aliases against.
	pub(crate) fn set_order_fields(&mut self, fields: Option<LogicalFields>) {
		self.order_fields = fields;
	}

	/// The projection fields left for the `ORDER BY` being bound, which no other `ORDER BY` sees.
	pub(crate) fn take_order_fields(&mut self) -> Option<LogicalFields> {
		self.order_fields.take()
	}

	/// Leaves `keys` as the fields of the projection about to be bound that read the keys of the
	/// sort beneath it.
	pub(crate) fn set_sort_keys(&mut self, keys: Vec<(u16, u16)>) {
		self.sort_keys = keys;
	}

	/// The fields left for the projection being bound that read the keys of the sort beneath it,
	/// which no other projection sees.
	pub(crate) fn take_sort_keys(&mut self) -> Vec<(u16, u16)> {
		mem::take(&mut self.sort_keys)
	}

	/// Runs `body` with the aggregate functions called in it belonging to the enclosing statement,
	/// and restores the previous answer whether `body` succeeded or not.
	pub(crate) fn with_aggregating<R>(
		&mut self,
		body: impl FnOnce(&mut Self) -> Result<R, IrError>,
	) -> Result<R, IrError> {
		let outer = mem::replace(&mut self.aggregating, true);
		let outcome = body(self);
		self.aggregating = outer;
		outcome
	}

	/// Marks the closure literal bound next as the closure argument a builtin runs in place over an
	/// array's elements: its body reads `$this` as the call reads it, and captures no `$this` a
	/// statement around the call is entered with. The literal takes the mark
	/// ([`Self::take_closure_in_place`]).
	pub(crate) fn mark_closure_in_place(&mut self) {
		self.closure_in_place = true;
	}

	/// Makes `row` the row of the closure body being bound, and answers the previous one.
	pub(crate) fn replace_closure_row(&mut self, row: Option<ValueId>) -> Option<ValueId> {
		mem::replace(&mut self.closure_row, row)
	}

	/// Whether the closure literal being bound was marked to run in place
	/// ([`Self::mark_closure_in_place`]), clearing the mark.
	pub(crate) fn take_closure_in_place(&mut self) -> bool {
		mem::take(&mut self.closure_in_place)
	}

	/// Runs `body` with no aggregate function called in it belonging to an enclosing statement,
	/// and restores the previous answer whether `body` succeeded or not. A statement in value
	/// position is planned on its own, so a grouping around it does not take its calls.
	pub(crate) fn without_aggregating<R>(
		&mut self,
		body: impl FnOnce(&mut Self) -> Result<R, IrError>,
	) -> Result<R, IrError> {
		let outer = mem::replace(&mut self.aggregating, false);
		let outcome = body(self);
		self.aggregating = outer;
		outcome
	}

	/// Runs `body` as code that sees nothing of the row, the current value or the aggregate
	/// context around it, except `row`, which it reads as its row and as its current value, and
	/// `root`, which a step's argument reads in place of `row`; restores all of them whether `body`
	/// succeeded or not. A region isolated from above runs its body this way, since the values
	/// those name are defined outside it.
	pub(crate) fn with_isolation<R>(
		&mut self,
		row: Option<ValueId>,
		root: Option<ValueId>,
		body: impl FnOnce(&mut Self) -> Result<R, IrError>,
	) -> Result<R, IrError> {
		let outer_row = mem::replace(&mut self.row, row);
		let outer_root = mem::replace(&mut self.step_root, root);
		let outer_root_row = mem::replace(&mut self.step_root_row, root.and(row));
		let outer_base = self.field_base.take();
		let outer_current = mem::replace(&mut self.current, row);
		let outer_aggregating = mem::replace(&mut self.aggregating, false);
		let outcome = body(self);
		self.row = outer_row;
		self.step_root = outer_root;
		self.step_root_row = outer_root_row;
		self.field_base = outer_base;
		self.current = outer_current;
		self.aggregating = outer_aggregating;
		outcome
	}

	/// Enters a `FunctionBody` region owned by `owner` and runs `body` in it, leaving the region
	/// whether `body` succeeded or not.
	///
	/// The region's block arguments are the statement timestamp, one value per declared parameter
	/// and one per row name the call passes, in that order; the parameters are bound under their
	/// names and the row names under theirs. `types` states what is known of each parameter, in
	/// order; a parameter it does not reach is unknown. A name the caller binds, whether by
	/// `LET`, `FOR` or an argument of its own, does not resolve in the body: it reads as a free
	/// name. The body reads no row of its caller.
	pub(crate) fn within_function_body<R>(
		&mut self,
		owner: OpId,
		params: &[Strand],
		types: &[TypeKnowledge],
		row_names: &[Strand],
		body: impl FnOnce(&mut Self) -> Result<R, IrError>,
	) -> Result<R, IrError> {
		let mut args = vec![timestamp_arg()];
		args.extend(
			params
				.iter()
				.enumerate()
				.map(|(at, _)| types.get(at).cloned().unwrap_or(TypeKnowledge::Unknown)),
		);
		args.extend(row_names.iter().map(|_| TypeKnowledge::Unknown));
		self.within(owner, RegionKind::FunctionBody, &args, |cx, entry| {
			let values = entry.get(1..).unwrap_or(&[]);
			let (declared, rows) = values.split_at(params.len().min(values.len()));
			for (name, value) in params.iter().zip(declared) {
				cx.declare_variable(name.clone(), *value);
			}
			for (name, value) in row_names.iter().zip(rows) {
				cx.declare(name.clone(), *value);
			}
			cx.with_isolation(None, None, body)
		})
	}

	/// Binds each of `names` to `element` in the innermost region, and `parent` to the row the
	/// enclosing frame reads, when it has one. The bindings end with the region.
	pub(crate) fn declare_row_names(&mut self, element: ValueId, names: &[&str]) {
		for name in names {
			self.declare(Strand::new(*name), element);
		}
		self.declare_parent();
	}

	/// Whether a `$parent` read here, or by a statement entered here, is bound: by a name, or to
	/// the row the enclosing frame reads ([`Self::declare_parent`]).
	pub(crate) fn binds_parent(&self) -> bool {
		self.row.or(self.field_base).is_some() || matches!(self.lookup("parent"), NameRef::Value(_))
	}

	/// Binds `parent` to the row the enclosing frame reads, in the innermost region: the frame's
	/// row, or where it has none the element of the element body it stands in. In a closure body
	/// invoked over a row that is not its document root, the frame's row is that root. Where
	/// neither is entered `parent` stays unbound and reads as a free name.
	pub(crate) fn declare_parent(&mut self) {
		let root = self.step_root.filter(|_| self.row.is_some() && self.row == self.step_root_row);
		if let Some(parent) = root.or(self.row).or(self.field_base) {
			self.declare(Strand::new("parent"), parent);
		}
	}

	/// Binds the row names a statement in value position takes from the frame it is entered from
	/// to that frame's document, the row [`Self::declare_parent`] binds: `this` and `parent`, and
	/// `self` as well where `inherits_row` holds. The bindings shadow every outer binding of the
	/// names, a closure's parameters included. A frame with no row binds none, and neither does an
	/// element body, whose element is not the document.
	///
	/// `this` and `parent` are bound as the statement's entry names, which a closure literal the
	/// statement holds may capture ([`Self::lookup_variable_or_entered`]), where the document is a
	/// row the bind knows the frame has; the row or document root of a closure body, which an
	/// invocation may not pass, binds them as row names. `self` is bound as a row name.
	pub(crate) fn declare_statement_row_names(&mut self, inherits_row: bool) {
		if self.field_base.is_some() {
			return;
		}
		let root = self.step_root.filter(|_| self.row.is_some() && self.row == self.step_root_row);
		let Some(document) = root.or(self.row) else {
			return;
		};
		let invoked = self.closure_row.is_some()
			&& (self.closure_row == Some(document) || self.step_root == Some(document));
		let kind = if invoked {
			BindingKind::Row
		} else {
			BindingKind::Entered
		};
		for name in ["this", "parent"] {
			self.declare_unnamed_as(Strand::new(name), document, kind);
		}
		if inherits_row {
			self.declare_unnamed(Strand::new("self"), document);
		}
	}

	/// The ops of the current block, in order.
	pub(crate) fn block_ops(&self) -> Result<&[OpId], IrError> {
		let block = self.block()?;
		self.rw.ir().block(block).map(|data| data.ops.as_slice()).ok_or(IrError::Stale("block"))
	}

	/// Whether the current block ends in a terminator.
	pub(crate) fn ends_in_terminator(&self) -> Result<bool, IrError> {
		let ir = self.rw.ir();
		Ok(self
			.block_ops()?
			.last()
			.and_then(|op| ir.op(*op))
			.is_some_and(|data| data.kind.is_terminator()))
	}

	/// Enters a region of `kind` owned by `owner`, runs `body` with the region's block arguments
	/// and leaves the region whether `body` succeeded or not.
	pub(crate) fn within<R>(
		&mut self,
		owner: OpId,
		kind: RegionKind,
		args: &[TypeKnowledge],
		body: impl FnOnce(&mut Self, &[ValueId]) -> Result<R, IrError>,
	) -> Result<R, IrError> {
		let (_, block) = self.push_region(owner, kind, args)?;
		let entry: SmallVec<[ValueId; 4]> = match self.rw.ir().block(block) {
			Some(data) => data.args.iter().copied().collect(),
			None => SmallVec::new(),
		};
		let outcome = body(self, &entry);
		let left = self.pop_region();
		let value = outcome?;
		left?;
		Ok(value)
	}

	fn push_region(
		&mut self,
		owner: OpId,
		kind: RegionKind,
		args: &[TypeKnowledge],
	) -> Result<(RegionId, BlockId), IrError> {
		let (region, block) = self.rw.create_region(owner, kind, args)?;
		let first = self.rw.ir().block(block).and_then(|data| data.args.first().copied());
		let introduces_timestamp =
			matches!(kind, RegionKind::Statement(_)) || kind.isolated_from_above();
		let floor = match kind.binding_visibility() {
			Visibility::Inherits => self.stack.last().map_or(0, |entry| entry.floor),
			Visibility::ArgumentsOnly | Visibility::Isolated => self.env.len(),
		};
		self.stack.push(Entry {
			region,
			block,
			env_mark: self.env.len(),
			floor,
			timestamp: if introduces_timestamp {
				first
			} else {
				None
			},
		});
		self.kinds.push(kind);
		Ok((region, block))
	}

	fn pop_region(&mut self) -> Result<(), IrError> {
		if self.stack.len() <= 1 {
			return Err(invariant("a region was left that was never entered"));
		}
		if let Some(entry) = self.stack.pop() {
			self.env.truncate(entry.env_mark);
		}
		self.kinds.pop();
		Ok(())
	}

	/// The name a row-bound parameter resolves at this point, in the order of [`ROW_PARAMS`].
	fn row_names(&self) -> impl Iterator<Item = ValueId> + '_ {
		ROW_PARAMS.iter().filter_map(|name| match self.lookup(name) {
			NameRef::Value(value) => Some(value),
			NameRef::Free => None,
		})
	}

	/// The `expr.raise` that ends the current block with `kind`.
	pub(crate) fn raise(&mut self, kind: RaiseKind) -> Result<(), IrError> {
		self.emit(OpKind::Expr(ExprOp::Raise(Box::new(kind))), &[], &[]).map(drop)
	}

	fn location(&self) -> Location {
		Location {
			stmt: self.stmt,
			span: None,
			origin: self.origin.clone().unwrap_or(LocOrigin::Query),
		}
	}
}

impl ExprHost for BindCx<'_> {
	fn ir(&self) -> &Ir {
		self.rw.ir()
	}

	fn registry(&self) -> &FunctionRegistry {
		self.effects.registry
	}

	fn create(
		&mut self,
		kind: OpKind,
		operands: SmallVec<[ValueId; 4]>,
		results: SmallVec<[TypeKnowledge; 2]>,
	) -> Result<OpId, IrError> {
		let block = self.block()?;
		let spec = OpSpec {
			kind,
			operands,
			results,
			frame: self.frame,
			snapshot: self.snapshot,
			loc: self.location(),
		};
		self.rw.create(InsertPoint::End(block), spec)
	}

	fn enter_region(
		&mut self,
		owner: OpId,
		kind: RegionKind,
		args: &[TypeKnowledge],
	) -> Result<(RegionId, BlockId), IrError> {
		self.push_region(owner, kind, args)
	}

	fn leave_region(&mut self) -> Result<(), IrError> {
		self.pop_region()
	}

	fn lookup(&self, name: &str) -> NameRef {
		let floor = self.stack.last().map_or(0, |entry| entry.floor);
		match self.env[floor..].iter().rev().find(|binding| binding.name.as_str() == name) {
			Some(binding) => NameRef::Value(binding.value),
			None => NameRef::Free,
		}
	}

	/// Where no row frame is entered, a `NONE` literal: a field read or `@` of no document reads
	/// `NONE`. A region that receives a row (a clause, a frame) binds its body over that row, so a
	/// read with no row inside one, short of a region isolated from above, is an invariant break.
	fn row(&mut self) -> Result<ValueId, IrError> {
		if let Some(row) = self.frame_row() {
			return Ok(row);
		}
		for kind in self.kinds.iter().rev() {
			if receives_row(*kind) {
				return Err(invariant("a row is read where no row frame is entered"));
			}
			if kind.isolated_from_above() {
				break;
			}
		}
		self.none_literal()
	}

	/// The current value where one is bound. Where none is, a path that starts from it reads
	/// `NONE`, as a field read does with no row, unless a region that receives a row is entered
	/// without one: such a region binds its current value over its row, so the absence is an
	/// invariant break.
	fn current(&mut self) -> Result<ValueId, IrError> {
		if let Some(current) = self.current {
			return Ok(current);
		}
		for kind in self.kinds.iter().rev() {
			if receives_row(*kind) {
				return Err(invariant("the current value is read where none is bound"));
			}
			if kind.isolated_from_above() {
				break;
			}
		}
		self.none_literal()
	}

	fn replace_current(&mut self, value: Option<ValueId>) -> Option<ValueId> {
		mem::replace(&mut self.current, value)
	}

	fn replace_row(&mut self, value: Option<ValueId>) -> Option<ValueId> {
		mem::replace(&mut self.row, value)
	}

	fn replace_field_base(&mut self, value: Option<ValueId>) -> Option<ValueId> {
		mem::replace(&mut self.field_base, value)
	}

	fn step_root(&mut self, receiver: ValueId) -> Result<ValueId, IrError> {
		Ok(self.step_root.or(self.row).unwrap_or(receiver))
	}

	fn declare_parent_as(&mut self, value: ValueId) {
		self.declare(Strand::new("parent"), value);
	}

	fn declare_row_names_until(&mut self, value: ValueId) -> usize {
		let mark = self.env.len();
		let parent = self.row.is_none() && matches!(self.lookup("parent"), NameRef::Free);
		for name in ["this", "self"] {
			self.declare_unnamed(Strand::new(name), value);
		}
		if parent {
			self.declare_unnamed(Strand::new("parent"), value);
		}
		mark
	}

	fn end_row_names(&mut self, mark: usize) {
		self.env.truncate(mark);
	}

	fn timestamp(&self) -> Option<ValueId> {
		self.stack.iter().rev().find_map(|entry| entry.timestamp)
	}

	/// Answers `false` for every name, since the bind context holds no capability set. A call the
	/// binder does not fold into a read of the statement timestamp keeps its run-time capability
	/// check.
	fn allows_function(&self, name: &str) -> bool {
		let _ = name;
		false
	}

	fn field(&self, name: &Strand) -> FieldRead {
		let _ = name;
		FieldRead::Plain
	}

	fn aggregating(&self) -> bool {
		self.aggregating
	}

	fn next_convert_site(&mut self) -> ConvertSite {
		let site = ConvertSite(self.convert_sites);
		self.convert_sites += 1;
		site
	}

	fn row_name_operands(&self) -> SmallVec<[ValueId; 2]> {
		self.row_names().collect()
	}

	/// The statement takes the document of the frame it is entered from as its `$this` and
	/// `$parent`, and one that runs over that row ([`LogicalOp::inherits_row`]) as its `$self` as
	/// well ([`BindCx::declare_statement_row_names`]). The bindings end with the statement.
	fn bind_value_statement(&mut self, op: &LogicalOp) -> Result<Built, IrError> {
		let mark = self.env.len();
		self.declare_statement_row_names(op.inherits_row());
		let built = self.without_aggregating(|cx| bind_value_statement(cx, op));
		self.env.truncate(mark);
		built
	}

	fn bind_loop_site(&mut self, site: LoopSite<'_>) -> Result<Built, IrError> {
		bind_loop_site(self, site)
	}

	fn bind_closure(&mut self, closure: &LogicalClosure) -> Result<ValueId, IrError> {
		bind_closure(self, closure)
	}

	fn inlinable_definition(&mut self, name: &str) -> Option<Arc<FunctionDefinition>> {
		if self.in_stored_code()
			|| self.kinds.iter().any(|kind| matches!(kind, RegionKind::ClosureBody))
			|| self.late_bound
			|| !self.eager
			|| self.after_stored_code
			|| self.catalog_touched
			|| self.effects.catalog.calls_current()
			|| self.effects.catalog.plans_at_stamp()
		{
			return None;
		}
		let ir = self.rw.ir();
		if !matches!(ir.snapshot(self.snapshot), Some(Snapshot::Current))
			|| !ir.frame(self.frame).is_some_and(|frame| frame.eval.is_caller())
		{
			return None;
		}
		let CatalogAnswer::Found(definition) = self.effects.catalog.function(name) else {
			return None;
		};
		(matches!(definition.permissions, Permission::Full)
			&& self.effects.catalog.function_limit_is_inert(&definition))
		.then_some(definition)
	}

	fn bind_function_body(
		&mut self,
		owner: OpId,
		definition: &FunctionDefinition,
		types: &[TypeKnowledge],
		body: &LogicalExpr,
		text: Option<Arc<String>>,
	) -> Result<(), IrError> {
		let params: Vec<Strand> =
			definition.args.iter().map(|(name, _)| Strand::new(name.as_str())).collect();
		let origin = LocOrigin::Catalog(Box::new((
			CatalogObjectKey {
				kind: CatalogKind::Function,
				ns: None,
				db: None,
				table: None,
				name: Some(definition.name.clone()),
			},
			ObjectVersion::Content(FunctionSnapshot::hash_of(definition)),
		)));
		let outer = self.origin.replace(origin);
		let bound = self.with_current_state(|cx| {
			cx.within_function_body(owner, &params, types, &[], |cx| match bind_expr(cx, body)? {
				Built::Value(value) => {
					if let Some(text) = text {
						cx.state_text_of(value, text)?;
					}
					cx.create(
						OpKind::Expr(ExprOp::Yield),
						smallvec::smallvec![value],
						SmallVec::new(),
					)
					.map(|_| ())
				}
				Built::Diverged => Ok(()),
			})
		});
		self.origin = outer;
		bound
	}

	fn note_call(&mut self, function: &Function) {
		use crate::expr::logical::{EffectOracle, EffectSet};
		let touches = match function {
			Function::Normal(name) => {
				crate::exec::logical::effects::Oracle::new(self.effects.registry)
					.builtin(name)
					.is_none_or(|set| {
						set.intersects(EffectSet::CHANGES_CATALOG | EffectSet::OPAQUE_BODY)
					})
			}
			Function::Custom(_)
			| Function::Script(_)
			| Function::Model(_)
			| Function::Module(..)
			| Function::Silo {
				..
			} => true,
		};
		self.catalog_touched |= touches;
	}

	fn writing(&self) -> bool {
		self.writing
	}

	fn block_len(&self) -> usize {
		self.block_ops().map_or(0, <[OpId]>::len)
	}

	fn write_text(&mut self, mark: usize, value: ValueId, expr: &LogicalExpr) {
		let Ok(block) = self.block() else {
			return;
		};
		let ir = self.rw.ir();
		let Some(&ValueDef::OpResult {
			op,
			..
		}) = ir.value(value).map(|data| &data.def)
		else {
			return;
		};
		let Some(ops) = ir.block(block).map(|data| data.ops.as_slice()) else {
			return;
		};
		let Some(at) = ops.iter().rposition(|held| *held == op) else {
			return;
		};
		if at < mark {
			return;
		}
		let Some(data) = ir.op(op) else {
			return;
		};
		let loc = data.loc.clone().with_text(Arc::new(expr.to_sql()));
		let _ = self.rw.set_location(op, loc);
	}

	fn note_knn_vector(&mut self, knn: ValueId, vector: &LogicalExpr) {
		let ir = self.rw.ir();
		let Some(right) = ir
			.value(knn)
			.and_then(|data| match data.def {
				ValueDef::OpResult {
					op,
					..
				} => ir.op(op),
				ValueDef::BlockArg {
					..
				} => None,
			})
			.and_then(|data| data.operands.get(1).copied())
		else {
			return;
		};
		let Some(held) = ir.value(right) else {
			return;
		};
		let ValueDef::OpResult {
			op,
			..
		} = held.def
		else {
			return;
		};
		if held.hint_name.is_some() {
			return;
		}
		let Some(data) = ir.op(op) else {
			return;
		};
		if data.loc.text().is_some() {
			return;
		}
		let loc = data.loc.clone().with_text(Arc::new(vector.to_sql()));
		let _ = self.rw.set_location(op, loc);
	}

	fn note_matches(&mut self, matches: ValueId, expr: &LogicalExpr) {
		let ir = self.rw.ir();
		let Some(&ValueDef::OpResult {
			op,
			..
		}) = ir.value(matches).map(|data| &data.def)
		else {
			return;
		};
		let Some(data) = ir.op(op) else {
			return;
		};
		if data.loc.text().is_some() {
			return;
		}
		let loc = data.loc.clone().with_text(Arc::new(expr.to_sql()));
		let _ = self.rw.set_location(op, loc);
	}

	fn bind_control(&mut self, control: Control) -> Result<Built, IrError> {
		match scope::loop_exit(self.kinds()) {
			LoopExit::Target(_) => {
				let exit = match control {
					Control::Break => LogOp::Break,
					Control::Continue => LogOp::Next,
				};
				self.emit(OpKind::Logical(exit), &[], &[])?;
			}
			LoopExit::Barrier => self.raise(RaiseKind::InvalidControlFlow)?,
		}
		Ok(Built::Diverged)
	}

	fn in_recursion_body(&self) -> bool {
		let ir = self.rw.ir();
		for (entry, kind) in self.stack.iter().zip(&self.kinds).rev() {
			if let Some(OpKind::Logical(LogOp::Loop {
				kind: loop_kind,
				..
			})) = ir.parent_op(entry.region).and_then(|owner| ir.op(owner)).map(|data| &data.kind)
			{
				return *loop_kind == LoopKind::Recurse;
			}
			if kind.isolated_from_above() {
				return false;
			}
		}
		false
	}
}
