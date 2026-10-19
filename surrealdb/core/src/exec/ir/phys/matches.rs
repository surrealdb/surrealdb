//! The `MATCHES` operators of a `SELECT`: whether its `WHERE` registers each one, and the tables
//! whose rows an index answers it for.
//!
//! - **Registration.** A filter stage of a select loop registers every `MATCHES` operator its
//!   predicate reaches through binary operators, `AND`, `OR` and negations
//!   ([`crate::exec::vm::matches`]). An operator elsewhere in the loop's body, such as a
//!   projection, a function argument or a nested statement, is registered when the predicate
//!   reaches it, or when it is written the same as one the predicate reaches: the same operator
//!   over operands built the same way from the row.
//! - **Executor tables.** The loop's read names them: a table read whole is known, a record-id
//!   source is excluded, and a parameter or an expression that names its table only when the
//!   statement runs leaves the set open.
//! - **Nesting.** A select loop inside another decides for its own operators. A loop whose stages
//!   register nothing leaves the decision of the enclosing loop in force for the operators inside
//!   it, as a nested statement inherits the scope of the statement that holds it.
//!
//! - **Index functions and `KNN`.** An index function call in a select loop reports against the
//!   `MATCHES` clauses of the loop's stages and whether they hold a `KNN` operator. A call that the
//!   conversion rebuilt finds its scope through the region that holds the loop. A `KNN` operator is
//!   in a search's scope where those stages hold one.
//! - **The loop's clauses.** The ops after a select loop in its region, up to the next select loop
//!   that does not read its rows, shape its rows (a sort, and the select loop that projects the
//!   sorted rows) and are decided with it; a select loop among them decides nothing of its own.
//!
//! An operator outside every select loop, or in a closure body, a function body or a permission or
//! field frame, has no attribute: lowering answers it `false`. The pass reads the logical select
//! loops before the conversion rewrites their stages, so a conjunct an access later answers is
//! registered all the same.

use std::collections::HashSet;
use std::sync::Arc;

use super::knn::{is_select_loop, stages_of_loop};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::side_map::SideMap;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::expr::ops::{ExprOp, ShortCircuitOp};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::iface::statements::WriteForm;
use crate::exec::ir::logical::ops::{LogOp, LoopKind, SourceRefShape};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::vm::matches::ExecutorTables;
use crate::expr::logical::{FieldPath, FieldSegment};
use crate::expr::operator::{MatchRef, PrefixOperator};
use crate::expr::{BinaryOperator, Function};
use crate::val::TableName;

/// How deep two operand cones are compared before they are taken as different.
const SAME_DEPTH: usize = 32;

/// What the physical plan decides for one `MATCHES` operator.
#[derive(Clone, Debug)]
pub(crate) struct MatchesAttr {
	/// Whether the `WHERE` of the statement registers the operator.
	pub(crate) registered: bool,
	/// The tables whose rows an index answers the operator for.
	pub(crate) executor_tables: ExecutorTables,
	/// The declared position the operator's left operand reads off the row ([`field_path`]),
	/// resolved where the operator is decided. A container the decision is carried into may hold
	/// the left operand as a value it is handed, which names no position of its own.
	pub(crate) field: Option<FieldPath>,
	/// Whether the stages of the loop that decided this register any operator: a loop nested in it
	/// whose own stages register none leaves this decision in force.
	scoped: bool,
}

/// The decision for every `MATCHES` operator of one container, keyed by the id of its
/// `expr.binary` op. Never part of identity.
#[derive(Clone, Debug, Default)]
pub(crate) struct MatchesTable {
	matches: SideMap<OpId, MatchesAttr>,
	/// The `KNN` operators held by a select loop whose stages, or those of a loop around it, hold
	/// a `KNN` operator: the operators a search is in scope for.
	knn: SideMap<OpId, ()>,
	/// Of those, the ones that judge the subjects of a write that no search of the statement
	/// reads ([`crate::exec::ir::phys::knn`]).
	unsearched: SideMap<OpId, ()>,
	/// The literals that stand for a `KNN` operator no search answers
	/// ([`crate::exec::ir::phys::knn`]): each reads the row as the operator it replaces does.
	unanswered: SideMap<OpId, ()>,
	/// The scope each index function call held by a select loop reports against, keyed by the id
	/// of its `expr.call` op.
	calls: SideMap<OpId, Arc<IndexScope>>,
	/// The scope of the first select loop in each region that declares one. Used for an index
	/// function call the conversion rebuilt, which has no entry in `calls`.
	regions: SideMap<RegionId, Arc<IndexScope>>,
}

/// What an index function call in a select loop reports against: the `MATCHES` clauses of the
/// loop's stages and whether they hold a `KNN` operator. A loop whose stages declare neither leaves
/// the scope of the enclosing loop in force.
#[derive(Clone, Debug)]
pub(crate) struct IndexScope {
	/// The table the clauses are read against: the first table the loop reads whole.
	pub(crate) table: Option<TableName>,
	/// The clauses, in the order the stages write them; a later clause with a reference replaces
	/// an earlier one with the same reference.
	pub(crate) clauses: Vec<Clause>,
	/// Whether the stages hold a `KNN` operator.
	pub(crate) knn: bool,
}

/// One `MATCHES` clause of an [`IndexScope`].
#[derive(Clone, Debug)]
pub(crate) struct Clause {
	pub(crate) rf: MatchRef,
	/// The declared position the clause's left operand reads, when it reads one.
	pub(crate) field: Option<FieldPath>,
	/// The clause's query.
	pub(crate) query: ClauseQuery,
}

/// The query of a [`Clause`].
#[derive(Clone, Debug)]
pub(crate) enum ClauseQuery {
	/// Known when the plan is made: the text the index searches for.
	Text(String),
	/// A parameter, read by name when the call runs.
	Param(surrealdb_strand::Strand),
	/// A name bound around the statement, read through the value that holds it.
	Bound(ValueId),
}

impl IndexScope {
	/// This scope in a container whose values `values` names for those of the container it was
	/// decided for ([`MatchesTable::carry`]).
	fn mapped(&self, values: &dyn Fn(ValueId) -> Option<ValueId>) -> Self {
		let clauses = self
			.clauses
			.iter()
			.filter_map(|clause| {
				let query = match &clause.query {
					ClauseQuery::Bound(value) => ClauseQuery::Bound(values(*value)?),
					other => other.clone(),
				};
				Some(Clause {
					rf: clause.rf,
					field: clause.field.clone(),
					query,
				})
			})
			.collect();
		Self {
			table: self.table.clone(),
			clauses,
			knn: self.knn,
		}
	}

	/// The clause match reference `rf` names, or the only clause the scope holds when none carries
	/// that reference.
	pub(crate) fn resolve(&self, rf: MatchRef) -> Option<&Clause> {
		let only = match self.clauses.as_slice() {
			[only] => Some(only),
			_ => None,
		};
		self.clauses.iter().rev().find(|clause| clause.rf == rf).or(only)
	}
}

impl MatchesTable {
	/// An empty table, with no decisions.
	pub(crate) fn new() -> Self {
		Self {
			matches: SideMap::new(),
			knn: SideMap::new(),
			unsearched: SideMap::new(),
			unanswered: SideMap::new(),
			calls: SideMap::new(),
			regions: SideMap::new(),
		}
	}

	/// The decision for the operator `op`, or `None` when no select loop holds it.
	pub(crate) fn get(&self, op: OpId) -> Option<&MatchesAttr> {
		self.matches.get(op)
	}

	/// The scope the index function call `op` reports against, or `None` when no select loop
	/// declaring one holds it.
	pub(crate) fn index_scope(&self, op: OpId) -> Option<&IndexScope> {
		self.calls.get(op).map(|scope| &**scope)
	}

	/// The scope the region `region` holds for the index function calls inside it.
	pub(crate) fn region_scope(&self, region: RegionId) -> Option<&IndexScope> {
		self.regions.get(region).map(|scope| &**scope)
	}

	/// Whether a search is in scope for the `KNN` operator `op`.
	pub(crate) fn knn_scoped(&self, op: OpId) -> bool {
		self.knn.contains_key(op)
	}

	/// Whether the `KNN` operator `op` judges the subjects of a write no search reads.
	pub(crate) fn knn_unsearched(&self, op: OpId) -> bool {
		self.unsearched.contains_key(op)
	}

	/// Records that the `KNN` operator `op` judges the subjects of a write no search reads.
	pub(crate) fn mark_unsearched(&mut self, op: OpId) {
		self.unsearched.insert(op, ());
	}

	/// Whether the literal `op` stands for a `KNN` operator no search answers.
	pub(crate) fn knn_unanswered(&self, op: OpId) -> bool {
		self.unanswered.contains_key(op)
	}

	/// Records that the literal `op` stands for a `KNN` operator no search answers.
	pub(crate) fn mark_unanswered(&mut self, op: OpId) {
		self.unanswered.insert(op, ());
	}

	/// Whether this table holds a decision for `op`.
	pub(in crate::exec::ir) fn holds(&self, op: OpId) -> bool {
		self.matches.contains_key(op)
			|| self.knn.contains_key(op)
			|| self.unsearched.contains_key(op)
			|| self.unanswered.contains_key(op)
			|| self.calls.contains_key(op)
	}

	/// Gives `to` the decisions this table holds for `from`.
	pub(in crate::exec::ir) fn copy_entry(&mut self, from: OpId, to: OpId) {
		if let Some(attr) = self.matches.get(from).cloned() {
			self.matches.insert(to, attr);
		}
		if self.knn.contains_key(from) {
			self.knn.insert(to, ());
		}
		if self.unsearched.contains_key(from) {
			self.unsearched.insert(to, ());
		}
		if self.unanswered.contains_key(from) {
			self.unanswered.insert(to, ());
		}
		if let Some(scope) = self.calls.get(from).cloned() {
			self.calls.insert(to, scope);
		}
	}

	/// Gives each `to` of `pairs` the decision `source` holds for the `from` it is paired with, in
	/// a container whose values and regions `values` and `regions` name for those of the container
	/// `source` decides for. The values a scope's clauses read are those `values` names, and a
	/// clause whose query value it names none for is dropped. The scope a region `regions` names
	/// holds is the one its original holds.
	pub(in crate::exec::ir) fn carry(
		&mut self,
		source: &MatchesTable,
		pairs: impl IntoIterator<Item = (OpId, OpId)>,
		values: &dyn Fn(ValueId) -> Option<ValueId>,
		regions: &dyn Fn(RegionId) -> Option<RegionId>,
	) {
		let mut mapped: Vec<(*const IndexScope, Arc<IndexScope>)> = Vec::new();
		let mut remap = |scope: &Arc<IndexScope>| {
			let key = Arc::as_ptr(scope);
			if let Some((_, held)) = mapped.iter().find(|(at, _)| *at == key) {
				return Arc::clone(held);
			}
			let held = Arc::new(scope.mapped(values));
			mapped.push((key, Arc::clone(&held)));
			held
		};
		for (from, to) in pairs {
			if let Some(attr) = source.matches.get(from) {
				self.matches.insert(to, attr.clone());
			}
			if source.knn.contains_key(from) {
				self.knn.insert(to, ());
			}
			if source.unsearched.contains_key(from) {
				self.unsearched.insert(to, ());
			}
			if source.unanswered.contains_key(from) {
				self.unanswered.insert(to, ());
			}
			if let Some(scope) = source.calls.get(from) {
				self.calls.insert(to, remap(scope));
			}
		}
		for (region, scope) in source.regions.iter() {
			if let Some(copy) = regions(region) {
				self.regions.insert(copy, remap(scope));
			}
		}
	}
}

/// Decides every `MATCHES` operator held by a select loop of `ir`, as the module docs describe.
/// Runs on the logical container, before the conversion rewrites its stages.
pub(crate) fn run(ir: &mut Ir, registry: &FunctionRegistry) {
	let mut loops = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if ir.op(op).is_some_and(|data| is_select_loop(&data.kind)) {
			loops.push(op);
		}
		Walk::Continue
	});
	let mut table = MatchesTable::new();
	// The select loops that shape the rows of a loop before them, decided with that loop.
	let mut shaping: HashSet<OpId> = HashSet::new();
	for looped in loops {
		if shaping.contains(&looped) {
			continue;
		}
		let stages = stages_of_loop(ir, looped);
		let mut registered = HashSet::new();
		let mut written = Vec::new();
		let mut searched = false;
		for op in &stages.ops {
			let Some(data) = ir.op(*op) else {
				continue;
			};
			for region in &data.regions {
				if ir
					.region(*region)
					.is_some_and(|held| held.kind == RegionKind::Clause(ClauseRole::Where))
					&& let Some(value) = yielded(ir, *region)
				{
					collect(ir, value, &mut registered, &mut written, 0);
					searched |= holds_knn(ir, *region);
				}
			}
		}
		let executor_tables = executor_tables(ir, looped, false);
		let scoped = !registered.is_empty();
		let mut held = Held::default();
		if let Some(data) = ir.op(looped) {
			for region in &data.regions {
				operators_in(ir, registry, *region, &mut held);
			}
		}
		// The clauses that shape the loop's rows after it — a projection over sorted rows, a
		// grouping — stand after it in its region, up to the next select loop that does not read
		// those rows. A select loop over them is the statement's projection over its sorted rows.
		let mut rows: HashSet<ValueId> =
			ir.op(looped).map(|data| data.results.iter().copied().collect()).unwrap_or_default();
		for after in ops_in(ir, ir.enclosing_region(looped)).skip_while(|op| *op != looped).skip(1)
		{
			let Some(data) = ir.op(after) else {
				break;
			};
			let reads_rows = data.operands.first().is_some_and(|input| rows.contains(input));
			if is_select_loop(&data.kind) && !reads_rows {
				break;
			}
			if reads_rows {
				rows.extend(data.results.iter().copied());
				if is_select_loop(&data.kind) {
					shaping.insert(after);
				}
			}
			operator_of(ir, registry, after, &mut held);
			for region in &data.regions {
				operators_in(ir, registry, *region, &mut held);
			}
		}
		let Held {
			matches: held,
			nearest,
			calls,
		} = held;
		let mut clauses: Vec<Clause> = Vec::new();
		for op in written {
			let Some(clause) = clause_of(ir, op) else {
				continue;
			};
			match clauses.iter_mut().find(|held| held.rf == clause.rf) {
				Some(held) => *held = clause,
				None => clauses.push(clause),
			}
		}
		if !clauses.is_empty() || searched {
			let scope = Arc::new(IndexScope {
				table: if clauses.is_empty() {
					None
				} else {
					executor_tables.first_known().cloned()
				},
				clauses,
				knn: searched,
			});
			for op in calls {
				table.calls.insert(op, Arc::clone(&scope));
			}
			let statement = ir.enclosing_region(looped);
			if !table.regions.contains_key(statement) {
				table.regions.insert(statement, scope);
			}
		}
		if searched {
			for op in nearest {
				table.knn.insert(op, ());
			}
		}
		for op in held {
			if !scoped && table.get(op).is_some_and(|outer| outer.scoped) {
				continue;
			}
			let is_registered = registered.contains(&op)
				|| registered.iter().any(|other| same_op(ir, *other, op, 0));
			let attr = MatchesAttr {
				registered: is_registered,
				executor_tables: executor_tables.clone(),
				field: left_field(ir, op),
				scoped,
			};
			table.matches.insert(op, attr);
		}
	}
	subjects_scopes(ir, registry, &mut table);
	*ir.matches_table_mut() = table;
}

/// Decides the `MATCHES` operators, `KNN` operators and index function calls of each write's
/// clauses as a select loop's are decided: the write's `WHERE` is its stage, and its subjects'
/// source names the executor tables. Only an `UPDATE`, a `DELETE` and an `UPSERT` under a `WHERE`
/// judge their subjects this way; any other write decides none. An operator or a call a select
/// loop already decided keeps that decision.
fn subjects_scopes(ir: &Ir, registry: &FunctionRegistry, table: &mut MatchesTable) {
	let mut writes = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Logical(LogOp::Loop {
				kind: LoopKind::Write,
				..
			}))
		) {
			writes.push(op);
		}
		Walk::Continue
	});
	for write in writes {
		let Some(data) = ir.op(write) else {
			continue;
		};
		let mut conds = Vec::new();
		for region in &data.regions {
			ir.walk(*region, WalkOrder::PreOrder, &mut |op| {
				if let Some(held) = ir.op(op) {
					conds.extend(held.regions.iter().copied().filter(|inner| {
						ir.region(*inner).is_some_and(|held| {
							held.kind == RegionKind::Clause(ClauseRole::WriteCond)
						})
					}));
				}
				Walk::Continue
			});
		}
		let mut registered = HashSet::new();
		let mut written = Vec::new();
		let mut searched = false;
		for region in &conds {
			if let Some(value) = yielded(ir, *region) {
				collect(ir, value, &mut registered, &mut written, 0);
				searched |= holds_knn(ir, *region);
			}
		}
		let judged = match write_form(ir, write) {
			Some(WriteForm::Update | WriteForm::Delete) => true,
			Some(WriteForm::Upsert) => !conds.is_empty(),
			Some(WriteForm::Create | WriteForm::Insert | WriteForm::Relate) | None => false,
		};
		if !judged {
			continue;
		}
		let executor_tables = executor_tables(ir, write, true);
		let mut held = Held::default();
		for region in &data.regions {
			operators_in(ir, registry, *region, &mut held);
		}
		let mut clauses: Vec<Clause> = Vec::new();
		for op in written {
			let Some(clause) = clause_of(ir, op) else {
				continue;
			};
			match clauses.iter_mut().find(|held| held.rf == clause.rf) {
				Some(held) => *held = clause,
				None => clauses.push(clause),
			}
		}
		if !clauses.is_empty() || searched {
			let scope = Arc::new(IndexScope {
				table: if clauses.is_empty() {
					None
				} else {
					executor_tables.first_known().cloned()
				},
				clauses,
				knn: searched,
			});
			for op in &held.calls {
				if !table.calls.contains_key(*op) {
					table.calls.insert(*op, Arc::clone(&scope));
				}
			}
		}
		if searched {
			for op in &held.nearest {
				table.knn.insert(*op, ());
			}
		}
		let scoped = !registered.is_empty();
		for op in held.matches {
			if table.get(op).is_some() {
				continue;
			}
			let is_registered = registered.contains(&op)
				|| registered.iter().any(|other| same_op(ir, *other, op, 0));
			table.matches.insert(
				op,
				MatchesAttr {
					registered: is_registered,
					executor_tables: executor_tables.clone(),
					field: left_field(ir, op),
					scoped,
				},
			);
		}
	}
}

/// The form of the write whose loop over its subjects is `write`: the write op that reads the
/// loop's rows names it.
fn write_form(ir: &Ir, write: OpId) -> Option<WriteForm> {
	let data = ir.op(write)?;
	data.results.iter().flat_map(|result| ir.uses(*result)).find_map(|held| {
		match ir.op(held.user).map(|user| &user.kind) {
			Some(OpKind::Logical(LogOp::Write(attrs))) => Some(attrs.form),
			_ => None,
		}
	})
}

/// The value the single block of `region` yields.
fn yielded(ir: &Ir, region: RegionId) -> Option<ValueId> {
	let last = ops_in(ir, region).last()?;
	let data = ir.op(last)?;
	match &data.kind {
		OpKind::Expr(ExprOp::Yield) => data.operands.first().copied(),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

/// The op that defines `value`, with its kind, when an expression op does.
fn expr_def(ir: &Ir, value: ValueId) -> Option<(OpId, &ExprOp)> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	match &ir.op(op)?.kind {
		OpKind::Expr(kind) => Some((op, kind)),
		OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

/// Adds to `out` the `MATCHES` operators `value` reaches through binary operators, `AND`, `OR`,
/// negations and truthiness tests.
fn collect(
	ir: &Ir,
	value: ValueId,
	out: &mut HashSet<OpId>,
	written: &mut Vec<OpId>,
	depth: usize,
) {
	if depth > SAME_DEPTH {
		return;
	}
	let Some((op, kind)) = expr_def(ir, value) else {
		return;
	};
	let Some(data) = ir.op(op) else {
		return;
	};
	match kind {
		ExprOp::Binary {
			op: operator,
		} => {
			if matches!(operator, BinaryOperator::Matches(_)) && out.insert(op) {
				written.push(op);
			}
			for operand in &data.operands {
				collect(ir, *operand, out, written, depth + 1);
			}
		}
		ExprOp::ShortCircuit {
			op: ShortCircuitOp::And | ShortCircuitOp::Or,
		} => {
			if let Some(left) = data.operands.first() {
				collect(ir, *left, out, written, depth + 1);
			}
			if let Some(right) = data.regions.first().and_then(|region| yielded(ir, *region)) {
				collect(ir, right, out, written, depth + 1);
			}
		}
		ExprOp::Prefix(prefix) if matches!(**prefix, PrefixOperator::Not) => {
			if let Some(inner) = data.operands.first() {
				collect(ir, *inner, out, written, depth + 1);
			}
		}
		ExprOp::Truthy => {
			if let Some(inner) = data.operands.first() {
				collect(ir, *inner, out, written, depth + 1);
			}
		}
		_ => {}
	}
}

/// The operators and index function calls a select loop holds.
#[derive(Default)]
struct Held {
	matches: Vec<OpId>,
	nearest: Vec<OpId>,
	calls: Vec<OpId>,
}

/// Adds to `out` every `MATCHES` operator, `KNN` operator and index function call of `region` and
/// of the regions of its ops, except those of a closure body, a function body, and a permission,
/// field, event or subscriber frame.
fn operators_in(ir: &Ir, registry: &FunctionRegistry, region: RegionId, out: &mut Held) {
	let entered = ir.region(region).is_some_and(|held| {
		!matches!(
			held.kind,
			RegionKind::ClosureBody
				| RegionKind::FunctionBody
				| RegionKind::PermissionFrame
				| RegionKind::FieldFrame(_)
				| RegionKind::EventFrame
				| RegionKind::SubscriberFrame
		)
	});
	if !entered {
		return;
	}
	for op in ops_in(ir, region).collect::<Vec<_>>() {
		let Some(data) = ir.op(op) else {
			continue;
		};
		operator_of(ir, registry, op, out);
		for inner in &data.regions {
			operators_in(ir, registry, *inner, out);
		}
	}
}

/// Adds `op` to `out` when it is a `MATCHES` operator, a `KNN` operator or an index function call.
fn operator_of(ir: &Ir, registry: &FunctionRegistry, op: OpId, out: &mut Held) {
	let Some(data) = ir.op(op) else {
		return;
	};
	match &data.kind {
		OpKind::Expr(ExprOp::Binary {
			op: BinaryOperator::Matches(_),
		}) => out.matches.push(op),
		OpKind::Expr(ExprOp::Binary {
			op: BinaryOperator::NearestNeighbor(_),
		}) => out.nearest.push(op),
		OpKind::Expr(ExprOp::Call(call)) => {
			if let Function::Normal(name) = &call.function
				&& registry.is_index_function(name)
			{
				out.calls.push(op);
			}
		}
		_ => {}
	}
}

/// The clause the `MATCHES` operator `op` of a stage declares.
fn clause_of(ir: &Ir, op: OpId) -> Option<Clause> {
	let data = ir.op(op)?;
	let OpKind::Expr(ExprOp::Binary {
		op: BinaryOperator::Matches(operator),
	}) = &data.kind
	else {
		return None;
	};
	let [left, right] = data.operands.as_slice() else {
		return None;
	};
	let query = match query_operand(ir, *right) {
		QueryOperand::Literal(value) => ClauseQuery::Text(value.to_raw_string()),
		QueryOperand::Named(Some(name)) => ClauseQuery::Param(name),
		QueryOperand::Named(None) => ClauseQuery::Bound(*right),
		QueryOperand::Other => return None,
	};
	Some(Clause {
		rf: operator.rf.unwrap_or(0),
		field: field_path(ir, *left),
		query,
	})
}

/// The declared position the left operand of the `MATCHES` operator `op` reads off a row.
fn left_field(ir: &Ir, op: OpId) -> Option<FieldPath> {
	field_path(ir, *ir.op(op)?.operands.first()?)
}

/// The declared position `value` reads off a row: fields, accesses and `.*` over a block argument.
pub(crate) fn field_path(ir: &Ir, value: ValueId) -> Option<FieldPath> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	let data = ir.op(op)?;
	let OpKind::Expr(kind) = &data.kind else {
		return None;
	};
	let base = *data.operands.first()?;
	match kind {
		ExprOp::Field {
			name,
		} => match ir.value(base)?.def {
			ValueDef::BlockArg {
				..
			} => Some(FieldPath::field(name.clone())),
			ValueDef::OpResult {
				..
			} => None,
		},
		ExprOp::Access {
			field,
		} => Some(field_path(ir, base)?.child(FieldSegment::Field(field.clone()))),
		ExprOp::Each => Some(field_path(ir, base)?.child(FieldSegment::All)),
		_ => None,
	}
}

/// Whether an op of `region`, or of a region inside it, is a `KNN` operator.
fn holds_knn(ir: &Ir, region: RegionId) -> bool {
	let mut found = false;
	ir.walk(region, WalkOrder::PreOrder, &mut |op| {
		if let Some(OpKind::Expr(ExprOp::Binary {
			op: BinaryOperator::NearestNeighbor(_),
		})) = ir.op(op).map(|data| &data.kind)
		{
			found = true;
			return Walk::Stop;
		}
		Walk::Continue
	});
	found
}

/// Whether the ops `a` and `b` are written the same: the same kind and attributes over operands
/// that are each written the same, and no regions.
fn same_op(ir: &Ir, a: OpId, b: OpId, depth: usize) -> bool {
	if a == b {
		return true;
	}
	if depth > SAME_DEPTH {
		return false;
	}
	let (Some(left), Some(right)) = (ir.op(a), ir.op(b)) else {
		return false;
	};
	let (OpKind::Expr(left_kind), OpKind::Expr(right_kind)) = (&left.kind, &right.kind) else {
		return false;
	};
	left_kind.tag() == right_kind.tag()
		&& left_kind.attr_digest() == right_kind.attr_digest()
		&& left_kind.attrs_eq(right_kind)
		&& left.regions.is_empty()
		&& right.regions.is_empty()
		&& left.operands.len() == right.operands.len()
		&& left.operands.iter().zip(&right.operands).all(|(a, b)| same_value(ir, *a, *b, depth + 1))
}

/// Whether the values `a` and `b` are written the same: results at the same position of ops
/// written the same, or arguments at the same position of their blocks — the row of the region
/// that reads it.
fn same_value(ir: &Ir, a: ValueId, b: ValueId, depth: usize) -> bool {
	if a == b {
		return true;
	}
	let (Some(left), Some(right)) = (ir.value(a), ir.value(b)) else {
		return false;
	};
	match (left.def, right.def) {
		(
			ValueDef::OpResult {
				op: left,
				index: at,
			},
			ValueDef::OpResult {
				op: right,
				index: other,
			},
		) => at == other && same_op(ir, left, right, depth),
		(
			ValueDef::BlockArg {
				index: at,
				..
			},
			ValueDef::BlockArg {
				index: other,
				..
			},
		) => at == other,
		_ => false,
	}
}

/// The tables whose rows an index answers a `MATCHES` for, read off the sources beneath the loop
/// `looped`. For a write (`targets`), they are also read off the targets it names as values (an
/// array of them): a table is known, a record id is excluded, a `type::table` call names its table
/// only when the statement runs, which leaves the set open, and any other value names none.
fn executor_tables(ir: &Ir, looped: OpId, targets: bool) -> ExecutorTables {
	let mut known: Vec<TableName> = Vec::new();
	let mut excluded: Vec<TableName> = Vec::new();
	let mut open = false;
	let mut pending: Vec<ValueId> =
		ir.op(looped).and_then(|data| data.operands.first().copied()).into_iter().collect();
	while let Some(rows) = pending.pop() {
		let Some(ValueDef::OpResult {
			op,
			..
		}) = ir.value(rows).map(|data| data.def)
		else {
			continue;
		};
		let Some(data) = ir.op(op) else {
			continue;
		};
		match &data.kind {
			OpKind::Expr(ExprOp::Array) if targets => {
				for target in &data.operands {
					match expr_def(ir, *target).map(|(_, kind)| kind) {
						Some(ExprOp::Table {
							name,
						}) => known.push(name.clone()),
						Some(ExprOp::Call(call)) if matches!(&call.function, Function::Normal(name) if name == "type::table") =>
						{
							open = true;
						}
						_ => excluded.extend(record_table(ir, *target)),
					}
				}
			}
			OpKind::Logical(LogOp::UnionAll) => pending.extend(data.operands.iter().copied()),
			OpKind::Logical(LogOp::Source(attrs)) => match &attrs.source_ref {
				SourceRefShape::Table(table) => known.push(table.clone()),
				SourceRefShape::RecordId => {
					if let Some(table) =
						data.operands.first().and_then(|operand| record_table(ir, *operand))
					{
						excluded.push(table);
					}
				}
				SourceRefShape::Param | SourceRefShape::Dynamic => open = true,
				SourceRefShape::Mock => {}
			},
			_ => {}
		}
	}
	ExecutorTables::new(known, excluded, open)
}

/// The table of the record id `value` names, when a record-id op builds it.
fn record_table(ir: &Ir, value: ValueId) -> Option<TableName> {
	match expr_def(ir, value)? {
		(_, ExprOp::RecordId(attrs)) => Some(attrs.table.clone()),
		(_, ExprOp::Literal(literal)) => match &**literal {
			crate::val::Value::RecordId(rid) => Some(rid.table.clone()),
			_ => None,
		},
		_ => None,
	}
}

/// What the right operand of a `MATCHES` names as its query.
#[derive(Clone, Debug)]
pub(crate) enum QueryOperand {
	/// A value known when the plan is made.
	Literal(crate::val::Value),
	/// A name bound around the statement — a parameter, a `LET`, a `FOR` variable or a closure's
	/// argument — whose value is read when the operator runs; the name, when the op spells it.
	Named(Option<surrealdb_strand::Strand>),
	/// Any other expression, which names no query.
	Other,
}

/// What `value`, the right operand of a `MATCHES`, names as its query.
pub(crate) fn query_operand(ir: &Ir, value: ValueId) -> QueryOperand {
	let Some(data) = ir.value(value) else {
		return QueryOperand::Other;
	};
	let ValueDef::OpResult {
		op,
		..
	} = data.def
	else {
		return QueryOperand::Named(None);
	};
	match ir.op(op).map(|data| &data.kind) {
		Some(OpKind::Expr(ExprOp::Literal(literal))) => QueryOperand::Literal((**literal).clone()),
		Some(OpKind::Expr(
			ExprOp::FreeParam {
				name,
			}
			| ExprOp::SessionParam {
				name,
			}
			| ExprOp::CatalogParam {
				name,
			},
		)) => QueryOperand::Named(Some(name.clone())),
		Some(OpKind::Logical(_) | OpKind::Phys(_)) => QueryOperand::Named(None),
		Some(OpKind::Expr(_)) | None => QueryOperand::Other,
	}
}
