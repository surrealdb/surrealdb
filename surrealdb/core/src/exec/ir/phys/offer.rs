//! What a statement offers the access path of a table read: the predicate and the ordering it
//! states over the rows of the source, and whether a `LIMIT` can end the read early.
//!
//! [`offer_of`] follows the rows of a `logical.source` through the only user of each value: a
//! `Select` row loop is entered and its body read, a filter contributes the conjuncts of its
//! `Clause(Where)` region, a sort contributes its ordering and a `LIMIT` states that the read may
//! end early. A permission filter or a resolver layer met on the way says the read's table is
//! enforced above the read. The predicate of a permission filter over the source's own rows is
//! offered after the statement's conjuncts, unless those hold a `MATCHES` or `KNN` operator: only
//! its conjuncts that name no parameter, and the filter keeps its whole predicate. Everything the
//! walk cannot read stays where it is, a residual of the filter above the read: an offer states
//! less than the statement says, never more.
//!
//! The walk is conservative about what reshapes the rows. A projection that is not the identity,
//! a resolver layer, or any op of a loop body that is not a filter, a permission filter or a
//! projection ends the predicate: the filters beyond it read rows other than the source's. The
//! one exception is a `KNN` operator of a filter beyond resolver layers alone whose left side is a
//! path no layer writes: it reads what the source's row holds, so it is offered, and every other
//! conjunct of that filter is withheld. A
//! projection that is not the identity withholds the ordering as well, since a sort key then names
//! a projected value. A resolver layer withholds only an ordering a key of which may read a column
//! it writes: a key on any other path orders the source's rows as they stand beneath the layer,
//! which passes them through in order. An op that is not part of the logical dialect ends the walk.
//!
//! A union of sources ends the walk too, except for [`offer_across`], which crosses it to offer
//! one member the predicate the statement states over every member's rows. Such an offer states
//! no ordering and no paging, which apply to the union's rows and not to the member's, and the
//! filters it reads judge the rows of every member: an access path decided over it must leave them
//! as they stand. The same walk reads the `WHERE` of a write whose subjects the source reads,
//! which the write judges on each subject itself, and leaves it as it stands too.

use std::ops::Range;

use super::access::SortKeys;
use super::access_expr::{Reconstructed, expr_def, ordering_of, predicate_of_region};
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, FieldRole, RegionKind};
use crate::exec::ir::logical::ops::{
	FieldShape, FieldsShape, LogOp, LoopKind, LoopSink, ProjectAttrs, SortShape,
};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::logical::{Descend, visit_binding_pruning};
use crate::exec::planner::util::{expr_contains_matches, has_knn_operator};
use crate::expr::BinaryOperator;
use crate::expr::logical::{LogicalExpr, LogicalLiteral, LogicalOrdering, OutputPathPart};
use crate::val::{Number, Value};

/// The filter an offer's conjuncts were read from.
#[derive(Clone, Debug)]
pub(super) struct FilterPart {
	/// The filter op.
	pub(super) op: OpId,
	/// Its `Clause(Where)` region.
	pub(super) region: RegionId,
	/// The positions of its conjuncts in the offer's predicate.
	pub(super) positions: Range<usize>,
	/// Whether the part is the predicate of a permission filter: the access path is chosen from
	/// its conjuncts, and the filter judges every row on all of them.
	pub(super) permission: bool,
}

/// What the statement offers one table read.
#[derive(Clone, Debug, Default)]
pub(super) struct Offer {
	/// The conjuncts of the filters over the source's rows, in the order they apply, numbered
	/// across all of them.
	pub(super) predicate: Reconstructed,
	/// The filters the conjuncts came from, in the same order.
	pub(super) filters: Vec<FilterPart>,
	/// The ordering and paging the statement states, when it states either.
	pub(super) sort: Option<SortKeys>,
	/// Whether a permission filter or a resolver layer stands above the source, so those nodes
	/// enforce what the source's table declares, or the source is the subject of a write, whose
	/// stages judge each stored record themselves. Either way the read must not enforce it.
	pub(super) enforced_above: bool,
	/// Whether the source is the subject of a write.
	pub(super) write_subjects: bool,
	/// The permission filters over the source's own rows, innermost first: a `KNN` search gates
	/// each candidate on them.
	pub(super) permissions: Vec<OpId>,
	/// Whether a permission filter stands above a node that reshapes the source's rows.
	pub(super) permission_past_rows: bool,
	/// The positions of the conjuncts withheld because they stand past a resolver layer: they read
	/// the rows the layer resolves, which a search over the source's rows never holds.
	pub(super) past_layers: Vec<usize>,
	/// Whether the walk crossed a union of sources, so the filters read judge the rows of other
	/// sources too ([`offer_across`]).
	pub(super) united: bool,
	/// Whether the conjuncts were read from the `WHERE` of the write whose subjects the source
	/// reads ([`offer_across`]): the write judges each subject on them itself.
	pub(super) in_write: bool,
}

/// What the walk has learned about the rows so far.
struct State {
	/// Whether a filter met at this point reads the source's own rows.
	collecting: bool,
	/// Whether a sort key met at this point names a field of the source's rows, unless it names
	/// one of `ordering_layered`.
	ordering_open: bool,
	/// The first key of each column the resolver layers crossed so far write, other than those of
	/// the key part: a sort key reading one orders by what the layer derives, which no read of the
	/// source delivers.
	ordering_layered: Vec<String>,
	ordering: Option<LogicalOrdering>,
	limited: bool,
	limit: Option<u32>,
	/// Past resolver layers alone, the first key of each column they write: a `KNN` operator over
	/// any other path still reads the source's row. `None` while the rows are the source's, or
	/// once anything else reshapes them.
	layered: Option<Vec<String>>,
}

/// The predicate, ordering and paging the statement offers the read `source`.
pub(super) fn offer_of(ir: &Ir, source: OpId) -> Result<Offer, IrError> {
	offer_walk(ir, source, false)
}

/// The predicate the statement offers the read `source`, a member of a union of sources, read
/// across the union ([`Offer::united`]), or the subjects of a write, read with the write's `WHERE`
/// ([`Offer::in_write`]): what [`offer_of`] answers when neither stands above the source.
pub(super) fn offer_across(ir: &Ir, source: OpId) -> Result<Offer, IrError> {
	offer_walk(ir, source, true)
}

/// The walk of [`offer_of`], crossing a union of sources and reading a write's `WHERE` when
/// `across` is set.
fn offer_walk(ir: &Ir, source: OpId, across: bool) -> Result<Offer, IrError> {
	let mut offer = Offer::default();
	let mut state = State {
		collecting: true,
		ordering_open: true,
		ordering_layered: Vec::new(),
		ordering: None,
		limited: false,
		limit: None,
		layered: None,
	};
	let Some(mut rows) = ir.op(source).and_then(|data| data.results.first().copied()) else {
		return Ok(offer);
	};
	while let [only] = ir.uses(rows) {
		let Some(user) = ir.op(only.user) else {
			break;
		};
		let OpKind::Logical(kind) = &user.kind else {
			break;
		};
		if across && !offer.united && matches!(kind, LogOp::UnionAll) {
			offer.united = true;
			state.ordering_open = false;
			let Some(next) = user.results.first().copied() else {
				break;
			};
			rows = next;
			continue;
		}
		if only.operand != 0 {
			break;
		}
		match kind {
			LogOp::Loop {
				kind: LoopKind::Select,
				sink: LoopSink::Output,
				..
			} => {
				let [body] = user.regions.as_slice() else {
					break;
				};
				read_body(ir, *body, &mut offer, &mut state)?;
			}
			// A write's subjects are the stored records: its gates judge each one, and its data
			// clause and field schema apply to every field, whatever the caller may select.
			LogOp::Loop {
				kind: LoopKind::Write,
				..
			} => {
				offer.enforced_above = true;
				offer.write_subjects = true;
				if across {
					take_write_conditions(ir, only.user, &mut offer)?;
				}
				break;
			}
			LogOp::Filter => take_filter(ir, only.user, &mut offer, &state)?,
			LogOp::PermissionFilter {
				..
			} => take_permission(only.user, &mut offer, &state),
			LogOp::Project(attrs) => {
				offer.enforced_above |= attrs.layer;
				reshape(ir, only.user, attrs, &mut state);
			}
			LogOp::Sort(attrs) => {
				if state.ordering_open && state.ordering.is_none() {
					let ordering = match &attrs.ordering {
						SortShape::Random => Some(LogicalOrdering::Random),
						SortShape::Keys(keys) => {
							let flags: Vec<(bool, bool, bool)> = keys
								.iter()
								.map(|key| (key.collate, key.numeric, key.ascending))
								.collect();
							ordering_of(ir, &flags, &user.regions)
						}
					};
					match ordering {
						Some(LogicalOrdering::Order(keys))
							if keys
								.iter()
								.any(|key| reads_layered(&key.value, &state.ordering_layered)) =>
						{
							state.ordering_open = false;
						}
						ordering => state.ordering = ordering,
					}
				}
			}
			LogOp::Limit {
				limit,
				start,
			} => {
				state.limited = !offer.united;
				if !offer.united && start.is_none() {
					state.limit = limit
						.and_then(|slot| user.operands.get(usize::from(slot)))
						.and_then(|value| literal_limit_count(ir, *value));
				}
				break;
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
			| LogOp::Define(_)
			| LogOp::Remove(_)
			| LogOp::Alter(_)
			| LogOp::Rebuild(_)
			| LogOp::UnionAll
			| LogOp::Join {
				..
			}
			| LogOp::Split {
				..
			}
			| LogOp::Aggregate(_)
			| LogOp::Distinct
			// A window's values depend on every row of a partition: no filter, ordering or limit
			// above it reaches the access path.
			| LogOp::Window(_)
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
			| LogOp::Break => break,
		}
		let Some(next) = user.results.first().copied() else {
			break;
		};
		rows = next;
	}
	if !across {
		take_permission_predicates(ir, &mut offer)?;
	}
	if state.ordering.is_some() || state.limited {
		offer.sort = Some(SortKeys {
			ordering: state.ordering,
			limited: state.limited,
			limit: state.limit,
		});
	}
	Ok(offer)
}

/// The literal positive integer count of a `LIMIT` operand, when it is one.
fn literal_limit_count(ir: &Ir, value: ValueId) -> Option<u32> {
	let (ExprOp::Literal(literal), _) = expr_def(ir, value)? else {
		return None;
	};
	if let Value::Number(Number::Int(n)) = &**literal
		&& *n > 0
	{
		u32::try_from(*n).ok()
	} else {
		None
	}
}

/// Reads the ops of the body of a `Select` row loop in order.
fn read_body(ir: &Ir, body: RegionId, offer: &mut Offer, state: &mut State) -> Result<(), IrError> {
	for op in ops_in(ir, body) {
		let Some(OpKind::Logical(kind)) = ir.op(op).map(|data| &data.kind) else {
			state.collecting = false;
			state.ordering_open = false;
			state.layered = None;
			continue;
		};
		match kind {
			LogOp::Filter => take_filter(ir, op, offer, state)?,
			LogOp::PermissionFilter {
				..
			} => take_permission(op, offer, state),
			LogOp::Yield => {}
			LogOp::Project(attrs) => {
				offer.enforced_above |= attrs.layer;
				reshape(ir, op, attrs, state);
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
			| LogOp::Define(_)
			| LogOp::Remove(_)
			| LogOp::Alter(_)
			| LogOp::Rebuild(_)
			| LogOp::UnionAll
			| LogOp::Join {
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
			| LogOp::Next
			| LogOp::Break => {
				state.collecting = false;
				state.ordering_open = false;
				state.layered = None;
			}
		}
	}
	Ok(())
}

/// Adds the conjuncts of the `WHERE` of the write loop `write`, the `Clause(WriteCond)` regions of
/// its stages, each region a filter part standing in the stage op that hosts it.
fn take_write_conditions(ir: &Ir, write: OpId, offer: &mut Offer) -> Result<(), IrError> {
	let Some(body) = ir.op(write).and_then(|data| data.regions.first().copied()) else {
		return Ok(());
	};
	for stage in ops_in(ir, body) {
		let Some(data) = ir.op(stage) else {
			continue;
		};
		for region in &data.regions {
			if ir.region(*region).map(|held| held.kind)
				!= Some(RegionKind::Clause(ClauseRole::WriteCond))
			{
				continue;
			}
			let start = offer.predicate.len();
			if !predicate_of_region(ir, *region, &mut offer.predicate)? {
				continue;
			}
			offer.in_write = true;
			offer.filters.push(FilterPart {
				op: stage,
				region: *region,
				positions: start..offer.predicate.len(),
				permission: false,
			});
		}
	}
	Ok(())
}

/// Records the permission filter `op`, which enforces what the source's table declares.
fn take_permission(op: OpId, offer: &mut Offer, state: &State) {
	offer.enforced_above = true;
	match state.collecting {
		true => offer.permissions.push(op),
		false => offer.permission_past_rows = true,
	}
}

/// Adds the conjuncts of the predicates of the permission filters over the source's own rows, when
/// the statement's conjuncts hold no `MATCHES` or `KNN` operator: the access path is chosen from
/// what the permission admits as well as from what the statement asks. Every such filter keeps
/// its predicate whole ([`FilterPart::permission`]), so a row the access lists is still judged on
/// it.
///
/// A conjunct is offered only when its form names nothing the run binds: a parameter or a value
/// of the definer's frame stays in the permission and is never read beside the scan. A conjunct
/// that is the literal `false`, or that holds an index operator, is withheld.
fn take_permission_predicates(ir: &Ir, offer: &mut Offer) -> Result<(), IrError> {
	if offer.write_subjects || offer.united {
		return Ok(());
	}
	let scoped = |expr: &LogicalExpr| has_knn_operator(expr) || expr_contains_matches(expr);
	if offer.predicate.condition().as_ref().is_some_and(scoped) {
		return Ok(());
	}
	for filter in offer.permissions.clone() {
		let Some(region) = ir.op(filter).and_then(|data| data.regions.first().copied()) else {
			continue;
		};
		if ir.region(region).map(|held| held.kind) != Some(RegionKind::PermissionFrame) {
			continue;
		}
		let mut read = Reconstructed {
			operands: std::mem::take(&mut offer.predicate.operands),
			..Reconstructed::default()
		};
		let complete = predicate_of_region(ir, region, &mut read);
		offer.predicate.operands = std::mem::take(&mut read.operands);
		if !complete? {
			continue;
		}
		let start = offer.predicate.len();
		let total = read.len();
		let offered: Vec<(usize, LogicalExpr)> = read
			.conjuncts
			.into_iter()
			.filter(|(_, conjunct)| !scoped(conjunct) && offerable(conjunct))
			.collect();
		let positions: Vec<usize> = offered.iter().map(|(at, _)| *at).collect();
		offer.predicate.conjuncts.extend(offered.into_iter().map(|(at, c)| (start + at, c)));
		offer
			.predicate
			.withheld
			.extend((0..total).filter(|at| !positions.contains(at)).map(|at| start + at));
		offer.predicate.withheld.sort_unstable();
		offer.filters.push(FilterPart {
			op: filter,
			region,
			positions: start..offer.predicate.len(),
			permission: true,
		});
	}
	Ok(())
}

/// Whether the permission conjunct `conjunct` names nothing the run binds and is not the literal
/// `false`.
fn offerable(conjunct: &LogicalExpr) -> bool {
	if matches!(conjunct, LogicalExpr::Literal(LogicalLiteral::Bool(false))) {
		return false;
	}
	let mut bound = false;
	visit_binding_pruning(conjunct, &mut |expr| {
		if matches!(expr, LogicalExpr::Param(_)) {
			bound = true;
			return Descend::Over;
		}
		Descend::Into
	});
	!bound
}

/// Adds the conjuncts of the filter `op` when the rows it reads are still the source's, and past
/// resolver layers alone its `KNN` operators over a path no layer writes, the other conjuncts
/// withheld.
fn take_filter(ir: &Ir, op: OpId, offer: &mut Offer, state: &State) -> Result<(), IrError> {
	if !state.collecting && state.layered.is_none() {
		return Ok(());
	}
	let Some(data) = ir.op(op) else {
		return Ok(());
	};
	let [region] = data.regions.as_slice() else {
		return Ok(());
	};
	if ir.region(*region).map(|held| held.kind) != Some(RegionKind::Clause(ClauseRole::Where)) {
		return Ok(());
	}
	let start = offer.predicate.len();
	if let Some(columns) = state.layered.as_ref().filter(|_| !state.collecting) {
		let mut read = Reconstructed {
			operands: std::mem::take(&mut offer.predicate.operands),
			..Reconstructed::default()
		};
		let complete = predicate_of_region(ir, *region, &mut read)?;
		offer.predicate.operands = std::mem::take(&mut read.operands);
		if !complete {
			return Ok(());
		}
		let total = read.len();
		let readable: Vec<(usize, LogicalExpr)> = read
			.conjuncts
			.into_iter()
			.filter(|(_, conjunct)| unlayered_nearest(conjunct, columns))
			.collect();
		if readable.is_empty() {
			return Ok(());
		}
		let offered: Vec<usize> = readable.iter().map(|(at, _)| *at).collect();
		offer.predicate.conjuncts.extend(readable.into_iter().map(|(at, c)| (start + at, c)));
		let past: Vec<usize> =
			(0..total).filter(|at| !offered.contains(at)).map(|at| start + at).collect();
		offer.predicate.withheld.extend_from_slice(&past);
		offer.past_layers.extend(past);
		offer.predicate.withheld.sort_unstable();
		offer.filters.push(FilterPart {
			op,
			region: *region,
			positions: start..offer.predicate.len(),
			permission: false,
		});
		return Ok(());
	}
	if !predicate_of_region(ir, *region, &mut offer.predicate)? {
		return Ok(());
	}
	offer.filters.push(FilterPart {
		op,
		region: *region,
		positions: start..offer.predicate.len(),
		permission: false,
	});
	Ok(())
}

/// Records what the projection `op` does to the rows: the identity keeps them, any other
/// projection or resolver layer hands the nodes above it rows that are not the source's. A resolver
/// layer over the source's rows, or over layers alone, adds the columns it writes to
/// [`State::layered`]. A layer that only derives the components of the record id
/// ([`derives_key_only`]) keeps the ordering open: a sort key over one of its columns reads the
/// component the column is named after, which the access path reads by that name.
fn reshape(ir: &Ir, op: OpId, attrs: &ProjectAttrs, state: &mut State) {
	let identity = !attrs.layer
		&& attrs.omit == 0
		&& matches!(&attrs.fields, FieldsShape::Select(fields) if matches!(fields.as_ref(), [FieldShape::All]));
	if identity {
		return;
	}
	let layered = match (attrs.layer, state.collecting, state.layered.take()) {
		(true, true, None) => Some(Vec::new()),
		(true, false, Some(columns)) => Some(columns),
		(true, _, _) | (false, _, _) => None,
	};
	state.layered = layered.map(|mut columns| {
		columns.extend(layer_columns(attrs));
		columns
	});
	state.collecting = false;
	if !attrs.layer {
		state.ordering_open = false;
	} else if !derives_key_only(ir, op, attrs) {
		state.ordering_layered.extend(layer_columns(attrs));
	}
}

/// Whether the sort key `key` may read one of `columns`: a path whose first key is one of them,
/// or anything other than a path once any column is written.
fn reads_layered(key: &LogicalExpr, columns: &[String]) -> bool {
	if columns.is_empty() {
		return false;
	}
	if key.is_path() {
		return match key.output_path().parts().first() {
			Some(OutputPathPart::Field(name)) => columns.contains(name),
			Some(OutputPathPart::Lookup(_)) | None => true,
		};
	}
	match key {
		LogicalExpr::Literal(_) | LogicalExpr::Param(_) => false,
		LogicalExpr::Array(items) => items.iter().any(|item| reads_layered(item, columns)),
		LogicalExpr::Call(call) => call.args.iter().any(|arg| reads_layered(arg, columns)),
		LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
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
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => true,
	}
}

/// The first key of each column the resolver layer `attrs` writes.
fn layer_columns(attrs: &ProjectAttrs) -> Vec<String> {
	let FieldsShape::Select(fields) = &attrs.fields else {
		return Vec::new();
	};
	let mut names = attrs.names.iter();
	fields
		.iter()
		.filter_map(|field| match field {
			FieldShape::All => None,
			FieldShape::Single {
				alias,
				..
			} => match alias {
				Some(alias) => Some(alias),
				None => names.next(),
			},
		})
		.filter_map(|path| match path.parts().first() {
			Some(OutputPathPart::Field(name)) => Some(name.clone()),
			Some(OutputPathPart::Lookup(_)) | None => None,
		})
		.collect()
}

/// Whether `conjunct` is a `KNN` operator a search answers whole whose left side is a path whose
/// first key is none of `columns`.
fn unlayered_nearest(conjunct: &LogicalExpr, columns: &[String]) -> bool {
	let LogicalExpr::Binary {
		left,
		op: BinaryOperator::NearestNeighbor(_),
		..
	} = conjunct
	else {
		return false;
	};
	left.is_path()
		&& match left.output_path().parts().first() {
			Some(OutputPathPart::Field(name)) => !columns.contains(name),
			Some(OutputPathPart::Lookup(_)) | None => false,
		}
}

/// Whether the projection `op` is a layer over the whole row whose every column is of the key
/// part: each column's field frame region evaluates under the key-part frame
/// ([`EvalFrame::key_part`](crate::expr::logical::EvalFrame::key_part)).
fn derives_key_only(ir: &Ir, op: OpId, attrs: &ProjectAttrs) -> bool {
	let FieldsShape::Select(fields) = &attrs.fields else {
		return false;
	};
	let [FieldShape::All, columns @ ..] = fields.as_ref() else {
		return false;
	};
	if !attrs.layer
		|| attrs.omit != 0
		|| columns.is_empty()
		|| columns.iter().any(|field| matches!(field, FieldShape::All))
	{
		return false;
	}
	let Some(data) = ir.op(op) else {
		return false;
	};
	let frames: Vec<RegionId> = data
		.regions
		.iter()
		.copied()
		.filter(|region| {
			ir.region(*region)
				.is_some_and(|held| held.kind == RegionKind::FieldFrame(FieldRole::Computed))
		})
		.collect();
	frames.len() == columns.len()
		&& frames.iter().all(|region| {
			ops_in(ir, *region)
				.next()
				.and_then(|first| ir.op(first))
				.and_then(|first| ir.frame(first.frame))
				.is_some_and(|frame| frame.eval.key_part)
		})
}
