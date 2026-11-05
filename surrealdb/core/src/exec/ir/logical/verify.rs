//! The logical dialect's own verifier rules, added to the generic rules of the container.
//!
//! A rule that is absent rejects nothing. A container that breaks only a per-op rule that is not
//! declared here is accepted by this pass and caught, if at all, by the generic rules.
//!
//! Each rule is checked at the op it constrains, so the whole pass stays O(ops):
//!
//! - statement structure: a transaction stands only in the request region and never nests, a
//!   statement stands only in the request or a transaction, and those two regions hold no other
//!   logical op;
//! - a session transaction is the only op of its fragment's top block, so it holds every statement;
//!   an unterminated transaction is the last of a request that is one fragment, an open one is the
//!   last top-level region of its fragment and a continued one is the first;
//! - shape: the operand and result counts the op's arity states, the exact kind of each region by
//!   position and count, and the entry arguments of a statement, a loop body and an isolated
//!   region.
//!
//! `LogOp::region_kinds` answers the minimal form for an op whose attributes do not fix its region
//! count. Here, the kind it names at the open position may occur any number of times, none
//! included, in place of that entry. An op that hosts clause regions of its statement owns them
//! after the regions `region_kinds` names; [`tail_kinds`] lists the kinds it may host.

#![deny(clippy::wildcard_enum_match_arm)]

use super::ops::{
	LogOp, LoopKind, LoopSink, TxnRegionEnd, TxnRegionMode, TxnStart, WriteStageKind,
};
use crate::exec::ir::core::ir::{Ir, IrScope};
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::verify::{VerifyError, VerifyRule};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{
	ClauseRole, FieldRole, RegionKind, RegionKinds, RegionProperties, StoredRole,
};
use crate::exec::ir::iface::statements::{AccessOp, WriteForm};
use crate::exec::ir::iface::types::{FactSource, TypeKnowledge};
use crate::exec::ir::op_kind::{OpKind, OperandRule, Terminator};
use crate::exec::ir::phys::ops::PhysOp;
use crate::expr::Kind;

/// The type of the statement timestamp, which every statement region and every isolated region
/// receives as its first block argument.
pub(crate) fn timestamp_arg() -> TypeKnowledge {
	TypeKnowledge::Fact {
		kind: Kind::Datetime,
		source: FactSource::FunctionParam,
	}
}

/// The number of block arguments of the body of a loop with `sink`: the element and its position,
/// and the accumulator of a fold.
pub(crate) fn loop_body_args(sink: &LoopSink) -> usize {
	match sink {
		LoopSink::Fold => 3,
		LoopSink::Output
		| LoopSink::Filter
		| LoopSink::Decisive(_)
		| LoopSink::Discard
		| LoopSink::Write(_) => 2,
	}
}

/// Checks the rules of the logical op `op` that the generic rules do not cover.
///
/// An op of another dialect is accepted: its own dialect checks it.
pub(crate) fn verify_op(ir: &Ir, op: OpId) -> Result<(), VerifyError> {
	let Some(data) = ir.op(op) else {
		return Err(fail(op, VerifyRule::PerOp, "the op does not exist".into()));
	};
	let OpKind::Logical(log) = &data.kind else {
		return Ok(());
	};
	let Some(enclosing) = ir.region(ir.enclosing_region(op)).map(|region| region.kind) else {
		return Err(fail(op, VerifyRule::PerOp, "the region of the op does not exist".into()));
	};
	statement_structure(op, top_role(log), log.tag().mnemonic(), enclosing)?;
	if let LogOp::Txn {
		mode,
	} = log
	{
		transaction_ends(ir, op, data, *mode)?;
	}
	arity(op, data, log)?;
	write_body(ir, op, data, log)?;
	layer_computed(op, log)?;
	regions(ir, op, data, log)
}

/// Only a layer carries the computed fields of its input, or the computed fields its guards read:
/// a projection that is not one names neither.
fn layer_computed(op: OpId, log: &LogOp) -> Result<(), VerifyError> {
	if let LogOp::Project(attrs) = log
		&& !attrs.layer
		&& (!attrs.computed.is_empty() || attrs.judged.is_some())
	{
		return Err(fail(
			op,
			VerifyRule::PerOp,
			"a projection that is not a layer names computed fields".into(),
		));
	}
	Ok(())
}

/// A write's body must match the stage ops of its subject loop.
fn write_body(ir: &Ir, op: OpId, data: &OpData, log: &LogOp) -> Result<(), VerifyError> {
	if let LogOp::Events(attrs) = log {
		if attrs.events.iter().any(|event| event.dormant) {
			return Err(fail(
				op,
				VerifyRule::PerOp,
				"an event is dormant before the physical phase states it".into(),
			));
		}
		return match attrs.fault() {
			Some(detail) => Err(fail(op, VerifyRule::PerOp, detail.into())),
			None => Ok(()),
		};
	}
	let LogOp::Write(attrs) = log else {
		return Ok(());
	};
	let Some(run) = super::stages::write_run(ir, data) else {
		return Ok(());
	};
	if let Some(detail) = super::stages::body_fault(&attrs.body, run.len()) {
		return Err(fail(op, VerifyRule::PerOp, detail.into()));
	}
	if attrs.flags.where_never {
		let cut = matches!(attrs.form, WriteForm::Update | WriteForm::Delete)
			&& super::stages::after_cond(&attrs.body, &run).is_ok();
		if !cut {
			return Err(fail(
				op,
				VerifyRule::PerOp,
				"a write states its WHERE condition never satisfied, and is not an UPDATE or a DELETE \
				 of one order with a WHERE stage"
					.into(),
			));
		}
	}
	let hosts = super::stages::write_gate_hosts(ir, data).unwrap_or_default();
	if let Some(detail) = super::stages::gates_fault(attrs.gates, &hosts) {
		return Err(fail(op, VerifyRule::PerOp, detail.into()));
	}
	match super::stages::schema_fault(ir, data, &attrs.stored, attrs.image) {
		Some(detail) => Err(fail(op, VerifyRule::PerOp, detail.into())),
		None => Ok(()),
	}
}

fn fail(op: OpId, rule: VerifyRule, detail: String) -> VerifyError {
	VerifyError {
		op: Some(op),
		rule,
		detail,
	}
}

/// Whether `kind` is the region of a request or of a transaction.
fn is_top_list(kind: RegionKind) -> bool {
	matches!(kind, RegionKind::Request | RegionKind::Transaction(_))
}

/// The part an op plays among the top-level regions of a request, the same in the logical and the
/// physical dialect.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum TopRole {
	/// A transaction region.
	Txn,
	/// A statement of the request.
	Stmt,
	/// Any other op.
	Other,
}

fn top_role(log: &LogOp) -> TopRole {
	if let LogOp::Txn {
		..
	} = log
	{
		TopRole::Txn
	} else if let LogOp::Stmt {
		..
	} = log
	{
		TopRole::Stmt
	} else {
		TopRole::Other
	}
}

/// A transaction stands only in the request region. A statement stands in the request or in a
/// transaction. The two regions hold only transactions and statements. `name` is the mnemonic.
pub(crate) fn statement_structure(
	op: OpId,
	role: TopRole,
	name: &str,
	enclosing: RegionKind,
) -> Result<(), VerifyError> {
	let detail = match role {
		TopRole::Txn => (enclosing != RegionKind::Request)
			.then(|| format!("{name} stands in {enclosing:?}, not in the request region")),
		TopRole::Stmt => (!is_top_list(enclosing)).then(|| {
			format!("{name} stands in {enclosing:?}, not in the request or a transaction")
		}),
		TopRole::Other => is_top_list(enclosing)
			.then(|| format!("{name} stands in {enclosing:?}, which holds only statements")),
	};
	match detail {
		Some(detail) => Err(fail(op, VerifyRule::PerOp, detail)),
		None => Ok(()),
	}
}

/// Where a transaction of `mode` may begin and end among the top-level regions of its
/// fragment, given the ops that share its block.
pub(crate) fn transaction_ends(
	ir: &Ir,
	op: OpId,
	data: &OpData,
	mode: TxnRegionMode,
) -> Result<(), VerifyError> {
	let Some(block) = ir.block(data.parent) else {
		return Err(fail(op, VerifyRule::PerOp, "the block of the op does not exist".into()));
	};
	let ops = block.ops.as_slice();
	let broken = match mode {
		TxnRegionMode::Session {
			..
		} => (ops.len() != 1)
			.then_some("a session transaction is the only top-level region of its fragment"),
		TxnRegionMode::Explicit {
			start,
			end,
			..
		} => {
			let start_broken = match start {
				TxnStart::Begin => false,
				TxnStart::Continued => ops.first() != Some(&op),
			};
			if start_broken {
				Some("a continued transaction is the first top-level region of its fragment")
			} else {
				match end {
					TxnRegionEnd::Commit | TxnRegionEnd::Cancel => None,
					TxnRegionEnd::Open => (ops.last() != Some(&op)).then_some(
						"an open transaction is the last top-level region of its fragment",
					),
					TxnRegionEnd::Unterminated => unterminated_broken(ir, op, ops),
				}
			}
		}
	};
	match broken {
		Some(detail) => Err(fail(op, VerifyRule::PerOp, detail.into())),
		None => Ok(()),
	}
}

/// An `Unterminated` end needs a request that is one fragment, with no transaction after it.
fn unterminated_broken(ir: &Ir, op: OpId, ops: &[OpId]) -> Option<&'static str> {
	let whole_request = matches!(
		ir.scope(),
		IrScope::Request {
			fragment: None
		}
	);
	if !whole_request {
		return Some("an unterminated transaction ends a request that is one fragment");
	}
	let later_transaction =
		ops.iter().rev().take_while(|other| **other != op).filter_map(|other| ir.op(*other)).any(
			|other| {
				matches!(
					other.kind,
					OpKind::Logical(LogOp::Txn { .. }) | OpKind::Phys(PhysOp::Txn { .. })
				)
			},
		);
	later_transaction
		.then_some("an unterminated transaction is the last transaction of its request")
}

/// Whether `count` operands form groups whose sizes are among `sizes`.
fn grouped(sizes: &[u16], count: usize) -> bool {
	let mut reach = vec![false; count + 1];
	reach[0] = true;
	for at in 1..=count {
		reach[at] = sizes.iter().any(|size| {
			let size = usize::from(*size);
			size > 0 && size <= at && reach[at - size]
		});
	}
	reach[count]
}

/// The operand and result counts the op's arity states.
fn arity(op: OpId, data: &OpData, log: &LogOp) -> Result<(), VerifyError> {
	let arity = log.arity();
	let operands = data.operands.len();
	let accepted = match arity.operands {
		OperandRule::Exactly(count) => operands == usize::from(count),
		OperandRule::AtLeast(count) => operands >= usize::from(count),
		OperandRule::Grouped(sizes) => grouped(sizes, operands),
	};
	if !accepted {
		return Err(fail(
			op,
			VerifyRule::Arity,
			format!(
				"{} has {operands} operands, expected {:?}",
				log.tag().mnemonic(),
				arity.operands
			),
		));
	}
	if data.results.len() != usize::from(arity.results) {
		return Err(fail(
			op,
			VerifyRule::Arity,
			format!(
				"{} has {} results, expected {}",
				log.tag().mnemonic(),
				data.results.len(),
				arity.results
			),
		));
	}
	Ok(())
}

/// Whether `op`, standing in its enclosing region, is a statement of a statement list:
/// the region holds a statement list, and the only thing the op's results feed is the yield that
/// closes the op's own block. An op whose result is read by any other op is a value.
fn stands_as_statement(ir: &Ir, op: OpId, data: &OpData) -> bool {
	ir.holds_statement_list(ir.enclosing_region(op))
		&& data.results.iter().all(|result| {
			ir.uses(*result).iter().all(|used| {
				ir.op(used.user).is_some_and(|user| {
					user.parent == data.parent
						&& matches!(user.kind.terminator(), Some(Terminator::Yield))
				})
			})
		})
}

/// The kind that takes the place of `kind` when its op is a value and not a statement.
fn value_twin(kind: RegionKind) -> RegionKind {
	if kind == RegionKind::Block {
		RegionKind::ValueBlock
	} else if kind == RegionKind::StatementIf {
		RegionKind::ValueIf
	} else if kind == RegionKind::StatementFor {
		RegionKind::ValueFor
	} else {
		kind
	}
}

/// Whether the regions of `log` take the statement kind or its value twin by position.
fn has_position(log: &LogOp) -> bool {
	matches!(
		log,
		LogOp::Block
			| LogOp::If
			| LogOp::Loop {
				kind: LoopKind::For,
				..
			}
	)
}

/// The kinds `log` may own after the regions `region_kinds` names: the clause regions of the
/// statement it hosts. The regions of one kind stand together, any number of them, and the kinds
/// follow the order of the list.
///
/// The stage that evaluates a write's data clauses hosts them, the stage that evaluates its
/// condition hosts it, a permission gate hosts its predicate and then the computed fields the
/// predicate derives, and the stage that materialises the observed fields hosts the computed
/// fields of the table. The stage that applies the field schema hosts the clauses of the fields
/// by kind ([`WriteSchema::region_counts`](super::ops::WriteSchema::region_counts)), and the stage
/// that generates a record id hosts the `DEFAULT` of the `id` field, and the stage that keeps the
/// indexes in line hosts the columns and guards of the indexes that are maintained, the stage
/// that keeps the views in step hosts the clauses of the views that are maintained, and the stage
/// that purges a deleted record hosts the `THEN` expressions of the references it repairs. An
/// assignment or removal hosts the filters of its place. A live query holds the filter and the
/// fetch paths of its subscription after its projection, each when the statement has it.
pub(crate) fn tail_kinds(log: &LogOp) -> &'static [RegionKind] {
	if let LogOp::WriteStage {
		stage,
		..
	} = log
	{
		stage_tail(*stage)
	} else if let LogOp::Assign(..)
	| LogOp::Unset {
		..
	} = log
	{
		PLACE_TAIL
	} else if let LogOp::Live(..) = log {
		LIVE_TAIL
	} else {
		&[]
	}
}

/// The kinds the clause regions of a write stage may have, after the stage's own region: see
/// [`tail_kinds`].
pub(crate) fn stage_tail(stage: WriteStageKind) -> &'static [RegionKind] {
	const DATA: &[RegionKind] = &[
		RegionKind::Clause(ClauseRole::SetValue),
		RegionKind::Clause(ClauseRole::Content),
		RegionKind::Clause(ClauseRole::DuplicateUpdate),
	];
	const COND: &[RegionKind] = &[RegionKind::Clause(ClauseRole::WriteCond)];
	const GATE: &[RegionKind] =
		&[RegionKind::PermissionFrame, RegionKind::FieldFrame(FieldRole::Computed)];
	const OBSERVED: &[RegionKind] = &[RegionKind::FieldFrame(FieldRole::Computed)];
	const SCHEMA: &[RegionKind] = &[
		RegionKind::FieldFrame(FieldRole::Default),
		RegionKind::FieldFrame(FieldRole::Value),
		RegionKind::FieldFrame(FieldRole::Assert),
		RegionKind::PermissionFrame,
		RegionKind::FieldFrame(FieldRole::Computed),
	];
	const ID: &[RegionKind] = &[RegionKind::FieldFrame(FieldRole::Default)];
	const INDEXES: &[RegionKind] = &[RegionKind::Clause(ClauseRole::IndexColumn)];
	const VIEWS: &[RegionKind] = &[RegionKind::Clause(ClauseRole::ViewClause)];
	const REPAIRS: &[RegionKind] = &[RegionKind::Clause(ClauseRole::ReferenceRepair)];
	if stage == WriteStageKind::StoreIndexData {
		INDEXES
	} else if stage == WriteStageKind::PurgeRecordData {
		REPAIRS
	} else if matches!(stage, WriteStageKind::ProcessTableViews(_)) {
		VIEWS
	} else if stage == WriteStageKind::ComputeInputData {
		DATA
	} else if stage == WriteStageKind::ProcessTableFields {
		SCHEMA
	} else if stage == WriteStageKind::GenerateRecordId {
		ID
	} else if stage == WriteStageKind::CheckWhereCondition {
		COND
	} else if matches!(
		stage,
		WriteStageKind::CheckCreatePermissions
			| WriteStageKind::CheckUpdatePermissions
			| WriteStageKind::RecheckUpdatePermissions
			| WriteStageKind::CheckDeletePermissions
			| WriteStageKind::CheckSelectPermissions(_)
	) {
		GATE
	} else if stage == WriteStageKind::MaterialiseObservedFields {
		OBSERVED
	} else {
		&[]
	}
}

/// The kinds the filters of an assignment's or a removal's place may have.
pub(crate) const PLACE_TAIL: &[RegionKind] = &[RegionKind::Clause(ClauseRole::Where)];

/// The kinds the filter and the fetch paths of a live query's subscription may have.
pub(crate) const LIVE_TAIL: &[RegionKind] = &[
	RegionKind::StoredClause(StoredRole::LiveCond),
	RegionKind::StoredClause(StoredRole::LiveFetch),
];

/// Whether `actual` is `expected` followed by regions of the kinds of `tail`, each kind's regions
/// together and the kinds in the order of `tail`.
fn tail_matches(actual: &[RegionKind], expected: &[RegionKind], tail: &[RegionKind]) -> bool {
	let Some((head, rest)) = actual.split_at_checked(expected.len()) else {
		return false;
	};
	if head != expected {
		return false;
	}
	let mut next = 0;
	for kind in rest {
		let Some(offset) = tail.get(next..).and_then(|ahead| ahead.iter().position(|k| k == kind))
		else {
			return false;
		};
		next += offset;
	}
	true
}

/// The index in `log.region_kinds()` of the entry the attributes do not count, if the op has one:
/// that entry stands for any number of regions of its kind, none included.
pub(crate) fn open_position(log: &LogOp) -> Option<usize> {
	match log {
		LogOp::Split {
			..
		}
		| LogOp::Fetch
		| LogOp::Join {
			..
		}
		| LogOp::Match {
			..
		}
		| LogOp::MatchRead {
			..
		} => Some(0),
		LogOp::MatchOutput(shape) => Some(shape.columns.len()),
		LogOp::Access(attrs) => match attrs.op {
			AccessOp::Show {
				..
			}
			| AccessOp::Revoke {
				..
			} => Some(0),
			AccessOp::Grant {
				..
			}
			| AccessOp::Purge {
				..
			} => None,
		},
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
		| LogOp::Source(..)
		| LogOp::Aggregate(..)
		| LogOp::Values {
			..
		}
		| LogOp::MatchUpdate(..)
		| LogOp::MatchDelete {
			..
		}
		| LogOp::MatchInsert(..)
		| LogOp::BindStep(..)
		| LogOp::Subquery
		| LogOp::Write(..)
		| LogOp::Assign(..)
		| LogOp::Unset {
			..
		}
		| LogOp::Content {
			..
		}
		| LogOp::DdlGuard {
			..
		}
		| LogOp::Define(..)
		| LogOp::Remove(..)
		| LogOp::Alter(..)
		| LogOp::Rebuild(..)
		| LogOp::UnionAll
		| LogOp::Filter
		| LogOp::PermissionFilter {
			..
		}
		| LogOp::Distinct
		| LogOp::Sort(..)
		| LogOp::Window(..)
		| LogOp::Limit {
			..
		}
		| LogOp::Project(..)
		| LogOp::Timeout
		| LogOp::VersionScope
		| LogOp::Only
		| LogOp::Explain {
			..
		}
		| LogOp::Eval
		| LogOp::Block
		| LogOp::Let(..)
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
		| LogOp::Kill
		| LogOp::Live(..)
		| LogOp::Subscriber(..)
		| LogOp::Show(..)
		| LogOp::Choose(..)
		| LogOp::Events(..)
		| LogOp::WriteStage {
			..
		}
		| LogOp::Yield
		| LogOp::Next
		| LogOp::Break => None,
	}
}

/// Whether `actual` is `expected`, with the entry of `expected` at `open` standing for any number
/// of copies of itself.
fn kinds_match(actual: &[RegionKind], expected: &[RegionKind], open: Option<usize>) -> bool {
	let split = open.and_then(|at| expected.split_at_checked(at)).and_then(|(head, rest)| {
		let (repeated, tail) = rest.split_first()?;
		Some((head, repeated, tail))
	});
	let Some((head, repeated, tail)) = split else {
		return actual == expected;
	};
	let Some(middle) = actual.len().checked_sub(head.len() + tail.len()) else {
		return false;
	};
	let (front, rest) = actual.split_at(head.len());
	let (run, back) = rest.split_at(middle);
	front == head && back == tail && run.iter().all(|kind| kind == repeated)
}

/// The block arguments of the single block of `region`.
fn entry_args(ir: &Ir, region: RegionId) -> Option<&[ValueId]> {
	let block = *ir.region(region)?.blocks.first()?;
	ir.block(block).map(|block| block.args.as_slice())
}

/// Whether `value` is typed as the statement timestamp.
pub(crate) fn is_timestamp(ir: &Ir, value: ValueId) -> bool {
	matches!(
		ir.value(value).map(|value| &value.ty),
		Some(TypeKnowledge::Fact {
			kind: Kind::Datetime,
			..
		})
	)
}

/// The kind of every region of the op, in position order; `None` when one does not exist.
fn actual_kinds(ir: &Ir, data: &OpData) -> Option<RegionKinds> {
	data.regions.iter().map(|region| ir.region(*region).map(|region| region.kind)).collect()
}

/// The exact kind of each region by position and count, and the entry arguments each region
/// receives.
fn regions(ir: &Ir, op: OpId, data: &OpData, log: &LogOp) -> Result<(), VerifyError> {
	let name = log.tag().mnemonic();
	let Some(actual) = actual_kinds(ir, data) else {
		return Err(fail(
			op,
			VerifyRule::RegionKinds,
			format!("{name} owns a region that does not exist"),
		));
	};
	// An op with a position takes the value twin of each region kind when it is read as a value.
	// A statement may hold either kind: removing the last reader of a value leaves its regions
	// as they are, so the statement form is still accepted.
	let mut expected = log.region_kinds();
	if has_position(log) {
		let held_as_value = actual.first().is_some_and(|first| {
			expected.first().is_some_and(|kind| value_twin(*kind) == *first && *kind != *first)
		});
		if held_as_value || !stands_as_statement(ir, op, data) {
			for kind in &mut expected {
				*kind = value_twin(*kind);
			}
		}
	}
	if !kinds_match(&actual, &expected, open_position(log))
		&& !tail_matches(&actual, &expected, tail_kinds(log))
	{
		return Err(fail(
			op,
			VerifyRule::RegionKinds,
			format!("{name} has regions {actual:?}, expected {expected:?}"),
		));
	}
	for (region, kind) in data.regions.iter().zip(&actual) {
		let Some(args) = entry_args(ir, *region) else {
			return Err(fail(op, VerifyRule::PerOp, format!("a region of {name} holds no block")));
		};
		let has_timestamp = args.first().is_some_and(|first| is_timestamp(ir, *first));
		if let RegionKind::Statement(_) = kind {
			if args.len() != 1 || !has_timestamp {
				return Err(fail(
					op,
					VerifyRule::PerOp,
					format!(
						"the statement region of {name} has the timestamp as its only argument"
					),
				));
			}
		} else if kind.isolated_from_above() && !has_timestamp {
			return Err(fail(
				op,
				VerifyRule::PerOp,
				format!("an isolated region of {name} has the timestamp as its first argument"),
			));
		}
	}
	if let LogOp::Loop {
		sink,
		..
	} = log
	{
		let wanted = loop_body_args(sink);
		let body = data.regions.first().and_then(|region| entry_args(ir, *region));
		if body.map(<[ValueId]>::len) != Some(wanted) {
			return Err(fail(
				op,
				VerifyRule::PerOp,
				format!(
					"the body of {name} has {wanted} arguments: the element, its position and, for a fold, the accumulator"
				),
			));
		}
	}
	Ok(())
}
