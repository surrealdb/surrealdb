//! The verifier: the rules every container satisfies at a phase boundary and after a pass.
//!
//! Verification is O(ops + operands). It runs at every phase boundary in every build and after
//! every pass when `cfg!(debug_assertions)`; the rules do not depend on `debug_assertions`. A
//! failure is an internal error of the statement in a release build, and a panic in a debug
//! build.
//!
//! One pre-order walk from the top region checks every rule that is local to an op. The checks that
//! need the whole container, logical-id coverage and the legality snapshot at the physical
//! boundary, run once the walk is done. The verifier records the first violation of each rule, in
//! walk order, and answers the violation of the rule declared first in [`VerifyRule`], so the
//! answer does not depend on which check the walk reaches first.

use std::collections::HashMap;

use ::core::fmt;
use slotmap::{Key, SecondaryMap};
use smallvec::SmallVec;

use super::facts::FactCache;
use super::ir::{Ir, held_ids};
use super::location::LocOrigin;
use super::op::OpData;
use super::value::{TOKEN_SLOT_BASE, Use, ValueDef};
use super::walk::{ControlTarget, control_target};
use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::analysis::{effects, fallibility, tokens};
use crate::exec::ir::core::fxhash::{FxHashSet, set_with_capacity};
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::choose::{
	ArmBody, ChooseOrigin, ChooseSpec, GenericArm, Guard, MAX_CHOOSE_ARMS,
};
use crate::exec::ir::iface::conversion::{DialectSet, LegalityTarget, Phase};
use crate::exec::ir::iface::fallibility::Fallibility;
use crate::exec::ir::iface::frame::Snapshot;
use crate::exec::ir::iface::ids::{BlockId, FrameId, OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{
	FrameRule, RegionKind, RegionKinds, RegionProperties, ReturnRule,
};
use crate::exec::ir::logical::ops::LogOp;
use crate::exec::ir::op_kind::{OpKind, OperandRule, Terminator};
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::ir::{expr, logical, phys};
use crate::expr::logical::{EffectSet, EvalMode, GuardKind};

/// When a verification runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VerifyAt {
	/// After the named pass.
	AfterPass(&'static str),
	/// At the boundary that ends or begins the phase.
	PhaseBoundary(Phase),
}

/// The rule a verification failure breaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum VerifyRule {
	/// Every operand use appears in its value's use-list and the reverse.
	UseList,
	/// An operand is defined earlier in its block or in an enclosing region, never across a
	/// region that is isolated from above.
	Dominance,
	/// Exactly one terminator per block, last.
	Terminator,
	/// A terminator's target is the region the nesting rules compute for it.
	TerminatorTarget,
	/// An op's regions have the kinds its tag names, in position order.
	RegionKinds,
	/// A transaction region stands only under a request, and a statement region only under a
	/// request or a transaction.
	StatementStructure,
	/// Operand and result counts match the op's arity.
	Arity,
	/// Every op's frame id and snapshot id name an entry of the container's tables
	/// ([`Ir::op_outside_tables`] finds none), a version snapshot an op reads names a live
	/// value, and an op of a region that inherits its owner's
	/// frame runs under that frame, unless it opens a frame of the same principal that grants
	/// no more ([`opens_frame`]).
	FrameSnapshotIds,
	/// Every op's dialect is in the legality target, except an op inside a stored-clause region:
	/// stored code is never lowered.
	Legality,
	/// The transitive fallibility of an op covers what its effects imply.
	FallibilityConsistency,
	/// No op moved into or out of a region that changes the error leaving it.
	ErrorScope,
	/// A guarded choice satisfies its contract: arm count, generic arm, guard totality,
	/// equivalence and arm kinds.
	Choose,
	/// An op whose instruction or host service does not exist is absent.
	PhaseGated,
	/// At the physical boundary: every logical id is covered or eliminated, and every pipeline
	/// has a placement with a decision.
	Provenance,
	/// At the physical boundary: every loop of the legality snapshot taken at the logical
	/// boundary is still held by a live op or recorded as eliminated, and the decision recorded
	/// for each pipeline is one the snapshot's facts of the loop it stands for admit
	/// ([`phys::placement::admission_gap`]).
	LegalityRecompute,
	/// Every region admits the constructs it holds.
	RegionConstraints,
	/// No op of a stored-clause region changed after bind, and its digest equals the digest of
	/// the clause its definition template holds.
	StoredClause,
	/// In a container with effect tokens threaded through it, every op takes the tokens of the
	/// classes its effects consume and produces those it changes, and every region receives the
	/// token arguments its kind calls for ([`tokens::disagreement`]).
	TokenAgreement,
	/// A dialect's own per-op rules.
	PerOp,
}

/// A verification failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VerifyError {
	/// The op the rule failed at, when it fails at one.
	pub(crate) op: Option<OpId>,
	pub(crate) rule: VerifyRule,
	pub(crate) detail: String,
}

impl fmt::Display for VerifyError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:?}: {}", self.rule, self.detail)?;
		if let Some(op) = self.op {
			write!(f, " (at {op:?})")?;
		}
		Ok(())
	}
}

/// The number of [`VerifyRule`] variants.
const RULE_COUNT: usize = 19;

/// The position of `rule` in declaration order. Exhaustive, so a new rule fails to compile until
/// it is placed.
#[deny(clippy::wildcard_enum_match_arm)]
const fn rule_index(rule: VerifyRule) -> usize {
	match rule {
		VerifyRule::UseList => 0,
		VerifyRule::Dominance => 1,
		VerifyRule::Terminator => 2,
		VerifyRule::TerminatorTarget => 3,
		VerifyRule::RegionKinds => 4,
		VerifyRule::StatementStructure => 5,
		VerifyRule::Arity => 6,
		VerifyRule::FrameSnapshotIds => 7,
		VerifyRule::Legality => 8,
		VerifyRule::FallibilityConsistency => 9,
		VerifyRule::ErrorScope => 10,
		VerifyRule::Choose => 11,
		VerifyRule::PhaseGated => 12,
		VerifyRule::Provenance => 13,
		VerifyRule::LegalityRecompute => 14,
		VerifyRule::RegionConstraints => 15,
		VerifyRule::StoredClause => 16,
		VerifyRule::TokenAgreement => 17,
		VerifyRule::PerOp => 18,
	}
}

/// The region kind that stands in the same position as `kind` when the region is a value rather
/// than a statement, and the reverse. A declaration names one of the two, because it cannot see
/// which position the op stands in.
#[deny(clippy::wildcard_enum_match_arm)]
fn position_twin(kind: RegionKind) -> Option<RegionKind> {
	match kind {
		RegionKind::Block => Some(RegionKind::ValueBlock),
		RegionKind::ValueBlock => Some(RegionKind::Block),
		RegionKind::StatementIf => Some(RegionKind::ValueIf),
		RegionKind::ValueIf => Some(RegionKind::StatementIf),
		RegionKind::StatementFor => Some(RegionKind::ValueFor),
		RegionKind::ValueFor => Some(RegionKind::StatementFor),
		RegionKind::Request
		| RegionKind::Transaction(_)
		| RegionKind::Statement(_)
		| RegionKind::Loop
		| RegionKind::Subquery
		| RegionKind::FunctionBody
		| RegionKind::ClosureBody
		| RegionKind::PermissionFrame
		| RegionKind::FieldFrame(_)
		| RegionKind::EventFrame
		| RegionKind::SubscriberFrame
		| RegionKind::Clause(_)
		| RegionKind::LazyOperand
		| RegionKind::ErrorScope(_)
		| RegionKind::ChooseArm
		| RegionKind::Pipeline
		| RegionKind::JoinLookupArm
		| RegionKind::WriteStage
		| RegionKind::CatalogUnit
		| RegionKind::Deadline
		| RegionKind::StoredClause(_) => None,
	}
}

/// Whether a region of kind `actual` may stand where `declared` is named.
fn fills_position(declared: RegionKind, actual: RegionKind) -> bool {
	declared == actual || position_twin(declared) == Some(actual)
}

/// Whether regions of the kinds `actual`, in position order, fill a declaration: the kinds
/// `declared` names, in order, each filled by itself or its position twin, the entry at `open`
/// standing for any number of regions of its kind, none included; then regions of the kinds `tail`
/// names, each kind's regions together and the kinds in the order of `tail`.
fn region_shape_fits(
	actual: &[RegionKind],
	declared: &[RegionKind],
	open: Option<usize>,
	tail: &[RegionKind],
) -> bool {
	let fills = |wanted: &[RegionKind], held: &[RegionKind]| {
		wanted.len() == held.len()
			&& wanted.iter().zip(held).all(|(wanted, held)| fills_position(*wanted, *held))
	};
	let split = open.and_then(|at| declared.split_at_checked(at)).and_then(|(head, rest)| {
		let (repeated, after) = rest.split_first()?;
		Some((head, *repeated, after))
	});
	let (fixed, rest) = match split {
		Some((head, repeated, after)) => {
			let Some(run) = actual.len().checked_sub(head.len() + after.len()) else {
				return false;
			};
			let (front, back) = actual.split_at(head.len());
			let (middle, end) = back.split_at(run);
			return fills(head, front)
				&& fills(after, end)
				&& middle.iter().all(|held| fills_position(repeated, *held));
		}
		None => match actual.split_at_checked(declared.len()) {
			Some(parts) => parts,
			None => return false,
		},
	};
	if !fills(declared, fixed) {
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

/// Whether `count` operands split into groups, each of one of the sizes `sizes` names.
fn groups_fit(count: usize, sizes: &[u16]) -> bool {
	if count == 0 {
		return true;
	}
	if let [only] = sizes {
		return *only != 0 && count.is_multiple_of(usize::from(*only));
	}
	let mut reachable: SmallVec<[bool; 64]> = SmallVec::from_elem(false, count + 1);
	if let Some(start) = reachable.first_mut() {
		*start = true;
	}
	for at in 1..=count {
		let hit = sizes.iter().any(|size| {
			let size = usize::from(*size);
			size != 0 && size <= at && reachable.get(at - size).copied().unwrap_or(false)
		});
		if let Some(slot) = reachable.get_mut(at) {
			*slot = hit;
		}
	}
	reachable.last().copied().unwrap_or(false)
}

/// Whether two entries of `uses` name the same operand of the same op. A list longer than a
/// handful is checked through `scratch`, which is empty before and after the call.
fn has_duplicate_use(uses: &[Use], scratch: &mut FxHashSet<(OpId, u16)>) -> bool {
	if uses.len() <= 8 {
		return uses
			.iter()
			.enumerate()
			.any(|(at, held)| uses.iter().skip(at + 1).any(|o| o == held));
	}
	let duplicate = uses.iter().any(|held| !scratch.insert((held.user, held.operand)));
	for held in uses {
		scratch.remove(&(held.user, held.operand));
	}
	duplicate
}

/// The values visible at the walk's position: a set with an undo log, so that leaving a block
/// restores the set it had on entry.
#[derive(Default)]
struct Scope {
	visible: FxHashSet<ValueId>,
	/// The values added, in order. Each frame owns the entries from its recorded length on.
	log: Vec<ValueId>,
	/// Per open block: the log length at entry, and the set that was visible outside when the
	/// block's region is isolated from above.
	frames: Vec<(usize, Option<FxHashSet<ValueId>>)>,
}

impl Scope {
	/// Opens a block. An isolated block starts with nothing visible.
	fn enter(&mut self, isolated: bool) {
		let outer = isolated.then(|| ::core::mem::take(&mut self.visible));
		self.frames.push((self.log.len(), outer));
	}

	/// Closes the innermost block.
	fn exit(&mut self) {
		let Some((len, outer)) = self.frames.pop() else {
			return;
		};
		while self.log.len() > len {
			if let Some(value) = self.log.pop() {
				self.visible.remove(&value);
			}
		}
		if let Some(outer) = outer {
			self.visible = outer;
		}
	}

	fn insert(&mut self, value: ValueId) {
		if self.visible.insert(value) {
			self.log.push(value);
		}
	}

	fn contains(&self, value: ValueId) -> bool {
		self.visible.contains(&value)
	}
}

/// What the walk keeps of one enclosing region.
struct RegionCx {
	kind: RegionKind,
	/// The frame every op of the region carries, for a region that inherits its owner's, but an op
	/// that opens a frame of its own.
	inherited_frame: Option<FrameId>,
	/// Whether this region or an enclosing one is a transaction.
	in_transaction: bool,
	/// The owner of the innermost error-scope region that encloses the region, itself included;
	/// `None` outside every error scope. Computed as `Ir::error_scope_owner` computes it.
	error_scope: Option<OpId>,
	/// Whether this region or an enclosing one is a stored clause.
	in_stored_clause: bool,
}

/// How many operand slots read a value, and whether its use-list has been checked.
#[derive(Default)]
struct Tally {
	checked: bool,
	read: u32,
}

/// The state of one verification.
struct Walk<'a> {
	ir: &'a Ir,
	facts: &'a mut FactCache,
	cx: &'a EffectCx<'a>,
	target: &'a LegalityTarget,
	/// Whether this is the verification that ends the physical phase, which alone checks that
	/// conversion kept every logical id and placed every pipeline.
	physical_boundary: bool,
	/// The first violation of each rule, indexed by [`rule_index`].
	first: [Option<VerifyError>; RULE_COUNT],
	scope: Scope,
	regions: Vec<RegionCx>,
	tally: HashMap<ValueId, Tally>,
	/// Working space for the duplicate-use check; empty between values.
	scratch: FxHashSet<(OpId, u16)>,
	/// The ops the walk has reached. An op met a second time means an op or region is reachable
	/// twice; a live op missing from it after the walk is listed by no block.
	seen: SecondaryMap<OpId, ()>,
	stopped: bool,
	/// Whether the container holds no closure creation ([`effects::closure_free`]).
	closure_free: bool,
}

impl<'a> Walk<'a> {
	fn new(
		ir: &'a Ir,
		facts: &'a mut FactCache,
		at: VerifyAt,
		target: &'a LegalityTarget,
		cx: &'a EffectCx<'a>,
	) -> Self {
		Self {
			ir,
			facts,
			cx,
			target,
			physical_boundary: matches!(at, VerifyAt::PhaseBoundary(Phase::Physical)),
			first: ::core::array::from_fn(|_| None),
			scope: Scope {
				visible: set_with_capacity(ir.values.len()),
				log: Vec::with_capacity(ir.values.len()),
				frames: Vec::with_capacity(ir.blocks.len()),
			},
			regions: Vec::with_capacity(ir.regions.len()),
			tally: HashMap::with_capacity(ir.values.len()),
			scratch: FxHashSet::default(),
			seen: SecondaryMap::with_capacity(ir.ops.len()),
			stopped: false,
			closure_free: effects::closure_free(ir),
		}
	}

	/// Records a violation of `rule` unless the walk has recorded one already.
	fn fail(&mut self, rule: VerifyRule, op: Option<OpId>, detail: impl FnOnce() -> String) {
		if let Some(slot) = self.first.get_mut(rule_index(rule))
			&& slot.is_none()
		{
			*slot = Some(VerifyError {
				op,
				rule,
				detail: detail(),
			});
		}
	}

	/// Records a dialect's per-op error under the rule it names, at `op` when it names no op.
	fn file(&mut self, error: VerifyError, op: OpId) {
		self.fail(error.rule, error.op.or(Some(op)), || error.detail);
	}

	/// Checks the use-list of `value`, which `def` must define, against the ops that read it.
	fn check_uses(&mut self, value: ValueId, def: ValueDef, owner: Option<OpId>) {
		let ir = self.ir;
		let Some(data) = ir.value(value) else {
			self.fail(VerifyRule::UseList, owner, || format!("{value:?} names no value"));
			return;
		};
		if data.def != def {
			self.fail(VerifyRule::UseList, owner, || {
				format!("{value:?} is defined as {:?}, held as {def:?}", data.def)
			});
		}
		self.tally.entry(value).or_default().checked = true;
		for held in &data.uses {
			let reads = match held.token_slot() {
				Some(slot) => ir.token_operands(held.user).get(slot),
				None => {
					ir.op(held.user).and_then(|user| user.operands.get(usize::from(held.operand)))
				}
			}
			.is_some_and(|read| *read == value);
			if !reads {
				self.fail(VerifyRule::UseList, Some(held.user), || {
					format!(
						"{value:?} lists operand {} of an op that does not read it",
						held.operand
					)
				});
			}
		}
		if has_duplicate_use(&data.uses, &mut self.scratch) {
			self.fail(VerifyRule::UseList, owner, || format!("{value:?} lists a use twice"));
		}
	}

	/// Compares each checked value's use-list length with the operand slots that read it.
	fn finish_uses(&mut self) {
		let ir = self.ir;
		let mismatch = self.tally.iter().find_map(|(value, tally)| {
			let held = ir.uses(*value).len();
			(tally.checked && held != usize::try_from(tally.read).unwrap_or(usize::MAX))
				.then_some((*value, held, tally.read))
		});
		if let Some((value, held, read)) = mismatch {
			self.fail(VerifyRule::UseList, None, || {
				format!("{value:?} lists {held} uses and is read by {read} operands")
			});
		}
	}

	/// Walks `region`, owned by `owner`.
	fn region(&mut self, owner: Option<OpId>, region: RegionId) {
		if self.stopped {
			return;
		}
		let ir = self.ir;
		let Some(data) = ir.region(region) else {
			self.fail(VerifyRule::RegionKinds, owner, || format!("{region:?} names no region"));
			return;
		};
		if data.parent != owner {
			self.fail(VerifyRule::RegionKinds, owner, || {
				format!("{region:?} is owned by {:?}, held by {owner:?}", data.parent)
			});
		}
		if data.blocks.len() != 1 {
			self.fail(VerifyRule::RegionKinds, owner, || {
				format!("{region:?} holds {} blocks", data.blocks.len())
			});
		}
		let (enclosing, in_transaction) = match self.regions.last() {
			Some(cx) => (Some(cx.kind), cx.in_transaction),
			None => (None, false),
		};
		if enclosing.is_some() {
			match data.kind {
				RegionKind::Transaction(_) => {
					if !matches!(enclosing, Some(RegionKind::Request)) {
						self.fail(VerifyRule::StatementStructure, owner, || {
							format!("a transaction stands in {enclosing:?}, not a request")
						});
					} else if in_transaction {
						self.fail(VerifyRule::StatementStructure, owner, || {
							"a transaction stands inside a transaction".to_owned()
						});
					}
				}
				// A statement of the request stands in the request or a transaction. The statement
				// a block groups (`logical.statement`, `phys.statement`) stands in the block.
				RegionKind::Statement(_)
					if owner.and_then(|op| ir.op(op)).is_some_and(|held| {
						matches!(
							held.kind,
							OpKind::Logical(LogOp::Stmt { .. }) | OpKind::Phys(PhysOp::Stmt { .. })
						)
					}) && !matches!(
						enclosing,
						Some(RegionKind::Request | RegionKind::Transaction(_))
					) =>
				{
					self.fail(VerifyRule::StatementStructure, owner, || {
						format!("a statement stands in {enclosing:?}")
					});
				}
				RegionKind::Request
				| RegionKind::Statement(_)
				| RegionKind::Block
				| RegionKind::ValueBlock
				| RegionKind::StatementIf
				| RegionKind::ValueIf
				| RegionKind::StatementFor
				| RegionKind::ValueFor
				| RegionKind::Loop
				| RegionKind::Subquery
				| RegionKind::FunctionBody
				| RegionKind::ClosureBody
				| RegionKind::PermissionFrame
				| RegionKind::FieldFrame(_)
				| RegionKind::EventFrame
				| RegionKind::SubscriberFrame
				| RegionKind::Clause(_)
				| RegionKind::LazyOperand
				| RegionKind::ErrorScope(_)
				| RegionKind::ChooseArm
				| RegionKind::Pipeline
				| RegionKind::JoinLookupArm
				| RegionKind::WriteStage
				| RegionKind::CatalogUnit
				| RegionKind::Deadline
				| RegionKind::StoredClause(_) => {}
			}
		}
		let inherited_frame = match (data.kind.frame_rule(), owner.and_then(|op| ir.op(op))) {
			(FrameRule::Inherits, Some(held)) => Some(held.frame),
			_ => None,
		};
		let (outer_scope, in_stored_clause) = match self.regions.last() {
			Some(cx) => (cx.error_scope, cx.in_stored_clause),
			None => (None, false),
		};
		let error_scope = if let RegionKind::ErrorScope(_) = data.kind {
			data.parent
		} else {
			outer_scope
		};
		self.regions.push(RegionCx {
			kind: data.kind,
			inherited_frame,
			in_transaction: in_transaction || matches!(data.kind, RegionKind::Transaction(_)),
			error_scope,
			in_stored_clause: in_stored_clause || matches!(data.kind, RegionKind::StoredClause(_)),
		});
		for block in &data.blocks {
			self.block(*block, region);
		}
		self.regions.pop();
	}

	/// Walks `block`, which stands in `region`.
	fn block(&mut self, block: BlockId, region: RegionId) {
		let ir = self.ir;
		let Some(data) = ir.block(block) else {
			self.fail(VerifyRule::RegionKinds, None, || format!("{block:?} names no block"));
			return;
		};
		let Some(held) = ir.region(region) else {
			return;
		};
		if data.parent != region {
			self.fail(VerifyRule::RegionKinds, None, || {
				format!("{block:?} is held by {:?}, listed by {region:?}", data.parent)
			});
		}
		self.scope.enter(held.kind.isolated_from_above());
		for (index, arg) in data.args.iter().enumerate() {
			let index = u16::try_from(index).unwrap_or(u16::MAX);
			let owner = held.parent;
			self.check_uses(
				*arg,
				ValueDef::BlockArg {
					block,
					index,
				},
				owner,
			);
			self.scope.insert(*arg);
		}
		for (slot, arg) in data.token_args.iter().enumerate() {
			let index = TOKEN_SLOT_BASE.saturating_add(u16::try_from(slot).unwrap_or(u16::MAX));
			self.check_uses(
				*arg,
				ValueDef::BlockArg {
					block,
					index,
				},
				held.parent,
			);
			self.scope.insert(*arg);
		}
		let mut terminators = 0usize;
		let mut ends_with_terminator = false;
		for op in &data.ops {
			ends_with_terminator = self.op(*op, block);
			if ends_with_terminator {
				terminators += 1;
			}
		}
		if self.stopped {
			self.scope.exit();
			return;
		}
		let owner = held.parent;
		if held.kind.return_rule() == ReturnRule::NeverReached {
			if terminators > 1 || (terminators == 1 && !ends_with_terminator) {
				self.fail(VerifyRule::Terminator, owner, || {
					format!(
						"{:?} holds {terminators} terminators, or one that is not last",
						held.kind
					)
				});
			}
		} else if terminators != 1 || !ends_with_terminator {
			self.fail(VerifyRule::Terminator, owner, || {
				format!(
					"{:?} holds {terminators} terminators; exactly one, last, is required",
					held.kind
				)
			});
		}
		self.scope.exit();
	}

	/// Checks `id` and walks its regions; answers whether it is a terminator.
	fn op(&mut self, id: OpId, block: BlockId) -> bool {
		if self.stopped {
			return false;
		}
		let ir = self.ir;
		let Some(data) = ir.op(id) else {
			self.fail(VerifyRule::RegionKinds, None, || {
				format!("{block:?} lists {id:?}, a dead op")
			});
			return false;
		};
		if self.seen.insert(id, ()).is_some() {
			self.fail(VerifyRule::RegionKinds, Some(id), || {
				"an op or region is reachable twice".to_owned()
			});
			self.stopped = true;
			return false;
		}
		if data.parent != block {
			self.fail(VerifyRule::RegionKinds, Some(id), || {
				format!("the op is held by {:?}, listed by {block:?}", data.parent)
			});
		}

		for (index, value) in data.operands.iter().enumerate() {
			if ir.value(*value).is_none() {
				self.fail(VerifyRule::UseList, Some(id), || {
					format!("operand {index} names no value")
				});
				continue;
			}
			self.tally.entry(*value).or_default().read += 1;
			if !self.scope.contains(*value) {
				self.fail(VerifyRule::Dominance, Some(id), || {
					format!("operand {index}, {value:?}, is not defined before the op")
				});
			}
		}

		for (slot, value) in ir.token_operands(id).iter().enumerate() {
			if ir.value(*value).is_none() {
				self.fail(VerifyRule::UseList, Some(id), || {
					format!("token operand {slot} names no value")
				});
				continue;
			}
			self.tally.entry(*value).or_default().read += 1;
			if !self.scope.contains(*value) {
				self.fail(VerifyRule::Dominance, Some(id), || {
					format!("token operand {slot}, {value:?}, is not defined before the op")
				});
			}
		}

		let is_terminator = data.kind.is_terminator();
		self.arity(id, data.operands.len(), data.results.len(), &data.kind);

		let derived = control_target(ir, id);
		match derived {
			ControlTarget::NotControl => {}
			ControlTarget::Region(landing) => {
				if let Some(
					Terminator::Return {
						target,
					}
					| Terminator::Break {
						target,
					}
					| Terminator::Next {
						target,
					},
				) = data.kind.terminator()
					&& !target.is_null()
					&& target != landing
				{
					self.fail(VerifyRule::TerminatorTarget, Some(id), || {
						format!("the op names {target:?}, its nesting gives {landing:?}")
					});
				}
			}
			ControlTarget::Escapes(region) => {
				self.fail(VerifyRule::TerminatorTarget, Some(id), || {
					format!("a return reaches {region:?}, which no return reaches")
				})
			}
			ControlTarget::Barrier(region) => {
				self.fail(VerifyRule::TerminatorTarget, Some(id), || {
					format!("a loop exit meets {region:?}, which code of another body stands in")
				})
			}
		}

		let dialect = match data.kind {
			OpKind::Expr(_) => DialectSet::EXPR,
			OpKind::Logical(_) => DialectSet::LOGICAL,
			OpKind::Phys(_) => DialectSet::PHYS,
		};
		// Stored code is bound, verified and analysed, never lowered, so its dialect stays at
		// bind level whatever the phase requires.
		let stored = self.regions.last().is_some_and(|cx| cx.in_stored_clause);
		if !stored && !self.target.legal.contains(dialect) {
			let (name, target) = (data.kind.tag().mnemonic(), self.target.name);
			self.fail(VerifyRule::Legality, Some(id), || {
				format!("{name} is not legal in the {target} phase")
			});
		}

		if ir.frame(data.frame).is_none() || ir.snapshot(data.snapshot).is_none() {
			self.fail(VerifyRule::FrameSnapshotIds, Some(id), || {
				format!(
					"frame {:?} or snapshot {:?} is outside the tables",
					data.frame, data.snapshot
				)
			});
		} else if let Some(Snapshot::Version(version)) = ir.snapshot(data.snapshot)
			&& ir.value(*version).is_none()
		{
			self.fail(VerifyRule::FrameSnapshotIds, Some(id), || {
				format!(
					"snapshot {:?} names {version:?} as its version, which is not a live value",
					data.snapshot
				)
			});
		}
		if let Some(expected) = self.regions.last().and_then(|cx| cx.inherited_frame)
			&& data.frame != expected
			&& !matches!(data.loc.origin, LocOrigin::Catalog(_))
			&& !opens_frame(ir, data, expected)
		{
			self.fail(VerifyRule::FrameSnapshotIds, Some(id), || {
				format!(
					"the op runs under {:?}, in a region that inherits {expected:?}",
					data.frame
				)
			});
		}

		self.semantics(id, data);

		let result = match data.kind {
			OpKind::Expr(_) => expr::verify::verify_op(ir, id, self.cx),
			OpKind::Logical(_) => logical::verify::verify_op(ir, id),
			OpKind::Phys(_) => phys::verify::verify_op(ir, id),
		};
		if let Err(error) = result {
			self.file(error, id);
		}

		self.region_kinds(id, &data.kind, &data.regions);
		for region in &data.regions {
			self.region(Some(id), *region);
		}

		for (index, value) in data.results.iter().enumerate() {
			let index = u16::try_from(index).unwrap_or(u16::MAX);
			self.check_uses(
				*value,
				ValueDef::OpResult {
					op: id,
					index,
				},
				Some(id),
			);
			self.scope.insert(*value);
		}
		for (slot, value) in ir.token_results(id).iter().enumerate() {
			let index = TOKEN_SLOT_BASE.saturating_add(u16::try_from(slot).unwrap_or(u16::MAX));
			self.check_uses(
				*value,
				ValueDef::OpResult {
					op: id,
					index,
				},
				Some(id),
			);
			self.scope.insert(*value);
		}
		is_terminator
	}

	/// Checks the rules of `id` that read its facts, its provenance, its place or its attributes:
	/// fallibility consistency, error scope, `Choose`, phase gating, pipeline placement and the
	/// stored-clause seal.
	fn semantics(&mut self, id: OpId, data: &OpData) {
		let ir = self.ir;

		let under = structural_cx(self.cx.registry, ir);
		let answer = if self.closure_free {
			effects::summary_closure_free(ir, self.facts, &under, id)
		} else {
			effects::summary_with(ir, self.facts, &under, id)
		};
		let set = answer.set;
		let failures = answer.fallibility.union(fallibility::floor(set));
		if let Some(gap) = fallibility_gap(set, failures) {
			self.fail(VerifyRule::FallibilityConsistency, Some(id), || {
				format!("{gap}: the effects are {set:?} and the fallibility {failures:?}")
			});
		}

		let current = self.regions.last().and_then(|cx| cx.error_scope);
		if let Some(detail) = error_scope_gap(ir, id, data, current) {
			self.fail(VerifyRule::ErrorScope, Some(id), || detail);
		}

		match &data.kind {
			OpKind::Logical(LogOp::Choose(spec)) | OpKind::Phys(PhysOp::Choose(spec)) => {
				if let Some(detail) = choose_gap(ir, spec, &data.regions) {
					self.fail(VerifyRule::Choose, Some(id), || detail);
				}
			}
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {}
		}

		// The phase gates are the physical dialect's own rule, which reads whether each gated
		// op's instruction exists ([`phys::verify::verify_op`]).
		if let OpKind::Phys(op) = &data.kind
			&& self.physical_boundary
			&& matches!(op, PhysOp::Pipeline(_))
			&& ir.placement(id).and_then(|placed| placed.decision()).is_none()
		{
			self.fail(VerifyRule::Provenance, Some(id), || {
				"a pipeline reaches the physical boundary with no placement decision".to_owned()
			});
		}

		if let OpKind::Phys(PhysOp::Pipeline(attrs)) = &data.kind
			&& self.physical_boundary
			&& let Some(placed) = ir.placement(id)
			&& let Some(loop_facts) =
				attrs.logical.and_then(|logical| self.facts.legality_snapshot()?.get(&logical))
			&& let Some(gap) = phys::placement::admission_gap(placed, loop_facts)
		{
			self.fail(VerifyRule::LegalityRecompute, Some(id), || gap);
		}

		if self.regions.last().is_some_and(|cx| cx.in_stored_clause) {
			let now = ir.generation(id);
			match ir.sealed.get(id).copied() {
				Some(sealed) if sealed != now => {
					self.fail(VerifyRule::StoredClause, Some(id), || {
						format!(
							"a stored-clause op changed after bind: sealed at {sealed}, now {now}"
						)
					});
				}
				Some(_) => {}
				None if ir.phase() > Phase::Bound => {
					self.fail(VerifyRule::StoredClause, Some(id), || {
						"a stored-clause op was created after bind".to_owned()
					});
				}
				None => {}
			}
		}
	}

	/// Checks the operand and result counts of `id` against `kind`'s arity.
	fn arity(&mut self, id: OpId, operands: usize, results: usize, kind: &OpKind) {
		let arity = kind.arity();
		let fits = match arity.operands {
			OperandRule::Exactly(count) => operands == usize::from(count),
			OperandRule::AtLeast(count) => operands >= usize::from(count),
			OperandRule::Grouped(sizes) => groups_fit(operands, sizes),
		};
		if !fits {
			self.fail(VerifyRule::Arity, Some(id), || {
				format!("{operands} operands do not satisfy {:?}", arity.operands)
			});
		}
		if results != usize::from(arity.results) {
			self.fail(VerifyRule::Arity, Some(id), || {
				format!("{results} results, arity says {}", arity.results)
			});
		}
	}

	/// Checks the kinds and the count of the regions `id` owns against the kinds its tag declares
	/// ([`region_shape_fits`]), each of exactly its declared kind where the op's attributes state
	/// the kind ([`OpKind::region_kinds_exact`]). A region that does not exist is reported by the
	/// walk of the region.
	fn region_kinds(&mut self, id: OpId, kind: &OpKind, regions: &[RegionId]) {
		let ir = self.ir;
		let Some(actual) = regions
			.iter()
			.map(|region| ir.region(*region).map(|held| held.kind))
			.collect::<Option<RegionKinds>>()
		else {
			return;
		};
		let declared = kind.region_kinds();
		let open = kind.open_region_position();
		let exact = !kind.region_kinds_exact()
			|| actual.iter().zip(declared.iter()).all(|(actual, declared)| actual == declared);
		if !exact || !region_shape_fits(&actual, &declared, open, kind.tail_region_kinds()) {
			self.fail(VerifyRule::RegionKinds, Some(id), || {
				format!(
					"the op holds regions {actual:?}, which do not fill its declaration {declared:?}"
				)
			});
		}
	}

	/// Checks that a container the region constraints judge ([`Ir::judges_constraints`]) still
	/// admits, in every region, the constructs it holds by their tags. The binder refuses a
	/// container that does not, so a construct found here is one a pass moved or built into a
	/// region that does not admit it. A call's effects are not judged.
	fn finish_region_constraints(&mut self) {
		let ir = self.ir;
		if !ir.judges_constraints() {
			return;
		}
		match logical::constraints::rejection_in(ir, ir.top()) {
			Ok(Some(rejection)) => {
				self.fail(VerifyRule::RegionConstraints, Some(rejection.op), || {
					format!("{:?} does not admit {:?}", rejection.region, rejection.construct)
				});
			}
			Ok(None) => {}
			Err(_) => {
				self.fail(VerifyRule::RegionConstraints, None, || {
					"the region constraints cannot walk the container".to_owned()
				});
			}
		}
	}

	/// Checks, in a container with tokens threaded through it, that every op takes and produces the
	/// tokens its effects call for.
	fn finish_tokens(&mut self) {
		let ir = self.ir;
		if !ir.is_threaded() {
			return;
		}
		if let Some((op, detail)) = tokens::disagreement(ir, self.facts, self.cx) {
			self.fail(VerifyRule::TokenAgreement, op, || detail);
		}
	}

	/// Checks that every stored-clause region sealed when the container left [`Phase::Bound`]
	/// is still there and holds the number of ops it held: an op removed from, or moved out of,
	/// stored code changes no seal of the ops that remain.
	fn finish_stored_regions(&mut self) {
		let ir = self.ir;
		for (region, sealed) in ir.sealed_regions.iter() {
			let Some(data) = ir.region(region) else {
				self.fail(VerifyRule::StoredClause, None, || {
					"a sealed stored-clause region left its owner".to_owned()
				});
				continue;
			};
			let now = ir.region_op_count(region);
			if now != *sealed {
				self.fail(VerifyRule::StoredClause, data.parent, || {
					format!(
						"a stored-clause region changed after bind: sealed with {sealed} ops, now {now}"
					)
				});
			}
		}
	}

	/// The checks that need the whole container, at the verification that ends the physical
	/// phase: every logical id is held by a live op or recorded as eliminated, and every loop of
	/// the legality snapshot is still covered.
	fn finish_physical_boundary(&mut self) {
		if !self.physical_boundary {
			return;
		}
		let ir = self.ir;
		let eliminated: FxHashSet<OpId> = ir.eliminated().iter().map(|(id, _)| *id).collect();
		let covered = |id: OpId| {
			ir.holders.get(&id).is_some_and(|held| *held > 0) || eliminated.contains(&id)
		};
		if let Some(id) = ir.logical_ids.iter().copied().find(|id| !covered(*id)) {
			self.fail(VerifyRule::Provenance, Some(id), || {
				"a logical id is neither held by a live op nor recorded as eliminated".to_owned()
			});
		}
		let uncovered = self
			.facts
			.legality_snapshot()
			.and_then(|snapshot| snapshot.keys().copied().find(|id| !covered(*id)));
		if let Some(id) = uncovered {
			self.fail(VerifyRule::LegalityRecompute, Some(id), || {
				"a loop of the legality snapshot is neither held nor eliminated".to_owned()
			});
		}
	}

	/// The violation of the rule declared first, once the walk is done.
	fn into_first(self) -> Option<VerifyError> {
		self.first.into_iter().flatten().next()
	}
}

/// What `set` implies about failure that `fallibility` does not cover, if anything. A top answer
/// covers everything.
fn fallibility_gap(set: EffectSet, fallibility: Fallibility) -> Option<&'static str> {
	let context_and_data = Fallibility::CONTEXT.union(Fallibility::DATA);
	if set.intersects(EffectSet::READS_DATA) && !fallibility.contains(Fallibility::DATA) {
		return Some("an op that reads data cannot fail on data");
	}
	if set.intersects(EffectSet::MAY_REJECT) && !fallibility.contains(Fallibility::DATA) {
		return Some("an op that may reject cannot fail on data");
	}
	if set.intersects(EffectSet::OPAQUE_BODY) && !fallibility.contains(context_and_data) {
		return Some("an op with an opaque body cannot fail on context and data");
	}
	let external = EffectSet::READS_EXTERNAL.union(EffectSet::WRITES_EXTERNAL);
	if set.intersects(external) && !fallibility.contains(context_and_data) {
		return Some("an op that reaches outside cannot fail on context and data");
	}
	None
}

/// Whether the error scope `op` was created in, or each scope its provenance ids were created
/// in, is the one it stands in now. `current` is the owner of the innermost error-scope region
/// that encloses `op`.
///
/// The scope is named by its owner op, so a region recreated under an op that stands for the
/// owner still matches: the owner is `current` itself or an op `current` stands for. An id with
/// no record is skipped.
fn error_scope_gap(ir: &Ir, id: OpId, data: &OpData, current: Option<OpId>) -> Option<String> {
	for held in held_ids(&data.provenance, id) {
		let recorded = match ir.bind_scope.get(held).or_else(|| ir.retired_scope.get(&held)) {
			Some(recorded) => *recorded,
			None => continue,
		};
		let fits = match (recorded, current) {
			(None, None) => true,
			(Some(owner), Some(scope)) => {
				scope == owner
					|| ir.op(scope).is_some_and(|op| {
						held_ids(&op.provenance, scope).any(|stands_for| stands_for == owner)
					})
			}
			(None, Some(_)) | (Some(_), None) => false,
		};
		if !fits {
			return Some(format!(
				"the op was bound in the error scope of {recorded:?} and stands in that of \
				 {current:?}"
			));
		}
	}
	None
}

/// What `spec` breaks of the `Choose` contract, given the `regions` the op owns: the contract of
/// every choice, logical or physical. The physical dialect adds the equivalence of arms that read
/// different access paths.
pub(crate) fn choose_gap(ir: &Ir, spec: &ChooseSpec, regions: &[RegionId]) -> Option<String> {
	let arms = spec.arms.len();
	if arms == 0 || arms > MAX_CHOOSE_ARMS {
		return Some(format!("{arms} arms; a choice holds 1 to {MAX_CHOOSE_ARMS}"));
	}
	if arms != regions.len() {
		return Some(format!("{arms} arms and {} regions", regions.len()));
	}
	if regions
		.iter()
		.any(|region| ir.region(*region).is_none_or(|held| held.kind != RegionKind::ChooseArm))
	{
		return Some("an arm is not a ChooseArm region".to_owned());
	}
	let (last, earlier) = spec.arms.split_last()?;
	if earlier.iter().any(|arm| matches!(arm.guard, Guard::Otherwise)) {
		return Some("an arm other than the last is guarded by Otherwise".to_owned());
	}
	if matches!(spec.generic, GenericArm::Present) && !matches!(last.guard, Guard::Otherwise) {
		return Some(
			"a generic arm is declared and the last arm is not guarded by Otherwise".to_owned(),
		);
	}
	if spec.origin != ChooseOrigin::LogicalDynamicTarget
		&& spec.arms.iter().any(|arm| matches!(arm.body, ArmBody::UnitLoad(_)))
	{
		return Some("a unit-load arm outside a dynamic-target choice".to_owned());
	}
	None
}

/// Walks `ir` and checks every rule, at `at`, with `target` as the legality the phase requires.
fn run<'a>(
	ir: &'a Ir,
	facts: &'a mut FactCache,
	at: VerifyAt,
	target: &'a LegalityTarget,
	cx: &'a EffectCx<'a>,
) -> Walk<'a> {
	let mut walk = Walk::new(ir, facts, at, target, cx);
	walk.region(None, ir.top());
	if walk.stopped {
		return walk;
	}
	walk.finish_uses();
	walk.finish_tokens();
	walk.finish_stored_regions();
	walk.finish_physical_boundary();
	walk.finish_region_constraints();
	// A live op no block lists was never reached by the walk. Its frame and snapshot ids are
	// still checked here.
	if walk.seen.len() != ir.ops.len() {
		if let Some(op) = ir.ops.keys().find(|op| !walk.seen.contains_key(*op)) {
			walk.fail(VerifyRule::RegionKinds, Some(op), || {
				"a live op is listed by no block".to_owned()
			});
		}
		if let Some(op) = ir.op_outside_tables() {
			walk.fail(VerifyRule::FrameSnapshotIds, Some(op), || {
				"the op names a frame or snapshot outside the tables".to_owned()
			});
		}
	}
	walk
}

/// Whether `data` opens a frame of its own in a region that inherits `inherited`: a permission
/// filter or a join, which the plan binds under the frame its node names, with the regions it
/// judges in that frame. The frame it opens holds the principal of `inherited` and differs only in
/// how it evaluates, and never grants more than `inherited` does:
///
/// - its unit frame (principal class, names, suppression and permission contract) is the same;
/// - its `AUTH LIMIT` is the inherited one or within it, and present wherever the inherited one is;
/// - it refuses writes wherever the inherited frame does;
/// - a permission filter evaluates in the frame of its guard, and a join in the inherited mode or
///   as a read permission predicate;
/// - it is no record-id column frame.
fn opens_frame(ir: &Ir, data: &OpData, inherited: FrameId) -> bool {
	let guard = match &data.kind {
		OpKind::Logical(LogOp::PermissionFilter {
			guard,
			..
		}) => Some(*guard),
		OpKind::Phys(PhysOp::PermissionFilter(attrs)) => Some(attrs.guard),
		OpKind::Logical(LogOp::Join {
			..
		})
		| OpKind::Phys(PhysOp::Join(_)) => None,
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => return false,
	};
	let (Some(own), Some(outer)) = (ir.frame(data.frame), ir.frame(inherited)) else {
		return false;
	};
	let (eval, held) = (&own.eval, &outer.eval);
	let mode = match guard {
		Some(GuardKind::Read) => eval.mode == EvalMode::Permission,
		Some(GuardKind::Write) => eval.mode == EvalMode::MutablePermission,
		None => eval.mode == held.mode || eval.mode == EvalMode::Permission,
	};
	let auth = match (&eval.auth, &held.auth) {
		(_, None) => true,
		(None, Some(_)) => false,
		(Some(limit), Some(cap)) => limit.is_within(cap),
	};
	own.unit == outer.unit
		&& auth
		&& mode
		&& (eval.mode.forbids_writes() || !held.mode.forbids_writes())
		&& !eval.key_part
}

/// The violation of `rule` that a verification of `ir` records first, whatever rules rank lower.
#[cfg(test)]
pub(super) fn violation_of(
	ir: &Ir,
	facts: &mut FactCache,
	at: VerifyAt,
	target: &LegalityTarget,
	rule: VerifyRule,
) -> Option<VerifyError> {
	let cx = crate::exec::ir::analysis::cx::default_cx(ir);
	let mut walk = run(ir, facts, at, target, &cx);
	walk.first.get_mut(rule_index(rule)).and_then(Option::take)
}

/// Checks `ir` against every rule, at `at`, with `target` as the legality the phase requires.
///
/// The walk checks, in one pass over the ops: use-lists, dominance, terminators and their
/// targets, region kinds, statement and transaction structure, arity, frame and snapshot ids,
/// legality, fallibility consistency, error scopes, `Choose`, phase gates, pipeline placement,
/// stored-clause seals (of ops, and of the op count of each stored region) and each dialect's
/// own per-op rules. At the physical boundary it also checks that every logical id is held or
/// eliminated, that every loop of the legality snapshot is covered and that each pipeline's
/// decision is one its loop's snapshot facts admit. In a container the region
/// constraints judge, every region admits the constructs it holds by their tags
/// (`RegionConstraints`).
///
/// Answers the first violation of the rule that [`VerifyRule`] declares first, and `Ok` when no
/// rule is violated. A container that cannot be walked, such as one that reaches an op twice, is
/// reported and never passed, and so is one that holds a live op no block lists.
///
/// `cx` is the effect context the caller analyses with. The agreement of the tokens of a threaded
/// container with its effects is judged under it. The effects and fallibility the other rules read
/// are judged under the registry of `cx` and no definitions, as are the callees of calls.
pub(crate) fn verify(
	ir: &Ir,
	facts: &mut FactCache,
	at: VerifyAt,
	target: &LegalityTarget,
	cx: &EffectCx<'_>,
) -> Result<(), VerifyError> {
	match run(ir, facts, at, target, cx).into_first() {
		Some(error) => Err(error),
		None => Ok(()),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::iface::conversion::LOGICAL_PHASE;
	use crate::exec::ir::iface::region_kind::{ClauseRole, StatementCommit};

	const ALL: [VerifyRule; RULE_COUNT] = [
		VerifyRule::UseList,
		VerifyRule::Dominance,
		VerifyRule::Terminator,
		VerifyRule::TerminatorTarget,
		VerifyRule::RegionKinds,
		VerifyRule::StatementStructure,
		VerifyRule::Arity,
		VerifyRule::FrameSnapshotIds,
		VerifyRule::Legality,
		VerifyRule::FallibilityConsistency,
		VerifyRule::ErrorScope,
		VerifyRule::Choose,
		VerifyRule::PhaseGated,
		VerifyRule::Provenance,
		VerifyRule::LegalityRecompute,
		VerifyRule::RegionConstraints,
		VerifyRule::StoredClause,
		VerifyRule::TokenAgreement,
		VerifyRule::PerOp,
	];

	#[test]
	fn rule_index_follows_declaration_order() {
		for (at, rule) in ALL.iter().enumerate() {
			assert_eq!(rule_index(*rule), at);
		}
	}

	#[test]
	fn the_lowest_rule_wins_whatever_order_the_walk_records_them_in() {
		let ir = Ir::new(
			super::super::ir::IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::new();
		let cx = crate::exec::ir::analysis::cx::default_cx(&ir);
		let mut walk = Walk::new(&ir, &mut facts, VerifyAt::AfterPass("test"), &LOGICAL_PHASE, &cx);
		for rule in [VerifyRule::PerOp, VerifyRule::Arity, VerifyRule::Terminator] {
			walk.fail(rule, None, || format!("{rule:?}"));
		}
		walk.fail(VerifyRule::Terminator, None, || "later".to_owned());
		let first = walk.into_first().expect("a violation was recorded");
		assert_eq!(first.rule, VerifyRule::Terminator);
		assert_eq!(first.detail, "Terminator", "the first violation of a rule is kept");
	}

	#[test]
	fn a_dialect_error_is_filed_under_the_rule_it_names() {
		let ir = Ir::new(
			super::super::ir::IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::new();
		let cx = crate::exec::ir::analysis::cx::default_cx(&ir);
		let mut walk = Walk::new(&ir, &mut facts, VerifyAt::AfterPass("test"), &LOGICAL_PHASE, &cx);
		let at = OpId::default();
		walk.file(
			VerifyError {
				op: None,
				rule: VerifyRule::Choose,
				detail: "arms".to_owned(),
			},
			at,
		);
		walk.fail(VerifyRule::PerOp, None, || "own rule".to_owned());
		let first = walk.into_first().expect("a violation was recorded");
		assert_eq!(first.rule, VerifyRule::Choose);
		assert_eq!(first.op, Some(at));
	}

	#[test]
	fn region_constraints_are_filed_through_the_per_op_errors_that_name_them() {
		let ir = Ir::new(
			super::super::ir::IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::new();
		let cx = crate::exec::ir::analysis::cx::default_cx(&ir);
		let mut walk = Walk::new(&ir, &mut facts, VerifyAt::AfterPass("test"), &LOGICAL_PHASE, &cx);
		walk.file(
			VerifyError {
				op: None,
				rule: VerifyRule::RegionConstraints,
				detail: "a clause".to_owned(),
			},
			OpId::default(),
		);
		let first = walk.into_first().expect("a violation was recorded");
		assert_eq!(first.rule, VerifyRule::RegionConstraints);
	}

	#[test]
	fn a_fallibility_must_cover_what_the_effects_imply() {
		let none = Fallibility::INFALLIBLE;
		let both = Fallibility::CONTEXT | Fallibility::DATA;
		assert_eq!(fallibility_gap(EffectSet::default(), none), None);
		assert!(fallibility_gap(EffectSet::READS_DATA, none).is_some());
		assert!(fallibility_gap(EffectSet::READS_DATA, Fallibility::CONTEXT).is_some());
		assert_eq!(fallibility_gap(EffectSet::READS_DATA, Fallibility::DATA), None);
		assert!(fallibility_gap(EffectSet::MAY_REJECT, Fallibility::VALUE).is_some());
		assert_eq!(fallibility_gap(EffectSet::MAY_REJECT, Fallibility::DATA), None);
		assert!(fallibility_gap(EffectSet::OPAQUE_BODY, Fallibility::DATA).is_some());
		assert!(fallibility_gap(EffectSet::OPAQUE_BODY, Fallibility::CONTEXT).is_some());
		assert_eq!(fallibility_gap(EffectSet::OPAQUE_BODY, both), None);
		assert!(fallibility_gap(EffectSet::READS_EXTERNAL, Fallibility::DATA).is_some());
		assert!(fallibility_gap(EffectSet::WRITES_EXTERNAL, Fallibility::CONTEXT).is_some());
		assert_eq!(fallibility_gap(EffectSet::READS_EXTERNAL, both), None);
	}

	#[test]
	fn a_top_answer_covers_every_effect() {
		assert_eq!(fallibility_gap(EffectSet::TOP, Fallibility::TOP), None);
	}

	#[test]
	fn operand_groups_must_sum_to_the_count() {
		assert!(groups_fit(0, &[]));
		assert!(groups_fit(0, &[2]));
		assert!(groups_fit(6, &[2]));
		assert!(!groups_fit(5, &[2]));
		assert!(groups_fit(7, &[2, 3]));
		assert!(!groups_fit(1, &[2, 3]));
		assert!(!groups_fit(7, &[3]));
		assert!(groups_fit(200, &[3, 5]));
		assert!(!groups_fit(3, &[0]));
	}

	#[test]
	fn the_regions_fill_the_declaration_exactly() {
		use RegionKind::{Block, ChooseArm, ValueBlock};
		let where_ = RegionKind::Clause(ClauseRole::Where);
		assert!(region_shape_fits(&[ValueBlock], &[Block], None, &[]));
		assert!(!region_shape_fits(&[], &[Block], None, &[]));
		assert!(!region_shape_fits(&[Block, Block], &[Block], None, &[]), "an extra region");
		assert!(region_shape_fits(&[], &[where_], Some(0), &[]), "an open entry may be empty");
		assert!(region_shape_fits(&[where_, where_], &[where_], Some(0), &[]));
		assert!(!region_shape_fits(&[ChooseArm], &[where_], Some(0), &[]));
		assert!(region_shape_fits(&[Block, where_, where_], &[Block], None, &[where_]));
		assert!(!region_shape_fits(&[Block, ChooseArm], &[Block], None, &[where_]));
		let (a, b) = (RegionKind::PermissionFrame, RegionKind::Clause(ClauseRole::Projection));
		assert!(region_shape_fits(&[Block, a, b], &[Block], None, &[a, b]));
		assert!(!region_shape_fits(&[Block, b, a], &[Block], None, &[a, b]), "tail kinds in order");
	}

	#[test]
	fn a_position_twin_fills_the_position_its_declaration_names() {
		assert!(fills_position(RegionKind::Block, RegionKind::Block));
		assert!(fills_position(RegionKind::Block, RegionKind::ValueBlock));
		assert!(fills_position(RegionKind::ValueIf, RegionKind::StatementIf));
		assert!(fills_position(RegionKind::StatementFor, RegionKind::ValueFor));
		assert!(!fills_position(RegionKind::Block, RegionKind::Loop));
		assert!(!fills_position(
			RegionKind::Statement(StatementCommit::Auto),
			RegionKind::Clause(ClauseRole::Where)
		));
	}
}
