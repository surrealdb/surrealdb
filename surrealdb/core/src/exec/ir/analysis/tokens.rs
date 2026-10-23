//! Effect tokens: which ops take and produce a token of which class, and the threading of the
//! tokens through a container.
//!
//! A token orders the ops that touch one [`TokenClass`] of world state. An op takes the current
//! token of every class it consumes and, when it changes the class, produces the next one. The
//! classes an op uses are the transitive answer of its effects: [`token_use`] of the effect set,
//! joined with the use of every op in the regions that run as part of it.
//!
//! # Threading
//!
//! Every top-level statement, and the top region of a container that is a unit of its own, starts
//! from one token argument per class. Ops are threaded in program order:
//!
//! - An op takes the current token of each class it consumes, and each class it produces becomes
//!   the current token after it.
//! - A region that is neither a loop body nor isolated reads the tokens of the op that owns it
//!   directly, unless it runs after the op's own effect.
//! - A region that runs after the op's own effect, the projection and image layers of a write,
//!   reads the tokens the op produces: its block receives one argument for each class it consumes
//!   that the op produces, which stands for the op's produced token of the class. The op's result
//!   is not visible inside the op, so the argument is how the region names it.
//! - A loop body carries a token of each class its ops produce: its block receives one argument for
//!   the class, and the loop takes the incoming token and produces the final one. A body that
//!   produces nothing of a class reads the outer token of the class, which is what lets a read in a
//!   read-only loop stand outside the loop.
//! - A region isolated from above receives one block argument for each class its ops consume. The
//!   owner passes the token of the class it takes: the argument stands for the owner's operand of
//!   the same class, or for the token it produces when the region runs after its effect.
//! - A stored clause is never run and holds no token.
//!
//! A terminator takes and produces no token. The token a body ends with is not carried back to
//! its first block argument: the argument is a value the loop defines, which is all the
//! dependence of an op on the loop needs.
//!
//! Threading is a function of the container. [`rethread`] derives it again after a pass changed
//! the container.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;

use super::effects;
use super::multiplicity::is_loop_body;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashMap;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::rewriter::Rewriter;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::effects::{TokenUse, token_use};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{RegionKind, RegionProperties};
use crate::exec::ir::iface::types::{TokenClass, TokenSet};
use crate::exec::ir::logical::seam::invariant;

/// The classes `op` takes and produces, memoised for one container that is not changed while the
/// memo is used.
#[derive(Default)]
pub(crate) struct Uses {
	memo: FxHashMap<OpId, TokenUse>,
}

impl Uses {
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// The classes `op` takes and produces: [`token_use`] of its transitive effects, joined with
	/// the use of every op in the regions that run as part of it.
	///
	/// The join makes the answer for an op cover the answer for every op it holds, which the
	/// effect answer alone does not when an op deep in the subtree is non-deterministic for a
	/// reason no flag names.
	pub(crate) fn of(
		&mut self,
		ir: &Ir,
		facts: &mut FactCache,
		cx: &EffectCx<'_>,
		op: OpId,
	) -> TokenUse {
		if let Some(known) = self.memo.get(&op) {
			return *known;
		}
		let Some(data) = ir.op(op) else {
			return TokenUse {
				consumes: TokenSet::ALL,
				produces: TokenSet::ALL,
			};
		};
		let answer = effects::summary_with(ir, facts, cx, op);
		let mut used = token_use(answer.set, answer.determinism);
		for region in data.regions.iter().copied() {
			let Some(info) = ir.region(region) else {
				continue;
			};
			if !effects::runs(&data.kind, info.kind) {
				continue;
			}
			used = used.union(self.of_region(ir, facts, cx, region));
		}
		self.memo.insert(op, used);
		used
	}

	/// The classes the ops of `region` take and produce.
	pub(crate) fn of_region(
		&mut self,
		ir: &Ir,
		facts: &mut FactCache,
		cx: &EffectCx<'_>,
		region: RegionId,
	) -> TokenUse {
		let mut used = TokenUse::default();
		for child in ops_in(ir, region) {
			used = used.union(self.of(ir, facts, cx, child));
		}
		used
	}
}

/// The classes of the tokens in `values`, in order.
pub(crate) fn classes_of(ir: &Ir, values: &[ValueId]) -> SmallVec<[Option<TokenClass>; 6]> {
	values.iter().map(|value| ir.token_class(*value)).collect()
}

/// The classes of `set`, in threading order, as the classes of a list of tokens.
pub(crate) fn listed(set: TokenSet) -> SmallVec<[Option<TokenClass>; 6]> {
	set.iter().map(Some).collect()
}

/// The roots of a container: one per statement, so a request of a few statements holds them inline.
pub(crate) type Roots = SmallVec<[RegionId; 4]>;

/// The regions the tokens are threaded from: the statements of a request, and the top region of a
/// container that is a unit of its own.
pub(crate) fn roots(ir: &Ir) -> Roots {
	let mut out = Roots::new();
	collect_roots(ir, ir.top(), &mut out);
	out
}

fn collect_roots(ir: &Ir, region: RegionId, out: &mut Roots) {
	let Some(info) = ir.region(region) else {
		return;
	};
	match info.kind {
		RegionKind::Request | RegionKind::Transaction(_) => {
			for op in ops_in(ir, region) {
				for held in ir.op(op).map_or(&[][..], |data| data.regions.as_slice()) {
					collect_roots(ir, *held, out);
				}
			}
		}
		RegionKind::StoredClause(_) => {}
		RegionKind::Statement(_)
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
		| RegionKind::JoinLookupArm
		| RegionKind::Pipeline
		| RegionKind::WriteStage
		| RegionKind::CatalogUnit
		| RegionKind::Deadline => out.push(region),
	}
}

/// Threads effect tokens through every statement of `ir` and marks the container threaded.
///
/// The container must hold no token: a container that does is [`rethread`]n.
pub(crate) fn thread(ir: &mut Ir, facts: &mut FactCache, cx: &EffectCx<'_>) -> Result<(), IrError> {
	let mut threader = Threader {
		uses: Uses::new(),
		cx,
	};
	for root in roots(ir) {
		threader.root(ir, facts, root)?;
	}
	Rewriter::new(ir, facts).set_threaded(true);
	Ok(())
}

/// Removes every token from `ir` and marks the container unthreaded.
pub(crate) fn strip(ir: &mut Ir, facts: &mut FactCache) {
	let top = ir.top();
	let mut rw = Rewriter::new(ir, facts);
	rw.set_threaded(false);
	rw.strip_tokens(top);
}

/// Derives the threading again: removes every token and threads the container.
pub(crate) fn rethread(
	ir: &mut Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
) -> Result<(), IrError> {
	strip(ir, facts);
	thread(ir, facts, cx)
}

/// The generation of the op that owns each statement of a threaded container, for
/// [`rethread_changed`] to compare against.
pub(crate) struct Marks {
	/// `None` when a root is the top region of a container that is a unit of its own, which has no
	/// owner to compare.
	owners: Option<SmallVec<[(OpId, u32); 4]>>,
}

/// Records the generation of the op that owns each statement of `ir`.
pub(crate) fn mark(ir: &Ir) -> Marks {
	let owners = roots(ir)
		.into_iter()
		.map(|root| ir.parent_op(root).map(|owner| (owner, ir.generation(owner))))
		.collect::<Option<SmallVec<_>>>();
	Marks {
		owners,
	}
}

/// Derives the threading again for the statements a pass changed since `marks` was taken: those
/// whose owner is new or whose generation moved, a rewrite beneath a statement moving the
/// generation of its owner. The tokens of a statement are derived from that statement alone, so
/// the others stay as they are. A container that is a unit of its own is threaded again whole.
pub(crate) fn rethread_changed(
	ir: &mut Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	marks: &Marks,
) -> Result<(), IrError> {
	let Some(before) = &marks.owners else {
		return rethread(ir, facts, cx);
	};
	let mut changed = Vec::new();
	for root in roots(ir) {
		let Some(owner) = ir.parent_op(root) else {
			return rethread(ir, facts, cx);
		};
		let known = before.iter().find(|(held, _)| *held == owner);
		if known.is_none_or(|(_, generation)| *generation != ir.generation(owner))
			&& !changed.contains(&owner)
		{
			changed.push(owner);
		}
	}
	for owner in changed {
		rethread_statement(ir, facts, cx, owner)?;
	}
	Ok(())
}

/// Derives the threading again for the statement `stmt`: removes every token beneath it and
/// threads each region it owns.
///
/// For a statement whose code was replaced by a pass, which leaves the code without tokens. A
/// container that is not threaded is left alone.
pub(crate) fn rethread_statement(
	ir: &mut Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	stmt: OpId,
) -> Result<(), IrError> {
	if !ir.is_threaded() {
		return Ok(());
	}
	let regions: SmallVec<[RegionId; 2]> =
		ir.op(stmt).map(|data| data.regions.iter().copied().collect()).unwrap_or_default();
	let mut threader = Threader {
		uses: Uses::new(),
		cx,
	};
	for region in regions {
		Rewriter::new(ir, facts).strip_tokens(region);
		if roots(ir).contains(&region) {
			threader.root(ir, facts, region)?;
		}
	}
	Ok(())
}

/// The current token of each class at one point of a region.
type Current = [Option<ValueId>; TokenClass::ALL.len()];

struct Threader<'a, 'b> {
	uses: Uses,
	cx: &'a EffectCx<'b>,
}

impl Threader<'_, '_> {
	/// Gives the block of `region` one token argument per class and threads the region from them.
	fn root(
		&mut self,
		ir: &mut Ir,
		facts: &mut FactCache,
		region: RegionId,
	) -> Result<(), IrError> {
		let block = first_block(ir, region)?;
		let mut current: Current = [None; TokenClass::ALL.len()];
		for class in TokenClass::ALL {
			let arg = Rewriter::new(ir, facts).add_token_arg(block, class)?;
			current[class as usize] = Some(arg);
		}
		self.region(ir, facts, region, &mut current)
	}

	/// Threads the ops of `region` in order, from the tokens `current` holds on entry.
	fn region(
		&mut self,
		ir: &mut Ir,
		facts: &mut FactCache,
		region: RegionId,
		current: &mut Current,
	) -> Result<(), IrError> {
		let ops: Vec<OpId> = ops_in(ir, region).collect();
		for op in ops {
			self.op(ir, facts, op, current)?;
		}
		Ok(())
	}

	fn op(
		&mut self,
		ir: &mut Ir,
		facts: &mut FactCache,
		op: OpId,
		current: &mut Current,
	) -> Result<(), IrError> {
		let Some(data) = ir.op(op) else {
			return Ok(());
		};
		if data.kind.is_terminator() {
			return Ok(());
		}
		let regions: SmallVec<[RegionId; 2]> = data.regions.iter().copied().collect();
		let follows = effects::follows_effect(&data.kind);
		Rewriter::new(ir, facts).mark_threaded(op);
		let used = self.uses.of(ir, facts, self.cx, op);
		for class in used.consumes.iter() {
			let token = current[class as usize]
				.ok_or_else(|| invariant("an op takes a token of a class no token reaches"))?;
			Rewriter::new(ir, facts).add_token_operand(op, token)?;
		}
		for held in regions {
			self.nested(ir, facts, held, *current, follows, used)?;
		}
		for class in used.produces.iter() {
			let token = Rewriter::new(ir, facts).add_token_result(op, class)?;
			current[class as usize] = Some(token);
		}
		Ok(())
	}

	/// Threads a region of an op that uses `owner_used` and stands where the tokens of `outer` are
	/// current. `follows` is whether the region runs after the op's own effect.
	fn nested(
		&mut self,
		ir: &mut Ir,
		facts: &mut FactCache,
		region: RegionId,
		outer: Current,
		follows: bool,
		owner_used: TokenUse,
	) -> Result<(), IrError> {
		let Some(info) = ir.region(region) else {
			return Ok(());
		};
		let kind = info.kind;
		if matches!(kind, RegionKind::StoredClause(_)) {
			return Ok(());
		}
		let args = region_args(&mut self.uses, ir, facts, self.cx, follows, owner_used, region);
		let mut inner = if kind.isolated_from_above() {
			[None; TokenClass::ALL.len()]
		} else {
			outer
		};
		if !args.is_empty() {
			let block = first_block(ir, region)?;
			for class in args.iter() {
				let arg = Rewriter::new(ir, facts).add_token_arg(block, class)?;
				inner[class as usize] = Some(arg);
			}
		}
		self.region(ir, facts, region, &mut inner)
	}
}

/// The classes of the token arguments the block of `region`, owned by an op that uses `owner_used`,
/// receives. `follows` is whether the region runs after the op's own effect:
///
/// - none for a stored clause;
/// - the classes its ops consume, for a region isolated from above;
/// - the classes its ops produce, for a loop body;
/// - the classes its ops consume that the owner produces, for a region that runs after the owner's
///   effect;
/// - none for any other region.
fn region_args(
	uses: &mut Uses,
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	follows: bool,
	owner_used: TokenUse,
	region: RegionId,
) -> TokenSet {
	let Some(kind) = ir.region(region).map(|info| info.kind) else {
		return TokenSet::NONE;
	};
	if matches!(kind, RegionKind::StoredClause(_)) {
		TokenSet::NONE
	} else if kind.isolated_from_above() {
		uses.of_region(ir, facts, cx, region).consumes
	} else if is_loop_body(kind) {
		uses.of_region(ir, facts, cx, region).produces
	} else if follows {
		uses.of_region(ir, facts, cx, region).consumes.intersection(owner_used.produces)
	} else {
		TokenSet::NONE
	}
}

fn first_block(ir: &Ir, region: RegionId) -> Result<crate::exec::ir::iface::ids::BlockId, IrError> {
	ir.region(region)
		.and_then(|info| info.blocks.first().copied())
		.ok_or_else(|| invariant("a region holds a block"))
}

/// The first op or region of the threaded container `ir` that breaks agreement between its
/// effects and its tokens, with what is wrong.
///
/// An op takes exactly the classes it consumes and produces exactly the classes it produces, in
/// class order. The block of a region that is the root of a threading receives one argument per
/// class, a loop body one for each class its ops produce, a region isolated from above one for each
/// class its ops consume, and a region that runs after its owner's effect one for each class its
/// ops consume that the owner produces. A terminator and a stored clause hold no token.
pub(crate) fn disagreement(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
) -> Option<(Option<OpId>, String)> {
	let mut uses = Uses::new();
	for root in roots(ir) {
		let block = ir.region(root)?.blocks.first().copied()?;
		let all = listed(TokenSet::ALL);
		if classes_of(ir, ir.token_args(block)) != all {
			return Some((
				ir.parent_op(root),
				format!(
					"{root:?} is threaded from {} tokens, not one per class",
					ir.token_args(block).len()
				),
			));
		}
		if let Some(found) = disagreement_in(ir, facts, cx, &mut uses, root) {
			return Some(found);
		}
	}
	None
}

fn disagreement_in(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	uses: &mut Uses,
	region: RegionId,
) -> Option<(Option<OpId>, String)> {
	for op in ops_in(ir, region) {
		let data = ir.op(op)?;
		if data.kind.is_terminator() {
			if !ir.token_operands(op).is_empty() || !ir.token_results(op).is_empty() {
				return Some((Some(op), "a terminator holds a token".to_owned()));
			}
			continue;
		}
		let used = uses.of(ir, facts, cx, op);
		let takes = classes_of(ir, ir.token_operands(op));
		if takes != listed(used.consumes) {
			return Some((
				Some(op),
				format!("the op takes tokens of {takes:?} and consumes {:?}", used.consumes),
			));
		}
		let makes = classes_of(ir, ir.token_results(op));
		if makes != listed(used.produces) {
			return Some((
				Some(op),
				format!("the op produces tokens of {makes:?} and produces {:?}", used.produces),
			));
		}
		for held in data.regions.iter().copied() {
			let info = ir.region(held)?;
			let block = info.blocks.first().copied()?;
			let given = classes_of(ir, ir.token_args(block));
			let wanted =
				region_args(uses, ir, facts, cx, effects::follows_effect(&data.kind), used, held);
			if given != listed(wanted) {
				return Some((
					Some(op),
					format!(
						"{held:?} of kind {:?} receives tokens of {given:?}, not {wanted:?}",
						info.kind
					),
				));
			}
			if !matches!(info.kind, RegionKind::StoredClause(_))
				&& let Some(found) = disagreement_in(ir, facts, cx, uses, held)
			{
				return Some(found);
			}
		}
	}
	None
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::function::FunctionRegistry;
	use crate::exec::ir::build::logical::bind_request;
	use crate::exec::ir::core::ir::{IrScope, Walk, WalkOrder};
	use crate::exec::ir::core::value::ValueDef;
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::ir::iface::region_kind::ClauseRole;
	use crate::exec::ir::op_kind::OpTag;
	use crate::exec::ir::{RequestInput, RequestStatement};

	struct Env {
		registry: FunctionRegistry,
		frames: FrameTable,
	}

	impl Env {
		fn new() -> Self {
			Self {
				registry: FunctionRegistry::with_builtins(),
				frames: FrameTable::default(),
			}
		}

		fn cx(&self) -> EffectCx<'_> {
			EffectCx {
				registry: &self.registry,
				catalog: &super::super::cx::NoDefinitions,
				frames: &self.frames,
			}
		}
	}

	async fn bind(env: &Env, sql: &str) -> Ir {
		let parsed = crate::syn::parse(sql).expect("the request parses");
		let statements = RequestStatement::of(&parsed.expressions);
		let input = RequestInput::request(
			&statements,
			IrScope::Request {
				fragment: None,
			},
		);
		bind_request(&input, &env.cx()).await.expect("the request binds")
	}

	fn ops(ir: &Ir) -> Vec<OpId> {
		let mut out = Vec::new();
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			out.push(op);
			Walk::Continue
		});
		out
	}

	fn classes(ir: &Ir, values: &[ValueId]) -> Vec<TokenClass> {
		values.iter().filter_map(|value| ir.token_class(*value)).collect()
	}

	/// The ops of tag `tag`, in pre-order.
	fn tagged(ir: &Ir, tag: OpTag) -> Vec<OpId> {
		ops(ir).into_iter().filter(|op| ir.op(*op).is_some_and(|d| d.kind.tag() == tag)).collect()
	}

	/// The body region of the loop `op`.
	fn body_of(ir: &Ir, op: OpId) -> RegionId {
		crate::exec::ir::analysis::multiplicity::loop_body(ir, op).expect("the op is a loop")
	}

	fn body_block(ir: &Ir, op: OpId) -> crate::exec::ir::iface::ids::BlockId {
		let region = body_of(ir, op);
		ir.region(region).and_then(|info| info.blocks.first().copied()).expect("a body block")
	}

	/// The loops of `ir`, in pre-order.
	fn loops(ir: &Ir) -> Vec<OpId> {
		tagged(ir, OpTag::Logical(crate::exec::ir::logical::ops::LogTag::Loop))
	}

	/// A readable account of the tokens of every op, for a failure message.
	fn describe(ir: &Ir) -> String {
		let mut out = String::new();
		for op in ops(ir) {
			let Some(data) = ir.op(op) else {
				continue;
			};
			out.push_str(&format!(
				"{op:?} {} takes {:?} makes {:?}\n",
				data.kind.tag().mnemonic(),
				classes(ir, ir.token_operands(op)),
				classes(ir, ir.token_results(op)),
			));
		}
		out
	}

	#[tokio::test]
	async fn a_statement_starts_from_one_token_per_class() {
		let env = Env::new();
		let ir = bind(&env, "SELECT * FROM t; RETURN 1;").await;
		assert!(ir.is_threaded());
		let found = roots(&ir);
		assert_eq!(found.len(), 2, "one root per statement");
		for root in found {
			let block =
				ir.region(root).and_then(|info| info.blocks.first().copied()).expect("block");
			assert_eq!(classes(&ir, ir.token_args(block)), TokenClass::ALL, "{root:?}");
		}
		let mut facts = FactCache::new();
		assert_eq!(disagreement(&ir, &mut facts, &env.cx()), None);
	}

	#[tokio::test]
	async fn a_read_only_loop_carries_nothing_and_reads_the_outer_token() {
		let env = Env::new();
		let ir = bind(&env, "SELECT * FROM t WHERE n > (SELECT VALUE count() FROM t GROUP ALL)[0]")
			.await;
		let found = loops(&ir);
		assert!(found.len() >= 2, "{}", describe(&ir));
		for op in &found {
			let block = body_block(&ir, *op);
			assert!(ir.token_args(block).is_empty(), "a body that writes nothing carries nothing");
			assert!(ir.token_results(*op).is_empty());
		}
		// The inner read takes the token the statement starts from: no write precedes it.
		let sources = tagged(&ir, OpTag::Logical(crate::exec::ir::logical::ops::LogTag::Source));
		let inner = *sources.last().expect("a source");
		let store = ir.token_of_class(ir.token_operands(inner), TokenClass::Store).expect("a read");
		let Some(ValueDef::BlockArg {
			block,
			..
		}) = ir.value(store).map(|value| value.def)
		else {
			panic!("the read takes a block argument: {}", describe(&ir));
		};
		assert_eq!(
			ir.parent_op(ir.block(block).expect("block").parent),
			ir.parent_op(roots(&ir)[0])
		);
	}

	#[tokio::test]
	async fn a_loop_that_draws_entropy_carries_the_rng_token() {
		let env = Env::new();
		let ir = bind(&env, "SELECT *, rand::uuid::v4() FROM t").await;
		let select = loops(&ir)[0];
		let block = body_block(&ir, select);
		assert_eq!(classes(&ir, ir.token_args(block)), [TokenClass::Rng], "{}", describe(&ir));
		assert_eq!(classes(&ir, ir.token_results(select)), [TokenClass::Rng]);
		let draw = tagged(&ir, OpTag::Expr(crate::exec::ir::expr::ops::ExprTag::Call))
			.into_iter()
			.find(|op| classes(&ir, ir.token_operands(*op)).contains(&TokenClass::Rng))
			.expect("the draw takes the Rng token");
		let rng = ir.token_of_class(ir.token_operands(draw), TokenClass::Rng).expect("token");
		assert_eq!(ir.token_args(block), [rng], "the draw takes the token the body carries");
	}

	#[tokio::test]
	async fn the_clock_and_a_session_parameter_take_no_token() {
		let env = Env::new();
		let ir = bind(&env, "SELECT time::now(), $session FROM t").await;
		for op in ops(&ir) {
			let tag = ir.op(op).map(|d| d.kind.tag());
			if matches!(
				tag,
				Some(
					OpTag::Expr(crate::exec::ir::expr::ops::ExprTag::Now)
						| OpTag::Expr(crate::exec::ir::expr::ops::ExprTag::SessionParam)
				)
			) {
				assert!(ir.token_operands(op).is_empty(), "{}", describe(&ir));
			}
		}
	}

	#[tokio::test]
	async fn a_write_takes_and_produces_the_store() {
		let env = Env::new();
		let ir = bind(&env, "CREATE t:1 SET n = 1; SELECT * FROM t").await;
		let write = tagged(&ir, OpTag::Logical(crate::exec::ir::logical::ops::LogTag::Write))[0];
		assert!(classes(&ir, ir.token_operands(write)).contains(&TokenClass::Store));
		assert!(classes(&ir, ir.token_results(write)).contains(&TokenClass::Store));
	}

	/// Whether `op` stands inside a region of kind `kind`.
	fn inside(ir: &Ir, op: OpId, kind: RegionKind) -> bool {
		let mut region = ir.enclosing_region(op);
		loop {
			if ir.region(region).is_some_and(|info| info.kind == kind) {
				return true;
			}
			match ir.parent_op(region) {
				Some(owner) => region = ir.enclosing_region(owner),
				None => return false,
			}
		}
	}

	#[tokio::test]
	async fn a_return_clause_reads_the_token_its_write_produced() {
		let env = Env::new();
		let ir = bind(
			&env,
			"{ LET $a = (SELECT VALUE n FROM t:1); \
			   UPDATE t:1 SET n += 1 RETURN (SELECT VALUE n FROM t:1) AS m }",
		)
		.await;
		let returned = RegionKind::Clause(ClauseRole::ReturnClause);
		let sources = tagged(&ir, OpTag::Logical(crate::exec::ir::logical::ops::LogTag::Source));
		let store_of = |op: OpId| {
			ir.token_of_class(ir.token_operands(op), TokenClass::Store).expect("a read takes Store")
		};
		let before = *sources
			.iter()
			.find(|op| !inside(&ir, **op, returned))
			.expect("the read before the write");
		let after = *sources
			.iter()
			.find(|op| inside(&ir, **op, returned))
			.expect("the read in the RETURN clause");
		let Some(ValueDef::BlockArg {
			block,
			..
		}) = ir.value(store_of(after)).map(|value| value.def)
		else {
			panic!("the read takes a block argument of the clause: {}", describe(&ir));
		};
		assert_eq!(
			ir.block(block).map(|held| held.parent).and_then(|r| ir.region(r)).map(|r| r.kind),
			Some(returned)
		);
		assert_ne!(store_of(before), store_of(after), "the write stands between the two reads");
		let mut facts = FactCache::new();
		assert_eq!(disagreement(&ir, &mut facts, &env.cx()), None);
	}

	#[tokio::test]
	async fn stripping_removes_every_token_and_rethreading_derives_them_again() {
		let env = Env::new();
		let mut ir =
			bind(&env, "UPDATE t SET n = (SELECT VALUE count() FROM t GROUP ALL)[0]").await;
		let before = describe(&ir);
		let mut facts = FactCache::new();
		strip(&mut ir, &mut facts);
		assert!(!ir.is_threaded());
		assert!(ops(&ir).into_iter().all(|op| {
			ir.token_operands(op).is_empty()
				&& ir.token_results(op).is_empty()
				&& ir.op(op).is_some_and(|data| {
					data.regions.iter().all(|region| {
						ir.region(*region).is_some_and(|info| {
							info.blocks.iter().all(|b| ir.token_args(*b).is_empty())
						})
					})
				})
		}));
		rethread(&mut ir, &mut facts, &env.cx()).expect("the container is threaded again");
		assert_eq!(describe(&ir), before);
		assert_eq!(disagreement(&ir, &mut facts, &env.cx()), None);
	}

	#[tokio::test]
	async fn rethreading_the_changed_statements_leaves_the_others_alone() {
		use crate::exec::ir::core::location::{LocOrigin, Location};
		use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec};
		use crate::exec::ir::expr::ops::ExprOp;
		use crate::exec::ir::iface::types::TypeKnowledge;
		use crate::exec::ir::op_kind::OpKind;
		use crate::val::Value;

		let env = Env::new();
		let mut ir = bind(&env, "SELECT * FROM t; SELECT * FROM u").await;
		let sources = tagged(&ir, OpTag::Logical(crate::exec::ir::logical::ops::LogTag::Source));
		let (first, second) = (sources[0], sources[1]);
		let first_token = ir.token_operands(first).to_vec();
		let marks = mark(&ir);
		let (frame, snapshot) = {
			let data = ir.op(second).expect("the source exists");
			(data.frame, data.snapshot)
		};
		let mut facts = FactCache::new();
		let created = Rewriter::new(&mut ir, &mut facts)
			.create(
				InsertPoint::After(second),
				OpSpec {
					kind: OpKind::Expr(ExprOp::Literal(Box::new(Value::from(1)))),
					operands: Default::default(),
					results: vec![TypeKnowledge::Unknown].into(),
					frame,
					snapshot,
					loc: Location {
						stmt: 0,
						span: None,
						origin: LocOrigin::Synthesized,
					},
				},
			)
			.expect("the literal is created");
		assert!(ir.holds_no_tokens(created));
		rethread_changed(&mut ir, &mut facts, &env.cx(), &marks).expect("threaded again");
		assert!(!ir.holds_no_tokens(created), "the changed statement is threaded again");
		assert_eq!(ir.token_operands(first), first_token, "the other statement keeps its tokens");
		assert_eq!(disagreement(&ir, &mut facts, &env.cx()), None);
	}

	#[tokio::test]
	async fn an_op_that_lacks_a_token_it_consumes_is_a_disagreement() {
		let env = Env::new();
		let mut ir = bind(&env, "SELECT * FROM t").await;
		let source = tagged(&ir, OpTag::Logical(crate::exec::ir::logical::ops::LogTag::Source))[0];
		assert!(!ir.token_operands(source).is_empty());
		let mut facts = FactCache::new();
		let region = ir.enclosing_region(source);
		Rewriter::new(&mut ir, &mut facts).strip_tokens(region);
		let found = disagreement(&ir, &mut facts, &env.cx());
		assert!(found.is_some(), "{}", describe(&ir));
	}
}
