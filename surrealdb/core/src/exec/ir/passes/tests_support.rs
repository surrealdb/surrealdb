//! Helpers the pass tests share: binding SQL, walking and building ops, a recording observer and a
//! scripted folder.

use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::Mutex;

use smallvec::SmallVec;

use crate::exec::function::FunctionRegistry;
use crate::exec::ir::build::logical::bind_request;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, IrScope, Walk, WalkOrder};
use crate::exec::ir::core::location::{LocOrigin, Location};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::verify::{VerifyAt, VerifyError, verify};
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::catalog::{EffectCx, SemanticCatalog};
use crate::exec::ir::iface::conversion::{LOGICAL_PHASE, Phase};
use crate::exec::ir::iface::fold::{
	FoldConsumer, FoldOutcome, FoldRequest, NotFoldable, PlanTimeFolder,
};
use crate::exec::ir::iface::frame::FrameTable;
use crate::exec::ir::iface::ids::{FrameId, OpId, SnapshotId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::ir::passes::driver::FoldObserver;
use crate::exec::ir::{RequestInput, RequestStatement};
use crate::val::Value;

/// A semantic catalog that holds no definition.
struct NoCatalog;

impl SemanticCatalog for NoCatalog {}

/// What an effect context borrows: the builtin registry and a frame table.
pub(super) struct Env {
	registry: FunctionRegistry,
	frames: FrameTable,
}

impl Env {
	pub(super) fn new() -> Self {
		Self {
			registry: FunctionRegistry::with_builtins(),
			frames: FrameTable::default(),
		}
	}

	pub(super) fn cx(&self) -> EffectCx<'_> {
		EffectCx {
			registry: &self.registry,
			catalog: &NoCatalog,
			frames: &self.frames,
		}
	}
}

pub(super) fn request_scope() -> IrScope {
	IrScope::Request {
		fragment: None,
	}
}

/// An empty container whose top region is a request.
pub(super) fn empty_ir() -> Ir {
	Ir::new(request_scope(), RegionKind::Request)
}

/// Threads effect tokens through a container built by hand, as the binder threads one it builds.
pub(super) fn thread_tokens(ir: &mut Ir) {
	let env = Env::new();
	crate::exec::ir::analysis::tokens::thread(ir, &mut FactCache::new(), &env.cx())
		.expect("the container is threaded");
}

/// Binds `sql` into a container in the logical dialect.
pub(super) async fn bind(sql: &str) -> Ir {
	let env = Env::new();
	let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
	let request_statements = RequestStatement::of(&parsed);
	let input = RequestInput::request(&request_statements, request_scope());
	bind_request(&input, &env.cx()).await.expect("the request binds")
}

/// Every op of `ir`, in pre-order from the top region.
pub(super) fn all_ops(ir: &Ir) -> Vec<OpId> {
	let mut out = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		out.push(op);
		Walk::Continue
	});
	out
}

pub(super) fn kind_of(ir: &Ir, op: OpId) -> &OpKind {
	&ir.op(op).expect("the op exists").kind
}

/// The ops of `ir` whose tag is `tag`, in pre-order.
pub(super) fn ops_of_tag(ir: &Ir, tag: OpTag) -> Vec<OpId> {
	all_ops(ir).into_iter().filter(|op| kind_of(ir, *op).tag() == tag).collect()
}

fn synthesized() -> Location {
	Location {
		stmt: 0,
		span: None,
		origin: LocOrigin::Synthesized,
	}
}

/// The description of an op with `results` untyped results, under the default frame and snapshot.
pub(super) fn spec(kind: OpKind, operands: &[ValueId], results: usize) -> OpSpec {
	OpSpec {
		kind,
		operands: operands.iter().copied().collect(),
		results: (0..results).map(|_| TypeKnowledge::Unknown).collect(),
		frame: FrameId::new(0),
		snapshot: SnapshotId::new(0),
		loc: synthesized(),
	}
}

/// An integer literal.
pub(super) fn literal(value: i64) -> OpKind {
	OpKind::Expr(ExprOp::Literal(Box::new(Value::from(value))))
}

/// Creates `kind` at `at` and answers the op and its results.
pub(super) fn create(
	rw: &mut Rewriter<'_>,
	at: InsertPoint,
	kind: OpKind,
	operands: &[ValueId],
	results: usize,
) -> (OpId, SmallVec<[ValueId; 2]>) {
	let op = rw.create(at, spec(kind, operands, results)).expect("the op is created");
	let values = rw.ir().op(op).expect("the op exists").results.clone();
	(op, values)
}

/// Verifies `ir` at the logical phase boundary.
pub(super) fn verify_logical(ir: &Ir, facts: &mut FactCache, env: &Env) -> Result<(), VerifyError> {
	verify(ir, facts, VerifyAt::PhaseBoundary(Phase::Logical), &LOGICAL_PHASE, &env.cx())
}

/// A [`FoldObserver`] that records each consumed value, and whether that value's defining op was
/// still in the container when it was reported.
#[derive(Debug, Default)]
pub(super) struct RecordingObserver {
	pub(super) seen: Vec<(ValueId, FoldConsumer)>,
	pub(super) defined: Vec<bool>,
}

impl FoldObserver for RecordingObserver {
	fn consumed(&mut self, ir: &Ir, value: ValueId, consumer: FoldConsumer) {
		let live = match ir.value(value).map(|data| data.def) {
			Some(ValueDef::OpResult {
				op,
				..
			}) => ir.op(op).is_some(),
			Some(ValueDef::BlockArg {
				..
			}) => true,
			None => false,
		};
		self.seen.push((value, consumer));
		self.defined.push(live);
	}
}

/// A [`PlanTimeFolder`] that answers from a script, so a pass test does not depend on a real
/// folder. The first entry whose needle occurs in the debug text of the op defining the folded
/// value answers; any other value is `NotFoldable(NotYet)`.
#[derive(Default)]
pub(super) struct ScriptedFolder {
	script: Vec<(String, FoldOutcome)>,
	asked: Mutex<Vec<FoldRequest>>,
}

impl ScriptedFolder {
	pub(super) fn new() -> Self {
		Self::default()
	}

	/// Answers `outcome` for a value defined by an op whose debug text contains `needle`.
	pub(super) fn on(mut self, needle: &str, outcome: FoldOutcome) -> Self {
		self.script.push((needle.to_owned(), outcome));
		self
	}

	/// The requests the folder has been asked, in order.
	pub(super) fn asked(&self) -> Vec<FoldRequest> {
		self.asked.lock().expect("the log is not poisoned").clone()
	}
}

impl PlanTimeFolder for ScriptedFolder {
	fn fold<'a>(
		&'a self,
		ir: &'a Ir,
		_cx: &'a EffectCx<'a>,
		req: FoldRequest,
	) -> Pin<Box<dyn Future<Output = FoldOutcome> + Send + 'a>> {
		let text = match ir.value(req.value).map(|data| data.def) {
			Some(ValueDef::OpResult {
				op,
				..
			}) => ir.op(op).map(|data| format!("{:?}", data.kind)).unwrap_or_default(),
			Some(ValueDef::BlockArg {
				..
			})
			| None => String::new(),
		};
		self.asked.lock().expect("the log is not poisoned").push(req);
		let outcome = self
			.script
			.iter()
			.find(|(needle, _)| text.contains(needle.as_str()))
			.map_or(FoldOutcome::NotFoldable(NotFoldable::NotYet), |(_, outcome)| outcome.clone());
		Box::pin(ready(outcome))
	}
}
