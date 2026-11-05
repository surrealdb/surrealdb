//! Unit tests of the lowering that span its modules. Tests of one module sit beside it.
//!
//! [`Builder`] builds the containers those tests lower: ops of the expression dialect created
//! through the rewriter, one result each, in the default authority frame.

use smallvec::SmallVec;
use surrealdb_strand::Strand;

use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, IrScope};
use crate::exec::ir::core::location::{LocOrigin, Location};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{DispatchTarget, ExprOp, ShortCircuitOp};
use crate::exec::ir::iface::conversion::Phase;
use crate::exec::ir::iface::ids::{BlockId, FrameId, OpId, SnapshotId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::LogOp;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::expr::logical::EvalFrame;
use crate::expr::{BinaryOperator, Function};
use crate::val::Value;

/// A container under construction, whose ops go to the end of the innermost open region's block.
pub(super) struct Builder {
	pub(super) ir: Ir,
	pub(super) facts: FactCache,
	blocks: Vec<BlockId>,
	/// The frame the ops created next run under.
	frame: FrameId,
}

impl Builder {
	/// A container whose top region holds no op.
	pub(super) fn new() -> Self {
		let ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let top = ir
			.region(ir.top())
			.and_then(|region| region.blocks.first().copied())
			.expect("the top region holds one block");
		Self {
			ir,
			facts: FactCache::new(),
			blocks: vec![top],
			frame: FrameId::new(0),
		}
	}

	/// A container whose top region is of `kind` and takes block arguments of the types `args`,
	/// and holds no op.
	pub(super) fn with_top(kind: RegionKind, args: &[TypeKnowledge]) -> Self {
		let ir = Ir::with_top_args(
			IrScope::Request {
				fragment: None,
			},
			kind,
			args,
		);
		let top = ir
			.region(ir.top())
			.and_then(|region| region.blocks.first().copied())
			.expect("the top region holds one block");
		Self {
			ir,
			facts: FactCache::new(),
			blocks: vec![top],
			frame: FrameId::new(0),
		}
	}

	/// The id of the frame that evaluates under `eval`, with the principal of the default frame.
	pub(super) fn frame(&mut self, eval: EvalFrame) -> FrameId {
		let mut frame = self.ir.frame(FrameId::new(0)).expect("the default frame").clone();
		frame.eval = eval;
		Rewriter::new(&mut self.ir, &mut self.facts)
			.intern_frame(frame)
			.expect("the frame is interned")
	}

	/// Runs `body` with every op it creates under the frame `frame`.
	pub(super) fn in_frame<T>(&mut self, frame: FrameId, body: impl FnOnce(&mut Self) -> T) -> T {
		let outer = std::mem::replace(&mut self.frame, frame);
		let built = body(self);
		self.frame = outer;
		built
	}

	/// Creates `op` over `operands` with `results` results of unknown type.
	fn create(&mut self, op: ExprOp, operands: &[ValueId], results: usize) -> OpId {
		self.create_kind(OpKind::Expr(op), operands, results)
	}

	/// Creates the op `kind` over `operands` with `results` results of unknown type.
	pub(super) fn create_kind(
		&mut self,
		kind: OpKind,
		operands: &[ValueId],
		results: usize,
	) -> OpId {
		let block = *self.blocks.last().expect("a block is open");
		let spec = OpSpec {
			kind,
			operands: operands.iter().copied().collect(),
			results: (0..results).map(|_| TypeKnowledge::Unknown).collect::<SmallVec<_>>(),
			frame: self.frame,
			snapshot: SnapshotId::new(0),
			loc: Location {
				stmt: 0,
				span: None,
				origin: LocOrigin::Query,
			},
		};
		let point = self.hoisted(&spec.kind, block).unwrap_or(InsertPoint::End(block));
		Rewriter::new(&mut self.ir, &mut self.facts).create(point, spec).expect("the op is created")
	}

	/// Where an expression op goes when `block` is the block of a pipeline region: right before
	/// the pipeline op, in the block that holds it. A pipeline region holds physical ops alone, so
	/// the values a paging clause or a source reads are defined outside it.
	fn hoisted(&self, kind: &OpKind, block: BlockId) -> Option<InsertPoint> {
		if !matches!(kind, OpKind::Expr(_)) {
			return None;
		}
		let region = self.ir.block(block)?.parent;
		if self.ir.region(region)?.kind != RegionKind::Pipeline {
			return None;
		}
		self.ir.parent_op(region).map(InsertPoint::Before)
	}

	/// The value `op` defines.
	pub(super) fn result_of(&self, op: OpId) -> ValueId {
		self.ir.op(op).and_then(|data| data.results.first().copied()).expect("the op has a result")
	}

	/// An op of one result over `operands`.
	pub(super) fn add(&mut self, op: ExprOp, operands: &[ValueId]) -> ValueId {
		let id = self.create(op, operands, 1);
		self.result_of(id)
	}

	/// An op of one result over `operands` whose regions the caller opens with
	/// [`Builder::region`].
	pub(super) fn add_with_regions(&mut self, op: ExprOp, operands: &[ValueId]) -> OpId {
		self.create(op, operands, 1)
	}

	/// An op of no result over `operands`: a terminator.
	pub(super) fn terminate(&mut self, op: ExprOp, operands: &[ValueId]) -> OpId {
		self.create(op, operands, 0)
	}

	/// Opens a region of `kind` owned by `owner` with block arguments of the types `args`, builds
	/// its body with `body`, which is handed the arguments, and ends it with the yield of the
	/// value `body` answers.
	pub(super) fn region_with_args(
		&mut self,
		owner: OpId,
		kind: RegionKind,
		args: &[TypeKnowledge],
		body: impl FnOnce(&mut Self, &[ValueId]) -> ValueId,
	) {
		let (_, block) = Rewriter::new(&mut self.ir, &mut self.facts)
			.create_region(owner, kind, args)
			.expect("the region is created");
		self.blocks.push(block);
		let block_args = self.ir.block(block).map(|data| data.args.to_vec()).unwrap_or_default();
		let value = body(self, &block_args);
		self.create(ExprOp::Yield, &[value], 0);
		self.blocks.pop();
	}

	/// Opens a region of `kind` owned by `owner` with block arguments of the types `args`, builds
	/// its body with `body`, which is handed the arguments and ends the block itself.
	pub(super) fn open_with(
		&mut self,
		owner: OpId,
		kind: RegionKind,
		args: &[TypeKnowledge],
		body: impl FnOnce(&mut Self, &[ValueId]),
	) {
		let (_, block) = Rewriter::new(&mut self.ir, &mut self.facts)
			.create_region(owner, kind, args)
			.expect("the region is created");
		self.blocks.push(block);
		let block_args = self.ir.block(block).map(|data| data.args.to_vec()).unwrap_or_default();
		body(self, &block_args);
		self.blocks.pop();
	}

	/// A logical op of no result over `operands`.
	pub(super) fn add_logical(&mut self, op: LogOp, operands: &[ValueId]) -> OpId {
		self.create_kind(OpKind::Logical(op), operands, 0)
	}

	/// A physical op of no result over `operands`.
	pub(super) fn add_phys(&mut self, op: PhysOp, operands: &[ValueId]) -> OpId {
		self.create_kind(OpKind::Phys(op), operands, 0)
	}

	/// Records `text` as the surface text of the expression `op` is built from.
	pub(super) fn written(&mut self, op: OpId, text: &str) {
		let loc = self.ir.op(op).expect("the op exists").loc.clone();
		Rewriter::new(&mut self.ir, &mut self.facts)
			.set_location(op, loc.with_text(std::sync::Arc::new(text.to_owned())))
			.expect("the location is set");
	}

	/// A physical op of one result over `operands`, whose regions the caller opens.
	pub(super) fn add_phys_value(&mut self, op: PhysOp, operands: &[ValueId]) -> OpId {
		self.create_kind(OpKind::Phys(op), operands, 1)
	}

	/// The `phys.yield` that ends the body of a loop with `value`.
	pub(super) fn yield_phys(&mut self, value: ValueId) -> OpId {
		self.create_kind(OpKind::Phys(PhysOp::Yield), &[value], 0)
	}

	/// A physical op of `results` results over `operands`.
	pub(super) fn add_phys_results(
		&mut self,
		op: PhysOp,
		operands: &[ValueId],
		results: usize,
	) -> OpId {
		self.create_kind(OpKind::Phys(op), operands, results)
	}

	/// The arguments of the block of the innermost open region.
	pub(super) fn block_args(&self) -> Vec<ValueId> {
		let block = *self.blocks.last().expect("a block is open");
		self.ir.block(block).map(|data| data.args.to_vec()).expect("the block exists")
	}

	pub(super) fn literal(&mut self, value: Value) -> ValueId {
		self.add(ExprOp::Literal(Box::new(value)), &[])
	}

	pub(super) fn binary(&mut self, op: BinaryOperator, left: ValueId, right: ValueId) -> ValueId {
		self.add(
			ExprOp::Binary {
				op,
			},
			&[left, right],
		)
	}

	/// The closure literal `|| NONE`, made where the ops go, which captures nothing.
	pub(super) fn closure(&mut self) -> ValueId {
		let attrs = crate::exec::ir::expr::ops::ClosureAttrs {
			params: Box::new([]),
			returns: None,
			body_sql: std::sync::Arc::from("NONE"),
			captures: Default::default(),
			reads_row: false,
			reads_root: false,
			literal: crate::exec::ir::expr::ops::ClosureLiteral::none_written_as("NONE"),
		};
		let owner = self.add_with_regions(ExprOp::ClosureCreate(Box::new(attrs)), &[]);
		self.region_with_args(owner, RegionKind::ClosureBody, &[TypeKnowledge::Unknown], |b, _| {
			b.literal(Value::None)
		});
		self.result_of(owner)
	}

	/// A call of the builtin `name`.
	pub(super) fn call(&mut self, name: &str, args: &[ValueId]) -> ValueId {
		self.add(ExprOp::call(Function::Normal(name.to_owned())), args)
	}

	/// A call of the method `name` on `receiver`: the dispatch check, then the call.
	pub(super) fn method(&mut self, name: &str, receiver: ValueId, args: &[ValueId]) -> ValueId {
		let dispatch = self.add(
			ExprOp::Dispatch(Box::new(DispatchTarget::Method(Strand::new(name)))),
			&[receiver],
		);
		let mut operands = vec![dispatch, receiver];
		operands.extend_from_slice(args);
		self.add(
			ExprOp::Method {
				name: Strand::new(name),
			},
			&operands,
		)
	}

	/// Opens a region of `kind` owned by `owner`, with block arguments of the types `args`, and
	/// makes its block the one ops go to until [`Builder::close`].
	pub(super) fn open(&mut self, owner: OpId, kind: RegionKind, args: &[TypeKnowledge]) {
		let (_, block) = Rewriter::new(&mut self.ir, &mut self.facts)
			.create_region(owner, kind, args)
			.expect("the region is created");
		self.blocks.push(block);
	}

	/// Closes the region [`Builder::open`] opened last.
	pub(super) fn close(&mut self) {
		self.blocks.pop();
	}

	/// Moves the container past the physical boundary and lowers it. The lowering must refuse it
	/// with `IrError::NotYet`; answers the `what` the refusal names.
	pub(super) fn refused(&mut self) -> &'static str {
		self.physical();
		match super::lower(&self.ir, &FactCache::new()) {
			Err(IrError::NotYet {
				what,
			}) => what,
			Err(error) => panic!("an unexpected error: {error}"),
			Ok(_) => panic!("the container lowers"),
		}
	}

	/// Moves the container past the physical boundary, as the placement of its pipelines does.
	pub(super) fn physical(&mut self) {
		Rewriter::new(&mut self.ir, &mut self.facts)
			.advance_phase(Phase::Physical)
			.expect("the phase advances");
	}

	/// Opens a region of `kind` owned by `owner`, builds its body with `body` and ends it with
	/// the yield of the value `body` answers.
	pub(super) fn region(
		&mut self,
		owner: OpId,
		kind: RegionKind,
		body: impl FnOnce(&mut Self) -> ValueId,
	) {
		let (_, block) = Rewriter::new(&mut self.ir, &mut self.facts)
			.create_region(owner, kind, &[])
			.expect("the region is created");
		self.blocks.push(block);
		let value = body(self);
		self.create(ExprOp::Yield, &[value], 0);
		self.blocks.pop();
	}

	/// `left op right`, whose right side is the value `right` builds in a lazy region.
	pub(super) fn short_circuit(
		&mut self,
		op: ShortCircuitOp,
		left: ValueId,
		right: impl FnOnce(&mut Self) -> ValueId,
	) -> ValueId {
		let owner = self.add_with_regions(
			ExprOp::ShortCircuit {
				op,
			},
			&[left],
		);
		self.region(owner, RegionKind::LazyOperand, right);
		self.result_of(owner)
	}

	/// The conditional on `condition`, whose arms are the values `then` and `otherwise` build.
	pub(super) fn if_else(
		&mut self,
		condition: ValueId,
		then: impl FnOnce(&mut Self) -> ValueId,
		otherwise: impl FnOnce(&mut Self) -> ValueId,
	) -> ValueId {
		let owner = self.add_with_regions(ExprOp::If, &[condition]);
		self.region(owner, RegionKind::ValueIf, then);
		self.region(owner, RegionKind::ValueIf, otherwise);
		self.result_of(owner)
	}
}

/// The source of every file of the lowering, with its name.
const SOURCES: [(&str, &str); 28] = [
	("batch_read.rs", include_str!("batch_read.rs")),
	("block.rs", include_str!("block.rs")),
	("closure.rs", include_str!("closure.rs")),
	("control.rs", include_str!("control.rs")),
	("ddl.rs", include_str!("ddl.rs")),
	("dispatch.rs", include_str!("dispatch.rs")),
	("emit.rs", include_str!("emit.rs")),
	("expr.rs", include_str!("expr.rs")),
	("fold.rs", include_str!("fold.rs")),
	("graph.rs", include_str!("graph.rs")),
	("info.rs", include_str!("info.rs")),
	("kernel.rs", include_str!("kernel.rs")),
	("memo.rs", include_str!("memo.rs")),
	("mod.rs", include_str!("mod.rs")),
	("nested.rs", include_str!("nested.rs")),
	("path.rs", include_str!("path.rs")),
	("pipe.rs", include_str!("pipe.rs")),
	("place.rs", include_str!("place.rs")),
	("read.rs", include_str!("read.rs")),
	("reg.rs", include_str!("reg.rs")),
	("select.rs", include_str!("select.rs")),
	("stage.rs", include_str!("stage.rs")),
	("tests.rs", include_str!("tests.rs")),
	("units.rs", include_str!("units.rs")),
	("write.rs", include_str!("write.rs")),
	("write_place.rs", include_str!("write_place.rs")),
	("write_source.rs", include_str!("write_source.rs")),
	("write_stmt.rs", include_str!("write_stmt.rs")),
];

/// Whether `line` declares a `static` item, whatever its visibility.
fn declares_static(line: &str) -> bool {
	let line = line.trim_start();
	let rest = match line.strip_prefix("pub") {
		Some(rest) => {
			let rest = rest.trim_start();
			match rest.strip_prefix('(') {
				Some(restriction) => restriction.split_once(')').map_or(rest, |(_, after)| after),
				None => rest,
			}
		}
		None => line,
	};
	rest.trim_start().starts_with("static ")
}

#[test]
fn lowering_holds_no_static_cache() {
	// Lowering the same container twice yields equal programs: nothing carries over between runs.
	let mut b = Builder::new();
	let one = b.literal(Value::from(1_i64));
	let two = b.literal(Value::from(2_i64));
	let sum = b.binary(BinaryOperator::Add, one, two);
	b.add_phys(PhysOp::Eval, &[sum]);
	b.physical();
	let facts = FactCache::new();
	let first = super::lower(&b.ir, &facts).expect("the container lowers");
	let second = super::lower(&b.ir, &facts).expect("the container lowers");
	assert_eq!(first.program, second.program);
	assert_eq!(first.provenance, second.provenance);

	// No source line of the lowering declares a static or a lazily initialised value. The needles
	// are built from pieces so that this file does not match itself, and a comment may name them.
	let needles = [
		concat!("static", " mut"),
		concat!("Once", "Lock"),
		concat!("Lazy", "Lock"),
		concat!("lazy", "_static!"),
		concat!("thread", "_local!"),
	];
	for (file, source) in SOURCES {
		for (number, line) in source.lines().enumerate() {
			if line.trim_start().starts_with("//") {
				continue;
			}
			assert!(!declares_static(line), "{file}:{}: a static item: {line}", number + 1);
			for needle in needles {
				assert!(!line.contains(needle), "{file}:{}: `{needle}`: {line}", number + 1);
			}
		}
	}
}
