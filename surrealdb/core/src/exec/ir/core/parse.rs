//! Reading the text form of a container back into one.
//!
//! The parser reads the text into a tree of ops first, then builds the container from the tree
//! through the [`Rewriter`], so use-lists, the provenance holder counts and the logical-id list
//! hold by construction. The container is built apart and returned whole: a failure at any line
//! answers an error and drops it. Every error carries the 1-based line it was met on.

use std::collections::HashMap;

use smallvec::SmallVec;
use thiserror::Error;

use super::facts::FactCache;
use super::ir::{Ir, IrScope};
use super::location::{LocOrigin, Location};
use super::op::OpData;
use super::print::AttrSyntaxes;
use super::rewriter::{InsertPoint, OpSpec, Provenance, Rewriter};
use super::text::{
	Cursor, DIALECT_NAMES, OpRef, attr_placeholder_index, dialect_index, parse_frame, parse_loc,
	parse_type, phase_from_name, reading_attr_ids, region_kind_from_name, tag_from_mnemonic,
};
use super::verify::{VerifyAt, verify};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::conversion::{LOGICAL_PHASE, PHYSICAL_PHASE, Phase};
use crate::exec::ir::iface::frame::Snapshot;
use crate::exec::ir::iface::ids::{BlockId, FrameId, OpId, SnapshotId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::op_kind::{OpKind, OpTag};

/// The deepest region nesting the parser reads, which bounds its recursion.
const MAX_DEPTH: usize = 256;

/// Why a text is not a container.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("line {line}: {detail}")]
pub(crate) struct ParseError {
	/// The 1-based line, or `0` when the failure is not at a line.
	pub(crate) line: u32,
	pub(crate) detail: String,
}

fn error(line: u32, detail: impl Into<String>) -> ParseError {
	ParseError {
		line,
		detail: detail.into(),
	}
}

fn ir_error(line: u32, failure: &IrError) -> ParseError {
	error(line, failure.to_string())
}

/// Parses `text` into a container of `scope`, reading each dialect's attributes through the
/// syntax `syntaxes` holds.
///
/// The text starts with the `ir` header and the frame table, `#f0` the default frame. A value
/// is named by the number the printer gave it and is defined before it is read. A provenance
/// clause names an earlier op that stands for itself, or an id that names no op. The header names
/// the container's phase when it is past `bound`, and the container is verified at the boundary of
/// that phase, against the dialects the phase admits. A parser that cannot parse a text, or whose
/// container does not verify, answers an error and never an empty or partial container. The
/// container is verified under the full builtin function registry and no catalog definitions.
pub(crate) fn parse(text: &str, scope: IrScope, syntaxes: &AttrSyntaxes) -> Result<Ir, ParseError> {
	let ir = parse_unverified(text, scope, syntaxes)?;
	let registry = FunctionRegistry::with_builtins();
	let target = if ir.phase() >= Phase::Physical {
		&PHYSICAL_PHASE
	} else {
		&LOGICAL_PHASE
	};
	verify(
		&ir,
		&mut FactCache::new(),
		VerifyAt::PhaseBoundary(ir.phase()),
		target,
		&structural_cx(&registry, &ir),
	)
	.map_err(|failure| error(0, format!("the container does not verify: {failure}")))?;
	Ok(ir)
}

/// [`parse`] without the verification: the container as the text describes it, at the phase its
/// header names, whether or not it keeps the rules of that phase.
pub(crate) fn parse_unverified(
	text: &str,
	scope: IrScope,
	syntaxes: &AttrSyntaxes,
) -> Result<Ir, ParseError> {
	let mut parser = Parser {
		c: Cursor::new(text),
		syntaxes,
		frames: 1,
		snapshots: 1,
		unresolved: 0,
	};
	let (mut ir, document) = parser.document(scope)?;
	build(&mut ir, document)?;
	Ok(ir)
}

#[derive(Debug)]
struct OpNode {
	line: u32,
	results: Vec<u32>,
	kind: OpKind,
	operands: Vec<u32>,
	frame: u16,
	snapshot: u16,
	regions: Vec<RegionNode>,
	types: Vec<TypeKnowledge>,
	loc: Option<Location>,
	prov: Option<Vec<OpRef>>,
	/// The op ids the attributes name, in the order of the placeholders `kind` holds.
	attr_refs: Vec<OpRef>,
}

#[derive(Debug)]
struct RegionNode {
	kind: RegionKind,
	args: Vec<(u32, TypeKnowledge)>,
	ops: Vec<OpNode>,
}

/// A whole text: the snapshots beyond the current state, as the line that declares each and the
/// value its version is read from, the ops of the top block and the count of dangling ids.
struct Document {
	phase: Phase,
	snapshots: Vec<(u32, u32)>,
	ops: Vec<OpNode>,
	dangling: u32,
}

struct Parser<'a, 'r> {
	c: Cursor<'a>,
	syntaxes: &'r AttrSyntaxes,
	/// Bounds on the ids an op may name. `frames` counts the frames the header declares.
	/// `snapshots` counts the current state and the declared versions.
	frames: u32,
	snapshots: u32,
	/// The dangling ids named so far.
	unresolved: u32,
}

impl Parser<'_, '_> {
	fn document(&mut self, scope: IrScope) -> Result<(Ir, Document), ParseError> {
		if !self.c.eat("ir ") {
			return Err(self.c.error("expected `ir <region kind>`"));
		}
		let name = self.c.take_while(|c| !c.is_whitespace());
		let Some(top) = region_kind_from_name(name) else {
			return Err(self.c.error(format!("unknown region kind `{name}`")));
		};
		let phase = if self.c.eat(" phase(") {
			let name = self.c.take_while(|c| c != ')');
			let Some(phase) = phase_from_name(name) else {
				return Err(self.c.error(format!("unknown phase `{name}`")));
			};
			self.c.expect(")")?;
			phase
		} else {
			Phase::Bound
		};
		self.end_of_line()?;
		let mut ir = Ir::new(scope, top);

		let mut frames = 0u32;
		while self.c.eat("#f") {
			let n = self.c.number()?;
			self.c.expect(" = ")?;
			if n != frames {
				return Err(self.c.error("frames are numbered from #f0 in order"));
			}
			let frame = parse_frame(&mut self.c)?;
			if n == 0 {
				if ir.frame(FrameId::new(0)) != Some(&frame) {
					return Err(self.c.error("#f0 is not the default frame"));
				}
			} else {
				let id =
					ir.intern_frame(frame).map_err(|failure| self.c.error(failure.to_string()))?;
				if u32::from(id.index()) != n {
					return Err(self.c.error("the frame repeats an earlier one"));
				}
			}
			frames += 1;
			self.end_of_line()?;
		}
		self.frames = frames.max(1);

		let mut snapshots = Vec::new();
		let mut declared = 0u32;
		while self.c.eat("#s") {
			let line = self.c.line();
			let n = self.c.number()?;
			self.c.expect(" = ")?;
			if n != declared {
				return Err(self.c.error("snapshots are numbered from #s0 in order"));
			}
			if u16::try_from(n).is_err() {
				return Err(self.c.error("too many snapshots"));
			}
			if self.c.eat("current") {
				if n != 0 {
					return Err(self.c.error("only #s0 is the current state"));
				}
			} else if self.c.eat("version(%") {
				if n == 0 {
					return Err(self.c.error("#s0 is the current state"));
				}
				if self.c.peek() == Some('?') {
					return Err(self.c.error("a snapshot reads a value outside the text"));
				}
				let value = self.c.number()?;
				self.c.expect(")")?;
				snapshots.push((line, value));
			} else {
				return Err(self.c.error("expected `current` or `version(%<n>)`"));
			}
			declared += 1;
			self.end_of_line()?;
		}
		self.snapshots = declared.max(1);

		let ops = self.block_body(0, true)?;
		Ok((
			ir,
			Document {
				phase,
				snapshots,
				ops,
				dangling: self.unresolved,
			},
		))
	}

	/// Reads the end of a line: blanks, then a newline or the end of the text.
	fn end_of_line(&mut self) -> Result<(), ParseError> {
		self.c.skip_blanks();
		if self.c.eat_newline() || self.c.at_end() {
			Ok(())
		} else {
			let next: String = self.c.rest().chars().take_while(|c| *c != '\n').take(16).collect();
			Err(self.c.error(format!("unexpected `{next}`")))
		}
	}

	/// Reads ops until the `}` that closes the region, or until the end of the text at the top.
	/// Blank lines stand between ops. The closing `}` is left unread.
	fn block_body(&mut self, depth: usize, top: bool) -> Result<Vec<OpNode>, ParseError> {
		let mut ops = Vec::new();
		loop {
			self.c.skip_blanks();
			if self.c.at_end() {
				if top {
					return Ok(ops);
				}
				return Err(self.c.error("a region is not closed"));
			}
			if self.c.eat_newline() {
				continue;
			}
			if self.c.peek() == Some('}') {
				if top {
					return Err(self.c.error("`}` closes no region"));
				}
				return Ok(ops);
			}
			if self.c.peek() == Some('^') {
				return Err(self.c.error("a region holds one block"));
			}
			ops.push(self.op(depth)?);
		}
	}

	fn value_name(&mut self) -> Result<u32, ParseError> {
		self.c.expect("%")?;
		self.c.number()
	}

	fn operand(&mut self) -> Result<u32, ParseError> {
		self.c.expect("%")?;
		if self.c.peek() == Some('?') {
			return Err(self.c.error("an operand reads a value outside the text"));
		}
		self.c.number()
	}

	fn op(&mut self, depth: usize) -> Result<OpNode, ParseError> {
		if depth > MAX_DEPTH {
			return Err(self.c.error("regions nest too deeply"));
		}
		let line = self.c.line();
		let mut results = Vec::new();
		if self.c.peek() == Some('%') {
			loop {
				results.push(self.value_name()?);
				self.c.skip_blanks();
				if !self.c.eat(",") {
					break;
				}
				self.c.skip_blanks();
			}
			self.c.expect("=")?;
			self.c.skip_blanks();
		}
		let mnemonic = self.c.take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.'));
		if mnemonic.is_empty() {
			return Err(self.c.error("expected an op"));
		}
		let Some(tag) = tag_from_mnemonic(mnemonic) else {
			return Err(self.c.error(format!("unknown mnemonic `{mnemonic}`")));
		};
		self.c.expect("(")?;
		let mut operands = Vec::new();
		self.c.skip_blanks();
		if !self.c.eat(")") {
			loop {
				operands.push(self.operand()?);
				self.c.skip_blanks();
				if self.c.eat(",") {
					self.c.skip_blanks();
					continue;
				}
				self.c.expect(")")?;
				break;
			}
		}

		self.c.skip_blanks();
		let attrs = if self.c.eat("attrs(") {
			let text = self.c.quoted()?;
			self.c.expect(")")?;
			text
		} else {
			String::new()
		};
		let (kind, attr_refs) = self.kind_of(tag, &attrs, line)?;

		self.c.skip_blanks();
		let (frame, snapshot) = if self.c.eat("[frame=#f") {
			let frame = self.c.number()?;
			self.c.expect(" snap=#s")?;
			let snapshot = self.c.number()?;
			self.c.expect("]")?;
			if frame >= self.frames {
				return Err(error(line, format!("frame #f{frame} is not defined")));
			}
			if snapshot >= self.snapshots {
				return Err(error(line, format!("snapshot #s{snapshot} is not defined")));
			}
			let frame = u16::try_from(frame)
				.map_err(|_| error(line, format!("frame #f{frame} is not defined")))?;
			let snapshot = u16::try_from(snapshot)
				.map_err(|_| error(line, format!("snapshot #s{snapshot} is not defined")))?;
			(frame, snapshot)
		} else {
			(0, 0)
		};

		let mut regions = Vec::new();
		loop {
			self.c.skip_blanks();
			if !self.c.eat("{") {
				break;
			}
			regions.push(self.region(depth)?);
		}

		self.c.skip_blanks();
		let mut types = Vec::new();
		if self.c.eat(":") {
			loop {
				self.c.skip_blanks();
				types.push(parse_type(&mut self.c)?);
				self.c.skip_blanks();
				if !self.c.eat(",") {
					break;
				}
			}
			if types.len() != results.len() {
				return Err(error(
					line,
					format!("{} types for {} results", types.len(), results.len()),
				));
			}
		}

		self.c.skip_blanks();
		let loc = if self.c.eat("loc(") {
			Some(parse_loc(&mut self.c)?)
		} else {
			None
		};
		self.c.skip_blanks();
		let prov = if self.c.eat("prov(") {
			self.prov()?
		} else {
			None
		};

		self.c.skip_blanks();
		if self.c.peek() == Some(';') {
			self.c.rest_of_line();
		}
		self.end_of_line()?;
		Ok(OpNode {
			line,
			results,
			kind,
			operands,
			frame,
			snapshot,
			regions,
			types,
			loc,
			prov,
			attr_refs,
		})
	}

	/// Builds the kind of an op from the attribute text through the syntax of its dialect, with
	/// the op ids the attributes name. A dangling name counts toward the order of first appearance
	/// that provenance clauses keep.
	fn kind_of(
		&mut self,
		tag: OpTag,
		attrs: &str,
		line: u32,
	) -> Result<(OpKind, Vec<OpRef>), ParseError> {
		let at = dialect_index(tag);
		let Some(syntax) = self.syntaxes.0.get(at).copied().flatten() else {
			let dialect = DIALECT_NAMES.get(at).copied().unwrap_or("?");
			return Err(error(line, format!("no attribute syntax for {dialect}")));
		};
		let (kind, refs) = reading_attr_ids(|| syntax.parse_attrs(tag, attrs));
		let kind = kind.map_err(|failure| {
			if failure.line == 0 {
				error(line, failure.detail)
			} else {
				failure
			}
		})?;
		for named in &refs {
			if let OpRef::Dangling(k) = named {
				if *k > self.unresolved {
					return Err(error(
						line,
						"dangling ids are numbered in order of first appearance",
					));
				}
				if *k == self.unresolved {
					self.unresolved += 1;
				}
			}
		}
		Ok((kind, refs))
	}

	/// Reads a region after its `{`.
	fn region(&mut self, depth: usize) -> Result<RegionNode, ParseError> {
		let name = self.c.take_while(|c| !c.is_whitespace());
		let Some(kind) = region_kind_from_name(name) else {
			return Err(self.c.error(format!("unknown region kind `{name}`")));
		};
		self.c.skip_blanks();
		self.c.expect("^bb(")?;
		let mut args = Vec::new();
		self.c.skip_blanks();
		if !self.c.eat(")") {
			loop {
				let name = self.value_name()?;
				self.c.skip_blanks();
				let ty = if self.c.eat(":") {
					self.c.skip_blanks();
					parse_type(&mut self.c)?
				} else {
					TypeKnowledge::Unknown
				};
				args.push((name, ty));
				self.c.skip_blanks();
				if self.c.eat(",") {
					self.c.skip_blanks();
					continue;
				}
				self.c.expect(")")?;
				break;
			}
		}
		self.c.expect(":")?;
		self.end_of_line()?;
		let ops = self.block_body(depth + 1, false)?;
		self.c.expect("}")?;
		Ok(RegionNode {
			kind,
			args,
			ops,
		})
	}

	/// Reads a provenance clause after its `prov(`: `None` for `bound`.
	fn prov(&mut self) -> Result<Option<Vec<OpRef>>, ParseError> {
		if self.c.eat("bound)") {
			return Ok(None);
		}
		let mut refs = Vec::new();
		if self.c.eat(")") {
			return Ok(Some(refs));
		}
		loop {
			self.c.expect("@")?;
			if self.c.eat("?") {
				let k = self.c.number()?;
				if k > self.unresolved {
					return Err(self
						.c
						.error("dangling ids are numbered in order of first appearance"));
				}
				if k == self.unresolved {
					self.unresolved += 1;
				}
				refs.push(OpRef::Dangling(k));
			} else {
				refs.push(OpRef::Op(self.c.number()?));
			}
			if self.c.eat(",") {
				self.c.skip_blanks();
				continue;
			}
			self.c.expect(")")?;
			break;
		}
		Ok(Some(refs))
	}
}

/// Ids that name no op in `ir`: each key of an op inserted and removed again, which the arena
/// never hands to another op.
fn mint_dangling(ir: &mut Ir, count: u32) -> Vec<OpId> {
	let placeholder = || OpData {
		kind: OpKind::Expr(ExprOp::Yield),
		operands: SmallVec::new(),
		results: SmallVec::new(),
		regions: SmallVec::new(),
		frame: FrameId::new(0),
		snapshot: SnapshotId::new(0),
		loc: default_location(),
		provenance: Provenance::Bound,
		parent: BlockId::default(),
	};
	let ids: Vec<OpId> = (0..count).map(|_| ir.ops.insert(placeholder())).collect();
	for id in &ids {
		ir.ops.remove(*id);
	}
	ids
}

/// The location an op without a `loc` clause records.
fn default_location() -> Location {
	Location {
		stmt: 0,
		span: None,
		origin: LocOrigin::Query,
	}
}

/// Builds the top block of `ir` from `document` and interns its snapshots.
fn build(ir: &mut Ir, document: Document) -> Result<(), ParseError> {
	let Some(block) = ir.region(ir.top()).and_then(|top| top.blocks.first().copied()) else {
		return Err(error(0, "the top region holds no block"));
	};
	let dangling = mint_dangling(ir, document.dangling);
	let names = {
		let mut facts = FactCache::new();
		let mut builder = Builder {
			rw: Rewriter::new(ir, &mut facts),
			names: HashMap::new(),
			ops: Vec::new(),
			dangling,
			named_in_attrs: Vec::new(),
		};
		for node in document.ops {
			builder.op(node, block)?;
		}
		builder.resolve_attr_ids()?;
		builder.names
	};

	for (at, (line, number)) in document.snapshots.iter().enumerate() {
		let Some(value) = names.get(number) else {
			return Err(error(*line, format!("value %{number} is not defined")));
		};
		// Entries are pushed, not interned: rewriting can leave two entries naming one value, and
		// each keeps the number its readers print.
		debug_assert_eq!(ir.snapshots.len(), at + 1);
		ir.snapshots.push(Snapshot::Version(*value));
	}
	Rewriter::new(ir, &mut FactCache::new())
		.advance_phase(document.phase)
		.map_err(|failure| ir_error(0, &failure))
}

struct Builder<'a> {
	rw: Rewriter<'a>,
	/// The value each printed number names.
	names: HashMap<u32, ValueId>,
	/// The ops built so far, by print position.
	ops: Vec<OpId>,
	dangling: Vec<OpId>,
	/// The ops whose attributes name op ids, with the names, resolved once every op is built.
	named_in_attrs: Vec<(OpId, Vec<OpRef>, u32)>,
}

impl Builder<'_> {
	fn define(&mut self, name: u32, value: ValueId, line: u32) -> Result<(), ParseError> {
		if self.names.insert(name, value).is_some() {
			return Err(error(line, format!("value %{name} is defined twice")));
		}
		Ok(())
	}

	/// The ids a provenance clause names: an earlier op that stands for itself, or a dangling id.
	fn provenance_ids(&self, refs: &[OpRef], line: u32) -> Result<Vec<OpId>, ParseError> {
		let mut ids = Vec::with_capacity(refs.len());
		for held in refs {
			match held {
				OpRef::Op(n) => {
					let Some(id) =
						usize::try_from(*n).ok().and_then(|at| self.ops.get(at).copied())
					else {
						return Err(error(
							line,
							format!("@{n} is not an op built before this one"),
						));
					};
					let bound = self.rw.ir().op(id).map(|data| &data.provenance)
						== Some(&Provenance::Bound);
					if !bound {
						return Err(error(line, format!("@{n} does not stand for itself")));
					}
					ids.push(id);
				}
				OpRef::Dangling(k) => {
					let Some(id) =
						usize::try_from(*k).ok().and_then(|at| self.dangling.get(at).copied())
					else {
						return Err(error(line, format!("@?{k} is not a dangling id")));
					};
					ids.push(id);
				}
			}
		}
		Ok(ids)
	}

	fn op(&mut self, node: OpNode, block: BlockId) -> Result<(), ParseError> {
		let line = node.line;
		let mut operands = SmallVec::new();
		for name in &node.operands {
			let Some(value) = self.names.get(name) else {
				return Err(error(line, format!("value %{name} is not defined")));
			};
			operands.push(*value);
		}
		let results = if node.types.is_empty() {
			(0..node.results.len()).map(|_| TypeKnowledge::Unknown).collect()
		} else {
			node.types.iter().cloned().collect()
		};
		let spec = OpSpec {
			kind: node.kind,
			operands,
			results,
			frame: FrameId::new(node.frame),
			snapshot: SnapshotId::new(node.snapshot),
			loc: node.loc.unwrap_or_else(default_location),
		};
		let at = InsertPoint::End(block);
		let created = match &node.prov {
			None => self.rw.create(at, spec),
			Some(refs) => {
				let from = self.provenance_ids(refs, line)?;
				self.rw.derived(&from, |rw| rw.create(at, spec))
			}
		};
		let id = created.map_err(|failure| ir_error(line, &failure))?;
		self.ops.push(id);
		if !node.attr_refs.is_empty() {
			self.named_in_attrs.push((id, node.attr_refs, line));
		}
		let Some(values) = self.rw.ir().op(id).map(|data| data.results.clone()) else {
			return Err(error(line, "the created op is missing"));
		};
		for (name, value) in node.results.iter().zip(values) {
			self.define(*name, value, line)?;
		}
		for region in node.regions {
			self.region(id, region, line)?;
		}
		Ok(())
	}

	/// Replaces the placeholders the attributes of each op hold by the ops their names name: an op
	/// printed at a position, of any provenance, or a dangling id.
	fn resolve_attr_ids(&mut self) -> Result<(), ParseError> {
		for (op, refs, line) in ::core::mem::take(&mut self.named_in_attrs) {
			let mut resolved = Vec::with_capacity(refs.len());
			for named in refs {
				let (found, name) = match named {
					OpRef::Op(n) => {
						(usize::try_from(n).ok().and_then(|at| self.ops.get(at)), format!("@{n}"))
					}
					OpRef::Dangling(k) => (
						usize::try_from(k).ok().and_then(|at| self.dangling.get(at)),
						format!("@?{k}"),
					),
				};
				let Some(id) = found else {
					return Err(error(line, format!("{name} names no op of the text")));
				};
				resolved.push(*id);
			}
			self.rw
				.update_kind(op, |kind| {
					for slot in kind.attr_op_ids_mut() {
						if let Some(id) = resolved.get(attr_placeholder_index(*slot)) {
							*slot = *id;
						}
					}
				})
				.map_err(|failure| ir_error(line, &failure))?;
		}
		Ok(())
	}

	fn region(&mut self, owner: OpId, node: RegionNode, line: u32) -> Result<(), ParseError> {
		let types: Vec<TypeKnowledge> = node.args.iter().map(|(_, ty)| ty.clone()).collect();
		let (_, block) = self
			.rw
			.create_region(owner, node.kind, &types)
			.map_err(|failure| ir_error(line, &failure))?;
		let Some(values) = self.rw.ir().block(block).map(|data| data.args.clone()) else {
			return Err(error(line, "the created block is missing"));
		};
		for ((name, _), value) in node.args.iter().zip(values) {
			self.define(*name, value, line)?;
		}
		for op in node.ops {
			self.op(op, block)?;
		}
		Ok(())
	}
}
