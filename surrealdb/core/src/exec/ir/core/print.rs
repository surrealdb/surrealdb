//! The text form of a container.
//!
//! One op per line, regions in braces. Values are numbered in print order, never by id, because
//! ids are not stable across runs; frames and snapshots print as their intern tables at the head.
//! Printing then parsing then printing yields the text printed first, for every op tag.

use std::collections::HashMap;
use std::fmt::Write as _;

use slotmap::SecondaryMap;
use smallvec::SmallVec;

use super::facts::FactCache;
use super::ir::Ir;
use super::parse::ParseError;
use super::rewriter::Provenance;
use super::text::{
	OP_ID_MARK, dialect_index, phase_name, push_frame, push_loc, push_quoted, push_type,
	region_kind_name, writing_attr_ids,
};
#[cfg(test)]
use crate::exec::ir::analysis::effects;
use crate::exec::ir::iface::conversion::Phase;
use crate::exec::ir::iface::frame::Snapshot;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::ir::registry;

/// What a print shows beyond the ops and their attributes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PrintOpts {
	pub(crate) locations: bool,
	pub(crate) provenance: bool,
	/// Whether each op prints its effects. Only a test build prints them.
	pub(crate) facts: bool,
}

/// The text form of the attributes of one dialect's ops, supplied by the dialect.
///
/// `print_attrs` appends the attributes of `kind` structurally, and nothing for an op without
/// any; `parse_attrs` reads them back into the kind `tag` names, from the text `print_attrs`
/// appended (empty when it appended nothing).
pub(crate) trait AttrSyntax: Send + Sync {
	fn print_attrs(&self, kind: &OpKind, out: &mut String);
	fn parse_attrs(&self, tag: OpTag, text: &str) -> Result<OpKind, ParseError>;
}

/// The attribute syntax of each dialect, indexed expression, logical, physical as
/// [`dialect_index`] numbers them.
///
/// The table is passed to the printer and the parser rather than held by the registry, so the
/// syntaxes are linked only into a build that prints or parses IR.
pub(crate) struct AttrSyntaxes(pub(crate) [Option<&'static dyn AttrSyntax>; 3]);

impl AttrSyntaxes {
	/// A table holding no syntax: every dialect prints `attrs("?")` and parses nothing.
	pub(crate) const NONE: Self = Self([None; 3]);
}

/// The text form of the ops beneath `root`, with the attribute syntaxes of the built-in dialects.
///
/// See [`print_with`].
pub(crate) fn print(ir: &Ir, root: RegionId, opts: PrintOpts) -> String {
	print_with(ir, root, opts, registry::attr_syntaxes())
}

/// The text form of the ops beneath `root`, with the attribute syntaxes `syntaxes` holds.
///
/// The top region prints the `ir` header, with the container's phase past [`Phase::Bound`], the
/// frame and snapshot tables and its ops. Any other region prints as one braced region at depth
/// zero, and its text is not a program. Values print as `%<n>` and ops as `@<n>` in print order.
/// A value or op the printed text does not hold prints as `%?<k>` or `@?<k>`, numbered in order
/// of first appearance. Locations, provenance and facts print only when `opts` asks. A dialect
/// without an attribute syntax prints `attrs("?")`. The text always ends in a newline.
pub(crate) fn print_with(
	ir: &Ir,
	root: RegionId,
	opts: PrintOpts,
	syntaxes: &AttrSyntaxes,
) -> String {
	let Some(region) = ir.region(root) else {
		return "; no such region\n".to_owned();
	};
	let mut printer = Printer {
		ir,
		syntaxes,
		opts,
		out: String::new(),
		scratch: String::new(),
		values: SecondaryMap::new(),
		ops: SecondaryMap::new(),
		next_value: 0,
		next_op: 0,
		emitted: 0,
		outside_values: HashMap::new(),
		outside_ops: HashMap::new(),
		facts: FactCache::new(),
	};
	printer.number_region(root);
	if root == ir.top() {
		printer.header(region.kind);
		printer.body(root, 0);
	} else {
		printer.region(root, 0, false);
		printer.out.push('\n');
	}
	printer.out
}

struct Printer<'a> {
	ir: &'a Ir,
	syntaxes: &'a AttrSyntaxes,
	opts: PrintOpts,
	out: String,
	/// Holds the attribute text of one op while it is escaped.
	scratch: String,
	values: SecondaryMap<ValueId, u32>,
	ops: SecondaryMap<OpId, u32>,
	next_value: u32,
	next_op: u32,
	/// The number of ops printed so far; an op prints when its number is this one.
	emitted: u32,
	outside_values: HashMap<ValueId, u32>,
	outside_ops: HashMap<OpId, u32>,
	facts: FactCache,
}

/// The count of `len`, saturating at `u32::MAX`.
fn count(len: usize) -> u32 {
	u32::try_from(len).unwrap_or(u32::MAX)
}

impl Printer<'_> {
	/// Numbers the values and ops of `region` in print order: an op, its results, then the regions
	/// it owns, each with its block arguments first.
	///
	/// An op met a second time, which happens only when a region or op is listed twice in a
	/// container, keeps its first number and is not entered again, so a cyclic container
	/// terminates.
	fn number_region(&mut self, region: RegionId) {
		let ir = self.ir;
		let Some(data) = ir.region(region) else {
			return;
		};
		for block in &data.blocks {
			let Some(block) = ir.block(*block) else {
				continue;
			};
			for arg in &block.args {
				self.number_value(*arg);
			}
			for op in &block.ops {
				self.number_op(*op);
			}
		}
	}

	fn number_value(&mut self, value: ValueId) {
		if self.values.get(value).is_none() {
			self.values.insert(value, self.next_value);
			self.next_value += 1;
		}
	}

	fn number_op(&mut self, op: OpId) {
		let ir = self.ir;
		let Some(data) = ir.op(op) else {
			return;
		};
		if self.ops.get(op).is_some() {
			return;
		}
		self.ops.insert(op, self.next_op);
		self.next_op += 1;
		for result in &data.results {
			self.number_value(*result);
		}
		for region in &data.regions {
			self.number_region(*region);
		}
	}

	fn indent(&mut self, depth: usize) {
		for _ in 0..depth {
			self.out.push('\t');
		}
	}

	fn header(&mut self, top: RegionKind) {
		let ir = self.ir;
		match ir.phase() {
			Phase::Bound => {
				let _ = writeln!(self.out, "ir {}", region_kind_name(top));
			}
			phase @ (Phase::Logical | Phase::Physical | Phase::Lowered) => {
				let _ =
					writeln!(self.out, "ir {} phase({})", region_kind_name(top), phase_name(phase));
			}
		}
		for (at, frame) in ir.frames().frames.iter().enumerate() {
			let _ = write!(self.out, "#f{at} = ");
			push_frame(&mut self.out, frame);
			self.out.push('\n');
		}
		for (at, snapshot) in ir.snapshots.iter().enumerate() {
			let _ = write!(self.out, "#s{at} = ");
			match snapshot {
				Snapshot::Current => self.out.push_str("current\n"),
				Snapshot::Version(value) => {
					self.out.push_str("version(");
					self.value(*value);
					self.out.push_str(")\n");
				}
			}
		}
	}

	/// Prints the ops of the blocks of `region`, one line each, at `depth`.
	fn body(&mut self, region: RegionId, depth: usize) {
		let ir = self.ir;
		let Some(data) = ir.region(region) else {
			return;
		};
		for block in &data.blocks {
			let Some(block) = ir.block(*block) else {
				continue;
			};
			for op in &block.ops {
				self.op(*op, depth);
			}
		}
	}

	/// Appends `%<n>`, or `%?<k>` for a value the text does not define.
	fn value(&mut self, value: ValueId) {
		if let Some(n) = self.values.get(value) {
			let _ = write!(self.out, "%{n}");
		} else {
			let next = count(self.outside_values.len());
			let k = *self.outside_values.entry(value).or_insert(next);
			let _ = write!(self.out, "%?{k}");
		}
	}

	/// Appends `^bb(<args>):` and a newline, each argument with its type unless it is unknown.
	fn block_args(&mut self, args: &[ValueId]) {
		let ir = self.ir;
		self.out.push_str("^bb(");
		for (at, arg) in args.iter().enumerate() {
			if at > 0 {
				self.out.push_str(", ");
			}
			self.value(*arg);
			if let Some(value) = ir.value(*arg)
				&& value.ty != TypeKnowledge::Unknown
			{
				self.out.push_str(": ");
				push_type(&mut self.out, &value.ty);
			}
		}
		self.out.push_str("):\n");
	}

	/// Prints `{<kind> ^bb(<args>):`, the ops of the block at `depth + 1` and the closing `}`. A
	/// region with a second block prints its further blocks as `^bb(<args>):` lines, which the
	/// parser does not read. `inline` puts a space before the brace.
	fn region(&mut self, region: RegionId, depth: usize, inline: bool) {
		let ir = self.ir;
		if inline {
			self.out.push(' ');
		}
		let Some(data) = ir.region(region) else {
			self.out.push_str("{missing region}");
			return;
		};
		self.out.push('{');
		self.out.push_str(region_kind_name(data.kind));
		self.out.push(' ');
		for (at, block) in data.blocks.iter().enumerate() {
			let Some(block) = ir.block(*block) else {
				continue;
			};
			if at > 0 {
				self.indent(depth + 1);
			}
			self.block_args(&block.args);
			for op in &block.ops {
				self.op(*op, depth + 1);
			}
		}
		if data.blocks.is_empty() {
			self.out.push_str("^bb():\n");
		}
		self.indent(depth);
		self.out.push('}');
	}

	fn op(&mut self, id: OpId, depth: usize) {
		let ir = self.ir;
		let Some(op) = ir.op(id) else {
			self.indent(depth);
			self.out.push_str("; dangling op\n");
			return;
		};
		if self.ops.get(id).copied() != Some(self.emitted) {
			self.indent(depth);
			self.out.push_str("; repeated op\n");
			return;
		}
		self.emitted += 1;
		self.indent(depth);

		for (at, result) in op.results.iter().enumerate() {
			if at > 0 {
				self.out.push_str(", ");
			}
			self.value(*result);
		}
		if !op.results.is_empty() {
			self.out.push_str(" = ");
		}
		let tag = op.kind.tag();
		self.out.push_str(tag.mnemonic());
		self.out.push('(');
		for (at, operand) in op.operands.iter().enumerate() {
			if at > 0 {
				self.out.push_str(", ");
			}
			self.value(*operand);
		}
		self.out.push(')');

		self.attrs(&op.kind, tag);
		if op.frame.index() != 0 || op.snapshot.index() != 0 {
			let _ =
				write!(self.out, " [frame=#f{} snap=#s{}]", op.frame.index(), op.snapshot.index());
		}
		for region in &op.regions {
			self.region(*region, depth, true);
		}

		let typed = op.results.iter().any(|result| {
			ir.value(*result).is_some_and(|value| value.ty != TypeKnowledge::Unknown)
		});
		if typed {
			self.out.push_str(" : ");
			for (at, result) in op.results.iter().enumerate() {
				if at > 0 {
					self.out.push(',');
				}
				match ir.value(*result) {
					Some(value) => push_type(&mut self.out, &value.ty),
					None => self.out.push_str("unknown"),
				}
			}
		}
		if self.opts.locations {
			push_loc(&mut self.out, &op.loc);
		}
		if self.opts.provenance {
			self.provenance(&op.provenance);
		}
		// The effects print under the full builtin registry, which only a test prints them under.
		#[cfg(test)]
		if self.opts.facts {
			let facts = effects::of(ir, &mut self.facts, id);
			let _ = write!(self.out, " ; facts(effects: {facts:?})");
		}
		self.out.push('\n');
	}

	/// Appends ` attrs("<text>")` for the text the dialect's syntax appends, and nothing when it
	/// appends none. An op id the attributes hold is named as provenance names it.
	fn attrs(&mut self, kind: &OpKind, tag: OpTag) {
		let syntax = self.syntaxes.0.get(dialect_index(tag)).copied().flatten();
		self.scratch.clear();
		let mut ids = Vec::new();
		match syntax {
			Some(syntax) => {
				let scratch = &mut self.scratch;
				ids = writing_attr_ids(|| syntax.print_attrs(kind, scratch));
			}
			None => self.scratch.push('?'),
		}
		if !ids.is_empty() {
			let marked = ::core::mem::take(&mut self.scratch);
			for (at, piece) in marked.split(OP_ID_MARK).enumerate() {
				let named = (at % 2 == 1)
					.then(|| piece.parse::<usize>().ok().and_then(|index| ids.get(index)))
					.flatten();
				match named {
					Some(id) => {
						let name = self.op_name(*id);
						self.scratch.push_str(&name);
					}
					None => self.scratch.push_str(piece),
				}
			}
		}
		if !self.scratch.is_empty() {
			self.out.push_str(" attrs(");
			push_quoted(&mut self.out, &self.scratch);
			self.out.push(')');
		}
	}

	/// `@<n>` for an op the text holds, else `@?<k>`, numbered in order of first appearance.
	fn op_name(&mut self, id: OpId) -> String {
		match self.ops.get(id) {
			Some(number) => format!("@{number}"),
			None => {
				let next = count(self.outside_ops.len());
				let k = *self.outside_ops.entry(id).or_insert(next);
				format!("@?{k}")
			}
		}
	}

	/// Appends ` prov(bound)` or ` prov(<ids>)`: the ops the text holds by number, then the ids it
	/// does not.
	fn provenance(&mut self, provenance: &Provenance) {
		let ids = match provenance {
			Provenance::Bound => {
				self.out.push_str(" prov(bound)");
				return;
			}
			Provenance::From(ids) => ids,
		};
		let mut held: SmallVec<[u32; 4]> = SmallVec::new();
		let mut outside: SmallVec<[OpId; 2]> = SmallVec::new();
		for id in ids {
			match self.ops.get(*id) {
				Some(number) => held.push(*number),
				None => outside.push(*id),
			}
		}
		held.sort_unstable();
		self.out.push_str(" prov(");
		let mut first = true;
		for number in held {
			if !first {
				self.out.push(',');
			}
			first = false;
			let _ = write!(self.out, "@{number}");
		}
		for id in outside {
			if !first {
				self.out.push(',');
			}
			first = false;
			let next = count(self.outside_ops.len());
			let k = *self.outside_ops.entry(id).or_insert(next);
			let _ = write!(self.out, "@?{k}");
		}
		self.out.push(')');
	}
}
