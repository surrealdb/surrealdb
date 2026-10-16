//! Lowering of the loops that walk a path: the broadcast of a path over the elements of a value,
//! and the graph step.
//!
//! # Broadcast
//!
//! A `phys.loop` of kind `Broadcast` applies its body to each element of an array or a set, in
//! order, and collects the results into an empty collection of the same kind. Any other value is
//! the one element the body is applied to, and the body's one result is the broadcast's own. The
//! body reads the element as its first block argument and the element's position as its second.
//!
//! The body is lowered once, for both cases: a value that is neither an array nor a set enters
//! the body as its one element, and its result leaves the loop without being collected. A loop
//! that states [`StepAttrs::Flatten`](crate::exec::ir::logical::ops::StepAttrs::Flatten) splices
//! the arrays its results are into the collection by one level when the elements have been walked;
//! the result of a lone value is never spliced.
//!
//! A body that applies `.*` to the element itself takes that step, while it walks a collection,
//! only over an element that is a record id: any other element is its own step's value, which
//! `expr.each` is not, because over a value that is not a record id it answers `NONE` or maps
//! over it. Such a body is lowered for each of the two paths: for the elements, with the step
//! guarded ([`Lowerer::each_guard`]), and for a lone value, with the step as written.
//!
//! # Graph step
//!
//! A `phys.loop` of kind `GraphStep` is one hop of a graph or reference lookup
//! ([`StepAttrs::Lookup`](crate::exec::ir::logical::ops::StepAttrs::Lookup)). A chain of hops, each
//! reading the result of the one before, is one [`Instr::Lookup`] over a
//! [`LookupSpec`] that holds a hop for each loop, in path order: the loop that ends the chain
//! lowers the whole of it, and the loops it holds lower nothing of their own. A loop joins the
//! chain of the loop that reads it when both stand in one region and one authority frame, it does
//! not end a run of hops of its own path, and it has no `ONLY` under a page. The lookup reads in
//! the authority frame its loop stands in: inside a permission predicate, the predicate's frame,
//! whose reads are not permission-checked, and otherwise the caller's.
//!
//! A hop states nothing but its subjects, `ONLY`, an optional `WHERE` and an optional `*`
//! projection, with a name it may be given. The last hop of a chain may page its records by
//! `LIMIT` and `START` that are written as literal non-negative integers, when no hop of the path
//! has `ONLY`: the page is what the lookup answers over all its origins together. `ONLY` on a
//! lookup of the path below a run of hops is stated by the run's last hop
//! ([`LookupAttrs::only_path`]): the lookup answers one value over all its origins together. The
//! binder builds a lookup with any other clause as the statement of its clauses over the records it
//! reaches (`ir::build::lookup`), so a hop of another form is an invariant break. A graph lookup
//! that names a referencing field has no lowering, and is refused with an error.
//!
//! The hops read nothing from the catalog. A hop holds no per-table permission or field view,
//! so each record a hop reaches is read by a scan of its table, which resolves the table's
//! permission and field view when it reads the record. The `WHERE` of a hop is a host pipeline
//! over the record reached, whose block argument is the row. A hop the physical plan gave a key
//! condition ([`crate::exec::ir::phys::hop_key`]) judges that pipeline over each adjacency entry
//! instead, before its edge is read. The bounds of a range subject are host scalars, evaluated
//! with no row when the hop is taken. The program needs the database context.

#![deny(clippy::wildcard_enum_match_arm)]

use std::ops::Bound;
use std::sync::Arc;

use super::super::plain::ValueData;
use super::super::program::{
	CompareOp, EdgeSpec, HopRead, HostScalar, Instr, KeyCheck, KeyCond, KeyShape as RecordKey,
	LookupHop, LookupPage, LookupSpec, NavOp, Operand, ReferenceSubject,
};
use super::expr::{Lowerer, exactly, invariant, region_at};
use crate::exec::ContextLevel;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, KeyBound};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::statements::OperandSlot;
use crate::exec::ir::logical::ops::{FieldShape, FieldsShape, LookupAttrs, LoopKind, StepAttrs};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::hop_key::HopKey;
use crate::exec::ir::phys::ops::PhysOp;
use crate::expr::Dir;
use crate::expr::lookup::LookupKind;
use crate::val::{Number, TableName, Value};

impl Lowerer<'_> {
	/// The value the broadcast `data` yields.
	pub(super) fn broadcast(&mut self, data: &OpData, flatten: bool) -> Result<Operand, IrError> {
		let [source] = exactly::<1>(data)?;
		let body = region_at(data, 0, 1)?;
		if self.applies_each_to_the_element(body)? {
			return self.broadcast_each(source, body, flatten);
		}
		let src = self.value(source)?;
		let element = self.em.reg()?;
		self.emit(Instr::Load {
			dst: element,
			src,
		});
		let single = self.em.reg()?;
		let no = self.em.constant(Value::from(false))?;
		self.emit(Instr::Load {
			dst: single,
			src: Operand::Const(no),
		});
		let items = self.em.reg()?;
		let dst = self.em.reg()?;
		let other = self.em.label()?;
		let done = self.em.label()?;
		let unwrapped = self.em.label()?;
		let end = self.em.label()?;
		self.emit(Instr::MapBegin {
			src: Operand::Reg(element),
			items,
			dst,
			other,
			sets: true,
		});
		let pos = self.em.reg()?;
		let zero = self.em.constant(Value::from(0i64))?;
		self.emit(Instr::Load {
			dst: pos,
			src: Operand::Const(zero),
		});
		let index = self.em.reg()?;
		let top = self.em.label()?;
		self.em.bind(top)?;
		self.emit(Instr::ElemNext {
			array: items,
			pos,
			elem: element,
			index,
			done,
		});
		let body_start = self.em.label()?;
		self.em.bind(body_start)?;
		let mapped = self.element_value(body, element, &[Operand::Reg(index)])?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(single),
			truthy: true,
			target: unwrapped,
		});
		self.emit(Instr::CollectPush {
			collection: dst,
			value: mapped,
		});
		self.emit(Instr::Jump {
			target: top,
		});
		self.em.bind(done)?;
		if flatten {
			self.emit(Instr::Nav {
				op: NavOp::Flatten,
				src: Operand::Reg(dst),
				index: None,
				dst,
			});
		}
		self.emit(Instr::Jump {
			target: end,
		});
		// Anything but an array or a set: the body runs once, over the value itself.
		self.em.bind(other)?;
		let yes = self.em.constant(Value::from(true))?;
		self.emit(Instr::Load {
			dst: single,
			src: Operand::Const(yes),
		});
		self.emit(Instr::Jump {
			target: body_start,
		});
		self.em.bind(unwrapped)?;
		self.emit(Instr::Load {
			dst,
			src: mapped,
		});
		self.em.bind(end)?;
		Ok(Operand::Reg(dst))
	}

	/// [`Lowerer::broadcast`] of a body that applies `.*` to its element: over an array or a set,
	/// that step is taken only over an element that is a record id, and any other element is its
	/// own step's value; over anything else, the body runs once over the value, with the step as
	/// written. The body is lowered for each of the two paths.
	fn broadcast_each(
		&mut self,
		source: ValueId,
		body: RegionId,
		flatten: bool,
	) -> Result<Operand, IrError> {
		let Some(&element_arg) = self.region_args(body)?.first() else {
			return Err(IrError::Stale("ir_lower::graph: broadcast arguments"));
		};
		let src = self.value(source)?;
		let held = self.em.reg()?;
		self.emit(Instr::Load {
			dst: held,
			src,
		});
		let items = self.em.reg()?;
		let dst = self.em.reg()?;
		let other = self.em.label()?;
		let done = self.em.label()?;
		let end = self.em.label()?;
		self.emit(Instr::MapBegin {
			src: Operand::Reg(held),
			items,
			dst,
			other,
			sets: true,
		});
		let pos = self.em.reg()?;
		let zero = self.em.constant(Value::from(0i64))?;
		self.emit(Instr::Load {
			dst: pos,
			src: Operand::Const(zero),
		});
		let element = self.em.reg()?;
		let index = self.em.reg()?;
		let top = self.em.label()?;
		self.em.bind(top)?;
		self.emit(Instr::ElemNext {
			array: items,
			pos,
			elem: element,
			index,
			done,
		});
		let outer = self.each_guard.replace(element_arg);
		let mapped = self.element_value(body, element, &[Operand::Reg(index)]);
		self.each_guard = outer;
		self.emit(Instr::CollectPush {
			collection: dst,
			value: mapped?,
		});
		self.emit(Instr::Jump {
			target: top,
		});
		self.em.bind(done)?;
		if flatten {
			self.emit(Instr::Nav {
				op: NavOp::Flatten,
				src: Operand::Reg(dst),
				index: None,
				dst,
			});
		}
		self.emit(Instr::Jump {
			target: end,
		});
		// Anything but an array or a set: the body runs once, over the value itself.
		self.em.bind(other)?;
		let position = self.em.reg()?;
		let once = self.element_value(body, held, &[Operand::Reg(position)])?;
		self.emit(Instr::Load {
			dst,
			src: once,
		});
		self.em.bind(end)?;
		Ok(Operand::Reg(dst))
	}

	/// Whether an op of `body` applies `expr.each` to the body's first block argument, the
	/// element.
	fn applies_each_to_the_element(&self, body: RegionId) -> Result<bool, IrError> {
		let Some(&element) = self.region_args(body)?.first() else {
			return Err(IrError::Stale("ir_lower::graph: broadcast arguments"));
		};
		Ok(ops_in(self.ir, body).any(|op| {
			self.ir.op(op).is_some_and(|data| {
				matches!(data.kind, OpKind::Expr(ExprOp::Each))
					&& data.operands.first() == Some(&element)
			})
		}))
	}
}

/// A hop of a graph lookup: the loop that states it.
#[derive(Clone, Copy)]
struct Hop<'a> {
	/// The loop.
	op: OpId,
	data: &'a OpData,
	attrs: &'a LookupAttrs,
	/// The region the loop stands in.
	region: RegionId,
}

/// The key condition `key` states over the host pipeline `pipeline` of the hop's `WHERE`. Its
/// comparisons are checked in place of the pipeline only when every one compares by an operator
/// the check applies.
fn key_cond(pipeline: u16, key: &HopKey) -> KeyCond {
	let checks = key.checks.as_ref().and_then(|checks| {
		checks
			.iter()
			.map(|check| {
				Some(KeyCheck {
					field: check.field.clone(),
					op: CompareOp::of(&check.op)?,
					value: ValueData::new(check.value.clone(), "a key check").ok()?,
					swapped: check.swapped,
				})
			})
			.collect()
	});
	KeyCond {
		pipeline,
		needs_id: key.needs_id,
		needs_in: key.needs_in,
		needs_out: key.needs_out,
		payload: key.payload.clone(),
		checks,
	}
}

/// How a hop pages the records it reaches.
enum Paging {
	/// The hop does not page.
	None,
	/// `LIMIT` and `START` are written as literal non-negative integers.
	Literal(LookupPage),
	/// A paging value is computed, or is not a non-negative integer.
	Computed,
}

/// Why a hop with clauses of its own is not one hop of a lookup: the binder builds such a lookup as
/// the statement of its clauses (`ir::build::lookup`), never as a hop.
const HOP_STATEMENT: &str =
	"ir_lower::graph: a lookup with clauses of its own, which the binder builds as a statement";

/// A graph lookup that names a referencing field. The lowering has no form for it, so it is
/// refused with an error.
const NO_LOWERING: &str = "a lookup with no lowering here";

/// Why a hop that ends a chain is not one hop of a lookup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NotAHop {
	/// A lookup with clauses of its own ([`HOP_STATEMENT`]).
	Statement,
	/// A graph lookup that names a referencing field ([`NO_LOWERING`]).
	ReferencingField,
}

impl NotAHop {
	/// The error the step that ends a chain answers.
	fn error(self) -> IrError {
		match self {
			NotAHop::Statement => invariant(HOP_STATEMENT),
			NotAHop::ReferencingField => IrError::Unsupported {
				reason: NO_LOWERING.to_owned(),
			},
		}
	}
}

/// The hop the loop that defines `value` states, when it is a graph step.
fn hop_of(ir: &Ir, value: ValueId) -> Option<Hop<'_>> {
	let ValueDef::OpResult {
		op,
		index: 0,
	} = ir.value(value)?.def
	else {
		return None;
	};
	let data = ir.op(op)?;
	let OpKind::Phys(PhysOp::Loop {
		kind: LoopKind::GraphStep,
		step,
		..
	}) = &data.kind
	else {
		return None;
	};
	let Some(StepAttrs::Lookup(attrs)) = step.as_deref() else {
		return None;
	};
	Some(Hop {
		op,
		data,
		attrs,
		region: ir.enclosing_region(op),
	})
}

/// How `hop` pages: the integers `LIMIT` and `START` name when both are literals.
fn paging(ir: &Ir, hop: Hop<'_>) -> Paging {
	let attrs = hop.attrs;
	if attrs.limit.is_none() && attrs.start.is_none() {
		return Paging::None;
	}
	let literal = |slot: OperandSlot| -> Option<usize> {
		let value = *hop.data.operands.get(usize::from(slot))?;
		let ValueDef::OpResult {
			op,
			index: 0,
		} = ir.value(value)?.def
		else {
			return None;
		};
		let OpKind::Expr(ExprOp::Literal(literal)) = &ir.op(op)?.kind else {
			return None;
		};
		let Value::Number(Number::Int(n)) = &**literal else {
			return None;
		};
		usize::try_from(*n).ok()
	};
	let limit = match attrs.limit {
		Some(slot) => match literal(slot) {
			Some(limit) => Some(limit),
			None => return Paging::Computed,
		},
		None => None,
	};
	let start = match attrs.start {
		Some(slot) => match literal(slot) {
			Some(start) => start,
			None => return Paging::Computed,
		},
		None => 0,
	};
	Paging::Literal(LookupPage {
		start,
		limit,
	})
}

/// Whether `fields` is the `*` projection alone: the records shown whole.
fn is_whole_record(fields: &FieldsShape) -> bool {
	matches!(fields, FieldsShape::Select(list) if matches!(&list[..], [FieldShape::All]))
}

/// Why `hop` is not one hop of a lookup, or `None` when it is. The last hop of a chain may page.
fn refusal(ir: &Ir, hop: Hop<'_>, last: bool) -> Option<NotAHop> {
	let attrs = hop.attrs;
	if attrs.split.is_some() || attrs.group.is_some() || attrs.order.is_some() {
		return Some(NotAHop::Statement);
	}
	if attrs.fields.as_ref().is_some_and(|fields| !is_whole_record(fields)) {
		return Some(NotAHop::Statement);
	}
	match paging(ir, hop) {
		Paging::None => {}
		Paging::Literal(_) => {
			if !last || attrs.fields.is_some() || attrs.cond || attrs.only {
				return Some(NotAHop::Statement);
			}
		}
		Paging::Computed => return Some(NotAHop::Statement),
	}
	let names_a_field = attrs.subjects.iter().any(|subject| subject.referencing_field.is_some());
	if names_a_field && !matches!(attrs.kind, LookupKind::Reference) {
		return Some(NotAHop::ReferencingField);
	}
	None
}

/// `table` encoded as the leading segment of a cached entry that names it.
fn encoded(table: &TableName) -> Result<Vec<u8>, IrError> {
	storekey::encode_vec(table)
		.map_err(|_| IrError::Stale("ir_lower::graph: a table name that does not encode"))
}

impl Lowerer<'_> {
	/// The value the chain of hops that the graph step `data` ends yields: one [`Instr::Lookup`].
	///
	/// The step is the last hop. Walking back through first operands, each loop that defines one
	/// joins the chain while it stands in the same region and authority frame, is a hop
	/// ([`refusal`]), does not end a run of its own ([`LookupAttrs::only_path`]), and, under a
	/// page, has no `ONLY`. The first operand of the first hop is the value the lookup starts
	/// from, read as a value of its own.
	pub(super) fn graph_step(
		&mut self,
		data: &OpData,
		attrs: &LookupAttrs,
	) -> Result<Operand, IrError> {
		let ir = self.ir;
		let result =
			*data.results.first().ok_or(IrError::Stale("ir_lower::graph: a step result"))?;
		let Some(last) = hop_of(ir, result) else {
			return Err(IrError::Stale("ir_lower::graph: a step that is not a hop"));
		};
		if !std::ptr::eq(last.attrs, attrs) {
			return Err(IrError::Stale("ir_lower::graph: a step that is not the one lowered"));
		}
		if let Some(not_a_hop) = refusal(ir, last, true) {
			return Err(not_a_hop.error());
		}
		let page = match paging(ir, last) {
			Paging::Literal(page) => Some(page),
			Paging::None => None,
			Paging::Computed => return Err(IrError::Stale("ir_lower::graph: a computed page")),
		};
		let mut chain = vec![last];
		let mut source = first_operand(last.data)?;
		while let Some(inner) = hop_of(ir, source) {
			// A page covers everything one path reaches. The binder builds a paged path that has
			// `ONLY` as a statement, so a hop under a page that has `ONLY` is a value of its
			// own.
			if inner.region != last.region
				|| inner.data.frame != last.data.frame
				|| refusal(ir, inner, false).is_some()
				|| inner.attrs.only_path
				|| (page.is_some() && inner.attrs.only)
			{
				break;
			}
			source = first_operand(inner.data)?;
			chain.push(inner);
		}
		chain.reverse();
		let only = chain.iter().any(|hop| hop.attrs.only);
		if page.is_some() && (only || last.attrs.only_path) {
			return Err(invariant(HOP_STATEMENT));
		}
		let mut hops = Vec::with_capacity(chain.len());
		for hop in &chain {
			hops.push(self.lookup_hop(*hop)?);
		}
		let spec = LookupSpec {
			hops,
			extract_id: last.attrs.cond && last.attrs.fields.is_none(),
			only,
			only_path: last.attrs.only_path,
			checked: true,
			page,
		};
		let src = self.value(source)?;
		let lookup = self.em.lookup(spec)?;
		let dst = self.em.reg()?;
		self.emit(Instr::Lookup {
			lookup,
			src,
			dst,
			frame: self.frame.program,
		});
		self.requires(ContextLevel::Database);
		Ok(Operand::Reg(dst))
	}

	/// The hop `hop` states: what it reads from each record it is handed, and the host pipeline of
	/// its `WHERE` over each record it reaches.
	///
	/// Its operands after the source are the operands of the key bounds of its range subjects,
	/// subject by subject, start before end, and then its paging values, which the hop does not
	/// read. The `WHERE` reads the row of the pipeline the lookup stands in as `$parent`, which the
	/// lookup binds for every hop's `WHERE`; it reads no other value of that pipeline.
	fn lookup_hop(&mut self, hop: Hop<'_>) -> Result<LookupHop, IrError> {
		let attrs = hop.attrs;
		let regions = 1 + usize::from(attrs.cond);
		if hop.data.regions.len() != regions {
			return Err(IrError::Stale("ir_lower::graph: a hop's regions"));
		}
		let mut operands = hop.data.operands.iter().copied();
		operands.next();
		let read = match &attrs.kind {
			LookupKind::Graph(direction) => {
				let mut specs = Vec::with_capacity(attrs.subjects.len());
				for subject in attrs.subjects.iter() {
					let (start, end) =
						self.subject_bounds(subject.range.as_ref(), &mut operands)?;
					specs.push(EdgeSpec {
						table: subject.table.clone(),
						start,
						end,
					});
				}
				let prefixes =
					specs.iter().map(|spec| encoded(&spec.table)).collect::<Result<Vec<_>, _>>()?;
				let directions = match direction {
					Dir::Out => vec![Dir::Out],
					Dir::In => vec![Dir::In],
					Dir::Both => vec![Dir::In, Dir::Out],
				};
				HopRead::graph(directions, specs, prefixes, None, Vec::new())
			}
			LookupKind::Reference => {
				let mut subjects = Vec::with_capacity(attrs.subjects.len().max(1));
				for subject in attrs.subjects.iter() {
					let (start, end) =
						self.subject_bounds(subject.range.as_ref(), &mut operands)?;
					let mut prefix = encoded(&subject.table)?;
					if let Some(field) = &subject.referencing_field {
						prefix.extend(storekey::encode_vec(field.as_str()).map_err(|_| {
							IrError::Stale("ir_lower::graph: a field name that does not encode")
						})?);
					}
					subjects.push(ReferenceSubject {
						table: Some(subject.table.clone()),
						field: subject.referencing_field.clone(),
						start,
						end,
						prefix: Some(prefix),
					});
				}
				// `<~?` is every table that references the record.
				if subjects.is_empty() {
					subjects.push(ReferenceSubject {
						table: None,
						field: None,
						start: Bound::Unbounded,
						end: Bound::Unbounded,
						prefix: None,
					});
				}
				HopRead::References(subjects)
			}
		};
		let paged = usize::from(attrs.limit.is_some()) + usize::from(attrs.start.is_some());
		if operands.count() != paged {
			return Err(IrError::Stale("ir_lower::graph: a hop's operands"));
		}
		let key = self.ir.hop_key(hop.op);
		let cond = match attrs.cond {
			true => {
				let region = region_at(hop.data, 1, regions)?;
				let parents = self
					.ir
					.region_inputs(region)?
					.into_iter()
					.filter(|value| self.is_pipeline_row(*value))
					.collect();
				let outer = self.em.read_as_parent(parents);
				let cond = self.host_code_over_row(region);
				self.em.restore_parent_rows(outer);
				Some(cond?)
			}
			false => None,
		};
		let (cond, key_cond) = match (cond, key) {
			(Some(pipeline), Some(key)) => (None, Some(key_cond(pipeline, key))),
			(cond, _) => (cond, None),
		};
		Ok(LookupHop {
			read: Arc::new(read),
			full: cond.is_some() || attrs.fields.is_some(),
			tables: Vec::new(),
			cond,
			key_cond,
		})
	}

	/// The bounds of the key range of a subject, each a host scalar the hop evaluates with no row;
	/// both unbounded for a subject with none. `operands` yields the values the bounds name.
	fn subject_bounds(
		&mut self,
		range: Option<&crate::exec::ir::expr::ops::KeyRange>,
		operands: &mut std::iter::Copied<std::slice::Iter<'_, ValueId>>,
	) -> Result<(Bound<HostScalar>, Bound<HostScalar>), IrError> {
		let Some(range) = range else {
			return Ok((Bound::Unbounded, Bound::Unbounded));
		};
		let start = self.bound_scalar(&range.start, operands)?;
		let end = self.bound_scalar(&range.end, operands)?;
		Ok((start, end))
	}

	/// One bound of a key range as a host scalar. A key of literal parts is a constant. An array
	/// or an object key is a host pipeline that builds it from the values it names, which no row
	/// is read for. A generated key has no value to bound by and answers an invariant error.
	fn bound_scalar(
		&mut self,
		bound: &Bound<KeyBound>,
		operands: &mut std::iter::Copied<std::slice::Iter<'_, ValueId>>,
	) -> Result<Bound<HostScalar>, IrError> {
		let (key, wrap): (_, fn(HostScalar) -> Bound<HostScalar>) = match bound {
			Bound::Unbounded => return Ok(Bound::Unbounded),
			Bound::Included(key) => (key, Bound::Included),
			Bound::Excluded(key) => (key, Bound::Excluded),
		};
		let constant = |value: Value| {
			ValueData::new(value, "a hop's key bound")
				.map(HostScalar::Const)
				.map_err(|_| IrError::Stale("ir_lower::graph: a key bound"))
		};
		let scalar = match key {
			KeyBound::Number(n) => constant(Value::from(*n))?,
			KeyBound::String(s) => constant(Value::String(s.clone()))?,
			KeyBound::Uuid(u) => constant(Value::Uuid(*u))?,
			KeyBound::Generate(_) => {
				return Err(invariant("a graph range bound that is a generated key"));
			}
			KeyBound::Array {
				len,
			} => HostScalar::Code(self.collection_bound(
				&crate::exec::ir::expr::ops::KeyShape::Array {
					len: *len,
				},
				operands,
			)?),
			KeyBound::Object {
				keys,
			} => HostScalar::Code(self.collection_bound(
				&crate::exec::ir::expr::ops::KeyShape::Object {
					keys: keys.clone(),
				},
				operands,
			)?),
		};
		Ok(wrap(scalar))
	}

	/// The host pipeline that emits the array or the object key `shape` names once, over no row.
	/// The pipeline is built beside the one in flight and reads the values it names afresh.
	fn collection_bound(
		&mut self,
		shape: &crate::exec::ir::expr::ops::KeyShape,
		operands: &mut std::iter::Copied<std::slice::Iter<'_, ValueId>>,
	) -> Result<u16, IrError> {
		let aside = self.em.set_aside();
		let beside = self.beside();
		let built = self.record_key(shape, operands).and_then(|key| match key {
			RecordKey::Array(built) | RecordKey::Object(built) => self.emit_value_pipeline(built),
			RecordKey::Fixed(_) | RecordKey::Generate(_) | RecordKey::Range(..) => {
				Err(invariant("a collection key that is not one"))
			}
		});
		self.rejoin(beside);
		self.em.restore(aside);
		self.em.host_code(built?)
	}
}

/// The first operand of `data`, the value a step starts from.
fn first_operand(data: &OpData) -> Result<ValueId, IrError> {
	data.operands.first().copied().ok_or(IrError::Stale("ir_lower::graph: a step's source"))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::AccessMode;
	use crate::exec::ir::core::facts::FactCache;
	use crate::exec::ir::core::ir::Ir;
	use crate::exec::ir::iface::ids::ValueId;
	use crate::exec::ir::iface::region_kind::RegionKind;
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::logical::ops::{LoopKind, StepAttrs};
	use crate::exec::ir::phys::ops::{Batch, PhysOp};
	use crate::exec::vm::ir_lower::emit::Emitter;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::program::{Program, Source};
	use crate::expr::BinaryOperator;

	fn broadcast_op() -> PhysOp {
		broadcast_with(None)
	}

	fn broadcast_with(step: Option<StepAttrs>) -> PhysOp {
		PhysOp::Loop {
			kind: LoopKind::Broadcast,
			first: false,
			batch: Batch {
				max_iterations: 1,
				max_bytes: 1,
			},
			unroll: 1,
			value: false,
			step: step.map(Box::new),
		}
	}

	/// Lowers `value` of `ir` into one scalar pipeline that ends the row.
	fn lower_value(ir: &Ir, value: ValueId) -> Result<(Operand, Program), IrError> {
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, ir, crate::exec::ir::analysis::cx::builtin_registry());
		let operand = lowerer.value(value)?;
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once)?;
		let program = em.finish_program(vec![pipeline], AccessMode::ReadOnly)?;
		Ok((operand, program))
	}

	/// A broadcast over the free parameter `xs` whose body is the one op `body` builds over the
	/// element, and the broadcast's result.
	fn broadcast_of(
		b: &mut Builder,
		body: impl FnOnce(&mut Builder, ValueId) -> ValueId,
	) -> ValueId {
		broadcast_stating(b, None, body)
	}

	/// [`broadcast_of`] for a loop that states `step`.
	fn broadcast_stating(
		b: &mut Builder,
		step: Option<StepAttrs>,
		body: impl FnOnce(&mut Builder, ValueId) -> ValueId,
	) -> ValueId {
		let xs = b.add(
			ExprOp::FreeParam {
				name: "xs".into(),
			},
			&[],
		);
		let looped = b.add_phys_value(broadcast_with(step), &[xs]);
		b.open_with(
			looped,
			RegionKind::Loop,
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			|b, args| {
				let value = body(b, args[0]);
				b.yield_phys(value);
			},
		);
		b.result_of(looped)
	}

	fn code_of(program: &Program) -> &[Instr] {
		&program.pipelines[0].code
	}

	#[test]
	fn a_broadcast_lowers_its_body_once_for_a_collection_and_for_a_lone_value() {
		let mut b = Builder::new();
		let result = broadcast_of(&mut b, |b, element| {
			let one = b.literal(Value::from(1_i64));
			b.binary(BinaryOperator::Add, element, one)
		});
		let (operand, program) = lower_value(&b.ir, result).expect("the broadcast lowers");
		let code = code_of(&program);
		// One copy of the body serves both paths.
		assert_eq!(code.iter().filter(|i| matches!(i, Instr::Arith { .. })).count(), 1);
		let begin = code.iter().position(|i| matches!(i, Instr::MapBegin { .. })).expect("begins");
		let Instr::MapBegin {
			items,
			dst,
			other,
			sets,
			..
		} = code[begin]
		else {
			unreachable!()
		};
		assert!(sets, "a broadcast maps sets");
		assert_eq!(operand, Operand::Reg(dst));
		let next = code.iter().position(|i| matches!(i, Instr::ElemNext { .. })).expect("walks");
		let Instr::ElemNext {
			array,
			done,
			..
		} = code[next]
		else {
			unreachable!()
		};
		assert_eq!(array, items);
		// A value that is no collection continues at the code that marks it single and enters the
		// body, and the end of the elements leaves the loop.
		let Instr::Load {
			src: Operand::Const(marked),
			..
		} = code[other as usize]
		else {
			panic!("the single path marks the value")
		};
		assert_eq!(program.consts[marked as usize], Value::Bool(true));
		assert!(matches!(code[other as usize + 1], Instr::Jump { .. }));
		assert!(matches!(code[done as usize], Instr::Jump { .. }));
		// A single value leaves the loop uncollected, as the value of the body.
		assert!(code.iter().any(|i| matches!(
			i,
			Instr::JumpIf {
				truthy: true,
				..
			}
		)));
		assert_eq!(code.iter().filter(|i| matches!(i, Instr::CollectPush { .. })).count(), 1);
	}

	#[test]
	fn a_flattening_broadcast_splices_the_collected_results_and_not_a_lone_value() {
		let mut b = Builder::new();
		let plain = broadcast_of(&mut b, |_, element| element);
		let (_, program) = lower_value(&b.ir, plain).expect("the broadcast lowers");
		assert!(
			!code_of(&program).iter().any(|i| matches!(
				i,
				Instr::Nav {
					op: NavOp::Flatten,
					..
				}
			)),
			"a broadcast that does not flatten splices nothing"
		);
		let mut b = Builder::new();
		let flat = broadcast_stating(&mut b, Some(StepAttrs::Flatten), |_, element| element);
		let (operand, program) = lower_value(&b.ir, flat).expect("the broadcast lowers");
		let code = code_of(&program);
		let flatten = code
			.iter()
			.position(|i| {
				matches!(
					i,
					Instr::Nav {
						op: NavOp::Flatten,
						..
					}
				)
			})
			.expect("the results are spliced");
		let Instr::Nav {
			src,
			dst,
			..
		} = code[flatten]
		else {
			unreachable!()
		};
		// The collection is spliced in place, after the elements have been walked and before the
		// loop is left, so the result of a lone value does not pass through it.
		assert_eq!(src, Operand::Reg(dst));
		assert_eq!(operand, Operand::Reg(dst));
		let push =
			code.iter().position(|i| matches!(i, Instr::CollectPush { .. })).expect("pushes");
		assert!(push < flatten);
		assert!(matches!(code[flatten + 1], Instr::Jump { .. }));
	}

	#[test]
	fn a_broadcast_that_states_a_step_of_another_kind_is_an_invariant_error() {
		let mut b = Builder::new();
		let result = broadcast_stating(
			&mut b,
			Some(StepAttrs::Recurse(crate::exec::ir::logical::ops::RecurseAttrs {
				depth: crate::expr::part::Recurse::Fixed(1),
				form: crate::exec::ir::logical::ops::RecursePathForm::Absent,
				instruction: None,
			})),
			|_, element| element,
		);
		assert!(matches!(lower_value(&b.ir, result), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn a_broadcast_whose_body_applies_each_to_the_element_takes_it_over_record_ids_only() {
		for flatten in [false, true] {
			let mut b = Builder::new();
			let step = flatten.then_some(StepAttrs::Flatten);
			let result =
				broadcast_stating(&mut b, step, |b, element| b.add(ExprOp::Each, &[element]));
			let (operand, program) = lower_value(&b.ir, result).expect("the broadcast lowers");
			let code = code_of(&program);
			let each = |i: &Instr| matches!(i, Instr::Each { .. });
			// The body is lowered twice: once for the elements, behind the record-id test, and
			// once for a lone value, as written.
			assert_eq!(code.iter().filter(|i| each(i)).count(), 2);
			assert_eq!(
				code.iter().filter(|i| matches!(i, Instr::JumpIfNotRecordId { .. })).count(),
				1
			);
			let next =
				code.iter().position(|i| matches!(i, Instr::ElemNext { .. })).expect("walks");
			let test = code
				.iter()
				.position(|i| matches!(i, Instr::JumpIfNotRecordId { .. }))
				.expect("tests");
			let first = code.iter().position(each).expect("takes the step");
			assert!(next < test && test < first);
			// A test that fails continues past the step, which leaves the element as it is.
			let Instr::JumpIfNotRecordId {
				cond,
				target,
			} = code[test]
			else {
				unreachable!()
			};
			assert_eq!(target as usize, first + 1);
			let Instr::ElemNext {
				elem,
				..
			} = code[next]
			else {
				unreachable!()
			};
			assert_eq!(cond, Operand::Reg(elem));
			// The collected value is the result, and a flattening broadcast splices it.
			let push =
				code.iter().position(|i| matches!(i, Instr::CollectPush { .. })).expect("pushes");
			let Instr::CollectPush {
				collection,
				value,
			} = code[push]
			else {
				unreachable!()
			};
			assert_eq!(operand, Operand::Reg(collection));
			let Instr::Each {
				dst: plain,
				..
			} = code[first]
			else {
				unreachable!()
			};
			assert_eq!(value, Operand::Reg(plain), "the element is its own value past the test");
			assert_eq!(
				code.iter().any(|i| matches!(
					i,
					Instr::Nav {
						op: NavOp::Flatten,
						..
					}
				)),
				flatten
			);
		}
	}

	#[test]
	fn each_over_a_value_that_is_not_the_element_is_lowered() {
		let mut b = Builder::new();
		let result = broadcast_of(&mut b, |b, _| {
			let other = b.add(
				ExprOp::FreeParam {
					name: "y".into(),
				},
				&[],
			);
			b.add(ExprOp::Each, &[other])
		});
		let (_, program) = lower_value(&b.ir, result).expect("the broadcast lowers");
		assert!(code_of(&program).iter().any(|i| matches!(i, Instr::Each { .. })));
	}

	#[test]
	fn a_broadcast_is_not_in_the_folders_subset() {
		let mut b = Builder::new();
		let result = broadcast_of(&mut b, |_, element| element);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::for_fold(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert!(matches!(
			lowerer.value(result),
			Err(IrError::Invariant {
				what: "ir_lower::fold: phys.loop"
			})
		));
	}

	#[test]
	fn a_broadcast_through_the_direct_entry_lowers() {
		let mut b = Builder::new();
		let result = broadcast_of(&mut b, |_, element| element);
		b.add_phys(PhysOp::Eval, &[result]);
		b.physical();
		let lowered = crate::exec::vm::ir_lower::lower(&b.ir, &FactCache::new())
			.expect("the broadcast lowers");
		assert!(
			lowered.program.pipelines[0].code.iter().any(|i| matches!(i, Instr::MapBegin { .. }))
		);
	}
}

#[cfg(test)]
mod step_tests {
	use super::*;
	use crate::exec::AccessMode;
	use crate::exec::ir::core::facts::FactCache;
	use crate::exec::ir::expr::ops::KeyRange;
	use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::logical::ops::{LookupSubjectShape, SortShape};
	use crate::exec::ir::phys::ops::Batch;
	use crate::exec::vm::ir_lower::emit::Emitter;
	use crate::exec::vm::ir_lower::expr::Needs;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::program::{Program, Source};

	fn table(name: &str) -> TableName {
		TableName::from(name)
	}

	/// A subject that names the table `name` and nothing else.
	fn subject(name: &str) -> LookupSubjectShape {
		LookupSubjectShape {
			table: table(name),
			referencing_field: None,
			range: None,
		}
	}

	/// A lookup of `kind` over `subjects` that states nothing else.
	fn attrs(kind: LookupKind, subjects: Vec<LookupSubjectShape>) -> LookupAttrs {
		LookupAttrs {
			kind,
			only: false,
			only_path: false,
			subjects: subjects.into_boxed_slice(),
			fields: None,
			order: None,
			alias: None,
			cond: false,
			split: None,
			group: None,
			limit: None,
			start: None,
		}
	}

	fn out(name: &str) -> LookupAttrs {
		attrs(LookupKind::Graph(Dir::Out), vec![subject(name)])
	}

	/// The hop `attrs` states over `operands`: the loop's result. A hop that has a `WHERE` holds
	/// a region that reads the row as `$this` and yields its truth.
	fn hop(b: &mut Builder, attrs: LookupAttrs, operands: &[ValueId]) -> ValueId {
		let cond = attrs.cond;
		let looped = b.add_phys_value(
			PhysOp::Loop {
				kind: LoopKind::GraphStep,
				first: false,
				batch: Batch {
					max_iterations: 1,
					max_bytes: 1,
				},
				unroll: 1,
				value: false,
				step: Some(Box::new(StepAttrs::Lookup(attrs))),
			},
			operands,
		);
		b.open_with(
			looped,
			RegionKind::Loop,
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			|b, args| {
				b.yield_phys(args[0]);
			},
		);
		if cond {
			b.region_with_args(
				looped,
				RegionKind::Clause(ClauseRole::Where),
				&[TypeKnowledge::Unknown],
				|b, args| b.add(ExprOp::Truthy, &[args[0]]),
			);
		}
		b.result_of(looped)
	}

	fn source(b: &mut Builder) -> ValueId {
		b.add(
			ExprOp::FreeParam {
				name: "a".into(),
			},
			&[],
		)
	}

	fn lower_value(ir: &Ir, value: ValueId) -> Result<(Program, Needs), IrError> {
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, ir, crate::exec::ir::analysis::cx::builtin_registry());
		lowerer.value(value)?;
		let needs = lowerer.needs();
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once)?;
		let program = em.finish_program(vec![pipeline], AccessMode::ReadOnly)?;
		Ok((program, needs))
	}

	fn lookups_in(program: &Program) -> usize {
		program.pipelines[0].code.iter().filter(|i| matches!(i, Instr::Lookup { .. })).count()
	}

	fn graph_read(hop: &LookupHop) -> (&Vec<Dir>, &Vec<EdgeSpec>, &Option<Vec<TableName>>) {
		let HopRead::Graph {
			directions,
			specs,
			targets,
			..
		} = &*hop.read
		else {
			panic!("a graph hop");
		};
		(directions, specs, targets)
	}

	#[test]
	fn a_hop_is_one_lookup_over_the_value_it_starts_from() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let result = hop(&mut b, out("knows"), &[a]);
		let (program, needs) = lower_value(&b.ir, result).expect("the hop lowers");
		assert_eq!(lookups_in(&program), 1);
		assert_eq!(needs.context, ContextLevel::Database);
		let [spec] = &program.lookups[..] else {
			panic!("one lookup");
		};
		let [only] = &spec.hops[..] else {
			panic!("one hop");
		};
		let (directions, specs, targets) = graph_read(only);
		assert_eq!(directions, &vec![Dir::Out]);
		assert_eq!(specs, &vec![EdgeSpec::whole(table("knows"))]);
		assert!(targets.is_none());
		assert!(!only.full);
		assert!(only.cond.is_none() && only.key_cond.is_none());
		assert!(only.tables.is_empty(), "the hop reads no table of the catalog");
		assert!(!spec.extract_id && !spec.only && !spec.only_path);
		assert!(spec.page.is_none());
		let Some(Instr::Lookup {
			lookup,
			frame,
			..
		}) = program.pipelines[0].code.iter().find(|i| matches!(i, Instr::Lookup { .. }))
		else {
			panic!("a lookup instruction");
		};
		assert_eq!((*lookup, *frame), (0, None));
	}

	#[test]
	fn a_chain_of_hops_is_one_lookup_with_a_hop_for_each_in_path_order() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let knows = hop(&mut b, out("knows"), &[a]);
		let person =
			hop(&mut b, attrs(LookupKind::Graph(Dir::In), vec![subject("person")]), &[knows]);
		let (program, _) = lower_value(&b.ir, person).expect("the chain lowers");
		assert_eq!(lookups_in(&program), 1, "the hops the last one holds lower nothing");
		let [spec] = &program.lookups[..] else {
			panic!("one lookup");
		};
		let tables: Vec<_> = spec
			.hops
			.iter()
			.map(|hop| {
				let (directions, specs, _) = graph_read(hop);
				(directions.clone(), specs[0].table.clone())
			})
			.collect();
		assert_eq!(
			tables,
			vec![(vec![Dir::Out], table("knows")), (vec![Dir::In], table("person"))]
		);
	}

	#[test]
	fn a_hop_in_both_directions_reads_in_before_out() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let result = hop(&mut b, attrs(LookupKind::Graph(Dir::Both), vec![subject("knows")]), &[a]);
		let (program, _) = lower_value(&b.ir, result).expect("the hop lowers");
		let (directions, _, _) = graph_read(&program.lookups[0].hops[0]);
		assert_eq!(directions, &vec![Dir::In, Dir::Out]);
	}

	#[test]
	fn a_hop_over_no_table_reads_every_edge_table() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let result = hop(&mut b, attrs(LookupKind::Graph(Dir::Out), vec![]), &[a]);
		let (program, _) = lower_value(&b.ir, result).expect("the hop lowers");
		let (_, specs, _) = graph_read(&program.lookups[0].hops[0]);
		assert!(specs.is_empty());
	}

	#[test]
	fn a_where_is_a_host_pipeline_over_the_record_reached() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let mut with_where = out("knows");
		with_where.cond = true;
		let result = hop(&mut b, with_where, &[a]);
		let (program, _) = lower_value(&b.ir, result).expect("the hop lowers");
		let [spec] = &program.lookups[..] else {
			panic!("one lookup");
		};
		assert!(spec.hops[0].full, "a WHERE reads each record whole");
		assert!(spec.extract_id, "the records were read for the WHERE alone");
		assert_eq!(spec.hops[0].cond, Some(0));
		assert_eq!(program.host_code.len(), 1);
		let code = &program.host_code[0].code;
		assert!(code.iter().any(|i| matches!(i, Instr::LoadRow { .. })), "the record is the row");
		assert!(code.iter().any(|i| matches!(i, Instr::Emit { .. })));
	}

	#[test]
	fn the_star_projection_shows_the_records_whole() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let mut star = out("knows");
		star.fields = Some(FieldsShape::Select(Box::new([FieldShape::All])));
		let result = hop(&mut b, star, &[a]);
		let (program, _) = lower_value(&b.ir, result).expect("the hop lowers");
		let spec = &program.lookups[0];
		assert!(spec.hops[0].full);
		assert!(!spec.extract_id, "a projection keeps the records");
	}

	#[test]
	fn the_last_hop_pages_by_literal_integers() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let limit = b.literal(Value::from(5_i64));
		let start = b.literal(Value::from(2_i64));
		let mut paged = out("knows");
		paged.limit = Some(1);
		paged.start = Some(2);
		let result = hop(&mut b, paged, &[a, limit, start]);
		let (program, _) = lower_value(&b.ir, result).expect("the hop lowers");
		assert_eq!(
			program.lookups[0].page,
			Some(LookupPage {
				start: 2,
				limit: Some(5)
			})
		);
	}

	#[test]
	fn a_start_alone_leaves_the_rest_of_the_page_open() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let start = b.literal(Value::from(3_i64));
		let mut paged = out("knows");
		paged.start = Some(1);
		let result = hop(&mut b, paged, &[a, start]);
		let (program, _) = lower_value(&b.ir, result).expect("the hop lowers");
		assert_eq!(
			program.lookups[0].page,
			Some(LookupPage {
				start: 3,
				limit: None
			})
		);
	}

	#[test]
	fn a_page_that_is_computed_or_negative_is_a_statement_the_binder_builds() {
		for written in [None, Some(-1_i64)] {
			let mut b = Builder::new();
			let a = source(&mut b);
			let limit = match written {
				Some(n) => b.literal(Value::from(n)),
				None => b.add(
					ExprOp::FreeParam {
						name: "n".into(),
					},
					&[],
				),
			};
			let mut paged = out("knows");
			paged.limit = Some(1);
			let result = hop(&mut b, paged, &[a, limit]);
			assert!(
				matches!(
					lower_value(&b.ir, result),
					Err(IrError::Invariant {
						what: HOP_STATEMENT
					})
				),
				"{written:?}"
			);
		}
	}

	/// The binder builds a paged path that has `ONLY` as a statement, so a hop that has it under a
	/// page belongs to a value the path reads, which is a lookup of its own.
	#[test]
	fn a_hop_with_only_under_a_page_is_a_value_of_its_own() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let limit = b.literal(Value::from(1_i64));
		let first = hop(&mut b, out("x"), &[a]);
		let mut only = out("y");
		only.only = true;
		let second = hop(&mut b, only, &[first]);
		let mut last = out("knows");
		last.limit = Some(1);
		let result = hop(&mut b, last, &[second, limit]);
		let (program, _) = lower_value(&b.ir, result).expect("the paged hop lowers");
		assert_eq!(lookups_in(&program), 2);
		let paged = program.lookups.iter().find(|spec| spec.page.is_some()).expect("a page");
		assert_eq!(paged.hops.len(), 1);
		assert!(!paged.only && !paged.only_path);
		let unwrapped = program.lookups.iter().find(|spec| spec.only).expect("the ONLY path");
		assert_eq!(unwrapped.hops.len(), 2);
	}

	#[test]
	fn a_page_under_only_on_its_own_hop_is_an_invariant_break() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let limit = b.literal(Value::from(1_i64));
		let mut last = out("knows");
		last.limit = Some(1);
		last.only_path = true;
		let result = hop(&mut b, last, &[a, limit]);
		assert!(matches!(
			lower_value(&b.ir, result),
			Err(IrError::Invariant {
				what: HOP_STATEMENT
			})
		));
	}

	#[test]
	fn only_on_any_hop_unwraps_the_whole_answer() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let mut only = out("x");
		only.only = true;
		let first = hop(&mut b, only, &[a]);
		let last = hop(&mut b, out("y"), &[first]);
		let (program, _) = lower_value(&b.ir, last).expect("the chain lowers");
		assert!(program.lookups[0].only);
		assert!(!program.lookups[0].only_path);
		assert_eq!(program.lookups[0].hops.len(), 2);
	}

	#[test]
	fn a_hop_with_a_clause_of_its_own_is_a_statement_the_binder_builds() {
		let cases: [fn(&mut LookupAttrs); 4] = [
			|a| a.split = Some(1),
			|a| a.group = Some(0),
			|a| a.order = Some(SortShape::Random),
			|a| {
				a.fields = Some(FieldsShape::Value {
					alias: None,
				})
			},
		];
		for case in cases {
			let mut b = Builder::new();
			let a = source(&mut b);
			let mut clause = out("knows");
			case(&mut clause);
			let result = hop(&mut b, clause, &[a]);
			assert!(matches!(
				lower_value(&b.ir, result),
				Err(IrError::Invariant {
					what: HOP_STATEMENT
				})
			));
		}
	}

	#[test]
	fn a_hop_with_a_clause_ends_the_chain_the_next_hop_lowers() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let mut ordered = out("x");
		ordered.order = Some(SortShape::Random);
		let first = hop(&mut b, ordered, &[a]);
		let last = hop(&mut b, out("y"), &[first]);
		// The hop with a clause is the source of the chain that follows, and lowers on its own.
		assert!(matches!(
			lower_value(&b.ir, last),
			Err(IrError::Invariant {
				what: HOP_STATEMENT
			})
		));
	}

	#[test]
	fn only_path_on_the_last_hop_unwraps_the_answer_over_every_origin() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let first = hop(&mut b, out("x"), &[a]);
		let mut last = out("y");
		last.only_path = true;
		let result = hop(&mut b, last, &[first]);
		let (program, _) = lower_value(&b.ir, result).expect("the chain lowers");
		assert_eq!(lookups_in(&program), 1);
		let spec = &program.lookups[0];
		assert!(spec.only_path && !spec.only);
		assert_eq!(spec.hops.len(), 2);
	}

	#[test]
	fn a_hop_that_ends_a_run_of_its_own_ends_the_chain() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let mut inner = out("x");
		inner.only_path = true;
		let first = hop(&mut b, inner, &[a]);
		let last = hop(&mut b, out("y"), &[first]);
		let (program, _) = lower_value(&b.ir, last).expect("both lower");
		assert_eq!(lookups_in(&program), 2);
		assert!(program.lookups.iter().any(|spec| spec.only_path && spec.hops.len() == 1));
		assert!(program.lookups.iter().any(|spec| !spec.only_path && spec.hops.len() == 1));
	}

	#[test]
	fn a_referencing_field_belongs_to_a_reference_lookup() {
		let mut named = subject("post");
		named.referencing_field = Some("author".to_owned());
		let mut b = Builder::new();
		let a = source(&mut b);
		let result = hop(&mut b, attrs(LookupKind::Graph(Dir::Out), vec![named.clone()]), &[a]);
		assert!(matches!(
			lower_value(&b.ir, result),
			Err(IrError::Unsupported { reason }) if reason == NO_LOWERING
		));

		let mut b = Builder::new();
		let a = source(&mut b);
		let result = hop(&mut b, attrs(LookupKind::Reference, vec![named]), &[a]);
		let (program, _) = lower_value(&b.ir, result).expect("the reference lookup lowers");
		let HopRead::References(subjects) = &*program.lookups[0].hops[0].read else {
			panic!("a reference hop");
		};
		let [reference] = &subjects[..] else {
			panic!("one subject");
		};
		assert_eq!(reference.table, Some(table("post")));
		assert_eq!(reference.field.as_deref(), Some("author"));
		let mut prefix = storekey::encode_vec(&table("post")).expect("encodes");
		prefix.extend(storekey::encode_vec("author").expect("encodes"));
		assert_eq!(reference.prefix, Some(prefix));
	}

	#[test]
	fn a_reference_lookup_over_no_table_reads_every_table() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let result = hop(&mut b, attrs(LookupKind::Reference, vec![]), &[a]);
		let (program, _) = lower_value(&b.ir, result).expect("the reference lookup lowers");
		let HopRead::References(subjects) = &*program.lookups[0].hops[0].read else {
			panic!("a reference hop");
		};
		let [every] = &subjects[..] else {
			panic!("one subject");
		};
		assert!(every.table.is_none() && every.field.is_none() && every.prefix.is_none());
	}

	#[test]
	fn a_range_subject_bounds_its_edges_by_host_scalars() {
		let mut ranged = subject("knows");
		ranged.range = Some(KeyRange {
			start: Bound::Included(KeyBound::Number(3)),
			end: Bound::Excluded(KeyBound::Array {
				len: 1,
			}),
		});
		let mut b = Builder::new();
		let a = source(&mut b);
		let element = b.literal(Value::from(9_i64));
		let result = hop(&mut b, attrs(LookupKind::Graph(Dir::Out), vec![ranged]), &[a, element]);
		let (program, _) = lower_value(&b.ir, result).expect("the hop lowers");
		let (_, specs, _) = graph_read(&program.lookups[0].hops[0]);
		let [spec] = &specs[..] else {
			panic!("one edge table");
		};
		assert!(
			matches!(&spec.start, Bound::Included(HostScalar::Const(v)) if **v == Value::from(3_i64))
		);
		assert!(matches!(&spec.end, Bound::Excluded(HostScalar::Code(0))));
		let code = &program.host_code[0].code;
		assert!(code.iter().any(|i| matches!(i, Instr::NewArray { .. })));
		assert!(code.iter().any(|i| matches!(i, Instr::ArrayPush { .. })));
	}

	#[test]
	fn a_generated_range_bound_is_an_invariant_error() {
		let mut ranged = subject("knows");
		ranged.range = Some(KeyRange {
			start: Bound::Included(KeyBound::Generate(crate::expr::RecordIdKeyGen::Rand)),
			end: Bound::Unbounded,
		});
		let mut b = Builder::new();
		let a = source(&mut b);
		let result = hop(&mut b, attrs(LookupKind::Graph(Dir::Out), vec![ranged]), &[a]);
		assert!(matches!(lower_value(&b.ir, result), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn a_graph_step_is_not_in_the_folders_subset() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let result = hop(&mut b, out("knows"), &[a]);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::for_fold(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert!(matches!(
			lowerer.value(result),
			Err(IrError::Invariant {
				what: "ir_lower::fold: phys.loop"
			})
		));
	}

	#[test]
	fn a_graph_step_that_states_no_lookup_is_an_invariant_error() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let looped = b.add_phys_value(
			PhysOp::Loop {
				kind: LoopKind::GraphStep,
				first: false,
				batch: Batch {
					max_iterations: 1,
					max_bytes: 1,
				},
				unroll: 1,
				value: false,
				step: None,
			},
			&[a],
		);
		let result = b.result_of(looped);
		assert!(matches!(lower_value(&b.ir, result), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn the_chain_is_lowered_where_the_path_is_read() {
		let mut b = Builder::new();
		let a = source(&mut b);
		let knows = hop(&mut b, out("knows"), &[a]);
		let person = hop(&mut b, out("person"), &[knows]);
		b.add_phys(PhysOp::Eval, &[person]);
		b.physical();
		let lowered =
			crate::exec::vm::ir_lower::lower(&b.ir, &FactCache::new()).expect("the path lowers");
		assert_eq!(lowered.program.lookups.len(), 1);
		assert_eq!(lowered.program.lookups[0].hops.len(), 2);
		assert_eq!(lowered.program.required_context, ContextLevel::Database);
	}
}
