//! The source of a write that names its own subjects: its targets, its edges and the rows of an
//! `INSERT`.
//!
//! A write over records its statement names is a pipeline whose source produces the records the
//! write runs over. Each subject the statement names is evaluated by the host, outside the
//! pipeline: a literal is the constant it is, any other value is a host pipeline built beside the
//! one in flight ([`Lowerer::host_scalar`]), and a place an `INSERT` writes at is the steps the
//! write walks, whose subscripts and filters are host pipelines of their own ([`HostPlaces`]).
//!
//! # What a subject is
//!
//! - The targets of `CREATE`, `UPDATE`, `UPSERT` and `DELETE` are the elements of an array: an
//!   `expr.array` of the values the statement named, or the literal array they collapse to. Each
//!   element is one target. An `expr.mock` element is a mock target, whose ids are generated as the
//!   write consumes them; every other element is a scalar that names a table, a record id or an
//!   array of either.
//! - The endpoints of `RELATE` are the three elements `from`, `through` and `to`, in that order.
//! - The rows of an `INSERT` are one value, or an `expr.array` whose elements are all
//!   `expr.tuple_row`. A tuple row is one `VALUES` tuple: its places are the steps of
//!   [`InsertFieldSpec::place`] and each value is the host scalar of the field. A value that is not
//!   an array of tuple rows is one host scalar, an object being one row and an array one row per
//!   element. An array that holds tuple rows beside other values is an invariant break: only the
//!   `VALUES` form builds a tuple row, and it builds one for every tuple.
//!
//! The rows are evaluated by the write before any is stored, so a row never observes a record the
//! statement has already written; that is the write's own order, and nothing here sequences it.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::payload::{Refusal, ValuesRead};
use super::super::plain::ValueData;
use super::super::program::{
	HostScalar, InsertFieldSpec, InsertInto, InsertRowsSpec, Instr, Operand, Pipeline, Source,
	StoreInstr, Target,
};
use super::expr::Lowerer;
use super::nested::LET_IN_AN_EXPRESSION;
use super::write_place::{PlaceHosts, place_path};
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::val::Value;

/// What builds the host pipelines the subscripts and the filters of a place need, beside the
/// pipeline a [`Lowerer`] has in flight.
pub(super) struct HostPlaces<'l, 'a> {
	lowerer: &'l mut Lowerer<'a>,
}

impl<'l, 'a> HostPlaces<'l, 'a> {
	pub(super) fn new(lowerer: &'l mut Lowerer<'a>) -> Self {
		Self {
			lowerer,
		}
	}
}

impl PlaceHosts for HostPlaces<'_, '_> {
	/// A subscript is evaluated with no record in scope: the pipeline emits the value once, over
	/// no row, and the value was bound against a row that holds `NONE`.
	fn subscript(&mut self, value: ValueId) -> Result<u16, IrError> {
		refusing_when_evaluated(self.lowerer, |lowerer| lowerer.host_code(value))
	}

	/// A filter judges one element: the pipeline's row is the element, the region's argument, and
	/// it emits what the predicate yields.
	fn filter(&mut self, region: RegionId) -> Result<u16, IrError> {
		refusing_when_evaluated(self.lowerer, |lowerer| filter_pipeline(lowerer, region))
	}
}

/// The host pipeline `build` lowers for an expression a place embeds, or, for an expression that
/// is a `LET` written as a statement, the pipeline that raises that refusal when the place is
/// evaluated for a record.
///
/// A place no record reaches raises nothing. Any other refusal is the statement's and is not
/// answered here.
fn refusing_when_evaluated(
	lowerer: &mut Lowerer<'_>,
	build: impl FnOnce(&mut Lowerer<'_>) -> Result<u16, IrError>,
) -> Result<u16, IrError> {
	match build(lowerer) {
		Err(IrError::Rejected(refusal))
			if matches!(
				&*refusal,
				Refusal::InvalidStatement { message } if message == LET_IN_AN_EXPRESSION
			) =>
		{
			refusing_pipeline(lowerer, *refusal)
		}
		built => built,
	}
}

/// The host pipeline that raises `refusal` and, were it let through, would emit `NONE` once, over
/// no row.
fn refusing_pipeline(lowerer: &mut Lowerer<'_>, refusal: Refusal) -> Result<u16, IrError> {
	let aside = lowerer.em.set_aside();
	let beside = lowerer.beside();
	let built = refusing_body(lowerer, refusal);
	lowerer.rejoin(beside);
	lowerer.em.restore(aside);
	lowerer.em.host_code(built?)
}

/// The pipeline of [`refusing_pipeline`], built into the emitter's fresh code.
fn refusing_body(lowerer: &mut Lowerer<'_>, refusal: Refusal) -> Result<Pipeline, IrError> {
	lowerer.emit(Instr::Store(StoreInstr::Refuse {
		refusal: Box::new(refusal),
	}));
	let none = lowerer.em.constant(Value::None)?;
	lowerer.emit_value_pipeline(Operand::Const(none))
}

/// The host pipeline that emits what the predicate region `region` yields for the element it is
/// run over.
///
/// It is built beside the pipeline in flight and holds no register of it: the region's one block
/// argument is the row of the new pipeline, which is not an object row, because an element is any
/// value.
fn filter_pipeline(lowerer: &mut Lowerer<'_>, region: RegionId) -> Result<u16, IrError> {
	let aside = lowerer.em.set_aside();
	let beside = lowerer.beside();
	let built = filter_body(lowerer, region);
	lowerer.rejoin(beside);
	lowerer.em.restore(aside);
	lowerer.em.host_code(built?)
}

/// The pipeline of [`filter_pipeline`], built into the emitter's fresh code.
fn filter_body(
	lowerer: &mut Lowerer<'_>,
	region: RegionId,
) -> Result<super::super::program::Pipeline, IrError> {
	let args = lowerer.region_args(region)?;
	let [element] = <[ValueId; 1]>::try_from(args)
		.map_err(|_| IrError::Stale("ir_lower::write_source: a place filter takes one element"))?;
	lowerer.bind_row(element, false);
	let verdict = lowerer.region_value(region)?;
	lowerer.emit_value_pipeline(verdict)
}

/// One element of an array a write names.
enum Element {
	/// A value an op defines.
	Value(ValueId),
	/// A constant the array was folded to as a literal.
	Const(Value),
}

/// The elements of the array `value` names: its operands, or the items of the literal it folded
/// to. Any other value is no array a write names its subjects in.
fn elements(ir: &Ir, value: ValueId) -> Result<Vec<Element>, IrError> {
	let defining = ir
		.value(value)
		.and_then(|data| match data.def {
			ValueDef::OpResult {
				op,
				index: 0,
			} => ir.op(op),
			ValueDef::OpResult {
				..
			}
			| ValueDef::BlockArg {
				..
			} => None,
		})
		.ok_or(IrError::Invariant {
			what: "ir_lower::write_source: the subjects of a write are no array an op defines",
		})?;
	match &defining.kind {
		OpKind::Expr(ExprOp::Array) => {
			Ok(defining.operands.iter().map(|operand| Element::Value(*operand)).collect())
		}
		OpKind::Expr(ExprOp::Literal(literal)) => {
			if let Value::Array(items) = &**literal {
				Ok(items.iter().map(|item| Element::Const(item.clone())).collect())
			} else {
				Err(IrError::Invariant {
					what: "ir_lower::write_source: the subjects of a write are a literal that is no array",
				})
			}
		}
		// The values an expression yields, read as the write's targets: the one value is a target
		// that names a table, a record id or an array of either, as an element of an array is.
		OpKind::Phys(PhysOp::Values {
			read: ValuesRead::Expression {
				..
			},
		}) => match defining.operands.as_slice() {
			[value] => Ok(vec![Element::Value(*value)]),
			_ => Err(IrError::Stale("ir_lower::write_source: phys.values reads one value")),
		},
		// The rows of a subquery, read as the write's targets: the one value is the rows, each a
		// record or an object whose id names one.
		OpKind::Phys(PhysOp::Subquery) => Ok(vec![Element::Value(value)]),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => Err(IrError::Invariant {
			what: "ir_lower::write_source: the subjects of a write are no array",
		}),
	}
}

/// The host scalar of one element: a constant, or the host pipeline that emits the value.
fn scalar(lowerer: &mut Lowerer<'_>, element: Element) -> Result<HostScalar, IrError> {
	match element {
		Element::Value(value) => lowerer.host_scalar(value),
		Element::Const(constant) => ValueData::new(constant, "a host scalar")
			.map(HostScalar::Const)
			.map_err(|_| IrError::Stale("ir_lower::write_source: a closure among the subjects")),
	}
}

/// The source of a write over the targets in the array `subjects`.
///
/// `deferable` and `guarantees` are attributes of the write: whether a table names a record to
/// invent, and whether the targets are the records an `UPSERT` guarantees, which another pipeline
/// of the write reads.
pub(super) fn targets_source(
	lowerer: &mut Lowerer<'_>,
	subjects: ValueId,
	deferable: bool,
	guarantees: bool,
) -> Result<Source, IrError> {
	let ir = lowerer.ir;
	let mut targets = Vec::new();
	for element in elements(ir, subjects)? {
		targets.push(match element {
			Element::Value(value) => match mock_of(ir, value) {
				Some(mock) => Target::Mock(mock),
				None => Target::Scalar(lowerer.host_scalar(value)?),
			},
			constant @ Element::Const(_) => Target::Scalar(scalar(lowerer, constant)?),
		});
	}
	Ok(Source::WriteTargets {
		targets,
		deferable,
		guarantees,
	})
}

/// The source of the pipeline that writes the records an `UPSERT` over rows guarantees: one scalar
/// target per value of `targets`, read once the pipeline of the rows has judged every record.
pub(super) fn guaranteed_source(
	lowerer: &mut Lowerer<'_>,
	targets: &[ValueId],
) -> Result<Source, IrError> {
	let mut held = Vec::with_capacity(targets.len());
	for value in targets {
		held.push(Target::Scalar(lowerer.host_scalar(*value)?));
	}
	Ok(Source::WriteTargets {
		targets: held,
		deferable: false,
		guarantees: true,
	})
}

/// The mock the value `value` is, when an `expr.mock` defines it.
fn mock_of(ir: &Ir, value: ValueId) -> Option<crate::expr::Mock> {
	let ValueDef::OpResult {
		op,
		index: 0,
	} = ir.value(value)?.def
	else {
		return None;
	};
	match &ir.op(op)?.kind {
		OpKind::Expr(ExprOp::Mock(mock)) => Some((**mock).clone()),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

/// The source of a `RELATE`, over the endpoints `[from, through, to]` of the array `subjects`.
pub(super) fn edges_source(
	lowerer: &mut Lowerer<'_>,
	subjects: ValueId,
) -> Result<Source, IrError> {
	let ir = lowerer.ir;
	let [from, through, to] =
		<[Element; 3]>::try_from(elements(ir, subjects)?).map_err(|_| IrError::Invariant {
			what: "ir_lower::write_source: a RELATE names other than three endpoints",
		})?;
	let from = scalar(lowerer, from)?;
	let through = scalar(lowerer, through)?;
	let to = scalar(lowerer, to)?;
	Ok(Source::WriteEdges {
		from,
		through,
		to,
	})
}

/// The source of an `INSERT` over the value `rows`.
///
/// `into` is the value of the `INTO` expression with its text as the statement wrote it, which the
/// refusal of a value that names no table quotes. `relation` is `INSERT RELATION`.
pub(super) fn inserts_source(
	lowerer: &mut Lowerer<'_>,
	rows: ValueId,
	into: Option<(ValueId, &str)>,
	relation: bool,
) -> Result<Source, IrError> {
	let into = match into {
		Some((value, sql)) => Some(InsertInto {
			scalar: lowerer.host_scalar(value)?,
			sql: sql.to_owned(),
		}),
		None => None,
	};
	let rows = insert_rows(lowerer, rows)?;
	Ok(Source::WriteInserts {
		into,
		rows,
		relation,
	})
}

/// How the value `rows` spells the rows of an `INSERT`.
///
/// An array of tuple rows is [`InsertRowsSpec::Tuples`]: one tuple per row, each building a fresh
/// object by writing its values through the places paired with them, in the order the tuple
/// wrote them. Any other value is [`InsertRowsSpec::Value`].
pub(super) fn insert_rows(
	lowerer: &mut Lowerer<'_>,
	rows: ValueId,
) -> Result<InsertRowsSpec, IrError> {
	let ir = lowerer.ir;
	let Some(tuples) = tuple_rows(ir, rows)? else {
		return Ok(InsertRowsSpec::Value(lowerer.host_scalar(rows)?));
	};
	let mut compiled = Vec::with_capacity(tuples.len());
	for tuple in tuples {
		compiled.push(tuple_fields(lowerer, tuple)?);
	}
	Ok(InsertRowsSpec::Tuples(compiled))
}

/// The tuple rows `rows` is an array of, or `None` when it is not an array of tuple rows.
fn tuple_rows(ir: &Ir, rows: ValueId) -> Result<Option<Vec<OpId>>, IrError> {
	let Some(ValueDef::OpResult {
		op,
		index: 0,
	}) = ir.value(rows).map(|data| data.def)
	else {
		return Ok(None);
	};
	let Some(array) = ir.op(op) else {
		return Err(IrError::Stale("ir_lower::write_source: the op of the rows is gone"));
	};
	if !matches!(array.kind, OpKind::Expr(ExprOp::Array)) {
		return Ok(None);
	}
	let mut found = Vec::new();
	for element in &array.operands {
		let defined = match ir.value(*element).map(|data| data.def) {
			Some(ValueDef::OpResult {
				op,
				index: 0,
			}) => Some(op),
			Some(ValueDef::OpResult {
				..
			})
			| Some(ValueDef::BlockArg {
				..
			})
			| None => None,
		};
		match defined.and_then(|op| ir.op(op).map(|data| (op, &data.kind))) {
			Some((
				tuple,
				OpKind::Expr(ExprOp::TupleRow {
					..
				}),
			)) => found.push(tuple),
			Some((_, OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_))) | None => {}
		}
	}
	if found.is_empty() {
		return Ok(None);
	}
	if found.len() != array.operands.len() {
		return Err(IrError::Invariant {
			what: "ir_lower::write_source: the rows of an INSERT mix tuple rows with other values",
		});
	}
	Ok(Some(found))
}

/// The fields of the tuple row `tuple`: each place as its steps, and each value as a host scalar.
fn tuple_fields(lowerer: &mut Lowerer<'_>, tuple: OpId) -> Result<Vec<InsertFieldSpec>, IrError> {
	let ir = lowerer.ir;
	let data =
		ir.op(tuple).ok_or(IrError::Stale("ir_lower::write_source: the tuple row is gone"))?;
	let OpKind::Expr(ExprOp::TupleRow {
		places,
	}) = &data.kind
	else {
		return Err(IrError::Stale("ir_lower::write_source: a tuple row is no tuple row"));
	};
	let mut fields = Vec::with_capacity(places.len());
	for (at, place) in places.iter().enumerate() {
		let value = data.operands.get(at).copied().ok_or(IrError::Stale(
			"ir_lower::write_source: a tuple row holds fewer values than places",
		))?;
		let steps = place_path(ir, tuple, place, &mut HostPlaces::new(lowerer))?.steps;
		fields.push(InsertFieldSpec {
			place: steps,
			value: lowerer.host_scalar(value)?,
		});
	}
	Ok(fields)
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::{Strand, TableName};

	use super::super::super::program::PlaceStepSpec;
	use super::super::emit::Emitter;
	use super::super::tests::Builder;
	use super::*;
	use crate::exec::ir::expr::ops::NestedStrategy;
	use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
	use crate::exec::ir::iface::statements::{Place, PlacePart};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::logical::ops::{LetAttrs, LetBinding};
	use crate::exec::ir::phys::ops::PhysOp;
	use crate::expr::{BinaryOperator, Mock};
	use crate::val::Array;

	fn place(parts: Vec<PlacePart>) -> Place {
		Place {
			parts: parts.into_boxed_slice(),
		}
	}

	fn field(name: &str) -> PlacePart {
		PlacePart::Field(Strand::new(name))
	}

	fn tuple_row(b: &mut Builder, places: Vec<Place>, operands: &[ValueId]) -> ValueId {
		b.add(
			ExprOp::TupleRow {
				places: places.into_boxed_slice(),
			},
			operands,
		)
	}

	fn constant(value: Value) -> HostScalar {
		HostScalar::Const(ValueData::new(value, "a host scalar").expect("a plain value"))
	}

	/// An `INSERT` whose rows are the one tuple `(a) VALUES (1)`.
	#[test]
	fn a_tuple_row_is_the_steps_of_its_places() {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let row = tuple_row(
			&mut b,
			vec![place(vec![field("a")]), place(vec![field("b"), PlacePart::All])],
			&[one, two],
		);
		let rows = b.add(ExprOp::Array, &[row]);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		let spec = insert_rows(&mut lowerer, rows).expect("the rows lower");
		assert_eq!(
			spec,
			InsertRowsSpec::Tuples(vec![vec![
				InsertFieldSpec {
					place: vec![PlaceStepSpec::Field("a".to_owned())],
					value: constant(Value::from(1_i64)),
				},
				InsertFieldSpec {
					place: vec![PlaceStepSpec::Field("b".to_owned()), PlaceStepSpec::All],
					value: constant(Value::from(2_i64)),
				},
			]])
		);
	}

	/// Every tuple is a row, in the order the statement wrote them.
	#[test]
	fn each_tuple_is_a_row() {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let first = tuple_row(&mut b, vec![place(vec![field("a")])], &[one]);
		let second = tuple_row(&mut b, vec![place(vec![field("a")])], &[two]);
		let rows = b.add(ExprOp::Array, &[first, second]);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		let InsertRowsSpec::Tuples(tuples) =
			insert_rows(&mut lowerer, rows).expect("the rows lower")
		else {
			panic!("an array of tuple rows lowers as tuples");
		};
		assert_eq!(
			tuples.iter().map(|tuple| tuple[0].value.clone()).collect::<Vec<_>>(),
			vec![constant(Value::from(1_i64)), constant(Value::from(2_i64))]
		);
	}

	/// A subscript is a host pipeline that runs over no row. A literal index is marked as literal,
	/// and a dynamic subscript is not.
	#[test]
	fn a_subscript_is_a_host_pipeline() {
		let mut b = Builder::new();
		let value = b.literal(Value::from(7_i64));
		let zero = b.literal(Value::from(0_i64));
		let one = b.literal(Value::from(1_i64));
		let one = b.binary(BinaryOperator::Add, one, one);
		let row = tuple_row(
			&mut b,
			vec![place(vec![field("a"), PlacePart::Index(1), PlacePart::Dynamic(2)])],
			&[value, zero, one],
		);
		let rows = b.add(ExprOp::Array, &[row]);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		let InsertRowsSpec::Tuples(tuples) =
			insert_rows(&mut lowerer, rows).expect("the rows lower")
		else {
			panic!("an array of tuple rows lowers as tuples");
		};
		assert_eq!(
			tuples[0][0].place,
			vec![
				PlaceStepSpec::Field("a".to_owned()),
				PlaceStepSpec::Subscript {
					expr: 0,
					literal_index: true,
				},
				PlaceStepSpec::Subscript {
					expr: 1,
					literal_index: false,
				},
			]
		);
		let program = em
			.finish_program(Vec::new(), crate::exec::AccessMode::ReadWrite)
			.expect("the program is built");
		assert_eq!(program.host_code.len(), 2);
	}

	/// A `LET` written as the expression of a subscript is refused when the place is evaluated for
	/// a record, so its pipeline raises the refusal and the lowering of the place succeeds.
	#[test]
	fn a_let_in_a_subscript_is_a_pipeline_that_raises_when_the_place_is_evaluated() {
		let mut b = Builder::new();
		let value = b.literal(Value::from(7_i64));
		let owner = b.add_with_regions(ExprOp::Subquery(NestedStrategy::Apply), &[]);
		b.open(owner, RegionKind::Subquery, &[]);
		let bound = b.literal(Value::from(1_i64));
		b.add_phys(
			PhysOp::Let(Box::new(LetAttrs {
				name: Strand::new("a"),
				binding: LetBinding::Local,
				ty: None,
			})),
			&[bound],
		);
		b.close();
		let index = b.result_of(owner);
		let row = tuple_row(
			&mut b,
			vec![place(vec![field("a"), PlacePart::Dynamic(1)])],
			&[value, index],
		);
		let rows = b.add(ExprOp::Array, &[row]);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		insert_rows(&mut lowerer, rows).expect("a place the refusal waits in lowers");
		let program = em
			.finish_program(Vec::new(), crate::exec::AccessMode::ReadWrite)
			.expect("the program is built");
		let [subscript] = program.host_code.as_slice() else {
			panic!("the subscript is one host pipeline");
		};
		assert!(matches!(
			subscript.code.first(),
			Some(Instr::Store(StoreInstr::Refuse { refusal }))
				if **refusal == Refusal::InvalidStatement { message: LET_IN_AN_EXPRESSION.to_owned() }
		));
		let tail = &subscript.code[subscript.code.len() - 2..];
		assert!(matches!(tail, [Instr::Emit { .. }, Instr::EndRow]));
	}

	/// A filter is a host pipeline whose row is the element.
	#[test]
	fn a_filter_is_a_host_pipeline_over_the_element() {
		let mut b = Builder::new();
		let value = b.literal(Value::from(7_i64));
		let owner = b.add_with_regions(
			ExprOp::TupleRow {
				places: Box::new([place(vec![field("a"), PlacePart::Filter(0)])]),
			},
			&[value],
		);
		b.region_with_args(
			owner,
			RegionKind::Clause(ClauseRole::Where),
			&[TypeKnowledge::Unknown],
			|b, args| {
				let threshold = b.literal(Value::from(3_i64));
				b.binary(BinaryOperator::MoreThan, args[0], threshold)
			},
		);
		let row = b.result_of(owner);
		let rows = b.add(ExprOp::Array, &[row]);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		let InsertRowsSpec::Tuples(tuples) =
			insert_rows(&mut lowerer, rows).expect("the rows lower")
		else {
			panic!("an array of tuple rows lowers as tuples");
		};
		assert_eq!(tuples[0][0].place[1], PlaceStepSpec::Where(0));
		let program = em
			.finish_program(Vec::new(), crate::exec::AccessMode::ReadWrite)
			.expect("the program is built");
		assert_eq!(program.host_code.len(), 1);
	}

	/// Rows that are not an array of tuple rows are one host scalar, and an array that mixes tuple
	/// rows with other values is an invariant break.
	#[test]
	fn other_rows_are_one_value() {
		let mut b = Builder::new();
		let rows = b.literal(Value::from(1_i64));
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert_eq!(
			insert_rows(&mut lowerer, rows).expect("the rows lower"),
			InsertRowsSpec::Value(constant(Value::from(1_i64)))
		);

		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let row = tuple_row(&mut b, vec![place(vec![field("a")])], &[one]);
		let plain = b.add(
			ExprOp::Table {
				name: TableName::new(Strand::new("t")),
			},
			&[],
		);
		let rows = b.add(ExprOp::Array, &[row, plain]);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert!(matches!(insert_rows(&mut lowerer, rows), Err(IrError::Invariant { .. })));
	}

	/// The `INTO` expression is a host scalar with the text it was written with.
	#[test]
	fn an_insert_names_its_table_and_its_text() {
		let mut b = Builder::new();
		let rows = b.literal(Value::from(1_i64));
		let into = b.add(
			ExprOp::Table {
				name: TableName::new(Strand::new("t")),
			},
			&[],
		);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		let Source::WriteInserts {
			into: Some(into),
			relation,
			..
		} = inserts_source(&mut lowerer, rows, Some((into, "t")), true).expect("the source lowers")
		else {
			panic!("an insert lowers as an insert source");
		};
		assert_eq!(into.sql, "t");
		assert!(relation);
	}

	/// The targets are one per element of the array, whether it stayed an array or folded to a
	/// literal, and a mock is a mock target.
	#[test]
	fn targets_are_the_elements_of_the_array() {
		let mut b = Builder::new();
		let literal = b.literal(Value::Array(Array(vec![Value::from(1_i64), Value::from(2_i64)])));
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		let Source::WriteTargets {
			targets,
			deferable,
			guarantees,
		} = targets_source(&mut lowerer, literal, true, false).expect("the source lowers")
		else {
			panic!("a target list lowers as targets");
		};
		assert_eq!(
			targets,
			vec![
				Target::Scalar(constant(Value::from(1_i64))),
				Target::Scalar(constant(Value::from(2_i64))),
			]
		);
		assert!(deferable);
		assert!(!guarantees);

		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let mock =
			b.add(ExprOp::Mock(Box::new(Mock::Count(TableName::new(Strand::new("t")), 3))), &[]);
		let array = b.add(ExprOp::Array, &[one, mock]);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		let Source::WriteTargets {
			targets,
			..
		} = targets_source(&mut lowerer, array, false, true).expect("the source lowers")
		else {
			panic!("a target list lowers as targets");
		};
		assert_eq!(
			targets,
			vec![
				Target::Scalar(constant(Value::from(1_i64))),
				Target::Mock(Mock::Count(TableName::new(Strand::new("t")), 3)),
			]
		);
	}

	/// A `RELATE` names exactly three endpoints.
	#[test]
	fn edges_are_three_endpoints() {
		let mut b = Builder::new();
		let from = b.literal(Value::from(1_i64));
		let through = b.literal(Value::from(2_i64));
		let two = b.add(ExprOp::Array, &[from, through]);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert!(matches!(edges_source(&mut lowerer, two), Err(IrError::Invariant { .. })));
		let mut b = Builder::new();
		let from = b.literal(Value::from(1_i64));
		let through = b.literal(Value::from(2_i64));
		let to_value = b.binary(BinaryOperator::Add, from, from);
		let all = b.add(ExprOp::Array, &[from, through, to_value]);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		let Source::WriteEdges {
			from,
			through,
			to,
		} = edges_source(&mut lowerer, all).expect("the source lowers")
		else {
			panic!("a RELATE lowers as edges");
		};
		assert_eq!(from, constant(Value::from(1_i64)));
		assert_eq!(through, constant(Value::from(2_i64)));
		assert!(matches!(to, HostScalar::Code(_)));
	}
}
