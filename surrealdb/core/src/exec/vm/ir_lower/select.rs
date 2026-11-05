//! Per-value instruction selection: the form a compare takes from what is known of its operands.
//!
//! The functions here read a fact or a hint of an operand's type and derive none. The one method
//! of [`Lowerer`] among them reads the declared kinds of the rows the lowerer was given.

use super::super::program::{CompareOp, Instr, IntCmp, Operand};
use super::expr::{Lowerer, exactly};
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::{FrameId, ValueId};
use crate::exec::ir::iface::types::{HintSource, TypeKnowledge};
use crate::exec::ir::op_kind::OpKind;
use crate::expr::Kind;
use crate::expr::logical::FieldPath;
use crate::val::{Number, Value};

/// The instruction family a comparison lowers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CompareForm {
	/// A top-level field read as an integer against an integer literal: `CompareFieldInt`,
	/// `JumpIfFieldInt` or `FilterFieldInt`, by the position of the comparison.
	///
	/// `op` is the instruction's own operator: the comparison with its operands swapped when
	/// `reversed`, so that the instruction reads `field op lit` in both cases. `fallback` is the
	/// comparison as written, which answers a field holding anything but an integer.
	FieldInt {
		field: u32,
		lit: i64,
		op: IntCmp,
		reversed: bool,
		fallback: CompareOp,
	},
	/// The general `Compare`, `JumpIfCompare` or `FilterCompare` of `op` over two operands.
	Generic(CompareOp),
}

/// The form of the comparison `op`.
///
/// `field` is the top-level field read, as its slot in the field pool and what is known of its
/// type, and `lit` the integer literal it meets; `reversed` says the literal is the left operand.
/// The field-integer form is chosen when both are present, `op` has an integer comparison
/// ([`IntCmp::of`]) and the field's type admits an integer ([`admits_int`]). Every other
/// comparison is [`CompareForm::Generic`].
pub(super) fn compare_form(
	op: CompareOp,
	field: Option<(u32, &TypeKnowledge)>,
	lit: Option<i64>,
	reversed: bool,
) -> CompareForm {
	let (Some((field, knowledge)), Some(lit)) = (field, lit) else {
		return CompareForm::Generic(op);
	};
	let Some(int_op) = IntCmp::of(op) else {
		return CompareForm::Generic(op);
	};
	if !admits_int(knowledge) {
		return CompareForm::Generic(op);
	}
	CompareForm::FieldInt {
		field,
		lit,
		op: if reversed {
			int_op.flipped()
		} else {
			int_op
		},
		reversed,
		fallback: op,
	}
}

impl Lowerer<'_> {
	/// The typed comparison of a top-level field of an object row against an integer literal, when
	/// `data`, the comparison `op` of `left` and `right`, is one.
	///
	/// The field is read from the stored row, as an integer where it holds one; any other value is
	/// compared as the general comparison would. The form is chosen when the declared kind of the
	/// field admits an integer, or when no kind is declared. The instruction carries the general
	/// comparison as its fallback. The field is read as an operand of the row; the literal is the
	/// instruction's own. `None` leaves the comparison to the general form, which emits nothing
	/// for this call.
	pub(super) fn field_int_compare(
		&mut self,
		data: &OpData,
		op: CompareOp,
		left: ValueId,
		right: ValueId,
	) -> Result<Option<Operand>, IrError> {
		if data.frame != FrameId::new(0) {
			return Ok(None);
		}
		let (field, lit, reversed) = match (self.object_field(left), int_literal(self.ir, right)) {
			(Some(field), Some(lit)) => (field, lit, false),
			_ => match (self.object_field(right), int_literal(self.ir, left)) {
				(Some(field), Some(lit)) => (field, lit, true),
				_ => return Ok(None),
			},
		};
		let read = match reversed {
			true => right,
			false => left,
		};
		let knowledge = self.declared_kind(&field);
		let slot = self.em.field(field.as_str())?;
		match compare_form(op, Some((slot, &knowledge)), Some(lit), reversed) {
			CompareForm::Generic(_) => Ok(None),
			CompareForm::FieldInt {
				field,
				lit,
				op,
				reversed,
				fallback,
			} => {
				// The field of an object row is an operand of the row and emits nothing, so the
				// read stands for the field the instruction reads.
				self.value(read)?;
				let dst = self.em.reg()?;
				self.emit(Instr::CompareFieldInt {
					field,
					op,
					lit,
					reversed,
					fallback,
					dst,
				});
				Ok(Some(Operand::Reg(dst)))
			}
		}
	}

	/// The name of the field of an object row that `value` reads.
	fn object_field(&self, value: ValueId) -> Option<surrealdb_strand::Strand> {
		let ValueDef::OpResult {
			op,
			index: 0,
		} = self.ir.value(value)?.def
		else {
			return None;
		};
		let data = self.ir.op(op)?;
		let (
			OpKind::Expr(ExprOp::Field {
				name,
			}),
			Ok([row]),
		) = (&data.kind, exactly::<1>(data))
		else {
			return None;
		};
		(data.frame == FrameId::new(0) && self.is_object_row(row)).then(|| name.clone())
	}

	/// What the rows' schema declares for the top-level field `name`: a hint of its kind, or
	/// nothing where no kind is declared.
	fn declared_kind(&self, name: &surrealdb_strand::Strand) -> TypeKnowledge {
		let declared = self.schema.as_ref().and_then(|schema| {
			schema.field(&FieldPath::field(name.clone())).map(|field| field.kind.clone())
		});
		match declared {
			Some(kind) => TypeKnowledge::Hint {
				kind,
				source: HintSource::Annotation,
			},
			None => TypeKnowledge::Unknown,
		}
	}
}

/// The integer the literal `value` is, when it is one.
fn int_literal(ir: &crate::exec::ir::core::ir::Ir, value: ValueId) -> Option<i64> {
	let ValueDef::OpResult {
		op,
		index: 0,
	} = ir.value(value)?.def
	else {
		return None;
	};
	if let OpKind::Expr(ExprOp::Literal(literal)) = &ir.op(op)?.kind {
		return match &**literal {
			Value::Number(Number::Int(n)) => Some(*n),
			Value::Number(Number::I8(n)) => Some(*n as i64),
			Value::Number(Number::U8(n)) => Some(*n as i64),
			Value::Number(Number::I16(n)) => Some(*n as i64),
			Value::Number(Number::U16(n)) => Some(*n as i64),
			Value::Number(Number::I32(n)) => Some(*n as i64),
			Value::Number(Number::U32(n)) => Some(*n as i64),
			Value::Number(Number::U64(n)) => i64::try_from(*n).ok(),
			Value::Number(
				Number::Float(_)
				| Number::Decimal(_)
				| Number::F16(_)
				| Number::F32(_)
				| Number::BigDecimal(_),
			) => None,
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Array(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => None,
		};
	}
	None
}

/// Whether a value with the knowledge `ty` may be an integer.
///
/// A value no knowledge covers may be one, as may a value whose kind is `int`, `number`, `any`, or
/// an `either` (so `option<int>`) holding one of those. A value of any other kind is none, and its
/// comparison against an integer is never the field-integer form.
pub(super) fn admits_int(ty: &TypeKnowledge) -> bool {
	match ty {
		TypeKnowledge::Unknown => true,
		TypeKnowledge::Token(_) => false,
		TypeKnowledge::Fact {
			kind,
			..
		}
		| TypeKnowledge::Hint {
			kind,
			..
		} => kind_admits_int(kind),
	}
}

/// Whether a value of `kind` can be an integer.
fn kind_admits_int(kind: &Kind) -> bool {
	match kind {
		Kind::Any
		| Kind::Named(_)
		| Kind::Int
		| Kind::Number
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64 => true,
		Kind::Either(kinds) => kinds.iter().any(kind_admits_int),
		Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::F16
		| Kind::F32
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Set(..)
		| Kind::Array(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_)
		| Kind::Enum(_) => false,
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::TableName;

	use super::*;
	use crate::exec::ir::iface::types::{FactSource, HintSource};

	const FIELD: u32 = 3;

	fn hint(kind: Kind) -> TypeKnowledge {
		TypeKnowledge::Hint {
			kind,
			source: HintSource::Annotation,
		}
	}

	fn fact(kind: Kind) -> TypeKnowledge {
		TypeKnowledge::Fact {
			kind,
			source: FactSource::Definition,
		}
	}

	/// Reference model of the integer rule, over the kinds it names. No kind at all answers
	/// `true`.
	fn main_may_hold_int(kind: Option<&Kind>) -> bool {
		match kind {
			None => true,
			Some(
				Kind::Any
				| Kind::Int
				| Kind::Number
				| Kind::I8
				| Kind::U8
				| Kind::I16
				| Kind::U16
				| Kind::I32
				| Kind::U32
				| Kind::U64,
			) => true,
			Some(Kind::Either(kinds)) => kinds.iter().any(|kind| main_may_hold_int(Some(kind))),
			Some(_) => false,
		}
	}

	#[test]
	fn compare_form_matches_may_hold_int() {
		let record = Kind::Record(vec![TableName::from("t")]);
		let kinds = [
			Kind::Int,
			Kind::option(Kind::Int),
			Kind::Number,
			Kind::Any,
			Kind::String,
			Kind::Bool,
			record.clone(),
			Kind::Float,
			Kind::either(vec![Kind::String, Kind::Int]),
			Kind::either(vec![Kind::String, Kind::Bool]),
			Kind::either(vec![Kind::String, Kind::either(vec![record, Kind::Number])]),
			Kind::option(Kind::String),
		];
		let mut knowledge = vec![TypeKnowledge::Unknown];
		for kind in &kinds {
			knowledge.push(hint(kind.clone()));
			knowledge.push(fact(kind.clone()));
		}
		for known in &knowledge {
			let expected = match known {
				TypeKnowledge::Unknown => main_may_hold_int(None),
				TypeKnowledge::Token(_) => false,
				TypeKnowledge::Fact {
					kind,
					..
				}
				| TypeKnowledge::Hint {
					kind,
					..
				} => main_may_hold_int(Some(kind)),
			};
			assert_eq!(admits_int(known), expected, "{known:?}");
			for op in [CompareOp::Equal, CompareOp::LessThan, CompareOp::MoreThanEqual] {
				let form = compare_form(op, Some((FIELD, known)), Some(7), false);
				match form {
					CompareForm::FieldInt {
						field,
						lit,
						op: int_op,
						reversed,
						fallback,
					} => {
						assert!(expected, "{known:?}");
						assert_eq!(field, FIELD);
						assert_eq!(lit, 7);
						assert_eq!(Some(int_op), IntCmp::of(op));
						assert!(!reversed);
						assert_eq!(fallback, op);
					}
					CompareForm::Generic(generic) => {
						assert!(!expected, "{known:?}");
						assert_eq!(generic, op);
					}
				}
			}
		}
	}

	#[test]
	fn compare_form_flips_the_operator_of_a_reversed_literal() {
		let known = TypeKnowledge::Unknown;
		let form = compare_form(CompareOp::LessThan, Some((FIELD, &known)), Some(7), true);
		assert_eq!(
			form,
			CompareForm::FieldInt {
				field: FIELD,
				lit: 7,
				op: IntCmp::Gt,
				reversed: true,
				fallback: CompareOp::LessThan,
			}
		);
		let form = compare_form(CompareOp::Equal, Some((FIELD, &known)), Some(7), true);
		assert!(matches!(
			form,
			CompareForm::FieldInt {
				op: IntCmp::Eq,
				reversed: true,
				fallback: CompareOp::Equal,
				..
			}
		));
	}

	#[test]
	fn compare_form_generic_without_literal() {
		let known = TypeKnowledge::Unknown;
		// No integer literal.
		assert_eq!(
			compare_form(CompareOp::Equal, Some((FIELD, &known)), None, false),
			CompareForm::Generic(CompareOp::Equal)
		);
		// No field.
		assert_eq!(
			compare_form(CompareOp::LessThan, None, Some(1), false),
			CompareForm::Generic(CompareOp::LessThan)
		);
		assert_eq!(
			compare_form(CompareOp::LessThan, None, None, true),
			CompareForm::Generic(CompareOp::LessThan)
		);
		// An operator with no integer comparison.
		for op in [CompareOp::Contain, CompareOp::Inside, CompareOp::AllEqual, CompareOp::AnyEqual]
		{
			assert!(IntCmp::of(op).is_none());
			assert_eq!(
				compare_form(op, Some((FIELD, &known)), Some(1), false),
				CompareForm::Generic(op)
			);
		}
	}
}
