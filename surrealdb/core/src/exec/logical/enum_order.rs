//! Declaration-order comparison of enum fields.
//!
//! The stored value of an enum field is its label, and `Value::cmp` orders labels lexically. A
//! table's schema says which fields hold an enum, so where a comparison, a membership test of a
//! range written in place (`field IN 'sad'..='ok'`, `'sad'.. CONTAINS field`) or an `ORDER BY`
//! meets one the plan is rewritten to compare `enum::rank(enum, field)` instead, ranking the
//! range's bounds by the same enum, which orders by declaration. A range held by a parameter or
//! computed at run time is compared as written. An index keys enum fields by label, so a rewritten
//! range predicate or ordering no longer names the field and is not served by one; equality and
//! `IN` of anything but a range written in place are left as written. A comparison of two fields
//! holding different enums has no declaration order to follow and is compared as written.
//!
//! The `MIN` and `MAX` of an enum field over a group (`math::min`, `math::max`, `array::min` and
//! `array::max` of the field in an aggregation) are the label that ranks first or last, which is
//! `enum::min` or `enum::max` over the labels the group holds.
//!
//! Only a field of the table the pipeline reads is typed, by the kind its definition declares at
//! its top level (an enum, or an enum that may also be `NONE` or `NULL`). A field of a type
//! declared over a union of labels (an alias of it, or a domain that may also be `NONE` or
//! `NULL`), at any depth of such types, orders by the declaration of that union.

use surrealdb_strand::Strand;

use super::{Descend, visit_binding_pruning_mut};
use crate::expr::logical::{
	LogicalCall, LogicalExpr, LogicalField, LogicalFields, LogicalLiteral, LogicalOp, LogicalOrder,
	LogicalOrdering, OutputPathPart,
};
use crate::expr::{BinaryOperator, Function, Kind, PrefixOperator};

/// The enum a field holds: a union type of the catalog, by name, or the labels of an inline enum.
#[derive(Clone, Debug, PartialEq, Eq)]
enum EnumRef {
	Named(String),
	Inline(Vec<String>),
}

/// The one enum an ordering comparison follows: the enum of whichever side is an enum field, or
/// `None` when both sides are fields of different enums.
fn one_enum(left: Option<EnumRef>, right: Option<EnumRef>) -> Option<EnumRef> {
	match (left, right) {
		(Some(left), Some(right)) => (left == right).then_some(left),
		(left, right) => left.or(right),
	}
}

/// The enum a field of `kind` holds: the kind itself, or a union of one enum with `NONE` and
/// `NULL`. A named type holds an enum when `union_of` names the union of labels it holds, which
/// is the type itself, or the union an alias or a domain of it is declared over.
fn enum_of(kind: &Kind, union_of: &dyn Fn(&str) -> Option<String>) -> Option<EnumRef> {
	let reference = |kind: &Kind| {
		if let Kind::Enum(labels) = kind {
			Some(EnumRef::Inline(labels.clone()))
		} else if let Kind::Named(name) = kind {
			union_of(name).map(EnumRef::Named)
		} else {
			None
		}
	};
	match kind {
		Kind::Enum(_) | Kind::Named(_) => reference(kind),
		Kind::Either(kinds) => {
			let mut found = None;
			for kind in kinds {
				match kind {
					Kind::None | Kind::Null => {}
					Kind::Enum(_) | Kind::Named(_) if found.is_none() => {
						found = Some(reference(kind)?);
					}
					Kind::Enum(_)
					| Kind::Named(_)
					| Kind::Any
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
					| Kind::Int
					| Kind::I8
					| Kind::U8
					| Kind::I16
					| Kind::U16
					| Kind::I32
					| Kind::U32
					| Kind::U64
					| Kind::F16
					| Kind::F32
					| Kind::Number
					| Kind::Object
					| Kind::String
					| Kind::Uuid
					| Kind::Regex
					| Kind::Table(_)
					| Kind::Record(_)
					| Kind::Geometry(_)
					| Kind::Either(_)
					| Kind::Set(..)
					| Kind::Array(..)
					| Kind::Function(..)
					| Kind::Range
					| Kind::Literal(_)
					| Kind::File(_) => return None,
				}
			}
			found
		}
		Kind::Any
		| Kind::None
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
		| Kind::Int
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32
		| Kind::Number
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
		| Kind::File(_) => None,
	}
}

fn string(value: &str) -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::String(Strand::new(value)))
}

/// `enum::rank(reference, value)`, where `reference` is the enum's name, or its labels for an
/// inline enum.
fn rank(reference: &EnumRef, value: LogicalExpr) -> LogicalExpr {
	let first = match reference {
		EnumRef::Named(name) => string(name),
		EnumRef::Inline(labels) => {
			LogicalExpr::Array(labels.iter().map(|label| string(label)).collect())
		}
	};
	LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal("enum::rank".to_owned()),
		args: vec![first, value],
	}))
}

/// The output fields of the stages beneath the one being rewritten: those that hold the labels of
/// an enum, and every other name an output field shadows a table field with.
#[derive(Default)]
struct Outputs {
	enums: Vec<(String, EnumRef)>,
	shadowed: Vec<String>,
}

/// The enum the field `expr` reads holds, where `expr` is a read of an output field that
/// `outputs` says holds one, or of a field `fields` types that no output field shadows.
fn field_enum<'k>(
	expr: &LogicalExpr,
	fields: &'k dyn Fn(&str) -> Option<EnumRef>,
	outputs: &Outputs,
) -> Option<EnumRef> {
	let LogicalExpr::Field(name) = expr else {
		return None;
	};
	if let Some((_, reference)) = outputs.enums.iter().find(|(alias, _)| alias == name.as_str()) {
		return Some(reference.clone());
	}
	if outputs.shadowed.iter().any(|shadowed| shadowed == name.as_str()) {
		return None;
	}
	fields(name.as_str())
}

/// The label the group's `value` holds that ranks first (`last` false) or last: `enum::min` or
/// `enum::max` over the distinct labels of the group, `NONE` for a group with none.
fn extreme(reference: &EnumRef, value: LogicalExpr, last: bool) -> LogicalExpr {
	let first = match reference {
		EnumRef::Named(name) => string(name),
		EnumRef::Inline(labels) => {
			LogicalExpr::Array(labels.iter().map(|label| string(label)).collect())
		}
	};
	let labels = LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal("array::group".to_owned()),
		args: vec![value],
	}));
	LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal(
			if last {
				"enum::max"
			} else {
				"enum::min"
			}
			.to_owned(),
		),
		args: vec![first, labels],
	}))
}

/// Whether `name` is an aggregate that folds a group to its smallest or largest value, and which.
fn extreme_of(name: &str) -> Option<bool> {
	match name {
		"math::min" | "array::min" => Some(false),
		"math::max" | "array::max" => Some(true),
		_ => None,
	}
}

fn is_ordering(op: &BinaryOperator) -> bool {
	matches!(
		op,
		BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
	)
}

/// Which operand of a membership test `op` is the member: the left one (`true`) for `INSIDE` and
/// `NOT INSIDE`, the right one for `CONTAINS` and `CONTAINS NOT`; `None` for any other operator.
fn membership(op: &BinaryOperator) -> Option<bool> {
	if matches!(op, BinaryOperator::Inside | BinaryOperator::NotInside) {
		Some(true)
	} else if matches!(op, BinaryOperator::Contain | BinaryOperator::NotContain) {
		Some(false)
	} else {
		None
	}
}

/// Ranks the bounds of `expr` by `reference` where `expr` is written as a range (`a..b`, `a..=b`,
/// `a>..b`, `a>..=b`, `..b`, `..=b`, `a..`, `a>..`), and answers whether it is one. Any other
/// expression is left as written.
fn rank_range(reference: &EnumRef, expr: &mut LogicalExpr) -> bool {
	let rank_in_place = |bound: &mut LogicalExpr| {
		let read = std::mem::replace(bound, LogicalExpr::Current);
		*bound = rank(reference, read);
	};
	if let LogicalExpr::Binary {
		left,
		op,
		right,
	} = expr
		&& matches!(
			op,
			BinaryOperator::Range
				| BinaryOperator::RangeInclusive
				| BinaryOperator::RangeSkip
				| BinaryOperator::RangeSkipInclusive
		) {
		rank_in_place(left);
		rank_in_place(right);
		return true;
	}
	if let LogicalExpr::Prefix {
		op: PrefixOperator::Range | PrefixOperator::RangeInclusive,
		expr: bound,
	}
	| LogicalExpr::Postfix {
		expr: bound,
		..
	} = expr
	{
		rank_in_place(bound);
		return true;
	}
	false
}

/// The output fields of the aggregation `fields`: those that hold the labels of an enum (a group
/// field of an enum, and the `MIN` or `MAX` of one), and the names of all the others.
fn enum_outputs<'k>(
	fields: &LogicalFields,
	typed: &'k dyn Fn(&str) -> Option<EnumRef>,
	below: &Outputs,
) -> Outputs {
	let mut found = Outputs::default();
	let LogicalFields::Select(selected) = fields else {
		return found;
	};
	for field in selected {
		let LogicalField::Single {
			expr,
			alias,
			..
		} = field
		else {
			continue;
		};
		let name = match (alias, expr) {
			(Some(path), _) => match path.parts.as_slice() {
				[OutputPathPart::Field(name)] => name.clone(),
				_ => continue,
			},
			(None, LogicalExpr::Field(name)) => name.as_str().to_owned(),
			(None, _) => continue,
		};
		let held = if let LogicalExpr::Call(call) = expr {
			match (&call.receiver, call.args.as_slice()) {
				(Function::Normal(function), [argument]) if extreme_of(function).is_some() => {
					field_enum(argument, typed, below)
				}
				_ => None,
			}
		} else {
			field_enum(expr, typed, below)
		};
		match held {
			Some(reference) => found.enums.push((name, reference)),
			None => found.shadowed.push(name),
		}
	}
	found
}

/// Rewrites the comparison, range membership or (`aggregates`) group extreme `expr` is, over enum
/// fields, and answers whether the walk enters what it holds. The walk passes over the bodies
/// that read another value than the row (an element filter, a graph or reference hop, a
/// recursion, a destructure, a guarded or broadcast rest of a path) and enters only their base.
fn rewrite_expr(
	expr: &mut LogicalExpr,
	fields: &dyn Fn(&str) -> Option<EnumRef>,
	outputs: &Outputs,
	aggregates: bool,
) {
	visit_binding_pruning_mut(expr, &mut |expr| {
		if let LogicalExpr::Filter {
			base,
			..
		}
		| LogicalExpr::Lookup {
			base,
			..
		}
		| LogicalExpr::Destructure {
			base,
			..
		}
		| LogicalExpr::Optional {
			base,
			..
		}
		| LogicalExpr::Broadcast {
			base,
			..
		} = expr
		{
			rewrite_expr(base, fields, outputs, aggregates);
			return Descend::Over;
		}
		if matches!(expr, LogicalExpr::Recurse(_)) {
			return Descend::Over;
		}
		if aggregates
			&& let LogicalExpr::Call(call) = expr
			&& let Function::Normal(name) = &call.receiver
			&& let Some(last) = extreme_of(name)
			&& let [argument] = call.args.as_slice()
			&& let Some(reference) = field_enum(argument, fields, outputs)
		{
			*expr = extreme(&reference, argument.clone(), last);
			return Descend::Over;
		}
		if let LogicalExpr::Binary {
			left,
			op,
			right,
		} = expr && is_ordering(op)
			&& let Some(reference) =
				one_enum(field_enum(left, fields, outputs), field_enum(right, fields, outputs))
		{
			let (l, r) = (
				std::mem::replace(left.as_mut(), LogicalExpr::Current),
				std::mem::replace(right.as_mut(), LogicalExpr::Current),
			);
			**left = rank(&reference, l);
			**right = rank(&reference, r);
			return Descend::Into;
		}
		if let LogicalExpr::Binary {
			left,
			op,
			right,
		} = expr && let Some(left_member) = membership(op)
		{
			let (member, range) = if left_member {
				(left, right)
			} else {
				(right, left)
			};
			if let Some(reference) = field_enum(member, fields, outputs)
				&& rank_range(&reference, range)
			{
				let read = std::mem::replace(member.as_mut(), LogicalExpr::Current);
				**member = rank(&reference, read);
			}
		}
		Descend::Into
	});
}

/// Rewrites the ordering comparisons of a mutation's `WHERE`, `cond`, over the enum fields of the
/// table it writes, whose top-level field kinds `kinds` answers.
pub(super) fn rewrite_condition<'k>(
	cond: &mut LogicalExpr,
	kinds: &'k dyn Fn(&str) -> Option<&'k Kind>,
	union_of: &dyn Fn(&str) -> Option<String>,
) {
	let resolved = |name: &str| enum_of(kinds(name)?, union_of);
	rewrite_expr(cond, &resolved, &Outputs::default(), false);
}

/// Rewrites the ordering comparisons and sort keys of `stages` over enum fields. `fields` answers
/// the declared kind of a top-level field of the table the stages read; the output fields of an
/// aggregation that hold labels of an enum order by declaration in the stages above it.
pub(super) fn rewrite_stages<'k>(
	stages: &mut [LogicalOp],
	kinds: &'k dyn Fn(&str) -> Option<&'k Kind>,
	union_of: &dyn Fn(&str) -> Option<String>,
) {
	let resolved = |name: &str| enum_of(kinds(name)?, union_of);
	let fields: &dyn Fn(&str) -> Option<EnumRef> = &resolved;
	let mut outputs = Outputs::default();
	// The stages are outermost first, and a stage reads what the ones beneath it produce.
	for stage in stages.iter_mut().rev() {
		let produced = if let LogicalOp::Aggregate(aggregate) = &*stage {
			Some(enum_outputs(&aggregate.fields, fields, &outputs))
		} else {
			None
		};
		if let LogicalOp::Sort(sort) = stage
			&& let LogicalOrdering::Order(keys) = &mut sort.ordering
		{
			for LogicalOrder {
				value,
				..
			} in keys
			{
				if let Some(reference) = field_enum(value, fields, &outputs) {
					let read = std::mem::replace(value, LogicalExpr::Current);
					*value = rank(&reference, read);
				}
			}
		}
		let aggregates = matches!(stage, LogicalOp::Aggregate(_));
		let _ = stage.visit_scalars_mut(&mut |scalar| {
			rewrite_expr(scalar, fields, &outputs, aggregates);
			Ok::<(), std::convert::Infallible>(())
		});
		if let Some(produced) = produced {
			// An aggregation's output replaces the row: above it, a name reads its output field.
			outputs = produced;
		}
	}
}
