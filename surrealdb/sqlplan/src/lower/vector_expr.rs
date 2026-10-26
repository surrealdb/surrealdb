//! Expression lowering of the pgvector surface: the distance functions and casts to `vector`.

use surrealdb_expr::expr::logical::{LogicalCall, LogicalExpr, LogicalIf, LogicalLiteral};
use surrealdb_expr::expr::{BinaryOperator, Function, Kind, PrefixOperator};

use super::expr::is_null_expr;
use crate::error::PlanError;
use crate::vector::{VectorMetric, parse_text};

fn call(name: &str, args: Vec<LogicalExpr>) -> LogicalExpr {
	LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal(name.to_string()),
		args,
	}))
}

fn current_item(index: i64) -> LogicalExpr {
	LogicalExpr::Index {
		base: Box::new(LogicalExpr::Current),
		index: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(index))),
	}
}

/// The distance `metric` between the lowered operands `a` and `b`, which is NULL when either is.
///
/// The operands are evaluated once each, and the engine's function is applied to them as items of
/// the array they are held in.
pub(super) fn lower_distance(metric: VectorMetric, a: LogicalExpr, b: LogicalExpr) -> LogicalExpr {
	let (x, y) = (current_item(0), current_item(1));
	let distance = match metric {
		VectorMetric::L2 => call("vector::distance::euclidean", vec![x, y]),
		VectorMetric::L1 => call("vector::distance::manhattan", vec![x, y]),
		VectorMetric::Cosine => LogicalExpr::Binary {
			left: Box::new(LogicalExpr::Literal(LogicalLiteral::Float(1.0))),
			op: BinaryOperator::Subtract,
			right: Box::new(call("vector::similarity::cosine", vec![x, y])),
		},
		VectorMetric::NegativeInnerProduct => LogicalExpr::Prefix {
			op: PrefixOperator::Negate,
			expr: Box::new(call("vector::dot", vec![x, y])),
		},
	};
	guarded(a, b, distance)
}

/// The inner product of the lowered operands `a` and `b`, which is NULL when either is.
pub(super) fn lower_inner_product(a: LogicalExpr, b: LogicalExpr) -> LogicalExpr {
	guarded(a, b, call("vector::dot", vec![current_item(0), current_item(1)]))
}

fn guarded(a: LogicalExpr, b: LogicalExpr, value: LogicalExpr) -> LogicalExpr {
	let null = LogicalExpr::Binary {
		left: Box::new(is_null_expr(current_item(0))),
		op: BinaryOperator::Or,
		right: Box::new(is_null_expr(current_item(1))),
	};
	LogicalExpr::Optional {
		base: Box::new(LogicalExpr::Array(vec![a, b])),
		then: Box::new(LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![(null, LogicalExpr::Literal(LogicalLiteral::Null))],
			otherwise: Some(value),
		}))),
	}
}

/// A cast of the lowered `operand` to `vector`, or to `vector(dimensions)`.
///
/// A text literal is read when the statement is lowered. Any other operand is read when it is
/// evaluated: text is read as the text form of a vector, and an array is a vector as it is. A
/// vector of dimensions has exactly that many elements.
pub(super) fn lower_cast(
	operand: LogicalExpr,
	dimensions: Option<usize>,
) -> Result<LogicalExpr, PlanError> {
	let kind = Kind::Array(
		Box::new(Kind::Float),
		dimensions.and_then(|dimensions| u64::try_from(dimensions).ok()),
	);
	if matches!(operand, LogicalExpr::Literal(LogicalLiteral::Null | LogicalLiteral::None)) {
		return Ok(LogicalExpr::Literal(LogicalLiteral::Null));
	}
	if let LogicalExpr::Literal(LogicalLiteral::String(text)) = &operand {
		let elements = parse_text(text.as_str())?;
		if let Some(expected) = dimensions
			&& expected != elements.len()
		{
			return Err(PlanError::DataException(format!(
				"expected {expected} dimensions, not {}",
				elements.len()
			)));
		}
		return Ok(LogicalExpr::Array(
			elements
				.into_iter()
				.map(|element| LogicalExpr::Literal(LogicalLiteral::Float(element)))
				.collect(),
		));
	}
	// The text is read as pgvector reads it, not as JSON: spaces and brackets are dropped, the rest
	// is split at the commas, and each piece is read as a float.
	let text = |s: &str| LogicalExpr::Literal(LogicalLiteral::String(s.into()));
	let mut inner = LogicalExpr::Current;
	for dropped in [" ", "\t", "\r", "\n", "[", "]"] {
		inner = call("string::replace", vec![inner, text(dropped), text("")]);
	}
	let split = LogicalExpr::Prefix {
		op: PrefixOperator::Cast(Kind::Array(Box::new(Kind::Float), None)),
		expr: Box::new(call("string::split", vec![inner, text(",")])),
	};
	let checked = LogicalExpr::If(Box::new(LogicalIf {
		branches: vec![(
			call(
				"string::matches",
				vec![
					LogicalExpr::Current,
					LogicalExpr::Literal(LogicalLiteral::Regex(crate::vector::text_form()?)),
				],
			),
			split,
		)],
		otherwise: Some(LogicalExpr::Throw(Box::new(LogicalExpr::Literal(
			LogicalLiteral::String(crate::vector::MALFORMED.into()),
		)))),
	}));
	let read = LogicalExpr::If(Box::new(LogicalIf {
		branches: vec![(call("type::is_string", vec![LogicalExpr::Current]), checked)],
		otherwise: Some(LogicalExpr::Current),
	}));
	let cast = LogicalExpr::Prefix {
		op: PrefixOperator::Cast(kind),
		expr: Box::new(read),
	};
	Ok(LogicalExpr::Binary {
		left: Box::new(LogicalExpr::Optional {
			base: Box::new(operand),
			then: Box::new(cast),
		}),
		op: BinaryOperator::NullCoalescing,
		right: Box::new(LogicalExpr::Literal(LogicalLiteral::Null)),
	})
}
