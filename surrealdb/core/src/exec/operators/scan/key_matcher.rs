//! Plan-time-compiled comparisons for key-resident graph predicates.
//!
//! A key-resident predicate (every referenced field is `id`, `in` or
//! `out`) whose shape is a conjunction of comparisons against record-id
//! literals compiles into a [`KeyPredicateMatcher`]: a synchronous, typed
//! test the scan runs on each adjacency entry as it streams off the
//! cursor — no candidate object, no evaluation context, no async
//! machinery. Every comparison delegates to [`RecordId`]'s own ordering,
//! which is exactly what `Value` comparison uses for two record ids, so
//! the matcher answers precisely as the evaluator would; any predicate
//! shape it cannot express stays on the evaluator path.

use crate::exec::parts::LookupDirection;
use crate::expr::{BinaryOperator, Expr, Literal, Part, RecordIdKeyLit};
use crate::val::{RecordId, RecordIdKey};

/// Which synthesized field one comparison reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyField {
	Id,
	In,
	Out,
}

/// One `field <op> record-id-literal` comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyTest {
	field: KeyField,
	op: BinaryOperator,
	operand: RecordId,
}

/// A conjunction of [`KeyTest`]s covering an entire predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyPredicateMatcher {
	tests: Vec<KeyTest>,
}

impl KeyPredicateMatcher {
	/// Compiles `cond` when every conjunct is a comparison between a bare
	/// `id`/`in`/`out` idiom and a static record-id literal, in either
	/// operand order. `direction` must be a single direction: under `Both`
	/// the `in`/`out` mapping differs per emitted edge in a way the
	/// evaluator resolves by falling back, so nothing is compiled.
	pub(crate) fn compile(cond: &Expr, direction: LookupDirection) -> Option<Self> {
		if matches!(direction, LookupDirection::Both | LookupDirection::Reference) {
			return None;
		}
		let mut tests = Vec::new();
		let mut stack = vec![cond];
		while let Some(expr) = stack.pop() {
			match expr {
				Expr::Binary {
					left,
					op: BinaryOperator::And,
					right,
				} => {
					stack.push(left);
					stack.push(right);
				}
				Expr::Binary {
					left,
					op,
					right,
				} => tests.push(compile_test(left, op, right)?),
				_ => return None,
			}
		}
		Some(Self {
			tests,
		})
	}

	/// Evaluates the conjunction against one adjacency entry. `source` is
	/// the scanned vertex, `edge` the edge id, `target` the far vertex;
	/// `out_direction` says whether the scan follows `->` (source is `in`)
	/// or `<-` (source is `out`).
	pub(crate) fn matches(
		&self,
		source: &RecordId,
		edge: &RecordId,
		target: &RecordId,
		out_direction: bool,
	) -> bool {
		self.tests.iter().all(|test| {
			let subject = match test.field {
				KeyField::Id => edge,
				KeyField::In if out_direction => source,
				KeyField::In => target,
				KeyField::Out if out_direction => target,
				KeyField::Out => source,
			};
			let ordering = subject.cmp(&test.operand);
			match test.op {
				BinaryOperator::Equal | BinaryOperator::ExactEqual => ordering.is_eq(),
				BinaryOperator::NotEqual => ordering.is_ne(),
				BinaryOperator::LessThan => ordering.is_lt(),
				BinaryOperator::LessThanEqual => ordering.is_le(),
				BinaryOperator::MoreThan => ordering.is_gt(),
				BinaryOperator::MoreThanEqual => ordering.is_ge(),
				_ => unreachable!("compile admits only the comparison operators"),
			}
		})
	}
}

/// Compiles one comparison, accepting the literal on either side (the
/// operator flips accordingly).
fn compile_test(left: &Expr, op: &BinaryOperator, right: &Expr) -> Option<KeyTest> {
	let (field, operand, op) = match (key_field(left), key_field(right)) {
		(Some(field), None) => (field, record_id_literal(right)?, op.clone()),
		(None, Some(field)) => (field, record_id_literal(left)?, flip(op)?),
		_ => return None,
	};
	match op {
		BinaryOperator::Equal
		| BinaryOperator::ExactEqual
		| BinaryOperator::NotEqual
		| BinaryOperator::LessThan
		| BinaryOperator::LessThanEqual
		| BinaryOperator::MoreThan
		| BinaryOperator::MoreThanEqual => Some(KeyTest {
			field,
			op,
			operand,
		}),
		_ => None,
	}
}

/// The comparison with its operands swapped.
fn flip(op: &BinaryOperator) -> Option<BinaryOperator> {
	Some(match op {
		BinaryOperator::Equal => BinaryOperator::Equal,
		BinaryOperator::ExactEqual => BinaryOperator::ExactEqual,
		BinaryOperator::NotEqual => BinaryOperator::NotEqual,
		BinaryOperator::LessThan => BinaryOperator::MoreThan,
		BinaryOperator::LessThanEqual => BinaryOperator::MoreThanEqual,
		BinaryOperator::MoreThan => BinaryOperator::LessThan,
		BinaryOperator::MoreThanEqual => BinaryOperator::LessThanEqual,
		_ => return None,
	})
}

/// Recognizes a bare `id` / `in` / `out` idiom.
fn key_field(expr: &Expr) -> Option<KeyField> {
	let Expr::Idiom(idiom) = expr else {
		return None;
	};
	let [Part::Field(name)] = idiom.0.as_slice() else {
		return None;
	};
	match name.as_str() {
		"id" => Some(KeyField::Id),
		"in" => Some(KeyField::In),
		"out" => Some(KeyField::Out),
		_ => None,
	}
}

/// Recognizes a static record-id literal with a scalar key.
fn record_id_literal(expr: &Expr) -> Option<RecordId> {
	let Expr::Literal(Literal::RecordId(lit)) = expr else {
		return None;
	};
	let key = match &lit.key {
		RecordIdKeyLit::Number(n) => RecordIdKey::Number(*n),
		RecordIdKeyLit::String(s) => RecordIdKey::String(s.clone()),
		RecordIdKeyLit::Uuid(u) => RecordIdKey::Uuid(*u),
		// Generated keys are not static; composite keys can nest
		// arbitrary expressions — leave both to the evaluator.
		RecordIdKeyLit::Array(_)
		| RecordIdKeyLit::Object(_)
		| RecordIdKeyLit::Generate(_)
		| RecordIdKeyLit::Range(_) => return None,
	};
	Some(RecordId::new(lit.table.clone(), key))
}
