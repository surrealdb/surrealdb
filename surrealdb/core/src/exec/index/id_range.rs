//! Record-id bounds on a table scan.
//!
//! A predicate that bounds `id` of the scanned table below, above or both selects a contiguous
//! stretch of the table's keys, since storage orders the keys of a table by record id.
//! [`plan_id_range`] decides, from the conjuncts of a read's predicate, which bounds a table scan
//! may take as a key range. It is a pure function of its inputs: it reads no catalog and
//! evaluates nothing.
//!
//! # Contract
//!
//! - **Only a narrowing of a scan already chosen.** The bounds never change which access path a
//!   read takes; a table scan that is chosen anyway reads the stretch between them. Every row read
//!   is a row the unbounded scan reads, in the same order and direction.
//! - **A superset, never exact.** The stretch holds every record the predicate admits and may hold
//!   others. No conjunct is consumed: the whole predicate is judged again over the rows the scan
//!   yields.
//! - **Scalar keys of the scanned table.** A literal bound is pushed only when it is a record id of
//!   the scanned table whose key is a number, a string or a uuid: for those the order of the stored
//!   keys is the order record ids compare in. A literal of any other kind (another table, an array,
//!   object, range or generated key, a value that is no record id, `NONE`, `NULL`) leaves its side
//!   open.
//! - **Deferred bounds are bare parameters.** A parameter that reads no row and is effect free is
//!   evaluated once when the scan opens ([`deferrable`]); a value that is not such a record id
//!   leaves its side open at run time. Under a permissioned offer only literals are pushed, since a
//!   deferred bound would be evaluated in the caller's frame, not the definer's.
//! - **Error order.** A conjunct judged on a row the bounds skip would have been judged in the
//!   unbounded scan, and could have raised. The bounds apply only when every conjunct that is not a
//!   bounding comparison against a literal or a parameter answers for every row without raising
//!   ([`answers_every_row`]), and no conjunct is withheld from the analysis.
//! - **`id = …` never bounds.** The point lookup owns equality.

use surrealdb_strand::TableName;

use super::key_range::{KeyBound, deferrable};
use crate::exec::function::FunctionRegistry;
use crate::exec::logical::pipelines::answers_every_row;
use crate::exec::logical::summaries::CatalogSummaries;
use crate::exec::planner::util::{idiom_is_id, try_literal_to_value};
use crate::expr::BinaryOperator;
use crate::expr::logical::LogicalExpr;
use crate::val::{RecordIdKey, Value};

/// One side of the record-id interval of a table scan.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IdBound {
	/// The record id, known at plan time or evaluated when the scan opens.
	pub(crate) bound: KeyBound,
	/// Whether a record equal to the bound is admitted (`>=`, `<=`).
	pub(crate) inclusive: bool,
}

/// The record-id interval a table scan reads: either side may be open.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IdRangeAccess {
	/// The lower side (`>`, `>=`), or `None` when open.
	pub(crate) low: Option<IdBound>,
	/// The upper side (`<`, `<=`), or `None` when open.
	pub(crate) high: Option<IdBound>,
}

/// Which side of the interval a comparison bounds.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
	Low,
	High,
}

/// The interval of record ids of `table` that the conjuncts `conjuncts` of a read's predicate
/// bound, or `None` when they bound neither side or may not be applied.
///
/// `withheld` says the reconstruction of the predicate withheld some conjunct, and `permissioned`
/// that a table permission governs the caller's reads of the table. See the module docs for the
/// rule.
pub(crate) fn plan_id_range(
	table: &TableName,
	conjuncts: &[&LogicalExpr],
	withheld: bool,
	permissioned: bool,
	registry: &FunctionRegistry,
) -> Option<IdRangeAccess> {
	if withheld {
		return None;
	}
	// A table with a `COMPUTED` field is never offered ids, so no field read here derives code.
	let summaries = CatalogSummaries::default();
	let mut low: Option<IdBound> = None;
	let mut high: Option<IdBound> = None;
	for conjunct in conjuncts {
		let Some((side, inclusive, other)) = comparison(conjunct) else {
			if !answers_every_row(conjunct, &summaries) {
				return None;
			}
			continue;
		};
		let slot = match side {
			Side::Low => &mut low,
			Side::High => &mut high,
		};
		match bound_of(table, other, permissioned, registry) {
			Bounding::Literal(Some(key)) => {
				let value = Value::RecordId(crate::val::RecordId {
					table: table.clone(),
					key,
				});
				*slot = Some(tighter(slot.take(), value, inclusive, side));
			}
			// A literal that is no usable bound is a total comparison that bounds nothing.
			Bounding::Literal(None) => {}
			Bounding::Deferred => {
				if slot.is_none() {
					*slot = Some(IdBound {
						bound: KeyBound::Deferred(other.clone()),
						inclusive,
					});
				}
			}
			// A bound that is neither a literal nor a bare parameter is judged like any other
			// conjunct: it must answer for every row.
			Bounding::Neither => {
				if !answers_every_row(conjunct, &summaries) {
					return None;
				}
			}
		}
	}
	(low.is_some() || high.is_some()).then_some(IdRangeAccess {
		low,
		high,
	})
}

/// The side, inclusivity and bound operand of `conjunct` when it is a comparison of `id` against
/// an operand with `<`, `<=`, `>` or `>=`, in either spelling.
fn comparison(conjunct: &LogicalExpr) -> Option<(Side, bool, &LogicalExpr)> {
	let LogicalExpr::Binary {
		left,
		op,
		right,
	} = conjunct
	else {
		return None;
	};
	// `id op b` bounds the side `op` names; `b op id` is `id (flipped op) b`.
	let (side, inclusive) = match op {
		BinaryOperator::MoreThan => (Side::Low, false),
		BinaryOperator::MoreThanEqual => (Side::Low, true),
		BinaryOperator::LessThan => (Side::High, false),
		BinaryOperator::LessThanEqual => (Side::High, true),
		BinaryOperator::Subtract
		| BinaryOperator::Add
		| BinaryOperator::Multiply
		| BinaryOperator::Divide
		| BinaryOperator::Remainder
		| BinaryOperator::Power
		| BinaryOperator::Equal
		| BinaryOperator::ExactEqual
		| BinaryOperator::NotEqual
		| BinaryOperator::AllEqual
		| BinaryOperator::AnyEqual
		| BinaryOperator::Or
		| BinaryOperator::And
		| BinaryOperator::NullCoalescing
		| BinaryOperator::TenaryCondition
		| BinaryOperator::Contain
		| BinaryOperator::NotContain
		| BinaryOperator::ContainAll
		| BinaryOperator::ContainAny
		| BinaryOperator::ContainNone
		| BinaryOperator::Inside
		| BinaryOperator::NotInside
		| BinaryOperator::AllInside
		| BinaryOperator::AnyInside
		| BinaryOperator::NoneInside
		| BinaryOperator::Outside
		| BinaryOperator::Intersects
		| BinaryOperator::Range
		| BinaryOperator::RangeInclusive
		| BinaryOperator::RangeSkip
		| BinaryOperator::RangeSkipInclusive
		| BinaryOperator::Matches(_)
		| BinaryOperator::NearestNeighbor(_) => return None,
	};
	if idiom_is_id(left) && !idiom_is_id(right) {
		return Some((side, inclusive, right));
	}
	if idiom_is_id(right) && !idiom_is_id(left) {
		let flipped = match side {
			Side::Low => Side::High,
			Side::High => Side::Low,
		};
		return Some((flipped, inclusive, left));
	}
	None
}

/// What the operand of a bounding comparison is.
enum Bounding {
	/// A literal, with the key of the record id of the scanned table it names when it is one the
	/// bounds may use.
	Literal(Option<RecordIdKey>),
	/// A bare parameter that may be evaluated when the scan opens.
	Deferred,
	/// Any other expression.
	Neither,
}

fn bound_of(
	table: &TableName,
	other: &LogicalExpr,
	permissioned: bool,
	registry: &FunctionRegistry,
) -> Bounding {
	if let Some(value) = try_literal_to_value(other) {
		let key = if let Value::RecordId(record) = value {
			if record.table == *table
				&& matches!(
					record.key,
					RecordIdKey::Number(_) | RecordIdKey::String(_) | RecordIdKey::Uuid(_)
				) {
				Some(record.key)
			} else {
				None
			}
		} else {
			None
		};
		return Bounding::Literal(key);
	}
	if matches!(other, LogicalExpr::Param(_)) && !permissioned && deferrable(other, registry) {
		Bounding::Deferred
	} else {
		Bounding::Neither
	}
}

/// The tighter of the bound `held` on `side` and the literal `value`: the greater low bound or
/// the lesser high bound, and at an equal key the exclusive one.
fn tighter(held: Option<IdBound>, value: Value, inclusive: bool, side: Side) -> IdBound {
	let new = IdBound {
		bound: KeyBound::Literal(value),
		inclusive,
	};
	let Some(held) = held else {
		return new;
	};
	let (KeyBound::Literal(Value::RecordId(old)), KeyBound::Literal(Value::RecordId(fresh))) =
		(&held.bound, &new.bound)
	else {
		// A deferred bound yields to a literal on its side.
		return new;
	};
	let new_is_tighter = match (old.key.cmp(&fresh.key), side) {
		(std::cmp::Ordering::Less, Side::Low) | (std::cmp::Ordering::Greater, Side::High) => true,
		(std::cmp::Ordering::Equal, _) => held.inclusive && !inclusive,
		(std::cmp::Ordering::Greater, Side::Low) | (std::cmp::Ordering::Less, Side::High) => false,
	};
	if new_is_tighter {
		new
	} else {
		held
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::expr::logical::{LogicalLiteral, LogicalRecordIdKey, LogicalRecordIdLit};
	use crate::expr::{Function, Param};

	fn table() -> TableName {
		TableName::from("t")
	}

	fn id() -> LogicalExpr {
		LogicalExpr::Field(Strand::new("id"))
	}

	fn field(name: &str) -> LogicalExpr {
		LogicalExpr::Field(Strand::new(name))
	}

	fn record(table: &str, key: LogicalRecordIdKey) -> LogicalExpr {
		LogicalExpr::RecordId(Box::new(LogicalRecordIdLit {
			table: TableName::from(table),
			key,
		}))
	}

	fn t(n: i64) -> LogicalExpr {
		record("t", LogicalRecordIdKey::Number(n))
	}

	fn int(n: i64) -> LogicalExpr {
		LogicalExpr::Literal(LogicalLiteral::Integer(n))
	}

	fn param(name: &str) -> LogicalExpr {
		LogicalExpr::Param(Param::from(name.to_owned()))
	}

	fn cmp(left: LogicalExpr, op: BinaryOperator, right: LogicalExpr) -> LogicalExpr {
		LogicalExpr::Binary {
			left: Box::new(left),
			op,
			right: Box::new(right),
		}
	}

	fn plan(conjuncts: &[LogicalExpr]) -> Option<IdRangeAccess> {
		plan_with(conjuncts, false, false)
	}

	fn plan_with(
		conjuncts: &[LogicalExpr],
		withheld: bool,
		permissioned: bool,
	) -> Option<IdRangeAccess> {
		let registry = FunctionRegistry::with_builtins();
		let refs: Vec<&LogicalExpr> = conjuncts.iter().collect();
		plan_id_range(&table(), &refs, withheld, permissioned, &registry)
	}

	fn literal(n: i64, inclusive: bool) -> Option<IdBound> {
		Some(IdBound {
			bound: KeyBound::Literal(Value::RecordId(crate::val::RecordId {
				table: table(),
				key: RecordIdKey::Number(n),
			})),
			inclusive,
		})
	}

	#[test]
	fn each_comparison_bounds_the_side_it_names() {
		use BinaryOperator::{LessThan, LessThanEqual, MoreThan, MoreThanEqual};
		for (op, low, high, inclusive) in [
			(MoreThan, true, false, false),
			(MoreThanEqual, true, false, true),
			(LessThan, false, true, false),
			(LessThanEqual, false, true, true),
		] {
			let access = plan(&[cmp(id(), op, t(2))]).expect("a bound");
			assert_eq!(access.low, low.then(|| literal(2, inclusive)).flatten());
			assert_eq!(access.high, high.then(|| literal(2, inclusive)).flatten());
		}
	}

	#[test]
	fn the_operands_of_a_comparison_may_be_written_either_way_round() {
		use BinaryOperator::{LessThan, MoreThan};
		assert_eq!(plan(&[cmp(t(2), LessThan, id())]), plan(&[cmp(id(), MoreThan, t(2))]));
		assert_eq!(plan(&[cmp(t(2), MoreThan, id())]), plan(&[cmp(id(), LessThan, t(2))]));
		assert_eq!(
			plan(&[cmp(t(2), BinaryOperator::LessThanEqual, id())]).expect("a bound").low,
			literal(2, true)
		);
	}

	#[test]
	fn the_tightest_literal_bound_of_a_side_is_kept() {
		use BinaryOperator::{LessThan, LessThanEqual, MoreThan, MoreThanEqual};
		let access = plan(&[
			cmp(id(), MoreThan, t(2)),
			cmp(id(), MoreThanEqual, t(5)),
			cmp(id(), MoreThan, t(3)),
			cmp(id(), LessThan, t(20)),
			cmp(id(), LessThanEqual, t(9)),
		])
		.expect("bounds");
		assert_eq!(access.low, literal(5, true));
		assert_eq!(access.high, literal(9, true));
		// A strict bound beats an inclusive one at an equal key, whichever is written first.
		for conjuncts in [
			[cmp(id(), MoreThanEqual, t(5)), cmp(id(), MoreThan, t(5))],
			[cmp(id(), MoreThan, t(5)), cmp(id(), MoreThanEqual, t(5))],
		] {
			assert_eq!(plan(&conjuncts).expect("a bound").low, literal(5, false));
		}
		for conjuncts in [
			[cmp(id(), LessThanEqual, t(5)), cmp(id(), LessThan, t(5))],
			[cmp(id(), LessThan, t(5)), cmp(id(), LessThanEqual, t(5))],
		] {
			assert_eq!(plan(&conjuncts).expect("a bound").high, literal(5, false));
		}
		// Keys compare in the order record ids do: numbers before strings.
		let access = plan(&[
			cmp(id(), MoreThan, record("t", LogicalRecordIdKey::String(Strand::new("a")))),
			cmp(id(), MoreThan, t(100)),
		])
		.expect("a bound");
		assert_eq!(
			access.low.expect("a low bound").bound,
			KeyBound::Literal(Value::RecordId(crate::val::RecordId {
				table: table(),
				key: RecordIdKey::String(Strand::new("a")),
			}))
		);
	}

	#[test]
	fn a_literal_that_is_not_a_scalar_key_of_the_table_bounds_nothing() {
		use BinaryOperator::MoreThan;
		let unusable = [
			record("u", LogicalRecordIdKey::Number(1)),
			record("t", LogicalRecordIdKey::Array(vec![int(1)])),
			record("t", LogicalRecordIdKey::Object(vec![("a".to_owned(), int(1))])),
			int(5),
			LogicalExpr::Literal(LogicalLiteral::String(Strand::new("a"))),
			LogicalExpr::Literal(LogicalLiteral::None),
			LogicalExpr::Literal(LogicalLiteral::Null),
		];
		for bound in unusable {
			assert_eq!(plan(&[cmp(id(), MoreThan, bound.clone())]), None, "{bound:?}");
		}
		// An unusable side leaves the other standing.
		let access =
			plan(&[cmp(id(), MoreThan, int(5)), cmp(id(), BinaryOperator::LessThan, t(9))])
				.expect("a bound");
		assert_eq!(access.low, None);
		assert_eq!(access.high, literal(9, false));
	}

	#[test]
	fn a_bare_parameter_is_a_deferred_bound_unless_the_table_is_permissioned() {
		use BinaryOperator::MoreThan;
		let access = plan(&[cmp(id(), MoreThan, param("c"))]).expect("a deferred bound");
		assert_eq!(
			access.low,
			Some(IdBound {
				bound: KeyBound::Deferred(param("c")),
				inclusive: false,
			})
		);
		assert_eq!(plan_with(&[cmp(id(), MoreThan, param("c"))], false, true), None);
		// A literal still bounds under a permission.
		assert!(plan_with(&[cmp(id(), MoreThan, t(2))], false, true).is_some());
	}

	#[test]
	fn a_literal_bound_on_a_side_wins_over_a_deferred_one() {
		use BinaryOperator::MoreThan;
		for conjuncts in [
			[cmp(id(), MoreThan, param("c")), cmp(id(), MoreThan, t(2))],
			[cmp(id(), MoreThan, t(2)), cmp(id(), MoreThan, param("c"))],
		] {
			assert_eq!(plan(&conjuncts).expect("a bound").low, literal(2, false));
		}
		// Of several deferred bounds the first is kept.
		let access = plan(&[cmp(id(), MoreThan, param("a")), cmp(id(), MoreThan, param("b"))])
			.expect("a bound");
		assert_eq!(access.low.expect("a bound").bound, KeyBound::Deferred(param("a")));
	}

	#[test]
	fn only_comparisons_of_id_bound() {
		use BinaryOperator::{Equal, MoreThan, NotEqual};
		assert_eq!(plan(&[cmp(id(), Equal, t(2))]), None);
		assert_eq!(plan(&[cmp(id(), NotEqual, t(2))]), None);
		assert_eq!(plan(&[cmp(field("x"), MoreThan, t(2))]), None);
		assert_eq!(plan(&[cmp(id(), MoreThan, id())]), None);
		// A comparison that is not a bare conjunct of the chain is no bound.
		let wrapped =
			cmp(cmp(id(), MoreThan, t(2)), Equal, LogicalExpr::Literal(LogicalLiteral::Bool(true)));
		assert_eq!(plan(&[wrapped]), None);
		let or = cmp(cmp(id(), MoreThan, t(2)), BinaryOperator::Or, cmp(id(), MoreThan, t(3)));
		assert_eq!(plan(&[or]), None);
	}

	#[test]
	fn another_conjunct_that_may_raise_keeps_the_scan_whole() {
		use BinaryOperator::{Equal, MoreThan};
		let bound = cmp(id(), MoreThan, t(2));
		// Total conjuncts are judged on the rows read and on no other that could raise.
		assert!(plan(&[bound.clone(), cmp(field("x"), Equal, int(1))]).is_some());
		assert!(plan(&[cmp(field("x"), Equal, int(1)), bound.clone()]).is_some());
		let call = LogicalExpr::Call(Box::new(crate::expr::logical::LogicalCall {
			receiver: Function::Normal("type::int".to_owned()),
			args: vec![field("s")],
		}));
		let raising = cmp(call, MoreThan, int(0));
		assert_eq!(plan(&[bound.clone(), raising.clone()]), None);
		assert_eq!(plan(&[raising, bound.clone()]), None);
		// A parameter is not total in a conjunct that is not itself a bound.
		assert_eq!(plan(&[bound.clone(), cmp(field("x"), Equal, param("p"))]), None);
		// A bound that is neither a literal nor a parameter must answer for every row.
		let sum = cmp(int(1), BinaryOperator::Add, int(1));
		assert_eq!(plan(&[bound.clone(), cmp(id(), MoreThan, sum)]), None);
		// A conjunct the reconstruction withheld may raise.
		assert_eq!(plan_with(&[bound], true, false), None);
	}
}
