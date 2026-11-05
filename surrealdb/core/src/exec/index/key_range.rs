//! Key-range access over a table whose record ids name their components.
//!
//! A table with `DEFINE FIELD id ON reading TYPE [city: string, time: datetime]`
//! stores every record under its array key, and storage orders the keys by
//! their encoded bytes. A predicate that pins a leading run of components and
//! bounds the next one therefore selects a contiguous stretch of keys (or a
//! few stretches, one per value of an `IN`), which a scan of those key ranges
//! reads instead of the whole table. [`plan_key_range`] decides, from the
//! predicate and ordering offered to a table source, whether such an access
//! exists and what it is. It is a pure function of its inputs: it reads no
//! catalog and evaluates nothing.
//!
//! # Contract
//!
//! - **A superset, never exact.** The ranges an access describes hold every record the predicate
//!   admits and may hold others: datetime bounds widen to whole seconds, an exclusive bound may be
//!   read inclusively, and a key written before the structure was enforced may be anywhere. The
//!   predicate is always evaluated again over the rows the scan yields; no conjunct is ever
//!   consumed by the access.
//! - **Components are matched canonically.** A component is read as `id[i]`, as `id` with a
//!   first-element step for position 0, or by any idiom [`KeyStructure::component_of`] resolves:
//!   its name for an `id` field. A bare name is never read as a stored field here. A table whose id
//!   names `city` stores no `city` field, and readers take it from `id[0]`.
//! - **Bounds are literals or deferred scalars.** A literal is pushed only when it is a value the
//!   component can hold ([`ComponentKind::refusal`]), after an int component's normalisation. A
//!   deferred bound is an expression that reads no row, holds no nested statement, closure or index
//!   operator, is effect free — no clock, entropy, write or external read — and reads no stored
//!   record, so it names one value per evaluation, evaluated each time the scan opens. Under a
//!   permissioned offer only literals are pushed: a deferred bound would be evaluated in the
//!   caller's frame, not the definer's.
//! - **NONE is never a bound.** `NONE`, `NULL`, regexes, `!=`, `NOT`, `?=`, `*=`, containment
//!   operators and a whole-array parameter in an `IN` are never pushed.
//! - **An aggregated view keys its rows by their group values.** A row of `DEFINE TABLE v AS SELECT
//!   a, b, count() FROM t GROUP BY a, b` is stored under `v:[a, b]`, and a path that reads a group
//!   projected as `a` reads the element the key holds. The same rule plans a range over them, with
//!   bounds that are literals alone, of the kinds a key orders exactly (see
//!   [`view_literal_bound`]): a group value is whatever its expression yields, so no declared type
//!   vouches for a bound. A group the expression fixes to `int` (`<int> x`) is bounded as an int
//!   component is, since every value it holds is an int; any other number is never a bound, since
//!   numbers of different kinds that are equal do not share a key. A view claims no order.

use std::sync::Arc;

use chrono::TimeDelta;

use crate::catalog::{ComponentKind, ComponentOrder, KeyOrigin, KeyStructure};
use crate::exec::function::FunctionRegistry;
use crate::exec::logical::effects::effects_of_expr;
use crate::exec::planner::util::{
	idiom_is_id, idiom_matches_column, is_plan_time_computable, try_literal_to_value,
};
use crate::expr::BinaryOperator;
use crate::expr::logical::{EffectSet, LogicalExpr, LogicalLiteral, LogicalOrdering};
use crate::kvs::Direction;
use crate::val::{Datetime, Number, RecordIdKey, Value};

/// The largest number of key ranges an `IN` run expands to: the product of
/// the sizes of the `IN` lists the equality run takes. An `IN` that would
/// take the product past it ends the run and stays in the residual. The same
/// cap bounds an index `IN` expansion.
pub(crate) const MAX_KEY_RANGES: usize = 32;

/// A value a key range is built from.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum KeyBound {
	/// A value known at plan time, already one the component can hold.
	Literal(Value),
	/// An expression evaluated each time the scan opens: it reads no row of
	/// the scanned table and has no effect. Its value may be one the component
	/// cannot hold; the scan then widens the side it bounds (or ends the
	/// equality run at its position) rather than raising.
	Deferred(LogicalExpr),
}

/// The values one position of the equality run is pinned to.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum KeyEq {
	/// One value.
	One(KeyBound),
	/// Each of several values: one key range per value. Only a written list
	/// with every element pushable becomes one, with duplicates removed.
	In(Box<[KeyBound]>),
}

/// One side of the range on the position after the equality run.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct KeyRangeBound {
	pub(crate) bound: KeyBound,
	/// Whether a key equal to the bound is admitted (`>=`, `<=`).
	pub(crate) inclusive: bool,
}

/// The range on the position right after the equality run.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct KeyRangeOn {
	/// The position the range bounds: the length of the equality run.
	pub(crate) position: u32,
	/// The lower side (`>`, `>=`), or `None` when open.
	pub(crate) low: Option<KeyRangeBound>,
	/// The upper side (`<`, `<=`), or `None` when open.
	pub(crate) high: Option<KeyRangeBound>,
}

/// The order a key-range scan yields its rows in, as far as the `ORDER BY`
/// offered with it is concerned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeliveredKeyOrder {
	/// The scan delivers no order the `ORDER BY` asks for.
	None,
	/// The scan delivers the `ORDER BY` exactly: the sort is not needed.
	Exact,
	/// The scan delivers the `ORDER BY` up to the whole second of the datetime
	/// component at `position`: rows arrive in order of the second, and within
	/// one second in no particular order. The sort stays; with a `LIMIT` it
	/// may stop once a second past the rows it keeps has begun.
	Coarse {
		position: u32,
	},
}

/// A key-range access over a table whose record ids name their components:
/// the planner's decision, which builds nothing.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct KeyRangeAccess {
	/// The structure of the table's keys the access was planned against.
	pub(crate) key: Arc<KeyStructure>,
	/// The values positions `0..eq.len()` are pinned to, in position order.
	pub(crate) eq: Box<[KeyEq]>,
	/// The range on position `eq.len()`, if any.
	pub(crate) range: Option<KeyRangeOn>,
	/// The direction the ranges are read in.
	pub(crate) direction: Direction,
	/// What the scan delivers of the offered `ORDER BY`.
	pub(crate) delivers: DeliveredKeyOrder,
	/// The component position each `ORDER BY` key reads, in key order, when
	/// [`Self::delivers`] is not [`DeliveredKeyOrder::None`]; empty otherwise.
	pub(crate) ordered_by: Vec<u32>,
}

/// The key-range access the conjuncts `conjuncts` and the ordering `order`
/// admit over a table whose keys have the structure `key`, or `None` when they
/// admit none.
///
/// The rule:
///
/// 1. **Classify.** Each conjunct that compares a component with a pushable bound (see the module
///    docs) is an equality (`=`, `==`, a single-element `IN`), an `IN` over a written list, an `OR`
///    of equalities on one component (read as an `IN`), or one side of a range (`<`, `<=`, `>`,
///    `>=`), in either spelling (`c < b` or `b > c`).
/// 2. **Equality run.** For positions 0, 1, … take the first single equality on the position,
///    preferring a literal; failing that, the first `IN` whose size keeps the product of the run's
///    `IN` sizes within [`MAX_KEY_RANGES`]. The run stops at the first position with neither. Two
///    conflicting literal equalities on one position pin the first; the residual then admits
///    nothing.
/// 3. **Range.** The position after the run, unless it is past the arity or holds a record
///    (equality only), takes its tightest literal lower and upper bound, or failing a literal on a
///    side the first deferred one.
/// 4. **Applicability.** With an empty run and no range there is no access: the table is scanned.
/// 5. **Order.** When every `ORDER BY` key reads a component, with no `COLLATE` or `NUMERIC`, the
///    keys left once those on positions the run pins to one value are dropped must read the
///    positions right after the run, in order, all ascending or all descending, and every position
///    of the run must hold one value on the rows the residual admits: no `IN`, and no string
///    component pinned by a deferred value, which may be a regex that `=` matches against many
///    strings. The scan then reads forwards (ascending) or backwards (descending) and delivers the
///    order exactly, unless one of those positions is a datetime, whose keys order by the whole
///    second only ([`DeliveredKeyOrder::Coarse`] at the first such position), or a record, which
///    delivers no order. Otherwise the scan reads forwards and delivers nothing, except that an
///    ordering led by `id DESC` reads backwards, as a table scan does.
///
/// Declines outright for a structure with a component of unknown order from
/// an `id` field.
///
/// Over an aggregated view's structure ([`KeyOrigin::AggregateView`]) the
/// bounds are literals alone, never a deferred expression, because a group
/// value is whatever its group expression yielded. A component whose group
/// expression fixes its kind is bounded as the same kind of an `id` field's
/// component is; one of unknown type takes the literals
/// [`view_literal_bound`] admits, whose encodings the key orders by kind. The
/// access delivers no order whatever the kinds are: the sort stays, and an
/// ordering led by `id DESC` alone reads backwards.
pub(crate) fn plan_key_range(
	key: &Arc<KeyStructure>,
	conjuncts: &[&LogicalExpr],
	order: Option<&LogicalOrdering>,
	permissioned: bool,
	registry: &FunctionRegistry,
) -> Option<KeyRangeAccess> {
	match key.origin {
		KeyOrigin::IdField => {
			if key.components.iter().any(|c| c.kind.order() == ComponentOrder::Unknown) {
				return None;
			}
		}
		// A view's key is read whatever its components' orders are: a
		// component of unknown order still takes the bounds
		// `view_literal_bound` admits, and none of them delivers an order.
		KeyOrigin::AggregateView => {}
		// An origin added to the catalog is not one this rule reasons about.
		_ => return None,
	}
	let classifier = Classifier {
		key,
		permissioned,
		registry,
	};
	let mut constraints = Vec::new();
	for conjunct in conjuncts {
		classifier.classify(conjunct, &mut constraints);
	}

	// Step 2: the equality run.
	let mut eq: Vec<KeyEq> = Vec::new();
	let mut ranges = 1usize;
	for position in 0..key.arity() as u32 {
		let mut ones = constraints.iter().filter_map(|c| {
			if let Constraint::Eq {
				position: p,
				bound,
			} = c
			{
				(*p == position).then_some(bound)
			} else {
				None
			}
		});
		let first = ones.next();
		let one = match first {
			Some(KeyBound::Literal(_)) => first,
			_ => ones.find(|b| matches!(b, KeyBound::Literal(_))).or(first),
		};
		if let Some(bound) = one {
			eq.push(KeyEq::One(bound.clone()));
			continue;
		}
		let within_cap = constraints.iter().find_map(|c| {
			if let Constraint::In {
				position: p,
				values,
			} = c
			{
				(*p == position && ranges.saturating_mul(values.len()) <= MAX_KEY_RANGES)
					.then_some(values)
			} else {
				None
			}
		});
		match within_cap {
			Some(values) => {
				ranges *= values.len();
				eq.push(KeyEq::In(values.clone().into_boxed_slice()));
			}
			None => break,
		}
	}
	let k = eq.len() as u32;

	// Step 3: the range on the next position.
	let range = (k < key.arity() as u32 && key.order(k) != ComponentOrder::EqualityOnly)
		.then(|| range_on(&constraints, k))
		.flatten();
	let range = if key.origin == KeyOrigin::AggregateView {
		range.and_then(view_range)
	} else {
		range
	};

	// Step 4: applicability.
	if eq.is_empty() && range.is_none() {
		return None;
	}

	// Step 5: the order. Every row the residual admits holds one value at a
	// run position only when its equality is a literal, or a deferred value
	// on a component no value of another kind equals: `=` holds between a
	// string and every regex that matches it, and a deferred value that is
	// not a string widens the scan to every string at its position.
	let single_valued = eq.iter().zip(key.components.iter()).all(|(e, component)| match e {
		KeyEq::One(KeyBound::Literal(_)) => true,
		KeyEq::One(KeyBound::Deferred(_)) => !matches!(component.kind, ComponentKind::String),
		KeyEq::In(_) => false,
	});
	let pinned_to_one = |position: u32| matches!(eq.get(position as usize), Some(KeyEq::One(_)));
	let (direction, delivers, ordered_by) =
		delivered_order(key, order, k, !single_valued, pinned_to_one);
	Some(KeyRangeAccess {
		key: Arc::clone(key),
		eq: eq.into_boxed_slice(),
		range,
		direction,
		delivers,
		ordered_by,
	})
}

/// The tightest bounds on `position` among `constraints`, or `None` when it
/// has none.
fn range_on(constraints: &[Constraint], position: u32) -> Option<KeyRangeOn> {
	let mut low: Option<KeyRangeBound> = None;
	let mut high: Option<KeyRangeBound> = None;
	for constraint in constraints {
		let Constraint::Side {
			position: p,
			side,
			bound,
		} = constraint
		else {
			continue;
		};
		if *p != position {
			continue;
		}
		let slot = match side {
			Side::Low => &mut low,
			Side::High => &mut high,
		};
		*slot = Some(match slot.take() {
			None => bound.clone(),
			Some(held) => tighter(held, bound.clone(), *side),
		});
	}
	(low.is_some() || high.is_some()).then_some(KeyRangeOn {
		position,
		low,
		high,
	})
}

/// The part of `range`, on a component of an aggregated view's key, that a
/// key range can take: all of it, except a datetime lower bound whose upper
/// side is open or bounded by a value that is not a datetime. `None` when no
/// side is left.
///
/// A datetime outside the years 0000 to 9999 is keyed by text that opens with
/// its sign, which sorts before the key of every datetime within those years,
/// and a group value may be such a datetime, since no declared type keeps it
/// out. A range starting at a datetime lower bound therefore passes over the
/// keys of every later year that has a sign. A datetime upper bound within
/// the years excludes those datetimes from what the predicate admits, so
/// beside one the lower bound stands.
fn view_range(mut range: KeyRangeOn) -> Option<KeyRangeOn> {
	let datetime = |side: &Option<KeyRangeBound>| {
		matches!(
			side,
			Some(KeyRangeBound {
				bound: KeyBound::Literal(Value::Datetime(_)),
				..
			})
		)
	};
	if datetime(&range.low) && !datetime(&range.high) {
		range.low = None;
	}
	(range.low.is_some() || range.high.is_some()).then_some(range)
}

/// The tighter of two bounds on one side: of two literals the one admitting
/// less (the exclusive one at an equal value); a literal over a deferred
/// bound; of two deferred bounds the first.
fn tighter(held: KeyRangeBound, new: KeyRangeBound, side: Side) -> KeyRangeBound {
	match (&held.bound, &new.bound) {
		(KeyBound::Literal(a), KeyBound::Literal(b)) => {
			let ordering = b.cmp(a);
			let new_wins = match side {
				Side::Low => ordering.is_gt() || (ordering.is_eq() && !new.inclusive),
				Side::High => ordering.is_lt() || (ordering.is_eq() && !new.inclusive),
			};
			if new_wins {
				new
			} else {
				held
			}
		}
		(KeyBound::Deferred(_), KeyBound::Literal(_)) => new,
		(KeyBound::Literal(_), KeyBound::Deferred(_))
		| (KeyBound::Deferred(_), KeyBound::Deferred(_)) => held,
	}
}

/// The direction, the delivered order and the ordered positions of step 5 of
/// [`plan_key_range`]. `run_varies` says a position of the equality run may
/// hold several values among the rows the residual admits.
fn delivered_order(
	key: &KeyStructure,
	order: Option<&LogicalOrdering>,
	k: u32,
	run_varies: bool,
	pinned_to_one: impl Fn(u32) -> bool,
) -> (Direction, DeliveredKeyOrder, Vec<u32>) {
	let undelivered = |direction| (direction, DeliveredKeyOrder::None, Vec::new());
	let Some(LogicalOrdering::Order(keys)) = order else {
		return undelivered(Direction::Forward);
	};
	// An ordering led by `id DESC` reads backwards whatever is delivered, as a
	// table scan of the same table does; whether the key order is the order of
	// `id` is the physical plan's to judge.
	let id_desc = keys.first().is_some_and(|first| !first.direction && idiom_is_id(&first.value));
	let default = if id_desc {
		Direction::Backward
	} else {
		Direction::Forward
	};
	// The components of a view's key hold whatever their group expressions
	// yield, which no key encoding orders as the values compare.
	if run_varies || keys.is_empty() || key.origin == KeyOrigin::AggregateView {
		return undelivered(default);
	}
	let mut positions = Vec::with_capacity(keys.len());
	for order_key in keys {
		if order_key.collate || order_key.numeric {
			return undelivered(default);
		}
		match component_position(key, &order_key.value) {
			Some(position) => positions.push(position),
			None => return undelivered(default),
		}
	}
	// The keys on positions the run pins to one value order nothing.
	let free: Vec<(u32, bool)> = keys
		.iter()
		.zip(&positions)
		.filter(|(_, position)| !pinned_to_one(**position))
		.map(|(order_key, position)| (*position, order_key.direction))
		.collect();
	let Some(&(_, ascending)) = free.first() else {
		// Every key reads a position pinned to one value: any order is it.
		return (default, DeliveredKeyOrder::Exact, positions);
	};
	let consecutive = free.iter().zip(k..).all(|((position, _), expected)| *position == expected);
	if !consecutive || free.iter().any(|(_, asc)| *asc != ascending) {
		return undelivered(default);
	}
	let direction = if ascending {
		Direction::Forward
	} else {
		Direction::Backward
	};
	let mut delivers = DeliveredKeyOrder::Exact;
	for (position, _) in &free {
		let order = key.order(*position);
		if order == ComponentOrder::Exact {
		} else if order == ComponentOrder::CoarseSecond {
			delivers = DeliveredKeyOrder::Coarse {
				position: *position,
			};
			break;
		} else {
			// A record orders by nothing a key range claims; an unknown or
			// newer order is not reasoned about.
			return undelivered(default);
		}
	}
	(direction, delivers, positions)
}

/// The component position `expr` reads: `id[i]` for an `i` within the arity,
/// `id` with a first-element step for position 0, or a path that is an alias
/// of a component ([`KeyStructure::aliases`]). `None` for anything else,
/// including a path beneath a component (`city.x`, `id[0].x`).
pub(crate) fn component_position(key: &KeyStructure, expr: &LogicalExpr) -> Option<u32> {
	let is_id = |e: &LogicalExpr| matches!(e, LogicalExpr::Field(name) if name.as_str() == "id");
	let position = if let LogicalExpr::Index {
		base,
		index,
	} = expr
	{
		if is_id(base) {
			if let LogicalExpr::Literal(LogicalLiteral::Integer(i)) = index.as_ref() {
				u32::try_from(*i).ok()
			} else {
				None
			}
		} else {
			None
		}
	} else if let LogicalExpr::First(base) = expr {
		if is_id(base) {
			Some(0)
		} else {
			None
		}
	} else {
		None
	};
	if let Some(position) = position {
		return ((position as usize) < key.arity()).then_some(position);
	}
	if !expr.is_path() {
		return None;
	}
	key.aliases
		.iter()
		.find(|(alias, _)| idiom_matches_column(expr, alias))
		.map(|(_, position)| *position)
}

/// Which side of a range a bound limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
	/// `>` or `>=`.
	Low,
	/// `<` or `<=`.
	High,
}

/// What one conjunct says about one component.
#[derive(Debug)]
enum Constraint {
	/// The component equals the bound.
	Eq {
		position: u32,
		bound: KeyBound,
	},
	/// The component equals one of the bounds: at least two, distinct.
	In {
		position: u32,
		values: Vec<KeyBound>,
	},
	/// The component lies on one side of the bound.
	Side {
		position: u32,
		side: Side,
		bound: KeyRangeBound,
	},
}

/// What a comparison asks of a value, as the bound classifier needs it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Use {
	/// An equality or an element of an `IN`: the value must be one the
	/// component can hold exactly.
	Equality,
	/// A lower bound; `true` for inclusive.
	Low(bool),
	/// An upper bound; `true` for inclusive.
	High(bool),
}

struct Classifier<'a> {
	key: &'a KeyStructure,
	permissioned: bool,
	registry: &'a FunctionRegistry,
}

impl Classifier<'_> {
	/// Push what `conjunct` says about a component onto `out`, if anything.
	fn classify(&self, conjunct: &LogicalExpr, out: &mut Vec<Constraint>) {
		let LogicalExpr::Binary {
			left,
			op,
			right,
		} = conjunct
		else {
			return;
		};
		match op {
			BinaryOperator::Equal | BinaryOperator::ExactEqual => {
				if let Some(eq) = self.equality(left, right).or_else(|| self.equality(right, left))
				{
					out.push(eq);
				}
			}
			BinaryOperator::Inside => {
				if let Some(position) = component_position(self.key, left)
					&& let Some(values) = self.written_list(position, right)
				{
					out.push(one_or_in(position, values));
				}
			}
			BinaryOperator::Or => {
				if let Some(constraint) = self.or_of_equalities(conjunct) {
					out.push(constraint);
				}
			}
			BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual => {
				let inclusive =
					matches!(op, BinaryOperator::LessThanEqual | BinaryOperator::MoreThanEqual);
				let upper = matches!(op, BinaryOperator::LessThan | BinaryOperator::LessThanEqual);
				// `c < b` bounds `c` from above; `b < c` bounds it from below.
				let (position, bound, upper) = match component_position(self.key, left) {
					Some(position) => (position, right, upper),
					None => match component_position(self.key, right) {
						Some(position) => (position, left, !upper),
						None => return,
					},
				};
				let using = if upper {
					Use::High(inclusive)
				} else {
					Use::Low(inclusive)
				};
				if let Some((bound, inclusive)) = self.bound(position, bound, using) {
					out.push(Constraint::Side {
						position,
						side: if upper {
							Side::High
						} else {
							Side::Low
						},
						bound: KeyRangeBound {
							bound,
							inclusive,
						},
					});
				}
			}
			BinaryOperator::Subtract
			| BinaryOperator::Add
			| BinaryOperator::Multiply
			| BinaryOperator::Divide
			| BinaryOperator::Remainder
			| BinaryOperator::Power
			| BinaryOperator::NotEqual
			| BinaryOperator::AllEqual
			| BinaryOperator::AnyEqual
			| BinaryOperator::And
			| BinaryOperator::NullCoalescing
			| BinaryOperator::TenaryCondition
			| BinaryOperator::Contain
			| BinaryOperator::NotContain
			| BinaryOperator::ContainAll
			| BinaryOperator::ContainAny
			| BinaryOperator::ContainNone
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
			| BinaryOperator::NearestNeighbor(_) => {}
		}
	}

	/// `component = bound`, when `component` reads one and `bound` is
	/// pushable as an equality on it.
	fn equality(&self, component: &LogicalExpr, bound: &LogicalExpr) -> Option<Constraint> {
		let position = component_position(self.key, component)?;
		let (bound, _) = self.bound(position, bound, Use::Equality)?;
		Some(Constraint::Eq {
			position,
			bound,
		})
	}

	/// The distinct pushable elements of a written list, or `None` when the
	/// list is not written out, is empty, or holds an element that is not
	/// pushable.
	fn written_list(&self, position: u32, list: &LogicalExpr) -> Option<Vec<KeyBound>> {
		let LogicalExpr::Array(elements) = list else {
			return None;
		};
		if elements.is_empty() {
			return None;
		}
		let mut values = Vec::with_capacity(elements.len());
		for element in elements {
			let (bound, _) = self.bound(position, element, Use::Equality)?;
			push_distinct(&mut values, bound);
		}
		Some(values)
	}

	/// An `OR` whose every branch pins one component, the same in each, to
	/// pushable values: the union of those values as one constraint.
	fn or_of_equalities(&self, or: &LogicalExpr) -> Option<Constraint> {
		let mut branches = Vec::new();
		flatten_or(or, &mut branches);
		let mut position = None;
		let mut values = Vec::new();
		for branch in branches {
			let mut found = Vec::new();
			self.classify(branch, &mut found);
			let [constraint] = found.as_slice() else {
				return None;
			};
			let (at, branch_values) = match constraint {
				Constraint::Eq {
					position,
					bound,
				} => (*position, std::slice::from_ref(bound)),
				Constraint::In {
					position,
					values,
				} => (*position, values.as_slice()),
				Constraint::Side {
					..
				} => return None,
			};
			if *position.get_or_insert(at) != at {
				return None;
			}
			for value in branch_values {
				push_distinct(&mut values, value.clone());
			}
		}
		Some(one_or_in(position?, values))
	}

	/// `expr` as a bound on the component at `position` used as `using`, with
	/// whether the bound is inclusive, or `None` when it cannot be pushed.
	///
	/// A literal must be a value the component can hold. For an int
	/// component, an integral float or decimal reads as the int it equals; a
	/// fractional one is no equality, and as a range bound rounds to the
	/// nearest int inside the range, inclusively (`> 2.5` and `>= 2.5` are
	/// `>= 3`); a bound that is not finite or rounds outside the ints is not
	/// pushed. A component of a view's key takes a literal alone and no
	/// expression; where its kind is unknown the literals are those
	/// [`view_literal_bound`] admits.
	fn bound(&self, position: u32, expr: &LogicalExpr, using: Use) -> Option<(KeyBound, bool)> {
		let inclusive = match using {
			Use::Equality => true,
			Use::Low(inclusive) | Use::High(inclusive) => inclusive,
		};
		let component = self.key.components.get(position as usize)?;
		if let Some(value) = try_literal_to_value(expr) {
			let (value, inclusive) =
				literal_bound(self.key.origin, &component.kind, value, using, inclusive)?;
			return Some((KeyBound::Literal(value), inclusive));
		}
		// A record component compares only with a literal record id, and a
		// group value of a view with a literal alone.
		if matches!(component.kind, ComponentKind::Record(_))
			|| self.key.origin == KeyOrigin::AggregateView
			|| self.permissioned
		{
			return None;
		}
		self.deferrable(expr).then(|| (KeyBound::Deferred(expr.clone()), inclusive))
	}

	/// Whether `expr` may be evaluated once when the scan opens in place of
	/// once per row: see [`deferrable`].
	fn deferrable(&self, expr: &LogicalExpr) -> bool {
		deferrable(expr, self.registry)
	}
}

/// Whether `expr` may be evaluated once when a scan opens in place of once
/// per row: it reads no row, holds no nested statement, closure or index
/// operator, is effect free (no clock, entropy, write or external read), and
/// reads no stored record, which the statement may write between one row and
/// the next.
pub(crate) fn deferrable(expr: &LogicalExpr, registry: &FunctionRegistry) -> bool {
	if !is_plan_time_computable(expr) {
		return false;
	}
	let effects = effects_of_expr(expr, registry);
	!effects.reads_row
		&& !effects.reads_row_opaquely
		&& !effects.index_operator
		&& effects.effect_free()
		&& !effects.set.intersects(EffectSet::READS_DATA)
}

/// A literal `value` as a bound on a component of kind `kind` used as `using`,
/// with whether it is inclusive, or `None` when it is not one the component
/// can be compared with through its key.
fn literal_bound(
	origin: KeyOrigin,
	kind: &ComponentKind,
	value: Value,
	using: Use,
	inclusive: bool,
) -> Option<(Value, bool)> {
	// NONE and NULL are no component's value, and a regex compares as no
	// component's value does; `refusal` turns them down with every other
	// value of the wrong kind.
	if matches!(value, Value::None | Value::Null) {
		return None;
	}
	match kind {
		ComponentKind::Int => int_bound(&value, using, inclusive),
		ComponentKind::Record(_) => {
			// Equality only, with a record id whose key is a scalar the key
			// encoding orders exactly.
			let Value::RecordId(rid) = &value else {
				return None;
			};
			let scalar = matches!(
				rid.key,
				RecordIdKey::String(_) | RecordIdKey::Number(_) | RecordIdKey::Uuid(_)
			);
			(using == Use::Equality && scalar && kind.refusal(&value).is_none())
				.then_some((value, inclusive))
		}
		// A component of unknown type is pushed only for a view's key, by the
		// literal's own kind; from an `id` field the rule is declined before a
		// bound is read.
		ComponentKind::Unknown => {
			if origin == KeyOrigin::AggregateView {
				view_literal_bound(value, using, inclusive)
			} else {
				None
			}
		}
		ComponentKind::String
		| ComponentKind::Datetime
		| ComponentKind::Uuid
		| ComponentKind::Duration
		| ComponentKind::Bool => kind.refusal(&value).is_none().then_some((value, inclusive)),
		// A kind added to the catalog is not one this rule reasons about.
		_ => None,
	}
}

/// A number literal as a bound on an int component: see
/// [`Classifier::bound`].
fn int_bound(value: &Value, using: Use, inclusive: bool) -> Option<(Value, bool)> {
	let Value::Number(number) = value.clone() else {
		return None;
	};
	if let Some(int) = number.as_int_lossless() {
		return Some((Value::Number(Number::Int(int)), inclusive));
	}
	let rounded = match (using, number) {
		(Use::Equality, _) => return None,
		(Use::Low(_), Number::Float(f)) => Number::Float(f.ceil()),
		(Use::High(_), Number::Float(f)) => Number::Float(f.floor()),
		(Use::Low(_), Number::F32(f)) => Number::F32(f.ceil()),
		(Use::High(_), Number::F32(f)) => Number::F32(f.floor()),
		(Use::Low(_), Number::F16(f)) => {
			Number::F16(surrealdb_types::f16::from_f64(f.to_f64().ceil()))
		}
		(Use::High(_), Number::F16(f)) => {
			Number::F16(surrealdb_types::f16::from_f64(f.to_f64().floor()))
		}
		(Use::Low(_), Number::Decimal(d)) => Number::Decimal(d.ceil()),
		(Use::High(_), Number::Decimal(d)) => Number::Decimal(d.floor()),
		(Use::Low(_), Number::BigDecimal(d)) => Number::BigDecimal(Box::new(d.ceil().ok()?)),
		(Use::High(_), Number::BigDecimal(d)) => Number::BigDecimal(Box::new(d.floor().ok()?)),
		// An integer within `i64` was returned above; a `u64` above `i64::MAX` has no `i64` bound.
		(
			_,
			Number::Int(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_),
		) => return None,
	};
	// Rounding a fractional bound inwards admits the rounded value itself.
	rounded.as_int_lossless().map(|int| (Value::Number(Number::Int(int)), true))
}

/// A literal as a bound on a component of an aggregated view's key used as
/// `using`, with whether it is inclusive, or `None` when it is not pushed.
///
/// A group value is whatever its expression yielded, so no declared type says
/// what a component holds and the literal's own kind decides. A kind is pushed
/// when the key's encoding orders the values of that kind as they compare, and
/// when a value of any other kind, which may sit at the same position of
/// another row, falls on the side of the bound the comparison puts it:
/// the encoding opens with the kind of the value, in the order the kinds
/// compare.
///
/// A string is pushed for `=` and `IN` although `=` also holds between a
/// string and a regex that matches it: a regex cannot be stored in a record or
/// in a record id, so no view row's key holds one.
///
/// - **Equality, `IN` and ranges:** strings, uuids, durations and bools.
/// - **Equality, `IN` and ranges, widened:** datetimes ([`view_datetime_bound`]); a lower bound
///   stands only beside a datetime upper bound ([`view_range`]).
/// - **Equality and `IN` only:** a record id whose key is a string, an int or a uuid.
/// - **Never:** numbers, whose equal values of different kinds (`5`, `5.0`, `5dec`) do not share a
///   key; `NONE`, `NULL`, regexes, arrays, objects and everything else.
fn view_literal_bound(value: Value, using: Use, inclusive: bool) -> Option<(Value, bool)> {
	if matches!(value, Value::String(_) | Value::Uuid(_) | Value::Duration(_) | Value::Bool(_)) {
		return Some((value, inclusive));
	}
	if let Value::Datetime(datetime) = &value {
		view_datetime_bound(datetime, using, inclusive)
	} else if let Value::RecordId(rid) = &value {
		let scalar = matches!(
			rid.key,
			RecordIdKey::String(_) | RecordIdKey::Number(_) | RecordIdKey::Uuid(_)
		);
		(using == Use::Equality && scalar).then_some((value, inclusive))
	} else {
		None
	}
}

/// A datetime literal as a bound on a component of an aggregated view's key.
///
/// A datetime is keyed as its RFC 3339 text, in which a fraction of a second
/// sorts before the whole second it follows, so the key of a datetime is not
/// where the datetime compares ([`Datetime::key_second_prefix`]). A bound that
/// reaches every key of its whole second is the key of a datetime the second
/// holds only in its extremes: the lower side is the last instant before the
/// second the literal lies in, whose key sorts before every key of that second,
/// and the upper side is the whole second itself, whose key sorts after every
/// key of it. Both are inclusive; the predicate decides the keys between.
///
/// An equality is the datetime itself: equal datetimes have equal keys. A
/// datetime outside the years 0000 to 9999 has no such text, and a lower bound
/// at the first second of year 0000 has no instant before it that does: neither
/// is pushed.
fn view_datetime_bound(datetime: &Datetime, using: Use, inclusive: bool) -> Option<(Value, bool)> {
	if !datetime.in_key_range() {
		return None;
	}
	let bound = match using {
		Use::Equality => *datetime,
		Use::High(_) => datetime.floor_second(),
		Use::Low(_) => {
			let before = datetime.floor_second().checked_sub_signed(TimeDelta::nanoseconds(1))?;
			let before = Datetime(before);
			before.in_key_range().then_some(before)?
		}
	};
	let inclusive = if using == Use::Equality {
		inclusive
	} else {
		true
	};
	Some((Value::Datetime(bound), inclusive))
}

/// One value as an equality, several as an `IN`.
fn one_or_in(position: u32, mut values: Vec<KeyBound>) -> Constraint {
	if values.len() == 1 {
		Constraint::Eq {
			position,
			bound: values.remove(0),
		}
	} else {
		Constraint::In {
			position,
			values,
		}
	}
}

/// Push `bound` onto `values` unless an equal bound is there.
fn push_distinct(values: &mut Vec<KeyBound>, bound: KeyBound) {
	if !values.contains(&bound) {
		values.push(bound);
	}
}

/// The branches of a tree of `OR`s.
fn flatten_or<'a>(expr: &'a LogicalExpr, out: &mut Vec<&'a LogicalExpr>) {
	if let LogicalExpr::Binary {
		left,
		op: BinaryOperator::Or,
		right,
	} = expr
	{
		flatten_or(left, out);
		flatten_or(right, out);
	} else {
		out.push(expr);
	}
}
