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
//! - **Components are matched canonically.** A component is read as `id[i]` (`id[0]` also as
//!   `id[0]` written with a first-element step) or by any idiom [`KeyStructure::component_of`]
//!   resolves: its name for an `id` field. A bare name is never read as a stored field here: a
//!   table whose id names `city` stores no `city`, and its every reader derives it from `id[0]`.
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
//!   vouches for a bound, and a number is never one, since numbers of different kinds that are
//!   equal do not share a key. A view claims no order.

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
/// bounds are literals alone ([`view_literal_bound`]), every group value is
/// an element of unknown type, and the access delivers no order: the sort
/// stays, and an ordering led by `id DESC` alone reads backwards.
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
		// Every component of a view's key is of unknown order; the bounds that
		// can be pushed are those `view_literal_bound` admits.
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
		let mut ones = constraints.iter().filter_map(|c| match c {
			Constraint::Eq {
				position: p,
				bound,
			} if *p == position => Some(bound),
			_ => None,
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
		let within_cap = constraints.iter().find_map(|c| match c {
			Constraint::In {
				position: p,
				values,
			} if *p == position && ranges.saturating_mul(values.len()) <= MAX_KEY_RANGES => Some(values),
			_ => None,
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
	let range = match key.origin {
		KeyOrigin::AggregateView => range.and_then(view_range),
		_ => range,
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
		match key.order(*position) {
			ComponentOrder::Exact => {}
			ComponentOrder::CoarseSecond => {
				delivers = DeliveredKeyOrder::Coarse {
					position: *position,
				};
				break;
			}
			// A record orders by nothing a key range claims; an unknown or
			// newer order is not reasoned about.
			_ => return undelivered(default),
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
	let position = match expr {
		LogicalExpr::Index {
			base,
			index,
		} if is_id(base) => match index.as_ref() {
			LogicalExpr::Literal(LogicalLiteral::Integer(i)) => u32::try_from(*i).ok(),
			_ => None,
		},
		LogicalExpr::First(base) if is_id(base) => Some(0),
		_ => None,
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
			_ => {}
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
	/// pushed. A component of a view's key takes the literals
	/// [`view_literal_bound`] admits, and no expression.
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
	/// once per row: it reads no row, holds no nested statement, closure or
	/// index operator, is effect free (no clock, entropy, write or external
	/// read), and reads no stored record, which the statement may write
	/// between one row and the next.
	fn deferrable(&self, expr: &LogicalExpr) -> bool {
		if !is_plan_time_computable(expr) {
			return false;
		}
		let effects = effects_of_expr(expr, self.registry);
		!effects.reads_row
			&& !effects.reads_row_opaquely
			&& !effects.index_operator
			&& effects.effect_free()
			&& !effects.set.intersects(EffectSet::READS_DATA)
	}
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
		ComponentKind::Unknown => match origin {
			KeyOrigin::AggregateView => view_literal_bound(value, using, inclusive),
			_ => None,
		},
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
	let Value::Number(number) = *value else {
		return None;
	};
	if let Some(int) = number.as_int_lossless() {
		return Some((Value::Number(Number::Int(int)), inclusive));
	}
	let rounded = match (using, number) {
		(Use::Equality, _) => return None,
		(Use::Low(_), Number::Float(f)) => Number::Float(f.ceil()),
		(Use::High(_), Number::Float(f)) => Number::Float(f.floor()),
		(Use::Low(_), Number::Decimal(d)) => Number::Decimal(d.ceil()),
		(Use::High(_), Number::Decimal(d)) => Number::Decimal(d.floor()),
		// An int is lossless, so it never reaches here.
		(_, Number::Int(_)) => return None,
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
	match &value {
		Value::Datetime(datetime) => view_datetime_bound(datetime, using, inclusive),
		Value::RecordId(rid) => {
			let scalar = matches!(
				rid.key,
				RecordIdKey::String(_) | RecordIdKey::Number(_) | RecordIdKey::Uuid(_)
			);
			(using == Use::Equality && scalar).then_some((value, inclusive))
		}
		_ => None,
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
	match expr {
		LogicalExpr::Binary {
			left,
			op: BinaryOperator::Or,
			right,
		} => {
			flatten_or(left, out);
			flatten_or(right, out);
		}
		other => out.push(other),
	}
}

#[cfg(test)]
mod tests {
	use chrono::{TimeZone, Timelike, Utc};

	use super::*;
	use crate::exec::logical::conjuncts;
	use crate::exec::planner::fold::collect_pipeline;
	use crate::expr::logical::lower_select;
	use crate::expr::{Group, Groups, Idiom, KeyNames, Kind, KindLiteral, View};
	use crate::val::{Strand, TableName, try_encode_component};

	/// The structure of a table whose id is `TYPE [name: kind, ...]`.
	fn key(components: &[(&str, Kind)]) -> Arc<KeyStructure> {
		let names = KeyNames::new(
			components.iter().map(|(name, _)| Strand::new(*name)).collect(),
			components.len(),
		)
		.unwrap();
		let kind = Kind::Literal(KindLiteral::Array(
			components.iter().map(|(_, kind)| kind.clone()).collect(),
		));
		Arc::new(KeyStructure::from_id_field(&kind, &names).unwrap())
	}

	/// `[city: string, time: datetime]`.
	fn reading() -> Arc<KeyStructure> {
		key(&[("city", Kind::String), ("time", Kind::Datetime)])
	}

	/// `[city: string, n: int, s: string]`, every component exactly ordered.
	fn exact() -> Arc<KeyStructure> {
		key(&[("city", Kind::String), ("n", Kind::Int), ("s", Kind::String)])
	}

	/// `[r: record<person>, n: int]`.
	fn linked() -> Arc<KeyStructure> {
		key(&[("r", Kind::Record(vec![TableName::new("person")])), ("n", Kind::Int)])
	}

	/// The `WHERE` and `ORDER BY` of `SELECT * FROM t <clauses>`.
	fn clauses(clauses: &str) -> (Option<LogicalExpr>, Option<LogicalOrdering>) {
		let sql = format!("SELECT * FROM t {clauses}");
		let ast = crate::syn::parse(&sql).expect("parse");
		let mut exprs = ast.expressions;
		assert_eq!(exprs.len(), 1, "expected one statement in {sql:?}");
		let surrealdb_expr::parser::TopLevelExpr::Expr(crate::expr::Expr::Select(select)) =
			exprs.remove(0)
		else {
			panic!("`{sql}` is not a SELECT");
		};
		let (clauses, _) = collect_pipeline(lower_select(*select).expect("SELECT lowers"));
		(clauses.cond, clauses.order)
	}

	/// The access `plan_key_range` plans over `key` for `SELECT * FROM t
	/// <sql>`, offered with or without a table permission predicate.
	fn plan_as(key: &Arc<KeyStructure>, sql: &str, permissioned: bool) -> Option<KeyRangeAccess> {
		let registry = FunctionRegistry::with_builtins();
		let (cond, order) = clauses(sql);
		let conjuncts = cond.as_ref().map(conjuncts).unwrap_or_default();
		plan_key_range(key, &conjuncts, order.as_ref(), permissioned, &registry)
	}

	fn plan(key: &Arc<KeyStructure>, sql: &str) -> Option<KeyRangeAccess> {
		plan_as(key, sql, false)
	}

	/// The value of the literal `sql`.
	fn value(sql: &str) -> Value {
		let (
			Some(LogicalExpr::Binary {
				right,
				..
			}),
			_,
		) = clauses(&format!("WHERE x = {sql}"))
		else {
			panic!("expected a comparison");
		};
		try_literal_to_value(&right).expect("a literal")
	}

	fn lit(sql: &str) -> KeyBound {
		KeyBound::Literal(value(sql))
	}

	fn int(i: i64) -> KeyBound {
		KeyBound::Literal(Value::Number(Number::Int(i)))
	}

	fn one(bound: KeyBound) -> KeyEq {
		KeyEq::One(bound)
	}

	fn side(bound: KeyBound, inclusive: bool) -> Option<KeyRangeBound> {
		Some(KeyRangeBound {
			bound,
			inclusive,
		})
	}

	const D1: &str = "d'2025-01-01T10:00:00Z'";
	const D2: &str = "d'2025-01-01T11:00:00Z'";

	#[test]
	fn an_equality_run_pins_leading_components_and_bounds_the_next() {
		let key = reading();
		let access = plan(&key, &format!("WHERE city = 'London' AND time > {D1}")).unwrap();
		assert!(Arc::ptr_eq(&access.key, &key));
		assert_eq!(access.eq.as_ref(), &[one(lit("'London'"))]);
		assert_eq!(
			access.range,
			Some(KeyRangeOn {
				position: 1,
				low: side(lit(D1), false),
				high: None,
			})
		);
		assert_eq!(access.direction, Direction::Forward);
		assert_eq!(access.delivers, DeliveredKeyOrder::None);
		assert!(access.ordered_by.is_empty());

		// Mirrored, by position, and with `==`: the same access.
		for written in [
			format!("WHERE 'London' = city AND {D1} < time"),
			format!("WHERE id[0] = 'London' AND id[1] > {D1}"),
			format!("WHERE city == 'London' AND time > {D1}"),
		] {
			let other = plan(&reading(), &written).unwrap();
			assert_eq!(other.eq, access.eq, "{written}");
			assert_eq!(other.range, access.range, "{written}");
		}

		// Every component pinned: no range.
		let access = plan(&reading(), &format!("WHERE city = 'London' AND time = {D1}")).unwrap();
		assert_eq!(access.eq.as_ref(), &[one(lit("'London'")), one(lit(D1))]);
		assert_eq!(access.range, None);

		// The run stops at the first component with no equality; a later one
		// is left to the residual.
		let access = plan(&exact(), "WHERE city = 'London' AND s = 'x'").unwrap();
		assert_eq!(access.eq.as_ref(), &[one(lit("'London'"))]);
		assert_eq!(access.range, None);

		// Two different values pin the first; the residual admits nothing.
		let access = plan(&exact(), "WHERE city = 'a' AND city = 'b'").unwrap();
		assert_eq!(access.eq.as_ref(), &[one(lit("'a'"))]);

		// A two-sided range, and a range on the first component alone.
		let access =
			plan(&reading(), &format!("WHERE city = 'L' AND time >= {D1} AND time < {D2}"))
				.unwrap();
		assert_eq!(
			access.range,
			Some(KeyRangeOn {
				position: 1,
				low: side(lit(D1), true),
				high: side(lit(D2), false),
			})
		);
		let access = plan(&reading(), "WHERE city > 'a' AND city <= 'm'").unwrap();
		assert!(access.eq.is_empty());
		assert_eq!(
			access.range,
			Some(KeyRangeOn {
				position: 0,
				low: side(lit("'a'"), false),
				high: side(lit("'m'"), true),
			})
		);
	}

	#[test]
	fn an_unconstrained_leading_component_gives_no_access() {
		assert_eq!(plan(&reading(), &format!("WHERE time > {D1}")), None);
		assert_eq!(plan(&reading(), &format!("WHERE time = {D1}")), None);
		assert_eq!(plan(&reading(), "WHERE value > 3"), None);
		assert_eq!(plan(&reading(), "ORDER BY city"), None);
		assert_eq!(plan(&reading(), ""), None);
	}

	#[test]
	fn of_two_bounds_on_one_side_the_tighter_is_kept() {
		let range = |sql: &str| plan(&exact(), sql).unwrap().range.unwrap();
		let r = range("WHERE city = 'a' AND n > 3 AND n > 5");
		assert_eq!((r.low, r.high), (side(int(5), false), None));
		let r = range("WHERE city = 'a' AND n >= 5 AND n > 5");
		assert_eq!(r.low, side(int(5), false));
		let r = range("WHERE city = 'a' AND n > 5 AND n >= 5");
		assert_eq!(r.low, side(int(5), false));
		let r = range("WHERE city = 'a' AND n < 10 AND n <= 5");
		assert_eq!(r.high, side(int(5), true));
		// A literal is preferred to a bound known only when the scan opens.
		let r = range("WHERE city = 'a' AND n > $x AND n > 3");
		assert_eq!(r.low, side(int(3), false));
		let r = range("WHERE city = 'a' AND n > 3 AND n > $x");
		assert_eq!(r.low, side(int(3), false));
		// An inverted range is planned; the scan finds it empty.
		let r = range("WHERE city = 'a' AND n > 5 AND n < 3");
		assert_eq!((r.low, r.high), (side(int(5), false), side(int(3), false)));
	}

	#[test]
	fn an_in_is_deduplicated_and_capped() {
		let access = plan(&exact(), "WHERE city IN ['a', 'b', 'a'] AND n > 1").unwrap();
		assert_eq!(access.eq.as_ref(), &[KeyEq::In(vec![lit("'a'"), lit("'b'")].into())]);
		assert_eq!(access.range.unwrap().position, 1);
		// `INSIDE` is the same operator, and one element is an equality.
		let access = plan(&exact(), "WHERE city INSIDE ['a']").unwrap();
		assert_eq!(access.eq.as_ref(), &[one(lit("'a'"))]);
		// An `OR` of equalities on one component is an `IN`.
		let access =
			plan(&exact(), "WHERE city = 'a' OR city = 'b' OR city IN ['c', 'a']").unwrap();
		assert_eq!(
			access.eq.as_ref(),
			&[KeyEq::In(vec![lit("'a'"), lit("'b'"), lit("'c'")].into())]
		);
		// An `OR` over two components, or over a range, is not.
		assert_eq!(plan(&exact(), "WHERE city = 'a' OR n = 1"), None);
		assert_eq!(plan(&exact(), "WHERE city = 'a' OR city > 'm'"), None);

		let list = |n: usize| (0..n).map(|i| format!("'v{i}'")).collect::<Vec<_>>().join(", ");
		let ints = |n: usize| (0..n).map(|i| i.to_string()).collect::<Vec<_>>().join(", ");
		// 32 values fit; 33 are left to the residual.
		let access = plan(&exact(), &format!("WHERE city IN [{}]", list(32))).unwrap();
		assert!(matches!(&access.eq[0], KeyEq::In(values) if values.len() == 32));
		assert_eq!(plan(&exact(), &format!("WHERE city IN [{}]", list(33))), None);
		// The cap is on the product of the run's sizes: 8 x 4 fit, 8 x 5 end
		// the run at the second `IN`.
		let access =
			plan(&exact(), &format!("WHERE city IN [{}] AND n IN [{}]", list(8), ints(4))).unwrap();
		assert_eq!(access.eq.len(), 2);
		let access =
			plan(&exact(), &format!("WHERE city IN [{}] AND n IN [{}]", list(8), ints(5))).unwrap();
		assert_eq!(access.eq.len(), 1);
		assert_eq!(access.range, None);
		// A single equality is taken over an `IN` on the same component.
		let access = plan(&exact(), "WHERE city IN ['a', 'b'] AND city = 'b'").unwrap();
		assert_eq!(access.eq.as_ref(), &[one(lit("'b'"))]);
	}

	#[test]
	fn forms_that_none_could_satisfy_or_that_compare_otherwise_are_not_pushed() {
		for written in [
			"WHERE city = NONE",
			"WHERE city = NULL",
			"WHERE city IS NONE",
			"WHERE city = /a/",
			"WHERE city != 'a'",
			"WHERE !(city = 'a')",
			"WHERE city ?= 'a'",
			"WHERE city *= 'a'",
			"WHERE city CONTAINS 'a'",
			"WHERE 'a' INSIDE city",
			"WHERE city IN $cities",
			"WHERE city IN []",
			"WHERE city IN ['a', NONE]",
			"WHERE city IN ['a', 1]",
			"WHERE city.x = 'a'",
			"WHERE id[0].x = 'a'",
			"WHERE id[2] = 'a'",
			"WHERE city = other",
			"WHERE city = 'a' OR value = 1",
		] {
			assert_eq!(plan(&exact(), written), None, "{written}");
		}
	}

	#[test]
	fn a_literal_of_another_kind_is_not_pushed() {
		for written in [
			"WHERE city = 1",
			"WHERE city > 1",
			"WHERE city = d'2025-01-01T00:00:00Z'",
			"WHERE city = 'a' AND n = 'x'",
			"WHERE city = 'a' AND n > '5'",
		] {
			let access = plan(&exact(), written);
			assert!(
				access.as_ref().is_none_or(|a| a.eq.len() == 1 && a.range.is_none()),
				"{written}"
			);
		}
		assert_eq!(
			plan(&reading(), "WHERE city = 'a' AND time > '2025-01-01'").unwrap().range,
			None
		);
		assert_eq!(plan(&reading(), "WHERE city = 'a' AND time > 3").unwrap().range, None);
		assert_eq!(plan(&reading(), "WHERE city = 'a' AND time = 'x'").unwrap().eq.len(), 1);

		// A record component takes an equality or `IN` with a record id of one
		// of its tables whose key is a scalar.
		let access = plan(&linked(), "WHERE r = person:a AND n > 1").unwrap();
		assert_eq!(access.eq.as_ref(), &[one(lit("person:a"))]);
		assert_eq!(access.range.unwrap().position, 1);
		let access = plan(&linked(), "WHERE r IN [person:a, person:1, person:a]").unwrap();
		assert_eq!(access.eq.as_ref(), &[KeyEq::In(vec![lit("person:a"), lit("person:1")].into())]);
		for written in [
			"WHERE r = other:a",
			"WHERE r = person:[1, 2]",
			"WHERE r = person:{ a: 1 }",
			"WHERE r > person:a",
			"WHERE r = 'person:a'",
			"WHERE r = $p",
		] {
			assert_eq!(plan(&linked(), written), None, "{written}");
		}
	}

	#[test]
	fn an_int_component_reads_an_integral_number_as_the_int_it_equals() {
		let eq = |sql: &str| plan(&exact(), &format!("WHERE city = 'a' AND n = {sql}"));
		assert_eq!(eq("5").unwrap().eq[1], one(int(5)));
		assert_eq!(eq("5.0").unwrap().eq[1], one(int(5)));
		assert_eq!(eq("5dec").unwrap().eq[1], one(int(5)));
		assert_eq!(eq("5.00dec").unwrap().eq[1], one(int(5)));
		// A fractional value equals no int: the equality stays residual.
		assert_eq!(eq("5.5").unwrap().eq.len(), 1);
		assert_eq!(eq("5.5dec").unwrap().eq.len(), 1);
		// So does an `IN` holding one.
		let access = plan(&exact(), "WHERE city = 'a' AND n IN [1, 2.0, 2dec]").unwrap();
		assert_eq!(access.eq[1], KeyEq::In(vec![int(1), int(2)].into()));
		assert_eq!(plan(&exact(), "WHERE city = 'a' AND n IN [1, 2.5]").unwrap().eq.len(), 1);

		// A fractional range bound rounds inwards and becomes inclusive; an
		// integral one keeps its operator.
		let range = |sql: &str| {
			let access = plan(&exact(), &format!("WHERE city = 'a' AND n {sql}")).unwrap();
			access.range.map(|r| (r.low, r.high))
		};
		assert_eq!(range("> 2.5"), Some((side(int(3), true), None)));
		assert_eq!(range(">= 2.5"), Some((side(int(3), true), None)));
		assert_eq!(range("< 2.5"), Some((None, side(int(2), true))));
		assert_eq!(range("<= 2.5"), Some((None, side(int(2), true))));
		assert_eq!(range("> -2.5"), Some((side(int(-2), true), None)));
		assert_eq!(range("< -2.5"), Some((None, side(int(-3), true))));
		assert_eq!(range("> 2.5dec"), Some((side(int(3), true), None)));
		assert_eq!(range("< 2.5dec"), Some((None, side(int(2), true))));
		assert_eq!(range("> 5.0"), Some((side(int(5), false), None)));
		assert_eq!(range("<= 5dec"), Some((None, side(int(5), true))));
		// A bound that rounds outside the ints leaves its side open.
		assert_eq!(range("< 1e30"), None);
		assert_eq!(range("> -1e30"), None);
		assert_eq!(range("> 1e30 AND n < 4"), Some((None, side(int(4), false))));
	}

	#[test]
	fn a_deferred_bound_is_pushed_only_when_it_reads_no_row_and_has_no_effect() {
		let access = plan(&reading(), "WHERE city = $city AND time > $since - 1h").unwrap();
		assert!(matches!(&access.eq[0], KeyEq::One(KeyBound::Deferred(LogicalExpr::Param(_)))));
		assert!(matches!(
			access.range.as_ref().and_then(|r| r.low.as_ref()).map(|b| &b.bound),
			Some(KeyBound::Deferred(LogicalExpr::Binary { .. }))
		));
		// A literal is preferred to a deferred equality on one component.
		let access = plan(&reading(), "WHERE city = $city AND city = 'a'").unwrap();
		assert_eq!(access.eq.as_ref(), &[one(lit("'a'"))]);
		// A deferred element of an `IN`.
		let access = plan(&reading(), "WHERE city IN [$a, 'b', $a]").unwrap();
		assert!(matches!(&access.eq[0], KeyEq::In(values) if values.len() == 2));
		// A pure builtin of a parameter.
		assert!(plan(&reading(), "WHERE city = string::lowercase($city)").is_some());

		for written in [
			// The clock, entropy and a user-defined function's unseen body.
			"WHERE city = 'a' AND time > time::now() - 1h",
			"WHERE city = rand::string()",
			"WHERE city = fn::pick()",
			// A stored record, which the statement may write between rows.
			"WHERE city = string::concat('a', record::exists(person:a))",
			// The row, by a field, a row parameter or the document.
			"WHERE city = other",
			"WHERE city = $this.other",
			"WHERE city = $parent.city",
			"WHERE city = string::lowercase(other)",
			// A nested statement or a closure.
			"WHERE city = (SELECT VALUE c FROM x LIMIT 1)",
			"WHERE city = (|| 'a')()",
		] {
			let access = plan(&reading(), written);
			assert!(
				access.as_ref().is_none_or(|a| a.range.is_none() && a.eq.len() == 1),
				"{written}: {access:?}"
			);
			if !written.contains("AND") {
				assert_eq!(access, None, "{written}");
			}
		}
	}

	#[test]
	fn a_permissioned_offer_pushes_literals_only() {
		let sql = "WHERE city = 'a' AND time > $since";
		let access = plan(&reading(), sql).unwrap();
		assert!(access.range.is_some());
		let access = plan_as(&reading(), sql, true).unwrap();
		assert_eq!(access.eq.as_ref(), &[one(lit("'a'"))]);
		assert_eq!(access.range, None);
		assert_eq!(plan_as(&reading(), "WHERE city = $city", true), None);
		assert_eq!(plan_as(&reading(), "WHERE city IN ['a', $b]", true), None);
		let access = plan_as(&reading(), &format!("WHERE city = 'a' AND time > {D1}"), true);
		assert_eq!(access.unwrap().range.unwrap().low, side(lit(D1), false));
	}

	#[test]
	fn exactly_ordered_components_deliver_the_order() {
		let access = plan(&exact(), "WHERE city = 'a' ORDER BY n").unwrap();
		assert_eq!(
			(access.direction, access.delivers, access.ordered_by),
			(Direction::Forward, DeliveredKeyOrder::Exact, vec![1])
		);
		let access = plan(&exact(), "WHERE city = 'a' AND n > 3 ORDER BY n, s").unwrap();
		assert_eq!((access.delivers, access.ordered_by), (DeliveredKeyOrder::Exact, vec![1, 2]));
		// A key on a pinned component orders nothing and is dropped.
		let access = plan(&exact(), "WHERE city = 'a' ORDER BY city, n").unwrap();
		assert_eq!((access.delivers, access.ordered_by), (DeliveredKeyOrder::Exact, vec![0, 1]));
		let access = plan(&exact(), "WHERE city = 'a' ORDER BY city").unwrap();
		assert_eq!((access.delivers, access.ordered_by), (DeliveredKeyOrder::Exact, vec![0]));
		// By position, and by name.
		let access = plan(&exact(), "WHERE city = 'a' ORDER BY id[1]").unwrap();
		assert_eq!(access.delivers, DeliveredKeyOrder::Exact);
		// A range on the first component orders by it.
		let access = plan(&exact(), "WHERE city > 'a' ORDER BY city, n").unwrap();
		assert_eq!((access.delivers, access.ordered_by), (DeliveredKeyOrder::Exact, vec![0, 1]));
		// A deferred value pins an int component to one value: no value of
		// another kind equals an int.
		let access = plan(&exact(), "WHERE city = 'a' AND n = $n ORDER BY s").unwrap();
		assert_eq!((access.delivers, access.ordered_by), (DeliveredKeyOrder::Exact, vec![2]));
	}

	#[test]
	fn a_datetime_component_delivers_the_order_by_the_second() {
		let access =
			plan(&reading(), &format!("WHERE city = 'a' AND time > {D1} ORDER BY time")).unwrap();
		assert_eq!(
			(access.direction, access.delivers, access.ordered_by),
			(
				Direction::Forward,
				DeliveredKeyOrder::Coarse {
					position: 1
				},
				vec![1]
			)
		);
		let timed = key(&[("city", Kind::String), ("n", Kind::Int), ("time", Kind::Datetime)]);
		let access = plan(&timed, "WHERE city = 'a' ORDER BY n, time").unwrap();
		assert_eq!(
			access.delivers,
			DeliveredKeyOrder::Coarse {
				position: 2
			}
		);
		// A record component orders nothing.
		let access = plan(&linked(), "WHERE r = person:a ORDER BY n").unwrap();
		assert_eq!(access.delivers, DeliveredKeyOrder::Exact);
		let linked_last = key(&[("n", Kind::Int), ("r", Kind::Record(vec![]))]);
		let access = plan(&linked_last, "WHERE n = 1 ORDER BY r").unwrap();
		assert_eq!(access.delivers, DeliveredKeyOrder::None);
		// A city known only when the scan opens may be a regex matching
		// several: the seconds of different cities interleave.
		let access = plan(&reading(), "WHERE city = $c ORDER BY time").unwrap();
		assert_eq!((access.delivers, access.ordered_by), (DeliveredKeyOrder::None, vec![]));
	}

	#[test]
	fn an_order_the_scan_cannot_deliver_claims_nothing() {
		for written in [
			// An `IN` in the run.
			"WHERE city IN ['a', 'b'] ORDER BY n",
			"WHERE city IN ['a', 'b'] ORDER BY city",
			// A string component pinned by a deferred value, which may be a
			// regex `=` matches against many strings.
			"WHERE city = $c ORDER BY n",
			"WHERE city = $c AND n = 1 ORDER BY s",
			// A gap, the wrong position, mixed directions.
			"WHERE city = 'a' ORDER BY s",
			"WHERE city = 'a' ORDER BY s, n",
			"WHERE city = 'a' ORDER BY n ASC, s DESC",
			// A key that reads no component, or compares otherwise.
			"WHERE city = 'a' ORDER BY n, value",
			"WHERE city = 'a' ORDER BY value",
			"WHERE city = 'a' ORDER BY s COLLATE",
			"WHERE city = 'a' ORDER BY n NUMERIC",
			"WHERE city = 'a' ORDER BY id",
			"WHERE city = 'a' ORDER BY RAND()",
		] {
			let access = plan(&exact(), written).unwrap();
			assert_eq!(
				(access.delivers, access.ordered_by.as_slice()),
				(DeliveredKeyOrder::None, &[][..]),
				"{written}"
			);
			assert_eq!(access.direction, Direction::Forward, "{written}");
		}
	}

	#[test]
	fn a_descending_order_reads_backwards() {
		let access = plan(&exact(), "WHERE city = 'a' ORDER BY n DESC, s DESC").unwrap();
		assert_eq!(
			(access.direction, access.delivers, access.ordered_by),
			(Direction::Backward, DeliveredKeyOrder::Exact, vec![1, 2])
		);
		let access = plan(&reading(), "WHERE city = 'a' ORDER BY city ASC, time DESC").unwrap();
		assert_eq!(
			(access.direction, access.delivers),
			(
				Direction::Backward,
				DeliveredKeyOrder::Coarse {
					position: 1
				}
			)
		);
		// `ORDER BY id DESC` reads backwards as a table scan does, claiming
		// nothing.
		let access = plan(&reading(), "WHERE city = 'a' ORDER BY id DESC").unwrap();
		assert_eq!(
			(access.direction, access.delivers),
			(Direction::Backward, DeliveredKeyOrder::None)
		);
		let access = plan(&reading(), "WHERE city = 'a' ORDER BY id DESC, time").unwrap();
		assert_eq!(
			(access.direction, access.delivers),
			(Direction::Backward, DeliveredKeyOrder::None)
		);
	}

	#[test]
	fn a_structure_the_rule_does_not_reason_about_is_declined() {
		// A stored component type outside the allow-list.
		let names = KeyNames::new(vec![Strand::new("city"), Strand::new("v")], 2).unwrap();
		let kind = Kind::Literal(KindLiteral::Array(vec![Kind::String, Kind::Float]));
		let unknown = Arc::new(KeyStructure::from_stored_id_field(&kind, &names).unwrap());
		assert_eq!(plan(&unknown, "WHERE city = 'a'"), None);
		// The same structure from an `id` field is planned.
		assert!(plan(&reading(), "WHERE city = 'a'").is_some());
	}

	#[test]
	fn a_component_is_read_by_its_name_or_its_position() {
		let key = reading();
		let id = || Box::new(LogicalExpr::Field("id".into()));
		let at = |i: i64| LogicalExpr::Index {
			base: id(),
			index: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(i))),
		};
		assert_eq!(component_position(&key, &at(0)), Some(0));
		assert_eq!(component_position(&key, &at(1)), Some(1));
		assert_eq!(component_position(&key, &at(2)), None);
		assert_eq!(component_position(&key, &at(-1)), None);
		assert_eq!(component_position(&key, &LogicalExpr::First(id())), Some(0));
		assert_eq!(component_position(&key, &LogicalExpr::Field("city".into())), Some(0));
		assert_eq!(component_position(&key, &LogicalExpr::Field("time".into())), Some(1));
		assert_eq!(component_position(&key, &LogicalExpr::Field("value".into())), None);
		assert_eq!(component_position(&key, &LogicalExpr::Field("id".into())), None);
		let beneath = LogicalExpr::Access {
			base: Box::new(LogicalExpr::Field("city".into())),
			field: "x".into(),
		};
		assert_eq!(component_position(&key, &beneath), None);
	}

	/// The structure of the rows of `DEFINE TABLE v AS SELECT <fields> FROM t
	/// GROUP BY <groups>`.
	fn view(fields: &str, groups: &[&str]) -> Arc<KeyStructure> {
		let view = View {
			expr: crate::syn::runtime::fields_for_definition(fields).expect("fields parse"),
			what: vec![TableName::new("t")],
			cond: None,
			group: Some(Groups(
				groups.iter().map(|g| Group(g.parse::<Idiom>().expect("an idiom"))).collect(),
			)),
		};
		let definition =
			crate::catalog::view::view_to_definition(&view).expect("an aggregated view");
		Arc::new(KeyStructure::from_view(&definition).expect("a view with groups has a key"))
	}

	/// `SELECT a, b, c, count() AS n FROM t GROUP BY a, b, c`.
	fn grouped() -> Arc<KeyStructure> {
		view("a, b, c, count() AS n", &["a", "b", "c"])
	}

	const UUID: &str = "u'8424486b-85b3-4448-ac8d-5d51083391c7'";

	#[test]
	fn an_aggregated_view_plans_a_key_range_over_its_group_values() {
		let key = grouped();
		assert_eq!(key.origin, KeyOrigin::AggregateView);
		let access = plan(&key, "WHERE a = 'x' AND b > 'm' AND b <= 'q'").unwrap();
		assert!(Arc::ptr_eq(&access.key, &key));
		assert_eq!(access.eq.as_ref(), &[one(lit("'x'"))]);
		assert_eq!(
			access.range,
			Some(KeyRangeOn {
				position: 1,
				low: side(lit("'m'"), false),
				high: side(lit("'q'"), true),
			})
		);
		assert_eq!(access.direction, Direction::Forward);

		// By position, mirrored, with `==`: the same access.
		for written in [
			"WHERE id[0] = 'x' AND id[1] > 'm' AND id[1] <= 'q'",
			"WHERE 'x' = a AND 'm' < b AND 'q' >= b",
			"WHERE a == 'x' AND b > 'm' AND b <= 'q'",
		] {
			let other = plan(&key, written).unwrap();
			assert_eq!(
				(other.eq, other.range),
				(access.eq.clone(), access.range.clone()),
				"{written}"
			);
		}

		// The whole key pinned; a range on the first group alone; an `IN` and an
		// `OR` of equalities, deduplicated; the run stops at an unpinned group.
		let access = plan(&key, "WHERE a = 'x' AND b = 'y' AND c = 'z'").unwrap();
		assert_eq!(access.eq.as_ref(), &[one(lit("'x'")), one(lit("'y'")), one(lit("'z'"))]);
		assert_eq!(access.range, None);
		let access = plan(&key, "WHERE a >= 'x'").unwrap();
		assert!(access.eq.is_empty());
		assert_eq!(access.range.unwrap().low, side(lit("'x'"), true));
		let access = plan(&key, "WHERE a IN ['x', 'y', 'x'] AND b = 'z'").unwrap();
		assert_eq!(
			access.eq.as_ref(),
			&[KeyEq::In(vec![lit("'x'"), lit("'y'")].into()), one(lit("'z'"))]
		);
		let access = plan(&key, "WHERE a = 'x' OR a = 'y'").unwrap();
		assert_eq!(access.eq.as_ref(), &[KeyEq::In(vec![lit("'x'"), lit("'y'")].into())]);
		let access = plan(&key, "WHERE a = 'x' AND c = 'z'").unwrap();
		assert_eq!(access.eq.as_ref(), &[one(lit("'x'"))]);

		// A group that is not the first alone, or no group at all: a scan.
		assert_eq!(plan(&key, "WHERE b = 'y'"), None);
		assert_eq!(plan(&key, "WHERE n = 1"), None);
		assert_eq!(plan(&key, ""), None);
		// The offer of a table permission changes nothing: no bound is deferred.
		let sql = "WHERE a = 'x' AND b > 'm'";
		assert_eq!(plan_as(&key, sql, true), plan(&key, sql));
	}

	/// A key orders a kind exactly when its bytes order the values as they
	/// compare, which is what a bound on a group of unknown type needs: a row
	/// of another kind at the same position falls on the side the kinds' order
	/// puts it.
	#[test]
	fn a_view_pushes_the_literals_a_key_orders_exactly() {
		let key = grouped();
		for literal in ["'x'", "true", "false", UUID, "1h30m"] {
			let access = plan(&key, &format!("WHERE a = {literal}")).expect(literal);
			assert_eq!(access.eq.as_ref(), &[one(lit(literal))], "{literal}");
			let access =
				plan(&key, &format!("WHERE a > {literal} AND a <= {literal}")).expect(literal);
			assert!(access.eq.is_empty(), "{literal}");
			assert_eq!(
				access.range,
				Some(KeyRangeOn {
					position: 0,
					low: side(lit(literal), false),
					high: side(lit(literal), true),
				}),
				"{literal}"
			);
			// An `IN` takes them too, mixed.
			let access = plan(&key, &format!("WHERE a IN [{literal}, 'other']")).expect(literal);
			assert!(matches!(&access.eq[0], KeyEq::In(values) if values.len() == 2), "{literal}");
		}

		// A record id with a key of one of the three scalar kinds, as an
		// equality alone.
		let by_uuid = format!("person:{UUID}");
		for record in ["person:x", "person:1", by_uuid.as_str()] {
			let access = plan(&key, &format!("WHERE a = {record}")).expect(record);
			assert_eq!(access.eq.as_ref(), &[one(lit(record))], "{record}");
			assert_eq!(plan(&key, &format!("WHERE a > {record}")), None, "{record}");
			assert_eq!(plan(&key, &format!("WHERE a <= {record}")), None, "{record}");
		}
		let access = plan(&key, "WHERE a IN [person:x, person:1, person:x]").unwrap();
		assert_eq!(access.eq.as_ref(), &[KeyEq::In(vec![lit("person:x"), lit("person:1")].into())]);
		for record in ["person:[1, 2]", "person:{ x: 1 }", "person:rand()"] {
			assert_eq!(plan(&key, &format!("WHERE a = {record}")), None, "{record}");
		}
	}

	#[test]
	fn a_view_never_pushes_a_number() {
		let key = grouped();
		// Equal numbers of different kinds do not share a key, so a number
		// bound reaches only the keys written the same way.
		for written in [
			"WHERE a = 5",
			"WHERE a = 5.0",
			"WHERE a = 5dec",
			"WHERE a > 5",
			"WHERE a <= 5.5",
			"WHERE a IN [1, 2]",
			// One element that is not pushable leaves the whole `IN` to the
			// predicate.
			"WHERE a IN ['x', 5]",
			"WHERE a = 5 OR a = 'x'",
		] {
			assert_eq!(plan(&key, written), None, "{written}");
		}
		// The run and the range stop at a number, and keep what precedes it.
		let access = plan(&key, "WHERE a = 'x' AND b = 5").unwrap();
		assert_eq!((access.eq.as_ref(), access.range), (&[one(lit("'x'"))][..], None));
		let access = plan(&key, "WHERE a = 'x' AND b > 5 AND b < 9").unwrap();
		assert_eq!((access.eq.as_ref(), access.range), (&[one(lit("'x'"))][..], None));
		// A bound of another kind on the same side is still pushed.
		let access = plan(&key, "WHERE a = 'x' AND b > 5 AND b > 'm'").unwrap();
		assert_eq!(access.range.unwrap().low, side(lit("'m'"), false));
	}

	#[test]
	fn a_view_pushes_no_value_that_none_could_satisfy_or_that_compares_otherwise() {
		let key = grouped();
		for written in [
			"WHERE a = NONE",
			"WHERE a = NULL",
			"WHERE a IS NONE",
			"WHERE a = /x/",
			"WHERE a = [1]",
			"WHERE a = ['x']",
			"WHERE a = { x: 1 }",
			"WHERE a != 'x'",
			"WHERE !(a = 'x')",
			"WHERE a ?= 'x'",
			"WHERE a *= 'x'",
			"WHERE a CONTAINS 'x'",
			"WHERE 'x' INSIDE a",
			"WHERE a IN $values",
			"WHERE a IN []",
			"WHERE a = 'x' OR n = 1",
			"WHERE a.x = 'x'",
			"WHERE id[3] = 'x'",
		] {
			assert_eq!(plan(&key, written), None, "{written}");
		}
	}

	/// A bound known only when the scan opens is a value of any kind, and a
	/// group of unknown type has no kind to check it against.
	#[test]
	fn a_view_pushes_no_bound_known_only_when_the_scan_opens() {
		let key = grouped();
		for written in [
			"WHERE a = $x",
			"WHERE a > $x",
			"WHERE a IN [$x, 'y']",
			"WHERE a = string::lowercase('X')",
			"WHERE a = time::now()",
		] {
			assert_eq!(plan(&key, written), None, "{written}");
		}
		let access = plan(&key, "WHERE a = 'x' AND b = $x AND c > $y").unwrap();
		assert_eq!((access.eq.as_ref(), access.range), (&[one(lit("'x'"))][..], None));
	}

	#[test]
	fn a_view_is_addressed_by_the_names_its_groups_are_projected_under() {
		// A renamed group is read by its new name alone.
		let renamed = view("a AS city, b, count() AS n", &["a", "b"]);
		let access = plan(&renamed, "WHERE city = 'x' AND b > 'm'").unwrap();
		assert_eq!(access.eq.as_ref(), &[one(lit("'x'"))]);
		assert_eq!(access.range.unwrap().position, 1);
		assert_eq!(plan(&renamed, "WHERE a = 'x'"), None);
		// A group no item projects has no name, and its position still reads it.
		let hidden = view("a, count() AS n", &["a", "b"]);
		assert_eq!(plan(&hidden, "WHERE b = 'y'"), None);
		let access = plan(&hidden, "WHERE id[0] = 'x' AND id[1] = 'y'").unwrap();
		assert_eq!(access.eq.as_ref(), &[one(lit("'x'")), one(lit("'y'"))]);
		// A group that is an expression is read under the alias it is projected as.
		let bucketed = view("time::floor(ts, 1h) AS bucket, count() AS n", &["bucket"]);
		let access = plan(&bucketed, "WHERE bucket = d'2025-01-01T10:00:00Z'").unwrap();
		assert_eq!(access.eq.as_ref(), &[one(lit("d'2025-01-01T10:00:00Z'"))]);
		assert_eq!(plan(&bucketed, "WHERE ts = d'2025-01-01T10:00:00Z'"), None);
		// An item that computes from a group is not a group.
		let computed = view("a + 1 AS bumped, count() AS n", &["a"]);
		assert_eq!(plan(&computed, "WHERE bumped = 'x'"), None);
	}

	#[test]
	fn a_view_claims_no_order() {
		let key = grouped();
		for written in [
			"WHERE a = 'x' ORDER BY b",
			"WHERE a = 'x' ORDER BY b, c",
			"WHERE a = 'x' ORDER BY a",
			"WHERE a = 'x' ORDER BY a, b",
			"WHERE a > 'x' ORDER BY a",
			"WHERE a = 'x' ORDER BY b DESC",
			"WHERE a = 'x' ORDER BY c",
		] {
			let access = plan(&key, written).unwrap();
			assert_eq!(
				(access.direction, access.delivers, access.ordered_by.as_slice()),
				(Direction::Forward, DeliveredKeyOrder::None, &[][..]),
				"{written}"
			);
		}
		// `ORDER BY id DESC` reads backwards as a table scan does, claiming nothing.
		let access = plan(&key, "WHERE a = 'x' ORDER BY id DESC").unwrap();
		assert_eq!(
			(access.direction, access.delivers, access.ordered_by.as_slice()),
			(Direction::Backward, DeliveredKeyOrder::None, &[][..])
		);
		// The same ordering over a key of declared kinds is delivered.
		let access = plan(&exact(), "WHERE city = 'a' ORDER BY city").unwrap();
		assert_eq!(access.delivers, DeliveredKeyOrder::Exact);
	}

	fn utc(year: i32, second: u32, nanos: u32) -> Datetime {
		let whole = Utc.with_ymd_and_hms(year, 1, 1, 0, 0, second).single().unwrap();
		Datetime(whole.with_nanosecond(nanos).unwrap())
	}

	#[test]
	fn a_view_keys_a_datetime_equality_by_itself_and_widens_a_range_to_its_second() {
		let key = view("ts, count() AS n", &["ts"]);
		let sub = "d'2025-01-01T10:00:00.5Z'";
		let access = plan(&key, &format!("WHERE ts = {sub}")).unwrap();
		assert_eq!(access.eq.as_ref(), &[one(lit(sub))]);
		let access = plan(&key, &format!("WHERE ts IN [{sub}, d'2025-01-01T10:00:01Z']")).unwrap();
		assert!(matches!(&access.eq[0], KeyEq::In(values) if values.len() == 2));

		// The lower side is the last instant before the second the literal lies
		// in, the upper side the whole second, and both are inclusive, whatever
		// the operator: the predicate decides the keys between. A lower side
		// stands beside a datetime upper side alone (see the next test).
		let before = lit("d'2025-01-01T09:59:59.999999999Z'");
		let whole = lit("d'2025-01-01T10:00:00Z'");
		let noon = lit("d'2025-01-01T12:00:00Z'");
		for literal in [sub, "d'2025-01-01T10:00:00Z'", "d'2025-01-01T10:00:00.999999999Z'"] {
			for op in [">", ">="] {
				let written =
					format!("WHERE ts {op} {literal} AND ts <= d'2025-01-01T12:00:00.5Z'");
				let range = plan(&key, &written).unwrap().range.unwrap();
				assert_eq!(
					(range.position, range.low, range.high),
					(0, side(before.clone(), true), side(noon.clone(), true)),
					"{op} {literal}"
				);
			}
			for op in ["<", "<="] {
				let range = plan(&key, &format!("WHERE ts {op} {literal}")).unwrap().range.unwrap();
				assert_eq!(
					(range.position, range.low, range.high),
					(0, None, side(whole.clone(), true)),
					"{op} {literal}"
				);
			}
		}
		// Mirrored, and two-sided.
		let range = plan(&key, &format!("WHERE {sub} <= ts AND ts < d'2025-01-01T11:00:00.25Z'"))
			.unwrap()
			.range
			.unwrap();
		assert_eq!(range.low, side(before, true));
		assert_eq!(range.high, side(lit("d'2025-01-01T11:00:00Z'"), true));
	}

	/// A datetime of a year past 9999 is keyed by text that opens with `+`,
	/// which sorts before every year within 0000 to 9999, so a range opening at
	/// a datetime lower bound would pass over it although it compares above the
	/// bound. A view's group may hold one, so a datetime lower bound is taken
	/// only beside a datetime upper bound, which excludes it.
	#[test]
	fn a_view_takes_a_datetime_lower_bound_only_beside_a_datetime_upper_bound() {
		let key_of = |d: &Datetime| try_encode_component(&Value::Datetime(*d)).expect("a datetime");
		let literal = utc(2025, 0, 0);
		let far = utc(10000, 0, 0);
		let (Value::Datetime(low), _) =
			view_datetime_bound(&literal, Use::Low(true), true).unwrap()
		else {
			panic!("a datetime bound is a datetime");
		};
		assert!(far > literal);
		assert!(key_of(&far) < key_of(&low), "the later year is keyed before the bound");

		let key = view("ts, count() AS n", &["ts"]);
		let d = "d'2025-01-01T00:00:00Z'";
		// Alone, or beside an upper bound of another kind: no lower bound.
		assert_eq!(plan(&key, &format!("WHERE ts > {d}")), None);
		assert_eq!(plan(&key, &format!("WHERE ts >= {d}")), None);
		assert_eq!(plan(&key, &format!("WHERE {d} < ts")), None);
		let range = plan(&key, &format!("WHERE ts > {d} AND ts < {UUID}")).unwrap().range.unwrap();
		assert_eq!((range.low, range.high), (None, side(lit(UUID), false)));
		// A range on a later group keeps the run before it.
		let grouped = grouped();
		let access = plan(&grouped, &format!("WHERE a = 'x' AND b >= {d}")).unwrap();
		assert_eq!((access.eq.as_ref(), access.range), (&[one(lit("'x'"))][..], None));
		// An upper bound alone stands: the keys of every year with a sign sort
		// before it, inside the range.
		let range = plan(&key, &format!("WHERE ts < {d}")).unwrap().range.unwrap();
		assert_eq!((range.low, range.high), (None, side(lit(d), true)));
		assert!(key_of(&far) < key_of(&utc(2025, 0, 0)));
		// A lower bound of another kind stands beside a datetime upper bound.
		let range = plan(&key, &format!("WHERE ts > 'm' AND ts < {d}")).unwrap().range.unwrap();
		assert_eq!((range.low, range.high), (side(lit("'m'"), false), side(lit(d), true)));
		// Within an `id` field's datetime component every key is within the
		// years, and the lower bound stands alone.
		let range =
			plan(&reading(), &format!("WHERE city = 'L' AND time > {d}")).unwrap().range.unwrap();
		assert_eq!(range.low, side(lit(d), false));
	}

	#[test]
	fn a_datetime_without_a_key_text_is_not_pushed_to_a_view() {
		let inclusive = |using| view_datetime_bound(&utc(2025, 0, 0), using, true);
		assert!(inclusive(Use::Equality).is_some());
		for outside in [
			// Years whose text is not four digits.
			utc(10000, 0, 0),
			utc(-1, 0, 0),
			// A leap second renders as second 60.
			utc(2025, 59, 1_500_000_000),
		] {
			for using in [Use::Equality, Use::Low(true), Use::High(true)] {
				assert_eq!(view_datetime_bound(&outside, using, true), None, "{outside} {using:?}");
			}
		}
		// The first second of year 0 has no instant before it that does.
		let first = utc(0, 0, 500);
		assert_eq!(view_datetime_bound(&first, Use::Low(true), true), None);
		assert_eq!(
			view_datetime_bound(&first, Use::High(true), true),
			Some((Value::Datetime(utc(0, 0, 0)), true))
		);
		assert_eq!(
			view_datetime_bound(&first, Use::Equality, true),
			Some((Value::Datetime(first), true))
		);
	}

	/// The bytes of the key of a datetime do not order as the datetimes do: a
	/// fraction of a second sorts before the whole second it follows. A bound
	/// that reaches every key of its second has to be a datetime whose own key
	/// does.
	#[test]
	fn a_widened_datetime_bound_reaches_every_key_of_its_second() {
		let key_of = |d: &Datetime| try_encode_component(&Value::Datetime(*d)).expect("a datetime");
		let mut instants = Vec::new();
		for seconds in [-1i64, 0, 1] {
			for nanos in [0i64, 1, 1_000, 500_000_000, 500_100_000, 999_999_999] {
				let at = utc(2025, 30, 0).0
					+ TimeDelta::seconds(seconds)
					+ TimeDelta::nanoseconds(nanos);
				instants.push(Datetime(at));
			}
		}
		for literal in &instants {
			let (low, _) = view_datetime_bound(literal, Use::Low(false), false).unwrap();
			let (high, _) = view_datetime_bound(literal, Use::High(false), false).unwrap();
			let (Value::Datetime(low), Value::Datetime(high)) = (low, high) else {
				panic!("a datetime bound is a datetime");
			};
			for other in &instants {
				if other >= literal {
					assert!(key_of(other) >= key_of(&low), "{other} >= {literal}: not above {low}");
				}
				if other <= literal {
					assert!(
						key_of(other) <= key_of(&high),
						"{other} <= {literal}: not below {high}"
					);
				}
			}
		}
	}
}
