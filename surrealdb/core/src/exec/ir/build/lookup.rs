//! The forms a graph or reference lookup binds as: one hop of a path, or a statement over the
//! records it reaches.
//!
//! A lookup is a hop ([`is_hop`]) when it names only its subjects, `ONLY`, an optional `WHERE`
//! and an optional `*` projection. A referencing field (`FIELD`) on a subject belongs to a
//! reference lookup. A graph lookup does not read it: it neither keeps the lookup from being a hop
//! nor is carried into the hop the binder builds. The last hop of a path may also page its records
//! with `LIMIT` and `START` written as non-negative integers, when it has no `WHERE`, projection or
//! `ONLY`. Consecutive hops each become one `GraphStep` loop, which lowering reads as one lookup.
//!
//! Any other lookup with clauses of its own is a statement over the records it reaches without
//! them, shown whole ([`statement`]):
//!
//! `SELECT fields FROM source->(SELECT * FROM subjects) WHERE … SPLIT … GROUP … ORDER … LIMIT …
//! START …`
//!
//! The records are taken back to their ids where the lookup neither projects nor groups them. The
//! statement is wrapped in `ONLY` where the lookup, or a lookup of the path below it, has `ONLY`.
//! A paged last hop is also a statement where a lookup of its path has `ONLY`: the page then
//! counts the records one path reaches, and `ONLY` unwraps them in its own way. The source is the
//! value the path below the lookup answers, read with `ONLY` removed from each of its lookups
//! ([`without_only`]). The binder binds it before the statement, in the statement's enclosing
//! region. The statement reads it by the name [`SOURCE`], so the path is evaluated against the row
//! the lookup is taken over, and the statement's own `$parent` is that row.
//!
//! `ONLY` on a lookup of the path below the hops a lookup ends ([`run_source`]) unwraps what those
//! hops answer over all their origins together. The path below is read without it
//! ([`unwrapped_below_run`]), and the last hop states it as
//! [`LookupAttrs::only_path`](crate::exec::ir::logical::ops::LookupAttrs::only_path).

use crate::expr::Param;
use crate::expr::logical::{
	LogicalAggregate, LogicalExpr, LogicalFields, LogicalFilter, LogicalLimit, LogicalLiteral,
	LogicalLookup, LogicalOnly, LogicalOp, LogicalProject, LogicalSort, LogicalSplit,
	LogicalValues, LookupTrace,
};

/// The name the statement form of a lookup reads its source by. The binder declares it. No query
/// can write a parameter with this name, so no request parameter resolves to it.
pub(crate) const SOURCE: &str = "\u{0}lookup source";

/// Whether `lookup` is one hop of the path it stands in. `last` is true when no hop of that path
/// follows it.
pub(crate) fn is_hop(lookup: &LogicalLookup, last: bool) -> bool {
	let paged = is_paged(lookup);
	lookup.expr.as_ref().is_none_or(|fields| *fields == LogicalFields::all())
		&& lookup.split.is_none()
		&& lookup.group.is_none()
		&& lookup.order.is_none()
		&& (!paged
			|| (last
				&& lookup.expr.is_none()
				&& lookup.cond.is_none()
				&& !lookup.only
				&& has_literal_page(lookup)))
}

/// Whether the lookup `lookup` over `base`, with no hop after it, binds as the statement of its
/// clauses.
pub(crate) fn is_statement(base: &LogicalExpr, lookup: &LogicalLookup) -> bool {
	let not_a_hop = !is_hop(lookup, true) || (is_paged(lookup) && only_in(base));
	not_a_hop && has_clauses(lookup)
}

/// The statement of `lookup`'s clauses over the records it reaches from the value named
/// [`SOURCE`], under `ONLY` when `only` is true.
pub(crate) fn statement(lookup: &LogicalLookup, only: bool) -> LogicalOp {
	// The records reached with no clauses. The statement reads them as its rows.
	let reached = LogicalLookup {
		kind: lookup.kind.clone(),
		expr: Some(LogicalFields::all()),
		only: false,
		what: lookup.what.clone(),
		cond: None,
		split: None,
		group: None,
		order: None,
		limit: None,
		start: None,
		alias: None,
		// The statement reshapes what the lookup reaches, so the records reached are not what the
		// result holds: a traced lookup of this form feeds the result through a change of value.
		trace: match lookup.trace {
			LookupTrace::Traced | LookupTrace::Partial | LookupTrace::Inner => LookupTrace::Partial,
			LookupTrace::None => LookupTrace::None,
		},
	};
	let mut op = LogicalOp::Values(LogicalValues {
		expr: LogicalExpr::Lookup {
			base: Box::new(LogicalExpr::Param(Param::from(SOURCE.to_owned()))),
			lookup: Box::new(reached),
		},
		for_update: false,
		written: false,
	});
	if let Some(cond) = &lookup.cond {
		op = LogicalOp::Filter(LogicalFilter {
			input: Box::new(op),
			predicate: cond.clone(),
		});
	}
	if let Some(split) = &lookup.split {
		op = LogicalOp::Split(LogicalSplit {
			input: Box::new(op),
			exprs: split.clone(),
		});
	}
	// The lookup answers the records' ids unless it projects or groups them.
	let fields = match &lookup.expr {
		Some(fields) => fields.clone(),
		None if lookup.group.is_some() => LogicalFields::all(),
		None => LogicalFields::Value {
			expr: LogicalExpr::Field("id".into()),
			alias: None,
		},
	};
	if let Some(group) = &lookup.group {
		op = LogicalOp::Aggregate(LogicalAggregate {
			input: Box::new(op),
			group: group.clone(),
			fields: fields.clone(),
		});
	}
	if let Some(ordering) = &lookup.order {
		op = LogicalOp::Sort(LogicalSort {
			input: Box::new(op),
			ordering: ordering.clone(),
			tempfiles: false,
		});
	}
	if is_paged(lookup) {
		op = LogicalOp::Limit(LogicalLimit {
			input: Box::new(op),
			limit: lookup.limit.clone(),
			start: lookup.start.clone(),
		});
	}
	if lookup.group.is_none() {
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields,
			omit: Vec::new(),
		});
	}
	if only {
		op = LogicalOp::Only(LogicalOnly {
			input: Box::new(op),
		});
	}
	op
}

/// Whether a lookup of the path `expr` ends in has `ONLY`.
pub(crate) fn only_in(expr: &LogicalExpr) -> bool {
	let mut node = expr;
	while let LogicalExpr::Lookup {
		base,
		lookup,
	} = node
	{
		if lookup.only {
			return true;
		}
		node = base;
	}
	false
}

/// `expr` with `ONLY` taken off every lookup of the path it ends in.
pub(crate) fn without_only(expr: &LogicalExpr) -> LogicalExpr {
	if let LogicalExpr::Lookup {
		base,
		lookup,
	} = expr
	{
		let mut lookup = lookup.clone();
		lookup.only = false;
		LogicalExpr::Lookup {
			base: Box::new(without_only(base)),
			lookup,
		}
	} else {
		expr.clone()
	}
}

/// The value the hops a lookup over `base` ends step from: `base` past every lookup that is a hop
/// with another after it.
pub(crate) fn run_source(base: &LogicalExpr) -> &LogicalExpr {
	let mut node = base;
	while let LogicalExpr::Lookup {
		base,
		lookup,
	} = node
		&& is_hop(lookup, false)
	{
		node = base;
	}
	node
}

/// `base` with `ONLY` taken off every lookup of the path below its hops ([`run_source`]); the
/// hops keep theirs.
pub(crate) fn unwrapped_below_run(base: &LogicalExpr) -> LogicalExpr {
	if let LogicalExpr::Lookup {
		base: inner_base,
		lookup,
	} = base
		&& is_hop(lookup, false)
	{
		LogicalExpr::Lookup {
			base: Box::new(unwrapped_below_run(inner_base)),
			lookup: lookup.clone(),
		}
	} else {
		without_only(base)
	}
}

/// Whether `lookup` writes `LIMIT` or `START`.
fn is_paged(lookup: &LogicalLookup) -> bool {
	lookup.limit.is_some() || lookup.start.is_some()
}

/// Whether the `LIMIT` and `START` `lookup` writes are each a non-negative integer literal.
fn has_literal_page(lookup: &LogicalLookup) -> bool {
	let integer = |expr: &LogicalExpr| matches!(expr, LogicalExpr::Literal(LogicalLiteral::Integer(n)) if *n >= 0);
	lookup.limit.as_ref().is_none_or(integer) && lookup.start.as_ref().is_none_or(integer)
}

/// Whether `lookup` has a clause of its own beyond `WHERE`, `ONLY` and the `*` projection.
fn has_clauses(lookup: &LogicalLookup) -> bool {
	lookup.expr.as_ref().is_some_and(|fields| *fields != LogicalFields::all())
		|| lookup.split.is_some()
		|| lookup.group.is_some()
		|| lookup.order.is_some()
		|| is_paged(lookup)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::Dir;
	use crate::expr::logical::{LogicalLookupSubject, LogicalOrdering};
	use crate::expr::lookup::LookupKind;

	fn table(name: &str) -> LogicalLookupSubject {
		LogicalLookupSubject::Table {
			table: name.into(),
			referencing_field: None,
		}
	}

	/// `->name` with no clause.
	fn out(name: &str) -> LogicalLookup {
		LogicalLookup {
			kind: LookupKind::Graph(Dir::Out),
			expr: None,
			only: false,
			what: vec![table(name)],
			cond: None,
			split: None,
			group: None,
			order: None,
			limit: None,
			start: None,
			alias: None,
			trace: crate::expr::logical::LookupTrace::None,
		}
	}

	fn integer(n: i64) -> LogicalExpr {
		LogicalExpr::Literal(LogicalLiteral::Integer(n))
	}

	fn over(base: LogicalExpr, lookup: LogicalLookup) -> LogicalExpr {
		LogicalExpr::Lookup {
			base: Box::new(base),
			lookup: Box::new(lookup),
		}
	}

	fn source() -> LogicalExpr {
		LogicalExpr::Param(Param::from("a".to_owned()))
	}

	#[test]
	fn a_where_and_the_star_projection_keep_a_hop() {
		let mut lookup = out("knows");
		lookup.cond = Some(LogicalExpr::Field("x".into()));
		lookup.expr = Some(LogicalFields::all());
		lookup.only = true;
		assert!(is_hop(&lookup, false) && is_hop(&lookup, true));
		assert!(!is_statement(&source(), &lookup));
	}

	#[test]
	fn a_literal_page_is_a_hop_only_where_it_ends_the_path() {
		let mut lookup = out("knows");
		lookup.limit = Some(integer(2));
		lookup.start = Some(integer(1));
		assert!(is_hop(&lookup, true));
		assert!(!is_hop(&lookup, false));
		assert!(!is_statement(&source(), &lookup));
		for computed in [integer(-1), source()] {
			lookup.limit = Some(computed);
			assert!(!is_hop(&lookup, true));
			assert!(is_statement(&source(), &lookup));
		}
	}

	#[test]
	fn a_page_under_only_anywhere_on_its_path_is_a_statement() {
		let mut first = out("x");
		first.only = true;
		let mut last = out("knows");
		last.limit = Some(integer(1));
		assert!(is_statement(&over(source(), first), &last));
		let mut only = last;
		only.only = true;
		assert!(is_statement(&source(), &only));
	}

	#[test]
	fn each_clause_of_its_own_makes_a_statement() {
		let cases: [fn(&mut LogicalLookup); 4] = [
			|l| l.split = Some(vec![LogicalExpr::Field("x".into())]),
			|l| l.group = Some(Vec::new()),
			|l| l.order = Some(LogicalOrdering::Random),
			|l| {
				l.expr = Some(LogicalFields::Value {
					expr: LogicalExpr::Field("x".into()),
					alias: None,
				})
			},
		];
		for case in cases {
			let mut lookup = out("knows");
			case(&mut lookup);
			assert!(!is_hop(&lookup, true));
			assert!(is_statement(&source(), &lookup));
		}
	}

	#[test]
	fn a_referencing_field_keeps_a_hop_in_either_kind_of_lookup() {
		let mut lookup = out("post");
		lookup.what = vec![LogicalLookupSubject::Table {
			table: "post".into(),
			referencing_field: Some("author".to_owned()),
		}];
		assert!(is_hop(&lookup, true) && is_hop(&lookup, false));
		assert!(!is_statement(&source(), &lookup));
		lookup.kind = LookupKind::Reference;
		assert!(is_hop(&lookup, true) && is_hop(&lookup, false));
	}

	#[test]
	fn the_statement_answers_ids_through_its_clauses_in_main_order() {
		let mut lookup = out("knows");
		lookup.cond = Some(LogicalExpr::Field("w".into()));
		lookup.order = Some(LogicalOrdering::Random);
		lookup.limit = Some(source());
		lookup.alias = Some(crate::expr::logical::OutputPath::field("n"));
		let LogicalOp::Only(only) = statement(&lookup, true) else {
			panic!("under ONLY");
		};
		let LogicalOp::Project(project) = *only.input else {
			panic!("a projection");
		};
		assert_eq!(
			project.fields,
			LogicalFields::Value {
				expr: LogicalExpr::Field("id".into()),
				alias: None,
			}
		);
		let LogicalOp::Limit(limit) = *project.input else {
			panic!("a page");
		};
		assert_eq!(limit.limit, Some(source()));
		let LogicalOp::Sort(sort) = *limit.input else {
			panic!("an order");
		};
		let LogicalOp::Filter(filter) = *sort.input else {
			panic!("a filter");
		};
		let LogicalOp::Values(values) = *filter.input else {
			panic!("the records reached");
		};
		let LogicalExpr::Lookup {
			base,
			lookup: reached,
		} = values.expr
		else {
			panic!("a lookup");
		};
		assert_eq!(*base, LogicalExpr::Param(Param::from(SOURCE.to_owned())));
		assert_eq!(reached.expr, Some(LogicalFields::all()));
		assert!(reached.cond.is_none() && reached.order.is_none() && reached.limit.is_none());
		assert!(reached.alias.is_none() && !reached.only);
	}

	#[test]
	fn a_grouped_statement_projects_through_its_aggregate() {
		let mut lookup = out("knows");
		lookup.group = Some(vec![LogicalExpr::Field("age".into())]);
		let LogicalOp::Aggregate(aggregate) = statement(&lookup, false) else {
			panic!("the aggregate is the statement's last node");
		};
		assert_eq!(aggregate.fields, LogicalFields::all());
	}

	#[test]
	fn only_below_the_hops_is_taken_off_and_the_hops_keep_theirs() {
		let mut paged = out("x");
		paged.only = true;
		paged.limit = Some(integer(1));
		let mut hop = out("y");
		hop.only = true;
		let base = over(over(source(), paged), hop);
		assert!(matches!(run_source(&base), LogicalExpr::Lookup { lookup, .. } if lookup.only));
		let unwrapped = unwrapped_below_run(&base);
		let LogicalExpr::Lookup {
			base: below,
			lookup: kept,
		} = &unwrapped
		else {
			panic!("the hop");
		};
		assert!(kept.only);
		assert!(!only_in(below));
	}
}
