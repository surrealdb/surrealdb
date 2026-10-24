//! Selectivity probes: index ranges whose entries bound the share of a table's rows a predicate
//! passes.
//!
//! A B-tree index lists every record under the key its columns evaluate to, so the entries of the
//! range an index access reads for a comparison are at least the records that satisfy it. The
//! count of those entries over the count of the table's records is then an upper bound on the
//! comparison's selectivity, read from storage without visiting a record. An index over an array
//! lists a record under each element, which only raises the bound.
//!
//! [`probe_set`] derives, from the catalog alone, the ranges to ask about and the rule that
//! combines their counts ([`ProbeBound`]):
//!
//! - an `AND` passes no more rows than its most selective part, so its bound is the smallest of its
//!   parts' bounds, and a part with no bound is left out;
//! - an `OR` passes no more rows than all its arms together, so its bound is the sum of its arms'
//!   bounds, and an arm with no bound leaves the `OR` with none;
//! - a comparison an index access answers is bounded by that access's entries, by each such access
//!   when several indexes answer it.
//!
//! Nothing here reads storage. The counts are asked for when a scan opens and combined by
//! [`ProbeBound::upper_keys`]; where a count is missing the rule above applies to it as to a part
//! with no bound.

use crate::catalog::Index;
use crate::exec::index::access_path::{BTreeAccess, IndexRef};
use crate::exec::index::analysis::IndexAnalyzer;
use crate::expr::BinaryOperator;
use crate::expr::logical::LogicalExpr;

/// The most index ranges one scan asks about as it opens, so the question stays one bounded call.
pub(crate) const MAX_PROBES: usize = 8;

/// The index ranges whose entry counts bound a scan's selectivity, and how their counts combine.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ProbeSet<R> {
	/// Each index and the access whose range is counted.
	pub(crate) probes: Vec<(R, BTreeAccess)>,
	pub(crate) bound: ProbeBound,
}

impl<R> ProbeSet<R> {
	/// The same set with each index reference mapped through `f`.
	pub(crate) fn try_map<S, E>(
		&self,
		mut f: impl FnMut(&R) -> Result<S, E>,
	) -> Result<ProbeSet<S>, E> {
		let mut probes = Vec::with_capacity(self.probes.len());
		for (index, access) in &self.probes {
			probes.push((f(index)?, access.clone()));
		}
		Ok(ProbeSet {
			probes,
			bound: self.bound.clone(),
		})
	}
}

/// How the entry counts of a [`ProbeSet`]'s ranges bound the rows a predicate passes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProbeBound {
	/// The entries of probe `i`.
	Probe(u16),
	/// No more than the smallest of the parts that have a count: an `AND`, or a comparison more
	/// than one index answers.
	All(Vec<ProbeBound>),
	/// No more than the sum of the parts, when every part has a count: an `OR`.
	Any(Vec<ProbeBound>),
}

impl ProbeBound {
	/// The most rows the predicate passes, from `keys[i]`, the entries probe `i` counted (`None`
	/// where it counted none), or `None` where the counts bound nothing.
	pub(crate) fn upper_keys(&self, keys: &[Option<u64>]) -> Option<u64> {
		match self {
			ProbeBound::Probe(at) => keys.get(usize::from(*at)).copied().flatten(),
			ProbeBound::All(parts) => parts.iter().filter_map(|part| part.upper_keys(keys)).min(),
			ProbeBound::Any(arms) => {
				let mut sum = 0u64;
				for arm in arms {
					sum = sum.saturating_add(arm.upper_keys(keys)?);
				}
				Some(sum)
			}
		}
	}

	/// The share in a million of `table_keys` rows the predicate passes at most, from the counts
	/// [`Self::upper_keys`] reads; `None` where they bound nothing or the table counted no row.
	pub(crate) fn upper_ppm(&self, keys: &[Option<u64>], table_keys: u64) -> Option<u32> {
		if table_keys == 0 {
			return None;
		}
		let passed = u128::from(self.upper_keys(keys)?);
		let ppm = (passed * 1_000_000 / u128::from(table_keys)).min(1_000_000);
		Some(ppm as u32)
	}
}

/// The probes that bound the selectivity of `cond` over a table whose indexes `analyzer` reads, or
/// `None` where no part of `cond` is bounded.
///
/// `cond` is the condition as the access-path analysis reads it. A comparison for which `usable`
/// answers `false` (one that reads a field the caller may not read) is given no bound. No more
/// than [`MAX_PROBES`] ranges are kept, the first ones met. A range already kept is reused at the
/// limit; a new range that would pass it is left out, and the other parts still bound the
/// predicate.
pub(crate) fn probe_set(
	analyzer: &IndexAnalyzer<'_>,
	cond: &LogicalExpr,
	usable: &dyn Fn(&LogicalExpr) -> bool,
) -> Option<ProbeSet<IndexRef>> {
	let mut probes = Vec::new();
	let bound = bound_of(analyzer, cond, usable, &mut probes)?;
	Some(ProbeSet {
		probes,
		bound,
	})
}

/// The bound of `expr`, adding the probes it reads to `probes`.
fn bound_of(
	analyzer: &IndexAnalyzer<'_>,
	expr: &LogicalExpr,
	usable: &dyn Fn(&LogicalExpr) -> bool,
	probes: &mut Vec<(IndexRef, BTreeAccess)>,
) -> Option<ProbeBound> {
	if let LogicalExpr::Binary {
		op: BinaryOperator::Or,
		..
	} = expr
	{
		let mut arms = Vec::new();
		flatten(expr, BinaryOperator::Or, &mut arms);
		let held = probes.len();
		let mut bounds = Vec::with_capacity(arms.len());
		for arm in arms {
			match bound_of(analyzer, arm, usable, probes) {
				Some(bound) => bounds.push(bound),
				None => {
					probes.truncate(held);
					return None;
				}
			}
		}
		return Some(ProbeBound::Any(bounds));
	}
	let mut conjuncts = Vec::new();
	flatten(expr, BinaryOperator::And, &mut conjuncts);
	let mut parts = Vec::new();
	let mut plain: Option<LogicalExpr> = None;
	for conjunct in conjuncts {
		if matches!(
			conjunct,
			LogicalExpr::Binary {
				op: BinaryOperator::Or,
				..
			}
		) {
			parts.extend(bound_of(analyzer, conjunct, usable, probes));
		} else if usable(conjunct) {
			plain = Some(match plain.take() {
				None => conjunct.clone(),
				Some(left) => LogicalExpr::Binary {
					left: Box::new(left),
					op: BinaryOperator::And,
					right: Box::new(conjunct.clone()),
				},
			});
		}
	}
	// The conjunction of the comparisons is analyzed whole, so a compound index pinning several of
	// them is a candidate beside the single-column ones.
	if let Some(plain) = plain {
		for candidate in analyzer.analyze(Some(&plain), None) {
			if candidate.empty || !counts_records(&candidate.index_ref, &candidate.access) {
				continue;
			}
			let at = match probes.iter().position(|(index, access)| {
				*index == candidate.index_ref && *access == candidate.access
			}) {
				Some(at) => at,
				None if probes.len() < MAX_PROBES => {
					probes.push((candidate.index_ref, candidate.access));
					probes.len() - 1
				}
				None => continue,
			};
			parts.push(ProbeBound::Probe(at as u16));
		}
	}
	match parts.len() {
		0 => None,
		1 => parts.pop(),
		_ => Some(ProbeBound::All(parts)),
	}
}

/// Whether the entries `access` reads of `index` are at least the records it answers for, and
/// fewer than the whole index: a B-tree range that is not every entry.
fn counts_records(index: &IndexRef, access: &BTreeAccess) -> bool {
	if !matches!(index.definition().index, Index::Idx | Index::Uniq) {
		return false;
	}
	match access {
		BTreeAccess::Equality(_) => true,
		BTreeAccess::Range {
			range,
		} => {
			!matches!(range.start, std::ops::Bound::Unbounded)
				|| !matches!(range.end, std::ops::Bound::Unbounded)
		}
		BTreeAccess::Compound {
			prefix,
			range,
			..
		} => !prefix.is_empty() || range.is_some(),
		BTreeAccess::FullText {
			..
		}
		| BTreeAccess::Knn {
			..
		} => false,
	}
}

/// The terms of the chain of `op` that `expr` heads, in order.
fn flatten<'a>(expr: &'a LogicalExpr, op: BinaryOperator, out: &mut Vec<&'a LogicalExpr>) {
	if let LogicalExpr::Binary {
		left,
		op: at,
		right,
	} = expr
		&& *at == op
	{
		flatten(left, op.clone(), out);
		flatten(right, op, out);
	} else {
		out.push(expr);
	}
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;
	use std::sync::Arc;

	use surrealdb_strand::Strand;

	use super::*;
	use crate::catalog::{IndexDefinition, IndexId};
	use crate::expr::Idiom;
	use crate::expr::logical::{LogicalFilter, LogicalOp, lower};

	fn idx(id: u32, name: &str, cols: &[&str], kind: Index) -> IndexDefinition {
		IndexDefinition {
			index_id: IndexId(id),
			name: Strand::from(name),
			table_name: "t".into(),
			cols: cols.iter().map(|c| Idiom::from_str(c).expect("valid idiom")).collect(),
			index: kind,
			count_cond: None,
			comment: None,
			prepare_remove: false,
			format_version: crate::catalog::INDEX_FORMAT_VERSION,
			nulls_not_distinct: false,
		}
	}

	fn analyzer(defs: Vec<IndexDefinition>) -> IndexAnalyzer<'static> {
		static REGISTRY: std::sync::LazyLock<crate::exec::function::FunctionRegistry> =
			std::sync::LazyLock::new(crate::exec::function::FunctionRegistry::with_builtins);
		let flags: Arc<[crate::exec::index::access_path::ColumnFlags]> =
			defs.iter().map(|ix| ix.cols.iter().map(|_| true).collect()).collect();
		IndexAnalyzer::new(Arc::<[_]>::from(defs.into_boxed_slice()), None, &REGISTRY)
			.with_scalar_columns(flags)
	}

	fn cond(snippet: &str) -> LogicalExpr {
		let src = format!("SELECT * FROM t WHERE {snippet}");
		let ast = crate::syn::parse(&src).expect("parse");
		let plan = match ast.expressions.into_iter().next() {
			Some(surrealdb_expr::parser::TopLevelExpr::Expr(
				select @ crate::expr::Expr::Select(_),
			)) => lower(select).expect("SELECT lowers"),
			other => panic!("expected SELECT, got {other:?}"),
		};
		let mut stack = vec![&plan];
		while let Some(op) = stack.pop() {
			if let LogicalOp::Filter(LogicalFilter {
				predicate,
				..
			}) = op
			{
				return predicate.clone();
			}
			stack.extend(op.children());
		}
		panic!("no WHERE in {src}");
	}

	fn indexes() -> Vec<IndexDefinition> {
		vec![
			idx(1, "by_a", &["a"], Index::Idx),
			idx(2, "by_b", &["b"], Index::Uniq),
			idx(3, "by_cd", &["c", "d"], Index::Idx),
		]
	}

	fn set(snippet: &str) -> Option<ProbeSet<IndexRef>> {
		probe_set(&analyzer(indexes()), &cond(snippet), &|_| true)
	}

	fn names(set: &ProbeSet<IndexRef>) -> Vec<&str> {
		set.probes.iter().map(|(index, _)| index.definition().name.as_str()).collect()
	}

	#[test]
	fn one_indexed_comparison_is_one_probe() {
		let set = set("a = 1").expect("a probe");
		assert_eq!(names(&set), ["by_a"]);
		assert!(matches!(set.probes[0].1, BTreeAccess::Equality(_)));
		assert_eq!(set.bound, ProbeBound::Probe(0));
		let set = self::set("b > 10").expect("a probe");
		assert_eq!(names(&set), ["by_b"]);
		assert!(matches!(set.probes[0].1, BTreeAccess::Range { .. }));
	}

	#[test]
	fn an_and_is_bounded_by_its_smallest_indexed_part() {
		let set = set("a = 1 AND b > 10 AND e = 3").expect("probes");
		assert_eq!(names(&set), ["by_a", "by_b"], "the unindexed `e` has no probe");
		let ProbeBound::All(parts) = &set.bound else {
			panic!("an AND: {:?}", set.bound);
		};
		assert_eq!(parts.len(), 2);
	}

	#[test]
	fn a_compound_index_pins_its_columns_together() {
		let set = set("c = 1 AND d = 2").expect("a probe");
		assert_eq!(names(&set), ["by_cd"]);
		assert!(matches!(
			&set.probes[0].1,
			BTreeAccess::Compound { prefix, .. } if prefix.len() == 2
		));
	}

	#[test]
	fn an_or_is_bounded_by_the_sum_of_its_arms() {
		let set = set("a = 1 OR b = 2").expect("probes");
		assert_eq!(names(&set), ["by_a", "by_b"]);
		assert_eq!(set.bound, ProbeBound::Any(vec![ProbeBound::Probe(0), ProbeBound::Probe(1)]));
	}

	#[test]
	fn an_or_with_an_unindexed_arm_has_no_bound() {
		assert_eq!(set("a = 1 OR e = 2"), None);
		// Under an AND the OR is left out and its sibling bounds the conjunction.
		let set = set("(a = 1 OR e = 2) AND b = 3").expect("a probe");
		assert_eq!(names(&set), ["by_b"]);
		assert_eq!(set.bound, ProbeBound::Probe(0));
	}

	#[test]
	fn an_unindexed_predicate_has_no_probe() {
		assert_eq!(set("e = 1"), None);
		assert_eq!(set("a != 1"), None, "an inequality reads every entry");
	}

	#[test]
	fn a_comparison_the_caller_may_not_read_has_no_probe() {
		let hidden = |expr: &LogicalExpr| !format!("{expr:?}").contains("\"a\"");
		let set = probe_set(&analyzer(indexes()), &cond("a = 1 AND b = 2"), &hidden)
			.expect("the readable comparison is probed");
		assert_eq!(names(&set), ["by_b"]);
	}

	#[test]
	fn no_more_than_the_limit_of_probes() {
		let defs: Vec<_> =
			(0..12).map(|i| idx(i, &format!("by_f{i}"), &[&format!("f{i}")], Index::Idx)).collect();
		let snippet = (0..12).map(|i| format!("f{i} = {i}")).collect::<Vec<_>>().join(" AND ");
		let set = probe_set(&analyzer(defs), &cond(&snippet), &|_| true).expect("probes");
		assert_eq!(set.probes.len(), MAX_PROBES);
	}

	#[test]
	fn a_range_already_held_is_reused_once_the_limit_is_reached() {
		let defs: Vec<_> =
			(0..9).map(|i| idx(i, &format!("by_f{i}"), &[&format!("f{i}")], Index::Idx)).collect();
		let conj = (0..9).map(|i| format!("f{i} = {i}")).collect::<Vec<_>>().join(" AND ");
		// The second arm's range is the first one held, met after the limit is full.
		let set = probe_set(&analyzer(defs), &cond(&format!("({conj}) OR f0 = 0")), &|_| true)
			.expect("probes");
		assert_eq!(set.probes.len(), MAX_PROBES);
		let ProbeBound::Any(arms) = &set.bound else {
			panic!("an OR: {:?}", set.bound);
		};
		assert_eq!(arms.len(), 2);
		assert_eq!(arms[1], ProbeBound::Probe(0));
	}

	#[test]
	fn and_takes_the_smallest_count_it_has() {
		let and = ProbeBound::All(vec![ProbeBound::Probe(0), ProbeBound::Probe(1)]);
		assert_eq!(and.upper_keys(&[Some(50), Some(7)]), Some(7));
		assert_eq!(and.upper_keys(&[None, Some(7)]), Some(7), "a part with no count is left out");
		assert_eq!(and.upper_keys(&[None, None]), None);
	}

	#[test]
	fn or_sums_its_arms_and_needs_every_count() {
		let or = ProbeBound::Any(vec![ProbeBound::Probe(0), ProbeBound::Probe(1)]);
		assert_eq!(or.upper_keys(&[Some(50), Some(7)]), Some(57));
		assert_eq!(or.upper_keys(&[Some(50), None]), None, "an arm with no count bounds nothing");
		let nested = ProbeBound::All(vec![or, ProbeBound::Probe(2)]);
		assert_eq!(nested.upper_keys(&[Some(50), None, Some(9)]), Some(9));
	}

	#[test]
	fn the_share_is_a_ratio_of_key_counts_capped_at_a_million() {
		let one = ProbeBound::Probe(0);
		assert_eq!(one.upper_ppm(&[Some(10)], 1_000), Some(10_000));
		assert_eq!(
			one.upper_ppm(&[Some(5_000)], 1_000),
			Some(1_000_000),
			"array entries overcount"
		);
		assert_eq!(one.upper_ppm(&[Some(10)], 0), None, "a table that counts no row");
		assert_eq!(one.upper_ppm(&[None], 1_000), None);
		let or = ProbeBound::Any(vec![ProbeBound::Probe(0), ProbeBound::Probe(0)]);
		assert_eq!(or.upper_ppm(&[Some(u64::MAX)], 1), Some(1_000_000), "the sum saturates");
	}
}
