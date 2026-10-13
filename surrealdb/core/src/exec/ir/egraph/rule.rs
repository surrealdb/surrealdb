//! Rules at run time: the registration checks, the printed form of a declaration, the search and
//! apply of a compiled rule, and the set a planner runs.
//!
//! A [`RuleDecl`] is data. [`check`] admits it, [`CompiledRule`] searches an e-graph for its left
//! side and instantiates its right side, and [`RuleSet`] holds the admitted rules in rank order,
//! where a rule's rank is its position. Matching is deterministic: roots and the nodes of a class
//! are visited in an order that depends on the graph's content and never on e-graph ids.

use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, OnceLock};

use egg::{EGraph, Id};
use slotmap::Key as _;
use smallvec::SmallVec;
use surrealdb_strand::TableName;

use super::analysis::IrAnalysis;
use super::decl::{
	AttrPred, AttrSrc, BinderId, BuildCx, ClassFacts, ClassRef, ENodeSnapshot, EgraphView,
	EpochKey, HeadPat, MATCH_CAP_PER_RULE, Match, NodeIdentity, NodeShape, NodeSpec, NodeSpecShape,
	Pat, PayloadId, PhysTableFacts, Rhs, RuleDecl, RuleTier, TEMPLATE_TAGS, TagBitmap, Tmpl,
	tag_index,
};
use super::guards::{UnionWhy, guarded_union, holds};
use super::lang::{Disc, ENode, identity_of, region_kind_key, snapshot_of};
use super::rules_logical::REGISTERED;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::expr::ops::ExprTag;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::egraph::{EgraphRule, PhysFactReq, RuleDialect, SideCondition};
use crate::exec::ir::iface::ids::{FrameId, OpId, SnapshotId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::expr::logical::EffectSet;

// ---------------------------------------------------------------------------------------------
// Registration checks
// ---------------------------------------------------------------------------------------------

/// The registration check that rejected a rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RCheck {
	/// A logical rule that names a physical tag or a physical fact.
	R1,
	/// An implementation rule whose right side is not rooted at a physical tag or builds a
	/// logical node.
	R2,
	/// No side condition and no soundness statement.
	R3,
	/// A name the left side does not bind, or a left side that cannot be searched.
	R4,
	/// A dropped variable that may fail or draw.
	R5,
	/// A duplicated variable that may fail or draw.
	R6,
	/// A variable moved toward eager evaluation that may fail or draw, or a `ConsumedAsTruthy`
	/// condition the pattern does not prove.
	R7,
	/// Built attributes for a tag outside [`TEMPLATE_TAGS`].
	R8,
	/// A new region that names no matched binder.
	R9,
	/// A repeated rule name, or more rules than a pre-match mask holds.
	R10,
}

/// Why a rule was not admitted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RuleReject {
	pub(crate) rule: &'static str,
	pub(crate) check: RCheck,
	pub(crate) detail: String,
}

impl fmt::Display for RuleReject {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "rule `{}` fails {:?}: {}", self.rule, self.check, self.detail)
	}
}

impl std::error::Error for RuleReject {}

/// The most rules one [`RuleSet`] holds: one bit of a `u64` pre-match mask each.
const MAX_RULES: usize = 64;

/// Calls `f` on every pattern of `pat`, parents before kids, with the number of `LazyOperand`
/// regions enclosing it.
fn walk_pat(pat: &Pat, depth: u32, f: &mut dyn FnMut(&Pat, u32)) {
	f(pat, depth);
	match *pat {
		Pat::Var(_)
		| Pat::Arg {
			..
		} => {}
		Pat::Node {
			kids,
			..
		} => {
			for kid in kids {
				walk_pat(kid, depth, f);
			}
		}
		Pat::Region {
			kind,
			kids,
			..
		} => {
			let inner = depth + u32::from(kind == RegionKind::LazyOperand);
			for kid in kids {
				walk_pat(kid, inner, f);
			}
		}
	}
}

/// Calls `f` on every template of `tmpl`, parents before kids, with the number of `LazyOperand`
/// regions enclosing it.
fn walk_tmpl(tmpl: &Tmpl, depth: u32, f: &mut dyn FnMut(&Tmpl, u32)) {
	f(tmpl, depth);
	match *tmpl {
		Tmpl::Var(_) => {}
		Tmpl::Node {
			kids,
			..
		} => {
			for kid in kids {
				walk_tmpl(kid, depth, f);
			}
		}
		Tmpl::Region {
			kind,
			kids,
			..
		} => {
			let inner = depth + u32::from(kind == RegionKind::LazyOperand);
			for kid in kids {
				walk_tmpl(kid, inner, f);
			}
		}
	}
}

/// How often a variable occurs in a side, and the fewest `LazyOperand` regions around an
/// occurrence.
#[derive(Clone, Copy, Debug)]
struct Occurrence {
	name: &'static str,
	count: usize,
	shallowest: u32,
}

fn note_occurrence(out: &mut Vec<Occurrence>, name: &'static str, depth: u32) {
	match out.iter_mut().find(|held| held.name == name) {
		Some(held) => {
			held.count += 1;
			held.shallowest = held.shallowest.min(depth);
		}
		None => out.push(Occurrence {
			name,
			count: 1,
			shallowest: depth,
		}),
	}
}

fn lhs_occurrences(lhs: &Pat) -> Vec<Occurrence> {
	let mut out = Vec::new();
	walk_pat(lhs, 0, &mut |pat, depth| {
		if let Pat::Var(name) = *pat {
			note_occurrence(&mut out, name, depth);
		}
	});
	out
}

fn rhs_occurrences(rhs: &Tmpl) -> Vec<Occurrence> {
	let mut out = Vec::new();
	walk_tmpl(rhs, 0, &mut |tmpl, depth| {
		if let Tmpl::Var(name) = *tmpl {
			note_occurrence(&mut out, name, depth);
		}
	});
	out
}

fn occurrence_of(all: &[Occurrence], name: &str) -> Option<Occurrence> {
	all.iter().find(|held| held.name == name).copied()
}

/// The variables a side condition names.
fn condition_vars(cond: &SideCondition) -> SmallVec<[&'static str; 2]> {
	match *cond {
		SideCondition::Infallible(v)
		| SideCondition::FallibilityAtMost(v, _)
		| SideCondition::Pure(v)
		| SideCondition::Deterministic(v)
		| SideCondition::NotPermissionConjunct(v)
		| SideCondition::FootprintDisjointFromStatementWrites(v)
		| SideCondition::ConsumedAsTruthy(v)
		| SideCondition::TypeFact(v, _) => SmallVec::from_slice(&[v]),
		SideCondition::SameFrame(a, b) | SideCondition::NoWriteBetween(a, b) => {
			SmallVec::from_slice(&[a, b])
		}
		SideCondition::PhysicalFact(_) => SmallVec::new(),
	}
}

fn declares_infallible(decl: &RuleDecl, var: &str) -> bool {
	decl.when.iter().any(|cond| matches!(*cond, SideCondition::Infallible(v) if v == var))
}

fn declares_deterministic(decl: &RuleDecl, var: &str) -> bool {
	decl.when.iter().any(|cond| matches!(*cond, SideCondition::Deterministic(v) if v == var))
}

/// The names a pattern introduces for bound nodes and region binders, in pre-order.
fn lhs_binds(lhs: &Pat) -> Vec<&'static str> {
	let mut out = Vec::new();
	walk_pat(lhs, 0, &mut |pat, _| match *pat {
		Pat::Node {
			bind: Some(name),
			..
		}
		| Pat::Region {
			binder: Some(name),
			..
		} => out.push(name),
		Pat::Node {
			..
		}
		| Pat::Region {
			..
		}
		| Pat::Var(_)
		| Pat::Arg {
			..
		} => {}
	});
	out
}

/// The tag of the node a pattern binds as `name`.
fn bound_node_tag(lhs: &Pat, name: &str) -> Option<OpTag> {
	let mut found = None;
	walk_pat(lhs, 0, &mut |pat, _| {
		if let Pat::Node {
			bind: Some(bound),
			head,
			..
		} = *pat && bound == name
		{
			found = Some(head.tag);
		}
	});
	found
}

type Step = fn(&RuleDecl, &[&RuleDecl]) -> Result<(), String>;

/// Runs registration checks R1 to R10 over `decl` in order and answers the first that fails.
///
/// `earlier` holds the rules already admitted, which R10 compares names against. A check that
/// cannot see a right side written as Rust code (a [`Rhs::Named`] applier) reads the variables
/// the declaration lists in `drops` and `duplicates` instead.
pub(crate) fn check(decl: &RuleDecl, earlier: &[&RuleDecl]) -> Result<(), RuleReject> {
	let steps: [(RCheck, Step); 10] = [
		(RCheck::R1, r1),
		(RCheck::R2, r2),
		(RCheck::R3, r3),
		(RCheck::R4, r4),
		(RCheck::R5, r5),
		(RCheck::R6, r6),
		(RCheck::R7, r7),
		(RCheck::R8, r8),
		(RCheck::R9, r9),
		(RCheck::R10, r10),
	];
	for (which, step) in steps {
		step(decl, earlier).map_err(|detail| RuleReject {
			rule: decl.name,
			check: which,
			detail,
		})?;
	}
	Ok(())
}

/// R1: a logical rule never names a physical tag or a physical fact.
fn r1(decl: &RuleDecl, _: &[&RuleDecl]) -> Result<(), String> {
	if decl.dialect != RuleDialect::LogicalRewrite {
		return Ok(());
	}
	let mut found: Option<String> = None;
	walk_pat(&decl.lhs, 0, &mut |pat, _| {
		if let Pat::Node {
			head,
			..
		} = *pat && matches!(head.tag, OpTag::Phys(_))
		{
			found.get_or_insert_with(|| format!("the left side names {}", head.tag.mnemonic()));
		}
	});
	if let Rhs::Tmpl(tmpl) = decl.rhs {
		walk_tmpl(&tmpl, 0, &mut |node, _| {
			if let Tmpl::Node {
				tag,
				..
			} = *node && matches!(tag, OpTag::Phys(_))
			{
				found.get_or_insert_with(|| format!("the right side builds {}", tag.mnemonic()));
			}
		});
	}
	if decl.when.iter().any(|cond| matches!(cond, SideCondition::PhysicalFact(_))) {
		found.get_or_insert_with(|| "the rule carries a physical fact".to_string());
	}
	found.map_or(Ok(()), Err)
}

/// R2: an implementation rule's right side is rooted at a physical tag and builds no logical node.
///
/// A [`Rhs::Named`] applier is not checked here: the view it adds nodes through refuses a node of
/// the wrong dialect in every build.
fn r2(decl: &RuleDecl, _: &[&RuleDecl]) -> Result<(), String> {
	if decl.dialect != RuleDialect::Implementation {
		return Ok(());
	}
	let Rhs::Tmpl(tmpl) = decl.rhs else {
		return Ok(());
	};
	match tmpl {
		Tmpl::Node {
			tag: OpTag::Phys(_),
			..
		} => {}
		Tmpl::Node {
			tag,
			..
		} => {
			return Err(format!(
				"the right side is rooted at {}, not a physical tag",
				tag.mnemonic()
			));
		}
		Tmpl::Var(_)
		| Tmpl::Region {
			..
		} => {
			return Err("the right side is not rooted at a physical op".to_string());
		}
	}
	let mut found: Option<String> = None;
	walk_tmpl(&tmpl, 0, &mut |node, _| {
		if let Tmpl::Node {
			tag: tag @ OpTag::Logical(_),
			..
		} = *node
		{
			found.get_or_insert_with(|| format!("the right side builds {}", tag.mnemonic()));
		}
	});
	found.map_or(Ok(()), Err)
}

/// R3: a rule with no side condition states why it is sound.
fn r3(decl: &RuleDecl, _: &[&RuleDecl]) -> Result<(), String> {
	if decl.when.is_empty() && decl.sound.is_none() {
		return Err("no side condition and no soundness statement".to_string());
	}
	Ok(())
}

/// R4: every name the rule uses is bound by its left side, and the left side is an op node.
///
/// The names are variables, bound nodes (a `CopyOf` source, a `frame_of`), region binders and the
/// variables of the side conditions, `answers`, `drops` and `duplicates`. A pattern is searched
/// by the tag of its root, so the root is an op node. A `CopyOf` source has the tag of the node
/// built from it, and a bound-node or binder name is introduced once.
fn r4(decl: &RuleDecl, _: &[&RuleDecl]) -> Result<(), String> {
	if !matches!(decl.lhs, Pat::Node { .. }) {
		return Err("the left side is not rooted at an op node".to_string());
	}
	let vars = lhs_occurrences(&decl.lhs);
	let binds = lhs_binds(&decl.lhs);
	for (at, name) in binds.iter().enumerate() {
		if binds[..at].contains(name) {
			return Err(format!("`{name}` names two bound nodes or binders"));
		}
	}
	let is_var = |name: &str| vars.iter().any(|held| held.name == name);
	let is_bind = |name: &str| binds.contains(&name);
	// A binder is visible to the `Arg` patterns that follow its region in pre-order.
	let mut seen: Vec<&'static str> = Vec::new();
	let mut bad: Option<String> = None;
	walk_pat(&decl.lhs, 0, &mut |pat, _| match *pat {
		Pat::Node {
			bind: Some(name),
			..
		}
		| Pat::Region {
			binder: Some(name),
			..
		} => seen.push(name),
		Pat::Arg {
			binder,
			..
		} if !seen.contains(&binder) => {
			bad.get_or_insert_with(|| format!("`arg ?{binder}` names no region before it"));
		}
		Pat::Node {
			..
		}
		| Pat::Region {
			..
		}
		| Pat::Var(_)
		| Pat::Arg {
			..
		} => {}
	});
	if let Some(detail) = bad {
		return Err(detail);
	}
	for cond in decl.when {
		for name in condition_vars(cond) {
			if !is_var(name) {
				return Err(format!(
					"a side condition names `?{name}`, which the left side does not bind"
				));
			}
		}
	}
	for (list, label) in
		[(decl.answers, "answers"), (decl.drops, "drops"), (decl.duplicates, "duplicates")]
	{
		for name in list {
			if !is_var(name) {
				return Err(format!(
					"`{label}` names `?{name}`, which the left side does not bind"
				));
			}
		}
	}
	let Rhs::Tmpl(tmpl) = decl.rhs else {
		return Ok(());
	};
	let mut bad: Option<String> = None;
	walk_tmpl(&tmpl, 0, &mut |node, _| match *node {
		Tmpl::Var(name) if !is_var(name) => {
			bad.get_or_insert_with(|| {
				format!("the right side uses `?{name}`, which the left side does not bind")
			});
		}
		Tmpl::Node {
			tag,
			attrs,
			frame_of,
			..
		} => {
			if !is_bind(frame_of) && !is_var(frame_of) {
				bad.get_or_insert_with(|| {
					format!("`frame {frame_of}` names neither a bound node nor a variable")
				});
			}
			if let AttrSrc::CopyOf(source) = attrs {
				match bound_node_tag(&decl.lhs, source) {
					Some(held) if held == tag => {}
					Some(held) => {
						bad.get_or_insert_with(|| {
							format!(
								"`copy {source}` is a {} node, built as {}",
								held.mnemonic(),
								tag.mnemonic()
							)
						});
					}
					None => {
						bad.get_or_insert_with(|| format!("`copy {source}` names no bound node"));
					}
				}
			}
		}
		Tmpl::Var(_)
		| Tmpl::Region {
			..
		} => {}
	});
	bad.map_or(Ok(()), Err)
}

/// R5: a variable the right side drops may be dropped only where that is unobservable.
///
/// A dropped variable needs `Infallible` and `Deterministic`. A variable listed in `answers` is
/// dropped because the new form answers it, and needs `Infallible` alone.
fn r5(decl: &RuleDecl, _: &[&RuleDecl]) -> Result<(), String> {
	let lhs = lhs_occurrences(&decl.lhs);
	let rhs = match decl.rhs {
		Rhs::Tmpl(tmpl) => Some(rhs_occurrences(&tmpl)),
		Rhs::Named {
			..
		} => None,
	};
	for held in &lhs {
		let name = held.name;
		let answered = decl.answers.contains(&name);
		let dropped = match &rhs {
			Some(rhs) => occurrence_of(rhs, name).is_none(),
			None => decl.drops.contains(&name),
		};
		if answered {
			if !declares_infallible(decl, name) {
				return Err(format!("`?{name}` is answered and is not declared infallible"));
			}
		} else if dropped
			&& !(declares_infallible(decl, name) && declares_deterministic(decl, name))
		{
			return Err(format!(
				"`?{name}` is dropped and is not declared infallible and deterministic"
			));
		}
	}
	Ok(())
}

/// R6: a variable the right side duplicates may be duplicated only where that is unobservable.
fn r6(decl: &RuleDecl, _: &[&RuleDecl]) -> Result<(), String> {
	let lhs = lhs_occurrences(&decl.lhs);
	let rhs = match decl.rhs {
		Rhs::Tmpl(tmpl) => Some(rhs_occurrences(&tmpl)),
		Rhs::Named {
			..
		} => None,
	};
	for held in &lhs {
		let name = held.name;
		let duplicated = match &rhs {
			Some(rhs) => occurrence_of(rhs, name).is_some_and(|there| there.count > held.count),
			None => decl.duplicates.contains(&name),
		};
		if duplicated && !(declares_infallible(decl, name) && declares_deterministic(decl, name)) {
			return Err(format!(
				"`?{name}` is duplicated and is not declared infallible and deterministic"
			));
		}
	}
	Ok(())
}

/// R7: a variable may move out of a `LazyOperand` region only where that is unobservable, and
/// `ConsumedAsTruthy(v)` is declared only for a direct operand of an `expr.truthy` node.
fn r7(decl: &RuleDecl, _: &[&RuleDecl]) -> Result<(), String> {
	if let Rhs::Tmpl(tmpl) = decl.rhs {
		let lhs = lhs_occurrences(&decl.lhs);
		let rhs = rhs_occurrences(&tmpl);
		for held in &rhs {
			let Some(there) = occurrence_of(&lhs, held.name) else {
				continue;
			};
			if held.shallowest < there.shallowest
				&& !(declares_infallible(decl, held.name)
					&& declares_deterministic(decl, held.name))
			{
				return Err(format!(
					"`?{}` moves out of a lazy operand and is not declared infallible and deterministic",
					held.name
				));
			}
		}
	}
	for cond in decl.when {
		let SideCondition::ConsumedAsTruthy(var) = *cond else {
			continue;
		};
		let mut direct = false;
		walk_pat(&decl.lhs, 0, &mut |pat, _| {
			if let Pat::Node {
				head,
				kids,
				..
			} = *pat && head.tag == OpTag::Expr(ExprTag::Truthy)
			{
				direct |= kids.iter().any(|kid| matches!(*kid, Pat::Var(name) if name == var));
			}
		});
		if !direct {
			return Err(format!("`?{var}` is not a direct operand of an expr.truthy node"));
		}
	}
	Ok(())
}

/// R8: attributes are built only for the tags of [`TEMPLATE_TAGS`].
fn r8(decl: &RuleDecl, _: &[&RuleDecl]) -> Result<(), String> {
	let Rhs::Tmpl(tmpl) = decl.rhs else {
		return Ok(());
	};
	let mut found: Option<String> = None;
	walk_tmpl(&tmpl, 0, &mut |node, _| {
		if let Tmpl::Node {
			tag,
			attrs: AttrSrc::Build {
				..
			},
			..
		} = *node && !TEMPLATE_TAGS.contains(&tag)
		{
			found.get_or_insert_with(|| format!("attributes are built for {}", tag.mnemonic()));
		}
	});
	found.map_or(Ok(()), Err)
}

/// R9: a new region reuses the binder of a region the left side matched.
fn r9(decl: &RuleDecl, _: &[&RuleDecl]) -> Result<(), String> {
	let Rhs::Tmpl(tmpl) = decl.rhs else {
		return Ok(());
	};
	let mut binders: Vec<&'static str> = Vec::new();
	walk_pat(&decl.lhs, 0, &mut |pat, _| {
		if let Pat::Region {
			binder: Some(name),
			..
		} = *pat
		{
			binders.push(name);
		}
	});
	let mut found: Option<String> = None;
	walk_tmpl(&tmpl, 0, &mut |node, _| {
		if let Tmpl::Region {
			binder_of: Some(name),
			..
		} = *node && !binders.contains(&name)
		{
			found.get_or_insert_with(|| {
				format!("a new region reuses `?{name}`, which no matched region binds")
			});
		}
	});
	found.map_or(Ok(()), Err)
}

/// R10: rule names are unique.
fn r10(decl: &RuleDecl, earlier: &[&RuleDecl]) -> Result<(), String> {
	if earlier.iter().any(|held| held.name == decl.name) {
		return Err("another rule has this name".to_string());
	}
	Ok(())
}

/// The op tags `decl` mentions on either side, in tag order and without duplicates.
pub(crate) fn tags_in(decl: &RuleDecl) -> Vec<OpTag> {
	let mut tags: Vec<OpTag> = Vec::new();
	walk_pat(&decl.lhs, 0, &mut |pat, _| {
		if let Pat::Node {
			head,
			..
		} = *pat
		{
			tags.push(head.tag);
		}
	});
	if let Rhs::Tmpl(tmpl) = decl.rhs {
		walk_tmpl(&tmpl, 0, &mut |node, _| {
			if let Tmpl::Node {
				tag,
				..
			} = *node
			{
				tags.push(tag);
			}
		});
	}
	tags.sort_by_key(|tag| tag_index(*tag));
	tags.dedup();
	tags
}

// ---------------------------------------------------------------------------------------------
// Printed form
// ---------------------------------------------------------------------------------------------

fn write_pat(f: &mut fmt::Formatter<'_>, pat: &Pat) -> fmt::Result {
	match *pat {
		Pat::Var(name) => write!(f, "?{name}"),
		Pat::Node {
			bind,
			head,
			kids,
		} => {
			write!(f, "({}", head.tag.mnemonic())?;
			if let Some(name) = bind {
				write!(f, "@{name}")?;
			}
			if let Some(pred) = head.attr {
				write!(f, "{{{}}}", pred.name)?;
			}
			for kid in kids {
				f.write_str(" ")?;
				write_pat(f, kid)?;
			}
			f.write_str(")")
		}
		Pat::Region {
			kind,
			binder,
			kids,
		} => {
			write!(f, "(region {kind:?}")?;
			if let Some(name) = binder {
				write!(f, " ?{name}")?;
			}
			for kid in kids {
				f.write_str(" ")?;
				write_pat(f, kid)?;
			}
			f.write_str(")")
		}
		Pat::Arg {
			binder,
			index,
		} => write!(f, "(arg ?{binder} {index})"),
	}
}

fn write_tmpl(f: &mut fmt::Formatter<'_>, tmpl: &Tmpl) -> fmt::Result {
	match *tmpl {
		Tmpl::Var(name) => write!(f, "?{name}"),
		Tmpl::Node {
			tag,
			attrs,
			frame_of,
			kids,
		} => {
			match attrs {
				AttrSrc::CopyOf(source) => {
					write!(f, "({}[copy {source} frame {frame_of}]", tag.mnemonic())?
				}
				AttrSrc::Build {
					name,
					..
				} => write!(f, "({}[build {name} frame {frame_of}]", tag.mnemonic())?,
			}
			for kid in kids {
				f.write_str(" ")?;
				write_tmpl(f, kid)?;
			}
			f.write_str(")")
		}
		Tmpl::Region {
			kind,
			binder_of,
			kids,
		} => {
			write!(f, "(region {kind:?}")?;
			if let Some(name) = binder_of {
				write!(f, " ?{name}")?;
			}
			for kid in kids {
				f.write_str(" ")?;
				write_tmpl(f, kid)?;
			}
			f.write_str(")")
		}
	}
}

/// The lower-case name of a fieldless enum's variant, as the printed form writes it.
fn lower<T: fmt::Debug>(value: T) -> String {
	format!("{value:?}").to_lowercase()
}

fn write_condition(f: &mut fmt::Formatter<'_>, cond: &SideCondition) -> fmt::Result {
	match *cond {
		SideCondition::Infallible(v) => write!(f, "(infallible ?{v})"),
		SideCondition::FallibilityAtMost(v, class) => {
			write!(f, "(fallibility-at-most ?{v} {})", lower(class))
		}
		SideCondition::Pure(v) => write!(f, "(pure ?{v})"),
		SideCondition::Deterministic(v) => write!(f, "(deterministic ?{v})"),
		SideCondition::SameFrame(a, b) => write!(f, "(same-frame ?{a} ?{b})"),
		SideCondition::NotPermissionConjunct(v) => write!(f, "(not-permission-conjunct ?{v})"),
		SideCondition::FootprintDisjointFromStatementWrites(v) => {
			write!(f, "(footprint-disjoint-from-statement-writes ?{v})")
		}
		SideCondition::NoWriteBetween(a, b) => write!(f, "(no-write-between ?{a} ?{b})"),
		SideCondition::ConsumedAsTruthy(v) => write!(f, "(consumed-as-truthy ?{v})"),
		SideCondition::TypeFact(v, class) => write!(f, "(type-fact ?{v} {})", lower(class)),
		SideCondition::PhysicalFact(req) => match req {
			PhysFactReq::IndexOn(v) => write!(f, "(physical-fact (index-on ?{v}))"),
			PhysFactReq::UniqueKey(v) => write!(f, "(physical-fact (unique-key ?{v}))"),
			PhysFactReq::OrderedBy(v) => write!(f, "(physical-fact (ordered-by ?{v}))"),
		},
	}
}

fn write_vars(f: &mut fmt::Formatter<'_>, label: &str, names: &[&str]) -> fmt::Result {
	write!(f, "\n  :{label} (")?;
	for (at, name) in names.iter().enumerate() {
		if at > 0 {
			f.write_str(" ")?;
		}
		write!(f, "?{name}")?;
	}
	f.write_str(")")
}

/// The rule as one s-expression, with the left and right sides, the side conditions and the
/// soundness statement each on a line of their own.
///
/// `:answers` is written when the rule lists any. `:drops` and `:duplicates` are written for a
/// rule whose right side is a Rust applier, and for any other rule when they list a variable, so
/// an applier's unreviewed empty declaration stays visible.
impl fmt::Display for RuleDecl {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let dialect = match self.dialect {
			RuleDialect::LogicalRewrite => "logical",
			RuleDialect::Implementation => "implementation",
		};
		let tier = match self.tier {
			RuleTier::Normal => "normal",
			RuleTier::Explore => "explore",
		};
		write!(
			f,
			"(rule {} :dialect {dialect} :tier {tier} :needs-total-order {}",
			self.name, self.needs_total_order
		)?;
		f.write_str("\n  :lhs ")?;
		write_pat(f, &self.lhs)?;
		f.write_str("\n  :rhs ")?;
		let named = match self.rhs {
			Rhs::Tmpl(tmpl) => {
				write_tmpl(f, &tmpl)?;
				false
			}
			Rhs::Named {
				name,
				..
			} => {
				write!(f, "(apply {name})")?;
				true
			}
		};
		f.write_str("\n  :when (")?;
		for (at, cond) in self.when.iter().enumerate() {
			if at > 0 {
				f.write_str(" ")?;
			}
			write_condition(f, cond)?;
		}
		f.write_str(")")?;
		if !self.answers.is_empty() {
			write_vars(f, "answers", self.answers)?;
		}
		if named || !self.drops.is_empty() {
			write_vars(f, "drops", self.drops)?;
		}
		if named || !self.duplicates.is_empty() {
			write_vars(f, "duplicates", self.duplicates)?;
		}
		if let Some(sound) = self.sound {
			write!(f, "\n  :sound {sound:?}")?;
		}
		f.write_str(")")
	}
}

// ---------------------------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------------------------

/// A key that orders nodes and classes by content: the shape, the identity attributes, the frame,
/// the snapshot, the epoch and the ordinals of the kids.
///
/// It names no e-graph id, so two graphs holding the same terms in a different insertion order
/// order them alike. It is also how the tie rule of extraction orders equal alternatives.
pub(crate) type NodeKey = SmallVec<[u64; 12]>;

/// The content key of `node` in `eg`.
pub(crate) fn node_key(eg: &EGraph<ENode, IrAnalysis>, node: &ENode) -> NodeKey {
	let an = &eg.analysis;
	let mut key = NodeKey::new();
	match node.id.shape {
		NodeShape::Op {
			tag,
			payload,
		} => {
			key.push(0);
			key.push(tag_index(tag) as u64);
			let digest = an.payload(payload).map_or([0u8; 16], |entry| entry.kind.attr_digest().0);
			key.push(u64::from_be_bytes([
				digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6],
				digest[7],
			]));
			key.push(u64::from_be_bytes([
				digest[8], digest[9], digest[10], digest[11], digest[12], digest[13], digest[14],
				digest[15],
			]));
		}
		NodeShape::Region {
			kind_key,
			binder,
			results,
		} => {
			key.push(1);
			key.push(u64::from(kind_key));
			key.push(u64::from(results));
			key.push(binder_key(an, binder));
		}
		NodeShape::Arg {
			binder,
			index,
		} => {
			key.push(2);
			key.push(u64::from(index));
			key.push(binder_key(an, binder));
		}
		NodeShape::Opaque {
			leaf,
		} => {
			key.push(3);
			let held = an.leaves.get(leaf.0 as usize);
			key.push(held.and_then(|held| held.op).map_or(0, |op| op.data().as_ffi()));
			key.push(held.and_then(|held| held.region).map_or(0, |region| region.data().as_ffi()));
		}
	}
	key.push(u64::from(node.id.frame.index()));
	key.push(u64::from(node.id.snapshot.index()));
	match node.id.epoch {
		EpochKey::None => key.push(0),
		EpochKey::Stable(n) => key.push(1 + (u64::from(n) << 1)),
		EpochKey::Unique(n) => key.push(2 + (u64::from(n) << 1)),
	}
	for kid in &node.kids {
		key.push(u64::from(eg[*kid].data.ordinal));
	}
	if let NodeShape::Op {
		payload,
		..
	} = node.id.shape
	{
		key.push(u64::from(payload.0));
	}
	key
}

/// The origin of `binder`'s region as a number, or 0 for no binder.
fn binder_key(an: &IrAnalysis, binder: BinderId) -> u64 {
	an.binders.get(binder.0 as usize).map_or(0, |entry| entry.origin.data().as_ffi())
}

/// The nodes of the class of `class` in content order.
pub(crate) fn sorted_nodes(eg: &EGraph<ENode, IrAnalysis>, class: Id) -> Vec<ENode> {
	let mut nodes: Vec<(NodeKey, ENode)> = eg[eg.find(class)]
		.nodes
		.iter()
		.map(|node| {
			let mut node = node.clone();
			for kid in node.kids.iter_mut() {
				*kid = eg.find(*kid);
			}
			(node_key(eg, &node), node)
		})
		.collect();
	nodes.sort_by(|a, b| a.0.cmp(&b.0));
	nodes.into_iter().map(|(_, node)| node).collect()
}

/// Whether `a` comes before `b` among classes: by ordinal, then by the content of their nodes.
fn class_order(eg: &EGraph<ENode, IrAnalysis>, a: Id, b: Id) -> std::cmp::Ordering {
	eg[a].data.ordinal.cmp(&eg[b].data.ordinal).then_with(|| {
		let least = |class: Id| eg[class].nodes.iter().map(|node| node_key(eg, node)).min();
		least(a).cmp(&least(b))
	})
}

/// The snapshot of `node` with its kids canonical.
fn canonical_snapshot(eg: &EGraph<ENode, IrAnalysis>, node: &ENode) -> ENodeSnapshot {
	let mut node = node.clone();
	for kid in node.kids.iter_mut() {
		*kid = eg.find(*kid);
	}
	snapshot_of(&node)
}

/// The class of `c` canonicalised.
fn find(eg: &EGraph<ENode, IrAnalysis>, c: ClassRef) -> Id {
	eg.find(Id::from(c))
}

fn view_nodes(eg: &EGraph<ENode, IrAnalysis>, c: ClassRef) -> SmallVec<[ENodeSnapshot; 4]> {
	sorted_nodes(eg, Id::from(c)).iter().map(snapshot_of).collect()
}

fn view_is_seed(eg: &EGraph<ENode, IrAnalysis>, n: &ENodeSnapshot) -> bool {
	let mut node = identity_of(n);
	for kid in node.kids.iter_mut() {
		*kid = eg.find(*kid);
	}
	eg.analysis.seeds.contains_key(&node)
}

/// A read-only view of a graph, for a guard or an attribute builder.
///
/// Both take the view by shared reference, so [`EgraphView::add`] cannot be reached through it:
/// it adds nothing and answers the class of `spec.frame_of`.
struct ReadView<'a> {
	eg: &'a EGraph<ENode, IrAnalysis>,
}

impl EgraphView for ReadView<'_> {
	fn find(&self, c: ClassRef) -> ClassRef {
		ClassRef::from(find(self.eg, c))
	}

	fn facts(&self, c: ClassRef) -> &ClassFacts {
		&self.eg[find(self.eg, c)].data
	}

	fn nodes(&self, c: ClassRef) -> SmallVec<[ENodeSnapshot; 4]> {
		view_nodes(self.eg, c)
	}

	fn payload(&self, p: PayloadId) -> Option<&OpKind> {
		self.eg.analysis.payload(p).map(|entry| &*entry.kind)
	}

	fn table(&self, t: &TableName) -> Option<&PhysTableFacts> {
		self.eg.analysis.tables.get(t)
	}

	fn total_order_ok(&self, c: ClassRef) -> bool {
		self.facts(c).total_order_ok
	}

	fn is_seed(&self, n: &ENodeSnapshot) -> bool {
		view_is_seed(self.eg, n)
	}

	fn add(&mut self, spec: NodeSpec) -> ClassRef {
		debug_assert!(false, "a read-only view cannot add a node");
		spec.frame_of
	}
}

/// The view a named applier adds nodes through, over a graph the apply holds exclusively.
///
/// A node added through it is built in the frame and snapshot of the class `frame_of` names and
/// takes the ordinal the applying rule set. It refuses a node of the other dialect, in every
/// build: a logical rule adds no physical op, and an implementation rule adds no logical op. A
/// refusal adds nothing, is counted in [`IrAnalysis::union_refused`], and sets `refused`, which
/// makes the apply discard what the applier answers.
struct ViewAdapter<'a> {
	eg: &'a mut EGraph<ENode, IrAnalysis>,
	cx: EffectCx<'a>,
	dialect: RuleDialect,
	refused: bool,
}

impl EgraphView for ViewAdapter<'_> {
	fn find(&self, c: ClassRef) -> ClassRef {
		ClassRef::from(find(self.eg, c))
	}

	fn facts(&self, c: ClassRef) -> &ClassFacts {
		&self.eg[find(self.eg, c)].data
	}

	fn nodes(&self, c: ClassRef) -> SmallVec<[ENodeSnapshot; 4]> {
		view_nodes(self.eg, c)
	}

	fn payload(&self, p: PayloadId) -> Option<&OpKind> {
		self.eg.analysis.payload(p).map(|entry| &*entry.kind)
	}

	fn table(&self, t: &TableName) -> Option<&PhysTableFacts> {
		self.eg.analysis.tables.get(t)
	}

	fn total_order_ok(&self, c: ClassRef) -> bool {
		self.facts(c).total_order_ok
	}

	fn is_seed(&self, n: &ENodeSnapshot) -> bool {
		view_is_seed(self.eg, n)
	}

	/// Adds the node, or answers the class of `spec.frame_of` unchanged when that class has no
	/// known frame and snapshot to build the node in. An op whose payload reads stored data takes a
	/// unique epoch, so two such reads never share a class; every other node has no epoch.
	fn add(&mut self, spec: NodeSpec) -> ClassRef {
		let (frame, snapshot) = {
			let facts = self.facts(spec.frame_of);
			match (facts.frame, facts.snapshot) {
				(Some(frame), Some(snapshot)) => (frame, snapshot),
				_ => return spec.frame_of,
			}
		};
		let kids: SmallVec<[Id; 4]> = spec.kids.iter().map(|kid| find(self.eg, *kid)).collect();
		let mut epoch = EpochKey::None;
		let shape = match spec.shape {
			NodeSpecShape::Op(kind) => {
				let tag = kind.tag();
				let admitted = match self.dialect {
					RuleDialect::LogicalRewrite => !matches!(tag, OpTag::Phys(_)),
					RuleDialect::Implementation => !matches!(tag, OpTag::Logical(_)),
				};
				if !admitted {
					self.refused = true;
					self.eg.analysis.union_refused =
						self.eg.analysis.union_refused.saturating_add(1);
					return spec.frame_of;
				}
				let payload = self.eg.analysis.intern_payload(Arc::new(kind), &self.cx);
				let reads_data = self
					.eg
					.analysis
					.payload(payload)
					.is_some_and(|entry| entry.local.set.intersects(EffectSet::READS_DATA));
				if reads_data {
					epoch = self.eg.analysis.fresh_rule_epoch();
				}
				NodeShape::Op {
					tag,
					payload,
				}
			}
			NodeSpecShape::Region {
				kind,
				binder,
			} => NodeShape::Region {
				kind_key: region_kind_key(kind),
				binder,
				results: u8::try_from(kids.len()).unwrap_or(u8::MAX),
			},
			NodeSpecShape::Arg {
				binder,
				index,
			} => NodeShape::Arg {
				binder,
				index,
			},
		};
		let id = self.eg.add(ENode {
			id: NodeIdentity {
				shape,
				frame,
				snapshot,
				epoch,
			},
			kids,
		});
		ClassRef::from(id)
	}
}

// ---------------------------------------------------------------------------------------------
// Compiled rules
// ---------------------------------------------------------------------------------------------

/// A left-hand side with its names replaced by positions.
#[derive(Debug)]
enum CPat {
	Var(usize),
	Node {
		bind: Option<usize>,
		tag: OpTag,
		attr: Option<AttrPred>,
		kids: Box<[CPat]>,
	},
	Region {
		kind_key: u16,
		binder: Option<usize>,
		kids: Box<[CPat]>,
	},
	Arg {
		binder: usize,
		index: u8,
	},
}

/// The position of `name` in `names`, adding it when it is new.
fn slot(names: &mut Vec<&'static str>, name: &'static str) -> usize {
	match names.iter().position(|held| *held == name) {
		Some(at) => at,
		None => {
			names.push(name);
			names.len() - 1
		}
	}
}

fn compile_pat(pat: &Pat, vars: &mut Vec<&'static str>, binds: &mut Vec<&'static str>) -> CPat {
	match *pat {
		Pat::Var(name) => CPat::Var(slot(vars, name)),
		Pat::Node {
			bind,
			head: HeadPat {
				tag,
				attr,
			},
			kids,
		} => CPat::Node {
			bind: bind.map(|name| slot(binds, name)),
			tag,
			attr,
			kids: kids.iter().map(|kid| compile_pat(kid, vars, binds)).collect(),
		},
		Pat::Region {
			kind,
			binder,
			kids,
		} => CPat::Region {
			kind_key: region_kind_key(kind),
			binder: binder.map(|name| slot(binds, name)),
			kids: kids.iter().map(|kid| compile_pat(kid, vars, binds)).collect(),
		},
		Pat::Arg {
			binder,
			index,
		} => CPat::Arg {
			binder: slot(binds, binder),
			index,
		},
	}
}

/// What one apply of a rule did to the graph.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Applied {
	/// Two distinct classes were joined.
	pub(crate) unioned: bool,
	/// The graph holds a node it did not hold before.
	pub(crate) added: bool,
}

/// A rule ready to search and apply: its left side with variables indexed by position.
#[derive(Debug)]
pub(crate) struct CompiledRule {
	decl: &'static RuleDecl,
	root_tag: OpTag,
	pat: CPat,
	/// The variables of the left side, in first-occurrence order.
	vars: Vec<&'static str>,
	/// The bound nodes and region binders of the left side, in first-occurrence order.
	binds: Vec<&'static str>,
}

/// The variable and binder bindings of a partial match.
struct Bindings {
	vars: SmallVec<[Option<Id>; 6]>,
	binds: SmallVec<[Option<ENodeSnapshot>; 4]>,
}

/// The state of one search.
struct Search<'a> {
	eg: &'a EGraph<ENode, IrAnalysis>,
	rule: &'a CompiledRule,
	view: ReadView<'a>,
	root: Id,
	out: SmallVec<[Match; 8]>,
	capped: bool,
}

impl<'a> Search<'a> {
	/// Matches the obligations `todo`, the last first, extending `b`.
	fn go(&mut self, todo: &[(Id, &'a CPat)], b: &mut Bindings) {
		if self.capped {
			return;
		}
		let Some((&(class, pat), rest)) = todo.split_last() else {
			self.finish(b);
			return;
		};
		let class = self.eg.find(class);
		match pat {
			CPat::Var(at) => match b.vars[*at] {
				Some(held) => {
					if held == class {
						self.go(rest, b);
					}
				}
				None => {
					b.vars[*at] = Some(class);
					self.go(rest, b);
					b.vars[*at] = None;
				}
			},
			CPat::Node {
				bind,
				tag,
				attr,
				kids,
			} => {
				for node in sorted_nodes(self.eg, class) {
					let NodeShape::Op {
						tag: held,
						payload,
					} = node.id.shape
					else {
						continue;
					};
					if held != *tag || node.kids.len() != kids.len() {
						continue;
					}
					if let Some(pred) = attr {
						let admitted = self
							.eg
							.analysis
							.payload(payload)
							.is_some_and(|entry| (pred.test)(&entry.kind));
						if !admitted {
							continue;
						}
					}
					self.descend(rest, b, &node, *bind, kids);
					if self.capped {
						return;
					}
				}
			}
			CPat::Region {
				kind_key,
				binder,
				kids,
			} => {
				for node in sorted_nodes(self.eg, class) {
					let NodeShape::Region {
						kind_key: held,
						results,
						..
					} = node.id.shape
					else {
						continue;
					};
					if held != *kind_key
						|| node.kids.len() != kids.len()
						|| usize::from(results) != kids.len()
					{
						continue;
					}
					self.descend(rest, b, &node, *binder, kids);
					if self.capped {
						return;
					}
				}
			}
			CPat::Arg {
				binder,
				index,
			} => {
				let Some(bound) = &b.binds[*binder] else {
					return;
				};
				let NodeShape::Region {
					binder: wanted,
					..
				} = bound.identity.shape
				else {
					return;
				};
				let held = sorted_nodes(self.eg, class).into_iter().any(|node| {
					matches!(
						node.id.shape,
						NodeShape::Arg { binder: of, index: at } if of == wanted && at == *index
					)
				});
				if held {
					self.go(rest, b);
				}
			}
		}
	}

	/// Binds `node` under `bind` and matches its kids against `kids`, then the rest.
	fn descend(
		&mut self,
		rest: &[(Id, &'a CPat)],
		b: &mut Bindings,
		node: &ENode,
		bind: Option<usize>,
		kids: &'a [CPat],
	) {
		if let Some(at) = bind {
			b.binds[at] = Some(canonical_snapshot(self.eg, node));
		}
		let mut next: SmallVec<[(Id, &'a CPat); 8]> = rest.iter().copied().collect();
		for (kid, pat) in node.kids.iter().zip(kids.iter()).rev() {
			next.push((*kid, pat));
		}
		self.go(&next, b);
		if let Some(at) = bind {
			b.binds[at] = None;
		}
	}

	/// Records the complete match `b` when every side condition holds.
	fn finish(&mut self, b: &Bindings) {
		let rule = self.rule;
		let mut m = Match {
			root: ClassRef::from(self.root),
			vars: SmallVec::new(),
			nodes: SmallVec::new(),
		};
		for (name, held) in rule.vars.iter().zip(b.vars.iter()) {
			if let Some(class) = held {
				m.vars.push((*name, ClassRef::from(*class)));
			}
		}
		for (name, held) in rule.binds.iter().zip(b.binds.iter()) {
			if let Some(snapshot) = held {
				m.nodes.push((*name, snapshot.clone()));
			}
		}
		if rule.decl.when.iter().all(|cond| holds(cond, &m, &self.view)) {
			self.out.push(m);
			self.capped = self.out.len() >= MATCH_CAP_PER_RULE;
		}
	}
}

/// The node bound as `name`.
fn bound<'m>(m: &'m Match, name: &str) -> Option<&'m ENodeSnapshot> {
	m.nodes.iter().find(|(held, _)| *held == name).map(|(_, node)| node)
}

/// The frame and snapshot of the bound node or the variable `name`.
fn frame_and_snapshot(
	eg: &EGraph<ENode, IrAnalysis>,
	m: &Match,
	name: &str,
) -> Option<(FrameId, SnapshotId)> {
	if let Some(node) = bound(m, name) {
		return Some((node.identity.frame, node.identity.snapshot));
	}
	let (_, class) = m.vars.iter().find(|(held, _)| *held == name)?;
	let facts = &eg[find(eg, *class)].data;
	Some((facts.frame?, facts.snapshot?))
}

/// Instantiates `tmpl` in `eg` and answers its class, or `None` when a name the template uses is
/// not in the match or has no known frame, or when a builder answers attributes of a tag other
/// than the one the template node names. The last case adds no node and is counted in
/// [`IrAnalysis::union_refused`].
fn instantiate(
	eg: &mut EGraph<ENode, IrAnalysis>,
	tmpl: &Tmpl,
	m: &Match,
	cx: &EffectCx<'_>,
	root: Id,
) -> Option<Id> {
	match *tmpl {
		Tmpl::Var(name) => {
			let (_, class) = m.vars.iter().find(|(held, _)| *held == name)?;
			Some(find(eg, *class))
		}
		Tmpl::Node {
			tag,
			attrs,
			frame_of,
			kids,
		} => {
			let mut ids: SmallVec<[Id; 4]> = SmallVec::new();
			for kid in kids {
				ids.push(instantiate(eg, kid, m, cx, root)?);
			}
			let (frame, snapshot) = frame_and_snapshot(eg, m, frame_of)?;
			let (payload, epoch) = match attrs {
				AttrSrc::CopyOf(source) => {
					let node = bound(m, source)?;
					let NodeShape::Op {
						payload,
						..
					} = node.identity.shape
					else {
						return None;
					};
					(payload, node.identity.epoch)
				}
				AttrSrc::Build {
					build,
					..
				} => {
					let kind = build(&BuildCx {
						view: &ReadView {
							eg: &*eg,
						},
						m,
					});
					if kind.tag() != tag {
						eg.analysis.union_refused = eg.analysis.union_refused.saturating_add(1);
						return None;
					}
					(eg.analysis.intern_payload(Arc::new(kind), cx), EpochKey::None)
				}
			};
			Some(eg.add(ENode {
				id: NodeIdentity {
					shape: NodeShape::Op {
						tag,
						payload,
					},
					frame,
					snapshot,
					epoch,
				},
				kids: ids,
			}))
		}
		Tmpl::Region {
			kind,
			binder_of,
			kids,
		} => {
			let mut ids: SmallVec<[Id; 4]> = SmallVec::new();
			for kid in kids {
				ids.push(instantiate(eg, kid, m, cx, root)?);
			}
			let (binder, frame, snapshot) = match binder_of {
				Some(name) => {
					let node = bound(m, name)?;
					let NodeShape::Region {
						binder,
						..
					} = node.identity.shape
					else {
						return None;
					};
					(binder, node.identity.frame, node.identity.snapshot)
				}
				None => {
					let facts = &eg[root].data;
					(BinderId::NONE, facts.frame?, facts.snapshot?)
				}
			};
			Some(eg.add(ENode {
				id: NodeIdentity {
					shape: NodeShape::Region {
						kind_key: region_kind_key(kind),
						binder,
						results: u8::try_from(ids.len()).ok()?,
					},
					frame,
					snapshot,
					epoch: EpochKey::None,
				},
				kids: ids,
			}))
		}
	}
}

/// In debug builds, checks that a named applier kept every variable it did not declare dropped:
/// the class of each such variable is among the classes the returned classes' nodes reach.
fn check_kept(eg: &EGraph<ENode, IrAnalysis>, decl: &RuleDecl, m: &Match, returned: &[ClassRef]) {
	let mut seen: HashSet<Id> = HashSet::new();
	let mut stack: Vec<Id> = returned.iter().map(|class| find(eg, *class)).collect();
	while let Some(class) = stack.pop() {
		if !seen.insert(class) {
			continue;
		}
		for node in &eg[class].nodes {
			stack.extend(node.kids.iter().map(|kid| eg.find(*kid)));
		}
	}
	for (name, class) in &m.vars {
		if decl.drops.contains(name) || decl.answers.contains(name) {
			continue;
		}
		debug_assert!(
			seen.contains(&find(eg, *class)),
			"rule `{}` dropped `?{name}` without declaring it",
			decl.name
		);
	}
}

impl CompiledRule {
	fn new(decl: &'static RuleDecl) -> Self {
		let mut vars = Vec::new();
		let mut binds = Vec::new();
		let pat = compile_pat(&decl.lhs, &mut vars, &mut binds);
		let root_tag = match &pat {
			CPat::Node {
				tag,
				..
			} => *tag,
			// Registration refuses a left side that is not an op node.
			CPat::Var(_)
			| CPat::Region {
				..
			}
			| CPat::Arg {
				..
			} => OpTag::Expr(ExprTag::Literal),
		};
		Self {
			decl,
			root_tag,
			pat,
			vars,
			binds,
		}
	}

	/// The declaration this rule compiles.
	pub(crate) fn decl(&self) -> &'static RuleDecl {
		self.decl
	}

	pub(crate) fn name(&self) -> &'static str {
		self.decl.name
	}

	pub(crate) fn dialect(&self) -> RuleDialect {
		self.decl.dialect
	}

	pub(crate) fn tier(&self) -> RuleTier {
		self.decl.tier
	}

	/// The tag of the op the left side is rooted at.
	pub(crate) fn root_tag(&self) -> OpTag {
		self.root_tag
	}

	/// The matches of the left side in `eg` that pass every side condition, and whether the search
	/// stopped at [`MATCH_CAP_PER_RULE`].
	///
	/// Root classes are visited by ordinal, then by the content of their nodes, and the nodes of a
	/// class by content, so the matches and the cut the cap makes depend on the graph's terms and
	/// never on its ids. A rule that admits `MATCH_CAP_PER_RULE` matches reports the cap hit even
	/// where no further match exists, because the search stops there. Every match carries its
	/// bound nodes by value, so it stays correct after an earlier match's union.
	///
	/// The graph is clean: `EGraph::rebuild` fills the per-tag class index the search starts from.
	pub(crate) fn search(&self, eg: &EGraph<ENode, IrAnalysis>) -> (SmallVec<[Match; 8]>, bool) {
		debug_assert!(eg.clean, "a rule is searched over a graph that was not rebuilt");
		let mut roots: Vec<Id> = match eg.classes_for_op(&Disc::Op(self.root_tag)) {
			Some(classes) => classes.map(|class| eg.find(class)).collect(),
			None => return (SmallVec::new(), false),
		};
		roots.sort_unstable();
		roots.dedup();
		roots.sort_by(|a, b| class_order(eg, *a, *b));
		let mut search = Search {
			eg,
			rule: self,
			view: ReadView {
				eg,
			},
			root: Id::from(0usize),
			out: SmallVec::new(),
			capped: false,
		};
		for root in roots {
			search.root = root;
			let mut bindings = Bindings {
				vars: SmallVec::from_elem(None, self.vars.len()),
				binds: SmallVec::from_elem(None, self.binds.len()),
			};
			search.go(&[(root, &self.pat)], &mut bindings);
			if search.capped {
				break;
			}
		}
		(search.out, search.capped)
	}

	/// Instantiates the right side for `m` and joins it to the match's root through
	/// [`guarded_union`].
	///
	/// Nodes the apply adds take the ordinal of the root class. A template node takes its payload
	/// and epoch from the node it copies, or interns the attributes its builder answers; a named
	/// applier adds nodes through a view. An apply that cannot instantiate (a name missing from
	/// the match, a class with no known frame) changes nothing but what it had already added. A
	/// named applier that adds a node of the other dialect has its answer discarded, so none of
	/// its classes is joined to the root.
	pub(crate) fn apply(
		&self,
		eg: &mut EGraph<ENode, IrAnalysis>,
		m: &Match,
		cx: &EffectCx<'_>,
	) -> Applied {
		let size = eg.total_size();
		let root = find(eg, m.root);
		eg.analysis.creating_for = Some(eg[root].data.ordinal);
		let mut unioned = false;
		match self.decl.rhs {
			Rhs::Tmpl(tmpl) => {
				if let Some(class) = instantiate(eg, &tmpl, m, cx, root) {
					unioned = guarded_union(
						eg,
						root,
						class,
						UnionWhy::Rule {
							root,
						},
					);
				}
			}
			Rhs::Named {
				apply,
				..
			} => {
				let (returned, refused) = {
					let mut view = ViewAdapter {
						eg: &mut *eg,
						cx: *cx,
						dialect: self.decl.dialect,
						refused: false,
					};
					let returned = apply(&mut view, m);
					(returned, view.refused)
				};
				let returned = if refused {
					SmallVec::new()
				} else {
					returned
				};
				if cfg!(debug_assertions) && !refused {
					check_kept(eg, self.decl, m, &returned);
				}
				for class in returned {
					unioned |= guarded_union(
						eg,
						root,
						Id::from(class),
						UnionWhy::Rule {
							root,
						},
					);
				}
			}
		}
		eg.analysis.creating_for = None;
		Applied {
			unioned,
			added: eg.total_size() > size,
		}
	}

	/// Whether `op` of `ir` can be the root of a match: the pattern's root head, its arity, and the
	/// heads of the direct kids that are op patterns, which an operand's defining op must carry,
	/// and the kinds of the direct kids that are region patterns.
	///
	/// Kids of an op are its operands and then its regions. A kid pattern that is a variable
	/// matches anything. Allocation-free, and never false for an op that matches.
	fn pre_matches(&self, ir: &Ir, op: OpId) -> bool {
		let Some(data) = ir.op(op) else {
			return false;
		};
		let CPat::Node {
			tag,
			attr,
			kids,
			..
		} = &self.pat
		else {
			return false;
		};
		if !head_admits(*tag, *attr, &data.kind)
			|| kids.len() != data.operands.len() + data.regions.len()
		{
			return false;
		}
		let operands = data.operands.len();
		kids.iter().enumerate().all(|(at, kid)| match kid {
			CPat::Var(_) => true,
			CPat::Node {
				tag,
				attr,
				..
			} => {
				at < operands
					&& ir
						.value(data.operands[at])
						.and_then(|value| match value.def {
							ValueDef::OpResult {
								op,
								..
							} => ir.op(op),
							ValueDef::BlockArg {
								..
							} => None,
						})
						.is_some_and(|def| head_admits(*tag, *attr, &def.kind))
			}
			CPat::Region {
				kind_key,
				..
			} => {
				at >= operands
					&& ir
						.region(data.regions[at - operands])
						.is_some_and(|region| region_kind_key(region.kind) == *kind_key)
			}
			CPat::Arg {
				..
			} => at < operands,
		})
	}
}

/// Whether `kind` has tag `tag` and passes the attribute test, if there is one.
fn head_admits(tag: OpTag, attr: Option<AttrPred>, kind: &OpKind) -> bool {
	kind.tag() == tag && attr.is_none_or(|pred| (pred.test)(kind))
}

// ---------------------------------------------------------------------------------------------
// Rule sets
// ---------------------------------------------------------------------------------------------

/// The admitted rules in rank order, compiled, with the tags their left sides are rooted at.
///
/// A rule's rank is its position, and a set holds at most 64 rules so that a pre-match answer is
/// one `u64`.
#[derive(Debug, Default)]
pub(crate) struct RuleSet {
	decls: Vec<&'static RuleDecl>,
	compiled: Vec<CompiledRule>,
	root_tags: TagBitmap,
}

impl RuleSet {
	/// Admits `decls` in order through [`check`] and compiles them.
	///
	/// More than 64 rules are rejected as [`RCheck::R10`], before any rule is checked. Either
	/// dialect is accepted.
	pub(crate) fn new(decls: &[&'static RuleDecl]) -> Result<Self, RuleReject> {
		Self::admit(decls, None)
	}

	/// Admits the `logical` rules, then the `implementation` rules, in rank order.
	///
	/// A `logical` rule of any other dialect is rejected as [`RCheck::R1`], and an
	/// `implementation` rule of any other dialect as [`RCheck::R2`]; the detail names the set.
	/// Every rule is checked against all earlier rules of both sets, so a name repeated across the
	/// sets is [`RCheck::R10`]. The 64-rule cap counts both sets.
	pub(crate) fn from_sets(
		logical: &[&'static RuleDecl],
		implementation: &[&'static RuleDecl],
	) -> Result<Self, RuleReject> {
		let decls: Vec<&'static RuleDecl> = logical.iter().chain(implementation).copied().collect();
		Self::admit(&decls, Some(logical.len()))
	}

	/// Admits `decls` in order. With `logical_len`, the first `logical_len` rules must be logical
	/// rewrites and the rest implementations.
	fn admit(decls: &[&'static RuleDecl], logical_len: Option<usize>) -> Result<Self, RuleReject> {
		if decls.len() > MAX_RULES {
			return Err(RuleReject {
				rule: decls[MAX_RULES].name,
				check: RCheck::R10,
				detail: format!(
					"{} rules exceed the {MAX_RULES} a pre-match mask holds",
					decls.len()
				),
			});
		}
		let mut set = Self::default();
		for (index, &decl) in decls.iter().enumerate() {
			if let Some(split) = logical_len {
				let (want, reject_as, set_name) = if index < split {
					(RuleDialect::LogicalRewrite, RCheck::R1, "logical")
				} else {
					(RuleDialect::Implementation, RCheck::R2, "implementation")
				};
				if decl.dialect != want {
					return Err(RuleReject {
						rule: decl.name,
						check: reject_as,
						detail: format!("the {set_name} set holds a {:?} rule", decl.dialect),
					});
				}
			}
			check(decl, &set.decls)?;
			let rule = CompiledRule::new(decl);
			set.root_tags.insert(rule.root_tag());
			set.decls.push(decl);
			set.compiled.push(rule);
		}
		Ok(set)
	}

	pub(crate) fn len(&self) -> usize {
		self.compiled.len()
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.compiled.is_empty()
	}

	/// The tags the rules' left sides are rooted at.
	pub(crate) fn root_tags(&self) -> &TagBitmap {
		&self.root_tags
	}

	/// The rule of rank `rank`.
	pub(crate) fn rule(&self, rank: usize) -> Option<&CompiledRule> {
		self.compiled.get(rank)
	}

	/// Every rule with its rank.
	pub(crate) fn iter(&self) -> impl Iterator<Item = (usize, &CompiledRule)> + '_ {
		self.compiled.iter().enumerate()
	}

	/// The rules of `dialect` with their ranks.
	pub(crate) fn of_dialect(
		&self,
		dialect: RuleDialect,
	) -> impl Iterator<Item = (usize, &CompiledRule)> + '_ {
		self.iter().filter(move |(_, rule)| rule.dialect() == dialect)
	}

	/// The bit mask of the rules `op` can be the root of a match for: bit `i` is set when rule `i`
	/// passes [`CompiledRule`]'s shape test over `ir` at `op`. Allocation-free.
	pub(crate) fn pre_match(&self, ir: &Ir, op: OpId) -> u64 {
		let Some(data) = ir.op(op) else {
			return 0;
		};
		if !self.root_tags.contains(data.kind.tag()) {
			return 0;
		}
		let mut mask = 0u64;
		for (rank, rule) in self.compiled.iter().enumerate() {
			if rule.pre_matches(ir, op) {
				mask |= 1 << rank;
			}
		}
		mask
	}
}

/// The rules production plans with, compiled once.
///
/// The logical set is [`REGISTERED`]. The implementation set is empty when no physical rule is
/// declared as a [`RuleDecl`]. Seeded mode then has no rule to run, and production planning is
/// logical-only. The set holds rules compiled from `static` code and is built once per process.
///
/// Panics when a rule fails registration: a bad rule is a defect found at the first use, never a
/// runtime condition.
pub(crate) fn registered() -> &'static RuleSet {
	static SET: OnceLock<RuleSet> = OnceLock::new();
	SET.get_or_init(|| match RuleSet::from_sets(REGISTERED, &[]) {
		Ok(set) => set,
		Err(reject) => panic!("a registered e-graph rule is invalid: {reject}"),
	})
}

/// A declaration offered to the registry as an [`EgraphRule`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct DeclRule {
	decl: &'static RuleDecl,
	tags: &'static [OpTag],
}

impl DeclRule {
	/// The rule declared by `decl`, which mentions the op tags `tags`.
	pub(crate) fn new(decl: &'static RuleDecl, tags: &'static [OpTag]) -> Self {
		Self {
			decl,
			tags,
		}
	}
}

impl EgraphRule for DeclRule {
	fn name(&self) -> &'static str {
		self.decl.name
	}

	fn dialect(&self) -> RuleDialect {
		self.decl.dialect
	}

	fn tags(&self) -> &'static [OpTag] {
		self.tags
	}

	fn side_conditions(&self) -> &[SideCondition] {
		self.decl.when
	}

	fn soundness(&self) -> Option<&'static str> {
		self.decl.sound
	}
}
