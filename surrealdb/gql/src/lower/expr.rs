//! GQL expression lowering with uniform binding addressing.
//!
//! Implements the three-valued-logic guard rules of `doc/gql/V2_DESIGN.md`
//! §8 (kept verbatim from the v1 contract): predicates are normalized to
//! negation normal form on the fly — a `negated` flag is pushed through
//! `NOT`/`AND`/`OR` (De Morgan, never distributing ORs) and into the comparison
//! and truth-test leaves, where each effective comparison is guarded so that a
//! `NULL`/`NONE` operand excludes the row, matching GQL's UNKNOWN-excluded
//! `WHERE` semantics under SurrealQL's two-valued total-order comparisons.
//!
//! The v1 scope tables (Role × ScopeKind, the `__a`/`__m`/`$parent`
//! rewrites) are gone: every binding is a row field addressed by its name, so
//! a variable `v` lowers to `Idiom[Field("v")]` and `v.x` to
//! `Idiom[Field("v"), Field("x")]`, in every position. Group and path bindings
//! hold composite values (an edge list / an alternating array) with no field
//! structure, so property access on them is rejected.
//!
//! All recursion over expressions runs on a [`reblessive`] stack: the parser
//! deliberately builds arbitrarily deep *linear* chains (binary operator
//! spines, property and `NOT` chains) without consuming its nesting budget,
//! so the lowering must not recurse on the machine stack either.
//!
//! Expressions are still built as [`crate::sql::Expr`] here and converted to
//! [`crate::expr::Expr`] per slot by the caller; the guard and NNF machinery is
//! unchanged from v1, only the variable-addressing leaf differs.

use reblessive::Stk;

use crate::expr::match_plan::{BindingId, BindingKind};
use crate::gql::ast::{BinaryOp, GqlExpr, GqlLiteral, Ident, SetQuantifier, TruthValue, UnaryOp};
use crate::gql::lower::binding::Registry;
use crate::gql::lower::naming;
use crate::sql::function::{Function, FunctionCall};
use crate::sql::literal::ObjectEntry;
use crate::sql::{BinaryOperator, Expr, Idiom, Literal, Param, Part, PrefixOperator};
use crate::syn::error::{SyntaxError, bail, syntax_error};
use crate::syn::token::Span;

/// The binding context an expression is lowered in: the clause registry, used
/// to resolve variable references to bindings and to reject property access on
/// composite (group/path) bindings.
///
/// A unit struct in PR-A — every expression lowers against the one clause's
/// registry — but kept as a named scope so the OPTIONAL-clause scoping of PR-C
/// has a seam to extend.
pub(super) struct Scope<'a> {
	pub(super) registry: &'a Registry,
	/// Whether aggregate function calls (`count`, `sum`, …) may appear in this
	/// position. `true` only for `RETURN` value position (and `ORDER BY` keys of
	/// an aggregating query); `false` everywhere else (`WHERE` predicates,
	/// `GROUP BY` keys, and an aggregate's own arguments — so nested aggregates
	/// are rejected).
	pub(super) allow_aggregates: bool,
}

impl<'a> Scope<'a> {
	/// A copy of this scope that forbids aggregates — used when lowering an
	/// aggregate's arguments so a nested aggregate is rejected.
	fn no_aggregates(&self) -> Scope<'a> {
		Scope {
			registry: self.registry,
			allow_aggregates: false,
		}
	}
}

impl Scope<'_> {
	/// Whether a bare variable reference is optional-bound — declared inside an
	/// `OPTIONAL` operand (`optional_depth > 0`) — and so can hold `Value::Null`
	/// on an optional miss (R3). The `nullable()` amendment (V2_DESIGN §8) uses
	/// this: a bare `Variable(v)` is nullable iff this is `true`. An unknown
	/// variable is not optional-bound here (the reference is reported elsewhere
	/// when it is actually lowered).
	fn variable_is_optional(&self, ident: &Ident) -> bool {
		self.registry.resolve(ident).map(|id| self.registry.optional_depth(id) > 0).unwrap_or(false)
	}

	/// Lowers a reference to a pattern variable, optionally suffixed with a
	/// property chain, to the binding-row idiom `binding(.prop)*`.
	fn binding_expr(&self, ident: &Ident, props: &[&Ident]) -> Result<Expr, SyntaxError> {
		let id = self.registry.resolve(ident)?;
		let kind = self.registry.kind(id);
		let prop_span = props.first().map(|p| p.span);
		self.binding_idiom(&ident.name, kind, props, prop_span)
	}

	/// Builds the binding-row idiom `binding(.prop)*` for a resolved binding,
	/// rejecting property access on a group or path binding: those bindings
	/// hold composite values (an edge list / an alternating node-edge array)
	/// with no addressable field structure yet.
	fn binding_idiom(
		&self,
		name: &str,
		kind: BindingKind,
		props: &[&Ident],
		prop_span: Option<Span>,
	) -> Result<Expr, SyntaxError> {
		if !props.is_empty() {
			match kind {
				BindingKind::EdgeGroup | BindingKind::Path => {
					bail!(
						"Property access on a group or path variable is not supported yet",
						@prop_span.unwrap_or_else(Span::empty) => "return the variable itself"
					);
				}
				BindingKind::Node | BindingKind::Edge => {}
			}
		}
		let mut parts = Vec::with_capacity(props.len() + 1);
		parts.push(Part::Field(name.to_owned().into()));
		parts.extend(props.iter().map(|p| Part::Field(p.name.clone().into())));
		Ok(Expr::Idiom(Idiom(parts)))
	}
}

/// Lowers a GQL expression in value position (projections, sort keys,
/// comparison operands): variable references resolve to binding-row idioms;
/// comparisons and logical operators lower two-valued — the §4 guards apply to
/// predicate position only.
pub(super) async fn lower_value(
	stk: &mut Stk,
	expr: &GqlExpr,
	scope: &Scope<'_>,
) -> Result<Expr, SyntaxError> {
	match expr {
		GqlExpr::Literal(lit, _) => Ok(lower_literal(lit)),
		GqlExpr::Param {
			name,
			span,
		} => lower_param(name, *span),
		GqlExpr::Variable(ident) => scope.binding_expr(ident, &[]),
		GqlExpr::Property(..) => lower_property(stk, expr, scope).await,
		GqlExpr::Unary {
			op,
			expr: operand,
			..
		} => {
			let operand = stk.run(|stk| lower_value(stk, operand, scope)).await?;
			let op = match op {
				UnaryOp::Not => PrefixOperator::Not,
				UnaryOp::Neg => PrefixOperator::Negate,
				UnaryOp::Plus => PrefixOperator::Positive,
			};
			Ok(Expr::Prefix {
				op,
				expr: Box::new(operand),
			})
		}
		GqlExpr::Binary {
			left,
			op,
			right,
			span,
		} => {
			let op = binary_op(*op, *span)?;
			let left = stk.run(|stk| lower_value(stk, left, scope)).await?;
			let right = stk.run(|stk| lower_value(stk, right, scope)).await?;
			Ok(Expr::Binary {
				left: Box::new(left),
				op,
				right: Box::new(right),
			})
		}
		GqlExpr::IsBool {
			..
		}
		| GqlExpr::IsNull {
			..
		} => lower_truth_test(stk, expr, false, scope).await,
		GqlExpr::FunctionCall {
			name,
			quantifier,
			star,
			args,
			span,
		} => lower_function(stk, name, *quantifier, *star, args, *span, scope).await,
		GqlExpr::List(items, _) => {
			let mut exprs = Vec::with_capacity(items.len());
			for item in items {
				exprs.push(stk.run(|stk| lower_value(stk, item, scope)).await?);
			}
			Ok(Expr::Literal(Literal::Array(exprs)))
		}
		GqlExpr::Map(fields, _) => {
			let mut entries = Vec::with_capacity(fields.len());
			for (key, value) in fields {
				let value = stk.run(|stk| lower_value(stk, value, scope)).await?;
				entries.push(ObjectEntry {
					key: key.name.clone().into(),
					value,
				});
			}
			Ok(Expr::Literal(Literal::Object(entries)))
		}
	}
}

/// Lowers a GQL expression in predicate (`WHERE`) position with GQL's
/// three-valued semantics: rows are kept only when the predicate is TRUE.
/// `negated` is the pending NNF negation pushed down from enclosing `NOT`s.
pub(super) async fn lower_predicate(
	stk: &mut Stk,
	expr: &GqlExpr,
	negated: bool,
	scope: &Scope<'_>,
) -> Result<Expr, SyntaxError> {
	match expr {
		GqlExpr::Unary {
			op: UnaryOp::Not,
			expr: inner,
			..
		} => stk.run(|stk| lower_predicate(stk, inner, !negated, scope)).await,
		GqlExpr::Binary {
			op: op @ (BinaryOp::And | BinaryOp::Or),
			left,
			right,
			..
		} => {
			let lowered_left = stk.run(|stk| lower_predicate(stk, left, negated, scope)).await?;
			let lowered_right = stk.run(|stk| lower_predicate(stk, right, negated, scope)).await?;
			// De Morgan: a negated AND becomes an OR of the negated branches.
			let and = (*op == BinaryOp::And) != negated;
			Ok(Expr::Binary {
				left: Box::new(lowered_left),
				op: if and {
					BinaryOperator::And
				} else {
					BinaryOperator::Or
				},
				right: Box::new(lowered_right),
			})
		}
		GqlExpr::Binary {
			op: BinaryOp::Xor,
			span,
			..
		} => reject_xor(*span),
		GqlExpr::Binary {
			op:
				op @ (BinaryOp::Eq
				| BinaryOp::Neq
				| BinaryOp::Lt
				| BinaryOp::Lte
				| BinaryOp::Gt
				| BinaryOp::Gte),
			left,
			right,
			span,
		} => lower_comparison(stk, *op, negated, left, right, *span, scope).await,
		GqlExpr::IsBool {
			..
		}
		| GqlExpr::IsNull {
			..
		} => lower_truth_test(stk, expr, negated, scope).await,
		GqlExpr::Literal(GqlLiteral::Bool(b), _) => Ok(Expr::Literal(Literal::Bool(*b != negated))),
		// Any other expression in predicate position is tested against an
		// explicit boolean, so that a NULL/NONE value excludes the row:
		// `b.flag` → `b.flag = true`, `NOT b.flag` → `b.flag = false` (§4).
		other => {
			let value = stk.run(|stk| lower_value(stk, other, scope)).await?;
			Ok(equality(value, Literal::Bool(!negated), false))
		}
	}
}

/// Lowers a property-map entry on a pattern element into the
/// `<element>.key = value` equality conjunct (§3), with the §4 equality
/// guards. The element is addressed by its binding (an anonymous element has a
/// hidden binding, so it still lowers).
pub(super) async fn lower_prop_equality(
	stk: &mut Stk,
	binding: BindingId,
	binding_name: &str,
	key: &Ident,
	value: &GqlExpr,
	scope: &Scope<'_>,
) -> Result<Expr, SyntaxError> {
	let kind = scope.registry.kind(binding);
	let left = scope.binding_idiom(binding_name, kind, &[key], Some(key.span))?;
	// The property side is always nullable; guard only when the value side
	// is nullable too (§4: `x = <literal>` needs no guard).
	let guards = if nullable(value, scope) {
		let mut atoms = vec![left.clone()];
		for atom in nullable_atoms(value, scope) {
			let atom = stk.run(|stk| lower_value(stk, atom, scope)).await?;
			if !atoms.contains(&atom) {
				atoms.push(atom);
			}
		}
		guard_conjuncts(&atoms)
	} else {
		Vec::new()
	};
	let right = stk.run(|stk| lower_value(stk, value, scope)).await?;
	let comparison = Expr::Binary {
		left: Box::new(left),
		op: BinaryOperator::Equal,
		right: Box::new(right),
	};
	Ok(match and_chain(guards) {
		Some(guards) => Expr::Binary {
			left: Box::new(guards),
			op: BinaryOperator::And,
			right: Box::new(comparison),
		},
		None => comparison,
	})
}

/// Folds expressions into a left-associative `AND` chain.
pub(super) fn and_chain(exprs: impl IntoIterator<Item = Expr>) -> Option<Expr> {
	exprs.into_iter().reduce(|left, right| Expr::Binary {
		left: Box::new(left),
		op: BinaryOperator::And,
		right: Box::new(right),
	})
}

/// Lowers a comparison leaf with the §4 null guards. `negated` complements
/// the operator first (NNF), so the guards apply to the effective
/// comparison.
async fn lower_comparison(
	stk: &mut Stk,
	op: BinaryOp,
	negated: bool,
	left: &GqlExpr,
	right: &GqlExpr,
	span: Span,
	scope: &Scope<'_>,
) -> Result<Expr, SyntaxError> {
	let op = if negated {
		complement(op)
	} else {
		op
	};
	let guard_atoms: Vec<&GqlExpr> = match op {
		// Ordering comparisons: SurrealQL's total order sorts NULL/NONE
		// below numbers, so every nullable operand needs a guard (E8a/E8b).
		BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Gt | BinaryOp::Gte => {
			let mut atoms = nullable_atoms(left, scope);
			atoms.extend(nullable_atoms(right, scope));
			atoms
		}
		// `=` deviates from GQL only when both sides can be null
		// (`NULL = NULL` is true in SurrealQL — E8c); a one-sided null
		// already compares unequal and excludes the row.
		BinaryOp::Eq if nullable(left, scope) && nullable(right, scope) => {
			let mut atoms = nullable_atoms(left, scope);
			atoms.extend(nullable_atoms(right, scope));
			atoms
		}
		// `<>` deviates whenever either side can be null (`NULL != 1` is
		// true in SurrealQL but UNKNOWN in GQL), so guard one-sided too.
		BinaryOp::Neq if nullable(left, scope) || nullable(right, scope) => {
			let mut atoms = nullable_atoms(left, scope);
			atoms.extend(nullable_atoms(right, scope));
			atoms
		}
		_ => Vec::new(),
	};
	// Lower and deduplicate the guarded atoms, preserving first-occurrence
	// order.
	let mut atoms: Vec<Expr> = Vec::new();
	for atom in guard_atoms {
		let atom = stk.run(|stk| lower_value(stk, atom, scope)).await?;
		if !atoms.contains(&atom) {
			atoms.push(atom);
		}
	}
	let lowered_left = stk.run(|stk| lower_value(stk, left, scope)).await?;
	let lowered_right = stk.run(|stk| lower_value(stk, right, scope)).await?;
	let comparison = Expr::Binary {
		left: Box::new(lowered_left),
		op: binary_op(op, span)?,
		right: Box::new(lowered_right),
	};
	Ok(match and_chain(guard_conjuncts(&atoms)) {
		Some(guards) => Expr::Binary {
			left: Box::new(guards),
			op: BinaryOperator::And,
			right: Box::new(comparison),
		},
		None => comparison,
	})
}

/// Builds the `atom != NONE AND atom != NULL` guard pair for each atom (§4).
fn guard_conjuncts(atoms: &[Expr]) -> Vec<Expr> {
	let mut out = Vec::with_capacity(atoms.len() * 2);
	for atom in atoms {
		out.push(Expr::Binary {
			left: Box::new(atom.clone()),
			op: BinaryOperator::NotEqual,
			right: Box::new(Expr::Literal(Literal::None)),
		});
		out.push(Expr::Binary {
			left: Box::new(atom.clone()),
			op: BinaryOperator::NotEqual,
			right: Box::new(Expr::Literal(Literal::Null)),
		});
	}
	out
}

/// Lowers `IS [NOT] NULL` and `IS [NOT] TRUE|FALSE|UNKNOWN` boolean tests
/// (§4). Truth tests are two-valued in GQL, so they need no guards;
/// `outer_negated` is the NNF negation pushed down from enclosing `NOT`s,
/// which negates the whole test.
async fn lower_truth_test(
	stk: &mut Stk,
	expr: &GqlExpr,
	outer_negated: bool,
	scope: &Scope<'_>,
) -> Result<Expr, SyntaxError> {
	match expr {
		GqlExpr::IsNull {
			expr: operand,
			negated,
			..
		} => {
			let value = stk.run(|stk| lower_value(stk, operand, scope)).await?;
			Ok(null_test(value, outer_negated != *negated))
		}
		GqlExpr::IsBool {
			expr: operand,
			value,
			negated,
			..
		} => {
			let operand = stk.run(|stk| lower_value(stk, operand, scope)).await?;
			let negated = outer_negated != *negated;
			match value {
				TruthValue::True => Ok(equality(operand, Literal::Bool(true), negated)),
				TruthValue::False => Ok(equality(operand, Literal::Bool(false), negated)),
				TruthValue::Unknown => Ok(null_test(operand, negated)),
			}
		}
		// Only called with `IsNull`/`IsBool` expressions.
		other => Err(syntax_error!("Internal error: not a truth test", @other.span())),
	}
}

/// `x IS NULL` → `x = NULL OR x = NONE`; negated, `x != NULL AND x != NONE`.
/// GQL cannot observe SurrealDB's NONE-vs-NULL distinction (§4).
fn null_test(value: Expr, negated: bool) -> Expr {
	let (cmp, combine) = if negated {
		(BinaryOperator::NotEqual, BinaryOperator::And)
	} else {
		(BinaryOperator::Equal, BinaryOperator::Or)
	};
	Expr::Binary {
		left: Box::new(Expr::Binary {
			left: Box::new(value.clone()),
			op: cmp.clone(),
			right: Box::new(Expr::Literal(Literal::Null)),
		}),
		op: combine,
		right: Box::new(Expr::Binary {
			left: Box::new(value),
			op: cmp,
			right: Box::new(Expr::Literal(Literal::None)),
		}),
	}
}

/// An (in)equality against a literal.
fn equality(left: Expr, literal: Literal, negated: bool) -> Expr {
	Expr::Binary {
		left: Box::new(left),
		op: if negated {
			BinaryOperator::NotEqual
		} else {
			BinaryOperator::Equal
		},
		right: Box::new(Expr::Literal(literal)),
	}
}

/// Collects the §4 guard atoms of a comparison operand: the property
/// accesses and parameters which can evaluate to `NULL`/`NONE`, reachable
/// through arithmetic, concatenation and sign operators. Literals, containers
/// and boolean-valued sub-expressions are never guarded.
///
/// Amendment (V2_DESIGN §8): a bare optional-bound variable `v` (one declared
/// inside an `OPTIONAL`) is also a guard atom — on an optional miss its whole
/// binding is `Value::Null` (R3), so a comparison reading it must exclude the
/// pre-null row. A mandatory bare variable still never needs a guard.
fn nullable_atoms<'a>(expr: &'a GqlExpr, scope: &Scope<'_>) -> Vec<&'a GqlExpr> {
	let mut out = Vec::new();
	let mut stack = vec![expr];
	while let Some(e) = stack.pop() {
		match e {
			GqlExpr::Property(..)
			| GqlExpr::Param {
				..
			} => out.push(e),
			GqlExpr::Variable(ident) if scope.variable_is_optional(ident) => out.push(e),
			GqlExpr::Unary {
				op: UnaryOp::Neg | UnaryOp::Plus,
				expr,
				..
			} => stack.push(expr),
			GqlExpr::Binary {
				op: BinaryOp::Concat | BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div,
				left,
				right,
				..
			} => {
				stack.push(right);
				stack.push(left);
			}
			_ => {}
		}
	}
	out
}

/// Returns whether a comparison operand can evaluate to `NULL`/`NONE`.
///
/// Amendment (V2_DESIGN §8): a bare optional-bound variable is nullable (it is
/// `Value::Null` on an optional miss, R3); a mandatory bare variable is not.
fn nullable(expr: &GqlExpr, scope: &Scope<'_>) -> bool {
	let mut stack = vec![expr];
	while let Some(e) = stack.pop() {
		match e {
			GqlExpr::Property(..)
			| GqlExpr::Param {
				..
			}
			| GqlExpr::Literal(GqlLiteral::Null, _) => return true,
			GqlExpr::Variable(ident) if scope.variable_is_optional(ident) => return true,
			GqlExpr::Unary {
				op: UnaryOp::Neg | UnaryOp::Plus,
				expr,
				..
			} => stack.push(expr),
			GqlExpr::Binary {
				op: BinaryOp::Concat | BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div,
				left,
				right,
				..
			} => {
				stack.push(right);
				stack.push(left);
			}
			_ => {}
		}
	}
	false
}

/// The complement of a comparison under NNF: `NOT (x < y)` ≡ `x >= y`.
fn complement(op: BinaryOp) -> BinaryOp {
	match op {
		BinaryOp::Eq => BinaryOp::Neq,
		BinaryOp::Neq => BinaryOp::Eq,
		BinaryOp::Lt => BinaryOp::Gte,
		BinaryOp::Lte => BinaryOp::Gt,
		BinaryOp::Gt => BinaryOp::Lte,
		BinaryOp::Gte => BinaryOp::Lt,
		other => other,
	}
}

/// Maps a GQL binary operator onto its SurrealQL counterpart. `XOR` has no
/// exactly-equivalent three-valued lowering and is rejected (§4, §7).
fn binary_op(op: BinaryOp, span: Span) -> Result<BinaryOperator, SyntaxError> {
	Ok(match op {
		BinaryOp::Or => BinaryOperator::Or,
		BinaryOp::Xor => return reject_xor(span),
		BinaryOp::And => BinaryOperator::And,
		BinaryOp::Eq => BinaryOperator::Equal,
		BinaryOp::Neq => BinaryOperator::NotEqual,
		BinaryOp::Lt => BinaryOperator::LessThan,
		BinaryOp::Lte => BinaryOperator::LessThanEqual,
		BinaryOp::Gt => BinaryOperator::MoreThan,
		BinaryOp::Gte => BinaryOperator::MoreThanEqual,
		// GQL `||` is string concatenation, which SurrealQL spells `+`.
		BinaryOp::Concat | BinaryOp::Add => BinaryOperator::Add,
		BinaryOp::Sub => BinaryOperator::Subtract,
		BinaryOp::Mul => BinaryOperator::Multiply,
		BinaryOp::Div => BinaryOperator::Divide,
	})
}

fn reject_xor<T>(span: Span) -> Result<T, SyntaxError> {
	bail!(
		"`XOR` is not supported yet",
		@span => "rewrite `a XOR b` as `(a OR b) AND NOT (a AND b)`"
	);
}

fn lower_param(name: &str, span: Span) -> Result<Expr, SyntaxError> {
	naming::validate_param_name(name, span)?;
	Ok(Expr::Param(Param::new(name)))
}

fn lower_literal(literal: &GqlLiteral) -> Expr {
	Expr::Literal(match literal {
		GqlLiteral::Null => Literal::Null,
		GqlLiteral::Bool(b) => Literal::Bool(*b),
		GqlLiteral::Integer(i) => Literal::Integer(*i),
		GqlLiteral::Float(f) => Literal::Float(*f),
		GqlLiteral::String(s) => Literal::String(s.clone().into()),
	})
}

/// Lowers a property access chain: a chain rooted at a pattern variable
/// resolves to a binding-row idiom; any other root lowers as a value and the
/// chain is appended as idiom fields.
async fn lower_property(
	stk: &mut Stk,
	expr: &GqlExpr,
	scope: &Scope<'_>,
) -> Result<Expr, SyntaxError> {
	let mut names: Vec<&Ident> = Vec::new();
	let mut base = expr;
	while let GqlExpr::Property(inner, name, _) = base {
		names.push(name);
		base = inner;
	}
	names.reverse();
	match base {
		GqlExpr::Variable(ident) => scope.binding_expr(ident, &names),
		other => {
			let start = stk.run(|stk| lower_value(stk, other, scope)).await?;
			let mut parts = match start {
				Expr::Idiom(idiom) => idiom.0,
				start => vec![Part::Start(start)],
			};
			parts.extend(names.iter().map(|name| Part::Field(name.name.clone().into())));
			Ok(Expr::Idiom(Idiom(parts)))
		}
	}
}

/// §5: the supported aggregates and how each maps onto a SurrealDB aggregate
/// function. `count` is special (`count(*)` counts rows, `count(x)` counts
/// non-null `x`); the rest take a single argument and map straight onto a
/// `math::*` / `array::*` accumulator the streaming `Aggregate` operator already
/// knows. Names compare case-insensitively.
enum AggregateTarget {
	/// `count` — handled specially (star form vs. single-argument null count).
	Count,
	/// A single-argument aggregate that lowers to the named SurrealDB function.
	Mapped(&'static str),
}

/// Resolves a (lowercased) GQL aggregate name to its lowering target, or `None`
/// if the name is not a supported aggregate.
///
/// Numeric contract (v1): `sum`/`avg` are numeric by the GQL spec, and `min`/`max`
/// map onto the numeric `math::*` accumulators, which **silently ignore
/// non-numeric values** (including `NULL`/`NONE`). So over a non-numeric column
/// they accumulate nothing and return the empty-accumulator identity (`0` for
/// `sum`, `±∞` for `min`/`max`) rather than an orderable min/max or an error.
/// Orderable (datetime/string/duration) `MIN`/`MAX` is a deliberate follow-up;
/// see `language-tests/tests/gql/aggregate_min_non_numeric.gql`.
fn aggregate_target(name: &str) -> Option<AggregateTarget> {
	Some(match name {
		"count" => AggregateTarget::Count,
		"sum" => AggregateTarget::Mapped("math::sum"),
		"collect" | "collect_list" => AggregateTarget::Mapped("array::group"),
		"min" => AggregateTarget::Mapped("math::min"),
		"max" => AggregateTarget::Mapped("math::max"),
		"avg" => AggregateTarget::Mapped("math::mean"),
		_ => return None,
	})
}

/// GQL aggregate names that are recognised but not implemented yet — reported
/// with the aggregate-specific message rather than the generic one.
const UNSUPPORTED_AGGREGATES: &[&str] =
	&["percentile_cont", "percentile_disc", "stddev_pop", "stddev_samp"];

/// §5: lowers a function call. The only functions supported are the aggregates
/// (in `RETURN`/aggregating-`ORDER BY` position); every other call is rejected.
async fn lower_function(
	stk: &mut Stk,
	name: &Ident,
	quantifier: Option<SetQuantifier>,
	star: Option<Span>,
	args: &[GqlExpr],
	span: Span,
	scope: &Scope<'_>,
) -> Result<Expr, SyntaxError> {
	let lowered = name.name.to_ascii_lowercase();
	let Some(target) = aggregate_target(&lowered) else {
		// `*` and a `DISTINCT`/`ALL` set quantifier are aggregate-only syntax, so
		// flag such calls as aggregates even when the name is unknown.
		if star.is_some()
			|| quantifier.is_some()
			|| UNSUPPORTED_AGGREGATES.contains(&lowered.as_str())
		{
			bail!("Aggregate functions are not supported yet", @span);
		}
		bail!("The function `{}` is not supported yet", name.name, @span);
	};

	if !scope.allow_aggregates {
		bail!(
			"Aggregate functions are only allowed in RETURN items and ORDER BY keys",
			@span => "an aggregate cannot appear in WHERE, GROUP BY, or inside another aggregate"
		);
	}
	if quantifier.is_some() {
		bail!(
			"DISTINCT/ALL inside an aggregate is not supported yet",
			@span => "remove the set quantifier"
		);
	}

	match target {
		AggregateTarget::Count => {
			if star.is_some() {
				// `count(*)` counts every row in the group: a zero-argument
				// SurrealDB `count`.
				return Ok(function_call("count", Vec::new()));
			}
			let arg = single_arg(name, args, span)?;
			let arg_scope = scope.no_aggregates();
			let lowered = stk.run(|stk| lower_value(stk, arg, &arg_scope)).await?;
			// GQL `count(x)` counts rows where `x` is not null. SurrealDB's
			// `count(<arg>)` counts truthy arguments, so feed it the non-null
			// guard: `x != NONE AND x != NULL` is `true` exactly when `x` is
			// present, and the truthy count is then the non-null count.
			Ok(function_call("count", vec![null_test(lowered, true)]))
		}
		AggregateTarget::Mapped(surreal_name) => {
			if let Some(star) = star {
				bail!(
					"`{}(*)` is not supported; only `count(*)` takes `*`",
					name.name,
					@star => "pass an expression to aggregate"
				);
			}
			let arg = single_arg(name, args, span)?;
			let arg_scope = scope.no_aggregates();
			let lowered = stk.run(|stk| lower_value(stk, arg, &arg_scope)).await?;
			Ok(function_call(surreal_name, vec![lowered]))
		}
	}
}

/// Returns the single argument of an aggregate call, rejecting any other arity.
fn single_arg<'a>(
	name: &Ident,
	args: &'a [GqlExpr],
	span: Span,
) -> Result<&'a GqlExpr, SyntaxError> {
	match args {
		[arg] => Ok(arg),
		_ => bail!(
			"`{}` takes exactly one argument",
			name.name,
			@span => "aggregate over a single expression"
		),
	}
}

/// Builds a normal SurrealDB function-call expression.
fn function_call(name: &str, arguments: Vec<Expr>) -> Expr {
	Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal(name.to_owned()),
		arguments,
	}))
}

/// Whether a (parsed, not-yet-lowered) expression contains a supported
/// aggregate call (`count(*)` or any name in [`aggregate_target`]). Used by the
/// RETURN-clause lowering to decide whether the query aggregates and to enforce
/// the "every column is a grouping key or an aggregate" rule. Walks on an
/// explicit stack so a deep operator spine cannot overflow.
pub(super) fn gql_contains_aggregate(expr: &GqlExpr) -> bool {
	let mut stack = vec![expr];
	while let Some(e) = stack.pop() {
		match e {
			GqlExpr::FunctionCall {
				name,
				star,
				args,
				..
			} => {
				if star.is_some() || aggregate_target(&name.name.to_ascii_lowercase()).is_some() {
					return true;
				}
				stack.extend(args.iter());
			}
			GqlExpr::Property(inner, _, _) => stack.push(inner),
			GqlExpr::Unary {
				expr,
				..
			}
			| GqlExpr::IsBool {
				expr,
				..
			}
			| GqlExpr::IsNull {
				expr,
				..
			} => stack.push(expr),
			GqlExpr::Binary {
				left,
				right,
				..
			} => {
				stack.push(left);
				stack.push(right);
			}
			GqlExpr::List(items, _) => stack.extend(items.iter()),
			GqlExpr::Map(fields, _) => stack.extend(fields.iter().map(|(_, v)| v)),
			GqlExpr::Literal(..)
			| GqlExpr::Param {
				..
			}
			| GqlExpr::Variable(_) => {}
		}
	}
	false
}
