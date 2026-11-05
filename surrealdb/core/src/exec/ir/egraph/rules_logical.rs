//! The e-graph's logical rewrite rules: equalities between logical and expression terms.
//!
//! Each rule names the side conditions that make it sound, or a soundness statement when it has
//! none. No rule reads the physical catalog.
//!
//! [`CATALOGUE`] lists every rule that is written and printed. [`REGISTERED`] lists the rules
//! production plans with, the same decls in the same order. Each rule has an op-tag list beside
//! its declaration, which [`tags_of`] answers by rule name.

use super::decl::{AttrPred, AttrSrc, BuildCx, HeadPat, Pat, Rhs, RuleDecl, RuleTier, Tmpl};
use crate::exec::ir::expr::ops::{ExprOp, ExprTag, ShortCircuitOp};
use crate::exec::ir::iface::egraph::{KindClass, RuleDialect, SideCondition};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::expr::BinaryOperator;
use crate::val::{Number, Value};

const SHORT_CIRCUIT: OpTag = OpTag::Expr(ExprTag::ShortCircuit);
const TRUTHY: OpTag = OpTag::Expr(ExprTag::Truthy);
const LITERAL: OpTag = OpTag::Expr(ExprTag::Literal);
const BINARY: OpTag = OpTag::Expr(ExprTag::Binary);

/// Whether `kind` is the short-circuit operator `op`.
fn is_short_circuit(kind: &OpKind, op: ShortCircuitOp) -> bool {
	matches!(kind, OpKind::Expr(ExprOp::ShortCircuit { op: held }) if *held == op)
}

/// Whether `kind` is a literal whose value satisfies `test`.
///
/// The test matches the value by variant, because numbers of different variants may compare equal
/// while a rewrite must not confuse them.
fn is_literal(kind: &OpKind, test: fn(&Value) -> bool) -> bool {
	matches!(kind, OpKind::Expr(ExprOp::Literal(held)) if test(held))
}

fn is_and(kind: &OpKind) -> bool {
	is_short_circuit(kind, ShortCircuitOp::And)
}

fn is_or(kind: &OpKind) -> bool {
	is_short_circuit(kind, ShortCircuitOp::Or)
}

fn is_false(kind: &OpKind) -> bool {
	is_literal(kind, |value| matches!(value, Value::Bool(false)))
}

fn is_true(kind: &OpKind) -> bool {
	is_literal(kind, |value| matches!(value, Value::Bool(true)))
}

fn is_int_zero(kind: &OpKind) -> bool {
	is_literal(kind, |value| matches!(value, Value::Number(Number::Int(0))))
}

fn is_mul(kind: &OpKind) -> bool {
	matches!(
		kind,
		OpKind::Expr(ExprOp::Binary {
			op: BinaryOperator::Multiply
		})
	)
}

fn literal(value: Value) -> OpKind {
	OpKind::Expr(ExprOp::Literal(Box::new(value)))
}

fn build_false(_: &BuildCx<'_>) -> OpKind {
	literal(Value::Bool(false))
}

fn build_true(_: &BuildCx<'_>) -> OpKind {
	literal(Value::Bool(true))
}

fn build_int_zero(_: &BuildCx<'_>) -> OpKind {
	literal(Value::Number(Number::Int(0)))
}

const AND: AttrPred = AttrPred {
	name: "and",
	test: is_and,
};
const OR: AttrPred = AttrPred {
	name: "or",
	test: is_or,
};
const FALSE: AttrPred = AttrPred {
	name: "false",
	test: is_false,
};
const TRUE: AttrPred = AttrPred {
	name: "true",
	test: is_true,
};
const MUL: AttrPred = AttrPred {
	name: "mul",
	test: is_mul,
};
const INT_ZERO: AttrPred = AttrPred {
	name: "int 0",
	test: is_int_zero,
};

/// The pattern `(expr.literal{pred})`.
const fn literal_pat(pred: AttrPred) -> Pat {
	Pat::Node {
		bind: None,
		head: HeadPat {
			tag: LITERAL,
			attr: Some(pred),
		},
		kids: &[],
	}
}

/// The conditions that make dropping the operand `x` unobservable: it cannot raise, and it draws,
/// writes and reads nothing a second evaluation could see differently.
const DROPPED_OPERAND: &[SideCondition] =
	&[SideCondition::Infallible("x"), SideCondition::Deterministic("x")];

/// The conditions that make a zero product independent of the other operand `x`.
const ZERO_PRODUCT: &[SideCondition] = &[
	SideCondition::TypeFact("x", KindClass::Int),
	SideCondition::Infallible("x"),
	SideCondition::Deterministic("x"),
];

const SOUND_AND: &str = "rooted at expr.truthy, so the result is consumed as a boolean: a false \
                         right operand decides it, and the left operand cannot raise, draw or write";
const SOUND_OR: &str = "rooted at expr.truthy, so the result is consumed as a boolean: a true \
                        right operand decides it, and the left operand cannot raise, draw or write";
const SOUND_MUL: &str = "an int zero operand gives int zero when the other operand is an int; \
                         1.5 * 0 is a float and NONE * 0 raises, and the type fact excludes both";

/// `truthy(x AND false)` is `false`.
///
/// Applies where the conjunction is consumed through `expr.truthy`: element filters and closure
/// predicates. A `WHERE` clause yields its predicate without `expr.truthy` and is not matched.
pub(crate) static AND_FALSE_TRUTHY: RuleDecl = RuleDecl {
	name: "and_false_truthy",
	dialect: RuleDialect::LogicalRewrite,
	tier: RuleTier::Normal,
	lhs: Pat::Node {
		bind: Some("t"),
		head: HeadPat {
			tag: TRUTHY,
			attr: None,
		},
		kids: &[Pat::Node {
			bind: None,
			head: HeadPat {
				tag: SHORT_CIRCUIT,
				attr: Some(AND),
			},
			kids: &[
				Pat::Var("x"),
				Pat::Region {
					kind: RegionKind::LazyOperand,
					binder: None,
					kids: &[literal_pat(FALSE)],
				},
			],
		}],
	},
	rhs: Rhs::Tmpl(Tmpl::Node {
		tag: LITERAL,
		attrs: AttrSrc::Build {
			name: "false",
			build: build_false,
		},
		frame_of: "t",
		kids: &[],
	}),
	when: DROPPED_OPERAND,
	sound: Some(SOUND_AND),
	answers: &[],
	drops: &[],
	duplicates: &[],
	needs_total_order: false,
};
static AND_FALSE_TRUTHY_TAGS: &[OpTag] = &[LITERAL, SHORT_CIRCUIT, TRUTHY];

/// `truthy(x OR true)` is `true`.
///
/// Applies where the disjunction is consumed through `expr.truthy`: element filters and closure
/// predicates. A `WHERE` clause yields its predicate without `expr.truthy` and is not matched.
pub(crate) static OR_TRUE_TRUTHY: RuleDecl = RuleDecl {
	name: "or_true_truthy",
	dialect: RuleDialect::LogicalRewrite,
	tier: RuleTier::Normal,
	lhs: Pat::Node {
		bind: Some("t"),
		head: HeadPat {
			tag: TRUTHY,
			attr: None,
		},
		kids: &[Pat::Node {
			bind: None,
			head: HeadPat {
				tag: SHORT_CIRCUIT,
				attr: Some(OR),
			},
			kids: &[
				Pat::Var("x"),
				Pat::Region {
					kind: RegionKind::LazyOperand,
					binder: None,
					kids: &[literal_pat(TRUE)],
				},
			],
		}],
	},
	rhs: Rhs::Tmpl(Tmpl::Node {
		tag: LITERAL,
		attrs: AttrSrc::Build {
			name: "true",
			build: build_true,
		},
		frame_of: "t",
		kids: &[],
	}),
	when: DROPPED_OPERAND,
	sound: Some(SOUND_OR),
	answers: &[],
	drops: &[],
	duplicates: &[],
	needs_total_order: false,
};
static OR_TRUE_TRUTHY_TAGS: &[OpTag] = &[LITERAL, SHORT_CIRCUIT, TRUTHY];

/// `x * 0` is `0` for an int `x`.
///
/// Applies where the other operand is an int by type fact; a field read carries no type fact.
pub(crate) static MUL_ZERO_INT_R: RuleDecl = RuleDecl {
	name: "mul_zero_int_r",
	dialect: RuleDialect::LogicalRewrite,
	tier: RuleTier::Normal,
	lhs: Pat::Node {
		bind: Some("b"),
		head: HeadPat {
			tag: BINARY,
			attr: Some(MUL),
		},
		kids: &[Pat::Var("x"), literal_pat(INT_ZERO)],
	},
	rhs: Rhs::Tmpl(Tmpl::Node {
		tag: LITERAL,
		attrs: AttrSrc::Build {
			name: "int 0",
			build: build_int_zero,
		},
		frame_of: "b",
		kids: &[],
	}),
	when: ZERO_PRODUCT,
	sound: Some(SOUND_MUL),
	answers: &[],
	drops: &[],
	duplicates: &[],
	needs_total_order: false,
};
static MUL_ZERO_INT_R_TAGS: &[OpTag] = &[LITERAL, BINARY];

/// `0 * x` is `0` for an int `x`.
///
/// Applies where the other operand is an int by type fact; a field read carries no type fact.
pub(crate) static MUL_ZERO_INT_L: RuleDecl = RuleDecl {
	name: "mul_zero_int_l",
	dialect: RuleDialect::LogicalRewrite,
	tier: RuleTier::Normal,
	lhs: Pat::Node {
		bind: Some("b"),
		head: HeadPat {
			tag: BINARY,
			attr: Some(MUL),
		},
		kids: &[literal_pat(INT_ZERO), Pat::Var("x")],
	},
	rhs: Rhs::Tmpl(Tmpl::Node {
		tag: LITERAL,
		attrs: AttrSrc::Build {
			name: "int 0",
			build: build_int_zero,
		},
		frame_of: "b",
		kids: &[],
	}),
	when: ZERO_PRODUCT,
	sound: Some(SOUND_MUL),
	answers: &[],
	drops: &[],
	duplicates: &[],
	needs_total_order: false,
};
static MUL_ZERO_INT_L_TAGS: &[OpTag] = &[LITERAL, BINARY];

/// Every written rule, in rank order. The printed form of this list is pinned by a golden test.
pub(crate) static CATALOGUE: &[&RuleDecl] =
	&[&AND_FALSE_TRUTHY, &OR_TRUE_TRUTHY, &MUL_ZERO_INT_R, &MUL_ZERO_INT_L];

/// The rules production plans with: [`CATALOGUE`], element for element, in rank order.
pub(crate) static REGISTERED: &[&RuleDecl] =
	&[&AND_FALSE_TRUTHY, &OR_TRUE_TRUTHY, &MUL_ZERO_INT_R, &MUL_ZERO_INT_L];

/// The op tags the rule named like `decl` mentions, in tag order, or `None` for a rule this file
/// does not declare.
pub(crate) fn tags_of(decl: &RuleDecl) -> Option<&'static [OpTag]> {
	match decl.name {
		"and_false_truthy" => Some(AND_FALSE_TRUTHY_TAGS),
		"or_true_truthy" => Some(OR_TRUE_TRUTHY_TAGS),
		"mul_zero_int_r" => Some(MUL_ZERO_INT_R_TAGS),
		"mul_zero_int_l" => Some(MUL_ZERO_INT_L_TAGS),
		_ => None,
	}
}
