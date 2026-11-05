//! Operator definitions for PostgreSQL AST expressions.

use std::fmt;

/// Unary operators in PostgreSQL expressions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UnaryOperator {
	/// Positive sign (`+`)
	Plus,
	/// Negative sign (`-`)
	Minus,
	/// Logical negation (`NOT`)
	Not,
	/// Bitwise negation (`~`)
	BitNot,
	/// Custom user-defined or extension prefix operator (e.g. `@`, `|/`, `||/`)
	Custom(String),
}

impl fmt::Display for UnaryOperator {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Plus => f.write_str("+"),
			Self::Minus => f.write_str("-"),
			Self::Not => f.write_str("NOT "),
			Self::BitNot => f.write_str("~"),
			Self::Custom(op) => write!(f, "{}", op),
		}
	}
}

/// Binary operators in PostgreSQL expressions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
	// --- Arithmetic ---
	Add,
	Sub,
	Mul,
	Div,
	Mod,
	Exp,

	// --- Bitwise ---
	BitAnd,
	BitOr,
	BitXor,
	ShiftLeft,
	ShiftRight,

	// --- Logical ---
	And,
	Or,

	// --- Comparison ---
	Eq,
	NotEq,
	Lt,
	LtEq,
	Gt,
	GtEq,

	// --- String / Pattern Matching ---
	Concat,
	Like,
	NotLike,
	ILike,
	NotILike,
	SimilarTo,
	NotSimilarTo,
	RegexMatch,
	RegexMatchCi,
	RegexNotMatch,
	RegexNotMatchCi,

	// --- JSON / JSONB ---
	Arrow,
	LongArrow,
	HashArrow,
	HashLongArrow,

	// --- Geometric & Containment ---
	Contains,
	ContainedBy,

	// --- Custom / Extension Operators ---
	Custom(String),
}

impl fmt::Display for BinaryOperator {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Add => f.write_str("+"),
			Self::Sub => f.write_str("-"),
			Self::Mul => f.write_str("*"),
			Self::Div => f.write_str("/"),
			Self::Mod => f.write_str("%"),
			Self::Exp => f.write_str("^"),
			Self::BitAnd => f.write_str("&"),
			Self::BitOr => f.write_str("|"),
			Self::BitXor => f.write_str("#"),
			Self::ShiftLeft => f.write_str("<<"),
			Self::ShiftRight => f.write_str(">>"),
			Self::And => f.write_str("AND"),
			Self::Or => f.write_str("OR"),
			Self::Eq => f.write_str("="),
			Self::NotEq => f.write_str("<>"),
			Self::Lt => f.write_str("<"),
			Self::LtEq => f.write_str("<="),
			Self::Gt => f.write_str(">"),
			Self::GtEq => f.write_str(">="),
			Self::Concat => f.write_str("||"),
			Self::Like => f.write_str("LIKE"),
			Self::NotLike => f.write_str("NOT LIKE"),
			Self::ILike => f.write_str("ILIKE"),
			Self::NotILike => f.write_str("NOT ILIKE"),
			Self::SimilarTo => f.write_str("SIMILAR TO"),
			Self::NotSimilarTo => f.write_str("NOT SIMILAR TO"),
			Self::RegexMatch => f.write_str("~"),
			Self::RegexMatchCi => f.write_str("~*"),
			Self::RegexNotMatch => f.write_str("!~"),
			Self::RegexNotMatchCi => f.write_str("!~*"),
			Self::Arrow => f.write_str("->"),
			Self::LongArrow => f.write_str("->>"),
			Self::HashArrow => f.write_str("#>"),
			Self::HashLongArrow => f.write_str("#>>"),
			Self::Contains => f.write_str("@>"),
			Self::ContainedBy => f.write_str("<@"),
			Self::Custom(op) => write!(f, "{}", op),
		}
	}
}
