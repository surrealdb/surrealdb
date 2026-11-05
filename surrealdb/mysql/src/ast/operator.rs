//! MySQL operators enumeration.

/// Unary operators in MySQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOperator {
	Plus,
	Minus,
	Not,
	BitNot,
	Binary,
}

impl UnaryOperator {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Plus => "+",
			Self::Minus => "-",
			Self::Not => "NOT",
			Self::BitNot => "~",
			Self::Binary => "BINARY",
		}
	}
}

/// Binary operators in MySQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
	// Arithmetic
	Add,
	Sub,
	Mul,
	Div,
	IntDiv, // DIV
	Mod,

	// Bitwise
	BitAnd,
	BitOr,
	BitXor,
	ShiftLeft,
	ShiftRight,

	// Comparison
	Equal,
	NullSafeEqual, // <=>
	NotEqual,
	Less,
	LessEqual,
	Greater,
	GreaterEqual,
	Like,
	Regexp,
	Rlike,

	// Logical
	And,
	Or,
	Xor,

	// String
	Concat,

	// Assignment
	Assign, // :=
}

impl BinaryOperator {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Add => "+",
			Self::Sub => "-",
			Self::Mul => "*",
			Self::Div => "/",
			Self::IntDiv => "DIV",
			Self::Mod => "%",
			Self::BitAnd => "&",
			Self::BitOr => "|",
			Self::BitXor => "^",
			Self::ShiftLeft => "<<",
			Self::ShiftRight => ">>",
			Self::Equal => "=",
			Self::NullSafeEqual => "<=>",
			Self::NotEqual => "!=",
			Self::Less => "<",
			Self::LessEqual => "<=",
			Self::Greater => ">",
			Self::GreaterEqual => ">=",
			Self::Like => "LIKE",
			Self::Regexp => "REGEXP",
			Self::Rlike => "RLIKE",
			Self::And => "AND",
			Self::Or => "OR",
			Self::Xor => "XOR",
			Self::Concat => "||",
			Self::Assign => ":=",
		}
	}
}
