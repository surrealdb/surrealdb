//! Tokens produced by the MySQL lexical scanner.

use crate::lexer::keyword::Keyword;
use crate::lexer::span::Span;

/// A single lexical token with its kind and source span.
#[derive(Debug, Clone, PartialEq)]
pub struct Token {
	/// Token category and payload.
	pub kind: TokenKind,
	/// Source span covering this token.
	pub span: Span,
}

impl Token {
	/// Create a new token.
	pub const fn new(kind: TokenKind, span: Span) -> Self {
		Self {
			kind,
			span,
		}
	}
}

/// Category and content of a token.
#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
	// Keyword
	Keyword(Keyword),

	// Identifiers
	Ident(String),
	CharsetIntro(String),

	// Literals
	IntLiteral(i64),
	FloatLiteral(f64),
	StringLiteral(String),
	HexLiteral(Vec<u8>),
	BitLiteral(Vec<u8>),

	// Variables
	UserVar(String),
	SystemVar {
		name: String,
		is_global: bool,
	},

	// Positional parameter '?'
	Param(usize),

	// Delimiters
	Comma,
	Semicolon,
	Dot,
	LParen,
	RParen,
	LBracket,
	RBracket,
	LBrace,
	RBrace,
	Colon,
	ColonEqual,
	At,

	// Arithmetic & Bitwise Operators
	Plus,
	Minus,
	Star,
	Slash,
	Percent,
	ShiftLeft,
	ShiftRight,
	Ampersand,
	Pipe,
	Caret,
	Tilde,

	// Comparison & Logical Operators
	Equal,
	NullSafeEqual, // <=>
	NotEqual,      // != or <>
	Less,
	LessEqual,
	Greater,
	GreaterEqual,
	Bang,            // !
	DoubleAmpersand, // &&
	DoublePipe,      // ||

	// End of Input
	Eof,
}

impl std::fmt::Display for TokenKind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Keyword(kw) => write!(f, "{}", kw.as_str()),
			Self::Ident(id) => write!(f, "{id}"),
			Self::CharsetIntro(cs) => write!(f, "_{cs}"),
			Self::IntLiteral(n) => write!(f, "{n}"),
			Self::FloatLiteral(fl) => write!(f, "{fl}"),
			Self::StringLiteral(s) => write!(f, "'{s}'"),
			Self::HexLiteral(_) => write!(f, "HEX_LITERAL"),
			Self::BitLiteral(_) => write!(f, "BIT_LITERAL"),
			Self::UserVar(v) => write!(f, "@{v}"),
			Self::SystemVar {
				name,
				is_global,
			} => {
				if *is_global {
					write!(f, "@@global.{name}")
				} else {
					write!(f, "@@{name}")
				}
			}
			Self::Param(idx) => write!(f, "?{idx}"),
			Self::Comma => write!(f, ","),
			Self::Semicolon => write!(f, ";"),
			Self::Dot => write!(f, "."),
			Self::LParen => write!(f, "("),
			Self::RParen => write!(f, ")"),
			Self::LBracket => write!(f, "["),
			Self::RBracket => write!(f, "]"),
			Self::LBrace => write!(f, "{{"),
			Self::RBrace => write!(f, "}}"),
			Self::Colon => write!(f, ":"),
			Self::ColonEqual => write!(f, ":="),
			Self::At => write!(f, "@"),
			Self::Plus => write!(f, "+"),
			Self::Minus => write!(f, "-"),
			Self::Star => write!(f, "*"),
			Self::Slash => write!(f, "/"),
			Self::Percent => write!(f, "%"),
			Self::ShiftLeft => write!(f, "<<"),
			Self::ShiftRight => write!(f, ">>"),
			Self::Ampersand => write!(f, "&"),
			Self::Pipe => write!(f, "|"),
			Self::Caret => write!(f, "^"),
			Self::Tilde => write!(f, "~"),
			Self::Equal => write!(f, "="),
			Self::NullSafeEqual => write!(f, "<=>"),
			Self::NotEqual => write!(f, "!="),
			Self::Less => write!(f, "<"),
			Self::LessEqual => write!(f, "<="),
			Self::Greater => write!(f, ">"),
			Self::GreaterEqual => write!(f, ">="),
			Self::Bang => write!(f, "!"),
			Self::DoubleAmpersand => write!(f, "&&"),
			Self::DoublePipe => write!(f, "||"),
			Self::Eof => write!(f, "<EOF>"),
		}
	}
}
