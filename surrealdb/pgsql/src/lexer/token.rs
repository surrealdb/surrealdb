//! Token representations for PostgreSQL lexical analysis.

use std::borrow::Cow;
use std::fmt;

use crate::lexer::keyword::Keyword;
use crate::span::Span;

/// A lexical token with its kind and source span.
#[derive(Debug, Clone, PartialEq)]
pub struct Token<'a> {
	/// The category and payload of the token.
	pub kind: TokenKind<'a>,
	/// Source span covering the full token.
	pub span: Span,
}

impl<'a> Token<'a> {
	/// Create a new token.
	pub const fn new(kind: TokenKind<'a>, span: Span) -> Self {
		Self {
			kind,
			span,
		}
	}

	/// Return `true` if this is an EOF token.
	pub const fn is_eof(&self) -> bool {
		matches!(self.kind, TokenKind::Eof)
	}
}

/// Token classification matching PostgreSQL lexical rules.
#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind<'a> {
	/// A recognized SQL keyword.
	Keyword(Keyword),

	/// An unquoted identifier (case-folded to lowercase in PostgreSQL convention).
	Ident(Cow<'a, str>),

	/// A double-quoted identifier (`"..."`), case preserved and unescaped.
	QuotedIdent(String),

	/// A standard single-quoted string constant (`'...'` or `E'...'`).
	StringConstant(String),

	/// A dollar-quoted string literal (`$$...$$` or `$tag$...$tag$`).
	DollarQuotedString {
		/// Tag name (empty for `$$`).
		tag: &'a str,
		/// Content between tags.
		content: &'a str,
	},

	/// A bit-string literal (`B'1010'` or `X'DEADBEEF'`).
	BitString {
		/// True if hexadecimal bit string (`X'...'`), false if binary (`B'...'`).
		is_hex: bool,
		/// Content inside the literal.
		content: &'a str,
	},

	/// Integer numeric literal.
	Integer(i64),

	/// Large integer or integer representation when exact digits must be preserved.
	IntegerRaw(&'a str),

	/// Floating-point or decimal numeric literal.
	Float(f64),

	/// Float representation preserving exact representation.
	FloatRaw(&'a str),

	/// Positional parameter token (`$1`, `$2`, etc.).
	Param(u32),

	/// An operator (e.g. `+`, `-`, `*`, `/`, `%`, `^`, `=`, `<>`, `!=`, `<=`, `>=`, `->`, `||`).
	Operator(&'a str),

	// --- Delimiters & Punctuation ---
	/// Comma (`,`)
	Comma,
	/// Semicolon (`;`)
	Semi,
	/// Period / dot (`.`)
	Dot,
	/// Double dot / range (`..`)
	DotDot,
	/// Colon (`:`)
	Colon,
	/// Double colon typecast (`::`)
	DoubleColon,
	/// Left parenthesis (`(`)
	LParen,
	/// Right parenthesis (`)`)
	RParen,
	/// Left bracket (`[`)
	LBracket,
	/// Right bracket (`]`)
	RBracket,
	/// Left curly brace (`{`)
	LBrace,
	/// Right curly brace (`}`)
	RBrace,

	/// End of input stream.
	Eof,
}

impl<'a> fmt::Display for TokenKind<'a> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Keyword(kw) => write!(f, "keyword '{}'", kw),
			Self::Ident(id) => write!(f, "identifier '{}'", id),
			Self::QuotedIdent(id) => write!(f, "quoted identifier \"{}\"", id),
			Self::StringConstant(s) => write!(f, "string literal '{}'", s),
			Self::DollarQuotedString {
				tag,
				..
			} => write!(f, "dollar-quoted string '${}$'", tag),
			Self::BitString {
				is_hex,
				..
			} => {
				if *is_hex {
					f.write_str("hex bit string")
				} else {
					f.write_str("binary bit string")
				}
			}
			Self::Integer(n) => write!(f, "integer {}", n),
			Self::IntegerRaw(s) => write!(f, "integer {}", s),
			Self::Float(n) => write!(f, "float {}", n),
			Self::FloatRaw(s) => write!(f, "float {}", s),
			Self::Param(n) => write!(f, "parameter ${}", n),
			Self::Operator(op) => write!(f, "operator '{}'", op),
			Self::Comma => f.write_str("','"),
			Self::Semi => f.write_str("';'"),
			Self::Dot => f.write_str("'.'"),
			Self::DotDot => f.write_str("'..'"),
			Self::Colon => f.write_str("':'"),
			Self::DoubleColon => f.write_str("'::'"),
			Self::LParen => f.write_str("'('"),
			Self::RParen => f.write_str("')'"),
			Self::LBracket => f.write_str("'['"),
			Self::RBracket => f.write_str("']'"),
			Self::LBrace => f.write_str("'{'"),
			Self::RBrace => f.write_str("'}'"),
			Self::Eof => f.write_str("end of file"),
		}
	}
}
