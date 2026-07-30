//! Module specifying the token representation of the GQL parser.
//!
//! The token vocabulary is transcribed from the vendored grammar
//! `doc/gql/GQL.g4` (sections 21.1-21.4) as distilled in
//! `doc/gql/REFERENCE.md`. Spans reuse [`crate::syn::token::Span`] so GQL
//! errors render through the same machinery as SurrealQL errors.

use std::fmt;

pub use crate::syn::token::Span;

mod keyword;
pub use keyword::Keyword;
pub(crate) use keyword::keyword_t;
mod mac;
pub(crate) use mac::t;

/// The kind of a numeric literal token.
///
/// From `unsignedNumericLiteral` (GQL.g4:2977-3002) and the numeric lexer
/// rules (GQL.g4:3192-3272). Underscore digit separators are allowed in all
/// integer forms. The token carries no sign: `-3` is unary minus applied to
/// `3`.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum NumberKind {
	/// A decimal integer: `123`, `1_000_000`.
	Integer,
	/// A hexadecimal integer: `0xdead_beef`. The `0x` prefix is lowercase
	/// only.
	Hex,
	/// An octal integer: `0o777`. The `0o` prefix is lowercase only.
	Octal,
	/// A binary integer: `0b1010`. The `0b` prefix is lowercase only.
	Binary,
	/// Common (decimal point) notation: `123.`, `123.456`, `.456`.
	Float,
	/// Scientific notation: `1.5e10`, `2E-3`.
	Scientific,
}

/// An optional suffix attached to a numeric literal token.
///
/// From `exactNumericLiteral` / `approximateNumericLiteral`
/// (GQL.g4:2982-2995).
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum NumberSuffix {
	/// `M`: an exact (decimal) number.
	Exact,
	/// `F`: an approximate (float) number.
	Float,
	/// `D`: an approximate (double) number.
	Double,
}

/// The type of a token.
///
/// The compound edge-bracket, arrow and slash tokens are single lexer tokens
/// (GQL.g4:3629-3658); the lexer must emit them longest-match — the parser
/// never assembles them from their constituent characters.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum TokenKind {
	/// A keyword: a reserved, prereserved or non-reserved word.
	Keyword(Keyword),
	/// A regular identifier.
	Identifier,
	/// A single-quoted character sequence: `'…'`. Always a string literal.
	/// `no_escape` is set when prefixed with `@` (`@'…'`), disabling escape
	/// processing.
	SingleQuoted {
		no_escape: bool,
	},
	/// A double-quoted character sequence: `"…"`. One token for both a
	/// character string literal and a delimited identifier; the parser
	/// disambiguates by parse position. `no_escape` is set when prefixed
	/// with `@`.
	DoubleQuoted {
		no_escape: bool,
	},
	/// An accent-quoted (backtick) character sequence: `` `…` ``. Always a
	/// delimited identifier. `no_escape` is set when prefixed with `@`.
	AccentQuoted {
		no_escape: bool,
	},
	/// A general (value) parameter reference: `$name`.
	Parameter,
	/// A substituted parameter reference: `$$name`.
	SubstitutedParameter,
	/// A numeric literal.
	Number {
		kind: NumberKind,
		suffix: Option<NumberSuffix>,
	},

	// Compound tokens, transcribed from GQL.g4:3629-3658.
	/// `|+|`
	MultisetAlternation,
	/// `]->`
	BracketRightArrow,
	/// `]~>`
	BracketTildeRightArrow,
	/// `||`
	Concat,
	/// `::`
	DoubleColon,
	/// `..`
	DoublePeriod,
	/// `>=`
	Gte,
	/// `<-`
	LeftArrow,
	/// `<~`
	LeftArrowTilde,
	/// `<-[`
	LeftArrowBracket,
	/// `<~[`
	LeftArrowTildeBracket,
	/// `<->`
	LeftMinusRight,
	/// `<-/`
	LeftMinusSlash,
	/// `<~/`
	LeftTildeSlash,
	/// `<=`
	Lte,
	/// `-[`
	MinusLeftBracket,
	/// `-/`
	MinusSlash,
	/// `<>` — the only not-equals operator: GQL has no `!=`.
	Neq,
	/// `->`
	RightArrow,
	/// `]-`
	RightBracketMinus,
	/// `]~`
	RightBracketTilde,
	/// `=>`
	RightDoubleArrow,
	/// `/-`
	SlashMinus,
	/// `/->`
	SlashMinusRight,
	/// `/~`
	SlashTilde,
	/// `/~>`
	SlashTildeRight,
	/// `~[`
	TildeLeftBracket,
	/// `~>`
	TildeRightArrow,
	/// `~/`
	TildeSlash,

	// Terminal characters, from GQL.g4 section 21.4.
	/// `&`
	Ampersand,
	/// `@`
	At,
	/// `:`
	Colon,
	/// `,`
	Comma,
	/// `=`
	Eq,
	/// `!`
	Exclamation,
	/// `>`
	Gt,
	/// `<`
	Lt,
	/// `-`
	Minus,
	/// `(`
	OpenParen,
	/// `)`
	CloseParen,
	/// `[`
	OpenBracket,
	/// `]`
	CloseBracket,
	/// `{`
	OpenBrace,
	/// `}`
	CloseBrace,
	/// `%`
	Percent,
	/// `.`
	Period,
	/// `+`
	Plus,
	/// `?`
	Question,
	/// `/`
	Slash,
	/// `*`
	Star,
	/// `~`
	Tilde,
	/// `|`
	VerticalBar,
	/// A token which indicates the end of the source.
	Eof,
	/// A token which could not be properly lexed.
	Invalid,
}

impl TokenKind {
	pub fn as_str(self) -> &'static str {
		match self {
			TokenKind::Keyword(x) => x.as_str(),
			TokenKind::Identifier => "an identifier",
			TokenKind::SingleQuoted {
				..
			} => "a string literal",
			TokenKind::DoubleQuoted {
				..
			} => "a string literal or delimited identifier",
			TokenKind::AccentQuoted {
				..
			} => "a delimited identifier",
			TokenKind::Parameter => "a parameter",
			TokenKind::SubstitutedParameter => "a substituted parameter",
			TokenKind::Number {
				..
			} => "a number",
			TokenKind::MultisetAlternation => "|+|",
			TokenKind::BracketRightArrow => "]->",
			TokenKind::BracketTildeRightArrow => "]~>",
			TokenKind::Concat => "||",
			TokenKind::DoubleColon => "::",
			TokenKind::DoublePeriod => "..",
			TokenKind::Gte => ">=",
			TokenKind::LeftArrow => "<-",
			TokenKind::LeftArrowTilde => "<~",
			TokenKind::LeftArrowBracket => "<-[",
			TokenKind::LeftArrowTildeBracket => "<~[",
			TokenKind::LeftMinusRight => "<->",
			TokenKind::LeftMinusSlash => "<-/",
			TokenKind::LeftTildeSlash => "<~/",
			TokenKind::Lte => "<=",
			TokenKind::MinusLeftBracket => "-[",
			TokenKind::MinusSlash => "-/",
			TokenKind::Neq => "<>",
			TokenKind::RightArrow => "->",
			TokenKind::RightBracketMinus => "]-",
			TokenKind::RightBracketTilde => "]~",
			TokenKind::RightDoubleArrow => "=>",
			TokenKind::SlashMinus => "/-",
			TokenKind::SlashMinusRight => "/->",
			TokenKind::SlashTilde => "/~",
			TokenKind::SlashTildeRight => "/~>",
			TokenKind::TildeLeftBracket => "~[",
			TokenKind::TildeRightArrow => "~>",
			TokenKind::TildeSlash => "~/",
			TokenKind::Ampersand => "&",
			TokenKind::At => "@",
			TokenKind::Colon => ":",
			TokenKind::Comma => ",",
			TokenKind::Eq => "=",
			TokenKind::Exclamation => "!",
			TokenKind::Gt => ">",
			TokenKind::Lt => "<",
			TokenKind::Minus => "-",
			TokenKind::OpenParen => "(",
			TokenKind::CloseParen => ")",
			TokenKind::OpenBracket => "[",
			TokenKind::CloseBracket => "]",
			TokenKind::OpenBrace => "{",
			TokenKind::CloseBrace => "}",
			TokenKind::Percent => "%",
			TokenKind::Period => ".",
			TokenKind::Plus => "+",
			TokenKind::Question => "?",
			TokenKind::Slash => "/",
			TokenKind::Star => "*",
			TokenKind::Tilde => "~",
			TokenKind::VerticalBar => "|",
			TokenKind::Eof => "Eof",
			TokenKind::Invalid => "Invalid",
		}
	}
}

impl fmt::Display for TokenKind {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str((*self).as_str())
	}
}

/// A single token in GQL source text.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct Token {
	pub kind: TokenKind,
	pub span: Span,
}

impl Token {
	pub const fn invalid() -> Token {
		Token {
			kind: TokenKind::Invalid,
			span: Span::empty(),
		}
	}

	/// Returns if the token is `end of file`.
	pub fn is_eof(&self) -> bool {
		matches!(self.kind, TokenKind::Eof)
	}
}

#[cfg(test)]
mod test {
	use super::{Keyword, TokenKind, t};

	#[test]
	fn keyword_classification() {
		assert!(Keyword::Match.is_reserved());
		assert!(!Keyword::Match.is_prereserved());
		assert!(!Keyword::Match.is_non_reserved());
		assert!(Keyword::Abstract.is_prereserved());
		assert!(!Keyword::Abstract.is_reserved());
		assert!(Keyword::Node.is_non_reserved());
		assert!(!Keyword::Node.is_reserved());
		// Boolean literals behave as reserved words.
		assert!(Keyword::True.is_reserved());
		assert!(Keyword::Unknown.is_reserved());
		assert_eq!(Keyword::Skip.as_str(), "SKIP");
		assert_eq!(Keyword::Null.as_str(), "NULL");
		assert_eq!(Keyword::AllDifferent.as_str(), "ALL_DIFFERENT");
	}

	#[test]
	fn token_macro() {
		assert_eq!(t!("MATCH"), TokenKind::Keyword(Keyword::Match));
		assert_eq!(t!("NODE"), TokenKind::Keyword(Keyword::Node));
		assert_eq!(t!("<-["), TokenKind::LeftArrowBracket);
		assert_eq!(t!("]~>"), TokenKind::BracketTildeRightArrow);
		assert_eq!(t!("<>"), TokenKind::Neq);
		assert_eq!(t!("||"), TokenKind::Concat);
		assert_eq!(t!("$param"), TokenKind::Parameter);
		assert_eq!(t!("$$param"), TokenKind::SubstitutedParameter);
		assert_eq!(
			t!("@\""),
			TokenKind::DoubleQuoted {
				no_escape: true,
			}
		);
		// The macro must also be usable in pattern position.
		assert!(matches!(t!("RETURN"), t!("RETURN")));
		assert!(matches!(t!("-["), t!("-[")));
	}
}
