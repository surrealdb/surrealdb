//! Module specifying the token representation of the parser.

use std::hash::Hash;

mod keyword;
pub(crate) use keyword::keyword_t;
pub use keyword::Keyword;
mod mac;
pub(crate) use mac::t;

use crate::sql::change_feed_include::ChangeFeedInclude;
use crate::sql::{language::Language, Algorithm};

/// A location in the source passed to the lexer.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
#[non_exhaustive]
pub struct Span {
	/// Offset in bytes.
	pub offset: u32,
	/// The amount of bytes this location encompasses.
	pub len: u32,
}

impl Span {
	/// Create a new empty span.
	pub const fn empty() -> Self {
		Span {
			offset: 0,
			len: 0,
		}
	}

	pub fn is_empty(&self) -> bool {
		self.len == 0
	}

	/// Create a span that covers the range of both spans as well as possible space inbetween.
	pub fn covers(self, other: Span) -> Span {
		let start = self.offset.min(other.offset);
		let end = (self.offset + self.len).max(other.offset + other.len);
		let len = end - start;
		Span {
			offset: start,
			len,
		}
	}

	// returns a zero-length span that starts after the current span.
	pub fn after(self) -> Span {
		Span {
			offset: self.offset + self.len,
			len: 0,
		}
	}
}

#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
#[non_exhaustive]
pub enum Operator {
	/// `!`
	Not,
	/// `+`
	Add,
	/// `-`
	Subtract,
	/// `÷`
	Divide,
	/// `×` or `∙`
	Mult,
	/// `||`
	Or,
	/// `&&`
	And,
	/// `<=`
	LessEqual,
	/// `>=`
	GreaterEqual,
	/// `*`
	Star,
	/// `**`
	Power,
	/// `=`
	Equal,
	/// `==`
	Exact,
	/// `!=`
	NotEqual,
	/// `*=`
	AllEqual,
	/// `?=`
	AnyEqual,
	/// `~`
	Like,
	/// `!~`
	NotLike,
	/// `*~`
	AllLike,
	/// `?~`
	AnyLike,
	/// `∋`
	Contains,
	/// `∌`
	NotContains,
	/// `⊇`
	ContainsAll,
	/// `⊃`
	ContainsAny,
	/// `⊅`
	ContainsNone,
	/// `∈`
	Inside,
	/// `∉`
	NotInside,
	/// `⊆`
	AllInside,
	/// `⊂`
	AnyInside,
	/// `⊄`
	NoneInside,
	/// `@123@`
	Matches,
	/// `+=`
	Inc,
	/// `-=`
	Dec,
	/// `+?=`
	Ext,
	/// `?:`
	Tco,
	/// `??`
	Nco,
	/// `<|`
	KnnOpen,
	/// `|>`
	KnnClose,
}

impl Operator {
	fn as_str(&self) -> &'static str {
		match self {
			Operator::Not => "!",
			Operator::Add => "+",
			Operator::Subtract => "-",
			Operator::Divide => "÷",
			Operator::Or => "||",
			Operator::And => "&&",
			Operator::Mult => "×",
			Operator::LessEqual => "<=",
			Operator::GreaterEqual => ">=",
			Operator::Star => "*",
			Operator::Power => "**",
			Operator::Equal => "=",
			Operator::Exact => "==",
			Operator::NotEqual => "!=",
			Operator::AllEqual => "*=",
			Operator::AnyEqual => "?=",
			Operator::Like => "~",
			Operator::NotLike => "!~",
			Operator::AllLike => "*~",
			Operator::AnyLike => "?~",
			Operator::Contains => "∋",
			Operator::NotContains => "∌",
			Operator::ContainsAll => "⊇",
			Operator::ContainsAny => "⊃",
			Operator::ContainsNone => "⊅",
			Operator::Inside => "∈",
			Operator::NotInside => "∉",
			Operator::AllInside => "⊆",
			Operator::AnyInside => "⊂",
			Operator::NoneInside => "⊄",
			Operator::Matches => "@@",
			Operator::Inc => "+=",
			Operator::Dec => "-=",
			Operator::Ext => "+?=",
			Operator::Tco => "?:",
			Operator::Nco => "??",
			Operator::KnnOpen => "<|",
			Operator::KnnClose => "|>",
		}
	}
}

/// A delimiting token, denoting the start or end of a certain production.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
#[non_exhaustive]
pub enum Delim {
	/// `()`
	Paren,
	/// `[]`
	Bracket,
	/// `{}`
	Brace,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
#[non_exhaustive]
pub enum DistanceKind {
	Chebyshev,
	Cosine,
	Euclidean,
	Hamming,
	Jaccard,
	Manhattan,
	Minkowski,
	Pearson,
}

impl DistanceKind {
	pub fn as_str(&self) -> &'static str {
		match self {
			DistanceKind::Chebyshev => "CHEBYSHEV",
			DistanceKind::Cosine => "COSINE",
			DistanceKind::Euclidean => "EUCLIDEAN",
			DistanceKind::Hamming => "HAMMING",
			DistanceKind::Jaccard => "JACCARD",
			DistanceKind::Manhattan => "MANHATTAN",
			DistanceKind::Minkowski => "MINKOWSKI",
			DistanceKind::Pearson => "PEARSON",
		}
	}
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
#[non_exhaustive]
pub enum VectorTypeKind {
	F64,
	F32,
	I64,
	I32,
	I16,
}

impl VectorTypeKind {
	pub fn as_str(&self) -> &'static str {
		match self {
			Self::F64 => "F64",
			Self::F32 => "F32",
			Self::I64 => "I64",
			Self::I32 => "I32",
			Self::I16 => "I16",
		}
	}
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
#[non_exhaustive]
pub enum NumberKind {
	// A plain integer number.
	Integer,
	// A number with a decimal postfix.
	Decimal,
	// A number with a decimal postfix.
	DecimalExponent,
	// A number with a float postfix.
	Float,
	// A number with a `.3` part.
	Mantissa,
	// A number with a `.3e10` part.
	MantissaExponent,
	// A number with a `.3e10` part.
	Exponent,
	NaN,
}

/// The type of token
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
#[non_exhaustive]
pub enum TokenKind {
	Keyword(Keyword),
	Algorithm(Algorithm),
	ChangeFeedInclude(ChangeFeedInclude),
	Language(Language),
	Distance(DistanceKind),
	VectorType(VectorTypeKind),
	Operator(Operator),
	OpenDelim(Delim),
	CloseDelim(Delim),
	// a token denoting the opening of a record string, i.e. `r"`
	OpenRecordString {
		double: bool,
	},
	/// a token denoting the clsoing of a record string, i.e. `"`
	/// Never produced normally by the lexer.
	CloseRecordString {
		double: bool,
	},
	Regex,
	Uuid,
	DateTime,
	Strand,
	/// A parameter like `$name`.
	Parameter,
	/// A duration.
	Duration,
	Number(NumberKind),
	Identifier,
	/// `<`
	LeftChefron,
	/// `>`
	RightChefron,
	/// `*`
	Star,
	/// `?`
	Question,
	/// `$`
	Dollar,
	/// `->`
	ArrowRight,
	/// `<-`
	ArrowLeft,
	/// `<->`
	BiArrow,
	/// '/'
	ForwardSlash,
	/// `.`
	Dot,
	/// `..`
	DotDot,
	/// `...` or `…`
	DotDotDot,
	/// `;`
	SemiColon,
	/// `::`
	PathSeperator,
	/// `:`
	Colon,
	/// `,`
	Comma,
	/// `|`
	Vert,
	/// `@`
	At,
	/// A token which could not be properly lexed.
	Invalid,
	/// A token which indicates the end of the file.
	Eof,
}

/// An assertion statically checking that the size of Tokenkind remains two bytes
const _TOKEN_KIND_SIZE_ASSERT: [(); 2] = [(); std::mem::size_of::<TokenKind>()];

impl TokenKind {
	pub fn has_data(&self) -> bool {
		matches!(
			self,
			TokenKind::Identifier
				| TokenKind::Uuid
				| TokenKind::DateTime
				| TokenKind::Strand
				| TokenKind::Parameter
				| TokenKind::Regex
		)
	}

	pub fn can_be_identifier(&self) -> bool {
		matches!(
			self,
			TokenKind::Identifier
				| TokenKind::Keyword(_)
				| TokenKind::Language(_)
				| TokenKind::Algorithm(_)
		)
	}

	fn algorithm_as_str(algo: Algorithm) -> &'static str {
		match algo {
			Algorithm::EdDSA => "EDDSA",
			Algorithm::Es256 => "ES256",
			Algorithm::Es384 => "ES384",
			Algorithm::Es512 => "ES512",
			Algorithm::Hs256 => "HS256",
			Algorithm::Hs384 => "HS384",
			Algorithm::Hs512 => "HS512",
			Algorithm::Ps256 => "PS256",
			Algorithm::Ps384 => "PS384",
			Algorithm::Ps512 => "PS512",
			Algorithm::Rs256 => "RS256",
			Algorithm::Rs384 => "RS384",
			Algorithm::Rs512 => "RS512",
			Algorithm::Jwks => "JWKS",
		}
	}

	pub fn as_str(&self) -> &'static str {
		match *self {
			TokenKind::Keyword(x) => x.as_str(),
			TokenKind::Operator(x) => x.as_str(),
			TokenKind::Algorithm(x) => Self::algorithm_as_str(x),
			TokenKind::Language(x) => x.as_str(),
			TokenKind::Distance(x) => x.as_str(),
			TokenKind::VectorType(x) => x.as_str(),
			TokenKind::OpenDelim(Delim::Paren) => "(",
			TokenKind::OpenDelim(Delim::Brace) => "{",
			TokenKind::OpenDelim(Delim::Bracket) => "[",
			TokenKind::CloseDelim(Delim::Paren) => ")",
			TokenKind::CloseDelim(Delim::Brace) => "}",
			TokenKind::CloseDelim(Delim::Bracket) => "]",
			TokenKind::OpenRecordString {
				..
			} => "a record string",
			TokenKind::CloseRecordString {
				..
			} => "a closing record string",
			TokenKind::Uuid => "a uuid",
			TokenKind::DateTime => "a date-time",
			TokenKind::Strand => "a strand",
			TokenKind::Parameter => "a parameter",
			TokenKind::Duration => "a duration",
			TokenKind::Number(_) => "a number",
			TokenKind::Identifier => "an identifier",
			TokenKind::Regex => "a regex",
			TokenKind::LeftChefron => "<",
			TokenKind::RightChefron => ">",
			TokenKind::Star => "*",
			TokenKind::Dollar => "$",
			TokenKind::Question => "?",
			TokenKind::ArrowRight => "->",
			TokenKind::ArrowLeft => "<-",
			TokenKind::BiArrow => "<->",
			TokenKind::ForwardSlash => "/",
			TokenKind::Dot => ".",
			TokenKind::DotDot => "..",
			TokenKind::DotDotDot => "...",
			TokenKind::SemiColon => ";",
			TokenKind::PathSeperator => "::",
			TokenKind::Colon => ":",
			TokenKind::Comma => ",",
			TokenKind::Vert => "|",
			TokenKind::At => "@",
			TokenKind::Invalid => "Invalid",
			TokenKind::Eof => "Eof",
			TokenKind::ChangeFeedInclude(_) => "change feed include",
		}
	}
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
#[non_exhaustive]
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

	/// Returns if the token is invalid.
	pub fn is_invalid(&self) -> bool {
		matches!(self.kind, TokenKind::Invalid)
	}

	/// Returns if the token is `end of file`.
	pub fn is_eof(&self) -> bool {
		matches!(self.kind, TokenKind::Eof)
	}
}
