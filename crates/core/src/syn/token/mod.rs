//! Module specifying the token representation of the parser.

use std::fmt;
use std::hash::Hash;

mod keyword;
pub use keyword::Keyword;
pub(crate) use keyword::keyword_t;
mod mac;
pub(crate) use mac::t;

use crate::sql::Algorithm;
use crate::sql::language::Language;

/// A location in the source passed to the lexer.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
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

	/// Create a span that covers the range of both spans as well as possible
	/// space inbetween.
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
	// returns a zero-length span that starts after the current span.
	pub fn after_offset(self) -> u32 {
		self.offset + self.len
	}

	/// Returns if the given span is the next span after this one.
	pub fn is_followed_by(&self, other: &Self) -> bool {
		let end = self.offset as usize + self.len as usize;
		other.offset as usize == end
	}

	/// Returns if this span immediately follows the given.
	pub fn follows_from(&self, other: &Self) -> bool {
		let end = self.offset as usize + self.len as usize;
		other.offset as usize == end
	}
}

#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
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
	/// `%`
	Modulo,
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
			Operator::Modulo => "%",
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
pub enum Delim {
	/// `()`
	Paren,
	/// `[]`
	Bracket,
	/// `{}`
	Brace,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
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

impl Algorithm {
	pub fn as_str(&self) -> &'static str {
		match self {
			Self::EdDSA => "EDDSA",
			Self::Es256 => "ES256",
			Self::Es384 => "ES384",
			Self::Es512 => "ES512",
			Self::Hs256 => "HS256",
			Self::Hs384 => "HS384",
			Self::Hs512 => "HS512",
			Self::Ps256 => "PS256",
			Self::Ps384 => "PS384",
			Self::Ps512 => "PS512",
			Self::Rs256 => "RS256",
			Self::Rs384 => "RS384",
			Self::Rs512 => "RS512",
		}
	}
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum QouteKind {
	/// `'`
	Plain,
	/// `"`
	PlainDouble,
	/// `r'`
	RecordId,
	/// `r"`
	RecordIdDouble,
	/// `u'`
	Uuid,
	/// `u"`
	UuidDouble,
	/// `d'`
	DateTime,
	/// `d"`
	DateTimeDouble,
	/// `b'`
	Bytes,
	/// `b"`
	BytesDouble,
	/// `f'`
	File,
	/// `f"`
	FileDouble,
}

impl QouteKind {
	pub fn as_str(&self) -> &'static str {
		match self {
			QouteKind::Plain | QouteKind::PlainDouble => "a strand",
			QouteKind::RecordId | QouteKind::RecordIdDouble => "a record-id strand",
			QouteKind::Uuid | QouteKind::UuidDouble => "a uuid",
			QouteKind::DateTime | QouteKind::DateTimeDouble => "a datetime",
			QouteKind::Bytes | QouteKind::BytesDouble => "a bytestring",
			QouteKind::File | QouteKind::FileDouble => "a file",
		}
	}
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum Glued {
	Number,
	Duration,
	Strand,
	Datetime,
	Uuid,
	Bytes,
	File,
}

impl Glued {
	fn as_str(&self) -> &'static str {
		match self {
			Glued::Number => "a number",
			Glued::Strand => "a strand",
			Glued::Uuid => "a uuid",
			Glued::Datetime => "a datetime",
			Glued::Duration => "a duration",
			Glued::Bytes => "a bytestring",
			Glued::File => "a file",
		}
	}
}

/// The type of token
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum TokenKind {
	WhiteSpace,
	Keyword(Keyword),
	Algorithm(Algorithm),
	Language(Language),
	Distance(DistanceKind),
	VectorType(VectorTypeKind),
	Operator(Operator),
	OpenDelim(Delim),
	CloseDelim(Delim),
	/// a token denoting the opening of a string, i.e. `r"`
	Qoute(QouteKind),
	/// A parameter like `$name`.
	Parameter,
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
	/// A token which indicates the end of the file.
	Eof,
	/// A token consiting of one or more ascii digits.
	Digits,
	/// The Not-A-Number number token.
	NaN,
	/// A token which is a compound token which has been glued together and then
	/// put back into the token buffer. This is required for some places where
	/// we need to look past possible compound tokens.
	Glued(Glued),
	/// A token which could not be properly lexed.
	Invalid,
}

impl fmt::Display for TokenKind {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(self.as_str())
	}
}

/// An assertion statically checking that the size of Tokenkind remains two
/// bytes
const _TOKEN_KIND_SIZE_ASSERT: [(); 2] = [(); std::mem::size_of::<TokenKind>()];

impl TokenKind {
	pub fn has_data(&self) -> bool {
		matches!(self, TokenKind::Identifier | TokenKind::Glued(_))
	}

	fn algorithm_as_str(alg: Algorithm) -> &'static str {
		match alg {
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
			TokenKind::Parameter => "a parameter",
			TokenKind::Identifier => "an identifier",
			TokenKind::LeftChefron => "<",
			TokenKind::RightChefron => ">",
			TokenKind::Star => "*",
			TokenKind::Dollar => "$",
			TokenKind::Question => "?",
			TokenKind::ArrowRight => "->",
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
			TokenKind::WhiteSpace => "whitespace",
			TokenKind::Qoute(x) => x.as_str(),
			TokenKind::Digits => "a number",
			TokenKind::NaN => "NaN",
			TokenKind::Glued(x) => x.as_str(),
			// below are small broken up tokens which are most of the time identifiers.
		}
	}
}

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

	/// Returns if the token is invalid.
	pub fn is_invalid(&self) -> bool {
		matches!(self.kind, TokenKind::Invalid)
	}

	/// Returns if the token is `end of file`.
	pub fn is_eof(&self) -> bool {
		matches!(self.kind, TokenKind::Eof)
	}

	pub fn is_followed_by(&self, other: &Token) -> bool {
		self.span.is_followed_by(&other.span)
	}

	pub fn follows_from(&self, other: &Token) -> bool {
		self.span.follows_from(&other.span)
	}
}

/// A token which is mad up of more complex inner parts.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct CompoundToken<T> {
	pub value: T,
	pub span: Span,
}

/// A compound token which lexes a javascript function body.
pub struct JavaScript;
