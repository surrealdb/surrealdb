//! Module specifying the token representation of the parser.

use std::hash::Hash;

mod keyword;
pub(crate) use keyword::keyword_t;
pub use keyword::Keyword;
mod mac;
pub(crate) use mac::t;

use crate::sql::{language::Language, Algorithm};

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
}

impl Operator {
	fn as_str(&self) -> &'static str {
		match self {
			Operator::Not => "'!'",
			Operator::Add => "'+'",
			Operator::Subtract => "'-'",
			Operator::Divide => "'÷'",
			Operator::Or => "'||'",
			Operator::And => "'&&'",
			Operator::Mult => "'×'",
			Operator::LessEqual => "'<='",
			Operator::GreaterEqual => "'>='",
			Operator::Star => "'*'",
			Operator::Power => "'**'",
			Operator::Equal => "'='",
			Operator::Exact => "'=='",
			Operator::NotEqual => "'!='",
			Operator::AllEqual => "'*='",
			Operator::AnyEqual => "'?='",
			Operator::Like => "'~'",
			Operator::NotLike => "'!~'",
			Operator::AllLike => "'*~'",
			Operator::AnyLike => "'?~'",
			Operator::Contains => "'∋'",
			Operator::NotContains => "'∌'",
			Operator::ContainsAll => "'⊇'",
			Operator::ContainsAny => "'⊃'",
			Operator::ContainsNone => "'⊅'",
			Operator::Inside => "'∈'",
			Operator::NotInside => "'∉'",
			Operator::AllInside => "'⊆'",
			Operator::AnyInside => "'⊂'",
			Operator::NoneInside => "'⊄'",
			Operator::Matches => "'@@'",
			Operator::Inc => "'+='",
			Operator::Dec => "'-='",
			Operator::Ext => "'+?='",
			Operator::Tco => "'?:'",
			Operator::Nco => "'??'",
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
	Euclidean,
	Manhattan,
	Hamming,
	Minkowski,
}

impl DistanceKind {
	pub fn as_str(&self) -> &'static str {
		match self {
			DistanceKind::Euclidean => "EUCLIDEAN",
			DistanceKind::Manhattan => "MANHATTAN",
			DistanceKind::Hamming => "HAMMING",
			DistanceKind::Minkowski => "MINKOWSKI",
		}
	}
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum NumberKind {
	// A plain integer number.
	Integer,
	// A number with a decimal postfix.
	Decimal,
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
pub enum TokenKind {
	Keyword(Keyword),
	Algorithm(Algorithm),
	Language(Language),
	Distance(DistanceKind),
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

	pub fn as_str(&self) -> &'static str {
		match *self {
			TokenKind::Keyword(x) => x.as_str(),
			TokenKind::Operator(x) => x.as_str(),
			TokenKind::Algorithm(_) => todo!(),
			TokenKind::Language(x) => x.as_str(),
			TokenKind::Distance(x) => x.as_str(),
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
			TokenKind::LeftChefron => "'<'",
			TokenKind::RightChefron => "'>'",
			TokenKind::Star => "'*'",
			TokenKind::Dollar => "'$'",
			TokenKind::Question => "'?'",
			TokenKind::ArrowRight => "'->'",
			TokenKind::ArrowLeft => "'<-'",
			TokenKind::BiArrow => "'<->'",
			TokenKind::ForwardSlash => "'/'",
			TokenKind::Dot => "'.'",
			TokenKind::DotDot => "'..'",
			TokenKind::DotDotDot => "'...'",
			TokenKind::SemiColon => "';'",
			TokenKind::PathSeperator => "'::'",
			TokenKind::Colon => "':'",
			TokenKind::Comma => "','",
			TokenKind::Vert => "'|'",
			TokenKind::At => "'@'",
			TokenKind::Invalid => "Invalid",
			TokenKind::Eof => "Eof",
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
}
