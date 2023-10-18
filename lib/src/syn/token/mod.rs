//! Module specifying the token representation of the parser.

use std::{hash::Hash, num::NonZeroU32};

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
}

/// A surrealql keyword.
#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum Keyword {
	After,
	All,
	Analyze,
	As,
	Ascii,
	Assert,
	Before,
	Begin,
	Blank,
	Bm25,
	Break,
	By,
	Camel,
	Cancel,
	ChangeFeed,
	Changes,
	Class,
	Comment,
	Commit,
	Content,
	Continue,
	Cosine,
	Create,
	Database,
	Default,
	Define,
	Delete,
	Diff,
	Dimension,
	Dist,
	Drop,
	Edgengram,
	Euclidean,
	Event,
	Explain,
	False,
	Fetch,
	Field,
	Fields,
	Filters,
	Flexible,
	Fn,
	For,
	From,
	Full,
	Function,
	Group,
	Hamming,
	Ignore,
	Index,
	Info,
	Insert,
	Into,
	If,
	Is,
	Kill,
	Let,
	Limit,
	Live,
	Lowercase,
	Mahalanobis,
	Manhattan,
	Merge,
	Minkowski,
	Model,
	MTree,
	Namespace,
	Ngram,
	NoIndex,
	None,
	Null,
	Omit,
	On,
	Only,
	Option,
	Order,
	Parallel,
	Param,
	Passhash,
	Password,
	Patch,
	Permissions,
	Punct,
	Relate,
	Remove,
	Replace,
	Return,
	Roles,
	Root,
	Schemafull,
	Schemaless,
	Scope,
	Search,
	Select,
	Session,
	Set,
	Show,
	Signim,
	Signup,
	Since,
	Sleep,
	Snowball,
	Split,
	Start,
	Table,
	Then,
	Throw,
	Timeout,
	Tokeizers,
	Token,
	Transaction,
	True,
	Type,
	Unique,
	Unset,
	Update,
	Uppercase,
	Use,
	User,
	Value,
	Version,
	Vs,
	When,
	Where,
	With,
	AllInside,
	AndKw,
	AnyInside,
	Inside,
	Intersects,
	NoneInside,
	NotInside,
	OrKw,
	Outside,
	Not,
	And,
	ContainsAll,
	ContainsAny,
	ContainsNone,
	ContainsNot,
	Contains,
	In,

	Any,
	Future,
	Bool,
	Bytes,
	Datetime,
	Decimal,
	Duration,
	Float,
	Int,
	Number,
	Object,
	Point,
	String,
	Uuid,
	Ulid,
	Rand,
}

impl Keyword {
	fn as_str(&self) -> &'static str {
		match *self {
			Keyword::After => "AFTER",
			Keyword::All => "ALL",
			Keyword::Analyze => "ANALYZE",
			Keyword::As => "AS",
			Keyword::Ascii => "ASCII",
			Keyword::Assert => "ASSERT",
			Keyword::Before => "BEFORE",
			Keyword::Begin => "BEGIN",
			Keyword::Blank => "BLANK",
			Keyword::Bm25 => "BM25",
			Keyword::Break => "BREAK",
			Keyword::By => "BY",
			Keyword::Camel => "CAMEL",
			Keyword::Cancel => "CANCEL",
			Keyword::ChangeFeed => "CHANGEFEED",
			Keyword::Changes => "CHANGES",
			Keyword::Class => "CLASS",
			Keyword::Comment => "COMMENT",
			Keyword::Commit => "COMMIT",
			Keyword::Content => "CONTENT",
			Keyword::Continue => "CONTINUE",
			Keyword::Cosine => "COSINE",
			Keyword::Create => "CREATE",
			Keyword::Database => "DATABASE",
			Keyword::Default => "DEFAULT",
			Keyword::Define => "DEFINE",
			Keyword::Delete => "DELETE",
			Keyword::Diff => "DIFF",
			Keyword::Dimension => "DIMENSION",
			Keyword::Dist => "DIST",
			Keyword::Drop => "DROP",
			Keyword::Edgengram => "EDGENGRAM",
			Keyword::Euclidean => "EUCLIDEAN",
			Keyword::Event => "EVENT",
			Keyword::Explain => "EXPLAIN",
			Keyword::False => "false",
			Keyword::Fetch => "FETCH",
			Keyword::Field => "FIELD",
			Keyword::Fields => "FIELDS",
			Keyword::Filters => "FILTERS",
			Keyword::Flexible => "FLEXIBLE",
			Keyword::For => "FOR",
			Keyword::From => "FROM",
			Keyword::Full => "FULL",
			Keyword::Function => "FUNCTION",
			Keyword::Group => "GROUP",
			Keyword::Hamming => "HAMMING",
			Keyword::Ignore => "IGNORE",
			Keyword::Index => "INDEX",
			Keyword::Info => "INFO",
			Keyword::Insert => "INSERT",
			Keyword::Into => "INTO",
			Keyword::If => "IF",
			Keyword::Is => "IS",
			Keyword::Kill => "KILL",
			Keyword::Let => "LET",
			Keyword::Limit => "LIMIT",
			Keyword::Live => "LIVE",
			Keyword::Lowercase => "LOWERCASE",
			Keyword::Mahalanobis => "MAHALANOBIS",
			Keyword::Manhattan => "MANHATTAN",
			Keyword::Merge => "MERGE",
			Keyword::Minkowski => "MINKOWSKI",
			Keyword::Model => "MODEL",
			Keyword::MTree => "MTREE",
			Keyword::Namespace => "NAMESPACE",
			Keyword::Ngram => "NGRAM",
			Keyword::NoIndex => "NOINDEX",
			Keyword::None => "NONE",
			Keyword::Null => "NULL",
			Keyword::Omit => "OMIT",
			Keyword::On => "ON",
			Keyword::Only => "ONLY",
			Keyword::Option => "OPTION",
			Keyword::Order => "ORDER",
			Keyword::Parallel => "PARALLEL",
			Keyword::Param => "PARAM",
			Keyword::Passhash => "PASSHASH",
			Keyword::Password => "PASSWORD",
			Keyword::Patch => "PATCH",
			Keyword::Permissions => "PERMISSIONS",
			Keyword::Punct => "PUNCT",
			Keyword::Relate => "RELATE",
			Keyword::Remove => "REMOVE",
			Keyword::Replace => "REPLACE",
			Keyword::Return => "RETURN",
			Keyword::Roles => "ROLES",
			Keyword::Root => "ROOT",
			Keyword::Schemafull => "SCHEMAFULL",
			Keyword::Schemaless => "SCHEMALESS",
			Keyword::Scope => "SCOPE",
			Keyword::Search => "SEARCH",
			Keyword::Select => "SELECT",
			Keyword::Session => "SESSION",
			Keyword::Set => "SET",
			Keyword::Show => "SHOW",
			Keyword::Signim => "SIGNIM",
			Keyword::Signup => "SIGNUP",
			Keyword::Since => "SINCE",
			Keyword::Sleep => "SLEEP",
			Keyword::Snowball => "SNOWBALL",
			Keyword::Split => "SPLIT",
			Keyword::Start => "START",
			Keyword::Table => "TABLE",
			Keyword::Then => "THEN",
			Keyword::Throw => "THROW",
			Keyword::Timeout => "TIMEOUT",
			Keyword::Tokeizers => "TOKEIZERS",
			Keyword::Token => "TOKEN",
			Keyword::Transaction => "TRANSACTION",
			Keyword::True => "true",
			Keyword::Type => "TYPE",
			Keyword::Unique => "UNIQUE",
			Keyword::Unset => "UNSET",
			Keyword::Update => "UPDATE",
			Keyword::Uppercase => "UPPERCASE",
			Keyword::Use => "USE",
			Keyword::User => "USER",
			Keyword::Value => "VALUE",
			Keyword::Version => "VERSION",
			Keyword::Vs => "VS",
			Keyword::When => "WHEN",
			Keyword::Where => "WHERE",
			Keyword::With => "WITH",
			Keyword::AllInside => "ALLINSIDE",
			Keyword::AndKw => "ANDKW",
			Keyword::AnyInside => "ANYINSIDE",
			Keyword::Inside => "INSIDE",
			Keyword::Intersects => "INTERSECTS",
			Keyword::NoneInside => "NONEINSIDE",
			Keyword::NotInside => "NOTINSIDE",
			Keyword::OrKw => "ORKW",
			Keyword::Outside => "OUTSIDE",
			Keyword::Not => "NOT",
			Keyword::And => "AND",
			Keyword::ContainsAll => "CONTAINSALL",
			Keyword::ContainsAny => "CONTAINSANY",
			Keyword::ContainsNone => "CONTAINSNONE",
			Keyword::ContainsNot => "CONTAINSNOT",
			Keyword::Contains => "CONTAINS",
			Keyword::In => "IN",

			Keyword::Any => "ANY",
			Keyword::Future => "FUTURE",
			Keyword::Bool => "BOOL",
			Keyword::Bytes => "BYTES",
			Keyword::Datetime => "DATETIME",
			Keyword::Decimal => "DECIMAL",
			Keyword::Duration => "DURATION",
			Keyword::Float => "FLOAT",
			Keyword::Fn => "fn",
			Keyword::Int => "INT",
			Keyword::Number => "NUMBER",
			Keyword::Object => "OBJECT",
			Keyword::Point => "POINT",
			Keyword::String => "STRING",
			Keyword::Uuid => "UUID",
			Keyword::Ulid => "ULID",
			Keyword::Rand => "RAND",
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
	/// `??`
	Tco,
	/// `?:`
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
			Operator::Tco => "'??'",
			Operator::Nco => "'?:'",
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

/// The type of token
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum TokenKind {
	Keyword(Keyword),
	Algorithm(Algorithm),
	Language(Language),
	Operator(Operator),
	OpenDelim(Delim),
	CloseDelim(Delim),
	Strand,
	/// A parameter like `$name`.
	Parameter,
	/// A duration.
	Duration,
	Number,
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

impl TokenKind {
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
			TokenKind::Algorithm(x) => todo!(),
			TokenKind::Language(x) => todo!(),
			TokenKind::OpenDelim(Delim::Paren) => "(",
			TokenKind::OpenDelim(Delim::Brace) => "{",
			TokenKind::OpenDelim(Delim::Bracket) => "[",
			TokenKind::CloseDelim(Delim::Paren) => ")",
			TokenKind::CloseDelim(Delim::Brace) => "}",
			TokenKind::CloseDelim(Delim::Bracket) => "]",
			TokenKind::Strand => "a strand",
			TokenKind::Parameter => "a parameter",
			TokenKind::Duration => "a duration",
			TokenKind::Number => "a number",
			TokenKind::Identifier => "an identifier",
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

/// A index for extra data associated with the token.
#[derive(Clone, Copy, Eq, PartialEq, Debug, Hash)]
pub struct DataIndex(NonZeroU32);

impl From<u32> for DataIndex {
	fn from(value: u32) -> Self {
		let idx = NonZeroU32::new(value.checked_add(1).unwrap()).unwrap();
		DataIndex(idx)
	}
}
impl From<DataIndex> for u32 {
	fn from(value: DataIndex) -> Self {
		u32::from(value.0) - 1
	}
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct Token {
	pub kind: TokenKind,
	pub span: Span,
	pub data_index: Option<DataIndex>,
}

impl Token {
	pub const fn invalid() -> Token {
		Token {
			kind: TokenKind::Invalid,
			span: Span::empty(),
			data_index: None,
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
