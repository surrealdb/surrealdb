//! Module specifying the token representation of the parser.

use std::{hash::Hash, num::NonZeroU32};

mod mac;
pub(crate) use mac::t;

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
	Arabic,
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
	Danish,
	Database,
	Default,
	Define,
	Delete,
	Diff,
	Drop,
	Dutch,
	EdDSA,
	Edgengram,
	English,
	Es256,
	Es384,
	Es512,
	Euclidean,
	Event,
	Explain,
	False,
	Fetch,
	Fields,
	Filters,
	Flexibile,
	For,
	French,
	From,
	Full,
	Function,
	German,
	Greek,
	Group,
	Hamming,
	Hs256,
	Hs384,
	Hs512,
	Hungarian,
	Ignore,
	Index,
	Info,
	Insert,
	Into,
	If,
	Is,
	Italian,
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
	Namespace,
	Ngram,
	NoIndex,
	None,
	Norwegian,
	Null,
	Omit,
	On,
	Only,
	Option,
	Order,
	Parallel,
	Param,
	Passhash,
	Patch,
	Permissions,
	Portuguese,
	Ps256,
	Ps384,
	Ps512,
	Punct,
	Relate,
	Remove,
	Replace,
	Return,
	Roles,
	Romanian,
	Root,
	Rs256,
	Rs384,
	Rs512,
	Russian,
	Schemafull,
	Schemaless,
	Scope,
	Select,
	Session,
	Set,
	Show,
	Signim,
	Signup,
	Since,
	Sleep,
	Snowball,
	Spanish,
	Split,
	Start,
	Swedish,
	Table,
	Tamil,
	Then,
	Throw,
	Timeout,
	Tokeizers,
	Transaction,
	True,
	Turkish,
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
}

impl Keyword {
	fn as_str(&self) -> &'static str {
		match *self {
			Keyword::After => "AFTER",
			Keyword::All => "ALL",
			Keyword::Analyze => "ANALYZE",
			Keyword::Arabic => "ARABIC",
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
			Keyword::Danish => "DANISH",
			Keyword::Database => "DATABASE",
			Keyword::Default => "DEFAULT",
			Keyword::Define => "DEFINE",
			Keyword::Delete => "DELETE",
			Keyword::Diff => "DIFF",
			Keyword::Drop => "DROP",
			Keyword::Dutch => "DUTCH",
			Keyword::EdDSA => "EDDSA",
			Keyword::Edgengram => "EDGENGRAM",
			Keyword::English => "ENGLISH",
			Keyword::Es256 => "ES256",
			Keyword::Es384 => "ES384",
			Keyword::Es512 => "ES512",
			Keyword::Euclidean => "EUCLIDEAN",
			Keyword::Event => "EVENT",
			Keyword::Explain => "EXPLAIN",
			Keyword::False => "false",
			Keyword::Fetch => "FETCH",
			Keyword::Fields => "FIELDS",
			Keyword::Filters => "FILTERS",
			Keyword::Flexibile => "FLEXIBILE",
			Keyword::For => "FOR",
			Keyword::French => "FRENCH",
			Keyword::From => "FROM",
			Keyword::Full => "FULL",
			Keyword::Function => "FUNCTION",
			Keyword::German => "GERMAN",
			Keyword::Greek => "GREEK",
			Keyword::Group => "GROUP",
			Keyword::Hamming => "HAMMING",
			Keyword::Hs256 => "HS256",
			Keyword::Hs384 => "HS384",
			Keyword::Hs512 => "HS512",
			Keyword::Hungarian => "HUNGARIAN",
			Keyword::Ignore => "IGNORE",
			Keyword::Index => "INDEX",
			Keyword::Info => "INFO",
			Keyword::Insert => "INSERT",
			Keyword::Into => "INTO",
			Keyword::If => "IF",
			Keyword::Is => "IS",
			Keyword::Italian => "ITALIAN",
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
			Keyword::Namespace => "NAMESPACE",
			Keyword::Ngram => "NGRAM",
			Keyword::NoIndex => "NOINDEX",
			Keyword::None => "NONE",
			Keyword::Norwegian => "NORWEGIAN",
			Keyword::Null => "NULL",
			Keyword::Omit => "OMIT",
			Keyword::On => "ON",
			Keyword::Only => "ONLY",
			Keyword::Option => "OPTION",
			Keyword::Order => "ORDER",
			Keyword::Parallel => "PARALLEL",
			Keyword::Param => "PARAM",
			Keyword::Passhash => "PASSHASH",
			Keyword::Patch => "PATCH",
			Keyword::Permissions => "PERMISSIONS",
			Keyword::Portuguese => "PORTUGUESE",
			Keyword::Ps256 => "PS256",
			Keyword::Ps384 => "PS384",
			Keyword::Ps512 => "PS512",
			Keyword::Punct => "PUNCT",
			Keyword::Relate => "RELATE",
			Keyword::Remove => "REMOVE",
			Keyword::Replace => "REPLACE",
			Keyword::Return => "RETURN",
			Keyword::Roles => "ROLES",
			Keyword::Romanian => "ROMANIAN",
			Keyword::Root => "ROOT",
			Keyword::Rs256 => "RS256",
			Keyword::Rs384 => "RS384",
			Keyword::Rs512 => "RS512",
			Keyword::Russian => "RUSSIAN",
			Keyword::Schemafull => "SCHEMAFULL",
			Keyword::Schemaless => "SCHEMALESS",
			Keyword::Scope => "SCOPE",
			Keyword::Select => "SELECT",
			Keyword::Session => "SESSION",
			Keyword::Set => "SET",
			Keyword::Show => "SHOW",
			Keyword::Signim => "SIGNIM",
			Keyword::Signup => "SIGNUP",
			Keyword::Since => "SINCE",
			Keyword::Sleep => "SLEEP",
			Keyword::Snowball => "SNOWBALL",
			Keyword::Spanish => "SPANISH",
			Keyword::Split => "SPLIT",
			Keyword::Start => "START",
			Keyword::Swedish => "SWEDISH",
			Keyword::Table => "TABLE",
			Keyword::Tamil => "TAMIL",
			Keyword::Then => "THEN",
			Keyword::Throw => "THROW",
			Keyword::Timeout => "TIMEOUT",
			Keyword::Tokeizers => "TOKEIZERS",
			Keyword::Transaction => "TRANSACTION",
			Keyword::True => "true",
			Keyword::Turkish => "TURKISH",
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
			Keyword::Int => "INT",
			Keyword::Number => "NUMBER",
			Keyword::Object => "OBJECT",
			Keyword::Point => "POINT",
			Keyword::String => "STRING",
			Keyword::Uuid => "UUID",
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
	/// `/`
	Divide,
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
			Operator::Divide => "'/'",
			Operator::Or => "'||'",
			Operator::And => "'&&'",
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
	Operator(Operator),
	OpenDelim(Delim),
	CloseDelim(Delim),
	Strand,
	/// A parameter like `$name`.
	Parameter,
	/// A duration.
	Duration {
		/// If the duration can be a valid identifier.
		/// The only time this is not the case is if the duration contains 'µs` suffix.
		valid_identifier: bool,
	},
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
				| TokenKind::Number
				| TokenKind::Keyword(_)
				| TokenKind::Duration {
					valid_identifier: true,
				}
		)
	}

	pub fn as_str(&self) -> &'static str {
		match *self {
			TokenKind::Keyword(x) => x.as_str(),
			TokenKind::Operator(x) => x.as_str(),
			TokenKind::OpenDelim(Delim::Paren) => "(",
			TokenKind::OpenDelim(Delim::Brace) => "{",
			TokenKind::OpenDelim(Delim::Bracket) => "[",
			TokenKind::CloseDelim(Delim::Paren) => ")",
			TokenKind::CloseDelim(Delim::Brace) => "}",
			TokenKind::CloseDelim(Delim::Bracket) => "]",
			TokenKind::Strand => "a strand",
			TokenKind::Parameter => "a parameter",
			TokenKind::Duration {
				..
			} => "a duration",
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
