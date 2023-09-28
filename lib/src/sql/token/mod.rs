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
	pub fn empty() -> Self {
		Span {
			offset: 0,
			len: 0,
		}
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
	Turkish,
	Type,
	Unique,
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
}

#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum Operator {
	/// '!'
	Not,
	/// `+`
	Add,
	/// `-`
	Subtract,
	/// `/`
	Divide,
	/// `||`
	Or,
	// '&&',
	And,
	/// `<=`
	LessEqual,
	/// `>=`
	GreaterEqual,
	/// `*`
	Star,
	/// `**`
	Power,
	/// '='
	Equal,
	/// '=='
	Exact,
	/// '!='
	NotEqual,
	/// '*='
	AllEqual,
	/// '?='
	AnyEqual,
	/// '~`
	Like,
	/// '!~`
	NotLike,
	/// '*~`
	AllLike,
	/// '?~`
	AnyLike,
	/// `∋`
	Contain,
	/// `∌`
	NotContain,
	/// `⊇`
	ContainAll,
	/// `⊃`
	ContainAny,
	/// `⊅`
	ContainNone,
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
pub enum Ambiguous {}

#[derive(Clone, Copy)]
pub enum Number {
	Unspecified,
	Integer,
	Float,
	Decimal,
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
	Duration,
	Number,
	Identifier,
	Record,
	/// `<`
	LeftChefron,
	/// `>`
	RightChefron,
	/// `*`
	Star,
	/// `?`
	Question,
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

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct Token {
	pub kind: TokenKind,
	pub span: Span,
	pub data_index: Option<DataIndex>,
}

impl Token {
	pub fn is_invalid(&self) -> bool {
		matches!(self.kind, TokenKind::Invalid)
	}

	pub fn is_eof(&self) -> bool {
		matches!(self.kind, TokenKind::Eof)
	}
}
