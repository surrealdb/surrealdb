//! Keywords.
//!
//! SurrealQL keywords are context sensitive: almost every keyword is also a
//! valid identifier, so the lexer never emits a keyword token. It emits an
//! identifier and tags it with the keyword it spells, compared ASCII case
//! insensitively. The parser decides per position whether the tag matters.
//!
//! The lookup table is an open-addressed hash table built by a `const fn`, so
//! it costs about a kilobyte of data and one small probe loop of code.
//!
//! The list is `surrealdb-syn`'s keyword list. `NaN` and `Infinity` are not
//! keywords: they are spelled case-sensitively and are not identifiers, so the
//! lexer gives them token kinds of their own.

macro_rules! keywords {
	($($name:ident = $text:literal,)*) => {
		/// A keyword an identifier token spells. `Kw::None` means the identifier
		/// is not a keyword.
		#[repr(u16)]
		#[derive(Clone, Copy, PartialEq, Eq, Debug)]
		pub enum Kw {
			None = 0,
			$($name,)*
		}

		/// The upper-case spelling of each keyword, indexed by `Kw as usize - 1`.
		pub(crate) const KW_TEXT: &[&str] = &[$($text,)*];

		impl Kw {
			const ALL: &[Kw] = &[$(Kw::$name,)*];
		}
	};
}

keywords! {
	Access = "ACCESS",
	After = "AFTER",
	Algorithm = "ALGORITHM",
	All = "ALL",
	AllInside = "ALLINSIDE",
	Alter = "ALTER",
	Always = "ALWAYS",
	Analyzer = "ANALYZER",
	And = "AND",
	AndKw = "ANDKW",
	Any = "ANY",
	AnyInside = "ANYINSIDE",
	Api = "API",
	Array = "ARRAY",
	As = "AS",
	Ascending = "ASCENDING",
	Ascii = "ASCII",
	Assert = "ASSERT",
	Async = "ASYNC",
	At = "AT",
	Audience = "AUDIENCE",
	Authenticate = "AUTHENTICATE",
	Auto = "AUTO",
	Backend = "BACKEND",
	Batch = "BATCH",
	Bearer = "BEARER",
	Before = "BEFORE",
	Begin = "BEGIN",
	Blank = "BLANK",
	Bm25 = "BM25",
	Bool = "BOOL",
	Break = "BREAK",
	Bucket = "BUCKET",
	By = "BY",
	Bytes = "BYTES",
	Camel = "CAMEL",
	Cancel = "CANCEL",
	Capacity = "CAPACITY",
	Cascade = "CASCADE",
	ChangeFeed = "CHANGEFEED",
	Changes = "CHANGES",
	Chinese = "CHINESE",
	Class = "CLASS",
	Collate = "COLLATE",
	Collection = "COLLECTION",
	Fields = "FIELDS",
	Comment = "COMMENT",
	Commit = "COMMIT",
	Compact = "COMPACT",
	Computed = "COMPUTED",
	Concurrently = "CONCURRENTLY",
	Config = "CONFIG",
	Contains = "CONTAINS",
	ContainsAll = "CONTAINSALL",
	ContainsAny = "CONTAINSANY",
	ContainsNone = "CONTAINSNONE",
	ContainsNot = "CONTAINSNOT",
	Content = "CONTENT",
	Context = "CONTEXT",
	Continue = "CONTINUE",
	Count = "COUNT",
	Create = "CREATE",
	Database = "DATABASE",
	Datetime = "DATETIME",
	Decimal = "DECIMAL",
	Default = "DEFAULT",
	Define = "DEFINE",
	Delete = "DELETE",
	Descending = "DESCENDING",
	Diff = "DIFF",
	Dimension = "DIMENSION",
	Distance = "DISTANCE",
	Drop = "DROP",
	Duplicate = "DUPLICATE",
	Duration = "DURATION",
	Edgengram = "EDGENGRAM",
	Edges = "EDGES",
	Efc = "EFC",
	Else = "ELSE",
	End = "END",
	Enforced = "ENFORCED",
	Event = "EVENT",
	Exclude = "EXCLUDE",
	Exists = "EXISTS",
	Expired = "EXPIRED",
	Explain = "EXPLAIN",
	Expunge = "EXPUNGE",
	ExtendCandidates = "EXTEND_CANDIDATES",
	False = "FALSE",
	Feature = "FEATURE",
	Fetch = "FETCH",
	Field = "FIELD",
	File = "FILE",
	Filters = "FILTERS",
	Flexible = "FLEXIBLE",
	Float = "FLOAT",
	Fn = "FN",
	For = "FOR",
	From = "FROM",
	Full = "FULL",
	Fulltext = "FULLTEXT",
	Function = "FUNCTION",
	Functions = "FUNCTIONS",
	Geometry = "GEOMETRY",
	Get = "GET",
	Grant = "GRANT",
	Graphql = "GRAPHQL",
	GraphqlAlias = "GRAPHQL_ALIAS",
	GraphqlDeprecated = "GRAPHQL_DEPRECATED",
	Group = "GROUP",
	HashedVector = "HASHED_VECTOR",
	Headers = "HEADERS",
	Highlights = "HIGHLIGHTS",
	Hnsw = "HNSW",
	If = "IF",
	Ignore = "IGNORE",
	In = "IN",
	Include = "INCLUDE",
	Index = "INDEX",
	Info = "INFO",
	Inline = "INLINE",
	Insert = "INSERT",
	Inside = "INSIDE",
	Int = "INT",
	Intersects = "INTERSECTS",
	Into = "INTO",
	Is = "IS",
	Issuer = "ISSUER",
	Japanese = "JAPANESE",
	Jwks = "JWKS",
	Jwt = "JWT",
	KeepPrunedConnections = "KEEP_PRUNED_CONNECTIONS",
	Key = "KEY",
	Kill = "KILL",
	Korean = "KOREAN",
	Root = "ROOT",
	Let = "LET",
	Lightweight = "LIGHTWEIGHT",
	Limit = "LIMIT",
	Line = "LINE",
	Live = "LIVE",
	Lm = "LM",
	Lowercase = "LOWERCASE",
	M = "M",
	M0 = "M0",
	Mapper = "MAPPER",
	MaxDepth = "MAXDEPTH",
	Merge = "MERGE",
	Middleware = "MIDDLEWARE",
	Ml = "ML",
	Mod = "MOD",
	Model = "MODEL",
	Module = "MODULE",
	MultiLine = "MULTILINE",
	MultiPoint = "MULTIPOINT",
	MultiPolygon = "MULTIPOLYGON",
	Namespace = "NAMESPACE",
	Ngram = "NGRAM",
	No = "NO",
	NoIndex = "NOINDEX",
	None_ = "NONE",
	NoneInside = "NONEINSIDE",
	Normal = "NORMAL",
	Not = "NOT",
	NotInside = "NOTINSIDE",
	Null = "NULL",
	Number = "NUMBER",
	Numeric = "NUMERIC",
	Object = "OBJECT",
	Omit = "OMIT",
	On = "ON",
	Only = "ONLY",
	Option = "OPTION",
	Or = "OR",
	Order = "ORDER",
	Original = "ORIGINAL",
	Out = "OUT",
	Outside = "OUTSIDE",
	Overwrite = "OVERWRITE",
	Parallel = "PARALLEL",
	Param = "PARAM",
	Passhash = "PASSHASH",
	Passscram = "PASSSCRAM",
	Password = "PASSWORD",
	Patch = "PATCH",
	Permissions = "PERMISSIONS",
	Point = "POINT",
	Polygon = "POLYGON",
	Post = "POST",
	PostingsCache = "POSTINGS_CACHE",
	PostingsOrder = "POSTINGS_ORDER",
	Prepare = "PREPARE",
	Punct = "PUNCT",
	Purge = "PURGE",
	Put = "PUT",
	Rand = "RAND",
	Range = "RANGE",
	Readonly = "READONLY",
	Rebuild = "REBUILD",
	Record = "RECORD",
	Reference = "REFERENCE",
	References = "REFERENCES",
	Refresh = "REFRESH",
	Regex = "REGEX",
	Reject = "REJECT",
	Relate = "RELATE",
	Relation = "RELATION",
	Remove = "REMOVE",
	Replace = "REPLACE",
	Retry = "RETRY",
	Return = "RETURN",
	Revoke = "REVOKE",
	Revoked = "REVOKED",
	Roles = "ROLES",
	Scope = "SC",
	Schemafull = "SCHEMAFULL",
	Schemaless = "SCHEMALESS",
	Segment = "SEGMENT",
	Select = "SELECT",
	Sequence = "SEQUENCE",
	Session = "SESSION",
	Set = "SET",
	Show = "SHOW",
	Signin = "SIGNIN",
	Signup = "SIGNUP",
	Silo = "SILO",
	Since = "SINCE",
	Sleep = "SLEEP",
	Snowball = "SNOWBALL",
	Split = "SPLIT",
	Start = "START",
	Strict = "STRICT",
	String = "STRING",
	Structure = "STRUCTURE",
	System = "SYSTEM",
	Table = "TABLE",
	Tables = "TABLES",
	Tb = "TB",
	Tempfiles = "TEMPFILES",
	TermsCache = "TERMS_CACHE",
	TermsOrder = "TERMS_ORDER",
	Then = "THEN",
	Throw = "THROW",
	Timeout = "TIMEOUT",
	To = "TO",
	Token = "TOKEN",
	Tokenizers = "TOKENIZERS",
	Trace = "TRACE",
	Transaction = "TRANSACTION",
	True = "TRUE",
	Type = "TYPE",
	Ulid = "ULID",
	Unique = "UNIQUE",
	Unset = "UNSET",
	Unsigned = "UNSIGNED",
	Update = "UPDATE",
	Uppercase = "UPPERCASE",
	Upsert = "UPSERT",
	Url = "URL",
	Use = "USE",
	User = "USER",
	Uuid = "UUID",
	Value = "VALUE",
	Values = "VALUES",
	Version = "VERSION",
	Vs = "VS",
	When = "WHEN",
	Where = "WHERE",
	With = "WITH",
}

/// Other spellings of keywords, in upper case.
const ALIASES: &[(&str, Kw)] = &[
	("ASC", Kw::Ascending),
	("COLUMNS", Kw::Fields),
	("DB", Kw::Database),
	("DESC", Kw::Descending),
	("DIST", Kw::Distance),
	("FLEX", Kw::Flexible),
	("FLEXI", Kw::Flexible),
	("KV", Kw::Root),
	("NS", Kw::Namespace),
	("SCHEMAFUL", Kw::Schemafull),
];

const TABLE_BITS: u32 = 9;
const TABLE_SIZE: usize = 1 << TABLE_BITS;
const MAX_KW_LEN: usize = 24;
const ENTRIES: usize = KW_TEXT.len() + ALIASES.len();

/// FNV-1a over the ASCII upper-cased bytes.
const fn hash(bytes: &[u8]) -> usize {
	let mut h: u32 = 0x811c_9dc5;
	let mut i = 0;
	while i < bytes.len() {
		h ^= bytes[i].to_ascii_uppercase() as u32;
		h = h.wrapping_mul(0x0100_0193);
		i += 1;
	}
	(h >> (32 - TABLE_BITS)) as usize
}

/// The spelling and keyword of entry `i`: keywords first, then aliases.
const fn entry(i: usize) -> (&'static str, Kw) {
	if i < KW_TEXT.len() {
		(KW_TEXT[i], Kw::ALL[i])
	} else {
		ALIASES[i - KW_TEXT.len()]
	}
}

const _: () = assert!(ENTRIES * 3 < TABLE_SIZE * 2, "keyword table too full");

const fn build() -> [u16; TABLE_SIZE] {
	let mut table = [0u16; TABLE_SIZE];
	let mut i = 0;
	while i < ENTRIES {
		let text = entry(i).0.as_bytes();
		assert!(text.len() <= MAX_KW_LEN);
		let mut slot = hash(text);
		while table[slot] != 0 {
			slot = (slot + 1) & (TABLE_SIZE - 1);
		}
		table[slot] = (i + 1) as u16;
		i += 1;
	}
	table
}

static TABLE: [u16; TABLE_SIZE] = build();

/// Returns the keyword an identifier spells, or `Kw::None`.
pub fn lookup(ident: &[u8]) -> Kw {
	if ident.len() > MAX_KW_LEN {
		return Kw::None;
	}
	let mut slot = hash(ident);
	loop {
		let e = TABLE[slot];
		if e == 0 {
			return Kw::None;
		}
		let (text, kw) = entry(e as usize - 1);
		if text.as_bytes().eq_ignore_ascii_case(ident) {
			return kw;
		}
		slot = (slot + 1) & (TABLE_SIZE - 1);
	}
}

impl Kw {
	/// The keyword's canonical upper-case spelling.
	pub fn text(self) -> &'static str {
		match self {
			Kw::None => "",
			k => KW_TEXT[k as usize - 1],
		}
	}
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn every_keyword_round_trips() {
		for (i, text) in KW_TEXT.iter().enumerate() {
			assert_eq!(lookup(text.as_bytes()), Kw::ALL[i]);
			assert_eq!(lookup(text.to_ascii_lowercase().as_bytes()), Kw::ALL[i]);
		}
		for (text, kw) in ALIASES {
			assert_eq!(lookup(text.as_bytes()), *kw);
		}
		assert_eq!(lookup(b"selects"), Kw::None);
		assert_eq!(lookup(b"x"), Kw::None);
		assert_eq!(lookup(b"m"), Kw::M);
	}
}
