macro_rules! keyword {
	($($name:ident => $value:tt),* $(,)?) => {

		#[repr(u8)]
		#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
		pub enum Keyword{
			$($name,)*
		}

		impl Keyword{
			pub fn as_str(&self) -> &'static str{
				match self{
					$(Keyword::$name => $value,)*
				}
			}
		}

		macro_rules! keyword_t {
			$(($value) => {
				$crate::syn::v2::token::Keyword::$name
			};)*
		}
	};
}

keyword! {
	After => "AFTER",
	All => "ALL",
	Analyze => "ANALYZE",
	Analyzer => "ANALYZER",
	As => "AS",
	Ascending => "ASCENDING",
	Ascii => "ASCII",
	Assert => "ASSERT",
	At => "AT",
	Before => "BEFORE",
	Begin => "BEGIN",
	Blank => "BLANK",
	Bm25 => "BM25",
	Break => "BREAK",
	By => "BY",
	Camel => "CAMEL",
	Cancel => "CANCEL",
	ChangeFeed => "CHANGEFEED",
	Changes => "CHANGES",
	Capacity => "CAPACITY",
	Class => "CLASS",
	Comment => "COMMENT",
	Commit => "COMMIT",
	Content => "CONTENT",
	Continue => "CONTINUE",
	Create => "CREATE",
	Database => "DATABASE",
	Default => "DEFAULT",
	Define => "DEFINE",
	Delete => "DELETE",
	Descending => "DESCENDING",
	Diff => "DIFF",
	Dimension => "DIMENSION",
	Distance => "DISTANCE",
	DocIdsCache => "DOC_IDS_CACHE",
	DocIdsOrder => "DOC_IDS_ORDER",
	DocLengthsCache => "DOC_LENGTHS_CACHE",
	DocLengthsOrder => "DOC_LENGTHS_ORDER",
	Drop => "DROP",
	Duplicate => "DUPLICATE",
	Edgengram => "EDGENGRAM",
	Event => "EVENT",
	Else => "ELSE",
	End => "END",
	Exists => "EXISTS",
	Explain => "EXPLAIN",
	False => "false",
	Fetch => "FETCH",
	Field => "FIELD",
	Fields => "FIELDS",
	Filters => "FILTERS",
	Flexible => "FLEXIBLE",
	For => "FOR",
	From => "FROM",
	Full => "FULL",
	Function => "FUNCTION",
	Group => "GROUP",
	Highlights => "HIGHLIGHTS",
	Ignore => "IGNORE",
	Index => "INDEX",
	Info => "INFO",
	Insert => "INSERT",
	Into => "INTO",
	If => "IF",
	Is => "IS",
	Key => "KEY",
	Kill => "KILL",
	Knn => "KNN",
	Let => "LET",
	Limit => "LIMIT",
	Live => "LIVE",
	Lowercase => "LOWERCASE",
	Merge => "MERGE",
	Model => "MODEL",
	MTree => "MTREE",
	MTreeCache => "MTREE_CACHE",
	Namespace => "NAMESPACE",
	Ngram => "NGRAM",
	No => "NO",
	NoIndex => "NOINDEX",
	None => "NONE",
	Null => "NULL",
	Numeric => "NUMERIC",
	Omit => "OMIT",
	On => "ON",
	Only => "ONLY",
	Option => "OPTION",
	Order => "ORDER",
	Parallel => "PARALLEL",
	Param => "PARAM",
	Passhash => "PASSHASH",
	Password => "PASSWORD",
	Patch => "PATCH",
	Permissions => "PERMISSIONS",
	PostingsCache => "POSTINGS_CACHE",
	PostingsOrder => "POSTINGS_ORDER",
	Punct => "PUNCT",
	Readonly => "READONLY",
	Relate => "RELATE",
	Relation => "RELATION",
	Remove => "REMOVE",
	Replace => "REPLACE",
	Return => "RETURN",
	Roles => "ROLES",
	Root => "ROOT",
	Schemafull => "SCHEMAFULL",
	Schemaless => "SCHEMALESS",
	Scope => "SCOPE",
	Search => "SEARCH",
	Select => "SELECT",
	Session => "SESSION",
	Set => "SET",
	Show => "SHOW",
	Signin => "SIGNIN",
	Signup => "SIGNUP",
	Since => "SINCE",
	Sleep => "SLEEP",
	Snowball => "SNOWBALL",
	Split => "SPLIT",
	Start => "START",
	Table => "TABLE",
	TermsCache => "TERMS_CACHE",
	TermsOrder => "TERMS_ORDER",
	Then => "THEN",
	Throw => "THROW",
	Timeout => "TIMEOUT",
	Tokenizers => "TOKENIZERS",
	Token => "TOKEN",
	To => "TO",
	Transaction => "TRANSACTION",
	True => "true",
	Type => "TYPE",
	Unique => "UNIQUE",
	Unset => "UNSET",
	Update => "UPDATE",
	Uppercase => "UPPERCASE",
	Use => "USE",
	User => "USER",
	Value => "VALUE",
	Values => "VALUES",
	Version => "VERSION",
	Vs => "VS",
	When => "WHEN",
	Where => "WHERE",
	With => "WITH",
	AllInside => "ALLINSIDE",
	AndKw => "ANDKW",
	AnyInside => "ANYINSIDE",
	Inside => "INSIDE",
	Intersects => "INTERSECTS",
	NoneInside => "NONEINSIDE",
	NotInside => "NOTINSIDE",
	OrKw => "OR",
	Outside => "OUTSIDE",
	Not => "NOT",
	And => "AND",
	Collate => "COLLATE",
	ContainsAll => "CONTAINSALL",
	ContainsAny => "CONTAINSANY",
	ContainsNone => "CONTAINSNONE",
	ContainsNot => "CONTAINSNOT",
	Contains => "CONTAINS",
	In => "IN",

	Any => "ANY",
	Array => "ARRAY",
	Geometry => "GEOMETRY",
	Record => "RECORD",
	Future => "FUTURE",
	Bool => "BOOL",
	Bytes => "BYTES",
	Datetime => "DATETIME",
	Decimal => "DECIMAL",
	Duration => "DURATION",
	Float => "FLOAT",
	Fn => "fn",
	Int => "INT",
	Number => "NUMBER",
	Object => "OBJECT",
	String => "STRING",
	Uuid => "UUID",
	Ulid => "ULID",
	Rand => "RAND",
	Feature => "FEATURE",
	Line => "LINE",
	Point => "POINT",
	Polygon => "POLYGON",
	MultiPoint => "MULTIPOINT",
	MultiLine => "MULTILINE",
	MultiPolygon => "MULTIPOLYGON",
	Collection => "COLLECTION",

	FN => "fn",
	ML => "ml",
}

pub(crate) use keyword_t;
