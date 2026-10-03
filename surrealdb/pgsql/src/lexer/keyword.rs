//! PostgreSQL keywords and classification according to `kwlist.h`.

use std::fmt;

/// Category of PostgreSQL keyword determining where it can appear unquoted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeywordCategory {
	/// Reserved in all contexts: cannot be used as column name or table name without quoting.
	Reserved,
	/// Non-reserved: can be used as an identifier in any context.
	NonReserved,
	/// Can be used as a column or attribute name without quoting.
	ColName,
	/// Can be used as a type or function name without quoting.
	TypeFuncName,
}

/// Enumeration of PostgreSQL keywords (PostgreSQL 17/18/19).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Keyword {
	// --- Reserved Keywords ---
	All,
	Analyse,
	Analyze,
	And,
	Any,
	Array,
	As,
	Asc,
	Asymmetric,
	Authorization,
	Binary,
	Both,
	Case,
	Cast,
	Check,
	Collate,
	Collation,
	Column,
	Concurrently,
	Constraint,
	Create,
	Cross,
	CurrentCatalog,
	CurrentDate,
	CurrentRole,
	CurrentSchema,
	CurrentTime,
	CurrentTimestamp,
	CurrentUser,
	Default,
	Deferrable,
	Desc,
	Distinct,
	Do,
	Else,
	End,
	Except,
	False,
	Fetch,
	For,
	Foreign,
	Freeze,
	From,
	Full,
	Grant,
	Group,
	Having,
	Ilike,
	In,
	Initially,
	Inner,
	Intersect,
	Into,
	Is,
	Isnull,
	Join,
	Lateral,
	Leading,
	Left,
	Like,
	Limit,
	Localtime,
	Localtimestamp,
	Natural,
	Not,
	Notnull,
	Null,
	On,
	Only,
	Or,
	Order,
	Outer,
	Overlaps,
	Placing,
	Primary,
	References,
	Returning,
	Right,
	Select,
	SessionUser,
	Similar,
	Some,
	Symmetric,
	Table,
	Tablesample,
	Then,
	To,
	Trailing,
	True,
	Union,
	Unique,
	User,
	Using,
	Variadic,
	Verbose,
	When,
	Where,
	Window,
	With,

	// --- Column Name Keywords ---
	Bigint,
	Bit,
	Boolean,
	Char,
	Character,
	Coalesce,
	Dec,
	Decimal,
	Exists,
	Extract,
	Float,
	Greatest,
	Grouping,
	Inout,
	Int,
	Integer,
	Interval,
	Least,
	National,
	Nchar,
	None,
	Nullif,
	Numeric,
	Out,
	Overlay,
	Position,
	Precision,
	Real,
	Row,
	Setof,
	Smallint,
	Substring,
	Time,
	Timestamp,
	Treat,
	Trim,
	Values,
	Varchar,
	Xmlattributes,
	Xmlconcat,
	Xmlelement,
	Xmlexists,
	Xmlforest,
	Xmlnamespaces,
	Xmlparse,
	Xmlpi,
	Xmlroot,
	Xmlserialize,
	Xmltable,

	// --- Non-Reserved Keywords (common subset) ---
	Abort,
	Absolute,
	Access,
	Action,
	Add,
	Admin,
	After,
	Aggregate,
	Also,
	Alter,
	Always,
	Assertion,
	Assignment,
	At,
	Attach,
	Attribute,
	Backward,
	Before,
	Begin,
	Between,
	By,
	Cache,
	Call,
	Cascade,
	Cascaded,
	Catalog,
	Chain,
	Characteristics,
	Checkpoint,
	Close,
	Cluster,
	Comment,
	Commit,
	Committed,
	Configuration,
	Conflict,
	Connection,
	Constraints,
	Content,
	Continue,
	Conversion,
	Copy,
	Cost,
	Csv,
	Cube,
	Current,
	Cursor,
	Cycle,
	Data,
	Database,
	Day,
	Deallocate,
	Declare,
	Defaults,
	Deferred,
	Definer,
	Delete,
	Delimiters,
	Depends,
	Detach,
	Dictionary,
	Disable,
	Discard,
	Document,
	Drop,
	Each,
	Enable,
	Encoding,
	Encrypted,
	Enum,
	Escape,
	Event,
	Exclude,
	Excluding,
	Exclusive,
	Execute,
	Explain,
	Extension,
	External,
	Family,
	Filter,
	Finalize,
	First,
	Following,
	Force,
	Forward,
	Function,
	Functions,
	Generated,
	Global,
	Granted,
	Groups,
	Handler,
	Header,
	Hold,
	Hour,
	Identity,
	If,
	Immediate,
	Immutable,
	Implicit,
	Import,
	Include,
	Including,
	Increment,
	Index,
	Indexes,
	Inherit,
	Inherits,
	Inline,
	Insensitive,
	Insert,
	Instead,
	Invoker,
	Isolation,
	Key,
	Label,
	Language,
	Large,
	Last,
	Leakproof,
	Level,
	Listen,
	Load,
	Local,
	Location,
	Lock,
	Locked,
	Logged,
	Mapping,
	Match,
	Matched,
	Materialized,
	Maxvalue,
	Merge,
	Method,
	Minute,
	Minvalue,
	Mode,
	Month,
	Move,
	Name,
	Names,
	Next,
	No,
	Nothing,
	Notify,
	Nowait,
	Nulls,
	Object,
	Of,
	Off,
	Offset,
	Oids,
	Operator,
	Option,
	Options,
	Ordinality,
	Others,
	Over,
	Overriding,
	Owned,
	Owner,
	Parallel,
	Parser,
	Partial,
	Partition,
	Passing,
	Password,
	Plans,
	Policy,
	Preceding,
	Prepare,
	Prepared,
	Preserve,
	Prior,
	Privileges,
	Procedural,
	Procedure,
	Procedures,
	Program,
	Publication,
	Quote,
	Range,
	Read,
	Reassign,
	Recheck,
	Recursive,
	Ref,
	Refresh,
	Reindex,
	Relative,
	Release,
	Rename,
	Replace,
	Replica,
	Reset,
	Restart,
	Restrict,
	Returns,
	Revoke,
	Role,
	Rollback,
	Rollup,
	Routine,
	Routines,
	Rows,
	Rule,
	Savepoint,
	Schema,
	Schemas,
	Scroll,
	Search,
	Second,
	Security,
	Sequence,
	Sequences,
	Serializable,
	Server,
	Session,
	Set,
	Sets,
	Share,
	Show,
	Simple,
	Skip,
	Snapshot,
	Sql,
	Stable,
	Standalone,
	Start,
	Statement,
	Statistics,
	Stdin,
	Stdout,
	Storage,
	Stored,
	Strict,
	Strip,
	Subscription,
	Support,
	Sysid,
	System,
	Tables,
	Tablespace,
	Target,
	Temp,
	Template,
	Temporary,
	Text,
	Ties,
	Transaction,
	Transform,
	Trigger,
	Truncate,
	Trusted,
	Type,
	Types,
	Unbounded,
	Uncommitted,
	Unencrypted,
	Unknown,
	Unlisten,
	Unlogged,
	Until,
	Update,
	Vacuum,
	Valid,
	Validate,
	Validator,
	Value,
	Version,
	View,
	Views,
	Volatile,
	Whitespace,
	Without,
	Work,
	Wrapper,
	Write,
	Xml,
	Year,
	Zone,
}

impl Keyword {
	/// Return the keyword category.
	pub const fn category(&self) -> KeywordCategory {
		match self {
			Self::All
			| Self::Analyse
			| Self::Analyze
			| Self::And
			| Self::Any
			| Self::Array
			| Self::As
			| Self::Asc
			| Self::Asymmetric
			| Self::Authorization
			| Self::Binary
			| Self::Both
			| Self::Case
			| Self::Cast
			| Self::Check
			| Self::Collate
			| Self::Collation
			| Self::Column
			| Self::Concurrently
			| Self::Constraint
			| Self::Create
			| Self::Cross
			| Self::CurrentCatalog
			| Self::CurrentDate
			| Self::CurrentRole
			| Self::CurrentSchema
			| Self::CurrentTime
			| Self::CurrentTimestamp
			| Self::CurrentUser
			| Self::Default
			| Self::Deferrable
			| Self::Desc
			| Self::Distinct
			| Self::Do
			| Self::Else
			| Self::End
			| Self::Except
			| Self::False
			| Self::Fetch
			| Self::For
			| Self::Foreign
			| Self::Freeze
			| Self::From
			| Self::Full
			| Self::Grant
			| Self::Group
			| Self::Having
			| Self::Ilike
			| Self::In
			| Self::Initially
			| Self::Inner
			| Self::Intersect
			| Self::Into
			| Self::Is
			| Self::Isnull
			| Self::Join
			| Self::Lateral
			| Self::Leading
			| Self::Left
			| Self::Like
			| Self::Limit
			| Self::Localtime
			| Self::Localtimestamp
			| Self::Natural
			| Self::Not
			| Self::Notnull
			| Self::Null
			| Self::On
			| Self::Only
			| Self::Or
			| Self::Order
			| Self::Outer
			| Self::Overlaps
			| Self::Placing
			| Self::Primary
			| Self::References
			| Self::Returning
			| Self::Right
			| Self::Select
			| Self::SessionUser
			| Self::Similar
			| Self::Some
			| Self::Symmetric
			| Self::Table
			| Self::Tablesample
			| Self::Then
			| Self::To
			| Self::Trailing
			| Self::True
			| Self::Union
			| Self::Unique
			| Self::User
			| Self::Using
			| Self::Variadic
			| Self::Verbose
			| Self::When
			| Self::Where
			| Self::Window
			| Self::With => KeywordCategory::Reserved,

			Self::Bigint
			| Self::Bit
			| Self::Boolean
			| Self::Char
			| Self::Character
			| Self::Coalesce
			| Self::Dec
			| Self::Decimal
			| Self::Exists
			| Self::Extract
			| Self::Float
			| Self::Greatest
			| Self::Grouping
			| Self::Inout
			| Self::Int
			| Self::Integer
			| Self::Interval
			| Self::Least
			| Self::National
			| Self::Nchar
			| Self::None
			| Self::Nullif
			| Self::Numeric
			| Self::Out
			| Self::Overlay
			| Self::Position
			| Self::Precision
			| Self::Real
			| Self::Row
			| Self::Setof
			| Self::Smallint
			| Self::Substring
			| Self::Time
			| Self::Timestamp
			| Self::Treat
			| Self::Trim
			| Self::Values
			| Self::Varchar
			| Self::Xmlattributes
			| Self::Xmlconcat
			| Self::Xmlelement
			| Self::Xmlexists
			| Self::Xmlforest
			| Self::Xmlnamespaces
			| Self::Xmlparse
			| Self::Xmlpi
			| Self::Xmlroot
			| Self::Xmlserialize
			| Self::Xmltable => KeywordCategory::ColName,

			_ => KeywordCategory::NonReserved,
		}
	}

	/// Return the uppercase canonical representation of this keyword.
	pub const fn as_str(&self) -> &'static str {
		match self {
			Self::All => "ALL",
			Self::Analyse => "ANALYSE",
			Self::Analyze => "ANALYZE",
			Self::And => "AND",
			Self::Any => "ANY",
			Self::Array => "ARRAY",
			Self::As => "AS",
			Self::Asc => "ASC",
			Self::Asymmetric => "ASYMMETRIC",
			Self::Authorization => "AUTHORIZATION",
			Self::Binary => "BINARY",
			Self::Both => "BOTH",
			Self::Case => "CASE",
			Self::Cast => "CAST",
			Self::Check => "CHECK",
			Self::Collate => "COLLATE",
			Self::Collation => "COLLATION",
			Self::Column => "COLUMN",
			Self::Concurrently => "CONCURRENTLY",
			Self::Constraint => "CONSTRAINT",
			Self::Create => "CREATE",
			Self::Cross => "CROSS",
			Self::CurrentCatalog => "CURRENT_CATALOG",
			Self::CurrentDate => "CURRENT_DATE",
			Self::CurrentRole => "CURRENT_ROLE",
			Self::CurrentSchema => "CURRENT_SCHEMA",
			Self::CurrentTime => "CURRENT_TIME",
			Self::CurrentTimestamp => "CURRENT_TIMESTAMP",
			Self::CurrentUser => "CURRENT_USER",
			Self::Default => "DEFAULT",
			Self::Deferrable => "DEFERRABLE",
			Self::Desc => "DESC",
			Self::Distinct => "DISTINCT",
			Self::Do => "DO",
			Self::Else => "ELSE",
			Self::End => "END",
			Self::Except => "EXCEPT",
			Self::False => "FALSE",
			Self::Fetch => "FETCH",
			Self::For => "FOR",
			Self::Foreign => "FOREIGN",
			Self::Freeze => "FREEZE",
			Self::From => "FROM",
			Self::Full => "FULL",
			Self::Grant => "GRANT",
			Self::Group => "GROUP",
			Self::Having => "HAVING",
			Self::Ilike => "ILIKE",
			Self::In => "IN",
			Self::Initially => "INITIALLY",
			Self::Inner => "INNER",
			Self::Intersect => "INTERSECT",
			Self::Into => "INTO",
			Self::Is => "IS",
			Self::Isnull => "ISNULL",
			Self::Join => "JOIN",
			Self::Lateral => "LATERAL",
			Self::Leading => "LEADING",
			Self::Left => "LEFT",
			Self::Like => "LIKE",
			Self::Limit => "LIMIT",
			Self::Localtime => "LOCALTIME",
			Self::Localtimestamp => "LOCALTIMESTAMP",
			Self::Natural => "NATURAL",
			Self::Not => "NOT",
			Self::Notnull => "NOTNULL",
			Self::Null => "NULL",
			Self::On => "ON",
			Self::Only => "ONLY",
			Self::Or => "OR",
			Self::Order => "ORDER",
			Self::Outer => "OUTER",
			Self::Overlaps => "OVERLAPS",
			Self::Placing => "PLACING",
			Self::Primary => "PRIMARY",
			Self::References => "REFERENCES",
			Self::Returning => "RETURNING",
			Self::Right => "RIGHT",
			Self::Select => "SELECT",
			Self::SessionUser => "SESSION_USER",
			Self::Similar => "SIMILAR",
			Self::Some => "SOME",
			Self::Symmetric => "SYMMETRIC",
			Self::Table => "TABLE",
			Self::Tablesample => "TABLESAMPLE",
			Self::Then => "THEN",
			Self::To => "TO",
			Self::Trailing => "TRAILING",
			Self::True => "TRUE",
			Self::Union => "UNION",
			Self::Unique => "UNIQUE",
			Self::User => "USER",
			Self::Using => "USING",
			Self::Variadic => "VARIADIC",
			Self::Verbose => "VERBOSE",
			Self::When => "WHEN",
			Self::Where => "WHERE",
			Self::Window => "WINDOW",
			Self::With => "WITH",

			Self::Bigint => "BIGINT",
			Self::Bit => "BIT",
			Self::Boolean => "BOOLEAN",
			Self::Char => "CHAR",
			Self::Character => "CHARACTER",
			Self::Coalesce => "COALESCE",
			Self::Dec => "DEC",
			Self::Decimal => "DECIMAL",
			Self::Exists => "EXISTS",
			Self::Extract => "EXTRACT",
			Self::Float => "FLOAT",
			Self::Greatest => "GREATEST",
			Self::Grouping => "GROUPING",
			Self::Inout => "INOUT",
			Self::Int => "INT",
			Self::Integer => "INTEGER",
			Self::Interval => "INTERVAL",
			Self::Least => "LEAST",
			Self::National => "NATIONAL",
			Self::Nchar => "NCHAR",
			Self::None => "NONE",
			Self::Nullif => "NULLIF",
			Self::Numeric => "NUMERIC",
			Self::Out => "OUT",
			Self::Overlay => "OVERLAY",
			Self::Position => "POSITION",
			Self::Precision => "PRECISION",
			Self::Real => "REAL",
			Self::Row => "ROW",
			Self::Setof => "SETOF",
			Self::Smallint => "SMALLINT",
			Self::Substring => "SUBSTRING",
			Self::Time => "TIME",
			Self::Timestamp => "TIMESTAMP",
			Self::Treat => "TREAT",
			Self::Trim => "TRIM",
			Self::Values => "VALUES",
			Self::Varchar => "VARCHAR",
			Self::Xmlattributes => "XMLATTRIBUTES",
			Self::Xmlconcat => "XMLCONCAT",
			Self::Xmlelement => "XMLELEMENT",
			Self::Xmlexists => "XMLEXISTS",
			Self::Xmlforest => "XMLFOREST",
			Self::Xmlnamespaces => "XMLNAMESPACES",
			Self::Xmlparse => "XMLPARSE",
			Self::Xmlpi => "XMLPI",
			Self::Xmlroot => "XMLROOT",
			Self::Xmlserialize => "XMLSERIALIZE",
			Self::Xmltable => "XMLTABLE",

			Self::Abort => "ABORT",
			Self::Absolute => "ABSOLUTE",
			Self::Access => "ACCESS",
			Self::Action => "ACTION",
			Self::Add => "ADD",
			Self::Admin => "ADMIN",
			Self::After => "AFTER",
			Self::Aggregate => "AGGREGATE",
			Self::Also => "ALSO",
			Self::Alter => "ALTER",
			Self::Always => "ALWAYS",
			Self::Assertion => "ASSERTION",
			Self::Assignment => "ASSIGNMENT",
			Self::At => "AT",
			Self::Attach => "ATTACH",
			Self::Attribute => "ATTRIBUTE",
			Self::Backward => "BACKWARD",
			Self::Before => "BEFORE",
			Self::Begin => "BEGIN",
			Self::Between => "BETWEEN",
			Self::By => "BY",
			Self::Cache => "CACHE",
			Self::Call => "CALL",
			Self::Cascade => "CASCADE",
			Self::Cascaded => "CASCADED",
			Self::Catalog => "CATALOG",
			Self::Chain => "CHAIN",
			Self::Characteristics => "CHARACTERISTICS",
			Self::Checkpoint => "CHECKPOINT",
			Self::Close => "CLOSE",
			Self::Cluster => "CLUSTER",
			Self::Comment => "COMMENT",
			Self::Commit => "COMMIT",
			Self::Committed => "COMMITTED",
			Self::Configuration => "CONFIGURATION",
			Self::Conflict => "CONFLICT",
			Self::Connection => "CONNECTION",
			Self::Constraints => "CONSTRAINTS",
			Self::Content => "CONTENT",
			Self::Continue => "CONTINUE",
			Self::Conversion => "CONVERSION",
			Self::Copy => "COPY",
			Self::Cost => "COST",
			Self::Csv => "CSV",
			Self::Cube => "CUBE",
			Self::Current => "CURRENT",
			Self::Cursor => "CURSOR",
			Self::Cycle => "CYCLE",
			Self::Data => "DATA",
			Self::Database => "DATABASE",
			Self::Day => "DAY",
			Self::Deallocate => "DEALLOCATE",
			Self::Declare => "DECLARE",
			Self::Defaults => "DEFAULTS",
			Self::Deferred => "DEFERRED",
			Self::Definer => "DEFINER",
			Self::Delete => "DELETE",
			Self::Delimiters => "DELIMITERS",
			Self::Depends => "DEPENDS",
			Self::Detach => "DETACH",
			Self::Dictionary => "DICTIONARY",
			Self::Disable => "DISABLE",
			Self::Discard => "DISCARD",
			Self::Document => "DOCUMENT",
			Self::Drop => "DROP",
			Self::Each => "EACH",
			Self::Enable => "ENABLE",
			Self::Encoding => "ENCODING",
			Self::Encrypted => "ENCRYPTED",
			Self::Enum => "ENUM",
			Self::Escape => "ESCAPE",
			Self::Event => "EVENT",
			Self::Exclude => "EXCLUDE",
			Self::Excluding => "EXCLUDING",
			Self::Exclusive => "EXCLUSIVE",
			Self::Execute => "EXECUTE",
			Self::Explain => "EXPLAIN",
			Self::Extension => "EXTENSION",
			Self::External => "EXTERNAL",
			Self::Family => "FAMILY",
			Self::Filter => "FILTER",
			Self::Finalize => "FINALIZE",
			Self::First => "FIRST",
			Self::Following => "FOLLOWING",
			Self::Force => "FORCE",
			Self::Forward => "FORWARD",
			Self::Function => "FUNCTION",
			Self::Functions => "FUNCTIONS",
			Self::Generated => "GENERATED",
			Self::Global => "GLOBAL",
			Self::Granted => "GRANTED",
			Self::Groups => "GROUPS",
			Self::Handler => "HANDLER",
			Self::Header => "HEADER",
			Self::Hold => "HOLD",
			Self::Hour => "HOUR",
			Self::Identity => "IDENTITY",
			Self::If => "IF",
			Self::Immediate => "IMMEDIATE",
			Self::Immutable => "IMMUTABLE",
			Self::Implicit => "IMPLICIT",
			Self::Import => "IMPORT",
			Self::Include => "INCLUDE",
			Self::Including => "INCLUDING",
			Self::Increment => "INCREMENT",
			Self::Index => "INDEX",
			Self::Indexes => "INDEXES",
			Self::Inherit => "INHERIT",
			Self::Inherits => "INHERITS",
			Self::Inline => "INLINE",
			Self::Insensitive => "INSENSITIVE",
			Self::Insert => "INSERT",
			Self::Instead => "INSTEAD",
			Self::Invoker => "INVOKER",
			Self::Isolation => "ISOLATION",
			Self::Key => "KEY",
			Self::Label => "LABEL",
			Self::Language => "LANGUAGE",
			Self::Large => "LARGE",
			Self::Last => "LAST",
			Self::Leakproof => "LEAKPROOF",
			Self::Level => "LEVEL",
			Self::Listen => "LISTEN",
			Self::Load => "LOAD",
			Self::Local => "LOCAL",
			Self::Location => "LOCATION",
			Self::Lock => "LOCK",
			Self::Locked => "LOCKED",
			Self::Logged => "LOGGED",
			Self::Mapping => "MAPPING",
			Self::Match => "MATCH",
			Self::Matched => "MATCHED",
			Self::Materialized => "MATERIALIZED",
			Self::Maxvalue => "MAXVALUE",
			Self::Merge => "MERGE",
			Self::Method => "METHOD",
			Self::Minute => "MINUTE",
			Self::Minvalue => "MINVALUE",
			Self::Mode => "MODE",
			Self::Month => "MONTH",
			Self::Move => "MOVE",
			Self::Name => "NAME",
			Self::Names => "NAMES",
			Self::Next => "NEXT",
			Self::No => "NO",
			Self::Nothing => "NOTHING",
			Self::Notify => "NOTIFY",
			Self::Nowait => "NOWAIT",
			Self::Nulls => "NULLS",
			Self::Object => "OBJECT",
			Self::Of => "OF",
			Self::Off => "OFF",
			Self::Offset => "OFFSET",
			Self::Oids => "OIDS",
			Self::Operator => "OPERATOR",
			Self::Option => "OPTION",
			Self::Options => "OPTIONS",
			Self::Ordinality => "ORDINALITY",
			Self::Others => "OTHERS",
			Self::Over => "OVER",
			Self::Overriding => "OVERRIDING",
			Self::Owned => "OWNED",
			Self::Owner => "OWNER",
			Self::Parallel => "PARALLEL",
			Self::Parser => "PARSER",
			Self::Partial => "PARTIAL",
			Self::Partition => "PARTITION",
			Self::Passing => "PASSING",
			Self::Password => "PASSWORD",
			Self::Plans => "PLANS",
			Self::Policy => "POLICY",
			Self::Preceding => "PRECEDING",
			Self::Prepare => "PREPARE",
			Self::Prepared => "PREPARED",
			Self::Preserve => "PRESERVE",
			Self::Prior => "PRIOR",
			Self::Privileges => "PRIVILEGES",
			Self::Procedural => "PROCEDURAL",
			Self::Procedure => "PROCEDURE",
			Self::Procedures => "PROCEDURES",
			Self::Program => "PROGRAM",
			Self::Publication => "PUBLICATION",
			Self::Quote => "QUOTE",
			Self::Range => "RANGE",
			Self::Read => "READ",
			Self::Reassign => "REASSIGN",
			Self::Recheck => "RECHECK",
			Self::Recursive => "RECURSIVE",
			Self::Ref => "REF",
			Self::Refresh => "REFRESH",
			Self::Reindex => "REINDEX",
			Self::Relative => "RELATIVE",
			Self::Release => "RELEASE",
			Self::Rename => "RENAME",
			Self::Replace => "REPLACE",
			Self::Replica => "REPLICA",
			Self::Reset => "RESET",
			Self::Restart => "RESTART",
			Self::Restrict => "RESTRICT",
			Self::Returns => "RETURNS",
			Self::Revoke => "REVOKE",
			Self::Role => "ROLE",
			Self::Rollback => "ROLLBACK",
			Self::Rollup => "ROLLUP",
			Self::Routine => "ROUTINE",
			Self::Routines => "ROUTINES",
			Self::Rows => "ROWS",
			Self::Rule => "RULE",
			Self::Savepoint => "SAVEPOINT",
			Self::Schema => "SCHEMA",
			Self::Schemas => "SCHEMAS",
			Self::Scroll => "SCROLL",
			Self::Search => "SEARCH",
			Self::Second => "SECOND",
			Self::Security => "SECURITY",
			Self::Sequence => "SEQUENCE",
			Self::Sequences => "SEQUENCES",
			Self::Serializable => "SERIALIZABLE",
			Self::Server => "SERVER",
			Self::Session => "SESSION",
			Self::Set => "SET",
			Self::Sets => "SETS",
			Self::Share => "SHARE",
			Self::Show => "SHOW",
			Self::Simple => "SIMPLE",
			Self::Skip => "SKIP",
			Self::Snapshot => "SNAPSHOT",
			Self::Sql => "SQL",
			Self::Stable => "STABLE",
			Self::Standalone => "STANDALONE",
			Self::Start => "START",
			Self::Statement => "STATEMENT",
			Self::Statistics => "STATISTICS",
			Self::Stdin => "STDIN",
			Self::Stdout => "STDOUT",
			Self::Storage => "STORAGE",
			Self::Stored => "STORED",
			Self::Strict => "STRICT",
			Self::Strip => "STRIP",
			Self::Subscription => "SUBSCRIPTION",
			Self::Support => "SUPPORT",
			Self::Sysid => "SYSID",
			Self::System => "SYSTEM",
			Self::Tables => "TABLES",
			Self::Tablespace => "TABLESPACE",
			Self::Target => "TARGET",
			Self::Temp => "TEMP",
			Self::Template => "TEMPLATE",
			Self::Temporary => "TEMPORARY",
			Self::Text => "TEXT",
			Self::Ties => "TIES",
			Self::Transaction => "TRANSACTION",
			Self::Transform => "TRANSFORM",
			Self::Trigger => "TRIGGER",
			Self::Truncate => "TRUNCATE",
			Self::Trusted => "TRUSTED",
			Self::Type => "TYPE",
			Self::Types => "TYPES",
			Self::Unbounded => "UNBOUNDED",
			Self::Uncommitted => "UNCOMMITTED",
			Self::Unencrypted => "UNENCRYPTED",
			Self::Unknown => "UNKNOWN",
			Self::Unlisten => "UNLISTEN",
			Self::Unlogged => "UNLOGGED",
			Self::Until => "UNTIL",
			Self::Update => "UPDATE",
			Self::Vacuum => "VACUUM",
			Self::Valid => "VALID",
			Self::Validate => "VALIDATE",
			Self::Validator => "VALIDATOR",
			Self::Value => "VALUE",
			Self::Version => "VERSION",
			Self::View => "VIEW",
			Self::Views => "VIEWS",
			Self::Volatile => "VOLATILE",
			Self::Whitespace => "WHITESPACE",
			Self::Without => "WITHOUT",
			Self::Work => "WORK",
			Self::Wrapper => "WRAPPER",
			Self::Write => "WRITE",
			Self::Xml => "XML",
			Self::Year => "YEAR",
			Self::Zone => "ZONE",
		}
	}

	/// Lookup a keyword from an ASCII case-insensitive string.
	pub fn from_str_ignore_ascii_case(s: &str) -> Option<Self> {
		// Fast match based on length and characters
		match s.len() {
			2 => match_len_2(s),
			3 => match_len_3(s),
			4 => match_len_4(s),
			5 => match_len_5(s),
			6 => match_len_6(s),
			7 => match_len_7(s),
			8 => match_len_8(s),
			9 => match_len_9(s),
			10 => match_len_10(s),
			11 => match_len_11(s),
			12 => match_len_12(s),
			13 => match_len_13(s),
			14 => match_len_14(s),
			15 => match_len_15(s),
			16 => match_len_16(s),
			17 => match_len_17(s),
			_ => None,
		}
	}
}

impl fmt::Display for Keyword {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(self.as_str())
	}
}

fn match_len_2(s: &str) -> Option<Keyword> {
	if s.eq_ignore_ascii_case("as") {
		Some(Keyword::As)
	} else if s.eq_ignore_ascii_case("at") {
		Some(Keyword::At)
	} else if s.eq_ignore_ascii_case("by") {
		Some(Keyword::By)
	} else if s.eq_ignore_ascii_case("do") {
		Some(Keyword::Do)
	} else if s.eq_ignore_ascii_case("if") {
		Some(Keyword::If)
	} else if s.eq_ignore_ascii_case("in") {
		Some(Keyword::In)
	} else if s.eq_ignore_ascii_case("is") {
		Some(Keyword::Is)
	} else if s.eq_ignore_ascii_case("no") {
		Some(Keyword::No)
	} else if s.eq_ignore_ascii_case("of") {
		Some(Keyword::Of)
	} else if s.eq_ignore_ascii_case("on") {
		Some(Keyword::On)
	} else if s.eq_ignore_ascii_case("or") {
		Some(Keyword::Or)
	} else if s.eq_ignore_ascii_case("to") {
		Some(Keyword::To)
	} else {
		None
	}
}

fn match_len_3(s: &str) -> Option<Keyword> {
	if s.eq_ignore_ascii_case("add") {
		Some(Keyword::Add)
	} else if s.eq_ignore_ascii_case("all") {
		Some(Keyword::All)
	} else if s.eq_ignore_ascii_case("and") {
		Some(Keyword::And)
	} else if s.eq_ignore_ascii_case("any") {
		Some(Keyword::Any)
	} else if s.eq_ignore_ascii_case("asc") {
		Some(Keyword::Asc)
	} else if s.eq_ignore_ascii_case("bit") {
		Some(Keyword::Bit)
	} else if s.eq_ignore_ascii_case("csv") {
		Some(Keyword::Csv)
	} else if s.eq_ignore_ascii_case("day") {
		Some(Keyword::Day)
	} else if s.eq_ignore_ascii_case("dec") {
		Some(Keyword::Dec)
	} else if s.eq_ignore_ascii_case("end") {
		Some(Keyword::End)
	} else if s.eq_ignore_ascii_case("for") {
		Some(Keyword::For)
	} else if s.eq_ignore_ascii_case("int") {
		Some(Keyword::Int)
	} else if s.eq_ignore_ascii_case("key") {
		Some(Keyword::Key)
	} else if s.eq_ignore_ascii_case("not") {
		Some(Keyword::Not)
	} else if s.eq_ignore_ascii_case("off") {
		Some(Keyword::Off)
	} else if s.eq_ignore_ascii_case("out") {
		Some(Keyword::Out)
	} else if s.eq_ignore_ascii_case("ref") {
		Some(Keyword::Ref)
	} else if s.eq_ignore_ascii_case("row") {
		Some(Keyword::Row)
	} else if s.eq_ignore_ascii_case("set") {
		Some(Keyword::Set)
	} else if s.eq_ignore_ascii_case("sql") {
		Some(Keyword::Sql)
	} else if s.eq_ignore_ascii_case("xml") {
		Some(Keyword::Xml)
	} else {
		None
	}
}

fn match_len_4(s: &str) -> Option<Keyword> {
	if s.eq_ignore_ascii_case("also") {
		Some(Keyword::Also)
	} else if s.eq_ignore_ascii_case("both") {
		Some(Keyword::Both)
	} else if s.eq_ignore_ascii_case("call") {
		Some(Keyword::Call)
	} else if s.eq_ignore_ascii_case("case") {
		Some(Keyword::Case)
	} else if s.eq_ignore_ascii_case("cast") {
		Some(Keyword::Cast)
	} else if s.eq_ignore_ascii_case("char") {
		Some(Keyword::Char)
	} else if s.eq_ignore_ascii_case("copy") {
		Some(Keyword::Copy)
	} else if s.eq_ignore_ascii_case("cost") {
		Some(Keyword::Cost)
	} else if s.eq_ignore_ascii_case("cube") {
		Some(Keyword::Cube)
	} else if s.eq_ignore_ascii_case("data") {
		Some(Keyword::Data)
	} else if s.eq_ignore_ascii_case("desc") {
		Some(Keyword::Desc)
	} else if s.eq_ignore_ascii_case("drop") {
		Some(Keyword::Drop)
	} else if s.eq_ignore_ascii_case("each") {
		Some(Keyword::Each)
	} else if s.eq_ignore_ascii_case("else") {
		Some(Keyword::Else)
	} else if s.eq_ignore_ascii_case("enum") {
		Some(Keyword::Enum)
	} else if s.eq_ignore_ascii_case("from") {
		Some(Keyword::From)
	} else if s.eq_ignore_ascii_case("full") {
		Some(Keyword::Full)
	} else if s.eq_ignore_ascii_case("hold") {
		Some(Keyword::Hold)
	} else if s.eq_ignore_ascii_case("hour") {
		Some(Keyword::Hour)
	} else if s.eq_ignore_ascii_case("into") {
		Some(Keyword::Into)
	} else if s.eq_ignore_ascii_case("join") {
		Some(Keyword::Join)
	} else if s.eq_ignore_ascii_case("last") {
		Some(Keyword::Last)
	} else if s.eq_ignore_ascii_case("left") {
		Some(Keyword::Left)
	} else if s.eq_ignore_ascii_case("like") {
		Some(Keyword::Like)
	} else if s.eq_ignore_ascii_case("load") {
		Some(Keyword::Load)
	} else if s.eq_ignore_ascii_case("lock") {
		Some(Keyword::Lock)
	} else if s.eq_ignore_ascii_case("mode") {
		Some(Keyword::Mode)
	} else if s.eq_ignore_ascii_case("move") {
		Some(Keyword::Move)
	} else if s.eq_ignore_ascii_case("name") {
		Some(Keyword::Name)
	} else if s.eq_ignore_ascii_case("next") {
		Some(Keyword::Next)
	} else if s.eq_ignore_ascii_case("none") {
		Some(Keyword::None)
	} else if s.eq_ignore_ascii_case("null") {
		Some(Keyword::Null)
	} else if s.eq_ignore_ascii_case("oids") {
		Some(Keyword::Oids)
	} else if s.eq_ignore_ascii_case("only") {
		Some(Keyword::Only)
	} else if s.eq_ignore_ascii_case("over") {
		Some(Keyword::Over)
	} else if s.eq_ignore_ascii_case("real") {
		Some(Keyword::Real)
	} else if s.eq_ignore_ascii_case("role") {
		Some(Keyword::Role)
	} else if s.eq_ignore_ascii_case("rows") {
		Some(Keyword::Rows)
	} else if s.eq_ignore_ascii_case("rule") {
		Some(Keyword::Rule)
	} else if s.eq_ignore_ascii_case("sets") {
		Some(Keyword::Sets)
	} else if s.eq_ignore_ascii_case("show") {
		Some(Keyword::Show)
	} else if s.eq_ignore_ascii_case("skip") {
		Some(Keyword::Skip)
	} else if s.eq_ignore_ascii_case("some") {
		Some(Keyword::Some)
	} else if s.eq_ignore_ascii_case("temp") {
		Some(Keyword::Temp)
	} else if s.eq_ignore_ascii_case("text") {
		Some(Keyword::Text)
	} else if s.eq_ignore_ascii_case("then") {
		Some(Keyword::Then)
	} else if s.eq_ignore_ascii_case("ties") {
		Some(Keyword::Ties)
	} else if s.eq_ignore_ascii_case("time") {
		Some(Keyword::Time)
	} else if s.eq_ignore_ascii_case("true") {
		Some(Keyword::True)
	} else if s.eq_ignore_ascii_case("type") {
		Some(Keyword::Type)
	} else if s.eq_ignore_ascii_case("trim") {
		Some(Keyword::Trim)
	} else if s.eq_ignore_ascii_case("user") {
		Some(Keyword::User)
	} else if s.eq_ignore_ascii_case("view") {
		Some(Keyword::View)
	} else if s.eq_ignore_ascii_case("when") {
		Some(Keyword::When)
	} else if s.eq_ignore_ascii_case("with") {
		Some(Keyword::With)
	} else if s.eq_ignore_ascii_case("work") {
		Some(Keyword::Work)
	} else if s.eq_ignore_ascii_case("year") {
		Some(Keyword::Year)
	} else if s.eq_ignore_ascii_case("zone") {
		Some(Keyword::Zone)
	} else {
		None
	}
}

fn match_len_5(s: &str) -> Option<Keyword> {
	if s.eq_ignore_ascii_case("abort") {
		Some(Keyword::Abort)
	} else if s.eq_ignore_ascii_case("after") {
		Some(Keyword::After)
	} else if s.eq_ignore_ascii_case("alter") {
		Some(Keyword::Alter)
	} else if s.eq_ignore_ascii_case("array") {
		Some(Keyword::Array)
	} else if s.eq_ignore_ascii_case("begin") {
		Some(Keyword::Begin)
	} else if s.eq_ignore_ascii_case("cache") {
		Some(Keyword::Cache)
	} else if s.eq_ignore_ascii_case("check") {
		Some(Keyword::Check)
	} else if s.eq_ignore_ascii_case("close") {
		Some(Keyword::Close)
	} else if s.eq_ignore_ascii_case("cross") {
		Some(Keyword::Cross)
	} else if s.eq_ignore_ascii_case("cycle") {
		Some(Keyword::Cycle)
	} else if s.eq_ignore_ascii_case("defer") {
		None
	} else if s.eq_ignore_ascii_case("event") {
		Some(Keyword::Event)
	} else if s.eq_ignore_ascii_case("false") {
		Some(Keyword::False)
	} else if s.eq_ignore_ascii_case("fetch") {
		Some(Keyword::Fetch)
	} else if s.eq_ignore_ascii_case("first") {
		Some(Keyword::First)
	} else if s.eq_ignore_ascii_case("float") {
		Some(Keyword::Float)
	} else if s.eq_ignore_ascii_case("force") {
		Some(Keyword::Force)
	} else if s.eq_ignore_ascii_case("grant") {
		Some(Keyword::Grant)
	} else if s.eq_ignore_ascii_case("group") {
		Some(Keyword::Group)
	} else if s.eq_ignore_ascii_case("ilike") {
		Some(Keyword::Ilike)
	} else if s.eq_ignore_ascii_case("index") {
		Some(Keyword::Index)
	} else if s.eq_ignore_ascii_case("inout") {
		Some(Keyword::Inout)
	} else if s.eq_ignore_ascii_case("inner") {
		Some(Keyword::Inner)
	} else if s.eq_ignore_ascii_case("least") {
		Some(Keyword::Least)
	} else if s.eq_ignore_ascii_case("level") {
		Some(Keyword::Level)
	} else if s.eq_ignore_ascii_case("limit") {
		Some(Keyword::Limit)
	} else if s.eq_ignore_ascii_case("local") {
		Some(Keyword::Local)
	} else if s.eq_ignore_ascii_case("match") {
		Some(Keyword::Match)
	} else if s.eq_ignore_ascii_case("merge") {
		Some(Keyword::Merge)
	} else if s.eq_ignore_ascii_case("month") {
		Some(Keyword::Month)
	} else if s.eq_ignore_ascii_case("names") {
		Some(Keyword::Names)
	} else if s.eq_ignore_ascii_case("nchar") {
		Some(Keyword::Nchar)
	} else if s.eq_ignore_ascii_case("nulls") {
		Some(Keyword::Nulls)
	} else if s.eq_ignore_ascii_case("order") {
		Some(Keyword::Order)
	} else if s.eq_ignore_ascii_case("outer") {
		Some(Keyword::Outer)
	} else if s.eq_ignore_ascii_case("owned") {
		Some(Keyword::Owned)
	} else if s.eq_ignore_ascii_case("owner") {
		Some(Keyword::Owner)
	} else if s.eq_ignore_ascii_case("plans") {
		Some(Keyword::Plans)
	} else if s.eq_ignore_ascii_case("prior") {
		Some(Keyword::Prior)
	} else if s.eq_ignore_ascii_case("quote") {
		Some(Keyword::Quote)
	} else if s.eq_ignore_ascii_case("range") {
		Some(Keyword::Range)
	} else if s.eq_ignore_ascii_case("reset") {
		Some(Keyword::Reset)
	} else if s.eq_ignore_ascii_case("right") {
		Some(Keyword::Right)
	} else if s.eq_ignore_ascii_case("setof") {
		Some(Keyword::Setof)
	} else if s.eq_ignore_ascii_case("share") {
		Some(Keyword::Share)
	} else if s.eq_ignore_ascii_case("strip") {
		Some(Keyword::Strip)
	} else if s.eq_ignore_ascii_case("sysid") {
		Some(Keyword::Sysid)
	} else if s.eq_ignore_ascii_case("table") {
		Some(Keyword::Table)
	} else if s.eq_ignore_ascii_case("treat") {
		Some(Keyword::Treat)
	} else if s.eq_ignore_ascii_case("types") {
		Some(Keyword::Types)
	} else if s.eq_ignore_ascii_case("union") {
		Some(Keyword::Union)
	} else if s.eq_ignore_ascii_case("until") {
		Some(Keyword::Until)
	} else if s.eq_ignore_ascii_case("valid") {
		Some(Keyword::Valid)
	} else if s.eq_ignore_ascii_case("value") {
		Some(Keyword::Value)
	} else if s.eq_ignore_ascii_case("views") {
		Some(Keyword::Views)
	} else if s.eq_ignore_ascii_case("where") {
		Some(Keyword::Where)
	} else if s.eq_ignore_ascii_case("write") {
		Some(Keyword::Write)
	} else {
		None
	}
}

fn match_len_6(s: &str) -> Option<Keyword> {
	if s.eq_ignore_ascii_case("access") {
		Some(Keyword::Access)
	} else if s.eq_ignore_ascii_case("action") {
		Some(Keyword::Action)
	} else if s.eq_ignore_ascii_case("always") {
		Some(Keyword::Always)
	} else if s.eq_ignore_ascii_case("attach") {
		Some(Keyword::Attach)
	} else if s.eq_ignore_ascii_case("before") {
		Some(Keyword::Before)
	} else if s.eq_ignore_ascii_case("bigint") {
		Some(Keyword::Bigint)
	} else if s.eq_ignore_ascii_case("binary") {
		Some(Keyword::Binary)
	} else if s.eq_ignore_ascii_case("cannot") {
		None
	} else if s.eq_ignore_ascii_case("column") {
		Some(Keyword::Column)
	} else if s.eq_ignore_ascii_case("commit") {
		Some(Keyword::Commit)
	} else if s.eq_ignore_ascii_case("create") {
		Some(Keyword::Create)
	} else if s.eq_ignore_ascii_case("cursor") {
		Some(Keyword::Cursor)
	} else if s.eq_ignore_ascii_case("delete") {
		Some(Keyword::Delete)
	} else if s.eq_ignore_ascii_case("detach") {
		Some(Keyword::Detach)
	} else if s.eq_ignore_ascii_case("enable") {
		Some(Keyword::Enable)
	} else if s.eq_ignore_ascii_case("escape") {
		Some(Keyword::Escape)
	} else if s.eq_ignore_ascii_case("except") {
		Some(Keyword::Except)
	} else if s.eq_ignore_ascii_case("exists") {
		Some(Keyword::Exists)
	} else if s.eq_ignore_ascii_case("filter") {
		Some(Keyword::Filter)
	} else if s.eq_ignore_ascii_case("freeze") {
		Some(Keyword::Freeze)
	} else if s.eq_ignore_ascii_case("global") {
		Some(Keyword::Global)
	} else if s.eq_ignore_ascii_case("groups") {
		Some(Keyword::Groups)
	} else if s.eq_ignore_ascii_case("having") {
		Some(Keyword::Having)
	} else if s.eq_ignore_ascii_case("header") {
		Some(Keyword::Header)
	} else if s.eq_ignore_ascii_case("isnull") {
		Some(Keyword::Isnull)
	} else if s.eq_ignore_ascii_case("listen") {
		Some(Keyword::Listen)
	} else if s.eq_ignore_ascii_case("locked") {
		Some(Keyword::Locked)
	} else if s.eq_ignore_ascii_case("logged") {
		Some(Keyword::Logged)
	} else if s.eq_ignore_ascii_case("method") {
		Some(Keyword::Method)
	} else if s.eq_ignore_ascii_case("minute") {
		Some(Keyword::Minute)
	} else if s.eq_ignore_ascii_case("notify") {
		Some(Keyword::Notify)
	} else if s.eq_ignore_ascii_case("nowait") {
		Some(Keyword::Nowait)
	} else if s.eq_ignore_ascii_case("nullif") {
		Some(Keyword::Nullif)
	} else if s.eq_ignore_ascii_case("object") {
		Some(Keyword::Object)
	} else if s.eq_ignore_ascii_case("offset") {
		Some(Keyword::Offset)
	} else if s.eq_ignore_ascii_case("option") {
		Some(Keyword::Option)
	} else if s.eq_ignore_ascii_case("others") {
		Some(Keyword::Others)
	} else if s.eq_ignore_ascii_case("parser") {
		Some(Keyword::Parser)
	} else if s.eq_ignore_ascii_case("policy") {
		Some(Keyword::Policy)
	} else if s.eq_ignore_ascii_case("repeat") {
		None
	} else if s.eq_ignore_ascii_case("revoke") {
		Some(Keyword::Revoke)
	} else if s.eq_ignore_ascii_case("rollup") {
		Some(Keyword::Rollup)
	} else if s.eq_ignore_ascii_case("schema") {
		Some(Keyword::Schema)
	} else if s.eq_ignore_ascii_case("scroll") {
		Some(Keyword::Scroll)
	} else if s.eq_ignore_ascii_case("search") {
		Some(Keyword::Search)
	} else if s.eq_ignore_ascii_case("second") {
		Some(Keyword::Second)
	} else if s.eq_ignore_ascii_case("select") {
		Some(Keyword::Select)
	} else if s.eq_ignore_ascii_case("server") {
		Some(Keyword::Server)
	} else if s.eq_ignore_ascii_case("simple") {
		Some(Keyword::Simple)
	} else if s.eq_ignore_ascii_case("stable") {
		Some(Keyword::Stable)
	} else if s.eq_ignore_ascii_case("strict") {
		Some(Keyword::Strict)
	} else if s.eq_ignore_ascii_case("system") {
		Some(Keyword::System)
	} else if s.eq_ignore_ascii_case("tables") {
		Some(Keyword::Tables)
	} else if s.eq_ignore_ascii_case("target") {
		Some(Keyword::Target)
	} else if s.eq_ignore_ascii_case("unique") {
		Some(Keyword::Unique)
	} else if s.eq_ignore_ascii_case("update") {
		Some(Keyword::Update)
	} else if s.eq_ignore_ascii_case("vacuum") {
		Some(Keyword::Vacuum)
	} else if s.eq_ignore_ascii_case("values") {
		Some(Keyword::Values)
	} else if s.eq_ignore_ascii_case("window") {
		Some(Keyword::Window)
	} else {
		None
	}
}

fn match_len_7(s: &str) -> Option<Keyword> {
	if s.eq_ignore_ascii_case("analyse") {
		Some(Keyword::Analyse)
	} else if s.eq_ignore_ascii_case("analyze") {
		Some(Keyword::Analyze)
	} else if s.eq_ignore_ascii_case("between") {
		Some(Keyword::Between)
	} else if s.eq_ignore_ascii_case("boolean") {
		Some(Keyword::Boolean)
	} else if s.eq_ignore_ascii_case("cascade") {
		Some(Keyword::Cascade)
	} else if s.eq_ignore_ascii_case("catalog") {
		Some(Keyword::Catalog)
	} else if s.eq_ignore_ascii_case("cluster") {
		Some(Keyword::Cluster)
	} else if s.eq_ignore_ascii_case("comment") {
		Some(Keyword::Comment)
	} else if s.eq_ignore_ascii_case("collate") {
		Some(Keyword::Collate)
	} else if s.eq_ignore_ascii_case("content") {
		Some(Keyword::Content)
	} else if s.eq_ignore_ascii_case("current") {
		Some(Keyword::Current)
	} else if s.eq_ignore_ascii_case("declare") {
		Some(Keyword::Declare)
	} else if s.eq_ignore_ascii_case("decimal") {
		Some(Keyword::Decimal)
	} else if s.eq_ignore_ascii_case("definer") {
		Some(Keyword::Definer)
	} else if s.eq_ignore_ascii_case("depends") {
		Some(Keyword::Depends)
	} else if s.eq_ignore_ascii_case("disable") {
		Some(Keyword::Disable)
	} else if s.eq_ignore_ascii_case("discard") {
		Some(Keyword::Discard)
	} else if s.eq_ignore_ascii_case("exclude") {
		Some(Keyword::Exclude)
	} else if s.eq_ignore_ascii_case("execute") {
		Some(Keyword::Execute)
	} else if s.eq_ignore_ascii_case("explain") {
		Some(Keyword::Explain)
	} else if s.eq_ignore_ascii_case("extract") {
		Some(Keyword::Extract)
	} else if s.eq_ignore_ascii_case("foreign") {
		Some(Keyword::Foreign)
	} else if s.eq_ignore_ascii_case("forward") {
		Some(Keyword::Forward)
	} else if s.eq_ignore_ascii_case("handler") {
		Some(Keyword::Handler)
	} else if s.eq_ignore_ascii_case("indexes") {
		Some(Keyword::Indexes)
	} else if s.eq_ignore_ascii_case("inherit") {
		Some(Keyword::Inherit)
	} else if s.eq_ignore_ascii_case("instead") {
		Some(Keyword::Instead)
	} else if s.eq_ignore_ascii_case("integer") {
		Some(Keyword::Integer)
	} else if s.eq_ignore_ascii_case("invoker") {
		Some(Keyword::Invoker)
	} else if s.eq_ignore_ascii_case("lateral") {
		Some(Keyword::Lateral)
	} else if s.eq_ignore_ascii_case("leading") {
		Some(Keyword::Leading)
	} else if s.eq_ignore_ascii_case("mapping") {
		Some(Keyword::Mapping)
	} else if s.eq_ignore_ascii_case("matched") {
		Some(Keyword::Matched)
	} else if s.eq_ignore_ascii_case("natural") {
		Some(Keyword::Natural)
	} else if s.eq_ignore_ascii_case("nothing") {
		Some(Keyword::Nothing)
	} else if s.eq_ignore_ascii_case("notnull") {
		Some(Keyword::Notnull)
	} else if s.eq_ignore_ascii_case("numeric") {
		Some(Keyword::Numeric)
	} else if s.eq_ignore_ascii_case("options") {
		Some(Keyword::Options)
	} else if s.eq_ignore_ascii_case("overlay") {
		Some(Keyword::Overlay)
	} else if s.eq_ignore_ascii_case("partial") {
		Some(Keyword::Partial)
	} else if s.eq_ignore_ascii_case("passing") {
		Some(Keyword::Passing)
	} else if s.eq_ignore_ascii_case("placing") {
		Some(Keyword::Placing)
	} else if s.eq_ignore_ascii_case("prepare") {
		Some(Keyword::Prepare)
	} else if s.eq_ignore_ascii_case("primary") {
		Some(Keyword::Primary)
	} else if s.eq_ignore_ascii_case("refresh") {
		Some(Keyword::Refresh)
	} else if s.eq_ignore_ascii_case("reindex") {
		Some(Keyword::Reindex)
	} else if s.eq_ignore_ascii_case("release") {
		Some(Keyword::Release)
	} else if s.eq_ignore_ascii_case("replace") {
		Some(Keyword::Replace)
	} else if s.eq_ignore_ascii_case("replica") {
		Some(Keyword::Replica)
	} else if s.eq_ignore_ascii_case("restart") {
		Some(Keyword::Restart)
	} else if s.eq_ignore_ascii_case("returns") {
		Some(Keyword::Returns)
	} else if s.eq_ignore_ascii_case("routine") {
		Some(Keyword::Routine)
	} else if s.eq_ignore_ascii_case("schemas") {
		Some(Keyword::Schemas)
	} else if s.eq_ignore_ascii_case("session") {
		Some(Keyword::Session)
	} else if s.eq_ignore_ascii_case("similar") {
		Some(Keyword::Similar)
	} else if s.eq_ignore_ascii_case("storage") {
		Some(Keyword::Storage)
	} else if s.eq_ignore_ascii_case("support") {
		Some(Keyword::Support)
	} else if s.eq_ignore_ascii_case("trigger") {
		Some(Keyword::Trigger)
	} else if s.eq_ignore_ascii_case("unknown") {
		Some(Keyword::Unknown)
	} else if s.eq_ignore_ascii_case("unlogged") {
		Some(Keyword::Unlogged)
	} else if s.eq_ignore_ascii_case("varchar") {
		Some(Keyword::Varchar)
	} else if s.eq_ignore_ascii_case("verbose") {
		Some(Keyword::Verbose)
	} else if s.eq_ignore_ascii_case("version") {
		Some(Keyword::Version)
	} else if s.eq_ignore_ascii_case("without") {
		Some(Keyword::Without)
	} else if s.eq_ignore_ascii_case("wrapper") {
		Some(Keyword::Wrapper)
	} else {
		None
	}
}

fn match_len_8(s: &str) -> Option<Keyword> {
	if s.eq_ignore_ascii_case("backward") {
		Some(Keyword::Backward)
	} else if s.eq_ignore_ascii_case("cascaded") {
		Some(Keyword::Cascaded)
	} else if s.eq_ignore_ascii_case("coalesce") {
		Some(Keyword::Coalesce)
	} else if s.eq_ignore_ascii_case("conflict") {
		Some(Keyword::Conflict)
	} else if s.eq_ignore_ascii_case("continue") {
		Some(Keyword::Continue)
	} else if s.eq_ignore_ascii_case("database") {
		Some(Keyword::Database)
	} else if s.eq_ignore_ascii_case("defaults") {
		Some(Keyword::Defaults)
	} else if s.eq_ignore_ascii_case("deferred") {
		Some(Keyword::Deferred)
	} else if s.eq_ignore_ascii_case("distinct") {
		Some(Keyword::Distinct)
	} else if s.eq_ignore_ascii_case("document") {
		Some(Keyword::Document)
	} else if s.eq_ignore_ascii_case("encoding") {
		Some(Keyword::Encoding)
	} else if s.eq_ignore_ascii_case("finalize") {
		Some(Keyword::Finalize)
	} else if s.eq_ignore_ascii_case("function") {
		Some(Keyword::Function)
	} else if s.eq_ignore_ascii_case("greatest") {
		Some(Keyword::Greatest)
	} else if s.eq_ignore_ascii_case("grouping") {
		Some(Keyword::Grouping)
	} else if s.eq_ignore_ascii_case("identity") {
		Some(Keyword::Identity)
	} else if s.eq_ignore_ascii_case("implicit") {
		Some(Keyword::Implicit)
	} else if s.eq_ignore_ascii_case("inherits") {
		Some(Keyword::Inherits)
	} else if s.eq_ignore_ascii_case("interval") {
		Some(Keyword::Interval)
	} else if s.eq_ignore_ascii_case("location") {
		Some(Keyword::Location)
	} else if s.eq_ignore_ascii_case("maxvalue") {
		Some(Keyword::Maxvalue)
	} else if s.eq_ignore_ascii_case("minvalue") {
		Some(Keyword::Minvalue)
	} else if s.eq_ignore_ascii_case("national") {
		Some(Keyword::National)
	} else if s.eq_ignore_ascii_case("operator") {
		Some(Keyword::Operator)
	} else if s.eq_ignore_ascii_case("overlaps") {
		Some(Keyword::Overlaps)
	} else if s.eq_ignore_ascii_case("parallel") {
		Some(Keyword::Parallel)
	} else if s.eq_ignore_ascii_case("password") {
		Some(Keyword::Password)
	} else if s.eq_ignore_ascii_case("position") {
		Some(Keyword::Position)
	} else if s.eq_ignore_ascii_case("prepared") {
		Some(Keyword::Prepared)
	} else if s.eq_ignore_ascii_case("preserve") {
		Some(Keyword::Preserve)
	} else if s.eq_ignore_ascii_case("relative") {
		Some(Keyword::Relative)
	} else if s.eq_ignore_ascii_case("restrict") {
		Some(Keyword::Restrict)
	} else if s.eq_ignore_ascii_case("rollback") {
		Some(Keyword::Rollback)
	} else if s.eq_ignore_ascii_case("routines") {
		Some(Keyword::Routines)
	} else if s.eq_ignore_ascii_case("security") {
		Some(Keyword::Security)
	} else if s.eq_ignore_ascii_case("sequence") {
		Some(Keyword::Sequence)
	} else if s.eq_ignore_ascii_case("smallint") {
		Some(Keyword::Smallint)
	} else if s.eq_ignore_ascii_case("snapshot") {
		Some(Keyword::Snapshot)
	} else if s.eq_ignore_ascii_case("template") {
		Some(Keyword::Template)
	} else if s.eq_ignore_ascii_case("trailing") {
		Some(Keyword::Trailing)
	} else if s.eq_ignore_ascii_case("truncate") {
		Some(Keyword::Truncate)
	} else if s.eq_ignore_ascii_case("unlisten") {
		Some(Keyword::Unlisten)
	} else if s.eq_ignore_ascii_case("validate") {
		Some(Keyword::Validate)
	} else if s.eq_ignore_ascii_case("variadic") {
		Some(Keyword::Variadic)
	} else if s.eq_ignore_ascii_case("volatile") {
		Some(Keyword::Volatile)
	} else if s.eq_ignore_ascii_case("xmlparse") {
		Some(Keyword::Xmlparse)
	} else if s.eq_ignore_ascii_case("xmltable") {
		Some(Keyword::Xmltable)
	} else {
		None
	}
}

fn match_len_9(s: &str) -> Option<Keyword> {
	if s.eq_ignore_ascii_case("aggregate") {
		Some(Keyword::Aggregate)
	} else if s.eq_ignore_ascii_case("assertion") {
		Some(Keyword::Assertion)
	} else if s.eq_ignore_ascii_case("character") {
		Some(Keyword::Character)
	} else if s.eq_ignore_ascii_case("collation") {
		Some(Keyword::Collation)
	} else if s.eq_ignore_ascii_case("committed") {
		Some(Keyword::Committed)
	} else if s.eq_ignore_ascii_case("encrypted") {
		Some(Keyword::Encrypted)
	} else if s.eq_ignore_ascii_case("excluding") {
		Some(Keyword::Excluding)
	} else if s.eq_ignore_ascii_case("exclusive") {
		Some(Keyword::Exclusive)
	} else if s.eq_ignore_ascii_case("extension") {
		Some(Keyword::Extension)
	} else if s.eq_ignore_ascii_case("following") {
		Some(Keyword::Following)
	} else if s.eq_ignore_ascii_case("functions") {
		Some(Keyword::Functions)
	} else if s.eq_ignore_ascii_case("generated") {
		Some(Keyword::Generated)
	} else if s.eq_ignore_ascii_case("immediate") {
		Some(Keyword::Immediate)
	} else if s.eq_ignore_ascii_case("immutable") {
		Some(Keyword::Immutable)
	} else if s.eq_ignore_ascii_case("including") {
		Some(Keyword::Including)
	} else if s.eq_ignore_ascii_case("increment") {
		Some(Keyword::Increment)
	} else if s.eq_ignore_ascii_case("initially") {
		Some(Keyword::Initially)
	} else if s.eq_ignore_ascii_case("intersect") {
		Some(Keyword::Intersect)
	} else if s.eq_ignore_ascii_case("isolation") {
		Some(Keyword::Isolation)
	} else if s.eq_ignore_ascii_case("leakproof") {
		Some(Keyword::Leakproof)
	} else if s.eq_ignore_ascii_case("localtime") {
		Some(Keyword::Localtime)
	} else if s.eq_ignore_ascii_case("partition") {
		Some(Keyword::Partition)
	} else if s.eq_ignore_ascii_case("preceding") {
		Some(Keyword::Preceding)
	} else if s.eq_ignore_ascii_case("precision") {
		Some(Keyword::Precision)
	} else if s.eq_ignore_ascii_case("procedure") {
		Some(Keyword::Procedure)
	} else if s.eq_ignore_ascii_case("recursing") {
		None
	} else if s.eq_ignore_ascii_case("recursive") {
		Some(Keyword::Recursive)
	} else if s.eq_ignore_ascii_case("returning") {
		Some(Keyword::Returning)
	} else if s.eq_ignore_ascii_case("savepoint") {
		Some(Keyword::Savepoint)
	} else if s.eq_ignore_ascii_case("sequences") {
		Some(Keyword::Sequences)
	} else if s.eq_ignore_ascii_case("statement") {
		Some(Keyword::Statement)
	} else if s.eq_ignore_ascii_case("substring") {
		Some(Keyword::Substring)
	} else if s.eq_ignore_ascii_case("symmetric") {
		Some(Keyword::Symmetric)
	} else if s.eq_ignore_ascii_case("temporary") {
		Some(Keyword::Temporary)
	} else if s.eq_ignore_ascii_case("timestamp") {
		Some(Keyword::Timestamp)
	} else if s.eq_ignore_ascii_case("transform") {
		Some(Keyword::Transform)
	} else if s.eq_ignore_ascii_case("unbounded") {
		Some(Keyword::Unbounded)
	} else if s.eq_ignore_ascii_case("validator") {
		Some(Keyword::Validator)
	} else if s.eq_ignore_ascii_case("xmlconcat") {
		Some(Keyword::Xmlconcat)
	} else if s.eq_ignore_ascii_case("xmlelement") {
		Some(Keyword::Xmlelement)
	} else if s.eq_ignore_ascii_case("xmlexists") {
		Some(Keyword::Xmlexists)
	} else if s.eq_ignore_ascii_case("xmlforest") {
		Some(Keyword::Xmlforest)
	} else {
		None
	}
}

fn match_len_10(s: &str) -> Option<Keyword> {
	if s.eq_ignore_ascii_case("asymmetric") {
		Some(Keyword::Asymmetric)
	} else if s.eq_ignore_ascii_case("checkpoint") {
		Some(Keyword::Checkpoint)
	} else if s.eq_ignore_ascii_case("connection") {
		Some(Keyword::Connection)
	} else if s.eq_ignore_ascii_case("constraint") {
		Some(Keyword::Constraint)
	} else if s.eq_ignore_ascii_case("conversion") {
		Some(Keyword::Conversion)
	} else if s.eq_ignore_ascii_case("deallocate") {
		Some(Keyword::Deallocate)
	} else if s.eq_ignore_ascii_case("deferrable") {
		Some(Keyword::Deferrable)
	} else if s.eq_ignore_ascii_case("delimiters") {
		Some(Keyword::Delimiters)
	} else if s.eq_ignore_ascii_case("dictionary") {
		Some(Keyword::Dictionary)
	} else if s.eq_ignore_ascii_case("ordinality") {
		Some(Keyword::Ordinality)
	} else if s.eq_ignore_ascii_case("overriding") {
		Some(Keyword::Overriding)
	} else if s.eq_ignore_ascii_case("privileges") {
		Some(Keyword::Privileges)
	} else if s.eq_ignore_ascii_case("procedural") {
		Some(Keyword::Procedural)
	} else if s.eq_ignore_ascii_case("procedures") {
		Some(Keyword::Procedures)
	} else if s.eq_ignore_ascii_case("references") {
		Some(Keyword::References)
	} else if s.eq_ignore_ascii_case("standalone") {
		Some(Keyword::Standalone)
	} else if s.eq_ignore_ascii_case("statistics") {
		Some(Keyword::Statistics)
	} else if s.eq_ignore_ascii_case("whitespace") {
		Some(Keyword::Whitespace)
	} else {
		None
	}
}

fn match_len_11(s: &str) -> Option<Keyword> {
	if s.eq_ignore_ascii_case("assignment") {
		Some(Keyword::Assignment)
	} else if s.eq_ignore_ascii_case("constraints") {
		Some(Keyword::Constraints)
	} else if s.eq_ignore_ascii_case("insensitive") {
		Some(Keyword::Insensitive)
	} else if s.eq_ignore_ascii_case("publication") {
		Some(Keyword::Publication)
	} else if s.eq_ignore_ascii_case("tablesample") {
		Some(Keyword::Tablesample)
	} else if s.eq_ignore_ascii_case("tablespace") {
		Some(Keyword::Tablespace)
	} else if s.eq_ignore_ascii_case("transaction") {
		Some(Keyword::Transaction)
	} else if s.eq_ignore_ascii_case("uncommitted") {
		Some(Keyword::Uncommitted)
	} else if s.eq_ignore_ascii_case("unencrypted") {
		Some(Keyword::Unencrypted)
	} else {
		None
	}
}

fn match_len_12(s: &str) -> Option<Keyword> {
	if s.eq_ignore_ascii_case("concurrently") {
		Some(Keyword::Concurrently)
	} else if s.eq_ignore_ascii_case("current_date") {
		Some(Keyword::CurrentDate)
	} else if s.eq_ignore_ascii_case("current_role") {
		Some(Keyword::CurrentRole)
	} else if s.eq_ignore_ascii_case("current_time") {
		Some(Keyword::CurrentTime)
	} else if s.eq_ignore_ascii_case("current_user") {
		Some(Keyword::CurrentUser)
	} else if s.eq_ignore_ascii_case("materialized") {
		Some(Keyword::Materialized)
	} else if s.eq_ignore_ascii_case("serializable") {
		Some(Keyword::Serializable)
	} else if s.eq_ignore_ascii_case("session_user") {
		Some(Keyword::SessionUser)
	} else if s.eq_ignore_ascii_case("subscription") {
		Some(Keyword::Subscription)
	} else if s.eq_ignore_ascii_case("xmlserialize") {
		Some(Keyword::Xmlserialize)
	} else {
		None
	}
}

fn match_len_13(s: &str) -> Option<Keyword> {
	if s.eq_ignore_ascii_case("authorization") {
		Some(Keyword::Authorization)
	} else if s.eq_ignore_ascii_case("configuration") {
		Some(Keyword::Configuration)
	} else if s.eq_ignore_ascii_case("xmlattributes") {
		Some(Keyword::Xmlattributes)
	} else if s.eq_ignore_ascii_case("xmlnamespaces") {
		Some(Keyword::Xmlnamespaces)
	} else {
		None
	}
}

fn match_len_14(s: &str) -> Option<Keyword> {
	if s.eq_ignore_ascii_case("current_schema") {
		Some(Keyword::CurrentSchema)
	} else if s.eq_ignore_ascii_case("localtimestamp") {
		Some(Keyword::Localtimestamp)
	} else {
		None
	}
}

fn match_len_15(s: &str) -> Option<Keyword> {
	if s.eq_ignore_ascii_case("characteristics") {
		Some(Keyword::Characteristics)
	} else if s.eq_ignore_ascii_case("current_catalog") {
		Some(Keyword::CurrentCatalog)
	} else {
		None
	}
}

fn match_len_16(_s: &str) -> Option<Keyword> {
	None
}

fn match_len_17(s: &str) -> Option<Keyword> {
	if s.eq_ignore_ascii_case("current_timestamp") {
		Some(Keyword::CurrentTimestamp)
	} else {
		None
	}
}
