//! MySQL keywords enumeration and lookup tables.

/// Known MySQL reserved and non-reserved keywords.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Keyword {
	// DML / Query
	Select,
	Insert,
	Update,
	Delete,
	Replace,
	From,
	Where,
	Group,
	By,
	Having,
	Order,
	Limit,
	Offset,
	Join,
	Inner,
	Left,
	Right,
	Cross,
	Outer,
	StraightJoin,
	On,
	Using,
	As,
	Distinct,
	Distinctrow,
	All,
	Into,
	Values,
	Value,
	Set,
	Duplicate,

	// DDL
	Create,
	Drop,
	Alter,
	Table,
	Tables,
	Database,
	Databases,
	Schema,
	Schemas,
	Index,
	View,
	Add,
	Column,
	Columns,
	Modify,
	Change,
	Rename,
	To,
	Default,
	Primary,
	Key,
	Foreign,
	References,
	Unique,
	Check,
	Constraint,
	AutoIncrement,
	Cascade,
	Restrict,
	Temporary,
	Truncate,

	// Utility / Admin
	Use,
	Show,
	Describe,
	Desc,
	Explain,
	Status,
	Variables,
	Processlist,
	Warnings,
	Errors,
	Global,
	Session,
	Local,
	Persist,
	PersistOnly,

	// Transactions & Locking
	Start,
	Transaction,
	Commit,
	Rollback,
	Begin,
	Work,
	Savepoint,
	Release,
	Lock,
	Unlock,
	Read,
	Write,
	For,
	Share,
	Nowait,
	Skip,
	Locked,
	Mode,

	// CTE & Set Ops
	With,
	Recursive,
	Union,
	Intersect,
	Except,

	// Operators & Predicates
	And,
	Or,
	Xor,
	Not,
	Is,
	Null,
	True,
	False,
	Unknown,
	In,
	Between,
	Like,
	Regexp,
	Rlike,
	Binary,
	Collate,
	Div,
	Mod,
	Interval,

	// Control Flow & Expressions
	Case,
	When,
	Then,
	Else,
	End,
	Cast,
	Convert,
	If,
	Ifnull,
	Nullif,
	Coalesce,

	// Modifiers & Flags
	Asc,
	Exists,
	Ignore,
	Force,
}

impl Keyword {
	/// Canonical uppercase name of the keyword.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Select => "SELECT",
			Self::Insert => "INSERT",
			Self::Update => "UPDATE",
			Self::Delete => "DELETE",
			Self::Replace => "REPLACE",
			Self::From => "FROM",
			Self::Where => "WHERE",
			Self::Group => "GROUP",
			Self::By => "BY",
			Self::Having => "HAVING",
			Self::Order => "ORDER",
			Self::Limit => "LIMIT",
			Self::Offset => "OFFSET",
			Self::Join => "JOIN",
			Self::Inner => "INNER",
			Self::Left => "LEFT",
			Self::Right => "RIGHT",
			Self::Cross => "CROSS",
			Self::Outer => "OUTER",
			Self::StraightJoin => "STRAIGHT_JOIN",
			Self::On => "ON",
			Self::Using => "USING",
			Self::As => "AS",
			Self::Distinct => "DISTINCT",
			Self::Distinctrow => "DISTINCTROW",
			Self::All => "ALL",
			Self::Into => "INTO",
			Self::Values => "VALUES",
			Self::Value => "VALUE",
			Self::Set => "SET",
			Self::Duplicate => "DUPLICATE",

			Self::Create => "CREATE",
			Self::Drop => "DROP",
			Self::Alter => "ALTER",
			Self::Table => "TABLE",
			Self::Tables => "TABLES",
			Self::Database => "DATABASE",
			Self::Databases => "DATABASES",
			Self::Schema => "SCHEMA",
			Self::Schemas => "SCHEMAS",
			Self::Index => "INDEX",
			Self::View => "VIEW",
			Self::Add => "ADD",
			Self::Column => "COLUMN",
			Self::Columns => "COLUMNS",
			Self::Modify => "MODIFY",
			Self::Change => "CHANGE",
			Self::Rename => "RENAME",
			Self::To => "TO",
			Self::Default => "DEFAULT",
			Self::Primary => "PRIMARY",
			Self::Key => "KEY",
			Self::Foreign => "FOREIGN",
			Self::References => "REFERENCES",
			Self::Unique => "UNIQUE",
			Self::Check => "CHECK",
			Self::Constraint => "CONSTRAINT",
			Self::AutoIncrement => "AUTO_INCREMENT",
			Self::Cascade => "CASCADE",
			Self::Restrict => "RESTRICT",
			Self::Temporary => "TEMPORARY",
			Self::Truncate => "TRUNCATE",

			Self::Use => "USE",
			Self::Show => "SHOW",
			Self::Describe => "DESCRIBE",
			Self::Desc => "DESC",
			Self::Explain => "EXPLAIN",
			Self::Status => "STATUS",
			Self::Variables => "VARIABLES",
			Self::Processlist => "PROCESSLIST",
			Self::Warnings => "WARNINGS",
			Self::Errors => "ERRORS",
			Self::Global => "GLOBAL",
			Self::Session => "SESSION",
			Self::Local => "LOCAL",
			Self::Persist => "PERSIST",
			Self::PersistOnly => "PERSIST_ONLY",

			Self::Start => "START",
			Self::Transaction => "TRANSACTION",
			Self::Commit => "COMMIT",
			Self::Rollback => "ROLLBACK",
			Self::Begin => "BEGIN",
			Self::Work => "WORK",
			Self::Savepoint => "SAVEPOINT",
			Self::Release => "RELEASE",
			Self::Lock => "LOCK",
			Self::Unlock => "UNLOCK",
			Self::Read => "READ",
			Self::Write => "WRITE",
			Self::For => "FOR",
			Self::Share => "SHARE",
			Self::Nowait => "NOWAIT",
			Self::Skip => "SKIP",
			Self::Locked => "LOCKED",
			Self::Mode => "MODE",

			Self::With => "WITH",
			Self::Recursive => "RECURSIVE",
			Self::Union => "UNION",
			Self::Intersect => "INTERSECT",
			Self::Except => "EXCEPT",

			Self::And => "AND",
			Self::Or => "OR",
			Self::Xor => "XOR",
			Self::Not => "NOT",
			Self::Is => "IS",
			Self::Null => "NULL",
			Self::True => "TRUE",
			Self::False => "FALSE",
			Self::Unknown => "UNKNOWN",
			Self::In => "IN",
			Self::Between => "BETWEEN",
			Self::Like => "LIKE",
			Self::Regexp => "REGEXP",
			Self::Rlike => "RLIKE",
			Self::Binary => "BINARY",
			Self::Collate => "COLLATE",
			Self::Div => "DIV",
			Self::Mod => "MOD",
			Self::Interval => "INTERVAL",

			Self::Case => "CASE",
			Self::When => "WHEN",
			Self::Then => "THEN",
			Self::Else => "ELSE",
			Self::End => "END",
			Self::Cast => "CAST",
			Self::Convert => "CONVERT",
			Self::If => "IF",
			Self::Ifnull => "IFNULL",
			Self::Nullif => "NULLIF",
			Self::Coalesce => "COALESCE",

			Self::Asc => "ASC",
			Self::Exists => "EXISTS",
			Self::Ignore => "IGNORE",
			Self::Force => "FORCE",
		}
	}

	/// Lookup keyword from case-insensitive string slice.
	pub fn from_str_ignore_ascii_case(s: &str) -> Option<Self> {
		match s.len() {
			2 => match s.as_bytes() {
				b"as" | b"AS" | b"As" | b"aS" => Some(Self::As),
				b"by" | b"BY" | b"By" | b"bY" => Some(Self::By),
				b"on" | b"ON" | b"On" | b"oN" => Some(Self::On),
				b"to" | b"TO" | b"To" | b"tO" => Some(Self::To),
				b"is" | b"IS" | b"Is" | b"iS" => Some(Self::Is),
				b"in" | b"IN" | b"In" | b"iN" => Some(Self::In),
				b"or" | b"OR" | b"Or" | b"oR" => Some(Self::Or),
				b"if" | b"IF" | b"If" | b"iF" => Some(Self::If),
				_ => None,
			},
			3 => {
				if s.eq_ignore_ascii_case("all") {
					Some(Self::All)
				} else if s.eq_ignore_ascii_case("and") {
					Some(Self::And)
				} else if s.eq_ignore_ascii_case("add") {
					Some(Self::Add)
				} else if s.eq_ignore_ascii_case("asc") {
					Some(Self::Asc)
				} else if s.eq_ignore_ascii_case("div") {
					Some(Self::Div)
				} else if s.eq_ignore_ascii_case("end") {
					Some(Self::End)
				} else if s.eq_ignore_ascii_case("for") {
					Some(Self::For)
				} else if s.eq_ignore_ascii_case("key") {
					Some(Self::Key)
				} else if s.eq_ignore_ascii_case("mod") {
					Some(Self::Mod)
				} else if s.eq_ignore_ascii_case("not") {
					Some(Self::Not)
				} else if s.eq_ignore_ascii_case("set") {
					Some(Self::Set)
				} else if s.eq_ignore_ascii_case("use") {
					Some(Self::Use)
				} else if s.eq_ignore_ascii_case("xor") {
					Some(Self::Xor)
				} else {
					None
				}
			}
			4 => {
				if s.eq_ignore_ascii_case("case") {
					Some(Self::Case)
				} else if s.eq_ignore_ascii_case("cast") {
					Some(Self::Cast)
				} else if s.eq_ignore_ascii_case("desc") {
					Some(Self::Desc)
				} else if s.eq_ignore_ascii_case("drop") {
					Some(Self::Drop)
				} else if s.eq_ignore_ascii_case("else") {
					Some(Self::Else)
				} else if s.eq_ignore_ascii_case("from") {
					Some(Self::From)
				} else if s.eq_ignore_ascii_case("into") {
					Some(Self::Into)
				} else if s.eq_ignore_ascii_case("join") {
					Some(Self::Join)
				} else if s.eq_ignore_ascii_case("left") {
					Some(Self::Left)
				} else if s.eq_ignore_ascii_case("like") {
					Some(Self::Like)
				} else if s.eq_ignore_ascii_case("lock") {
					Some(Self::Lock)
				} else if s.eq_ignore_ascii_case("mode") {
					Some(Self::Mode)
				} else if s.eq_ignore_ascii_case("null") {
					Some(Self::Null)
				} else if s.eq_ignore_ascii_case("read") {
					Some(Self::Read)
				} else if s.eq_ignore_ascii_case("show") {
					Some(Self::Show)
				} else if s.eq_ignore_ascii_case("skip") {
					Some(Self::Skip)
				} else if s.eq_ignore_ascii_case("then") {
					Some(Self::Then)
				} else if s.eq_ignore_ascii_case("true") {
					Some(Self::True)
				} else if s.eq_ignore_ascii_case("view") {
					Some(Self::View)
				} else if s.eq_ignore_ascii_case("when") {
					Some(Self::When)
				} else if s.eq_ignore_ascii_case("with") {
					Some(Self::With)
				} else if s.eq_ignore_ascii_case("work") {
					Some(Self::Work)
				} else {
					None
				}
			}
			5 => {
				if s.eq_ignore_ascii_case("alter") {
					Some(Self::Alter)
				} else if s.eq_ignore_ascii_case("begin") {
					Some(Self::Begin)
				} else if s.eq_ignore_ascii_case("check") {
					Some(Self::Check)
				} else if s.eq_ignore_ascii_case("cross") {
					Some(Self::Cross)
				} else if s.eq_ignore_ascii_case("false") {
					Some(Self::False)
				} else if s.eq_ignore_ascii_case("force") {
					Some(Self::Force)
				} else if s.eq_ignore_ascii_case("group") {
					Some(Self::Group)
				} else if s.eq_ignore_ascii_case("index") {
					Some(Self::Index)
				} else if s.eq_ignore_ascii_case("inner") {
					Some(Self::Inner)
				} else if s.eq_ignore_ascii_case("limit") {
					Some(Self::Limit)
				} else if s.eq_ignore_ascii_case("local") {
					Some(Self::Local)
				} else if s.eq_ignore_ascii_case("order") {
					Some(Self::Order)
				} else if s.eq_ignore_ascii_case("outer") {
					Some(Self::Outer)
				} else if s.eq_ignore_ascii_case("right") {
					Some(Self::Right)
				} else if s.eq_ignore_ascii_case("rlike") {
					Some(Self::Rlike)
				} else if s.eq_ignore_ascii_case("share") {
					Some(Self::Share)
				} else if s.eq_ignore_ascii_case("start") {
					Some(Self::Start)
				} else if s.eq_ignore_ascii_case("table") {
					Some(Self::Table)
				} else if s.eq_ignore_ascii_case("union") {
					Some(Self::Union)
				} else if s.eq_ignore_ascii_case("using") {
					Some(Self::Using)
				} else if s.eq_ignore_ascii_case("value") {
					Some(Self::Value)
				} else if s.eq_ignore_ascii_case("where") {
					Some(Self::Where)
				} else if s.eq_ignore_ascii_case("write") {
					Some(Self::Write)
				} else {
					None
				}
			}
			6 => {
				if s.eq_ignore_ascii_case("binary") {
					Some(Self::Binary)
				} else if s.eq_ignore_ascii_case("change") {
					Some(Self::Change)
				} else if s.eq_ignore_ascii_case("column") {
					Some(Self::Column)
				} else if s.eq_ignore_ascii_case("commit") {
					Some(Self::Commit)
				} else if s.eq_ignore_ascii_case("create") {
					Some(Self::Create)
				} else if s.eq_ignore_ascii_case("delete") {
					Some(Self::Delete)
				} else if s.eq_ignore_ascii_case("errors") {
					Some(Self::Errors)
				} else if s.eq_ignore_ascii_case("except") {
					Some(Self::Except)
				} else if s.eq_ignore_ascii_case("exists") {
					Some(Self::Exists)
				} else if s.eq_ignore_ascii_case("global") {
					Some(Self::Global)
				} else if s.eq_ignore_ascii_case("having") {
					Some(Self::Having)
				} else if s.eq_ignore_ascii_case("ifnull") {
					Some(Self::Ifnull)
				} else if s.eq_ignore_ascii_case("ignore") {
					Some(Self::Ignore)
				} else if s.eq_ignore_ascii_case("insert") {
					Some(Self::Insert)
				} else if s.eq_ignore_ascii_case("locked") {
					Some(Self::Locked)
				} else if s.eq_ignore_ascii_case("modify") {
					Some(Self::Modify)
				} else if s.eq_ignore_ascii_case("nowait") {
					Some(Self::Nowait)
				} else if s.eq_ignore_ascii_case("nullif") {
					Some(Self::Nullif)
				} else if s.eq_ignore_ascii_case("offset") {
					Some(Self::Offset)
				} else if s.eq_ignore_ascii_case("regexp") {
					Some(Self::Regexp)
				} else if s.eq_ignore_ascii_case("rename") {
					Some(Self::Rename)
				} else if s.eq_ignore_ascii_case("schema") {
					Some(Self::Schema)
				} else if s.eq_ignore_ascii_case("select") {
					Some(Self::Select)
				} else if s.eq_ignore_ascii_case("status") {
					Some(Self::Status)
				} else if s.eq_ignore_ascii_case("tables") {
					Some(Self::Tables)
				} else if s.eq_ignore_ascii_case("unique") {
					Some(Self::Unique)
				} else if s.eq_ignore_ascii_case("unlock") {
					Some(Self::Unlock)
				} else if s.eq_ignore_ascii_case("update") {
					Some(Self::Update)
				} else if s.eq_ignore_ascii_case("values") {
					Some(Self::Values)
				} else {
					None
				}
			}
			7 => {
				if s.eq_ignore_ascii_case("between") {
					Some(Self::Between)
				} else if s.eq_ignore_ascii_case("cascade") {
					Some(Self::Cascade)
				} else if s.eq_ignore_ascii_case("collate") {
					Some(Self::Collate)
				} else if s.eq_ignore_ascii_case("columns") {
					Some(Self::Columns)
				} else if s.eq_ignore_ascii_case("convert") {
					Some(Self::Convert)
				} else if s.eq_ignore_ascii_case("default") {
					Some(Self::Default)
				} else if s.eq_ignore_ascii_case("explain") {
					Some(Self::Explain)
				} else if s.eq_ignore_ascii_case("foreign") {
					Some(Self::Foreign)
				} else if s.eq_ignore_ascii_case("persist") {
					Some(Self::Persist)
				} else if s.eq_ignore_ascii_case("primary") {
					Some(Self::Primary)
				} else if s.eq_ignore_ascii_case("release") {
					Some(Self::Release)
				} else if s.eq_ignore_ascii_case("replace") {
					Some(Self::Replace)
				} else if s.eq_ignore_ascii_case("schemas") {
					Some(Self::Schemas)
				} else if s.eq_ignore_ascii_case("session") {
					Some(Self::Session)
				} else if s.eq_ignore_ascii_case("unknown") {
					Some(Self::Unknown)
				} else {
					None
				}
			}
			8 => {
				if s.eq_ignore_ascii_case("coalesce") {
					Some(Self::Coalesce)
				} else if s.eq_ignore_ascii_case("database") {
					Some(Self::Database)
				} else if s.eq_ignore_ascii_case("describe") {
					Some(Self::Describe)
				} else if s.eq_ignore_ascii_case("distinct") {
					Some(Self::Distinct)
				} else if s.eq_ignore_ascii_case("interval") {
					Some(Self::Interval)
				} else if s.eq_ignore_ascii_case("restrict") {
					Some(Self::Restrict)
				} else if s.eq_ignore_ascii_case("rollback") {
					Some(Self::Rollback)
				} else if s.eq_ignore_ascii_case("truncate") {
					Some(Self::Truncate)
				} else if s.eq_ignore_ascii_case("warnings") {
					Some(Self::Warnings)
				} else {
					None
				}
			}
			9 => {
				if s.eq_ignore_ascii_case("databases") {
					Some(Self::Databases)
				} else if s.eq_ignore_ascii_case("duplicate") {
					Some(Self::Duplicate)
				} else if s.eq_ignore_ascii_case("intersect") {
					Some(Self::Intersect)
				} else if s.eq_ignore_ascii_case("recursive") {
					Some(Self::Recursive)
				} else if s.eq_ignore_ascii_case("savepoint") {
					Some(Self::Savepoint)
				} else if s.eq_ignore_ascii_case("temporary") {
					Some(Self::Temporary)
				} else if s.eq_ignore_ascii_case("variables") {
					Some(Self::Variables)
				} else {
					None
				}
			}
			10 => {
				if s.eq_ignore_ascii_case("constraint") {
					Some(Self::Constraint)
				} else if s.eq_ignore_ascii_case("references") {
					Some(Self::References)
				} else {
					None
				}
			}
			11 => {
				if s.eq_ignore_ascii_case("distinctrow") {
					Some(Self::Distinctrow)
				} else if s.eq_ignore_ascii_case("processlist") {
					Some(Self::Processlist)
				} else if s.eq_ignore_ascii_case("transaction") {
					Some(Self::Transaction)
				} else {
					None
				}
			}
			12 => {
				if s.eq_ignore_ascii_case("persist_only") {
					Some(Self::PersistOnly)
				} else {
					None
				}
			}
			13 => {
				if s.eq_ignore_ascii_case("straight_join") {
					Some(Self::StraightJoin)
				} else {
					None
				}
			}
			14 => {
				if s.eq_ignore_ascii_case("auto_increment") {
					Some(Self::AutoIncrement)
				} else {
					None
				}
			}
			_ => None,
		}
	}
}
