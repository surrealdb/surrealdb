//! GQL keyword definitions.
//!
//! Transcribed from the vendored grammar `doc/gql/GQL.g4` section 21.3
//! (lines 3276-3584) as distilled in `doc/gql/REFERENCE.md` section (b).
//! Keyword lookup is case-insensitive; reserved and prereserved words can
//! never be identifiers, while non-reserved words are valid wherever the
//! grammar requires a `regularIdentifier`.

/// Defines the [`Keyword`] enum from the three keyword classes of the GQL
/// grammar, together with classification methods and the `keyword_t!` macro
/// mapping keyword text to enum variants for use by the `t!` token macro.
macro_rules! keyword {
	(
		reserved => {
			$($r_name:ident => $r_value:tt,)*
		},
		prereserved => {
			$($p_name:ident => $p_value:tt,)*
		},
		non_reserved => {
			$($n_name:ident => $n_value:tt,)*
		} $(,)?
	) => {
		#[repr(u16)]
		#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
		pub enum Keyword {
			$($r_name,)*
			$($p_name,)*
			$($n_name,)*
		}

		impl Keyword {
			pub fn as_str(self) -> &'static str {
				match self {
					$(Keyword::$r_name => $r_value,)*
					$(Keyword::$p_name => $p_value,)*
					$(Keyword::$n_name => $n_value,)*
				}
			}

			/// Returns if this keyword is a reserved word, which can never be
			/// used as an identifier.
			pub fn is_reserved(self) -> bool {
				matches!(self, $(Keyword::$r_name)|*)
			}

			/// Returns if this keyword is reserved for future use by the GQL
			/// standard, and so can also never be used as an identifier.
			pub fn is_prereserved(self) -> bool {
				matches!(self, $(Keyword::$p_name)|*)
			}

			/// Returns if this keyword is a non-reserved word, which is valid
			/// wherever the grammar requires a regular identifier.
			pub fn is_non_reserved(self) -> bool {
				matches!(self, $(Keyword::$n_name)|*)
			}
		}

		macro_rules! keyword_t {
			$(($r_value) => {
				$crate::gql::token::Keyword::$r_name
			};)*
			$(($p_value) => {
				$crate::gql::token::Keyword::$p_name
			};)*
			$(($n_value) => {
				$crate::gql::token::Keyword::$n_name
			};)*
		}
	};
}

keyword! {
	reserved => {
		Abs => "ABS",
		Acos => "ACOS",
		All => "ALL",
		AllDifferent => "ALL_DIFFERENT",
		And => "AND",
		Any => "ANY",
		Array => "ARRAY",
		As => "AS",
		Asc => "ASC",
		Ascending => "ASCENDING",
		Asin => "ASIN",
		At => "AT",
		Atan => "ATAN",
		Avg => "AVG",
		Big => "BIG",
		Bigint => "BIGINT",
		Binary => "BINARY",
		Bool => "BOOL",
		Boolean => "BOOLEAN",
		Both => "BOTH",
		Btrim => "BTRIM",
		By => "BY",
		ByteLength => "BYTE_LENGTH",
		Bytes => "BYTES",
		Call => "CALL",
		Cardinality => "CARDINALITY",
		Case => "CASE",
		Cast => "CAST",
		Ceil => "CEIL",
		Ceiling => "CEILING",
		Char => "CHAR",
		CharLength => "CHAR_LENGTH",
		CharacterLength => "CHARACTER_LENGTH",
		Characteristics => "CHARACTERISTICS",
		Close => "CLOSE",
		Coalesce => "COALESCE",
		CollectList => "COLLECT_LIST",
		Commit => "COMMIT",
		Copy => "COPY",
		Cos => "COS",
		Cosh => "COSH",
		Cot => "COT",
		Count => "COUNT",
		Create => "CREATE",
		CurrentDate => "CURRENT_DATE",
		CurrentGraph => "CURRENT_GRAPH",
		CurrentPropertyGraph => "CURRENT_PROPERTY_GRAPH",
		CurrentSchema => "CURRENT_SCHEMA",
		CurrentTime => "CURRENT_TIME",
		CurrentTimestamp => "CURRENT_TIMESTAMP",
		Date => "DATE",
		Datetime => "DATETIME",
		Day => "DAY",
		Dec => "DEC",
		Decimal => "DECIMAL",
		Degrees => "DEGREES",
		Delete => "DELETE",
		Desc => "DESC",
		Descending => "DESCENDING",
		Detach => "DETACH",
		Distinct => "DISTINCT",
		Double => "DOUBLE",
		Drop => "DROP",
		Duration => "DURATION",
		DurationBetween => "DURATION_BETWEEN",
		ElementId => "ELEMENT_ID",
		Else => "ELSE",
		End => "END",
		Except => "EXCEPT",
		Exists => "EXISTS",
		Exp => "EXP",
		Filter => "FILTER",
		Finish => "FINISH",
		Float => "FLOAT",
		Float16 => "FLOAT16",
		Float32 => "FLOAT32",
		Float64 => "FLOAT64",
		Float128 => "FLOAT128",
		Float256 => "FLOAT256",
		Floor => "FLOOR",
		For => "FOR",
		From => "FROM",
		Group => "GROUP",
		Having => "HAVING",
		HomeGraph => "HOME_GRAPH",
		HomePropertyGraph => "HOME_PROPERTY_GRAPH",
		HomeSchema => "HOME_SCHEMA",
		Hour => "HOUR",
		If => "IF",
		In => "IN",
		Insert => "INSERT",
		Int => "INT",
		Integer => "INTEGER",
		Int8 => "INT8",
		Integer8 => "INTEGER8",
		Int16 => "INT16",
		Integer16 => "INTEGER16",
		Int32 => "INT32",
		Integer32 => "INTEGER32",
		Int64 => "INT64",
		Integer64 => "INTEGER64",
		Int128 => "INT128",
		Integer128 => "INTEGER128",
		Int256 => "INT256",
		Integer256 => "INTEGER256",
		Intersect => "INTERSECT",
		Interval => "INTERVAL",
		Is => "IS",
		Leading => "LEADING",
		Left => "LEFT",
		Let => "LET",
		Like => "LIKE",
		Limit => "LIMIT",
		List => "LIST",
		Ln => "LN",
		Local => "LOCAL",
		LocalDatetime => "LOCAL_DATETIME",
		LocalTime => "LOCAL_TIME",
		LocalTimestamp => "LOCAL_TIMESTAMP",
		Log => "LOG",
		Log10 => "LOG10",
		Lower => "LOWER",
		Ltrim => "LTRIM",
		Match => "MATCH",
		Max => "MAX",
		Min => "MIN",
		Minute => "MINUTE",
		Mod => "MOD",
		Month => "MONTH",
		Next => "NEXT",
		Nodetach => "NODETACH",
		Normalize => "NORMALIZE",
		Not => "NOT",
		Nothing => "NOTHING",
		Null => "NULL",
		Nulls => "NULLS",
		Nullif => "NULLIF",
		OctetLength => "OCTET_LENGTH",
		Of => "OF",
		Offset => "OFFSET",
		Optional => "OPTIONAL",
		Or => "OR",
		Order => "ORDER",
		Otherwise => "OTHERWISE",
		Parameter => "PARAMETER",
		Parameters => "PARAMETERS",
		Path => "PATH",
		PathLength => "PATH_LENGTH",
		Paths => "PATHS",
		PercentileCont => "PERCENTILE_CONT",
		PercentileDisc => "PERCENTILE_DISC",
		Power => "POWER",
		Precision => "PRECISION",
		PropertyExists => "PROPERTY_EXISTS",
		Radians => "RADIANS",
		Real => "REAL",
		Record => "RECORD",
		Remove => "REMOVE",
		Replace => "REPLACE",
		Reset => "RESET",
		Return => "RETURN",
		Right => "RIGHT",
		Rollback => "ROLLBACK",
		Rtrim => "RTRIM",
		Same => "SAME",
		Schema => "SCHEMA",
		Second => "SECOND",
		Select => "SELECT",
		Session => "SESSION",
		SessionUser => "SESSION_USER",
		Set => "SET",
		Signed => "SIGNED",
		Sin => "SIN",
		Sinh => "SINH",
		Size => "SIZE",
		Skip => "SKIP",
		Small => "SMALL",
		Smallint => "SMALLINT",
		Sqrt => "SQRT",
		Start => "START",
		StddevPop => "STDDEV_POP",
		StddevSamp => "STDDEV_SAMP",
		String => "STRING",
		Sum => "SUM",
		Tan => "TAN",
		Tanh => "TANH",
		Then => "THEN",
		Time => "TIME",
		Timestamp => "TIMESTAMP",
		Trailing => "TRAILING",
		Trim => "TRIM",
		Typed => "TYPED",
		Ubigint => "UBIGINT",
		Uint => "UINT",
		Uint8 => "UINT8",
		Uint16 => "UINT16",
		Uint32 => "UINT32",
		Uint64 => "UINT64",
		Uint128 => "UINT128",
		Uint256 => "UINT256",
		Union => "UNION",
		Unsigned => "UNSIGNED",
		Upper => "UPPER",
		Use => "USE",
		Usmallint => "USMALLINT",
		Value => "VALUE",
		Varbinary => "VARBINARY",
		Varchar => "VARCHAR",
		Variable => "VARIABLE",
		When => "WHEN",
		Where => "WHERE",
		With => "WITH",
		Xor => "XOR",
		Year => "YEAR",
		Yield => "YIELD",
		Zoned => "ZONED",
		ZonedDatetime => "ZONED_DATETIME",
		ZonedTime => "ZONED_TIME",
		// `BOOLEAN_LITERAL` (GQL.g4:3111): TRUE/FALSE/UNKNOWN are lexer-level
		// literals which behave as reserved words (never identifiers). The
		// parser surfaces them as boolean literals, not keywords.
		True => "TRUE",
		False => "FALSE",
		Unknown => "UNKNOWN",
	},
	prereserved => {
		Abstract => "ABSTRACT",
		Aggregate => "AGGREGATE",
		Aggregates => "AGGREGATES",
		Alter => "ALTER",
		Catalog => "CATALOG",
		Clear => "CLEAR",
		Clone => "CLONE",
		Constraint => "CONSTRAINT",
		CurrentRole => "CURRENT_ROLE",
		CurrentUser => "CURRENT_USER",
		Data => "DATA",
		Directory => "DIRECTORY",
		Dryrun => "DRYRUN",
		Exact => "EXACT",
		Existing => "EXISTING",
		Function => "FUNCTION",
		Gqlstatus => "GQLSTATUS",
		Grant => "GRANT",
		Instant => "INSTANT",
		Infinity => "INFINITY",
		Number => "NUMBER",
		Numeric => "NUMERIC",
		On => "ON",
		Open => "OPEN",
		Partition => "PARTITION",
		Procedure => "PROCEDURE",
		Product => "PRODUCT",
		Project => "PROJECT",
		Query => "QUERY",
		Records => "RECORDS",
		Reference => "REFERENCE",
		Rename => "RENAME",
		Revoke => "REVOKE",
		Substring => "SUBSTRING",
		SystemUser => "SYSTEM_USER",
		Temporal => "TEMPORAL",
		Unique => "UNIQUE",
		Unit => "UNIT",
		Values => "VALUES",
	},
	non_reserved => {
		Acyclic => "ACYCLIC",
		Binding => "BINDING",
		Bindings => "BINDINGS",
		Connecting => "CONNECTING",
		Destination => "DESTINATION",
		Different => "DIFFERENT",
		Directed => "DIRECTED",
		Edge => "EDGE",
		Edges => "EDGES",
		Element => "ELEMENT",
		Elements => "ELEMENTS",
		First => "FIRST",
		Graph => "GRAPH",
		Groups => "GROUPS",
		Keep => "KEEP",
		Label => "LABEL",
		Labeled => "LABELED",
		Labels => "LABELS",
		Last => "LAST",
		Nfc => "NFC",
		Nfd => "NFD",
		Nfkc => "NFKC",
		Nfkd => "NFKD",
		No => "NO",
		Node => "NODE",
		Normalized => "NORMALIZED",
		Only => "ONLY",
		Ordinality => "ORDINALITY",
		Property => "PROPERTY",
		Read => "READ",
		Relationship => "RELATIONSHIP",
		Relationships => "RELATIONSHIPS",
		Repeatable => "REPEATABLE",
		Shortest => "SHORTEST",
		Simple => "SIMPLE",
		Source => "SOURCE",
		Table => "TABLE",
		To => "TO",
		Trail => "TRAIL",
		Transaction => "TRANSACTION",
		Type => "TYPE",
		Undirected => "UNDIRECTED",
		Vertex => "VERTEX",
		Walk => "WALK",
		Without => "WITHOUT",
		Write => "WRITE",
		Zone => "ZONE",
	},
}

pub(crate) use keyword_t;
