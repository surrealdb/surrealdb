//! The declared SQL type of a field: one parser for the `SQL TYPE <dialect> '<text>'` clause, and
//! the rules that the write path, the wire layers, and `information_schema` read from it.
//!
//! A declared type is the text a client wrote for a column, such as `varchar(20)`,
//! `numeric(10,2)`, or `timestamp(3) with time zone`. [`DeclaredSqlType::parse`] reads it once
//! into its dialect, base name, modifiers, attributes, and array dimensions. Every accessor and
//! check reads that structure, so DDL lowering, enforcement, and reporting agree on one reading.

use std::borrow::Cow;
use std::fmt::Write as _;

use super::{SqlType, SqlTypeDialect};

/// A declared type text that does not read as a SQL type of its dialect.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid SQL type \"{text}\": {reason}")]
pub struct DeclaredTypeError {
	/// The text as it was declared.
	pub text: String,
	/// What is wrong with it.
	pub reason: String,
}

/// The unit a length limit counts in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LengthUnit {
	/// Unicode scalar values (PostgreSQL character types, MySQL `CHAR` and `VARCHAR`).
	Chars,
	/// Bytes (MySQL `BINARY`, `VARBINARY`, the `BLOB` and `TEXT` families).
	Bytes,
	/// Bits of a PostgreSQL bit string.
	Bits,
}

/// A maximum length, in the unit that counts it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Length {
	pub max: u64,
	pub unit: LengthUnit,
}

/// How a value of the type is padded to its declared length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Padding {
	/// The value is stored and returned as written.
	None,
	/// PostgreSQL `char(n)` returns the value padded with spaces to `n` characters.
	Blank,
	/// MySQL `BINARY(n)` stores the value padded with `0x00` to `n` bytes.
	Zero,
}

/// The JSON representation a `json` or `jsonb` column keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsonFlavour {
	/// The document is kept as the text it was written with (PostgreSQL `json`).
	Text,
	/// The document is kept as a value, so of duplicate keys the last one stays (PostgreSQL
	/// `jsonb`, and MySQL `JSON`, which stores a binary form).
	Binary,
}

/// The field qualifier of a PostgreSQL interval type, such as the `day to second` of
/// `interval day to second(3)`. It names the fields a value of the type keeps: a written interval
/// is truncated toward zero below the finest of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntervalFields {
	Year,
	Month,
	Day,
	Hour,
	Minute,
	Second,
	YearToMonth,
	DayToHour,
	DayToMinute,
	DayToSecond,
	HourToMinute,
	HourToSecond,
	MinuteToSecond,
}

/// The finest field an interval type keeps, below which a written value is truncated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntervalFinestField {
	/// Whole years of months; no days and no time.
	Year,
	/// Whole months; no days and no time.
	Month,
	/// Months and days; no time.
	Day,
	/// Time truncated to whole hours.
	Hour,
	/// Time truncated to whole minutes.
	Minute,
	/// Time kept, to the type's fractional-second digits.
	Second,
}

impl IntervalFields {
	const ALL: [Self; 13] = [
		Self::Year,
		Self::Month,
		Self::Day,
		Self::Hour,
		Self::Minute,
		Self::Second,
		Self::YearToMonth,
		Self::DayToHour,
		Self::DayToMinute,
		Self::DayToSecond,
		Self::HourToMinute,
		Self::HourToSecond,
		Self::MinuteToSecond,
	];

	/// The qualifier as PostgreSQL writes it, in lower case, such as `day to second`.
	pub fn words(self) -> &'static str {
		match self {
			Self::Year => "year",
			Self::Month => "month",
			Self::Day => "day",
			Self::Hour => "hour",
			Self::Minute => "minute",
			Self::Second => "second",
			Self::YearToMonth => "year to month",
			Self::DayToHour => "day to hour",
			Self::DayToMinute => "day to minute",
			Self::DayToSecond => "day to second",
			Self::HourToMinute => "hour to minute",
			Self::HourToSecond => "hour to second",
			Self::MinuteToSecond => "minute to second",
		}
	}

	/// The qualifier written as `words`, lower case with single spaces.
	pub fn from_words(words: &str) -> Option<Self> {
		Self::ALL.into_iter().find(|fields| fields.words() == words)
	}

	/// The finest field the qualifier keeps.
	pub fn finest(self) -> IntervalFinestField {
		match self {
			Self::Year => IntervalFinestField::Year,
			Self::Month | Self::YearToMonth => IntervalFinestField::Month,
			Self::Day => IntervalFinestField::Day,
			Self::Hour | Self::DayToHour => IntervalFinestField::Hour,
			Self::Minute | Self::DayToMinute | Self::HourToMinute => IntervalFinestField::Minute,
			Self::Second | Self::DayToSecond | Self::HourToSecond | Self::MinuteToSecond => {
				IntervalFinestField::Second
			}
		}
	}

	/// The field mask PostgreSQL stores in the upper half of an interval's `typmod`: one bit per
	/// field (`INTERVAL_MASK` of `MONTH` 1, `YEAR` 2, `DAY` 3, `HOUR` 10, `MINUTE` 11, `SECOND`
	/// 12), set for every field from the first to the last of the qualifier.
	pub fn pg_range_mask(self) -> i32 {
		const MONTH: i32 = 1 << 1;
		const YEAR: i32 = 1 << 2;
		const DAY: i32 = 1 << 3;
		const HOUR: i32 = 1 << 10;
		const MINUTE: i32 = 1 << 11;
		const SECOND: i32 = 1 << 12;
		match self {
			Self::Year => YEAR,
			Self::Month => MONTH,
			Self::Day => DAY,
			Self::Hour => HOUR,
			Self::Minute => MINUTE,
			Self::Second => SECOND,
			Self::YearToMonth => YEAR | MONTH,
			Self::DayToHour => DAY | HOUR,
			Self::DayToMinute => DAY | HOUR | MINUTE,
			Self::DayToSecond => DAY | HOUR | MINUTE | SECOND,
			Self::HourToMinute => HOUR | MINUTE,
			Self::HourToSecond => HOUR | MINUTE | SECOND,
			Self::MinuteToSecond => MINUTE | SECOND,
		}
	}

	/// The qualifier whose PostgreSQL field mask is `mask`, if one has it.
	pub fn from_pg_range_mask(mask: i32) -> Option<Self> {
		Self::ALL.into_iter().find(|fields| fields.pg_range_mask() == mask)
	}
}

/// A reason a value does not fit its declared type. `code` is the SQLSTATE the PostgreSQL wire
/// reports and the MySQL wire maps from it.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct TypeViolation {
	pub code: &'static str,
	pub message: String,
}

/// SQLSTATE `22001`, string data right truncation. MySQL reports its `ER_DATA_TOO_LONG` with the
/// same state.
const STRING_TRUNCATION: &str = "22001";
/// SQLSTATE `22026`, string data length mismatch (a bit string of the wrong length).
const BIT_LENGTH_MISMATCH: &str = "22026";
/// MySQL column attributes that can follow a type without a modifier list.
const ATTRIBUTE_WORDS: &[&str] = &["unsigned", "signed", "zerofill"];

/// A declared SQL type read into its parts.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DeclaredSqlType {
	dialect: SqlTypeDialect,
	/// The base name in lower case with single spaces, such as `character varying` or
	/// `timestamp with time zone`. A temporal base keeps its zone words so the base is unique.
	base: String,
	/// The modifiers inside the first parenthesised group, as written, without surrounding space.
	modifiers: Vec<String>,
	/// Words after the modifiers that are not part of the base (MySQL `unsigned`, `zerofill`).
	attributes: Vec<String>,
	/// One entry per array dimension: `None` for `[]`, `Some(n)` for `[n]`.
	array_dimensions: Vec<Option<u64>>,
}

impl DeclaredSqlType {
	/// Read the text of a `SQL TYPE` clause in `dialect`.
	pub fn parse(dialect: SqlTypeDialect, text: &str) -> Result<Self, DeclaredTypeError> {
		let error = |reason: &str| DeclaredTypeError {
			text: text.to_string(),
			reason: reason.to_string(),
		};
		let normalised = normalise(text);
		if normalised.is_empty() {
			return Err(error("the type is empty"));
		}
		let (body, array_dimensions) = split_array_dimensions(&normalised)
			.ok_or_else(|| error("an array dimension is not a count or empty brackets"))?;

		let (head, modifiers, tail) = match body.find('(') {
			Some(open) => {
				let close = matching_close(&body, open)
					.ok_or_else(|| error("a modifier list is not closed"))?;
				let inner = &body[open + 1..close];
				let modifiers = split_modifiers(inner)
					.ok_or_else(|| error("a modifier list has an unclosed quote"))?;
				(body[..open].trim().to_string(), modifiers, body[close + 1..].trim().to_string())
			}
			None => (body.trim().to_string(), Vec::new(), String::new()),
		};
		if head.is_empty() {
			return Err(error("the type has no name"));
		}

		// Attribute words written without a modifier list (`int unsigned`) leave the base at once.
		let mut head_words: Vec<String> = head.split(' ').map(str::to_string).collect();
		let mut attributes = Vec::new();
		loop {
			let is_attribute = head_words.len() > 1
				&& head_words.last().is_some_and(|word| ATTRIBUTE_WORDS.contains(&word.as_str()));
			if !is_attribute {
				break;
			}
			if let Some(word) = head_words.pop() {
				attributes.insert(0, word);
			}
		}
		let head = head_words.join(" ");

		let base = if tail.starts_with("with ") || tail.starts_with("without ") {
			// PostgreSQL writes the modifier before the zone words: `timestamp(3) with time zone`.
			format!("{head} {tail}")
		} else {
			if !tail.is_empty() {
				attributes.extend(tail.split(' ').map(str::to_string));
			}
			head
		};

		let parsed = Self {
			dialect,
			base,
			modifiers,
			attributes,
			array_dimensions,
		};
		parsed.validate().map_err(|reason| error(&reason))?;
		Ok(parsed)
	}

	/// Read the declared type of a field definition.
	pub fn from_sql_type(sql_type: &SqlType) -> Result<Self, DeclaredTypeError> {
		Self::parse(sql_type.dialect, &sql_type.text)
	}

	/// The dialect the type is written in.
	pub fn dialect(&self) -> SqlTypeDialect {
		self.dialect
	}

	/// The base name, lower case, without modifiers or array dimensions.
	pub fn base(&self) -> &str {
		&self.base
	}

	/// The modifiers as written.
	pub fn modifiers(&self) -> &[String] {
		&self.modifiers
	}

	/// The attributes that follow the modifiers, such as MySQL `unsigned`.
	pub fn attributes(&self) -> &[String] {
		&self.attributes
	}

	/// One entry per array dimension.
	pub fn array_dimensions(&self) -> &[Option<u64>] {
		&self.array_dimensions
	}

	/// The type of an element of an array type: this type with one array dimension fewer. `None`
	/// for a type that is not an array.
	pub fn element(&self) -> Option<Self> {
		if self.array_dimensions.is_empty() {
			return None;
		}
		let mut element = self.clone();
		element.array_dimensions.pop();
		Some(element)
	}

	/// The canonical text: lower case, single spaces, the modifiers as written joined by commas,
	/// and one `[]` or `[n]` per array dimension. Parsing the canonical text yields the same type.
	pub fn canonical_text(&self) -> String {
		let mut out = String::new();
		if let Some((first, rest)) = self
			.base
			.split_once(' ')
			.filter(|(_, rest)| rest.starts_with("with ") || rest.starts_with("without "))
		{
			out.push_str(first);
			push_modifiers(&mut out, &self.modifiers);
			out.push(' ');
			out.push_str(rest);
		} else {
			out.push_str(&self.base);
			push_modifiers(&mut out, &self.modifiers);
			for attribute in &self.attributes {
				out.push(' ');
				out.push_str(attribute);
			}
		}
		for dimension in &self.array_dimensions {
			match dimension {
				Some(bound) => {
					let _ = write!(out, "[{bound}]");
				}
				None => out.push_str("[]"),
			}
		}
		out
	}

	/// The maximum length the type admits, if it has one. A PostgreSQL `char` without a length is
	/// `char(1)`, a `bpchar` without one is unbounded, and a MySQL `TEXT` without one is the 64 KiB
	/// limit.
	pub fn max_length(&self) -> Option<Length> {
		if !self.array_dimensions.is_empty() {
			return None;
		}
		let modifier = self.modifier_u64(0);
		let chars = |max| {
			Some(Length {
				max,
				unit: LengthUnit::Chars,
			})
		};
		let bytes = |max| {
			Some(Length {
				max,
				unit: LengthUnit::Bytes,
			})
		};
		let bits = |max| {
			Some(Length {
				max,
				unit: LengthUnit::Bits,
			})
		};
		match (self.dialect, self.base.as_str()) {
			(SqlTypeDialect::PostgreSql, "varchar" | "character varying") => {
				modifier.and_then(chars)
			}
			(SqlTypeDialect::PostgreSql, "char" | "character") => chars(modifier.unwrap_or(1)),
			// A `bpchar` without a length is unbounded.
			(SqlTypeDialect::PostgreSql, "bpchar") => modifier.and_then(chars),
			(SqlTypeDialect::PostgreSql, "bit") => bits(modifier.unwrap_or(1)),
			(SqlTypeDialect::PostgreSql, "varbit" | "bit varying") => modifier.and_then(bits),
			(SqlTypeDialect::MySql, "char" | "nchar" | "national char" | "national character") => {
				chars(modifier.unwrap_or(1))
			}
			(
				SqlTypeDialect::MySql,
				"varchar"
				| "nvarchar"
				| "national varchar"
				| "char varying"
				| "character varying"
				| "national char varying"
				| "national character varying",
			) => modifier.and_then(chars),
			(SqlTypeDialect::MySql, "tinytext") => bytes(255),
			// `BLOB(n)` picks the smallest class that holds `n` bytes, and `TEXT(n)` the smallest
			// that holds `n` characters of four bytes each; the column holds what that class holds.
			(SqlTypeDialect::MySql, "text") => bytes(text_storage_class(modifier)),
			(SqlTypeDialect::MySql, "mediumtext") => bytes(16_777_215),
			(SqlTypeDialect::MySql, "longtext") => bytes(4_294_967_295),
			(SqlTypeDialect::MySql, "binary") => bytes(modifier.unwrap_or(1)),
			(SqlTypeDialect::MySql, "varbinary") => modifier.and_then(bytes),
			(SqlTypeDialect::MySql, "tinyblob") => bytes(255),
			(SqlTypeDialect::MySql, "blob") => bytes(storage_class(modifier)),
			(SqlTypeDialect::MySql, "mediumblob") => bytes(16_777_215),
			(SqlTypeDialect::MySql, "longblob") => bytes(4_294_967_295),
			(SqlTypeDialect::MySql, "bit") => bits(modifier.unwrap_or(1)),
			_ => None,
		}
	}

	/// How a value is padded to the declared length.
	pub fn padded(&self) -> Padding {
		if !self.array_dimensions.is_empty() {
			return Padding::None;
		}
		match (self.dialect, self.base.as_str()) {
			(SqlTypeDialect::PostgreSql, "char" | "character") => Padding::Blank,
			(SqlTypeDialect::PostgreSql, "bpchar") if !self.modifiers.is_empty() => Padding::Blank,
			(SqlTypeDialect::MySql, "binary") => Padding::Zero,
			_ => Padding::None,
		}
	}

	/// The precision of a `numeric` or `decimal` type. PostgreSQL `numeric` without a precision
	/// is unconstrained (`None`); MySQL `DECIMAL` without one is `DECIMAL(10,0)`.
	pub fn precision(&self) -> Option<u64> {
		if !self.is_decimal() {
			return None;
		}
		match self.modifier_u64(0) {
			Some(precision) => Some(precision),
			None if self.dialect == SqlTypeDialect::MySql => Some(10),
			None => None,
		}
	}

	/// The scale of a `numeric` or `decimal` type, when it has one. A scale is written as the
	/// second modifier; a precision alone has scale 0.
	pub fn scale(&self) -> Option<i64> {
		if !self.is_decimal() {
			return None;
		}
		match self.modifiers.get(1) {
			Some(scale) => scale.parse::<i64>().ok(),
			None if self.modifiers.is_empty() && self.dialect != SqlTypeDialect::MySql => None,
			None => Some(0),
		}
	}

	/// The field qualifier of an interval type, such as the `day to second` of
	/// `interval day to second(3)`. `None` for an interval of every field, for any other type,
	/// and for an array.
	pub fn interval_fields(&self) -> Option<IntervalFields> {
		if !self.array_dimensions.is_empty() {
			return None;
		}
		self.base.strip_prefix("interval ").and_then(IntervalFields::from_words)
	}

	/// The fractional-second digits a time, timestamp, or interval type keeps. PostgreSQL keeps
	/// microseconds (6) by default and MySQL keeps whole seconds (0). PostgreSQL caps the value at
	/// 6, as the server does. An interval with a field qualifier has them too: its `SECOND(p)`
	/// precision, or 6.
	pub fn fractional_seconds(&self) -> Option<u32> {
		if !self.array_dimensions.is_empty() {
			return None;
		}
		let temporal = self.interval_fields().is_some()
			|| matches!(
				self.base.as_str(),
				"time"
					| "time with time zone"
					| "time without time zone"
					| "timetz" | "timestamp"
					| "timestamp with time zone"
					| "timestamp without time zone"
					| "timestamptz" | "datetime"
					| "interval"
			);
		if !temporal {
			return None;
		}
		let default = match self.dialect {
			SqlTypeDialect::PostgreSql => 6,
			SqlTypeDialect::MySql | SqlTypeDialect::DynamoDb => 0,
		};
		let digits = self
			.modifier_u64(0)
			.map_or(default, |digits| u32::try_from(digits).unwrap_or(u32::MAX));
		Some(match self.dialect {
			SqlTypeDialect::PostgreSql => digits.min(6),
			SqlTypeDialect::MySql | SqlTypeDialect::DynamoDb => digits,
		})
	}

	/// The JSON representation of a `json` or `jsonb` type.
	pub fn json_flavour(&self) -> Option<JsonFlavour> {
		if !self.array_dimensions.is_empty() {
			return None;
		}
		match (self.dialect, self.base.as_str()) {
			(SqlTypeDialect::PostgreSql, "json") => Some(JsonFlavour::Text),
			(SqlTypeDialect::PostgreSql, "jsonb") | (SqlTypeDialect::MySql, "json") => {
				Some(JsonFlavour::Binary)
			}
			_ => None,
		}
	}

	/// Check a text value against the declared length. A text longer than a character length
	/// (`char(n)`, `varchar(n)` and their MySQL forms) whose excess is all spaces drops the excess
	/// and is accepted, as both dialects store it; any other excess is an error. A `CHAR(n)` of
	/// either dialect drops every trailing space, which is insignificant in it, so `'ab'` and
	/// `'ab '` are one value. A MySQL `TEXT` family limit
	/// counts bytes. `column` names the column in MySQL's message.
	pub fn check_text<'a>(
		&self,
		value: &'a str,
		column: &str,
	) -> Result<Cow<'a, str>, TypeViolation> {
		let Some(length) = self.max_length() else {
			return Ok(Cow::Borrowed(value));
		};
		match length.unit {
			LengthUnit::Chars => {
				let limit = usize::try_from(length.max).unwrap_or(usize::MAX);
				let mut kept = value;
				if self.base_is_fixed_width_text() {
					kept = kept.trim_end_matches(' ');
				}
				let mut count = kept.chars().count();
				while count > limit && kept.ends_with(' ') {
					kept = &kept[..kept.len() - 1];
					count -= 1;
				}
				if count > limit {
					return Err(self.too_long(column, length.max));
				}
				Ok(Cow::Borrowed(kept))
			}
			LengthUnit::Bytes => {
				if u64::try_from(value.len()).unwrap_or(u64::MAX) > length.max {
					return Err(self.too_long(column, length.max));
				}
				Ok(Cow::Borrowed(value))
			}
			LengthUnit::Bits => Ok(Cow::Borrowed(value)),
		}
	}

	/// Check a binary value against the declared length. A MySQL `BINARY(n)` is padded with
	/// `0x00` to `n` bytes; a longer value is an error.
	pub fn check_bytes(&self, mut value: Vec<u8>, column: &str) -> Result<Vec<u8>, TypeViolation> {
		let Some(length) = self.max_length() else {
			return Ok(value);
		};
		if length.unit != LengthUnit::Bytes {
			return Ok(value);
		}
		if u64::try_from(value.len()).unwrap_or(u64::MAX) > length.max {
			return Err(self.too_long(column, length.max));
		}
		if self.padded() == Padding::Zero {
			let target = usize::try_from(length.max).unwrap_or(usize::MAX);
			value.resize(target, 0);
		}
		Ok(value)
	}

	/// Check a PostgreSQL bit string of `0` and `1` against `bit(n)` or `bit varying(n)`. `bit(n)`
	/// takes exactly `n` bits; `bit varying(n)` takes at most `n`.
	pub fn check_bits(&self, bits: &str) -> Result<(), TypeViolation> {
		let Some(length) = self.max_length() else {
			return Ok(());
		};
		if length.unit != LengthUnit::Bits {
			return Ok(());
		}
		let len = u64::try_from(bits.len()).unwrap_or(u64::MAX);
		let fixed = self.base == "bit";
		if fixed && len != length.max {
			return Err(TypeViolation {
				code: BIT_LENGTH_MISMATCH,
				message: format!("bit string length {len} does not match type bit({})", length.max),
			});
		}
		if !fixed && len > length.max {
			return Err(TypeViolation {
				code: STRING_TRUNCATION,
				message: format!("bit string too long for type bit varying({})", length.max),
			});
		}
		Ok(())
	}

	/// The fractional-second value rounded to [`Self::fractional_seconds`] digits, rounding half
	/// away from zero. `nanos` is the sub-second part, below 1,000,000,000. The result is the new
	/// sub-second part, and `true` when the rounding carries into the next whole second.
	pub fn round_nanos(&self, nanos: u32) -> (u32, bool) {
		let Some(digits) = self.fractional_seconds() else {
			return (nanos, false);
		};
		if digits >= 9 {
			return (nanos, false);
		}
		let unit = 10u32.pow(9 - digits);
		let remainder = nanos % unit;
		let truncated = nanos - remainder;
		if remainder * 2 < unit {
			return (truncated, false);
		}
		let rounded = truncated + unit;
		if rounded >= 1_000_000_000 {
			(rounded - 1_000_000_000, true)
		} else {
			(rounded, false)
		}
	}

	fn is_decimal(&self) -> bool {
		self.array_dimensions.is_empty()
			&& matches!(self.base.as_str(), "numeric" | "decimal" | "dec" | "fixed")
	}

	fn base_is_fixed_width_text(&self) -> bool {
		matches!(
			(self.dialect, self.base.as_str()),
			(SqlTypeDialect::PostgreSql, "char" | "character" | "bpchar")
				| (
					SqlTypeDialect::MySql,
					"char" | "nchar" | "national char" | "national character"
				)
		)
	}

	fn modifier_u64(&self, index: usize) -> Option<u64> {
		self.modifiers.get(index).and_then(|m| m.parse::<u64>().ok())
	}

	fn too_long(&self, column: &str, max: u64) -> TypeViolation {
		let message = match self.dialect {
			SqlTypeDialect::MySql => format!("Data too long for column '{column}' at row 1"),
			SqlTypeDialect::PostgreSql | SqlTypeDialect::DynamoDb => {
				let name = match self.base.as_str() {
					"character varying" | "varchar" => "character varying",
					_ => "character",
				};
				format!("value too long for type {name}({max})")
			}
		};
		TypeViolation {
			code: STRING_TRUNCATION,
			message,
		}
	}

	/// The words after a type are MySQL's numeric attributes, `unsigned`, `signed` and
	/// `zerofill`, on a MySQL numeric type. Any other word, or one of these on another type or in
	/// PostgreSQL, is refused rather than stored and ignored, since `unsigned` changes what the
	/// type admits.
	fn validate_attributes(&self) -> Result<(), String> {
		let Some(first) = self.attributes.first() else {
			return Ok(());
		};
		if self.dialect != SqlTypeDialect::MySql {
			return Err(format!("\"{first}\" is not part of a PostgreSQL type"));
		}
		if let Some(bad) =
			self.attributes.iter().find(|word| !ATTRIBUTE_WORDS.contains(&word.as_str()))
		{
			return Err(format!("\"{bad}\" is not a column attribute"));
		}
		let numeric = matches!(
			self.base.as_str(),
			"tinyint"
				| "smallint" | "mediumint"
				| "middleint"
				| "int" | "integer"
				| "bigint" | "int1"
				| "int2" | "int3"
				| "int4" | "int8"
				| "decimal" | "numeric"
				| "dec" | "fixed"
				| "float" | "double"
				| "double precision"
				| "real" | "float4"
				| "float8"
		);
		if !numeric {
			return Err(format!("{} takes no {first} attribute", self.base));
		}
		Ok(())
	}

	/// How many modifiers the dialect's grammar gives the type. A decimal takes a precision and
	/// a scale, and MySQL's floating-point types a width and a scale. A length, a width, a
	/// fractional-second precision, a MySQL integer display width or a vector dimension is one
	/// modifier; every other type, a user-defined one included, takes none.
	fn most_modifiers(&self) -> usize {
		match (self.dialect, self.base.as_str()) {
			(_, "numeric" | "decimal" | "dec" | "fixed") => 2,
			(
				SqlTypeDialect::MySql,
				"float" | "double" | "double precision" | "real" | "float4" | "float8",
			) => 2,
			(
				SqlTypeDialect::MySql,
				"tinyint" | "smallint" | "mediumint" | "middleint" | "int" | "integer" | "bigint"
				| "int1" | "int2" | "int3" | "int4" | "int8" | "year" | "text" | "blob",
			) => 1,
			(
				SqlTypeDialect::PostgreSql,
				"float"
				| "bpchar"
				| "varbit"
				| "bit varying"
				| "timetz"
				| "timestamptz"
				| "time with time zone"
				| "time without time zone"
				| "timestamp with time zone"
				| "timestamp without time zone"
				| "interval"
				| "vector"
				| "halfvec"
				| "sparsevec",
			) => 1,
			(_, base) if base.starts_with("interval ") => 1,
			(
				_,
				"char"
				| "character"
				| "nchar"
				| "national char"
				| "national character"
				| "varchar"
				| "character varying"
				| "char varying"
				| "nvarchar"
				| "national varchar"
				| "national character varying"
				| "national char varying"
				| "bit"
				| "binary"
				| "varbinary"
				| "time"
				| "timestamp"
				| "datetime",
			) => 1,
			_ => 0,
		}
	}

	/// The checks that need the whole type: a modifier that is not a count where one is needed,
	/// a MySQL fractional-second digit beyond 6, and a `numeric` precision of zero.
	fn validate(&self) -> Result<(), String> {
		self.validate_attributes()?;
		// A MySQL spatial type's `SRID` is one a geometry value can be in: values hold longitude
		// and latitude and carry no SRID, so only 0 (unspecified) and 4326 (WGS 84) describe
		// them, as the binary protocol's geometry parameters admit.
		if self.dialect == SqlTypeDialect::MySql
			&& let Some((_, srid)) = self.base.split_once(" srid ")
			&& !matches!(srid, "0" | "4326")
		{
			return Err(format!("SRID {srid} is not supported; only SRID 0 and 4326 are"));
		}
		if matches!(self.base.as_str(), "enum" | "set") {
			return Ok(());
		}
		// An interval's field qualifier is one PostgreSQL's grammar has, and only a qualifier
		// ending in `second` takes a precision.
		if let Some(words) = self.base.strip_prefix("interval ") {
			let Some(fields) = IntervalFields::from_words(words) else {
				return Err(format!("\"{words}\" is not an interval field qualifier"));
			};
			if !self.modifiers.is_empty() && fields.finest() != IntervalFinestField::Second {
				return Err(format!("interval {words} takes no precision"));
			}
		}
		if let Some(bad) = self.modifiers.iter().find(|m| m.parse::<i64>().is_err()) {
			// Enum members are the only modifiers that are not counts.
			return Err(format!("the modifier \"{bad}\" is not a number"));
		}
		// PostgreSQL `float(p)` takes 1 to 53 binary digits.
		if self.dialect == SqlTypeDialect::PostgreSql
			&& self.base == "float"
			&& let Some(bits) = self.modifiers.first().and_then(|m| m.parse::<i64>().ok())
		{
			if bits < 1 {
				return Err("precision for type float must be at least 1 bit".to_string());
			}
			if bits > 53 {
				return Err("precision for type float must be less than 54 bits".to_string());
			}
		}
		let most = self.most_modifiers();
		if most == 0 && !self.modifiers.is_empty() {
			return Err(format!("{} takes no modifiers", self.base));
		}
		if self.modifiers.len() > most {
			return Err(format!("{} takes at most {most} modifier(s)", self.base));
		}
		// Only a PostgreSQL `numeric` scale, the second modifier, may be negative; a negative
		// length or precision would read as no limit at all.
		let negative_allowed = |index: usize| {
			index == 1 && self.is_decimal() && self.dialect == SqlTypeDialect::PostgreSql
		};
		if let Some((_, bad)) = self
			.modifiers
			.iter()
			.enumerate()
			.find(|(index, m)| m.starts_with('-') && !negative_allowed(*index))
		{
			return Err(format!("the modifier \"{bad}\" must not be negative"));
		}
		if self.is_decimal() && self.modifiers.first().is_some_and(|p| p.parse::<i64>() == Ok(0)) {
			return Err("the precision of a decimal must be at least 1".to_string());
		}
		// MySQL holds a `DECIMAL` of at most 65 digits, of which at most 30 are after the point.
		if self.is_decimal() && self.dialect == SqlTypeDialect::MySql {
			let (precision, scale) = (self.precision().unwrap_or(10), self.scale().unwrap_or(0));
			if precision > 65 {
				return Err("the precision of a decimal must be at most 65".to_string());
			}
			if scale > 30 {
				return Err("the scale of a decimal must be at most 30".to_string());
			}
			if u64::try_from(scale).is_ok_and(|scale| scale > precision) {
				return Err("the scale of a decimal must not exceed its precision".to_string());
			}
		}
		// PostgreSQL `numeric` takes a precision of at most 1000 and a scale from -1000 to 1000.
		if self.is_decimal() && self.dialect == SqlTypeDialect::PostgreSql {
			// A modifier too large to parse is out of range as well.
			let precision = self.modifiers.first().map(|p| p.parse::<u64>().unwrap_or(u64::MAX));
			if precision.is_some_and(|precision| precision > 1000) {
				return Err("the precision of a numeric must be at most 1000".to_string());
			}
			let scale = self.modifiers.get(1).map(|s| s.parse::<i64>().unwrap_or(i64::MAX));
			if scale.is_some_and(|scale| !(-1000..=1000).contains(&scale)) {
				return Err("the scale of a numeric must be from -1000 to 1000".to_string());
			}
		}
		if self.dialect == SqlTypeDialect::MySql
			&& self.fractional_seconds().is_some_and(|digits| digits > 6)
		{
			return Err("the fractional seconds precision must be at most 6".to_string());
		}
		// MySQL `BIT(n)` holds 1 to 64 bits.
		if self.dialect == SqlTypeDialect::MySql
			&& self.array_dimensions.is_empty()
			&& self.base == "bit"
			&& self.modifier_u64(0).is_some_and(|width| !(1..=64).contains(&width))
		{
			return Err("the width of a bit must be from 1 to 64".to_string());
		}
		// A padded type fills each value to its length, so the length is held to the dialect's
		// own limit: 255 bytes for MySQL `BINARY`, 10485760 characters for PostgreSQL `char`.
		let padding_limit = match self.padded() {
			Padding::Zero => Some(255),
			Padding::Blank => Some(10_485_760),
			Padding::None => None,
		};
		if let (Some(limit), Some(length)) = (padding_limit, self.max_length())
			&& length.max > limit
		{
			return Err(format!("the length of {} must be at most {limit}", self.base));
		}
		Ok(())
	}
}

/// The capacity in bytes of the MySQL `TEXT` or `BLOB` class a `TEXT(n)` or `BLOB(n)` selects:
/// the smallest of `TINY` (255), plain (65535), `MEDIUM` (16777215) and `LONG` (4294967295) that
/// holds `n`. Without `n` it is the plain class.
pub fn storage_class(modifier: Option<u64>) -> u64 {
	match modifier {
		Some(n) if n <= 255 => 255,
		Some(n) if n > 65_535 && n <= 16_777_215 => 16_777_215,
		Some(n) if n > 16_777_215 => 4_294_967_295,
		Some(_) | None => 65_535,
	}
}

/// The bytes a `character` of a MySQL `TEXT(n)` is sized as: the widest character of `utf8mb4`.
const MYSQL_BYTES_PER_CHARACTER: u64 = 4;

/// [`storage_class`] of a MySQL `TEXT(n)`, whose `n` counts characters: the class that holds `n`
/// characters of [`MYSQL_BYTES_PER_CHARACTER`] bytes.
pub fn text_storage_class(modifier: Option<u64>) -> u64 {
	storage_class(modifier.map(|n| n.saturating_mul(MYSQL_BYTES_PER_CHARACTER)))
}

/// Lower case, with each run of whitespace collapsed to one space and none at either end. A
/// quoted literal, such as an `ENUM` or `SET` member, is kept as written: its case and spacing are
/// its value.
fn normalise(text: &str) -> String {
	let mut out = String::with_capacity(text.len());
	let mut quoted = false;
	let mut space = false;
	for ch in text.trim().chars() {
		if quoted {
			out.push(ch);
			quoted = ch != '\'';
		} else if ch.is_whitespace() {
			space = true;
		} else {
			if space {
				out.push(' ');
				space = false;
			}
			out.push(ch.to_ascii_lowercase());
			quoted = ch == '\'';
		}
	}
	out
}

/// Split trailing `[]` or `[n]` groups from the body of a type. Returns the body without them and
/// one entry per group, in the order written.
fn split_array_dimensions(text: &str) -> Option<(String, Vec<Option<u64>>)> {
	let mut body = text.trim_end();
	let mut dimensions = Vec::new();
	while let Some(stripped) = body.strip_suffix(']') {
		let open = stripped.rfind('[')?;
		let inner = stripped[open + 1..].trim();
		let dimension = if inner.is_empty() {
			None
		} else {
			Some(inner.parse::<u64>().ok()?)
		};
		dimensions.push(dimension);
		body = stripped[..open].trim_end();
	}
	dimensions.reverse();
	Some((body.to_string(), dimensions))
}

/// The index of the `)` that closes the `(` at `open`, outside quotes.
fn matching_close(text: &str, open: usize) -> Option<usize> {
	let mut depth = 0usize;
	let mut quoted = false;
	for (index, ch) in text.char_indices().skip_while(|(i, _)| *i < open) {
		match ch {
			'\'' => quoted = !quoted,
			'(' if !quoted => depth += 1,
			')' if !quoted => {
				depth -= 1;
				if depth == 0 {
					return Some(index);
				}
			}
			_ => {}
		}
	}
	None
}

/// Split a modifier list at its top-level commas, outside quotes. A quoted enum member keeps its
/// commas. Returns `None` when a quote is left open.
fn split_modifiers(inner: &str) -> Option<Vec<String>> {
	if inner.trim().is_empty() {
		return Some(Vec::new());
	}
	let mut parts = Vec::new();
	let mut current = String::new();
	let mut quoted = false;
	for ch in inner.chars() {
		match ch {
			'\'' => {
				quoted = !quoted;
				current.push(ch);
			}
			',' if !quoted => {
				parts.push(current.trim().to_string());
				current.clear();
			}
			_ => current.push(ch),
		}
	}
	if quoted {
		return None;
	}
	parts.push(current.trim().to_string());
	Some(parts)
}

fn push_modifiers(out: &mut String, modifiers: &[String]) {
	if modifiers.is_empty() {
		return;
	}
	out.push('(');
	out.push_str(&modifiers.join(","));
	out.push(')');
}

#[cfg(test)]
mod tests {
	use super::*;

	fn pg(text: &str) -> DeclaredSqlType {
		DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, text).expect("a PostgreSQL type")
	}

	fn my(text: &str) -> DeclaredSqlType {
		DeclaredSqlType::parse(SqlTypeDialect::MySql, text).expect("a MySQL type")
	}

	#[test]
	fn canonical_text_lowers_case_and_collapses_spaces() {
		assert_eq!(pg("  VARCHAR( 20 ) ").canonical_text(), "varchar(20)");
		assert_eq!(pg("Character   Varying(20)").canonical_text(), "character varying(20)");
		assert_eq!(pg("numeric(10, 2)").canonical_text(), "numeric(10,2)");
		assert_eq!(my("DECIMAL(10,2)").canonical_text(), "decimal(10,2)");
		assert_eq!(my("int(11)   UNSIGNED").canonical_text(), "int(11) unsigned");
	}

	#[test]
	fn a_temporal_modifier_sits_before_the_zone_words() {
		let ty = pg("timestamp(3) with time zone");
		assert_eq!(ty.base(), "timestamp with time zone");
		assert_eq!(ty.canonical_text(), "timestamp(3) with time zone");
		assert_eq!(
			pg("TIMESTAMP WITHOUT TIME ZONE").canonical_text(),
			"timestamp without time zone"
		);
	}

	#[test]
	fn array_dimensions_are_counted_and_canonicalised() {
		let ty = pg("int[][]");
		assert_eq!(ty.array_dimensions(), &[None, None]);
		assert_eq!(ty.canonical_text(), "int[][]");
		assert_eq!(pg("text[3]").array_dimensions(), &[Some(3)]);
		assert_eq!(pg("varchar(5) [ ]").canonical_text(), "varchar(5)[]");
	}

	#[test]
	fn canonical_text_parses_to_the_same_type() {
		for text in [
			"varchar(20)",
			"numeric(10,2)",
			"timestamp(3) with time zone",
			"character varying(7)[]",
			"time(0) without time zone",
		] {
			let first = pg(text);
			let second = pg(&first.canonical_text());
			assert_eq!(first, second, "round trip of {text}");
		}
	}

	#[test]
	fn enum_members_keep_their_commas_and_spaces() {
		let ty = my("ENUM('a,b', 'c')");
		assert_eq!(ty.base(), "enum");
		assert_eq!(ty.modifiers(), &["'a,b'".to_string(), "'c'".to_string()]);
	}

	#[test]
	fn enum_members_keep_their_case_and_spacing() {
		let ty = my("ENUM('A  B',  'c')");
		assert_eq!(ty.base(), "enum");
		assert_eq!(ty.modifiers(), &["'A  B'".to_string(), "'c'".to_string()]);
		let ty = my("SET('It''s',\t'X')");
		assert_eq!(ty.modifiers(), &["'It''s'".to_string(), "'X'".to_string()]);
	}

	#[test]
	fn a_mysql_text_or_blob_length_selects_a_storage_class() {
		let max = |text: &str| {
			DeclaredSqlType::parse(SqlTypeDialect::MySql, text).unwrap().max_length().unwrap().max
		};
		// `TEXT(n)` counts characters of four bytes.
		assert_eq!(max("text(10)"), 255);
		assert_eq!(max("text(63)"), 255);
		assert_eq!(max("text(64)"), 65_535);
		assert_eq!(max("text(16383)"), 65_535);
		assert_eq!(max("text(16384)"), 16_777_215);
		assert_eq!(max("text"), 65_535);
		assert_eq!(max("blob(255)"), 255);
		assert_eq!(max("blob(256)"), 65_535);
		assert_eq!(max("blob(70000)"), 16_777_215);
		assert_eq!(max("blob(16777216)"), 4_294_967_295);
	}

	#[test]
	fn a_type_takes_only_the_modifiers_it_has() {
		for (dialect, ok) in [
			(SqlTypeDialect::MySql, "decimal(10,2)"),
			(SqlTypeDialect::MySql, "float(7,4)"),
			(SqlTypeDialect::MySql, "varchar(3)"),
			(SqlTypeDialect::PostgreSql, "numeric(10,2)"),
			(SqlTypeDialect::PostgreSql, "timestamp(3)"),
			(SqlTypeDialect::PostgreSql, "vector(3)"),
			(SqlTypeDialect::PostgreSql, "bit varying(5)"),
			(SqlTypeDialect::MySql, "int(11)"),
			(SqlTypeDialect::MySql, "text(70000)"),
			(SqlTypeDialect::MySql, "datetime(6)"),
		] {
			assert!(DeclaredSqlType::parse(dialect, ok).is_ok(), "{ok}");
		}
		for (dialect, bad) in [
			(SqlTypeDialect::MySql, "decimal(10,2,7)"),
			(SqlTypeDialect::MySql, "varchar(3,4)"),
			(SqlTypeDialect::PostgreSql, "varchar(3,4)"),
			(SqlTypeDialect::PostgreSql, "float(7,4)"),
			(SqlTypeDialect::PostgreSql, "text(3)"),
			(SqlTypeDialect::PostgreSql, "date(3)"),
			(SqlTypeDialect::PostgreSql, "integer(4)"),
			(SqlTypeDialect::MySql, "date(3)"),
			(SqlTypeDialect::MySql, "json(1)"),
		] {
			assert!(DeclaredSqlType::parse(dialect, bad).is_err(), "{bad}");
		}
	}

	#[test]
	fn only_a_mysql_numeric_type_takes_attributes() {
		for ok in
			["int unsigned", "int(11) unsigned zerofill", "decimal(5,2) unsigned", "double signed"]
		{
			assert!(DeclaredSqlType::parse(SqlTypeDialect::MySql, ok).is_ok(), "{ok}");
		}
		for (dialect, bad) in [
			(SqlTypeDialect::PostgreSql, "numeric(2,0) unsigned"),
			(SqlTypeDialect::PostgreSql, "integer unsigned"),
			(SqlTypeDialect::PostgreSql, "varchar(3) bogus"),
			(SqlTypeDialect::MySql, "varchar(3) bogus"),
			(SqlTypeDialect::MySql, "varchar(3) unsigned"),
			(SqlTypeDialect::MySql, "int(3) bogus"),
		] {
			assert!(DeclaredSqlType::parse(dialect, bad).is_err(), "{bad}");
		}
	}

	#[test]
	fn a_postgres_float_takes_1_to_53_bits() {
		for ok in ["float(1)", "float(53)", "float"] {
			assert!(DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, ok).is_ok(), "{ok}");
		}
		for bad in ["float(0)", "float(54)", "float(-1)"] {
			assert!(DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, bad).is_err(), "{bad}");
		}
	}

	#[test]
	fn a_mysql_bit_holds_1_to_64_bits() {
		assert!(DeclaredSqlType::parse(SqlTypeDialect::MySql, "bit(1)").is_ok());
		assert!(DeclaredSqlType::parse(SqlTypeDialect::MySql, "bit(64)").is_ok());
		assert!(DeclaredSqlType::parse(SqlTypeDialect::MySql, "bit(0)").is_err());
		assert!(DeclaredSqlType::parse(SqlTypeDialect::MySql, "bit(65)").is_err());
		assert!(DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "bit(65)").is_ok());
	}

	#[test]
	fn a_mysql_decimal_is_held_to_mysqls_limits() {
		assert!(DeclaredSqlType::parse(SqlTypeDialect::MySql, "decimal(65,30)").is_ok());
		for bad in ["decimal(66,0)", "decimal(10,11)", "decimal(40,31)", "numeric(66)"] {
			assert!(DeclaredSqlType::parse(SqlTypeDialect::MySql, bad).is_err(), "{bad}");
		}
		assert!(DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "numeric(100,50)").is_ok());
	}

	#[test]
	fn a_padded_type_is_held_to_its_dialects_length_limit() {
		assert!(DeclaredSqlType::parse(SqlTypeDialect::MySql, "binary(255)").is_ok());
		let err = DeclaredSqlType::parse(SqlTypeDialect::MySql, "binary(1000000000)").unwrap_err();
		assert!(err.reason.contains("at most 255"), "{err}");
		assert!(DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "char(10485760)").is_ok());
		assert!(DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "char(10485761)").is_err());
		// An unpadded type keeps its own limit, enforced by the length check alone.
		assert!(DeclaredSqlType::parse(SqlTypeDialect::MySql, "varbinary(65535)").is_ok());
	}

	#[test]
	fn invalid_text_is_refused_with_its_reason() {
		let err = DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "varchar(20").unwrap_err();
		assert!(err.reason.contains("not closed"), "{err}");
		assert!(DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "   ").is_err());
		assert!(DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "(10)").is_err());
		assert!(DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "int[x]").is_err());
		assert!(DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "varchar(n)").is_err());
		assert!(DeclaredSqlType::parse(SqlTypeDialect::MySql, "time(7)").is_err());
	}

	#[test]
	fn only_a_postgres_numeric_scale_may_be_negative() {
		for (dialect, text) in [
			(SqlTypeDialect::MySql, "varchar(-1)"),
			(SqlTypeDialect::PostgreSql, "bit(-1)"),
			(SqlTypeDialect::PostgreSql, "numeric(-5,2)"),
			(SqlTypeDialect::PostgreSql, "timestamp(-1)"),
			(SqlTypeDialect::MySql, "decimal(10,-2)"),
		] {
			let err = DeclaredSqlType::parse(dialect, text).unwrap_err();
			assert!(err.reason.contains("must not be negative"), "{text}: {err}");
		}
		assert_eq!(pg("numeric(2,-3)").scale(), Some(-3));
	}

	#[test]
	fn an_element_has_one_array_dimension_fewer() {
		assert_eq!(pg("numeric(5,2)[][]").element(), Some(pg("numeric(5,2)[]")));
		assert_eq!(pg("numeric(5,2)[]").element(), Some(pg("numeric(5,2)")));
		assert_eq!(pg("numeric(5,2)").element(), None);
	}

	#[test]
	fn max_length_follows_each_dialect() {
		let chars = |max| {
			Some(Length {
				max,
				unit: LengthUnit::Chars,
			})
		};
		assert_eq!(pg("varchar(255)").max_length(), chars(255));
		assert_eq!(pg("varchar").max_length(), None);
		assert_eq!(pg("character varying(7)").max_length(), chars(7));
		assert_eq!(pg("char").max_length(), chars(1));
		assert_eq!(pg("bpchar(4)").max_length(), chars(4));
		assert_eq!(pg("bpchar").max_length(), None);
		assert_eq!(pg("text").max_length(), None);
		assert_eq!(pg("varchar(4)[]").max_length(), None);
		assert_eq!(my("varchar(20)").max_length(), chars(20));
		assert_eq!(my("char").max_length(), chars(1));
		assert_eq!(my("nvarchar(9)").max_length(), chars(9));
		assert_eq!(
			my("tinytext").max_length(),
			Some(Length {
				max: 255,
				unit: LengthUnit::Bytes
			})
		);
		assert_eq!(
			my("text").max_length(),
			Some(Length {
				max: 65_535,
				unit: LengthUnit::Bytes
			})
		);
		assert_eq!(
			my("longtext").max_length(),
			Some(Length {
				max: 4_294_967_295,
				unit: LengthUnit::Bytes
			})
		);
		assert_eq!(
			my("binary(16)").max_length(),
			Some(Length {
				max: 16,
				unit: LengthUnit::Bytes
			})
		);
		assert_eq!(
			my("varbinary(16)").max_length(),
			Some(Length {
				max: 16,
				unit: LengthUnit::Bytes
			})
		);
		assert_eq!(
			pg("bit(8)").max_length(),
			Some(Length {
				max: 8,
				unit: LengthUnit::Bits
			})
		);
		assert_eq!(pg("varbit").max_length(), None);
		assert_eq!(
			my("bit").max_length(),
			Some(Length {
				max: 1,
				unit: LengthUnit::Bits
			})
		);
		assert_eq!(pg("numeric(10,2)").max_length(), None);
	}

	#[test]
	fn padding_is_blank_for_pg_char_and_zero_for_mysql_binary() {
		assert_eq!(pg("char(3)").padded(), Padding::Blank);
		assert_eq!(pg("bpchar(3)").padded(), Padding::Blank);
		assert_eq!(pg("bpchar").padded(), Padding::None);
		assert_eq!(pg("varchar(3)").padded(), Padding::None);
		assert_eq!(my("binary(3)").padded(), Padding::Zero);
		assert_eq!(my("varbinary(3)").padded(), Padding::None);
		assert_eq!(my("char(3)").padded(), Padding::None);
	}

	#[test]
	fn precision_and_scale_follow_the_written_modifiers_and_dialect_defaults() {
		assert_eq!(pg("numeric(10,2)").precision(), Some(10));
		assert_eq!(pg("numeric(10,2)").scale(), Some(2));
		assert_eq!(pg("numeric(7)").scale(), Some(0));
		assert_eq!(pg("numeric").precision(), None);
		assert_eq!(pg("numeric").scale(), None);
		assert_eq!(pg("decimal(4,-2)").scale(), Some(-2));
		assert_eq!(my("decimal").precision(), Some(10));
		assert_eq!(my("decimal").scale(), Some(0));
		assert_eq!(my("DEC(5,1)").scale(), Some(1));
		assert_eq!(my("integer(4)").precision(), None);
	}

	#[test]
	fn postgres_numeric_holds_precision_and_scale_to_their_limits() {
		for ok in ["numeric(1000)", "numeric(1000, 1000)", "numeric(5, -1000)", "numeric(5, 2)"] {
			assert!(DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, ok).is_ok(), "{ok}");
		}
		for bad in [
			"numeric(1001)",
			"numeric(99999999999999999999999)",
			"numeric(5, 1001)",
			"numeric(5, -1001)",
		] {
			assert!(DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, bad).is_err(), "{bad}");
		}
	}

	#[test]
	fn fractional_seconds_default_per_dialect_and_cap_on_postgres() {
		assert_eq!(pg("timestamp").fractional_seconds(), Some(6));
		assert_eq!(pg("timestamp(3)").fractional_seconds(), Some(3));
		assert_eq!(pg("timestamp(9)").fractional_seconds(), Some(6));
		assert_eq!(pg("time with time zone").fractional_seconds(), Some(6));
		assert_eq!(pg("interval(2)").fractional_seconds(), Some(2));
		assert_eq!(pg("date").fractional_seconds(), None);
		assert_eq!(my("datetime").fractional_seconds(), Some(0));
		assert_eq!(my("time(6)").fractional_seconds(), Some(6));
	}

	#[test]
	fn json_flavour_distinguishes_text_from_binary() {
		assert_eq!(pg("json").json_flavour(), Some(JsonFlavour::Text));
		assert_eq!(pg("jsonb").json_flavour(), Some(JsonFlavour::Binary));
		assert_eq!(my("json").json_flavour(), Some(JsonFlavour::Binary));
		assert_eq!(pg("json[]").json_flavour(), None);
		assert_eq!(pg("text").json_flavour(), None);
	}

	#[test]
	fn check_text_drops_trailing_spaces_of_a_char_beyond_its_length() {
		let ty = pg("char(3)");
		assert_eq!(ty.check_text("ab ", "c").unwrap(), "ab", "a trailing space is insignificant");
		assert_eq!(ty.check_text("abc", "c").unwrap(), "abc");
		assert_eq!(ty.check_text("abc  ", "c").unwrap(), "abc");
		let err = ty.check_text("abcd", "c").unwrap_err();
		assert_eq!(err.code, "22001");
		assert_eq!(err.message, "value too long for type character(3)");
	}

	#[test]
	fn check_text_counts_characters_not_bytes() {
		let ty = pg("varchar(3)");
		assert!(ty.check_text("héé", "c").is_ok());
		let err = ty.check_text("héé!", "c").unwrap_err();
		assert_eq!(err.message, "value too long for type character varying(3)");
		assert_eq!(
			pg("varchar(3)").check_text("ab  ", "c").unwrap(),
			"ab ",
			"excess spaces are dropped from a varchar too"
		);
		assert_eq!(pg("varchar(3)").check_text("a ", "c").unwrap(), "a ", "spaces within it stay");
		assert!(pg("text").check_text("a long text", "c").is_ok());
	}

	#[test]
	fn check_text_of_mysql_uses_its_wire_message_and_its_byte_limits() {
		let err = my("varchar(2)").check_text("abc", "name").unwrap_err();
		assert_eq!(err.message, "Data too long for column 'name' at row 1");
		assert!(my("varchar(2)").check_text("é", "name").is_ok());
		// A TINYTEXT is 255 bytes, so a 2-byte character counts twice.
		let long: String = "é".repeat(128);
		let err = my("tinytext").check_text(&long, "note").unwrap_err();
		assert_eq!(err.message, "Data too long for column 'note' at row 1");
		let fits: String = "é".repeat(127);
		assert!(my("tinytext").check_text(&fits, "note").is_ok());
		// A MySQL CHAR is read back with its trailing spaces removed, so none is kept.
		assert_eq!(my("char(4)").check_text("ab      ", "c").unwrap(), "ab");
		assert_eq!(my("varchar(4)").check_text("ab      ", "v").unwrap(), "ab  ");
	}

	#[test]
	fn check_bytes_pads_binary_and_refuses_excess() {
		let padded = my("binary(4)").check_bytes(vec![1, 2], "b").unwrap();
		assert_eq!(padded, vec![1, 2, 0, 0]);
		assert_eq!(my("varbinary(4)").check_bytes(vec![1, 2], "b").unwrap(), vec![1, 2]);
		let err = my("binary(2)").check_bytes(vec![1, 2, 3], "b").unwrap_err();
		assert_eq!(err.message, "Data too long for column 'b' at row 1");
		assert_eq!(my("blob").check_bytes(vec![0; 65_535], "b").unwrap().len(), 65_535);
		assert!(my("blob").check_bytes(vec![0; 65_536], "b").is_err());
	}

	#[test]
	fn check_bits_requires_an_exact_length_for_bit_and_a_maximum_for_varbit() {
		assert!(pg("bit(3)").check_bits("101").is_ok());
		let err = pg("bit(3)").check_bits("10").unwrap_err();
		assert_eq!(err.code, "22026");
		assert_eq!(err.message, "bit string length 2 does not match type bit(3)");
		assert!(pg("varbit(3)").check_bits("10").is_ok());
		let err = pg("varbit(3)").check_bits("1010").unwrap_err();
		assert_eq!(err.message, "bit string too long for type bit varying(3)");
		assert!(pg("varbit").check_bits("1010101010").is_ok());
	}

	#[test]
	fn round_nanos_rounds_half_away_from_zero_and_carries() {
		let ty = pg("timestamp(3)");
		assert_eq!(ty.round_nanos(123_456_789), (123_000_000, false));
		assert_eq!(ty.round_nanos(123_500_000), (124_000_000, false));
		assert_eq!(ty.round_nanos(999_500_000), (0, true));
		assert_eq!(pg("timestamp(0)").round_nanos(500_000_000), (0, true));
		assert_eq!(pg("timestamp(0)").round_nanos(499_999_999), (0, false));
		// PostgreSQL caps the precision at 6 digits, so 9 rounds to microseconds.
		assert_eq!(pg("timestamp(9)").round_nanos(123_456_789), (123_457_000, false));
		assert_eq!(pg("date").round_nanos(123_456_789), (123_456_789, false));
	}

	#[test]
	fn an_interval_field_qualifier_is_part_of_the_base() {
		let ty = pg("INTERVAL  Day To Second(3)");
		assert_eq!(ty.base(), "interval day to second");
		assert_eq!(ty.modifiers(), ["3"]);
		assert_eq!(ty.canonical_text(), "interval day to second(3)");
		assert_eq!(ty.interval_fields(), Some(IntervalFields::DayToSecond));
		assert_eq!(ty.fractional_seconds(), Some(3));
		assert_eq!(pg(&ty.canonical_text()), ty);

		assert_eq!(pg("interval").interval_fields(), None);
		assert_eq!(pg("interval(2)").interval_fields(), None);
		assert_eq!(pg("interval year").interval_fields(), Some(IntervalFields::Year));
		assert_eq!(pg("interval year").fractional_seconds(), Some(6));
		assert_eq!(pg("interval second(9)").fractional_seconds(), Some(6));
		assert_eq!(pg("interval hour to minute[]").interval_fields(), None);
		assert_eq!(
			pg("interval hour to minute[]").element().and_then(|e| e.interval_fields()),
			Some(IntervalFields::HourToMinute)
		);
		for fields in IntervalFields::ALL {
			assert_eq!(IntervalFields::from_words(fields.words()), Some(fields));
			assert_eq!(IntervalFields::from_pg_range_mask(fields.pg_range_mask()), Some(fields));
			assert_eq!(pg(&format!("interval {}", fields.words())).interval_fields(), Some(fields));
		}
		// PostgreSQL's `INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | ... | INTERVAL_MASK(SECOND)`.
		assert_eq!(IntervalFields::DayToSecond.pg_range_mask(), 7176);
		assert_eq!(IntervalFields::YearToMonth.pg_range_mask(), 6);
	}

	#[test]
	fn an_interval_qualifier_outside_the_grammar_or_a_precision_without_seconds_is_refused() {
		for text in [
			"interval day to year",
			"interval fortnight",
			"interval day(3)",
			"interval year to month(1)",
		] {
			assert!(DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, text).is_err(), "{text}");
		}
	}

	#[test]
	fn the_finest_field_decides_the_truncation() {
		assert_eq!(IntervalFields::Year.finest(), IntervalFinestField::Year);
		assert_eq!(IntervalFields::YearToMonth.finest(), IntervalFinestField::Month);
		assert_eq!(IntervalFields::Day.finest(), IntervalFinestField::Day);
		assert_eq!(IntervalFields::DayToHour.finest(), IntervalFinestField::Hour);
		assert_eq!(IntervalFields::HourToMinute.finest(), IntervalFinestField::Minute);
		assert_eq!(IntervalFields::MinuteToSecond.finest(), IntervalFinestField::Second);
	}
}
