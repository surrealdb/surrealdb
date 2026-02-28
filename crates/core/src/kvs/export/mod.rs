use core::fmt;

use crate::{err::Error, sql::Value};

mod v2;
mod v3;

pub use v3::export_v3;

#[derive(Clone, Debug)]
pub struct Config {
	pub users: bool,
	pub accesses: bool,
	pub params: bool,
	pub functions: bool,
	pub analyzers: bool,
	pub tables: TableConfig,
	pub versions: bool,
	pub records: bool,
	pub v3: bool,
}

impl Default for Config {
	fn default() -> Config {
		Config {
			users: true,
			accesses: true,
			params: true,
			functions: true,
			analyzers: true,
			tables: TableConfig::default(),
			versions: false,
			records: true,
			v3: false,
		}
	}
}

impl From<Config> for Value {
	fn from(config: Config) -> Value {
		let obj = map!(
			"users" => config.users.into(),
			"accesses" => config.accesses.into(),
			"params" => config.params.into(),
			"functions" => config.functions.into(),
			"analyzers" => config.analyzers.into(),
			"versions" => config.versions.into(),
			"records" => config.records.into(),
			"v3" => config.v3.into(),
			"tables" => match config.tables {
				TableConfig::All => true.into(),
				TableConfig::None => false.into(),
				TableConfig::Some(v) => v.into()
			}
		);

		obj.into()
	}
}

impl TryFrom<&Value> for Config {
	type Error = Error;
	fn try_from(value: &Value) -> Result<Self, Self::Error> {
		match value {
			Value::Object(obj) => {
				let mut config = Config::default();

				macro_rules! bool_prop {
					($prop:ident) => {{
						match obj.get(stringify!($prop)) {
							Some(Value::Bool(v)) => {
								config.$prop = v.to_owned();
							}
							Some(v) => {
								return Err(Error::InvalidExportConfig(
									v.to_owned(),
									"a bool".into(),
								))
							}
							_ => (),
						}
					}};
				}

				bool_prop!(users);
				bool_prop!(accesses);
				bool_prop!(params);
				bool_prop!(functions);
				bool_prop!(analyzers);
				bool_prop!(versions);
				bool_prop!(records);
				bool_prop!(v3);

				if let Some(v) = obj.get("tables") {
					config.tables = v.try_into()?;
				}

				Ok(config)
			}
			v => Err(Error::InvalidExportConfig(v.to_owned(), "an object".into())),
		}
	}
}

#[derive(Clone, Debug, Default)]
pub enum TableConfig {
	#[default]
	All,
	None,
	Some(Vec<String>),
}

impl From<bool> for TableConfig {
	fn from(value: bool) -> Self {
		match value {
			true => TableConfig::All,
			false => TableConfig::None,
		}
	}
}

impl From<Vec<String>> for TableConfig {
	fn from(value: Vec<String>) -> Self {
		TableConfig::Some(value)
	}
}

impl From<Vec<&str>> for TableConfig {
	fn from(value: Vec<&str>) -> Self {
		TableConfig::Some(value.into_iter().map(ToOwned::to_owned).collect())
	}
}

impl TryFrom<&Value> for TableConfig {
	type Error = Error;
	fn try_from(value: &Value) -> Result<Self, Self::Error> {
		match value {
			Value::Bool(b) => match b {
				true => Ok(TableConfig::All),
				false => Ok(TableConfig::None),
			},
			Value::None | Value::Null => Ok(TableConfig::None),
			Value::Array(v) => v
				.iter()
				.cloned()
				.map(|v| match v {
					Value::Strand(str) => Ok(str.0),
					v => Err(Error::InvalidExportConfig(v.to_owned(), "a string".into())),
				})
				.collect::<Result<Vec<String>, Error>>()
				.map(TableConfig::Some),
			v => Err(Error::InvalidExportConfig(
				v.to_owned(),
				"a bool, none, null or array<string>".into(),
			)),
		}
	}
}

impl TableConfig {
	/// Check if we should export tables
	pub(crate) fn is_any(&self) -> bool {
		matches!(self, Self::All | Self::Some(_))
	}
	// Check if we should export a specific table
	pub(crate) fn includes(&self, table: &str) -> bool {
		match self {
			Self::All => true,
			Self::None => false,
			Self::Some(v) => v.iter().any(|v| v.eq(table)),
		}
	}
}

struct InlineCommentWriter<'a, F>(&'a mut F);
impl<F: fmt::Write> fmt::Write for InlineCommentWriter<'_, F> {
	fn write_str(&mut self, s: &str) -> fmt::Result {
		for c in s.chars() {
			self.write_char(c)?
		}
		Ok(())
	}

	fn write_char(&mut self, c: char) -> fmt::Result {
		match c {
			'\n' => self.0.write_str("\\n"),
			'\r' => self.0.write_str("\\r"),
			// NEL/Next Line
			'\u{0085}' => self.0.write_str("\\u{0085}"),
			// line seperator
			'\u{2028}' => self.0.write_str("\\u{2028}"),
			// Paragraph seperator
			'\u{2029}' => self.0.write_str("\\u{2029}"),
			_ => self.0.write_char(c),
		}
	}
}

struct InlineCommentDisplay<F>(F);
impl<F: fmt::Display> fmt::Display for InlineCommentDisplay<F> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		fmt::Write::write_fmt(&mut InlineCommentWriter(f), format_args!("{}", self.0))
	}
}
