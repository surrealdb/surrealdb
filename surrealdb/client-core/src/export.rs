use surrealdb_types::SurrealValue;

#[derive(Clone, Debug, SurrealValue)]
#[surreal(crate = "surrealdb_types")]
#[surreal(default)]
pub struct Config {
	pub users: bool,
	pub accesses: bool,
	pub params: bool,
	pub functions: bool,
	pub analyzers: bool,
	pub apis: bool,
	pub buckets: bool,
	pub modules: bool,
	pub configs: bool,
	pub tables: TableConfig,
	pub versions: bool,
	pub records: bool,
	pub sequences: bool,
}

impl Default for Config {
	fn default() -> Config {
		Config {
			users: true,
			accesses: true,
			params: true,
			functions: true,
			analyzers: true,
			apis: true,
			buckets: true,
			modules: true,
			configs: true,
			tables: TableConfig::default(),
			versions: false,
			records: true,
			sequences: true,
		}
	}
}

/// Named-field wrapper so that the untagged `SurrealValue` serialization
/// can differentiate `Exclude` from `Some` (include).
#[derive(Clone, Debug, SurrealValue)]
#[surreal(crate = "surrealdb_types")]
pub struct ExcludedTables {
	pub exclude: Vec<String>,
}

#[derive(Clone, Debug, Default, SurrealValue)]
#[surreal(crate = "surrealdb_types")]
#[surreal(untagged)]
pub enum TableConfig {
	#[default]
	#[surreal(value = true)]
	All,
	#[surreal(value = false)]
	None,
	Some(Vec<String>),
	Exclude(ExcludedTables),
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

impl TableConfig {
	pub fn is_any(&self) -> bool {
		matches!(self, Self::All | Self::Some(_) | Self::Exclude(_))
	}

	pub fn includes(&self, table: &str) -> bool {
		match self {
			Self::All => true,
			Self::None => false,
			Self::Some(v) => v.iter().any(|v| v.eq(table)),
			Self::Exclude(v) => !v.exclude.iter().any(|v| v.eq(table)),
		}
	}

	pub fn names(&self) -> Option<&[String]> {
		match self {
			Self::Some(v) => Some(v.as_slice()),
			Self::Exclude(v) => Some(v.exclude.as_slice()),
			_ => None,
		}
	}
}
