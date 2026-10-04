//! Narrow catalog abstraction for the SQL binder.

use std::collections::HashMap;

/// Information about a single column in a table schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
	/// Column name.
	pub name: String,
	/// 0-based ordinal index of column within table.
	pub ordinal: usize,
	/// Optional type name/kind description.
	pub data_type: Option<String>,
	/// Whether column is nullable.
	pub is_nullable: bool,
}

impl ColumnInfo {
	pub fn new(name: impl Into<String>, ordinal: usize) -> Self {
		Self {
			name: name.into(),
			ordinal,
			data_type: None,
			is_nullable: true,
		}
	}

	pub fn with_type(mut self, data_type: impl Into<String>) -> Self {
		self.data_type = Some(data_type.into());
		self
	}
}

/// Metadata about a table or view schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
	/// Table name.
	pub name: String,
	/// Optional schema namespace (e.g. `public`).
	pub schema: Option<String>,
	/// Ordered list of columns.
	pub columns: Vec<ColumnInfo>,
}

impl TableSchema {
	pub fn new(name: impl Into<String>, columns: Vec<ColumnInfo>) -> Self {
		Self {
			name: name.into(),
			schema: None,
			columns,
		}
	}

	pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
		self.schema = Some(schema.into());
		self
	}

	/// Find a column by case-insensitive or exact name.
	pub fn find_column(&self, name: &str, case_insensitive: bool) -> Option<&ColumnInfo> {
		if case_insensitive {
			self.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name))
		} else {
			self.columns.iter().find(|c| c.name == name)
		}
	}

	/// Get column names in ordinal order.
	pub fn column_names(&self) -> Vec<String> {
		self.columns.iter().map(|c| c.name.clone()).collect()
	}
}

/// Narrow catalog trait required by the binder to resolve table and column names.
pub trait BinderCatalog: Send + Sync {
	/// Look up a table schema by optional namespace and table name.
	fn get_table(&self, schema: Option<&str>, name: &str) -> Option<TableSchema>;

	/// Check if a table exists.
	fn table_exists(&self, schema: Option<&str>, name: &str) -> bool {
		self.get_table(schema, name).is_some()
	}
}

/// An empty or schemaless catalog.
///
/// In SurrealDB, tables can be schemaless, where columns are not predefined.
/// Returning `None` signals to the binder that column resolution should proceed
/// flexibly without failing early on non-predefined column names.
#[derive(Debug, Default, Clone)]
pub struct EmptyCatalog;

impl BinderCatalog for EmptyCatalog {
	fn get_table(&self, _schema: Option<&str>, _name: &str) -> Option<TableSchema> {
		None
	}
}

/// An in-memory catalog used for tests and static schema definitions.
#[derive(Debug, Default, Clone)]
pub struct MemoryCatalog {
	tables: HashMap<(Option<String>, String), TableSchema>,
}

impl MemoryCatalog {
	pub fn new() -> Self {
		Self {
			tables: HashMap::new(),
		}
	}

	/// Register a table schema.
	pub fn add_table(&mut self, schema: TableSchema) {
		let key = (schema.schema.clone(), schema.name.clone());
		self.tables.insert(key, schema);
	}

	/// Helper to define a table quickly.
	pub fn define_table(
		&mut self,
		schema: Option<&str>,
		name: &str,
		columns: &[&str],
	) -> &mut Self {
		let cols = columns.iter().enumerate().map(|(i, &c)| ColumnInfo::new(c, i)).collect();
		let mut table = TableSchema::new(name, cols);
		if let Some(s) = schema {
			table = table.with_schema(s);
		}
		self.add_table(table);
		self
	}
}

impl BinderCatalog for MemoryCatalog {
	fn get_table(&self, schema: Option<&str>, name: &str) -> Option<TableSchema> {
		let key = (schema.map(|s| s.to_string()), name.to_string());
		if let Some(tbl) = self.tables.get(&key) {
			return Some(tbl.clone());
		}
		// If schema was not specified, try with None
		if schema.is_none() {
			for ((s, n), tbl) in &self.tables {
				if (s.is_none() || s.as_deref() == Some("public")) && n.eq_ignore_ascii_case(name) {
					return Some(tbl.clone());
				}
			}
		}
		None
	}
}
