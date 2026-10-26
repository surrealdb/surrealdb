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
	/// Whether `columns` is every column the table has, so that a name outside it is an error.
	/// A table whose list is a subset of what a client may read (a system catalog table) or that
	/// declares no columns (a schemaless table) is not closed.
	pub closed: bool,
}

impl TableSchema {
	pub fn new(name: impl Into<String>, columns: Vec<ColumnInfo>) -> Self {
		Self {
			name: name.into(),
			schema: None,
			columns,
			closed: false,
		}
	}

	/// Declare `columns` to be every column the table has.
	pub fn as_closed(mut self) -> Self {
		self.closed = true;
		self
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

pub mod system;

/// Narrow catalog trait required by the binder to resolve table and column names.
pub trait BinderCatalog: Send + Sync {
	/// Look up a table schema by optional namespace and table name.
	fn get_table(&self, schema: Option<&str>, name: &str) -> Option<TableSchema>;

	/// Check if a table exists.
	fn table_exists(&self, schema: Option<&str>, name: &str) -> bool {
		self.get_table(schema, name).is_some()
	}

	/// Current active database name, if known.
	fn current_database(&self) -> Option<&str> {
		None
	}

	/// Return all user-defined table schemas in the catalog.
	fn all_tables(&self) -> Vec<TableSchema> {
		Vec::new()
	}

	/// A declared column type, independent of whether the table's columns can be enumerated.
	fn column_type(&self, schema: Option<&str>, table: &str, column: &str) -> crate::SqlType {
		self.get_table(schema, table)
			.and_then(|table| {
				table.find_column(column, false).and_then(|column| column.data_type.clone())
			})
			.map(|name| crate::SqlType::from_name(&name))
			.unwrap_or(crate::SqlType::Unknown)
	}

	/// The table that column `column` of table `table` holds record links to.
	///
	/// A column holds links to one table when its declared type names exactly that table:
	/// `record<t>`, `option<record<t>>` or `record<t> | none`. The answer is `None` for a table or
	/// a column the catalog does not describe, for a `record` of several tables or of any table,
	/// and for every other type, including a collection of links.
	///
	/// The table and the column are named as the lowering names them, which is the name of the
	/// stored field, and are compared exactly: SurrealDB field names are case-sensitive, so a field
	/// that differs from a link column only in case is not a link.
	///
	/// The binder and the write lowering ask for each column they resolve against a table (a column
	/// may be asked about more than once), never for the columns of a table as a whole, so an
	/// implementation may answer from the definition of the one column. The answer does not change
	/// what [`BinderCatalog::get_table`] returns: a table that only has link columns described is
	/// still an undescribed table.
	fn link_table(&self, _schema: Option<&str>, _table: &str, _column: &str) -> Option<String> {
		None
	}

	/// The number of dimensions of column `column` of table `table` when it holds vectors
	/// (`vector(n)`, which the engine stores as `array<float, n>`), and `None` for a table or
	/// column the catalog does not describe and for every other type.
	///
	/// The table and the column are named as the lowering names them, and are compared exactly.
	fn vector_dimension(
		&self,
		_schema: Option<&str>,
		_table: &str,
		_column: &str,
	) -> Option<usize> {
		None
	}

	/// The HNSW indexes of table `table`, as the column each covers and the metric it ranks by.
	/// An index that is being removed is not one.
	fn hnsw_indexes(
		&self,
		_schema: Option<&str>,
		_table: &str,
	) -> Vec<(String, crate::vector::VectorMetric)> {
		Vec::new()
	}
}

/// An empty or schemaless catalog.
///
/// In SurrealDB, tables can be schemaless, where columns are not predefined.
/// Returning `None` signals to the binder that column resolution should proceed
/// flexibly without failing early on non-predefined column names.
/// System catalog tables are still recognized.
#[derive(Debug, Default, Clone)]
pub struct EmptyCatalog;

impl BinderCatalog for EmptyCatalog {
	fn get_table(&self, schema: Option<&str>, name: &str) -> Option<TableSchema> {
		system::get_system_table_schema(schema, name)
	}
}

/// An in-memory catalog used for tests and static schema definitions.
#[derive(Debug, Default, Clone)]
pub struct MemoryCatalog {
	tables: HashMap<(Option<String>, String), TableSchema>,
	/// The link columns, by (schema, table, column), and the table each holds links to. Kept apart
	/// from `tables`: describing a link column does not describe its table.
	links: HashMap<(Option<String>, String, String), String>,
	current_db: Option<String>,
	column_types: HashMap<(String, String), crate::SqlType>,
	/// The dimensions of the vector columns, by (table, column).
	vectors: HashMap<(String, String), usize>,
	/// The HNSW indexes, by table, as the column each covers and its metric.
	hnsw: HashMap<String, Vec<(String, crate::vector::VectorMetric)>>,
}

impl MemoryCatalog {
	pub fn new() -> Self {
		Self {
			tables: HashMap::new(),
			links: HashMap::new(),
			current_db: None,
			column_types: HashMap::new(),
			vectors: HashMap::new(),
			hnsw: HashMap::new(),
		}
	}

	/// Declare an HNSW index of table `table` over column `column`, ranking by `metric`.
	pub fn add_hnsw_index(
		&mut self,
		table: &str,
		column: &str,
		metric: crate::vector::VectorMetric,
	) -> &mut Self {
		self.hnsw.entry(table.to_owned()).or_default().push((column.to_owned(), metric));
		self
	}

	/// Declare that column `column` of table `table` holds vectors of `dimensions` dimensions.
	pub fn add_vector(&mut self, table: &str, column: &str, dimensions: usize) -> &mut Self {
		self.vectors.insert((table.to_owned(), column.to_owned()), dimensions);
		self
	}

	/// Declare a vector column, as [`Self::add_vector`] does.
	pub fn with_vector(mut self, table: &str, column: &str, dimensions: usize) -> Self {
		self.add_vector(table, column, dimensions);
		self
	}

	/// Supply a field's declared type without treating a schemaless table as a closed schema.
	pub fn with_column_type(mut self, table: &str, column: &str, ty: crate::SqlType) -> Self {
		self.add_column_type(table, column, ty);
		self
	}

	/// Supply a field's declared type, as [`Self::with_column_type`] does.
	pub fn add_column_type(&mut self, table: &str, column: &str, ty: crate::SqlType) -> &mut Self {
		self.column_types.insert((table.to_owned(), column.to_owned()), ty);
		self
	}

	/// Set the active database name.
	pub fn with_database(mut self, db: impl Into<String>) -> Self {
		self.current_db = Some(db.into());
		self
	}

	/// Set the active database name mutably.
	pub fn set_database(&mut self, db: Option<String>) {
		self.current_db = db;
	}

	/// Register a table schema.
	pub fn add_table(&mut self, schema: TableSchema) {
		let key = (schema.schema.clone(), schema.name.clone());
		self.tables.insert(key, schema);
	}

	/// Declare that column `column` of the schema-less table `table` holds record links to the
	/// table `target`, whether the table is named bare or qualified. The table itself stays as
	/// described (or undescribed) as it was.
	pub fn with_link(mut self, table: &str, column: &str, target: &str) -> Self {
		self.add_link(None, table, column, target);
		self
	}

	/// Declare that column `column` of table `table` in `schema` holds record links to the table
	/// `target`.
	pub fn add_link(
		&mut self,
		schema: Option<&str>,
		table: &str,
		column: &str,
		target: &str,
	) -> &mut Self {
		self.links.insert(
			(schema.map(str::to_string), table.to_string(), column.to_string()),
			target.to_string(),
		);
		self
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
	fn current_database(&self) -> Option<&str> {
		self.current_db.as_deref()
	}

	fn all_tables(&self) -> Vec<TableSchema> {
		self.tables.values().cloned().collect()
	}

	fn get_table(&self, schema: Option<&str>, name: &str) -> Option<TableSchema> {
		if let Some(sys_tbl) = system::get_system_table_schema(schema, name) {
			return Some(sys_tbl);
		}
		let key = (schema.map(|s| s.to_string()), name.to_string());
		if let Some(tbl) = self.tables.get(&key) {
			return Some(tbl.clone());
		}
		// A closed table is registered without a schema: the datastore holds one namespace of
		// tables per database, and a qualifier (`public`, the database name) names that same
		// table.
		if schema.is_some()
			&& let Some(tbl) = self.tables.get(&(None, name.to_string()))
			&& tbl.closed
		{
			return Some(tbl.clone());
		}
		// If schema was not specified, try with None. A closed table is found under the exact
		// name only: table names the datastore holds are case-sensitive.
		if schema.is_none() {
			for ((s, n), tbl) in &self.tables {
				if (s.is_none() || s.as_deref() == Some("public"))
					&& !tbl.closed && n.eq_ignore_ascii_case(name)
				{
					return Some(tbl.clone());
				}
			}
		}
		None
	}

	fn column_type(&self, schema: Option<&str>, table: &str, column: &str) -> crate::SqlType {
		if let Some(ty) = self.column_types.get(&(table.to_owned(), column.to_owned())) {
			return *ty;
		}
		// A closed table is typed by `column_types` alone, so looking it up would copy its columns
		// to find none.
		if self.tables.get(&(None, table.to_owned())).is_some_and(|table| table.closed) {
			return crate::SqlType::Unknown;
		}
		self.get_table(schema, table)
			.and_then(|table| {
				table.find_column(column, false).and_then(|column| column.data_type.clone())
			})
			.map(|name| crate::SqlType::from_name(&name))
			.unwrap_or(crate::SqlType::Unknown)
	}

	fn vector_dimension(&self, _schema: Option<&str>, table: &str, column: &str) -> Option<usize> {
		self.vectors.get(&(table.to_owned(), column.to_owned())).copied()
	}

	fn hnsw_indexes(
		&self,
		_schema: Option<&str>,
		table: &str,
	) -> Vec<(String, crate::vector::VectorMetric)> {
		self.hnsw.get(table).cloned().unwrap_or_default()
	}

	fn link_table(&self, schema: Option<&str>, table: &str, column: &str) -> Option<String> {
		let key = (schema.map(str::to_string), table.to_string(), column.to_string());
		if let Some(target) = self.links.get(&key) {
			return Some(target.clone());
		}
		// A link declared without a schema belongs to the table whatever qualifier names it: a
		// qualified name (`public.emp`, `db.emp`) reads the same table of the current database.
		if schema.is_some()
			&& let Some(target) = self.links.get(&(None, table.to_string(), column.to_string()))
		{
			return Some(target.clone());
		}
		// A bare table is the table of the `public` schema, as `get_table` reads it.
		if schema.is_none() {
			return self
				.links
				.get(&(Some("public".to_string()), table.to_string(), column.to_string()))
				.cloned();
		}
		None
	}
}
