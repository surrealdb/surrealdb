//! Narrow catalog abstraction for the SQL binder.

use std::collections::HashMap;

use surrealdb_expr::expr::{Expr, Kind};

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
	/// The sequence that numbers the column when a write gives it no value, as `pg_sequences`
	/// lists it.
	pub sequence: Option<String>,
}

impl ColumnInfo {
	pub fn new(name: impl Into<String>, ordinal: usize) -> Self {
		Self {
			name: name.into(),
			ordinal,
			data_type: None,
			is_nullable: true,
			sequence: None,
		}
	}

	pub fn with_sequence(mut self, sequence: impl Into<String>) -> Self {
		self.sequence = Some(sequence.into());
		self
	}

	pub fn with_type(mut self, data_type: impl Into<String>) -> Self {
		self.data_type = Some(data_type.into());
		self
	}
}

/// An index of a table, as the system views list it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInfo {
	/// Index name.
	pub name: String,
	/// The indexed columns, in index order.
	pub columns: Vec<String>,
	/// Whether the index rejects duplicate entries.
	pub unique: bool,
}

/// What happens to a referencing row when the row it references is deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferentialAction {
	/// The delete is refused.
	NoAction,
	/// The referencing row is deleted too.
	Cascade,
	/// The referencing column is set to `NONE`.
	SetNull,
}

impl ReferentialAction {
	/// The `pg_constraint.confdeltype` code of the action.
	pub const fn pg_code(self) -> char {
		match self {
			Self::NoAction => 'a',
			Self::Cascade => 'c',
			Self::SetNull => 'n',
		}
	}

	/// The action as `pg_get_constraintdef` words it.
	pub const fn sql(self) -> &'static str {
		match self {
			Self::NoAction => "NO ACTION",
			Self::Cascade => "CASCADE",
			Self::SetNull => "SET NULL",
		}
	}
}

/// A column that holds record links to the primary key (`id`) of another table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyInfo {
	/// The referencing column.
	pub column: String,
	/// The referenced table.
	pub target_table: String,
	/// What deleting the referenced row does to this one.
	pub on_delete: ReferentialAction,
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
	/// The indexes the system views list for the table.
	pub indexes: Vec<IndexInfo>,
	/// The columns that reference another table, listed as foreign keys.
	pub foreign_keys: Vec<ForeignKeyInfo>,
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
			indexes: Vec::new(),
			foreign_keys: Vec::new(),
			closed: false,
		}
	}

	pub fn with_indexes(mut self, indexes: Vec<IndexInfo>) -> Self {
		self.indexes = indexes;
		self
	}

	pub fn with_foreign_keys(mut self, foreign_keys: Vec<ForeignKeyInfo>) -> Self {
		self.foreign_keys = foreign_keys;
		self
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

pub mod relations;
pub mod staged;
pub mod system;

/// The assertion of a column, as far as a statement that changes the column's type needs to read
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnAssert {
	/// The column has no assertion.
	None,
	/// The assertion is the `NOT NULL` one the SQL DDL defines (`$value != NONE`).
	NotNull,
	/// Any other assertion, written in SurrealQL.
	Other,
}

/// How a column is declared: what `ALTER TABLE` must know to give the column a new type.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnShape {
	/// The declared type, `None` for a column that declares none.
	pub kind: Option<Kind>,
	/// The assertion of the column.
	pub assert: ColumnAssert,
	/// Whether the column has a `DEFAULT`.
	pub has_default: bool,
}

impl ColumnShape {
	/// The shape of a column declared as `kind` with `assert` and a `DEFAULT` when `has_default`.
	pub fn new(kind: Option<Kind>, assert: Option<&Expr>, has_default: bool) -> Self {
		let assert = match assert {
			None => ColumnAssert::None,
			Some(expr) if crate::lower::ddl::is_not_null_assertion(expr) => ColumnAssert::NotNull,
			Some(_) => ColumnAssert::Other,
		};
		Self {
			kind,
			assert,
			has_default,
		}
	}
}

/// The table that a field of this type holds links to, when the type names exactly one table and
/// admits nothing but a link besides `NONE` and `NULL`: `record<t>`, `option<record<t>>` and
/// `record<t> | none`.
pub fn link_target(kind: &Kind) -> Option<&str> {
	match kind {
		Kind::Record(tables) => match tables.as_slice() {
			[table] => Some(table.as_str()),
			_ => None,
		},
		Kind::Either(kinds) => {
			let mut target = None;
			for kind in kinds {
				if matches!(kind, Kind::None | Kind::Null) {
					continue;
				}
				if target.is_some() {
					return None;
				}
				target = Some(link_target(kind)?);
			}
			target
		}
		Kind::Any
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Decimal
		| Kind::Duration
		| Kind::Float
		| Kind::F16
		| Kind::F32
		| Kind::Int
		| Kind::I8
		| Kind::I16
		| Kind::I32
		| Kind::U8
		| Kind::U16
		| Kind::U32
		| Kind::U64
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Geometry(_)
		| Kind::Set(..)
		| Kind::Array(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_) => None,
	}
}

/// The `information_schema.columns.data_type` of a column declared as `kind` where it has one
/// wire type: `None` for a type with no single one (a record link, a mixed union, `any`).
/// Optionality does not change it, and an array is its element's name followed by `[]`.
pub(crate) fn kind_type_name(kind: &Kind) -> Option<String> {
	let name = match kind {
		Kind::Bool => "boolean",
		Kind::Bytes => "bytea",
		Kind::Datetime => "timestamp with time zone",
		Kind::Decimal | Kind::U64 => "numeric",
		Kind::Duration => "interval",
		Kind::Float => "double precision",
		Kind::F16 | Kind::F32 => "real",
		Kind::Int | Kind::U32 => "bigint",
		Kind::I8 | Kind::I16 | Kind::U8 => "smallint",
		Kind::I32 | Kind::U16 => "integer",
		Kind::String => "text",
		Kind::Uuid => "uuid",
		Kind::Object => "jsonb",
		Kind::Array(element, _) => return kind_type_name(element).map(|name| format!("{name}[]")),
		Kind::Either(kinds) => {
			let mut names = kinds
				.iter()
				.filter(|kind| !matches!(kind, Kind::None | Kind::Null))
				.map(kind_type_name);
			let first = names.next()??;
			return names.all(|name| name.as_deref() == Some(first.as_str())).then_some(first);
		}
		Kind::Any
		| Kind::None
		| Kind::Null
		| Kind::Number
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Set(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_) => return None,
	};
	Some(name.to_owned())
}

/// Whether a column of `kind` can hold `NONE`.
pub fn kind_admits_none(kind: &Kind) -> bool {
	if let Kind::Either(kinds) = kind {
		kinds.iter().any(kind_admits_none)
	} else {
		matches!(kind, Kind::Any | Kind::None)
	}
}

/// Whether a column of `kind` can hold `NULL`: its type is `any`, `null`, or has a member that is.
pub fn kind_admits_null(kind: &Kind) -> bool {
	if let Kind::Either(kinds) = kind {
		kinds.iter().any(kind_admits_null)
	} else {
		matches!(kind, Kind::Any | Kind::Null)
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

	/// Current active database name, if known.
	fn current_database(&self) -> Option<&str> {
		None
	}

	/// Return all user-defined table schemas in the catalog.
	fn all_tables(&self) -> Vec<TableSchema> {
		Vec::new()
	}

	/// The tables a statement resolves constraint names and serial columns against: `ALTER TABLE`
	/// constraints, `pg_get_constraintdef` and `pg_get_serial_sequence`. The same as
	/// [`BinderCatalog::all_tables`] unless the catalog says otherwise, so that a catalog that
	/// records what is asked of it can tell this question from the binder's.
	fn constraint_tables(&self) -> Vec<TableSchema> {
		self.all_tables()
	}

	/// Every column of `table` whose type is declared, in no particular order. A schemaless
	/// table may declare only some of its columns, so this is not the table's column list.
	fn declared_column_types(
		&self,
		_schema: Option<&str>,
		_table: &str,
	) -> Vec<(String, crate::SqlType)> {
		Vec::new()
	}

	/// The declared type of column `column` of table `table` when a text value written to it is
	/// read as that type: the integer, float, `numeric`, boolean, timestamp, `uuid` and `json`
	/// types. [`crate::SqlType::Unknown`] for every other column, and for a column the catalog does
	/// not describe.
	///
	/// The table and the column are named as the lowering names them, and are compared exactly.
	fn write_type(&self, _schema: Option<&str>, _table: &str, _column: &str) -> crate::SqlType {
		crate::SqlType::Unknown
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

	/// Whether this catalog only records what a lowering asks of it and describes no table, so
	/// that its silence says nothing about the schema. A lowering asks this exactly where it would
	/// refuse a statement for what the schema lacks (a column that is not a link); on `true` it
	/// does not refuse and returns the statements it would run if the schema allowed them, and
	/// the caller lowers again against the schema.
	fn defers_schema_checks(&self) -> bool {
		false
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

	/// Whether the link column `column` of table `table` admits `NONE` (`option<record<t>>`, or a
	/// union with `none`), so that SQL `NULL` written to it is stored as `NONE`. A link column that
	/// does not refuses `NULL`. `false` where the catalog does not say.
	fn link_admits_none(&self, _schema: Option<&str>, _table: &str, _column: &str) -> bool {
		false
	}

	/// The columns of table `table` that hold NULL in a row written without a value for them: the
	/// columns the table declares whose type admits NULL and that have none of a `DEFAULT`, a
	/// `VALUE` and a `COMPUTED` clause. A column left out of an `INSERT` is stored as `NONE`
	/// unless the write names it, and `NONE` and `NULL` are two values to a grouping, a
	/// `DISTINCT` and a set operation, so an `INSERT` names each of these as `NULL`.
	///
	/// Empty for a table the catalog does not describe and for one that does not declare all the
	/// columns it has, where the columns that are left out are not known. The table is named as the
	/// lowering names it and compared exactly.
	fn null_columns(&self, _schema: Option<&str>, _table: &str) -> Vec<String> {
		Vec::new()
	}

	/// The columns of table `table` that take a default in a row written without a value for them,
	/// with the expression that gives it: the columns the table declares with a `DEFAULT` clause
	/// and without a `VALUE` or a `COMPUTED` one. A `REPLACE` over an existing row writes the row
	/// as a new one, so each of these is set to its default, as each of
	/// [`null_columns`](Self::null_columns) is set to `NULL`, where an `INSERT` of a new row
	/// leaves the defaults to the table.
	///
	/// Empty where [`null_columns`](Self::null_columns) is, and compared as it is.
	fn default_columns(&self, _schema: Option<&str>, _table: &str) -> Vec<(String, Expr)> {
		Vec::new()
	}

	/// How column `column` of table `table` is declared, for the statements that give a column a
	/// new type (`ALTER TABLE ... ADD FOREIGN KEY` over a scalar column). The answer is `None` for
	/// a table or column the catalog does not describe.
	///
	/// The names are compared exactly, as for [`BinderCatalog::link_table`].
	fn column_shape(
		&self,
		_schema: Option<&str>,
		_table: &str,
		_column: &str,
	) -> Option<ColumnShape> {
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
	/// Tables that [`BinderCatalog::all_tables`] lists in the system views and
	/// [`BinderCatalog::get_table`] does not resolve, so a statement naming one binds against it
	/// as against an undescribed table.
	listed: Vec<TableSchema>,
	/// The link columns, by (schema, table, column), and the table each holds links to. Kept apart
	/// from `tables`: describing a link column does not describe its table.
	links: HashMap<(Option<String>, String, String), String>,
	current_db: Option<String>,
	column_types: HashMap<(String, String), crate::SqlType>,
	/// How columns are declared, by (table, column).
	shapes: HashMap<(String, String), ColumnShape>,
	/// The link columns that admit `NONE`, by (table, column).
	nullable_links: std::collections::HashSet<(String, String)>,
	/// The columns that hold NULL in a row written without them, by table.
	null_columns: HashMap<String, Vec<String>>,
	default_columns: HashMap<String, Vec<(String, Expr)>>,
	/// The dimensions of the vector columns, by (table, column).
	vectors: HashMap<(String, String), usize>,
	/// The types text is read as when written to a column, by (table, column).
	write_types: HashMap<(String, String), crate::SqlType>,
	/// The HNSW indexes, by table, as the column each covers and its metric.
	hnsw: HashMap<String, Vec<(String, crate::vector::VectorMetric)>>,
}

impl MemoryCatalog {
	pub fn new() -> Self {
		Self {
			tables: HashMap::new(),
			listed: Vec::new(),
			links: HashMap::new(),
			current_db: None,
			column_types: HashMap::new(),
			shapes: HashMap::new(),
			nullable_links: std::collections::HashSet::new(),
			null_columns: HashMap::new(),
			default_columns: HashMap::new(),
			vectors: HashMap::new(),
			write_types: HashMap::new(),
			hnsw: HashMap::new(),
		}
	}

	/// Describe the vector columns that `other` does, besides what this catalog describes.
	pub fn with_vectors_of(mut self, other: &MemoryCatalog) -> Self {
		self.vectors.extend(other.vectors.iter().map(|(key, count)| (key.clone(), *count)));
		self
	}

	/// Forget the HNSW indexes, as a catalog does that cannot say which indexes a request leaves.
	pub fn clear_hnsw_indexes(&mut self) {
		self.hnsw.clear();
	}

	/// Declare that the link column `column` of table `table` admits `NONE`; see
	/// [`BinderCatalog::link_admits_none`].
	pub fn with_nullable_link(mut self, table: &str, column: &str) -> Self {
		self.nullable_links.insert((table.to_owned(), column.to_owned()));
		self
	}

	pub fn add_nullable_link(&mut self, table: &str, column: &str) -> &mut Self {
		self.nullable_links.insert((table.to_owned(), column.to_owned()));
		self
	}

	/// Declare the columns of table `table` that hold NULL in a row written without them; see
	/// [`BinderCatalog::null_columns`].
	pub fn add_null_columns(&mut self, table: &str, columns: Vec<String>) -> &mut Self {
		self.null_columns.insert(table.to_owned(), columns);
		self
	}

	/// Declare the NULL-holding columns of a table, as [`Self::add_null_columns`] does.
	pub fn with_null_columns(mut self, table: &str, columns: &[&str]) -> Self {
		self.add_null_columns(table, columns.iter().map(|column| (*column).to_owned()).collect());
		self
	}

	/// Declare the columns of table `table` that take a default in a row written without them,
	/// with their default expressions; see [`BinderCatalog::default_columns`].
	pub fn add_default_columns(&mut self, table: &str, columns: Vec<(String, Expr)>) -> &mut Self {
		self.default_columns.insert(table.to_owned(), columns);
		self
	}

	/// Declare the defaulted columns of a table, as [`Self::add_default_columns`] does.
	pub fn with_default_columns(mut self, table: &str, columns: &[(&str, Expr)]) -> Self {
		self.add_default_columns(
			table,
			columns
				.iter()
				.map(|(column, default)| ((*column).to_owned(), default.clone()))
				.collect(),
		);
		self
	}

	/// Declare how column `column` of table `table` is declared; see
	/// [`BinderCatalog::column_shape`].
	pub fn with_column_shape(mut self, table: &str, column: &str, shape: ColumnShape) -> Self {
		self.shapes.insert((table.to_owned(), column.to_owned()), shape);
		self
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

	/// Declare that text written to column `column` of table `table` is read as `ty`.
	pub fn add_write_type(&mut self, table: &str, column: &str, ty: crate::SqlType) -> &mut Self {
		self.write_types.insert((table.to_owned(), column.to_owned()), ty);
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

	/// List `tables` in the system views (`information_schema.tables`, `pg_class`, ...) without
	/// describing them to the binder: a table named in a statement stays as described, or
	/// undescribed, as it was.
	pub fn with_listed_tables(mut self, tables: Vec<TableSchema>) -> Self {
		self.listed = tables;
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
		let mut seen =
			std::collections::HashSet::with_capacity(self.tables.len() + self.listed.len());
		self.tables
			.values()
			.chain(&self.listed)
			.filter(|t| seen.insert((t.schema.as_deref().unwrap_or("public"), t.name.as_str())))
			.cloned()
			.collect()
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

	fn link_admits_none(&self, _schema: Option<&str>, table: &str, column: &str) -> bool {
		self.nullable_links.contains(&(table.to_owned(), column.to_owned()))
	}

	fn null_columns(&self, _schema: Option<&str>, table: &str) -> Vec<String> {
		self.null_columns.get(table).cloned().unwrap_or_default()
	}

	fn default_columns(&self, _schema: Option<&str>, table: &str) -> Vec<(String, Expr)> {
		self.default_columns.get(table).cloned().unwrap_or_default()
	}

	fn column_shape(&self, schema: Option<&str>, table: &str, column: &str) -> Option<ColumnShape> {
		if schema.is_some_and(|schema| schema != "public") {
			return None;
		}
		self.shapes.get(&(table.to_owned(), column.to_owned())).cloned()
	}

	fn declared_column_types(
		&self,
		_schema: Option<&str>,
		table: &str,
	) -> Vec<(String, crate::SqlType)> {
		self.column_types
			.iter()
			.filter(|((name, _), _)| name == table)
			.map(|((_, column), ty)| (column.clone(), *ty))
			.collect()
	}

	fn vector_dimension(&self, _schema: Option<&str>, table: &str, column: &str) -> Option<usize> {
		self.vectors.get(&(table.to_owned(), column.to_owned())).copied()
	}

	fn write_type(&self, _schema: Option<&str>, table: &str, column: &str) -> crate::SqlType {
		self.write_types
			.get(&(table.to_owned(), column.to_owned()))
			.copied()
			.unwrap_or(crate::SqlType::Unknown)
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
