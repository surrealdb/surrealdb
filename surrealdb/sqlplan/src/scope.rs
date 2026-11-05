//! Scopes and name resolution for SQL binding.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use crate::catalog::{BinderCatalog, EnumColumn};
use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::ir::{
	BoundColumnRef, BoundCte, BoundExpr, BoundSelect, BoundTableSource, BoundTarget,
	BoundWindowSpec, CarriedColumn,
};

/// The most relations an unqualified column of a join may be read from. Each is a level of the
/// lowered expression, so a FROM clause cannot make the expression arbitrarily deep.
const MAX_COLUMN_HOLDERS: usize = 64;

/// The start of the name of a parameter that holds a column of an enclosing query
/// ([`BoundColumnRef::carried_as`]). A client's positional parameters are bound as `_<n>` and a
/// MySQL user variable as `user_var_<name>`, so no name a client binds starts with it.
pub const CARRIED_PREFIX: &str = "__sql_outer_";

/// A single relation registered in the current query scope (from a FROM clause or JOIN).
#[derive(Debug, Clone)]
pub struct RelationEntry {
	/// 0-based relation index (0 for primary, 1..N for joined tables).
	pub relation_index: usize,
	/// Table name (if a direct table).
	pub table_name: String,
	/// Optional schema namespace (e.g. `public`).
	pub schema: Option<String>,
	/// Alias given to this relation, or the table name if no alias was specified.
	pub exposed_name: String,
	/// Known column names from the catalog (if catalog has schema for this table).
	pub known_columns: Option<Vec<String>>,
	/// Whether `known_columns` is every column the relation has, so that any other name is an
	/// error: a table the datastore declares the columns of. A system catalog table's list is a
	/// subset of what a client may read, and a schemaless table has none.
	pub closed: bool,
	/// Whether this relation reads a base table, whose `id` column is the stored record id and
	/// is read as its key. False for subquery aliases and CTEs, whose `id` is a plain value.
	pub is_base_table: bool,
	/// Bound projection supplying scalar and record-link provenance for a derived relation.
	pub source_query: Option<Arc<BoundSelect>>,
}

impl RelationEntry {
	/// Whether this relation is closed and none of its columns is `column`.
	fn lacks_column(&self, column: &str, policy: &DialectPolicy) -> bool {
		self.closed
			&& !self
				.known_columns
				.as_ref()
				.is_some_and(|columns| columns.iter().any(|c| policy.idents_match(c, column)))
	}

	/// The target a `*` or `t.*` expands to for the declared column `column` of this closed
	/// relation: the column itself, as if the query had selected it by name, so that every column
	/// is read and named as declared.
	fn declared_target(&self, column: &str, catalog: &dyn BinderCatalog) -> BoundTarget {
		BoundTarget {
			expr: BoundExpr::Column(self.column_ref(column.to_string(), catalog)),
			alias: None,
			is_wildcard: false,
			table_wildcard: None,
		}
	}

	/// A reference to column `column` (already folded) of this relation, bound at
	/// `correlation_depth` 0.
	///
	/// For a base-table relation the catalog is asked which table the column holds record links to.
	/// `id` is asked about too, but never holds links.
	fn column_ref(&self, column: String, catalog: &dyn BinderCatalog) -> BoundColumnRef {
		let id_table = (self.is_base_table && column == "id").then(|| self.table_name.clone());
		let derived =
			self.source_query.as_ref().map(|query| projected_column(query, &column, catalog));
		let enum_column = if self.is_base_table && column != "*" && column != "id" {
			catalog.column_enum(self.schema.as_deref(), &self.table_name, &column)
		} else {
			derived.as_ref().and_then(|(_, _, held)| held.clone())
		}
		.map(Box::new);
		let composite = if self.is_base_table && column != "*" && column != "id" {
			catalog.column_composite(self.schema.as_deref(), &self.table_name, &column)
		} else {
			None
		};
		let declared_sql = if self.is_base_table && column != "*" && column != "id" {
			catalog.column_sql_type(self.schema.as_deref(), &self.table_name, &column).map(Box::new)
		} else {
			None
		};
		let link_table = if self.is_base_table && column != "*" {
			catalog
				.link_table(self.schema.as_deref(), &self.table_name, &column)
				.filter(|_| column != "id")
		} else {
			derived.as_ref().and_then(|(_, link, _)| link.clone())
		};
		let key_read = if id_table.is_some() {
			crate::ir::KeyRead::of(catalog.write_type(
				self.schema.as_deref(),
				&self.table_name,
				"id",
			))
		} else if let Some(target) = link_table.as_deref() {
			crate::ir::KeyRead::of(catalog.write_type(self.schema.as_deref(), target, "id"))
		} else {
			crate::ir::KeyRead::None
		};
		BoundColumnRef {
			relation_index: self.relation_index,
			relation_name: self.exposed_name.clone(),
			declared_type: if self.is_base_table
				|| (self.source_query.is_none()
					&& crate::catalog::system::is_system_catalog_table(
						self.schema.as_deref(),
						&self.table_name,
					)) {
				// A system relation is described by the catalog under its own name, whatever
				// alias the query gives it.
				catalog.column_type(self.schema.as_deref(), &self.table_name, &column)
			} else {
				derived.map(|(ty, _, _)| ty).unwrap_or(crate::SqlType::Unknown)
			},
			column,
			correlation_depth: 0,
			id_table,
			link_table,
			enum_column,
			composite,
			declared_sql,
			aggregate_output: false,
			key_read,
			also_in: Vec::new(),
			carried_as: None,
		}
	}
}

/// The scalar type and stored-link target of a projected column. Wildcards inherit only from
/// a single source; joins with unlisted columns do not supply unambiguous provenance.
fn projected_column(
	query: &BoundSelect,
	column: &str,
	catalog: &dyn BinderCatalog,
) -> (crate::SqlType, Option<String>, Option<EnumColumn>) {
	for target in &query.targets {
		let name = target.alias.as_deref().or(match &target.expr {
			BoundExpr::Column(col) => Some(col.column.as_str()),
			BoundExpr::Literal(_)
			| BoundExpr::Access {
				..
			}
			| BoundExpr::Param {
				..
			}
			| BoundExpr::Binary {
				..
			}
			| BoundExpr::Unary {
				..
			}
			| BoundExpr::FunctionCall {
				..
			}
			| BoundExpr::Case {
				..
			}
			| BoundExpr::Between {
				..
			}
			| BoundExpr::InList {
				..
			}
			| BoundExpr::IsNull {
				..
			}
			| BoundExpr::Cast {
				..
			}
			| BoundExpr::ScalarSubquery(_)
			| BoundExpr::Exists(_)
			| BoundExpr::InSubquery {
				..
			}
			| BoundExpr::ArraySubquery(_)
			| BoundExpr::WindowFunction {
				..
			}
			| BoundExpr::UserVar(_)
			| BoundExpr::SystemVar {
				..
			} => None,
		});
		if name == Some(column) && !target.is_unlisted_wildcard() {
			return match &target.expr {
				BoundExpr::Column(col) => (
					col.declared_type,
					col.link_table.clone(),
					col.enum_column.clone().map(|held| *held),
				),
				BoundExpr::Literal(_)
				| BoundExpr::Access {
					..
				}
				| BoundExpr::Param {
					..
				}
				| BoundExpr::Binary {
					..
				}
				| BoundExpr::Unary {
					..
				}
				| BoundExpr::FunctionCall {
					..
				}
				| BoundExpr::Case {
					..
				}
				| BoundExpr::Between {
					..
				}
				| BoundExpr::InList {
					..
				}
				| BoundExpr::IsNull {
					..
				}
				| BoundExpr::Cast {
					..
				}
				| BoundExpr::ScalarSubquery(_)
				| BoundExpr::Exists(_)
				| BoundExpr::InSubquery {
					..
				}
				| BoundExpr::ArraySubquery(_)
				| BoundExpr::WindowFunction {
					..
				}
				| BoundExpr::UserVar(_)
				| BoundExpr::SystemVar {
					..
				} => (query.infer_expr_type(&target.expr, catalog), None, None),
			};
		}
	}
	if query.targets.iter().any(BoundTarget::is_unlisted_wildcard) {
		match query.from.as_ref() {
			Some(BoundTableSource::Table {
				schema,
				name,
				..
			}) => {
				let link = (column != "id")
					.then(|| catalog.link_table(schema.as_deref(), name, column))
					.flatten();
				let held = (column != "id")
					.then(|| catalog.column_enum(schema.as_deref(), name, column))
					.flatten();
				return (catalog.column_type(schema.as_deref(), name, column), link, held);
			}
			Some(BoundTableSource::Subquery {
				query,
				..
			}) => return projected_column(query, column, catalog),
			_ => {}
		}
	}
	(crate::SqlType::Unknown, None, None)
}

/// The lexical scope of a query or subquery, maintaining available relations and target bindings.
#[derive(Debug, Clone)]
pub struct QueryScope {
	/// Optional parent scope for correlated subqueries.
	pub parent: Option<Arc<QueryScope>>,
	/// All relations available in this scope, indexed in join order.
	pub relations: Vec<RelationEntry>,
	/// Mapping from table alias or table name to relation index.
	pub name_to_relation: HashMap<String, usize>,
	/// Target projection expressions in this query, for resolving positional and alias GROUP BY /
	/// ORDER BY.
	pub targets: Vec<BoundTarget>,
	/// Common Table Expressions (CTEs) available in this query scope.
	pub ctes: HashMap<String, BoundCte>,
	/// Named window specifications defined in WINDOW clause.
	pub windows: HashMap<String, BoundWindowSpec>,
	/// The columns a `JOIN ... USING` merges, folded: one column each, whichever side holds it.
	using_columns: Vec<String>,
	/// How many scopes enclose this one: 0 for the outermost query.
	level: usize,
	/// The columns of the enclosing query that queries nested in this one read, which this query
	/// binds to parameters when it is entered ([`BoundSelect::carried`]). The list is shared by
	/// every copy of the scope: a subquery is bound against a copy of the scope it stands in.
	carried: Arc<Mutex<Vec<CarriedColumn>>>,
}

impl Default for QueryScope {
	fn default() -> Self {
		Self::new()
	}
}

impl QueryScope {
	/// Create an empty root scope.
	pub fn new() -> Self {
		Self {
			parent: None,
			relations: Vec::new(),
			name_to_relation: HashMap::new(),
			targets: Vec::new(),
			ctes: HashMap::new(),
			windows: HashMap::new(),
			using_columns: Vec::new(),
			level: 0,
			carried: Arc::default(),
		}
	}

	/// Create a child scope with a parent (for subqueries).
	pub fn with_parent(parent: Arc<QueryScope>) -> Self {
		Self {
			level: parent.level + 1,
			parent: Some(parent),
			relations: Vec::new(),
			name_to_relation: HashMap::new(),
			targets: Vec::new(),
			ctes: HashMap::new(),
			windows: HashMap::new(),
			using_columns: Vec::new(),
			carried: Arc::default(),
		}
	}

	/// Note the columns a `JOIN ... USING` merges, which name one column and are never ambiguous.
	pub fn note_using_columns(&mut self, columns: &[String], policy: &DialectPolicy) {
		self.using_columns.extend(columns.iter().map(|c| policy.fold_identifier(c).into_owned()));
	}

	/// Register a named window specification.
	pub fn register_window(&mut self, name: &str, spec: BoundWindowSpec, policy: &DialectPolicy) {
		let key = policy.fold_identifier(name).into_owned();
		self.windows.insert(key, spec);
	}

	/// Resolve a named window specification, traversing parent scopes if necessary.
	pub fn resolve_window(&self, name: &str, policy: &DialectPolicy) -> Option<&BoundWindowSpec> {
		let key = policy.fold_identifier(name).into_owned();
		if let Some(spec) = self.windows.get(&key) {
			Some(spec)
		} else if let Some(parent) = &self.parent {
			parent.resolve_window(name, policy)
		} else {
			None
		}
	}

	/// Register a relation in the scope.
	pub fn register_relation(
		&mut self,
		schema: Option<String>,
		table_name: &str,
		alias: Option<String>,
		catalog: &dyn BinderCatalog,
		policy: &DialectPolicy,
	) -> usize {
		let relation_index = self.relations.len();
		let aliased = alias.is_some();
		// An unaliased relation is exposed under its unqualified name, so `columns.*` and
		// `columns.column_name` resolve against `information_schema.columns`.
		let exposed_name = alias.unwrap_or_else(|| match schema.as_deref() {
			Some(s) => table_name
				.strip_prefix(s)
				.and_then(|rest| rest.strip_prefix('.'))
				.unwrap_or(table_name)
				.to_string(),
			None => table_name.to_string(),
		});

		// Query catalog for known columns if available
		let described = catalog.get_table(schema.as_deref(), table_name);
		let closed = described.as_ref().is_some_and(|table| table.closed);
		let known_columns = described.map(|table| table.column_names());

		let entry = RelationEntry {
			relation_index,
			table_name: table_name.to_string(),
			schema: schema.clone(),
			exposed_name: exposed_name.clone(),
			known_columns,
			closed,
			source_query: None,
			is_base_table: !crate::catalog::system::is_system_catalog_table(
				schema.as_deref(),
				table_name,
			),
		};

		self.relations.push(entry);

		// Register in name lookup under folded identifier
		let key_exposed = policy.fold_identifier(&exposed_name).into_owned();
		self.name_to_relation.insert(key_exposed, relation_index);

		// An aliased relation is reachable only by its alias: the table's own name is then free to
		// mean the same table of an enclosing query, or another relation of this one. A relation
		// without an alias is also reachable by its table name and, qualified, by its schema.
		if !aliased {
			let key_table = policy.fold_identifier(table_name).into_owned();
			self.name_to_relation.entry(key_table).or_insert(relation_index);

			if let Some(s) = schema {
				let full_name = format!("{s}.{table_name}");
				let key_full = policy.fold_identifier(&full_name).into_owned();
				self.name_to_relation.entry(key_full).or_insert(relation_index);
			}
		}

		relation_index
	}

	/// Register a subquery relation with a required alias.
	pub fn register_subquery(&mut self, alias: &str, policy: &DialectPolicy) -> usize {
		let relation_index = self.relations.len();
		let entry = RelationEntry {
			relation_index,
			table_name: alias.to_string(),
			schema: None,
			exposed_name: alias.to_string(),
			known_columns: None,
			closed: false,
			is_base_table: false,
			source_query: None,
		};

		self.relations.push(entry);
		let key = policy.fold_identifier(alias).into_owned();
		self.name_to_relation.insert(key, relation_index);
		relation_index
	}

	/// Register a subquery relation with optional known columns.
	pub fn register_subquery_with_columns(
		&mut self,
		alias: &str,
		columns: Option<Vec<String>>,
		policy: &DialectPolicy,
	) -> usize {
		let relation_index = self.relations.len();
		let entry = RelationEntry {
			relation_index,
			table_name: alias.to_string(),
			schema: None,
			exposed_name: alias.to_string(),
			known_columns: columns,
			closed: false,
			is_base_table: false,
			source_query: None,
		};

		self.relations.push(entry);
		let key = policy.fold_identifier(alias).into_owned();
		self.name_to_relation.insert(key, relation_index);
		relation_index
	}

	/// Register a Common Table Expression in this scope.
	pub fn register_cte(&mut self, cte: BoundCte, policy: &DialectPolicy) {
		let key = policy.fold_identifier(&cte.name).into_owned();
		self.ctes.insert(key, cte);
	}

	/// Lookup a Common Table Expression by name, searching local scope and parent scopes.
	pub fn get_cte(&self, name: &str, policy: &DialectPolicy) -> Option<&BoundCte> {
		let key = policy.fold_identifier(name).into_owned();
		if let Some(cte) = self.ctes.get(&key) {
			return Some(cte);
		}
		if let Some(parent) = &self.parent {
			return parent.get_cte(name, policy);
		}
		None
	}

	/// Resolve a column reference given identifier parts (e.g. `["col"]` or `["tbl", "col"]` or
	/// `["schema", "tbl", "col"]`). `catalog` says which columns of a base table hold record links.
	///
	/// A column of an enclosing query is read from the row of the query directly above (`$parent`)
	/// and from no row further up. The column of a query two or more levels above the reference
	/// is therefore carried: the query one level below the column's query reads it when it is
	/// entered and binds it to a parameter ([`CarriedColumn`]), and the reference reads the
	/// parameter ([`BoundColumnRef::carried_as`]).
	pub fn resolve_column(
		&self,
		parts: &[String],
		policy: &DialectPolicy,
		catalog: &dyn BinderCatalog,
	) -> Result<BoundColumnRef, PlanError> {
		let mut col = self.resolve_column_in_scopes(parts, policy, catalog)?;
		if col.correlation_depth > 1 {
			col.carried_as = Some(self.carry(&col)?);
		}
		Ok(col)
	}

	/// The columns of the enclosing query that queries nested in this one read, in the order they
	/// were first read.
	pub fn carried_columns(&self) -> Vec<CarriedColumn> {
		self.carried.lock().unwrap_or_else(PoisonError::into_inner).clone()
	}

	/// Have the query one level below the query `col` belongs to read `col`, and answer the name
	/// of the parameter it binds the value to. `col` was resolved from this scope at a
	/// `correlation_depth` above 1.
	///
	/// A column the query reads already keeps its parameter, so each is read once however many
	/// references name it. The name holds the level of the column's query and the position of the
	/// column among those the binding query carries, so the queries on one chain of enclosing
	/// queries bind parameters of different names.
	fn carry(&self, col: &BoundColumnRef) -> Result<String, PlanError> {
		let binder = (1..col.correlation_depth)
			.try_fold(self, |scope, _| scope.parent.as_deref())
			.ok_or_else(|| {
				PlanError::Custom(format!(
					"column \"{}\" belongs to a query {} levels above a query that has fewer",
					col.column, col.correlation_depth
				))
			})?;
		let mut carried = binder.carried.lock().unwrap_or_else(PoisonError::into_inner);
		let source = BoundColumnRef {
			correlation_depth: 1,
			carried_as: None,
			..col.clone()
		};
		let same = |held: &BoundColumnRef| {
			held.relation_index == source.relation_index && held.column == source.column
		};
		if let Some(held) = carried.iter().find(|held| same(&held.column)) {
			return Ok(held.name.clone());
		}
		let name = format!("{CARRIED_PREFIX}{}_{}", binder.level.saturating_sub(1), carried.len());
		carried.push(CarriedColumn {
			name: name.clone(),
			column: source,
		});
		Ok(name)
	}

	/// Resolve a column reference through this scope and the scopes of the enclosing queries.
	fn resolve_column_in_scopes(
		&self,
		parts: &[String],
		policy: &DialectPolicy,
		catalog: &dyn BinderCatalog,
	) -> Result<BoundColumnRef, PlanError> {
		match parts.len() {
			0 => Err(PlanError::Custom("empty column reference".into())),
			1 => {
				// Unqualified column name: e.g. `id`
				let col_name = parts[0].clone();
				self.resolve_unqualified_column(&col_name, policy, catalog)
			}
			2 => {
				// Qualified column name: e.g. `tbl.col` or `alias.col`
				let prefix = &parts[0];
				let col_name = &parts[1];
				self.resolve_qualified_column(prefix, col_name, policy, catalog)
			}
			3 => {
				// Schema-qualified column name: e.g. `public.users.id`
				let full_prefix = format!("{}.{}", parts[0], parts[1]);
				let col_name = &parts[2];
				match self.resolve_qualified_column(&full_prefix, col_name, policy, catalog) {
					Ok(col) => return Ok(col),
					Err(err @ PlanError::UndefinedColumn(_)) => return Err(err),
					Err(_) => {}
				}
				// If not matching a schema-qualified table, try prefix as table and parts[1..] as
				// nested field
				self.resolve_qualified_column(&parts[0], &parts[1], policy, catalog)
			}
			_ => {
				// Deeper dotted access: try parts[0] as relation, parts[1] as column
				self.resolve_qualified_column(&parts[0], &parts[1], policy, catalog)
			}
		}
	}

	/// Resolve a column named without its relation.
	///
	/// A relation that does not list its columns (a schemaless table) may hold any column, so a
	/// column that no relation lists is read from the first of those relations that has a value;
	/// it is then no error when it is in none of them or in several, as nothing says it is not.
	/// Every other column is resolved by [`Self::resolve_listed_column`].
	fn resolve_unqualified_column(
		&self,
		col_name: &str,
		policy: &DialectPolicy,
		catalog: &dyn BinderCatalog,
	) -> Result<BoundColumnRef, PlanError> {
		match self.resolve_open_column(col_name, policy, catalog)? {
			Some(col) => Ok(col),
			None => self.resolve_listed_column(col_name, policy, catalog),
		}
	}

	/// Resolve a column named without its relation among the relations of a join that do not list
	/// their columns.
	///
	/// The column is bound to the relation that leads and carries the others that may hold it in
	/// [`BoundColumnRef::also_in`]. A holder whose column the catalog describes (a link, a declared
	/// type) leads, and the others follow in the order of the FROM clause, as the value of a column
	/// merged by `USING` is that of whichever side has one. A relation whose columns are known and
	/// lack the column cannot hold it. The `id` of every base table is its key, so the first of
	/// these relations holds it and no other is read.
	///
	/// Returns `None` when the column is not one of these relations': a single relation or none, a
	/// relation that lists the column (it binds the column, or the column is ambiguous), or no
	/// relation that may hold it. More than `MAX_COLUMN_HOLDERS` holders is an error, as each is a
	/// level of the lowered expression.
	fn resolve_open_column(
		&self,
		col_name: &str,
		policy: &DialectPolicy,
		catalog: &dyn BinderCatalog,
	) -> Result<Option<BoundColumnRef>, PlanError> {
		let lists_column = |rel: &RelationEntry| {
			rel.known_columns
				.as_ref()
				.is_some_and(|cols| cols.iter().any(|c| policy.idents_match(c, col_name)))
		};
		if self.relations.len() < 2 || self.relations.iter().any(lists_column) {
			return Ok(None);
		}

		let folded = policy.fold_identifier(col_name).into_owned();
		let limit = if folded == "id" {
			1
		} else {
			MAX_COLUMN_HOLDERS + 1
		};
		let mut holders: Vec<BoundColumnRef> = self
			.relations
			.iter()
			.filter(|rel| rel.known_columns.is_none())
			.take(limit)
			.map(|rel| rel.column_ref(folded.clone(), catalog))
			.collect();
		if holders.len() > MAX_COLUMN_HOLDERS {
			return Err(PlanError::UnsupportedExpression(format!(
				"column \"{col_name}\" may be in more than {MAX_COLUMN_HOLDERS} tables of the FROM clause, qualify it with its table"
			)));
		}
		if holders.is_empty() {
			return Ok(None);
		}

		if let Some(described) = holders.iter().position(|col| {
			col.link_table.is_some()
				|| col.enum_column.is_some()
				|| col.declared_type != crate::SqlType::Unknown
		}) {
			holders.swap(0, described);
			// Each holder is a distinct relation, and `relation_index` is its position in the FROM
			// clause, so the keys are unique and the order does not depend on the sort's stability.
			holders[1..].sort_unstable_by_key(|col| col.relation_index);
		}
		let mut col = holders.remove(0);
		col.also_in = holders.iter().map(|other| other.relation_index).collect();
		Ok(Some(col))
	}

	/// Resolve a column named without its relation from the columns the relations list and from
	/// the outer queries.
	fn resolve_listed_column(
		&self,
		col_name: &str,
		policy: &DialectPolicy,
		catalog: &dyn BinderCatalog,
	) -> Result<BoundColumnRef, PlanError> {
		if self.relations.is_empty() {
			if let Some(parent) = &self.parent {
				let mut col = parent.resolve_unqualified_column(col_name, policy, catalog)?;
				col.correlation_depth += 1;
				return Ok(col);
			}
			// In a query without FROM clause, unqualified columns do not exist
			return Err(PlanError::UndefinedColumn(col_name.to_string()));
		}

		// If exactly 1 relation in scope, bind directly to relation 0
		if self.relations.len() == 1 {
			let rel = &self.relations[0];
			if rel.lacks_column(col_name, policy) {
				return self.resolve_undeclared_column(col_name, policy, catalog);
			}
			return Ok(rel.column_ref(policy.fold_identifier(col_name).into_owned(), catalog));
		}

		// Multiple relations in scope: check known columns across relations. A derived table
		// has the columns its projection names, whether or not the alias lists them.
		let mut matching_relations = Vec::new();
		for rel in &self.relations {
			let listed = rel
				.known_columns
				.as_ref()
				.is_some_and(|cols| cols.iter().any(|c| policy.idents_match(c, col_name)));
			let projected = rel.source_query.as_ref().is_some_and(|query| {
				query.targets.iter().any(|target| {
					let name = match (&target.alias, &target.expr) {
						(Some(alias), _) => Some(alias.as_str()),
						(None, BoundExpr::Column(col)) if col.column != "*" => {
							Some(col.column.as_str())
						}
						_ => None,
					};
					name.is_some_and(|name| policy.idents_match(name, col_name))
				})
			});
			if listed || projected {
				matching_relations.push(rel);
			}
		}

		if matching_relations.len() == 1 {
			let rel = matching_relations[0];
			return Ok(rel.column_ref(policy.fold_identifier(col_name).into_owned(), catalog));
		} else if matching_relations.len() > 1 {
			// A column `JOIN ... USING` merges is one column, which the first relation reads.
			let folded = policy.fold_identifier(col_name).into_owned();
			if self.using_columns.contains(&folded) {
				return Ok(matching_relations[0].column_ref(folded, catalog));
			}
			return Err(PlanError::AmbiguousColumn(col_name.to_string()));
		}

		// Every relation declares its columns and none has this one.
		if self.relations.iter().all(|rel| rel.closed) {
			return self.resolve_undeclared_column(col_name, policy, catalog);
		}

		// If relations have known columns and col was not found, check parent before fallback
		if self.relations.iter().any(|r| r.known_columns.is_some())
			&& let Some(parent) = &self.parent
			&& let Ok(mut col) = parent.resolve_unqualified_column(col_name, policy, catalog)
		{
			col.correlation_depth += 1;
			return Ok(col);
		}

		// Fallback when schema is not strictly known (schemaless tables): default to the first
		// relation whose columns are not all declared, as the name cannot be a column of one that
		// declares them.
		let rel = self.relations.iter().find(|rel| !rel.closed).unwrap_or(&self.relations[0]);
		Ok(rel.column_ref(policy.fold_identifier(col_name).into_owned(), catalog))
	}

	/// Resolve `col_name`, which no relation of this scope has as a column, though each declares
	/// all of its columns.
	///
	/// The name may be an output alias of this query (`HAVING total > 1`), a column of an enclosing
	/// query, or a relation's name (a whole-row reference), in that order; an alias and a relation
	/// name read as they would from a table that declares nothing. Otherwise it is no column at
	/// all.
	fn resolve_undeclared_column(
		&self,
		col_name: &str,
		policy: &DialectPolicy,
		catalog: &dyn BinderCatalog,
	) -> Result<BoundColumnRef, PlanError> {
		let folded = policy.fold_identifier(col_name).into_owned();
		let is_alias = self.targets.iter().any(|target| {
			target.alias.as_deref().is_some_and(|a| policy.idents_match(a, col_name))
		});
		if is_alias {
			return Ok(self.relations[0].column_ref(folded, catalog));
		}
		if let Some(parent) = &self.parent
			&& let Ok(mut col) = parent.resolve_unqualified_column(col_name, policy, catalog)
		{
			col.correlation_depth += 1;
			return Ok(col);
		}
		if self.name_to_relation.contains_key(&folded) {
			return Ok(self.relations[0].column_ref(folded, catalog));
		}
		Err(PlanError::UndefinedColumn(col_name.to_string()))
	}

	fn resolve_qualified_column(
		&self,
		prefix: &str,
		col_name: &str,
		policy: &DialectPolicy,
		catalog: &dyn BinderCatalog,
	) -> Result<BoundColumnRef, PlanError> {
		let key = policy.fold_identifier(prefix).into_owned();
		if let Some(&rel_idx) = self.name_to_relation.get(&key) {
			let rel = &self.relations[rel_idx];
			if rel.lacks_column(col_name, policy) {
				return Err(PlanError::UndefinedColumn(col_name.to_string()));
			}
			return Ok(rel.column_ref(policy.fold_identifier(col_name).into_owned(), catalog));
		}

		// Check parent scope for correlated subqueries
		if let Some(parent) = &self.parent
			&& let Ok(mut col) = parent.resolve_qualified_column(prefix, col_name, policy, catalog)
		{
			col.correlation_depth += 1;
			return Ok(col);
		}

		Err(PlanError::MissingFromClauseEntry(prefix.to_string()))
	}

	/// Resolve a `GROUP BY` positional ordinal (1-indexed).
	pub fn resolve_group_by_ordinal(&self, pos: usize) -> Result<BoundExpr, PlanError> {
		if pos == 0 || pos > self.targets.len() {
			return Err(PlanError::InvalidGroupByPosition(pos));
		}
		Ok(self.targets[pos - 1].expr.clone())
	}

	/// Resolve an `ORDER BY` positional ordinal (1-indexed).
	pub fn resolve_order_by_ordinal(&self, pos: usize) -> Result<BoundExpr, PlanError> {
		if pos == 0 || pos > self.targets.len() {
			return Err(PlanError::InvalidOrderByPosition(pos));
		}
		let target = &self.targets[pos - 1];
		if let Some(alias) = &target.alias {
			Ok(BoundExpr::Column(BoundColumnRef {
				relation_index: 0,
				relation_name: String::new(),
				column: alias.clone(),
				correlation_depth: 0,
				id_table: None,
				link_table: None,
				enum_column: None,
				composite: None,
				declared_sql: None,
				declared_type: crate::SqlType::Unknown,
				aggregate_output: false,
				key_read: crate::ir::KeyRead::None,
				also_in: Vec::new(),
				carried_as: None,
			}))
		} else if let BoundExpr::FunctionCall {
			name,
			..
		} = &target.expr
		{
			Ok(BoundExpr::Column(BoundColumnRef {
				relation_index: 0,
				relation_name: String::new(),
				column: name.clone(),
				correlation_depth: 0,
				id_table: None,
				link_table: None,
				enum_column: None,
				composite: None,
				declared_sql: None,
				declared_type: crate::SqlType::Unknown,
				aggregate_output: false,
				key_read: crate::ir::KeyRead::None,
				also_in: Vec::new(),
				carried_as: None,
			}))
		} else {
			Ok(target.expr.clone())
		}
	}

	/// Look up a target expression by its alias.
	pub fn find_target_by_alias(&self, alias: &str, policy: &DialectPolicy) -> Option<BoundExpr> {
		for target in &self.targets {
			if let Some(a) = &target.alias
				&& policy.idents_match(a, alias)
			{
				return Some(BoundExpr::Column(BoundColumnRef {
					relation_index: 0,
					relation_name: String::new(),
					column: a.clone(),
					correlation_depth: 0,
					id_table: None,
					link_table: None,
					enum_column: None,
					composite: None,
					declared_sql: None,
					declared_type: crate::SqlType::Unknown,
					aggregate_output: false,
					key_read: crate::ir::KeyRead::None,
					also_in: Vec::new(),
					carried_as: None,
				}));
			}
		}
		None
	}

	/// Expand `*` to all columns or a wildcard target.
	pub fn expand_wildcard(&self, catalog: &dyn BinderCatalog) -> Vec<BoundTarget> {
		let mut targets = Vec::new();
		let mut has_known = false;

		for rel in &self.relations {
			if let Some(cols) = &rel.known_columns {
				has_known = true;
				for col in cols {
					targets.push(if rel.closed {
						rel.declared_target(col, catalog)
					} else {
						BoundTarget {
							expr: BoundExpr::Column(rel.column_ref(col.clone(), catalog)),
							alias: Some(col.clone()),
							// A system view has no stored record to read whole: each of its
							// columns is selected by name.
							is_wildcard: rel.source_query.is_none() && rel.is_base_table,
							table_wildcard: None,
						}
					});
				}
			}
		}

		// The columns of a join that has a table declaring its columns stay the one wildcard: the
		// tables may name a column alike, which a flat list cannot keep apart.
		let joins_declared =
			self.relations.len() > 1 && self.relations.iter().any(|rel| rel.closed);

		if has_known && !targets.is_empty() && !joins_declared {
			targets
		} else {
			// Schemaless or unknown catalog: retain single wildcard
			vec![BoundTarget {
				expr: BoundExpr::Column(BoundColumnRef {
					relation_index: 0,
					relation_name: String::new(),
					column: "*".to_string(),
					correlation_depth: 0,
					id_table: None,
					link_table: None,
					enum_column: None,
					composite: None,
					declared_sql: None,
					declared_type: crate::SqlType::Unknown,
					aggregate_output: false,
					key_read: crate::ir::KeyRead::None,
					also_in: Vec::new(),
					carried_as: None,
				}),
				alias: None,
				is_wildcard: true,
				table_wildcard: None,
			}]
		}
	}

	/// Expand `tbl.*` for a specific relation.
	pub fn expand_table_wildcard(
		&self,
		table_prefix: &str,
		policy: &DialectPolicy,
		catalog: &dyn BinderCatalog,
	) -> Result<Vec<BoundTarget>, PlanError> {
		let key = policy.fold_identifier(table_prefix).into_owned();
		let rel_idx = self
			.name_to_relation
			.get(&key)
			.copied()
			.ok_or_else(|| PlanError::MissingFromClauseEntry(table_prefix.to_string()))?;

		let rel = &self.relations[rel_idx];
		if let Some(cols) = &rel.known_columns {
			let mut targets = Vec::new();
			for col in cols {
				targets.push(if rel.closed {
					rel.declared_target(col, catalog)
				} else {
					BoundTarget {
						expr: BoundExpr::Column(rel.column_ref(col.clone(), catalog)),
						alias: Some(col.clone()),
						is_wildcard: false,
						table_wildcard: Some(table_prefix.to_string()),
					}
				});
			}
			Ok(targets)
		} else if self.relations.len() == 1 {
			// The one relation of the query, whose columns are not known: its `*` is the `*` of
			// the query.
			Ok(vec![BoundTarget {
				expr: BoundExpr::Column(BoundColumnRef {
					relation_index: rel.relation_index,
					relation_name: rel.exposed_name.clone(),
					column: "*".to_string(),
					correlation_depth: 0,
					id_table: None,
					link_table: None,
					enum_column: None,
					composite: None,
					declared_sql: None,
					declared_type: crate::SqlType::Unknown,
					aggregate_output: false,
					key_read: crate::ir::KeyRead::None,
					also_in: Vec::new(),
					carried_as: None,
				}),
				alias: None,
				is_wildcard: true,
				table_wildcard: Some(table_prefix.to_string()),
			}])
		} else {
			// The rows of a join keep the fields of the relations after the first under internal
			// names, so the fields of one relation cannot be selected by a wildcard.
			Err(PlanError::unsupported(format!(
				"{table_prefix}.* of a table whose columns are not known, in a query of several tables"
			)))
		}
	}
}
