//! Scopes and name resolution for SQL binding.

use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog::BinderCatalog;
use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::ir::{
	BoundColumnRef, BoundCte, BoundExpr, BoundSelect, BoundTableSource, BoundTarget,
	BoundWindowSpec,
};

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
	/// Whether this relation reads a base table, whose `id` column is the stored record id and
	/// is read as its key. False for subquery aliases and CTEs, whose `id` is a plain value.
	pub is_base_table: bool,
	/// Bound projection supplying scalar and record-link provenance for a derived relation.
	pub source_query: Option<Arc<BoundSelect>>,
}

impl RelationEntry {
	/// A reference to column `column` (already folded) of this relation, bound at
	/// `correlation_depth` 0.
	///
	/// For a base-table relation the catalog is asked which table the column holds record links to.
	/// `id` is asked about too, but never holds links.
	fn column_ref(&self, column: String, catalog: &dyn BinderCatalog) -> BoundColumnRef {
		let id_table = (self.is_base_table && column == "id").then(|| self.table_name.clone());
		let derived =
			self.source_query.as_ref().map(|query| projected_column(query, &column, catalog));
		let link_table = if self.is_base_table && column != "*" {
			catalog
				.link_table(self.schema.as_deref(), &self.table_name, &column)
				.filter(|_| column != "id")
		} else {
			derived.as_ref().and_then(|(_, link)| link.clone())
		};
		BoundColumnRef {
			relation_index: self.relation_index,
			relation_name: self.exposed_name.clone(),
			declared_type: if self.is_base_table {
				catalog.column_type(self.schema.as_deref(), &self.table_name, &column)
			} else {
				derived.map(|(ty, _)| ty).unwrap_or(crate::SqlType::Unknown)
			},
			column,
			correlation_depth: 0,
			id_table,
			link_table,
			aggregate_output: false,
		}
	}
}

/// The scalar type and stored-link target of a projected column. Wildcards inherit only from
/// a single source; joins with unlisted columns do not supply unambiguous provenance.
fn projected_column(
	query: &BoundSelect,
	column: &str,
	catalog: &dyn BinderCatalog,
) -> (crate::SqlType, Option<String>) {
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
				BoundExpr::Column(col) => (col.declared_type, col.link_table.clone()),
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
				} => (query.infer_expr_type(&target.expr, catalog), None),
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
				return (catalog.column_type(schema.as_deref(), name, column), link);
			}
			Some(BoundTableSource::Subquery {
				query,
				..
			}) => return projected_column(query, column, catalog),
			_ => {}
		}
	}
	(crate::SqlType::Unknown, None)
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
		}
	}

	/// Create a child scope with a parent (for subqueries).
	pub fn with_parent(parent: Arc<QueryScope>) -> Self {
		Self {
			parent: Some(parent),
			relations: Vec::new(),
			name_to_relation: HashMap::new(),
			targets: Vec::new(),
			ctes: HashMap::new(),
			windows: HashMap::new(),
		}
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
		let exposed_name = alias.unwrap_or_else(|| table_name.to_string());

		// Query catalog for known columns if available
		let known_columns =
			catalog.get_table(schema.as_deref(), table_name).map(|t| t.column_names());

		let entry = RelationEntry {
			relation_index,
			table_name: table_name.to_string(),
			schema: schema.clone(),
			exposed_name: exposed_name.clone(),
			known_columns,
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

		// Also register under table_name if exposed_name was an alias
		let key_table = policy.fold_identifier(table_name).into_owned();
		self.name_to_relation.entry(key_table).or_insert(relation_index);

		// If schema was provided, also register under "schema.table"
		if let Some(s) = schema {
			let full_name = format!("{s}.{table_name}");
			let key_full = policy.fold_identifier(&full_name).into_owned();
			self.name_to_relation.entry(key_full).or_insert(relation_index);
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
	pub fn resolve_column(
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
				if let Ok(col) =
					self.resolve_qualified_column(&full_prefix, col_name, policy, catalog)
				{
					return Ok(col);
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

	fn resolve_unqualified_column(
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
			return Ok(rel.column_ref(policy.fold_identifier(col_name).into_owned(), catalog));
		}

		// Multiple relations in scope: check known columns across relations
		let mut matching_relations = Vec::new();
		for rel in &self.relations {
			if rel
				.known_columns
				.as_ref()
				.is_some_and(|cols| cols.iter().any(|c| policy.idents_match(c, col_name)))
			{
				matching_relations.push(rel);
			}
		}

		if matching_relations.len() == 1 {
			let rel = matching_relations[0];
			return Ok(rel.column_ref(policy.fold_identifier(col_name).into_owned(), catalog));
		} else if matching_relations.len() > 1 {
			return Err(PlanError::AmbiguousColumn(col_name.to_string()));
		}

		// If relations have known columns and col was not found, check parent before fallback
		if self.relations.iter().any(|r| r.known_columns.is_some())
			&& let Some(parent) = &self.parent
			&& let Ok(mut col) = parent.resolve_unqualified_column(col_name, policy, catalog)
		{
			col.correlation_depth += 1;
			return Ok(col);
		}

		// Fallback when schema is not strictly known (schemaless tables): default to relation 0
		let rel = &self.relations[0];
		Ok(rel.column_ref(policy.fold_identifier(col_name).into_owned(), catalog))
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
				declared_type: crate::SqlType::Unknown,
				aggregate_output: false,
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
				declared_type: crate::SqlType::Unknown,
				aggregate_output: false,
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
					declared_type: crate::SqlType::Unknown,
					aggregate_output: false,
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
					targets.push(BoundTarget {
						expr: BoundExpr::Column(rel.column_ref(col.clone(), catalog)),
						alias: Some(col.clone()),
						is_wildcard: rel.source_query.is_none(),
						table_wildcard: None,
					});
				}
			}
		}

		if has_known && !targets.is_empty() {
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
					declared_type: crate::SqlType::Unknown,
					aggregate_output: false,
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
				targets.push(BoundTarget {
					expr: BoundExpr::Column(rel.column_ref(col.clone(), catalog)),
					alias: Some(col.clone()),
					is_wildcard: false,
					table_wildcard: Some(table_prefix.to_string()),
				});
			}
			Ok(targets)
		} else {
			// Schemaless relation wildcard
			Ok(vec![BoundTarget {
				expr: BoundExpr::Column(BoundColumnRef {
					relation_index: rel.relation_index,
					relation_name: rel.exposed_name.clone(),
					column: "*".to_string(),
					correlation_depth: 0,
					id_table: None,
					link_table: None,
					declared_type: crate::SqlType::Unknown,
					aggregate_output: false,
				}),
				alias: None,
				is_wildcard: false,
				table_wildcard: Some(table_prefix.to_string()),
			}])
		}
	}
}
