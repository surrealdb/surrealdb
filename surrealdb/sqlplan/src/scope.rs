//! Scopes and name resolution for SQL binding.

use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog::BinderCatalog;
use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::ir::{BoundColumnRef, BoundExpr, BoundTarget};

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
}

impl QueryScope {
	/// Create an empty root scope.
	pub fn new() -> Self {
		Self {
			parent: None,
			relations: Vec::new(),
			name_to_relation: HashMap::new(),
			targets: Vec::new(),
		}
	}

	/// Create a child scope with a parent (for subqueries).
	pub fn with_parent(parent: Arc<QueryScope>) -> Self {
		Self {
			parent: Some(parent),
			relations: Vec::new(),
			name_to_relation: HashMap::new(),
			targets: Vec::new(),
		}
	}

	/// Register a relation in the scope.
	pub fn register_relation(
		&mut self,
		schema: Option<String>,
		table_name: String,
		alias: Option<String>,
		catalog: &dyn BinderCatalog,
		policy: &DialectPolicy,
	) -> usize {
		let relation_index = self.relations.len();
		let exposed_name = alias.unwrap_or_else(|| table_name.clone());

		// Query catalog for known columns if available
		let known_columns =
			catalog.get_table(schema.as_deref(), &table_name).map(|t| t.column_names());

		let entry = RelationEntry {
			relation_index,
			table_name: table_name.clone(),
			schema: schema.clone(),
			exposed_name: exposed_name.clone(),
			known_columns,
		};

		self.relations.push(entry);

		// Register in name lookup under folded identifier
		let key_exposed = policy.fold_identifier(&exposed_name).into_owned();
		self.name_to_relation.insert(key_exposed, relation_index);

		// Also register under table_name if exposed_name was an alias
		let key_table = policy.fold_identifier(&table_name).into_owned();
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
	pub fn register_subquery(&mut self, alias: String, policy: &DialectPolicy) -> usize {
		let relation_index = self.relations.len();
		let entry = RelationEntry {
			relation_index,
			table_name: alias.clone(),
			schema: None,
			exposed_name: alias.clone(),
			known_columns: None,
		};

		self.relations.push(entry);
		let key = policy.fold_identifier(&alias).into_owned();
		self.name_to_relation.insert(key, relation_index);
		relation_index
	}

	/// Resolve a column reference given identifier parts (e.g. `["col"]` or `["tbl", "col"]` or
	/// `["schema", "tbl", "col"]`).
	pub fn resolve_column(
		&self,
		parts: &[String],
		policy: &DialectPolicy,
	) -> Result<BoundColumnRef, PlanError> {
		match parts.len() {
			0 => Err(PlanError::Custom("empty column reference".into())),
			1 => {
				// Unqualified column name: e.g. `id`
				let col_name = parts[0].clone();
				self.resolve_unqualified_column(&col_name, policy)
			}
			2 => {
				// Qualified column name: e.g. `tbl.col` or `alias.col`
				let prefix = &parts[0];
				let col_name = &parts[1];
				self.resolve_qualified_column(prefix, col_name, policy)
			}
			3 => {
				// Schema-qualified column name: e.g. `public.users.id`
				let full_prefix = format!("{}.{}", parts[0], parts[1]);
				let col_name = &parts[2];
				if let Ok(col) = self.resolve_qualified_column(&full_prefix, col_name, policy) {
					return Ok(col);
				}
				// If not matching a schema-qualified table, try prefix as table and parts[1..] as
				// nested field
				self.resolve_qualified_column(&parts[0], &parts[1], policy)
			}
			_ => {
				// Deeper dotted access: try parts[0] as relation, parts[1] as column
				self.resolve_qualified_column(&parts[0], &parts[1], policy)
			}
		}
	}

	fn resolve_unqualified_column(
		&self,
		col_name: &str,
		policy: &DialectPolicy,
	) -> Result<BoundColumnRef, PlanError> {
		if self.relations.is_empty() {
			// In a query without FROM clause, unqualified columns do not exist
			return Err(PlanError::UndefinedColumn(col_name.to_string()));
		}

		// If exactly 1 relation in scope, bind directly to relation 0
		if self.relations.len() == 1 {
			let rel = &self.relations[0];
			let folded_col = policy.fold_identifier(col_name).into_owned();
			return Ok(BoundColumnRef {
				relation_index: rel.relation_index,
				relation_name: rel.exposed_name.clone(),
				column: folded_col,
			});
		}

		// Multiple relations in scope: check known columns across relations
		let mut matching_relations = Vec::new();
		for rel in &self.relations {
			if let Some(cols) = &rel.known_columns {
				if cols.iter().any(|c| policy.idents_match(c, col_name)) {
					matching_relations.push(rel);
				}
			}
		}

		if matching_relations.len() == 1 {
			let rel = matching_relations[0];
			let folded_col = policy.fold_identifier(col_name).into_owned();
			return Ok(BoundColumnRef {
				relation_index: rel.relation_index,
				relation_name: rel.exposed_name.clone(),
				column: folded_col,
			});
		} else if matching_relations.len() > 1 {
			return Err(PlanError::AmbiguousColumn(col_name.to_string()));
		}

		// Fallback when schema is not strictly known (schemaless tables): default to relation 0
		let rel = &self.relations[0];
		let folded_col = policy.fold_identifier(col_name).into_owned();
		Ok(BoundColumnRef {
			relation_index: rel.relation_index,
			relation_name: rel.exposed_name.clone(),
			column: folded_col,
		})
	}

	fn resolve_qualified_column(
		&self,
		prefix: &str,
		col_name: &str,
		policy: &DialectPolicy,
	) -> Result<BoundColumnRef, PlanError> {
		let key = policy.fold_identifier(prefix).into_owned();
		if let Some(&rel_idx) = self.name_to_relation.get(&key) {
			let rel = &self.relations[rel_idx];
			let folded_col = policy.fold_identifier(col_name).into_owned();
			return Ok(BoundColumnRef {
				relation_index: rel.relation_index,
				relation_name: rel.exposed_name.clone(),
				column: folded_col,
			});
		}

		// Check parent scope for correlated subqueries
		if let Some(parent) = &self.parent {
			if let Ok(col) = parent.resolve_qualified_column(prefix, col_name, policy) {
				return Ok(col);
			}
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
			}))
		} else {
			match &target.expr {
				BoundExpr::FunctionCall {
					name,
					..
				} => Ok(BoundExpr::Column(BoundColumnRef {
					relation_index: 0,
					relation_name: String::new(),
					column: name.clone(),
				})),
				_ => Ok(target.expr.clone()),
			}
		}
	}

	/// Look up a target expression by its alias.
	pub fn find_target_by_alias(&self, alias: &str, policy: &DialectPolicy) -> Option<BoundExpr> {
		for target in &self.targets {
			if let Some(a) = &target.alias {
				if policy.idents_match(a, alias) {
					return Some(BoundExpr::Column(BoundColumnRef {
						relation_index: 0,
						relation_name: String::new(),
						column: a.clone(),
					}));
				}
			}
		}
		None
	}

	/// Expand `*` to all columns or a wildcard target.
	pub fn expand_wildcard(&self) -> Vec<BoundTarget> {
		let mut targets = Vec::new();
		let mut has_known = false;

		for rel in &self.relations {
			if let Some(cols) = &rel.known_columns {
				has_known = true;
				for col in cols {
					targets.push(BoundTarget {
						expr: BoundExpr::Column(BoundColumnRef {
							relation_index: rel.relation_index,
							relation_name: rel.exposed_name.clone(),
							column: col.clone(),
						}),
						alias: Some(col.clone()),
						is_wildcard: true,
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
					expr: BoundExpr::Column(BoundColumnRef {
						relation_index: rel.relation_index,
						relation_name: rel.exposed_name.clone(),
						column: col.clone(),
					}),
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
				}),
				alias: None,
				is_wildcard: false,
				table_wildcard: Some(table_prefix.to_string()),
			}])
		}
	}
}
