//! The columns that earlier statements of one request declare.
//!
//! The statements of a request are lowered together, before any of them runs, so a table that an
//! earlier statement of the request creates is in no catalog yet. A migration is sent that way
//! (`CREATE TABLE` followed by `ALTER TABLE ... ADD FOREIGN KEY` over one of its columns), and the
//! alteration needs the declaration of the column it changes. [`StagedCatalog`] answers for the
//! columns the statements lowered so far declare, and for every other column asks the catalog it
//! wraps.
//!
//! The tables the staged statements create or remove are applied to the tables the catalog lists
//! ([`BinderCatalog::all_tables`], [`BinderCatalog::constraint_tables`]), so a statement that
//! follows `CREATE TABLE` or `DROP TABLE` in one request lists the tables as they then stand. A
//! created table lists the columns the staged statements declare, with no type name.
//!
//! A definition that does nothing when its object exists (`IF NOT EXISTS`) is tentative: the
//! catalog it wraps answers first, and what the statement declares answers only for a column that
//! catalog does not describe.

use std::collections::{HashMap, HashSet};

use surrealdb_expr::expr::reference::ReferenceDeleteStrategy;
use surrealdb_expr::expr::statements::AlterStatement;
use surrealdb_expr::expr::statements::alter::{AlterDefault, AlterFieldStatement, AlterKind};
use surrealdb_expr::expr::statements::define::{DefineDefault, DefineKind, DefineStatement};
use surrealdb_expr::expr::statements::remove::RemoveStatement;
use surrealdb_expr::expr::{Expr, Function, Literal};

use super::{
	BinderCatalog, ColumnAssert, ColumnInfo, ColumnShape, ForeignKeyInfo, IndexInfo,
	ReferentialAction, TableSchema, kind_admits_none, kind_type_name, link_target,
};

/// A catalog that answers for the columns of the statements staged on it, and otherwise for the
/// catalog it wraps.
pub struct StagedCatalog<'a> {
	inner: &'a dyn BinderCatalog,
	/// The shapes of the staged columns, by (table, column).
	shapes: HashMap<(String, String), ColumnShape>,
	/// The staged columns whose definition applies only if the column does not exist.
	tentative: HashSet<(String, String)>,
	/// Tables a staged statement creates or removes: the wrapped catalog describes none of their
	/// columns.
	own: HashSet<String>,
	/// The tables a staged statement creates, which the table listings include.
	created: HashSet<String>,
	/// The foreign keys the staged statements define, by table.
	keys: HashMap<String, Vec<ForeignKeyInfo>>,
	/// The ordered and unique indexes the staged statements define, by table.
	indexes: HashMap<String, Vec<IndexInfo>>,
	/// The sequence that numbers a staged column, by (table, column).
	sequences: HashMap<(String, String), String>,
	/// The columns whose foreign key and sequence a staged statement defines or removes, which
	/// replace what the wrapped catalog lists for them.
	touched: HashSet<(String, String)>,
	/// The indexes, by (table, name), that a staged statement defines or removes.
	replaced_indexes: HashSet<(String, String)>,
}

impl<'a> StagedCatalog<'a> {
	pub fn new(inner: &'a dyn BinderCatalog) -> Self {
		Self {
			inner,
			shapes: HashMap::new(),
			tentative: HashSet::new(),
			own: HashSet::new(),
			created: HashSet::new(),
			keys: HashMap::new(),
			indexes: HashMap::new(),
			sequences: HashMap::new(),
			touched: HashSet::new(),
			replaced_indexes: HashSet::new(),
		}
	}

	/// Record what the lowered statements `exprs` declare, for the statements lowered after them.
	pub fn stage(&mut self, exprs: &[Expr]) {
		for expr in exprs {
			if let Expr::Define(define) = expr {
				self.stage_define(define);
			} else if let Expr::Alter(alter) = expr
				&& let AlterStatement::Field(field) = alter.as_ref()
			{
				self.stage_alter(field);
			} else if let Expr::Remove(remove) = expr {
				self.stage_remove(remove);
			} else if let Expr::Block(block) = expr {
				self.stage(&block.0);
			}
		}
	}

	fn stage_define(&mut self, define: &DefineStatement) {
		if let DefineStatement::Table(table) = define {
			if let Expr::Table(name) = &table.name
				&& (table.kind != DefineKind::IfNotExists || self.is_absent(name.as_str()))
			{
				self.forget(name.as_str());
				self.created.insert(name.as_str().to_owned());
			}
		} else if let DefineStatement::Field(field) = define
			&& let (Some(table), Some(column), Some(kind)) =
				(table_name(&field.what), column_name(&field.name), &field.field_kind)
		{
			let shape = ColumnShape::new(
				Some(kind.clone()),
				field.assert.as_ref(),
				!matches!(field.default, DefineDefault::None),
			);
			let key = (table.to_owned(), column.to_owned());
			if field.kind == DefineKind::IfNotExists {
				self.tentative.insert(key.clone());
			} else {
				self.tentative.remove(&key);
			}
			self.shapes.insert(key, shape);
			// A definition that does nothing when the column exists leaves the foreign key and
			// the sequence of the column as they are.
			if field.kind != DefineKind::IfNotExists {
				self.stage_column_metadata(table, column, field);
			}
		} else if let DefineStatement::Index(index) = define {
			self.stage_index(index);
		}
	}

	/// Note the foreign key and the sequence that the definition of a column carries.
	fn stage_column_metadata(
		&mut self,
		table: &str,
		column: &str,
		field: &surrealdb_expr::expr::statements::define::DefineFieldStatement,
	) {
		self.touched.insert((table.to_owned(), column.to_owned()));
		let keys = self.keys.entry(table.to_owned()).or_default();
		keys.retain(|key| key.column != column);
		if let (Some(reference), Some(target)) =
			(&field.reference, field.field_kind.as_ref().and_then(link_target))
		{
			keys.push(ForeignKeyInfo {
				column: column.to_owned(),
				target_table: target.to_owned(),
				on_delete: match reference.on_delete {
					ReferenceDeleteStrategy::Cascade => ReferentialAction::Cascade,
					ReferenceDeleteStrategy::Unset => ReferentialAction::SetNull,
					ReferenceDeleteStrategy::Reject
					| ReferenceDeleteStrategy::Ignore
					| ReferenceDeleteStrategy::Custom(_) => ReferentialAction::NoAction,
				},
			});
		}
		let key = (table.to_owned(), column.to_owned());
		match &field.default {
			DefineDefault::Set(Expr::FunctionCall(call)) => {
				match (&call.receiver, call.arguments.as_slice()) {
					(Function::Normal(name), [Expr::Literal(Literal::String(sequence))])
						if name == "sequence::nextval" =>
					{
						self.sequences.insert(key, sequence.to_string());
					}
					_ => {
						self.sequences.remove(&key);
					}
				}
			}
			DefineDefault::Set(_) | DefineDefault::None | DefineDefault::Always(_) => {
				self.sequences.remove(&key);
			}
		}
	}

	/// Note an ordered or unique index over plain columns.
	fn stage_index(
		&mut self,
		index: &surrealdb_expr::expr::statements::define::DefineIndexStatement,
	) {
		use surrealdb_expr::expr::index_kind::Index;
		let (Some(table), Some(name)) = (table_name(&index.what), column_name(&index.name)) else {
			return;
		};
		// A definition that does nothing when the index exists leaves an existing one as it is.
		if index.kind == DefineKind::IfNotExists
			&& (self
				.indexes
				.get(table)
				.is_some_and(|indexes| indexes.iter().any(|i| i.name == name))
				|| self.inner.all_tables().iter().any(|listed| {
					listed.name == table && listed.indexes.iter().any(|i| i.name == name)
				})) {
			return;
		}
		let unique = match &index.index {
			Index::Idx => false,
			Index::Uniq {
				..
			} => true,
			Index::Hnsw(_)
			| Index::FullText(_)
			| Index::Count(_)
			| Index::DiskAnn(_)
			| Index::Spatial => return,
		};
		let columns: Option<Vec<String>> =
			index.cols.iter().map(|col| column_name(col).map(str::to_owned)).collect();
		let Some(columns) = columns else {
			return;
		};
		self.replaced_indexes.insert((table.to_owned(), name.to_owned()));
		let indexes = self.indexes.entry(table.to_owned()).or_default();
		indexes.retain(|existing| existing.name != name);
		indexes.push(IndexInfo {
			name: name.to_owned(),
			columns,
			unique,
		});
	}

	/// Note the foreign key an alteration gives a column or takes from it.
	fn stage_reference(&mut self, table: &str, column: &str, field: &AlterFieldStatement) {
		match &field.reference {
			AlterKind::Set(reference) => {
				let from_kind = match &field.kind {
					AlterKind::Set(kind) => link_target(kind).map(str::to_owned),
					AlterKind::Drop | AlterKind::None => None,
				};
				let target = from_kind.or_else(|| self.link_table(None, table, column));
				self.touched.insert((table.to_owned(), column.to_owned()));
				let keys = self.keys.entry(table.to_owned()).or_default();
				keys.retain(|existing| existing.column != column);
				if let Some(target) = target {
					keys.push(ForeignKeyInfo {
						column: column.to_owned(),
						target_table: target,
						on_delete: match reference.on_delete {
							ReferenceDeleteStrategy::Cascade => ReferentialAction::Cascade,
							ReferenceDeleteStrategy::Unset => ReferentialAction::SetNull,
							ReferenceDeleteStrategy::Reject
							| ReferenceDeleteStrategy::Ignore
							| ReferenceDeleteStrategy::Custom(_) => ReferentialAction::NoAction,
						},
					});
				}
			}
			AlterKind::Drop => {
				self.touched.insert((table.to_owned(), column.to_owned()));
				if let Some(keys) = self.keys.get_mut(table) {
					keys.retain(|existing| existing.column != column);
				}
			}
			AlterKind::None => {}
		}
	}

	fn stage_alter(&mut self, field: &AlterFieldStatement) {
		let (Some(table), Some(column)) = (table_name(&field.what), column_name(&field.name))
		else {
			return;
		};
		let key = (table.to_owned(), column.to_owned());
		self.stage_reference(table, column, field);
		// A column the catalog describes changes from the shape it has there, which an alteration
		// of the assertion or default alone leaves in part as it is.
		if !self.shapes.contains_key(&key) && !self.own.contains(table) {
			let Some(shape) = self.inner.column_shape(None, table, column) else {
				return;
			};
			self.shapes.insert(key.clone(), shape);
		}
		let Some(shape) = self.shapes.get_mut(&key) else {
			return;
		};
		if let AlterKind::Set(kind) = &field.kind {
			shape.kind = Some(kind.clone());
		}
		match &field.assert {
			AlterKind::Set(assert) => {
				shape.assert = ColumnShape::new(None, Some(assert), false).assert;
			}
			AlterKind::Drop => shape.assert = ColumnAssert::None,
			AlterKind::None => {}
		}
		match &field.default {
			AlterDefault::Set(_) | AlterDefault::Always(_) => shape.has_default = true,
			AlterDefault::Drop => shape.has_default = false,
			AlterDefault::None => {}
		}
	}

	fn stage_remove(&mut self, remove: &RemoveStatement) {
		if let RemoveStatement::Table(table) = remove {
			if let Expr::Table(name) = &table.name {
				self.forget(name.as_str());
				self.created.remove(name.as_str());
			}
		} else if let RemoveStatement::Field(field) = remove
			&& let (Some(table), Some(column)) =
				(table_name(&field.table_name), column_name(&field.name))
		{
			self.shapes.remove(&(table.to_owned(), column.to_owned()));
			self.touched.insert((table.to_owned(), column.to_owned()));
			if let Some(keys) = self.keys.get_mut(table) {
				keys.retain(|key| key.column != column);
			}
			self.sequences.remove(&(table.to_owned(), column.to_owned()));
		} else if let RemoveStatement::Index(index) = remove
			&& let (Some(table), Some(name)) = (table_name(&index.what), column_name(&index.name))
		{
			self.replaced_indexes.insert((table.to_owned(), name.to_owned()));
			if let Some(indexes) = self.indexes.get_mut(table) {
				indexes.retain(|existing| existing.name != name);
			}
		}
	}

	/// Whether the wrapped catalog, which lists tables, proves that it has no table `name`, and no
	/// staged statement has created it: a catalog that lists nothing proves nothing.
	fn is_absent(&self, name: &str) -> bool {
		if self.created.contains(name) {
			return false;
		}
		let listed = self.inner.all_tables();
		!listed.is_empty() && !listed.iter().any(|table| table.name == name)
	}

	fn is_tentative(&self, table: &str, column: &str) -> bool {
		self.tentative.contains(&(table.to_owned(), column.to_owned()))
	}

	/// Drop what is known of the columns of `table`, which a staged statement creates anew or
	/// removes.
	fn forget(&mut self, table: &str) {
		self.shapes.retain(|(name, _), _| name != table);
		self.tentative.retain(|(name, _)| name != table);
		self.keys.remove(table);
		self.indexes.remove(table);
		self.sequences.retain(|(name, _), _| name != table);
		self.own.insert(table.to_owned());
	}
}

impl StagedCatalog<'_> {
	/// `listed`, the tables the catalog lists, with the staged creations and removals applied. A
	/// catalog that lists no table (one that is not allowed to, or that only records) stays empty.
	fn listed(&self, mut listed: Vec<TableSchema>) -> Vec<TableSchema> {
		if listed.is_empty() {
			return listed;
		}
		listed.retain(|table| !self.own.contains(&table.name));
		// A table the wrapped catalog lists takes what the staged statements changed on it.
		for table in &mut listed {
			let name = &table.name;
			table
				.foreign_keys
				.retain(|key| !self.touched.contains(&(name.clone(), key.column.clone())));
			table.foreign_keys.extend(self.keys.get(name).into_iter().flatten().cloned());
			table.indexes.retain(|index| {
				!self.replaced_indexes.contains(&(name.clone(), index.name.clone()))
			});
			table.indexes.extend(self.indexes.get(name).into_iter().flatten().cloned());
			// A column a staged statement removed is gone; one it declares replaces or joins
			// what the wrapped catalog lists.
			table.columns.retain(|column| {
				let key = (name.clone(), column.name.clone());
				!self.touched.contains(&key) || self.shapes.contains_key(&key)
			});
			for column in &mut table.columns {
				let key = (name.clone(), column.name.clone());
				if self.touched.contains(&key) {
					column.sequence = self.sequences.get(&key).cloned();
				}
				if let Some(shape) = self.shapes.get(&key)
					&& !self.tentative.contains(&key)
				{
					column.is_nullable = shape.assert != ColumnAssert::NotNull
						&& shape.kind.as_ref().is_none_or(kind_admits_none);
					if let Some(kind) = &shape.kind {
						column.data_type = kind_type_name(kind);
					}
				}
			}
			let mut added: Vec<(&String, &ColumnShape)> = self
				.shapes
				.iter()
				.filter(|((shape_table, column), _)| {
					shape_table == name && !table.columns.iter().any(|c| &c.name == column)
				})
				.map(|((_, column), shape)| (column, shape))
				.collect();
			added.sort_by_key(|(column, _)| *column);
			for (column, shape) in added {
				let mut info = ColumnInfo::new(column.as_str(), 0);
				info.is_nullable = shape.assert != ColumnAssert::NotNull
					&& shape.kind.as_ref().is_none_or(kind_admits_none);
				info.sequence = self.sequences.get(&(name.clone(), column.clone())).cloned();
				info.data_type = shape.kind.as_ref().and_then(kind_type_name);
				table.columns.push(info);
			}
			for (ordinal, column) in table.columns.iter_mut().enumerate() {
				column.ordinal = ordinal;
			}
		}
		let mut created: Vec<&String> = self.created.iter().collect();
		created.sort();
		for name in created {
			let mut columns: Vec<(&str, &ColumnShape)> = self
				.shapes
				.iter()
				.filter(|((table, _), _)| table == name)
				.map(|((_, column), shape)| (column.as_str(), shape))
				.filter(|(column, _)| *column != "id")
				.collect();
			columns.sort_by_key(|(column, _)| *column);
			let sequence = |column: &str| self.sequences.get(&(name.clone(), column.to_owned()));
			let mut id = ColumnInfo::new("id", 0);
			id.sequence = sequence("id").cloned();
			let mut infos = vec![id];
			for (ordinal, (column, shape)) in columns.into_iter().enumerate() {
				let mut info = ColumnInfo::new(column, ordinal + 1);
				info.is_nullable = shape.assert != ColumnAssert::NotNull
					&& shape.kind.as_ref().is_none_or(kind_admits_none);
				info.sequence = sequence(column).cloned();
				info.data_type = shape.kind.as_ref().and_then(kind_type_name);
				infos.push(info);
			}
			listed.push(
				TableSchema::new(name.as_str(), infos)
					.with_schema("public")
					.with_indexes(self.indexes.get(name).cloned().unwrap_or_default())
					.with_foreign_keys(self.keys.get(name).cloned().unwrap_or_default()),
			);
		}
		listed
	}
}

fn table_name(expr: &Expr) -> Option<&str> {
	if let Expr::Table(name) = expr {
		Some(name.as_str())
	} else {
		None
	}
}

fn column_name(expr: &Expr) -> Option<&str> {
	if let Expr::Idiom(idiom) = expr {
		idiom.field_name()
	} else {
		None
	}
}

impl BinderCatalog for StagedCatalog<'_> {
	fn get_table(&self, schema: Option<&str>, name: &str) -> Option<TableSchema> {
		self.inner.get_table(schema, name)
	}

	fn table_exists(&self, schema: Option<&str>, name: &str) -> bool {
		self.inner.table_exists(schema, name)
	}

	fn current_database(&self) -> Option<&str> {
		self.inner.current_database()
	}

	fn all_tables(&self) -> Vec<TableSchema> {
		self.listed(self.inner.all_tables())
	}

	fn constraint_tables(&self) -> Vec<TableSchema> {
		self.listed(self.inner.constraint_tables())
	}

	fn column_type(&self, schema: Option<&str>, table: &str, column: &str) -> crate::SqlType {
		self.inner.column_type(schema, table, column)
	}

	fn defers_schema_checks(&self) -> bool {
		self.inner.defers_schema_checks()
	}

	fn vector_dimension(&self, schema: Option<&str>, table: &str, column: &str) -> Option<usize> {
		self.inner.vector_dimension(schema, table, column)
	}

	fn write_type(&self, schema: Option<&str>, table: &str, column: &str) -> crate::SqlType {
		self.inner.write_type(schema, table, column)
	}

	fn hnsw_indexes(
		&self,
		schema: Option<&str>,
		table: &str,
	) -> Vec<(String, crate::vector::VectorMetric)> {
		self.inner.hnsw_indexes(schema, table)
	}

	fn link_table(&self, schema: Option<&str>, table: &str, column: &str) -> Option<String> {
		if self.is_tentative(table, column)
			&& let Some(linked) = self.inner.link_table(schema, table, column)
		{
			return Some(linked);
		}
		match self.column_shape(schema, table, column) {
			Some(shape) => shape.kind.as_ref().and_then(link_target).map(str::to_owned),
			None if self.own.contains(table) => None,
			None => self.inner.link_table(schema, table, column),
		}
	}

	fn link_admits_none(&self, schema: Option<&str>, table: &str, column: &str) -> bool {
		if self.is_tentative(table, column)
			&& self.inner.link_table(schema, table, column).is_some()
		{
			return self.inner.link_admits_none(schema, table, column);
		}
		match self.column_shape(schema, table, column) {
			Some(shape) => shape.kind.as_ref().is_some_and(kind_admits_none),
			None if self.own.contains(table) => false,
			None => self.inner.link_admits_none(schema, table, column),
		}
	}

	fn column_shape(&self, schema: Option<&str>, table: &str, column: &str) -> Option<ColumnShape> {
		if schema.is_some_and(|schema| schema != "public") {
			return self.inner.column_shape(schema, table, column);
		}
		if self.is_tentative(table, column)
			&& let Some(shape) = self.inner.column_shape(schema, table, column)
		{
			return Some(shape);
		}
		match self.shapes.get(&(table.to_owned(), column.to_owned())) {
			Some(shape) => Some(shape.clone()),
			None if self.own.contains(table) => None,
			None => self.inner.column_shape(schema, table, column),
		}
	}
}
