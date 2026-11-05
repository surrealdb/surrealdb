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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use surrealdb_expr::expr::reference::ReferenceDeleteStrategy;
use surrealdb_expr::expr::statements::AlterStatement;
use surrealdb_expr::expr::statements::alter::{AlterDefault, AlterFieldStatement, AlterKind};
use surrealdb_expr::expr::statements::define::{DefineDefault, DefineKind, DefineStatement};
use surrealdb_expr::expr::statements::remove::{RemoveStatement, SqlTypeObject};
use surrealdb_expr::expr::{Expr, Function, Literal};

use super::{
	BinderCatalog, ColumnAssert, ColumnInfo, ColumnShape, ForeignKeyInfo, IndexInfo,
	ReferentialAction, TableSchema, kind_admits_none, kind_admits_null, kind_type_name,
	link_target,
};

/// A catalog that answers for the columns of the statements staged on it, and otherwise for the
/// catalog it wraps.
pub struct StagedCatalog<'a> {
	inner: &'a dyn BinderCatalog,
	/// The shapes of the staged columns, by (table, column).
	shapes: HashMap<(String, String), ColumnShape>,
	/// The SQL type each staged column is declared with, by (table, column), for the staged
	/// columns that declare one.
	declared: HashMap<(String, String), crate::declared_type::DeclaredSqlType>,
	/// The staged columns whose definition applies only if the column does not exist.
	tentative: HashSet<(String, String)>,
	/// Tables a staged statement creates or removes: the wrapped catalog describes none of their
	/// columns.
	own: HashSet<String>,
	/// The tables a staged statement creates, which the table listings include.
	created: HashSet<String>,
	/// The tables each staged view reads, by view.
	view_over: HashMap<String, Vec<String>>,
	/// The foreign keys the staged statements define, by table.
	keys: HashMap<String, Vec<ForeignKeyInfo>>,
	/// The ordered and unique indexes the staged statements define, by table.
	indexes: HashMap<String, Vec<IndexInfo>>,
	/// The sequence that numbers a staged column, by (table, column).
	sequences: HashMap<(String, String), String>,
	/// The columns whose foreign key and sequence a staged statement defines or removes, which
	/// replace what the wrapped catalog lists for them.
	touched: HashSet<(String, String)>,
	/// The columns of tables the wrapped catalog describes that a staged statement alters without
	/// changing their kind or declared SQL type, whose type the wrapped catalog still answers.
	retained_types: HashSet<(String, String)>,
	/// The indexes, by (table, name), that a staged statement defines or removes.
	replaced_indexes: HashSet<(String, String)>,
	/// The columns that hold NULL in a row written without them, for each `SCHEMAFULL` table a
	/// staged statement creates, and the columns a staged statement adds to a table the wrapped
	/// catalog describes; see [`BinderCatalog::null_columns`]. A column that a later statement
	/// alters or removes is dropped from it.
	nulls: HashMap<String, BTreeSet<String>>,
	/// The columns that take a default in a row written without them, with the expression that
	/// gives it, for each `SCHEMAFULL` table a staged statement creates; see
	/// [`BinderCatalog::default_columns`]. Kept as `nulls` is.
	defaults: HashMap<String, BTreeMap<String, Expr>>,
	/// The columns of tables the wrapped catalog describes that a staged statement defines,
	/// alters or removes, whose entry in the wrapped catalog's NULL-holding columns is no longer
	/// known to hold.
	changed: HashSet<(String, String)>,
	/// The enum types as the staged statements leave them, once one of them defines, alters or
	/// removes an enum; until then the wrapped catalog answers.
	enums: Option<Vec<crate::catalog::EnumType>>,
	/// The domains and composite types as the staged statements leave them, once one of them
	/// defines or removes one; until then the wrapped catalog answers.
	user_types: Option<StagedUserTypes>,
}

/// The domains and composite types of a [`StagedCatalog`].
struct StagedUserTypes {
	domains: Vec<crate::catalog::DomainType>,
	composites: Vec<crate::catalog::CompositeType>,
}

/// Whether `default` reads no field of the row it is a default of, and nothing that might.
fn reads_no_field(default: &Expr) -> bool {
	let reads = surrealdb_expr::expr::computed_deps::extract_computed_deps(default);
	reads.is_complete && reads.fields.is_empty()
}

impl<'a> StagedCatalog<'a> {
	pub fn new(inner: &'a dyn BinderCatalog) -> Self {
		Self {
			inner,
			shapes: HashMap::new(),
			declared: HashMap::new(),
			tentative: HashSet::new(),
			own: HashSet::new(),
			created: HashSet::new(),
			view_over: HashMap::new(),
			keys: HashMap::new(),
			indexes: HashMap::new(),
			sequences: HashMap::new(),
			touched: HashSet::new(),
			retained_types: HashSet::new(),
			replaced_indexes: HashSet::new(),
			nulls: HashMap::new(),
			defaults: HashMap::new(),
			changed: HashSet::new(),
			enums: None,
			user_types: None,
		}
	}

	/// The domains and composite types as they stand, copied from the wrapped catalog on first
	/// use.
	fn staged_user_types(&mut self) -> &mut StagedUserTypes {
		let inner = self.inner;
		self.user_types.get_or_insert_with(|| StagedUserTypes {
			domains: inner.domain_types(),
			composites: inner.composite_types(),
		})
	}

	/// Stage the definition of a type. A name already defined keeps its definition and id unless
	/// the statement overwrites it.
	fn stage_type(&mut self, stmt: &surrealdb_expr::expr::statements::define::DefineTypeStatement) {
		let name = stmt.name.as_str().to_owned();
		self.staged_enums();
		let enums = self.enums.take().unwrap_or_default();
		let types = self.staged_user_types();
		let existing = enums
			.iter()
			.map(|e| (e.name.as_str(), e.id))
			.chain(types.domains.iter().map(|d| (d.name.as_str(), d.id)))
			.chain(types.composites.iter().map(|c| (c.name.as_str(), c.id)))
			.find(|(n, _)| *n == name)
			.map(|(_, id)| id);
		let mut enums = enums;
		if existing.is_none() || stmt.kind == DefineKind::Overwrite {
			let id = existing.unwrap_or_else(|| {
				enums
					.iter()
					.map(|e| e.id + 1)
					.chain(types.domains.iter().map(|d| d.id + 1))
					.chain(types.composites.iter().map(|c| c.id + 1))
					.max()
					.unwrap_or(0)
			});
			enums.retain(|e| e.name != name);
			types.domains.retain(|d| d.name != name);
			types.composites.retain(|c| c.name != name);
			match crate::catalog::UserType::of_definition(
				&name,
				id,
				&stmt.body,
				&stmt.asserts,
				stmt.default.as_ref(),
				stmt.sql_type.as_deref(),
			) {
				crate::catalog::UserType::Enum(declared) => enums.push(declared),
				crate::catalog::UserType::Domain(domain) => types.domains.push(domain),
				crate::catalog::UserType::Composite(composite) => types.composites.push(composite),
			}
		}
		self.enums = Some(enums);
	}

	/// Record what the lowered statements `exprs` declare, for the statements lowered after them.
	pub fn stage(&mut self, exprs: &[Expr]) {
		for expr in exprs {
			if let Expr::Define(define) = expr {
				self.stage_define(define);
			} else if let Expr::Alter(alter) = expr
				&& let AlterStatement::Type(stmt) = alter.as_ref()
			{
				self.stage_alter_enum(stmt);
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

	/// The enum types as they stand, copied from the wrapped catalog on first use.
	fn staged_enums(&mut self) -> &mut Vec<crate::catalog::EnumType> {
		self.enums.get_or_insert_with(|| self.inner.enum_types())
	}

	fn stage_alter_enum(
		&mut self,
		stmt: &surrealdb_expr::expr::statements::alter::AlterTypeStatement,
	) {
		use surrealdb_expr::expr::statements::alter::{AlterTypeAction, EnumPosition};
		let name = stmt.name.as_str().to_owned();
		let Some(en) = self.staged_enums().iter_mut().find(|e| e.name == name) else {
			return;
		};
		for action in &stmt.actions {
			match action {
				AlterTypeAction::AddValue {
					value,
					position,
					..
				} => en.add_label(
					value,
					match position {
						Some(EnumPosition::Before(anchor)) => Some((anchor.as_str(), false)),
						Some(EnumPosition::After(anchor)) => Some((anchor.as_str(), true)),
						None => None,
					},
				),
				AlterTypeAction::RenameValue {
					from,
					to,
				} => en.rename_label(from, to),
			}
		}
	}

	fn stage_define(&mut self, define: &DefineStatement) {
		if let DefineStatement::Type(stmt) = define {
			self.stage_type(stmt);
		} else if let DefineStatement::Table(table) = define {
			if let Expr::Table(name) = &table.name
				&& (table.kind != DefineKind::IfNotExists || self.is_absent(name.as_str()))
			{
				self.forget(name.as_str());
				self.created.insert(name.as_str().to_owned());
				if let Some(view) = &table.view {
					let sources = view.what.iter().map(|source| source.as_str().to_owned());
					self.view_over.insert(name.as_str().to_owned(), sources.collect());
				}
				if table.full && table.view.is_none() {
					self.nulls.insert(name.as_str().to_owned(), BTreeSet::new());
					self.defaults.insert(name.as_str().to_owned(), BTreeMap::new());
				}
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
			let defined_before = self.shapes.contains_key(&key);
			// A column a staged statement removed is absent, so a definition that applies only
			// when the column does not exist applies.
			let dropped = self.is_dropped(table, column);
			if field.kind == DefineKind::IfNotExists && !dropped {
				self.tentative.insert(key.clone());
			} else {
				self.tentative.remove(&key);
			}
			match field.sql_type.as_ref().and_then(declared_of) {
				Some(declared) => self.declared.insert(key.clone(), declared),
				None => self.declared.remove(&key),
			};
			self.retained_types.remove(&key);
			self.shapes.insert(key, shape);
			// A definition that does nothing when the column exists may leave what the wrapped
			// catalog says of the column as it is.
			if field.kind != DefineKind::IfNotExists {
				self.changed.insert((table.to_owned(), column.to_owned()));
			}
			// A definition that does nothing when the column exists leaves a column that an earlier
			// statement of the request defined as it is. On a table the wrapped catalog describes,
			// only a definition that runs adds a column.
			let own = self.own.contains(table);
			let applies =
				field.kind != DefineKind::IfNotExists || dropped || (own && !defined_before);
			if applies && (!own || self.nulls.contains_key(table)) {
				let holds_null = column != "id"
					&& kind_admits_null(kind)
					&& matches!(field.default, DefineDefault::None)
					&& field.value.is_none()
					&& field.computed.is_none();
				if holds_null {
					self.nulls.entry(table.to_owned()).or_default().insert(column.to_owned());
				} else if let Some(columns) = self.nulls.get_mut(table) {
					columns.remove(column);
				}
			}
			if applies && (!own || self.defaults.contains_key(table)) {
				let default = if let DefineDefault::Set(default) | DefineDefault::Always(default) =
					&field.default && column != "id"
					&& field.value.is_none()
					&& field.computed.is_none()
					&& reads_no_field(default)
				{
					Some(default)
				} else {
					None
				};
				if let Some(default) = default {
					self.defaults
						.entry(table.to_owned())
						.or_default()
						.insert(column.to_owned(), default.clone());
				} else if let Some(columns) = self.defaults.get_mut(table) {
					columns.remove(column);
				}
			}
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
		self.forget_null_column(table, column);
		self.stage_reference(table, column, field);
		// A column the catalog describes changes from the shape it has there, which an alteration
		// of the assertion or default alone leaves in part as it is.
		if !self.shapes.contains_key(&key) && !self.own.contains(table) {
			let Some(shape) = self.inner.column_shape(None, table, column) else {
				return;
			};
			if let Some(declared) = self.inner.column_sql_type(None, table, column) {
				self.declared.insert(key.clone(), declared);
			}
			self.shapes.insert(key.clone(), shape);
			self.retained_types.insert(key.clone());
		}
		if !matches!(field.kind, AlterKind::None) || !matches!(field.sql_type, AlterKind::None) {
			self.retained_types.remove(&key);
		}
		let Some(shape) = self.shapes.get_mut(&key) else {
			return;
		};
		if let AlterKind::Set(kind) = &field.kind {
			shape.kind = Some(kind.clone());
		}
		match &field.sql_type {
			AlterKind::Set(sql_type) => match declared_of(sql_type) {
				Some(declared) => {
					self.declared.insert(key.clone(), declared);
				}
				None => {
					self.declared.remove(&key);
				}
			},
			AlterKind::Drop => {
				self.declared.remove(&key);
			}
			AlterKind::None => {}
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
		if let RemoveStatement::Type(stmt) = remove {
			// A removal that names the type as another kind of object is refused when it runs,
			// so it leaves the type in place.
			let name = stmt.name.as_str().to_owned();
			// The removal of a type something still names is refused when it runs, so the
			// statements after it read the type as it stands.
			if self.names_type(&name) {
				return;
			}
			let (removes_types, removes_domains) = match stmt.object {
				None => (true, true),
				Some(SqlTypeObject::Type) => (true, false),
				Some(SqlTypeObject::Domain) => (false, true),
			};
			if removes_types {
				self.staged_enums().retain(|e| e.name != name);
			}
			let types = self.staged_user_types();
			if removes_domains {
				types.domains.retain(|d| d.name != name);
			}
			if removes_types {
				types.composites.retain(|c| c.name != name);
			}
		} else if let RemoveStatement::Table(table) = remove {
			if let Expr::Table(name) = &table.name {
				self.forget(name.as_str());
				self.created.remove(name.as_str());
			}
		} else if let RemoveStatement::Field(field) = remove
			&& let (Some(table), Some(column)) =
				(table_name(&field.table_name), column_name(&field.name))
		{
			self.shapes.remove(&(table.to_owned(), column.to_owned()));
			self.declared.remove(&(table.to_owned(), column.to_owned()));
			self.tentative.remove(&(table.to_owned(), column.to_owned()));
			self.retained_types.remove(&(table.to_owned(), column.to_owned()));
			self.forget_null_column(table, column);
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

	/// Whether a staged statement removed `column` of `table` and none defines it again: the
	/// column is gone, whatever the wrapped catalog says of it.
	fn is_dropped(&self, table: &str, column: &str) -> bool {
		let key = (table.to_owned(), column.to_owned());
		self.touched.contains(&key) && !self.shapes.contains_key(&key)
	}

	/// Drop what is known of the columns of `table`, which a staged statement creates anew or
	/// removes.
	fn forget(&mut self, table: &str) {
		self.shapes.retain(|(name, _), _| name != table);
		self.declared.retain(|(name, _), _| name != table);
		self.retained_types.retain(|(name, _)| name != table);
		self.tentative.retain(|(name, _)| name != table);
		self.keys.remove(table);
		self.indexes.remove(table);
		self.sequences.retain(|(name, _), _| name != table);
		self.view_over.remove(table);
		self.nulls.remove(table);
		self.defaults.remove(table);
		self.changed.retain(|(name, _)| name != table);
		self.own.insert(table.to_owned());
	}

	/// Stop saying that `column` of `table` holds NULL, or takes a default, in a row written
	/// without it, which a staged statement alters or removes.
	fn forget_null_column(&mut self, table: &str, column: &str) {
		if let Some(columns) = self.nulls.get_mut(table) {
			columns.remove(column);
		}
		if let Some(columns) = self.defaults.get_mut(table) {
			columns.remove(column);
		}
		self.changed.insert((table.to_owned(), column.to_owned()));
	}
}
impl StagedCatalog<'_> {
	/// The description of the table `name`, which a staged statement creates.
	/// Whether a column of a table, a domain or a composite type names the type `name`.
	fn names_type(&self, name: &str) -> bool {
		use crate::catalog::BinderCatalog;
		let staged = self
			.shapes
			.values()
			.any(|shape| shape.kind.as_ref().is_some_and(|kind| kind.references_type(name)));
		// The tables the wrapped catalog lists, asked for as constraint tables so that a catalog
		// that records what is asked of it lists them for the lowering that follows. A column a
		// staged statement removed names nothing; one whose shape is not known names the type
		// when the listing declares it with the type as its domain or composite type.
		let held = self.inner.constraint_tables().into_iter().any(|table| {
			!self.own.contains(&table.name)
				&& table.columns.iter().any(|column| {
					match self.column_shape(table.schema.as_deref(), &table.name, &column.name) {
						Some(shape) => shape.kind.is_some_and(|kind| kind.references_type(name)),
						None => {
							!self.is_dropped(&table.name, &column.name)
								&& (column.domain.as_deref() == Some(name)
									|| column.composite.as_deref() == Some(name))
						}
					}
				})
		});
		staged
			|| held || self
			.domain_types()
			.iter()
			.any(|domain| domain.name != name && domain.base.references_type(name))
			|| self.composite_types().iter().any(|composite| {
				composite.name != name
					&& composite.fields.iter().any(|(_, kind)| kind.references_type(name))
			})
	}

	fn created_table(&self, name: &String) -> TableSchema {
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
			info.declared = self.declared.get(&(name.clone(), column.to_owned())).cloned();
			infos.push(info);
		}
		TableSchema::new(name.as_str(), infos)
			.with_schema("public")
			.with_indexes(self.indexes.get(name).cloned().unwrap_or_default())
			.with_foreign_keys(self.keys.get(name).cloned().unwrap_or_default())
			.with_view_over(self.view_over.get(name).cloned().unwrap_or_default())
	}

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
					if let Some(kind) = &shape.kind
						&& !self.retained_types.contains(&key)
					{
						column.data_type = kind_type_name(kind);
					}
					column.declared = self.declared.get(&key).cloned();
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
				info.declared = self.declared.get(&(name.clone(), column.clone())).cloned();
				table.columns.push(info);
			}
			for (ordinal, column) in table.columns.iter_mut().enumerate() {
				column.ordinal = ordinal;
			}
		}
		let mut created: Vec<&String> = self.created.iter().collect();
		created.sort();
		for name in created {
			listed.push(self.created_table(name));
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
	fn enum_type(&self, name: &str) -> Option<crate::catalog::EnumType> {
		match &self.enums {
			Some(enums) => enums.iter().find(|e| e.name == name).cloned(),
			None => self.inner.enum_type(name),
		}
	}

	fn enum_types(&self) -> Vec<crate::catalog::EnumType> {
		match &self.enums {
			Some(enums) => enums.clone(),
			None => self.inner.enum_types(),
		}
	}

	fn domain_type(&self, name: &str) -> Option<crate::catalog::DomainType> {
		match &self.user_types {
			Some(types) => types.domains.iter().find(|d| d.name == name).cloned(),
			None => self.inner.domain_type(name),
		}
	}

	fn composite_type(&self, name: &str) -> Option<crate::catalog::CompositeType> {
		match &self.user_types {
			Some(types) => types.composites.iter().find(|c| c.name == name).cloned(),
			None => self.inner.composite_type(name),
		}
	}

	fn domain_types(&self) -> Vec<crate::catalog::DomainType> {
		match &self.user_types {
			Some(types) => types.domains.clone(),
			None => self.inner.domain_types(),
		}
	}

	fn composite_types(&self) -> Vec<crate::catalog::CompositeType> {
		match &self.user_types {
			Some(types) => types.composites.clone(),
			None => self.inner.composite_types(),
		}
	}

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
		let key = (table.to_owned(), column.to_owned());
		let staged = schema.is_none_or(|schema| schema == "public")
			&& (self.created.contains(table)
				|| (self.shapes.contains_key(&key)
					&& !self.tentative.contains(&key)
					&& !self.retained_types.contains(&key)));
		if !staged {
			if schema.is_none_or(|schema| schema == "public") && self.is_dropped(table, column) {
				return crate::SqlType::Unknown;
			}
			return self.inner.column_type(schema, table, column);
		}
		if let Some(ty) = self.declared.get(&key).and_then(crate::SqlType::of_declared) {
			return ty;
		}
		self.shapes
			.get(&key)
			.and_then(|shape| shape.kind.as_ref())
			.and_then(kind_type_name)
			.map(|name| crate::SqlType::from_name(&name))
			.unwrap_or(crate::SqlType::Unknown)
	}

	fn column_sql_type(
		&self,
		schema: Option<&str>,
		table: &str,
		column: &str,
	) -> Option<crate::declared_type::DeclaredSqlType> {
		let key = (table.to_owned(), column.to_owned());
		if schema.is_some_and(|schema| schema != "public")
			|| self.tentative.contains(&key)
			|| self.retained_types.contains(&key)
		{
			return self.inner.column_sql_type(schema, table, column);
		}
		if self.created.contains(table) || self.shapes.contains_key(&key) {
			return self.declared.get(&key).cloned();
		}
		if self.own.contains(table) || self.is_dropped(table, column) {
			return None;
		}
		self.inner.column_sql_type(schema, table, column)
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
			None if self.own.contains(table) || self.is_dropped(table, column) => None,
			None => self.inner.link_table(schema, table, column),
		}
	}

	fn column_enum(
		&self,
		schema: Option<&str>,
		table: &str,
		column: &str,
	) -> Option<crate::catalog::EnumColumn> {
		if self.is_tentative(table, column)
			&& let Some(held) = self.inner.column_enum(schema, table, column)
		{
			return Some(held);
		}
		match self.column_shape(schema, table, column) {
			Some(shape) => shape.kind.as_ref().and_then(|kind| {
				crate::catalog::EnumColumn::of_kind(kind, &|name| self.enum_type(name).is_some())
			}),
			None if self.own.contains(table) || self.is_dropped(table, column) => None,
			None => self.inner.column_enum(schema, table, column),
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
			None if self.own.contains(table) || self.is_dropped(table, column) => false,
			None => self.inner.link_admits_none(schema, table, column),
		}
	}

	/// The columns the wrapped catalog describes, without those a staged statement defines, alters
	/// or removes; for a table a staged statement creates or removes, whose columns are not those
	/// the wrapped catalog holds, the columns the staged definitions give.
	fn null_columns(&self, schema: Option<&str>, table: &str) -> Vec<String> {
		if self.own.contains(table) {
			return self
				.nulls
				.get(table)
				.map(|columns| columns.iter().cloned().collect())
				.unwrap_or_default();
		}
		let mut columns = self.inner.null_columns(schema, table);
		columns.retain(|column| !self.changed.contains(&(table.to_owned(), column.clone())));
		if let Some(added) = self.nulls.get(table) {
			columns.extend(added.iter().cloned());
			columns.sort_unstable();
			columns.dedup();
		}
		columns
	}

	/// As [`Self::null_columns`]: the wrapped catalog's without the columns a staged statement
	/// changes, or the staged definitions' for a table a staged statement creates or removes.
	fn default_columns(&self, schema: Option<&str>, table: &str) -> Vec<(String, Expr)> {
		if self.own.contains(table) {
			return self
				.defaults
				.get(table)
				.map(|columns| {
					columns
						.iter()
						.map(|(column, default)| (column.clone(), default.clone()))
						.collect()
				})
				.unwrap_or_default();
		}
		let mut columns = self.inner.default_columns(schema, table);
		columns.retain(|(column, _)| !self.changed.contains(&(table.to_owned(), column.clone())));
		if let Some(added) = self.defaults.get(table) {
			columns.extend(added.iter().map(|(column, default)| (column.clone(), default.clone())));
			columns.sort_by(|a, b| a.0.cmp(&b.0));
			columns.dedup_by(|a, b| a.0 == b.0);
		}
		columns
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
			None if self.own.contains(table) || self.is_dropped(table, column) => None,
			None => self.inner.column_shape(schema, table, column),
		}
	}
}

/// The SQL type `sql_type` declares, as the catalog describes a column's.
fn declared_of(
	sql_type: &surrealdb_expr::expr::statements::define::SqlType,
) -> Option<crate::declared_type::DeclaredSqlType> {
	crate::declared_type::DeclaredSqlType::parse(sql_type.dialect, &sql_type.text).ok()
}
