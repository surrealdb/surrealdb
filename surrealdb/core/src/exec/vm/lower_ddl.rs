//! Lowering schema statements into programs of store instructions.
//!
//! A schema statement is one plan node; lowering spells out its steps, in the
//! order its body takes them: the frame and permission checks first, then the
//! reads, the existence branch, the definition built from the statement's
//! constants and what the reads returned, the writes, and the cache
//! invalidation. Its value is `NONE` unless it answers otherwise.
//!
//! A statement whose name or clauses are only known by evaluating them is
//! declined; its written form — a bare identifier, a literal — is lowered.

use std::sync::Arc;

use super::{Lowered, Lowering, NotCovered, lower_view_rows};
use crate::catalog::stored::StoredField;
use crate::ddl::define::table::TableConversion;
use crate::exec::vm::codec::TypeId;
use crate::exec::vm::codec::key::RouteId;
use crate::exec::vm::program::{
	AccessCheck, Backfill, ClauseConversion, ColumnSource, FieldCheck, FieldRefusal, IdSequence,
	Instr, Label, NameClause, Operand, PathSource, Reg, Service, Source, StatementError,
	StoreInstr, StoreRefusal, UnitId,
};
use crate::expr::logical::LogicalDdlStatement;
use crate::expr::statements::alter::access::AlterAccessStatement;
use crate::expr::statements::alter::analyzer::AlterAnalyzerStatement;
use crate::expr::statements::alter::api::AlterApiStatement;
use crate::expr::statements::alter::bucket::AlterBucketStatement;
use crate::expr::statements::alter::config::AlterConfigStatement;
use crate::expr::statements::alter::event::AlterEventStatement;
use crate::expr::statements::alter::field::AlterFieldStatement;
use crate::expr::statements::alter::function::AlterFunctionStatement;
use crate::expr::statements::alter::index::AlterIndexStatement;
use crate::expr::statements::alter::module::AlterModuleStatement;
use crate::expr::statements::alter::param::AlterParamStatement;
use crate::expr::statements::alter::sequence::AlterSequenceStatement;
use crate::expr::statements::alter::system::AlterSystemStatement;
use crate::expr::statements::alter::table::AlterTableStatement;
use crate::expr::statements::alter::user::AlterUserStatement;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::access::DefineAccessStatement;
use crate::expr::statements::define::analyzer::DefineAnalyzerStatement;
use crate::expr::statements::define::api::DefineApiStatement;
use crate::expr::statements::define::bucket::DefineBucketStatement;
use crate::expr::statements::define::config::DefineConfigStatement;
use crate::expr::statements::define::database::DefineDatabaseStatement;
use crate::expr::statements::define::event::DefineEventStatement;
use crate::expr::statements::define::field::DefineFieldStatement;
use crate::expr::statements::define::function::DefineFunctionStatement;
use crate::expr::statements::define::model::DefineModelStatement;
use crate::expr::statements::define::module::DefineModuleStatement;
use crate::expr::statements::define::namespace::DefineNamespaceStatement;
use crate::expr::statements::define::param::DefineParamStatement;
use crate::expr::statements::define::sequence::DefineSequenceStatement;
use crate::expr::statements::define::table::DefineTableStatement;
use crate::expr::statements::define::user::DefineUserStatement;
use crate::expr::statements::rebuild::{RebuildIndexStatement, RebuildStatement};
use crate::expr::statements::remove::access::RemoveAccessStatement;
use crate::expr::statements::remove::analyzer::RemoveAnalyzerStatement;
use crate::expr::statements::remove::api::RemoveApiStatement;
use crate::expr::statements::remove::config::RemoveConfigStatement;
use crate::expr::statements::remove::database::RemoveDatabaseStatement;
use crate::expr::statements::remove::event::RemoveEventStatement;
use crate::expr::statements::remove::field::RemoveFieldStatement;
use crate::expr::statements::remove::index::RemoveIndexStatement;
use crate::expr::statements::remove::model::RemoveModelStatement;
use crate::expr::statements::remove::module::RemoveModuleStatement;
use crate::expr::statements::remove::namespace::RemoveNamespaceStatement;
use crate::expr::statements::remove::sequence::RemoveSequenceStatement;
use crate::expr::statements::remove::table::RemoveTableStatement;
use crate::expr::statements::remove::user::RemoveUserStatement;
use crate::expr::statements::{AlterStatement, DefineStatement, RemoveStatement};
use crate::expr::{Base, Expr, Literal};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

/// What a schema statement's body checks it against in the catalog, and
/// what those checks decide, read by the statement's mapping
/// ([`ddl_checks`]): the lowering takes them from the plan node and reads no
/// catalog. A statement whose names are evaluated is checked by its program
/// against the catalog it finds when it runs, and has none.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) enum DdlChecks {
	#[default]
	None,
	/// `DEFINE TABLE`: the refusal of the definition once the table is read,
	/// and the definition's type as its checks leave it.
	DefineTable {
		definition_refusal: Option<FieldRefusal>,
		table_type: crate::catalog::TableType,
		/// For a table whose name is written, the definition its clauses
		/// form, which the statement judges against the table it finds when
		/// it runs.
		redefined: Option<Arc<crate::catalog::TableDefinition>>,
	},
	/// `DEFINE FIELD`: the table and the field as the catalog holds them, the
	/// table's fields, and the refusals of the definition's checks: `refused`
	/// of those the body makes before `FLEXIBLE`'s, and `flexible` of
	/// `FLEXIBLE`'s. The record id components an `id` definition names are not judged here: what
	/// they are judged against includes the rows the table's views hold, which
	/// the statements run before this one may change, so the statement judges
	/// them when it runs (`FieldCheck::KeyComponents`), between the two.
	DefineField {
		existing_table: Option<Arc<crate::catalog::TableDefinition>>,
		existing_field: Option<Arc<crate::catalog::FieldDefinition>>,
		fields: Arc<[crate::catalog::FieldDefinition]>,
		indexed: Option<FieldRefusal>,
		refused: Option<FieldRefusal>,
		flexible: Option<FieldRefusal>,
	},
	/// `REMOVE TABLE`, which checks nothing of the catalog when it is
	/// planned: the tables a view reads from, and the edges of the relations
	/// over the table, are read when it runs (`StoreInstr::ViewSources`,
	/// `StoreInstr::RefuseRemovingLightweight`).
	RemoveTable,
	/// `REMOVE FIELD` and `ALTER FIELD`: the table, the field and the table's
	/// fields as the catalog holds them — the fields nested in the field,
	/// which `REMOVE FIELD` removes with it, and those an altered kind writes
	/// or drops — and the refusal of the field's name against the record id
	/// components of the table: a component name has no definition of its
	/// own to remove or alter. Whether a definition still reads a component
	/// of the `id` a `REMOVE FIELD` removes is judged when the statement runs
	/// (`FieldCheck::IdUnread`), against the rows the views over the table
	/// hold then.
	Field {
		existing_table: Option<Arc<crate::catalog::TableDefinition>>,
		existing_field: Option<Arc<crate::catalog::FieldDefinition>>,
		fields: Arc<[crate::catalog::FieldDefinition]>,
		key_refusal: Option<FieldRefusal>,
	},
}

/// The checks `statement`'s body makes against the catalog, made by
/// `planner` as the body makes them; see [`DdlChecks`]. A check that fails
/// with anything but a refusal of the definition leaves the statement not
/// covered.
pub(in crate::exec::vm) async fn ddl_checks(
	planner: &crate::exec::planner::Planner<'_>,
	statement: &LogicalDdlStatement,
) -> Lowered<DdlChecks> {
	use crate::catalog::TableType;
	use crate::catalog::providers::{DatabaseProvider, TableProvider};
	let txn = planner.txn();
	let unread = |e: anyhow::Error| {
		crate::exec::vm::plan_catalog::unread_catalog(&e, || {
			format!("the catalog did not read: {e}")
		})
	};
	// The session's database, as the body resolves it.
	let ids = match (txn, planner.ns(), planner.db()) {
		(Some(txn), Some(ns), Some(db)) => txn
			.get_db_by_name(ns, db, None)
			.await
			.map_err(unread)?
			.map(|d| (d.namespace_id, d.database_id)),
		_ => None,
	};
	let table_of = async |name: &crate::val::TableName| match (txn, ids) {
		(Some(txn), Some((ns, db))) => txn.get_tb(ns, db, name, None).await.map_err(unread),
		_ => Ok(None),
	};
	let field_of = async |table: &crate::val::TableName, fd: &str| match (txn, ids) {
		(Some(txn), Some((ns, db))) => {
			txn.get_tb_field(ns, db, table, fd, None).await.map_err(unread)
		}
		_ => Ok(None),
	};
	// The definitions of the fields of `table`.
	let fields_of = async |table: &crate::val::TableName| match (txn, ids) {
		(Some(txn), Some((ns, db))) => txn.all_tb_fields(ns, db, table, None).await.map_err(unread),
		_ => Ok(Arc::from(Vec::new())),
	};
	match statement {
		LogicalDdlStatement::Define(define) => match define.as_ref() {
			DefineStatement::Table(stmt) => {
				// A view the catalog refuses is refused by the statement's
				// lowering ([`Lowering::define_table`]); the rest of the
				// definition is judged without it.
				let view = stmt
					.view
					.as_ref()
					.map(crate::catalog::view::view_to_definition)
					.transpose()
					.ok()
					.flatten();
				let written = written_text(&stmt.name);
				let tb = crate::val::TableName::new(written.as_deref().unwrap_or(""));
				let mut template = table_template(stmt, tb, view);
				// The definition's own clauses are judged here; what the
				// catalog and the table's contents hold of it is judged when
				// the statement runs, against the table it meets
				// (`StoreInstr::RefuseRedefiningTable`).
				let mut definition_refusal = None;
				if matches!(&stmt.table_type, TableType::Relation(rel) if rel.lightweight)
					&& let Err(e) =
						crate::ddl::define::table::validate_lightweight_clauses(&mut template)
				{
					definition_refusal = Some(refusal_of(&e, "a lightweight relation")?);
				}
				Ok(DdlChecks::DefineTable {
					definition_refusal,
					table_type: template.table_type.clone(),
					redefined: written.is_some().then(|| Arc::new(template)),
				})
			}
			DefineStatement::Field(stmt) => {
				// Reached only on a storage fault or a validator's error the
				// field refusal does not name.
				let decline = |why: &str| NotCovered::internal(why.to_owned());
				let written_name = match &stmt.name {
					Expr::Idiom(name) => canonical_field_name(name),
					_ => None,
				};
				let (Some(name), Some(table), Some(comment)) =
					(written_name, written_text(&stmt.what), written_comment(&stmt.comment))
				else {
					return Ok(DdlChecks::None);
				};
				let table = crate::val::TableName::new(table);
				// A database that does not exist yet has no catalog to check
				// the definition against: the program checks it as it runs.
				let Some((ns, db)) = ids else {
					return Ok(DdlChecks::None);
				};
				let txn = txn.ok_or_else(|| decline("planning without a transaction"))?;
				let refused =
					|e: anyhow::Error| match crate::exec::vm::plan_catalog::catalog_refusal(&e) {
						Some(entry) => {
							NotCovered::refused(crate::exec::vm::plan::Refusal::CatalogEntry(entry))
						}
						None => decline("a field definition whose catalog did not read"),
					};
				let existing_table = txn.get_tb(ns, db, &table, None).await.map_err(refused)?;
				let fd = name.to_raw_string();
				let existing_field =
					txn.get_tb_field(ns, db, &table, &fd, None).await.map_err(refused)?;
				let fields = txn.all_tb_fields(ns, db, &table, None).await.map_err(refused)?;
				let definition = field_definition(stmt, &name, &table, &comment);
				// One check while the definition is formed, the rest once the
				// database is resolved.
				let refusal = |checked: anyhow::Result<()>| match checked {
					Ok(()) => Ok(None),
					Err(e) => FieldRefusal::of(&e)
						.map(Some)
						.ok_or_else(|| decline("a field definition whose checks did not complete")),
				};
				let indexed = refusal(
					crate::ddl::define::field::define_field_definition_check_indexed_computed(
						txn,
						ns,
						db,
						&definition,
					)
					.await,
				)?;
				// The components an `id` names are judged when the statement
				// runs; the clash of any other field with one reads only the
				// catalog.
				let refused = refusal(
					async {
						crate::ddl::define::field::define_field_definition_validate_clauses(
							txn,
							ns,
							db,
							&definition,
						)
						.await?;
						if !definition.name.is_id() {
							crate::ddl::define_field_statement_validate_key_components(
								txn,
								ns,
								db,
								&stmt.kind,
								&definition,
							)
							.await?;
						}
						Ok(())
					}
					.await,
				)?;
				let flexible = refusal(
					crate::ddl::define_field_statement_validate_flexible_restrictions(
						txn,
						ns,
						db,
						&definition,
					)
					.await,
				)?;
				Ok(DdlChecks::DefineField {
					existing_table,
					existing_field,
					fields,
					indexed,
					refused,
					flexible,
				})
			}
			_ => Ok(DdlChecks::None),
		},
		LogicalDdlStatement::Remove(remove) => match remove.as_ref() {
			// The relations over the table are checked when the statement runs
			// (`StoreInstr::RefuseRemovingLightweight`), against the edges the
			// block's statements before it leave.
			RemoveStatement::Table(_) => Ok(DdlChecks::RemoveTable),
			RemoveStatement::Field(stmt) => {
				let written_name = match &stmt.name {
					Expr::Idiom(name) => canonical_field_name(name),
					_ => None,
				};
				let (Some(name), Some(table)) = (written_name, written_text(&stmt.table_name))
				else {
					return Ok(DdlChecks::None);
				};
				let table = crate::val::TableName::new(table);
				let key_refusal = match (txn, ids) {
					(Some(txn), Some((ns, db))) => key_name_refusal(
						crate::ddl::key_scan::refuse_component_name(txn, ns, db, &table, &name)
							.await,
					)?,
					_ => None,
				};
				Ok(DdlChecks::Field {
					existing_table: table_of(&table).await?,
					existing_field: field_of(&table, &name.to_raw_string()).await?,
					fields: fields_of(&table).await?,
					key_refusal,
				})
			}
			_ => Ok(DdlChecks::None),
		},
		LogicalDdlStatement::Alter(alter) => match alter.as_ref() {
			AlterStatement::Field(stmt) => {
				let written_name = match &stmt.name {
					Expr::Idiom(name) => canonical_field_name(name),
					_ => None,
				};
				let (Some(name), Some(table)) = (written_name, written_text(&stmt.what)) else {
					return Ok(DdlChecks::None);
				};
				// An alteration with no database to read the field from is
				// checked by its program against the catalog it runs against.
				if ids.is_none() || txn.is_none() {
					return Ok(DdlChecks::None);
				}
				let table = crate::val::TableName::new(table);
				let key_refusal = match (txn, ids) {
					(Some(txn), Some((ns, db))) => key_name_refusal(
						crate::ddl::key_scan::refuse_component_name(txn, ns, db, &table, &name)
							.await,
					)?,
					_ => None,
				};
				Ok(DdlChecks::Field {
					existing_table: table_of(&table).await?,
					existing_field: field_of(&table, &name.to_raw_string()).await?,
					fields: fields_of(&table).await?,
					key_refusal,
				})
			}
			_ => Ok(DdlChecks::None),
		},
		LogicalDdlStatement::Rebuild(_) => Ok(DdlChecks::None),
	}
}

/// The definition `DEFINE FIELD` `stmt` forms for field `name` of `table`,
/// with its written `comment`, before the database is resolved.
fn field_definition(
	stmt: &DefineFieldStatement,
	name: &crate::expr::Idiom,
	table: &crate::val::TableName,
	comment: &Value,
) -> crate::catalog::FieldDefinition {
	use crate::catalog;
	use crate::expr::statements::define::field::DefineDefault;
	let mut definition = catalog::FieldDefinition {
		name: name.clone(),
		table: table.clone(),
		field_kind: None,
		key: None,
		key_part: None,
		flexible: stmt.flexible,
		readonly: stmt.readonly,
		inline: stmt.inline,
		value: stmt.value.clone(),
		value_body: stmt.value.as_ref().map(catalog::LogicalBody::of),
		assert: stmt.assert.clone(),
		assert_body: stmt.assert.as_ref().map(catalog::LogicalBody::of),
		computed: stmt.computed.clone(),
		computed_body: stmt.computed.as_ref().map(catalog::LogicalBody::of),
		default: match &stmt.default {
			DefineDefault::None => catalog::DefineDefault::None,
			DefineDefault::Set(x) => catalog::DefineDefault::Set(x.clone()),
			DefineDefault::Always(x) => catalog::DefineDefault::Always(x.clone()),
		},
		default_body: match &stmt.default {
			DefineDefault::None => None,
			DefineDefault::Set(x) | DefineDefault::Always(x) => Some(catalog::LogicalBody::of(x)),
		},
		select_permission: stmt.permissions.select.clone(),
		select_body: catalog::LogicalPermissionBody::of(&stmt.permissions.select),
		create_permission: stmt.permissions.create.clone(),
		update_permission: stmt.permissions.update.clone(),
		comment: match &comment {
			Value::String(s) => Some(s.as_str().to_owned()),
			_ => None,
		},
		reference: stmt.reference.clone(),
		auth_limit: Default::default(),
		graphql_alias: stmt.graphql_alias.clone(),
		graphql_deprecated: stmt.graphql_deprecated.clone(),
	};
	// The type and the names of the record id components it carries are set
	// together, so the definition's key structure always matches its type.
	definition.set_type(stmt.field_kind.clone(), stmt.key_names.clone());
	definition
}

/// The definition `DEFINE TABLE` `stmt` forms for table `tb` with `view`, as
/// the catalog stores it, with the holes the program fills: ids, cache stamps
/// and the graph markers a redefinition keeps.
fn table_template(
	stmt: &DefineTableStatement,
	tb: crate::val::TableName,
	view: Option<crate::catalog::ViewDefinition>,
) -> crate::catalog::TableDefinition {
	let nil = uuid::Uuid::nil();
	crate::catalog::TableDefinition {
		namespace_id: crate::catalog::NamespaceId(0),
		database_id: crate::catalog::DatabaseId(0),
		table_id: crate::catalog::TableId(0),
		name: tb,
		drop: stmt.drop,
		schemafull: stmt.full,
		view,
		permissions: stmt.permissions.clone(),
		permissions_body: crate::catalog::LogicalPermissions::of(&stmt.permissions),
		table_type: stmt.table_type.clone(),
		changefeed: stmt.changefeed,
		comment: None,
		cache_fields_ts: nil,
		cache_events_ts: nil,
		cache_tables_ts: nil,
		cache_indexes_ts: nil,
		cache_lives_ts: nil,
		graphql_alias: stmt.graphql_alias.clone(),
		graphql_deprecated: stmt.graphql_deprecated.clone(),
		graph_folded: false,
		graph_doc_ids: false,
		inline_edges_cap: stmt.inline_edges_cap,
		inline_refs_cap: stmt.inline_refs_cap,
		graph_inline_gen: 0,
	}
}

impl Lowering<'_, '_> {
	/// The checks the schema statement being lowered was mapped with.
	fn ddl_checks(&self) -> Lowered<DdlChecks> {
		self.ddl_checks
			.as_deref()
			.cloned()
			.ok_or_else(|| NotCovered::internal("a schema statement lowered with no checks"))
	}
}

impl Lowering<'_, '_> {
	/// Lower a schema statement into its program.
	pub(super) async fn ddl(&mut self, statement: &LogicalDdlStatement) -> Lowered<()> {
		self.id_draws = 0;
		match statement {
			LogicalDdlStatement::Define(define) => match define.as_ref() {
				DefineStatement::Namespace(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_namespace(stmt)).await?
				}
				DefineStatement::Database(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_database(stmt)).await?
				}
				DefineStatement::Table(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_table(stmt)).await?
				}
				DefineStatement::Field(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_field(stmt)).await?
				}
				DefineStatement::Param(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_param(stmt)).await?
				}
				DefineStatement::Event(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_event(stmt)).await?
				}
				DefineStatement::Function(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_function(stmt)).await?
				}
				DefineStatement::User(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_user(stmt)).await?
				}
				DefineStatement::Analyzer(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_analyzer(stmt)).await?
				}
				DefineStatement::Sequence(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_sequence(stmt)).await?
				}
				DefineStatement::Api(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_api(stmt)).await?
				}
				DefineStatement::Config(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_config(stmt)).await?
				}
				DefineStatement::Bucket(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_bucket(stmt)).await?
				}
				DefineStatement::Module(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_module(stmt)).await?
				}
				DefineStatement::Access(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_access(stmt)).await?
				}
				DefineStatement::Index(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_index(stmt)).await?
				}
				DefineStatement::Model(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.define_model(stmt)).await?
				}
			},
			LogicalDdlStatement::Remove(remove) => match remove.as_ref() {
				RemoveStatement::Namespace(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.remove_namespace(stmt)).await?
				}
				RemoveStatement::Database(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.remove_database(stmt)).await?
				}
				RemoveStatement::Table(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.remove_table(stmt)).await?
				}
				RemoveStatement::Event(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.remove_event(stmt)).await?
				}
				RemoveStatement::Field(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.remove_field(stmt)).await?
				}
				RemoveStatement::Index(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.remove_index(stmt)).await?
				}
				RemoveStatement::Param(stmt) => {
					self.remove_db_definition(
						ResourceKind::Parameter,
						("ParamKey", "pa", "StoredParamDefinition"),
						DbName::Written(stmt.name.as_str(), stmt.name.as_str().to_owned()),
						stmt.if_exists,
						StatementError::PaNotFound,
					)
					.await?
				}
				RemoveStatement::Function(stmt) => {
					self.remove_db_definition(
						ResourceKind::Function,
						("FunctionKey", "fc", "StoredFunctionDefinition"),
						DbName::Written(stmt.name.as_str(), format!("fn::{}", stmt.name.as_str())),
						stmt.if_exists,
						StatementError::FcNotFound,
					)
					.await?
				}
				RemoveStatement::Sequence(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.remove_sequence(stmt)).await?
				}
				RemoveStatement::Analyzer(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.remove_analyzer(stmt)).await?
				}
				RemoveStatement::Access(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.remove_access(stmt)).await?
				}
				RemoveStatement::User(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.remove_user(stmt)).await?
				}
				RemoveStatement::Bucket(stmt) => {
					self.remove_db_definition(
						ResourceKind::Bucket,
						("BucketKey", "bu", "StoredBucketDefinition"),
						DbName::Named(&stmt.name, NameClause::Bucket),
						stmt.if_exists,
						StatementError::BuNotFound,
					)
					.await?
				}
				RemoveStatement::Module(stmt) => self.remove_module(stmt)?,
				RemoveStatement::Api(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.remove_api(stmt)).await?
				}
				RemoveStatement::Config(stmt) => self.remove_config(stmt)?,
				RemoveStatement::Model(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.remove_model(stmt)).await?
				}
			},
			LogicalDdlStatement::Alter(alter) => match alter.as_ref() {
				AlterStatement::Table(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.alter_table(stmt)).await?
				}
				AlterStatement::Field(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.alter_field(stmt)).await?
				}
				AlterStatement::Namespace(stmt) => self.alter_namespace(stmt.compact)?,
				AlterStatement::Database(stmt) => self.alter_database(stmt.compact)?,
				AlterStatement::Param(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.alter_param(stmt)).await?
				}
				AlterStatement::Function(stmt) => self.alter_function(stmt)?,
				AlterStatement::Event(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.alter_event(stmt)).await?
				}
				AlterStatement::Sequence(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.alter_sequence(stmt)).await?
				}
				AlterStatement::Analyzer(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.alter_analyzer(stmt)).await?
				}
				AlterStatement::User(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.alter_user(stmt)).await?
				}
				AlterStatement::Config(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.alter_config(stmt)).await?
				}
				AlterStatement::Bucket(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.alter_bucket(stmt)).await?
				}
				AlterStatement::Index(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.alter_index(stmt)).await?
				}
				AlterStatement::Access(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.alter_access(stmt)).await?
				}
				AlterStatement::System(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.alter_system(stmt)).await?
				}
				AlterStatement::Module(stmt) => self.alter_module(stmt)?,
				AlterStatement::Api(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.alter_api(stmt)).await?
				}
			},
			LogicalDdlStatement::Rebuild(rebuild) => match rebuild.as_ref() {
				RebuildStatement::Index(stmt) => {
					crate::exec::vm::host::out_of_line(|| self.rebuild_index(stmt)).await?
				}
			},
		}
		// Every path through a schema statement's program answers it: the
		// program ends on the row it answers, never past its last step.
		if !matches!(
			self.code.last(),
			Some(
				Instr::EndRow
					| Instr::EmitEnd { .. }
					| Instr::BuildObjectEmit { .. }
					| Instr::StopPipeline
			)
		) {
			return Err(NotCovered::internal(
				"a schema statement whose program does not answer it",
			));
		}
		self.push_write_pipeline(Source::Once)?;
		self.scalar = true;
		self.writes = true;
		Ok(())
	}

	/// A statement the planner refuses, raising the refusal when it runs: a
	/// late-bound statement of a block, refused where the block reaches it.
	pub(super) fn refused_when_run(
		&mut self,
		refusal: &crate::exec::vm::plan::Refusal,
	) -> Lowered<()> {
		self.store(StoreInstr::Refuse {
			refusal: Box::new(refusal.clone()),
		});
		self.answer_none()?;
		self.push_write_pipeline(Source::Once)?;
		self.scalar = true;
		Ok(())
	}

	fn store(&mut self, mut instr: StoreInstr) -> Label {
		if let StoreInstr::NextId {
			draw,
			reserved,
			..
		}
		| StoreInstr::NextIndexId {
			draw,
			reserved,
			..
		}
		| StoreInstr::NewUuid {
			draw,
			reserved,
			..
		}
		| StoreInstr::StampCatalog {
			draw,
			reserved,
			..
		} = &mut instr
		{
			*draw = self.id_draws;
			self.id_draws += 1;
			if let Some(&(_, id)) = self.reserving.iter().find(|(at, _)| *at == *draw) {
				*reserved = Some(self.reservations.len() as u32);
				self.reservations.push(id);
			}
		}
		self.emit(Instr::Store(instr))
	}

	/// An operand denoting `value`.
	pub(super) fn constant(&mut self, value: Value) -> Operand {
		self.consts.push(value);
		Operand::Const((self.consts.len() - 1) as u32)
	}

	/// A register holding `value`.
	fn load(&mut self, value: Operand) -> Lowered<Reg> {
		let dst = self.register()?;
		self.emit(Instr::Load {
			dst,
			src: value,
		});
		Ok(dst)
	}

	/// A register holding a new object with `fields`, set in order.
	pub(super) fn object(&mut self, fields: &[(&str, Operand)]) -> Lowered<Reg> {
		let dst = self.register()?;
		self.emit(Instr::NewObject {
			dst,
		});
		for (name, value) in fields {
			self.paths.push(crate::exec::field_path::FieldPath::field(*name));
			let path = (self.paths.len() - 1) as u32;
			self.emit(Instr::SetField {
				object: dst,
				path,
				value: *value,
			});
		}
		Ok(dst)
	}

	/// End the program answering `NONE`.
	fn answer_none(&mut self) -> Lowered<()> {
		let none = self.constant(Value::None);
		let out = self.load(none)?;
		self.emit(Instr::Emit {
			src: out,
		});
		self.emit(Instr::EndRow);
		Ok(())
	}

	/// The existence branch over the definition in `existing`: raise `error`
	/// naming `name` unless the statement runs as an import, stop answering
	/// `NONE` under `IF NOT EXISTS`, or continue under `OVERWRITE`. Falls
	/// through when the definition exists and the statement continues; answers
	/// the jump to patch to where a statement over a missing definition goes.
	fn exists(
		&mut self,
		kind: &DefineKind,
		existing: Reg,
		error: StatementError,
		name: Reg,
	) -> Lowered<Label> {
		let missing = self.emit(Instr::JumpIf {
			cond: Operand::Reg(existing),
			truthy: false,
			target: 0,
		});
		match kind {
			DefineKind::Default => {
				let import = self.store(StoreInstr::JumpIfImport {
					target: 0,
				});
				self.store(StoreInstr::Raise {
					error,
					arg: name,
				});
				let at = self.here();
				self.patch(import, at);
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => self.answer_none()?,
		}
		Ok(missing)
	}

	/// `DEFINE NAMESPACE` — `ddl/define/namespace.rs`.
	async fn define_namespace(&mut self, stmt: &DefineNamespaceStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Namespace,
			base: Base::Root,
		});
		let name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Namespace))
				.await?;
		let name = name.operand;
		let name_reg = self.load(name)?;
		let key = self.object(&[("ns", name)])?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: existing,
		});
		let id = self.register()?;
		let missing =
			self.exists(&stmt.kind, existing, StatementError::NsAlreadyExists, name_reg)?;
		self.fields.push("namespace_id".to_owned());
		let field = (self.fields.len() - 1) as u32;
		self.store(StoreInstr::GetField {
			src: existing,
			name: field,
			dst: id,
		});
		let built = self.emit(Instr::Jump {
			target: 0,
		});
		let at = self.here();
		self.patch(missing, at);
		self.store(StoreInstr::NextId {
			sequence: IdSequence::Namespace,
			ns: None,
			db: None,
			dst: id,
			draw: 0,
			reserved: None,
		});
		let at = self.here();
		self.patch(built, at);
		let comment =
			crate::exec::vm::host::out_of_line(|| self.comment(&stmt.comment)).await?.operand;
		let definition = self.object(&[
			("namespace_id", Operand::Reg(id)),
			("name", name),
			("comment", comment),
		])?;
		self.store(StoreInstr::Put {
			route: RouteId::named("NamespaceKey"),
			key,
			ty: TypeId::named("NamespaceDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// `dst` = the field `name` of the object in `src`.
	fn get_field(&mut self, src: Reg, name: &str, dst: Reg) {
		self.fields.push(name.to_owned());
		let name = (self.fields.len() - 1) as u32;
		self.store(StoreInstr::GetField {
			src,
			name,
			dst,
		});
	}

	/// The id of the session's namespace, in a register, creating the
	/// namespace when it does not exist yet (`get_or_add_ns`).
	fn namespace_id_creating(&mut self) -> Lowered<Reg> {
		let ns_name = self.register()?;
		self.store(StoreInstr::SessionNs {
			dst: ns_name,
		});
		let key = self.object(&[("ns", Operand::Reg(ns_name))])?;
		let namespace = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: namespace,
		});
		let present = self.emit(Instr::JumpIf {
			cond: Operand::Reg(namespace),
			truthy: true,
			target: 0,
		});
		let id = self.register()?;
		self.store(StoreInstr::NextId {
			sequence: IdSequence::Namespace,
			ns: None,
			db: None,
			dst: id,
			draw: 0,
			reserved: None,
		});
		let none = self.constant(Value::None);
		let created = self.object(&[
			("namespace_id", Operand::Reg(id)),
			("name", Operand::Reg(ns_name)),
			("comment", none),
		])?;
		self.store(StoreInstr::Put {
			route: RouteId::named("NamespaceKey"),
			key,
			ty: TypeId::named("NamespaceDefinition"),
			value: created,
		});
		self.emit(Instr::Load {
			dst: namespace,
			src: Operand::Reg(created),
		});
		let at = self.here();
		self.patch(present, at);
		self.get_field(namespace, "namespace_id", id);
		Ok(id)
	}

	/// `DEFINE DATABASE` — `ddl/define/database.rs`.
	async fn define_database(&mut self, stmt: &DefineDatabaseStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Database,
			base: Base::Ns,
		});
		let ns = self.namespace_id_creating()?;
		let name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Database))
				.await?;
		let name = name.operand;
		let name_reg = self.load(name)?;
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", name)])?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("DatabaseKey"),
			key,
			ty: TypeId::named("DatabaseDefinition"),
			dst: existing,
		});
		let id = self.register()?;
		let missing =
			self.exists(&stmt.kind, existing, StatementError::DbAlreadyExists, name_reg)?;
		self.get_field(existing, "database_id", id);
		let built = self.emit(Instr::Jump {
			target: 0,
		});
		let at = self.here();
		self.patch(missing, at);
		self.store(StoreInstr::NextId {
			sequence: IdSequence::Database,
			ns: Some(ns),
			db: None,
			dst: id,
			draw: 0,
			reserved: None,
		});
		let at = self.here();
		self.patch(built, at);
		let comment =
			crate::exec::vm::host::out_of_line(|| self.comment(&stmt.comment)).await?.operand;
		let changefeed = self.constant(match &stmt.changefeed {
			Some(cf) => stored(*cf)?,
			None => Value::None,
		});
		let strict = self.constant(Value::Bool(stmt.strict));
		let definition = self.object(&[
			("namespace_id", Operand::Reg(ns)),
			("database_id", Operand::Reg(id)),
			("name", name),
			("comment", comment),
			("changefeed", changefeed),
			("strict", strict),
		])?;
		self.store(StoreInstr::Put {
			route: RouteId::named("DatabaseKey"),
			key,
			ty: TypeId::named("DatabaseDefinition"),
			value: definition,
		});
		// A retention written here makes the changefeed collector's watermark
		// stale, so its fence moves in the same transaction.
		if stmt.changefeed.is_some() {
			let fence = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(id))])?;
			self.store(StoreInstr::Increment {
				route: RouteId::named("ChangefeedFenceKey"),
				key: fence,
			});
		}
		self.store(StoreInstr::ClearDatastoreCache);
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// `dst`, of the object in `src`, set to `value`.
	/// Set `value` at the path `path`, a field name per step, of the object in
	/// `object`.
	fn set_path(&mut self, object: Reg, path: &[&str], value: Operand) {
		use crate::exec::field_path::{FieldPath, FieldPathPart};
		self.paths.push(FieldPath(
			path.iter().map(|name| FieldPathPart::Field((*name).to_owned())).collect(),
		));
		let path = (self.paths.len() - 1) as u32;
		self.emit(Instr::SetField {
			object,
			path,
			value,
		});
	}

	fn set_field(&mut self, object: Reg, name: &str, value: Operand) {
		self.paths.push(crate::exec::field_path::FieldPath::field(name));
		let path = (self.paths.len() - 1) as u32;
		self.emit(Instr::SetField {
			object,
			path,
			value,
		});
	}

	/// Raise `error` naming the value in `name` unless `value` holds one.
	fn expect(&mut self, value: Reg, error: StatementError, name: Reg) {
		let present = self.emit(Instr::JumpIf {
			cond: Operand::Reg(value),
			truthy: true,
			target: 0,
		});
		self.store(StoreInstr::Raise {
			error,
			arg: name,
		});
		let at = self.here();
		self.patch(present, at);
	}

	/// The ids of the session's namespace and database, which must exist
	/// (`expect_ns_db_ids`): a namespace that does not exist holds no
	/// database, so either missing is the database not found.
	fn session_ids(&mut self) -> Lowered<(Reg, Reg)> {
		let (ns, db, database) = self.session_ids_and_database()?;
		self.release(database);
		Ok((ns, db))
	}

	/// [`Self::session_ids`], and the database's definition in a register.
	fn session_ids_and_database(&mut self) -> Lowered<(Reg, Reg, Reg)> {
		self.selected_ids(StatementError::DbNotFound)
	}

	/// The ids of the session's namespace and database, which must exist, and
	/// the database's definition in a register: a namespace that does not
	/// exist raises `no_namespace` (`NsNotFound` naming the namespace, or
	/// `DbNotFound` naming the database), a database that does not exist
	/// `DbNotFound`.
	fn selected_ids(&mut self, no_namespace: StatementError) -> Lowered<(Reg, Reg, Reg)> {
		let ns_name = self.register()?;
		self.store(StoreInstr::SessionNs {
			dst: ns_name,
		});
		let db_name = self.register()?;
		self.store(StoreInstr::SessionDb {
			dst: db_name,
		});
		let ns_key = self.object(&[("ns", Operand::Reg(ns_name))])?;
		let namespace = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key: ns_key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: namespace,
		});
		let named = match no_namespace {
			StatementError::NsNotFound => ns_name,
			_ => db_name,
		};
		self.expect(namespace, no_namespace, named);
		let ns = self.register()?;
		self.get_field(namespace, "namespace_id", ns);
		let db_key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db_name))])?;
		let database = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("DatabaseKey"),
			key: db_key,
			ty: TypeId::named("DatabaseDefinition"),
			dst: database,
		});
		self.expect(database, StatementError::DbNotFound, db_name);
		let db = self.register()?;
		self.get_field(database, "database_id", db);
		for reg in [ns_name, db_name, ns_key, namespace, db_key] {
			self.release(reg);
		}
		Ok((ns, db, database))
	}

	/// Bump the table's catalog version (`bump_catalog_version`).
	fn stamp(&mut self, ns: Reg, db: Reg, tb: Operand) -> Lowered<()> {
		let (tb, loaded) = match tb {
			Operand::Reg(reg) => (reg, false),
			other => {
				let reg = self.register()?;
				self.emit(Instr::Load {
					dst: reg,
					src: other,
				});
				(reg, true)
			}
		};
		self.store(StoreInstr::StampCatalog {
			ns,
			db,
			tb,
			draw: 0,
			reserved: None,
		});
		if loaded {
			self.release(tb);
		}
		Ok(())
	}

	/// `DEFINE TABLE` — `ddl/define/table.rs`, for a table that is not a view
	/// and not a lightweight relation.
	async fn define_table(&mut self, stmt: &DefineTableStatement) -> Lowered<()> {
		use crate::catalog::TableType;
		let table_name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Table)).await?;
		let name_op = table_name.operand;
		// What the body refuses from the statement alone is raised where the
		// body raises it, once the permission check has passed.
		let mut statement_refusal = None;
		if let Err(e) = crate::ddl::define::validate_graphql_alias(&stmt.graphql_alias, "table") {
			statement_refusal = Some(refusal_of(&e, "a table definition")?);
		} else if [stmt.inline_edges_cap, stmt.inline_refs_cap]
			.into_iter()
			.any(|cap| cap.is_some_and(|cap| cap > crate::idx::inline_cache::MAX_INLINE_CACHE_CAP))
		{
			statement_refusal = Some(inline_cap_refusal());
		}
		// A view's definition is formed, and validated, from the statement;
		// one the body refuses is raised where the body raises it.
		let view =
			match stmt.view.as_ref().map(crate::catalog::view::view_to_definition).transpose() {
				Ok(view) => view,
				Err(e) => {
					statement_refusal.get_or_insert(refusal_of(&e, "a view")?);
					None
				}
			};
		// A view is filled by a plan of its table, so its name is known when
		// the statement is planned: written, or evaluated from what the plan
		// knows already. A name that evaluates to no name raises its error
		// where the program evaluates it, before the view is filled.
		let mut name_refused = false;
		let planned_name = match &table_name.written {
			Some(written) => Some(written.clone()),
			None if stmt.view.is_some() => {
				match plan_time_name(self.planner, &stmt.name, NameClause::Table) {
					Some(Ok(name)) => Some(name),
					Some(Err(())) => {
						name_refused = true;
						Some(String::new())
					}
					None => None,
				}
			}
			None => None,
		};
		let tb = match &planned_name {
			Some(s) => crate::val::TableName::new(s.as_str()),
			None => crate::val::TableName::new(""),
		};
		// The definition as the catalog stores it, with the holes the program
		// fills: ids, cache stamps and the graph markers a redefinition keeps.
		let mut template = table_template(stmt, tb.clone(), view.clone());
		// What the body refuses of the definition's own clauses, as the
		// statement's mapping decided it ([`ddl_checks`]), and what it refuses
		// once it has read the table, judged against the table the statement
		// meets when it runs (`StoreInstr::RefuseRedefiningTable`).
		let DdlChecks::DefineTable {
			definition_refusal,
			table_type,
			redefined,
		} = self.ddl_checks()?
		else {
			return Err(NotCovered::internal("a DEFINE TABLE mapped with no checks"));
		};
		template.table_type = table_type;
		let template = stored(template.to_stored())?;

		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Table,
			base: Base::Db,
		});
		if let Some(refusal) = statement_refusal {
			self.store(StoreInstr::RefuseField {
				refusal,
			});
		}
		// A table is defined in a database the session reaches: a namespace
		// that does not exist is raised as itself.
		let (ns, db, database) = self.selected_ids(StatementError::NsNotFound)?;
		self.release(database);
		let name_reg = self.load(name_op)?;
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("tb", name_op)])?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			dst: existing,
		});
		let table_id = self.register()?;
		let [folded, doc_ids, inline_gen, old_edges, old_refs] = [
			self.register()?,
			self.register()?,
			self.register()?,
			self.register()?,
			self.register()?,
		];
		let missing =
			self.exists(&stmt.kind, existing, StatementError::TbAlreadyExists, name_reg)?;
		// A redefinition keeps the table's id and its graph markers.
		self.get_field(existing, "table_id", table_id);
		self.get_field(existing, "graph_folded", folded);
		self.get_field(existing, "graph_doc_ids", doc_ids);
		self.get_field(existing, "graph_inline_gen", inline_gen);
		self.get_field(existing, "inline_edges_cap", old_edges);
		self.get_field(existing, "inline_refs_cap", old_refs);
		let built = self.emit(Instr::Jump {
			target: 0,
		});
		let at = self.here();
		self.patch(missing, at);
		self.store(StoreInstr::NextId {
			sequence: IdSequence::Table,
			ns: Some(ns),
			db: Some(db),
			dst: table_id,
			draw: 0,
			reserved: None,
		});
		let (f, zero, none) = (
			self.constant(Value::Bool(false)),
			self.constant(Value::from(0)),
			self.constant(Value::None),
		);
		for (dst, value) in
			[(folded, f), (doc_ids, f), (inline_gen, zero), (old_edges, none), (old_refs, none)]
		{
			self.emit(Instr::Load {
				dst,
				src: value,
			});
		}
		let at = self.here();
		self.patch(built, at);

		let template = self.constant(template);
		let definition = self.load(template)?;
		let comment =
			crate::exec::vm::host::out_of_line(|| self.comment(&stmt.comment)).await?.operand;
		if let Some(refusal) = definition_refusal {
			self.store(StoreInstr::RefuseField {
				refusal,
			});
		}
		if let Some(definition) = redefined {
			self.store(StoreInstr::RefuseRedefiningTable {
				ns,
				db,
				definition,
			});
		}
		// A relation or a view has no record id components to name: a table
		// whose `id` names them is refused, whether its name is written or
		// evaluated.
		let conversion = match (&stmt.table_type, &stmt.view) {
			(TableType::Relation(_), _) => Some(TableConversion::Relation),
			(_, Some(_)) => Some(TableConversion::View),
			_ => None,
		};
		if let Some(to) = conversion {
			let table = self.load(name_op)?;
			self.store(StoreInstr::RefuseNamedIdConversion {
				ns,
				db,
				table,
				to,
			});
		}
		let stamp = self.register()?;
		self.store(StoreInstr::NewUuid {
			dst: stamp,
			draw: 0,
			reserved: None,
		});
		for (field, value) in [
			("namespace_id", Operand::Reg(ns)),
			("database_id", Operand::Reg(db)),
			("table_id", Operand::Reg(table_id)),
			("name", name_op),
			("comment", comment),
			("cache_fields_ts", Operand::Reg(stamp)),
			("cache_events_ts", Operand::Reg(stamp)),
			("cache_tables_ts", Operand::Reg(stamp)),
			("cache_indexes_ts", Operand::Reg(stamp)),
			("cache_lives_ts", Operand::Reg(stamp)),
			("graph_folded", Operand::Reg(folded)),
			("graph_doc_ids", Operand::Reg(doc_ids)),
			("graph_inline_gen", Operand::Reg(inline_gen)),
		] {
			self.set_field(definition, field, value);
		}

		// A relation's `in` and `out` are field definitions of their own.
		if let TableType::Relation(rel) = &stmt.table_type {
			for (field, tables) in [("in", &rel.from), ("out", &rel.to)] {
				let fd = crate::catalog::FieldDefinition {
					name: crate::expr::Idiom::field(field),
					table: tb.clone(),
					field_kind: Some(crate::expr::Kind::Record(tables.clone())),
					..Default::default()
				};
				let value = stored(fd.to_stored())?;
				let value = self.constant(value);
				let value = self.load(value)?;
				let fd = self.constant(Value::String(field.into()));
				let key = self.object(&[
					("ns", Operand::Reg(ns)),
					("db", Operand::Reg(db)),
					("tb", name_op),
					("fd", fd),
				])?;
				self.store(StoreInstr::Put {
					route: RouteId::named("FieldKey"),
					key,
					ty: TypeId::named("StoredFieldDefinition"),
					value,
				});
				self.release(key);
				self.release(value);
				self.stamp(ns, db, name_op)?;
			}
			let fields_ts = self.register()?;
			self.store(StoreInstr::NewUuid {
				dst: fields_ts,
				draw: 0,
				reserved: None,
			});
			self.set_field(definition, "cache_fields_ts", Operand::Reg(fields_ts));
		}

		// The definition change is recorded, and the database's changefeed
		// fence moves, in the same transaction.
		if stmt.changefeed.is_some() {
			self.store(StoreInstr::BufferTableChange {
				ns,
				db,
				tb: name_reg,
				definition,
			});
			let fence = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))])?;
			self.store(StoreInstr::Increment {
				route: RouteId::named("ChangefeedFenceKey"),
				key: fence,
			});
		}

		// `replace_tb`: a locked read serialises this write against a
		// concurrent fold's marker flip, then the write and its stamp.
		self.store(StoreInstr::Lock {
			route: RouteId::named("TableKey"),
			key,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			value: definition,
		});
		self.stamp(ns, db, name_op)?;

		// A cache built under one cap regime is not trusted under another.
		for (old, new, route) in [
			(old_edges, stmt.inline_edges_cap, "EdgeCachePrefix"),
			(old_refs, stmt.inline_refs_cap, "RefCachePrefix"),
		] {
			let new = self.constant(match new {
				Some(cap) => Value::from(cap as i64),
				None => Value::None,
			});
			let same = self.register()?;
			self.emit(Instr::Compare {
				op: crate::exec::vm::program::CompareOp::Equal,
				left: Operand::Reg(old),
				right: new,
				dst: same,
			});
			let skip = self.emit(Instr::JumpIf {
				cond: Operand::Reg(same),
				truthy: true,
				target: 0,
			});
			let prefix = self.object(&[
				("ns", Operand::Reg(ns)),
				("db", Operand::Reg(db)),
				("tb", name_op),
			])?;
			self.store(StoreInstr::DeleteRange {
				route: RouteId::named(route),
				key: prefix,
			});
			let at = self.here();
			self.patch(skip, at);
			self.release(same);
			self.release(prefix);
		}
		self.store(StoreInstr::ClearCache);
		// A view's table is emptied of what it held and every subscription on
		// it killed; each table it reads records the view and refreshes its
		// views' stamp; then the view is filled from them.
		if let Some(view) = view.filter(|_| !name_refused) {
			let view_table = planned_name.is_some().then(|| tb.clone());
			let tb = name_reg;
			self.with_service(Service::Broker, |l| l.kill_table_subscriptions(ns, db, tb))?;
			let root = self.object(&[
				("ns", Operand::Reg(ns)),
				("db", Operand::Reg(db)),
				("tb", name_op),
			])?;
			self.store(StoreInstr::DeleteRange {
				route: RouteId::named("TblRoot"),
				key: root,
			});
			self.release(root);
			// The range holds the table's layout descriptors, so the delete removes the
			// generation the stamp above minted. The stamp that follows mints one for the rows
			// the backfill writes, and the committed catalog version names the state that holds
			// it.
			self.stamp(ns, db, name_op)?;
			for source in view.source_tables() {
				let ft = self.constant(Value::String(source.as_str().into()));
				let foreign = self.object(&[
					("ns", Operand::Reg(ns)),
					("db", Operand::Reg(db)),
					("tb", ft),
					("ft", name_op),
				])?;
				self.store(StoreInstr::Put {
					route: RouteId::named("ForeignTableKey"),
					key: foreign,
					ty: TypeId::named("StoredTableDefinition"),
					value: definition,
				});
				self.release(foreign);
				let ft_key =
					self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("tb", ft)])?;
				let ft_name = self.load(ft)?;
				let source_table = self.expect_table(ft_key, ft_name)?;
				self.release(ft_name);
				self.replace_table((ns, db, ft), ft_key, source_table, "cache_tables_ts")?;
				self.store(StoreInstr::ClearCache);
				self.release(source_table);
				self.release(ft_key);
			}
			crate::exec::vm::host::out_of_line(|| {
				self.backfill_view(&view, view_table.as_ref(), tb)
			})
			.await?;
			self.release(tb);
		}
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// Fill the view table whose name is in `name`, `table` where the plan
	/// knows it, from the tables `view` reads: a select over them computes the
	/// rows (or, for an aggregated view, the groups) the view holds, and each
	/// is stored under its key and run through the view table's own order. A
	/// speculative run leaves the backfill to the statement's real run.
	async fn backfill_view(
		&mut self,
		view: &crate::catalog::ViewDefinition,
		table: Option<&crate::val::TableName>,
		name: Reg,
	) -> Lowered<()> {
		use crate::catalog::ViewDefinition;
		use crate::catalog::aggregation::AggregateFields;
		// The view was validated as it was defined, so its backfill forms.
		let not_covered = |why: anyhow::Error| NotCovered::internal(why.to_string());
		let (select, aggregated) = match view {
			ViewDefinition::Select {
				..
			} => return Ok(()),
			ViewDefinition::Materialized {
				fields,
				tables,
				condition,
				..
			} => (
				crate::catalog::view::materialized_backfill_select(
					fields,
					tables,
					condition.as_ref(),
				),
				None,
			),
			ViewDefinition::Aggregated {
				analysis,
				tables,
				condition,
				..
			} => {
				let AggregateFields::Fields(items) = &analysis.fields else {
					return Err(NotCovered::internal("a view that selects a value"));
				};
				let values = analysis.backfill_values().map_err(not_covered)?;
				let select = analysis
					.backfill_select(&values, tables, condition.as_ref())
					.map_err(not_covered)?;
				let backfill = Backfill {
					aggregations: analysis.aggregations.clone(),
					values,
					groups: analysis.group_expressions.len(),
				};
				// Each field is evaluated over the document the group's keys and
				// aggregates make.
				let mut fields = Vec::with_capacity(items.len());
				for (path, expr) in items {
					let logical = crate::expr::logical::lower_expr(expr.clone())
						.map_err(|_| NotCovered::internal("a view field with no logical form"))?;
					fields.push((
						path.clone(),
						crate::exec::vm::host::out_of_line(|| self.host_code(&logical)).await?,
					));
				}
				(select, Some((backfill, fields)))
			}
		};
		let speculating = self.store(StoreInstr::JumpIfSpeculating {
			target: 0,
		});
		let op = crate::expr::logical::lower_select(select)
			.map_err(|_| NotCovered::internal("a view's select with no logical form"))?;
		let rows = self
			.once_value(&crate::expr::logical::LogicalExpr::Subquery(Box::new(op)), false)
			.await?;
		let order = match table {
			Some(table) => {
				Some(crate::exec::vm::host::out_of_line(|| self.view_order(table)).await?)
			}
			None => None,
		};
		self.each(rows, |l, row| {
			match aggregated {
				None => {
					l.store(StoreInstr::BackfillRow {
						table: name,
						row,
						order,
					});
				}
				Some((backfill, fields)) => {
					let document = l.register()?;
					let empty = l.store(StoreInstr::BackfillGroup {
						backfill: backfill.clone(),
						row,
						dst: document,
						empty: 0,
					});
					let mut values = Vec::with_capacity(fields.len());
					for (path, pipeline) in fields {
						let dst = l.register()?;
						l.store(StoreInstr::EvaluateOver {
							pipeline,
							row: document,
							dst,
						});
						values.push((path, dst));
					}
					l.store(StoreInstr::BackfillGroupRow {
						backfill,
						table: name,
						row,
						fields: values.clone(),
						order,
					});
					let end = l.here();
					l.patch(empty, end);
					for (_, dst) in values {
						l.release(dst);
					}
					l.release(document);
				}
			}
			Ok(())
		})?;
		self.release(rows);
		let end = self.here();
		self.patch(speculating, end);
		Ok(())
	}

	/// The order of view table `table`'s rows, lowered from the view table's
	/// definitions as a unit of the program: what runs over each row the
	/// backfill stores.
	async fn view_order(&mut self, table: &crate::val::TableName) -> Lowered<UnitId> {
		use crate::exec::vm::program::{UnitHead, UnitSig};
		let mut program =
			Box::pin(lower_view_rows(self.planner, true, table.clone(), None, &self.catalog))
				.await?;
		self.delegate_context = self.delegate_context.max(program.required_context);
		program.unit = Some(UnitHead {
			sig: UnitSig {
				params: Vec::new(),
				takes_row: false,
			},
			frame: Self::statement_frame(self.planner),
			entry: None,
			closure: None,
		});
		let id = u32::try_from(self.units.len())
			.map_err(|_| NotCovered::internal("more units than a program holds"))?;
		self.units.push(Arc::new(program));
		Ok(UnitId(id))
	}
}

impl Lowering<'_, '_> {
	/// `DEFINE FIELD` — `ddl/define/field.rs`, for a field whose name is
	/// written.
	///
	/// The body's checks of the definition read the table, its fields and its
	/// indexes. The statement runs against the catalog it is lowered against
	/// (`lower_block` declines a block that would change those between the
	/// two), so the checks are made here, and a refusal is an instruction
	/// raising it where the body raises it.
	async fn define_field(&mut self, stmt: &DefineFieldStatement) -> Lowered<()> {
		use crate::catalog::TableType;
		// A field, table or comment that is evaluated is resolved as the
		// program runs, and the definition is checked against the catalog it
		// finds then.
		let written_name = match &stmt.name {
			Expr::Idiom(name) => canonical_field_name(name),
			_ => None,
		};
		// A late-bound statement is checked against the catalog it meets when
		// it runs, which no check made now can know.
		let (Some(name), Some(table), Some(comment), false) = (
			written_name,
			written_text(&stmt.what),
			written_comment(&stmt.comment),
			self.planner.is_late_bound(),
		) else {
			return crate::exec::vm::host::out_of_line(|| self.define_field_resolved(stmt)).await;
		};
		let table = crate::val::TableName::new(table);
		// The body's checks of the definition, made against the catalog by
		// the statement's mapping ([`ddl_checks`]).
		let DdlChecks::DefineField {
			existing_table,
			existing_field,
			fields,
			indexed,
			refused,
			flexible,
		} = self.ddl_checks()?
		else {
			// A database that does not exist yet has no catalog to check the
			// definition against before it runs: the statement creates it.
			return crate::exec::vm::host::out_of_line(|| self.define_field_resolved(stmt)).await;
		};
		let fd = name.to_raw_string();
		// What the body refuses of an `INLINE` field, raised where the body
		// raises it: after the refusal of a field on a lightweight relation,
		// which the program makes first, and not on an import.
		let mut inline_refusal = None;
		let lightweight_table = existing_table.as_ref().is_some_and(|tb| {
			crate::kvs::lightweight::lightweight_relation(&tb.table_type).is_some()
		});
		if stmt.inline && !lightweight_table {
			let others = fields.iter().filter(|f| f.inline && f.name != name).count();
			inline_refusal = inline_field_refusal(
				existing_table.as_deref(),
				(fd.as_str(), name.len(), stmt.computed.is_some()),
				others,
			);
		}
		let definition = field_definition(stmt, &name, &table, &comment);
		// What the body refuses of a field nested in something that cannot
		// hold it, judged against the fields the definition leaves.
		let table_fields: &[crate::catalog::FieldDefinition] = &fields;
		let nesting_refusal = match crate::exec::field_rules::define_field_nesting_restrictions(
			table_fields,
			&definition,
		) {
			Ok(()) => None,
			Err(e) => Some(FieldRefusal::of(&e).ok_or_else(|| {
				// Reached only on a storage fault.
				NotCovered::internal("a field definition whose nesting did not judge".to_owned())
			})?),
		};
		// A record kind for a relation's `in` or `out` that is not the one the
		// table names retargets the relation: the table is written with it,
		// and nothing else the field's kind implies is derived.
		let mut retarget = None;
		// A kind that is no record for a relation's `in` or `out` is raised
		// once the definition is found not to exist already, as the body
		// raises it.
		let mut relation_refusal = None;
		if matches!(fd.as_str(), "in" | "out")
			&& let Some(existing) = existing_table.as_ref()
			&& let TableType::Relation(ref relation) = existing.table_type
			&& let Some(kind) = &stmt.field_kind
		{
			if let crate::expr::Kind::Record(field_kind) = kind {
				let current = if fd == "in" {
					&relation.from
				} else {
					&relation.to
				};
				if field_kind != current {
					let mut relation = relation.clone();
					if fd == "in" {
						relation.from.clone_from(field_kind);
					} else {
						relation.to.clone_from(field_kind);
					}
					retarget =
						Some(TableType::Relation(relation).into_vm_value().map_err(|e| {
							NotCovered::internal(format!(
								"table type the codec does not convert: {e}"
							))
						})?);
				}
			} else {
				relation_refusal = Some(FieldRefusal::Thrown(format!(
					"{fd} field on a relation must be a record"
				)));
			}
		}
		// The sub-field definitions an array or set kind implies, as the body
		// derives them from the fields already defined, and the ones the kind
		// it replaces implied that go with it.
		let mut nested = Vec::new();
		let mut dropped = Vec::new();
		if retarget.is_none() {
			let implied =
				crate::exec::field_rules::implied_definitions(&name, stmt.field_kind.as_ref());
			for (path, kind) in implied {
				let existing = table_fields.iter().find(|f| f.name == path);
				let sub =
					crate::exec::field_rules::implied_definition(&definition, existing, path, kind);
				nested.push((surrealdb_types::ToSql::to_sql(&sub.name), stored(sub.to_stored())?));
			}
			dropped = crate::exec::field_rules::implied_definitions_dropped(
				table_fields,
				existing_field.as_deref(),
				&definition,
			);
		}
		let template = stored(definition.to_stored())?;
		let template = self.constant(template);
		// A refusal raised whatever the statement meets leaves nothing after it
		// to raise.
		let refused_before = indexed.is_some()
			|| refused.is_some()
			|| flexible.is_some()
			|| relation_refusal.is_some();

		self.store(StoreInstr::RefuseNoWrite);
		// A computed field an index of the table covers is refused as its
		// definition is formed, before the permission check.
		if let Some(refusal) = indexed {
			self.store(StoreInstr::RefuseField {
				refusal,
			});
		}
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Field,
			base: Base::Db,
		});
		let (ns, db, database) = self.session_ids_and_database()?;
		if let Some(refusal) = refused {
			self.store(StoreInstr::RefuseField {
				refusal,
			});
		}
		// The record id components an `id` names, judged against the catalog
		// and the rows of the table's views as the statements before this one
		// leave them.
		if name.is_id() {
			let field = self.load(template)?;
			self.store(StoreInstr::FieldCheck {
				check: FieldCheck::KeyComponents {
					field,
					ns,
					db,
					kind: stmt.kind.clone(),
				},
			});
			self.release(field);
		}
		if let Some(refusal) = flexible {
			self.store(StoreInstr::RefuseField {
				refusal,
			});
		}
		let table_name = self.constant(Value::String(table.as_str().into()));
		let tb_key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("tb", table_name)])?;
		let tb = self.table_creating((ns, db, database), table_name, tb_key)?;
		self.store(StoreInstr::RefuseLightweight {
			table: tb,
			refusal: StoreRefusal::Field,
		});
		self.release(database);
		if let Some(refusal) = inline_refusal {
			let import = self.store(StoreInstr::JumpIfImport {
				target: 0,
			});
			self.store(StoreInstr::RefuseField {
				refusal,
			});
			let at = self.here();
			self.patch(import, at);
		}

		let fd = self.constant(Value::String(fd.into()));
		let fd_key = self.object(&[
			("ns", Operand::Reg(ns)),
			("db", Operand::Reg(db)),
			("tb", table_name),
			("fd", fd),
		])?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("FieldKey"),
			key: fd_key,
			ty: TypeId::named("StoredFieldDefinition"),
			dst: existing,
		});
		let shown = self.constant(Value::String(surrealdb_types::ToSql::to_sql(&name).into()));
		let shown = self.load(shown)?;
		let missing = self.exists(&stmt.kind, existing, StatementError::FdAlreadyExists, shown)?;
		let at = self.here();
		self.patch(missing, at);
		if let Some(refusal) = relation_refusal {
			self.store(StoreInstr::RefuseField {
				refusal,
			});
		}
		// Raised where the body raises it, and not on an import, which replays
		// definitions written before the rule held.
		if let Some(refusal) = nesting_refusal.filter(|_| !refused_before) {
			let import = self.store(StoreInstr::JumpIfImport {
				target: 0,
			});
			self.store(StoreInstr::RefuseField {
				refusal,
			});
			let at = self.here();
			self.patch(import, at);
		}
		// A record id that names its components must admit every record the
		// table holds, once the statement is known to define the field; the
		// scan is skipped when the stored `id` already has the same structure.
		if let Some(key) = definition.live_key() {
			let table_reg = self.load(table_name)?;
			self.store(StoreInstr::ValidateRecordKeys {
				ns,
				db,
				table: table_reg,
				key: Arc::clone(key),
			});
		}
		let field = self.load(template)?;
		let limit = self.register()?;
		self.store(StoreInstr::SessionAuthLimit {
			dst: limit,
		});
		self.set_field(field, "auth_limit", Operand::Reg(limit));
		self.store(StoreInstr::Put {
			route: RouteId::named("FieldKey"),
			key: fd_key,
			ty: TypeId::named("StoredFieldDefinition"),
			value: field,
		});
		// Overwriting a reference field can strand the reference keys it wrote
		// under target tables it no longer references; an import restores the
		// keys verbatim.
		if matches!(stmt.kind, DefineKind::Overwrite) {
			let import = self.store(StoreInstr::JumpIfImport {
				target: 0,
			});
			let table_reg = self.load(table_name)?;
			self.store(StoreInstr::PurgeDroppedReferences {
				ns,
				db,
				table: table_reg,
				old: existing,
				new: Some(field),
			});
			let at = self.here();
			self.patch(import, at);
		}
		self.stamp(ns, db, table_name)?;
		// `replace_tb` with the fields' cache stamp refreshed.
		let fields_ts = self.register()?;
		self.store(StoreInstr::NewUuid {
			dst: fields_ts,
			draw: 0,
			reserved: None,
		});
		self.set_field(tb, "cache_fields_ts", Operand::Reg(fields_ts));
		if let Some(table_type) = retarget.as_ref() {
			let table_type = self.constant(table_type.clone());
			self.set_field(tb, "table_type", table_type);
		}
		let was_inline = existing_field.as_ref().is_some_and(|f| f.inline);
		let inline_toggled = stmt.inline != was_inline;
		let inline_kind_changed = stmt.inline
			&& was_inline
			&& existing_field.as_ref().is_some_and(|f| f.field_kind != stmt.field_kind);
		if retarget.is_none()
			&& (inline_toggled || inline_kind_changed)
			&& let Some(et) = &existing_table
		{
			let next_gen = et.graph_inline_gen.wrapping_add(1);
			let gen_val = self.constant(Value::from(next_gen));
			self.set_field(tb, "graph_inline_gen", gen_val);
		}
		// Nested definitions are written before the table's rewrite and its stamp, so the stamp
		// settles the field set the statement leaves (as the host path does).
		for (path, value) in nested {
			let path = self.constant(Value::String(path.into()));
			let key = self.object(&[
				("ns", Operand::Reg(ns)),
				("db", Operand::Reg(db)),
				("tb", table_name),
				("fd", path),
			])?;
			let value = self.constant(value);
			let value = self.load(value)?;
			self.store(StoreInstr::Put {
				route: RouteId::named("FieldKey"),
				key,
				ty: TypeId::named("StoredFieldDefinition"),
				value,
			});
			self.release(key);
			self.release(value);
		}
		// An import restores the reference keys of the ones dropped verbatim.
		self.delete_field_definitions((ns, db, table_name), &dropped, true)?;
		self.store(StoreInstr::Lock {
			route: RouteId::named("TableKey"),
			key: tb_key,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("TableKey"),
			key: tb_key,
			ty: TypeId::named("StoredTableDefinition"),
			value: tb,
		});
		self.stamp(ns, db, table_name)?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// Delete the definitions of the fields `dropped` names on the table named
	/// by `tb` in database `db` of namespace `ns`, each under every key it can
	/// be stored under (`field_key_spellings`), and the reference keys each
	/// wrote — which an import keeps, when `import_restores` says it restores
	/// them verbatim.
	fn delete_field_definitions(
		&mut self,
		(ns, db, tb): (Reg, Reg, Operand),
		dropped: &[crate::expr::Idiom],
		import_restores: bool,
	) -> Lowered<()> {
		for name in dropped {
			for spelling in crate::ddl::define::field::field_key_spellings(name) {
				let fd = self.constant(Value::String(spelling.into()));
				let key = self.object(&[
					("ns", Operand::Reg(ns)),
					("db", Operand::Reg(db)),
					("tb", tb),
					("fd", fd),
				])?;
				let old = self.register()?;
				self.store(StoreInstr::Get {
					route: RouteId::named("FieldKey"),
					key,
					ty: TypeId::named("StoredFieldDefinition"),
					dst: old,
				});
				self.store(StoreInstr::Delete {
					route: RouteId::named("FieldKey"),
					key,
				});
				let import = import_restores.then(|| {
					self.store(StoreInstr::JumpIfImport {
						target: 0,
					})
				});
				let table = self.load(tb)?;
				self.store(StoreInstr::PurgeDroppedReferences {
					ns,
					db,
					table,
					old,
					new: None,
				});
				if let Some(import) = import {
					let at = self.here();
					self.patch(import, at);
				}
				self.release(key);
				self.release(old);
				self.release(table);
			}
		}
		Ok(())
	}

	/// Stop answering `NONE` when `IF EXISTS` and the definition in `found`
	/// is missing; raise `error` naming `name` when it is missing otherwise.
	fn must_exist(
		&mut self,
		if_exists: bool,
		found: Reg,
		error: StatementError,
		name: Reg,
	) -> Lowered<()> {
		let present = self.emit(Instr::JumpIf {
			cond: Operand::Reg(found),
			truthy: true,
			target: 0,
		});
		if if_exists {
			self.answer_none()?;
		} else {
			self.store(StoreInstr::Raise {
				error,
				arg: name,
			});
		}
		let at = self.here();
		self.patch(present, at);
		Ok(())
	}

	/// `REMOVE CONFIG` — `ddl/remove/config.rs`.
	fn remove_config(&mut self, stmt: &RemoveConfigStatement) -> Lowered<()> {
		use crate::iam::ConfigKind;
		let base = crate::ddl::define::config::config_kind_base(&stmt.kind);
		let cg = match &stmt.kind {
			ConfigKind::GraphQL => "graphql",
			ConfigKind::Api => "api",
			ConfigKind::Default => "default",
		};
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Config(stmt.kind.clone()),
			base: base.clone().into(),
		});
		let ty = self.constant(Value::String(cg.into()));
		let (route, key) = match base {
			crate::catalog::base::Base::Root => ("RootConfigKey", self.object(&[("ty", ty)])?),
			crate::catalog::base::Base::Db => {
				let (ns, db) = self.session_ids()?;
				(
					"DbConfigKey",
					self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("ty", ty)])?,
				)
			}
			crate::catalog::base::Base::Ns => {
				return Err(NotCovered::internal("a namespace config"));
			}
		};
		let found = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredConfigDefinition"),
			dst: found,
		});
		let name = self.load(ty)?;
		self.must_exist(stmt.if_exists, found, StatementError::CgNotFound, name)?;
		self.store(StoreInstr::Delete {
			route: RouteId::named(route),
			key,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// Run `body` over each element of the array in `list`, the element in the
	/// register `body` is given.
	fn each(&mut self, list: Reg, body: impl FnOnce(&mut Self, Reg) -> Lowered<()>) -> Lowered<()> {
		let zero = self.constant(Value::from(0i64));
		let at = self.load(zero)?;
		let item = self.register()?;
		let head = self.emit(Instr::Next {
			list,
			at,
			dst: item,
			done: 0,
		});
		body(self, item)?;
		self.emit(Instr::Jump {
			target: head,
		});
		let done = self.here();
		self.patch(head, done);
		self.release(item);
		self.release(at);
		Ok(())
	}

	/// A register holding the values stored beneath the `route` bound over
	/// `fields`, decoded as `ty`.
	fn scan(
		&mut self,
		route: &'static str,
		fields: &[(&str, Operand)],
		ty: &'static str,
	) -> Lowered<Reg> {
		let key = self.object(fields)?;
		let dst = self.register()?;
		self.store(StoreInstr::Scan {
			route: RouteId::named(route),
			key,
			ty: TypeId::named(ty),
			dst,
			unread: None,
		});
		self.release(key);
		Ok(dst)
	}

	/// Delete the key, and under `EXPUNGE` every earlier version of it.
	fn delete(&mut self, route: &'static str, key: Reg, expunge: bool) {
		self.store(match expunge {
			true => StoreInstr::Clear {
				route: RouteId::named(route),
				key,
			},
			false => StoreInstr::Delete {
				route: RouteId::named(route),
				key,
			},
		});
	}

	/// Delete every key beneath the bound, and under `EXPUNGE` every earlier
	/// version of each.
	fn delete_range(&mut self, route: &'static str, key: Reg, expunge: bool) {
		self.store(match expunge {
			true => StoreInstr::ClearRange {
				route: RouteId::named(route),
				key,
			},
			false => StoreInstr::DeleteRange {
				route: RouteId::named(route),
				key,
			},
		});
	}

	/// The databases of the namespace whose id is in `ns`.
	fn databases(&mut self, ns: Reg) -> Lowered<Reg> {
		self.scan("DatabasePrefix", &[("ns", Operand::Reg(ns))], "DatabaseDefinition")
	}

	/// Run `body` over each database of the namespace in `ns`, given the
	/// database's id.
	fn each_database(
		&mut self,
		ns: Reg,
		body: impl FnOnce(&mut Self, Reg) -> Lowered<()>,
	) -> Lowered<()> {
		let databases = self.databases(ns)?;
		self.each(databases, |l, database| {
			let db = l.register()?;
			l.get_field(database, "database_id", db);
			body(l, db)?;
			l.release(db);
			Ok(())
		})?;
		self.release(databases);
		Ok(())
	}

	/// The tables of the database in `ns`/`db`.
	fn tables(&mut self, ns: Reg, db: Reg) -> Lowered<Reg> {
		self.scan(
			"TablePrefix",
			&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))],
			"StoredTableDefinition",
		)
	}

	/// Run `body` over each table of the database in `ns`/`db`, given the
	/// table's definition and its name.
	fn each_table(
		&mut self,
		ns: Reg,
		db: Reg,
		body: impl FnOnce(&mut Self, Reg, Reg) -> Lowered<()>,
	) -> Lowered<()> {
		let tables = self.tables(ns, db)?;
		self.each(tables, |l, table| {
			let tb = l.register()?;
			l.get_field(table, "name", tb);
			body(l, table, tb)?;
			l.release(tb);
			Ok(())
		})?;
		self.release(tables);
		Ok(())
	}

	/// Retire every index of every table of the database in `ns`/`db`
	/// (`retire_database_indexes`).
	fn retire_database_indexes(&mut self, ns: Reg, db: Reg, expunge: bool) -> Lowered<()> {
		self.each_table(ns, db, |l, table, tb| l.retire_table_indexes(ns, db, table, tb, expunge))
	}

	/// Retire every index of the table defined in `table`, named in `tb`
	/// (`retire_table_indexes`): the process's loaded state and local build,
	/// and the build's durable state and queues.
	fn retire_table_indexes(
		&mut self,
		ns: Reg,
		db: Reg,
		table: Reg,
		tb: Reg,
		expunge: bool,
	) -> Lowered<()> {
		let (ns_op, db_op, tb_op) = (Operand::Reg(ns), Operand::Reg(db), Operand::Reg(tb));
		let indexes = self.scan(
			"IndexDefPrefix",
			&[("ns", ns_op), ("db", db_op), ("tb", tb_op)],
			"StoredIndexDefinition",
		)?;
		self.each(indexes, |l, index| {
			l.store(StoreInstr::RetireIndexLocally {
				ns,
				db,
				table,
				index,
			});
			let ix = l.register()?;
			l.get_field(index, "index_id", ix);
			let key =
				l.object(&[("ns", ns_op), ("db", db_op), ("tb", tb_op), ("ix", Operand::Reg(ix))])?;
			l.delete("BuildStateKey", key, expunge);
			for route in [
				"BuildAppendIxPrefix",
				"BuildPrimaryIxPrefix",
				"BuildReservationIxPrefix",
				"BuildTicketIxPrefix",
			] {
				l.delete_range(route, key, expunge);
			}
			l.release(key);
			l.release(ix);
			Ok(())
		})?;
		self.release(indexes);
		Ok(())
	}

	/// Queue a `KILLED`, sent on commit, for every subscription on every table
	/// of the database in `ns`/`db` (`kill_database_subscriptions`).
	fn kill_database_subscriptions(&mut self, ns: Reg, db: Reg) -> Lowered<()> {
		self.each_table(ns, db, |l, _, tb| l.kill_table_subscriptions(ns, db, tb))
	}

	/// Queue a `KILLED`, sent on commit, for every subscription on the table
	/// named in `tb` (`kill_table_subscriptions`). Subscriptions that do not
	/// read are passed over.
	fn kill_table_subscriptions(&mut self, ns: Reg, db: Reg, tb: Reg) -> Lowered<()> {
		let key = self.object(&[
			("ns", Operand::Reg(ns)),
			("db", Operand::Reg(db)),
			("tb", Operand::Reg(tb)),
		])?;
		let lives = self.register()?;
		let scan = self.store(StoreInstr::Scan {
			route: RouteId::named("SubscriptionPrefix"),
			key,
			ty: TypeId::named("StoredSubscriptionDefinition"),
			dst: lives,
			unread: Some(0),
		});
		self.each(lives, |l, subscription| {
			l.store(StoreInstr::KillOnCommit {
				subscription,
			});
			Ok(())
		})?;
		let unread = self.here();
		self.patch(scan, unread);
		self.release(lives);
		self.release(key);
		Ok(())
	}

	/// Evict the sequence service's state for every sequence of the database
	/// in `ns`/`db` (`Sequences::database_removed`).
	fn evict_database_sequences(&mut self, ns: Reg, db: Reg) -> Lowered<()> {
		let sequences = self.scan(
			"SequencePrefix",
			&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))],
			"SequenceDefinition",
		)?;
		self.each(sequences, |l, sequence| {
			let name = l.register()?;
			l.get_field(sequence, "name", name);
			l.store(StoreInstr::EvictSequence {
				ns,
				db,
				name,
			});
			l.release(name);
			Ok(())
		})?;
		self.release(sequences);
		Ok(())
	}

	/// Skip what `body` emits when the datastore runs without `service`.
	fn with_service(
		&mut self,
		service: Service,
		body: impl FnOnce(&mut Self) -> Lowered<()>,
	) -> Lowered<()> {
		let absent = self.store(StoreInstr::JumpIfNoService {
			service,
			target: 0,
		});
		body(self)?;
		let at = self.here();
		self.patch(absent, at);
		Ok(())
	}

	/// Enqueue a reclaim of `kind`, the `ReclaimKind` discriminant its key
	/// codec writes, over the scope `ns`, `db`, `tb`, `ix`; a kind leaves the
	/// fields it does not use zeroed (`ReclaimKey::namespace` and the rest).
	fn enqueue_reclaim(&mut self, kind: i64, scope: [Operand; 4], expunge: bool) -> Lowered<()> {
		let [ns, db, tb, ix] = scope;
		let uid = self.register()?;
		self.store(StoreInstr::NewUuid {
			dst: uid,
			draw: 0,
			reserved: None,
		});
		let kind = self.constant(Value::from(kind));
		let expunge = self.constant(Value::from(expunge as i64));
		let key = self.object(&[
			("kind", kind),
			("ns", ns),
			("db", db),
			("tb", tb),
			("ix", ix),
			("expunge", expunge),
			("uid", Operand::Reg(uid)),
		])?;
		let zero = self.constant(Value::from(0i64));
		let none = self.constant(Value::None);
		let state = self.object(&[("observed_ms", zero), ("cursor", none)])?;
		self.store(StoreInstr::Put {
			route: RouteId::named("ReclaimKey"),
			key,
			ty: TypeId::named("ReclaimState"),
			value: state,
		});
		for reg in [uid, key, state] {
			self.release(reg);
		}
		Ok(())
	}

	/// The operands of a reclaim scope's unused fields: id `0` and the empty
	/// table name.
	fn unused_scope(&mut self) -> (Operand, Operand) {
		(self.constant(Value::from(0i64)), self.constant(Value::String("".into())))
	}

	/// `REMOVE NAMESPACE` — `ddl/remove/namespace.rs`.
	async fn remove_namespace(&mut self, stmt: &RemoveNamespaceStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Namespace,
			base: Base::Root,
		});
		let name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Namespace))
				.await?;
		let name = name.operand;
		let name_reg = self.load(name)?;
		let key = self.object(&[("ns", name)])?;
		let namespace = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: namespace,
		});
		self.must_exist(stmt.if_exists, namespace, StatementError::NsNotFound, name_reg)?;
		let ns = self.register()?;
		self.get_field(namespace, "namespace_id", ns);
		let expunge = stmt.expunge;
		self.each_database(ns, |l, db| l.retire_database_indexes(ns, db, expunge))?;
		self.with_service(Service::Broker, |l| {
			l.each_database(ns, |l, db| l.kill_database_subscriptions(ns, db))
		})?;
		self.with_service(Service::Sequences, |l| {
			l.each_database(ns, |l, db| l.evict_database_sequences(ns, db))
		})?;
		self.delete("NamespaceKey", key, expunge);
		let (zero, empty) = self.unused_scope();
		self.enqueue_reclaim(0, [Operand::Reg(ns), zero, empty, zero], expunge)?;
		self.store(StoreInstr::ClearDatastoreCache);
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE DATABASE` — `ddl/remove/database.rs`.
	async fn remove_database(&mut self, stmt: &RemoveDatabaseStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Database,
			base: Base::Ns,
		});
		let name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Database))
				.await?;
		let name = name.operand;
		let name_reg = self.load(name)?;
		let ns_name = self.register()?;
		self.store(StoreInstr::SessionNs {
			dst: ns_name,
		});
		let ns_key = self.object(&[("ns", Operand::Reg(ns_name))])?;
		let namespace = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key: ns_key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: namespace,
		});
		// A database of a namespace that does not exist is missing.
		let no_namespace = self.emit(Instr::JumpIf {
			cond: Operand::Reg(namespace),
			truthy: false,
			target: 0,
		});
		let ns = self.register()?;
		self.get_field(namespace, "namespace_id", ns);
		for reg in [ns_name, ns_key, namespace] {
			self.release(reg);
		}
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", name)])?;
		let database = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("DatabaseKey"),
			key,
			ty: TypeId::named("DatabaseDefinition"),
			dst: database,
		});
		let found = self.emit(Instr::JumpIf {
			cond: Operand::Reg(database),
			truthy: true,
			target: 0,
		});
		let missing = self.here();
		self.patch(no_namespace, missing);
		if stmt.if_exists {
			self.answer_none()?;
		} else {
			self.store(StoreInstr::Raise {
				error: StatementError::DbNotFound,
				arg: name_reg,
			});
		}
		let at = self.here();
		self.patch(found, at);
		let db = self.register()?;
		self.get_field(database, "database_id", db);
		let expunge = stmt.expunge;
		self.retire_database_indexes(ns, db, expunge)?;
		self.with_service(Service::Broker, |l| l.kill_database_subscriptions(ns, db))?;
		self.with_service(Service::Sequences, |l| l.evict_database_sequences(ns, db))?;
		self.delete("DatabaseKey", key, expunge);
		let (zero, empty) = self.unused_scope();
		self.enqueue_reclaim(1, [Operand::Reg(ns), Operand::Reg(db), empty, zero], expunge)?;
		self.store(StoreInstr::ClearDatastoreCache);
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// `REMOVE TABLE` — `ddl/remove/table.rs`, for a table that is not a view,
	/// not a LIGHTWEIGHT relation, and no LIGHTWEIGHT relation's endpoint.
	async fn remove_table(&mut self, stmt: &RemoveTableStatement) -> Lowered<()> {
		let table_name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Table)).await?;
		let name_op = table_name.operand;
		let DdlChecks::RemoveTable = self.ddl_checks()? else {
			return Err(NotCovered::internal("a REMOVE TABLE mapped with no checks"));
		};
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Table,
			base: Base::Db,
		});
		let (ns, db) = self.session_ids()?;
		let tb = self.load(name_op)?;
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("tb", name_op)])?;
		let table = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			dst: table,
		});
		self.must_exist(stmt.if_exists, table, StatementError::TbNotFound, tb)?;
		// The tables a view reads from, as its definition holds them when the
		// statement runs: a statement of the block may have defined the view.
		let sources = self.register()?;
		self.store(StoreInstr::ViewSources {
			ns,
			db,
			tb,
			dst: sources,
		});
		let views = self.scan(
			"ForeignTablePrefix",
			&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("tb", name_op)],
			"StoredTableDefinition",
		)?;
		let no_views = self.emit(Instr::JumpIf {
			cond: Operand::Reg(views),
			truthy: false,
			target: 0,
		});
		let detail = self.object(&[("table", name_op), ("views", Operand::Reg(views))])?;
		self.store(StoreInstr::Raise {
			error: StatementError::TbHasViews,
			arg: detail,
		});
		let at = self.here();
		self.patch(no_views, at);
		self.release(detail);
		self.release(views);
		// A non-empty relation using the table as an endpoint, before one that
		// is the table, refuses the removal, as the edges stand when it runs.
		self.store(StoreInstr::RefuseRemovingLightweight {
			ns,
			db,
			tb,
		});
		let expunge = stmt.expunge;
		self.retire_table_indexes(ns, db, table, tb, expunge)?;
		self.with_service(Service::Broker, |l| l.kill_table_subscriptions(ns, db, tb))?;
		self.delete("TableKey", key, expunge);
		self.stamp(ns, db, name_op)?;
		self.delete_range("TblRoot", key, expunge);
		self.each(sources, |l, source| {
			let ft = Operand::Reg(source);
			let foreign = l.object(&[
				("ns", Operand::Reg(ns)),
				("db", Operand::Reg(db)),
				("tb", ft),
				("ft", name_op),
			])?;
			l.delete("ForeignTableKey", foreign, expunge);
			l.release(foreign);
			let ft_key =
				l.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("tb", ft)])?;
			let ft_name = l.load(ft)?;
			let source_table = l.expect_table(ft_key, ft_name)?;
			l.release(ft_name);
			l.replace_table((ns, db, ft), ft_key, source_table, "cache_tables_ts")?;
			l.store(StoreInstr::ClearCache);
			l.release(source_table);
			l.release(ft_key);
			Ok(())
		})?;
		self.release(sources);
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// `replace_tb` of the table defined in `table`, at `key`, with its
	/// `stamp_field` cache stamp refreshed: a locked read serialises the write
	/// against a concurrent one, then the write and the catalog version.
	fn replace_table(
		&mut self,
		(ns, db, tb): (Reg, Reg, Operand),
		key: Reg,
		table: Reg,
		stamp_field: &str,
	) -> Lowered<()> {
		let refreshed = self.register()?;
		self.store(StoreInstr::NewUuid {
			dst: refreshed,
			draw: 0,
			reserved: None,
		});
		self.set_field(table, stamp_field, Operand::Reg(refreshed));
		self.release(refreshed);
		self.store(StoreInstr::Lock {
			route: RouteId::named("TableKey"),
			key,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			value: table,
		});
		self.stamp(ns, db, tb)
	}

	/// The session's database's definition, in namespace `ns`.
	fn database_definition(&mut self, ns: Reg) -> Lowered<Reg> {
		let name = self.register()?;
		self.store(StoreInstr::SessionDb {
			dst: name,
		});
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(name))])?;
		let database = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("DatabaseKey"),
			key,
			ty: TypeId::named("DatabaseDefinition"),
			dst: database,
		});
		self.release(key);
		self.release(name);
		Ok(database)
	}

	/// The table defined at `key`, named `table`, created when it does not
	/// exist yet (`get_or_add_tb`): refused in a `STRICT` database, whose
	/// definition is in `database`, and otherwise defined with the defaults a
	/// table takes, a fresh id and fresh cache stamps, and the table's catalog
	/// version bumped.
	fn table_creating(
		&mut self,
		(ns, db, database): (Reg, Reg, Reg),
		table: Operand,
		key: Reg,
	) -> Lowered<Reg> {
		let defined = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			dst: defined,
		});
		let present = self.emit(Instr::JumpIf {
			cond: Operand::Reg(defined),
			truthy: true,
			target: 0,
		});
		let strict = self.register()?;
		self.get_field(database, "strict", strict);
		let open = self.emit(Instr::JumpIf {
			cond: Operand::Reg(strict),
			truthy: false,
			target: 0,
		});
		let tb = table;
		let name = self.load(tb)?;
		self.store(StoreInstr::Raise {
			error: StatementError::TbNotFound,
			arg: name,
		});
		self.release(name);
		let at = self.here();
		self.patch(open, at);
		self.release(strict);
		let template = crate::catalog::StoredTableDefinition::new(
			crate::catalog::NamespaceId(0),
			crate::catalog::DatabaseId(0),
			crate::catalog::TableId(0),
			crate::val::TableName::new(""),
		);
		let template = stored(template)?;
		let template = self.constant(template);
		self.emit(Instr::Load {
			dst: defined,
			src: template,
		});
		let id = self.register()?;
		let id_drawn = self.store(StoreInstr::NextId {
			sequence: IdSequence::Table,
			ns: Some(ns),
			db: Some(db),
			dst: id,
			draw: 0,
			reserved: None,
		});
		let stamp = self.register()?;
		let stamp_drawn = self.store(StoreInstr::NewUuid {
			dst: stamp,
			draw: 0,
			reserved: None,
		});
		for (field, value) in [
			("namespace_id", Operand::Reg(ns)),
			("database_id", Operand::Reg(db)),
			("table_id", Operand::Reg(id)),
			("name", tb),
			("cache_fields_ts", Operand::Reg(stamp)),
			("cache_events_ts", Operand::Reg(stamp)),
			("cache_tables_ts", Operand::Reg(stamp)),
			("cache_indexes_ts", Operand::Reg(stamp)),
			("cache_lives_ts", Operand::Reg(stamp)),
		] {
			self.set_field(defined, field, value);
		}
		self.release(stamp);
		self.release(id);
		self.store(StoreInstr::Put {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			value: defined,
		});
		self.stamp(ns, db, tb)?;
		let version_drawn = self.here() - 1;
		self.share_table_definition(tb, [id_drawn, stamp_drawn, version_drawn]);
		let at = self.here();
		self.patch(present, at);
		Ok(defined)
	}

	/// Note table `table`'s definition, made by the draws at `drawn` — its id,
	/// its stamp, its catalog version — as one a write of the block may make
	/// first ([`crate::exec::vm::program::ImplicitTable`]), when the draws take what a
	/// speculative run of the statement reserved and the table is named as
	/// written.
	fn share_table_definition(&mut self, table: Operand, drawn: [Label; 3]) {
		let Operand::Const(at) = table else {
			return;
		};
		let table = match &self.consts[at as usize] {
			Value::String(name) => crate::val::TableName::from(name.as_str()),
			Value::Table(name) => name.clone(),
			_ => return,
		};
		let reserved = drawn.map(|label| match &self.code[label as usize] {
			Instr::Store(
				StoreInstr::NextId {
					reserved,
					..
				}
				| StoreInstr::NewUuid {
					reserved,
					..
				}
				| StoreInstr::StampCatalog {
					reserved,
					..
				},
			) => *reserved,
			_ => None,
		});
		if let [Some(id), Some(stamp), Some(version)] = reserved {
			self.implicit_tables.push(crate::exec::vm::program::ImplicitTable {
				table,
				id,
				stamp,
				version,
			});
		}
	}

	/// The table defined at `key`, which must exist.
	fn expect_table(&mut self, key: Reg, name: Reg) -> Lowered<Reg> {
		let table = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			dst: table,
		});
		self.expect(table, StatementError::TbNotFound, name);
		Ok(table)
	}

	/// `REMOVE EVENT` — `ddl/remove/event.rs`.
	async fn remove_event(&mut self, stmt: &RemoveEventStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Event,
			base: Base::Db,
		});
		self.session_selected(&[&stmt.table_name, &stmt.name])?;
		let tb =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.table_name, NameClause::Table))
				.await?
				.operand;
		let name = crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Event))
			.await?
			.operand;
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ev", name)])?;
		let event = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("EventKey"),
			key,
			ty: TypeId::named("StoredEventDefinition"),
			dst: event,
		});
		let name_reg = self.load(name)?;
		self.must_exist(stmt.if_exists, event, StatementError::EvNotFound, name_reg)?;
		// The key the event is removed at is the one its definition names.
		let (target, ev) = (self.register()?, self.register()?);
		self.get_field(event, "target_table", target);
		self.get_field(event, "name", ev);
		let stored_at = self.object(&[
			("ns", ns_op),
			("db", db_op),
			("tb", Operand::Reg(target)),
			("ev", Operand::Reg(ev)),
		])?;
		self.store(StoreInstr::Delete {
			route: RouteId::named("EventKey"),
			key: stored_at,
		});
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let tb_name = self.load(tb)?;
		let table = self.expect_table(tb_key, tb_name)?;
		self.replace_table((ns, db, tb), tb_key, table, "cache_events_ts")?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE FIELD` — `ddl/remove/field.rs`, for a field on a table that is
	/// not a LIGHTWEIGHT relation.
	async fn remove_field(&mut self, stmt: &RemoveFieldStatement) -> Lowered<()> {
		let written_name = match &stmt.name {
			Expr::Idiom(name) => canonical_field_name(name),
			_ => None,
		};
		let (Some(name), Some(table), false) =
			(written_name, written_text(&stmt.table_name), self.planner.is_late_bound())
		else {
			return crate::exec::vm::host::out_of_line(|| self.remove_field_resolved(stmt)).await;
		};
		let table = crate::val::TableName::new(table);
		let fd = name.to_raw_string();
		// The body refuses to remove a lightweight relation's field before it
		// looks for the field; the program raises that where the body does.
		let DdlChecks::Field {
			existing_table,
			existing_field,
			fields,
			key_refusal,
		} = self.ddl_checks()?
		else {
			return Err(NotCovered::internal("a REMOVE FIELD mapped with no checks"));
		};
		let lightweight_table = existing_table.as_ref().is_some_and(|existing| {
			crate::kvs::lightweight::lightweight_relation(&existing.table_type).is_some()
		});
		let was_inline = existing_field.as_ref().is_some_and(|f| f.inline);
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Field,
			base: Base::Db,
		});
		if lightweight_table {
			self.store(StoreInstr::RefuseField {
				refusal: FieldRefusal::Thrown(
					"a LIGHTWEIGHT relation's fields cannot be removed: its in/out contract is \
					 managed by the table definition"
						.to_owned(),
				),
			});
		}
		// A component of a named record id has no definition to remove, and the
		// `id` stays while a definition reads a component, as the catalog and
		// the rows of the table's views stand when the statement runs.
		if let Some(refusal) = key_refusal {
			self.store(StoreInstr::RefuseField {
				refusal,
			});
		}
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let tb = self.constant(Value::String(table.as_str().into()));
		if name.is_id() {
			let table_reg = self.load(tb)?;
			self.store(StoreInstr::FieldCheck {
				check: FieldCheck::IdUnread {
					ns,
					db,
					table: table_reg,
				},
			});
			self.release(table_reg);
		}
		let fd = self.constant(Value::String(fd.into()));
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("fd", fd)])?;
		let field = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("FieldKey"),
			key,
			ty: TypeId::named("StoredFieldDefinition"),
			dst: field,
		});
		let name_reg = self.load(fd)?;
		self.must_exist(stmt.if_exists, field, StatementError::FdNotFound, name_reg)?;
		// `del_tb_field`.
		self.store(StoreInstr::Delete {
			route: RouteId::named("FieldKey"),
			key,
		});
		// The keys a reference field wrote under its target tables are not
		// the definition's to remove; a removed field leaves none behind,
		// whether or not the statement is an import.
		let table_reg = self.load(tb)?;
		self.store(StoreInstr::PurgeDroppedReferences {
			ns,
			db,
			table: table_reg,
			old: field,
			new: None,
		});
		// Every field nested in the removed one goes with it, and so do the
		// keys any of them wrote.
		let mut nested: Vec<crate::expr::Idiom> = Vec::new();
		for fd in fields.iter() {
			if fd.name.len() > name.len()
				&& fd.name.starts_with(&name)
				&& !nested.contains(&fd.name)
			{
				nested.push(fd.name.clone());
			}
		}
		self.delete_field_definitions((ns, db, tb), &nested, false)?;
		self.stamp(ns, db, tb)?;
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let tb_name = self.load(tb)?;
		let table_def = self.expect_table(tb_key, tb_name)?;
		if was_inline && let Some(existing) = &existing_table {
			let next_gen = existing.graph_inline_gen.wrapping_add(1);
			let gen_val = self.constant(Value::from(next_gen));
			self.set_field(table_def, "graph_inline_gen", gen_val);
		}
		self.replace_table((ns, db, tb), tb_key, table_def, "cache_fields_ts")?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `DEFINE INDEX` — `ddl/define/index.rs`, on a table that exists and is
	/// not a LIGHTWEIGHT relation, over columns written as paths. The catalog
	/// work is the program's: the existence check, each column's check against
	/// the table's fields, the replaced index's retirement, the definition and
	/// its id's name lookup, the table's refreshed stamp. The build is the
	/// index builder's (`BuildIndex`), waited for unless `CONCURRENTLY`.
	async fn define_index(
		&mut self,
		stmt: &crate::expr::statements::define::index::DefineIndexStatement,
	) -> Lowered<()> {
		use crate::expr::index_kind::Index;
		// Columns that are evaluated are resolved, and checked against the
		// catalog, as the program runs.
		let written_cols = written_index_columns(&stmt.cols);
		let cols = written_cols.clone().unwrap_or_default();
		let is_count = matches!(stmt.index, Index::Count(_));
		let template = crate::catalog::IndexDefinition {
			index_id: crate::catalog::IndexId(0),
			name: Name::text_of(&stmt.name).into(),
			table_name: crate::val::TableName::new(Name::text_of(&stmt.what)),
			cols: cols.clone(),
			index: stmt.index.clone().into(),
			count_cond: match &stmt.index {
				Index::Count(Some(cond)) => Some(cond.clone()),
				_ => None,
			},
			comment: comment_text(&stmt.comment),
			prepare_remove: false,
			format_version: crate::catalog::INDEX_FORMAT_VERSION,
		};
		let template = stored(template.to_stored())?;

		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Index,
			base: Base::Db,
		});
		let name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Index)).await?;
		let table =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.what, NameClause::IndexTable))
				.await?;
		let tb = table.operand;
		let (ns, db, database) = self.session_ids_and_database()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let table_def = self.table_creating((ns, db, database), tb, tb_key)?;
		self.release(database);
		self.store(StoreInstr::RefuseLightweight {
			table: table_def,
			refusal: StoreRefusal::Index,
		});
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ix", name.operand)])?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("IndexDefKey"),
			key,
			ty: TypeId::named("StoredIndexDefinition"),
			dst: existing,
		});
		// The error names the index as the statement writes it.
		let shown = self.constant(Value::String(surrealdb_types::ToSql::to_sql(&stmt.name).into()));
		let shown = self.load(shown)?;
		let missing = self.exists(&stmt.kind, existing, StatementError::IxAlreadyExists, shown)?;
		let at = self.here();
		self.patch(missing, at);
		self.release(shown);

		// Each column names a field the table defines, or a path beneath one
		// whose type admits sub-fields, and no computed one, or a component of
		// the table's record id; on a schemaless table a column may name no
		// field at all.
		let schemafull = self.register()?;
		self.get_field(table_def, "schemafull", schemafull);
		let evaluated_cols = match written_cols {
			Some(_) => None,
			None => {
				let list = self.register()?;
				self.emit(Instr::NewArray {
					dst: list,
				});
				for expr in &stmt.cols {
					crate::exec::vm::host::out_of_line(|| self.index_column(expr, list)).await?;
				}
				let table_name = self.load(tb)?;
				let index_name = self.load(name.operand)?;
				self.store(StoreInstr::CheckIndexColumns {
					ns,
					db,
					table: table_name,
					index: index_name,
					columns: list,
					schemafull,
				});
				self.release(table_name);
				self.release(index_name);
				Some(list)
			}
		};
		for idiom in &cols {
			let fd = idiom.to_raw_string();
			let mut next = Vec::new();
			let checked =
				self.index_column_field((ns_op, db_op, tb), &fd, &fd, name.operand, None)?;
			next.push(checked);
			if let Some(crate::expr::Part::Field(first)) = idiom.0.first() {
				let admitted = self.index_column_field(
					(ns_op, db_op, tb),
					first.as_str(),
					first.as_str(),
					name.operand,
					Some(()),
				)?;
				next.push(admitted);
			}
			// What neither the field nor its parent settles is the catalog's
			// check of the column: a component of the table's record id passes
			// as the whole column and is refused beneath it, and on a schemafull
			// table any other path no field covers is missing.
			let named = self.constant(Value::String(fd.as_str().into()));
			let named = self.load(named)?;
			let list = self.register()?;
			self.emit(Instr::NewArray {
				dst: list,
			});
			self.emit(Instr::ArrayPush {
				array: list,
				value: Operand::Reg(named),
			});
			let table_name = self.load(tb)?;
			let index_name = self.load(name.operand)?;
			self.store(StoreInstr::CheckIndexColumns {
				ns,
				db,
				table: table_name,
				index: index_name,
				columns: list,
				schemafull,
			});
			self.release(table_name);
			self.release(index_name);
			self.release(list);
			self.release(named);
			let at = self.here();
			self.patch_all(next, at);
		}
		self.release(schemafull);

		let comment = crate::exec::vm::host::out_of_line(|| self.comment(&stmt.comment)).await?;
		let definition = self.constant(template);
		let definition = self.load(definition)?;
		self.set_name(definition, "name", &name);
		self.set_name(definition, "table_name", &table);
		self.set_comment(definition, &comment);
		if let Some(list) = evaluated_cols {
			self.set_field(definition, "cols", Operand::Reg(list));
		}

		// An import replaying a definition whose index is unchanged keeps the
		// index's data and its on-disk format, refreshing only the definition.
		if matches!(stmt.kind, DefineKind::Default) {
			let fresh = self.emit(Instr::JumpIf {
				cond: Operand::Reg(existing),
				truthy: false,
				target: 0,
			});
			let reuses = self.register()?;
			self.store(StoreInstr::IndexReplayReuses {
				existing,
				template: definition,
				dst: reuses,
			});
			let changed = self.emit(Instr::JumpIf {
				cond: Operand::Reg(reuses),
				truthy: false,
				target: 0,
			});
			self.release(reuses);
			for field in ["index_id", "format_version"] {
				let kept = self.register()?;
				self.get_field(existing, field, kept);
				self.set_field(definition, field, Operand::Reg(kept));
				self.release(kept);
			}
			self.put_index_definition((ns, db, tb), key, definition, name.operand)?;
			self.replace_table((ns, db, tb), tb_key, table_def, "cache_indexes_ts")?;
			self.store(StoreInstr::ClearCache);
			self.answer_none()?;
			let at = self.here();
			self.patch(fresh, at);
			self.patch(changed, at);
		}

		// A definition that consumes the table's shared doc-id space adopts
		// its mappings, so a queued reclaim of the space is cancelled first.
		if !is_count {
			let tb_reg = self.load(tb)?;
			self.store(StoreInstr::ClaimDocIdsReclaim {
				ns,
				db,
				tb: tb_reg,
				upgrading: None,
			});
			self.release(tb_reg);
		}
		// A replaced index is retired; its doc-id space goes too when it was
		// the last consumer and the replacement consumes none.
		let fresh = self.emit(Instr::JumpIf {
			cond: Operand::Reg(existing),
			truthy: false,
			target: 0,
		});
		self.retire_index((ns, db, tb), key, existing, table_def, is_count)?;
		let at = self.here();
		self.patch(fresh, at);

		// A (re)defined index takes a fresh id.
		let tb_reg = self.load(tb)?;
		let id = self.register()?;
		self.store(StoreInstr::NextIndexId {
			ns,
			db,
			tb: tb_reg,
			dst: id,
			draw: 0,
			reserved: None,
		});
		self.release(tb_reg);
		self.set_field(definition, "index_id", Operand::Reg(id));
		self.release(id);
		self.put_index_definition((ns, db, tb), key, definition, name.operand)?;
		self.replace_table((ns, db, tb), tb_key, table_def, "cache_indexes_ts")?;
		self.store(StoreInstr::ClearCache);
		self.store(StoreInstr::BuildIndex {
			table: table_def,
			index: definition,
			blocking: !stmt.concurrently,
			fresh: true,
		});
		self.answer_none()
	}

	/// Append to the array in `list` the paths the written index column `expr`
	/// names, evaluating what it holds as the body resolves one
	/// (`ddl::scalar::fields`).
	async fn index_column(&mut self, expr: &Expr, list: Reg) -> Lowered<()> {
		use crate::expr::Function;
		let call = match expr {
			Expr::FunctionCall(call) => match &call.receiver {
				Function::Normal(name) if name == "type::fields" || name == "type::field" => {
					Some((call, name == "type::fields"))
				}
				_ => None,
			},
			_ => None,
		};
		let source = match call {
			Some((call, many)) => match call.arguments.first() {
				Some(arg) => {
					let value = crate::exec::vm::host::out_of_line(|| self.evaluated(arg)).await?;
					if many {
						ColumnSource::TypeFields(value)
					} else {
						ColumnSource::TypeField(value)
					}
				}
				None if many => ColumnSource::Refused(
					"Expected an argument for type::fields function call".to_owned(),
				),
				None => ColumnSource::Refused(
					"Expected an argument for type::field function call".to_owned(),
				),
			},
			None => ColumnSource::Path(
				crate::exec::vm::host::out_of_line(|| self.path_source(expr)).await?,
			),
		};
		self.store(StoreInstr::ResolveColumns {
			source,
			list,
		});
		Ok(())
	}

	/// What a field path written as `expr` resolves from: a path headed by a
	/// field keeps its structure and has the subscripts that are not literals
	/// evaluated, in order; any other expression is evaluated whole
	/// (`ddl::scalar::raw_idiom`).
	async fn path_source(&mut self, expr: &Expr) -> Lowered<PathSource> {
		use crate::expr::Part;
		match expr {
			Expr::Idiom(idiom) if matches!(idiom.0.first(), Some(Part::Field(_))) => {
				let mut holes = Vec::new();
				for part in &idiom.0 {
					if let Part::Value(subscript) = part
						&& !matches!(subscript, Expr::Literal(_))
					{
						holes.push(
							crate::exec::vm::host::out_of_line(|| self.evaluated(subscript))
								.await?,
						);
					}
				}
				Ok(PathSource::Written {
					idiom: idiom.clone(),
					holes,
				})
			}
			_ => Ok(PathSource::Evaluated(
				crate::exec::vm::host::out_of_line(|| self.evaluated(expr)).await?,
			)),
		}
	}

	/// A register holding the text of the field path `expr` names
	/// (`ddl::scalar::idiom`): the path is refused where a stored field name
	/// could not hold it.
	async fn field_path(&mut self, expr: &Expr) -> Lowered<Reg> {
		let source = crate::exec::vm::host::out_of_line(|| self.path_source(expr)).await?;
		let dst = self.register()?;
		self.store(StoreInstr::ResolvePath {
			source,
			validated: true,
			dst,
		});
		Ok(dst)
	}

	/// `REBUILD INDEX` — `ddl/rebuild.rs`.
	async fn rebuild_index(&mut self, stmt: &RebuildIndexStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Index,
			base: Base::Db,
		});
		let name = self.constant(Value::String(stmt.name.clone()));
		let tb = self.constant(Value::String(stmt.table.as_str().into()));
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		// The index is looked for before its table, so `IF EXISTS` answers
		// NONE for an index on a table that does not exist either.
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ix", name)])?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("IndexDefKey"),
			key,
			ty: TypeId::named("StoredIndexDefinition"),
			dst: existing,
		});
		let name_reg = self.load(name)?;
		self.must_exist(stmt.if_exists, existing, StatementError::IxNotFound, name_reg)?;
		self.release(name_reg);
		let tb_name = self.load(tb)?;
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let table_def = self.expect_table(tb_key, tb_name)?;
		self.release(tb_name);
		let tb_reg = self.load(tb)?;
		self.store(StoreInstr::ClaimDocIdsReclaim {
			ns,
			db,
			tb: tb_reg,
			upgrading: Some(existing),
		});
		self.release(tb_reg);
		let format_version_val = self.constant(Value::from(crate::catalog::INDEX_FORMAT_VERSION));
		self.set_field(existing, "format_version", format_version_val);
		self.put_index_definition((ns, db, tb), key, existing, name)?;
		self.replace_table((ns, db, tb), tb_key, table_def, "cache_indexes_ts")?;
		self.store(StoreInstr::ClearCache);
		self.store(StoreInstr::BuildIndex {
			table: table_def,
			index: existing,
			blocking: !stmt.concurrently,
			fresh: false,
		});
		self.answer_none()
	}

	/// The check one index column makes of the field it names, `fd`, reported
	/// under `shown`: a field the table defines — or, with `parent`, one whose
	/// type admits sub-fields — that is computed is refused; one that is not
	/// passes, and the answer is the jump to where the column's check ends.
	/// A field that is missing, or a parent whose type admits no sub-fields,
	/// falls through to what the caller checks next.
	fn index_column_field(
		&mut self,
		(ns, db, tb): (Operand, Operand, Operand),
		fd: &str,
		shown: &str,
		index: Operand,
		parent: Option<()>,
	) -> Lowered<Label> {
		let fd = self.constant(Value::String(fd.into()));
		let key = self.object(&[("ns", ns), ("db", db), ("tb", tb), ("fd", fd)])?;
		let field = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("FieldKey"),
			key,
			ty: TypeId::named("StoredFieldDefinition"),
			dst: field,
		});
		self.release(key);
		let absent = self.emit(Instr::JumpIf {
			cond: Operand::Reg(field),
			truthy: false,
			target: 0,
		});
		let mut past = vec![absent];
		if parent.is_some() {
			let admits = self.register()?;
			self.store(StoreInstr::FieldAdmitsSubFields {
				field,
				dst: admits,
			});
			past.push(self.emit(Instr::JumpIf {
				cond: Operand::Reg(admits),
				truthy: false,
				target: 0,
			}));
			self.release(admits);
		}
		let computed = self.register()?;
		self.get_field(field, "computed", computed);
		let plain = self.emit(Instr::JumpIf {
			cond: Operand::Reg(computed),
			truthy: false,
			target: 0,
		});
		let shown = self.constant(Value::String(shown.into()));
		let detail = self.object(&[("field", shown), ("index", index)])?;
		self.store(StoreInstr::Raise {
			error: StatementError::ComputedFieldCannotBeIndexed,
			arg: detail,
		});
		self.release(detail);
		let at = self.here();
		self.patch(plain, at);
		self.release(computed);
		self.release(field);
		let passed = self.emit(Instr::Jump {
			target: 0,
		});
		let at = self.here();
		self.patch_all(past, at);
		Ok(passed)
	}

	/// Write the index definition in `definition` at `key`, with its id's
	/// name lookup, and bump the table's catalog version (`put_tb_index`).
	fn put_index_definition(
		&mut self,
		(ns, db, tb): (Reg, Reg, Operand),
		key: Reg,
		definition: Reg,
		name: Operand,
	) -> Lowered<()> {
		self.store(StoreInstr::Put {
			route: RouteId::named("IndexDefKey"),
			key,
			ty: TypeId::named("StoredIndexDefinition"),
			value: definition,
		});
		let id = self.register()?;
		self.get_field(definition, "index_id", id);
		let lookup = self.object(&[
			("ns", Operand::Reg(ns)),
			("db", Operand::Reg(db)),
			("tb", tb),
			("ix", Operand::Reg(id)),
		])?;
		let name = self.load(name)?;
		self.store(StoreInstr::Put {
			route: RouteId::named("IndexNameKey"),
			key: lookup,
			ty: TypeId::UTF8,
			value: name,
		});
		for reg in [name, lookup, id] {
			self.release(reg);
		}
		self.stamp(ns, db, tb)
	}

	/// Retire the index defined in `index`, at `key`, of the table defined in
	/// `table` — `REMOVE INDEX`'s retirement, and `DEFINE INDEX OVERWRITE`'s
	/// of the index it replaces: the process's loaded state evicted and a local
	/// build aborted once the transaction commits, the durable build state
	/// deleted, the definition and its id's name lookup deleted and the index's
	/// data enqueued for reclaim. With `purge`, the table's shared doc-id space
	/// is enqueued for reclaim too when this was its last consumer, read before
	/// the index goes.
	fn retire_index(
		&mut self,
		(ns, db, tb): (Reg, Reg, Operand),
		key: Reg,
		index: Reg,
		table: Reg,
		purge: bool,
	) -> Lowered<()> {
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let ix = self.register()?;
		self.get_field(index, "index_id", ix);
		let last = self.register()?;
		if purge {
			// Whether this is the table's last index over its shared doc-id space,
			// read before the index goes.
			self.store(StoreInstr::SharesDocIds {
				index,
				dst: last,
			});
			let not_last = self.emit(Instr::JumpIf {
				cond: Operand::Reg(last),
				truthy: false,
				target: 0,
			});
			let graph = self.register()?;
			self.get_field(table, "graph_doc_ids", graph);
			let graph_held = self.emit(Instr::JumpIf {
				cond: Operand::Reg(graph),
				truthy: true,
				target: 0,
			});
			self.release(graph);
			let others = self.scan(
				"IndexDefPrefix",
				&[("ns", ns_op), ("db", db_op), ("tb", tb)],
				"StoredIndexDefinition",
			)?;
			let mut held = Vec::new();
			self.each(others, |l, other| {
				let other_ix = l.register()?;
				l.get_field(other, "index_id", other_ix);
				let same = l.register()?;
				l.emit(Instr::Compare {
					op: crate::exec::vm::program::CompareOp::Equal,
					left: Operand::Reg(other_ix),
					right: Operand::Reg(ix),
					dst: same,
				});
				let skip = l.emit(Instr::JumpIf {
					cond: Operand::Reg(same),
					truthy: true,
					target: 0,
				});
				let shares = l.register()?;
				l.store(StoreInstr::SharesDocIds {
					index: other,
					dst: shares,
				});
				held.push(l.emit(Instr::JumpIf {
					cond: Operand::Reg(shares),
					truthy: true,
					target: 0,
				}));
				let at = l.here();
				l.patch(skip, at);
				for reg in [shares, same, other_ix] {
					l.release(reg);
				}
				Ok(())
			})?;
			self.release(others);
			let still_last = self.emit(Instr::Jump {
				target: 0,
			});
			let at = self.here();
			for jump in held.into_iter().chain([not_last, graph_held]) {
				self.patch(jump, at);
			}
			let no = self.constant(Value::Bool(false));
			self.emit(Instr::Load {
				dst: last,
				src: no,
			});
			let at = self.here();
			self.patch(still_last, at);
		}
		self.store(StoreInstr::RetireIndexLocally {
			ns,
			db,
			table,
			index,
		});
		// The index's durable build state and its queues, deleted at the current version: a
		// removal of one index is never an EXPUNGE.
		let build =
			self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ix", Operand::Reg(ix))])?;
		self.delete("BuildStateKey", build, false);
		for route in [
			"BuildAppendIxPrefix",
			"BuildPrimaryIxPrefix",
			"BuildReservationIxPrefix",
			"BuildTicketIxPrefix",
		] {
			self.delete_range(route, build, false);
		}
		// `del_tb_index_deferred`.
		self.store(StoreInstr::Delete {
			route: RouteId::named("IndexDefKey"),
			key,
		});
		self.store(StoreInstr::Delete {
			route: RouteId::named("IndexNameKey"),
			key: build,
		});
		self.stamp(ns, db, tb)?;
		self.enqueue_reclaim(2, [ns_op, db_op, tb, Operand::Reg(ix)], false)?;
		if purge {
			// `del_tb_doc_ids_deferred`, in its order of kinds: DocLookup,
			// DocLookupIdentity, DocKey, DocPending.
			let kept = self.emit(Instr::JumpIf {
				cond: Operand::Reg(last),
				truthy: false,
				target: 0,
			});
			let (zero, _) = self.unused_scope();
			for kind in [4, 6, 3, 5] {
				self.enqueue_reclaim(kind, [ns_op, db_op, tb, zero], false)?;
			}
			let at = self.here();
			self.patch(kept, at);
		}
		Ok(())
	}

	/// `REMOVE INDEX` — `ddl/remove/index.rs`.
	async fn remove_index(&mut self, stmt: &RemoveIndexStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Index,
			base: Base::Db,
		});
		let name = crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Index))
			.await?
			.operand;
		let tb = crate::exec::vm::host::out_of_line(|| self.name(&stmt.what, NameClause::What))
			.await?
			.operand;
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ix", name)])?;
		let index = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("IndexDefKey"),
			key,
			ty: TypeId::named("StoredIndexDefinition"),
			dst: index,
		});
		let name_reg = self.load(name)?;
		self.must_exist(stmt.if_exists, index, StatementError::IxNotFound, name_reg)?;
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let tb_name = self.load(tb)?;
		let table = self.expect_table(tb_key, tb_name)?;
		self.retire_index((ns, db, tb), key, index, table, true)?;
		self.replace_table((ns, db, tb), tb_key, table, "cache_indexes_ts")?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

/// The name of the definition [`Lowering::remove_db_definition`] removes.
enum DbName<'e> {
	/// A name fixed by the statement's syntax: the key's name, and the name
	/// the missing error shows.
	Written(&'e str, String),
	/// A name the statement resolves as [`Lowering::name`] does, `into` naming
	/// it in a refusal; the key's name and the name shown.
	Named(&'e Expr, NameClause),
}

impl Lowering<'_, '_> {
	/// `REMOVE PARAM`, `REMOVE FUNCTION` and `REMOVE BUCKET` —
	/// `ddl/remove/param.rs`, `ddl/remove/function.rs`, `ddl/remove/bucket.rs`:
	/// a database-level definition deleted at the key its definition names.
	async fn remove_db_definition(
		&mut self,
		resource: ResourceKind,
		(route, field, ty): (&'static str, &str, &'static str),
		name: DbName<'_>,
		if_exists: bool,
		missing: StatementError,
	) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource,
			base: Base::Db,
		});
		let (name, shown) = match name {
			DbName::Written(name, shown) => (
				self.constant(Value::String(name.into())),
				self.constant(Value::String(shown.into())),
			),
			DbName::Named(expr, into) => {
				let name =
					crate::exec::vm::host::out_of_line(|| self.name(expr, into)).await?.operand;
				(name, name)
			}
		};
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let key = self.object(&[("ns", ns_op), ("db", db_op), (field, name)])?;
		let definition = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named(ty),
			dst: definition,
		});
		let shown = self.load(shown)?;
		self.must_exist(if_exists, definition, missing, shown)?;
		let stored_name = self.register()?;
		self.get_field(definition, "name", stored_name);
		let stored_at =
			self.object(&[("ns", ns_op), ("db", db_op), (field, Operand::Reg(stored_name))])?;
		self.store(StoreInstr::Delete {
			route: RouteId::named(route),
			key: stored_at,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE MODULE` — `ddl/remove/module.rs`: the definition, at its
	/// storage name, and the process's loaded runtime for it.
	fn remove_module(&mut self, stmt: &RemoveModuleStatement) -> Lowered<()> {
		let storage = stmt.name.get_storage_name();
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Module,
			base: Base::Db,
		});
		let (ns, db) = self.session_ids()?;
		let name = self.constant(Value::String(storage.into()));
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("md", name)])?;
		let module = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("ModuleKey"),
			key,
			ty: TypeId::named("StoredModuleDefinition"),
			dst: module,
		});
		let name_reg = self.load(name)?;
		self.must_exist(stmt.if_exists, module, StatementError::MdNotFound, name_reg)?;
		self.store(StoreInstr::Delete {
			route: RouteId::named("ModuleKey"),
			key,
		});
		self.store(StoreInstr::ClearCache);
		self.store(StoreInstr::EvictModule {
			ns,
			db,
			module,
		});
		self.answer_none()
	}

	/// `REMOVE SEQUENCE` — `ddl/remove/sequence.rs`.
	async fn remove_sequence(&mut self, stmt: &RemoveSequenceStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Sequence,
			base: Base::Db,
		});
		let name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Sequence))
				.await?;
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let name = name.operand;
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("sq", name)])?;
		let sequence = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("SequenceKey"),
			key,
			ty: TypeId::named("SequenceDefinition"),
			dst: sequence,
		});
		let name_reg = self.load(name)?;
		self.must_exist(stmt.if_exists, sequence, StatementError::SeqNotFound, name_reg)?;
		self.with_service(Service::Sequences, |l| {
			l.store(StoreInstr::EvictSequence {
				ns,
				db,
				name: name_reg,
			});
			Ok(())
		})?;
		let stored_name = self.register()?;
		self.get_field(sequence, "name", stored_name);
		let state =
			self.object(&[("ns", ns_op), ("db", db_op), ("sq", Operand::Reg(stored_name))])?;
		self.delete_range("SeqBatchPrefix", state, false);
		self.delete_range("SeqStatePrefix", state, false);
		self.store(StoreInstr::Delete {
			route: RouteId::named("SequenceKey"),
			key,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE ANALYZER` — `ddl/remove/analyzer.rs`, which resolves the
	/// database before its permission check.
	async fn remove_analyzer(&mut self, stmt: &RemoveAnalyzerStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		let (ns, db) = self.session_ids()?;
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Analyzer,
			base: Base::Db,
		});
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Analyzer))
				.await?
				.operand;
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("az", name)])?;
		let analyzer = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("AnalyzerKey"),
			key,
			ty: TypeId::named("AnalyzerDefinition"),
			dst: analyzer,
		});
		let name_reg = self.load(name)?;
		self.must_exist(stmt.if_exists, analyzer, StatementError::AzNotFound, name_reg)?;
		let az = self.register()?;
		self.get_field(analyzer, "name", az);
		// Refuse while a full-text index of any table uses the analyzer.
		self.each_table(ns, db, |l, _, tb| {
			let indexes = l.scan(
				"IndexDefPrefix",
				&[("ns", ns_op), ("db", db_op), ("tb", Operand::Reg(tb))],
				"StoredIndexDefinition",
			)?;
			l.each(indexes, |l, index| {
				let params = l.register()?;
				l.get_field(index, "index", params);
				l.get_field(params, "FullText", params);
				let other = l.emit(Instr::JumpIf {
					cond: Operand::Reg(params),
					truthy: false,
					target: 0,
				});
				let used = l.register()?;
				l.get_field(params, "analyzer", used);
				let same = l.register()?;
				l.emit(Instr::Compare {
					op: crate::exec::vm::program::CompareOp::Equal,
					left: Operand::Reg(used),
					right: Operand::Reg(az),
					dst: same,
				});
				let unused = l.emit(Instr::JumpIf {
					cond: Operand::Reg(same),
					truthy: false,
					target: 0,
				});
				let ix = l.register()?;
				l.get_field(index, "name", ix);
				let detail = l.object(&[
					("name", Operand::Reg(az)),
					("table", Operand::Reg(tb)),
					("index", Operand::Reg(ix)),
				])?;
				l.store(StoreInstr::Raise {
					error: StatementError::AzInUse,
					arg: detail,
				});
				let at = l.here();
				l.patch(other, at);
				l.patch(unused, at);
				for reg in [detail, ix, same, used, params] {
					l.release(reg);
				}
				Ok(())
			})?;
			l.release(indexes);
			Ok(())
		})?;
		let stored_at = self.object(&[("ns", ns_op), ("db", db_op), ("az", Operand::Reg(az))])?;
		self.store(StoreInstr::Delete {
			route: RouteId::named("AnalyzerKey"),
			key: stored_at,
		});
		self.store(StoreInstr::ClearCache);
		let analyzers =
			self.scan("AnalyzerPrefix", &[("ns", ns_op), ("db", db_op)], "AnalyzerDefinition")?;
		self.store(StoreInstr::CleanupMappers {
			analyzers,
		});
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// `REMOVE ACCESS` — `ddl/remove/access.rs`: the method's definition and
	/// everything beneath its root, its grants among them. On a namespace the
	/// session's namespace is created when missing (`get_ns_id`).
	async fn remove_access(&mut self, stmt: &RemoveAccessStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Actor,
			base: stmt.base,
		});
		let name = crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Access))
			.await?;
		let ac = name.operand;
		let (scope, missing): (Vec<(&str, Operand)>, _) = match stmt.base {
			Base::Root => (Vec::new(), StatementError::AccessRootNotFound),
			Base::Ns => {
				let ns = self.namespace_id_creating()?;
				(vec![("ns", Operand::Reg(ns))], StatementError::AccessNsNotFound)
			}
			Base::Db => {
				let (ns, db) = self.session_ids()?;
				(
					vec![("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))],
					StatementError::AccessDbNotFound,
				)
			}
		};
		let (method, root) = match stmt.base {
			Base::Root => ("RootAccessMethodKey", "RootAccessRoot"),
			Base::Ns => ("NsAccessMethodKey", "NsAccessRoot"),
			Base::Db => ("DbAccessMethodKey", "DbAccessRoot"),
		};
		let mut fields = scope.clone();
		fields.push(("ac", ac));
		let key = self.object(&fields)?;
		let access = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(method),
			key,
			ty: TypeId::named("StoredAccessDefinition"),
			dst: access,
		});
		let present = self.emit(Instr::JumpIf {
			cond: Operand::Reg(access),
			truthy: true,
			target: 0,
		});
		self.missing_at_base(stmt.if_exists, stmt.base, ("ac", ac), missing)?;
		let at = self.here();
		self.patch(present, at);
		// `del_*_access`, at the name the definition holds.
		let stored_name = self.register()?;
		self.get_field(access, "name", stored_name);
		let mut fields = scope;
		fields.push(("ac", Operand::Reg(stored_name)));
		let stored_at = self.object(&fields)?;
		self.store(StoreInstr::Delete {
			route: RouteId::named(method),
			key: stored_at,
		});
		self.delete_range(root, stored_at, false);
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// Answer `NONE` under `IF EXISTS`; otherwise raise `missing`, whose
	/// fields are `name` and the session's namespace and database names down
	/// to `base`.
	fn missing_at_base(
		&mut self,
		if_exists: bool,
		base: Base,
		name: (&str, Operand),
		missing: StatementError,
	) -> Lowered<()> {
		if if_exists {
			return self.answer_none();
		}
		let mut detail = vec![name];
		if !matches!(base, Base::Root) {
			let ns = self.register()?;
			self.store(StoreInstr::SessionNs {
				dst: ns,
			});
			detail.push(("ns", Operand::Reg(ns)));
		}
		if matches!(base, Base::Db) {
			let db = self.register()?;
			self.store(StoreInstr::SessionDb {
				dst: db,
			});
			detail.push(("db", Operand::Reg(db)));
		}
		let detail = self.object(&detail)?;
		self.store(StoreInstr::Raise {
			error: missing,
			arg: detail,
		});
		Ok(())
	}

	/// `REMOVE USER` — `ddl/remove/user.rs`: the user's definition, and every
	/// subscription running as the user in its scope, removed and told. On a
	/// namespace the session's namespace is created when missing
	/// (`get_ns_id`).
	async fn remove_user(&mut self, stmt: &RemoveUserStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Actor,
			base: stmt.base,
		});
		let name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::User)).await?;
		let user = name.operand;
		let (route, scope, missing) = match stmt.base {
			Base::Root => ("RootUserKey", None, StatementError::UserRootNotFound),
			Base::Ns => {
				let ns = self.namespace_id_creating()?;
				("NsUserKey", Some((ns, None)), StatementError::UserNsNotFound)
			}
			Base::Db => {
				let (ns, db) = self.session_ids()?;
				("DbUserKey", Some((ns, Some(db))), StatementError::UserDbNotFound)
			}
		};
		let fields = |user: Operand| -> Vec<(&str, Operand)> {
			match scope {
				None => vec![("user", user)],
				Some((ns, None)) => vec![("ns", Operand::Reg(ns)), ("user", user)],
				Some((ns, Some(db))) => {
					vec![("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("user", user)]
				}
			}
		};
		let key = self.object(&fields(user))?;
		let definition = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("UserDefinition"),
			dst: definition,
		});
		let present = self.emit(Instr::JumpIf {
			cond: Operand::Reg(definition),
			truthy: true,
			target: 0,
		});
		self.missing_at_base(stmt.if_exists, stmt.base, ("name", user), missing)?;
		let at = self.here();
		self.patch(present, at);
		let actor = self.register()?;
		self.get_field(definition, "name", actor);
		match scope {
			None => {
				let namespaces = self.scan("NamespacePrefix", &[], "NamespaceDefinition")?;
				self.each(namespaces, |l, namespace| {
					let ns = l.register()?;
					l.get_field(namespace, "namespace_id", ns);
					l.each_database(ns, |l, db| l.revoke_database_subscriptions(ns, db, actor))?;
					l.release(ns);
					Ok(())
				})?;
				self.release(namespaces);
			}
			Some((ns, None)) => {
				self.each_database(ns, |l, db| l.revoke_database_subscriptions(ns, db, actor))?
			}
			Some((ns, Some(db))) => self.revoke_database_subscriptions(ns, db, actor)?,
		}
		let stored_at = self.object(&fields(Operand::Reg(actor)))?;
		self.store(StoreInstr::Delete {
			route: RouteId::named(route),
			key: stored_at,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// Remove every subscription in the database in `ns`/`db` that runs as
	/// the actor named in `actor`, and queue its `KILLED` for commit
	/// (`kill_principal_subscriptions`). A table whose subscriptions do not
	/// read is passed over; one that loses a subscription has its lives cache
	/// stamp moved (`bump_table_lives_cache`).
	fn revoke_database_subscriptions(&mut self, ns: Reg, db: Reg, actor: Reg) -> Lowered<()> {
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		self.each_table(ns, db, |l, _, tb| {
			let tb_op = Operand::Reg(tb);
			let key = l.object(&[("ns", ns_op), ("db", db_op), ("tb", tb_op)])?;
			let lives = l.register()?;
			let scan = l.store(StoreInstr::Scan {
				route: RouteId::named("SubscriptionPrefix"),
				key,
				ty: TypeId::named("StoredSubscriptionDefinition"),
				dst: lives,
				unread: Some(0),
			});
			let no = l.constant(Value::Bool(false));
			let removed = l.load(no)?;
			l.each(lives, |l, lv| {
				let owner = l.register()?;
				l.get_field(lv, "auth", owner);
				let anonymous = l.emit(Instr::JumpIf {
					cond: Operand::Reg(owner),
					truthy: false,
					target: 0,
				});
				for field in ["actor", "res", "id"] {
					l.get_field(owner, field, owner);
				}
				l.emit(Instr::Compare {
					op: crate::exec::vm::program::CompareOp::Equal,
					left: Operand::Reg(owner),
					right: Operand::Reg(actor),
					dst: owner,
				});
				let other = l.emit(Instr::JumpIf {
					cond: Operand::Reg(owner),
					truthy: false,
					target: 0,
				});
				let (lq, nd) = (l.register()?, l.register()?);
				l.get_field(lv, "id", lq);
				l.get_field(lv, "node", nd);
				let subscription = l.object(&[
					("ns", ns_op),
					("db", db_op),
					("tb", tb_op),
					("lq", Operand::Reg(lq)),
				])?;
				l.store(StoreInstr::Clear {
					route: RouteId::named("SubscriptionKey"),
					key: subscription,
				});
				let node = l.object(&[("nd", Operand::Reg(nd)), ("lq", Operand::Reg(lq))])?;
				l.store(StoreInstr::Clear {
					route: RouteId::named("NodeLiveQueryKey"),
					key: node,
				});
				let yes = l.constant(Value::Bool(true));
				l.emit(Instr::Load {
					dst: removed,
					src: yes,
				});
				l.store(StoreInstr::KillOnCommit {
					subscription: lv,
				});
				let at = l.here();
				l.patch(anonymous, at);
				l.patch(other, at);
				for reg in [node, subscription, nd, lq, owner] {
					l.release(reg);
				}
				Ok(())
			})?;
			let untouched = l.emit(Instr::JumpIf {
				cond: Operand::Reg(removed),
				truthy: false,
				target: 0,
			});
			// `bump_table_lives_cache`: under a locked read, the table's
			// lives stamp moves, when the table is still defined.
			let table_key = l.object(&[("ns", ns_op), ("db", db_op), ("tb", tb_op)])?;
			l.store(StoreInstr::Lock {
				route: RouteId::named("TableKey"),
				key: table_key,
			});
			let table = l.register()?;
			l.store(StoreInstr::Get {
				route: RouteId::named("TableKey"),
				key: table_key,
				ty: TypeId::named("StoredTableDefinition"),
				dst: table,
			});
			let gone = l.emit(Instr::JumpIf {
				cond: Operand::Reg(table),
				truthy: false,
				target: 0,
			});
			let stamp = l.register()?;
			l.store(StoreInstr::NewUuid {
				dst: stamp,
				draw: 0,
				reserved: None,
			});
			l.set_field(table, "cache_lives_ts", Operand::Reg(stamp));
			l.store(StoreInstr::Put {
				route: RouteId::named("TableKey"),
				key: table_key,
				ty: TypeId::named("StoredTableDefinition"),
				value: table,
			});
			l.store(StoreInstr::ClearCache);
			let at = l.here();
			l.patch(gone, at);
			l.patch(untouched, at);
			l.patch(scan, at);
			for reg in [stamp, table, table_key, removed, lives, key] {
				l.release(reg);
			}
			Ok(())
		})
	}

	/// `REMOVE API` — `ddl/remove/api.rs`, for an API whose path renders as
	/// the name it is removed by.
	async fn remove_api(&mut self, stmt: &RemoveApiStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Api,
			base: Base::Db,
		});
		let name = crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Api))
			.await?
			.operand;
		let (ns, db) = self.session_ids()?;
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("ap", name)])?;
		let api = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("ApiKey"),
			key,
			ty: TypeId::named("StoredApiDefinition"),
			dst: api,
		});
		let detail = self.object(&[("value", name)])?;
		self.must_exist(stmt.if_exists, api, StatementError::ApNotFound, detail)?;
		// The definition is deleted at its path's rendering, which is not
		// always the name the statement gives.
		let (text, rendered) = (self.register()?, self.register()?);
		self.get_field(api, "path", text);
		self.store(StoreInstr::ParseApiPath {
			src: text,
			text,
			rendered,
		});
		let stored_at = self.object(&[
			("ns", Operand::Reg(ns)),
			("db", Operand::Reg(db)),
			("ap", Operand::Reg(rendered)),
		])?;
		self.store(StoreInstr::Delete {
			route: RouteId::named("ApiKey"),
			key: stored_at,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// A register holding what `expr` evaluates to in the statement's frame,
	/// a `RETURN` answering its value (`ddl::scalar::scalar`).
	async fn evaluated(&mut self, expr: &Expr) -> Lowered<Reg> {
		// What has no logical form, or writes where the engine has no plan,
		// is not lowered: the statement is refused.
		let Ok(logical) = crate::expr::logical::lower_expr(expr.clone()) else {
			return Err(NotCovered::internal("an expression with no logical form"));
		};
		if crate::exec::logical::unplannable_write_in_expr(&logical).is_some() {
			return Err(NotCovered::internal("an expression that writes"));
		}
		// A value a second evaluation may answer differently — from the clock,
		// entropy, the world outside, the rows it reads — or that acts outside
		// the transaction is evaluated by the statement's run alone.
		let effects =
			crate::exec::logical::effects_of_expr(&logical, self.planner.function_registry());
		if effects.impure || !crate::exec::vm::speculate::admits(effects.set) {
			self.store(StoreInstr::RealRunOnly);
		}
		let outer = std::mem::replace(&mut self.writing_labels, true);
		let value = crate::exec::vm::host::out_of_line(|| self.once_value(&logical, true)).await;
		self.writing_labels = outer;
		value
	}

	/// The ids of the session's namespace and database, creating whichever
	/// does not exist yet (`ensure_ns_db`).
	fn ids_creating(&mut self) -> Lowered<(Reg, Reg)> {
		let ns = self.namespace_id_creating()?;
		let db_name = self.register()?;
		self.store(StoreInstr::SessionDb {
			dst: db_name,
		});
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db_name))])?;
		let database = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("DatabaseKey"),
			key,
			ty: TypeId::named("DatabaseDefinition"),
			dst: database,
		});
		let present = self.emit(Instr::JumpIf {
			cond: Operand::Reg(database),
			truthy: true,
			target: 0,
		});
		let id = self.register()?;
		self.store(StoreInstr::NextId {
			sequence: IdSequence::Database,
			ns: Some(ns),
			db: None,
			dst: id,
			draw: 0,
			reserved: None,
		});
		let (none, no) = (self.constant(Value::None), self.constant(Value::Bool(false)));
		let created = self.object(&[
			("namespace_id", Operand::Reg(ns)),
			("database_id", Operand::Reg(id)),
			("name", Operand::Reg(db_name)),
			("comment", none),
			("changefeed", none),
			("strict", no),
		])?;
		self.store(StoreInstr::Put {
			route: RouteId::named("DatabaseKey"),
			key,
			ty: TypeId::named("DatabaseDefinition"),
			value: created,
		});
		self.emit(Instr::Load {
			dst: database,
			src: Operand::Reg(created),
		});
		let at = self.here();
		self.patch(present, at);
		self.get_field(database, "database_id", id);
		for reg in [created, database, key, db_name] {
			self.release(reg);
		}
		Ok((ns, id))
	}

	/// `DEFINE PARAM` — `ddl/define/param.rs`: its value is evaluated before
	/// the database is resolved.
	async fn define_param(&mut self, stmt: &DefineParamStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Parameter,
			base: Base::Db,
		});
		let value = crate::exec::vm::host::out_of_line(|| self.evaluated(&stmt.value)).await?;
		self.store(StoreInstr::CheckDepth {
			value,
		});
		let (ns, db) = self.ids_creating()?;
		let name = self.constant(Value::String(stmt.name.as_str().into()));
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("pa", name)])?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("ParamKey"),
			key,
			ty: TypeId::named("StoredParamDefinition"),
			dst: existing,
		});
		let name_reg = self.load(name)?;
		let missing =
			self.exists(&stmt.kind, existing, StatementError::PaAlreadyExists, name_reg)?;
		let at = self.here();
		self.patch(missing, at);
		let comment = crate::exec::vm::host::out_of_line(|| self.comment(&stmt.comment)).await?;
		let template = crate::catalog::ParamDefinition {
			value: Value::None,
			name: stmt.name.clone(),
			comment: comment_text(&stmt.comment),
			permissions: stmt.permissions.clone(),
			permissions_body: crate::catalog::LogicalPermissionBody::of(&stmt.permissions),
		};
		let template = stored(template.to_stored())?;
		let template = self.constant(template);
		let definition = self.load(template)?;
		self.set_comment(definition, &comment);
		self.set_field(definition, "value", Operand::Reg(value));
		self.store(StoreInstr::Put {
			route: RouteId::named("ParamKey"),
			key,
			ty: TypeId::named("StoredParamDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `DEFINE EVENT` — `ddl/define/event.rs`, on a table that exists and is
	/// not a LIGHTWEIGHT relation.
	async fn define_event(&mut self, stmt: &DefineEventStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		let name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Event)).await?;
		let table = crate::exec::vm::host::out_of_line(|| {
			self.name(&stmt.target_table, NameClause::TargetTable)
		})
		.await?;
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Event,
			base: Base::Db,
		});
		let (ns, db) = self.ids_creating()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let tb = table.operand;
		let ev = name.operand;
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ev", ev)])?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("EventKey"),
			key,
			ty: TypeId::named("StoredEventDefinition"),
			dst: existing,
		});
		let name_reg = self.load(ev)?;
		let missing =
			self.exists(&stmt.kind, existing, StatementError::EvAlreadyExists, name_reg)?;
		let at = self.here();
		self.patch(missing, at);
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let database = self.database_definition(ns)?;
		let table_def = self.table_creating((ns, db, database), tb, tb_key)?;
		self.store(StoreInstr::RefuseLightweight {
			table: table_def,
			refusal: StoreRefusal::Event,
		});
		self.release(database);
		let comment = crate::exec::vm::host::out_of_line(|| self.comment(&stmt.comment)).await?;
		let template = crate::catalog::EventDefinition::new(
			name.text().into(),
			crate::val::TableName::new(Name::text_of(&stmt.target_table)),
			stmt.when.clone(),
			stmt.then.clone(),
			stmt.event_kind.clone().into(),
			crate::catalog::auth::AuthLimit::default(),
			comment_text(&stmt.comment),
		);
		let template = stored(template.to_stored())?;
		let template = self.constant(template);
		let definition = self.load(template)?;
		self.set_comment(definition, &comment);
		self.set_name(definition, "name", &name);
		self.set_name(definition, "target_table", &table);
		let limit = self.register()?;
		self.store(StoreInstr::SessionAuthLimit {
			dst: limit,
		});
		self.set_field(definition, "auth_limit", Operand::Reg(limit));
		self.store(StoreInstr::Put {
			route: RouteId::named("EventKey"),
			key,
			ty: TypeId::named("StoredEventDefinition"),
			value: definition,
		});
		self.replace_table((ns, db, tb), tb_key, table_def, "cache_events_ts")?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `DEFINE FUNCTION` — `ddl/define/function.rs`.
	async fn define_function(&mut self, stmt: &DefineFunctionStatement) -> Lowered<()> {
		// An alias the body refuses is raised once the permission check has
		// passed, as the body raises it.
		let refusal =
			match crate::ddl::define::validate_graphql_alias(&stmt.graphql_alias, "function") {
				Ok(()) => None,
				Err(e) => Some(refusal_of(&e, "a function definition")?),
			};
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Function,
			base: Base::Db,
		});
		if let Some(refusal) = refusal {
			self.store(StoreInstr::RefuseField {
				refusal,
			});
		}
		let (ns, db) = self.ids_creating()?;
		let name = self.constant(Value::String(stmt.name.as_str().into()));
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("fc", name)])?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("FunctionKey"),
			key,
			ty: TypeId::named("StoredFunctionDefinition"),
			dst: existing,
		});
		let shown = self.constant(Value::String(format!("fn::{}", stmt.name.as_str()).into()));
		let shown = self.load(shown)?;
		let missing = self.exists(&stmt.kind, existing, StatementError::FcAlreadyExists, shown)?;
		let at = self.here();
		self.patch(missing, at);
		let comment = crate::exec::vm::host::out_of_line(|| self.comment(&stmt.comment)).await?;
		let template = crate::catalog::FunctionDefinition {
			name: stmt.name.clone(),
			args: stmt.args.clone(),
			block: stmt.block.clone(),
			body: crate::catalog::LogicalBlockBody::of(&stmt.block),
			permissions: stmt.permissions.clone(),
			permissions_body: crate::catalog::LogicalPermissionBody::of(&stmt.permissions),
			returns: stmt.returns.clone(),
			comment: comment_text(&stmt.comment),
			auth_limit: crate::catalog::auth::AuthLimit::default(),
			graphql_alias: stmt.graphql_alias.clone(),
			graphql_deprecated: stmt.graphql_deprecated.clone(),
		};
		let template = stored(template.to_stored())?;
		let template = self.constant(template);
		let definition = self.load(template)?;
		self.set_comment(definition, &comment);
		let limit = self.register()?;
		self.store(StoreInstr::SessionAuthLimit {
			dst: limit,
		});
		self.set_field(definition, "auth_limit", Operand::Reg(limit));
		self.store(StoreInstr::Put {
			route: RouteId::named("FunctionKey"),
			key,
			ty: TypeId::named("StoredFunctionDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// `DEFINE USER` — `ddl/define/user.rs`. On a namespace or database the
	/// session's namespace and database are created when missing.
	async fn define_user(&mut self, stmt: &DefineUserStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Actor,
			base: stmt.base,
		});
		let (token_duration, token) =
			crate::exec::vm::host::out_of_line(|| self.duration(&stmt.duration.token)).await?;
		let (session_duration, session) =
			crate::exec::vm::host::out_of_line(|| self.duration(&stmt.duration.session)).await?;
		let comment = crate::exec::vm::host::out_of_line(|| self.comment(&stmt.comment)).await?;
		let name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::User)).await?;
		let user = name.operand;
		let (route, scope, exists) = match stmt.base {
			Base::Root => ("RootUserKey", Vec::new(), StatementError::UserRootAlreadyExists),
			Base::Ns => {
				let ns = self.namespace_id_creating()?;
				("NsUserKey", vec![("ns", Operand::Reg(ns))], StatementError::UserNsAlreadyExists)
			}
			Base::Db => {
				let (ns, db) = self.ids_creating()?;
				(
					"DbUserKey",
					vec![("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))],
					StatementError::UserDbAlreadyExists,
				)
			}
		};
		let mut fields = scope;
		fields.push(("user", user));
		let key = self.object(&fields)?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("UserDefinition"),
			dst: existing,
		});
		let missing = self.emit(Instr::JumpIf {
			cond: Operand::Reg(existing),
			truthy: false,
			target: 0,
		});
		match stmt.kind {
			DefineKind::Default => {
				let import = self.store(StoreInstr::JumpIfImport {
					target: 0,
				});
				let shown = self.register()?;
				self.get_field(existing, "name", shown);
				self.missing_at_base(false, stmt.base, ("name", Operand::Reg(shown)), exists)?;
				let at = self.here();
				self.patch(import, at);
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => self.answer_none()?,
		}
		let at = self.here();
		self.patch(missing, at);
		let definition = crate::catalog::UserDefinition {
			name: name.text().into(),
			hash: stmt.hash.clone(),
			code: stmt.code.clone(),
			roles: stmt.roles.clone(),
			token_duration: token_duration.map(crate::val::BoundedDuration::from),
			session_duration: session_duration.map(crate::val::BoundedDuration::from),
			comment: comment_text(&stmt.comment),
			base: stmt.base.into(),
			scram: stmt.scram.clone(),
		};
		let definition = stored(definition)?;
		let definition = self.constant(definition);
		let definition = self.load(definition)?;
		self.set_comment(definition, &comment);
		for (field, value) in [("token_duration", token), ("session_duration", session)] {
			if let Some(value) = value {
				self.set_field(definition, field, Operand::Reg(value));
			}
		}
		self.set_name(definition, "name", &name);
		self.store(StoreInstr::Put {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("UserDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `DEFINE ANALYZER` — `ddl/define/analyzer.rs`: the analyzer's mapper
	/// files are loaded, and its segmenter checked, before it is written.
	async fn define_analyzer(&mut self, stmt: &DefineAnalyzerStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Analyzer,
			base: Base::Db,
		});
		let comment = crate::exec::vm::host::out_of_line(|| self.comment(&stmt.comment)).await?;
		let name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Analyzer))
				.await?;
		let (ns, db) = self.ids_creating()?;
		let az = name.operand;
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("az", az)])?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("AnalyzerKey"),
			key,
			ty: TypeId::named("AnalyzerDefinition"),
			dst: existing,
		});
		let name_reg = self.load(az)?;
		let missing =
			self.exists(&stmt.kind, existing, StatementError::AzAlreadyExists, name_reg)?;
		let at = self.here();
		self.patch(missing, at);
		let definition = crate::catalog::AnalyzerDefinition {
			name: name.text().into(),
			function: stmt.function.clone(),
			tokenizers: stmt.tokenizers.clone(),
			filters: stmt.filters.clone(),
			comment: comment_text(&stmt.comment),
		};
		let definition = stored(definition)?;
		let definition = self.constant(definition);
		let definition = self.load(definition)?;
		self.set_comment(definition, &comment);
		self.set_name(definition, "name", &name);
		self.store(StoreInstr::LoadAnalyzer {
			analyzer: definition,
			mappers: true,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("AnalyzerKey"),
			key,
			ty: TypeId::named("AnalyzerDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// `DEFINE SEQUENCE` — `ddl/define/sequence.rs`: the definition, and the
	/// sequence's batches and state from before, dropped.
	async fn define_sequence(&mut self, stmt: &DefineSequenceStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Sequence,
			base: Base::Db,
		});
		let name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Sequence))
				.await?;
		let (timeout, evaluated_timeout) =
			crate::exec::vm::host::out_of_line(|| self.duration(&stmt.timeout)).await?;
		let (ns, db) = self.ids_creating()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let sq = name.operand;
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("sq", sq)])?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("SequenceKey"),
			key,
			ty: TypeId::named("SequenceDefinition"),
			dst: existing,
		});
		let name_reg = self.load(sq)?;
		let missing =
			self.exists(&stmt.kind, existing, StatementError::SeqAlreadyExists, name_reg)?;
		let at = self.here();
		self.patch(missing, at);
		// A batch is converted where the body converts it, after the existence
		// check, and one out of range is refused there; a written start is
		// read here.
		let batch = match &stmt.batch {
			Expr::Literal(Literal::Integer(i)) if u32::try_from(*i).is_ok() => (*i as u32, None),
			expr => {
				let value = match expr {
					Expr::Literal(Literal::Integer(i)) => {
						let written = self.constant(Value::from(*i));
						self.load(written)?
					}
					expr => crate::exec::vm::host::out_of_line(|| self.evaluated(expr)).await?,
				};
				self.store(StoreInstr::Convert {
					src: value,
					dst: value,
					clause: ClauseConversion::Batch,
				});
				(0, Some(value))
			}
		};
		let start = match &stmt.start {
			Expr::Literal(Literal::Integer(i)) => (*i, None),
			expr => (
				0,
				Some(
					crate::exec::vm::host::out_of_line(|| {
						self.converted(expr, ClauseConversion::Integer)
					})
					.await?,
				),
			),
		};
		let definition = crate::catalog::SequenceDefinition {
			name: name.text().into(),
			batch: batch.0,
			start: start.0,
			timeout: timeout.map(crate::val::BoundedDuration::from),
		};
		let definition = stored(definition)?;
		let definition = self.constant(definition);
		let definition = self.load(definition)?;
		self.set_name(definition, "name", &name);
		for (field, value) in
			[("batch", batch.1), ("start", start.1), ("timeout", evaluated_timeout)]
		{
			if let Some(value) = value {
				self.set_field(definition, field, Operand::Reg(value));
			}
		}
		self.store(StoreInstr::Put {
			route: RouteId::named("SequenceKey"),
			key,
			ty: TypeId::named("SequenceDefinition"),
			value: definition,
		});
		self.delete_range("SeqBatchPrefix", key, false);
		self.delete_range("SeqStatePrefix", key, false);
		// The node's allocator reads definitions through transactions of its
		// own and cannot see this one until it commits, so the transaction
		// records the definition it wrote, and the node drops the allocator it
		// cached for the sequence this one replaces.
		let defined = self.load(sq)?;
		self.store(StoreInstr::SequenceDefinedHere {
			ns,
			db,
			name: defined,
		});
		self.with_service(Service::Sequences, |l| {
			l.store(StoreInstr::EvictSequence {
				ns,
				db,
				name: defined,
			});
			Ok(())
		})?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// `DEFINE API`— `ddl/define/api.rs`, with middleware arguments that
	/// are written as values. The API is looked up by the path it is given
	/// and written at the path's rendering.
	async fn define_api(&mut self, stmt: &DefineApiStatement) -> Lowered<()> {
		// A path is parsed where the body parses it, after its existence check:
		// when the program runs, for one that is evaluated or that the parse
		// refuses, which raises the refusal; a written path the parse admits is
		// parsed here.
		let parsed_path: Option<crate::api::path::Path> =
			written_text(&stmt.path).and_then(|path| path.parse().ok());
		let parsed_at_run = parsed_path.is_none();
		let path: crate::api::path::Path = match parsed_path {
			Some(path) => path,
			None => "/".parse().map_err(|_| NotCovered::internal("the root API path"))?,
		};
		// A method in more than one `FOR` clause is refused once the path is
		// parsed.
		let mut seen = Vec::new();
		let mut duplicate = None;
		for method in stmt.actions.iter().flat_map(|action| &action.methods) {
			if seen.contains(method) {
				duplicate = Some(method.to_string());
				break;
			}
			seen.push(*method);
		}
		let config = api_config(&stmt.config)?;
		let actions = stmt
			.actions
			.iter()
			.map(|action| {
				Ok(crate::catalog::ApiAction {
					methods: action.methods.iter().map(|m| (*m).into()).collect(),
					action: action.action.clone(),
					config: api_config(&action.config)?,
				})
			})
			.collect::<Lowered<Vec<_>>>()?;
		let definition = crate::catalog::ApiDefinition {
			path,
			actions,
			fallback: stmt.fallback.clone(),
			config,
			auth_limit: crate::catalog::auth::AuthLimit::default(),
			comment: comment_text(&stmt.comment),
		};
		let rendered = definition.path.to_string();
		let template = stored(definition.to_stored())?;
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Api,
			base: Base::Db,
		});
		let (ns, db) = self.ids_creating()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let path_name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.path, NameClause::ApiPath))
				.await?;
		let given = path_name.operand;
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("ap", given)])?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("ApiKey"),
			key,
			ty: TypeId::named("StoredApiDefinition"),
			dst: existing,
		});
		let name_reg = self.load(given)?;
		let missing =
			self.exists(&stmt.kind, existing, StatementError::ApAlreadyExists, name_reg)?;
		let at = self.here();
		self.patch(missing, at);
		let parsed = if parsed_at_run {
			let given = self.load(path_name.operand)?;
			let (text, at) = (self.register()?, self.register()?);
			self.store(StoreInstr::ParseApiPath {
				src: given,
				text,
				rendered: at,
			});
			Some((text, at))
		} else {
			None
		};
		if let Some(method) = duplicate {
			let method = self.constant(Value::String(method.into()));
			let detail = self.object(&[("value", path_name.operand), ("method", method)])?;
			self.store(StoreInstr::Raise {
				error: StatementError::ApMethodDuplicate,
				arg: detail,
			});
		}
		// The middleware arguments that are evaluated are, in the order the
		// body evaluates them: the API's, then each action's.
		let middleware =
			crate::exec::vm::host::out_of_line(|| self.evaluated_middleware(&stmt.config)).await?;
		let mut action_middleware = Vec::new();
		for action in &stmt.actions {
			action_middleware.push(
				crate::exec::vm::host::out_of_line(|| self.evaluated_middleware(&action.config))
					.await?,
			);
		}
		let comment = crate::exec::vm::host::out_of_line(|| self.comment(&stmt.comment)).await?;
		let actions_value = match &template {
			Value::Object(api) => api.get("actions").cloned(),
			_ => None,
		};
		let template = self.constant(template);
		let definition = self.load(template)?;
		self.set_comment(definition, &comment);
		if let Some(list) = middleware {
			self.set_path(definition, &["config", "middleware"], Operand::Reg(list));
		}
		if action_middleware.iter().any(Option::is_some) {
			let Some(Value::Array(actions_value)) = actions_value else {
				return Err(NotCovered::internal("stored API actions that are not a list"));
			};
			let actions = self.register()?;
			self.emit(Instr::NewArray {
				dst: actions,
			});
			for (action, list) in actions_value.iter().zip(&action_middleware) {
				let action = self.constant(action.clone());
				let action = self.load(action)?;
				if let Some(list) = list {
					self.set_path(action, &["config", "middleware"], Operand::Reg(*list));
				}
				self.emit(Instr::ArrayPush {
					array: actions,
					value: Operand::Reg(action),
				});
				self.release(action);
			}
			self.set_field(definition, "actions", Operand::Reg(actions));
			self.release(actions);
		}
		let limit = self.register()?;
		self.store(StoreInstr::SessionAuthLimit {
			dst: limit,
		});
		self.set_field(definition, "auth_limit", Operand::Reg(limit));
		let rendered = match parsed {
			Some((text, at)) => {
				self.set_field(definition, "path", Operand::Reg(text));
				self.release(text);
				Operand::Reg(at)
			}
			None => self.constant(Value::String(rendered.into())),
		};
		let stored_at = self.object(&[("ns", ns_op), ("db", db_op), ("ap", rendered)])?;
		self.store(StoreInstr::Put {
			route: RouteId::named("ApiKey"),
			key: stored_at,
			ty: TypeId::named("StoredApiDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// `DEFINE CONFIG` — `ddl/define/config/mod.rs`, whose definition is
	/// written through the store's replace.
	async fn define_config(&mut self, stmt: &DefineConfigStatement) -> Lowered<()> {
		use crate::expr::statements::define::config::ConfigInner;
		let kind = stmt.inner.kind();
		let base = crate::ddl::define::config::config_kind_base(&kind);
		let (cg, definition) = match &stmt.inner {
			ConfigInner::GraphQL(g) => {
				("graphql", crate::catalog::ConfigDefinition::GraphQL(g.clone()))
			}
			ConfigInner::Api(a) => ("api", crate::catalog::ConfigDefinition::Api(api_config(a)?)),
			ConfigInner::Default(d) => (
				"default",
				crate::catalog::ConfigDefinition::Default(crate::catalog::DefaultConfig {
					namespace: written_text(&d.namespace),
					database: written_text(&d.database),
				}),
			),
		};
		if matches!(base, crate::catalog::base::Base::Ns) {
			return Err(NotCovered::internal("a namespace config"));
		}
		let definition = stored(definition.to_stored())?;
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Config(kind),
			base: base.clone().into(),
		});
		let ty = self.constant(Value::String(cg.into()));
		let (route, key) = match base {
			crate::catalog::base::Base::Db => {
				let (ns, db) = self.ids_creating()?;
				(
					"DbConfigKey",
					self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("ty", ty)])?,
				)
			}
			_ => ("RootConfigKey", self.object(&[("ty", ty)])?),
		};
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredConfigDefinition"),
			dst: existing,
		});
		let name_reg = self.load(ty)?;
		let missing =
			self.exists(&stmt.kind, existing, StatementError::CgAlreadyExists, name_reg)?;
		let at = self.here();
		self.patch(missing, at);
		// A default's namespace and database are resolved where the body
		// computes the definition, once the existing one is read.
		let mut defaults = Vec::new();
		if let ConfigInner::Default(d) = &stmt.inner {
			for (field, expr, into) in [
				("namespace", &d.namespace, NameClause::ConfigNamespace),
				("database", &d.database, NameClause::ConfigDatabase),
			] {
				if let (_, Some(value)) =
					crate::exec::vm::host::out_of_line(|| self.optional_name(expr, into)).await?
				{
					defaults.push((field, value));
				}
			}
		}
		let middleware = match &stmt.inner {
			ConfigInner::Api(config) => {
				crate::exec::vm::host::out_of_line(|| self.evaluated_middleware(config)).await?
			}
			_ => None,
		};
		let definition = self.constant(definition);
		let definition = self.load(definition)?;
		for (field, value) in defaults {
			self.set_path(definition, &["Default", field], Operand::Reg(value));
		}
		if let Some(list) = middleware {
			self.set_path(definition, &["Api", "middleware"], Operand::Reg(list));
		}
		self.store(StoreInstr::Replace {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredConfigDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// `DEFINE BUCKET` — `ddl/define/bucket.rs`: the bucket's backend is
	/// connected before the definition is written.
	async fn define_bucket(&mut self, stmt: &DefineBucketStatement) -> Lowered<()> {
		let backend = match &stmt.backend {
			Some(Expr::Literal(Literal::String(s))) => Some(s.clone()),
			_ => None,
		};
		let definition = crate::catalog::BucketDefinition {
			id: None,
			name: Name::text_of(&stmt.name).into(),
			backend,
			permissions: stmt.permissions.clone(),
			readonly: stmt.readonly,
			comment: comment_text(&stmt.comment),
		};
		let definition = stored(definition.to_stored())?;
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Bucket,
			base: Base::Db,
		});
		let name = crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Bucket))
			.await?;
		let (ns, db) = self.ids_creating()?;
		let bu = name.operand;
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("bu", bu)])?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("BucketKey"),
			key,
			ty: TypeId::named("StoredBucketDefinition"),
			dst: existing,
		});
		let missing = self.emit(Instr::JumpIf {
			cond: Operand::Reg(existing),
			truthy: false,
			target: 0,
		});
		match stmt.kind {
			DefineKind::Default => {
				let import = self.store(StoreInstr::JumpIfImport {
					target: 0,
				});
				let shown = self.register()?;
				self.get_field(existing, "name", shown);
				self.store(StoreInstr::Raise {
					error: StatementError::BuAlreadyExists,
					arg: shown,
				});
				let at = self.here();
				self.patch(import, at);
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => self.answer_none()?,
		}
		let at = self.here();
		self.patch(missing, at);
		// The backend is converted, and the bucket connected to it, before the
		// comment is evaluated.
		let evaluated_backend = match &stmt.backend {
			Some(Expr::Literal(Literal::String(_))) | None => None,
			Some(expr) => Some(
				crate::exec::vm::host::out_of_line(|| {
					self.converted(expr, ClauseConversion::Coerced)
				})
				.await?,
			),
		};
		let definition = self.constant(definition);
		let definition = self.load(definition)?;
		self.set_name(definition, "name", &name);
		if let Some(backend) = evaluated_backend {
			self.set_field(definition, "backend", Operand::Reg(backend));
		}
		self.store(StoreInstr::ConnectBucket {
			ns,
			db,
			bucket: definition,
		});
		let comment = crate::exec::vm::host::out_of_line(|| self.comment(&stmt.comment)).await?;
		self.set_comment(definition, &comment);
		self.store(StoreInstr::Put {
			route: RouteId::named("BucketKey"),
			key,
			ty: TypeId::named("StoredBucketDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `DEFINE MODEL` — `ddl/define/model.rs`.
	async fn define_model(&mut self, stmt: &DefineModelStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Model,
			base: Base::Db,
		});
		let (ns, db) = self.ids_creating()?;
		let name = self.constant(Value::String(stmt.name.as_str().into()));
		let version = self.constant(Value::String(stmt.version.as_str().into()));
		let key = self.object(&[
			("ns", Operand::Reg(ns)),
			("db", Operand::Reg(db)),
			("ml", name),
			("vn", version),
		])?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("MlModelKey"),
			key,
			ty: TypeId::named("StoredMlModelDefinition"),
			dst: existing,
		});
		let name_reg = self.load(name)?;
		let missing =
			self.exists(&stmt.kind, existing, StatementError::MlAlreadyExists, name_reg)?;
		let at = self.here();
		self.patch(missing, at);
		let comment = crate::exec::vm::host::out_of_line(|| self.comment(&stmt.comment)).await?;
		let template = crate::catalog::MlModelDefinition {
			name: stmt.name.clone(),
			version: stmt.version.clone(),
			hash: stmt.hash.clone(),
			comment: comment_text(&stmt.comment),
			permissions: stmt.permissions.clone(),
			permissions_body: crate::catalog::LogicalPermissionBody::of(&stmt.permissions),
		};
		let template = stored(template.to_stored())?;
		let template = self.constant(template);
		let definition = self.load(template)?;
		self.set_comment(definition, &comment);
		self.store(StoreInstr::Put {
			route: RouteId::named("MlModelKey"),
			key,
			ty: TypeId::named("StoredMlModelDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE MODEL` — `ddl/remove/model.rs`.
	async fn remove_model(&mut self, stmt: &RemoveModelStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Model,
			base: Base::Db,
		});
		let name = self.constant(Value::String(stmt.name.as_str().into()));
		let version = self.constant(Value::String(stmt.version.as_str().into()));
		let shown = self.constant(Value::String(
			format!("{}<{}>", stmt.name.as_str(), stmt.version.as_str()).into(),
		));
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("ml", name), ("vn", version)])?;
		let definition = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("MlModelKey"),
			key,
			ty: TypeId::named("StoredMlModelDefinition"),
			dst: definition,
		});
		let shown = self.load(shown)?;
		self.must_exist(stmt.if_exists, definition, StatementError::MlNotFound, shown)?;
		self.store(StoreInstr::Delete {
			route: RouteId::named("MlModelKey"),
			key,
		});
		self.store(StoreInstr::DeleteModelArtifact {
			model: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `DEFINE MODULE` — `ddl/define/module.rs`: an overwrite evicts the
	/// process's runtime for the new executable, and the runtime is loaded
	/// once the definition is written.
	async fn define_module(&mut self, stmt: &DefineModuleStatement) -> Lowered<()> {
		let storage = crate::catalog::ModuleName::try_from(stmt)
			.map_err(|_| NotCovered::internal("a module name the body refuses"))?
			.get_storage_name();
		let definition = crate::catalog::ModuleDefinition {
			name: stmt.name.clone(),
			executable: stmt.executable.clone().into(),
			unsigned: stmt.unsigned,
			comment: comment_text(&stmt.comment),
			permissions: stmt.permissions.clone(),
			permissions_body: crate::catalog::LogicalPermissionBody::of(&stmt.permissions),
		};
		let definition = definition.to_stored();
		let stored_name = definition
			.get_storage_name()
			.map_err(|_| NotCovered::internal("a module the catalog cannot name"))?;
		let definition = stored(definition)?;
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Module,
			base: Base::Db,
		});
		let (ns, db) = self.ids_creating()?;
		let md = self.constant(Value::String(storage.into()));
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("md", md)])?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("ModuleKey"),
			key,
			ty: TypeId::named("StoredModuleDefinition"),
			dst: existing,
		});
		let definition = self.constant(definition);
		let definition = self.load(definition)?;
		let missing = self.emit(Instr::JumpIf {
			cond: Operand::Reg(existing),
			truthy: false,
			target: 0,
		});
		match stmt.kind {
			DefineKind::Default => {
				let import = self.store(StoreInstr::JumpIfImport {
					target: 0,
				});
				let name_reg = self.load(md)?;
				self.store(StoreInstr::Raise {
					error: StatementError::MdAlreadyExists,
					arg: name_reg,
				});
				let at = self.here();
				self.patch(import, at);
			}
			DefineKind::Overwrite => {
				self.store(StoreInstr::EvictModule {
					ns,
					db,
					module: definition,
				});
			}
			DefineKind::IfNotExists => self.answer_none()?,
		}
		let at = self.here();
		self.patch(missing, at);
		let comment = crate::exec::vm::host::out_of_line(|| self.comment(&stmt.comment)).await?;
		self.set_comment(definition, &comment);
		let stored_name = self.constant(Value::String(stored_name.into()));
		let stored_at = self.object(&[
			("ns", Operand::Reg(ns)),
			("db", Operand::Reg(db)),
			("md", stored_name),
		])?;
		self.store(StoreInstr::Put {
			route: RouteId::named("ModuleKey"),
			key: stored_at,
			ty: TypeId::named("StoredModuleDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.store(StoreInstr::LoadModule {
			ns,
			db,
			module: definition,
		});
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// `DEFINE ACCESS` — `ddl/define/access.rs`, with its clauses written as
	/// values. On a namespace or database the session's namespace and database
	/// are created when missing.
	async fn define_access(&mut self, stmt: &DefineAccessStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Actor,
			base: stmt.base,
		});
		let AccessLowered {
			definition,
			fills,
			keys,
		} = crate::exec::vm::host::out_of_line(|| self.access_definition(stmt)).await?;
		let name = crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Access))
			.await?;
		// A written redacted key refuses the statement here, an evaluated one
		// where the body checks it, once the name is resolved. ES512 is
		// refused unless the statement imports or overwrites an ES512 method,
		// which only the catalog read decides.
		if crate::ddl::define::access::define_access_statement_reject_redacted(&definition).is_err()
		{
			return Err(NotCovered::refused(crate::exec::vm::plan::Refusal::AccessRedactedKey {
				ac: definition.name.to_string(),
			}));
		}
		let uses_es512 = definition.access_type.uses_es512();
		if !keys.is_empty() {
			let shown = self.load(name.operand)?;
			for key in keys {
				self.store(StoreInstr::AccessCheck {
					check: AccessCheck::Unredacted {
						key,
						name: shown,
					},
				});
			}
			self.release(shown);
		}
		let definition = stored(definition.to_stored())?;
		let ac = name.operand;
		let (route, scope, exists) = match stmt.base {
			Base::Root => {
				("RootAccessMethodKey", Vec::new(), StatementError::AccessRootAlreadyExists)
			}
			Base::Ns => {
				let ns = self.namespace_id_creating()?;
				(
					"NsAccessMethodKey",
					vec![("ns", Operand::Reg(ns))],
					StatementError::AccessNsAlreadyExists,
				)
			}
			Base::Db => {
				let (ns, db) = self.ids_creating()?;
				(
					"DbAccessMethodKey",
					vec![("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))],
					StatementError::AccessDbAlreadyExists,
				)
			}
		};
		let mut fields = scope;
		fields.push(("ac", ac));
		let key = self.object(&fields)?;
		let existing = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredAccessDefinition"),
			dst: existing,
		});
		let missing = self.emit(Instr::JumpIf {
			cond: Operand::Reg(existing),
			truthy: false,
			target: 0,
		});
		match stmt.kind {
			DefineKind::Default => {
				let import = self.store(StoreInstr::JumpIfImport {
					target: 0,
				});
				let shown = self.register()?;
				self.get_field(existing, "name", shown);
				self.missing_at_base(false, stmt.base, ("ac", Operand::Reg(shown)), exists)?;
				let at = self.here();
				self.patch(import, at);
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => self.answer_none()?,
		}
		let at = self.here();
		self.patch(missing, at);
		// ES512 is refused once the statement goes on to write: after the
		// existence branch, so `IF NOT EXISTS` over a method answers `NONE`.
		if uses_es512 {
			self.store(StoreInstr::AccessCheck {
				check: AccessCheck::RejectEs512 {
					existing,
					overwrite: stmt.kind == DefineKind::Overwrite,
				},
			});
		}
		let definition = self.constant(definition);
		let definition = self.load(definition)?;
		self.set_name(definition, "name", &name);
		for (path, value) in fills {
			self.set_path(definition, &path, Operand::Reg(value));
		}
		self.store(StoreInstr::Put {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredAccessDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

/// An index's columns when each is written as a path from a field, as the
/// body reads them (`ddl::scalar::fields`): a literal subscript stays, a
/// string one names a field. `None` when one is evaluated — a subscript that
/// is not a literal, or a `type::field(s)` call.
fn written_index_columns(cols: &[Expr]) -> Option<Vec<crate::expr::Idiom>> {
	use crate::expr::Part;
	cols.iter()
		.map(|col| {
			let Expr::Idiom(idiom) = col else {
				return None;
			};
			if !matches!(idiom.0.first(), Some(Part::Field(_))) {
				return None;
			}
			let parts = idiom
				.0
				.iter()
				.map(|part| match part {
					Part::Value(Expr::Literal(Literal::String(s))) => Some(Part::Field(s.clone())),
					Part::Value(Expr::Literal(_)) => Some(part.clone()),
					Part::Value(_) => None,
					other => Some(other.clone()),
				})
				.collect::<Option<Vec<_>>>()?;
			Some(crate::expr::Idiom(parts))
		})
		.collect()
}

/// A clause's value when it is written, a literal: `None` when it is
/// evaluated.
fn literal_value(expr: &Expr) -> Option<Value> {
	match expr {
		Expr::Literal(literal) => literal.as_static_value(),
		_ => None,
	}
}

/// What `DEFINE FIELD` and `ALTER FIELD` refuse of a field that is `INLINE`, in
/// the order they check it, for the table as the catalog holds it
/// (`table`, `None` when it does not exist yet), the field's written name,
/// its path length and whether it is computed (`field`), and the number of
/// other `INLINE` fields the table carries (`others`).
fn inline_field_refusal(
	table: Option<&crate::catalog::TableDefinition>,
	(name, path_len, computed): (&str, usize, bool),
	others: usize,
) -> Option<FieldRefusal> {
	let thrown = |message: &str| Some(FieldRefusal::Thrown(message.to_owned()));
	if !table.is_some_and(|tb| matches!(tb.table_type, crate::catalog::TableType::Relation(_))) {
		return thrown(
			"INLINE requires an existing TYPE RELATION table: the value is embedded into the \
			 edges' adjacency payloads",
		);
	}
	if matches!(name, "id" | "in" | "out") {
		return thrown(
			"the `id`, `in` and `out` fields already live in the adjacency keys and cannot be \
			 INLINE",
		);
	}
	if path_len != 1 {
		return thrown("only a top-level field can be INLINE");
	}
	if computed {
		return thrown(
			"a COMPUTED field cannot be INLINE: its value is derived at read time, so there is \
			 nothing stored to embed",
		);
	}
	if others >= u8::MAX as usize {
		return thrown(
			"a table cannot carry more than 255 INLINE fields: the adjacency payload's value \
			 count is a single byte",
		);
	}
	None
}

/// The refusal of an `INLINE` cache cap above the hard bound, as `DEFINE
/// TABLE` and `ALTER TABLE` raise it.
fn inline_cap_refusal() -> FieldRefusal {
	FieldRefusal::Thrown(format!(
		"an INLINE cache cap cannot exceed {}",
		crate::idx::inline_cache::MAX_INLINE_CACHE_CAP
	))
}

/// The refusal `error`, raised by the body of `what`, as a program raises it
/// where the body does. An error that is no refusal of a definition leaves
/// the statement to fail with the reason it is not covered.
/// The name evaluated clause `expr` gives, where `planner` knows its value
/// when it plans: a parameter its context binds, or an expression that folds
/// to a constant. `Some(Err(()))` where that value is no name, which the
/// program's evaluation of it raises; `None` where only the run knows it.
pub(in crate::exec::vm) fn plan_time_name(
	planner: &crate::exec::planner::Planner<'_>,
	expr: &Expr,
	into: NameClause,
) -> Option<Result<String, ()>> {
	let value = match expr {
		Expr::Param(param) if !planner.param_is_shadowed(param.as_str()) => {
			planner.frozen().value(param.as_str())?.clone()
		}
		_ => {
			let mut logical = crate::expr::logical::lower_expr(expr.clone()).ok()?;
			let capabilities = planner.frozen().get_capabilities();
			crate::exec::planner::util::fold_expressions(
				&mut logical,
				planner.function_registry(),
				&capabilities,
				None,
				false,
				false,
			);
			crate::exec::planner::util::try_literal_to_value(&logical)?
		}
	};
	Some(value.coerce_naming::<String>(into.label()).map_err(|_| ()))
}

fn refusal_of(error: &anyhow::Error, what: &str) -> Lowered<FieldRefusal> {
	FieldRefusal::of(error).ok_or_else(|| {
		NotCovered::internal(format!("{what} whose checks failed unexpectedly: {error}"))
	})
}

/// The plan-time refusal of a field name against the record id components of
/// its table, from the check that judged it; `None` when the name passes. A
/// check that fails with anything but a refusal leaves the statement not
/// covered.
fn key_name_refusal(checked: anyhow::Result<()>) -> Lowered<Option<FieldRefusal>> {
	match checked {
		Ok(()) => Ok(None),
		Err(error) => FieldRefusal::of(&error).map(Some).ok_or_else(|| {
			NotCovered::internal(format!("a field name whose checks failed unexpectedly: {error}"))
		}),
	}
}

fn access_algorithm(alg: crate::expr::Algorithm) -> crate::catalog::Algorithm {
	use crate::catalog::Algorithm as A;
	use crate::expr::Algorithm;
	match alg {
		Algorithm::EdDSA => A::EdDSA,
		Algorithm::Es256 => A::Es256,
		Algorithm::Es384 => A::Es384,
		Algorithm::Es512 => A::Es512,
		Algorithm::Hs256 => A::Hs256,
		Algorithm::Hs384 => A::Hs384,
		Algorithm::Hs512 => A::Hs512,
		Algorithm::Ps256 => A::Ps256,
		Algorithm::Ps384 => A::Ps384,
		Algorithm::Ps512 => A::Ps512,
		Algorithm::Rs256 => A::Rs256,
		Algorithm::Rs384 => A::Rs384,
		Algorithm::Rs512 => A::Rs512,
	}
}

/// An access method's definition as lowering forms it: its written clauses
/// read, and the stored fields its evaluated clauses fill once the program
/// has converted them.
struct AccessLowered {
	definition: crate::catalog::AccessDefinition,
	/// The stored definition's fields, each a path from its root, and the
	/// register the evaluated value for it is in.
	fills: Vec<(Vec<&'static str>, Reg)>,
	/// The evaluated keys, checked against the redaction placeholder once
	/// the name is resolved.
	keys: Vec<Reg>,
}

impl Lowering<'_, '_> {
	/// An access method's definition from its clauses, in the order the body
	/// reads them (`define_access_statement_to_definition`): the durations,
	/// the comment, then the access type's keys, URLs and audiences. A written
	/// clause is read here, and what the body refuses of one is declined. An
	/// evaluated clause is evaluated where the body evaluates it and converted
	/// as the body converts it, and the checks the body makes between clauses
	/// are made where it makes them.
	async fn access_definition(&mut self, stmt: &DefineAccessStatement) -> Lowered<AccessLowered> {
		use crate::catalog;
		use crate::expr::AccessType;
		let mut out = AccessLowered {
			definition: catalog::AccessDefinition {
				name: Name::text_of(&stmt.name).into(),
				base: stmt.base.into(),
				access_type: catalog::AccessType::Jwt(catalog::JwtAccess {
					verify: catalog::JwtAccessVerify::Jwks(catalog::JwtAccessVerifyJwks {
						url: String::new(),
					}),
					issue: None,
					audience: None,
				}),
				authenticate: stmt.authenticate.clone(),
				context: stmt.context.clone(),
				signup: None,
				signin: None,
				grant_duration: None,
				token_duration: None,
				session_duration: None,
				comment: None,
			},
			fills: Vec::new(),
			keys: Vec::new(),
		};
		out.definition.grant_duration = crate::exec::vm::host::out_of_line(|| {
			self.access_duration(&stmt.duration.grant, "grant_duration", &mut out)
		})
		.await?;
		let token = crate::exec::vm::host::out_of_line(|| {
			self.access_duration(&stmt.duration.token, "token_duration", &mut out)
		})
		.await?;
		if matches!(&stmt.access_type, AccessType::Record(_)) {
			match out.fills.last() {
				Some((path, token)) if path == &["token_duration"] => {
					self.store(StoreInstr::AccessCheck {
						check: AccessCheck::RecordToken {
							token: *token,
						},
					});
				}
				// A written `NONE` is evaluated and checked where the body checks the
				// duration, so the statement fails with the body's error there and
				// not before an earlier clause has been evaluated.
				_ if token.is_none() => {
					let token = crate::exec::vm::host::out_of_line(|| {
						self.converted(&stmt.duration.token, ClauseConversion::Duration)
					})
					.await?;
					self.store(StoreInstr::AccessCheck {
						check: AccessCheck::RecordToken {
							token,
						},
					});
				}
				_ => {}
			}
		}
		out.definition.token_duration = token;
		out.definition.session_duration = crate::exec::vm::host::out_of_line(|| {
			self.access_duration(&stmt.duration.session, "session_duration", &mut out)
		})
		.await?;
		// A written comment the body refuses is cast when the statement runs,
		// which raises the body's error there.
		out.definition.comment =
			match literal_value(&stmt.comment).map(|comment| comment.cast_to::<Option<String>>()) {
				Some(Ok(comment)) => comment,
				_ => {
					let comment =
						crate::exec::vm::host::out_of_line(|| self.comment(&stmt.comment)).await?;
					if let Operand::Reg(reg) = comment.operand {
						out.fills.push((vec!["comment"], reg));
					}
					None
				}
			};
		out.definition.access_type = match &stmt.access_type {
			AccessType::Record(record) => {
				let jwt = self
					.access_jwt(&record.jwt, &["access_type", "Record", "jwt"], &mut out)
					.await?;
				let bearer = match &record.bearer {
					Some(bearer) => Some(
						self.access_bearer(
							bearer,
							&["access_type", "Record", "bearer", "jwt"],
							&mut out,
						)
						.await?,
					),
					None => None,
				};
				catalog::AccessType::Record(catalog::RecordAccess {
					signup: record.signup.as_ref().map(catalog::ExprText::new),
					signin: record.signin.as_ref().map(catalog::ExprText::new),
					jwt,
					bearer,
				})
			}
			AccessType::Jwt(access) => catalog::AccessType::Jwt(
				crate::exec::vm::host::out_of_line(|| {
					self.access_jwt(access, &["access_type", "Jwt"], &mut out)
				})
				.await?,
			),
			AccessType::Bearer(access) => catalog::AccessType::Bearer(
				crate::exec::vm::host::out_of_line(|| {
					self.access_bearer(access, &["access_type", "Bearer", "jwt"], &mut out)
				})
				.await?,
			),
		};
		(out.definition.signup, out.definition.signin) = match &stmt.access_type {
			AccessType::Record(record) => (record.signup.clone(), record.signin.clone()),
			_ => (None, None),
		};
		if let Err(e) = crate::ddl::define::access::validate_access_context(
			&out.definition.base,
			&out.definition.access_type,
			stmt.context.as_ref(),
		) {
			return Err(match e.downcast_ref::<crate::exec::Error>() {
				Some(crate::exec::Error::AccessContextNotReadOnly) => {
					NotCovered::refused(crate::exec::vm::plan::Refusal::AccessContextNotReadOnly)
				}
				Some(crate::exec::Error::AccessContextRequiresRecord) => {
					NotCovered::refused(crate::exec::vm::plan::Refusal::AccessContextRequiresRecord)
				}
				// `validate_access_context` raises nothing else.
				_ => NotCovered::internal(format!("an access context refused otherwise: {e}")),
			});
		}
		Ok(out)
	}

	/// A `DURATION FOR …` clause, filling `field` when it is evaluated.
	async fn access_duration(
		&mut self,
		expr: &Expr,
		field: &'static str,
		out: &mut AccessLowered,
	) -> Lowered<Option<std::time::Duration>> {
		let (written, evaluated) =
			crate::exec::vm::host::out_of_line(|| self.duration(expr)).await?;
		if let Some(reg) = evaluated {
			out.fills.push((vec![field], reg));
		}
		Ok(written)
	}

	/// A duration clause (`scalar` cast to an optional duration): its value
	/// when it is written, and when it is evaluated, none and the register the
	/// program converts it into, here. A written value the body refuses is
	/// converted as an evaluated one is, and raises the body's error.
	async fn duration(
		&mut self,
		expr: &Expr,
	) -> Lowered<(Option<std::time::Duration>, Option<Reg>)> {
		// A written value the body refuses is converted when the statement
		// runs, which raises the body's error there.
		if let Some(Ok(written)) =
			literal_value(expr).map(|value| value.cast_to::<Option<crate::val::Duration>>())
		{
			return Ok((written.map(|d| d.0), None));
		}
		let reg =
			crate::exec::vm::host::out_of_line(|| self.converted(expr, ClauseConversion::Duration))
				.await?;
		Ok((None, Some(reg)))
	}

	/// An evaluated clause, converted as the body converts `clause`.
	async fn converted(&mut self, expr: &Expr, clause: ClauseConversion) -> Lowered<Reg> {
		let reg = crate::exec::vm::host::out_of_line(|| self.evaluated(expr)).await?;
		self.store(StoreInstr::Convert {
			src: reg,
			dst: reg,
			clause,
		});
		Ok(reg)
	}

	/// A string clause of a JWT configuration at `path` under `at`, and the
	/// register holding it when it is evaluated.
	async fn access_string(
		&mut self,
		expr: &Expr,
		(clause, _what): (ClauseConversion, &str),
		(at, path): (&[&'static str], &[&'static str]),
		out: &mut AccessLowered,
	) -> Lowered<(String, Option<Reg>)> {
		// A written value the body refuses is converted when the statement
		// runs, which raises the body's error there.
		if let Some(value) = literal_value(expr) {
			let written = match clause {
				ClauseConversion::Coerced => value.coerce_to::<String>().ok(),
				_ => value.cast_to::<String>().ok(),
			};
			if let Some(written) = written {
				return Ok((written, None));
			}
		}
		let reg = crate::exec::vm::host::out_of_line(|| self.converted(expr, clause)).await?;
		out.fills.push((at.iter().chain(path).copied().collect(), reg));
		Ok((String::new(), Some(reg)))
	}

	/// A JWT configuration at `at` in the stored definition
	/// (`convert_jwt_access`).
	async fn access_jwt(
		&mut self,
		access: &crate::expr::access_type::JwtAccess,
		at: &[&'static str],
		out: &mut AccessLowered,
	) -> Lowered<crate::catalog::JwtAccess> {
		use crate::catalog;
		use crate::expr::access_type::JwtAccessVerify;
		let (verify, verify_key) = match &access.verify {
			JwtAccessVerify::Key(k) => {
				let (key, reg) = self
					.access_string(
						&k.key,
						(ClauseConversion::Coerced, "key"),
						(at, &["verify", "Key", "key"]),
						out,
					)
					.await?;
				out.keys.extend(reg);
				let verify = catalog::JwtAccessVerify::Key(catalog::JwtAccessVerifyKey {
					alg: access_algorithm(k.alg),
					key,
				});
				(verify, reg)
			}
			JwtAccessVerify::Jwks(j) => {
				let (url, _) = self
					.access_string(
						&j.url,
						(ClauseConversion::Cast, "url"),
						(at, &["verify", "Jwks", "url"]),
						out,
					)
					.await?;
				(
					catalog::JwtAccessVerify::Jwks(catalog::JwtAccessVerifyJwks {
						url,
					}),
					None,
				)
			}
		};
		let (issue, issue_key) = match &access.issue {
			None => (None, None),
			Some(x) => {
				let (key, reg) = self
					.access_string(
						&x.key,
						(ClauseConversion::Cast, "issuer key"),
						(at, &["issue", "key"]),
						out,
					)
					.await?;
				out.keys.extend(reg);
				(
					Some(catalog::JwtAccessIssue {
						alg: access_algorithm(x.alg),
						key,
					}),
					reg,
				)
			}
		};
		if let (catalog::JwtAccessVerify::Key(ver), Some(iss)) = (&verify, &issue)
			&& ver.alg.is_symmetric()
		{
			match (verify_key, issue_key) {
				(None, None) if ver.key == iss.key => {}
				(verify, issue) => {
					let mut written = |key: &str, reg: Option<Reg>| match reg {
						Some(reg) => Ok(reg),
						None => {
							let key = self.constant(Value::String(key.into()));
							self.load(key)
						}
					};
					let verify = written(&ver.key, verify)?;
					let issue = written(&iss.key, issue)?;
					self.store(StoreInstr::AccessCheck {
						check: AccessCheck::SymmetricKeys {
							alg: ver.alg,
							verify,
							issue,
						},
					});
				}
			}
		}
		let audience = match &access.audience {
			None => None,
			Some(exprs)
				if exprs.iter().all(|expr| {
					literal_value(expr).is_some_and(|value| value.coerce_to::<String>().is_ok())
				}) =>
			{
				Some(
					exprs
						.iter()
						.filter_map(|expr| literal_value(expr)?.coerce_to::<String>().ok())
						.collect(),
				)
			}
			Some(exprs) => {
				let list = self.register()?;
				self.emit(Instr::NewArray {
					dst: list,
				});
				for expr in exprs {
					let written =
						literal_value(expr).and_then(|value| value.coerce_to::<String>().ok());
					let entry = match written {
						Some(value) => self.constant(Value::String(value.into())),
						None => Operand::Reg(
							crate::exec::vm::host::out_of_line(|| {
								self.converted(expr, ClauseConversion::Coerced)
							})
							.await?,
						),
					};
					self.emit(Instr::ArrayPush {
						array: list,
						value: entry,
					});
				}
				out.fills.push((at.iter().chain(&["audience"]).copied().collect(), list));
				Some(Vec::new())
			}
		};
		Ok(catalog::JwtAccess {
			verify,
			issue,
			audience,
		})
	}

	/// A bearer configuration whose JWT configuration is at `at`
	/// (`convert_bearer_access`).
	async fn access_bearer(
		&mut self,
		access: &crate::expr::access_type::BearerAccess,
		at: &[&'static str],
		out: &mut AccessLowered,
	) -> Lowered<crate::catalog::BearerAccess> {
		use crate::catalog;
		use crate::expr::access_type::{BearerAccessSubject, BearerAccessType};
		Ok(catalog::BearerAccess {
			kind: match access.kind {
				BearerAccessType::Bearer => catalog::BearerAccessType::Bearer,
				BearerAccessType::Refresh => catalog::BearerAccessType::Refresh,
			},
			subject: match access.subject {
				BearerAccessSubject::Record => catalog::BearerAccessSubject::Record,
				BearerAccessSubject::User => catalog::BearerAccessSubject::User,
			},
			jwt: crate::exec::vm::host::out_of_line(|| self.access_jwt(&access.jwt, at, out))
				.await?,
		})
	}
}

impl Lowering<'_, '_> {
	/// `ALTER TABLE` — `ddl/alter/table.rs`. The table as altered is validated
	/// as the program runs, against the table it found.
	async fn alter_table(&mut self, stmt: &AlterTableStatement) -> Lowered<()> {
		use crate::expr::statements::alter::AlterKind;
		let table_name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Table)).await?;
		let name_op = table_name.operand;
		let cap_refused = [&stmt.inline_edges_cap, &stmt.inline_refs_cap].into_iter().any(|cap| {
			matches!(cap, AlterKind::Set(cap) if *cap > crate::idx::inline_cache::MAX_INLINE_CACHE_CAP)
		});
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Table,
			base: Base::Db,
		});
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let tb = name_op;
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let table = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			dst: table,
		});
		let tb_reg = self.load(tb)?;
		self.must_exist(stmt.if_exists, table, StatementError::TbNotFound, tb_reg)?;
		// What the rewrite is compared against, read before it.
		let previous = self.load(Operand::Reg(table))?;
		let (old_changefeed, old_edges, old_refs) =
			(self.register()?, self.register()?, self.register()?);
		self.get_field(table, "changefeed", old_changefeed);
		self.get_field(table, "inline_edges_cap", old_edges);
		self.get_field(table, "inline_refs_cap", old_refs);
		if cap_refused {
			self.store(StoreInstr::RefuseField {
				refusal: inline_cap_refusal(),
			});
		}
		if let Some(kind) = &stmt.kind {
			let decoded_type = kind.to_vm_value().map_err(|e| {
				NotCovered::internal(format!("table type the codec does not convert: {e}"))
			})?;
			let decoded_type = self.constant(decoded_type);
			self.set_field(table, "table_type", decoded_type);
		}
		match stmt.schemafull {
			AlterKind::Set(()) => {
				let yes = self.constant(Value::Bool(true));
				self.set_field(table, "schemafull", yes);
			}
			AlterKind::Drop => {
				let no = self.constant(Value::Bool(false));
				self.set_field(table, "schemafull", no);
			}
			AlterKind::None => {}
		}
		if let Some(permissions) = &stmt.permissions {
			let stored = crate::catalog::StoredPermissions::from_runtime(permissions)
				.into_vm_value()
				.map_err(|e| {
					NotCovered::internal(format!("permissions the codec does not convert: {e}"))
				})?;
			let stored = self.constant(stored);
			self.set_field(table, "permissions", stored);
		}
		let changefeed_set = matches!(stmt.changefeed, AlterKind::Set(_));
		match &stmt.changefeed {
			AlterKind::Set(cf) => {
				let cf = self.constant(stored(*cf)?);
				self.set_field(table, "changefeed", cf);
			}
			AlterKind::Drop => {
				let none = self.constant(Value::None);
				self.set_field(table, "changefeed", none);
			}
			AlterKind::None => {}
		}
		match &stmt.comment {
			AlterKind::Set(comment) => {
				let comment = self.constant(Value::String(comment.as_str().into()));
				self.set_field(table, "comment", comment);
			}
			AlterKind::Drop => {
				let none = self.constant(Value::None);
				self.set_field(table, "comment", none);
			}
			AlterKind::None => {}
		}
		for (field, cap) in [
			("inline_edges_cap", &stmt.inline_edges_cap),
			("inline_refs_cap", &stmt.inline_refs_cap),
		] {
			let value = match cap {
				AlterKind::Set(cap) => Value::from(*cap as i64),
				AlterKind::Drop => Value::None,
				AlterKind::None => continue,
			};
			let value = self.constant(value);
			self.set_field(table, field, value);
		}
		// The body validates the table as altered against the table as it
		// was, then forms the relation's `in` and `out` fields.
		self.store(StoreInstr::CheckTableAlter {
			previous,
			updated: table,
		});
		if let Some(crate::catalog::TableType::Relation(rel)) = &stmt.kind {
			let relation_table_name = crate::val::TableName::new(table_name.text());
			for (field, tables) in [("in", &rel.from), ("out", &rel.to)] {
				let fd = crate::catalog::FieldDefinition {
					name: crate::expr::Idiom::field(field),
					table: relation_table_name.clone(),
					field_kind: Some(crate::expr::Kind::Record(tables.clone())),
					..Default::default()
				};
				let value = stored(fd.to_stored())?;
				let value = self.constant(value);
				let value = self.load(value)?;
				self.set_name(value, "table", &table_name);
				let fd = self.constant(Value::String(field.into()));
				let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("fd", fd)])?;
				self.store(StoreInstr::Put {
					route: RouteId::named("FieldKey"),
					key,
					ty: TypeId::named("StoredFieldDefinition"),
					value,
				});
				self.release(key);
				self.release(value);
				self.stamp(ns, db, tb)?;
			}
			let fields_ts = self.register()?;
			self.store(StoreInstr::NewUuid {
				dst: fields_ts,
				draw: 0,
				reserved: None,
			});
			self.set_field(table, "cache_fields_ts", Operand::Reg(fields_ts));
		}
		// A changefeed replacing another is recorded, as the table rewritten.
		if changefeed_set {
			let first = self.emit(Instr::JumpIf {
				cond: Operand::Reg(old_changefeed),
				truthy: false,
				target: 0,
			});
			self.store(StoreInstr::BufferTableChange {
				ns,
				db,
				tb: tb_reg,
				definition: table,
			});
			let at = self.here();
			self.patch(first, at);
		}
		if stmt.compact {
			self.store(StoreInstr::Compact {
				route: RouteId::named("TblRoot"),
				key,
			});
		}
		// `replace_tb`.
		self.store(StoreInstr::Lock {
			route: RouteId::named("TableKey"),
			key,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			value: table,
		});
		self.stamp(ns, db, tb)?;
		if changefeed_set {
			let fence = self.object(&[("ns", ns_op), ("db", db_op)])?;
			self.store(StoreInstr::Increment {
				route: RouteId::named("ChangefeedFenceKey"),
				key: fence,
			});
		}
		// A cache built under one cap regime is not trusted under another.
		for (old, field, route) in [
			(old_edges, "inline_edges_cap", "EdgeCachePrefix"),
			(old_refs, "inline_refs_cap", "RefCachePrefix"),
		] {
			let new = self.register()?;
			self.get_field(table, field, new);
			let same = self.register()?;
			self.emit(Instr::Compare {
				op: crate::exec::vm::program::CompareOp::Equal,
				left: Operand::Reg(old),
				right: Operand::Reg(new),
				dst: same,
			});
			let skip = self.emit(Instr::JumpIf {
				cond: Operand::Reg(same),
				truthy: true,
				target: 0,
			});
			self.store(StoreInstr::DeleteRange {
				route: RouteId::named(route),
				key,
			});
			let at = self.here();
			self.patch(skip, at);
			self.release(same);
			self.release(new);
		}
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// `ALTER FIELD` — `ddl/alter/field.rs`, on a table that is not a
	/// LIGHTWEIGHT relation, for a field that is `INLINE` only where the
	/// planned catalog admits it. The stored field, read when the statement runs, has each altered
	/// clause replaced, and the definition that forms is checked as the body
	/// checks it — the `id` field's restrictions, the record id components it names, `INLINE`'s,
	/// then `FLEXIBLE`'s, and the stored records against the record id the type names — before
	/// it is written.
	async fn alter_field(&mut self, stmt: &AlterFieldStatement) -> Lowered<()> {
		use crate::expr::statements::alter::AlterKind;
		use crate::expr::statements::alter::field::AlterDefault;
		let written_name = match &stmt.name {
			Expr::Idiom(name) => canonical_field_name(name),
			_ => None,
		};
		let (Some(name), Some(table), false) =
			(written_name, written_text(&stmt.what), self.planner.is_late_bound())
		else {
			return crate::exec::vm::host::out_of_line(|| self.alter_field_resolved(stmt)).await;
		};
		let table = crate::val::TableName::new(table);
		let fd = name.to_raw_string();
		let DdlChecks::Field {
			existing_table,
			existing_field,
			fields: table_fields,
			key_refusal,
		} = self.ddl_checks()?
		else {
			return crate::exec::vm::host::out_of_line(|| self.alter_field_resolved(stmt)).await;
		};
		// The body refuses an alteration of a lightweight relation's field once
		// it has found the field; the program raises that where the body does.
		let lightweight_table = existing_table.as_ref().is_some_and(|tb| {
			crate::kvs::lightweight::lightweight_relation(&tb.table_type).is_some()
		});
		let was_inline = existing_field.as_ref().is_some_and(|f| f.inline);
		let will_be_inline = match stmt.inline {
			AlterKind::Set(()) => true,
			AlterKind::Drop => false,
			AlterKind::None => was_inline,
		};

		// The altered clauses in their stored form.
		let mut altered = crate::catalog::FieldDefinition {
			name: name.clone(),
			table: table.clone(),
			..Default::default()
		};
		let mut fields: Vec<&str> = Vec::new();
		// The type and the names of the record id components it carries are set
		// together, so the stored type text carries the names.
		match &stmt.kind {
			AlterKind::Set(kind) => {
				let names = match &stmt.key_names {
					AlterKind::Set(names) => Some(names.clone()),
					AlterKind::Drop | AlterKind::None => None,
				};
				altered.set_type(Some(kind.clone()), names);
			}
			AlterKind::Drop => {}
			AlterKind::None => {}
		}
		if !matches!(stmt.kind, AlterKind::None) {
			fields.push("field_kind");
		}
		match stmt.flexible {
			AlterKind::Set(()) => {
				altered.flexible = true;
				fields.push("flexible");
			}
			AlterKind::Drop => {
				altered.flexible = false;
				fields.push("flexible");
			}
			AlterKind::None => {}
		}
		match stmt.readonly {
			AlterKind::Set(()) => {
				altered.readonly = true;
				fields.push("readonly");
			}
			AlterKind::Drop => fields.push("readonly"),
			AlterKind::None => {}
		}
		match stmt.inline {
			AlterKind::Set(()) => {
				altered.inline = true;
				fields.push("inline");
			}
			AlterKind::Drop => {
				altered.inline = false;
				fields.push("inline");
			}
			AlterKind::None => {}
		}
		for (clause, slot, field) in [
			(&stmt.value, &mut altered.value, "value"),
			(&stmt.assert, &mut altered.assert, "assert"),
		] {
			match clause {
				AlterKind::Set(expr) => *slot = Some(expr.clone()),
				AlterKind::Drop => {}
				AlterKind::None => continue,
			}
			fields.push(field);
		}
		match &stmt.default {
			AlterDefault::None => {}
			AlterDefault::Drop => fields.push("default"),
			AlterDefault::Always(expr) => {
				altered.default = crate::catalog::DefineDefault::Always(expr.clone());
				fields.push("default");
			}
			AlterDefault::Set(expr) => {
				altered.default = crate::catalog::DefineDefault::Set(expr.clone());
				fields.push("default");
			}
		}
		if let Some(permissions) = &stmt.permissions {
			altered.select_permission = permissions.select.clone();
			altered.create_permission = permissions.create.clone();
			altered.update_permission = permissions.update.clone();
			fields.extend(["select_permission", "create_permission", "update_permission"]);
		}
		match &stmt.comment {
			AlterKind::Set(comment) => {
				altered.comment = Some(comment.clone());
				fields.push("comment");
			}
			AlterKind::Drop => fields.push("comment"),
			AlterKind::None => {}
		}
		match &stmt.reference {
			AlterKind::Set(reference) => {
				altered.reference = Some(reference.clone());
				fields.push("reference");
			}
			AlterKind::Drop => fields.push("reference"),
			AlterKind::None => {}
		}
		// The record id structure the statement gives the table, when it names
		// components: the records are checked against it before it is written.
		let scanned = altered.live_key().cloned();
		let altered = match stored(altered.to_stored())? {
			Value::Object(altered) => altered,
			other => return Err(NotCovered::internal(format!("a stored field that is {other:?}"))),
		};
		// An altered `TYPE` is written as `DEFINE FIELD` writes one: with the
		// sub-field definitions it implies, derived from the fields already
		// defined, and without the ones the kind it replaces implied that go
		// with it. The stored field the statement finds, which the catalog it
		// is lowered against holds, decides the clauses the statement leaves.
		let kind_altered = !matches!(stmt.kind, AlterKind::None);
		let mut nested = Vec::new();
		let mut dropped = Vec::new();
		if kind_altered && let Some(previous) = existing_field.as_deref() {
			let mut formed = crate::catalog::FieldDefinition {
				flexible: match stmt.flexible {
					AlterKind::Set(()) => true,
					AlterKind::Drop => false,
					AlterKind::None => previous.flexible,
				},
				..previous.clone()
			};
			// The type and the record id component names it carries are set
			// together, as the body sets them.
			match &stmt.kind {
				AlterKind::Set(kind) => {
					let names = match &stmt.key_names {
						AlterKind::Set(names) => Some(names.clone()),
						AlterKind::Drop | AlterKind::None => None,
					};
					formed.set_type(Some(kind.clone()), names);
				}
				AlterKind::Drop => formed.set_type(None, None),
				AlterKind::None => {}
			}
			let implied =
				crate::exec::field_rules::implied_definitions(&name, formed.field_kind.as_ref());
			for (path, kind) in implied {
				let existing = table_fields.iter().find(|f| f.name == path);
				let sub =
					crate::exec::field_rules::implied_definition(&formed, existing, path, kind);
				nested.push((surrealdb_types::ToSql::to_sql(&sub.name), stored(sub.to_stored())?));
			}
			dropped = crate::exec::field_rules::implied_definitions_dropped(
				&table_fields,
				Some(previous),
				&formed,
			);
		}
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Field,
			base: Base::Db,
		});
		let (ns, db) = self.session_ids()?;
		// A component of a named record id has no definition to alter.
		if let Some(refusal) = key_refusal {
			self.store(StoreInstr::RefuseField {
				refusal,
			});
		}
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let tb = self.constant(Value::String(table.as_str().into()));
		let fd = self.constant(Value::String(fd.into()));
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("fd", fd)])?;
		let field = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("FieldKey"),
			key,
			ty: TypeId::named("StoredFieldDefinition"),
			dst: field,
		});
		let name_reg = self.load(fd)?;
		self.must_exist(stmt.if_exists, field, StatementError::FdNotFound, name_reg)?;
		if lightweight_table {
			self.store(StoreInstr::RefuseField {
				refusal: FieldRefusal::Thrown(
					"a LIGHTWEIGHT relation's fields cannot be altered: its in/out contract is \
					 managed by the table definition"
						.to_owned(),
				),
			});
		}
		// The definition as the statement reads it, which the keys a dropped
		// reference left behind are judged against.
		let before = self.load(Operand::Reg(field))?;
		for name in fields {
			let value = altered.get(name).cloned().unwrap_or(Value::None);
			let value = self.constant(value);
			self.set_field(field, name, value);
		}
		let limit = self.register()?;
		self.store(StoreInstr::SessionAuthLimit {
			dst: limit,
		});
		self.set_field(field, "auth_limit", Operand::Reg(limit));
		// The checks of the definition the clauses merge into, which only the
		// statement's read of the stored field decides.
		if name.is_id() {
			self.store(StoreInstr::FieldCheck {
				check: FieldCheck::IdField {
					field,
				},
			});
			// Only a statement that sets or drops the type can change the record
			// id components the definition names; any other keeps the structure
			// that was checked when it was defined.
			if !matches!(stmt.kind, AlterKind::None) {
				self.store(StoreInstr::FieldCheck {
					check: FieldCheck::KeyComponents {
						field,
						ns,
						db,
						kind: DefineKind::Overwrite,
					},
				});
			}
		}
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let tb_name = self.load(tb)?;
		let table_def = self.expect_table(tb_key, tb_name)?;
		if !matches!(stmt.inline, AlterKind::Drop) {
			self.store(StoreInstr::FieldCheck {
				check: FieldCheck::Inline {
					field,
					table: table_def,
				},
			});
		}
		if !matches!(stmt.flexible, AlterKind::Drop) {
			self.store(StoreInstr::FieldCheck {
				check: FieldCheck::Flexible {
					field,
					table: table_def,
				},
			});
		}
		// An altered kind still holds the fields nested in the field; an
		// import replays definitions written before the rule held.
		if kind_altered {
			let import = self.store(StoreInstr::JumpIfImport {
				target: 0,
			});
			self.store(StoreInstr::FieldCheck {
				check: FieldCheck::Nesting {
					field,
					table: table_def,
				},
			});
			let at = self.here();
			self.patch(import, at);
		}
		if let Some(key) = scanned {
			let table_reg = self.load(tb)?;
			self.store(StoreInstr::ValidateRecordKeys {
				ns,
				db,
				table: table_reg,
				key,
			});
		}
		// `put_tb_field`.
		self.store(StoreInstr::Put {
			route: RouteId::named("FieldKey"),
			key,
			ty: TypeId::named("StoredFieldDefinition"),
			value: field,
		});
		for (path, value) in nested {
			let path = self.constant(Value::String(path.into()));
			let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("fd", path)])?;
			let value = self.constant(value);
			let value = self.load(value)?;
			self.store(StoreInstr::Put {
				route: RouteId::named("FieldKey"),
				key,
				ty: TypeId::named("StoredFieldDefinition"),
				value,
			});
			self.release(key);
			self.release(value);
		}
		// An import restores the reference keys of the ones dropped verbatim.
		self.delete_field_definitions((ns, db, tb), &dropped, true)?;
		// Dropping the reference or changing the record kind can strand the
		// reference keys the field wrote; an import restores the keys
		// verbatim.
		let import = self.store(StoreInstr::JumpIfImport {
			target: 0,
		});
		let table_reg = self.load(tb)?;
		self.store(StoreInstr::PurgeDroppedReferences {
			ns,
			db,
			table: table_reg,
			old: before,
			new: Some(field),
		});
		let at = self.here();
		self.patch(import, at);
		self.stamp(ns, db, tb)?;
		let inline_toggled = will_be_inline != was_inline;
		let inline_kind_changed = will_be_inline
			&& was_inline
			&& match &stmt.kind {
				AlterKind::Set(k) => {
					existing_field.as_ref().is_some_and(|f| f.field_kind.as_ref() != Some(k))
				}
				AlterKind::Drop => existing_field.as_ref().is_some_and(|f| f.field_kind.is_some()),
				AlterKind::None => false,
			};
		if (inline_toggled || inline_kind_changed)
			&& let Some(et) = &existing_table
		{
			let next_gen = et.graph_inline_gen.wrapping_add(1);
			let gen_val = self.constant(Value::from(next_gen));
			self.set_field(table_def, "graph_inline_gen", gen_val);
		}
		self.replace_table((ns, db, tb), tb_key, table_def, "cache_fields_ts")?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// `DEFINE FIELD` — `ddl/define/field.rs`, for a field, table or comment
	/// that is evaluated. They are resolved as the body resolves them — the
	/// comment, the field's path, the table — and the definition they form is
	/// handed to the host, which checks it against the catalog the statement
	/// runs against and writes it. Only the statement's real run defines the
	/// field: the host allocates the identifiers a table the field defines on
	/// demand takes, which a speculative run would allocate again.
	async fn define_field_resolved(&mut self, stmt: &DefineFieldStatement) -> Lowered<()> {
		use crate::expr::statements::define::field::DefineDefault;
		self.store(StoreInstr::RefuseNoWrite);
		let comment = crate::exec::vm::host::out_of_line(|| self.comment(&stmt.comment)).await?;
		let name = crate::exec::vm::host::out_of_line(|| self.field_path(&stmt.name)).await?;
		let table =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.what, NameClause::Table)).await?;
		// The name is overwritten by the evaluated one before the definition is
		// read back, so the stored type must already carry the record id
		// component names: only the type of an `id` field can, and a statement
		// that names components is parsed with the name `id` written.
		let mut definition = crate::catalog::FieldDefinition {
			name: crate::expr::Idiom::field(if stmt.key_names.is_some() {
				"id"
			} else {
				"_"
			}),
			table: crate::val::TableName::new(Name::text_of(&stmt.what)),
			flexible: stmt.flexible,
			readonly: stmt.readonly,
			inline: stmt.inline,
			value: stmt.value.clone(),
			assert: stmt.assert.clone(),
			computed: stmt.computed.clone(),
			default: match &stmt.default {
				DefineDefault::None => crate::catalog::DefineDefault::None,
				DefineDefault::Set(x) => crate::catalog::DefineDefault::Set(x.clone()),
				DefineDefault::Always(x) => crate::catalog::DefineDefault::Always(x.clone()),
			},
			select_permission: stmt.permissions.select.clone(),
			create_permission: stmt.permissions.create.clone(),
			update_permission: stmt.permissions.update.clone(),
			comment: comment_text(&stmt.comment),
			reference: stmt.reference.clone(),
			graphql_alias: stmt.graphql_alias.clone(),
			graphql_deprecated: stmt.graphql_deprecated.clone(),
			..Default::default()
		};
		definition.set_type(stmt.field_kind.clone(), stmt.key_names.clone());
		let template = stored(definition.to_stored())?;
		self.store(StoreInstr::RealRunOnly);
		let template = self.constant(template);
		let definition = self.load(template)?;
		self.set_field(definition, "name", Operand::Reg(name));
		self.set_name(definition, "table", &table);
		self.set_comment(definition, &comment);
		self.store(StoreInstr::DefineField {
			definition,
			kind: stmt.kind.clone(),
		});
		self.answer_none()
	}

	/// `REMOVE FIELD` — `ddl/remove/field.rs`, for a field or table that is
	/// evaluated: resolved as the body resolves them, the table and then the
	/// field's path, and removed by the host against the catalog the
	/// statement runs against.
	async fn remove_field_resolved(&mut self, stmt: &RemoveFieldStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Field,
			base: Base::Db,
		});
		let table =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.table_name, NameClause::Table))
				.await?;
		let table = self.load(table.operand)?;
		let name = crate::exec::vm::host::out_of_line(|| self.field_path(&stmt.name)).await?;
		self.store(StoreInstr::RealRunOnly);
		self.store(StoreInstr::RemoveFieldDefinition {
			name,
			table,
			if_exists: stmt.if_exists,
		});
		self.answer_none()
	}

	/// `ALTER FIELD` — `ddl/alter/field.rs`, for a field or table that is
	/// evaluated: resolved as the body resolves them, the field's path and then
	/// the table, once the session's namespace and database are known to
	/// exist, and altered by the host against the catalog the statement runs
	/// against.
	async fn alter_field_resolved(&mut self, stmt: &AlterFieldStatement) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Field,
			base: Base::Db,
		});
		self.session_ids()?;
		let name = crate::exec::vm::host::out_of_line(|| self.field_path(&stmt.name)).await?;
		let table =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.what, NameClause::Table)).await?;
		let table = self.load(table.operand)?;
		self.store(StoreInstr::RealRunOnly);
		self.store(StoreInstr::AlterFieldDefinition {
			name,
			table,
			statement: Box::new(stmt.clone()),
		});
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// The stored definition at `key`, read as `ty`, which must exist:
	/// `IF EXISTS` stops answering `NONE`, and otherwise `missing` is raised
	/// naming `name`.
	fn stored_at(
		&mut self,
		(route, ty): (&'static str, &'static str),
		key: Reg,
		if_exists: bool,
		(missing, name): (StatementError, Operand),
	) -> Lowered<Reg> {
		let definition = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named(ty),
			dst: definition,
		});
		let name = self.load(name)?;
		self.must_exist(if_exists, definition, missing, name)?;
		self.release(name);
		Ok(definition)
	}

	/// Replace the fields `fields` of the definition in `definition` with
	/// those of `altered`, a stored definition built from the statement.
	fn replace_fields(&mut self, definition: Reg, altered: &Value, fields: &[&str]) {
		for name in fields {
			let value = match altered {
				Value::Object(altered) => altered.get(name).cloned().unwrap_or(Value::None),
				_ => Value::None,
			};
			let value = self.constant(value);
			self.set_field(definition, name, value);
		}
	}

	/// Set the definition's auth limit to the session's.
	fn session_auth_limit(&mut self, definition: Reg) -> Lowered<()> {
		let limit = self.register()?;
		self.store(StoreInstr::SessionAuthLimit {
			dst: limit,
		});
		self.set_field(definition, "auth_limit", Operand::Reg(limit));
		self.release(limit);
		Ok(())
	}

	/// `ALTER NAMESPACE` — `ddl/alter/namespace.rs`: the session's namespace,
	/// which must exist, compacted.
	fn alter_namespace(&mut self, compact: bool) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Namespace,
			base: Base::Root,
		});
		let ns_name = self.register()?;
		self.store(StoreInstr::SessionNs {
			dst: ns_name,
		});
		let key = self.object(&[("ns", Operand::Reg(ns_name))])?;
		let namespace = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: namespace,
		});
		self.expect(namespace, StatementError::NsNotFound, ns_name);
		if compact {
			let ns = self.register()?;
			self.get_field(namespace, "namespace_id", ns);
			let root = self.object(&[("ns", Operand::Reg(ns))])?;
			self.store(StoreInstr::Compact {
				route: RouteId::named("NsRoot"),
				key: root,
			});
		}
		self.answer_none()
	}

	/// `ALTER DATABASE` — `ddl/alter/database.rs`: the session's database,
	/// which must exist, compacted.
	fn alter_database(&mut self, compact: bool) -> Lowered<()> {
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Database,
			base: Base::Ns,
		});
		let (ns, db) = self.session_ids()?;
		if compact {
			let root = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))])?;
			self.store(StoreInstr::Compact {
				route: RouteId::named("DbRoot"),
				key: root,
			});
		}
		self.answer_none()
	}

	/// `ALTER PARAM` — `ddl/alter/param.rs`: a new value is evaluated once the
	/// parameter is read.
	async fn alter_param(&mut self, stmt: &AlterParamStatement) -> Lowered<()> {
		use crate::expr::statements::alter::AlterKind;
		let mut altered = crate::catalog::ParamDefinition {
			value: Value::None,
			name: stmt.name.clone(),
			comment: None,
			permissions: stmt.permissions.clone().unwrap_or_default(),
			permissions_body: crate::catalog::LogicalPermissionBody::of(
				&stmt.permissions.clone().unwrap_or_default(),
			),
		};
		let mut fields = Vec::new();
		match &stmt.comment {
			AlterKind::Set(comment) => {
				altered.comment = Some(comment.clone());
				fields.push("comment");
			}
			AlterKind::Drop => fields.push("comment"),
			AlterKind::None => {}
		}
		if stmt.permissions.is_some() {
			fields.push("permissions");
		}
		let altered = stored(altered.to_stored())?;
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Parameter,
			base: Base::Db,
		});
		let (ns, db) = self.session_ids()?;
		let name = self.constant(Value::String(stmt.name.as_str().into()));
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("pa", name)])?;
		let param = self.stored_at(
			("ParamKey", "StoredParamDefinition"),
			key,
			stmt.if_exists,
			(StatementError::PaNotFound, name),
		)?;
		if let Some(value) = &stmt.value {
			let value = crate::exec::vm::host::out_of_line(|| self.evaluated(value)).await?;
			self.store(StoreInstr::CheckDepth {
				value,
			});
			self.set_field(param, "value", Operand::Reg(value));
		}
		self.replace_fields(param, &altered, &fields);
		self.store(StoreInstr::Put {
			route: RouteId::named("ParamKey"),
			key,
			ty: TypeId::named("StoredParamDefinition"),
			value: param,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER FUNCTION` — `ddl/alter/function.rs`. Dropping the block leaves
	/// it, as the body does.
	fn alter_function(&mut self, stmt: &AlterFunctionStatement) -> Lowered<()> {
		use crate::expr::statements::alter::AlterKind;
		let mut altered = crate::catalog::FunctionDefinition {
			name: stmt.name.clone(),
			args: Vec::new(),
			block: Default::default(),
			body: crate::catalog::LogicalBlockBody::of(&Default::default()),
			permissions: stmt.permissions.clone().unwrap_or_default(),
			permissions_body: crate::catalog::LogicalPermissionBody::of(
				&stmt.permissions.clone().unwrap_or_default(),
			),
			returns: None,
			comment: None,
			auth_limit: crate::catalog::auth::AuthLimit::default(),
			graphql_alias: None,
			graphql_deprecated: None,
		};
		let mut fields = Vec::new();
		match &stmt.args {
			AlterKind::Set(args) => {
				altered.args.clone_from(args);
				fields.push("args");
			}
			AlterKind::Drop => fields.push("args"),
			AlterKind::None => {}
		}
		if let AlterKind::Set(block) = &stmt.block {
			altered.block = block.clone();
			altered.body = crate::catalog::LogicalBlockBody::of(block);
			fields.push("block");
		}
		match &stmt.comment {
			AlterKind::Set(comment) => {
				altered.comment = Some(comment.clone());
				fields.push("comment");
			}
			AlterKind::Drop => fields.push("comment"),
			AlterKind::None => {}
		}
		if stmt.permissions.is_some() {
			fields.push("permissions");
		}
		match &stmt.returns {
			AlterKind::Set(kind) => {
				altered.returns = Some(kind.clone());
				fields.push("returns");
			}
			AlterKind::Drop => fields.push("returns"),
			AlterKind::None => {}
		}
		let altered = stored(altered.to_stored())?;
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Function,
			base: Base::Db,
		});
		let (ns, db) = self.session_ids()?;
		let name = self.constant(Value::String(stmt.name.as_str().into()));
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("fc", name)])?;
		let shown = self.constant(Value::String(format!("fn::{}", stmt.name.as_str()).into()));
		let function = self.stored_at(
			("FunctionKey", "StoredFunctionDefinition"),
			key,
			stmt.if_exists,
			(StatementError::FcNotFound, shown),
		)?;
		self.replace_fields(function, &altered, &fields);
		self.session_auth_limit(function)?;
		self.store(StoreInstr::Put {
			route: RouteId::named("FunctionKey"),
			key,
			ty: TypeId::named("StoredFunctionDefinition"),
			value: function,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER EVENT` — `ddl/alter/event.rs`. Dropping `WHEN` or `THEN` leaves
	/// it, as the body does; the table, when defined, is rewritten with its
	/// events stamp refreshed.
	async fn alter_event(&mut self, stmt: &AlterEventStatement) -> Lowered<()> {
		use crate::expr::statements::alter::AlterKind;
		let mut fields = Vec::new();
		let when = match &stmt.when {
			AlterKind::Set(when) => {
				fields.push("when");
				when.clone()
			}
			_ => Expr::Literal(Literal::Bool(true)),
		};
		let then = match &stmt.then {
			AlterKind::Set(then) => {
				fields.push("then");
				then.clone()
			}
			_ => Vec::new(),
		};
		let comment = match &stmt.comment {
			AlterKind::Set(comment) => {
				fields.push("comment");
				Some(comment.clone())
			}
			AlterKind::Drop => {
				fields.push("comment");
				None
			}
			AlterKind::None => None,
		};
		let kind = match &stmt.kind {
			AlterKind::Set(kind) => {
				fields.push("kind");
				kind.clone().into()
			}
			AlterKind::Drop => {
				fields.push("kind");
				crate::catalog::EventKind::Sync
			}
			AlterKind::None => crate::catalog::EventKind::Sync,
		};
		let altered = crate::catalog::EventDefinition::new(
			Default::default(),
			Default::default(),
			when,
			then,
			kind,
			crate::catalog::auth::AuthLimit::default(),
			comment,
		);
		let altered = stored(altered.to_stored())?;
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Event,
			base: Base::Db,
		});
		let ev = crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Event))
			.await?
			.operand;
		let tb = crate::exec::vm::host::out_of_line(|| self.name(&stmt.what, NameClause::Table))
			.await?
			.operand;
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ev", ev)])?;
		let event = self.stored_at(
			("EventKey", "StoredEventDefinition"),
			key,
			stmt.if_exists,
			(StatementError::EvNotFound, ev),
		)?;
		self.replace_fields(event, &altered, &fields);
		self.session_auth_limit(event)?;
		self.store(StoreInstr::Put {
			route: RouteId::named("EventKey"),
			key,
			ty: TypeId::named("StoredEventDefinition"),
			value: event,
		});
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let table_def = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("TableKey"),
			key: tb_key,
			ty: TypeId::named("StoredTableDefinition"),
			dst: table_def,
		});
		let undefined = self.emit(Instr::JumpIf {
			cond: Operand::Reg(table_def),
			truthy: false,
			target: 0,
		});
		self.replace_table((ns, db, tb), tb_key, table_def, "cache_events_ts")?;
		let at = self.here();
		self.patch(undefined, at);
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER SEQUENCE` — `ddl/alter/sequence.rs`.
	async fn alter_sequence(&mut self, stmt: &AlterSequenceStatement) -> Lowered<()> {
		// A written timeout is a constant; an evaluated one is evaluated once
		// the sequence is read, and cast to a duration or none.
		let timeout = stmt.timeout.as_ref().map(|expr| literal_duration(expr).ok_or(expr));
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Sequence,
			base: Base::Db,
		});
		let name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Sequence))
				.await?;
		let (ns, db) = self.session_ids()?;
		let sq = name.operand;
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("sq", sq)])?;
		let sequence = self.stored_at(
			("SequenceKey", "SequenceDefinition"),
			key,
			stmt.if_exists,
			(StatementError::SeqNotFound, sq),
		)?;
		match timeout {
			None => {}
			Some(Ok(written)) => {
				let timeout = self.constant(match written {
					Some(d) => Value::Duration(crate::val::Duration(d)),
					None => Value::None,
				});
				self.set_field(sequence, "timeout", timeout);
			}
			Some(Err(expr)) => {
				let timeout = crate::exec::vm::host::out_of_line(|| {
					self.converted(expr, ClauseConversion::Duration)
				})
				.await?;
				self.set_field(sequence, "timeout", Operand::Reg(timeout));
				self.release(timeout);
			}
		}
		self.store(StoreInstr::Put {
			route: RouteId::named("SequenceKey"),
			key,
			ty: TypeId::named("SequenceDefinition"),
			value: sequence,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER ANALYZER` — `ddl/alter/analyzer.rs`: the altered analyzer's
	/// segmenter is checked before it is written.
	async fn alter_analyzer(&mut self, stmt: &AlterAnalyzerStatement) -> Lowered<()> {
		use crate::expr::statements::alter::AlterKind;
		let mut altered = crate::catalog::AnalyzerDefinition {
			name: Default::default(),
			function: None,
			tokenizers: None,
			filters: None,
			comment: None,
		};
		let mut fields = Vec::new();
		match &stmt.function {
			AlterKind::Set(function) => {
				altered.function = Some(function.clone().into());
				fields.push("function");
			}
			AlterKind::Drop => fields.push("function"),
			AlterKind::None => {}
		}
		match &stmt.tokenizers {
			AlterKind::Set(tokenizers) => {
				altered.tokenizers = Some(tokenizers.clone());
				fields.push("tokenizers");
			}
			AlterKind::Drop => fields.push("tokenizers"),
			AlterKind::None => {}
		}
		match &stmt.filters {
			AlterKind::Set(filters) => {
				altered.filters = Some(filters.clone());
				fields.push("filters");
			}
			AlterKind::Drop => fields.push("filters"),
			AlterKind::None => {}
		}
		match &stmt.comment {
			AlterKind::Set(comment) => {
				altered.comment = Some(comment.clone());
				fields.push("comment");
			}
			AlterKind::Drop => fields.push("comment"),
			AlterKind::None => {}
		}
		let altered = stored(altered)?;
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Analyzer,
			base: Base::Db,
		});
		let (ns, db) = self.session_ids()?;
		let az = crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Analyzer))
			.await?
			.operand;
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("az", az)])?;
		let analyzer = self.stored_at(
			("AnalyzerKey", "AnalyzerDefinition"),
			key,
			stmt.if_exists,
			(StatementError::AzNotFound, az),
		)?;
		self.replace_fields(analyzer, &altered, &fields);
		self.store(StoreInstr::LoadAnalyzer {
			analyzer,
			mappers: false,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("AnalyzerKey"),
			key,
			ty: TypeId::named("AnalyzerDefinition"),
			value: analyzer,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// `ALTER USER` — `ddl/alter/user.rs`. On a namespace the session's
	/// namespace is created when missing (`get_ns_id`).
	async fn alter_user(&mut self, stmt: &AlterUserStatement) -> Lowered<()> {
		use crate::expr::statements::alter::AlterKind;
		let mut altered = crate::catalog::UserDefinition {
			name: Default::default(),
			hash: String::new(),
			code: String::new(),
			roles: Vec::new(),
			token_duration: None,
			session_duration: None,
			comment: None,
			base: stmt.base.into(),
			scram: None,
		};
		let mut fields = Vec::new();
		if let Some(hash) = &stmt.hash {
			altered.hash.clone_from(hash);
			fields.push("hash");
		}
		if let Some(scram) = &stmt.scram {
			altered.scram.clone_from(scram);
			fields.push("scram");
		}
		match &stmt.roles {
			AlterKind::Set(roles) => {
				altered.roles.clone_from(roles);
				fields.push("roles");
			}
			AlterKind::Drop => fields.push("roles"),
			AlterKind::None => {}
		}
		for (clause, slot, field) in [
			(&stmt.token_duration, &mut altered.token_duration, "token_duration"),
			(&stmt.session_duration, &mut altered.session_duration, "session_duration"),
		] {
			match clause {
				AlterKind::Set(duration) => {
					*slot = (*duration).map(crate::val::BoundedDuration::from)
				}
				AlterKind::Drop => *slot = None,
				AlterKind::None => continue,
			}
			fields.push(field);
		}
		match &stmt.comment {
			AlterKind::Set(comment) => {
				altered.comment = Some(comment.clone());
				fields.push("comment");
			}
			AlterKind::Drop => fields.push("comment"),
			AlterKind::None => {}
		}
		let altered = stored(altered)?;
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Actor,
			base: stmt.base,
		});
		let name =
			crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::User)).await?;
		let user = name.operand;
		let (route, mut key_fields, missing) = match stmt.base {
			Base::Root => ("RootUserKey", Vec::new(), StatementError::UserRootNotFound),
			Base::Ns => {
				let ns = self.namespace_id_creating()?;
				("NsUserKey", vec![("ns", Operand::Reg(ns))], StatementError::UserNsNotFound)
			}
			Base::Db => {
				let (ns, db) = self.session_ids()?;
				(
					"DbUserKey",
					vec![("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))],
					StatementError::UserDbNotFound,
				)
			}
		};
		key_fields.push(("user", user));
		let key = self.object(&key_fields)?;
		let definition = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("UserDefinition"),
			dst: definition,
		});
		let present = self.emit(Instr::JumpIf {
			cond: Operand::Reg(definition),
			truthy: true,
			target: 0,
		});
		self.missing_at_base(stmt.if_exists, stmt.base, ("name", user), missing)?;
		let at = self.here();
		self.patch(present, at);
		self.replace_fields(definition, &altered, &fields);
		self.store(StoreInstr::Put {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("UserDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER CONFIG` — `ddl/alter/config.rs`: the session's database's
	/// config of the kind is written whether or not it existed, unless the
	/// statement asks `IF EXISTS`.
	async fn alter_config(&mut self, stmt: &AlterConfigStatement) -> Lowered<()> {
		use crate::expr::statements::define::config::ConfigInner;
		use crate::iam::ConfigKind;
		let (kind, cg, definition) = match &stmt.inner {
			ConfigInner::GraphQL(g) => (
				ConfigKind::GraphQL,
				"graphql",
				crate::catalog::ConfigDefinition::GraphQL(g.clone()),
			),
			ConfigInner::Api(a) => {
				(ConfigKind::Api, "api", crate::catalog::ConfigDefinition::Api(api_config(a)?))
			}
			ConfigInner::Default(d) => (
				ConfigKind::Default,
				"default",
				crate::catalog::ConfigDefinition::Default(crate::catalog::DefaultConfig {
					namespace: written_text(&d.namespace),
					database: written_text(&d.database),
				}),
			),
		};
		let definition = stored(definition.to_stored())?;
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Config(kind),
			base: Base::Db,
		});
		let (ns, db) = self.session_ids()?;
		let ty = self.constant(Value::String(cg.into()));
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("ty", ty)])?;
		if stmt.if_exists {
			let existing = self.register()?;
			self.store(StoreInstr::Get {
				route: RouteId::named("DbConfigKey"),
				key,
				ty: TypeId::named("StoredConfigDefinition"),
				dst: existing,
			});
			let present = self.emit(Instr::JumpIf {
				cond: Operand::Reg(existing),
				truthy: true,
				target: 0,
			});
			self.answer_none()?;
			let at = self.here();
			self.patch(present, at);
		}
		// A default's namespace and database are resolved where the body
		// computes the definition, once the existing one is read.
		let mut defaults = Vec::new();
		if let ConfigInner::Default(d) = &stmt.inner {
			for (field, expr, into) in [
				("namespace", &d.namespace, NameClause::ConfigNamespace),
				("database", &d.database, NameClause::ConfigDatabase),
			] {
				if let (_, Some(value)) =
					crate::exec::vm::host::out_of_line(|| self.optional_name(expr, into)).await?
				{
					defaults.push((field, value));
				}
			}
		}
		let middleware = match &stmt.inner {
			ConfigInner::Api(config) => {
				crate::exec::vm::host::out_of_line(|| self.evaluated_middleware(config)).await?
			}
			_ => None,
		};
		let definition = self.constant(definition);
		let definition = self.load(definition)?;
		for (field, value) in defaults {
			self.set_path(definition, &["Default", field], Operand::Reg(value));
		}
		if let Some(list) = middleware {
			self.set_path(definition, &["Api", "middleware"], Operand::Reg(list));
		}
		self.store(StoreInstr::Put {
			route: RouteId::named("DbConfigKey"),
			key,
			ty: TypeId::named("StoredConfigDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER BUCKET` — `ddl/alter/bucket.rs`.
	async fn alter_bucket(&mut self, stmt: &AlterBucketStatement) -> Lowered<()> {
		use crate::expr::statements::alter::AlterKind;
		let mut altered = crate::catalog::BucketDefinition {
			id: None,
			name: Default::default(),
			backend: None,
			permissions: stmt.permissions.clone().unwrap_or_default(),
			readonly: false,
			comment: None,
		};
		let mut fields = Vec::new();
		match &stmt.backend {
			AlterKind::Set(backend) => {
				altered.backend = Some(backend.as_str().into());
				fields.push("backend");
			}
			AlterKind::Drop => fields.push("backend"),
			AlterKind::None => {}
		}
		if stmt.permissions.is_some() {
			fields.push("permissions");
		}
		match stmt.readonly {
			AlterKind::Set(()) => {
				altered.readonly = true;
				fields.push("readonly");
			}
			AlterKind::Drop => fields.push("readonly"),
			AlterKind::None => {}
		}
		match &stmt.comment {
			AlterKind::Set(comment) => {
				altered.comment = Some(comment.clone());
				fields.push("comment");
			}
			AlterKind::Drop => fields.push("comment"),
			AlterKind::None => {}
		}
		let altered = stored(altered.to_stored())?;
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Bucket,
			base: Base::Db,
		});
		let (ns, db) = self.session_ids()?;
		let name = crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Bucket))
			.await?;
		let bu = name.operand;
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("bu", bu)])?;
		let bucket = self.stored_at(
			("BucketKey", "StoredBucketDefinition"),
			key,
			stmt.if_exists,
			(StatementError::BuNotFound, bu),
		)?;
		self.replace_fields(bucket, &altered, &fields);
		self.store(StoreInstr::Put {
			route: RouteId::named("BucketKey"),
			key,
			ty: TypeId::named("StoredBucketDefinition"),
			value: bucket,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER INDEX` — `ddl/alter/index.rs`: the definition and its id's name
	/// lookup rewritten (`put_tb_index`), and the table with its indexes stamp
	/// refreshed.
	async fn alter_index(&mut self, stmt: &AlterIndexStatement) -> Lowered<()> {
		use crate::expr::statements::alter::AlterKind;
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Index,
			base: Base::Db,
		});
		let ix = crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Index))
			.await?
			.operand;
		let tb = crate::exec::vm::host::out_of_line(|| self.name(&stmt.table, NameClause::Table))
			.await?
			.operand;
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ix", ix)])?;
		let index = self.stored_at(
			("IndexDefKey", "StoredIndexDefinition"),
			key,
			stmt.if_exists,
			(StatementError::IxNotFound, ix),
		)?;
		match &stmt.comment {
			AlterKind::Set(comment) => {
				let comment = self.constant(Value::String(comment.as_str().into()));
				self.set_field(index, "comment", comment);
			}
			AlterKind::Drop => {
				let none = self.constant(Value::None);
				self.set_field(index, "comment", none);
			}
			AlterKind::None => {}
		}
		if stmt.prepare_remove {
			let yes = self.constant(Value::Bool(true));
			self.set_field(index, "prepare_remove", yes);
		}
		self.store(StoreInstr::Put {
			route: RouteId::named("IndexDefKey"),
			key,
			ty: TypeId::named("StoredIndexDefinition"),
			value: index,
		});
		let (id, stored_name) = (self.register()?, self.register()?);
		self.get_field(index, "index_id", id);
		self.get_field(index, "name", stored_name);
		let lookup =
			self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ix", Operand::Reg(id))])?;
		self.store(StoreInstr::Put {
			route: RouteId::named("IndexNameKey"),
			key: lookup,
			ty: TypeId::UTF8,
			value: stored_name,
		});
		self.stamp(ns, db, tb)?;
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let tb_name = self.load(tb)?;
		let table_def = self.expect_table(tb_key, tb_name)?;
		self.replace_table((ns, db, tb), tb_key, table_def, "cache_indexes_ts")?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

impl Lowering<'_, '_> {
	/// `ALTER ACCESS` — `ddl/alter/access.rs`. On a namespace the session's
	/// namespace is created when missing (`get_ns_id`). The checks the body
	/// makes of the altered method are made as the program runs, of the
	/// definition it reads and alters.
	async fn alter_access(&mut self, stmt: &AlterAccessStatement) -> Lowered<()> {
		use crate::expr::statements::alter::AlterKind;
		let name_text = Name::text_of(&stmt.name);
		let mut altered = crate::catalog::AccessDefinition {
			name: name_text.as_str().into(),
			base: stmt.base.into(),
			// Only the altered clauses are read out of this definition.
			access_type: crate::catalog::AccessType::Jwt(crate::catalog::JwtAccess {
				verify: crate::catalog::JwtAccessVerify::Key(crate::catalog::JwtAccessVerifyKey {
					alg: crate::catalog::Algorithm::Hs512,
					key: String::new(),
				}),
				issue: None,
				audience: None,
			}),
			authenticate: None,
			context: None,
			signup: None,
			signin: None,
			grant_duration: None,
			token_duration: None,
			session_duration: None,
			comment: None,
		};
		let mut fields = Vec::new();
		for (clause, slot, field) in [
			(&stmt.authenticate, &mut altered.authenticate, "authenticate"),
			(&stmt.context, &mut altered.context, "context"),
		] {
			match clause {
				AlterKind::Set(expr) => *slot = Some(expr.clone()),
				AlterKind::Drop => {}
				AlterKind::None => continue,
			}
			fields.push(field);
		}
		for (clause, slot, field) in [
			(&stmt.grant_duration, &mut altered.grant_duration, "grant_duration"),
			(&stmt.token_duration, &mut altered.token_duration, "token_duration"),
			(&stmt.session_duration, &mut altered.session_duration, "session_duration"),
		] {
			match clause {
				AlterKind::Set(duration) => *slot = *duration,
				AlterKind::Drop => {}
				AlterKind::None => continue,
			}
			fields.push(field);
		}
		match &stmt.comment {
			AlterKind::Set(comment) => {
				altered.comment = Some(comment.clone());
				fields.push("comment");
			}
			AlterKind::Drop => fields.push("comment"),
			AlterKind::None => {}
		}
		let altered = stored(altered.to_stored())?;
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Access,
			base: stmt.base,
		});
		let ac = crate::exec::vm::host::out_of_line(|| self.name(&stmt.name, NameClause::Access))
			.await?
			.operand;
		let (route, mut key_fields, missing) = match stmt.base {
			Base::Root => ("RootAccessMethodKey", Vec::new(), StatementError::AccessRootNotFound),
			Base::Ns => {
				let ns = self.namespace_id_creating()?;
				(
					"NsAccessMethodKey",
					vec![("ns", Operand::Reg(ns))],
					StatementError::AccessNsNotFound,
				)
			}
			Base::Db => {
				let (ns, db) = self.session_ids()?;
				(
					"DbAccessMethodKey",
					vec![("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))],
					StatementError::AccessDbNotFound,
				)
			}
		};
		key_fields.push(("ac", ac));
		let key = self.object(&key_fields)?;
		let access = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredAccessDefinition"),
			dst: access,
		});
		let present = self.emit(Instr::JumpIf {
			cond: Operand::Reg(access),
			truthy: true,
			target: 0,
		});
		self.missing_at_base(stmt.if_exists, stmt.base, ("ac", ac), missing)?;
		let at = self.here();
		self.patch(present, at);
		self.replace_fields(access, &altered, &fields);
		// The body refuses an alteration the definition cannot take before it
		// writes the definition back.
		self.store(StoreInstr::AccessCheck {
			check: AccessCheck::Altered {
				access,
			},
		});
		self.store(StoreInstr::Put {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredAccessDefinition"),
			value: access,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER SYSTEM` — `ddl/alter/system.rs`: a compaction of the whole
	/// store, and the process's query timeout.
	async fn alter_system(&mut self, stmt: &AlterSystemStatement) -> Lowered<()> {
		use crate::expr::statements::alter::AlterKind;
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Any,
			base: Base::Root,
		});
		if stmt.compact {
			self.store(StoreInstr::CompactAll);
		}
		// The timeout is evaluated, and cast to a duration, once the
		// compaction has run.
		let timeout = match &stmt.query_timeout {
			AlterKind::None => None,
			AlterKind::Set(expr) => Some(
				crate::exec::vm::host::out_of_line(|| {
					self.converted(expr, ClauseConversion::RequiredDuration)
				})
				.await?,
			),
			AlterKind::Drop => {
				let none = self.constant(Value::None);
				Some(self.load(none)?)
			}
		};
		if let Some(timeout) = timeout {
			self.store(StoreInstr::SetQueryTimeout {
				timeout,
			});
		}
		self.answer_none()
	}

	/// `ALTER MODULE` — `ddl/alter/module.rs`: the module at its storage
	/// name, rewritten at the name its definition gives (`put_db_module`).
	fn alter_module(&mut self, stmt: &AlterModuleStatement) -> Lowered<()> {
		use crate::expr::statements::alter::AlterKind;
		let storage = stmt.name.get_storage_name();
		let mut fields = Vec::new();
		let comment = match &stmt.comment {
			AlterKind::Set(comment) => {
				fields.push("comment");
				Some(comment.clone())
			}
			AlterKind::Drop => {
				fields.push("comment");
				None
			}
			AlterKind::None => None,
		};
		if stmt.permissions.is_some() {
			fields.push("permissions");
		}
		let permissions = stmt.permissions.clone().unwrap_or_default();
		let altered = crate::catalog::ModuleDefinition {
			name: None,
			// Only the altered clauses are read out of this definition.
			executable: crate::catalog::ModuleExecutable::Silo(crate::catalog::SiloExecutable {
				organisation: String::new(),
				package: String::new(),
				major: 0,
				minor: 0,
				patch: 0,
			}),
			unsigned: false,
			comment,
			permissions_body: crate::catalog::LogicalPermissionBody::of(&permissions),
			permissions,
		};
		let altered = stored(altered.to_stored())?;
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Module,
			base: Base::Db,
		});
		let (ns, db) = self.session_ids()?;
		let md = self.constant(Value::String(storage.into()));
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("md", md)])?;
		let module = self.stored_at(
			("ModuleKey", "StoredModuleDefinition"),
			key,
			stmt.if_exists,
			(StatementError::MdNotFound, md),
		)?;
		self.replace_fields(module, &altered, &fields);
		self.store(StoreInstr::Put {
			route: RouteId::named("ModuleKey"),
			key,
			ty: TypeId::named("StoredModuleDefinition"),
			value: module,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER API` — `ddl/alter/api.rs`: its clauses applied in order to the
	/// stored API, with middleware arguments written as values.
	async fn alter_api(&mut self, stmt: &AlterApiStatement) -> Lowered<()> {
		use crate::expr::statements::alter::AlterKind;
		use crate::expr::statements::alter::api::AlterApiClause;
		// The stored forms of what the clauses set, read out of an API built
		// with them.
		let stored_api = |actions: Vec<crate::catalog::ApiAction>,
		                  config: crate::catalog::ApiConfig,
		                  fallback: Option<Expr>|
		 -> Lowered<crate::val::Object> {
			let api = crate::catalog::ApiDefinition {
				path: "/".parse().map_err(|_| NotCovered::internal("the root API path"))?,
				actions,
				fallback,
				config,
				auth_limit: crate::catalog::auth::AuthLimit::default(),
				comment: None,
			};
			match stored(api.to_stored())? {
				Value::Object(api) => Ok(api),
				other => Err(NotCovered::internal(format!("a stored API that is {other:?}"))),
			}
		};
		let field = |object: &crate::val::Object, name: &str| {
			object.get(name).cloned().unwrap_or(Value::None)
		};
		enum Step<'a> {
			Set(&'static str, Value),
			/// The evaluated middleware of a configuration set by the step before.
			Middleware(&'a crate::expr::statements::define::config::api::ApiConfig),
			Drop(Value),
			Push(Value, &'a crate::expr::statements::define::config::api::ApiConfig),
		}
		let mut steps = Vec::new();
		for clause in &stmt.clauses {
			match clause {
				AlterApiClause::ForAny {
					config,
					fallback,
				} => {
					if let Some(config) = config {
						let api = stored_api(Vec::new(), api_config(config)?, None)?;
						steps.push(Step::Set("config", field(&api, "config")));
						steps.push(Step::Middleware(config));
					}
					match fallback {
						AlterKind::Set(expr) => {
							let api = stored_api(
								Vec::new(),
								api_config(&Default::default())?,
								Some(expr.clone()),
							)?;
							steps.push(Step::Set("fallback", field(&api, "fallback")));
						}
						AlterKind::Drop => steps.push(Step::Set("fallback", Value::None)),
						AlterKind::None => {}
					}
				}
				AlterApiClause::SetAction(action_config) => {
					let action = action_config;
					let api = stored_api(
						vec![crate::catalog::ApiAction {
							methods: action.methods.iter().map(|m| (*m).into()).collect(),
							action: action.action.clone(),
							config: api_config(&action.config)?,
						}],
						api_config(&Default::default())?,
						None,
					)?;
					let Value::Array(actions) = field(&api, "actions") else {
						return Err(NotCovered::internal("stored API actions that are not a list"));
					};
					let action = actions.first().cloned().unwrap_or(Value::None);
					let methods = match &action {
						Value::Object(action) => field(action, "methods"),
						_ => Value::None,
					};
					steps.push(Step::Drop(methods));
					steps.push(Step::Push(action, &action_config.config));
				}
				AlterApiClause::DropAction {
					methods,
				} => {
					let api = stored_api(
						vec![crate::catalog::ApiAction {
							methods: methods.iter().map(|m| (*m).into()).collect(),
							action: Expr::Literal(Literal::None),
							config: api_config(&Default::default())?,
						}],
						api_config(&Default::default())?,
						None,
					)?;
					let methods = match field(&api, "actions") {
						Value::Array(actions) => match actions.first() {
							Some(Value::Object(action)) => field(action, "methods"),
							_ => Value::None,
						},
						_ => Value::None,
					};
					steps.push(Step::Drop(methods));
				}
			}
		}
		let comment = match &stmt.comment {
			AlterKind::Set(comment) => Some(Value::String(comment.as_str().into())),
			AlterKind::Drop => Some(Value::None),
			AlterKind::None => None,
		};
		self.store(StoreInstr::RefuseNoWrite);
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Api,
			base: Base::Db,
		});
		let (ns, db) = self.session_ids()?;
		let ap = crate::exec::vm::host::out_of_line(|| self.name(&stmt.path, NameClause::ApiPath))
			.await?
			.operand;
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("ap", ap)])?;
		let api = self.register()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("ApiKey"),
			key,
			ty: TypeId::named("StoredApiDefinition"),
			dst: api,
		});
		let detail = self.object(&[("value", ap)])?;
		self.must_exist(stmt.if_exists, api, StatementError::ApNotFound, detail)?;
		let actions = self.register()?;
		for step in steps {
			match step {
				Step::Set(name, value) => {
					let value = self.constant(value);
					self.set_field(api, name, value);
				}
				Step::Drop(methods) => {
					self.get_field(api, "actions", actions);
					let methods = self.constant(methods);
					let methods = self.load(methods)?;
					self.store(StoreInstr::DropMethods {
						actions,
						methods,
					});
					self.set_field(api, "actions", Operand::Reg(actions));
					self.release(methods);
				}
				Step::Middleware(config) => {
					if let Some(list) =
						crate::exec::vm::host::out_of_line(|| self.evaluated_middleware(config))
							.await?
					{
						self.set_path(api, &["config", "middleware"], Operand::Reg(list));
					}
				}
				Step::Push(action, config) => {
					self.get_field(api, "actions", actions);
					let evaluated =
						crate::exec::vm::host::out_of_line(|| self.evaluated_middleware(config))
							.await?;
					let action = self.constant(action);
					let action = self.load(action)?;
					if let Some(list) = evaluated {
						self.set_path(action, &["config", "middleware"], Operand::Reg(list));
					}
					self.store(StoreInstr::Push {
						list: actions,
						value: action,
					});
					self.set_field(api, "actions", Operand::Reg(actions));
					self.release(action);
				}
			}
		}
		if let Some(comment) = comment {
			let comment = self.constant(comment);
			self.set_field(api, "comment", comment);
		}
		self.session_auth_limit(api)?;
		self.store(StoreInstr::Put {
			route: RouteId::named("ApiKey"),
			key,
			ty: TypeId::named("StoredApiDefinition"),
			value: api,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}

/// An API configuration as it is stored (`api_config_compute`). A middleware
/// argument that is written as a value is kept; one that is evaluated holds
/// `NONE`, and the program replaces the configuration's middleware with the
/// list [`Lowering::evaluated_middleware`] evaluates.
fn api_config(
	config: &crate::expr::statements::define::config::api::ApiConfig,
) -> Lowered<crate::catalog::ApiConfig> {
	let middleware = config
		.middleware
		.iter()
		.map(|m| crate::catalog::MiddlewareDefinition {
			name: m.name.clone(),
			args: m.args.iter().map(|arg| written_value_of(arg).unwrap_or(Value::None)).collect(),
		})
		.collect();
	Ok(crate::catalog::ApiConfig {
		middleware,
		permissions: config.permissions.clone(),
	})
}

/// A middleware argument's value, when it is written as one.
fn written_value_of(arg: &Expr) -> Option<Value> {
	match arg {
		Expr::Literal(literal) => literal.as_static_value(),
		_ => None,
	}
}

impl Lowering<'_, '_> {
	/// A register holding the middleware of `config` with its arguments
	/// evaluated, each in the statement's frame and refused where it nests
	/// deeper than a value may be stored (`api_config_compute`); none when
	/// every argument is written as a value.
	async fn evaluated_middleware(
		&mut self,
		config: &crate::expr::statements::define::config::api::ApiConfig,
	) -> Lowered<Option<Reg>> {
		let written =
			config.middleware.iter().all(|m| m.args.iter().all(|a| written_value_of(a).is_some()));
		if written {
			return Ok(None);
		}
		let list = self.register()?;
		self.emit(Instr::NewArray {
			dst: list,
		});
		for middleware in &config.middleware {
			let args = self.register()?;
			self.emit(Instr::NewArray {
				dst: args,
			});
			for arg in &middleware.args {
				let value = crate::exec::vm::host::out_of_line(|| self.evaluated(arg)).await?;
				self.store(StoreInstr::CheckDepth {
					value,
				});
				self.emit(Instr::ArrayPush {
					array: args,
					value: Operand::Reg(value),
				});
				self.release(value);
			}
			let name = self.constant(Value::String(middleware.name.as_str().into()));
			let entry = self.object(&[("name", name), ("args", Operand::Reg(args))])?;
			self.emit(Instr::ArrayPush {
				array: list,
				value: Operand::Reg(entry),
			});
			self.release(args);
			self.release(entry);
		}
		Ok(Some(list))
	}
}

/// A duration clause written as a duration (`Some(Some(_))`) or as `NONE`
/// (`Some(None)`); `None` for one that is evaluated.
fn literal_duration(expr: &Expr) -> Option<Option<std::time::Duration>> {
	match expr {
		Expr::Literal(Literal::None) => Some(None),
		Expr::Literal(Literal::Duration(d)) => Some(Some(d.0)),
		_ => None,
	}
}

/// A stored definition as the VM holds one: its type's schema converts it to
/// a constant the program writes with the holes it fills.
fn stored<T: StoredField>(value: T) -> Lowered<Value> {
	value
		.into_vm_value()
		.map_err(|e| NotCovered::internal(format!("a definition the codec does not convert: {e}")))
}

/// A name a schema statement is written with, as the program reads it.
struct Name {
	/// The name, when it is written rather than evaluated.
	written: Option<String>,
	/// A constant holding the written name, or the register the evaluated
	/// name is read into.
	operand: Operand,
}

/// A `COMMENT` clause, as the program reads it.
struct Comment {
	/// Whether the comment is evaluated rather than written.
	evaluated: bool,
	/// A constant holding the written comment, or the register the
	/// evaluated comment is read into.
	operand: Operand,
}

impl Name {
	/// The name to form a definition with while lowering: the written name,
	/// or a placeholder that [`Lowering::set_name`] replaces once the name is
	/// evaluated.
	fn text(&self) -> &str {
		self.written.as_deref().unwrap_or_default()
	}

	/// [`Self::text`] for the name `expr` gives, before it is resolved.
	fn text_of(expr: &Expr) -> String {
		written_text(expr).unwrap_or_default()
	}
}

impl Lowering<'_, '_> {
	/// The name `expr` gives (`ddl::scalar::ident`). A written name emits
	/// nothing. Anything else is evaluated here, in the statement's frame, and
	/// read as a string, a value of another kind raising a refusal that names
	/// `into`; so a caller lowers it at the point the statement's body resolves
	/// it, which fixes which of its failures and the checks around it is raised
	/// first.
	async fn name(&mut self, expr: &Expr, into: NameClause) -> Lowered<Name> {
		if let Some(written) = written_text(expr) {
			let operand = self.constant(Value::String(written.as_str().into()));
			return Ok(Name {
				written: Some(written),
				operand,
			});
		}
		let value = crate::exec::vm::host::out_of_line(|| self.evaluated(expr)).await?;
		self.store(StoreInstr::CoerceName {
			src: value,
			dst: value,
			into,
			optional: false,
		});
		Ok(Name {
			written: None,
			operand: Operand::Reg(value),
		})
	}

	/// The name an optional clause `expr` gives, or `NONE`
	/// (`ddl::scalar::optional_ident`), resolved here as [`Self::name`]
	/// resolves one: its value when it is written, and when it is evaluated,
	/// none and the register the program reads it into.
	async fn optional_name(
		&mut self,
		expr: &Expr,
		into: NameClause,
	) -> Lowered<(Option<String>, Option<Reg>)> {
		if matches!(expr, Expr::Literal(Literal::None)) {
			return Ok((None, None));
		}
		if let Some(written) = written_text(expr) {
			return Ok((Some(written), None));
		}
		let value = crate::exec::vm::host::out_of_line(|| self.evaluated(expr)).await?;
		self.store(StoreInstr::CoerceName {
			src: value,
			dst: value,
			into,
			optional: true,
		});
		Ok((None, Some(value)))
	}

	/// The comment `expr` gives, a string or `NONE` (`scalar` cast to
	/// `Option<String>`). A written comment emits nothing. Anything else is
	/// evaluated here, in the statement's frame, and cast as the body casts
	/// it; so a caller lowers it at the point the statement's body evaluates
	/// it — for most definitions after their existence check, so a statement
	/// that finds its definition present never evaluates it.
	async fn comment(&mut self, expr: &Expr) -> Lowered<Comment> {
		if let Some(written) = written_comment(expr) {
			let operand = self.constant(written);
			return Ok(Comment {
				evaluated: false,
				operand,
			});
		}
		let value = crate::exec::vm::host::out_of_line(|| self.evaluated(expr)).await?;
		self.store(StoreInstr::CastComment {
			src: value,
			dst: value,
		});
		Ok(Comment {
			evaluated: true,
			operand: Operand::Reg(value),
		})
	}

	/// Set the `comment` of the definition in `definition`, formed with
	/// [`comment_text`], to the comment, when it was evaluated.
	fn set_comment(&mut self, definition: Reg, comment: &Comment) {
		if comment.evaluated {
			self.set_field(definition, "comment", comment.operand);
		}
	}

	/// Raise what the session's missing namespace or database raises
	/// (`Options::ns_db`), before a body that checks the session resolves an
	/// evaluated name; nothing when every name in `exprs` is written, which
	/// cannot fail.
	fn session_selected(&mut self, exprs: &[&Expr]) -> Lowered<()> {
		if exprs.iter().all(|expr| written_text(expr).is_some()) {
			return Ok(());
		}
		let selected = self.register()?;
		self.store(StoreInstr::SessionNs {
			dst: selected,
		});
		self.store(StoreInstr::SessionDb {
			dst: selected,
		});
		self.release(selected);
		Ok(())
	}

	/// Set `field` of the definition in `definition`, formed with
	/// [`Name::text`], to the name, when it was evaluated.
	fn set_name(&mut self, definition: Reg, field: &str, name: &Name) {
		if name.written.is_none() {
			self.set_field(definition, field, name.operand);
		}
	}
}

/// The name `expr` gives by being written, an identifier or a string
/// literal, or `None` when it gives one only by being evaluated.
fn written_text(expr: &Expr) -> Option<String> {
	match expr {
		Expr::Literal(Literal::String(s)) => Some(s.as_str().to_owned()),
		expr => crate::ddl::scalar::written_name(expr),
	}
}

/// The comment `expr` gives by being written, a string or `NONE`, or `None`
/// when it gives one only by being evaluated.
fn written_comment(expr: &Expr) -> Option<Value> {
	match expr {
		Expr::Literal(Literal::None) => Some(Value::None),
		Expr::Literal(Literal::String(s)) => Some(Value::String(s.clone())),
		_ => None,
	}
}

/// The comment to form a definition with while lowering: the written
/// comment, or none, which [`Lowering::set_comment`] replaces once the
/// comment is evaluated.
fn comment_text(expr: &Expr) -> Option<String> {
	match written_comment(expr) {
		Some(Value::String(s)) => Some(s.as_str().to_owned()),
		_ => None,
	}
}

/// Canonicalise an idiom's parts: string literal subscripts become fields,
/// integer literal subscripts are kept, and unsupported parts return None.
fn canonical_field_name(idiom: &crate::expr::Idiom) -> Option<crate::expr::Idiom> {
	use crate::expr::{Expr, Literal, Part};
	let mut out = Vec::with_capacity(idiom.0.len());
	for part in &idiom.0 {
		match part {
			Part::Field(_) | Part::All | Part::First | Part::Last | Part::Flatten => {
				out.push(part.clone())
			}
			Part::Value(Expr::Literal(Literal::String(s))) => out.push(Part::Field(s.clone())),
			Part::Value(Expr::Literal(Literal::Integer(i))) => {
				out.push(Part::Value(Expr::Literal(Literal::Integer(*i))))
			}
			_ => return None,
		}
	}
	Some(crate::expr::Idiom(out))
}

#[cfg(all(test, feature = "kv-mem"))]
mod key_component_tests {
	use crate::exec::operators::test_util::TestDb;
	use crate::exec::vm::program::{FieldCheck, FieldRefusal, Instr, Program, StoreInstr};
	use crate::exec::vm::tests::{bind, planner};

	/// The database the statements are lowered against: a table whose record
	/// id names its components, one a permission reads, one a field reads
	/// through a function it hands the record to, one a view that selects every
	/// field holds rows of, one an index reads, a view, a relation and a plain
	/// table.
	const SCHEMA: &str = "DEFINE TABLE reading; \
		 DEFINE FIELD id ON reading TYPE [city: string, time: datetime]; \
		 DEFINE FUNCTION fn::city_of($r: any) { RETURN $r.city; }; \
		 DEFINE FIELD id ON opaque TYPE [city: string, time: datetime]; \
		 DEFINE FIELD label ON opaque COMPUTED fn::city_of($this); \
		 DEFINE FIELD id ON stars TYPE [city: string, n: int]; \
		 CREATE stars:['London', 1]; \
		 DEFINE TABLE everything AS SELECT * FROM stars; \
		 DEFINE FIELD temp ON reading TYPE int; \
		 DEFINE TABLE watched PERMISSIONS FOR select WHERE city = 'London'; \
		 DEFINE FIELD id ON watched TYPE [city: string, time: datetime]; \
		 DEFINE TABLE plain; \
		 DEFINE FIELD place ON plain TYPE string; \
		 DEFINE TABLE by_place AS SELECT place, count() AS n FROM plain GROUP BY place; \
		 DEFINE TABLE rel TYPE RELATION; \
		 DEFINE TABLE tagged; \
		 DEFINE INDEX by_tag ON tagged FIELDS tag;";

	/// The program `sql` lowers to over [`SCHEMA`].
	async fn lowered(db: &TestDb, sql: &str) -> Program {
		let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
		let bound = bind(&ctx, sql).await;
		let physical = crate::exec::vm::optimize::optimize(
			crate::exec::vm::map::map(&planner(&ctx), &bound).await.expect("maps"),
		);
		let program =
			crate::exec::vm::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		ctx.txn().cancel().await.expect("cancel");
		program
	}

	/// The store instructions of `program` and of the programs it holds.
	fn store_instrs<'a>(program: &'a Program, out: &mut Vec<&'a StoreInstr>) {
		for pipeline in program.pipelines.iter().chain(&program.host_code) {
			for instr in &pipeline.code {
				if let Instr::Store(store) = instr {
					out.push(store);
				}
			}
		}
		for unit in program.units.iter() {
			store_instrs(unit, out);
		}
	}

	/// The refusals `program` makes of a field definition.
	fn refusals(program: &Program) -> Vec<FieldRefusal> {
		let mut stores = Vec::new();
		store_instrs(program, &mut stores);
		stores
			.into_iter()
			.filter_map(|store| match store {
				StoreInstr::RefuseField {
					refusal,
				} => Some(refusal.clone()),
				_ => None,
			})
			.collect()
	}

	/// What a field statement refuses of its own clauses, and of a field under
	/// the name of a record id component, lowers to the refusal the body raises
	/// against the catalog it reads; one that refuses neither lowers to none.
	/// The components an `id` names, and the definitions reading them that
	/// `REMOVE FIELD id` would strand, are judged when the statement runs
	/// ([`the_record_id_components_of_the_id_are_judged_when_the_statement_runs`]).
	#[tokio::test]
	async fn the_clauses_of_a_field_definition_are_checked_when_it_is_lowered() {
		let cases = [
			("DEFINE FIELD id ON plain TYPE [place: string, time: datetime]", vec![]),
			("DEFINE FIELD id ON rel TYPE [city: string, time: datetime]", vec![]),
			(
				"DEFINE FIELD id ON plain TYPE [zone: string, price: float]",
				vec![FieldRefusal::IdComponentUnsupportedKind {
					table: "plain".to_owned(),
					component: "price".to_owned(),
					kind: "float".to_owned(),
				}],
			),
			(
				"DEFINE FIELD id ON plain TYPE [zone: string, time: datetime] PERMISSIONS NONE",
				vec![FieldRefusal::IdFieldSelectPermission("plain".to_owned())],
			),
			(
				"DEFINE FIELD city ON reading TYPE string",
				vec![FieldRefusal::IdComponentClash {
					name: "city".to_owned(),
					table: "reading".to_owned(),
				}],
			),
			("DEFINE FIELD pressure ON reading TYPE int", vec![]),
			(
				"REMOVE FIELD city ON reading",
				vec![FieldRefusal::IdComponentNotAField {
					name: "city".to_owned(),
					table: "reading".to_owned(),
				}],
			),
			(
				"ALTER FIELD time ON reading TYPE string",
				vec![FieldRefusal::IdComponentNotAField {
					name: "time".to_owned(),
					table: "reading".to_owned(),
				}],
			),
			("REMOVE FIELD id ON watched", vec![]),
			("REMOVE FIELD id ON stars", vec![]),
		];
		let db = TestDb::new(SCHEMA).await;
		for (sql, expected) in cases {
			assert_eq!(refusals(&lowered(&db, sql).await), expected, "`{sql}`");
		}
	}

	/// The error each statement of `sql` raises when it runs, in a database
	/// [`SCHEMA`] leaves; `None` for one that succeeds.
	async fn run_errors(sql: &str) -> Vec<Option<String>> {
		let db = TestDb::new(SCHEMA).await;
		db.datastore()
			.execute(sql, &TestDb::owner(), None)
			.await
			.expect("the query runs")
			.into_iter()
			.map(|response| response.result.err().map(|error| error.to_string()))
			.collect()
	}

	/// The record id components a `DEFINE FIELD id` names, and the definitions
	/// a `REMOVE FIELD id` would leave reading a component, are judged when the
	/// statement runs, against the catalog and the rows of the views over the
	/// table as the statements before it leave them: a view that selects every
	/// field reads every name once it holds a row, whether that row was written
	/// before the block or by a statement of the block before the change.
	#[tokio::test]
	async fn the_record_id_components_of_the_id_are_judged_when_the_statement_runs() {
		let under_wildcard = |table: &str, view: &str| {
			Some(
				FieldRefusal::IdComponentsUnderWildcardView {
					table: table.to_owned(),
					view: view.to_owned(),
				}
				.error()
				.to_string(),
			)
		};
		let cases: Vec<(&str, Vec<Option<String>>)> = vec![
			(
				"DEFINE FIELD id ON plain TYPE [place: string, time: datetime]",
				vec![Some(
					FieldRefusal::IdComponentShadowsField {
						name: "place".to_owned(),
						table: "plain".to_owned(),
					}
					.error()
					.to_string(),
				)],
			),
			("DEFINE FIELD id ON plain TYPE [zone: string, time: datetime]", vec![None]),
			(
				"DEFINE FIELD id ON rel TYPE [city: string, time: datetime]",
				vec![Some(
					FieldRefusal::IdComponentsOnRelation("rel".to_owned()).error().to_string(),
				)],
			),
			(
				"DEFINE FIELD id ON by_place TYPE [place: string, n: int]",
				vec![Some(
					FieldRefusal::IdComponentsOnView("by_place".to_owned()).error().to_string(),
				)],
			),
			(
				"DEFINE FIELD id ON tagged TYPE [tag: string, time: datetime]",
				vec![Some(
					FieldRefusal::IdComponentReadByExisting {
						table: "tagged".to_owned(),
						component: "tag".to_owned(),
						what: "index".to_owned(),
						name: "by_tag".to_owned(),
					}
					.error()
					.to_string(),
				)],
			),
			(
				"REMOVE FIELD id ON watched",
				vec![Some(
					FieldRefusal::IdComponentInUse {
						table: "watched".to_owned(),
						component: "city".to_owned(),
						what: "table permission".to_owned(),
						name: "select".to_owned(),
					}
					.error()
					.to_string(),
				)],
			),
			("REMOVE FIELD id ON reading", vec![None]),
			(
				"REMOVE FIELD id ON opaque",
				vec![Some(
					FieldRefusal::IdComponentReadUnresolved {
						table: "opaque".to_owned(),
						component: "city".to_owned(),
						what: "field".to_owned(),
						name: "label".to_owned(),
					}
					.error()
					.to_string(),
				)],
			),
			(
				"DEFINE FIELD OVERWRITE id ON stars TYPE [place: string, n: int]",
				vec![under_wildcard("stars", "everything")],
			),
			("REMOVE FIELD id ON stars", vec![under_wildcard("stars", "everything")]),
			// The rows the block's own statements leave decide.
			("{ DELETE stars; REMOVE FIELD id ON stars; }", vec![None]),
			(
				"DEFINE TABLE fresh AS SELECT * FROM opaque; \
				 { CREATE opaque:['Paris', d'2024-01-01T00:00:00Z'] RETURN NONE; \
				 DEFINE FIELD OVERWRITE id ON opaque TYPE [time: datetime, city: string]; }",
				vec![None, under_wildcard("opaque", "fresh")],
			),
			// `IF NOT EXISTS` over an existing `id` judges nothing.
			("DEFINE FIELD IF NOT EXISTS id ON stars TYPE [place: string, n: int]", vec![None]),
		];
		for (sql, expected) in cases {
			assert_eq!(run_errors(sql).await, expected, "`{sql}`");
		}
	}

	/// The scan of the stored records, the run-time check of the components
	/// and the refusal of a conversion are instructions of the statements that
	/// need them: every definition, alteration of the type or removal of the
	/// `id` checks its components, a definition of an id that names none scans
	/// no record, and a field that is not the id, an alteration that keeps the
	/// type and a table that stays a plain table lower to none.
	#[tokio::test]
	async fn the_instructions_of_the_record_id_components_are_lowered_only_where_they_apply() {
		// The statement, then how many record scans, run-time checks of the
		// components and conversion refusals it lowers to.
		let cases = [
			("DEFINE FIELD id ON plain TYPE string", 0, 1, 0),
			("DEFINE FIELD id ON plain TYPE [zone: string, time: datetime]", 1, 1, 0),
			("DEFINE FIELD pressure ON reading TYPE int", 0, 0, 0),
			("REMOVE FIELD id ON reading", 0, 1, 0),
			("REMOVE FIELD temp ON reading", 0, 0, 0),
			("ALTER FIELD id ON reading TYPE [city: string, time: datetime]", 1, 1, 0),
			("ALTER FIELD id ON reading COMMENT 'c'", 0, 0, 0),
			("ALTER FIELD id ON plain TYPE string", 0, 1, 0),
			("ALTER FIELD id ON plain DROP TYPE", 0, 1, 0),
			("ALTER FIELD temp ON reading COMMENT 'c'", 0, 0, 0),
			("DEFINE TABLE OVERWRITE plain SCHEMAFULL", 0, 0, 0),
			("DEFINE TABLE OVERWRITE plain TYPE RELATION", 0, 0, 1),
			("DEFINE TABLE OVERWRITE plain AS SELECT * FROM reading", 0, 0, 1),
			("ALTER TABLE plain COMMENT 'c'", 0, 0, 0),
			("ALTER TABLE rel COMMENT 'c'", 0, 0, 0),
		];
		let db = TestDb::new(SCHEMA).await;
		for (sql, scans, checks, conversions) in cases {
			let program = lowered(&db, sql).await;
			let mut stores = Vec::new();
			store_instrs(&program, &mut stores);
			let count = |wanted: fn(&StoreInstr) -> bool| {
				stores.iter().filter(|store| wanted(store)).count()
			};
			assert_eq!(
				count(|store| matches!(store, StoreInstr::ValidateRecordKeys { .. })),
				scans,
				"record scans of `{sql}`"
			);
			assert_eq!(
				count(|store| matches!(
					store,
					StoreInstr::FieldCheck {
						check: FieldCheck::KeyComponents { .. } | FieldCheck::IdUnread { .. }
					}
				)),
				checks,
				"component checks of `{sql}`"
			);
			assert_eq!(
				count(|store| matches!(store, StoreInstr::RefuseNamedIdConversion { .. })),
				conversions,
				"conversion refusals of `{sql}`"
			);
		}
	}
}
