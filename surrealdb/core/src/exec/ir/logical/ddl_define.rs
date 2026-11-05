//! Planning `DEFINE`: the operands, stored clauses, attributes and definition template of each
//! form.
//!
//! Operands are allocated in the order the form's body evaluates them (see
//! [`guard_order`](super::ddl::guard_order)), so a slot is also a position in evaluation order.
//! Each form's template is the stored type the body writes, built from the statement alone, with
//! one hole for each field an operand fills when the statement runs.

use surrealdb_strand::{Strand, TableName};

use super::ddl::{
	DdlForm, DdlOperand, DdlPlan, PlanError, Planned, Slots, alias_refusal, comment_text,
	literal_value, protected_arg_refusal, stored_value, template, written_comment, written_text,
};
use super::ops::{DefineAttrs, LogOp};
use crate::exec::ir::iface::region_kind::StoredRole;
use crate::exec::ir::iface::statements::{
	AccessDurationShape, AccessSecrets, AccessTypeShape, BearerShape, ConfigShape, DdlChecks,
	DefaultConfigShape, DefaultShape, DefineAccess, DefineApi, DefineField, DefineFunction,
	DefineObject, DefineTable, DefineUser, IndexColumn, IndexSpec, JwtIssueShape, JwtShape,
	JwtVerifyShape, OnDeleteShape, OperandSlot, Place, PlacePart, RecordAccessShape,
	ReferenceShape, TemplateHole, UnaddressedPart, UserDurationShape, UserSecrets, ViewShape,
};
use crate::exec::vm::codec::Codec;
use crate::expr::access_type::{AccessType, BearerAccess, JwtAccess, JwtAccessVerify};
use crate::expr::index_kind::Index;
use crate::expr::reference::ReferenceDeleteStrategy;
use crate::expr::statements::DefineStatement;
use crate::expr::statements::define::config::ConfigInner;
use crate::expr::statements::define::field::DefineDefault;
use crate::expr::{Base, Expr, Literal, Part, SelectStatement, View};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

const EDIT: Action = Action::Edit;

/// The holes of each stored type, by the field an operand fills.
const NAMESPACE_HOLES: &[TemplateHole] =
	&[TemplateHole::Other("namespace_id"), TemplateHole::Name, TemplateHole::Comment];
const DATABASE_HOLES: &[TemplateHole] = &[
	TemplateHole::Other("namespace_id"),
	TemplateHole::Other("database_id"),
	TemplateHole::Name,
	TemplateHole::Comment,
];
const FUNCTION_HOLES: &[TemplateHole] = &[TemplateHole::Comment, TemplateHole::AuthLimit];
const ANALYZER_HOLES: &[TemplateHole] = &[TemplateHole::Name, TemplateHole::Comment];
const PARAM_HOLES: &[TemplateHole] = &[TemplateHole::Value, TemplateHole::Comment];
const TABLE_HOLES: &[TemplateHole] = &[
	TemplateHole::Other("namespace_id"),
	TemplateHole::Other("database_id"),
	TemplateHole::Other("table_id"),
	TemplateHole::Name,
	TemplateHole::Comment,
	TemplateHole::Other("cache_fields_ts"),
	TemplateHole::Other("cache_events_ts"),
	TemplateHole::Other("cache_tables_ts"),
	TemplateHole::Other("cache_indexes_ts"),
	TemplateHole::Other("cache_lives_ts"),
	TemplateHole::Other("graph_folded"),
	TemplateHole::Other("graph_doc_ids"),
	TemplateHole::Other("graph_inline_gen"),
];
const EVENT_HOLES: &[TemplateHole] = &[
	TemplateHole::Name,
	TemplateHole::Other("target_table"),
	TemplateHole::Comment,
	TemplateHole::AuthLimit,
];
const FIELD_HOLES: &[TemplateHole] =
	&[TemplateHole::Other("name"), TemplateHole::Other("table"), TemplateHole::Comment];
const INDEX_HOLES: &[TemplateHole] = &[
	TemplateHole::Name,
	TemplateHole::Other("table_name"),
	TemplateHole::Comment,
	TemplateHole::Other("cols"),
	TemplateHole::Other("index_id"),
];
const USER_HOLES: &[TemplateHole] = &[
	TemplateHole::Name,
	TemplateHole::Comment,
	TemplateHole::Other("hash"),
	TemplateHole::Other("code"),
	TemplateHole::Other("scram"),
	TemplateHole::Other("token_duration"),
	TemplateHole::Other("session_duration"),
];
const MODEL_HOLES: &[TemplateHole] = &[TemplateHole::Comment];
const MODULE_HOLES: &[TemplateHole] = &[TemplateHole::Comment, TemplateHole::AuthLimit];
const ACCESS_HOLES: &[TemplateHole] = &[
	TemplateHole::Name,
	TemplateHole::Comment,
	TemplateHole::Other("grant_duration"),
	TemplateHole::Other("token_duration"),
	TemplateHole::Other("session_duration"),
	TemplateHole::Other("credentials"),
];
const CONFIG_HOLES: &[TemplateHole] = &[
	TemplateHole::Other("namespace"),
	TemplateHole::Other("database"),
	TemplateHole::Other("middleware"),
];
const API_HOLES: &[TemplateHole] = &[
	TemplateHole::Other("path"),
	TemplateHole::Comment,
	TemplateHole::Other("middleware"),
	TemplateHole::AuthLimit,
];
const TYPE_HOLES: &[TemplateHole] = &[TemplateHole::Other("id"), TemplateHole::Comment];
const BUCKET_HOLES: &[TemplateHole] = &[
	TemplateHole::Name,
	TemplateHole::Other("backend"),
	TemplateHole::Comment,
	TemplateHole::Other("presign_nonce"),
];
const SEQUENCE_HOLES: &[TemplateHole] = &[
	TemplateHole::Name,
	TemplateHole::Other("batch"),
	TemplateHole::Other("start"),
	TemplateHole::Other("timeout"),
];

/// The holes of the template each `DEFINE` form carries.
pub(crate) fn template_holes(form: DdlForm) -> Option<&'static [TemplateHole]> {
	Some(match form {
		DdlForm::DefineNamespace => NAMESPACE_HOLES,
		DdlForm::DefineDatabase => DATABASE_HOLES,
		DdlForm::DefineFunction => FUNCTION_HOLES,
		DdlForm::DefineAnalyzer => ANALYZER_HOLES,
		DdlForm::DefineParam => PARAM_HOLES,
		DdlForm::DefineTable => TABLE_HOLES,
		DdlForm::DefineEvent => EVENT_HOLES,
		DdlForm::DefineField => FIELD_HOLES,
		DdlForm::DefineIndex => INDEX_HOLES,
		DdlForm::DefineUser => USER_HOLES,
		DdlForm::DefineModel => MODEL_HOLES,
		DdlForm::DefineModule => MODULE_HOLES,
		DdlForm::DefineAccess => ACCESS_HOLES,
		DdlForm::DefineConfig => CONFIG_HOLES,
		DdlForm::DefineApi => API_HOLES,
		DdlForm::DefineBucket => BUCKET_HOLES,
		DdlForm::DefineSequence => SEQUENCE_HOLES,
		DdlForm::DefineType => TYPE_HOLES,
		DdlForm::RemoveNamespace
		| DdlForm::RemoveDatabase
		| DdlForm::RemoveFunction
		| DdlForm::RemoveAnalyzer
		| DdlForm::RemoveAccess
		| DdlForm::RemoveParam
		| DdlForm::RemoveTable
		| DdlForm::RemoveEvent
		| DdlForm::RemoveField
		| DdlForm::RemoveIndex
		| DdlForm::RemoveUser
		| DdlForm::RemoveModel
		| DdlForm::RemoveApi
		| DdlForm::RemoveBucket
		| DdlForm::RemoveSequence
		| DdlForm::RemoveModule
		| DdlForm::RemoveConfig
		| DdlForm::RemoveType
		| DdlForm::AlterType
		| DdlForm::AlterSystem
		| DdlForm::AlterNamespace
		| DdlForm::AlterDatabase
		| DdlForm::AlterTable
		| DdlForm::AlterApi
		| DdlForm::AlterEvent
		| DdlForm::AlterIndex
		| DdlForm::AlterSequence
		| DdlForm::AlterField
		| DdlForm::AlterParam
		| DdlForm::AlterBucket
		| DdlForm::AlterConfig
		| DdlForm::AlterAnalyzer
		| DdlForm::AlterFunction
		| DdlForm::AlterUser
		| DdlForm::AlterAccess
		| DdlForm::AlterModule
		| DdlForm::RebuildIndex => return None,
	})
}

/// Plans a `DEFINE`. `checks` is what the statement checks against the catalog.
pub(super) fn plan_define<'e>(
	define: &'e DefineStatement,
	checks: DdlChecks,
	codec: &'static Codec,
) -> Planned<DdlPlan<'e>> {
	let mut s = Slots::new();
	let mut planning_refusal = None;
	let (form, allow, object, tmpl, mode, refusal) = match define {
		DefineStatement::Namespace(stmt) => {
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let comment = s.comment(&stmt.comment)?;
			let definition = crate::catalog::NamespaceDefinition {
				namespace_id: crate::catalog::NamespaceId(stmt.id.unwrap_or(0)),
				name: Strand::new(written_text(&stmt.name).unwrap_or_default()),
				comment: comment_text(&stmt.comment),
			};
			(
				DdlForm::DefineNamespace,
				(EDIT, ResourceKind::Namespace, Base::Root),
				DefineObject::Namespace {
					id: stmt.id,
					name,
					comment,
				},
				template(
					"NamespaceDefinition",
					stored_value(codec, &definition, "NamespaceDefinition")?,
					NAMESPACE_HOLES,
				),
				stmt.kind.clone(),
				None,
			)
		}
		DefineStatement::Database(stmt) => {
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let comment = s.comment(&stmt.comment)?;
			let definition = crate::catalog::DatabaseDefinition {
				namespace_id: crate::catalog::NamespaceId(0),
				database_id: crate::catalog::DatabaseId(stmt.id.unwrap_or(0)),
				name: Strand::new(written_text(&stmt.name).unwrap_or_default()),
				comment: comment_text(&stmt.comment),
				changefeed: stmt.changefeed,
				strict: stmt.strict,
				compatibility: Default::default(),
			};
			(
				DdlForm::DefineDatabase,
				(EDIT, ResourceKind::Database, Base::Ns),
				DefineObject::Database {
					id: stmt.id,
					name,
					strict: stmt.strict,
					changefeed: stmt.changefeed,
					comment,
					compatibility: stmt.compatibility,
				},
				template(
					"DatabaseDefinition",
					stored_value(codec, &definition, "DatabaseDefinition")?,
					DATABASE_HOLES,
				),
				stmt.kind.clone(),
				None,
			)
		}
		DefineStatement::Function(stmt) => {
			let comment = s.comment(&stmt.comment)?;
			let params = stmt.args.iter().map(|(name, _)| Strand::new(name)).collect();
			let body = s.region(
				crate::exec::ir::iface::region_kind::RegionKind::StoredClause(StoredRole::Body),
				super::ddl::Body::Function {
					block: Box::new(Expr::Block(Box::new(stmt.block.clone()))),
					params,
				},
			)?;
			let permissions = s.permission(&stmt.permissions)?;
			let definition = crate::catalog::FunctionDefinition {
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
			(
				DdlForm::DefineFunction,
				(EDIT, ResourceKind::Function, Base::Db),
				DefineObject::Function(Box::new(DefineFunction {
					name: stmt.name.clone(),
					args: stmt.args.clone().into_boxed_slice(),
					returns: stmt.returns.clone(),
					graphql_alias: stmt.graphql_alias.clone(),
					graphql_deprecated: stmt.graphql_deprecated.clone(),
					comment,
					body,
					permissions,
				})),
				template(
					"StoredFunctionDefinition",
					stored_value(codec, &definition.to_stored(), "StoredFunctionDefinition")?,
					FUNCTION_HOLES,
				),
				stmt.kind.clone(),
				protected_arg_refusal(&stmt.args)
					.or_else(|| alias_refusal(&stmt.graphql_alias, "function")),
			)
		}
		DefineStatement::Analyzer(stmt) => {
			let comment = s.comment(&stmt.comment)?;
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let definition = crate::catalog::AnalyzerDefinition {
				name: Strand::new(written_text(&stmt.name).unwrap_or_default()),
				function: stmt.function.clone(),
				tokenizers: stmt.tokenizers.clone(),
				filters: stmt.filters.clone(),
				comment: comment_text(&stmt.comment),
			};
			(
				DdlForm::DefineAnalyzer,
				(EDIT, ResourceKind::Analyzer, Base::Db),
				DefineObject::Analyzer {
					name,
					function: stmt.function.clone(),
					tokenizers: stmt.tokenizers.clone().map(Vec::into_boxed_slice),
					filters: stmt.filters.clone().map(Vec::into_boxed_slice),
					comment,
				},
				template(
					"AnalyzerDefinition",
					stored_value(codec, &definition, "AnalyzerDefinition")?,
					ANALYZER_HOLES,
				),
				stmt.kind.clone(),
				None,
			)
		}
		DefineStatement::Param(stmt) => {
			let value = s.evaluated(DdlOperand::Value, &stmt.value)?;
			let comment = s.comment(&stmt.comment)?;
			let permissions = s.permission(&stmt.permissions)?;
			let definition = crate::catalog::ParamDefinition {
				value: Value::None,
				name: stmt.name.clone(),
				comment: comment_text(&stmt.comment),
				permissions: stmt.permissions.clone(),
				permissions_body: crate::catalog::LogicalPermissionBody::of(&stmt.permissions),
			};
			(
				DdlForm::DefineParam,
				(EDIT, ResourceKind::Parameter, Base::Db),
				DefineObject::Param {
					name: stmt.name.clone(),
					value,
					comment,
					permissions,
				},
				template(
					"StoredParamDefinition",
					stored_value(codec, &definition.to_stored(), "StoredParamDefinition")?,
					PARAM_HOLES,
				),
				stmt.kind.clone(),
				None,
			)
		}
		DefineStatement::Table(stmt) => {
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let (view, view_definition) = match stmt
				.view
				.as_ref()
				.map(|view| (view, crate::catalog::view::view_to_definition(view)))
			{
				Some((view, Ok(definition))) => {
					let query = s.region(
						crate::exec::ir::iface::region_kind::RegionKind::StoredClause(
							StoredRole::View,
						),
						super::ddl::Body::Select(Box::new(view_select(view))),
					)?;
					let backfill = match backfill_select(&definition)? {
						Some(select) => Some(s.region(
							crate::exec::ir::iface::region_kind::RegionKind::Subquery,
							super::ddl::Body::Select(Box::new(select)),
						)?),
						None => None,
					};
					let mut group_fields = Vec::new();
					for expr in group_field_exprs(&definition)? {
						group_fields.push(s.region(
							crate::exec::ir::iface::region_kind::RegionKind::Clause(
								crate::exec::ir::iface::region_kind::ClauseRole::Projection,
							),
							super::ddl::Body::Clause(std::borrow::Cow::Owned(expr)),
						)?);
					}
					(
						Some(ViewShape {
							what: view.what.clone().into_boxed_slice(),
							query,
							backfill,
							group_fields: group_fields.into_boxed_slice(),
						}),
						Some(definition),
					)
				}
				// A view the catalog refuses holds no regions: the statement's checks carry its
				// refusal (`DdlChecks::DefineTable::view_refusal`), and the definition is formed
				// without it, as the catalog forms it.
				Some((_, Err(_))) | None => (None, None),
			};
			let permissions = s.permissions(&stmt.permissions)?;
			let comment = s.comment(&stmt.comment)?;
			let tb = TableName::new(written_text(&stmt.name).unwrap_or_default());
			let mut definition = table_template(stmt, tb, view_definition);
			// The type as the clauses of a lightweight relation leave it.
			if let DdlChecks::DefineTable {
				table_type,
				..
			} = &checks
			{
				definition.table_type = table_type.clone();
			}
			(
				DdlForm::DefineTable,
				(EDIT, ResourceKind::Table, Base::Db),
				DefineObject::Table(Box::new(DefineTable {
					id: stmt.id,
					name,
					drop: stmt.drop,
					full: stmt.full,
					view,
					permissions,
					changefeed: stmt.changefeed,
					comment,
					table_type: stmt.table_type.clone(),
					graphql_alias: stmt.graphql_alias.clone(),
					graphql_deprecated: stmt.graphql_deprecated.clone(),
					inline_edges_cap: stmt.inline_edges_cap,
					inline_refs_cap: stmt.inline_refs_cap,
				})),
				template(
					"StoredTableDefinition",
					stored_value(codec, &definition.to_stored(), "StoredTableDefinition")?,
					TABLE_HOLES,
				),
				stmt.kind.clone(),
				alias_refusal(&stmt.graphql_alias, "table"),
			)
		}
		DefineStatement::Event(stmt) => {
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let target_table = s.name(DdlOperand::Table, &stmt.target_table)?;
			let comment = s.comment(&stmt.comment)?;
			let when = s.stored(StoredRole::When, &stmt.when)?;
			let mut then = Vec::with_capacity(stmt.then.len());
			for expr in &stmt.then {
				then.push(s.stored(StoredRole::Then, expr)?);
			}
			let definition = crate::catalog::EventDefinition::new(
				Strand::new(written_text(&stmt.name).unwrap_or_default()),
				TableName::new(written_text(&stmt.target_table).unwrap_or_default()),
				stmt.when.clone(),
				stmt.then.clone(),
				stmt.event_kind.clone().into(),
				crate::catalog::auth::AuthLimit::default(),
				comment_text(&stmt.comment),
			);
			(
				DdlForm::DefineEvent,
				(EDIT, ResourceKind::Event, Base::Db),
				DefineObject::Event {
					name,
					target_table,
					when,
					then: then.into_boxed_slice(),
					comment,
					event_kind: stmt.event_kind.clone(),
				},
				template(
					"StoredEventDefinition",
					stored_value(codec, &definition.to_stored(), "StoredEventDefinition")?,
					EVENT_HOLES,
				),
				stmt.kind.clone(),
				None,
			)
		}
		DefineStatement::Field(stmt) => {
			let comment = s.comment(&stmt.comment)?;
			let name = s.field_name(&stmt.name)?;
			let what = s.name(DdlOperand::Table, &stmt.what)?;
			let value = stmt.value.as_ref().map(|e| s.stored(StoredRole::Value, e)).transpose()?;
			let assert =
				stmt.assert.as_ref().map(|e| s.stored(StoredRole::Assert, e)).transpose()?;
			let computed =
				stmt.computed.as_ref().map(|e| s.stored(StoredRole::Computed, e)).transpose()?;
			let default = match &stmt.default {
				DefineDefault::None => DefaultShape::None,
				DefineDefault::Always(expr) => {
					DefaultShape::Always(s.stored(StoredRole::Default, expr)?)
				}
				DefineDefault::Set(expr) => DefaultShape::Set(s.stored(StoredRole::Default, expr)?),
			};
			let permissions = s.permissions(&stmt.permissions)?;
			let reference = match &stmt.reference {
				None => None,
				Some(reference) => Some(ReferenceShape {
					on_delete: match &reference.on_delete {
						ReferenceDeleteStrategy::Reject => OnDeleteShape::Reject,
						ReferenceDeleteStrategy::Ignore => OnDeleteShape::Ignore,
						ReferenceDeleteStrategy::Cascade => OnDeleteShape::Cascade,
						ReferenceDeleteStrategy::Unset => OnDeleteShape::Unset,
						ReferenceDeleteStrategy::Custom(expr) => {
							OnDeleteShape::Custom(s.stored(StoredRole::Then, expr)?)
						}
					},
				}),
			};
			let definition = field_template(stmt, &comment_text(&stmt.comment));
			(
				DdlForm::DefineField,
				(EDIT, ResourceKind::Field, Base::Db),
				DefineObject::Field(Box::new(DefineField {
					name,
					what,
					field_kind: stmt.field_kind.clone(),
					flexible: stmt.flexible,
					readonly: stmt.readonly,
					inline: stmt.inline,
					value,
					assert,
					computed,
					default,
					permissions,
					comment,
					reference,
					graphql_alias: stmt.graphql_alias.clone(),
					graphql_deprecated: stmt.graphql_deprecated.clone(),
					migrate: stmt.migrate.clone().map(Box::new),
					sql_type: stmt.sql_type.clone(),
				})),
				template(
					"StoredFieldDefinition",
					stored_value(codec, &definition.to_stored(), "StoredFieldDefinition")?,
					FIELD_HOLES,
				),
				stmt.kind.clone(),
				None,
			)
		}
		DefineStatement::Index(stmt) => {
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let what = s.name(DdlOperand::Table, &stmt.what)?;
			let mut columns = Vec::with_capacity(stmt.cols.len());
			for column in &stmt.cols {
				columns.push(index_column(&mut s, column)?);
			}
			let comment = s.comment(&stmt.comment)?;
			let index = match &stmt.index {
				Index::Idx => IndexSpec::Idx,
				Index::Uniq {
					..
				} => IndexSpec::Uniq,
				Index::Hnsw(params) => IndexSpec::Hnsw(params.clone()),
				Index::FullText(params) => IndexSpec::FullText(params.clone()),
				Index::Count(condition) => IndexSpec::Count(
					condition
						.as_ref()
						.map(|cond| s.stored(StoredRole::When, &cond.0))
						.transpose()?,
				),
				Index::DiskAnn(params) => IndexSpec::DiskAnn(params.clone()),
				Index::Spatial => IndexSpec::Spatial,
			};
			let definition = crate::catalog::IndexDefinition {
				index_id: crate::catalog::IndexId(0),
				name: Strand::new(written_text(&stmt.name).unwrap_or_default()),
				table_name: TableName::new(written_text(&stmt.what).unwrap_or_default()),
				cols: written_index_columns(&stmt.cols).unwrap_or_default(),
				index: stmt.index.clone().into(),
				count_cond: if let Index::Count(Some(cond)) = &stmt.index {
					Some(cond.clone())
				} else {
					None
				},
				comment: comment_text(&stmt.comment),
				prepare_remove: false,
				format_version: crate::catalog::INDEX_FORMAT_VERSION,
				nulls_not_distinct: matches!(
					stmt.index,
					Index::Uniq {
						nulls_not_distinct: true
					}
				),
			};
			(
				DdlForm::DefineIndex,
				(EDIT, ResourceKind::Index, Base::Db),
				DefineObject::Index {
					name,
					name_text: surrealdb_types::ToSql::to_sql(&stmt.name).into_boxed_str(),
					what,
					columns: columns.into_boxed_slice(),
					index,
					comment,
					concurrently: stmt.concurrently,
				},
				template(
					"StoredIndexDefinition",
					stored_value(codec, &definition.to_stored(), "StoredIndexDefinition")?,
					INDEX_HOLES,
				),
				stmt.kind.clone(),
				None,
			)
		}
		DefineStatement::User(stmt) => {
			let token = s.duration(DdlOperand::TokenDuration, &stmt.duration.token)?;
			let session = s.duration(DdlOperand::SessionDuration, &stmt.duration.session)?;
			let comment = s.comment(&stmt.comment)?;
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let password = s.password(&stmt.pending_password)?;
			let definition = crate::catalog::UserDefinition {
				name: Strand::new(written_text(&stmt.name).unwrap_or_default()),
				// The derived credentials are holes filled from `UserSecrets` when the statement
				// runs, so the encoded template, which the text form prints, holds none.
				hash: String::new(),
				code: String::new(),
				roles: stmt.roles.clone(),
				token_duration: cast_duration(&stmt.duration.token),
				session_duration: cast_duration(&stmt.duration.session),
				comment: comment_text(&stmt.comment),
				base: stmt.base.into(),
				scram: None,
			};
			(
				DdlForm::DefineUser,
				(EDIT, ResourceKind::Actor, stmt.base),
				DefineObject::User(Box::new(DefineUser {
					name,
					base: stmt.base,
					password,
					secrets: UserSecrets {
						hash: stmt.hash.clone(),
						hash_is_set: stmt.hash_is_set,
						code: stmt.code.clone(),
						scram: stmt.scram.clone(),
					},
					roles: stmt.roles.clone().into_boxed_slice(),
					duration: UserDurationShape {
						token,
						session,
					},
					comment,
				})),
				template(
					"UserDefinition",
					stored_value(codec, &definition, "UserDefinition")?,
					USER_HOLES,
				),
				stmt.kind.clone(),
				None,
			)
		}
		DefineStatement::Model(_) => {
			return Err(PlanError::Unsupported(String::from(
				"DEFINE MODEL: this build does not run SurrealML models",
			)));
		}
		DefineStatement::Module(stmt) => {
			let comment = s.comment(&stmt.comment)?;
			let permissions = s.permission(&stmt.permissions)?;
			let definition = crate::catalog::ModuleDefinition {
				name: stmt.name.clone(),
				executable: stmt.executable.clone().into(),
				digest: stmt.digest.clone(),
				signer: stmt.signer.clone(),
				unsigned: stmt.unsigned,
				capabilities: stmt.capabilities.clone(),
				bindings: stmt.bindings.clone(),
				// A hole the session's limit fills when the statement runs.
				auth_limit: crate::catalog::auth::AuthLimit::default(),
				comment: comment_text(&stmt.comment),
				permissions: stmt.permissions.clone(),
				permissions_body: crate::catalog::LogicalPermissionBody::of(&stmt.permissions),
			};
			(
				DdlForm::DefineModule,
				(EDIT, ResourceKind::Module, Base::Db),
				DefineObject::Module {
					name: stmt.name.clone(),
					executable: stmt.executable.clone(),
					unsigned: stmt.unsigned,
					signed: stmt.signer.is_some(),
					comment,
					permissions,
				},
				template(
					"StoredModuleDefinition",
					stored_value(codec, &definition.to_stored(), "StoredModuleDefinition")?,
					MODULE_HOLES,
				),
				stmt.kind.clone(),
				None,
			)
		}
		DefineStatement::Access(stmt) => {
			let grant = s.duration(DdlOperand::GrantDuration, &stmt.duration.grant)?;
			let token = s.duration(DdlOperand::TokenDuration, &stmt.duration.token)?;
			let session = s.duration(DdlOperand::SessionDuration, &stmt.duration.session)?;
			let comment = s.comment(&stmt.comment)?;
			let access_type = access_type_shape(&mut s, &stmt.access_type)?;
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let authenticate = stmt
				.authenticate
				.as_ref()
				.map(|e| s.stored(StoredRole::Authenticate, e))
				.transpose()?;
			let context =
				stmt.context.as_ref().map(|e| s.stored(StoredRole::Context, e)).transpose()?;
			let (signup, signin) = match &stmt.access_type {
				AccessType::Record(record) => (record.signup.as_ref(), record.signin.as_ref()),
				AccessType::Jwt(_) | AccessType::Bearer(_) => (None, None),
			};
			let signup = signup.map(|e| s.stored(StoredRole::Signup, e)).transpose()?;
			let signin = signin.map(|e| s.stored(StoredRole::Signin, e)).transpose()?;
			let access_type = match access_type {
				AccessTypeBuilt::Record {
					jwt,
					bearer,
				} => AccessTypeShape::Record(Box::new(RecordAccessShape {
					signup,
					signin,
					jwt,
					bearer,
				})),
				AccessTypeBuilt::Jwt(jwt) => AccessTypeShape::Jwt(jwt),
				AccessTypeBuilt::Bearer(bearer) => AccessTypeShape::Bearer(bearer),
			};
			planning_refusal = access_refusal(&access_definition(stmt, Credentials::Written), stmt);
			let definition = access_definition(stmt, Credentials::Emptied);
			(
				DdlForm::DefineAccess,
				(EDIT, ResourceKind::Actor, stmt.base),
				DefineObject::Access(Box::new(DefineAccess {
					name,
					base: stmt.base,
					access_type,
					secrets: AccessSecrets {
						written: s.take_credentials().into_boxed_slice(),
					},
					authenticate,
					context,
					duration: AccessDurationShape {
						grant,
						token,
						session,
					},
					comment,
				})),
				template(
					"StoredAccessDefinition",
					stored_value(codec, &definition.to_stored(), "StoredAccessDefinition")?,
					ACCESS_HOLES,
				),
				stmt.kind.clone(),
				None,
			)
		}
		DefineStatement::Config(stmt) => {
			let kind = stmt.inner.kind();
			let base = crate::ddl::define::config::config_kind_base(&kind);
			let shape = match &stmt.inner {
				ConfigInner::GraphQL(config) => ConfigShape::GraphQL(config.clone()),
				ConfigInner::Api(config) => ConfigShape::Api(s.api_config(config)?),
				ConfigInner::Default(config) => ConfigShape::Default(DefaultConfigShape {
					namespace: s.name(DdlOperand::ConfigNamespace, &config.namespace)?,
					database: s.name(DdlOperand::ConfigDatabase, &config.database)?,
				}),
			};
			let definition = match &stmt.inner {
				ConfigInner::GraphQL(config) => {
					crate::catalog::ConfigDefinition::GraphQL(config.clone())
				}
				ConfigInner::Api(config) => {
					crate::catalog::ConfigDefinition::Api(catalog_api_config(config))
				}
				ConfigInner::Default(config) => {
					crate::catalog::ConfigDefinition::Default(crate::catalog::DefaultConfig {
						namespace: written_text(&config.namespace),
						database: written_text(&config.database),
					})
				}
			};
			(
				DdlForm::DefineConfig,
				(EDIT, ResourceKind::Config(kind), base.into()),
				DefineObject::Config(Box::new(shape)),
				template(
					"StoredConfigDefinition",
					stored_value(codec, &definition.to_stored(), "StoredConfigDefinition")?,
					CONFIG_HOLES,
				),
				stmt.kind.clone(),
				None,
			)
		}
		DefineStatement::Api(stmt) => {
			let path = s.name(DdlOperand::Path, &stmt.path)?;
			let config = s.api_config(&stmt.config)?;
			let mut actions = Vec::with_capacity(stmt.actions.len());
			for action in &stmt.actions {
				let handler = s.stored(StoredRole::Action, &action.action)?;
				let config = s.api_config(&action.config)?;
				actions.push(crate::exec::ir::iface::statements::ApiActionShape {
					methods: action.methods.clone().into_boxed_slice(),
					action: handler,
					config,
				});
			}
			let comment = s.comment(&stmt.comment)?;
			let fallback =
				stmt.fallback.as_ref().map(|e| s.stored(StoredRole::Fallback, e)).transpose()?;
			let path_text = written_text(&stmt.path).and_then(|text| text.parse().ok());
			let definition = crate::catalog::ApiDefinition {
				path: match path_text {
					Some(path) => path,
					None => "/".parse().map_err(|_| {
						PlanError::Invariant("the root API path did not parse".into())
					})?,
				},
				actions: stmt
					.actions
					.iter()
					.map(|action| crate::catalog::ApiAction {
						methods: action.methods.iter().map(|m| (*m).into()).collect(),
						action: action.action.clone(),
						config: catalog_api_config(&action.config),
					})
					.collect(),
				fallback: stmt.fallback.clone(),
				config: catalog_api_config(&stmt.config),
				auth_limit: crate::catalog::auth::AuthLimit::default(),
				security: stmt.security.into(),
				comment: comment_text(&stmt.comment),
			};
			(
				DdlForm::DefineApi,
				(EDIT, ResourceKind::Api, Base::Db),
				DefineObject::Api(Box::new(DefineApi {
					path,
					actions: actions.into_boxed_slice(),
					fallback,
					config,
					comment,
				})),
				template(
					"StoredApiDefinition",
					stored_value(codec, &definition.to_stored(), "StoredApiDefinition")?,
					API_HOLES,
				),
				stmt.kind.clone(),
				None,
			)
		}
		DefineStatement::Bucket(stmt) => {
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let backend = match &stmt.backend {
				Some(expr) => Some(s.evaluated(DdlOperand::Backend, expr)?),
				None => None,
			};
			let comment = s.comment(&stmt.comment)?;
			let permissions = s.permission(&stmt.permissions)?;
			let definition = crate::catalog::BucketDefinition {
				id: None,
				name: Strand::new(written_text(&stmt.name).unwrap_or_default()),
				backend: match &stmt.backend {
					Some(Expr::Literal(Literal::String(text))) => Some(text.clone()),
					_ => None,
				},
				permissions: stmt.permissions.clone(),
				readonly: stmt.readonly,
				comment: comment_text(&stmt.comment),
				presign_nonce: uuid::Uuid::nil(),
			};
			(
				DdlForm::DefineBucket,
				(EDIT, ResourceKind::Bucket, stmt.base),
				DefineObject::Bucket {
					name,
					base: stmt.base,
					backend,
					permissions,
					readonly: stmt.readonly,
					comment,
				},
				template(
					"StoredBucketDefinition",
					stored_value(codec, &definition.to_stored(), "StoredBucketDefinition")?,
					BUCKET_HOLES,
				),
				stmt.kind.clone(),
				None,
			)
		}
		DefineStatement::Type(stmt) => {
			let comment = s.comment(&stmt.comment)?;
			let permissions = s.permission(&stmt.permissions)?;
			let body = crate::catalog::TypeBody::declared(stmt.body.clone());
			let labels = match &body {
				crate::catalog::TypeBody::Union(labels) => labels.labels.len(),
				crate::catalog::TypeBody::Kind(_) => 0,
			};
			let definition = crate::catalog::TypeDefinition {
				id: crate::catalog::TypeId(0),
				name: stmt.name.clone(),
				body,
				asserts: stmt.asserts.clone(),
				default: stmt.default.clone(),
				sql_type: stmt.sql_type.clone(),
				permissions: stmt.permissions.clone(),
				comment: comment_text(&stmt.comment),
			};
			let refusal = (labels >= crate::catalog::ENUM_LABEL_IDENT_LIMIT as usize).then(|| {
				crate::exec::vm::payload::Refusal::InvalidStatement {
					message: format!(
						"The type '{}' lists {labels} labels; a union holds at most {}",
						stmt.name,
						crate::catalog::ENUM_LABEL_IDENT_LIMIT - 1
					),
				}
			});
			(
				DdlForm::DefineType,
				(EDIT, ResourceKind::Parameter, Base::Db),
				DefineObject::Type {
					name: stmt.name.clone(),
					comment,
					permissions,
				},
				template(
					"StoredTypeDefinition",
					stored_value(codec, &definition.to_stored(), "StoredTypeDefinition")?,
					TYPE_HOLES,
				),
				stmt.kind.clone(),
				refusal,
			)
		}
		DefineStatement::Sequence(stmt) => {
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let timeout = s.duration(DdlOperand::Timeout, &stmt.timeout)?;
			let batch = s.evaluated(DdlOperand::Batch, &stmt.batch)?;
			let start = s.evaluated(DdlOperand::Start, &stmt.start)?;
			let definition = crate::catalog::SequenceDefinition {
				name: Strand::new(written_text(&stmt.name).unwrap_or_default()),
				batch: if let Expr::Literal(Literal::Integer(i)) = &stmt.batch {
					u32::try_from(*i).unwrap_or(0)
				} else {
					0
				},
				start: if let Expr::Literal(Literal::Integer(i)) = &stmt.start {
					*i
				} else {
					0
				},
				timeout: cast_duration(&stmt.timeout),
			};
			(
				DdlForm::DefineSequence,
				(EDIT, ResourceKind::Sequence, Base::Db),
				DefineObject::Sequence {
					name,
					batch,
					start,
					timeout,
				},
				template(
					"SequenceDefinition",
					stored_value(codec, &definition, "SequenceDefinition")?,
					SEQUENCE_HOLES,
				),
				stmt.kind.clone(),
				None,
			)
		}
	};
	let op = LogOp::Define(Box::new(DefineAttrs {
		object,
		mode,
		template: tmpl,
		checks,
	}));
	let mut plan = s.finish(form, allow, op);
	plan.refusal = refusal;
	plan.planning_refusal = planning_refusal;
	Ok(plan)
}

/// The definition `DEFINE TABLE` `stmt` forms for table `tb` with `view`, as the catalog stores
/// it, with the holes the statement fills when it runs: ids, cache stamps and the graph markers a
/// redefinition keeps.
pub(super) fn table_template(
	stmt: &crate::expr::statements::define::DefineTableStatement,
	tb: TableName,
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

/// The definition `DEFINE FIELD` `stmt` forms before its name and table are resolved, with its
/// written `comment`.
fn field_template(
	stmt: &crate::expr::statements::define::DefineFieldStatement,
	comment: &Option<String>,
) -> crate::catalog::FieldDefinition {
	// The name is a hole the statement fills when it runs, except that only the type of an `id`
	// can name the components of a record id, so a statement that names them is held under the
	// name `id`.
	let mut definition = crate::catalog::FieldDefinition {
		name: crate::expr::Idiom::field(if stmt.key_names.is_some() {
			"id"
		} else {
			"_"
		}),
		table: TableName::new(written_text(&stmt.what).unwrap_or_default()),
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
		comment: comment.clone(),
		reference: stmt.reference.clone(),
		graphql_alias: stmt.graphql_alias.clone(),
		graphql_deprecated: stmt.graphql_deprecated.clone(),
		sql_type: stmt.sql_type.clone(),
		..Default::default()
	};
	// The type and the names of the record id components it carries are set together, so the
	// definition's key structure always matches its type.
	definition.set_type(stmt.field_kind.clone(), stmt.key_names.clone());
	definition
}

/// The query of a view as the `SELECT` it is stored as.
fn view_select(view: &View) -> SelectStatement {
	SelectStatement {
		fields: view.expr.clone(),
		omit: Vec::new(),
		only: false,
		what: view.what.iter().map(|table| Expr::Table(table.clone())).collect(),
		with: None,
		cond: view.cond.clone(),
		split: None,
		group: view.group.clone(),
		order: None,
		limit: None,
		start: None,
		fetch: None,
		version: Expr::Literal(Literal::None),
		timeout: Expr::Literal(Literal::None),
		explain: None,
		tempfiles: false,
		for_update: false,
	}
}

/// The `SELECT` that fills a view once, over the tables it reads: `None` for a view computed by
/// its own select whenever it is read, which holds no rows to fill.
fn backfill_select(
	definition: &crate::catalog::ViewDefinition,
) -> Planned<Option<SelectStatement>> {
	use crate::catalog::ViewDefinition;
	match definition {
		ViewDefinition::Materialized {
			fields,
			tables,
			condition,
			..
		} => Ok(Some(crate::catalog::view::materialized_backfill_select(
			fields,
			tables,
			condition.as_ref(),
		))),
		ViewDefinition::Aggregated {
			analysis,
			tables,
			condition,
			..
		} => {
			let values = analysis.backfill_values().map_err(|error| {
				PlanError::Invariant(format!("a grouped view with no backfill values: {error}"))
			})?;
			analysis.backfill_select(&values, tables, condition.as_ref()).map(Some).map_err(
				|error| {
					PlanError::Invariant(format!("a grouped view with no backfill select: {error}"))
				},
			)
		}
		ViewDefinition::Select {
			..
		} => Ok(None),
	}
}

/// The expression of each field of a grouped view, in the order its aggregation analysis lists
/// them: each is evaluated over the document a group's keys and aggregate values make. None for
/// any other view.
fn group_field_exprs(definition: &crate::catalog::ViewDefinition) -> Planned<Vec<Expr>> {
	use crate::catalog::ViewDefinition;
	use crate::catalog::aggregation::AggregateFields;
	match definition {
		ViewDefinition::Aggregated {
			analysis,
			..
		} => match &analysis.fields {
			AggregateFields::Fields(items) => {
				Ok(items.iter().map(|(_, expr)| expr.clone()).collect())
			}
			// The catalog refuses a grouped view that selects a value as it forms the definition.
			AggregateFields::Value(_) => {
				Err(PlanError::Invariant("a grouped view that selects a value".to_owned()))
			}
		},
		ViewDefinition::Materialized {
			..
		}
		| ViewDefinition::Select {
			..
		} => Ok(Vec::new()),
	}
}

/// The columns of an index as written, as the body reads them: a literal subscript stays, a
/// string one names a field. `None` when one is not a plain path.
fn written_index_columns(cols: &[Expr]) -> Option<Vec<crate::expr::Idiom>> {
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
					other @ (Part::All
					| Part::Flatten
					| Part::Last
					| Part::First
					| Part::Field(_)
					| Part::Where(_)
					| Part::Lookup(_)
					| Part::Start(_)
					| Part::Method(..)
					| Part::Destructure(_)
					| Part::Optional
					| Part::Recurse(..)
					| Part::Doc
					| Part::RepeatRecurse) => Some(other.clone()),
				})
				.collect::<Option<Vec<_>>>()?;
			Some(crate::expr::Idiom(parts))
		})
		.collect()
}

/// One column of an index: a path headed by a field, whose non-literal subscripts are operands and
/// whose other steps are kept as written, or any other expression as a stored region.
fn index_column<'e>(s: &mut Slots<'e>, column: &'e Expr) -> Planned<IndexColumn> {
	use surrealdb_types::ToSql as _;
	if let Expr::Idiom(idiom) = column
		&& matches!(idiom.0.first(), Some(Part::Field(_)))
	{
		let mut parts = Vec::with_capacity(idiom.0.len());
		for part in &idiom.0 {
			let kept = |kind| PlacePart::Unaddressed(kind, part.to_sql().into_boxed_str());
			parts.push(match part {
				Part::Field(name) => PlacePart::Field(name.clone()),
				Part::All => PlacePart::All,
				Part::First => PlacePart::First,
				Part::Last => PlacePart::Last,
				Part::Flatten => PlacePart::Flatten,
				Part::Optional => PlacePart::Optional,
				Part::Doc => PlacePart::Doc,
				Part::RepeatRecurse => PlacePart::RepeatRecurse,
				Part::Value(Expr::Literal(Literal::String(text))) => PlacePart::Field(text.clone()),
				Part::Value(expr) => PlacePart::Dynamic(s.evaluated(DdlOperand::Columns, expr)?),
				Part::Where(_) => kept(UnaddressedPart::Filter),
				Part::Start(_) => kept(UnaddressedPart::Start),
				Part::Method(..) => kept(UnaddressedPart::Method),
				Part::Lookup(_) => kept(UnaddressedPart::Lookup),
				Part::Destructure(_) => kept(UnaddressedPart::Destructure),
				Part::Recurse(..) => kept(UnaddressedPart::Recurse),
			});
		}
		return Ok(IndexColumn::Path(Place {
			parts: parts.into_boxed_slice(),
		}));
	}
	Ok(IndexColumn::Stored(s.stored(StoredRole::IndexColumn, column)?))
}

/// The access type's shape before its stored clauses have slots.
enum AccessTypeBuilt {
	Record {
		jwt: JwtShape,
		bearer: Option<BearerShape>,
	},
	Jwt(JwtShape),
	Bearer(BearerShape),
}

fn access_type_shape<'e>(s: &mut Slots<'e>, access: &'e AccessType) -> Planned<AccessTypeBuilt> {
	Ok(match access {
		AccessType::Record(record) => {
			let jwt = jwt_shape(s, &record.jwt)?;
			let bearer = match &record.bearer {
				Some(bearer) => Some(bearer_shape(s, bearer)?),
				None => None,
			};
			AccessTypeBuilt::Record {
				jwt,
				bearer,
			}
		}
		AccessType::Jwt(jwt) => AccessTypeBuilt::Jwt(jwt_shape(s, jwt)?),
		AccessType::Bearer(bearer) => AccessTypeBuilt::Bearer(bearer_shape(s, bearer)?),
	})
}

fn bearer_shape<'e>(s: &mut Slots<'e>, bearer: &'e BearerAccess) -> Planned<BearerShape> {
	Ok(BearerShape {
		kind: bearer.kind.clone(),
		subject: bearer.subject.clone(),
		jwt: jwt_shape(s, &bearer.jwt)?,
	})
}

/// The JWT half of an access method: its verification key or URL, its issuer key and its
/// audiences, as operands in the order the body reads them. A key or URL the statement writes
/// binds as a marker and never as a literal of its text (see [`Slots::credential`]).
fn jwt_shape<'e>(s: &mut Slots<'e>, jwt: &'e JwtAccess) -> Planned<JwtShape> {
	let verify = match &jwt.verify {
		JwtAccessVerify::Key(key) => JwtVerifyShape::Key {
			alg: key.alg,
			key: s.credential(&key.key, true)?,
		},
		JwtAccessVerify::Jwks(jwks) => JwtVerifyShape::Jwks {
			url: s.credential(&jwks.url, false)?,
		},
	};
	let issue = match &jwt.issue {
		Some(issue) => Some(JwtIssueShape {
			alg: issue.alg,
			key: s.credential(&issue.key, false)?,
		}),
		None => None,
	};
	let audience = match &jwt.audience {
		Some(audiences) => {
			let mut slots: Vec<OperandSlot> = Vec::with_capacity(audiences.len());
			for audience in audiences {
				slots.push(s.evaluated(DdlOperand::Credentials, audience)?);
			}
			Some(slots.into_boxed_slice())
		}
		None => None,
	};
	Ok(JwtShape {
		verify,
		issue,
		audience,
	})
}

/// The `catalog` form of an access algorithm.
fn catalog_algorithm(alg: crate::expr::Algorithm) -> crate::catalog::Algorithm {
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

/// A string clause of a JWT configuration when it is written: the empty string where it is
/// evaluated, which the hole fills.
fn written_string(expr: &Expr, coerce: bool) -> String {
	let Some(value) = literal_value(expr) else {
		return String::new();
	};
	if coerce {
		value.coerce_to::<String>().unwrap_or_default()
	} else {
		value.cast_to::<String>().unwrap_or_default()
	}
}

/// The `catalog` form of a JWT configuration. The verification key, the JWKS URL and the issuer
/// key are the written text when `credentials` is `Credentials::Written` and empty when it is
/// `Credentials::Emptied`.
fn catalog_jwt(jwt: &JwtAccess, credentials: Credentials) -> crate::catalog::JwtAccess {
	use crate::catalog;
	let verify = match &jwt.verify {
		JwtAccessVerify::Key(key) => catalog::JwtAccessVerify::Key(catalog::JwtAccessVerifyKey {
			alg: catalog_algorithm(key.alg),
			key: credentials.string(&key.key, true),
		}),
		JwtAccessVerify::Jwks(jwks) => {
			catalog::JwtAccessVerify::Jwks(catalog::JwtAccessVerifyJwks {
				url: credentials.string(&jwks.url, false),
			})
		}
	};
	let issue = jwt.issue.as_ref().map(|issue| catalog::JwtAccessIssue {
		alg: catalog_algorithm(issue.alg),
		key: credentials.string(&issue.key, false),
	});
	let audience = jwt.audience.as_ref().map(|audiences| {
		let written: Option<Vec<String>> =
			audiences.iter().map(|expr| literal_value(expr)?.coerce_to::<String>().ok()).collect();
		written.unwrap_or_default()
	});
	catalog::JwtAccess {
		verify,
		issue,
		audience,
	}
}

fn catalog_bearer(bearer: &BearerAccess, credentials: Credentials) -> crate::catalog::BearerAccess {
	use crate::catalog;
	use crate::expr::access_type::{BearerAccessSubject, BearerAccessType};
	catalog::BearerAccess {
		kind: match bearer.kind {
			BearerAccessType::Bearer => catalog::BearerAccessType::Bearer,
			BearerAccessType::Refresh => catalog::BearerAccessType::Refresh,
		},
		subject: match bearer.subject {
			BearerAccessSubject::Record => catalog::BearerAccessSubject::Record,
			BearerAccessSubject::User => catalog::BearerAccessSubject::User,
		},
		jwt: catalog_jwt(&bearer.jwt, credentials),
	}
}

/// Whether the definition of `DEFINE ACCESS` holds the keys the statement writes.
#[derive(Clone, Copy)]
enum Credentials {
	/// The text as written, for the checks the planner runs on the definition.
	Written,
	/// Empty: the definition is encoded into a template whose `credentials` hole is filled from
	/// the statement's `AccessSecrets` and operands when it runs, so no key reaches the printed or
	/// `Debug` form of the container.
	Emptied,
}

impl Credentials {
	fn string(self, expr: &Expr, coerce: bool) -> String {
		match self {
			Self::Written => written_string(expr, coerce),
			Self::Emptied => String::new(),
		}
	}
}

/// The definition `DEFINE ACCESS` `stmt` forms, with every evaluated clause left at the value a
/// hole replaces and its keys as `credentials` says.
fn access_definition(
	stmt: &crate::expr::statements::define::DefineAccessStatement,
	credentials: Credentials,
) -> crate::catalog::AccessDefinition {
	use crate::catalog;
	let access_type = match &stmt.access_type {
		AccessType::Record(record) => catalog::AccessType::Record(catalog::RecordAccess {
			signup: record.signup.as_ref().map(catalog::ExprText::new),
			signin: record.signin.as_ref().map(catalog::ExprText::new),
			jwt: catalog_jwt(&record.jwt, credentials),
			bearer: record.bearer.as_ref().map(|bearer| catalog_bearer(bearer, credentials)),
		}),
		AccessType::Jwt(jwt) => catalog::AccessType::Jwt(catalog_jwt(jwt, credentials)),
		AccessType::Bearer(bearer) => {
			catalog::AccessType::Bearer(catalog_bearer(bearer, credentials))
		}
	};
	let (signup, signin) = match &stmt.access_type {
		AccessType::Record(record) => (record.signup.clone(), record.signin.clone()),
		AccessType::Jwt(_) | AccessType::Bearer(_) => (None, None),
	};
	let duration = |expr: &Expr| cast_duration(expr).map(|duration| duration.0);
	catalog::AccessDefinition {
		name: Strand::new(written_text(&stmt.name).unwrap_or_default()),
		base: stmt.base.into(),
		access_type,
		authenticate: stmt.authenticate.clone(),
		context: stmt.context.clone(),
		signup,
		signin,
		grant_duration: duration(&stmt.duration.grant),
		token_duration: duration(&stmt.duration.token),
		session_duration: duration(&stmt.duration.session),
		comment: match written_comment(&stmt.comment) {
			Some(Value::String(text)) => Some(text.as_str().to_owned()),
			_ => None,
		},
	}
}

/// The refusal the planner raises for a definition it cannot store: a `CONTEXT` clause that writes
/// or that stands on a method that authenticates no record, or a key that is the redaction
/// placeholder. The statement is refused before any of it runs.
fn access_refusal(
	definition: &crate::catalog::AccessDefinition,
	stmt: &crate::expr::statements::define::DefineAccessStatement,
) -> Option<crate::exec::vm::payload::Refusal> {
	use crate::exec::vm::payload::Refusal;
	if let Err(error) = crate::ddl::define::access::validate_access_context(
		&definition.base,
		&definition.access_type,
		stmt.context.as_ref(),
	) {
		return match error.downcast_ref::<crate::exec::Error>() {
			Some(crate::exec::Error::AccessContextNotReadOnly) => {
				Some(Refusal::AccessContextNotReadOnly)
			}
			Some(crate::exec::Error::AccessContextRequiresRecord) => {
				Some(Refusal::AccessContextRequiresRecord)
			}
			_ => None,
		};
	}
	crate::ddl::define::access::define_access_statement_reject_redacted(definition).err().map(
		|_| Refusal::AccessRedactedKey {
			ac: definition.name.to_string(),
		},
	)
}

/// A duration clause that is written as a value of any kind that casts to an optional duration:
/// the duration it casts to. `None` when it is evaluated, written as `NONE`, or written as a value
/// that does not cast, which the program converts when the statement runs.
fn cast_duration(expr: &Expr) -> Option<crate::val::BoundedDuration> {
	literal_value(expr)
		.and_then(|value| value.cast_to::<Option<crate::val::Duration>>().ok())
		.flatten()
		.map(|duration| crate::val::BoundedDuration(duration.0))
}

/// The `catalog` form of an API configuration. A middleware argument written as a value is kept;
/// one that is evaluated holds `NONE`, which the middleware hole replaces.
pub(super) fn catalog_api_config(
	config: &crate::expr::statements::define::config::api::ApiConfig,
) -> crate::catalog::ApiConfig {
	crate::catalog::ApiConfig {
		middleware: config
			.middleware
			.iter()
			.map(|m| crate::catalog::MiddlewareDefinition {
				name: m.name.clone(),
				args: m
					.args
					.iter()
					.map(|arg| {
						if let Expr::Literal(literal) = arg {
							literal.as_static_value().unwrap_or(Value::None)
						} else {
							Value::None
						}
					})
					.collect(),
			})
			.collect(),
		permissions: config.permissions.clone(),
	}
}
