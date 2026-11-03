//! Planning `ALTER`: the operands, stored clauses and attributes of each form.
//!
//! A clause that `DEFINE` stores and `ALTER` sets is a `StoredClause` region; a clause that
//! `DEFINE` evaluates is an operand. `Drop` and `None` are attributes. Operands are allocated in
//! the order the form's body evaluates them (see [`guard_order`](super::ddl::guard_order)).

#![deny(clippy::wildcard_enum_match_arm)]

use surrealdb_strand::Strand;

use super::ddl::{
	DdlForm, DdlOperand, DdlPlan, PlanError, Planned, Slots, alter_template, canonical_field_name,
	protected_arg_refusal, stored_value, written_text,
};
use super::ddl_define::catalog_api_config;
use super::ops::{AlterAttrs, LogOp};
use crate::exec::ir::iface::region_kind::{RegionKind, StoredRole};
use crate::exec::ir::iface::statements::{
	AlterAccess, AlterApiClauseShape, AlterDefaultShape, AlterEvent, AlterField, AlterFunction,
	AlterObject, AlterTable, AlterUser, ApiActionShape, ConfigShape, DdlChecks, DefaultConfigShape,
	EnumEdit, OnDeleteShape, ReferenceShape,
};
use crate::exec::vm::codec::Codec;
use crate::expr::reference::ReferenceDeleteStrategy;
use crate::expr::statements::AlterStatement;
use crate::expr::statements::alter::api::AlterApiClause;
use crate::expr::statements::alter::field::AlterDefault;
use crate::expr::statements::alter::{AlterKind, AlterTypeAction, EnumPosition};
use crate::expr::statements::define::config::ConfigInner;
use crate::expr::{Base, Expr};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

const EDIT: Action = Action::Edit;

/// Maps the value inside `kind` through `f`. `None` and `Drop` pass through unchanged.
fn map_kind<T, U>(kind: &AlterKind<T>, f: impl FnOnce(&T) -> U) -> AlterKind<U> {
	match kind {
		AlterKind::None => AlterKind::None,
		AlterKind::Drop => AlterKind::Drop,
		AlterKind::Set(value) => AlterKind::Set(f(value)),
	}
}

/// A stored clause that is set.
fn stored_kind<'e>(
	s: &mut Slots<'e>,
	role: StoredRole,
	kind: &'e AlterKind<Expr>,
) -> Planned<AlterKind<crate::exec::ir::iface::statements::RegionSlot>> {
	Ok(match kind {
		AlterKind::None => AlterKind::None,
		AlterKind::Drop => AlterKind::Drop,
		AlterKind::Set(expr) => AlterKind::Set(s.stored(role, expr)?),
	})
}

/// A reference declaration that is set.
fn reference_kind<'e>(
	s: &mut Slots<'e>,
	kind: &'e AlterKind<crate::expr::reference::Reference>,
) -> Planned<AlterKind<ReferenceShape>> {
	Ok(match kind {
		AlterKind::None => AlterKind::None,
		AlterKind::Drop => AlterKind::Drop,
		AlterKind::Set(reference) => AlterKind::Set(ReferenceShape {
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
	})
}

/// Records a `COMMENT` clause of an alteration: a set comment is the altered definition's, and a
/// set or dropped one is a field the alteration replaces.
fn set_comment(
	kind: &AlterKind<String>,
	comment: &mut Option<String>,
	fields: &mut Vec<&'static str>,
) {
	match kind {
		AlterKind::Set(text) => {
			*comment = Some(text.clone());
			fields.push("comment");
		}
		AlterKind::Drop => fields.push("comment"),
		AlterKind::None => {}
	}
}

/// The stored form of an API definition holding `actions`, `config` and `fallback`, as an object.
fn stored_api(
	codec: &'static Codec,
	actions: Vec<crate::catalog::ApiAction>,
	config: crate::catalog::ApiConfig,
	fallback: Option<Expr>,
) -> Planned<crate::val::Object> {
	stored_api_secured(codec, actions, config, fallback, crate::catalog::ApiSecurity::default())
}

/// As [`stored_api`], with the handler security mode `security`.
fn stored_api_secured(
	codec: &'static Codec,
	actions: Vec<crate::catalog::ApiAction>,
	config: crate::catalog::ApiConfig,
	fallback: Option<Expr>,
	security: crate::catalog::ApiSecurity,
) -> Planned<crate::val::Object> {
	let path =
		"/".parse().map_err(|_| PlanError::Invariant("the root API path did not parse".into()))?;
	let api = crate::catalog::ApiDefinition {
		path,
		actions,
		fallback,
		config,
		auth_limit: crate::catalog::auth::AuthLimit::default(),
		security,
		comment: None,
	};
	if let Value::Object(api) = stored_value(codec, &api.to_stored(), "StoredApiDefinition")? {
		Ok(api)
	} else {
		Err(PlanError::Invariant("a stored API that is not an object".into()))
	}
}

/// The values an `ALTER API` sets: `clauses` holds one object per clause in order, `security`
/// the stored handler security mode the statement sets, or `NONE`, and `types` the names of the
/// types the clauses name ([`api_clause_named_types`]). A `FOR ANY` clause holds the
/// stored `config` and `fallback` it sets, a `FOR` clause the stored `methods` and `action`, and a
/// `DROP` the `methods` it removes.
fn api_alteration_values(
	codec: &'static Codec,
	clauses: &[AlterApiClause],
	security: Option<crate::expr::ApiSecurity>,
) -> Planned<Value> {
	let security = match security {
		Some(security) => {
			let api = stored_api_secured(
				codec,
				Vec::new(),
				catalog_api_config(&Default::default()),
				None,
				security.into(),
			)?;
			api.get("security").cloned().unwrap_or(Value::None)
		}
		None => Value::None,
	};
	let mut values = crate::val::Object::default();
	values.insert("clauses".to_owned(), api_clause_values(codec, clauses)?);
	values.insert("security".to_owned(), security);
	values.insert("types".to_owned(), api_clause_named_types(clauses)?);
	Ok(Value::Object(values))
}

/// The names of the types the clauses of an `ALTER API` name, each once, as a list of strings: in
/// the configuration and the fallback a `FOR ANY` sets, and in the action and the configuration of
/// a `FOR`.
fn api_clause_named_types(clauses: &[AlterApiClause]) -> Planned<Value> {
	let mut names: Vec<String> = Vec::new();
	for clause in clauses {
		let (actions, config, fallback) = match clause {
			AlterApiClause::ForAny {
				config,
				fallback,
			} => {
				let fallback = match fallback {
					AlterKind::Set(expr) => Some(expr.clone()),
					AlterKind::Drop | AlterKind::None => None,
				};
				(Vec::new(), config.as_ref().map(catalog_api_config), fallback)
			}
			AlterApiClause::SetAction(action) => (
				vec![crate::catalog::ApiAction {
					methods: action.methods.iter().map(|m| (*m).into()).collect(),
					action: action.action.clone(),
					config: catalog_api_config(&action.config),
				}],
				None,
				None,
			),
			AlterApiClause::DropAction {
				..
			} => continue,
		};
		let api = crate::catalog::ApiDefinition {
			path: "/"
				.parse()
				.map_err(|_| PlanError::Invariant("the root API path did not parse".into()))?,
			actions,
			fallback,
			config: config.unwrap_or_else(|| catalog_api_config(&Default::default())),
			auth_limit: crate::catalog::auth::AuthLimit::default(),
			security: crate::catalog::ApiSecurity::default(),
			comment: None,
		};
		for name in crate::exec::vm::schema_host::api_named_types(&api) {
			if !names.contains(&name) {
				names.push(name);
			}
		}
	}
	Ok(Value::Array(names.into_iter().map(|name| Value::String(name.into())).collect()))
}

/// The values the clauses of an `ALTER API` set, one object per clause in order: a `FOR ANY`
/// holds the stored `config` and `fallback` it sets, a `FOR` clause the stored `methods` and
/// `action`, and a `DROP` the `methods` it removes.
fn api_clause_values(codec: &'static Codec, clauses: &[AlterApiClause]) -> Planned<Value> {
	let field =
		|object: &crate::val::Object, name: &str| object.get(name).cloned().unwrap_or(Value::None);
	let first_action = |api: &crate::val::Object| {
		if let Value::Array(actions) = field(api, "actions")
			&& let Some(Value::Object(action)) = actions.first()
		{
			Some(action.clone())
		} else {
			None
		}
	};
	let mut values = Vec::with_capacity(clauses.len());
	for clause in clauses {
		let mut set = crate::val::Object::default();
		match clause {
			AlterApiClause::ForAny {
				config,
				fallback,
			} => {
				if let Some(config) = config {
					let api = stored_api(codec, Vec::new(), catalog_api_config(config), None)?;
					set.insert("config".to_owned(), field(&api, "config"));
				}
				let fallback = match fallback {
					AlterKind::Set(expr) => {
						let api = stored_api(
							codec,
							Vec::new(),
							catalog_api_config(&Default::default()),
							Some(expr.clone()),
						)?;
						Some(field(&api, "fallback"))
					}
					AlterKind::Drop => Some(Value::None),
					AlterKind::None => None,
				};
				if let Some(fallback) = fallback {
					set.insert("fallback".to_owned(), fallback);
				}
			}
			AlterApiClause::SetAction(action) => {
				let api = stored_api(
					codec,
					vec![crate::catalog::ApiAction {
						methods: action.methods.iter().map(|m| (*m).into()).collect(),
						action: action.action.clone(),
						config: catalog_api_config(&action.config),
					}],
					catalog_api_config(&Default::default()),
					None,
				)?;
				let action = first_action(&api).ok_or_else(|| {
					PlanError::Invariant("a stored API action that is not an object".into())
				})?;
				set.insert("methods".to_owned(), field(&action, "methods"));
				set.insert("action".to_owned(), Value::Object(action));
			}
			AlterApiClause::DropAction {
				methods,
			} => {
				let api = stored_api(
					codec,
					vec![crate::catalog::ApiAction {
						methods: methods.iter().map(|m| (*m).into()).collect(),
						action: Expr::Literal(crate::expr::Literal::None),
						config: catalog_api_config(&Default::default()),
					}],
					catalog_api_config(&Default::default()),
					None,
				)?;
				let action = first_action(&api).ok_or_else(|| {
					PlanError::Invariant("a stored API action that is not an object".into())
				})?;
				set.insert("methods".to_owned(), field(&action, "methods"));
			}
		}
		values.push(Value::Object(set));
	}
	Ok(Value::Array(crate::val::Array(values)))
}

/// Plans an `ALTER`.
pub(super) fn plan_alter<'e>(
	alter: &'e AlterStatement,
	checks: DdlChecks,
	codec: &'static Codec,
) -> Planned<DdlPlan<'e>> {
	let mut s = Slots::new();
	let mut template = None;
	// The refusal the body raises after the checks, for an `ALTER FUNCTION` that
	// sets a protected argument name ([`protected_arg_refusal`]). `DEFINE
	// FUNCTION` refuses one the same way (GHSA-ffvm-mqh3-7986).
	let mut refusal = None;
	let (form, allow, object, if_exists) = match alter {
		AlterStatement::System(stmt) => {
			let query_timeout = match &stmt.query_timeout {
				AlterKind::None => AlterKind::None,
				AlterKind::Drop => AlterKind::Drop,
				AlterKind::Set(expr) => AlterKind::Set(s.duration(DdlOperand::Timeout, expr)?),
			};
			(
				DdlForm::AlterSystem,
				(EDIT, ResourceKind::Any, Base::Root),
				AlterObject::System {
					query_timeout,
					compact: stmt.compact,
					compatibility: stmt.compatibility,
				},
				false,
			)
		}
		AlterStatement::Namespace(stmt) => (
			DdlForm::AlterNamespace,
			(EDIT, ResourceKind::Namespace, Base::Root),
			AlterObject::Namespace {
				compact: stmt.compact,
			},
			false,
		),
		AlterStatement::Database(stmt) => {
			let name = stmt.name.as_ref().map(|name| s.name(DdlOperand::Name, name)).transpose()?;
			(
				DdlForm::AlterDatabase,
				(EDIT, ResourceKind::Database, Base::Ns),
				AlterObject::Database {
					name,
					compact: stmt.compact,
					compatibility: stmt.compatibility,
				},
				false,
			)
		}
		AlterStatement::Table(stmt) => {
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let permissions = stmt
				.permissions
				.as_ref()
				.map(|permissions| s.permissions(permissions))
				.transpose()?;
			let mut fields = Vec::new();
			let permissions_written = stmt.permissions.clone().unwrap_or_default();
			let mut altered = crate::catalog::TableDefinition {
				namespace_id: crate::catalog::NamespaceId(0),
				database_id: crate::catalog::DatabaseId(0),
				table_id: crate::catalog::TableId(0),
				name: Default::default(),
				drop: false,
				schemafull: false,
				view: None,
				permissions_body: crate::catalog::LogicalPermissions::of(&permissions_written),
				permissions: permissions_written,
				table_type: Default::default(),
				changefeed: None,
				comment: None,
				cache_fields_ts: uuid::Uuid::nil(),
				cache_events_ts: uuid::Uuid::nil(),
				cache_tables_ts: uuid::Uuid::nil(),
				cache_indexes_ts: uuid::Uuid::nil(),
				cache_lives_ts: uuid::Uuid::nil(),
				graphql_alias: None,
				graphql_deprecated: None,
				graph_folded: false,
				graph_doc_ids: false,
				inline_edges_cap: None,
				inline_refs_cap: None,
				graph_inline_gen: 0,
			};
			if let Some(kind) = &stmt.kind {
				altered.table_type = kind.clone();
				fields.push("table_type");
			}
			match &stmt.schemafull {
				AlterKind::Set(()) => {
					altered.schemafull = true;
					fields.push("schemafull");
				}
				AlterKind::Drop => fields.push("schemafull"),
				AlterKind::None => {}
			}
			if stmt.permissions.is_some() {
				fields.push("permissions");
			}
			match &stmt.changefeed {
				AlterKind::Set(changefeed) => {
					altered.changefeed = Some(*changefeed);
					fields.push("changefeed");
				}
				AlterKind::Drop => fields.push("changefeed"),
				AlterKind::None => {}
			}
			set_comment(&stmt.comment, &mut altered.comment, &mut fields);
			for (field, cap, held) in [
				("inline_edges_cap", &stmt.inline_edges_cap, &mut altered.inline_edges_cap),
				("inline_refs_cap", &stmt.inline_refs_cap, &mut altered.inline_refs_cap),
			] {
				match cap {
					AlterKind::Set(cap) => {
						*held = Some(*cap);
						fields.push(field);
					}
					AlterKind::Drop => fields.push(field),
					AlterKind::None => {}
				}
			}
			template = Some(alter_template(
				"StoredTableDefinition",
				stored_value(codec, &altered.to_stored(), "StoredTableDefinition")?,
				&fields,
			));
			(
				DdlForm::AlterTable,
				(EDIT, ResourceKind::Table, Base::Db),
				AlterObject::Table(Box::new(AlterTable {
					name,
					schemafull: stmt.schemafull.clone(),
					permissions,
					changefeed: stmt.changefeed.clone(),
					comment: stmt.comment.clone(),
					compact: stmt.compact,
					kind: stmt.kind.clone(),
					inline_edges_cap: stmt.inline_edges_cap.clone(),
					inline_refs_cap: stmt.inline_refs_cap.clone(),
					migrate: stmt.migrate.clone(),
				})),
				stmt.if_exists,
			)
		}
		AlterStatement::Api(stmt) => {
			let path = s.name(DdlOperand::Path, &stmt.path)?;
			let mut clauses = Vec::with_capacity(stmt.clauses.len());
			for clause in &stmt.clauses {
				clauses.push(match clause {
					AlterApiClause::ForAny {
						config,
						fallback,
					} => {
						let config = match config {
							Some(config) => Some(s.api_config(config)?),
							None => None,
						};
						AlterApiClauseShape::ForAny {
							config,
							fallback: stored_kind(&mut s, StoredRole::Fallback, fallback)?,
						}
					}
					AlterApiClause::SetAction(action) => {
						let config = s.api_config(&action.config)?;
						AlterApiClauseShape::SetAction(ApiActionShape {
							methods: action.methods.clone().into_boxed_slice(),
							action: s.stored(StoredRole::Action, &action.action)?,
							config,
						})
					}
					AlterApiClause::DropAction {
						methods,
					} => AlterApiClauseShape::DropAction {
						methods: methods.clone().into_boxed_slice(),
					},
				});
			}
			template = Some(alter_template(
				"StoredApiDefinition",
				api_alteration_values(codec, &stmt.clauses, stmt.security)?,
				&[],
			));
			(
				DdlForm::AlterApi,
				(EDIT, ResourceKind::Api, Base::Db),
				AlterObject::Api {
					path,
					clauses: clauses.into_boxed_slice(),
					security: stmt.security.is_some(),
					comment: stmt.comment.clone(),
				},
				stmt.if_exists,
			)
		}
		AlterStatement::Event(stmt) => {
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let what = s.name(DdlOperand::Table, &stmt.what)?;
			let when = stored_kind(&mut s, StoredRole::When, &stmt.when)?;
			let then = match &stmt.then {
				AlterKind::None => AlterKind::None,
				AlterKind::Drop => AlterKind::Drop,
				AlterKind::Set(exprs) => {
					let mut slots = Vec::with_capacity(exprs.len());
					for expr in exprs {
						slots.push(s.stored(StoredRole::Then, expr)?);
					}
					AlterKind::Set(slots.into_boxed_slice())
				}
			};
			let mut fields = Vec::new();
			let written_when = match &stmt.when {
				AlterKind::Set(when) => {
					fields.push("when");
					when.clone()
				}
				AlterKind::Drop | AlterKind::None => {
					Expr::Literal(crate::expr::Literal::Bool(true))
				}
			};
			let written_then = match &stmt.then {
				AlterKind::Set(then) => {
					fields.push("then");
					then.clone()
				}
				AlterKind::Drop | AlterKind::None => Vec::new(),
			};
			let mut comment = None;
			set_comment(&stmt.comment, &mut comment, &mut fields);
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
				written_when,
				written_then,
				kind,
				crate::catalog::auth::AuthLimit::default(),
				comment,
			);
			template = Some(alter_template(
				"StoredEventDefinition",
				stored_value(codec, &altered.to_stored(), "StoredEventDefinition")?,
				&fields,
			));
			(
				DdlForm::AlterEvent,
				(EDIT, ResourceKind::Event, Base::Db),
				AlterObject::Event(Box::new(AlterEvent {
					name,
					what,
					when,
					then,
					comment: stmt.comment.clone(),
					kind: stmt.kind.clone(),
				})),
				stmt.if_exists,
			)
		}
		AlterStatement::Index(stmt) => {
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let table = s.name(DdlOperand::Table, &stmt.table)?;
			(
				DdlForm::AlterIndex,
				(EDIT, ResourceKind::Index, Base::Db),
				AlterObject::Index {
					name,
					table,
					prepare_remove: stmt.prepare_remove,
					comment: stmt.comment.clone(),
				},
				stmt.if_exists,
			)
		}
		AlterStatement::Sequence(stmt) => {
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let timeout = match &stmt.timeout {
				Some(expr) => Some(s.duration(DdlOperand::Timeout, expr)?),
				None => None,
			};
			(
				DdlForm::AlterSequence,
				(EDIT, ResourceKind::Sequence, Base::Db),
				AlterObject::Sequence {
					name,
					timeout,
				},
				stmt.if_exists,
			)
		}
		AlterStatement::Field(stmt) => {
			let name = s.field_name(&stmt.name)?;
			let what = s.name(DdlOperand::Table, &stmt.what)?;
			let value = stored_kind(&mut s, StoredRole::Value, &stmt.value)?;
			let assert = stored_kind(&mut s, StoredRole::Assert, &stmt.assert)?;
			let default = match &stmt.default {
				AlterDefault::None => AlterDefaultShape::None,
				AlterDefault::Drop => AlterDefaultShape::Drop,
				AlterDefault::Always(expr) => {
					AlterDefaultShape::Always(s.stored(StoredRole::Default, expr)?)
				}
				AlterDefault::Set(expr) => {
					AlterDefaultShape::Set(s.stored(StoredRole::Default, expr)?)
				}
			};
			let permissions = stmt
				.permissions
				.as_ref()
				.map(|permissions| s.permissions(permissions))
				.transpose()?;
			let reference = reference_kind(&mut s, &stmt.reference)?;
			// The definition the clauses form, under the name the statement writes. The type and
			// the names of the record id components it carries are set together, so the stored
			// type text carries the names.
			let mut altered = crate::catalog::FieldDefinition {
				name: if let Expr::Idiom(idiom) = &stmt.name {
					canonical_field_name(idiom)
				} else {
					None
				}
				.unwrap_or_else(|| crate::expr::Idiom::field("_")),
				table: surrealdb_strand::TableName::new(
					written_text(&stmt.what).unwrap_or_default(),
				),
				..Default::default()
			};
			let mut fields: Vec<&str> = Vec::new();
			if let AlterKind::Set(kind) = &stmt.kind {
				let names = match &stmt.key_names {
					AlterKind::Set(names) => Some(names.clone()),
					AlterKind::Drop | AlterKind::None => None,
				};
				altered.set_type(Some(kind.clone()), names);
			}
			if !matches!(stmt.kind, AlterKind::None) {
				fields.push("field_kind");
			}
			for (clause, held, field) in [
				(&stmt.flexible, &mut altered.flexible, "flexible"),
				(&stmt.readonly, &mut altered.readonly, "readonly"),
				(&stmt.inline, &mut altered.inline, "inline"),
			] {
				match clause {
					AlterKind::Set(()) => {
						*held = true;
						fields.push(field);
					}
					AlterKind::Drop => fields.push(field),
					AlterKind::None => {}
				}
			}
			for (clause, held, body, field) in [
				(&stmt.value, &mut altered.value, &mut altered.value_body, "value"),
				(&stmt.assert, &mut altered.assert, &mut altered.assert_body, "assert"),
			] {
				match clause {
					AlterKind::Set(expr) => {
						*held = Some(expr.clone());
						*body = Some(crate::catalog::LogicalBody::of(expr));
					}
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
					altered.default_body = Some(crate::catalog::LogicalBody::of(expr));
					fields.push("default");
				}
				AlterDefault::Set(expr) => {
					altered.default = crate::catalog::DefineDefault::Set(expr.clone());
					altered.default_body = Some(crate::catalog::LogicalBody::of(expr));
					fields.push("default");
				}
			}
			if let Some(permissions) = &stmt.permissions {
				altered.select_permission = permissions.select.clone();
				altered.select_body =
					crate::catalog::LogicalPermissionBody::of(&permissions.select);
				altered.create_permission = permissions.create.clone();
				altered.update_permission = permissions.update.clone();
				fields.extend(["select_permission", "create_permission", "update_permission"]);
			}
			set_comment(&stmt.comment, &mut altered.comment, &mut fields);
			match &stmt.reference {
				AlterKind::Set(reference) => {
					altered.reference = Some(reference.clone());
					fields.push("reference");
				}
				AlterKind::Drop => fields.push("reference"),
				AlterKind::None => {}
			}
			match &stmt.sql_type {
				AlterKind::Set(sql_type) => {
					altered.sql_type = Some(sql_type.clone());
					fields.push("sql_type");
				}
				AlterKind::Drop => fields.push("sql_type"),
				AlterKind::None => {}
			}
			template = Some(alter_template(
				"StoredFieldDefinition",
				stored_value(codec, &altered.to_stored(), "StoredFieldDefinition")?,
				&fields,
			));
			(
				DdlForm::AlterField,
				(EDIT, ResourceKind::Field, Base::Db),
				AlterObject::Field(Box::new(AlterField {
					name,
					what,
					kind: stmt.kind.clone(),
					flexible: stmt.flexible.clone(),
					readonly: stmt.readonly.clone(),
					inline: stmt.inline.clone(),
					value,
					assert,
					default,
					permissions,
					comment: stmt.comment.clone(),
					reference,
					migrate: stmt.migrate.clone().map(Box::new),
					sql_type: stmt.sql_type.clone(),
				})),
				stmt.if_exists,
			)
		}
		AlterStatement::Param(stmt) => {
			let value = match &stmt.value {
				Some(expr) => Some(s.evaluated(DdlOperand::Value, expr)?),
				None => None,
			};
			let permissions =
				stmt.permissions.as_ref().map(|permission| s.permission(permission)).transpose()?;
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
			set_comment(&stmt.comment, &mut altered.comment, &mut fields);
			if stmt.permissions.is_some() {
				fields.push("permissions");
			}
			template = Some(alter_template(
				"StoredParamDefinition",
				stored_value(codec, &altered.to_stored(), "StoredParamDefinition")?,
				&fields,
			));
			(
				DdlForm::AlterParam,
				(EDIT, ResourceKind::Parameter, Base::Db),
				AlterObject::Param {
					name: stmt.name.clone(),
					value,
					comment: stmt.comment.clone(),
					permissions,
				},
				stmt.if_exists,
			)
		}
		AlterStatement::Type(stmt) => (
			DdlForm::AlterType,
			(EDIT, ResourceKind::Parameter, Base::Db),
			AlterObject::Type {
				name: stmt.name.clone(),
				edits: stmt
					.actions
					.iter()
					.map(|action| match action {
						AlterTypeAction::AddValue {
							value,
							position,
							if_not_exists,
						} => EnumEdit::Add {
							value: value.clone(),
							anchor: match position {
								Some(
									EnumPosition::Before(anchor) | EnumPosition::After(anchor),
								) => Some(anchor.clone()),
								None => None,
							},
							after: matches!(position, Some(EnumPosition::After(_))),
							if_not_exists: *if_not_exists,
						},
						AlterTypeAction::RenameValue {
							from,
							to,
						} => EnumEdit::Rename {
							from: from.clone(),
							to: to.clone(),
						},
					})
					.collect(),
			},
			stmt.if_exists,
		),
		AlterStatement::Bucket(stmt) => {
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let permissions =
				stmt.permissions.as_ref().map(|permission| s.permission(permission)).transpose()?;
			let mut altered = crate::catalog::BucketDefinition {
				id: None,
				name: Default::default(),
				backend: None,
				permissions: stmt.permissions.clone().unwrap_or_default(),
				readonly: false,
				comment: None,
				presign_nonce: uuid::Uuid::nil(),
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
			set_comment(&stmt.comment, &mut altered.comment, &mut fields);
			template = Some(alter_template(
				"StoredBucketDefinition",
				stored_value(codec, &altered.to_stored(), "StoredBucketDefinition")?,
				&fields,
			));
			(
				DdlForm::AlterBucket,
				(EDIT, ResourceKind::Bucket, stmt.base),
				AlterObject::Bucket {
					name,
					base: stmt.base,
					backend: stmt.backend.clone(),
					permissions,
					readonly: stmt.readonly.clone(),
					comment: stmt.comment.clone(),
					revoke_signatures: stmt.revoke_signatures,
				},
				stmt.if_exists,
			)
		}
		AlterStatement::Config(stmt) => {
			let kind = stmt.inner.kind();
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
			template = Some(alter_template(
				"StoredConfigDefinition",
				stored_value(codec, &definition.to_stored(), "StoredConfigDefinition")?,
				&[],
			));
			(
				DdlForm::AlterConfig,
				(EDIT, ResourceKind::Config(kind), Base::Db),
				AlterObject::Config {
					inner: Box::new(shape),
					comment: stmt.comment.clone(),
				},
				stmt.if_exists,
			)
		}
		AlterStatement::Analyzer(stmt) => {
			let name = s.name(DdlOperand::Name, &stmt.name)?;
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
			set_comment(&stmt.comment, &mut altered.comment, &mut fields);
			template = Some(alter_template(
				"AnalyzerDefinition",
				stored_value(codec, &altered, "AnalyzerDefinition")?,
				&fields,
			));
			(
				DdlForm::AlterAnalyzer,
				(EDIT, ResourceKind::Analyzer, Base::Db),
				AlterObject::Analyzer {
					name,
					function: stmt.function.clone(),
					tokenizers: map_kind(&stmt.tokenizers, |list| list.clone().into_boxed_slice()),
					filters: map_kind(&stmt.filters, |list| list.clone().into_boxed_slice()),
					comment: stmt.comment.clone(),
				},
				stmt.if_exists,
			)
		}
		AlterStatement::Function(stmt) => {
			let block = match &stmt.block {
				AlterKind::None => AlterKind::None,
				AlterKind::Drop => AlterKind::Drop,
				AlterKind::Set(block) => {
					let params = match &stmt.args {
						AlterKind::Set(args) => args
							.iter()
							.map(|(name, _)| surrealdb_strand::Strand::new(name))
							.collect(),
						AlterKind::None | AlterKind::Drop => Vec::new(),
					};
					AlterKind::Set(s.region(
						RegionKind::StoredClause(StoredRole::Body),
						super::ddl::Body::Function {
							block: Box::new(Expr::Block(Box::new(block.clone()))),
							params,
						},
					)?)
				}
			};
			let permissions =
				stmt.permissions.as_ref().map(|permission| s.permission(permission)).transpose()?;
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
					// SECURITY: an argument may not take a protected name, for the
					// reason `DEFINE FUNCTION` refuses one: a function body reads
					// its argument of `$auth`, `$access`, `$token` or `$session` in
					// place of the session's value, so a permission predicate judged
					// beneath the body would be handed a caller-chosen identity
					// (GHSA-ffvm-mqh3-7986).
					refusal = protected_arg_refusal(args);
					altered.args.clone_from(args);
					fields.push("args");
				}
				AlterKind::Drop => fields.push("args"),
				AlterKind::None => {}
			}
			// Only a set block is copied into the altered definition. A dropped one is not.
			if let AlterKind::Set(block) = &stmt.block {
				altered.block = block.clone();
				altered.body = crate::catalog::LogicalBlockBody::of(block);
				fields.push("block");
			}
			set_comment(&stmt.comment, &mut altered.comment, &mut fields);
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
			template = Some(alter_template(
				"StoredFunctionDefinition",
				stored_value(codec, &altered.to_stored(), "StoredFunctionDefinition")?,
				&fields,
			));
			(
				DdlForm::AlterFunction,
				(EDIT, ResourceKind::Function, Base::Db),
				AlterObject::Function(Box::new(AlterFunction {
					name: stmt.name.clone(),
					args: map_kind(&stmt.args, |args| args.clone().into_boxed_slice()),
					block,
					comment: stmt.comment.clone(),
					permissions,
					returns: stmt.returns.clone(),
				})),
				stmt.if_exists,
			)
		}
		AlterStatement::User(stmt) => {
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let password = s.password(&stmt.pending_password)?;
			(
				DdlForm::AlterUser,
				(EDIT, ResourceKind::Actor, stmt.base),
				AlterObject::User(Box::new(AlterUser {
					name,
					base: stmt.base,
					password,
					hash: stmt.hash.clone(),
					scram: stmt.scram.clone(),
					roles: map_kind(&stmt.roles, |roles| roles.clone().into_boxed_slice()),
					token_duration: stmt.token_duration.clone(),
					session_duration: stmt.session_duration.clone(),
					comment: stmt.comment.clone(),
				})),
				stmt.if_exists,
			)
		}
		AlterStatement::Access(stmt) => {
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let authenticate = stored_kind(&mut s, StoredRole::Authenticate, &stmt.authenticate)?;
			let context = stored_kind(&mut s, StoredRole::Context, &stmt.context)?;
			let mut altered = crate::catalog::AccessDefinition {
				name: Strand::new(written_text(&stmt.name).unwrap_or_default()),
				base: stmt.base.into(),
				// Only the altered clauses are read out of this definition.
				access_type: crate::catalog::AccessType::Jwt(crate::catalog::JwtAccess {
					verify: crate::catalog::JwtAccessVerify::Key(
						crate::catalog::JwtAccessVerifyKey {
							alg: crate::catalog::Algorithm::Hs512,
							key: String::new(),
						},
					),
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
			set_comment(&stmt.comment, &mut altered.comment, &mut fields);
			template = Some(alter_template(
				"StoredAccessDefinition",
				stored_value(codec, &altered.to_stored(), "StoredAccessDefinition")?,
				&fields,
			));
			(
				DdlForm::AlterAccess,
				(EDIT, ResourceKind::Access, stmt.base),
				AlterObject::Access(Box::new(AlterAccess {
					name,
					base: stmt.base,
					authenticate,
					context,
					grant_duration: stmt.grant_duration.clone(),
					token_duration: stmt.token_duration.clone(),
					session_duration: stmt.session_duration.clone(),
					comment: stmt.comment.clone(),
				})),
				stmt.if_exists,
			)
		}
		AlterStatement::Module(stmt) => {
			let permissions =
				stmt.permissions.as_ref().map(|permission| s.permission(permission)).transpose()?;
			let mut fields = Vec::new();
			let mut comment = None;
			set_comment(&stmt.comment, &mut comment, &mut fields);
			if stmt.permissions.is_some() {
				fields.push("permissions");
			}
			let permission = stmt.permissions.clone().unwrap_or_default();
			let altered = crate::catalog::ModuleDefinition {
				name: None,
				// Only the altered clauses are read out of this definition.
				executable: crate::catalog::ModuleExecutable::Silo(
					crate::catalog::SiloExecutable {
						organisation: String::new(),
						package: String::new(),
						major: 0,
						minor: 0,
						patch: 0,
					},
				),
				digest: None,
				signer: None,
				unsigned: false,
				capabilities: None,
				bindings: Default::default(),
				auth_limit: Default::default(),
				comment,
				permissions_body: crate::catalog::LogicalPermissionBody::of(&permission),
				permissions: permission,
			};
			template = Some(alter_template(
				"StoredModuleDefinition",
				stored_value(codec, &altered.to_stored(), "StoredModuleDefinition")?,
				&fields,
			));
			(
				DdlForm::AlterModule,
				(EDIT, ResourceKind::Module, Base::Db),
				AlterObject::Module {
					name: stmt.name.clone(),
					comment: stmt.comment.clone(),
					permissions,
				},
				stmt.if_exists,
			)
		}
	};
	let op = LogOp::Alter(Box::new(AlterAttrs {
		object,
		if_exists,
		template,
		checks,
	}));
	let mut plan = s.finish(form, allow, op);
	plan.refusal = refusal;
	Ok(plan)
}
