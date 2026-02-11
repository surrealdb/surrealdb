use std::collections::BTreeMap;
use std::sync::Arc;

use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
	Enum, Field, FieldFuture, FieldValue, InputObject, InputValue, Interface, InterfaceField,
	Object, Scalar, Schema, Type, TypeRef, Union,
};
use async_graphql::{Name, Value as GqlValue};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use serde_json::Number;
use surrealdb_types::ToSql;

use super::error::{GqlError, resolver_error};
#[cfg(debug_assertions)]
use super::ext::ValidatorExt;
use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{GraphQLConfig, GraphQLFunctionsConfig, GraphQLTablesConfig};
use crate::dbs::Session;
use crate::expr::kind::{GeometryKind, KindLiteral};
use crate::expr::{Expr, Kind, Literal};
use crate::gql::error::{internal_error, schema_error, type_error};
use crate::gql::functions::process_fns;
use crate::gql::relations::collect_relations;
use crate::gql::tables::process_tbs;
use crate::kvs::{Datastore, LockType, TransactionType};
use crate::val::{
	Array as SurArray, Geometry as SurGeometry, Number as SurNumber, Object as SurObject,
	RecordId as SurRecordId, RecordIdKey as SurRecordIdKey, Set as SurSet, TableName,
	Value as SurValue,
};

pub async fn generate_schema(
	datastore: &Arc<Datastore>,
	session: &Session,
	gql_config: GraphQLConfig,
) -> Result<Schema, GqlError> {
	let kvs = datastore;
	let tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;
	let ns = session.ns.as_ref().ok_or(GqlError::UnspecifiedNamespace)?;
	let db = session.db.as_ref().ok_or(GqlError::UnspecifiedDatabase)?;

	let db_def = match tx.get_db_by_name(ns, db).await? {
		Some(db) => db,
		None => return Err(GqlError::NotConfigured),
	};

	// Get all tables
	let tbs = match gql_config.tables {
		GraphQLTablesConfig::None => None,
		_ => {
			let tbs = tx.all_tb(db_def.namespace_id, db_def.database_id, None).await?;

			match gql_config.tables {
				GraphQLTablesConfig::None => None,
				GraphQLTablesConfig::Auto => Some(tbs),
				GraphQLTablesConfig::Include(inc) => {
					Some(tbs.iter().filter(|t| inc.contains(&t.name)).cloned().collect())
				}
				GraphQLTablesConfig::Exclude(exc) => {
					Some(tbs.iter().filter(|t| !exc.contains(&t.name)).cloned().collect())
				}
			}
		}
	};

	// Get all functions
	let fns = match gql_config.functions {
		GraphQLFunctionsConfig::None => None,
		_ => {
			let fns = tx.all_db_functions(db_def.namespace_id, db_def.database_id).await?;
			match gql_config.functions {
				GraphQLFunctionsConfig::None => None,
				GraphQLFunctionsConfig::Auto => Some(fns),
				GraphQLFunctionsConfig::Include(inc) => {
					Some(fns.iter().filter(|f| inc.contains(&f.name)).cloned().collect())
				}
				GraphQLFunctionsConfig::Exclude(exc) => {
					Some(fns.iter().filter(|f| !exc.contains(&f.name)).cloned().collect())
				}
			}
		}
	};

	let mut query = Object::new("Query");
	let mut types: Vec<Type> = Vec::new();

	trace!(ns, db, ?tbs, ?fns, "generating schema");

	let has_tables = tbs.as_ref().is_some_and(|t| !t.is_empty());
	let has_fns = fns.as_ref().is_some_and(|f| !f.is_empty());

	// Check if there's anything to expose via GraphQL
	if !has_tables && !has_fns {
		return Err(schema_error(
			"no items found in database: GraphQL requires at least one table or function",
		));
	}

	// Collect relation info from table definitions for relation field generation
	let relations = match &tbs {
		Some(tbs) => collect_relations(tbs),
		None => Vec::new(),
	};

	match tbs {
		Some(tbs) if !tbs.is_empty() => {
			query = process_tbs(
				tbs,
				query,
				&mut types,
				&tx,
				db_def.namespace_id,
				db_def.database_id,
				session,
				datastore,
				&relations,
			)
			.await?;
		}
		_ => {}
	}

	if let Some(fns) = fns {
		query = process_fns(fns, query, &mut types, session, datastore).await?;
	}

	// Register all geometry-related types (enum, object types, union, input types)
	register_geometry_types(&mut types);

	trace!("current Query object for schema: {:?}", query);

	let mut schema = Schema::build("Query", None, None).register(query);
	for ty in types {
		trace!("adding type: {ty:?}");
		schema = schema.register(ty);
	}

	macro_rules! scalar_debug_validated {
		($schema:ident, $name:expr_2021, $kind:expr_2021) => {
			scalar_debug_validated!(
				$schema,
				$name,
				$kind,
				::std::option::Option::<&str>::None,
				::std::option::Option::<&str>::None
			)
		};
		($schema:ident, $name:expr_2021, $kind:expr_2021, $desc:literal) => {
			scalar_debug_validated!($schema, $name, $kind, std::option::Option::Some($desc), None)
		};
		($schema:ident, $name:expr_2021, $kind:expr_2021, $desc:literal, $url:literal) => {
			scalar_debug_validated!(
				$schema,
				$name,
				$kind,
				std::option::Option::Some($desc),
				Some($url)
			)
		};
		($schema:ident, $name:expr_2021, $kind:expr_2021, $desc:expr_2021, $url:expr_2021) => {{
			let new_type = Type::Scalar({
				let mut tmp = Scalar::new($name);
				if let Some(desc) = $desc {
					tmp = tmp.description(desc);
				}
				if let Some(url) = $url {
					tmp = tmp.specified_by_url(url);
				}
				#[cfg(debug_assertions)]
				tmp.add_validator(|v| gql_to_sql_kind(v, $kind).is_ok());
				tmp
			});
			$schema = $schema.register(new_type);
		}};
	}

	scalar_debug_validated!(
		schema,
		"uuid",
		Kind::Uuid,
		"String encoded UUID",
		"https://datatracker.ietf.org/doc/html/rfc4122"
	);

	scalar_debug_validated!(schema, "decimal", Kind::Decimal);
	scalar_debug_validated!(schema, "number", Kind::Number);
	scalar_debug_validated!(schema, "null", Kind::Null);
	scalar_debug_validated!(schema, "datetime", Kind::Datetime);
	scalar_debug_validated!(schema, "duration", Kind::Duration);
	scalar_debug_validated!(schema, "object", Kind::Object);
	scalar_debug_validated!(schema, "any", Kind::Any);

	let id_interface =
		Interface::new("record").field(InterfaceField::new("id", TypeRef::named_nn(TypeRef::ID)));
	schema = schema.register(id_interface);

	let relation_interface = Interface::new("relation")
		.field(InterfaceField::new("id", TypeRef::named_nn(TypeRef::ID)))
		.field(InterfaceField::new("in", TypeRef::named_nn("record")))
		.field(InterfaceField::new("out", TypeRef::named_nn("record")))
		.implement("record");
	schema = schema.register(relation_interface);

	schema
		.finish()
		.map_err(|e| schema_error(format!("there was an error generating schema: {e:?}")))
}

pub(crate) fn sql_value_to_gql_value(v: SurValue) -> Result<GqlValue, GqlError> {
	let out = match v {
		SurValue::None => GqlValue::Null,
		SurValue::Null => GqlValue::Null,
		SurValue::Bool(b) => GqlValue::Boolean(b),
		SurValue::Number(n) => match n {
			SurNumber::Int(i) => GqlValue::Number(i.into()),
			SurNumber::Float(f) => GqlValue::Number(
				Number::from_f64(f)
					.ok_or(resolver_error("unimplemented: graceful NaN and Inf handling"))?,
			),
			num @ SurNumber::Decimal(_) => GqlValue::String(num.to_string()),
		},
		SurValue::String(s) => GqlValue::String(s),
		d @ SurValue::Duration(_) => GqlValue::String(d.to_sql()),
		SurValue::Datetime(d) => GqlValue::String(d.to_rfc3339()),
		SurValue::Uuid(uuid) => GqlValue::String(uuid.to_string()),
		SurValue::Array(a) => GqlValue::List(
			a.into_iter()
				.map(|v| sql_value_to_gql_value(v).expect("value conversion should succeed"))
				.collect(),
		),
		SurValue::Object(o) => GqlValue::Object(
			o.0.into_iter()
				.map(|(k, v)| {
					(
						Name::new(k),
						sql_value_to_gql_value(v).expect("value conversion should succeed"),
					)
				})
				.collect(),
		),
		SurValue::Geometry(ref g) => return geometry_to_gql_object(g),
		SurValue::Bytes(b) => GqlValue::Binary(b.into_inner()),
		SurValue::RecordId(t) => GqlValue::String(t.to_sql()),
		v => return Err(internal_error(format!("found unsupported value variant: {v:?}"))),
	};
	Ok(out)
}

pub fn kind_to_type(
	kind: Kind,
	types: &mut Vec<Type>,
	is_input: bool,
) -> Result<TypeRef, GqlError> {
	let optional = kind.can_be_none();
	let out_ty = match kind {
		Kind::Any => TypeRef::named("any"),
		Kind::None => TypeRef::named("none"),
		Kind::Null => TypeRef::named("null"),
		Kind::Bool => TypeRef::named(TypeRef::BOOLEAN),
		Kind::Bytes => TypeRef::named("bytes"),
		Kind::Datetime => TypeRef::named("datetime"),
		Kind::Decimal => TypeRef::named("decimal"),
		Kind::Duration => TypeRef::named("duration"),
		Kind::Float => TypeRef::named(TypeRef::FLOAT),
		Kind::Int => TypeRef::named(TypeRef::INT),
		Kind::Number => TypeRef::named("number"),
		Kind::Object => TypeRef::named("object"),
		Kind::Regex => return Err(schema_error("Kind::Regex is not yet supported")),
		Kind::String => TypeRef::named(TypeRef::STRING),
		Kind::Uuid => TypeRef::named("uuid"),
		Kind::Table(ref _t) => TypeRef::named(kind.to_sql()),
		Kind::Record(mut tables) => match tables.len() {
			0 => TypeRef::named("record"),
			1 => TypeRef::named(tables.pop().expect("single table in record kind").into_string()),
			_ => {
				let ty_name = tables.join("_or_");

				let mut tmp_union = Union::new(ty_name.clone())
					.description(format!("A record which is one of: {}", tables.join(", ")));
				for n in tables {
					tmp_union = tmp_union.possible_type(n.into_string());
				}

				types.push(Type::Union(tmp_union));
				TypeRef::named(ty_name)
			}
		},
		Kind::Geometry(ref geo_kinds) => {
			if is_input {
				// Input context: return InputObject type names
				match geo_kinds.len() {
					0 => TypeRef::named("GeometryInput"),
					1 => TypeRef::named(geometry_kind_to_gql_input_type_name(&geo_kinds[0])),
					_ => {
						// GraphQL doesn't support union input types, so we use
						// the unified GeometryInput for multi-kind geometry fields
						TypeRef::named("GeometryInput")
					}
				}
			} else {
				// Output context: return Object type / Union names
				match geo_kinds.len() {
					0 => TypeRef::named("Geometry"),
					1 => TypeRef::named(geometry_kind_to_gql_type_name(&geo_kinds[0])),
					_ => {
						// Create a partial union of the allowed geometry types
						let names: Vec<&str> =
							geo_kinds.iter().map(geometry_kind_to_gql_type_name).collect();
						let ty_name = names.join("_or_");

						let mut partial_union = Union::new(ty_name.clone()).description(format!(
							"A geometry which is one of: {}",
							names.join(", ")
						));
						for name in &names {
							partial_union = partial_union.possible_type(*name);
						}
						types.push(Type::Union(partial_union));
						TypeRef::named(ty_name)
					}
				}
			}
		}
		Kind::Either(ks) => {
			let (ls, others): (Vec<Kind>, Vec<Kind>) =
				ks.into_iter().partition(|k| matches!(k, Kind::Literal(KindLiteral::String(_))));

			let enum_ty = if !ls.is_empty() {
				let vals: Vec<String> = ls
					.into_iter()
					.map(|l| {
						let Kind::Literal(KindLiteral::String(out)) = l else {
							unreachable!(
								"just checked that this is a Kind::Literal(Literal::String(_))"
							);
						};
						out
					})
					.collect();

				let mut tmp = Enum::new(vals.join("_or_"));
				tmp = tmp.items(vals);

				let enum_ty = tmp.type_name().to_string();

				types.push(Type::Enum(tmp));
				if others.is_empty() {
					return Ok(TypeRef::named(enum_ty));
				}
				Some(enum_ty)
			} else {
				None
			};

			let pos_names: Result<Vec<TypeRef>, GqlError> =
				others.into_iter().map(|k| kind_to_type(k, types, is_input)).collect();
			let pos_names: Vec<String> = pos_names?.into_iter().map(|tr| tr.to_string()).collect();
			let ty_name = pos_names.join("_or_");

			let mut tmp_union = Union::new(ty_name.clone());
			for n in pos_names {
				tmp_union = tmp_union.possible_type(n);
			}

			if let Some(ty) = enum_ty {
				tmp_union = tmp_union.possible_type(ty);
			}

			types.push(Type::Union(tmp_union));
			TypeRef::named(ty_name)
		}
		Kind::Set(_, _) => return Err(schema_error("Kind::Set is not yet supported")),
		Kind::Array(k, _) => TypeRef::List(Box::new(kind_to_type(*k, types, is_input)?)),
		Kind::Function(_, _) => return Err(schema_error("Kind::Function is not yet supported")),
		Kind::Range => return Err(schema_error("Kind::Range is not yet supported")),
		// TODO(raphaeldarley): check if union is of literals and generate enum
		// generate custom scalar from other literals?
		Kind::Literal(_) => return Err(schema_error("Kind::Literal is not yet supported")),
		Kind::File(_) => return Err(schema_error("Kind::File is not yet supported")),
	};

	let out = match optional {
		true => out_ty,
		false => TypeRef::NonNull(Box::new(out_ty)),
	};
	Ok(out)
}

pub fn unwrap_type(ty: TypeRef) -> TypeRef {
	match ty {
		TypeRef::NonNull(t) => unwrap_type(*t),
		_ => ty,
	}
}

macro_rules! either_try_kind {
	($ks:ident, $val:expr_2021, Kind::Array) => {
		for arr_kind in $ks.iter().filter(|k| matches!(k, Kind::Array(_, _))).cloned() {
			either_try_kind!($ks, $val, arr_kind);
		}
	};
	($ks:ident, $val:expr_2021, Array) => {
		for arr_kind in $ks.iter().filter(|k| matches!(k, Kind::Array(_, _))).cloned() {
			either_try_kind!($ks, $val, arr_kind);
		}
	};
	($ks:ident, $val:expr_2021, Record) => {
		for arr_kind in $ks.iter().filter(|k| matches!(k, Kind::Array(_, _))).cloned() {
			either_try_kind!($ks, $val, arr_kind);
		}
	};
	($ks:ident, $val:expr_2021, AllNumbers) => {
		either_try_kind!($ks, $val, Kind::Int);
		either_try_kind!($ks, $val, Kind::Float);
		either_try_kind!($ks, $val, Kind::Decimal);
		either_try_kind!($ks, $val, Kind::Number);
	};
	($ks:ident, $val:expr_2021, $kind:expr_2021) => {
		if $ks.contains(&$kind) {
			if let Ok(out) = gql_to_sql_kind($val, $kind) {
				return Ok(out);
			}
		}
	};
}

macro_rules! either_try_kinds {
	($ks:ident, $val:expr_2021, $($kind:tt),+) => {
		$(either_try_kind!($ks, $val, $kind));+
	};
}

macro_rules! any_try_kind {
	($val:expr_2021, $kind:expr_2021) => {
		if let Ok(out) = gql_to_sql_kind($val, $kind) {
			return Ok(out);
		}
	};
}
macro_rules! any_try_kinds {
	($val:expr_2021, $($kind:tt),+) => {
		$(any_try_kind!($val, $kind));+
	};
}

/// Convert a static RecordIdKeyLit to RecordIdKey
/// Only works for static literals (no expressions)
fn convert_static_record_id_key(
	key: crate::expr::RecordIdKeyLit,
) -> Result<SurRecordIdKey, GqlError> {
	use crate::expr::RecordIdKeyLit;
	match key {
		RecordIdKeyLit::Number(n) => Ok(SurRecordIdKey::Number(n)),
		RecordIdKeyLit::String(s) => Ok(SurRecordIdKey::String(s)),
		RecordIdKeyLit::Uuid(u) => Ok(SurRecordIdKey::Uuid(u)),
		RecordIdKeyLit::Array(exprs) => {
			let vals: Result<Vec<SurValue>, GqlError> =
				exprs.into_iter().map(convert_static_expr).collect();
			Ok(SurRecordIdKey::Array(SurArray(vals?)))
		}
		RecordIdKeyLit::Object(entries) => {
			let mut map = BTreeMap::new();
			for entry in entries {
				map.insert(entry.key, convert_static_expr(entry.value)?);
			}
			Ok(SurRecordIdKey::Object(SurObject(map)))
		}
		RecordIdKeyLit::Generate(_) => Err(resolver_error(
			"Generated RecordId keys (rand(), ulid(), uuid()) are not supported in GraphQL inputs",
		)),
		RecordIdKeyLit::Range(_) => {
			Err(resolver_error("RecordId key ranges are not supported in GraphQL"))
		}
	}
}

/// Convert a static Expr to Value
/// Only works for static literals (no parameters, idioms, function calls, etc.)
fn convert_static_expr(expr: Expr) -> Result<SurValue, GqlError> {
	match expr {
		Expr::Literal(lit) => convert_static_literal(lit),
		Expr::Table(t) => Ok(SurValue::Table(t)),
		_ => Err(resolver_error("Only literal values are supported in GraphQL inputs")),
	}
}

/// Convert a static Literal to Value
/// Only works for static literals (no expressions that need evaluation)
fn convert_static_literal(lit: Literal) -> Result<SurValue, GqlError> {
	match lit {
		Literal::None => Ok(SurValue::None),
		Literal::Null => Ok(SurValue::Null),
		Literal::Bool(b) => Ok(SurValue::Bool(b)),
		Literal::Float(f) => Ok(SurValue::Number(SurNumber::Float(f))),
		Literal::Integer(i) => Ok(SurValue::Number(SurNumber::Int(i))),
		Literal::Decimal(d) => Ok(SurValue::Number(SurNumber::Decimal(d))),
		Literal::String(s) => Ok(SurValue::String(s)),
		Literal::Bytes(b) => Ok(SurValue::Bytes(b)),
		Literal::Regex(r) => Ok(SurValue::Regex(r)),
		Literal::RecordId(record_id_lit) => {
			let key = convert_static_record_id_key(record_id_lit.key)?;
			Ok(SurValue::RecordId(SurRecordId {
				table: record_id_lit.table,
				key,
			}))
		}
		Literal::Array(exprs) => {
			let vals: Result<Vec<SurValue>, GqlError> =
				exprs.into_iter().map(convert_static_expr).collect();
			Ok(SurValue::Array(SurArray(vals?)))
		}
		Literal::Set(exprs) => {
			let vals: Result<Vec<SurValue>, GqlError> =
				exprs.into_iter().map(convert_static_expr).collect();
			Ok(SurValue::Set(SurSet::from(vals?)))
		}
		Literal::Object(entries) => {
			let mut map = BTreeMap::new();
			for entry in entries {
				map.insert(entry.key, convert_static_expr(entry.value)?);
			}
			Ok(SurValue::Object(SurObject(map)))
		}
		Literal::Duration(d) => Ok(SurValue::Duration(d)),
		Literal::Datetime(dt) => Ok(SurValue::Datetime(dt)),
		Literal::Uuid(u) => Ok(SurValue::Uuid(u)),
		Literal::Geometry(g) => Ok(SurValue::Geometry(g)),
		Literal::File(f) => Ok(SurValue::File(f)),
		Literal::UnboundedRange => {
			Err(resolver_error("Unbounded ranges are not supported in GraphQL"))
		}
	}
}

pub(crate) fn gql_to_sql_kind(val: &GqlValue, kind: Kind) -> Result<SurValue, GqlError> {
	use crate::syn;
	match kind {
		Kind::Any => match val {
			GqlValue::String(s) => {
				use Kind::*;
				any_try_kinds!(val, Datetime, Duration, Uuid);
				let expr = syn::expr_legacy_strand(s.as_str())?;
				convert_static_expr(expr.into())
			}
			GqlValue::Null => Ok(SurValue::Null),
			obj @ GqlValue::Object(_) => gql_to_sql_kind(obj, Kind::Object),
			num @ GqlValue::Number(_) => gql_to_sql_kind(num, Kind::Number),
			GqlValue::Boolean(b) => Ok(SurValue::Bool(*b)),
			bin @ GqlValue::Binary(_) => gql_to_sql_kind(bin, Kind::Bytes),
			GqlValue::Enum(s) => Ok(SurValue::String(s.as_str().into())),
			arr @ GqlValue::List(_) => gql_to_sql_kind(arr, Kind::Array(Box::new(Kind::Any), None)),
		},
		Kind::None => match val {
			GqlValue::Null => Ok(SurValue::None),
			_ => Err(type_error(kind, val)),
		},
		Kind::Null => match val {
			GqlValue::Null => Ok(SurValue::Null),
			_ => Err(type_error(kind, val)),
		},
		Kind::Bool => match val {
			GqlValue::Boolean(b) => Ok(SurValue::Bool(*b)),
			_ => Err(type_error(kind, val)),
		},
		Kind::Bytes => match val {
			GqlValue::Binary(b) => Ok(SurValue::Bytes(bytes::Bytes::copy_from_slice(b).into())),
			_ => Err(type_error(kind, val)),
		},
		Kind::Datetime => match val {
			GqlValue::String(s) => match syn::datetime(s) {
				Ok(dt) => Ok(SurValue::Datetime(dt.into())),
				Err(_) => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Decimal => match val {
			GqlValue::Number(n) => {
				if let Some(int) = n.as_i64() {
					Ok(SurValue::Number(SurNumber::Decimal(int.into())))
				} else if let Some(d) = n.as_f64().and_then(Decimal::from_f64) {
					Ok(SurValue::Number(SurNumber::Decimal(d)))
				} else if let Some(uint) = n.as_u64() {
					Ok(SurValue::Number(SurNumber::Decimal(uint.into())))
				} else {
					Err(type_error(kind, val))
				}
			}
			GqlValue::String(s) => {
				let decimal_expr: Expr = syn::expr(s)?.into();

				match decimal_expr {
					Expr::Literal(Literal::Decimal(d)) => {
						Ok(SurValue::Number(SurNumber::Decimal(d)))
					}
					_ => Err(type_error(kind, val)),
				}
			}
			_ => Err(type_error(kind, val)),
		},
		Kind::Duration => match val {
			GqlValue::String(s) => match syn::duration(s) {
				Ok(d) => Ok(SurValue::Duration(d.into())),
				Err(_) => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Float => match val {
			GqlValue::Number(n) => {
				if let Some(i) = n.as_i64() {
					Ok(SurValue::Number(SurNumber::Float(i as f64)))
				} else if let Some(f) = n.as_f64() {
					Ok(SurValue::Number(SurNumber::Float(f)))
				} else if let Some(uint) = n.as_u64() {
					Ok(SurValue::Number(SurNumber::Float(uint as f64)))
				} else {
					unreachable!("serde_json::Number must be either i64, u64 or f64")
				}
			}
			GqlValue::String(s) => {
				let float_expr: Expr = syn::expr(s)?.into();

				match float_expr {
					Expr::Literal(Literal::Float(f)) => Ok(SurValue::Number(SurNumber::Float(f))),
					_ => Err(type_error(kind, val)),
				}
			}
			_ => Err(type_error(kind, val)),
		},
		Kind::Int => match val {
			GqlValue::Number(n) => {
				if let Some(i) = n.as_i64() {
					Ok(SurValue::Number(SurNumber::Int(i)))
				} else {
					Err(type_error(kind, val))
				}
			}
			GqlValue::String(s) => {
				let int_expr: Expr = syn::expr(s)?.into();

				match int_expr {
					Expr::Literal(Literal::Integer(i)) => Ok(SurValue::Number(SurNumber::Int(i))),
					_ => Err(type_error(kind, val)),
				}
			}
			_ => Err(type_error(kind, val)),
		},
		Kind::Number => match val {
			GqlValue::Number(n) => {
				if let Some(i) = n.as_i64() {
					Ok(SurValue::Number(SurNumber::Int(i)))
				} else if let Some(f) = n.as_f64() {
					Ok(SurValue::Number(SurNumber::Float(f)))
				} else if let Some(uint) = n.as_u64() {
					Ok(SurValue::Number(SurNumber::Decimal(uint.into())))
				} else {
					unreachable!("serde_json::Number must be either i64, u64 or f64")
				}
			}
			GqlValue::String(s) => {
				let number_expr: Expr = syn::expr(s)?.into();

				match number_expr {
					Expr::Literal(Literal::Integer(i)) => Ok(SurValue::Number(SurNumber::Int(i))),
					Expr::Literal(Literal::Float(f)) => Ok(SurValue::Number(SurNumber::Float(f))),
					Expr::Literal(Literal::Decimal(d)) => {
						Ok(SurValue::Number(SurNumber::Decimal(d)))
					}
					_ => Err(type_error(kind, val)),
				}
			}
			_ => Err(type_error(kind, val)),
		},
		Kind::Object => match val {
			GqlValue::Object(o) => {
				let out: Result<BTreeMap<String, SurValue>, GqlError> = o
					.iter()
					.map(|(k, v)| gql_to_sql_kind(v, Kind::Any).map(|sqlv| (k.to_string(), sqlv)))
					.collect();
				Ok(SurValue::Object(out?.into()))
			}
			GqlValue::String(s) => {
				let expr = syn::expr_legacy_strand(s.as_str())?;
				let expr: Expr = expr.into();

				match expr {
					Expr::Literal(Literal::Object(o)) => {
						let mut map = BTreeMap::new();
						for entry in o {
							map.insert(entry.key, convert_static_expr(entry.value)?);
						}
						Ok(SurValue::Object(SurObject(map)))
					}
					_ => Err(type_error(kind, val)),
				}
			}
			_ => Err(type_error(kind, val)),
		},
		Kind::String => match val {
			GqlValue::String(s) => Ok(SurValue::String(s.to_owned())),
			GqlValue::Enum(s) => Ok(SurValue::String(s.as_str().into())),
			_ => Err(type_error(kind, val)),
		},
		Kind::Uuid => match val {
			GqlValue::String(s) => match s.parse::<uuid::Uuid>() {
				Ok(u) => Ok(SurValue::Uuid(u.into())),
				Err(_) => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Table(ref ts) => match val {
			GqlValue::String(s) => match ts.contains(&s.as_str().into()) {
				true => Ok(SurValue::Table(TableName::new(s.clone()))),
				false => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Record(ref ts) => match val {
			GqlValue::String(s) => {
				let record_id_expr = syn::expr(s)?;
				let record_id_expr: Expr = record_id_expr.into();

				match record_id_expr {
					Expr::Literal(Literal::RecordId(t)) => match ts.contains(&t.table) {
						true => {
							let key = convert_static_record_id_key(t.key)?;
							Ok(SurValue::RecordId(SurRecordId {
								table: t.table,
								key,
							}))
						}
						false => Err(type_error(kind, val)),
					},
					_ => Err(type_error(kind, val)),
				}
			}
			_ => Err(type_error(kind, val)),
		},
		Kind::Geometry(ref geo_kinds) => match val {
			GqlValue::Object(obj) => {
				let geometry = gql_geometry_from_object(obj, geo_kinds)?;
				Ok(SurValue::Geometry(geometry))
			}
			_ => Err(type_error(kind, val)),
		},
		// TODO: handle nested eithers
		Kind::Either(ref ks) => {
			use Kind::*;

			match val {
				GqlValue::Null => {
					if ks.contains(&Kind::None) {
						Ok(SurValue::None)
					} else if ks.contains(&Kind::Null) {
						Ok(SurValue::Null)
					} else {
						Err(type_error(kind, val))
					}
				}
				num @ GqlValue::Number(_) => {
					either_try_kind!(ks, num, AllNumbers);
					Err(type_error(kind, val))
				}
				string @ GqlValue::String(_) => {
					either_try_kinds!(
						ks, string, Datetime, Duration, AllNumbers, Uuid, Array, Any, String
					);
					either_try_kind!(ks, string, Kind::Object);
					Err(type_error(kind, val))
				}
				bool @ GqlValue::Boolean(_) => {
					either_try_kind!(ks, bool, Kind::Bool);
					Err(type_error(kind, val))
				}
				GqlValue::Binary(_) => {
					Err(resolver_error("binary input for Either is not yet supported"))
				}
				GqlValue::Enum(n) => {
					either_try_kind!(ks, &GqlValue::String(n.to_string()), Kind::String);
					Err(type_error(kind, val))
				}
				list @ GqlValue::List(_) => {
					either_try_kind!(ks, list, Kind::Array);
					Err(type_error(kind, val))
				}
				obj @ GqlValue::Object(_) => {
					// Try geometry kinds first (geometry inputs are objects)
					for geo_kind in ks.iter().filter(|k| matches!(k, Kind::Geometry(_))).cloned() {
						if let Ok(out) = gql_to_sql_kind(obj, geo_kind) {
							return Ok(out);
						}
					}
					either_try_kind!(ks, obj, Kind::Object);
					Err(type_error(kind, val))
				}
			}
		}
		Kind::Set(_k, _n) => Err(resolver_error("Sets are not yet supported")),
		Kind::Array(ref k, n) => match val {
			GqlValue::List(l) => {
				let list_iter = l.iter().map(|v| gql_to_sql_kind(v, *k.to_owned()));
				let list: Result<Vec<SurValue>, GqlError> = list_iter.collect();

				match (list, n) {
					(Ok(l), Some(n)) => {
						if l.len() as u64 == n {
							Ok(l.into())
						} else {
							Err(type_error(kind, val))
						}
					}
					(Ok(l), None) => Ok(l.into()),
					(Err(e), _) => Err(e),
				}
			}
			_ => Err(type_error(kind, val)),
		},
		Kind::Function(_, _) => Err(resolver_error("Functions are not yet supported")),
		Kind::Range => Err(resolver_error("Ranges are not yet supported")),
		Kind::Literal(_) => Err(resolver_error("Literals are not yet supported")),
		Kind::Regex => Err(resolver_error("Regexes are not yet supported")),
		Kind::File(_) => Err(resolver_error("Files are not yet supported")),
	}
}

// ---------------------------------------------------------------------------
// Geometry support: helpers, type registration, and conversion
// ---------------------------------------------------------------------------

/// Map a `GeometryKind` to the corresponding GraphQL output Object type name.
pub(crate) fn geometry_kind_to_gql_type_name(kind: &GeometryKind) -> &'static str {
	match kind {
		GeometryKind::Point => "GeometryPoint",
		GeometryKind::Line => "GeometryLineString",
		GeometryKind::Polygon => "GeometryPolygon",
		GeometryKind::MultiPoint => "GeometryMultiPoint",
		GeometryKind::MultiLine => "GeometryMultiLineString",
		GeometryKind::MultiPolygon => "GeometryMultiPolygon",
		GeometryKind::Collection => "GeometryCollection",
	}
}

/// Map a `GeometryKind` to the corresponding GraphQL InputObject type name.
fn geometry_kind_to_gql_input_type_name(kind: &GeometryKind) -> &'static str {
	match kind {
		GeometryKind::Point => "GeometryPointInput",
		GeometryKind::Line => "GeometryLineStringInput",
		GeometryKind::Polygon => "GeometryPolygonInput",
		GeometryKind::MultiPoint => "GeometryMultiPointInput",
		GeometryKind::MultiLine => "GeometryMultiLineStringInput",
		GeometryKind::MultiPolygon => "GeometryMultiPolygonInput",
		GeometryKind::Collection => "GeometryCollectionInput",
	}
}

/// Map a `Geometry` value to the GraphQL Object type name for that variant.
pub(crate) fn geometry_gql_type_name(g: &SurGeometry) -> &'static str {
	match g {
		SurGeometry::Point(_) => "GeometryPoint",
		SurGeometry::Line(_) => "GeometryLineString",
		SurGeometry::Polygon(_) => "GeometryPolygon",
		SurGeometry::MultiPoint(_) => "GeometryMultiPoint",
		SurGeometry::MultiLine(_) => "GeometryMultiLineString",
		SurGeometry::MultiPolygon(_) => "GeometryMultiPolygon",
		SurGeometry::Collection(_) => "GeometryCollection",
	}
}

/// Build a `TypeRef` for nested Float arrays at a given depth.
///
/// - depth 1 → `[Float!]!`       (Point coordinates)
/// - depth 2 → `[[Float!]!]!`    (LineString / MultiPoint coordinates)
/// - depth 3 → `[[[Float!]!]!]!` (Polygon / MultiLineString coordinates)
/// - depth 4 → `[[[[Float!]!]!]!]!` (MultiPolygon coordinates)
fn nested_float_list(depth: usize) -> TypeRef {
	let mut ty = TypeRef::named_nn(TypeRef::FLOAT); // Float!
	for _ in 0..depth {
		ty = TypeRef::NonNull(Box::new(TypeRef::List(Box::new(ty)))); // [...]!
	}
	ty
}

/// Build a geometry Object type for variants that have `coordinates`.
///
/// Creates an Object type with:
/// - `type: GeometryType!` (fixed enum value)
/// - `coordinates: <nested_float_list>!`
fn make_geometry_object_type(
	obj_name: &str,
	geojson_type: &'static str,
	coord_depth: usize,
) -> Object {
	let coords_ty = nested_float_list(coord_depth);
	Object::new(obj_name)
		.field(Field::new("type", TypeRef::named_nn("GeometryType"), {
			move |_ctx| {
				FieldFuture::new(async move {
					Ok(Some(FieldValue::value(GqlValue::Enum(Name::new(geojson_type)))))
				})
			}
		}))
		.field(Field::new("coordinates", coords_ty, |ctx| {
			FieldFuture::new(async move {
				let g = ctx.parent_value.try_downcast_ref::<SurGeometry>()?;
				let coords = g.as_coordinates();
				let gql_coords = sql_value_to_gql_value(coords)?;
				Ok(Some(FieldValue::value(gql_coords)))
			})
		}))
}

/// Build the `GeometryCollection` Object type, which uses `geometries` instead
/// of `coordinates`.
fn make_geometry_collection_type() -> Object {
	Object::new("GeometryCollection")
		.field(Field::new("type", TypeRef::named_nn("GeometryType"), |_ctx| {
			FieldFuture::new(async move {
				Ok(Some(FieldValue::value(GqlValue::Enum(Name::new("GeometryCollection")))))
			})
		}))
		.field(Field::new("geometries", TypeRef::named_nn_list_nn("Geometry"), |ctx| {
			FieldFuture::new(async move {
				let g = ctx.parent_value.try_downcast_ref::<SurGeometry>()?;
				match g {
					SurGeometry::Collection(geometries) => {
						let items: Vec<FieldValue> = geometries
							.iter()
							.map(|g| {
								let type_name = geometry_gql_type_name(g);
								FieldValue::owned_any(g.clone()).with_type(type_name)
							})
							.collect();
						Ok(Some(FieldValue::list(items)))
					}
					_ => Err(internal_error("Expected GeometryCollection value").into()),
				}
			})
		}))
}

/// Register all geometry-related types into the types list and return types
/// that must be registered directly on the Schema builder (enum, union).
///
/// Types registered into `types` vec:
/// - Object types: `GeometryPoint`, `GeometryLineString`, `GeometryPolygon`,
///   `GeometryMultiPoint`, `GeometryMultiLineString`, `GeometryMultiPolygon`,
///   `GeometryCollection`
///
/// Types that need `schema.register()`:
/// - Enum: `GeometryType`
/// - Union: `Geometry`
/// - InputObject types for each variant + unified `GeometryInput`
pub(crate) fn register_geometry_types(types: &mut Vec<Type>) {
	// GeometryType enum
	types.push(Type::Enum(
		Enum::new("GeometryType")
			.description("GeoJSON geometry type discriminator")
			.item("Point")
			.item("LineString")
			.item("Polygon")
			.item("MultiPoint")
			.item("MultiLineString")
			.item("MultiPolygon")
			.item("GeometryCollection"),
	));

	// Per-variant output Object types
	types.push(Type::Object(make_geometry_object_type("GeometryPoint", "Point", 1)));
	types.push(Type::Object(make_geometry_object_type("GeometryLineString", "LineString", 2)));
	types.push(Type::Object(make_geometry_object_type("GeometryPolygon", "Polygon", 3)));
	types.push(Type::Object(make_geometry_object_type("GeometryMultiPoint", "MultiPoint", 2)));
	types.push(Type::Object(make_geometry_object_type(
		"GeometryMultiLineString",
		"MultiLineString",
		3,
	)));
	types.push(Type::Object(make_geometry_object_type("GeometryMultiPolygon", "MultiPolygon", 4)));
	types.push(Type::Object(make_geometry_collection_type()));

	// Geometry union (covers all variants)
	types.push(Type::Union(
		Union::new("Geometry")
			.description("A GeoJSON geometry – one of Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, or GeometryCollection")
			.possible_type("GeometryPoint")
			.possible_type("GeometryLineString")
			.possible_type("GeometryPolygon")
			.possible_type("GeometryMultiPoint")
			.possible_type("GeometryMultiLineString")
			.possible_type("GeometryMultiPolygon")
			.possible_type("GeometryCollection"),
	));

	// Per-variant InputObject types
	types.push(Type::InputObject(
		InputObject::new("GeometryPointInput")
			.description("GeoJSON Point input – coordinates is [lng, lat]")
			.field(InputValue::new("coordinates", nested_float_list(1))),
	));
	types.push(Type::InputObject(
		InputObject::new("GeometryLineStringInput")
			.description("GeoJSON LineString input")
			.field(InputValue::new("coordinates", nested_float_list(2))),
	));
	types.push(Type::InputObject(
		InputObject::new("GeometryPolygonInput")
			.description("GeoJSON Polygon input")
			.field(InputValue::new("coordinates", nested_float_list(3))),
	));
	types.push(Type::InputObject(
		InputObject::new("GeometryMultiPointInput")
			.description("GeoJSON MultiPoint input")
			.field(InputValue::new("coordinates", nested_float_list(2))),
	));
	types.push(Type::InputObject(
		InputObject::new("GeometryMultiLineStringInput")
			.description("GeoJSON MultiLineString input")
			.field(InputValue::new("coordinates", nested_float_list(3))),
	));
	types.push(Type::InputObject(
		InputObject::new("GeometryMultiPolygonInput")
			.description("GeoJSON MultiPolygon input")
			.field(InputValue::new("coordinates", nested_float_list(4))),
	));
	types.push(Type::InputObject(
		InputObject::new("GeometryCollectionInput")
			.description("GeoJSON GeometryCollection input")
			.field(InputValue::new("geometries", TypeRef::named_nn_list_nn("GeometryInput"))),
	));

	// Unified GeometryInput (for fields typed as `geometry` without specific variant)
	types.push(Type::InputObject(
		InputObject::new("GeometryInput")
			.description(
				"Generic GeoJSON geometry input. Use `type` to select the variant, \
				 `coordinates` for coordinate-based types, `geometries` for GeometryCollection.",
			)
			.field(InputValue::new("type", TypeRef::named_nn("GeometryType")))
			.field(InputValue::new("coordinates", TypeRef::named("any")))
			.field(InputValue::new("geometries", TypeRef::named_list("GeometryInput"))),
	));
}

/// Convert a GraphQL coordinate value (nested arrays of numbers) to a SurrealDB
/// `Value` suitable for `Geometry::array_to_*` helpers.
fn gql_coords_to_sur_value(val: &GqlValue) -> Result<SurValue, GqlError> {
	match val {
		GqlValue::Number(n) => {
			let f = n
				.as_f64()
				.ok_or_else(|| resolver_error("Invalid coordinate: expected finite number"))?;
			Ok(SurValue::Number(SurNumber::Float(f)))
		}
		GqlValue::List(items) => {
			let vals: Result<Vec<SurValue>, GqlError> =
				items.iter().map(gql_coords_to_sur_value).collect();
			Ok(vals?.into())
		}
		_ => Err(resolver_error("Expected number or array in geometry coordinates")),
	}
}

/// Convert a GraphQL geometry input Object (GeoJSON format) to a SurrealDB
/// `Geometry` value.
///
/// For typed inputs (e.g. `GeometryPointInput`), the `type` field is optional
/// and the variant is inferred from `expected_kind`. For the unified
/// `GeometryInput`, the `type` field is required.
fn gql_geometry_from_object(
	obj: &IndexMap<Name, GqlValue>,
	expected_kind: &[GeometryKind],
) -> Result<SurGeometry, GqlError> {
	// Determine the geometry type: from explicit `type` field or from expected_kind
	let geo_type: &str = if let Some(ty) = obj.get("type") {
		match ty {
			GqlValue::Enum(s) => s.as_str(),
			GqlValue::String(s) => s.as_str(),
			_ => return Err(resolver_error("Geometry 'type' must be a GeometryType enum value")),
		}
	} else if expected_kind.len() == 1 {
		// Infer from the single expected kind
		match &expected_kind[0] {
			GeometryKind::Point => "Point",
			GeometryKind::Line => "LineString",
			GeometryKind::Polygon => "Polygon",
			GeometryKind::MultiPoint => "MultiPoint",
			GeometryKind::MultiLine => "MultiLineString",
			GeometryKind::MultiPolygon => "MultiPolygon",
			GeometryKind::Collection => "GeometryCollection",
		}
	} else {
		return Err(resolver_error(
			"Geometry input must include a 'type' field when multiple geometry types are allowed",
		));
	};

	// Validate that the type is allowed by the expected kinds
	if !expected_kind.is_empty() {
		let kind = match geo_type {
			"Point" => GeometryKind::Point,
			"LineString" => GeometryKind::Line,
			"Polygon" => GeometryKind::Polygon,
			"MultiPoint" => GeometryKind::MultiPoint,
			"MultiLineString" => GeometryKind::MultiLine,
			"MultiPolygon" => GeometryKind::MultiPolygon,
			"GeometryCollection" => GeometryKind::Collection,
			other => return Err(resolver_error(format!("Unknown geometry type: {other}"))),
		};
		if !expected_kind.contains(&kind) {
			return Err(resolver_error(format!(
				"Geometry type '{geo_type}' is not allowed here; expected one of: {}",
				expected_kind
					.iter()
					.map(|k| geometry_kind_to_gql_type_name(k))
					.collect::<Vec<_>>()
					.join(", ")
			)));
		}
	}

	// Parse based on type
	match geo_type {
		"Point" => {
			let coords = obj
				.get("coordinates")
				.ok_or_else(|| resolver_error("Point requires 'coordinates' field"))?;
			let sur_coords = gql_coords_to_sur_value(coords)?;
			SurGeometry::array_to_point(&sur_coords)
				.map(SurGeometry::Point)
				.ok_or_else(|| resolver_error("Invalid Point coordinates: expected [lng, lat]"))
		}
		"LineString" => {
			let coords = obj
				.get("coordinates")
				.ok_or_else(|| resolver_error("LineString requires 'coordinates' field"))?;
			let sur_coords = gql_coords_to_sur_value(coords)?;
			SurGeometry::array_to_line(&sur_coords).map(SurGeometry::Line).ok_or_else(|| {
				resolver_error("Invalid LineString coordinates: expected [[lng, lat], ...]")
			})
		}
		"Polygon" => {
			let coords = obj
				.get("coordinates")
				.ok_or_else(|| resolver_error("Polygon requires 'coordinates' field"))?;
			let sur_coords = gql_coords_to_sur_value(coords)?;
			SurGeometry::array_to_polygon(&sur_coords).map(SurGeometry::Polygon).ok_or_else(|| {
				resolver_error("Invalid Polygon coordinates: expected [[[lng, lat], ...], ...]")
			})
		}
		"MultiPoint" => {
			let coords = obj
				.get("coordinates")
				.ok_or_else(|| resolver_error("MultiPoint requires 'coordinates' field"))?;
			let sur_coords = gql_coords_to_sur_value(coords)?;
			SurGeometry::array_to_multipoint(&sur_coords).map(SurGeometry::MultiPoint).ok_or_else(
				|| resolver_error("Invalid MultiPoint coordinates: expected [[lng, lat], ...]"),
			)
		}
		"MultiLineString" => {
			let coords = obj
				.get("coordinates")
				.ok_or_else(|| resolver_error("MultiLineString requires 'coordinates' field"))?;
			let sur_coords = gql_coords_to_sur_value(coords)?;
			SurGeometry::array_to_multiline(&sur_coords).map(SurGeometry::MultiLine).ok_or_else(
				|| {
					resolver_error(
						"Invalid MultiLineString coordinates: expected [[[lng, lat], ...], ...]",
					)
				},
			)
		}
		"MultiPolygon" => {
			let coords = obj
				.get("coordinates")
				.ok_or_else(|| resolver_error("MultiPolygon requires 'coordinates' field"))?;
			let sur_coords = gql_coords_to_sur_value(coords)?;
			SurGeometry::array_to_multipolygon(&sur_coords)
				.map(SurGeometry::MultiPolygon)
				.ok_or_else(|| resolver_error("Invalid MultiPolygon coordinates"))
		}
		"GeometryCollection" => {
			let gql_geometries = obj
				.get("geometries")
				.ok_or_else(|| resolver_error("GeometryCollection requires 'geometries' field"))?;
			let list = match gql_geometries {
				GqlValue::List(l) => l,
				_ => {
					return Err(resolver_error("GeometryCollection 'geometries' must be an array"));
				}
			};
			let mut geometries = Vec::with_capacity(list.len());
			for item in list {
				match item {
					GqlValue::Object(sub_obj) => {
						// Recursively parse each sub-geometry (allow any type)
						geometries.push(gql_geometry_from_object(sub_obj, &[])?);
					}
					_ => {
						return Err(resolver_error(
							"Each item in 'geometries' must be a geometry object",
						));
					}
				}
			}
			Ok(SurGeometry::Collection(geometries))
		}
		other => Err(resolver_error(format!("Unknown geometry type: {other}"))),
	}
}

/// Convert a SurrealDB `Geometry` value to a `GqlValue::Object` in GeoJSON format.
///
/// Used by `sql_value_to_gql_value` for geometry values in arrays / nested objects
/// where we cannot use `FieldValue::owned_any`.
pub(crate) fn geometry_to_gql_object(g: &SurGeometry) -> Result<GqlValue, GqlError> {
	let mut map = IndexMap::new();
	map.insert(Name::new("type"), GqlValue::Enum(Name::new(g.as_type())));

	match g {
		SurGeometry::Collection(geometries) => {
			let gql_geometries: Result<Vec<GqlValue>, GqlError> =
				geometries.iter().map(geometry_to_gql_object).collect();
			map.insert(Name::new("geometries"), GqlValue::List(gql_geometries?));
		}
		_ => {
			let coords = g.as_coordinates();
			let gql_coords = sql_value_to_gql_value(coords)?;
			map.insert(Name::new("coordinates"), gql_coords);
		}
	}

	Ok(GqlValue::Object(map))
}
