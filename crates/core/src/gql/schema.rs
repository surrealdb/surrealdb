use std::collections::BTreeMap;
use std::sync::Arc;

use crate::dbs::Session;
use crate::gql::functions::process_fns;
use crate::gql::tables::process_tbs;
use crate::kvs::Datastore;
use crate::sql;
use crate::sql::kind::Literal;
use crate::sql::statements::define::config::graphql::{FunctionsConfig, TablesConfig};
use crate::sql::Geometry;
use crate::sql::Kind;
use async_graphql::dynamic::Interface;
use async_graphql::dynamic::InterfaceField;
use async_graphql::dynamic::Object;
use async_graphql::dynamic::Schema;
use async_graphql::dynamic::{Enum, Type, Union};
use async_graphql::dynamic::{Scalar, TypeRef};
use async_graphql::Name;
use async_graphql::Value as GqlValue;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde_json::Number;

use super::error::{resolver_error, GqlError};
#[cfg(debug_assertions)]
use super::ext::ValidatorExt;
use crate::gql::error::{internal_error, schema_error, type_error};
use crate::gql::ext::NamedContainer;
use crate::kvs::LockType;
use crate::kvs::TransactionType;
use crate::sql::Value as SqlValue;

pub async fn generate_schema(
	datastore: &Arc<Datastore>,
	session: &Session,
) -> Result<Schema, GqlError> {
	let kvs = datastore;
	let tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;
	let ns = session.ns.as_ref().ok_or(GqlError::UnspecifiedNamespace)?;
	let db = session.db.as_ref().ok_or(GqlError::UnspecifiedDatabase)?;

	let cg = tx.get_db_config(ns, db, "graphql").await.map_err(|e| match e {
		crate::err::Error::CgNotFound {
			..
		} => GqlError::NotConfigured,
		e => e.into(),
	})?;
	let config = cg.inner.clone().try_into_graphql()?;

	let tbs = tx.all_tb(ns, db, None).await?;

	let tbs = match config.tables {
		TablesConfig::None => None,
		TablesConfig::Auto => Some(tbs),
		TablesConfig::Include(inc) => {
			Some(tbs.iter().filter(|t| inc.contains_name(&t.name)).cloned().collect())
		}
		TablesConfig::Exclude(exc) => {
			Some(tbs.iter().filter(|t| !exc.contains_name(&t.name)).cloned().collect())
		}
	};

	let fns = tx.all_db_functions(ns, db).await?;

	let fns = match config.functions {
		FunctionsConfig::None => None,
		FunctionsConfig::Auto => Some(fns),
		FunctionsConfig::Include(inc) => {
			Some(fns.iter().filter(|f| inc.contains(&f.name)).cloned().collect())
		}
		FunctionsConfig::Exclude(exc) => {
			Some(fns.iter().filter(|f| !exc.contains(&f.name)).cloned().collect())
		}
	};

	match (&tbs, &fns) {
		(None, None) => return Err(GqlError::NotConfigured),
		(None, Some(fs)) if fs.len() == 0 => {
			return Err(schema_error("no functions found in database"))
		}
		(Some(ts), None) if ts.len() == 0 => {
			return Err(schema_error("no tables found in database"))
		}
		(Some(ts), Some(fs)) if ts.len() == 0 && fs.len() == 0 => {
			return Err(schema_error("no items found in database"));
		}
		_ => {}
	}

	let mut query = Object::new("Query");
	let mut types: Vec<Type> = Vec::new();

	trace!(ns, db, ?tbs, ?fns, "generating schema");

	match tbs {
		Some(tbs) if tbs.len() > 0 => {
			query = process_tbs(tbs, query, &mut types, &tx, ns, db, session, datastore).await?;
		}
		_ => {}
	}

	if let Some(fns) = fns {
		query = process_fns(fns, query, &mut types, session, datastore).await?;
	}

	trace!("current Query object for schema: {:?}", query);

	let mut schema = Schema::build("Query", None, None).register(query);
	for ty in types {
		trace!("adding type: {ty:?}");
		schema = schema.register(ty);
	}

	macro_rules! scalar_debug_validated {
		($schema:ident, $name:expr, $kind:expr) => {
			scalar_debug_validated!(
				$schema,
				$name,
				$kind,
				::std::option::Option::<&str>::None,
				::std::option::Option::<&str>::None
			)
		};
		($schema:ident, $name:expr, $kind:expr, $desc:literal) => {
			scalar_debug_validated!($schema, $name, $kind, std::option::Option::Some($desc), None)
		};
		($schema:ident, $name:expr, $kind:expr, $desc:literal, $url:literal) => {
			scalar_debug_validated!(
				$schema,
				$name,
				$kind,
				std::option::Option::Some($desc),
				Some($url)
			)
		};
		($schema:ident, $name:expr, $kind:expr, $desc:expr, $url:expr) => {{
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

	// TODO: when used get: `Result::unwrap()` on an `Err` value: SchemaError("Field \"like.in\" is not sub-type of \"relation.in\"")
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

pub fn sql_value_to_gql_value(v: SqlValue) -> Result<GqlValue, GqlError> {
	let out = match v {
		SqlValue::None => GqlValue::Null,
		SqlValue::Null => GqlValue::Null,
		SqlValue::Bool(b) => GqlValue::Boolean(b),
		SqlValue::Number(n) => match n {
			crate::sql::Number::Int(i) => GqlValue::Number(i.into()),
			crate::sql::Number::Float(f) => GqlValue::Number(
				Number::from_f64(f)
					.ok_or(resolver_error("unimplemented: graceful NaN and Inf handling"))?,
			),
			num @ crate::sql::Number::Decimal(_) => GqlValue::String(num.to_string()),
		},
		SqlValue::Strand(s) => GqlValue::String(s.0),
		d @ SqlValue::Duration(_) => GqlValue::String(d.to_string()),
		SqlValue::Datetime(d) => GqlValue::String(d.to_rfc3339()),
		SqlValue::Uuid(uuid) => GqlValue::String(uuid.to_string()),
		SqlValue::Array(a) => {
			GqlValue::List(a.into_iter().map(|v| sql_value_to_gql_value(v).unwrap()).collect())
		}
		SqlValue::Object(o) => GqlValue::Object(
			o.0.into_iter()
				.map(|(k, v)| (Name::new(k), sql_value_to_gql_value(v).unwrap()))
				.collect(),
		),
		SqlValue::Geometry(_) => return Err(resolver_error("unimplemented: Geometry types")),
		SqlValue::Bytes(b) => GqlValue::Binary(b.into_inner().into()),
		SqlValue::Thing(t) => GqlValue::String(t.to_string()),
		v => return Err(internal_error(format!("found unsupported value variant: {v:?}"))),
	};
	Ok(out)
}

pub fn kind_to_type(kind: Kind, types: &mut Vec<Type>) -> Result<TypeRef, GqlError> {
	let (optional, match_kind) = match kind {
		Kind::Option(op_ty) => (true, *op_ty),
		_ => (false, kind),
	};
	let out_ty = match match_kind {
		Kind::Any => TypeRef::named("any"),
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
		Kind::Point => return Err(schema_error("Kind::Point is not yet supported")),
		Kind::String => TypeRef::named(TypeRef::STRING),
		Kind::Uuid => TypeRef::named("uuid"),
		Kind::Record(mut r) => match r.len() {
			0 => TypeRef::named("record"),
			1 => TypeRef::named(r.pop().unwrap().0),
			_ => {
				let names: Vec<String> = r.into_iter().map(|t| t.0).collect();
				let ty_name = names.join("_or_");

				let mut tmp_union = Union::new(ty_name.clone())
					.description(format!("A record which is one of: {}", names.join(", ")));
				for n in names {
					tmp_union = tmp_union.possible_type(n);
				}

				types.push(Type::Union(tmp_union));
				TypeRef::named(ty_name)
			}
		},
		Kind::Geometry(_) => return Err(schema_error("Kind::Geometry is not yet supported")),
		Kind::Option(t) => {
			let mut non_op_ty = *t;
			while let Kind::Option(inner) = non_op_ty {
				non_op_ty = *inner;
			}
			kind_to_type(non_op_ty, types)?
		}
		Kind::Either(ks) => {
			let (ls, others): (Vec<Kind>, Vec<Kind>) =
				ks.into_iter().partition(|k| matches!(k, Kind::Literal(Literal::String(_))));

			let enum_ty = if !ls.is_empty() {
				let vals: Vec<String> = ls
					.into_iter()
					.map(|l| {
						let Kind::Literal(Literal::String(out)) = l else {
							unreachable!(
								"just checked that this is a Kind::Literal(Literal::String(_))"
							);
						};
						out.0
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
				others.into_iter().map(|k| kind_to_type(k, types)).collect();
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
		Kind::Array(k, _) => TypeRef::List(Box::new(kind_to_type(*k, types)?)),
		Kind::Function(_, _) => return Err(schema_error("Kind::Function is not yet supported")),
		Kind::Range => return Err(schema_error("Kind::Range is not yet supported")),
		// TODO(raphaeldarley): check if union is of literals and generate enum
		// generate custom scalar from other literals?
		Kind::Literal(_) => return Err(schema_error("Kind::Literal is not yet supported")),
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
	($ks:ident, $val:expr, Kind::Array) => {
		for arr_kind in $ks.iter().filter(|k| matches!(k, Kind::Array(_, _))).cloned() {
			either_try_kind!($ks, $val, arr_kind);
		}
	};
	($ks:ident, $val:expr, Array) => {
		for arr_kind in $ks.iter().filter(|k| matches!(k, Kind::Array(_, _))).cloned() {
			either_try_kind!($ks, $val, arr_kind);
		}
	};
	($ks:ident, $val:expr, Record) => {
		for arr_kind in $ks.iter().filter(|k| matches!(k, Kind::Array(_, _))).cloned() {
			either_try_kind!($ks, $val, arr_kind);
		}
	};
	($ks:ident, $val:expr, AllNumbers) => {
		either_try_kind!($ks, $val, Kind::Int);
		either_try_kind!($ks, $val, Kind::Float);
		either_try_kind!($ks, $val, Kind::Decimal);
		either_try_kind!($ks, $val, Kind::Number);
	};
	($ks:ident, $val:expr, $kind:expr) => {
		if $ks.contains(&$kind) {
			if let Ok(out) = gql_to_sql_kind($val, $kind) {
				return Ok(out);
			}
		}
	};
}

macro_rules! either_try_kinds {
	($ks:ident, $val:expr, $($kind:tt),+) => {
		$(either_try_kind!($ks, $val, $kind));+
	};
}

macro_rules! any_try_kind {
	($val:expr, $kind:expr) => {
		if let Ok(out) = gql_to_sql_kind($val, $kind) {
			return Ok(out);
		}
	};
}
macro_rules! any_try_kinds {
	($val:expr, $($kind:tt),+) => {
		$(any_try_kind!($val, $kind));+
	};
}

pub fn gql_to_sql_kind(val: &GqlValue, kind: Kind) -> Result<SqlValue, GqlError> {
	use crate::syn;
	match kind {
		Kind::Any => match val {
			GqlValue::String(s) => {
				use Kind::*;
				any_try_kinds!(val, Datetime, Duration, Uuid);
				syn::value_legacy_strand(s.as_str()).map_err(|_| type_error(kind, val))
			}
			GqlValue::Null => Ok(SqlValue::Null),
			obj @ GqlValue::Object(_) => gql_to_sql_kind(obj, Kind::Object),
			num @ GqlValue::Number(_) => gql_to_sql_kind(num, Kind::Number),
			GqlValue::Boolean(b) => Ok(SqlValue::Bool(*b)),
			bin @ GqlValue::Binary(_) => gql_to_sql_kind(bin, Kind::Bytes),
			GqlValue::Enum(s) => Ok(SqlValue::Strand(s.as_str().into())),
			arr @ GqlValue::List(_) => gql_to_sql_kind(arr, Kind::Array(Box::new(Kind::Any), None)),
		},
		Kind::Null => match val {
			GqlValue::Null => Ok(SqlValue::Null),
			_ => Err(type_error(kind, val)),
		},
		Kind::Bool => match val {
			GqlValue::Boolean(b) => Ok(SqlValue::Bool(*b)),
			_ => Err(type_error(kind, val)),
		},
		Kind::Bytes => match val {
			GqlValue::Binary(b) => Ok(SqlValue::Bytes(b.to_owned().to_vec().into())),
			_ => Err(type_error(kind, val)),
		},
		Kind::Datetime => match val {
			GqlValue::String(s) => match syn::datetime(s) {
				Ok(dt) => Ok(dt.into()),
				Err(_) => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Decimal => match val {
			GqlValue::Number(n) => {
				if let Some(int) = n.as_i64() {
					Ok(SqlValue::Number(sql::Number::Decimal(int.into())))
				} else if let Some(d) = n.as_f64().and_then(Decimal::from_f64) {
					Ok(SqlValue::Number(sql::Number::Decimal(d)))
				} else if let Some(uint) = n.as_u64() {
					Ok(SqlValue::Number(sql::Number::Decimal(uint.into())))
				} else {
					Err(type_error(kind, val))
				}
			}
			GqlValue::String(s) => match syn::value(s) {
				Ok(SqlValue::Number(n)) => match n {
					sql::Number::Int(i) => Ok(SqlValue::Number(sql::Number::Decimal(i.into()))),
					sql::Number::Float(f) => match Decimal::from_f64(f) {
						Some(d) => Ok(SqlValue::Number(sql::Number::Decimal(d))),
						None => Err(type_error(kind, val)),
					},
					sql::Number::Decimal(d) => Ok(SqlValue::Number(sql::Number::Decimal(d))),
				},
				_ => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Duration => match val {
			GqlValue::String(s) => match syn::duration(s) {
				Ok(d) => Ok(d.into()),
				Err(_) => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Float => match val {
			GqlValue::Number(n) => {
				if let Some(i) = n.as_i64() {
					Ok(SqlValue::Number(sql::Number::Float(i as f64)))
				} else if let Some(f) = n.as_f64() {
					Ok(SqlValue::Number(sql::Number::Float(f)))
				} else if let Some(uint) = n.as_u64() {
					Ok(SqlValue::Number(sql::Number::Float(uint as f64)))
				} else {
					unreachable!("serde_json::Number must be either i64, u64 or f64")
				}
			}
			GqlValue::String(s) => match syn::value(s) {
				Ok(SqlValue::Number(n)) => match n {
					sql::Number::Int(int) => Ok(SqlValue::Number(sql::Number::Float(int as f64))),
					sql::Number::Float(float) => Ok(SqlValue::Number(sql::Number::Float(float))),
					sql::Number::Decimal(d) => match d.try_into() {
						Ok(f) => Ok(SqlValue::Number(sql::Number::Float(f))),
						_ => Err(type_error(kind, val)),
					},
				},
				_ => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Int => match val {
			GqlValue::Number(n) => {
				if let Some(i) = n.as_i64() {
					Ok(SqlValue::Number(sql::Number::Int(i)))
				} else {
					Err(type_error(kind, val))
				}
			}
			GqlValue::String(s) => match syn::value(s) {
				Ok(SqlValue::Number(n)) => match n {
					sql::Number::Int(int) => Ok(SqlValue::Number(sql::Number::Int(int))),
					sql::Number::Float(float) => {
						if float.fract() == 0.0 {
							Ok(SqlValue::Number(sql::Number::Int(float as i64)))
						} else {
							Err(type_error(kind, val))
						}
					}
					sql::Number::Decimal(d) => match d.try_into() {
						Ok(i) => Ok(SqlValue::Number(sql::Number::Int(i))),
						_ => Err(type_error(kind, val)),
					},
				},
				_ => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Number => match val {
			GqlValue::Number(n) => {
				if let Some(i) = n.as_i64() {
					Ok(SqlValue::Number(sql::Number::Int(i)))
				} else if let Some(f) = n.as_f64() {
					Ok(SqlValue::Number(sql::Number::Float(f)))
				} else if let Some(uint) = n.as_u64() {
					Ok(SqlValue::Number(sql::Number::Decimal(uint.into())))
				} else {
					unreachable!("serde_json::Number must be either i64, u64 or f64")
				}
			}
			GqlValue::String(s) => match syn::value(s) {
				Ok(SqlValue::Number(n)) => Ok(SqlValue::Number(n)),
				_ => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Object => match val {
			GqlValue::Object(o) => {
				let out: Result<BTreeMap<String, SqlValue>, GqlError> = o
					.iter()
					.map(|(k, v)| gql_to_sql_kind(v, Kind::Any).map(|sqlv| (k.to_string(), sqlv)))
					.collect();
				Ok(SqlValue::Object(out?.into()))
			}
			GqlValue::String(s) => match syn::value_legacy_strand(s.as_str()) {
				Ok(obj @ SqlValue::Object(_)) => Ok(obj),
				_ => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Point => match val {
			GqlValue::List(l) => match l.as_slice() {
				[GqlValue::Number(x), GqlValue::Number(y)] => match (x.as_f64(), y.as_f64()) {
					(Some(x), Some(y)) => Ok(SqlValue::Geometry(Geometry::Point((x, y).into()))),
					_ => Err(type_error(kind, val)),
				},
				_ => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::String => match val {
			GqlValue::String(s) => Ok(SqlValue::Strand(s.to_owned().into())),
			GqlValue::Enum(s) => Ok(SqlValue::Strand(s.as_str().into())),
			_ => Err(type_error(kind, val)),
		},
		Kind::Uuid => match val {
			GqlValue::String(s) => match s.parse::<uuid::Uuid>() {
				Ok(u) => Ok(SqlValue::Uuid(u.into())),
				Err(_) => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Record(ref ts) => match val {
			GqlValue::String(s) => match syn::thing(s) {
				Ok(t) => match ts.contains(&t.tb.as_str().into()) {
					true => Ok(SqlValue::Thing(t)),
					false => Err(type_error(kind, val)),
				},
				Err(_) => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		// TODO: add geometry
		Kind::Geometry(_) => Err(resolver_error("Geometry is not yet supported")),
		Kind::Option(k) => match val {
			GqlValue::Null => Ok(SqlValue::None),
			v => gql_to_sql_kind(v, *k),
		},
		// TODO: handle nested eithers
		Kind::Either(ref ks) => {
			use Kind::*;

			match val {
				GqlValue::Null => {
					if ks.iter().any(|k| matches!(k, Kind::Option(_))) {
						Ok(SqlValue::None)
					} else if ks.contains(&Kind::Null) {
						Ok(SqlValue::Null)
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
						ks, string, Datetime, Duration, AllNumbers, Object, Uuid, Array, Any,
						String
					);
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
				// TODO: consider geometry and other types that can come from objects
				obj @ GqlValue::Object(_) => {
					either_try_kind!(ks, obj, Object);
					Err(type_error(kind, val))
				}
			}
		}
		Kind::Set(_k, _n) => Err(resolver_error("Sets are not yet supported")),
		Kind::Array(ref k, n) => match val {
			GqlValue::List(l) => {
				let list_iter = l.iter().map(|v| gql_to_sql_kind(v, *k.to_owned()));
				let list: Result<Vec<SqlValue>, GqlError> = list_iter.collect();

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
		Kind::Function(_, _) => Err(resolver_error("Sets are not yet supported")),
		Kind::Range => Err(resolver_error("Ranges are not yet supported")),
		Kind::Literal(_) => Err(resolver_error("Literals are not yet supported")),
	}
}
