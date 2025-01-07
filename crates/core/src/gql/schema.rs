use std::collections::BTreeMap;
use std::sync::Arc;

use crate::dbs::Session;
use crate::gql::functions::process_fns;
use crate::gql::tables::process_tbs;
use crate::gql::utils::GqlValueUtils;
use crate::kvs::Datastore;
use crate::sql;
use crate::sql::kind::Literal;
use crate::sql::statements::define::config::graphql::{FunctionsConfig, TablesConfig};
use crate::sql::Geometry;
use crate::sql::Kind;
use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{Enum, InputValue, Type, Union};
use async_graphql::dynamic::{Field, Interface};
use async_graphql::dynamic::{FieldFuture, Object};
use async_graphql::dynamic::{InputObject, Schema};
use async_graphql::dynamic::{InterfaceField, ResolverContext};
use async_graphql::dynamic::{Scalar, TypeRef};
use async_graphql::Name;
use async_graphql::Value as GqlValue;
use geo::{Coord, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde_json::Number;

use super::error::{resolver_error, GqlError};
#[cfg(debug_assertions)]
use super::ext::ValidatorExt;
use super::ext::{TryFromExt, TryIntoExt};
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

	macro_rules! geometry_type {
		($schema:ident, $name:expr, $type:expr) => {
			$schema = $schema.register(
				Object::new($name)
					.field(Field::new(
						"type",
						TypeRef::named("GeometryType"),
						|_: ResolverContext| {
							async_graphql::dynamic::FieldFuture::Value(Some(
								GqlValue::String($name.to_string()).into(),
							))
						},
					))
					.field(Field::new("coordinates", $type.clone(), |ctx: ResolverContext| {
						FieldFuture::new({
							async move {
								let val = ctx
									.parent_value
									.try_to_value()?
									.as_object()
									.ok_or_else(|| internal_error("Expected object"))?
									.get("coordinates");
								Ok(val.cloned())
							}
						})
					})),
			);
			$schema = $schema.register(
				InputObject::new(format!("{}_input", $name))
					.field(InputValue::new("type", TypeRef::named("GeometryType")))
					// .field(InputValue::new("geotype", TypeRef::named(TypeRef::STRING)))
					.field(InputValue::new("coordinates", $type.clone())),
			);
		};
	}

	scalar_debug_validated!(
		schema,
		"uuid",
		Kind::Uuid,
		"String encoded UUID",
		"https://datatracker.ietf.org/doc/html/rfc4122"
	);

	schema = schema.register(Enum::new("GeometryType").items([
		"GeometryPoint",
		"GeometryLineString",
		"GeometryPolygon",
		"GeometryMultiPoint",
		"GeometryMultiLineString",
		"GeometryMultiPolygon",
		"GeometryCollection",
	]));

	let coordinate_type = TypeRef::named_nn_list_nn(TypeRef::FLOAT);
	let coordinate_list_type = TypeRef::NonNull(Box::new(TypeRef::List(Box::new(
		TypeRef::NonNull(Box::new(coordinate_type.clone())),
	))));
	let coordinate_list_list_type = TypeRef::NonNull(Box::new(TypeRef::List(Box::new(
		TypeRef::NonNull(Box::new(coordinate_list_type.clone())),
	))));
	let coordinate_list_list_list_type = TypeRef::NonNull(Box::new(TypeRef::List(Box::new(
		TypeRef::NonNull(Box::new(coordinate_list_type.clone())),
	))));

	geometry_type!(schema, "GeometryPoint", coordinate_type);
	geometry_type!(schema, "GeometryLineString", coordinate_list_type);
	geometry_type!(schema, "GeometryPolygon", coordinate_list_list_type);
	geometry_type!(schema, "GeometryMultiPoint", coordinate_list_type);
	geometry_type!(schema, "GeometryMultiLineString", coordinate_list_list_type);
	geometry_type!(schema, "GeometryMultiPolygon", coordinate_list_list_list_type);

	schema = schema.register(
		Object::new("GeometryCollection")
			.field(Field::new("type", TypeRef::named("GeometryType"), |_: ResolverContext| {
				async_graphql::dynamic::FieldFuture::Value(Some(
					GqlValue::String("GeometryCollection".to_string()).into(),
				))
			}))
			.field(Field::new(
				"geometries",
				TypeRef::named_nn_list_nn("Geometry"),
				|ctx: ResolverContext| {
					FieldFuture::new({
						async move {
							let val = ctx
								.parent_value
								.try_to_value()?
								.as_object()
								.ok_or_else(|| internal_error("Expected Object"))?
								.get("geometries");
							Ok(val.cloned())
						}
					})
				},
			)),
	);
	schema = schema.register(
		InputObject::new("GeometryCollection_input")
			.field(InputValue::new("type", TypeRef::named("GeometryType")))
			.field(InputValue::new("geometries", TypeRef::named_nn_list_nn("Geometry_input"))),
	);

	let mut geometry_union = Union::new("Geometry");
	geometry_union = geometry_union.possible_type("GeometryPoint");
	geometry_union = geometry_union.possible_type("GeometryLineString");
	geometry_union = geometry_union.possible_type("GeometryPolygon");
	geometry_union = geometry_union.possible_type("GeometryMultiPoint");
	geometry_union = geometry_union.possible_type("GeometryMultiLineString");
	geometry_union = geometry_union.possible_type("GeometryMultiPolygon");
	geometry_union = geometry_union.possible_type("GeometryCollection");

	schema = schema.register(geometry_union);

	scalar_debug_validated!(schema, "Geometry_input", Kind::Geometry(vec![]));

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
		SqlValue::Geometry(kind) => match kind {
			Geometry::Point(point) => GqlValue::Object(
				[
					(Name::new("type"), GqlValue::String("Point".to_string())),
					(
						Name::new("coordinates"),
						GqlValue::List(vec![point.x().try_intox()?, point.y().try_intox()?]),
					),
				]
				.into(),
			),
			Geometry::Line(line) => GqlValue::Object(
				[
					(Name::new("type"), GqlValue::String("LineString".to_string())),
					(Name::new("coordinates"), coord_collection_to_list(line)?),
				]
				.into(),
			),
			Geometry::MultiLine(multiline) => GqlValue::Object(
				[
					(Name::new("type"), GqlValue::String("MultiLineString".to_string())),
					(
						Name::new("coordinates"),
						GqlValue::List(
							multiline
								.into_iter()
								.map(coord_collection_to_list)
								.collect::<Result<Vec<_>, GqlError>>()?,
						),
					),
				]
				.into(),
			),
			Geometry::MultiPoint(multipoint) => GqlValue::Object(
				[
					(Name::new("type"), GqlValue::String("MultiPoint".to_string())),
					(
						Name::new("coordinates"),
						coord_collection_to_list(multipoint.into_iter().map(|p| p.0))?,
					),
				]
				.into(),
			),
			Geometry::Polygon(polygon) => GqlValue::Object(
				[
					(Name::new("type"), GqlValue::String("Polygon".to_string())),
					(Name::new("coordinates"), polygon_to_list(&polygon)?),
				]
				.into(),
			),
			Geometry::MultiPolygon(multipolygon) => GqlValue::Object(
				[
					(Name::new("type"), GqlValue::String("MultiPolygon".to_string())),
					(
						Name::new("coordinates"),
						GqlValue::List(
							multipolygon
								.iter()
								.map(polygon_to_list)
								.collect::<Result<Vec<_>, _>>()?,
						),
					),
				]
				.into(),
			),
			Geometry::Collection(collection) => GqlValue::Object(
				[
					(Name::new("type"), GqlValue::String("GeometryCollection".to_string())),
					(
						Name::new("geometries"),
						GqlValue::List(
							collection
								.into_iter()
								.map(|g| sql_value_to_gql_value(SqlValue::Geometry(g)).unwrap())
								.collect(),
						),
					),
				]
				.into(),
			),
		},
		SqlValue::Bytes(b) => GqlValue::Binary(b.into_inner().into()),
		SqlValue::Thing(t) => GqlValue::String(t.to_string()),
		v => return Err(internal_error(format!("found unsupported value variant: {v:?}"))),
	};
	Ok(out)
}

fn coord_to_list(coord: Coord<f64>) -> Result<GqlValue, GqlError> {
	Ok(GqlValue::List(
		<[f64; 2]>::from(coord)
			.into_iter()
			.map(GqlValue::try_fromx)
			.collect::<Result<Vec<_>, _>>()?,
	))
}

fn coord_collection_to_list(
	coord_collection: impl IntoIterator<Item = Coord<f64>>,
) -> Result<GqlValue, GqlError> {
	Ok(GqlValue::List(
		coord_collection.into_iter().map(coord_to_list).collect::<Result<Vec<_>, _>>()?,
	))
}

fn polygon_to_list(polygon: &Polygon) -> Result<GqlValue, GqlError> {
	Ok(GqlValue::List(
		[polygon.exterior()]
			.into_iter()
			.chain(polygon.interiors().iter())
			.cloned()
			.map(coord_collection_to_list)
			.collect::<Result<_, _>>()?,
	))
}

fn geometry_kind_name_to_type_name(name: &str) -> Result<&'static str, GqlError> {
	match name {
		"point" => Ok("GeometryPoint"),
		"line" => Ok("GeometryLineString"),
		"polygon" => Ok("GeometryPolygon"),
		"multipoint" => Ok("GeometryMultiPoint"),
		"multiline" => Ok("GeometryMultiLineString"),
		"multipolygon" => Ok("GeometryMultiPolygon"),
		"collection" => Ok("GeometryCollection"),
		_ => Err(internal_error(format!("expected valid geometry name"))),
	}
}

pub fn kind_to_type(
	kind: Kind,
	types: &mut Vec<Type>,
	is_input: bool,
) -> Result<TypeRef, GqlError> {
	let (optional, match_kind) = match kind {
		Kind::Option(op_ty) => (true, *op_ty),
		_ => (false, kind),
	};
	let out_ty = match (match_kind, is_input) {
		(Kind::Any, _) => TypeRef::named("any"),
		(Kind::Null, _) => TypeRef::named("null"),
		(Kind::Bool, _) => TypeRef::named(TypeRef::BOOLEAN),
		(Kind::Bytes, _) => TypeRef::named("bytes"),
		(Kind::Datetime, _) => TypeRef::named("datetime"),
		(Kind::Decimal, _) => TypeRef::named("decimal"),
		(Kind::Duration, _) => TypeRef::named("duration"),
		(Kind::Float, _) => TypeRef::named(TypeRef::FLOAT),
		(Kind::Int, _) => TypeRef::named(TypeRef::INT),
		(Kind::Number, _) => TypeRef::named("number"),
		(Kind::Object, _) => TypeRef::named("object"),
		(Kind::Point, _) => return Err(schema_error("Kind::Point is not yet supported")),
		(Kind::String, _) => TypeRef::named(TypeRef::STRING),
		(Kind::Uuid, _) => TypeRef::named("uuid"),
		(Kind::Record(mut r), _) => match r.len() {
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
		(Kind::Geometry(g), false) => match g.len() {
			0 => TypeRef::named("Geometry"),
			1 => {
				let name = g.into_iter().next().expect("checked that length is 1");
				TypeRef::named(geometry_kind_name_to_type_name(&name)?)
			}
			_ => {
				let geo_types = g
					.iter()
					.map(|n| geometry_kind_name_to_type_name(n).map(TypeRef::named))
					.collect::<Result<Vec<_>, GqlError>>()?;
				let geo_union_name = format!("geometry_{}", g.join("_"));
				let mut geo_union = Union::new(&geo_union_name);
				for geo_type in geo_types {
					geo_union = geo_union.possible_type(geo_type.type_name())
				}
				types.push(Type::Union(geo_union));
				TypeRef::named(geo_union_name)
			}
		},
		(Kind::Geometry(g), true) => match g.len() {
			1 => {
				let name = g.into_iter().next().expect("checked that length is 1");
				TypeRef::named(format!("{}_input", geometry_kind_name_to_type_name(&name)?))
			}
			// TODO: more robust type checking on multiple geometries
			_ => TypeRef::named("Geometry_input"),
		},
		(Kind::Option(t), _) => {
			let mut non_op_ty = *t;
			while let Kind::Option(inner) = non_op_ty {
				non_op_ty = *inner;
			}
			kind_to_type(non_op_ty, types, is_input)?
		}
		(Kind::Either(ks), _) => {
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
		(Kind::Set(_, _), _) => return Err(schema_error("Kind::Set is not yet supported")),
		(Kind::Array(k, _), _) => TypeRef::List(Box::new(kind_to_type(*k, types, is_input)?)),
		(Kind::Function(_, _), _) => {
			return Err(schema_error("Kind::Function is not yet supported"))
		}
		(Kind::Range, _) => return Err(schema_error("Kind::Range is not yet supported")),
		// TODO(raphaeldarley): check if union is of literals and generate enum
		// generate custom scalar from other literals?
		(Kind::Literal(_), _) => return Err(schema_error("Kind::Literal is not yet supported")),
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
		// Kind::Geometry(_) => Err(resolver_error("Geometry is not yet supported")),
		Kind::Geometry(ref ts) => match &val {
			GqlValue::Object(map) => match map.get("type") {
				Some(t) => match t {
					GqlValue::String(acutal_t) => {
						let mut included = false;
						for ty in ts {
							if geometry_kind_name_to_type_name(ty)? == acutal_t {
								included = true;
								break;
							}
						}
						if included {
							extract_geometry(map)
								.map(SqlValue::Geometry)
								.ok_or_else(|| type_error(kind, val))
						} else {
							Err(type_error(kind, val))
						}
					}
					_ => Err(type_error(kind, val)),
				},
				None => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
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

fn extract_coord(arr: &[GqlValue]) -> Option<Coord> {
	match arr {
		[GqlValue::Number(y), GqlValue::Number(x)] => Some(Coord {
			x: x.as_f64()?,
			y: y.as_f64()?,
		}),
		_ => None,
	}
}

fn extract_coord_list(arr: &[GqlValue]) -> Option<Vec<Coord>> {
	arr.iter()
		.map(|c| match c {
			GqlValue::List(c) => extract_coord(c),
			_ => None,
		})
		.collect()
}

fn extract_coord_list_list(arr: &[GqlValue]) -> Option<Vec<Vec<Coord>>> {
	arr.iter()
		.map(|c| match c {
			GqlValue::List(c) => extract_coord_list(c),
			_ => None,
		})
		.collect()
}

fn extract_polygon(arr: &[GqlValue]) -> Option<Polygon> {
	let mut line_strings = extract_coord_list_list(arr)?.into_iter().map(LineString);
	let exterior = line_strings.next()?;
	let interior = line_strings.collect();
	Some(Polygon::new(exterior, interior))
}

fn extract_polygon_list(arr: &[GqlValue]) -> Option<Vec<Polygon>> {
	arr.iter()
		.map(|c| match c {
			GqlValue::List(c) => extract_polygon(c),
			_ => None,
		})
		.collect()
}

fn extract_geometry(map: &IndexMap<Name, GqlValue>) -> Option<Geometry> {
	let ty = match map.get("type") {
		Some(GqlValue::String(ty)) => Some(ty),
		_ => None,
	};

	let coordinates = match map.get("coordinates") {
		Some(GqlValue::List(cs)) => Some(cs.as_slice()),
		_ => None,
	};

	let geometries = match map.get("geometries") {
		Some(GqlValue::List(cs)) => Some(cs.as_slice()),
		_ => None,
	};

	match ty?.as_str() {
		"GeometryPoint" => Some(Geometry::Point(Point(extract_coord(coordinates?).unwrap()))),
		"GeometryLineString" => Some(Geometry::Line(LineString(extract_coord_list(coordinates?)?))),
		"GeometryPolygon" => Some(Geometry::Polygon(extract_polygon(coordinates?)?)),
		"GeometryMultiPoint" => Some(Geometry::MultiPoint(MultiPoint(
			extract_coord_list(&coordinates?)?.into_iter().map(Point).collect(),
		))),
		"GeometryMultiLineString" => Some(Geometry::MultiLine(MultiLineString(
			extract_coord_list_list(&coordinates?)?.into_iter().map(LineString).collect(),
		))),
		"GeometryMultiPolygon" => {
			Some(Geometry::MultiPolygon(MultiPolygon(extract_polygon_list(&coordinates?)?)))
		}
		"GeometryCollection" => Some(Geometry::Collection(
			geometries?
				.iter()
				.map(|g| match g {
					GqlValue::Object(inner_map) => extract_geometry(inner_map),
					_ => None,
				})
				.collect::<Option<_>>()?,
		)),
		_ => None,
	}
}
