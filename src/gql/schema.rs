use std::collections::BTreeMap;
use std::sync::Arc;

use async_graphql::dynamic::{Enum, Type, Union};
use async_graphql::dynamic::{Field, Interface};
use async_graphql::dynamic::{FieldFuture, InterfaceField};
use async_graphql::dynamic::{InputObject, Object};
use async_graphql::dynamic::{InputValue, Schema};
use async_graphql::dynamic::{Scalar, TypeRef};
use async_graphql::indexmap::IndexMap;
use async_graphql::Name;
use async_graphql::Value as GqlValue;
use chrono::DateTime;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde_json::Number;
use surrealdb::dbs::Session;
use surrealdb::kvs::Datastore;
use surrealdb::sql;
use surrealdb::sql::statements::{DefineFieldStatement, SelectStatement};
use surrealdb::sql::Expression;
use surrealdb::sql::Kind;
use surrealdb::sql::{Cond, Fields};
use surrealdb::sql::{Statement, Thing};
use surrealdb::syn;

use super::error::{resolver_error, GqlError};
use super::ext::IntoExt;
use super::ext::ValidatorExt;
use crate::gql::error::{internal_error, schema_error, type_error};
use crate::gql::utils::GqlValueUtils;
use crate::mac;
use surrealdb::kvs::LockType;
use surrealdb::kvs::TransactionType;
use surrealdb::sql::Value as SqlValue;

macro_rules! limit_input {
	() => {
		InputValue::new("limit", TypeRef::named(TypeRef::INT))
	};
}

macro_rules! start_input {
	() => {
		InputValue::new("start", TypeRef::named(TypeRef::INT))
	};
}

macro_rules! id_input {
	() => {
		InputValue::new("id", TypeRef::named_nn(TypeRef::ID))
	};
}

macro_rules! order {
	(asc, $field:expr) => {{
		let mut tmp = ::surrealdb::sql::Order::default();
		tmp.order = $field.into();
		tmp.direction = true;
		tmp
	}};
	(desc, $field:expr) => {{
		let mut tmp = ::surrealdb::sql::Order::default();
		tmp.order = $field.into();
		tmp
	}};
}

pub async fn generate_schema(
	datastore: &Arc<Datastore>,
	session: &Session,
) -> Result<Schema, GqlError> {
	let kvs = datastore.as_ref();
	let tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;
	let ns = session.ns.as_ref().expect("missing ns should have been caught");
	let db = session.db.as_ref().expect("missing db should have been caught");
	let tbs = tx.all_tb(&ns, &db).await?;
	let mut query = Object::new("Query");
	let mut types: Vec<Type> = Vec::new();

	info!(ns, db, ?tbs, "generating schema");

	if tbs.len() == 0 {
		return Err(schema_error("no tables found in database"));
	}

	for tb in tbs.iter() {
		trace!("Adding table: {}", tb.name);
		let tb_name = tb.name.to_string();
		let first_tb_name = tb_name.clone();
		let second_tb_name = tb_name.clone();

		let interface = "record";

		let table_orderable_name = format!("_orderable_{tb_name}");
		let mut table_orderable = Enum::new(&table_orderable_name).item("id");
		let table_order_name = format!("_order_{tb_name}");
		let table_order = InputObject::new(&table_order_name)
			.field(InputValue::new("asc", TypeRef::named(&table_orderable_name)))
			.field(InputValue::new("desc", TypeRef::named(&table_orderable_name)))
			.field(InputValue::new("then", TypeRef::named(&table_order_name)));

		let table_filter_name = format!("_filter_{tb_name}");
		let mut table_filter = InputObject::new(&table_filter_name);
		table_filter = table_filter
			.field(InputValue::new("id", TypeRef::named("_filter_id")))
			.field(InputValue::new("and", TypeRef::named_nn_list(&table_filter_name)))
			.field(InputValue::new("or", TypeRef::named_nn_list(&table_filter_name)))
			.field(InputValue::new("not", TypeRef::named(&table_filter_name)));
		types.push(Type::InputObject(filter_id()));

		let sess1 = session.to_owned();
		let fds = tx.all_tb_fields(&ns, &db, &tb.name.0).await?;
		let fds1 = fds.clone();
		let kvs1 = datastore.clone();

		query = query.field(
			Field::new(
				tb.name.to_string(),
				TypeRef::named_nn_list_nn(tb.name.to_string()),
				move |ctx| {
					let tb_name = first_tb_name.clone();
					let sess1 = sess1.clone();
					let fds1 = fds1.clone();
					let kvs1 = kvs1.clone();
					FieldFuture::new(async move {
						let kvs = kvs1.as_ref();

						let args = ctx.args.as_index_map();
						trace!("received request with args: {args:?}");

						let start = args.get("start").and_then(|v| v.as_i64()).map(|s| s.intox());

						let limit = args.get("limit").and_then(|v| v.as_i64()).map(|l| l.intox());

						let order = args.get("order");

						let filter = args.get("filter");

						let orders = match order {
							Some(GqlValue::Object(o)) => {
								let mut orders = vec![];
								let mut current = o;
								loop {
									let asc = current.get("asc");
									let desc = current.get("desc");
									match (asc, desc) {
										(Some(_), Some(_)) => {
											return Err("Found both asc and desc in order".into());
										}
										(Some(GqlValue::Enum(a)), None) => {
											orders.push(order!(asc, a.as_str()))
										}
										(None, Some(GqlValue::Enum(d))) => {
											orders.push(order!(desc, d.as_str()))
										}
										(_, _) => {
											break;
										}
									}
									if let Some(GqlValue::Object(next)) = current.get("then") {
										current = next;
									} else {
										break;
									}
								}
								Some(orders)
							}
							_ => None,
						};
						trace!("parsed orders: {orders:?}");

						let cond = match filter {
							Some(f) => {
								let o = match f {
									GqlValue::Object(o) => o,
									f => {
										error!("Found filter {f}, which should be object and should have been rejected by async graphql.");
										return Err("Value in cond doesn't fit schema".into());
									}
								};

								let cond = cond_from_filter(o, &fds1)?;

								Some(cond)
							}
							None => None,
						};

						trace!("parsed filter: {cond:?}");

						let ast = Statement::Select({
							let mut tmp = SelectStatement::default();
							tmp.what = vec![SqlValue::Table(tb_name.intox())].into();
							tmp.expr = Fields::all();
							tmp.start = start;
							tmp.limit = limit;
							tmp.order = orders.map(IntoExt::intox);
							tmp.cond = cond;

							tmp
						});

						trace!("generated query ast: {ast:?}");

						let query = ast.into();
						trace!("generated query: {}", query);

						let res = kvs.process(query, &sess1, Default::default()).await?;
						debug_assert_eq!(res.len(), 1);
						let res = res
							.into_iter()
							.next()
							.expect("response vector should have exactly one value")
							.result?;

						let res_vec =
							match res {
								SqlValue::Array(a) => a,
								v => {
									error!("Found top level value, in result which should be array: {v:?}");
									return Err("Internal Error".into());
								}
							};

						let out = res_vec
							.0
							.into_iter()
							.map(|v| sql_value_to_gql_value(v).unwrap())
							.collect();

						Ok(Some(GqlValue::List(out)))
					})
				},
			)
			.argument(limit_input!())
			.argument(start_input!())
			.argument(InputValue::new("order", TypeRef::named(&table_order_name)))
			.argument(InputValue::new("filter", TypeRef::named(&table_filter_name))),
		);

		let sess2 = session.to_owned();
		let kvs2 = datastore.to_owned();
		query = query.field(
			Field::new(
				format!("_get_{}", tb.name),
				TypeRef::named(tb.name.to_string()),
				move |ctx| {
					let tb_name = second_tb_name.clone();
					let kvs2 = kvs2.clone();
					FieldFuture::new({
						let sess2 = sess2.clone();
						async move {
							let kvs = kvs2.as_ref();

							let args = ctx.args.as_index_map();
							// async-graphql should validate that this is present as it is non-null
							let id = match args.get("id").and_then(GqlValueUtils::as_string) {
								Some(i) => i,
								None => {
									return Err(internal_error(
										"Schema validation failed: No id found in _get_",
									)
									.into());
								}
							};
							let thing = match id.clone().try_into() {
								Ok(t) => t,
								Err(_) => Thing::from((tb_name, id)),
							};

							let ast = Statement::Select({
								let mut tmp = SelectStatement::default();
								tmp.what = vec![SqlValue::Thing(thing)].into();
								tmp.expr = Fields::all();
								tmp.only = true;
								tmp
							});

							// let query = vec![use_stmt, ast].into();
							let query = ast.into();
							trace!("generated query: {}", query);

							let res = kvs.process(query, &sess2, Default::default()).await?;
							debug_assert_eq!(res.len(), 1);
							let res = res
								.into_iter()
								.next()
								.expect("response vector should have exactly one value")
								.result?;
							match res {
								obj @ SqlValue::Object(_) => {
									let out = sql_value_to_gql_value(obj)
										.map_err(|_| "SQL to GQL translation failed")?;

									Ok(Some(out))
								}
								_ => Ok(None),
							}
						}
					})
				},
			)
			.argument(id_input!()),
		);

		let mut table_ty_obj = Object::new(tb.name.to_string())
			.field(Field::new("id", TypeRef::named_nn(TypeRef::ID), |ctx| {
				FieldFuture::new(async move {
					let record = ctx.parent_value.as_value().unwrap();
					let GqlValue::Object(record_map) = record else {
						return Err(internal_error(format!(
							"record should be object, but found: {record:?}"
						))
						.into());
					};
					let id = record_map.get("id").unwrap();

					Ok(Some(id.to_owned()))
				})
			}))
			.implement(interface);

		for fd in fds.iter() {
			let Some(ref kind) = fd.kind else {
				continue;
			};
			let fd_name = Name::new(fd.name.to_string());
			let fd_type = kind_to_type(kind.clone(), &mut types)?;
			table_orderable = table_orderable.item(fd_name.to_string());
			let type_filter_name = format!("_filter_{}", unwrap_type(fd_type.clone()));

			let type_filter = Type::InputObject(filter_from_type(
				kind.clone(),
				type_filter_name.clone(),
				&mut types,
			)?);
			trace!("\n{type_filter:?}\n");
			types.push(type_filter);

			table_filter = table_filter
				.field(InputValue::new(fd.name.to_string(), TypeRef::named(type_filter_name)));

			table_ty_obj =
				table_ty_obj.field(Field::new(fd.name.to_string(), fd_type, move |ctx| {
					let fd_name = fd_name.clone();
					FieldFuture::new(async move {
						let record = ctx.parent_value.as_value().unwrap();
						let GqlValue::Object(record_map) = record else {
							return Err(internal_error(format!(
								"record should be an object, but found: {record:?}"
							))
							.into());
						};
						let val = record_map.get(&fd_name).unwrap();

						Ok(Some(val.to_owned()))
					})
				}));
		}

		types.push(Type::Object(table_ty_obj));
		types.push(table_order.into());
		types.push(Type::Enum(table_orderable));
		types.push(Type::InputObject(table_filter));
	}

	let sess3 = session.to_owned();
	let kvs3 = datastore.to_owned();
	query = query.field(
		Field::new("_get", TypeRef::named("object"), move |ctx| {
			let kvs3 = kvs3.clone();
			FieldFuture::new({
				let sess3 = sess3.clone();
				async move {
					let kvs = kvs3.as_ref();

					let args = ctx.args.as_index_map();
					// async-graphql should validate that this is present as it is non-null
					let id = match args.get("id").and_then(GqlValueUtils::as_string) {
						Some(i) => i,
						None => {
							return Err(internal_error(
								"Schema validation failed: No id found in _get",
							)
							.into());
						}
					};

					let thing = match id.clone().try_into() {
						Ok(t) => t,
						Err(_) => return Err(resolver_error(format!("invalid id: {id}")).into()),
					};

					let ast = Statement::Select({
						let mut tmp = SelectStatement::default();
						tmp.what = vec![SqlValue::Thing(thing)].into();
						tmp.expr = Fields::all();
						tmp.only = true;
						tmp
					});

					// let query = vec![use_stmt, ast].into();
					let query = ast.into();
					trace!("generated query: {}", query);

					let res = kvs.process(query, &sess3, Default::default()).await?;
					debug_assert_eq!(res.len(), 1);
					let res = res
						.into_iter()
						.next()
						.expect("response vector should have exactly one value")
						.result?;
					match res {
						obj @ SqlValue::Object(_) => {
							let out = sql_value_to_gql_value(obj)
								.map_err(|_| "SQL to GQL translation failed")?;

							Ok(Some(out))
						}
						_ => Ok(None),
					}
					// let out: Result<Option<GqlValue>, async_graphql::Error> = todo!();
					// out
				}
			})
		})
		.argument(id_input!()),
	);

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
		"a string encoded uuid",
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

fn sql_value_to_gql_value(v: SqlValue) -> Result<GqlValue, GqlError> {
	let out = match v {
		SqlValue::None => GqlValue::Null,
		SqlValue::Null => GqlValue::Null,
		SqlValue::Bool(b) => GqlValue::Boolean(b),
		SqlValue::Number(n) => match n {
			surrealdb::sql::Number::Int(i) => GqlValue::Number(i.into()),
			surrealdb::sql::Number::Float(f) => GqlValue::Number(
				Number::from_f64(f)
					.ok_or(resolver_error("unimplemented: graceful NaN and Inf handling"))?,
			),
			num @ surrealdb::sql::Number::Decimal(_) => GqlValue::String(num.to_string()),
			n => {
				return Err(internal_error(format!("found unsupported number type: {n:?}")));
			}
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

fn kind_to_type(kind: Kind, types: &mut Vec<Type>) -> Result<TypeRef, GqlError> {
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
		// uuid type is always added
		Kind::Uuid => TypeRef::named("uuid"),
		Kind::Record(mut r) => match r.len() {
			// Table types should be added elsewhere
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
		Kind::Either(ts) => {
			let pos_names: Result<Vec<TypeRef>, GqlError> =
				ts.into_iter().map(|k| kind_to_type(k, types)).collect();
			let pos_names: Vec<String> = pos_names?.into_iter().map(|tr| tr.to_string()).collect();
			let ty_name = pos_names.join("_or_");

			let mut tmp_union = Union::new(ty_name.clone());
			for n in pos_names {
				tmp_union = tmp_union.possible_type(n);
			}

			types.push(Type::Union(tmp_union));
			TypeRef::named(ty_name)
		}
		Kind::Set(_, _) => return Err(schema_error("Kind::Set is not yet supported")),
		Kind::Array(k, _) => TypeRef::List(Box::new(kind_to_type(*k, types)?)),
		k => return Err(internal_error(format!("found unkown kind: {k:?}"))),
	};

	let out = match optional {
		true => out_ty,
		false => TypeRef::NonNull(Box::new(out_ty)),
	};
	Ok(out)
}

macro_rules! filter_impl {
	($filter:ident, $ty:ident, $name:expr) => {
		$filter = $filter.field(InputValue::new($name, $ty.clone()));
	};
}

fn filter_id() -> InputObject {
	let mut filter = InputObject::new("_filter_id");
	let ty = TypeRef::named(TypeRef::ID);
	filter_impl!(filter, ty, "eq");
	filter_impl!(filter, ty, "ne");
	filter
}
fn filter_from_type(
	kind: Kind,
	filter_name: String,
	types: &mut Vec<Type>,
) -> Result<InputObject, GqlError> {
	let ty = kind_to_type(kind.clone(), types)?;
	let ty = unwrap_type(ty);

	let mut filter = InputObject::new(filter_name);
	filter_impl!(filter, ty, "eq");
	filter_impl!(filter, ty, "ne");

	match kind {
		Kind::Any => {}
		Kind::Null => {}
		Kind::Bool => {}
		Kind::Bytes => {}
		Kind::Datetime => {}
		Kind::Decimal => {}
		Kind::Duration => {}
		Kind::Float => {}
		Kind::Int => {}
		Kind::Number => {}
		Kind::Object => {}
		Kind::Point => {}
		Kind::String => {}
		Kind::Uuid => {}
		Kind::Record(_) => {}
		Kind::Geometry(_) => {}
		Kind::Option(_) => {}
		Kind::Either(_) => {}
		Kind::Set(_, _) => {}
		Kind::Array(_, _) => {}
		_ => {}
	};
	Ok(filter)
}

fn unwrap_type(ty: TypeRef) -> TypeRef {
	match ty {
		TypeRef::NonNull(t) => unwrap_type(*t),
		_ => ty,
	}
}

fn cond_from_filter(
	filter: &IndexMap<Name, GqlValue>,
	fds: &[DefineFieldStatement],
) -> Result<Cond, GqlError> {
	val_from_filter(filter, fds).map(IntoExt::intox)
}

fn val_from_filter(
	filter: &IndexMap<Name, GqlValue>,
	fds: &[DefineFieldStatement],
) -> Result<SqlValue, GqlError> {
	if filter.len() != 1 {
		return Err(resolver_error("Table Filter must have one item"));
	}

	let (k, v) = filter.iter().next().unwrap();

	let cond = match k.as_str().to_lowercase().as_str() {
		"or" => aggregate(v, AggregateOp::Or, fds),
		"and" => aggregate(v, AggregateOp::And, fds),
		"not" => negate(v, fds),
		_ => binop(k.as_str(), v, fds),
	};

	cond
}

fn parse_op(name: impl AsRef<str>) -> Result<sql::Operator, GqlError> {
	match name.as_ref() {
		"eq" => Ok(sql::Operator::Equal),
		"ne" => Ok(sql::Operator::NotEqual),
		op => Err(resolver_error(format!("Unsupported op: {op}"))),
	}
}

fn negate(filter: &GqlValue, fds: &[DefineFieldStatement]) -> Result<SqlValue, GqlError> {
	let obj = filter.as_object().ok_or(resolver_error("Value of NOT must be object"))?;
	let inner_cond = val_from_filter(obj, fds)?;

	Ok(Expression::Unary {
		o: sql::Operator::Not,
		v: inner_cond,
	}
	.into())
}

enum AggregateOp {
	And,
	Or,
}

fn aggregate(
	filter: &GqlValue,
	op: AggregateOp,
	fds: &[DefineFieldStatement],
) -> Result<SqlValue, GqlError> {
	let op_str = match op {
		AggregateOp::And => "AND",
		AggregateOp::Or => "OR",
	};
	let op = match op {
		AggregateOp::And => sql::Operator::And,
		AggregateOp::Or => sql::Operator::Or,
	};
	let list =
		filter.as_list().ok_or(resolver_error(format!("Value of {op_str} should be a list")))?;
	let filter_arr = list
		.iter()
		.map(|v| v.as_object().map(|o| val_from_filter(o, fds)))
		.collect::<Option<Result<Vec<SqlValue>, GqlError>>>()
		.ok_or(resolver_error(format!("List of {op_str} should contain objects")))??;

	let mut iter = filter_arr.into_iter();

	let mut cond = iter
		.next()
		.ok_or(resolver_error(format!("List of {op_str} should contain at least one object")))?;

	for clause in iter {
		cond = Expression::Binary {
			l: clause,
			o: op.clone(),
			r: cond,
		}
		.into();
	}

	Ok(cond)
}

fn binop(
	field_name: &str,
	val: &GqlValue,
	fds: &[DefineFieldStatement],
) -> Result<SqlValue, GqlError> {
	let obj = val.as_object().ok_or(resolver_error("Field filter should be object"))?;

	let Some(fd) = fds.iter().find(|fd| fd.name.to_string() == field_name) else {
		return Err(resolver_error(format!("Field `{field_name}` not found")));
	};

	if obj.len() != 1 {
		return Err(resolver_error("Field Filter must have one item"));
	}

	let lhs = sql::Value::Idiom(field_name.intox());

	let (k, v) = obj.iter().next().unwrap();
	let op = parse_op(k)?;

	let rhs = gql_to_sql_kind(v, fd.kind.clone().unwrap_or_default())?;

	let expr = sql::Expression::Binary {
		l: lhs,
		o: op,
		r: rhs,
	};

	Ok(expr.into())
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

fn gql_to_sql_kind(val: &GqlValue, kind: Kind) -> Result<SqlValue, GqlError> {
	use surrealdb::syn;
	match kind {
		Kind::Any => match val {
			GqlValue::String(s) => {
				use Kind::*;
				// aren't parsed by syn
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
			GqlValue::String(s) => match syn::datetime_raw(s) {
				Ok(dt) => Ok(dt.into()),
				Err(_) => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Decimal => match val {
			GqlValue::Number(n) => {
				if let Some(int) = n.as_i64() {
					Ok(SqlValue::Number(sql::Number::Decimal(int.into())))
				} else if let Some(d) = n.as_f64().map(Decimal::from_f64).flatten() {
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
					_ => Err(type_error(kind, val)),
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
					_ => Err(type_error(kind, val)),
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
					_ => Err(type_error(kind, val)),
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
				Ok(SqlValue::Number(n)) => Ok(SqlValue::Number(n.clone())),
				_ => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Object => {
			error!(?val, "validating object");
			match val {
				GqlValue::Object(o) => {
					let out: Result<BTreeMap<String, SqlValue>, GqlError> = o
						.iter()
						.map(|(k, v)| {
							gql_to_sql_kind(v, Kind::Any).map(|sqlv| (k.to_string(), sqlv))
						})
						.collect();
					let out = out?;
					// let mut out = BTreeMap::new();
					// for (k, v) in o {
					// 	match gql_to_sql_kind(v, Kind::Any).map(|sqlv| (k.to_string(), sqlv)) {
					// 		Ok((name, val)) => {
					// 			out.insert(name, val);
					// 		}
					// 		Err(e) => {
					// 			error!(?e)
					// 		}
					// 	};
					// }

					Ok(SqlValue::Object(out.into()))
				}
				GqlValue::String(s) => match syn::value_legacy_strand(s.as_str()) {
					Ok(obj @ SqlValue::Object(_)) => Ok(obj),
					_ => Err(type_error(kind, val)),
				},
				_ => Err(type_error(kind, val)),
			}
		}
		// TODO: add geometry
		Kind::Point => Err(resolver_error("Geometry is not yet supported")),
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
					if ks.iter().find(|k| matches!(k, Kind::Option(_))).is_some() {
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
					// try parsing first
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
				GqlValue::Binary(_) => todo!(),
				GqlValue::Enum(n) => {
					either_try_kind!(ks, &GqlValue::String(n.to_string()), Kind::String);
					Err(type_error(kind, val))
				}
				list @ GqlValue::List(_) => {
					either_try_kind!(ks, list, Kind::Array);
					Err(type_error(kind, val))
				}
				GqlValue::Object(_) => todo!(),
			}
		}
		// TODO: figure out how to support sets
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
		k => Err(internal_error(format!("unknown kind: {k:?}"))),
	}
}
