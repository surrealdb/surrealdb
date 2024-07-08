use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::fmt::format;

use async_graphql::dynamic::TypeRef;
use async_graphql::dynamic::{Enum, Type};
use async_graphql::dynamic::{Field, Interface};
use async_graphql::dynamic::{FieldFuture, InterfaceField};
use async_graphql::dynamic::{InputObject, Object};
use async_graphql::dynamic::{InputValue, Schema};
use async_graphql::indexmap::{IndexMap, IndexSet};
use async_graphql::Name;
use async_graphql::Value as GqlValue;
use serde_json::Number;
use surrealdb::err::Error;
use surrealdb::sql;
use surrealdb::sql::statements::SelectStatement;
use surrealdb::sql::statements::UseStatement;
use surrealdb::sql::Expression;
use surrealdb::sql::{Cond, Fields, Start, TableType};
use surrealdb::sql::{Kind, Limit};
use surrealdb::sql::{Order, Table};
use surrealdb::sql::{Orders, Values};
use surrealdb::sql::{Statement, Thing};

use super::ext::IntoExt;
use crate::dbs::DB;
use crate::gql::utils::GqlValueUtils;
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
		InputValue::new("id", TypeRef::named_nn(TypeRef::STRING))
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

pub async fn get_schema() -> Result<Schema, Box<dyn std::error::Error>> {
	let kvs = DB.get().unwrap();
	let mut tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;
	let tbs = tx.all_tb("test", "test").await?;
	let mut query = Object::new("Query");
	let mut types: Vec<Type> = Vec::new();
	// TODO: remove hardcoded db and ns
	const DB_NAME: &str = "test";
	const NS_NAME: &str = "test";

	for tb in tbs.iter() {
		info!("Adding table: {}", tb.name);
		let tb_name = tb.name.to_string();
		let first_tb_name = tb_name.clone();
		let second_tb_name = tb_name.clone();

		let interface = match tb.kind {
			// TODO: re-enable relation interface
			// TableType::Relation(_) => "relation",
			_ => "record",
		};

		let table_orderable_name = format!("_orderable_{tb_name}");
		let mut table_orderable = Enum::new(&table_orderable_name).item("id");
		let table_order_name = format!("_order_{tb_name}");
		let table_order = InputObject::new(&table_order_name)
			.field(InputValue::new("asc", TypeRef::named(&table_orderable_name)))
			.field(InputValue::new("desc", TypeRef::named(&table_orderable_name)))
			.field(InputValue::new("then", TypeRef::named(&table_order_name)));

		let table_filter_name = format!("_filter_{tb_name}");
		let mut table_filter = InputObject::new(&table_filter_name);
		table_filter = table_filter.field(InputValue::new("id", TypeRef::named("_filter_id")));
		types.push(Type::InputObject(filter_id()));

		query = query.field(
			Field::new(
				tb.name.to_string(),
				TypeRef::named_nn_list_nn(tb.name.to_string()),
				move |ctx| {
					let tb_name = first_tb_name.clone();
					FieldFuture::new(async move {
						let kvs = DB.get().unwrap();

						let use_stmt = Statement::Use((NS_NAME, DB_NAME).intox());

						// -- debugging --
						// let args = ctx.args;
						// let map = args.as_index_map();
						// dbg!(map);

						let par_val = ctx.parent_value.as_value().unwrap().clone();
						dbg!(par_val);

						let inner = ctx.ctx.clone();
						let node = inner.path_node;
						dbg!(node);
						// ---------------

						let args = ctx.args.as_index_map();
						info!("args: {args:?}");
						let start =
							args.get("start").map(|v| v.as_i64()).flatten().map(|s| s.intox());

						let limit =
							args.get("limit").map(|v| v.as_i64()).flatten().map(|l| l.intox());

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
						info!("orders: {orders:?}");

						let cond = match filter {
							Some(f) => {
								let o = match f {
									GqlValue::Object(o) => o,
									f => {
										error!("Found filter {f}, which should be object and should have been rejected by async graphql.");
										return Err("Value in cond doesn't fit schema".into());
									}
								};

								let cond = cond_from_filter(o)?;

								Some(cond)
							}
							None => None,
						};

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

						info!("query ast: {ast:?}");

						let query = vec![use_stmt, ast].into();
						info!("query: {}", query);

						let res =
							kvs.process(query, &Default::default(), Default::default()).await?;
						// ast is constructed such that there will only be two responses the first of which is NONE
						let mut res_iter = res.into_iter();
						// this is none so can be disregarded
						let _ = res_iter.next();
						let res = res_iter.next().unwrap();
						let res = res.result?;

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

		query = query.field(
			Field::new(
				format!("_get_{}", tb.name),
				TypeRef::named(tb.name.to_string()),
				move |ctx| {
					let tb_name = second_tb_name.clone();
					FieldFuture::new(async move {
						let kvs = DB.get().unwrap();

						let args = ctx.args.as_index_map();
						// async-graphql should validate that this is present as it is non-null
						let id = args.get("id").map(GqlValueUtils::as_string).flatten().unwrap();
						let thing = match id.clone().try_into() {
							Ok(t) => t,
							Err(_) => Thing::from((tb_name, id)),
						};

						let use_stmt = Statement::Use(
							(Some(DB_NAME.to_string()), Some(NS_NAME.to_string())).intox(),
						);

						let ast = Statement::Select({
							let mut tmp = SelectStatement::default();
							tmp.what = vec![SqlValue::Thing(thing)].into();
							tmp.expr = Fields::all();
							tmp.only = true;
							tmp
						});

						let query = vec![use_stmt, ast].into();
						info!("query: {}", query);

						let res =
							kvs.process(query, &Default::default(), Default::default()).await?;
						// ast is constructed such that there will only be two responses the first of which is NONE
						let mut res_iter = res.into_iter();
						let _ = res_iter.next();
						let res = res_iter.next().unwrap();
						let res = res.result?;
						let out = sql_value_to_gql_value(res)
							.map_err(|_| "SQL to GQL translation failed")?;

						Ok(Some(out))
					})
				},
			)
			.argument(id_input!()),
		);

		let fds = tx.all_tb_fields(DB_NAME, NS_NAME, &tb.name.0).await?;

		let mut table_ty_obj = Object::new(tb.name.to_string())
			.field(Field::new("id", TypeRef::named_nn(TypeRef::ID), |ctx| {
				FieldFuture::new(async move {
					let record = ctx.parent_value.as_value().unwrap();
					let GqlValue::Object(record_map) = record else {
						todo!()
					};
					let id = record_map.get("id").unwrap();

					Ok(Some(id.to_owned()))
				})
			}))
			.implement(interface);

		for fd in fds.iter() {
			let fd_name = Name::new(fd.name.to_string());
			let fd_type = kind_to_type(fd.kind.clone());
			table_orderable = table_orderable.item(fd_name.to_string());
			let type_filter_name = format!("_filter_{}", unwrap_type(fd_type.clone()));

			let type_filter =
				Type::InputObject(filter_from_type(fd.kind.clone(), type_filter_name.clone()));
			println!("\n{type_filter:?}\n");
			types.push(type_filter);

			table_filter = table_filter
				.field(InputValue::new(fd.name.to_string(), TypeRef::named(type_filter_name)));

			table_ty_obj =
				table_ty_obj.field(Field::new(fd.name.to_string(), fd_type, move |ctx| {
					let fd_name = fd_name.clone();
					FieldFuture::new(async move {
						let record = ctx.parent_value.as_value().unwrap();
						let GqlValue::Object(record_map) = record else {
							todo!("got unexpected: {record:?}, processing field {fd_name}")
						};
						let val = record_map.get(&fd_name).unwrap();

						Ok(Some(val.to_owned()))
					})
				}));
		}

		types.push(Type::Object(table_ty_obj));
		types.push(table_order.into());
		types.push(Type::Enum(table_orderable));
		println!("\n\n\n{table_filter:?}");
		types.push(Type::InputObject(table_filter));
	}

	//TODO: This is broken
	query = query.field(
		Field::new("_get_record", TypeRef::named("record"), |ctx| {
			FieldFuture::new(async move {
				let kvs = DB.get().unwrap();

				let args = ctx.args.as_index_map();
				// async-graphql should validate that this is present as it is non-null
				let id = args.get("id").map(GqlValueUtils::as_string).flatten().unwrap();

				let use_stmt = Statement::Use((DB_NAME, NS_NAME).intox());

				let ast = Statement::Select({
					let mut tmp = SelectStatement::default();
					tmp.what = vec![SqlValue::Thing(id.try_into().unwrap())].into();
					tmp.expr = Fields::all();
					tmp.only = true;
					tmp
				});

				let query = vec![use_stmt, ast].into();
				info!("query: {}", query);

				let res = kvs.process(query, &Default::default(), Default::default()).await?;
				// ast is constructed such that there will only be two responses the first of which is NONE
				let mut res_iter = res.into_iter();
				let _ = res_iter.next();
				let res = res_iter.next().unwrap();
				let res = res.result?;
				let out =
					sql_value_to_gql_value(res).map_err(|_| "SQL to GQL translation failed")?;

				Ok(Some(out))
			})
		})
		.argument(id_input!()),
	);
	info!("current Query: {:?}", query);

	let mut schema = Schema::build("Query", None, None).register(query);
	for ty in types {
		println!("adding type: {ty:?}");
		schema = schema.register(ty);
	}

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

	Ok(schema.finish().unwrap())
}

fn sql_value_to_gql_value(v: SqlValue) -> Result<GqlValue, ()> {
	let out = match v {
		SqlValue::None => GqlValue::Null,
		SqlValue::Null => GqlValue::Null,
		SqlValue::Bool(b) => GqlValue::Boolean(b),
		SqlValue::Number(n) => match n {
			surrealdb::sql::Number::Int(i) => GqlValue::Number(i.into()),
			surrealdb::sql::Number::Float(f) => GqlValue::Number(Number::from_f64(f).ok_or(())?),
			surrealdb::sql::Number::Decimal(_) => todo!("surrealdb::sql::Number::Decimal(_) "),
			_ => todo!(),
		},
		SqlValue::Strand(s) => GqlValue::String(s.0),
		SqlValue::Duration(_) => todo!("SqlValue::Duration(_) "),
		SqlValue::Datetime(_) => todo!("SqlValue::Datetime(_) "),
		SqlValue::Uuid(_) => todo!("SqlValue::Uuid(_) "),
		SqlValue::Array(_) => todo!("SqlValue::Array(_) "),
		SqlValue::Object(o) => GqlValue::Object(
			o.0.into_iter()
				.map(|(k, v)| (Name::new(k), sql_value_to_gql_value(v).unwrap()))
				.collect(),
		),
		SqlValue::Geometry(_) => todo!("SqlValue::Geometry(_) "),
		SqlValue::Bytes(_) => todo!("SqlValue::Bytes(_) "),
		SqlValue::Thing(t) => GqlValue::String(t.to_string()),
		SqlValue::Param(_) => todo!("SqlValue::Param(_) "),
		SqlValue::Idiom(_) => todo!("SqlValue::Idiom(_) "),
		SqlValue::Table(_) => todo!("SqlValue::Table(_) "),
		SqlValue::Mock(_) => todo!("SqlValue::Mock(_) "),
		SqlValue::Regex(_) => todo!("SqlValue::Regex(_) "),
		SqlValue::Cast(_) => todo!("SqlValue::Cast(_) "),
		SqlValue::Block(_) => todo!("SqlValue::Block(_) "),
		SqlValue::Range(_) => todo!("SqlValue::Range(_) "),
		SqlValue::Edges(_) => todo!("SqlValue::Edges(_) "),
		SqlValue::Future(_) => todo!("SqlValue::Future(_) "),
		SqlValue::Constant(_) => todo!("SqlValue::Constant(_) "),
		SqlValue::Function(_) => todo!("SqlValue::Function(_) "),
		SqlValue::Subquery(_) => todo!("SqlValue::Subquery(_) "),
		SqlValue::Expression(_) => todo!("SqlValue::Expression(_) "),
		SqlValue::Query(_) => todo!("SqlValue::Query(_) "),
		SqlValue::Model(_) => todo!("SqlValue::Model(_) "),
		_ => todo!(),
	};
	Ok(out)
}

fn kind_to_type(kind: Option<Kind>) -> TypeRef {
	let kind = kind.unwrap();
	let (optional, match_kind) = match kind {
		Kind::Option(op_ty) => (true, *op_ty),
		_ => (false, kind),
	};
	let out_ty = match match_kind {
		Kind::Any => todo!("Kind::Any "),
		Kind::Null => todo!("Kind::Null "),
		Kind::Bool => TypeRef::named(TypeRef::BOOLEAN),
		Kind::Bytes => todo!("Kind::Bytes "),
		Kind::Datetime => todo!("Kind::Datetime "),
		Kind::Decimal => todo!("Kind::Decimal "),
		Kind::Duration => todo!("Kind::Duration "),
		Kind::Float => todo!("Kind::Float "),
		Kind::Int => TypeRef::named(TypeRef::INT),
		Kind::Number => todo!("Kind::Number "),
		Kind::Object => todo!("Kind::Object "),
		Kind::Point => todo!("Kind::Point "),
		Kind::String => TypeRef::named(TypeRef::STRING),
		Kind::Uuid => todo!("Kind::Uuid "),
		Kind::Record(mut r) => match r.len() {
			1 => TypeRef::named(r.pop().unwrap().0),
			_ => todo!("dynamic unions for multiple records"),
		},
		Kind::Geometry(_) => todo!("Kind::Geometry(_) "),
		Kind::Option(_) => todo!("Kind::Option(_) "),
		Kind::Either(_) => todo!("Kind::Either(_) "),
		Kind::Set(_, _) => todo!("Kind::Set(_, _) "),
		Kind::Array(k, _) => TypeRef::List(Box::new(kind_to_type(Some(*k)))),
		_ => todo!(),
	};

	match optional {
		true => out_ty,
		false => TypeRef::NonNull(Box::new(out_ty)),
	}
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
fn filter_from_type(kind: Option<Kind>, filter_name: String) -> InputObject {
	let ty = kind_to_type(kind.clone());
	let ty = unwrap_type(ty);

	let mut filter = InputObject::new(filter_name);
	filter_impl!(filter, ty, "eq");
	filter_impl!(filter, ty, "ne");

	match kind.unwrap() {
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
	filter
}

fn unwrap_type(ty: TypeRef) -> TypeRef {
	match ty {
		TypeRef::NonNull(t) => unwrap_type(*t),
		_ => ty,
	}
}

fn cond_from_filter(filter: &IndexMap<Name, GqlValue>) -> Result<Cond, Error> {
	val_from_filter(filter).map(IntoExt::intox)
}

fn val_from_filter(filter: &IndexMap<Name, GqlValue>) -> Result<SqlValue, Error> {
	// for (op_name, val) in filter.iter() {
	// 	let op = match op_name.as_str() {
	// 		"eq" => sql::Operator::Equal,
	// 		op => return Err(Error::Thrown(format!("Unsupported op: {op}"))),
	// 	};

	// 	let val =
	// 	break;
	// }
	if filter.len() != 1 {
		return Err(Error::Thrown(format!("Table Filter must have one item")));
	}

	let (k, v) = filter.iter().next().unwrap();

	let cond = match k.as_str().to_lowercase().as_str() {
		"or" => aggregate(v, AggregateOp::Or),
		"and" => aggregate(v, AggregateOp::And),
		"not" => negate(v),
		_ => binop(k.as_str(), v),
	};

	cond
}

fn parse_op(name: impl AsRef<str>) -> Result<sql::Operator, Error> {
	match name.as_ref() {
		"eq" => Ok(sql::Operator::Equal),
		"ne" => Ok(sql::Operator::NotEqual),
		op => return Err(Error::Thrown(format!("Unsupported op: {op}"))),
	}
}

fn negate(filter: &GqlValue) -> Result<SqlValue, Error> {
	let obj = filter.as_object().ok_or(Error::Thrown("Value of NOT must be object".to_string()))?;
	let inner_cond = val_from_filter(obj)?;

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

fn aggregate(filter: &GqlValue, op: AggregateOp) -> Result<SqlValue, Error> {
	let op_str = match op {
		AggregateOp::And => "AND",
		AggregateOp::Or => "OR",
	};
	let op = match op {
		AggregateOp::And => sql::Operator::And,
		AggregateOp::Or => sql::Operator::Or,
	};
	let list =
		filter.as_list().ok_or(Error::Thrown(format!("Value of {op_str} should be a list")))?;
	let filter_arr = list
		.iter()
		.map(|v| v.as_object().map(val_from_filter))
		.collect::<Option<Result<Vec<SqlValue>, Error>>>()
		.ok_or(Error::Thrown(format!("List of {op_str} should contain objects")))??;

	let mut iter = filter_arr.into_iter();

	let mut cond = iter
		.next()
		.ok_or(Error::Thrown(format!("List of {op_str} should contain at least one object")))?;

	for clause in iter {
		cond = Expression::Binary {
			l: clause,
			o: op.clone(),
			r: cond,
		}
		.into();
	}

	todo!()
}

fn binop(field_name: &str, val: &GqlValue) -> Result<SqlValue, Error> {
	let obj = val.as_object().ok_or(Error::Thrown("Field filter should be object".to_string()))?;

	if obj.len() != 1 {
		return Err(Error::Thrown(format!("Field Filter must have one item")));
	}

	let lhs = sql::Value::Idiom(field_name.intox());

	let (k, v) = obj.iter().next().unwrap();
	let op = parse_op(k)?;

	let rhs = gql_value_to_sql_value(v);

	let expr = sql::Expression::Binary {
		l: lhs,
		o: op,
		r: rhs,
	};

	Ok(expr.into())
}

fn gql_value_to_sql_value(val: &GqlValue) -> SqlValue {
	match val {
		GqlValue::Null => SqlValue::Null,
		GqlValue::Number(n) => {
			if let Some(i) = n.as_i64() {
				SqlValue::from(i)
			} else if let Some(f) = n.as_f64() {
				SqlValue::from(f)
			} else if let Some(u) = n.as_u64() {
				SqlValue::from(u)
			} else {
				unreachable!("a Number can only be i64, u64 or f64");
			}
		}
		GqlValue::String(s) => SqlValue::from(s.as_str()),
		GqlValue::Boolean(b) => SqlValue::Bool(*b),
		GqlValue::Binary(b) => SqlValue::Bytes(b.to_vec().into()),
		GqlValue::Enum(n) => SqlValue::Strand(n.as_str().into()),
		GqlValue::List(a) => {
			SqlValue::Array(a.iter().map(gql_value_to_sql_value).collect::<Vec<SqlValue>>().into())
		}
		GqlValue::Object(o) => SqlValue::Object(
			o.iter()
				.map(|(k, v)| (k.to_string(), gql_value_to_sql_value(v)))
				.collect::<BTreeMap<String, SqlValue>>()
				.into(),
		),
	}
}
