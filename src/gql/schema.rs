use std::collections::BTreeSet;
use std::collections::HashSet;

use async_graphql::dynamic::Type;
use async_graphql::dynamic::TypeRef;
use async_graphql::dynamic::{Field, Interface};
use async_graphql::dynamic::{FieldFuture, InterfaceField};
use async_graphql::dynamic::{InputObject, Object};
use async_graphql::dynamic::{InputValue, Schema};
use async_graphql::Name;
use async_graphql::Value as GqlValue;
use serde_json::Number;
use surrealdb::sql::statements::SelectStatement;
use surrealdb::sql::statements::UseStatement;
use surrealdb::sql::Table;
use surrealdb::sql::Values;
use surrealdb::sql::{Fields, Start};
use surrealdb::sql::{Kind, Limit};
use surrealdb::sql::{Statement, Thing};

use crate::dbs::DB;
use crate::gql::utils::GqlValueUtils;
use surrealdb::kvs::LockType;
use surrealdb::kvs::TransactionType;
use surrealdb::sql::Value as SqlValue;

pub async fn get_schema() -> Result<Schema, Box<dyn std::error::Error>> {
	let kvs = DB.get().unwrap();
	let mut tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;
	let tbs = tx.all_tb("test", "test").await?;
	let mut query = Object::new("Query");
	let mut types: Vec<Type> = Vec::new();
	// remove hardcoded db and ns
	const DB_NAME: &str = "test";
	const NS_NAME: &str = "test";

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

	for tb in tbs.iter() {
		info!("Adding table: {}", tb.name);
		let tb_name = tb.name.to_string();
		let first_tb_name = tb_name.clone();
		query = query.field(
			Field::new(
				tb.name.to_string(),
				TypeRef::named_nn_list_nn(tb.name.to_string()),
				move |ctx| {
					let tb_name = first_tb_name.clone();
					FieldFuture::new(async move {
						let kvs = DB.get().unwrap();

						let use_stmt = Statement::Use(UseStatement {
							db: Some(DB_NAME.to_string()),
							ns: Some(NS_NAME.to_string()),
						});

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
						let start = args
							.get("start")
							.map(|v| v.as_i64())
							.flatten()
							.map(|s| Start(SqlValue::from(s)));
						let limit = args
							.get("limit")
							.map(|v| v.as_i64())
							.flatten()
							.map(|l| Limit(SqlValue::from(l)));

						let ast = Statement::Select(SelectStatement {
							what: Values(vec![SqlValue::Table(Table(tb_name))]),
							expr: Fields::all(),
							start,
							limit,
							..Default::default()
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
						let SqlValue::Array(res_vec) = res.clone() else {
							panic!("top level value in array should be an array: {:?}", res)
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
			.argument(start_input!()),
		);

		query = query.field(
			Field::new(
				format!("_get_{}", tb.name),
				TypeRef::named(tb.name.to_string()),
				move |ctx| {
					let tb_name = tb_name.clone();
					FieldFuture::new(async move {
						let kvs = DB.get().unwrap();

						let args = ctx.args.as_index_map();
						// async-graphql should validate that this is present as it is non-null
						let id = args.get("id").map(GqlValueUtils::as_string).flatten().unwrap();
						// TODO: Don't know why this doesn't work
						// let thing = match Thing::try_from(&id) {
						// 	Ok(t) => t,
						// 	Err(_) => Thing::from((tb_name, id)),
						// };
						let thing = Thing::from((tb_name, id));

						let use_stmt = Statement::Use(UseStatement {
							db: Some(DB_NAME.to_string()),
							ns: Some(NS_NAME.to_string()),
						});

						let ast = Statement::Select(SelectStatement {
							what: Values(vec![SqlValue::Thing(thing)]),
							expr: Fields::all(),
							only: true,
							..Default::default()
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
			.implement("record");

		for fd in fds.iter() {
			let fd_name = Name::new(fd.name.to_string());
			table_ty_obj = table_ty_obj.field(Field::new(
				fd.name.to_string(),
				kind_to_type(fd.kind.clone()),
				move |ctx| {
					let fd_name = fd_name.clone();
					FieldFuture::new(async move {
						let record = ctx.parent_value.as_value().unwrap();
						let GqlValue::Object(record_map) = record else {
							todo!("got unexpected: {record:?}, processing field {fd_name}")
						};
						let val = record_map.get(&fd_name).unwrap();

						Ok(Some(val.to_owned()))
					})
				},
			));
		}

		types.push(Type::Object(table_ty_obj));
	}
	info!("current Query: {:?}", query);

	query = query.field(Field::new("value2", TypeRef::named_nn(TypeRef::INT), |ctx| {
		FieldFuture::new(async move { Ok(Some(GqlValue::from(100))) })
	}));

	let mut schema = Schema::build("Query", None, None).register(query);
	for ty in types {
		schema = schema.register(ty);
	}

	let id_interface =
		Interface::new("record").field(InterfaceField::new("id", TypeRef::named_nn(TypeRef::ID)));
	schema = schema.register(id_interface);

	let relation_interface = Interface::new("relation")
		.field(InterfaceField::new("in", TypeRef::named_nn(TypeRef::ID)))
		.field(InterfaceField::new("out", TypeRef::named_nn(TypeRef::ID)));
	schema = schema.register(relation_interface);

	// let limit_input = InputObject::new("limit").field(limit_val);

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
		Kind::Record(_) => todo!("Kind::Record(_) "),
		Kind::Geometry(_) => todo!("Kind::Geometry(_) "),
		Kind::Option(_) => todo!("Kind::Option(_) "),
		Kind::Either(_) => todo!("Kind::Either(_) "),
		Kind::Set(_, _) => todo!("Kind::Set(_, _) "),
		Kind::Array(k, _) => TypeRef::List(Box::new(kind_to_type(Some(*k)))),
	};

	match optional {
		true => out_ty,
		false => TypeRef::NonNull(Box::new(out_ty)),
	}
}
