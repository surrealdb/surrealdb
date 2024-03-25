use std::collections::BTreeSet;
use std::collections::HashSet;

use async_graphql::dynamic::Field;
use async_graphql::dynamic::FieldFuture;
use async_graphql::dynamic::Object;
use async_graphql::dynamic::Schema;
use async_graphql::dynamic::Type;
use async_graphql::dynamic::TypeRef;
use async_graphql::Name;
use async_graphql::Value as GqlValue;
use serde_json::Number;
use surrealdb::sql::statements::SelectStatement;
use surrealdb::sql::statements::UseStatement;
use surrealdb::sql::Fields;
use surrealdb::sql::Kind;
use surrealdb::sql::Statement;
use surrealdb::sql::Table;
use surrealdb::sql::Values;

use crate::dbs::DB;
use surrealdb::kvs::LockType;
use surrealdb::kvs::TransactionType;
use surrealdb::sql::Value as SqlValue;

pub async fn get_schema() -> Result<Schema, Box<dyn std::error::Error>> {
	let kvs = DB.get().unwrap();
	let mut tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;
	let tbs = tx.all_tb("test", "test").await?;
	let mut query = Object::new("Query");
	let mut types: Vec<Type> = Vec::new();

	for tb in tbs.iter() {
		info!("Adding table: {}", tb.name);
		let tb_name = tb.name.to_string();
		query = query.field(Field::new(
			tb.name.to_string(),
			TypeRef::named_nn_list_nn(tb.name.to_string()),
			move |_ctx| {
				let tb_name = tb_name.clone();
				FieldFuture::new(async move {
					let kvs = DB.get().unwrap();

					let use_stmt = Statement::Use(UseStatement {
						db: Some("test".to_string()),
						ns: Some("test".to_string()),
					});
					let ast = Statement::Select(SelectStatement {
						what: Values(vec![SqlValue::Table(Table(tb_name))]),
						expr: Fields::all(),
						..Default::default()
					});

					let query = vec![use_stmt, ast].into();
					info!("query: {}", query);

					let res = kvs.process(query, &Default::default(), Default::default()).await?;
					// ast is constructed such that there will only be two responses the first of which is NONE
					let mut res_iter = res.into_iter();
					let _ = res_iter.next();
					let res = res_iter.next().unwrap();
					let res = res.result?;
					let SqlValue::Array(res_vec) = res.clone() else {
						panic!("top level value in array should be an array: {:?}", res)
					};
					let out =
						res_vec.0.into_iter().map(|v| sql_value_to_gql_value(v).unwrap()).collect();

					Ok(Some(GqlValue::List(out)))
				})
			},
		));
		// TODO: remove hardcoded ns and db
		let fds = tx.all_tb_fields("test", "test", &tb.name.0).await?;

		let mut table_ty_obj = Object::new(tb.name.to_string()).field(Field::new(
			"id",
			TypeRef::named_nn(TypeRef::ID),
			|ctx| {
				FieldFuture::new(async move {
					let record = ctx.parent_value.as_value().unwrap();

					Ok(Some(GqlValue::from("1")))
				})
			},
		));

		for fd in fds.iter() {
			table_ty_obj = table_ty_obj.field(Field::new(
				fd.name.to_string(),
				kind_to_type(fd.kind.clone()),
				|_ctx| FieldFuture::new(async move { Ok(Some(GqlValue::Null)) }),
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
			surrealdb::sql::Number::Decimal(_) => todo!(),
		},
		SqlValue::Strand(s) => GqlValue::String(s.0),
		SqlValue::Duration(_) => todo!(),
		SqlValue::Datetime(_) => todo!(),
		SqlValue::Uuid(_) => todo!(),
		SqlValue::Array(_) => todo!(),
		SqlValue::Object(o) => GqlValue::Object(
			o.0.into_iter()
				.map(|(k, v)| (Name::new(k), sql_value_to_gql_value(v).unwrap()))
				.collect(),
		),
		SqlValue::Geometry(_) => todo!(),
		SqlValue::Bytes(_) => todo!(),
		SqlValue::Thing(t) => GqlValue::String(t.to_string()),
		SqlValue::Param(_) => todo!(),
		SqlValue::Idiom(_) => todo!(),
		SqlValue::Table(_) => todo!(),
		SqlValue::Mock(_) => todo!(),
		SqlValue::Regex(_) => todo!(),
		SqlValue::Cast(_) => todo!(),
		SqlValue::Block(_) => todo!(),
		SqlValue::Range(_) => todo!(),
		SqlValue::Edges(_) => todo!(),
		SqlValue::Future(_) => todo!(),
		SqlValue::Constant(_) => todo!(),
		SqlValue::Function(_) => todo!(),
		SqlValue::Subquery(_) => todo!(),
		SqlValue::Expression(_) => todo!(),
		SqlValue::Query(_) => todo!(),
		SqlValue::Model(_) => todo!(),
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
		Kind::Any => todo!(),
		Kind::Null => todo!(),
		Kind::Bool => TypeRef::named(TypeRef::BOOLEAN),
		Kind::Bytes => todo!(),
		Kind::Datetime => todo!(),
		Kind::Decimal => todo!(),
		Kind::Duration => todo!(),
		Kind::Float => todo!(),
		Kind::Int => TypeRef::named(TypeRef::INT),
		Kind::Number => todo!(),
		Kind::Object => todo!(),
		Kind::Point => todo!(),
		Kind::String => TypeRef::named(TypeRef::STRING),
		Kind::Uuid => todo!(),
		Kind::Record(_) => todo!(),
		Kind::Geometry(_) => todo!(),
		Kind::Option(_) => todo!(),
		Kind::Either(_) => todo!(),
		Kind::Set(_, _) => todo!(),
		Kind::Array(k, _) => TypeRef::List(Box::new(kind_to_type(Some(*k)))),
	};

	match optional {
		true => out_ty,
		false => TypeRef::NonNull(Box::new(out_ty)),
	}
}
