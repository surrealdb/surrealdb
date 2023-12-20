use graphql_parser::parse_query;
use graphql_parser::query;
use graphql_parser::schema;
use graphql_parser::schema::Definition;
use graphql_parser::schema::Field;
use graphql_parser::schema::ObjectType;
use graphql_parser::schema::SchemaDefinition;
use graphql_parser::schema::Type;
use graphql_parser::schema::TypeDefinition;

use crate::err::Error;
use crate::kvs::Datastore;
use crate::kvs::LockType;
use crate::kvs::TransactionType;
use crate::sql::statements::DefineFieldStatement;
use crate::sql::Kind;
use crate::sql::Query;

pub fn parse_and_transpile(txt: &str) -> Result<Query, Error> {
	let _ast: query::Document<&str> =
		parse_query::<&str>(txt).map_err(|e| Error::Thrown(e.to_string()))?;

	todo!()
}

// fn convert_type<'a>(_ty: &Kind) -> Result<TypeDefinition<'a, String>, Error> {
// 	todo!()
// }
fn convert_kind_to_type<'a>(ty: Kind) -> Result<Type<'a, String>, Error> {
	let (optional, match_ty) = match ty {
		Kind::Option(op_ty) => (true, *op_ty),
		_ => (false, ty),
	};

	let out_ty: Type<'_, String> = match match_ty {
		Kind::Any => Type::NamedType("Any".to_string()).into(),
		Kind::Null => Type::NamedType("Null".to_string()).into(),
		Kind::Bool => Type::NamedType("Bool".to_string()).into(),
		Kind::Bytes => Type::NamedType("Bytes".to_string()).into(),
		Kind::Datetime => Type::NamedType("Datetime".to_string()).into(),
		Kind::Decimal => Type::NamedType("Decimal".to_string()).into(),
		Kind::Duration => Type::NamedType("Duration".to_string()).into(),
		Kind::Float => Type::NamedType("Float".to_string()).into(),
		Kind::Int => Type::NamedType("Int".to_owned()).into(),
		Kind::Number => Type::NamedType("Number".to_string()).into(),
		Kind::Object => panic!("Object types are not currently supported for graphql"),
		Kind::Point => Type::NamedType("Point".to_string()).into(),
		Kind::String => Type::NamedType("String".to_string()).into(),
		Kind::Uuid => Type::NamedType("Uuid".to_string()).into(),
		Kind::Record(v) => {
			// println!("{:?}", v);
			assert!(v.len() == 1);
			Type::NamedType(v.first().unwrap().to_string()).into()
		}
		Kind::Geometry(_) => Type::NamedType("Geometry".to_string()).into(),
		Kind::Option(_) => convert_kind_to_type(match_ty)?,
		Kind::Either(_) => panic!("Union types are not currently supported for graphql"),
		Kind::Set(_, _) => Type::NamedType("Set".to_string()).into(),
		Kind::Array(t, _) => Type::ListType(Box::new(convert_kind_to_type(*t)?)).into(),
	};
	let final_ty = if optional {
		out_ty
	} else {
		Type::NonNullType(Box::new(out_ty))
	};

	Ok(final_ty)
}

fn convert_field<'a>(fd: &DefineFieldStatement) -> Result<schema::Field<'a, String>, Error> {
	let kind = fd.kind.clone().unwrap_or(Kind::Any);
	let ty = convert_kind_to_type(kind)?;

	Ok(schema::Field {
		description: None,
		name: fd.name.to_string(),
		arguments: vec![],
		field_type: ty,
		directives: vec![],
		position: Default::default(),
	})
}

pub async fn get_schema<'a>(
	ds: &Datastore,
	ns: String,
	db: String,
) -> Result<schema::Document<'a, String>, Error> {
	let mut tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
	let mut defs: Vec<Definition<String>> = Vec::new();
	defs.push(Definition::SchemaDefinition(SchemaDefinition {
		directives: vec![],
		query: Some("Query".to_string()),
		mutation: None,
		subscription: None,
		position: Default::default(),
	}));
	let tbs = tx.all_tb(&ns, &db).await?;
	println!("tbs: {:?}\n", tbs);
	let mut table_defs = vec![];
	for tb in tbs.iter() {
		let fds = tx.all_tb_fields(&ns, &db, &tb.name).await?;
		println!("fds(len:{}): {:?}\n", fds.len(), fds);
		let fds = fds.iter().map(convert_field).collect::<Result<Vec<_>, Error>>()?;
		let ty_def = TypeDefinition::Object(ObjectType {
			description: None,
			name: tb.name.to_string(),
			implements_interfaces: vec![],
			directives: vec![],
			fields: fds,
			position: Default::default(),
		});
		println!("ty_def: {:?}\n", ty_def);
		table_defs.push(Definition::TypeDefinition(ty_def));
	}

	defs.push(Definition::TypeDefinition(TypeDefinition::Object(ObjectType {
		description: None,
		name: "Query".to_string(),
		implements_interfaces: vec![],
		directives: vec![],
		fields: table_defs
			.iter()
			.map(|def| {
				let Definition::TypeDefinition(tb) = def else {
					panic!()
				};
				let TypeDefinition::Object(tb) = tb else {
					panic!()
				};
				Field {
					position: Default::default(),
					description: None,
					name: tb.name.to_string(),
					arguments: vec![],
					field_type: Type::NonNullType(
						Type::ListType(Type::NamedType(tb.name.to_string()).into()).into(),
					),
					directives: vec![],
				}
			})
			.collect(),
		position: Default::default(),
	})));

	defs.extend(table_defs);

	Ok(schema::Document {
		definitions: defs,
	})
}

#[cfg(test)]
mod test {
	use super::*;

	#[tokio::test]
	async fn test_schema_generation() {
		let ds = Datastore::new("memory").await.unwrap();
		ds.execute_sql(
			r#"USE NS test; USE DB test;
            DEFINE TABLE person SCHEMAFUL;
            DEFINE FIELD name ON person TYPE string;
            DEFINE FIELD companies ON person TYPE array<record<company>>;
            DEFINE TABLE company SCHEMAFUL;
            DEFINE FIELD name ON company TYPE string;
            "#,
			&Default::default(),
			None,
		)
		.await
		.unwrap();
		let schema = get_schema(&ds, "test".to_string(), "test".to_string()).await.unwrap();

		panic!("{}", schema);
	}
}
