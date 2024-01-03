use std::collections::BTreeMap;

use graphql_parser::parse_query;
use graphql_parser::query;
use graphql_parser::schema;
use graphql_parser::schema::Definition;
use graphql_parser::schema::Field;
use graphql_parser::schema::InterfaceType;
use graphql_parser::schema::ObjectType;
use graphql_parser::schema::SchemaDefinition;
use graphql_parser::schema::Type;
use graphql_parser::schema::TypeDefinition;
use graphql_parser::schema::UnionType;

use crate::err::Error;
use crate::kvs::Datastore;
use crate::kvs::LockType;
use crate::kvs::TransactionType;
use crate::sql;
use crate::sql::statements::DefineFieldStatement;
use crate::sql::statements::SelectStatement;
use crate::sql::Fields;
use crate::sql::Kind;
use crate::sql::Query;
use crate::sql::Statement;
use crate::sql::Table;
use crate::sql::Values;

pub fn parse_and_transpile(txt: &str) -> Result<Query, Error> {
	let ast: query::Document<&str> =
		parse_query::<&str>(txt).map_err(|e| Error::Thrown(e.to_string()))?;

	// info!("ast: {:#?}", ast);

	info!("surreal ast: {:#?}", crate::syn::parse("SELECT * FROM person")?);

	let mut query: Vec<Statement> = vec![];

	for d in ast.definitions.iter() {
		match d {
			query::Definition::Operation(o) => match o {
				query::OperationDefinition::SelectionSet(s) => {
					query.extend(transpile_selection_set(s.clone())?)
				}
				query::OperationDefinition::Query(_) => todo!(),
				query::OperationDefinition::Mutation(_) => todo!(),
				query::OperationDefinition::Subscription(_) => todo!(),
			},
			query::Definition::Fragment(_) => todo!(),
		}
	}

	info!("query: {:#?}", query);

	Ok(query.into())
}

macro_rules! id_field {
	() => {
		Field {
			description: None,
			name: "id".to_string(),
			arguments: vec![],
			field_type: Type::NonNullType(Type::NamedType("ID".to_string()).into()),
			directives: vec![],
			position: Default::default(),
		}
	};
}

fn custom_field<'a>(name: &str, ty: &str, non_null: bool) -> Field<'a, String> {
	let field_type = if non_null {
		Type::NonNullType(Type::NamedType(ty.to_string()).into())
	} else {
		Type::NamedType(ty.to_string()).into()
	};
	Field {
		description: None,
		name: name.to_string(),
		arguments: vec![],
		field_type,
		directives: vec![],
		position: Default::default(),
	}
}

fn transpile_selection_set<'a>(
	ss: query::SelectionSet<'a, &'a str>,
) -> Result<Vec<Statement>, Error> {
	let statements = ss
		.items
		.iter()
		.map(|s| match s {
			query::Selection::Field(f) => Statement::Select(SelectStatement {
				expr: Fields::all(),
				omit: None,
				only: false,
				what: Values(vec![sql::Value::Table(Table(f.name.to_string()))]),
				with: None,
				cond: None,
				split: None,
				group: None,
				order: None,
				limit: None,
				start: None,
				fetch: None,
				version: None,
				timeout: None,
				parallel: false,
				explain: None,
			}),
			query::Selection::FragmentSpread(_) => todo!(),
			query::Selection::InlineFragment(_) => todo!(),
		})
		.collect::<Vec<_>>();
	Ok(statements)
}

// fn convert_type<'a>(_ty: &Kind) -> Result<TypeDefinition<'a, String>, Error> {
// 	todo!()
// }
fn convert_kind_to_type<'a>(
	ty: Kind,
	def_acc: &mut BTreeMap<String, Definition<String>>,
) -> Result<Type<'a, String>, Error> {
	let (optional, match_ty) = match ty {
		Kind::Option(op_ty) => (true, *op_ty),
		_ => (false, ty),
	};

	let out_ty: Type<'_, String> = match match_ty {
		Kind::Any => Type::NamedType("Any".to_string()).into(),
		Kind::Null => Type::NamedType("Null".to_string()).into(),
		Kind::Bool => Type::NamedType("Boolean".to_string()).into(), // builtin
		Kind::Bytes => Type::NamedType("Bytes".to_string()).into(),
		Kind::Datetime => Type::NamedType("Datetime".to_string()).into(),
		Kind::Decimal => Type::NamedType("Decimal".to_string()).into(),
		Kind::Duration => Type::NamedType("Duration".to_string()).into(),
		Kind::Float => Type::NamedType("Float".to_string()).into(), // builtin
		Kind::Int => Type::NamedType("Int".to_owned()).into(),      // builtin
		Kind::Number => Type::NamedType("Number".to_string()).into(),
		Kind::Object => panic!("Object types are not currently supported for graphql"),
		Kind::Point => Type::NamedType("Point".to_string()).into(),
		Kind::String => Type::NamedType("String".to_string()).into(), // builtin
		Kind::Uuid => Type::NamedType("Uuid".to_string()).into(),
		Kind::Record(v) => {
			match v.len() {
				0 => panic!("record shouldn't have no elements"),
				1 => Type::NamedType(v.first().unwrap().to_string()).into(), // table names will be defined
				_ => {
					let name =
						v.iter().map(ToString::to_string).collect::<Vec<String>>().join("_or_");

					let def = Definition::TypeDefinition(TypeDefinition::Union(UnionType {
						description: Some(format!(
							"A union of the following tables: {}",
							v.iter().map(ToString::to_string).collect::<Vec<String>>().join(", ")
						)),
						name: name.clone(),
						directives: vec![],
						position: Default::default(),
						types: v.iter().map(ToString::to_string).collect(),
					}));
					def_acc.insert(name.clone(), def);
					Type::NamedType(name).into()
				}
			}
		}
		Kind::Geometry(_) => Type::NamedType("Geometry".to_string()).into(),
		Kind::Option(_) => convert_kind_to_type(match_ty, def_acc)?,
		Kind::Either(_) => panic!("Union types are not currently supported for graphql"),
		Kind::Set(_, _) => Type::NamedType("Set".to_string()).into(),
		Kind::Array(t, _) => Type::ListType(Box::new(convert_kind_to_type(*t, def_acc)?)).into(),
	};
	let final_ty = if optional {
		out_ty
	} else {
		Type::NonNullType(Box::new(out_ty))
	};

	Ok(final_ty)
}

fn convert_field<'a>(
	fd: &DefineFieldStatement,
	def_acc: &mut BTreeMap<String, Definition<String>>,
) -> Result<schema::Field<'a, String>, Error> {
	let kind = fd.kind.clone().unwrap_or(Kind::Any);
	let ty = convert_kind_to_type(kind, def_acc)?;

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

	// All graphql schemas start with a schema definition
	defs.push(Definition::SchemaDefinition(SchemaDefinition {
		directives: vec![],
		query: Some("Query".to_string()),
		mutation: None,
		subscription: None,
		position: Default::default(),
	}));

	let tbs = tx.all_tb(&ns, &db).await?;

	let mut table_defs = vec![];

	// TODO: check that two types aren't defined with the same name from different sources
	let mut def_acc = BTreeMap::new();

	for tb in tbs.iter() {
		let fds = tx.all_tb_fields(&ns, &db, &tb.name).await?;
		// println!("fds(len:{}): {:?}\n", fds.len(), fds);
		let mut fd_defs = vec![id_field!()];
		fd_defs.extend(
			fds.iter()
				.map(|f| convert_field(f, &mut def_acc))
				.collect::<Result<Vec<_>, Error>>()?,
		);
		let ty_def = TypeDefinition::Object(ObjectType {
			description: None,
			name: tb.name.to_string(),
			implements_interfaces: vec![if tb.relation {
				"Relation".to_string()
			} else {
				"Record".to_string()
			}],
			directives: vec![],
			fields: fd_defs,
			position: Default::default(),
		});
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

	defs.push(Definition::TypeDefinition(TypeDefinition::Interface(InterfaceType {
		position: Default::default(),
		description: Some("All records must have an id".to_string()),
		name: "Record".to_string(),
		implements_interfaces: vec![],
		directives: vec![],
		fields: vec![id_field!()],
	})));
	defs.push(Definition::TypeDefinition(TypeDefinition::Interface(InterfaceType {
		position: Default::default(),
		description: Some("All relations must be records and have in and out fields".to_string()),
		name: "Relation".to_string(),
		implements_interfaces: vec!["Record".to_string()],
		directives: vec![],
		fields: vec![
			id_field!(),
			custom_field("in", "Record", true),
			custom_field("out", "Record", true),
		],
	})));

	defs.extend(table_defs);
	defs.extend(def_acc.into_values());

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
