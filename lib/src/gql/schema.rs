use std::collections::BTreeMap;

use crate::err::Error;
use crate::kvs::Datastore;
use crate::kvs::LockType;
use crate::kvs::TransactionType;
use crate::sql::statements::DefineFieldStatement;
use crate::sql::Kind;
use graphql_parser::schema;
use graphql_parser::schema::Definition;
use graphql_parser::schema::Field;
use graphql_parser::schema::InterfaceType;
use graphql_parser::schema::ObjectType;
use graphql_parser::schema::ScalarType;
use graphql_parser::schema::SchemaDefinition;
use graphql_parser::schema::Type;
use graphql_parser::schema::TypeDefinition;
use graphql_parser::schema::UnionType;

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

macro_rules! field {
	($name:literal, $ty:literal, null, $description:literal) => {
		custom_field($name, $ty, false, Some($description.to_string()))
	};
	($name:literal, $ty:literal, non_null, $description:literal) => {
		custom_field($name, $ty, true, Some($description.to_string()))
	};
	($name:literal, $ty:literal, null) => {
		custom_field($name, $ty, false, None)
	};
	($name:literal, $ty:literal, non_null) => {
		custom_field($name, $ty, true, None)
	};
	($name:literal, $ty:literal, $non_null:literal) => {
		custom_field($name, $ty, $non_null, None)
	};
	($name:literal, $ty:literal, $non_null:literal, $description:literal) => {
		custom_field($name, $ty, $non_null, Some($description.to_string()))
	};
}

fn custom_field<'a>(
	name: &str,
	ty: &str,
	non_null: bool,
	description: Option<String>,
) -> Field<'a, String> {
	let field_type = if non_null {
		Type::NonNullType(Type::NamedType(ty.to_string()).into())
	} else {
		Type::NamedType(ty.to_string()).into()
	};
	Field {
		description,
		name: name.to_string(),
		arguments: vec![],
		field_type,
		directives: vec![],
		position: Default::default(),
	}
}
macro_rules! input_val {
	($name:literal, $ty:literal, null, $description:literal) => {
		custom_input_value($name, $ty, false, Some($description.to_string()))
	};
	($name:literal, $ty:literal, non_null, $description:literal) => {
		custom_input_value($name, $ty, true, Some($description.to_string()))
	};
	($name:literal, $ty:literal, null) => {
		custom_input_value($name, $ty, false, None)
	};
	($name:literal, $ty:literal, non_null) => {
		custom_input_value($name, $ty, true, None)
	};
	($name:literal, $ty:literal, $non_null:literal) => {
		custom_input_value($name, $ty, $non_null, None)
	};
	($name:literal, $ty:literal, $non_null:literal, $description:literal) => {
		custom_input_value($name, $ty, $non_null, Some($description.to_string()))
	};
}

fn custom_input_value<'a>(
	name: &str,
	ty: &str,
	non_null: bool,
	description: Option<String>,
) -> schema::InputValue<'a, String> {
	let field_type = if non_null {
		Type::NonNullType(Type::NamedType(ty.to_string()).into())
	} else {
		Type::NamedType(ty.to_string()).into()
	};
	schema::InputValue {
		description,
		name: name.to_string(),
		default_value: None,
		directives: vec![],
		position: Default::default(),
		value_type: field_type,
	}
}

// fn convert_type<'a>(_ty: &Kind) -> Result<TypeDefinition<'a, String>, Error> {
// 	todo!()
// }

macro_rules! scalar_def {
	($name:literal, $desc:literal) => {
		Definition::TypeDefinition(TypeDefinition::Scalar(ScalarType {
			name: $name.to_string(),
			description: Some($desc.to_string()),
			directives: vec![],
			position: Default::default(),
		}))
	};

	($name:literal) => {
		Definition::TypeDefinition(TypeDefinition::Scalar(ScalarType {
			name: $name.to_string(),
			description: None,
			directives: vec![],
			position: Default::default(),
		}))
	};
}

macro_rules! scalar_insert {
	($acc:ident, $name:literal, $desc:literal) => {
		$acc.insert($name.to_string(), scalar_def!($name, $desc));
	};
	($acc:ident, $name:literal) => {
		$acc.insert($name.to_string(), scalar_def!($name));
	};
}

fn convert_kind_to_type<'a>(
	ty: Kind,
	def_acc: &mut BTreeMap<String, Definition<String>>,
) -> Result<Type<'a, String>, Error> {
	let (optional, match_ty) = match ty {
		Kind::Option(op_ty) => (true, *op_ty),
		_ => (false, ty),
	};

	let out_ty: Type<'_, String> = match match_ty {
		Kind::Any => {
			scalar_insert!(def_acc, "Any", "An untyped value");
			Type::NamedType("Any".to_string()).into()
		}
		Kind::Null => {
			scalar_insert!(def_acc, "Null");
			Type::NamedType("Null".to_string()).into()
		}
		Kind::Bool => Type::NamedType("Boolean".to_string()).into(), // builtin
		Kind::Bytes => {
			scalar_insert!(def_acc, "Bytes");
			Type::NamedType("Bytes".to_string()).into()
		}
		Kind::Datetime => {
			scalar_insert!(def_acc, "Datetime", "An ISO-8601 datetime");
			Type::NamedType("Datetime".to_string()).into()
		}
		Kind::Decimal => {
			scalar_insert!(def_acc, "Decimal", "An arbitrary precision decimal number");
			Type::NamedType("Decimal".to_string()).into()
		}
		Kind::Duration => {
			scalar_insert!(def_acc, "Duration", "An ISO-8601 duration");
			Type::NamedType("Duration".to_string()).into()
		}
		Kind::Float => Type::NamedType("Float".to_string()).into(), // builtin
		Kind::Int => Type::NamedType("Int".to_owned()).into(),      // builtin
		Kind::Number => {
			scalar_insert!(
				def_acc,
				"Number",
				"A generic number which can be an Int, Float, or Decimal"
			);
			Type::NamedType("Number".to_string()).into()
		}
		Kind::Object => {
			scalar_insert!(def_acc, "Object", "A dynamic key-value object");
			Type::NamedType("Object".to_string()).into()
		}
		Kind::Point => {
			scalar_insert!(def_acc, "Point", "A GeoJSON point");
			Type::NamedType("Point".to_string()).into()
		}
		Kind::String => Type::NamedType("String".to_string()).into(), // builtin
		Kind::Uuid => {
			scalar_insert!(def_acc, "Uuid", "A Universally Unique Identifier");
			Type::NamedType("Uuid".to_string()).into()
		}
		Kind::Record(v) => {
			match v.len() {
				0 => Type::NamedType("Record".to_string()).into(),
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
		Kind::Either(v) => match v.len() {
			0 | 1 => {
				return Err(Error::Thrown("Either must have at least two options".to_string()));
			}
			_ => {
				let name = v.iter().map(ToString::to_string).collect::<Vec<String>>().join("_or_");

				let def = Definition::TypeDefinition(TypeDefinition::Union(UnionType {
					description: Some(format!(
						"A union of the following types: {}",
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
		},
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

	//TODO: check if field name has multiple parts, should .* be represented? create custom types to represent nested structure

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

	// let mut filter_defs = vec![];
	// filter_defs.push(Definition::TypeDefinition(TypeDefinition::InputObject(InputObjectType {
	// 	position: Default::default(),
	// 	description: None,
	// 	name: "IDFilter".to_string(),
	// 	directives: vec![],
	// 	fields: vec![Field {
	// 		position: Default::default(),
	// 		description: "=".to_string(),
	// 		name: "eq".to_string(),
	// 		arguments: todo!(),
	// 		field_type: Type::NamedType("String".to_string()).into(),
	// 		directives: todo!(),
	// 	}],
	// })));

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
			description: tb.comment.clone().map(|s| s.to_string()),
			name: tb.name.to_string(),
			implements_interfaces: vec![if tb.is_relation() {
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
					arguments: vec![
						input_val!("limit", "Int", null),
						input_val!("start", "Int", null),
					],
					field_type: Type::NonNullType(
						Type::ListType(
							Type::NonNullType(Type::NamedType(tb.name.to_string()).into()).into(),
						)
						.into(),
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
			field!("in", "Record", non_null),
			field!("out", "Record", non_null),
		],
	})));

	defs.extend(table_defs);
	defs.extend(def_acc.into_values());

	// defs.push(Definition::TypeDefinition(TypeDefinition::InputObject(InputObjectType {
	// 	position: Default::default(),
	// 	description: None,
	// 	name: "Pagination".to_string(),
	// 	directives: vec![],
	// 	fields: vec![input_val!("limit", "Int", null), input_val!("start", "Int", null)],
	// })));

	Ok(schema::Document {
		definitions: defs,
	})
}
