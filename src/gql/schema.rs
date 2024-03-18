use async_graphql::{dynamic::*, Value};

pub fn get_schema() -> Schema {
	let query =
		Object::new("Query").field(Field::new("value", TypeRef::named_nn(TypeRef::INT), |ctx| {
			FieldFuture::new(async move { Ok(Some(Value::from(100))) })
		}));

	Schema::build(query.type_name(), None, None).register(query).finish().unwrap()
}
