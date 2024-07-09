use async_graphql::{dynamic::indexmap::IndexMap, Name, Value as GqlValue};

// pub enum ConstValue {
//     /// `null`.
//     Null,
//     /// A number.
//     Number(Number),
//     /// A string.
//     String(String),
//     /// A boolean.
//     Boolean(bool),
//     /// A binary.
//     Binary(Bytes),
//     /// An enum. These are typically in `SCREAMING_SNAKE_CASE`.
//     Enum(Name),
//     /// A list of values.
//     List(Vec<ConstValue>),
//     /// An object. This is a map of keys to values.
//     Object(IndexMap<Name, ConstValue>),
// }
pub(crate) trait GqlValueUtils {
	// fn as_u64(&self) -> Option<u64>;
	fn as_i64(&self) -> Option<i64>;
	fn as_string(&self) -> Option<String>;
	fn as_list(&self) -> Option<&Vec<GqlValue>>;
	fn as_object(&self) -> Option<&IndexMap<Name, GqlValue>>;
}

impl GqlValueUtils for GqlValue {
	// fn as_u64(&self) -> Option<u64> {
	// 	if let GqlValue::Number(n) = self {
	// 		n.as_u64()
	// 	} else {
	// 		None
	// 	}
	// }

	fn as_i64(&self) -> Option<i64> {
		if let GqlValue::Number(n) = self {
			n.as_i64()
		} else {
			None
		}
	}

	fn as_string(&self) -> Option<String> {
		if let GqlValue::String(s) = self {
			Some(s.to_owned())
		} else {
			None
		}
	}
	fn as_list(&self) -> Option<&Vec<GqlValue>> {
		if let GqlValue::List(a) = self {
			Some(a)
		} else {
			None
		}
	}
	fn as_object(&self) -> Option<&IndexMap<Name, GqlValue>> {
		if let GqlValue::Object(o) = self {
			Some(o)
		} else {
			None
		}
	}
}
