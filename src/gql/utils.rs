use async_graphql::Value as GqlValue;

pub(crate) trait GqlValueUtils {
	fn as_u64(&self) -> Option<u64>;
	fn as_i64(&self) -> Option<i64>;
	fn as_string(&self) -> Option<String>;
}

impl GqlValueUtils for GqlValue {
	fn as_u64(&self) -> Option<u64> {
		if let GqlValue::Number(n) = self {
			n.as_u64()
		} else {
			None
		}
	}

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
}
