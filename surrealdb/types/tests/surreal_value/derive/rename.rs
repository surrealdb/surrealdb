use surrealdb_types::{SurrealValue, Value, object};

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(untagged)]
enum DisputeStatus {
	#[surreal(rename = "customer_won")]
	CustomerWon,
	#[surreal(rename = "brand_won")]
	BrandWon,
}

#[test]
fn test_enum_variant_rename_unit_variant() {
	assert_eq!(DisputeStatus::CustomerWon.into_value(), Value::String("customer_won".into()));
	assert_eq!(DisputeStatus::BrandWon.into_value(), Value::String("brand_won".into()));
	assert_eq!(
		DisputeStatus::from_value(Value::String("customer_won".into())).unwrap(),
		DisputeStatus::CustomerWon
	);
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
enum RenamedNamedVariant {
	#[surreal(rename = "customer_won")]
	CustomerWon {
		reason: String,
	},
}

#[test]
fn test_enum_variant_rename_named_variant() {
	let value = RenamedNamedVariant::CustomerWon {
		reason: "chargeback".into(),
	}
	.into_value();

	assert_eq!(
		value,
		Value::Object(object! {
			"customer_won": Value::Object(object! {
				"reason": Value::String("chargeback".into())
			})
		})
	);

	assert_eq!(
		RenamedNamedVariant::from_value(value).unwrap(),
		RenamedNamedVariant::CustomerWon {
			reason: "chargeback".into(),
		}
	);
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
enum RenamedUnnamedVariant {
	#[surreal(rename = "customer_won")]
	CustomerWon(String),
}

#[test]
fn test_enum_variant_rename_unnamed_variant() {
	let value = RenamedUnnamedVariant::CustomerWon("chargeback".into()).into_value();

	assert_eq!(
		value,
		Value::Object(object! {
			"customer_won": Value::String("chargeback".into())
		})
	);

	assert_eq!(
		RenamedUnnamedVariant::from_value(value).unwrap(),
		RenamedUnnamedVariant::CustomerWon("chargeback".into())
	);
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(rename_all = "camelCase")]
struct Order {
	customer_id: String,
	total_amount: f64,
}

#[test]
fn test_struct_rename_all() {
	let value = Order {
		customer_id: "c_1".into(),
		total_amount: 7.5,
	}
	.into_value();

	if let Value::Object(obj) = &value {
		assert_eq!(obj.get("customerId"), Some(&Value::String("c_1".into())));
		assert_eq!(obj.get("totalAmount"), Some(&Value::Number(7.5.into())));
		assert!(obj.get("customer_id").is_none());
		assert!(obj.get("total_amount").is_none());
	} else {
		panic!("Expected object value");
	}

	assert_eq!(
		Order::from_value(value).unwrap(),
		Order {
			customer_id: "c_1".into(),
			total_amount: 7.5,
		}
	);
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(rename_all = "camelCase")]
struct ExplicitRenameWins {
	#[surreal(rename = "EXPLICIT")]
	foo_bar: String,
}

#[test]
fn test_struct_rename_precedence_over_rename_all() {
	let value = ExplicitRenameWins {
		foo_bar: "value".into(),
	}
	.into_value();

	if let Value::Object(obj) = &value {
		assert_eq!(obj.get("EXPLICIT"), Some(&Value::String("value".into())));
		assert!(obj.get("fooBar").is_none());
	} else {
		panic!("Expected object value");
	}

	assert_eq!(
		ExplicitRenameWins::from_value(value).unwrap(),
		ExplicitRenameWins {
			foo_bar: "value".into(),
		}
	);
}

macro_rules! assert_enum_rename_all_case {
	($enum_ty:ty, $variant:path, $expected:expr) => {
		assert_eq!($variant.into_value(), Value::String($expected.into()));
		assert_eq!(
			<$enum_ty as SurrealValue>::from_value(Value::String($expected.into())).unwrap(),
			$variant
		);
	};
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(untagged, rename_all = "lowercase")]
enum EnumLowercase {
	InProgress,
	ShippedOut,
	OnHold,
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(untagged, rename_all = "UPPERCASE")]
enum EnumUppercase {
	InProgress,
	ShippedOut,
	OnHold,
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(untagged, rename_all = "PascalCase")]
enum EnumPascalCase {
	InProgress,
	ShippedOut,
	OnHold,
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(untagged, rename_all = "camelCase")]
enum EnumCamelCase {
	InProgress,
	ShippedOut,
	OnHold,
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(untagged, rename_all = "snake_case")]
enum EnumSnakeCase {
	InProgress,
	ShippedOut,
	OnHold,
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(untagged, rename_all = "SCREAMING_SNAKE_CASE")]
enum EnumScreamingSnakeCase {
	InProgress,
	ShippedOut,
	OnHold,
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(untagged, rename_all = "kebab-case")]
enum EnumKebabCase {
	InProgress,
	ShippedOut,
	OnHold,
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(untagged, rename_all = "SCREAMING-KEBAB-CASE")]
enum EnumScreamingKebabCase {
	InProgress,
	ShippedOut,
	OnHold,
}

#[test]
fn test_enum_rename_all_supported_values() {
	assert_enum_rename_all_case!(EnumLowercase, EnumLowercase::InProgress, "inprogress");
	assert_enum_rename_all_case!(EnumUppercase, EnumUppercase::InProgress, "INPROGRESS");
	assert_enum_rename_all_case!(EnumPascalCase, EnumPascalCase::InProgress, "InProgress");
	assert_enum_rename_all_case!(EnumCamelCase, EnumCamelCase::InProgress, "inProgress");
	assert_enum_rename_all_case!(EnumSnakeCase, EnumSnakeCase::InProgress, "in_progress");
	assert_enum_rename_all_case!(
		EnumScreamingSnakeCase,
		EnumScreamingSnakeCase::InProgress,
		"IN_PROGRESS"
	);
	assert_enum_rename_all_case!(EnumKebabCase, EnumKebabCase::InProgress, "in-progress");
	assert_enum_rename_all_case!(
		EnumScreamingKebabCase,
		EnumScreamingKebabCase::InProgress,
		"IN-PROGRESS"
	);
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(rename_all = "snake_case")]
struct NestedChild {
	order_total: i64,
	#[surreal(rename = "EXPLICIT")]
	order_status: String,
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(rename_all = "camelCase")]
struct NestedParent {
	child_payload: NestedChild,
}

#[test]
fn test_nested_struct_rename_all_is_container_local() {
	let value = NestedParent {
		child_payload: NestedChild {
			order_total: 42,
			order_status: "ok".into(),
		},
	}
	.into_value();

	if let Value::Object(obj) = &value {
		assert!(obj.get("child_payload").is_none());
		let Some(Value::Object(child)) = obj.get("childPayload") else {
			panic!("Expected childPayload to be an object");
		};

		assert_eq!(child.get("order_total"), Some(&Value::Number(42.into())));
		assert_eq!(child.get("EXPLICIT"), Some(&Value::String("ok".into())));
		assert!(child.get("orderTotal").is_none());
		assert!(child.get("order_status").is_none());
	} else {
		panic!("Expected object value");
	}

	assert_eq!(
		NestedParent::from_value(value).unwrap(),
		NestedParent {
			child_payload: NestedChild {
				order_total: 42,
				order_status: "ok".into(),
			},
		}
	);
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(tag = "type", content = "data", rename_all = "snake_case")]
enum TaggedEvent {
	UserLoggedIn {
		user_id: String,
	},
	SessionExpired,
}

#[test]
fn test_enum_rename_all_applies_to_tagged_strategy() {
	let login = TaggedEvent::UserLoggedIn {
		user_id: "u_1".into(),
	}
	.into_value();
	assert_eq!(
		login,
		Value::Object(object! {
			"type": Value::String("user_logged_in".into()),
			"data": Value::Object(object! {
				"user_id": Value::String("u_1".into()),
			}),
		})
	);
	assert_eq!(
		TaggedEvent::from_value(login).unwrap(),
		TaggedEvent::UserLoggedIn {
			user_id: "u_1".into(),
		}
	);

	let expired = TaggedEvent::SessionExpired.into_value();
	assert_eq!(
		expired,
		Value::Object(object! {
			"type": Value::String("session_expired".into()),
			"data": Value::Object(object! {}),
		})
	);
	assert_eq!(TaggedEvent::from_value(expired).unwrap(), TaggedEvent::SessionExpired);
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(rename_all = "snake_case")]
enum VariantKeyEvent {
	UserLoggedIn {
		user_id: String,
	},
	SessionExpired,
}

#[test]
fn test_enum_rename_all_applies_to_variant_key_strategy() {
	let login = VariantKeyEvent::UserLoggedIn {
		user_id: "u_1".into(),
	}
	.into_value();
	assert_eq!(
		login,
		Value::Object(object! {
			"user_logged_in": Value::Object(object! {
				"user_id": Value::String("u_1".into()),
			}),
		})
	);
	assert_eq!(
		VariantKeyEvent::from_value(login).unwrap(),
		VariantKeyEvent::UserLoggedIn {
			user_id: "u_1".into(),
		}
	);

	let expired = VariantKeyEvent::SessionExpired.into_value();
	assert_eq!(
		expired,
		Value::Object(object! {
			"session_expired": Value::Object(object! {}),
		})
	);
	assert_eq!(VariantKeyEvent::from_value(expired).unwrap(), VariantKeyEvent::SessionExpired);
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(tag = "type", rename_all = "snake_case")]
enum TagKeyEvent {
	UserLoggedIn {
		user_id: String,
	},
	SessionExpired,
}

#[test]
fn test_enum_rename_all_applies_to_tag_key_strategy() {
	let login = TagKeyEvent::UserLoggedIn {
		user_id: "u_1".into(),
	}
	.into_value();
	assert_eq!(
		login,
		Value::Object(object! {
			"type": Value::String("user_logged_in".into()),
			"user_id": Value::String("u_1".into()),
		})
	);
	assert_eq!(
		TagKeyEvent::from_value(login).unwrap(),
		TagKeyEvent::UserLoggedIn {
			user_id: "u_1".into(),
		}
	);

	let expired = TagKeyEvent::SessionExpired.into_value();
	assert_eq!(
		expired,
		Value::Object(object! {
			"type": Value::String("session_expired".into()),
		})
	);
	assert_eq!(TagKeyEvent::from_value(expired).unwrap(), TagKeyEvent::SessionExpired);
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(rename_all = "snake_case")]
struct RawIdentStruct {
	r#type: String,
	r#struct: i64,
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(untagged, rename_all = "snake_case")]
enum RawIdentEnum {
	r#Type,
	r#Struct,
}

#[test]
fn test_rename_all_applies_to_raw_identifiers() {
	let value = RawIdentStruct {
		r#type: "t".into(),
		r#struct: 3,
	}
	.into_value();

	if let Value::Object(obj) = &value {
		assert_eq!(obj.get("type"), Some(&Value::String("t".into())));
		assert_eq!(obj.get("struct"), Some(&Value::Number(3.into())));
		assert!(obj.get("r#type").is_none());
	} else {
		panic!("Expected object value");
	}

	assert_eq!(
		RawIdentStruct::from_value(value).unwrap(),
		RawIdentStruct {
			r#type: "t".into(),
			r#struct: 3,
		}
	);

	assert_eq!(RawIdentEnum::r#Type.into_value(), Value::String("type".into()));
	assert_eq!(
		RawIdentEnum::from_value(Value::String("type".into())).unwrap(),
		RawIdentEnum::r#Type,
	);
}
