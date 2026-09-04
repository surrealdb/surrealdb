// Keep `Value` out of scope so the generated code must honor the configured crate path.
mod surrealdb_custom_path {
	pub use surrealdb_types as types;
}

use surrealdb_custom_path::types::SurrealValue;

#[derive(Debug, PartialEq, SurrealValue)]
#[surreal(crate = "surrealdb_custom_path::types")]
#[surreal(tag = "kind")]
enum TagOnly {
	Named {
		value: String,
	},
}

#[derive(Debug, PartialEq, SurrealValue)]
#[surreal(crate = "surrealdb_custom_path::types")]
#[surreal(tag = "kind", content = "data")]
enum TagAndContent {
	Named {
		value: String,
	},
}

#[derive(Debug, PartialEq, SurrealValue)]
#[surreal(crate = "surrealdb_custom_path::types")]
#[surreal(
	tag = "kind",
	content = "data",
	skip_content_if = "surrealdb_custom_path::types::Value::is_empty"
)]
enum TagAndOptionalContent {
	Named {
		value: String,
	},
}

#[test]
fn custom_crate_path_tag_only_named_variant_roundtrips() {
	let value = TagOnly::Named {
		value: "tag-only".to_owned(),
	}
	.into_value();

	assert_eq!(
		TagOnly::from_value(value).unwrap(),
		TagOnly::Named {
			value: "tag-only".to_owned(),
		}
	);
}

#[test]
fn custom_crate_path_tag_content_named_variant_roundtrips() {
	let value = TagAndContent::Named {
		value: "tag-content".to_owned(),
	}
	.into_value();

	assert_eq!(
		TagAndContent::from_value(value).unwrap(),
		TagAndContent::Named {
			value: "tag-content".to_owned(),
		}
	);
}

#[test]
fn custom_crate_path_tag_optional_content_named_variant_roundtrips() {
	let value = TagAndOptionalContent::Named {
		value: "optional-content".to_owned(),
	}
	.into_value();

	assert_eq!(
		TagAndOptionalContent::from_value(value).unwrap(),
		TagAndOptionalContent::Named {
			value: "optional-content".to_owned(),
		}
	);
}
