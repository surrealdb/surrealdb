//! The derive must name every item through the `crate` path it is given, so
//! a type that does not import `Value` (or anything else) still compiles.

use surrealdb_types::SurrealValue;

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(tag = "kind")]
enum Tagged {
	Goo {
		x: u64,
	},
	Hoo {
		x: u64,
	},
}

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(crate = "surrealdb_types")]
#[surreal(tag = "kind", content = "data")]
enum TaggedWithContent {
	Goo {
		x: u64,
	},
	Hoo {
		x: u64,
	},
}

#[test]
fn tagged_enum_roundtrips_without_value_in_scope() {
	for original in [
		Tagged::Goo {
			x: 1,
		},
		Tagged::Hoo {
			x: 2,
		},
	] {
		let value = original.into_value();
		assert!(Tagged::is_value(&value));
		assert!(Tagged::from_value(value).is_ok());
	}
	let value = TaggedWithContent::Hoo {
		x: 3,
	}
	.into_value();
	assert_eq!(
		TaggedWithContent::from_value(value).unwrap(),
		TaggedWithContent::Hoo {
			x: 3
		}
	);
}
