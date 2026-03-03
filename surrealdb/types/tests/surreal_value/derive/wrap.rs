use serde::{Deserialize, Serialize};
use surrealdb_types::{SurrealValue, Value, object};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct GerardTheValueless {
	name: String,
	burgers_eaten: i64,
}

#[derive(SurrealValue, PartialEq, Debug)]
#[surreal(crate = "surrealdb_types")]
struct IncompatablePerson {
	name: String,
	age: i64,
	#[surreal(wrap)]
	friend: GerardTheValueless,
}

#[derive(SurrealValue, PartialEq, Debug)]
#[surreal(crate = "surrealdb_types")]
struct Blanket(#[surreal(wrap)] GerardTheValueless);

#[derive(SurrealValue, PartialEq, Debug)]
#[surreal(crate = "surrealdb_types")]
struct BlanketOnAComfyEvening(
	#[surreal(wrap)] GerardTheValueless,
	#[surreal(wrap)] GerardTheValueless,
);

#[test]
fn basic_struct_to_value() {
	let initial = IncompatablePerson {
		name: "mr. fresh".into(),
		age: 64,
		friend: GerardTheValueless {
			name: "Gerard the Valueless".into(),
			// gerard REALLY likes hamburgers :)
			burgers_eaten: i64::MAX,
		},
	};

	let to_value = initial.into_value();
	let expected_value = Value::Object(object! {
		name: "mr. fresh",
		age: 64,
		friend: object! {
			name: "Gerard the Valueless",
			burgers_eaten: i64::MAX
		}
	});

	assert_eq!(to_value, expected_value);
}

#[test]
fn value_to_basic_struct() {
	let initial = Value::Object(object! {
		name: "mr. fresh",
		age: 64,
		friend: object! {
			name: "Gerard the Valueless",
			burgers_eaten: i64::MAX
		}
	});

	let to_struct = IncompatablePerson::from_value(initial).expect("this should work!");
	let expected_value = IncompatablePerson {
		name: "mr. fresh".into(),
		age: 64,
		friend: GerardTheValueless {
			name: "Gerard the Valueless".into(),
			// gerard REALLY likes hamburgers :)
			burgers_eaten: i64::MAX,
		},
	};

	assert_eq!(to_struct, expected_value);
}

#[test]
fn tuple_struct_to_value() {
	let initial = Blanket(GerardTheValueless {
		name: "Gerard the Valueless".into(),
		// gerard REALLY likes hamburgers :)
		burgers_eaten: i64::MAX,
	});

	let to_value = initial.into_value();
	let expected_value = Value::Object(object! {
			name: "Gerard the Valueless",
			burgers_eaten: i64::MAX
	});

	assert_eq!(to_value, expected_value);
}

#[test]
fn value_to_tuple_struct() {
	let initial = Value::Object(object! {
			name: "Gerard the Valueless",
			burgers_eaten: i64::MAX
	});

	let to_struct = Blanket::from_value(initial).expect("this should work!");
	let expected_value = Blanket(GerardTheValueless {
		name: "Gerard the Valueless".into(),
		// gerard REALLY likes hamburgers :)
		burgers_eaten: i64::MAX,
	});

	assert_eq!(to_struct, expected_value);
}

#[test]
fn tuple_struct_2x_to_value() {
	let initial = BlanketOnAComfyEvening(
		GerardTheValueless {
			name: "Gerard the Valueless".into(),
			// gerard REALLY likes hamburgers :)
			burgers_eaten: i64::MAX,
		},
		GerardTheValueless {
			name: "Gerard the Valueless".into(),
			// gerard REALLY likes hamburgers :)
			burgers_eaten: i64::MAX,
		},
	);

	let to_value = initial.into_value();
	let expected_value = Value::Array(
		vec![
			Value::Object(object! {
					name: "Gerard the Valueless",
					burgers_eaten: i64::MAX
			});
			2
		]
		.into(),
	);

	assert_eq!(to_value, expected_value);
}

#[test]
fn value_to_tuple_struct_2x() {
	let initial = Value::Array(
		vec![
			Value::Object(object! {
					name: "Gerard the Valueless",
					burgers_eaten: i64::MAX
			});
			2
		]
		.into(),
	);

	let to_struct = BlanketOnAComfyEvening::from_value(initial).expect("this should work!");
	let expected_value = BlanketOnAComfyEvening(
		GerardTheValueless {
			name: "Gerard the Valueless".into(),
			// gerard REALLY likes hamburgers :)
			burgers_eaten: i64::MAX,
		},
		GerardTheValueless {
			name: "Gerard the Valueless".into(),
			// gerard REALLY likes hamburgers :)
			burgers_eaten: i64::MAX,
		},
	);

	assert_eq!(to_struct, expected_value);
}
