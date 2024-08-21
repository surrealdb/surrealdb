use serde_json::Value as JsonValue;
pub enum Expected {
	Any,
	One(&'static str),
	Two(&'static str, &'static str),
}

impl Expected {
	pub fn check_results(&self, q: &str, results: &[JsonValue]) {
		match self {
			Expected::Any => {}
			Expected::One(expected) => {
				assert_eq!(results.len(), 1, "Wrong number of result for {q}");
				Self::check_json(q, &results[0], expected);
			}
			Expected::Two(expected1, expected2) => {
				assert_eq!(results.len(), 2, "Wrong number of result for {q}");
				Self::check_json(q, &results[0], expected1);
				Self::check_json(q, &results[1], expected2);
			}
		}
	}

	pub fn check_json(q: &str, result: &JsonValue, expected: &str) {
		let expected: JsonValue = serde_json::from_str(expected).expect(expected);
		assert_eq!(result, &expected, "Unexpected result on query {q}");
	}
}
