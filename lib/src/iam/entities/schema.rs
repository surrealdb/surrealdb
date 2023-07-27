use cedar_policy::Schema;
use once_cell::sync::Lazy;

pub static DEFAULT_CEDAR_SCHEMA: Lazy<serde_json::Value> = Lazy::new(|| {
	serde_json::json!(
		{
			"": {
				"commonTypes": {
					// Represents a Resource
					"Resource": {
						"type": "Record",
						"attributes": {
							"type": { "type": "String", "required": true },
							"level" : { "type": "Entity", "name": "Level", "required": true },
						}
					},
				},
				"entityTypes": {
					// Represents the Root, Namespace, Database and Scope levels
					"Level": {
						"shape": {
							"type": "Record",
							"attributes": {
								"type": { "type": "String", "required": true },
								"ns": { "type": "String", "required": false },
								"db": { "type": "String", "required": false },
								"scope": { "type": "String", "required": false },
								"table": { "type": "String", "required": false },
								"level" : { "type": "Entity", "name": "Level", "required": true },
							}
						},
						"memberOfTypes": ["Level"],
					},

					// Base resource types
					"Any": {"shape": {"type": "Resource"}, "memberOfTypes": ["Level"]},
					"Namespace": {"shape": {"type": "Resource"}, "memberOfTypes": ["Level"]},
					"Database": {"shape": {"type": "Resource"}, "memberOfTypes": ["Level"]},
					"Scope": {"shape": {"type": "Resource"}, "memberOfTypes": ["Level"]},
					"Table": {"shape": {"type": "Resource"}, "memberOfTypes": ["Level"]},
					"Document": {"shape": {"type": "Resource"}, "memberOfTypes": ["Level"]},
					"Option": {"shape": {"type": "Resource"}, "memberOfTypes": ["Level"]},
					"Function": {"shape": {"type": "Resource"}, "memberOfTypes": ["Level"]},
					"Analyzer": {"shape": {"type": "Resource"}, "memberOfTypes": ["Level"]},
					"Parameter": {"shape": {"type": "Resource"}, "memberOfTypes": ["Level"]},
					"Event": {"shape": {"type": "Resource"}, "memberOfTypes": ["Level"]},
					"Field": {"shape": {"type": "Resource"}, "memberOfTypes": ["Level"]},
					"Index": {"shape": {"type": "Resource"}, "memberOfTypes": ["Level"]},

					// IAM resource types
					"Role": {},
					"Actor": {
						"shape": {
							"type": "Record",
							"attributes": {
								"type": { "type": "String", "required": true },
								"level" : { "type": "Entity", "name": "Level", "required": true },
								"roles": { "type": "Set", "element": { "type": "Entity", "name": "Role" }, "required": true},
							},
						},
						"memberOfTypes": ["Level"],
					},
				},
				"actions": {
					"View": {
						"appliesTo": {
							"principalTypes": [ "Actor" ],
							"resourceTypes": [ "Any", "Namespace", "Database", "Scope", "Table", "Document", "Option", "Function", "Analyzer", "Parameter", "Event", "Field", "Index", "Actor" ],

						},
					},
					"Edit": {
						"appliesTo": {
							"principalTypes": [ "Actor" ],
							"resourceTypes": [ "Any", "Namespace", "Database", "Scope", "Table", "Document", "Option", "Function", "Analyzer", "Parameter", "Event", "Field", "Index", "Actor" ],
						},
					},
				},
			}
		}
	)
});

pub fn default_schema() -> Schema {
	Schema::from_json_value(DEFAULT_CEDAR_SCHEMA.to_owned()).unwrap()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_default_schema() {
		let schema = default_schema();
		assert_eq!(schema.action_entities().unwrap().iter().count(), 2);
	}
}
