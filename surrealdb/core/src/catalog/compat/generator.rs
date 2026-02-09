//! Fixture generator for catalog compatibility tests.
//!
//! This module generates serialized byte arrays from the fixture definitions.
//! Run the generator with:
//! ```bash
//! cargo test -p surrealdb-core --lib catalog::compat::generator -- --ignored --nocapture
//! ```
//!
//! The output can be copy-pasted into the appropriate version module (e.g., `v3_0_0.rs`).

use super::fixtures as fix;
use crate::kvs::KVValue;

/// A fixture definition with its name, description, and serialized bytes.
struct Fixture {
	name: &'static str,
	description: &'static str,
	value: Box<dyn KVValue>,
}

/// A collection of fixtures for a single type.
struct TypeFixtures {
	type_name: &'static str,
	fixtures: Vec<Fixture>,
}

/// Format bytes as a Rust const array.
fn format_bytes(value: &dyn KVValue) -> String {
	let hex_bytes: Vec<String> =
		value.kv_encode_value().unwrap().iter().map(|b| format!("0x{:02x}", b)).collect();

	// Format with 12 bytes per line for readability
	let lines: Vec<String> = hex_bytes.chunks(12).map(|chunk| chunk.join(", ")).collect();

	lines.join(",\n    ")
}

/// Generate the Rust code for a fixture.
fn format_fixture(type_name: &str, fixture: &Fixture) -> String {
	format!(
		r#"/// {type_name}: {description}
pub const {name}: &[u8] = &[
    {bytes}
];"#,
		type_name = type_name,
		description = fixture.description,
		name = fixture.name,
		bytes = format_bytes(&*fixture.value)
	)
}

/// Generate all fixtures for NamespaceDefinition
fn namespace_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "NamespaceDefinition",
		fixtures: vec![
			Fixture {
				name: "NAMESPACE_BASIC",
				description: "minimal namespace without comment",
				value: Box::new(fix::namespace_basic()),
			},
			Fixture {
				name: "NAMESPACE_WITH_COMMENT",
				description: "namespace with optional comment",
				value: Box::new(fix::namespace_with_comment()),
			},
		],
	}
}

/// Generate all fixtures for DatabaseDefinition
fn database_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "DatabaseDefinition",
		fixtures: vec![
			Fixture {
				name: "DATABASE_BASIC",
				description: "minimal database without changefeed",
				value: Box::new(fix::database_basic()),
			},
			Fixture {
				name: "DATABASE_WITH_CHANGEFEED",
				description: "database with changefeed enabled",
				value: Box::new(fix::database_with_changefeed()),
			},
			Fixture {
				name: "DATABASE_STRICT",
				description: "database with strict mode enabled",
				value: Box::new(fix::database_strict()),
			},
		],
	}
}

/// Generate all fixtures for TableDefinition
fn table_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "TableDefinition",
		fixtures: vec![
			Fixture {
				name: "TABLE_BASIC",
				description: "minimal table definition",
				value: Box::new(fix::table_basic()),
			},
			Fixture {
				name: "TABLE_WITH_VIEW",
				description: "table with view definition",
				value: Box::new(fix::table_with_view()),
			},
			Fixture {
				name: "TABLE_SCHEMAFULL",
				description: "schemafull table with changefeed",
				value: Box::new(fix::table_schemafull()),
			},
		],
	}
}

/// Generate all fixtures for SubscriptionDefinition
fn subscription_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "SubscriptionDefinition",
		fixtures: vec![
			Fixture {
				name: "SUBSCRIPTION_BASIC",
				description: "minimal subscription with diff fields",
				value: Box::new(fix::subscription_basic()),
			},
			Fixture {
				name: "SUBSCRIPTION_WITH_FILTERS",
				description: "subscription with condition and fetch",
				value: Box::new(fix::subscription_with_filters()),
			},
		],
	}
}

/// Generate all fixtures for AccessDefinition
fn access_definition_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "AccessDefinition",
		fixtures: vec![
			Fixture {
				name: "ACCESS_BEARER",
				description: "bearer access with JWT",
				value: Box::new(fix::access_bearer()),
			},
			Fixture {
				name: "ACCESS_WITH_AUTHENTICATE",
				description: "access with custom authenticate expression",
				value: Box::new(fix::access_with_authenticate()),
			},
		],
	}
}

/// Generate all fixtures for AccessGrant
fn access_grant_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "AccessGrant",
		fixtures: vec![
			Fixture {
				name: "GRANT_JWT",
				description: "JWT access grant",
				value: Box::new(fix::grant_jwt()),
			},
			Fixture {
				name: "GRANT_REVOKED",
				description: "revoked access grant",
				value: Box::new(fix::grant_revoked()),
			},
		],
	}
}

/// Generate all fixtures for AnalyzerDefinition
fn analyzer_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "AnalyzerDefinition",
		fixtures: vec![
			Fixture {
				name: "ANALYZER_BASIC",
				description: "minimal analyzer",
				value: Box::new(fix::analyzer_basic()),
			},
			Fixture {
				name: "ANALYZER_WITH_TOKENIZERS",
				description: "analyzer with tokenizers and filters",
				value: Box::new(fix::analyzer_with_tokenizers()),
			},
		],
	}
}

/// Generate all fixtures for ApiDefinition
fn api_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "ApiDefinition",
		fixtures: vec![
			Fixture {
				name: "API_BASIC",
				description: "minimal API endpoint",
				value: Box::new(fix::api_basic()),
			},
			Fixture {
				name: "API_WITH_MIDDLEWARE",
				description: "API with middleware and multiple methods",
				value: Box::new(fix::api_with_middleware()),
			},
		],
	}
}

/// Generate all fixtures for BucketDefinition
fn bucket_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "BucketDefinition",
		fixtures: vec![
			Fixture {
				name: "BUCKET_BASIC",
				description: "minimal bucket",
				value: Box::new(fix::bucket_basic()),
			},
			Fixture {
				name: "BUCKET_READONLY",
				description: "readonly bucket with backend",
				value: Box::new(fix::bucket_readonly()),
			},
		],
	}
}

/// Generate all fixtures for ConfigDefinition
fn config_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "ConfigDefinition",
		fixtures: vec![Fixture {
			name: "CONFIG_GRAPHQL",
			description: "GraphQL configuration",
			value: Box::new(fix::config_graphql()),
		}],
	}
}

/// Generate all fixtures for EventDefinition
fn event_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "EventDefinition",
		fixtures: vec![Fixture {
			name: "EVENT_BASIC",
			description: "table event trigger",
			value: Box::new(fix::event_basic()),
		}],
	}
}

/// Generate all fixtures for FieldDefinition
fn field_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "FieldDefinition",
		fixtures: vec![
			Fixture {
				name: "FIELD_BASIC",
				description: "minimal field",
				value: Box::new(fix::field_basic()),
			},
			Fixture {
				name: "FIELD_WITH_TYPE",
				description: "field with type constraint and default",
				value: Box::new(fix::field_with_type()),
			},
			Fixture {
				name: "FIELD_READONLY",
				description: "readonly computed field",
				value: Box::new(fix::field_readonly()),
			},
		],
	}
}

/// Generate all fixtures for FunctionDefinition
fn function_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "FunctionDefinition",
		fixtures: vec![
			Fixture {
				name: "FUNCTION_BASIC",
				description: "simple function",
				value: Box::new(fix::function_basic()),
			},
			Fixture {
				name: "FUNCTION_WITH_ARGS",
				description: "function with arguments and return type",
				value: Box::new(fix::function_with_args()),
			},
		],
	}
}

/// Generate all fixtures for IndexDefinition
fn index_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "IndexDefinition",
		fixtures: vec![
			Fixture {
				name: "INDEX_BASIC",
				description: "basic index",
				value: Box::new(fix::index_basic()),
			},
			Fixture {
				name: "INDEX_UNIQUE",
				description: "unique index on multiple columns",
				value: Box::new(fix::index_unique()),
			},
		],
	}
}

/// Generate all fixtures for MlModelDefinition
fn model_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "MlModelDefinition",
		fixtures: vec![Fixture {
			name: "MODEL_BASIC",
			description: "ML model definition",
			value: Box::new(fix::model_basic()),
		}],
	}
}

/// Generate all fixtures for ParamDefinition
fn param_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "ParamDefinition",
		fixtures: vec![
			Fixture {
				name: "PARAM_BOOL",
				description: "boolean parameter",
				value: Box::new(fix::param_bool()),
			},
			Fixture {
				name: "PARAM_STRING",
				description: "string parameter",
				value: Box::new(fix::param_string()),
			},
		],
	}
}

/// Generate all fixtures for SequenceDefinition
fn sequence_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "SequenceDefinition",
		fixtures: vec![
			Fixture {
				name: "SEQUENCE_BASIC",
				description: "minimal sequence",
				value: Box::new(fix::sequence_basic()),
			},
			Fixture {
				name: "SEQUENCE_WITH_OPTIONS",
				description: "sequence with custom options",
				value: Box::new(fix::sequence_with_options()),
			},
		],
	}
}

/// Generate all fixtures for UserDefinition
fn user_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "UserDefinition",
		fixtures: vec![
			Fixture {
				name: "USER_BASIC",
				description: "minimal user",
				value: Box::new(fix::user_basic()),
			},
			Fixture {
				name: "USER_WITH_DURATIONS",
				description: "user with custom token/session durations",
				value: Box::new(fix::user_with_durations()),
			},
		],
	}
}

/// Generate all fixtures for Record
fn record_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "Record",
		fixtures: vec![
			Fixture {
				name: "RECORD_NONE",
				description: "record with None value",
				value: Box::new(fix::record_none()),
			},
			Fixture {
				name: "RECORD_NULL",
				description: "record with Null value",
				value: Box::new(fix::record_null()),
			},
			Fixture {
				name: "RECORD_BOOL",
				description: "record with boolean data",
				value: Box::new(fix::record_bool()),
			},
			Fixture {
				name: "RECORD_NUMBER_INT",
				description: "record with int number data",
				value: Box::new(fix::record_number_int()),
			},
			Fixture {
				name: "RECORD_NUMBER_FLOAT",
				description: "record with float number data",
				value: Box::new(fix::record_number_float()),
			},
			Fixture {
				name: "RECORD_NUMBER_DECIMAL",
				description: "record with decimal number data",
				value: Box::new(fix::record_number_decimal()),
			},
			Fixture {
				name: "RECORD_STRING",
				description: "record with string data",
				value: Box::new(fix::record_string()),
			},
			Fixture {
				name: "RECORD_BYTES",
				description: "record with bytes data",
				value: Box::new(fix::record_bytes()),
			},
			Fixture {
				name: "RECORD_DURATION",
				description: "record with duration data",
				value: Box::new(fix::record_duration()),
			},
			Fixture {
				name: "RECORD_DATETIME",
				description: "record with datetime data",
				value: Box::new(fix::record_datetime()),
			},
			Fixture {
				name: "RECORD_UUID",
				description: "record with UUID data",
				value: Box::new(fix::record_uuid()),
			},
			Fixture {
				name: "RECORD_GEOMETRY_POINT",
				description: "record with geometry data (point)",
				value: Box::new(fix::record_geometry_point()),
			},
			Fixture {
				name: "RECORD_GEOMETRY_LINE",
				description: "record with geometry data (line)",
				value: Box::new(fix::record_geometry_line()),
			},
			Fixture {
				name: "RECORD_GEOMETRY_POLYGON",
				description: "record with geometry data (polygon)",
				value: Box::new(fix::record_geometry_polygon()),
			},
			Fixture {
				name: "RECORD_GEOMETRY_MULTI_POINT",
				description: "record with geometry data (multi point)",
				value: Box::new(fix::record_geometry_multi_point()),
			},
			Fixture {
				name: "RECORD_GEOMETRY_MULTI_LINE",
				description: "record with geometry data (multi line)",
				value: Box::new(fix::record_geometry_multi_line()),
			},
			Fixture {
				name: "RECORD_GEOMETRY_MULTI_POLYGON",
				description: "record with geometry data (multi polygon)",
				value: Box::new(fix::record_geometry_multi_polygon()),
			},
			Fixture {
				name: "RECORD_GEOMETRY_COLLECTION",
				description: "record with geometry data (collection)",
				value: Box::new(fix::record_geometry_collection()),
			},
			Fixture {
				name: "RECORD_TABLE",
				description: "record with table data",
				value: Box::new(fix::record_table()),
			},
			Fixture {
				name: "RECORD_RECORDID",
				description: "record with record ID data",
				value: Box::new(fix::record_recordid()),
			},
			Fixture {
				name: "RECORD_FILE",
				description: "record with file data",
				value: Box::new(fix::record_file()),
			},
			Fixture {
				name: "RECORD_RANGE_UNBOUNDED",
				description: "record with range data",
				value: Box::new(fix::record_range_unbounded()),
			},
			Fixture {
				name: "RECORD_RANGE_BOUNDED",
				description: "record with range data",
				value: Box::new(fix::record_range_bounded()),
			},
			Fixture {
				name: "RECORD_REGEX",
				description: "record with regex data",
				value: Box::new(fix::record_regex()),
			},
			Fixture {
				name: "RECORD_ARRAY",
				description: "record with array data",
				value: Box::new(fix::record_array()),
			},
			Fixture {
				name: "RECORD_OBJECT",
				description: "record with object data",
				value: Box::new(fix::record_object()),
			},
			Fixture {
				name: "RECORD_SET",
				description: "record with set data",
				value: Box::new(fix::record_set()),
			},
			Fixture {
				name: "RECORD_WITH_METADATA",
				description: "record with metadata (Edge type)",
				value: Box::new(fix::record_with_metadata()),
			},
		],
	}
}

/// Generate all fixtures for MajorVersion
fn version_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "MajorVersion",
		fixtures: vec![
			Fixture {
				name: "VERSION_1",
				description: "major version 1",
				value: Box::new(fix::version_1()),
			},
			Fixture {
				name: "VERSION_3",
				description: "major version 3",
				value: Box::new(fix::version_3()),
			},
		],
	}
}

/// Generate all fixtures for ApiActionDefinition
fn api_action_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "ApiActionDefinition",
		fixtures: vec![
			Fixture {
				name: "API_ACTION_BASIC",
				description: "minimal API action definition",
				value: Box::new(fix::api_action_basic()),
			},
			Fixture {
				name: "API_ACTION_MULTI_METHOD",
				description: "API action with multiple methods",
				value: Box::new(fix::api_action_multi_method()),
			},
		],
	}
}

/// Generate all fixtures for Appending
fn appending_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "Appending",
		fixtures: vec![
			Fixture {
				name: "APPENDING_NONE",
				description: "appending with None values",
				value: Box::new(fix::appending_none()),
			},
			Fixture {
				name: "APPENDING_OLD_VALUES",
				description: "appending with old values",
				value: Box::new(fix::appending_old_values()),
			},
			Fixture {
				name: "APPENDING_NEW_VALUES",
				description: "appending with new values",
				value: Box::new(fix::appending_new_values()),
			},
			Fixture {
				name: "APPENDING_BOTH",
				description: "appending with both old and new values",
				value: Box::new(fix::appending_both()),
			},
		],
	}
}

/// Generate all fixtures for PrimaryAppending
fn primary_appending_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "PrimaryAppending",
		fixtures: vec![Fixture {
			name: "PRIMARY_APPENDING_BASIC",
			description: "primary appending with number value",
			value: Box::new(fix::primary_appending_basic()),
		}],
	}
}

/// Generate all fixtures for BatchValue
fn batch_value_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "BatchValue",
		fixtures: vec![Fixture {
			name: "BATCH_VALUE_BASIC",
			description: "batch value with number value",
			value: Box::new(fix::batch_value_basic()),
		}],
	}
}

/// Generate all fixtures for SequenceState
fn sequence_state_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "SequenceState",
		fixtures: vec![Fixture {
			name: "SEQUENCE_STATE_BASIC",
			description: "sequence state with number value",
			value: Box::new(fix::sequence_state_basic()),
		}],
	}
}

/// Generate all fixtures for TaskLease
fn task_lease_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "TaskLease",
		fixtures: vec![Fixture {
			name: "TASK_LEASE_BASIC",
			description: "task lease with UUID and datetime value",
			value: Box::new(fix::task_lease_basic()),
		}],
	}
}
/// Generate all fixtures for ID types
fn id_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "ID Types",
		fixtures: vec![
			Fixture {
				name: "INDEX_ID_BASIC",
				description: "IndexId fixture",
				value: Box::new(fix::index_id_basic()),
			},
			Fixture {
				name: "DATABASE_ID_BASIC",
				description: "DatabaseId fixture",
				value: Box::new(fix::database_id_basic()),
			},
			Fixture {
				name: "NAMESPACE_ID_BASIC",
				description: "NamespaceId fixture",
				value: Box::new(fix::namespace_id_basic()),
			},
			Fixture {
				name: "TABLE_ID_BASIC",
				description: "TableId fixture",
				value: Box::new(fix::table_id_basic()),
			},
		],
	}
}

/// Generate all fixtures for ModuleDefinition
fn module_definition_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "ModuleDefinition",
		fixtures: vec![
			Fixture {
				name: "MODULE_SURREALISM",
				description: "module with Surrealism executable",
				value: Box::new(fix::module_surrealism()),
			},
			Fixture {
				name: "MODULE_SILO",
				description: "module with Silo executable",
				value: Box::new(fix::module_silo()),
			},
		],
	}
}

/// Generate all fixtures for NodeLiveQuery
fn node_live_query_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "NodeLiveQuery",
		fixtures: vec![Fixture {
			name: "NODE_LIVE_QUERY_BASIC",
			description: "minimal node live query",
			value: Box::new(fix::node_live_query_basic()),
		}],
	}
}

/// Generate all fixtures for TableMutations
fn table_mutations_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "TableMutations",
		fixtures: vec![
			Fixture {
				name: "TABLE_MUTATIONS_SET",
				description: "table mutations with set operation",
				value: Box::new(fix::table_mutations_set()),
			},
			Fixture {
				name: "TABLE_MUTATIONS_DEL",
				description: "table mutations with delete operation",
				value: Box::new(fix::table_mutations_del()),
			},
		],
	}
}

/// Generate all fixtures for Node
fn node_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "Node",
		fixtures: vec![
			Fixture {
				name: "NODE_ACTIVE",
				description: "active node",
				value: Box::new(fix::node_active()),
			},
			Fixture {
				name: "NODE_ARCHIVED",
				description: "archived node",
				value: Box::new(fix::node_archived()),
			},
		],
	}
}

/// Generate all fixtures for TermDocument
fn term_document_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "TermDocument",
		fixtures: vec![Fixture {
			name: "TERM_DOCUMENT_BASIC",
			description: "term document with offsets",
			value: Box::new(fix::term_document_basic()),
		}],
	}
}

/// Generate all fixtures for DocLengthAndCount
fn doc_length_and_count_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "DocLengthAndCount",
		fixtures: vec![Fixture {
			name: "DOC_LENGTH_AND_COUNT_BASIC",
			description: "document length and count",
			value: Box::new(fix::doc_length_and_count_basic()),
		}],
	}
}

/// Generate all fixtures for RecordId
fn recordid_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "RecordId",
		fixtures: vec![
			Fixture {
				name: "RECORDID_NUMBER",
				description: "RecordId with number key",
				value: Box::new(fix::recordid_number()),
			},
			Fixture {
				name: "RECORDID_STRING",
				description: "RecordId with string key",
				value: Box::new(fix::recordid_string()),
			},
			Fixture {
				name: "RECORDID_UUID",
				description: "RecordId with UUID key",
				value: Box::new(fix::recordid_uuid()),
			},
		],
	}
}

/// Generate all fixtures for RecordIdKey
fn recordid_key_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "RecordIdKey",
		fixtures: vec![
			Fixture {
				name: "RECORDID_KEY_NUMBER",
				description: "RecordIdKey with number",
				value: Box::new(fix::recordid_key_number()),
			},
			Fixture {
				name: "RECORDID_KEY_STRING",
				description: "RecordIdKey with string",
				value: Box::new(fix::recordid_key_string()),
			},
			Fixture {
				name: "RECORDID_KEY_UUID",
				description: "RecordIdKey with UUID",
				value: Box::new(fix::recordid_key_uuid()),
			},
			Fixture {
				name: "RECORDID_KEY_ARRAY",
				description: "RecordIdKey with array",
				value: Box::new(fix::recordid_key_array()),
			},
			Fixture {
				name: "RECORDID_KEY_OBJECT",
				description: "RecordIdKey with object",
				value: Box::new(fix::recordid_key_object()),
			},
		],
	}
}

/// Generate all fixtures and output as Rust code
fn generate_all_fixtures() -> String {
	let all_fixtures = vec![
		access_definition_fixtures(),
		access_grant_fixtures(),
		analyzer_fixtures(),
		api_action_fixtures(),
		api_fixtures(),
		appending_fixtures(),
		batch_value_fixtures(),
		bucket_fixtures(),
		config_fixtures(),
		database_fixtures(),
		doc_length_and_count_fixtures(),
		event_fixtures(),
		field_fixtures(),
		function_fixtures(),
		id_fixtures(),
		index_fixtures(),
		model_fixtures(),
		module_definition_fixtures(),
		namespace_fixtures(),
		node_fixtures(),
		node_live_query_fixtures(),
		param_fixtures(),
		primary_appending_fixtures(),
		record_fixtures(),
		recordid_fixtures(),
		recordid_key_fixtures(),
		sequence_fixtures(),
		sequence_state_fixtures(),
		subscription_fixtures(),
		table_fixtures(),
		table_mutations_fixtures(),
		task_lease_fixtures(),
		term_document_fixtures(),
		user_fixtures(),
		version_fixtures(),
	];

	let mut output = String::new();
	output.push_str("//! v3_0_0.rs - Generated file, DO NOT EDIT\n");
	output.push_str("//! Catalog compatibility fixtures for SurrealDB 3.0.0\n");
	output.push_str("//!\n");
	output.push_str("//! These fixtures represent the exact serialization format used in\n");
	output.push_str("//! SurrealDB 3.0.0. They must NEVER be modified after being committed.\n");
	output.push_str("//! If deserialization of any fixture fails, it indicates a backwards\n");
	output.push_str("//! compatibility regression.\n");

	for type_fixtures in all_fixtures {
		output.push('\n');
		output.push_str(&format!("// {}\n", "=".repeat(70)));
		output.push_str(&format!("// {}\n", type_fixtures.type_name));
		output.push_str(&format!("// {}\n\n", "=".repeat(70)));

		for fixture in &type_fixtures.fixtures {
			output.push_str(&format_fixture(type_fixtures.type_name, fixture));
			output.push('\n');
		}
	}

	output
}

/// Test that generates fixture output - run with --ignored flag
#[test]
#[ignore]
fn generator() {
	use sha2::{Digest, Sha256};

	let output = generate_all_fixtures();
	println!("Copy the following output to surrealdb/core/src/catalog/compat/v3_0_0.rs");
	println!("--- EVERYTHING BELOW ---");
	println!("{}", output);
	println!("--- EVERYTHING ABOVE ---");
	println!("\n// Copy the above output to surrealdb/core/src/catalog/compat/v3_0_0.rs");

	let hash = Sha256::digest(output.as_bytes());
	let hash_str = hex::encode(hash);
	println!("The expected hash is: {}", hash_str);
}

#[test]
fn test_v3_0_0_beta_1_remains_unchanged() {
	use sha2::{Digest, Sha256};

	// Read the v3_0_0_beta_1.rs file, hash it and assert on the hash.
	let v3_0_0_beta_1 = include_bytes!("v3_0_0_beta_1.rs");
	let hash = Sha256::digest(v3_0_0_beta_1);
	let hash_str = hex::encode(hash);
	assert_eq!(hash_str, "def0c55d4279b9429795f9e2ff443309a8a243c3dc4bf593fd38e0109c6f53f2");
}

#[test]
fn test_v3_0_0_beta_3_remains_unchanged() {
	use sha2::{Digest, Sha256};

	// Read the v3_0_0_beta_3.rs file, hash it and assert on the hash.
	let v3_0_0_beta_3 = include_bytes!("v3_0_0_beta_3.rs");
	let hash = Sha256::digest(v3_0_0_beta_3);
	let hash_str = hex::encode(hash);
	assert_eq!(hash_str, "696a85c143d53c01f3f842ee45cc64e45e4d9c1251c99e083467d07db8c29805");
}
