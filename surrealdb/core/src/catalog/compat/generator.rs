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
///
/// Bytes are captured eagerly because `KVValue` is no longer object-safe
/// (associated `KeyContext`), so `Box<dyn KVValue>` doesn't work for the
/// heterogeneous fixture list.
struct Fixture {
	name: &'static str,
	description: &'static str,
	bytes: Vec<u8>,
}

/// A collection of fixtures for a single type.
struct TypeFixtures {
	type_name: &'static str,
	fixtures: Vec<Fixture>,
}

/// Format bytes as a Rust const array.
fn format_bytes(bytes: &[u8]) -> String {
	let hex_bytes: Vec<String> = bytes.iter().map(|b| format!("0x{:02x}", b)).collect();

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
		bytes = format_bytes(&fixture.bytes)
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
				bytes: fix::namespace_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "NAMESPACE_WITH_COMMENT",
				description: "namespace with optional comment",
				bytes: fix::namespace_with_comment().kv_encode_value().unwrap(),
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
				bytes: fix::database_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "DATABASE_WITH_CHANGEFEED",
				description: "database with changefeed enabled",
				bytes: fix::database_with_changefeed().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "DATABASE_STRICT",
				description: "database with strict mode enabled",
				bytes: fix::database_strict().kv_encode_value().unwrap(),
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
				bytes: fix::table_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "TABLE_WITH_VIEW",
				description: "table with view definition",
				bytes: fix::table_with_view().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "TABLE_SCHEMAFULL",
				description: "schemafull table with changefeed",
				bytes: fix::table_schemafull().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "TABLE_RELATION",
				description: "relation table with drop and non-default permissions",
				bytes: fix::table_relation().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "TABLE_WITH_MATERIALIZED_VIEW",
				description: "table with materialized view",
				bytes: fix::table_with_materialized_view().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "TABLE_ANY_TYPE",
				description: "table with TableType::Any",
				bytes: fix::table_any_type().kv_encode_value().unwrap(),
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
				bytes: fix::subscription_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "SUBSCRIPTION_WITH_FILTERS",
				description: "subscription with condition and fetch",
				bytes: fix::subscription_with_filters().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "SUBSCRIPTION_WITH_VARS",
				description: "subscription with non-empty vars",
				bytes: fix::subscription_with_vars().kv_encode_value().unwrap(),
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
				bytes: fix::access_bearer().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "ACCESS_WITH_AUTHENTICATE",
				description: "access with custom authenticate expression",
				bytes: fix::access_with_authenticate().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "ACCESS_RECORD",
				description: "record-based access with signup/signin",
				bytes: fix::access_record().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "ACCESS_JWT_JWKS",
				description: "JWT access with JWKS verification",
				bytes: fix::access_jwt_jwks().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "ACCESS_BEARER_REFRESH",
				description: "bearer access with refresh type",
				bytes: fix::access_bearer_refresh().kv_encode_value().unwrap(),
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
				bytes: fix::grant_jwt().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "GRANT_REVOKED",
				description: "revoked access grant",
				bytes: fix::grant_revoked().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "GRANT_RECORD",
				description: "record-type access grant with record subject",
				bytes: fix::grant_record().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "GRANT_BEARER",
				description: "bearer-type access grant",
				bytes: fix::grant_bearer().kv_encode_value().unwrap(),
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
				bytes: fix::analyzer_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "ANALYZER_WITH_TOKENIZERS",
				description: "analyzer with tokenizers and filters",
				bytes: fix::analyzer_with_tokenizers().kv_encode_value().unwrap(),
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
				bytes: fix::api_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "API_WITH_MIDDLEWARE",
				description: "API with middleware and multiple methods",
				bytes: fix::api_with_middleware().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "API_WITH_AUTH_LIMIT",
				description: "API with specific permissions and database-level auth limit",
				bytes: fix::api_with_auth_limit().kv_encode_value().unwrap(),
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
				bytes: fix::bucket_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "BUCKET_READONLY",
				description: "readonly bucket with backend",
				bytes: fix::bucket_readonly().kv_encode_value().unwrap(),
			},
		],
	}
}

/// Generate all fixtures for ConfigDefinition
fn config_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "ConfigDefinition",
		fixtures: vec![
			Fixture {
				name: "CONFIG_GRAPHQL",
				description: "GraphQL configuration (default)",
				bytes: fix::config_graphql().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "CONFIG_DEFAULT",
				description: "default config with namespace and database",
				bytes: fix::config_default().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "CONFIG_API",
				description: "API config definition",
				bytes: fix::config_api().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "CONFIG_GRAPHQL_FULL",
				description: "GraphQL config with all non-default fields",
				bytes: fix::config_graphql_full().kv_encode_value().unwrap(),
			},
		],
	}
}

/// Generate all fixtures for EventDefinition
fn event_fixtures() -> TypeFixtures {
	TypeFixtures {
		type_name: "EventDefinition",
		fixtures: vec![
			Fixture {
				name: "EVENT_BASIC",
				description: "table event trigger",
				bytes: fix::event_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "EVENT_ASYNC",
				description: "async event with retry and max_depth",
				bytes: fix::event_async().kv_encode_value().unwrap(),
			},
		],
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
				bytes: fix::field_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "FIELD_WITH_TYPE",
				description: "field with type constraint and default",
				bytes: fix::field_with_type().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "FIELD_READONLY",
				description: "readonly computed field",
				bytes: fix::field_readonly().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "FIELD_FLEXIBLE_WITH_REFERENCE",
				description: "flexible field with reference and computed deps",
				bytes: fix::field_flexible_with_reference().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "FIELD_WITH_DEFAULT_SET",
				description: "field with DefineDefault::Set and incomplete computed deps",
				bytes: fix::field_with_default_set().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "FIELD_RECORD_TYPE",
				description: "field with record type kind and custom reference delete",
				bytes: fix::field_record_type().kv_encode_value().unwrap(),
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
				bytes: fix::function_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "FUNCTION_WITH_ARGS",
				description: "function with arguments and return type",
				bytes: fix::function_with_args().kv_encode_value().unwrap(),
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
				bytes: fix::index_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "INDEX_UNIQUE",
				description: "unique index on multiple columns",
				bytes: fix::index_unique().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "INDEX_HNSW",
				description: "HNSW vector index",
				bytes: fix::index_hnsw().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "INDEX_FULLTEXT",
				description: "full-text search index with BM25 scoring",
				bytes: fix::index_fulltext().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "INDEX_COUNT",
				description: "count index with prepare_remove flag",
				bytes: fix::index_count().kv_encode_value().unwrap(),
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
			bytes: fix::model_basic().kv_encode_value().unwrap(),
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
				bytes: fix::param_bool().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "PARAM_STRING",
				description: "string parameter",
				bytes: fix::param_string().kv_encode_value().unwrap(),
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
				bytes: fix::sequence_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "SEQUENCE_WITH_OPTIONS",
				description: "sequence with custom options",
				bytes: fix::sequence_with_options().kv_encode_value().unwrap(),
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
				bytes: fix::user_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "USER_WITH_DURATIONS",
				description: "user with custom token/session durations",
				bytes: fix::user_with_durations().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "USER_DB_BASE",
				description: "user with database-level base",
				bytes: fix::user_db_base().kv_encode_value().unwrap(),
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
				bytes: fix::record_none().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_NULL",
				description: "record with Null value",
				bytes: fix::record_null().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_BOOL",
				description: "record with boolean data",
				bytes: fix::record_bool().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_NUMBER_INT",
				description: "record with int number data",
				bytes: fix::record_number_int().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_NUMBER_FLOAT",
				description: "record with float number data",
				bytes: fix::record_number_float().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_NUMBER_DECIMAL",
				description: "record with decimal number data",
				bytes: fix::record_number_decimal().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_STRING",
				description: "record with string data",
				bytes: fix::record_string().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_BYTES",
				description: "record with bytes data",
				bytes: fix::record_bytes().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_DURATION",
				description: "record with duration data",
				bytes: fix::record_duration().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_DATETIME",
				description: "record with datetime data",
				bytes: fix::record_datetime().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_UUID",
				description: "record with UUID data",
				bytes: fix::record_uuid().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_GEOMETRY_POINT",
				description: "record with geometry data (point)",
				bytes: fix::record_geometry_point().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_GEOMETRY_LINE",
				description: "record with geometry data (line)",
				bytes: fix::record_geometry_line().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_GEOMETRY_POLYGON",
				description: "record with geometry data (polygon)",
				bytes: fix::record_geometry_polygon().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_GEOMETRY_MULTI_POINT",
				description: "record with geometry data (multi point)",
				bytes: fix::record_geometry_multi_point().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_GEOMETRY_MULTI_LINE",
				description: "record with geometry data (multi line)",
				bytes: fix::record_geometry_multi_line().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_GEOMETRY_MULTI_POLYGON",
				description: "record with geometry data (multi polygon)",
				bytes: fix::record_geometry_multi_polygon().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_GEOMETRY_COLLECTION",
				description: "record with geometry data (collection)",
				bytes: fix::record_geometry_collection().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_TABLE",
				description: "record with table data",
				bytes: fix::record_table().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_RECORDID",
				description: "record with record ID data",
				bytes: fix::record_recordid().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_FILE",
				description: "record with file data",
				bytes: fix::record_file().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_RANGE_UNBOUNDED",
				description: "record with range data",
				bytes: fix::record_range_unbounded().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_RANGE_BOUNDED",
				description: "record with range data",
				bytes: fix::record_range_bounded().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_REGEX",
				description: "record with regex data",
				bytes: fix::record_regex().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_ARRAY",
				description: "record with array data",
				bytes: fix::record_array().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_OBJECT",
				description: "record with object data",
				bytes: fix::record_object().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_SET",
				description: "record with set data",
				bytes: fix::record_set().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_WITH_METADATA",
				description: "record with metadata (Edge type)",
				bytes: fix::record_with_metadata().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORD_WITH_TABLE_METADATA",
				description: "record with explicit Table metadata type",
				bytes: fix::record_with_table_metadata().kv_encode_value().unwrap(),
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
				bytes: fix::version_1().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "VERSION_3",
				description: "major version 3",
				bytes: fix::version_3().kv_encode_value().unwrap(),
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
				bytes: fix::api_action_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "API_ACTION_MULTI_METHOD",
				description: "API action with multiple methods",
				bytes: fix::api_action_multi_method().kv_encode_value().unwrap(),
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
				bytes: fix::appending_none().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "APPENDING_OLD_VALUES",
				description: "appending with old values",
				bytes: fix::appending_old_values().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "APPENDING_NEW_VALUES",
				description: "appending with new values",
				bytes: fix::appending_new_values().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "APPENDING_BOTH",
				description: "appending with both old and new values",
				bytes: fix::appending_both().kv_encode_value().unwrap(),
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
			bytes: fix::primary_appending_basic().kv_encode_value().unwrap(),
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
			bytes: fix::batch_value_basic().kv_encode_value().unwrap(),
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
			bytes: fix::sequence_state_basic().kv_encode_value().unwrap(),
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
			bytes: fix::task_lease_basic().kv_encode_value().unwrap(),
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
				bytes: fix::index_id_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "DATABASE_ID_BASIC",
				description: "DatabaseId fixture",
				bytes: fix::database_id_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "NAMESPACE_ID_BASIC",
				description: "NamespaceId fixture",
				bytes: fix::namespace_id_basic().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "TABLE_ID_BASIC",
				description: "TableId fixture",
				bytes: fix::table_id_basic().kv_encode_value().unwrap(),
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
				bytes: fix::module_surrealism().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "MODULE_SILO",
				description: "module with Silo executable",
				bytes: fix::module_silo().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "MODULE_NO_NAME",
				description: "module with no name and Permission::None",
				bytes: fix::module_no_name().kv_encode_value().unwrap(),
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
			bytes: fix::node_live_query_basic().kv_encode_value().unwrap(),
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
				bytes: fix::table_mutations_set().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "TABLE_MUTATIONS_DEL",
				description: "table mutations with delete operation",
				bytes: fix::table_mutations_del().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "TABLE_MUTATIONS_DEF",
				description: "table mutations with Def operation",
				bytes: fix::table_mutations_def().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "TABLE_MUTATIONS_SET_WITH_DIFF",
				description: "table mutations with SetWithDiff operation",
				bytes: fix::table_mutations_set_with_diff().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "TABLE_MUTATIONS_DEL_WITH_ORIGINAL",
				description: "table mutations with DelWithOriginal operation",
				bytes: fix::table_mutations_del_with_original().kv_encode_value().unwrap(),
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
				bytes: fix::node_active().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "NODE_ARCHIVED",
				description: "archived node",
				bytes: fix::node_archived().kv_encode_value().unwrap(),
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
			bytes: fix::term_document_basic().kv_encode_value().unwrap(),
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
			bytes: fix::doc_length_and_count_basic().kv_encode_value().unwrap(),
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
				bytes: fix::recordid_number().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORDID_STRING",
				description: "RecordId with string key",
				bytes: fix::recordid_string().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORDID_UUID",
				description: "RecordId with UUID key",
				bytes: fix::recordid_uuid().kv_encode_value().unwrap(),
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
				bytes: fix::recordid_key_number().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORDID_KEY_STRING",
				description: "RecordIdKey with string",
				bytes: fix::recordid_key_string().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORDID_KEY_UUID",
				description: "RecordIdKey with UUID",
				bytes: fix::recordid_key_uuid().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORDID_KEY_ARRAY",
				description: "RecordIdKey with array",
				bytes: fix::recordid_key_array().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORDID_KEY_OBJECT",
				description: "RecordIdKey with object",
				bytes: fix::recordid_key_object().kv_encode_value().unwrap(),
			},
			Fixture {
				name: "RECORDID_KEY_RANGE",
				description: "RecordIdKey with range",
				bytes: fix::recordid_key_range().kv_encode_value().unwrap(),
			},
		],
	}
}

/// Generate all fixtures and output as Rust code.
///
/// `file_stem` is the version-tag identifier used in the file name and
/// header (e.g. `"v3_0_0"`, `"v3_1_0"`). `human_version` is the
/// release-version string for the docstring (e.g. `"3.0.0"`, `"3.1.0"`).
fn generate_all_fixtures(file_stem: &str, human_version: &str) -> String {
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
	output.push_str(&format!("//! {file_stem}.rs - Generated file, DO NOT EDIT\n"));
	output.push_str(&format!("//! Catalog compatibility fixtures for SurrealDB {human_version}\n"));
	output.push_str("//!\n");
	output.push_str("//! These fixtures represent the exact serialization format used in\n");
	output.push_str(&format!(
		"//! SurrealDB {human_version}. They must NEVER be modified after being committed.\n"
	));
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

/// Shared body for the version-specific generator tests below.
fn run_generator(file_stem: &str, human_version: &str) {
	use sha2::{Digest, Sha256};

	let output = generate_all_fixtures(file_stem, human_version);
	println!("Copy the following output to surrealdb/core/src/catalog/compat/{file_stem}.rs");
	println!("--- EVERYTHING BELOW ---");
	println!("{}", output);
	println!("--- EVERYTHING ABOVE ---");
	println!("\n// Copy the above output to surrealdb/core/src/catalog/compat/{file_stem}.rs");

	let hash = Sha256::digest(output.as_bytes());
	let hash_str = hex::encode(hash);
	println!("The expected hash is: {}", hash_str);
}

/// Test that generates fixture output for 3.0.0 - run with --ignored flag.
/// Kept for parity with the original generator; the v3_0_0.rs file is
/// frozen at this point so re-running this should only be useful for
/// regenerating against a fresh fixtures.rs (which would also break the
/// hash check below).
#[test]
#[ignore]
fn generator() {
	run_generator("v3_0_0", "3.0.0");
}

/// Generate fixture bytes for the 3.1.0 wire format snapshot. Run with:
///
/// ```text
/// cargo test -p surrealdb-core --lib \
///     catalog::compat::generator::generator_v3_1_0 -- --ignored --nocapture
/// ```
///
/// Copy the output into `v3_1_0.rs`, then paste the printed hash into
/// the assertion in `test_v3_1_0_remains_unchanged` below.
#[test]
#[ignore]
fn generator_v3_1_0() {
	run_generator("v3_1_0", "3.1.0");
}

/// Generate fixture bytes for the 3.1.1 wire format snapshot. Run with:
///
/// ```text
/// cargo test -p surrealdb-core --lib \
///     catalog::compat::generator::generator_v3_1_1 -- --ignored --nocapture
/// ```
///
/// Copy the output into `v3_1_1.rs`, then paste the printed hash into
/// the assertion in `test_v3_1_1_remains_unchanged` below.
///
/// 3.1.1 stopped stripping the top-level `id` field from `Record` data,
/// so the `RECORD_OBJECT` / `RECORD_WITH_METADATA` /
/// `RECORD_WITH_TABLE_METADATA` fixtures now carry the id inline; every
/// other fixture is byte-identical to 3.1.0.
#[test]
#[ignore]
fn generator_v3_1_1() {
	run_generator("v3_1_1", "3.1.1");
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

#[test]
fn test_v3_0_0_remains_unchanged() {
	use sha2::{Digest, Sha256};

	// Read the v3_0_0.rs file, hash it and assert on the hash.
	let v3_0_0 = include_bytes!("v3_0_0.rs");
	let hash = Sha256::digest(v3_0_0);
	let hash_str = hex::encode(hash);
	assert_eq!(hash_str, "042aa56204bff3007e371be6968e9c684430556f32654b5cee7107a5c1677f4d");
}

#[test]
fn test_v3_1_0_remains_unchanged() {
	use sha2::{Digest, Sha256};

	// Read the v3_1_0.rs file, hash it and assert on the hash.
	//
	// v3_1_0 captures the rev-2 wire format (Value's rev-2 optimised
	// walker — see `surrealdb/core/src/val/mod.rs`), regenerated when
	// `Record` was bumped to `revision(2, optimised, indexed_struct)` so
	// the data field's bytes can be sliced in O(1) by the pre-decode
	// filter's descent path (via
	// `Record::walk_revisioned(...)?.into_data_bytes()?`).
	//
	// NEVER modify v3_1_0.rs after commit; if a real format change ships,
	// capture a new version snapshot rather than rotating this hash.
	let v3_1_0 = include_bytes!("v3_1_0.rs");
	let hash = Sha256::digest(v3_1_0);
	let hash_str = hex::encode(hash);
	assert_eq!(hash_str, "84897ab9a06cf136d1af5bb8ee0005462ebd56726de822724ff60c6dc3ba23a9");
}

#[test]
fn test_v3_1_1_remains_unchanged() {
	use sha2::{Digest, Sha256};

	// Read the v3_1_1.rs file, hash it and assert on the hash.
	//
	// v3_1_1 captures the wire format after `Record` stopped stripping the
	// top-level `id` field from its `data` (it is now stored inline; the
	// decoder only synthesises it from the storage key for legacy 3.1.0
	// data). Only the `RECORD_OBJECT` / `RECORD_WITH_METADATA` /
	// `RECORD_WITH_TABLE_METADATA` fixtures differ from 3.1.0; everything
	// else is byte-identical.
	//
	// NEVER modify v3_1_1.rs after commit; if a real format change ships,
	// capture a new version snapshot rather than rotating this hash.
	let v3_1_1 = include_bytes!("v3_1_1.rs");
	let hash = Sha256::digest(v3_1_1);
	let hash_str = hex::encode(hash);
	assert_eq!(hash_str, "f7d260a6bbd3d9efba605f550b009c1c6ad3a82fab79578bf3611b1acc8802ae");
}
