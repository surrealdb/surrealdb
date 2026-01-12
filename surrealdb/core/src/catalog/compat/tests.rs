//! Catalog backwards compatibility tests.
//!
//! These tests verify that:
//! 1. Serialized data from previous versions can be deserialized
//! 2. The deserialized values EXACTLY match the expected fixtures
//!
//! Failing either check indicates a backwards compatibility regression.

use super::super::*;
use super::{fixtures, v3_0_0 as bytes};
use crate::cf::TableMutations;
use crate::dbs::node::Node;
use crate::idx::ft::fulltext::{DocLengthAndCount, TermDocument};
use crate::kvs::KVValue;
use crate::kvs::index::{Appending, PrimaryAppending};
use crate::kvs::sequences::{BatchValue, SequenceState};
use crate::kvs::tasklease::TaskLease;
use crate::kvs::version::MajorVersion;
use crate::val::{RecordId, RecordIdKey};

/// Macro to generate backwards compatibility tests for a fixture.
///
/// This macro creates a test that:
/// 1. Decodes the fixture bytes using `kv_decode_value`
/// 2. Compares the decoded value against the expected fixture value
/// 3. Fails loudly if decoding fails OR if values don't match
macro_rules! compat_test {
	($name:ident, $type:ty, $bytes:expr, $expected:expr) => {
		#[test]
		fn $name() {
			let fixture_bytes = $bytes;

			// Attempt to decode - this MUST succeed for backwards compatibility
			let decoded = <$type>::kv_decode_value(fixture_bytes.to_vec()).unwrap_or_else(|e| {
				panic!(
					concat!(
						"BACKWARDS COMPATIBILITY BROKEN: Failed to decode ",
						stringify!($name),
						" fixture.\n",
						"Type: ",
						stringify!($type),
						"\n",
						"Error: {}\n",
						"Bytes: {:?}\n\n",
						"This indicates that the serialization format has changed in an ",
						"incompatible way. Old databases may fail to load.\n",
						"If this change is intentional, you MUST implement a migration path."
					),
					e, fixture_bytes
				)
			});

			// Get the expected value from fixtures
			let expected = $expected;

			// Assert that the decoded value matches the expected fixture
			assert_eq!(
				decoded, expected,
				concat!(
					"BACKWARDS COMPATIBILITY BROKEN: Decoded value does not match expected ",
					"fixture for ",
					stringify!($name),
					".\n\n",
					"This indicates that while deserialization succeeded, the data was ",
					"interpreted differently than expected. This could cause data corruption ",
					"or unexpected behavior when loading old databases.\n\n",
					"If this change is intentional, update the fixture in fixtures.rs to ",
					"reflect how old data should be interpreted by the current code."
				)
			);

			// Also verify re-encoding works (bytes may differ due to normalization)
			let _re_encoded = decoded.kv_encode_value().unwrap_or_else(|e| {
				panic!(
					concat!(
						"Failed to re-encode ",
						stringify!($name),
						" after successful decode.\n",
						"Error: {}"
					),
					e
				)
			});
		}
	};
}

// =============================================================================
// Version 3.0.0 Compatibility Tests
// =============================================================================

// NamespaceDefinition
compat_test!(
	v3_0_0_namespace_basic,
	NamespaceDefinition,
	bytes::NAMESPACE_BASIC,
	fixtures::namespace_basic()
);
compat_test!(
	v3_0_0_namespace_with_comment,
	NamespaceDefinition,
	bytes::NAMESPACE_WITH_COMMENT,
	fixtures::namespace_with_comment()
);

// DatabaseDefinition
compat_test!(
	v3_0_0_database_basic,
	DatabaseDefinition,
	bytes::DATABASE_BASIC,
	fixtures::database_basic()
);
compat_test!(
	v3_0_0_database_with_changefeed,
	DatabaseDefinition,
	bytes::DATABASE_WITH_CHANGEFEED,
	fixtures::database_with_changefeed()
);
compat_test!(
	v3_0_0_database_strict,
	DatabaseDefinition,
	bytes::DATABASE_STRICT,
	fixtures::database_strict()
);

// TableDefinition
compat_test!(v3_0_0_table_basic, TableDefinition, bytes::TABLE_BASIC, fixtures::table_basic());
compat_test!(
	v3_0_0_table_with_view,
	TableDefinition,
	bytes::TABLE_WITH_VIEW,
	fixtures::table_with_view()
);
compat_test!(
	v3_0_0_table_schemafull,
	TableDefinition,
	bytes::TABLE_SCHEMAFULL,
	fixtures::table_schemafull()
);

// SubscriptionDefinition
compat_test!(
	v3_0_0_subscription_basic,
	SubscriptionDefinition,
	bytes::SUBSCRIPTION_BASIC,
	fixtures::subscription_basic()
);
compat_test!(
	v3_0_0_subscription_with_filters,
	SubscriptionDefinition,
	bytes::SUBSCRIPTION_WITH_FILTERS,
	fixtures::subscription_with_filters()
);

// AccessDefinition
compat_test!(
	v3_0_0_access_bearer,
	AccessDefinition,
	bytes::ACCESS_BEARER,
	fixtures::access_bearer()
);
compat_test!(
	v3_0_0_access_with_authenticate,
	AccessDefinition,
	bytes::ACCESS_WITH_AUTHENTICATE,
	fixtures::access_with_authenticate()
);

// AccessGrant
compat_test!(v3_0_0_grant_jwt, AccessGrant, bytes::GRANT_JWT, fixtures::grant_jwt());
compat_test!(v3_0_0_grant_revoked, AccessGrant, bytes::GRANT_REVOKED, fixtures::grant_revoked());

// AnalyzerDefinition
compat_test!(
	v3_0_0_analyzer_basic,
	AnalyzerDefinition,
	bytes::ANALYZER_BASIC,
	fixtures::analyzer_basic()
);
compat_test!(
	v3_0_0_analyzer_with_tokenizers,
	AnalyzerDefinition,
	bytes::ANALYZER_WITH_TOKENIZERS,
	fixtures::analyzer_with_tokenizers()
);

// ApiDefinition
compat_test!(v3_0_0_api_basic, ApiDefinition, bytes::API_BASIC, fixtures::api_basic());
compat_test!(
	v3_0_0_api_with_middleware,
	ApiDefinition,
	bytes::API_WITH_MIDDLEWARE,
	fixtures::api_with_middleware()
);

// BucketDefinition
compat_test!(v3_0_0_bucket_basic, BucketDefinition, bytes::BUCKET_BASIC, fixtures::bucket_basic());
compat_test!(
	v3_0_0_bucket_readonly,
	BucketDefinition,
	bytes::BUCKET_READONLY,
	fixtures::bucket_readonly()
);

// ConfigDefinition
compat_test!(
	v3_0_0_config_graphql,
	ConfigDefinition,
	bytes::CONFIG_GRAPHQL,
	fixtures::config_graphql()
);

// EventDefinition
compat_test!(v3_0_0_event_basic, EventDefinition, bytes::EVENT_BASIC, fixtures::event_basic());

// FieldDefinition
compat_test!(v3_0_0_field_basic, FieldDefinition, bytes::FIELD_BASIC, fixtures::field_basic());
compat_test!(
	v3_0_0_field_with_type,
	FieldDefinition,
	bytes::FIELD_WITH_TYPE,
	fixtures::field_with_type()
);
compat_test!(
	v3_0_0_field_readonly,
	FieldDefinition,
	bytes::FIELD_READONLY,
	fixtures::field_readonly()
);

// FunctionDefinition
compat_test!(
	v3_0_0_function_basic,
	FunctionDefinition,
	bytes::FUNCTION_BASIC,
	fixtures::function_basic()
);
compat_test!(
	v3_0_0_function_with_args,
	FunctionDefinition,
	bytes::FUNCTION_WITH_ARGS,
	fixtures::function_with_args()
);

// IndexDefinition
compat_test!(v3_0_0_index_basic, IndexDefinition, bytes::INDEX_BASIC, fixtures::index_basic());
compat_test!(v3_0_0_index_unique, IndexDefinition, bytes::INDEX_UNIQUE, fixtures::index_unique());

// MlModelDefinition
compat_test!(v3_0_0_model_basic, MlModelDefinition, bytes::MODEL_BASIC, fixtures::model_basic());

// ParamDefinition
compat_test!(v3_0_0_param_bool, ParamDefinition, bytes::PARAM_BOOL, fixtures::param_bool());
compat_test!(v3_0_0_param_string, ParamDefinition, bytes::PARAM_STRING, fixtures::param_string());

// SequenceDefinition
compat_test!(
	v3_0_0_sequence_basic,
	SequenceDefinition,
	bytes::SEQUENCE_BASIC,
	fixtures::sequence_basic()
);
compat_test!(
	v3_0_0_sequence_with_options,
	SequenceDefinition,
	bytes::SEQUENCE_WITH_OPTIONS,
	fixtures::sequence_with_options()
);

// UserDefinition
compat_test!(v3_0_0_user_basic, UserDefinition, bytes::USER_BASIC, fixtures::user_basic());
compat_test!(
	v3_0_0_user_with_durations,
	UserDefinition,
	bytes::USER_WITH_DURATIONS,
	fixtures::user_with_durations()
);

// Record
compat_test!(v3_0_0_record_none, Record, bytes::RECORD_NONE, fixtures::record_none());
compat_test!(v3_0_0_record_null, Record, bytes::RECORD_NULL, fixtures::record_null());
compat_test!(v3_0_0_record_bool, Record, bytes::RECORD_BOOL, fixtures::record_bool());
compat_test!(
	v3_0_0_record_number_int,
	Record,
	bytes::RECORD_NUMBER_INT,
	fixtures::record_number_int()
);
compat_test!(
	v3_0_0_record_number_float,
	Record,
	bytes::RECORD_NUMBER_FLOAT,
	fixtures::record_number_float()
);
compat_test!(
	v3_0_0_record_number_decimal,
	Record,
	bytes::RECORD_NUMBER_DECIMAL,
	fixtures::record_number_decimal()
);
compat_test!(v3_0_0_record_string, Record, bytes::RECORD_STRING, fixtures::record_string());
compat_test!(v3_0_0_record_bytes, Record, bytes::RECORD_BYTES, fixtures::record_bytes());
compat_test!(v3_0_0_record_duration, Record, bytes::RECORD_DURATION, fixtures::record_duration());
compat_test!(v3_0_0_record_datetime, Record, bytes::RECORD_DATETIME, fixtures::record_datetime());
compat_test!(v3_0_0_record_uuid, Record, bytes::RECORD_UUID, fixtures::record_uuid());
compat_test!(
	v3_0_0_record_geometry_point,
	Record,
	bytes::RECORD_GEOMETRY_POINT,
	fixtures::record_geometry_point()
);
compat_test!(
	v3_0_0_record_geometry_line,
	Record,
	bytes::RECORD_GEOMETRY_LINE,
	fixtures::record_geometry_line()
);
compat_test!(
	v3_0_0_record_geometry_polygon,
	Record,
	bytes::RECORD_GEOMETRY_POLYGON,
	fixtures::record_geometry_polygon()
);
compat_test!(
	v3_0_0_record_geometry_multi_point,
	Record,
	bytes::RECORD_GEOMETRY_MULTI_POINT,
	fixtures::record_geometry_multi_point()
);
compat_test!(
	v3_0_0_record_geometry_multi_line,
	Record,
	bytes::RECORD_GEOMETRY_MULTI_LINE,
	fixtures::record_geometry_multi_line()
);
compat_test!(
	v3_0_0_record_geometry_multi_polygon,
	Record,
	bytes::RECORD_GEOMETRY_MULTI_POLYGON,
	fixtures::record_geometry_multi_polygon()
);
compat_test!(
	v3_0_0_record_geometry_collection,
	Record,
	bytes::RECORD_GEOMETRY_COLLECTION,
	fixtures::record_geometry_collection()
);
compat_test!(v3_0_0_record_table, Record, bytes::RECORD_TABLE, fixtures::record_table());
compat_test!(v3_0_0_record_recordid, Record, bytes::RECORD_RECORDID, fixtures::record_recordid());
compat_test!(v3_0_0_record_file, Record, bytes::RECORD_FILE, fixtures::record_file());
compat_test!(
	v3_0_0_record_range_unbounded,
	Record,
	bytes::RECORD_RANGE_UNBOUNDED,
	fixtures::record_range_unbounded()
);
compat_test!(
	v3_0_0_record_range_bounded,
	Record,
	bytes::RECORD_RANGE_BOUNDED,
	fixtures::record_range_bounded()
);
compat_test!(v3_0_0_record_regex, Record, bytes::RECORD_REGEX, fixtures::record_regex());
compat_test!(v3_0_0_record_array, Record, bytes::RECORD_ARRAY, fixtures::record_array());
compat_test!(v3_0_0_record_object, Record, bytes::RECORD_OBJECT, fixtures::record_object());
compat_test!(v3_0_0_record_set, Record, bytes::RECORD_SET, fixtures::record_set());
compat_test!(
	v3_0_0_record_with_metadata,
	Record,
	bytes::RECORD_WITH_METADATA,
	fixtures::record_with_metadata()
);

// MajorVersion
compat_test!(v3_0_0_version_1, MajorVersion, bytes::VERSION_1, fixtures::version_1());
compat_test!(v3_0_0_version_3, MajorVersion, bytes::VERSION_3, fixtures::version_3());

// ApiActionDefinition
compat_test!(
	v3_0_0_api_action_basic,
	ApiActionDefinition,
	bytes::API_ACTION_BASIC,
	fixtures::api_action_basic()
);
compat_test!(
	v3_0_0_api_action_multi_method,
	ApiActionDefinition,
	bytes::API_ACTION_MULTI_METHOD,
	fixtures::api_action_multi_method()
);

// Appending
compat_test!(v3_0_0_appending_none, Appending, bytes::APPENDING_NONE, fixtures::appending_none());
compat_test!(
	v3_0_0_appending_old_values,
	Appending,
	bytes::APPENDING_OLD_VALUES,
	fixtures::appending_old_values()
);
compat_test!(
	v3_0_0_appending_new_values,
	Appending,
	bytes::APPENDING_NEW_VALUES,
	fixtures::appending_new_values()
);
compat_test!(v3_0_0_appending_both, Appending, bytes::APPENDING_BOTH, fixtures::appending_both());

// DocLengthAndCount
compat_test!(
	v3_0_0_doc_length_and_count_basic,
	DocLengthAndCount,
	bytes::DOC_LENGTH_AND_COUNT_BASIC,
	fixtures::doc_length_and_count_basic()
);

// PrimaryAppending
compat_test!(
	v3_0_0_primary_appending_basic,
	PrimaryAppending,
	bytes::PRIMARY_APPENDING_BASIC,
	fixtures::primary_appending_basic()
);

// BatchValue
compat_test!(
	v3_0_0_batch_value_basic,
	BatchValue,
	bytes::BATCH_VALUE_BASIC,
	fixtures::batch_value_basic()
);

// SequenceState
compat_test!(
	v3_0_0_sequence_state_basic,
	SequenceState,
	bytes::SEQUENCE_STATE_BASIC,
	fixtures::sequence_state_basic()
);

// TaskLease
compat_test!(
	v3_0_0_task_lease_basic,
	TaskLease,
	bytes::TASK_LEASE_BASIC,
	fixtures::task_lease_basic()
);

// IDs
compat_test!(
	v3_0_0_namespace_id_basic,
	NamespaceId,
	bytes::NAMESPACE_ID_BASIC,
	fixtures::namespace_id_basic()
);
compat_test!(
	v3_0_0_database_id_basic,
	DatabaseId,
	bytes::DATABASE_ID_BASIC,
	fixtures::database_id_basic()
);
compat_test!(v3_0_0_table_id_basic, TableId, bytes::TABLE_ID_BASIC, fixtures::table_id_basic());
compat_test!(v3_0_0_index_id_basic, IndexId, bytes::INDEX_ID_BASIC, fixtures::index_id_basic());

// ModuleDefinition
compat_test!(
	v3_0_0_module_definition_surrealism,
	ModuleDefinition,
	bytes::MODULE_SURREALISM,
	fixtures::module_surrealism()
);
compat_test!(
	v3_0_0_module_definition_silo,
	ModuleDefinition,
	bytes::MODULE_SILO,
	fixtures::module_silo()
);

// NodeLiveQuery
compat_test!(
	v3_0_0_node_live_query_basic,
	NodeLiveQuery,
	bytes::NODE_LIVE_QUERY_BASIC,
	fixtures::node_live_query_basic()
);

// TableMutations
compat_test!(
	v3_0_0_table_mutations_set,
	TableMutations,
	bytes::TABLE_MUTATIONS_SET,
	fixtures::table_mutations_set()
);
compat_test!(
	v3_0_0_table_mutations_del,
	TableMutations,
	bytes::TABLE_MUTATIONS_DEL,
	fixtures::table_mutations_del()
);

// Node
compat_test!(v3_0_0_node_active, Node, bytes::NODE_ACTIVE, fixtures::node_active());
compat_test!(v3_0_0_node_archived, Node, bytes::NODE_ARCHIVED, fixtures::node_archived());

// RecordId
compat_test!(v3_0_0_recordid_number, RecordId, bytes::RECORDID_NUMBER, fixtures::recordid_number());
compat_test!(v3_0_0_recordid_string, RecordId, bytes::RECORDID_STRING, fixtures::recordid_string());
compat_test!(v3_0_0_recordid_uuid, RecordId, bytes::RECORDID_UUID, fixtures::recordid_uuid());

// RecordIdKey
compat_test!(
	v3_0_0_recordid_key_number,
	RecordIdKey,
	bytes::RECORDID_KEY_NUMBER,
	fixtures::recordid_key_number()
);
compat_test!(
	v3_0_0_recordid_key_string,
	RecordIdKey,
	bytes::RECORDID_KEY_STRING,
	fixtures::recordid_key_string()
);
compat_test!(
	v3_0_0_recordid_key_uuid,
	RecordIdKey,
	bytes::RECORDID_KEY_UUID,
	fixtures::recordid_key_uuid()
);
compat_test!(
	v3_0_0_recordid_key_array,
	RecordIdKey,
	bytes::RECORDID_KEY_ARRAY,
	fixtures::recordid_key_array()
);
compat_test!(
	v3_0_0_recordid_key_object,
	RecordIdKey,
	bytes::RECORDID_KEY_OBJECT,
	fixtures::recordid_key_object()
);

// TermDocument
compat_test!(
	v3_0_0_term_document_basic,
	TermDocument,
	bytes::TERM_DOCUMENT_BASIC,
	fixtures::term_document_basic()
);
