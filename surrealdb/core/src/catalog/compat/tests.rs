//! Catalog backwards compatibility tests.
//!
//! These tests verify that:
//! 1. Serialized data from previous versions can be deserialized
//! 2. The deserialized values EXACTLY match the expected fixtures
//!
//! Failing either check indicates a backwards compatibility regression.

use super::super::*;
use super::{fixtures, v3_0_0_beta_1, v3_0_0_beta_3};
use crate::cf::TableMutations;
use crate::dbs::node::Node;
use crate::idx::ft::fulltext::{DocLengthAndCount, TermDocument};
use crate::kvs::KVValue;
use crate::kvs::index::{Appending, PrimaryAppending};
use crate::kvs::sequences::{BatchValue, SequenceState};
use crate::kvs::tasklease::TaskLease;
use crate::kvs::version::MajorVersion;
use crate::val::{RecordId, RecordIdKey};

/// Macro to generate backwards compatibility tests for a fixture across multiple versions.
///
/// This macro creates tests that:
/// 1. Decode the fixture bytes using `kv_decode_value`
/// 2. Compare the decoded value against the expected fixture value
/// 3. Fail loudly if decoding fails OR if values don't match
///
/// For each version in the list, a test function is generated with the name
/// `{version}_{base_name}`.
macro_rules! compat_test {
	($base_name:ident, $type:ty, $const_name:ident, $expected:expr, [$($version:ident),+ $(,)?]) => {
		$(
			paste::paste! {
				#[test]
				fn [<$version _ $base_name>]() {
					let fixture_bytes = [<$version>]::$const_name;

					// Attempt to decode - this MUST succeed for backwards compatibility
					let decoded = <$type>::kv_decode_value(fixture_bytes.to_vec()).unwrap_or_else(|e| {
						panic!(
							concat!(
								"BACKWARDS COMPATIBILITY BROKEN: Failed to decode ",
								stringify!([<$version _ $base_name>]),
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
							stringify!([<$version _ $base_name>]),
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
								stringify!([<$version _ $base_name>]),
								" after successful decode.\n",
								"Error: {}"
							),
							e
						)
					});
				}
			}
		)+
	};
}

// =============================================================================
// Backwards Compatibility Tests
// =============================================================================

// NamespaceDefinition
compat_test!(
	namespace_basic,
	NamespaceDefinition,
	NAMESPACE_BASIC,
	fixtures::namespace_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	namespace_with_comment,
	NamespaceDefinition,
	NAMESPACE_WITH_COMMENT,
	fixtures::namespace_with_comment(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// DatabaseDefinition
compat_test!(
	database_basic,
	DatabaseDefinition,
	DATABASE_BASIC,
	fixtures::database_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	database_with_changefeed,
	DatabaseDefinition,
	DATABASE_WITH_CHANGEFEED,
	fixtures::database_with_changefeed(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	database_strict,
	DatabaseDefinition,
	DATABASE_STRICT,
	fixtures::database_strict(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// TableDefinition
compat_test!(
	table_basic,
	TableDefinition,
	TABLE_BASIC,
	fixtures::table_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	table_with_view,
	TableDefinition,
	TABLE_WITH_VIEW,
	fixtures::table_with_view(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	table_schemafull,
	TableDefinition,
	TABLE_SCHEMAFULL,
	fixtures::table_schemafull(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// SubscriptionDefinition
compat_test!(
	subscription_basic,
	SubscriptionDefinition,
	SUBSCRIPTION_BASIC,
	fixtures::subscription_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	subscription_with_filters,
	SubscriptionDefinition,
	SUBSCRIPTION_WITH_FILTERS,
	fixtures::subscription_with_filters(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// AccessDefinition
compat_test!(
	access_bearer,
	AccessDefinition,
	ACCESS_BEARER,
	fixtures::access_bearer(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	access_with_authenticate,
	AccessDefinition,
	ACCESS_WITH_AUTHENTICATE,
	fixtures::access_with_authenticate(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// AccessGrant
compat_test!(
	grant_jwt,
	AccessGrant,
	GRANT_JWT,
	fixtures::grant_jwt(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	grant_revoked,
	AccessGrant,
	GRANT_REVOKED,
	fixtures::grant_revoked(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// AnalyzerDefinition
compat_test!(
	analyzer_basic,
	AnalyzerDefinition,
	ANALYZER_BASIC,
	fixtures::analyzer_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	analyzer_with_tokenizers,
	AnalyzerDefinition,
	ANALYZER_WITH_TOKENIZERS,
	fixtures::analyzer_with_tokenizers(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// ApiDefinition
compat_test!(
	api_basic,
	ApiDefinition,
	API_BASIC,
	fixtures::api_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	api_with_middleware,
	ApiDefinition,
	API_WITH_MIDDLEWARE,
	fixtures::api_with_middleware(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// BucketDefinition
compat_test!(
	bucket_basic,
	BucketDefinition,
	BUCKET_BASIC,
	fixtures::bucket_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	bucket_readonly,
	BucketDefinition,
	BUCKET_READONLY,
	fixtures::bucket_readonly(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// ConfigDefinition
compat_test!(
	config_graphql,
	ConfigDefinition,
	CONFIG_GRAPHQL,
	fixtures::config_graphql(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// EventDefinition
compat_test!(
	event_basic,
	EventDefinition,
	EVENT_BASIC,
	fixtures::event_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// FieldDefinition
compat_test!(
	field_basic,
	FieldDefinition,
	FIELD_BASIC,
	fixtures::field_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	field_with_type,
	FieldDefinition,
	FIELD_WITH_TYPE,
	fixtures::field_with_type(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	field_readonly,
	FieldDefinition,
	FIELD_READONLY,
	fixtures::field_readonly(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// FunctionDefinition
compat_test!(
	function_basic,
	FunctionDefinition,
	FUNCTION_BASIC,
	fixtures::function_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	function_with_args,
	FunctionDefinition,
	FUNCTION_WITH_ARGS,
	fixtures::function_with_args(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// IndexDefinition
compat_test!(
	index_basic,
	IndexDefinition,
	INDEX_BASIC,
	fixtures::index_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	index_unique,
	IndexDefinition,
	INDEX_UNIQUE,
	fixtures::index_unique(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// MlModelDefinition
compat_test!(
	model_basic,
	MlModelDefinition,
	MODEL_BASIC,
	fixtures::model_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// ParamDefinition
compat_test!(
	param_bool,
	ParamDefinition,
	PARAM_BOOL,
	fixtures::param_bool(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	param_string,
	ParamDefinition,
	PARAM_STRING,
	fixtures::param_string(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// SequenceDefinition
compat_test!(
	sequence_basic,
	SequenceDefinition,
	SEQUENCE_BASIC,
	fixtures::sequence_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	sequence_with_options,
	SequenceDefinition,
	SEQUENCE_WITH_OPTIONS,
	fixtures::sequence_with_options(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// UserDefinition
compat_test!(
	user_basic,
	UserDefinition,
	USER_BASIC,
	fixtures::user_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	user_with_durations,
	UserDefinition,
	USER_WITH_DURATIONS,
	fixtures::user_with_durations(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// Record
compat_test!(
	record_none,
	Record,
	RECORD_NONE,
	fixtures::record_none(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_null,
	Record,
	RECORD_NULL,
	fixtures::record_null(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_bool,
	Record,
	RECORD_BOOL,
	fixtures::record_bool(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_number_int,
	Record,
	RECORD_NUMBER_INT,
	fixtures::record_number_int(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_number_float,
	Record,
	RECORD_NUMBER_FLOAT,
	fixtures::record_number_float(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_number_decimal,
	Record,
	RECORD_NUMBER_DECIMAL,
	fixtures::record_number_decimal(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_string,
	Record,
	RECORD_STRING,
	fixtures::record_string(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_bytes,
	Record,
	RECORD_BYTES,
	fixtures::record_bytes(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_duration,
	Record,
	RECORD_DURATION,
	fixtures::record_duration(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_datetime,
	Record,
	RECORD_DATETIME,
	fixtures::record_datetime(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_uuid,
	Record,
	RECORD_UUID,
	fixtures::record_uuid(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_geometry_point,
	Record,
	RECORD_GEOMETRY_POINT,
	fixtures::record_geometry_point(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_geometry_line,
	Record,
	RECORD_GEOMETRY_LINE,
	fixtures::record_geometry_line(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_geometry_polygon,
	Record,
	RECORD_GEOMETRY_POLYGON,
	fixtures::record_geometry_polygon(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_geometry_multi_point,
	Record,
	RECORD_GEOMETRY_MULTI_POINT,
	fixtures::record_geometry_multi_point(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_geometry_multi_line,
	Record,
	RECORD_GEOMETRY_MULTI_LINE,
	fixtures::record_geometry_multi_line(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_geometry_multi_polygon,
	Record,
	RECORD_GEOMETRY_MULTI_POLYGON,
	fixtures::record_geometry_multi_polygon(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_geometry_collection,
	Record,
	RECORD_GEOMETRY_COLLECTION,
	fixtures::record_geometry_collection(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_table,
	Record,
	RECORD_TABLE,
	fixtures::record_table(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_recordid,
	Record,
	RECORD_RECORDID,
	fixtures::record_recordid(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_file,
	Record,
	RECORD_FILE,
	fixtures::record_file(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_range_unbounded,
	Record,
	RECORD_RANGE_UNBOUNDED,
	fixtures::record_range_unbounded(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_range_bounded,
	Record,
	RECORD_RANGE_BOUNDED,
	fixtures::record_range_bounded(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_regex,
	Record,
	RECORD_REGEX,
	fixtures::record_regex(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_array,
	Record,
	RECORD_ARRAY,
	fixtures::record_array(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_object,
	Record,
	RECORD_OBJECT,
	fixtures::record_object(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_set,
	Record,
	RECORD_SET,
	fixtures::record_set(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	record_with_metadata,
	Record,
	RECORD_WITH_METADATA,
	fixtures::record_with_metadata(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// MajorVersion
compat_test!(
	version_1,
	MajorVersion,
	VERSION_1,
	fixtures::version_1(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	version_3,
	MajorVersion,
	VERSION_3,
	fixtures::version_3(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// ApiActionDefinition
compat_test!(
	api_action_basic,
	ApiActionDefinition,
	API_ACTION_BASIC,
	fixtures::api_action_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	api_action_multi_method,
	ApiActionDefinition,
	API_ACTION_MULTI_METHOD,
	fixtures::api_action_multi_method(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// Appending
compat_test!(
	appending_none,
	Appending,
	APPENDING_NONE,
	fixtures::appending_none(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	appending_old_values,
	Appending,
	APPENDING_OLD_VALUES,
	fixtures::appending_old_values(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	appending_new_values,
	Appending,
	APPENDING_NEW_VALUES,
	fixtures::appending_new_values(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	appending_both,
	Appending,
	APPENDING_BOTH,
	fixtures::appending_both(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// DocLengthAndCount
compat_test!(
	doc_length_and_count_basic,
	DocLengthAndCount,
	DOC_LENGTH_AND_COUNT_BASIC,
	fixtures::doc_length_and_count_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// PrimaryAppending
compat_test!(
	primary_appending_basic,
	PrimaryAppending,
	PRIMARY_APPENDING_BASIC,
	fixtures::primary_appending_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// BatchValue
compat_test!(
	batch_value_basic,
	BatchValue,
	BATCH_VALUE_BASIC,
	fixtures::batch_value_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// SequenceState
compat_test!(
	sequence_state_basic,
	SequenceState,
	SEQUENCE_STATE_BASIC,
	fixtures::sequence_state_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// TaskLease
compat_test!(
	task_lease_basic,
	TaskLease,
	TASK_LEASE_BASIC,
	fixtures::task_lease_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// IDs
compat_test!(
	namespace_id_basic,
	NamespaceId,
	NAMESPACE_ID_BASIC,
	fixtures::namespace_id_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	database_id_basic,
	DatabaseId,
	DATABASE_ID_BASIC,
	fixtures::database_id_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	table_id_basic,
	TableId,
	TABLE_ID_BASIC,
	fixtures::table_id_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	index_id_basic,
	IndexId,
	INDEX_ID_BASIC,
	fixtures::index_id_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// ModuleDefinition
compat_test!(
	module_definition_surrealism,
	ModuleDefinition,
	MODULE_SURREALISM,
	fixtures::module_surrealism(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	module_definition_silo,
	ModuleDefinition,
	MODULE_SILO,
	fixtures::module_silo(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// NodeLiveQuery
compat_test!(
	node_live_query_basic,
	NodeLiveQuery,
	NODE_LIVE_QUERY_BASIC,
	fixtures::node_live_query_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// TableMutations
compat_test!(
	table_mutations_set,
	TableMutations,
	TABLE_MUTATIONS_SET,
	fixtures::table_mutations_set(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	table_mutations_del,
	TableMutations,
	TABLE_MUTATIONS_DEL,
	fixtures::table_mutations_del(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// Node
compat_test!(
	node_active,
	Node,
	NODE_ACTIVE,
	fixtures::node_active(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	node_archived,
	Node,
	NODE_ARCHIVED,
	fixtures::node_archived(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// RecordId
compat_test!(
	recordid_number,
	RecordId,
	RECORDID_NUMBER,
	fixtures::recordid_number(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	recordid_string,
	RecordId,
	RECORDID_STRING,
	fixtures::recordid_string(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	recordid_uuid,
	RecordId,
	RECORDID_UUID,
	fixtures::recordid_uuid(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// RecordIdKey
compat_test!(
	recordid_key_number,
	RecordIdKey,
	RECORDID_KEY_NUMBER,
	fixtures::recordid_key_number(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	recordid_key_string,
	RecordIdKey,
	RECORDID_KEY_STRING,
	fixtures::recordid_key_string(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	recordid_key_uuid,
	RecordIdKey,
	RECORDID_KEY_UUID,
	fixtures::recordid_key_uuid(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	recordid_key_array,
	RecordIdKey,
	RECORDID_KEY_ARRAY,
	fixtures::recordid_key_array(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
compat_test!(
	recordid_key_object,
	RecordIdKey,
	RECORDID_KEY_OBJECT,
	fixtures::recordid_key_object(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);

// TermDocument
compat_test!(
	term_document_basic,
	TermDocument,
	TERM_DOCUMENT_BASIC,
	fixtures::term_document_basic(),
	[v3_0_0_beta_1, v3_0_0_beta_3]
);
