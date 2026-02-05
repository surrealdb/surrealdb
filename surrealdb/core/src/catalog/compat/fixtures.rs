//! Fixture definitions for catalog compatibility tests.
//!
//! These functions define the expected values for each catalog type fixture.
//! They serve as both:
//! 1. The source for generating serialized byte arrays (in generator.rs)
//! 2. The expected values for equality assertions (in tests.rs)
//!
//! When types evolve (fields added/removed), update these fixtures to reflect
//! how old serialized data should be interpreted by the current code.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::str::FromStr;
use std::time::Duration;

use chrono::DateTime;
use geo::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon, coord};
use rust_decimal::Decimal;
use uuid::Uuid as UuidExt;

use super::super::*;
use crate::catalog::auth::AuthLimit;
use crate::catalog::record::{Data, Record, RecordType};
use crate::catalog::schema::base::Base;
use crate::catalog::{
	ApiActionDefinition, ApiConfigDefinition, ApiMethod, DatabaseId, IndexId, ModuleDefinition,
	ModuleExecutable, NamespaceId, NodeLiveQuery, SiloExecutable, SurrealismExecutable, TableId,
};
use crate::cf::mutations::{TableMutation, TableMutations};
use crate::dbs::node::{Node, Timestamp};
use crate::expr::field::Selector;
use crate::expr::{
	Block, ChangeFeed, Expr, Fetch, Fetchs, Field, Fields, Filter, Groups, Idiom, Kind, Literal,
	Tokenizer,
};
use crate::iam::Auth;
use crate::idx::ft::fulltext::{DocLengthAndCount, TermDocument};
use crate::idx::ft::offset::Offset;
use crate::kvs::index::{Appending, PrimaryAppending};
use crate::kvs::sequences::{BatchValue, SequenceState};
use crate::kvs::tasklease::TaskLease;
use crate::kvs::version::MajorVersion;
use crate::val::{
	Array, Bytes, Datetime, Duration as ValDuration, File, Geometry, Number, Object, Range,
	RecordId, RecordIdKey, Regex, Set, TableName, Uuid, Value,
};

// ===========================================================================
// NamespaceDefinition fixtures
// ===========================================================================

/// Minimal namespace without comment
pub fn namespace_basic() -> NamespaceDefinition {
	NamespaceDefinition {
		namespace_id: NamespaceId(1),
		name: "test".to_string(),
		comment: None,
	}
}

/// Namespace with optional comment
pub fn namespace_with_comment() -> NamespaceDefinition {
	NamespaceDefinition {
		namespace_id: NamespaceId(123),
		name: "production".to_string(),
		comment: Some("Production namespace".to_string()),
	}
}

// ===========================================================================
// DatabaseDefinition fixtures
// ===========================================================================

/// Minimal database without changefeed
pub fn database_basic() -> DatabaseDefinition {
	DatabaseDefinition {
		namespace_id: NamespaceId(1),
		database_id: DatabaseId(1),
		name: "test".to_string(),
		strict: false,
		comment: None,
		changefeed: None,
	}
}

/// Database with changefeed enabled
pub fn database_with_changefeed() -> DatabaseDefinition {
	DatabaseDefinition {
		namespace_id: NamespaceId(123),
		database_id: DatabaseId(456),
		name: "events".to_string(),
		strict: false,
		comment: Some("Event store".to_string()),
		changefeed: Some(ChangeFeed {
			expiry: Duration::from_secs(3600),
			store_diff: true,
		}),
	}
}

/// Database with strict mode enabled
pub fn database_strict() -> DatabaseDefinition {
	DatabaseDefinition {
		namespace_id: NamespaceId(1),
		database_id: DatabaseId(2),
		name: "strict_db".to_string(),
		strict: true,
		comment: Some("Strict mode database".to_string()),
		changefeed: None,
	}
}

// ===========================================================================
// TableDefinition fixtures
// ===========================================================================

/// Minimal table definition
pub fn table_basic() -> TableDefinition {
	TableDefinition {
		namespace_id: NamespaceId(1),
		database_id: DatabaseId(1),
		table_id: TableId(1),
		name: TableName::from("users"),
		drop: false,
		schemafull: false,
		view: None,
		permissions: Permissions::default(),
		changefeed: None,
		comment: None,
		table_type: TableType::Normal,
		cache_fields_ts: UuidExt::nil(),
		cache_events_ts: UuidExt::nil(),
		cache_tables_ts: UuidExt::nil(),
		cache_indexes_ts: UuidExt::nil(),
	}
}

/// Table with view definition
pub fn table_with_view() -> TableDefinition {
	TableDefinition {
		namespace_id: NamespaceId(123),
		database_id: DatabaseId(456),
		table_id: TableId(789),
		name: TableName::from("user_stats"),
		drop: false,
		schemafull: false,
		view: Some(ViewDefinition::Select {
			fields: Fields::Select(vec![
				Field::All,
				Field::Single(Selector {
					expr: Expr::Literal(Literal::String("count".to_string())),
					alias: Some(Idiom::from_str("total").unwrap()),
				}),
			]),
			tables: vec![TableName::from("users")],
			condition: Some(Expr::Literal(Literal::String("active = true".to_string()))),
			groups: Some(Groups::default()),
		}),
		permissions: Permissions::default(),
		changefeed: None,
		comment: Some("User statistics view".to_string()),
		table_type: TableType::Normal,
		cache_fields_ts: UuidExt::nil(),
		cache_events_ts: UuidExt::nil(),
		cache_tables_ts: UuidExt::nil(),
		cache_indexes_ts: UuidExt::nil(),
	}
}

/// Schemafull table with changefeed
pub fn table_schemafull() -> TableDefinition {
	TableDefinition {
		namespace_id: NamespaceId(1),
		database_id: DatabaseId(1),
		table_id: TableId(2),
		name: TableName::from("orders"),
		drop: false,
		schemafull: true,
		view: None,
		permissions: Permissions::default(),
		changefeed: Some(ChangeFeed {
			expiry: Duration::from_secs(86400),
			store_diff: false,
		}),
		comment: Some("Order records".to_string()),
		table_type: TableType::Normal,
		cache_fields_ts: UuidExt::nil(),
		cache_events_ts: UuidExt::nil(),
		cache_tables_ts: UuidExt::nil(),
		cache_indexes_ts: UuidExt::nil(),
	}
}

// ===========================================================================
// SubscriptionDefinition fixtures
// ===========================================================================

/// Minimal subscription with diff fields
pub fn subscription_basic() -> SubscriptionDefinition {
	SubscriptionDefinition {
		id: UuidExt::nil(),
		node: UuidExt::nil(),
		fields: SubscriptionFields::Diff,
		what: Expr::Literal(Literal::String("users".to_string())),
		cond: None,
		fetch: None,
		auth: None,
		session: None,
		vars: BTreeMap::new(),
	}
}

/// Subscription with condition and fetch
pub fn subscription_with_filters() -> SubscriptionDefinition {
	SubscriptionDefinition {
		id: UuidExt::nil(),
		node: UuidExt::nil(),
		fields: SubscriptionFields::Select(Fields::Select(vec![
			Field::All,
			Field::Single(Selector {
				expr: Expr::Literal(Literal::String("name".to_string())),
				alias: None,
			}),
		])),
		what: Expr::Literal(Literal::String("users".to_string())),
		cond: Some(Expr::Literal(Literal::String("active = true".to_string()))),
		fetch: Some(Fetchs::new(vec![Fetch(Expr::Literal(Literal::String(
			"profile".to_string(),
		)))])),
		auth: Some(Auth::default()),
		session: Some(Value::default()),
		vars: BTreeMap::new(),
	}
}

// ===========================================================================
// AccessDefinition fixtures
// ===========================================================================

/// Bearer access with JWT
pub fn access_bearer() -> AccessDefinition {
	AccessDefinition {
		name: "api_access".to_string(),
		access_type: AccessType::Bearer(BearerAccess {
			kind: BearerAccessType::Bearer,
			subject: BearerAccessSubject::Record,
			jwt: JwtAccess {
				verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
					alg: Algorithm::EdDSA,
					key: "public_key".to_string(),
				}),
				issue: Some(JwtAccessIssue {
					alg: Algorithm::Hs512,
					key: "secret_key".to_string(),
				}),
			},
		}),
		base: Base::Root,
		authenticate: None,
		grant_duration: Some(Duration::from_secs(3600)),
		token_duration: Some(Duration::from_secs(900)),
		session_duration: Some(Duration::from_secs(86400)),
		comment: Some("API access".to_string()),
	}
}

/// Access with custom authenticate expression
pub fn access_with_authenticate() -> AccessDefinition {
	AccessDefinition {
		name: "custom_auth".to_string(),
		access_type: AccessType::Bearer(BearerAccess {
			kind: BearerAccessType::Bearer,
			subject: BearerAccessSubject::User,
			jwt: JwtAccess {
				verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
					alg: Algorithm::Hs256,
					key: "secret".to_string(),
				}),
				issue: None,
			},
		}),
		base: Base::Db,
		authenticate: Some(Expr::Literal(Literal::String(
			"SELECT * FROM user WHERE id = $auth.id".to_string(),
		))),
		grant_duration: None,
		token_duration: Some(Duration::from_secs(3600)),
		session_duration: None,
		comment: None,
	}
}

// ===========================================================================
// AccessGrant fixtures
// ===========================================================================

/// JWT access grant
pub fn grant_jwt() -> AccessGrant {
	AccessGrant {
		id: "grant_001".to_string(),
		ac: "api_access".to_string(),
		creation: Datetime::MIN_UTC,
		expiration: Some(Datetime::MIN_UTC),
		revocation: None,
		subject: Subject::User("admin".to_string()),
		grant: Grant::Jwt(GrantJwt {
			jti: UuidExt::nil(),
			token: Some("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9".to_string()),
		}),
	}
}

/// Revoked access grant
pub fn grant_revoked() -> AccessGrant {
	AccessGrant {
		id: "grant_002".to_string(),
		ac: "api_access".to_string(),
		creation: Datetime::MIN_UTC,
		expiration: Some(Datetime::MIN_UTC),
		revocation: Some(Datetime::MIN_UTC),
		subject: Subject::User("user".to_string()),
		grant: Grant::Jwt(GrantJwt {
			jti: UuidExt::nil(),
			token: None,
		}),
	}
}

// ===========================================================================
// AnalyzerDefinition fixtures
// ===========================================================================

/// Minimal analyzer
pub fn analyzer_basic() -> AnalyzerDefinition {
	AnalyzerDefinition {
		name: "simple".to_string(),
		function: None,
		tokenizers: None,
		filters: None,
		comment: None,
	}
}

/// Analyzer with tokenizers and filters
pub fn analyzer_with_tokenizers() -> AnalyzerDefinition {
	AnalyzerDefinition {
		name: "english".to_string(),
		function: Some("fn::custom_analyzer".to_string()),
		tokenizers: Some(vec![Tokenizer::Camel, Tokenizer::Class]),
		filters: Some(vec![Filter::Ascii, Filter::Lowercase]),
		comment: Some("English text analyzer".to_string()),
	}
}

// ===========================================================================
// ApiDefinition fixtures
// ===========================================================================

/// Minimal API endpoint
pub fn api_basic() -> ApiDefinition {
	ApiDefinition {
		path: "/api/v1/users".parse().unwrap(),
		actions: vec![ApiActionDefinition {
			methods: vec![ApiMethod::Get],
			action: Expr::Literal(Literal::String("SELECT * FROM users".to_string())),
			config: ApiConfigDefinition::default(),
		}],
		fallback: None,
		config: ApiConfigDefinition::default(),
		comment: None,
		auth_limit: AuthLimit::new_no_limit(),
	}
}

/// API with middleware and multiple methods
pub fn api_with_middleware() -> ApiDefinition {
	ApiDefinition {
		auth_limit: AuthLimit::new_no_limit(),
		path: "/api/v1/orders".parse().unwrap(),
		actions: vec![
			ApiActionDefinition {
				methods: vec![ApiMethod::Get, ApiMethod::Post],
				action: Expr::Literal(Literal::String("SELECT * FROM orders".to_string())),
				config: ApiConfigDefinition::default(),
			},
			ApiActionDefinition {
				methods: vec![ApiMethod::Delete],
				action: Expr::Literal(Literal::String("DELETE FROM orders".to_string())),
				config: ApiConfigDefinition::default(),
			},
		],
		fallback: Some(Expr::Literal(Literal::String("RETURN 404".to_string()))),
		config: ApiConfigDefinition {
			middleware: vec![
				MiddlewareDefinition {
					name: "auth".to_string(),
					args: vec![],
				},
				MiddlewareDefinition {
					name: "rate_limit".to_string(),
					args: vec![Value::from(100)],
				},
			],
			permissions: Permission::Full,
		},
		comment: Some("Order management API".to_string()),
	}
}

// ===========================================================================
// BucketDefinition fixtures
// ===========================================================================

/// Minimal bucket
pub fn bucket_basic() -> BucketDefinition {
	BucketDefinition {
		id: None,
		readonly: false,
		name: "uploads".to_string(),
		backend: None,
		comment: None,
		permissions: Permission::Full,
	}
}

/// Readonly bucket with backend
pub fn bucket_readonly() -> BucketDefinition {
	BucketDefinition {
		id: Some(BucketId(123)),
		readonly: true,
		name: "archives".to_string(),
		backend: Some("s3://bucket/archives".to_string()),
		comment: Some("Read-only archive storage".to_string()),
		permissions: Permission::None,
	}
}

// ===========================================================================
// ConfigDefinition fixtures
// ===========================================================================

/// GraphQL configuration
pub fn config_graphql() -> ConfigDefinition {
	ConfigDefinition::GraphQL(GraphQLConfig::default())
}

// ===========================================================================
// EventDefinition fixtures
// ===========================================================================

/// Table event trigger
pub fn event_basic() -> EventDefinition {
	EventDefinition {
		name: "on_create".to_string(),
		target_table: TableName::from("users"),
		when: Expr::Literal(Literal::String("$event = 'CREATE'".to_string())),
		then: vec![Expr::Literal(Literal::String(
			"CREATE audit SET action = 'create'".to_string(),
		))],
		comment: Some("Audit log on create".to_string()),
		auth_limit: AuthLimit::new_no_limit(),
		kind: EventKind::Sync,
	}
}

// ===========================================================================
// FieldDefinition fixtures
// ===========================================================================

/// Minimal field
pub fn field_basic() -> FieldDefinition {
	FieldDefinition {
		name: Idiom::from_str("name").unwrap(),
		table: TableName::from("users"),
		field_kind: None,
		readonly: false,
		flexible: false,
		value: None,
		assert: None,
		computed: None,
		default: DefineDefault::None,
		select_permission: Permission::Full,
		create_permission: Permission::Full,
		update_permission: Permission::Full,
		comment: None,
		reference: None,
		auth_limit: AuthLimit::new_no_limit(),
	}
}

/// Field with type constraint and default
pub fn field_with_type() -> FieldDefinition {
	FieldDefinition {
		name: Idiom::from_str("email").unwrap(),
		table: TableName::from("users"),
		field_kind: Some(Kind::String),
		readonly: false,
		flexible: false,
		value: Some(Expr::Literal(Literal::String("string::lowercase($value)".to_string()))),
		assert: Some(Expr::Literal(Literal::String("string::is::email($value)".to_string()))),
		computed: None,
		default: DefineDefault::Always(Expr::Literal(Literal::String("".to_string()))),
		select_permission: Permission::Full,
		create_permission: Permission::Full,
		update_permission: Permission::Full,
		comment: Some("User email address".to_string()),
		reference: None,
		auth_limit: AuthLimit::new_no_limit(),
	}
}

/// Readonly computed field
pub fn field_readonly() -> FieldDefinition {
	FieldDefinition {
		name: Idiom::from_str("created_at").unwrap(),
		table: TableName::from("users"),
		field_kind: Some(Kind::Datetime),
		readonly: true,
		flexible: false,
		value: None,
		assert: None,
		computed: Some(Expr::Literal(Literal::String("time::now()".to_string()))),
		default: DefineDefault::None,
		select_permission: Permission::Full,
		create_permission: Permission::None,
		update_permission: Permission::None,
		comment: Some("Record creation timestamp".to_string()),
		reference: None,
		auth_limit: AuthLimit::new_no_limit(),
	}
}

// ===========================================================================
// FunctionDefinition fixtures
// ===========================================================================

/// Simple function
pub fn function_basic() -> FunctionDefinition {
	FunctionDefinition {
		name: "greet".to_string(),
		args: vec![],
		block: Block(vec![Expr::Literal(Literal::String("RETURN 'Hello, World!'".to_string()))]),
		comment: None,
		permissions: Permission::Full,
		returns: None,
		auth_limit: AuthLimit::new_no_limit(),
	}
}

/// Function with arguments and return type
pub fn function_with_args() -> FunctionDefinition {
	FunctionDefinition {
		name: "add_numbers".to_string(),
		args: vec![("a".to_string(), Kind::Number), ("b".to_string(), Kind::Number)],
		block: Block(vec![Expr::Literal(Literal::String("RETURN $a + $b".to_string()))]),
		comment: Some("Add two numbers".to_string()),
		permissions: Permission::Full,
		returns: Some(Kind::Number),
		auth_limit: AuthLimit::new_no_limit(),
	}
}

// ===========================================================================
// IndexDefinition fixtures
// ===========================================================================

/// Basic index
pub fn index_basic() -> IndexDefinition {
	IndexDefinition {
		index_id: IndexId(1),
		name: "idx_name".to_string(),
		table_name: TableName::from("users"),
		cols: vec![Idiom::from_str("name").unwrap()],
		index: Index::Idx,
		comment: None,
		prepare_remove: false,
	}
}

/// Unique index on multiple columns
pub fn index_unique() -> IndexDefinition {
	IndexDefinition {
		index_id: IndexId(2),
		name: "idx_email_unique".to_string(),
		table_name: TableName::from("users"),
		cols: vec![Idiom::from_str("email").unwrap()],
		index: Index::Uniq,
		comment: Some("Unique email constraint".to_string()),
		prepare_remove: false,
	}
}

// ===========================================================================
// MlModelDefinition fixtures
// ===========================================================================

/// ML model definition
pub fn model_basic() -> MlModelDefinition {
	MlModelDefinition {
		name: "sentiment".to_string(),
		hash: "sha256:abc123def456".to_string(),
		version: "1.0.0".to_string(),
		comment: Some("Sentiment analysis model".to_string()),
		permissions: Permission::Full,
	}
}

// ===========================================================================
// ParamDefinition fixtures
// ===========================================================================

/// Boolean parameter
pub fn param_bool() -> ParamDefinition {
	ParamDefinition {
		name: "debug".to_string(),
		value: Value::Bool(true),
		comment: Some("Debug mode flag".to_string()),
		permissions: Permission::Full,
	}
}

/// String parameter
pub fn param_string() -> ParamDefinition {
	ParamDefinition {
		name: "app_name".to_string(),
		value: Value::String("MyApp".to_string()),
		comment: None,
		permissions: Permission::Full,
	}
}

// ===========================================================================
// SequenceDefinition fixtures
// ===========================================================================

/// Minimal sequence
pub fn sequence_basic() -> SequenceDefinition {
	SequenceDefinition {
		name: "order_id".to_string(),
		batch: 1,
		start: 1,
		timeout: None,
	}
}

/// Sequence with custom options
pub fn sequence_with_options() -> SequenceDefinition {
	SequenceDefinition {
		name: "invoice_number".to_string(),
		batch: 100,
		start: 1000,
		timeout: Some(Duration::from_secs(30)),
	}
}

// ===========================================================================
// UserDefinition fixtures
// ===========================================================================

/// Minimal user
pub fn user_basic() -> UserDefinition {
	UserDefinition {
		name: "admin".to_string(),
		hash: "$argon2id$v=19$m=65536,t=3,p=4$hash".to_string(),
		code: "TOTP_CODE".to_string(),
		roles: vec!["owner".to_string()],
		token_duration: None,
		session_duration: None,
		comment: None,
		base: Base::Root,
	}
}

/// User with custom token/session durations
pub fn user_with_durations() -> UserDefinition {
	UserDefinition {
		name: "api_user".to_string(),
		hash: "$argon2id$v=19$m=65536,t=3,p=4$hash".to_string(),
		code: "".to_string(),
		roles: vec!["viewer".to_string(), "editor".to_string()],
		token_duration: Some(Duration::from_secs(3600)),
		session_duration: Some(Duration::from_secs(86400)),
		comment: Some("API service account".to_string()),
		base: Base::Ns,
	}
}

// ===========================================================================
// Record fixtures
// ===========================================================================

/// Record with None value
pub fn record_none() -> Record {
	Record::new(Data::from(Value::None))
}

/// Record with Null value
pub fn record_null() -> Record {
	Record::new(Data::from(Value::Null))
}

/// Record with boolean data
pub fn record_bool() -> Record {
	Record::new(Data::from(Value::Bool(true)))
}

/// Record with int number data
pub fn record_number_int() -> Record {
	Record::new(Data::from(Value::Number(Number::Int(42))))
}

/// Record with float number data
pub fn record_number_float() -> Record {
	Record::new(Data::from(Value::Number(Number::Float(42.0))))
}

/// Record with decimal number data
pub fn record_number_decimal() -> Record {
	Record::new(Data::from(Value::Number(Number::Decimal(Decimal::from(42)))))
}

/// Record with string data
pub fn record_string() -> Record {
	Record::new(Data::from(Value::String("test data".to_string())))
}

/// Record with bytes data
pub fn record_bytes() -> Record {
	Record::new(Data::from(Value::Bytes(Bytes::from(vec![0x01, 0x02, 0x03, 0x04]))))
}

/// Record with duration data
pub fn record_duration() -> Record {
	Record::new(Data::from(Value::Duration(ValDuration::from_secs(3600))))
}

/// Record with datetime data
pub fn record_datetime() -> Record {
	Record::new(Data::from(Value::Datetime(Datetime::MIN_UTC)))
}

/// Record with UUID data
pub fn record_uuid() -> Record {
	Record::new(Data::from(Value::Uuid(Uuid(
		uuid::Uuid::from_str("123e4567-e89b-12d3-a456-426614174000").unwrap(),
	))))
}

/// Record with geometry data (point)
pub fn record_geometry_point() -> Record {
	Record::new(Data::from(Value::Geometry(Geometry::Point(Point::new(1.0, 2.0)))))
}

/// Record with geometry data (line)
pub fn record_geometry_line() -> Record {
	Record::new(Data::from(Value::Geometry(Geometry::Line(LineString::new(vec![
		coord! { x: 1.0, y: 2.0 },
		coord! { x: 3.0, y: 4.0 },
	])))))
}

/// Record with geometry data (polygon)
pub fn record_geometry_polygon() -> Record {
	Record::new(Data::from(Value::Geometry(Geometry::Polygon(Polygon::new(
		LineString::new(vec![
			coord! { x: 1.0, y: 2.0 },
			coord! { x: 3.0, y: 4.0 },
			coord! { x: 5.0, y: 6.0 },
		]),
		vec![LineString::new(vec![
			coord! { x: 7.0, y: 8.0 },
			coord! { x: 9.0, y: 10.0 },
			coord! { x: 11.0, y: 12.0 },
		])],
	)))))
}

/// Record with geometry data (multi point)
pub fn record_geometry_multi_point() -> Record {
	Record::new(Data::from(Value::Geometry(Geometry::MultiPoint(MultiPoint::new(vec![
		Point::new(1.0, 2.0),
		Point::new(3.0, 4.0),
	])))))
}

/// Record with geometry data (multi line)
pub fn record_geometry_multi_line() -> Record {
	Record::new(Data::from(Value::Geometry(Geometry::MultiLine(MultiLineString::new(vec![
		LineString::new(vec![coord! { x: 1.0, y: 2.0 }, coord! { x: 3.0, y: 4.0 }]),
		LineString::new(vec![coord! { x: 5.0, y: 6.0 }, coord! { x: 7.0, y: 8.0 }]),
	])))))
}

/// Record with geometry data (multi polygon)
pub fn record_geometry_multi_polygon() -> Record {
	Record::new(Data::from(Value::Geometry(Geometry::MultiPolygon(MultiPolygon::new(vec![
		Polygon::new(
			LineString::new(vec![
				coord! { x: 1.0, y: 2.0 },
				coord! { x: 3.0, y: 4.0 },
				coord! { x: 5.0, y: 6.0 },
			]),
			vec![],
		),
		Polygon::new(
			LineString::new(vec![
				coord! { x: 7.0, y: 8.0 },
				coord! { x: 9.0, y: 10.0 },
				coord! { x: 11.0, y: 12.0 },
			]),
			vec![],
		),
	])))))
}

/// Record with geometry data (collection)
pub fn record_geometry_collection() -> Record {
	Record::new(Data::from(Value::Geometry(Geometry::Collection(vec![
		Geometry::Point(Point::new(1.0, 2.0)),
		Geometry::Line(LineString::new(vec![coord! { x: 3.0, y: 4.0 }, coord! { x: 5.0, y: 6.0 }])),
		Geometry::Polygon(Polygon::new(
			LineString::new(vec![
				coord! { x: 7.0, y: 8.0 },
				coord! { x: 9.0, y: 10.0 },
				coord! { x: 11.0, y: 12.0 },
			]),
			vec![],
		)),
	]))))
}

/// Record with table data
pub fn record_table() -> Record {
	Record::new(Data::from(Value::Table(TableName::from("users"))))
}

/// Record with record ID data
pub fn record_recordid() -> Record {
	Record::new(Data::from(Value::RecordId(RecordId::new(TableName::from("users"), 123))))
}

/// Record with file data
pub fn record_file() -> Record {
	Record::new(Data::from(Value::File(File::new("bucket".to_string(), "key".to_string()))))
}

/// Record with range data
pub fn record_range_unbounded() -> Record {
	Record::new(Data::from(Value::Range(Box::new(Range::unbounded()))))
}

/// Record with range data
pub fn record_range_bounded() -> Record {
	Record::new(Data::from(Value::Range(Box::new(Range {
		start: Bound::Included(Value::Number(Number::Int(123))),
		end: Bound::Excluded(Value::Number(Number::Int(456))),
	}))))
}

/// Record with regex data
pub fn record_regex() -> Record {
	Record::new(Data::from(Value::Regex(Regex::from_str("^test.*").unwrap())))
}

/// Record with array data
pub fn record_array() -> Record {
	Record::new(Data::from(Value::Array(Array::from(vec![
		Value::String("item1".to_string()),
		Value::Number(Number::Int(123)),
		Value::Bool(true),
	]))))
}

/// Record with object data
pub fn record_object() -> Record {
	let mut obj = Object::default();
	obj.insert("name".to_string(), Value::String("Alice".to_string()));
	obj.insert("age".to_string(), Value::Number(Number::Int(30)));
	obj.insert("active".to_string(), Value::Bool(true));
	Record::new(Data::from(Value::Object(obj)))
}

/// Record with set data
pub fn record_set() -> Record {
	Record::new(Data::from(Value::Set(Set::from(vec![
		Value::String("tag1".to_string()),
		Value::String("tag2".to_string()),
		Value::String("tag3".to_string()),
	]))))
}

/// Record with metadata (Edge type)
pub fn record_with_metadata() -> Record {
	let mut record = Record::new(Data::from(Value::Object(Object::default())));
	record.set_record_type(RecordType::Edge);
	record
}

// ===========================================================================
// MajorVersion fixtures
// ===========================================================================

/// Major version 1
pub fn version_1() -> MajorVersion {
	MajorVersion::from(1)
}

/// Major version 3
pub fn version_3() -> MajorVersion {
	MajorVersion::from(3)
}

// ===========================================================================
// ApiActionDefinition fixtures
// ===========================================================================

/// Minimal API action definition
pub fn api_action_basic() -> ApiActionDefinition {
	ApiActionDefinition {
		methods: vec![ApiMethod::Get],
		action: Expr::Literal(Literal::String("SELECT * FROM users".to_string())),
		config: ApiConfigDefinition::default(),
	}
}

/// API action with multiple methods
pub fn api_action_multi_method() -> ApiActionDefinition {
	ApiActionDefinition {
		methods: vec![ApiMethod::Get, ApiMethod::Post, ApiMethod::Put],
		action: Expr::Literal(Literal::String("CREATE users CONTENT $body".to_string())),
		config: ApiConfigDefinition::default(),
	}
}

// ===========================================================================
// ID Type fixtures
// ===========================================================================

/// IndexId fixture
pub fn index_id_basic() -> IndexId {
	IndexId(42)
}

/// DatabaseId fixture
pub fn database_id_basic() -> DatabaseId {
	DatabaseId(123)
}

/// NamespaceId fixture
pub fn namespace_id_basic() -> NamespaceId {
	NamespaceId(456)
}

/// TableId fixture
pub fn table_id_basic() -> TableId {
	TableId(789)
}

// ===========================================================================
// ModuleDefinition fixtures
// ===========================================================================

/// Module with Surrealism executable
pub fn module_surrealism() -> ModuleDefinition {
	ModuleDefinition {
		name: Some("my_module".to_string()),
		comment: Some("Custom module".to_string()),
		permissions: Permission::Full,
		executable: ModuleExecutable::Surrealism(SurrealismExecutable {
			bucket: "my_bucket".to_string(),
			key: "module_key".to_string(),
		}),
	}
}

/// Module with Silo executable
pub fn module_silo() -> ModuleDefinition {
	ModuleDefinition {
		name: Some("silo_module".to_string()),
		comment: None,
		permissions: Permission::Full,
		executable: ModuleExecutable::Silo(SiloExecutable {
			organisation: "org".to_string(),
			package: "pkg".to_string(),
			major: 1,
			minor: 2,
			patch: 3,
		}),
	}
}

// ===========================================================================
// NodeLiveQuery fixtures
// ===========================================================================

/// Minimal node live query
pub fn node_live_query_basic() -> NodeLiveQuery {
	NodeLiveQuery {
		ns: NamespaceId(1),
		db: DatabaseId(2),
		tb: TableName::from("users"),
	}
}

// ===========================================================================
// TableMutations fixtures
// ===========================================================================

/// Table mutations with set operation
pub fn table_mutations_set() -> TableMutations {
	let mut mutations = TableMutations::new(TableName::from("users"));
	let mut obj = Object::default();
	obj.insert("name".to_string(), Value::String("Alice".to_string()));
	mutations
		.1
		.push(TableMutation::Set(RecordId::new(TableName::from("users"), 1), Value::Object(obj)));
	mutations
}

/// Table mutations with delete operation
pub fn table_mutations_del() -> TableMutations {
	let mut mutations = TableMutations::new(TableName::from("users"));
	mutations.1.push(TableMutation::Del(RecordId::new(TableName::from("users"), 1)));
	mutations
}

// ===========================================================================
// Node fixtures
// ===========================================================================

/// Active node
pub fn node_active() -> Node {
	Node::new(
		UuidExt::nil(),
		Timestamp {
			value: 1234567890,
		},
		false,
	)
}

/// Archived node
pub fn node_archived() -> Node {
	Node::new(
		UuidExt::nil(),
		Timestamp {
			value: 9876543210,
		},
		true,
	)
}

// ===========================================================================
// TermDocument fixtures
// ===========================================================================

/// Term document - basic default
pub fn term_document_basic() -> TermDocument {
	TermDocument::new(123, vec![Offset::new(1, 2, 3, 4)])
}

// ===========================================================================
// DocLengthAndCount fixtures
// ===========================================================================

/// Document length and count - basic default
pub fn doc_length_and_count_basic() -> DocLengthAndCount {
	DocLengthAndCount::new(123, 456)
}

// ===========================================================================
// Appending fixtures
// ===========================================================================

pub fn appending_none() -> Appending {
	Appending::new(None, None, RecordIdKey::Number(123))
}

pub fn appending_old_values() -> Appending {
	Appending::new(
		Some(vec![Value::String("old value".to_string())]),
		None,
		RecordIdKey::Number(123),
	)
}

pub fn appending_new_values() -> Appending {
	Appending::new(
		None,
		Some(vec![Value::String("new value".to_string())]),
		RecordIdKey::Number(123),
	)
}

pub fn appending_both() -> Appending {
	Appending::new(
		Some(vec![Value::String("old value".to_string())]),
		Some(vec![Value::String("new value".to_string())]),
		RecordIdKey::Number(123),
	)
}

// ===========================================================================
// PrimaryAppending fixtures
// ===========================================================================

pub fn primary_appending_basic() -> PrimaryAppending {
	PrimaryAppending::new(123, 456)
}

// ===========================================================================
// BatchValue fixtures
// ===========================================================================

pub fn batch_value_basic() -> BatchValue {
	BatchValue::new(123, uuid::Uuid::from_str("123e4567-e89b-12d3-a456-426614174000").unwrap())
}

// ===========================================================================
// SequenceState fixtures
// ===========================================================================

pub fn sequence_state_basic() -> SequenceState {
	SequenceState::new(123)
}

// ===========================================================================
// TaskLease fixtures
// ===========================================================================

pub fn task_lease_basic() -> TaskLease {
	TaskLease::new(
		uuid::Uuid::from_str("123e4567-e89b-12d3-a456-426614174000").unwrap(),
		DateTime::from_str("2026-01-12T12:00:00Z").unwrap(),
	)
}

// ===========================================================================
// RecordId fixtures (explicit)
// ===========================================================================

/// RecordId with number key
pub fn recordid_number() -> RecordId {
	RecordId::new(TableName::from("users"), 123)
}

/// RecordId with string key
pub fn recordid_string() -> RecordId {
	RecordId::new(TableName::from("users"), "abc123".to_string())
}

/// RecordId with UUID key
pub fn recordid_uuid() -> RecordId {
	RecordId::new(TableName::from("users"), Uuid(UuidExt::nil()))
}

// ===========================================================================
// RecordIdKey fixtures
// ===========================================================================

/// RecordIdKey with number
pub fn recordid_key_number() -> RecordIdKey {
	RecordIdKey::Number(42)
}

/// RecordIdKey with string
pub fn recordid_key_string() -> RecordIdKey {
	RecordIdKey::String("test_key".to_string())
}

/// RecordIdKey with UUID
pub fn recordid_key_uuid() -> RecordIdKey {
	RecordIdKey::Uuid(Uuid(UuidExt::nil()))
}

/// RecordIdKey with array
pub fn recordid_key_array() -> RecordIdKey {
	RecordIdKey::Array(Array::from(vec![
		Value::Number(Number::Int(1)),
		Value::String("a".to_string()),
	]))
}

/// RecordIdKey with object
pub fn recordid_key_object() -> RecordIdKey {
	let mut obj = Object::default();
	obj.insert("id".to_string(), Value::Number(Number::Int(123)));
	RecordIdKey::Object(obj)
}
