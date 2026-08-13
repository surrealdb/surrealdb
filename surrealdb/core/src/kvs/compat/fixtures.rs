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
use surrealdb_datastore::values::changefeed::{TableMutation, TableMutations};
use surrealdb_strand::Strand;
use uuid::Uuid as UuidExt;

use crate::catalog::auth::{AuthLevel, AuthLimit};
use crate::catalog::record::{Record, RecordType};
use crate::catalog::schema::base::Base;
use crate::catalog::schema::{StoredReference, StoredReferenceDeleteStrategy};
use crate::catalog::{
	ApiMethod, DatabaseId, IndexId, ModuleExecutable, NamespaceId, NodeLiveQuery, SiloExecutable,
	StoredApiActionDefinition, StoredApiConfigDefinition, StoredModuleDefinition,
	SurrealismExecutable, TableId, TaskLease, *,
};
use crate::dbs::node::{Node, Timestamp};
use crate::expr::field::Selector;
use crate::expr::{
	ChangeFeed, Expr, Field, Fields, Filter, Groups, Idiom, Kind, Literal, Operation, Tokenizer,
};
use crate::iam::Auth;
use crate::idx::ft::fulltext::{DocLengthAndCount, TermDocument};
use crate::idx::ft::offset::Offset;
use crate::kvs::index::{Appending, PrimaryAppending};
use crate::kvs::sequences::{BatchValue, SequenceState};
use crate::kvs::version::MajorVersion;
use crate::val::{
	Array, Bytes, Datetime, Duration as ValDuration, File, Geometry, Number, Object, Range,
	RecordId, RecordIdKey, RecordIdKeyRange, Regex, Set, TableName, Uuid, Value,
};

// ===========================================================================
// NamespaceDefinition fixtures
// ===========================================================================

/// Minimal namespace without comment
pub fn namespace_basic() -> NamespaceDefinition {
	NamespaceDefinition {
		namespace_id: NamespaceId(1),
		name: "test".into(),
		comment: None,
	}
}

/// Namespace with optional comment
pub fn namespace_with_comment() -> NamespaceDefinition {
	NamespaceDefinition {
		namespace_id: NamespaceId(123),
		name: "production".into(),
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
		name: "test".into(),
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
		name: "events".into(),
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
		name: "strict_db".into(),
		strict: true,
		comment: Some("Strict mode database".to_string()),
		changefeed: None,
	}
}

// ===========================================================================
// StoredTableDefinition fixtures
// ===========================================================================

/// Minimal table definition
pub fn table_basic() -> StoredTableDefinition {
	StoredTableDefinition {
		namespace_id: NamespaceId(1),
		database_id: DatabaseId(1),
		table_id: TableId(1),
		name: "users".into(),
		drop: false,
		schemafull: false,
		view: None,
		permissions: StoredPermissions::default(),
		changefeed: None,
		comment: None,
		table_type: TableType::Normal,
		cache_fields_ts: UuidExt::nil(),
		cache_events_ts: UuidExt::nil(),
		cache_tables_ts: UuidExt::nil(),
		cache_indexes_ts: UuidExt::nil(),
		cache_lives_ts: UuidExt::nil(),
		graphql_alias: None,
		graphql_deprecated: None,
	}
}

/// Table with a view, in the shape views were stored in before the clauses
/// became text. These bytes decode to the legacy variant, unchanged.
pub fn table_with_view() -> StoredTableDefinition {
	StoredTableDefinition {
		namespace_id: NamespaceId(123),
		database_id: DatabaseId(456),
		table_id: TableId(789),
		name: "user_stats".into(),
		drop: false,
		schemafull: false,
		view: Some(StoredViewDefinition::Select {
			fields: Fields::Select(vec![
				Field::All,
				Field::Single(Selector {
					expr: Expr::Literal(Literal::String(Strand::new_static("count"))),
					alias: Some(Idiom::from_str("total").unwrap()),
				}),
			]),
			tables: vec![TableName::from("users")],
			condition: Some(Expr::Literal(Literal::String(Strand::new_static("active = true")))),
			groups: Some(Groups::default()),
		}),
		permissions: StoredPermissions::default(),
		changefeed: None,
		comment: Some("User statistics view".to_string()),
		table_type: TableType::Normal,
		cache_fields_ts: UuidExt::nil(),
		cache_events_ts: UuidExt::nil(),
		cache_tables_ts: UuidExt::nil(),
		cache_indexes_ts: UuidExt::nil(),
		cache_lives_ts: UuidExt::nil(),
		graphql_alias: None,
		graphql_deprecated: None,
	}
}

/// Schemafull table with changefeed
pub fn table_schemafull() -> StoredTableDefinition {
	StoredTableDefinition {
		namespace_id: NamespaceId(1),
		database_id: DatabaseId(1),
		table_id: TableId(2),
		name: "orders".into(),
		drop: false,
		schemafull: true,
		view: None,
		permissions: StoredPermissions::default(),
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
		cache_lives_ts: UuidExt::nil(),
		graphql_alias: None,
		graphql_deprecated: None,
	}
}

/// Relation table with drop and non-default permissions
pub fn table_relation() -> StoredTableDefinition {
	StoredTableDefinition {
		namespace_id: NamespaceId(10),
		database_id: DatabaseId(20),
		table_id: TableId(30),
		name: "likes".into(),
		drop: true,
		schemafull: true,
		view: None,
		permissions: StoredPermissions {
			select: StoredPermission::Full,
			create: StoredPermission::Specific(ExprText::from_raw("\"$auth.role = 'admin'\"")),
			update: StoredPermission::None,
			delete: StoredPermission::None,
		},
		changefeed: None,
		comment: Some("User likes relation".to_string()),
		table_type: TableType::Relation(Relation {
			from: vec![TableName::from("users")],
			to: vec![TableName::from("posts"), TableName::from("comments")],
			enforced: true,
		}),
		cache_fields_ts: UuidExt::nil(),
		cache_events_ts: UuidExt::nil(),
		cache_tables_ts: UuidExt::nil(),
		cache_indexes_ts: UuidExt::nil(),
		cache_lives_ts: UuidExt::nil(),
		graphql_alias: None,
		graphql_deprecated: None,
	}
}

/// Table with a view stored as its clauses — the shape every write produces.
///
/// The two fixtures above freeze the shapes that predate it; this one freezes
/// the current one, so the byte-exact guard covers what the encoder actually
/// emits rather than only what it can still read.
pub fn table_with_stored_clauses() -> StoredTableDefinition {
	StoredTableDefinition {
		namespace_id: NamespaceId(7),
		database_id: DatabaseId(9),
		table_id: TableId(11),
		name: "order_totals".into(),
		drop: false,
		schemafull: false,
		view: Some(StoredViewDefinition::Clauses {
			fields: FieldsText::from_raw("customer, math::sum(amount) AS total"),
			tables: vec![TableName::from("orders")],
			condition: Some(ExprText::from_raw("paid = true")),
			groups: Some(vec![IdiomText::from_raw("customer")]),
		}),
		permissions: StoredPermissions::default(),
		changefeed: None,
		comment: Some("Order totals per customer".to_string()),
		table_type: TableType::Normal,
		cache_fields_ts: UuidExt::nil(),
		cache_events_ts: UuidExt::nil(),
		cache_tables_ts: UuidExt::nil(),
		cache_indexes_ts: UuidExt::nil(),
		cache_lives_ts: UuidExt::nil(),
		graphql_alias: None,
		graphql_deprecated: None,
	}
}

/// Table with a materialized view, in the pre-`Clauses` shape.
pub fn table_with_materialized_view() -> StoredTableDefinition {
	StoredTableDefinition {
		namespace_id: NamespaceId(1),
		database_id: DatabaseId(1),
		table_id: TableId(100),
		name: "active_users".into(),
		drop: false,
		schemafull: false,
		view: Some(StoredViewDefinition::Materialized {
			fields: Fields::Select(vec![Field::All]),
			tables: vec![TableName::from("users")],
			condition: Some(Expr::Literal(Literal::String(Strand::new_static("active = true")))),
		}),
		permissions: StoredPermissions::default(),
		changefeed: None,
		comment: Some("Materialized view of active users".to_string()),
		table_type: TableType::Normal,
		cache_fields_ts: UuidExt::nil(),
		cache_events_ts: UuidExt::nil(),
		cache_tables_ts: UuidExt::nil(),
		cache_indexes_ts: UuidExt::nil(),
		cache_lives_ts: UuidExt::nil(),
		graphql_alias: None,
		graphql_deprecated: None,
	}
}

/// Table with TableType::Any (default variant)
pub fn table_any_type() -> StoredTableDefinition {
	StoredTableDefinition {
		namespace_id: NamespaceId(1),
		database_id: DatabaseId(1),
		table_id: TableId(50),
		name: "flexible".into(),
		drop: false,
		schemafull: false,
		view: None,
		permissions: StoredPermissions::default(),
		changefeed: None,
		comment: None,
		table_type: TableType::Any,
		cache_fields_ts: UuidExt::nil(),
		cache_events_ts: UuidExt::nil(),
		cache_tables_ts: UuidExt::nil(),
		cache_indexes_ts: UuidExt::nil(),
		cache_lives_ts: UuidExt::nil(),
		graphql_alias: None,
		graphql_deprecated: None,
	}
}

// ===========================================================================
// StoredSubscriptionDefinition fixtures
// ===========================================================================

/// Minimal subscription with diff fields
pub fn subscription_basic() -> StoredSubscriptionDefinition {
	StoredSubscriptionDefinition {
		id: UuidExt::nil(),
		node: UuidExt::nil(),
		fields: StoredSubscriptionFields::Diff,
		what: ExprText::new(&Expr::Literal(Literal::String(Strand::new_static("users")))),
		cond: None,
		fetch: None,
		auth: None,
		session: None,
		vars: BTreeMap::new(),
	}
}

/// Subscription with condition and fetch
pub fn subscription_with_filters() -> StoredSubscriptionDefinition {
	StoredSubscriptionDefinition {
		id: UuidExt::nil(),
		node: UuidExt::nil(),
		fields: StoredSubscriptionFields::Select(FieldsText::new(&Fields::Select(vec![
			Field::All,
			Field::Single(Selector {
				expr: Expr::Literal(Literal::String(Strand::new_static("name"))),
				alias: None,
			}),
		]))),
		what: ExprText::new(&Expr::Literal(Literal::String(Strand::new_static("users")))),
		cond: Some(ExprText::new(&Expr::Literal(Literal::String(Strand::new_static(
			"active = true",
		))))),
		fetch: Some(vec![ExprText::new(&Expr::Literal(Literal::String(Strand::new_static(
			"profile",
		))))]),
		auth: Some(Auth::default()),
		session: Some(Value::default()),
		vars: BTreeMap::new(),
	}
}

/// Subscription with non-empty vars
pub fn subscription_with_vars() -> StoredSubscriptionDefinition {
	let mut vars = BTreeMap::new();
	vars.insert("user_id".to_string(), Value::String(Strand::new_static("user:123")));
	vars.insert("threshold".to_string(), Value::Number(Number::Int(50)));
	StoredSubscriptionDefinition {
		id: UuidExt::nil(),
		node: UuidExt::nil(),
		fields: StoredSubscriptionFields::Diff,
		what: ExprText::new(&Expr::Literal(Literal::String(Strand::new_static("orders")))),
		cond: Some(ExprText::new(&Expr::Literal(Literal::String(Strand::new_static(
			"amount > $threshold",
		))))),
		fetch: None,
		auth: Some(Auth::default()),
		session: Some(Value::default()),
		vars,
	}
}

// ===========================================================================
// StoredAccessDefinition fixtures
// ===========================================================================

/// Bearer access with JWT
pub fn access_bearer() -> StoredAccessDefinition {
	StoredAccessDefinition {
		name: "api_access".into(),
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
pub fn access_with_authenticate() -> StoredAccessDefinition {
	StoredAccessDefinition {
		name: "custom_auth".into(),
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
		authenticate: Some(ExprText::from_raw("'SELECT * FROM user WHERE id = $auth.id'")),
		grant_duration: None,
		token_duration: Some(Duration::from_secs(3600)),
		session_duration: None,
		comment: None,
	}
}

/// Record-based access with signup/signin
pub fn access_record() -> StoredAccessDefinition {
	StoredAccessDefinition {
		name: "user_access".into(),
		access_type: AccessType::Record(RecordAccess {
			signup: Some(ExprText::from_raw(
				"'CREATE user SET email = $email, pass = crypto::argon2::generate($pass)'",
			)),
			signin: Some(ExprText::from_raw(
				"'SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass)'",
			)),
			jwt: JwtAccess {
				verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
					alg: Algorithm::Hs256,
					key: "jwt_secret".to_string(),
				}),
				issue: Some(JwtAccessIssue {
					alg: Algorithm::Hs256,
					key: "jwt_secret".to_string(),
				}),
			},
			bearer: Some(BearerAccess {
				kind: BearerAccessType::Refresh,
				subject: BearerAccessSubject::Record,
				jwt: JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::Hs256,
						key: "refresh_secret".to_string(),
					}),
					issue: None,
				},
			}),
		}),
		base: Base::Db,
		authenticate: Some(ExprText::from_raw("'SELECT * FROM user WHERE id = $auth.id'")),
		grant_duration: Some(Duration::from_secs(604800)),
		token_duration: Some(Duration::from_secs(900)),
		session_duration: Some(Duration::from_secs(86400)),
		comment: Some("User record access".to_string()),
	}
}

/// JWT access with JWKS verification
pub fn access_jwt_jwks() -> StoredAccessDefinition {
	StoredAccessDefinition {
		name: "external_jwt".into(),
		access_type: AccessType::Jwt(JwtAccess {
			verify: JwtAccessVerify::Jwks(JwtAccessVerifyJwks {
				url: "https://auth.example.com/.well-known/jwks.json".to_string(),
			}),
			issue: None,
		}),
		base: Base::Ns,
		authenticate: None,
		grant_duration: None,
		token_duration: None,
		session_duration: Some(Duration::from_secs(3600)),
		comment: Some("External JWT verification via JWKS".to_string()),
	}
}

/// Bearer access with refresh type
pub fn access_bearer_refresh() -> StoredAccessDefinition {
	StoredAccessDefinition {
		name: "refresh_access".into(),
		access_type: AccessType::Bearer(BearerAccess {
			kind: BearerAccessType::Refresh,
			subject: BearerAccessSubject::Record,
			jwt: JwtAccess {
				verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
					alg: Algorithm::Rs256,
					key: "rsa_public_key".to_string(),
				}),
				issue: Some(JwtAccessIssue {
					alg: Algorithm::Rs256,
					key: "rsa_private_key".to_string(),
				}),
			},
		}),
		base: Base::Root,
		authenticate: None,
		grant_duration: Some(Duration::from_secs(2592000)),
		token_duration: Some(Duration::from_secs(300)),
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

/// Record-type access grant with record subject
pub fn grant_record() -> AccessGrant {
	AccessGrant {
		id: "grant_003".to_string(),
		ac: "user_access".to_string(),
		creation: Datetime::MIN_UTC,
		expiration: Some(Datetime::MIN_UTC),
		revocation: None,
		subject: Subject::Record(RecordId::new(TableName::from("users"), 42)),
		grant: Grant::Record(GrantRecord {
			rid: UuidExt::nil(),
			jti: UuidExt::nil(),
			token: Some("eyJhbGciOiJIUzI1NiJ9.record_token".to_string()),
		}),
	}
}

/// Bearer-type access grant
pub fn grant_bearer() -> AccessGrant {
	AccessGrant {
		id: "grant_004".to_string(),
		ac: "refresh_access".to_string(),
		creation: Datetime::MIN_UTC,
		expiration: None,
		revocation: None,
		subject: Subject::User("service_account".to_string()),
		grant: Grant::Bearer(GrantBearer {
			id: "surreal-bearer-key-001".to_string(),
			key: "surreal-bearer-xxxxxxxxxxxxxxxx".to_string(),
		}),
	}
}

// ===========================================================================
// AnalyzerDefinition fixtures
// ===========================================================================

/// Minimal analyzer
pub fn analyzer_basic() -> AnalyzerDefinition {
	AnalyzerDefinition {
		name: "simple".into(),
		function: None,
		tokenizers: None,
		filters: None,
		comment: None,
	}
}

/// Analyzer with tokenizers and filters
pub fn analyzer_with_tokenizers() -> AnalyzerDefinition {
	AnalyzerDefinition {
		name: "english".into(),
		function: Some("fn::custom_analyzer".into()),
		tokenizers: Some(vec![Tokenizer::Camel, Tokenizer::Class]),
		filters: Some(vec![Filter::Ascii, Filter::Lowercase]),
		comment: Some("English text analyzer".to_string()),
	}
}

// ===========================================================================
// StoredApiDefinition fixtures
// ===========================================================================

/// Minimal API endpoint
pub fn api_basic() -> StoredApiDefinition {
	StoredApiDefinition {
		path: PathText::from_raw("/api/v1/users"),
		actions: vec![StoredApiActionDefinition {
			methods: vec![ApiMethod::Get],
			action: ExprText::from_raw("'SELECT * FROM users'"),
			config: StoredApiConfigDefinition::default(),
		}],
		fallback: None,
		config: StoredApiConfigDefinition::default(),
		comment: None,
		auth_limit: AuthLimit::new_no_limit(),
	}
}

/// API with middleware and multiple methods
pub fn api_with_middleware() -> StoredApiDefinition {
	StoredApiDefinition {
		auth_limit: AuthLimit::new_no_limit(),
		path: PathText::from_raw("/api/v1/orders"),
		actions: vec![
			StoredApiActionDefinition {
				methods: vec![ApiMethod::Get, ApiMethod::Post],
				action: ExprText::from_raw("'SELECT * FROM orders'"),
				config: StoredApiConfigDefinition::default(),
			},
			StoredApiActionDefinition {
				methods: vec![ApiMethod::Delete],
				action: ExprText::from_raw("'DELETE FROM orders'"),
				config: StoredApiConfigDefinition::default(),
			},
		],
		fallback: Some(ExprText::from_raw("'RETURN 404'")),
		config: StoredApiConfigDefinition {
			middleware: vec![
				MiddlewareDefinition {
					name: "auth".into(),
					args: vec![],
				},
				MiddlewareDefinition {
					name: "rate_limit".into(),
					args: vec![Value::from(100)],
				},
			],
			permissions: StoredPermission::Full,
		},
		comment: Some("Order management API".to_string()),
	}
}

/// API with specific permissions, database-level auth limit, and more HTTP methods
pub fn api_with_auth_limit() -> StoredApiDefinition {
	StoredApiDefinition {
		path: PathText::from_raw("/api/v1/admin"),
		actions: vec![
			StoredApiActionDefinition {
				methods: vec![ApiMethod::Get, ApiMethod::Put, ApiMethod::Patch],
				action: ExprText::from_raw("'SELECT * FROM admin_data'"),
				config: StoredApiConfigDefinition {
					middleware: vec![],
					permissions: StoredPermission::Specific(ExprText::from_raw(
						"\"$auth.role = 'admin'\"",
					)),
				},
			},
			StoredApiActionDefinition {
				methods: vec![ApiMethod::Delete, ApiMethod::Trace],
				action: ExprText::from_raw("\"RETURN { status: 'ok' }\""),
				config: StoredApiConfigDefinition::default(),
			},
		],
		fallback: None,
		config: StoredApiConfigDefinition::default(),
		comment: Some("Admin API with restricted access".to_string()),
		auth_limit: AuthLimit::new(
			AuthLevel::Database("prod_ns".to_string(), "prod_db".to_string()),
			Some("Owner".to_string()),
		),
	}
}

// ===========================================================================
// StoredBucketDefinition fixtures
// ===========================================================================

/// Minimal bucket
pub fn bucket_basic() -> StoredBucketDefinition {
	StoredBucketDefinition {
		id: None,
		readonly: false,
		name: "uploads".into(),
		backend: None,
		comment: None,
		permissions: StoredPermission::Full,
	}
}

/// Readonly bucket with backend
pub fn bucket_readonly() -> StoredBucketDefinition {
	StoredBucketDefinition {
		id: Some(BucketId(123)),
		readonly: true,
		name: "archives".into(),
		backend: Some("s3://bucket/archives".into()),
		comment: Some("Read-only archive storage".to_string()),
		permissions: StoredPermission::None,
	}
}

// ===========================================================================
// StoredConfigDefinition fixtures
// ===========================================================================

/// GraphQL configuration (default)
pub fn config_graphql() -> StoredConfigDefinition {
	StoredConfigDefinition::GraphQL(GraphQLConfig::default())
}

/// Default config with namespace and database
pub fn config_default() -> StoredConfigDefinition {
	StoredConfigDefinition::Default(DefaultConfig {
		namespace: Some("production".to_string()),
		database: Some("main".to_string()),
	})
}

/// API config definition
pub fn config_api() -> StoredConfigDefinition {
	StoredConfigDefinition::Api(StoredApiConfigDefinition {
		middleware: vec![MiddlewareDefinition {
			name: "cors".into(),
			args: vec![Value::String(Strand::new_static("*"))],
		}],
		permissions: StoredPermission::Specific(ExprText::from_raw("\"$auth.role = 'admin'\"")),
	})
}

/// GraphQL config with all non-default fields populated
pub fn config_graphql_full() -> StoredConfigDefinition {
	StoredConfigDefinition::GraphQL(GraphQLConfig {
		tables: GraphQLTablesConfig::Include(vec![
			TableName::from("users"),
			TableName::from("posts"),
		]),
		functions: GraphQLFunctionsConfig::Auto,
		depth_limit: Some(10),
		complexity_limit: Some(1000),
		introspection: GraphQLIntrospectionConfig::None,
	})
}

// ===========================================================================
// StoredEventDefinition fixtures
// ===========================================================================

/// Table event trigger
pub fn event_basic() -> StoredEventDefinition {
	StoredEventDefinition {
		name: "on_create".into(),
		target_table: "users".into(),
		when: ExprText::from_raw("\"$event = 'CREATE'\""),
		then: vec![ExprText::from_raw("\"CREATE audit SET action = 'create'\"")],
		comment: Some("Audit log on create".to_string()),
		auth_limit: AuthLimit::new_no_limit(),
		kind: EventKind::Sync,
	}
}

/// Async event with retry and max_depth
pub fn event_async() -> StoredEventDefinition {
	StoredEventDefinition {
		name: "on_update_async".into(),
		target_table: "orders".into(),
		when: ExprText::from_raw("\"$event = 'UPDATE'\""),
		then: vec![ExprText::from_raw(
			"\"CREATE notification SET order = $after.id, type = 'updated'\"",
		)],
		comment: Some("Async notification on order update".to_string()),
		auth_limit: AuthLimit::new_no_limit(),
		kind: EventKind::Async {
			retry: 3,
			max_depth: 5,
		},
	}
}

// ===========================================================================
// StoredFieldDefinition fixtures
// ===========================================================================

/// Minimal field
pub fn field_basic() -> StoredFieldDefinition {
	StoredFieldDefinition {
		name: IdiomText::from_raw("name"),
		table: TableName::from("users"),
		field_kind: None,
		readonly: false,
		flexible: false,
		value: None,
		assert: None,
		computed: None,
		default: StoredDefineDefault::None,
		select_permission: StoredPermission::Full,
		create_permission: StoredPermission::Full,
		update_permission: StoredPermission::Full,
		comment: None,
		reference: None,
		auth_limit: AuthLimit::new_no_limit(),
		graphql_alias: None,
		graphql_deprecated: None,
	}
}

/// Field with type constraint and default
pub fn field_with_type() -> StoredFieldDefinition {
	StoredFieldDefinition {
		name: IdiomText::from_raw("email"),
		table: TableName::from("users"),
		field_kind: Some(KindText::new(&Kind::String)),
		readonly: false,
		flexible: false,
		value: Some(ExprText::from_raw("'string::lowercase($value)'")),
		assert: Some(ExprText::from_raw("'string::is::email($value)'")),
		computed: None,
		default: StoredDefineDefault::Always(ExprText::from_raw("''")),
		select_permission: StoredPermission::Full,
		create_permission: StoredPermission::Full,
		update_permission: StoredPermission::Full,
		comment: Some("User email address".to_string()),
		reference: None,
		auth_limit: AuthLimit::new_no_limit(),
		graphql_alias: None,
		graphql_deprecated: None,
	}
}

/// Readonly computed field
pub fn field_readonly() -> StoredFieldDefinition {
	StoredFieldDefinition {
		name: IdiomText::from_raw("created_at"),
		table: TableName::from("users"),
		field_kind: Some(KindText::new(&Kind::Datetime)),
		readonly: true,
		flexible: false,
		value: None,
		assert: None,
		computed: Some(ExprText::from_raw("'time::now()'")),
		default: StoredDefineDefault::None,
		select_permission: StoredPermission::Full,
		create_permission: StoredPermission::None,
		update_permission: StoredPermission::None,
		comment: Some("Record creation timestamp".to_string()),
		reference: None,
		auth_limit: AuthLimit::new_no_limit(),
		graphql_alias: None,
		graphql_deprecated: None,
	}
}

/// Flexible field with reference and computed deps
pub fn field_flexible_with_reference() -> StoredFieldDefinition {
	StoredFieldDefinition {
		name: IdiomText::from_raw("total_price"),
		table: TableName::from("orders"),
		field_kind: Some(KindText::new(&Kind::Number)),
		readonly: false,
		flexible: true,
		value: Some(ExprText::from_raw("'$price * $quantity'")),
		assert: None,
		computed: None,
		default: StoredDefineDefault::None,
		select_permission: StoredPermission::Full,
		create_permission: StoredPermission::Full,
		update_permission: StoredPermission::Specific(ExprText::from_raw(
			"\"$auth.role = 'admin'\"",
		)),
		comment: Some("Calculated total price".to_string()),
		reference: Some(StoredReference {
			on_delete: StoredReferenceDeleteStrategy::Cascade,
		}),
		auth_limit: AuthLimit::new_no_limit(),
		graphql_alias: None,
		graphql_deprecated: None,
	}
}

/// Field with DefineDefault::Set, Permission::Specific, and incomplete computed deps
pub fn field_with_default_set() -> StoredFieldDefinition {
	StoredFieldDefinition {
		name: IdiomText::from_raw("status"),
		table: TableName::from("orders"),
		field_kind: Some(KindText::new(&Kind::String)),
		readonly: false,
		flexible: false,
		value: None,
		assert: Some(ExprText::from_raw("\"$value INSIDE ['pending', 'active', 'closed']\"")),
		computed: None,
		default: StoredDefineDefault::Set(ExprText::from_raw("\"'pending'\"")),
		select_permission: StoredPermission::Full,
		create_permission: StoredPermission::Full,
		update_permission: StoredPermission::Specific(ExprText::from_raw(
			"\"$auth.role = 'manager'\"",
		)),
		comment: None,
		reference: Some(StoredReference {
			on_delete: StoredReferenceDeleteStrategy::Reject,
		}),
		auth_limit: AuthLimit::new_no_limit(),
		graphql_alias: None,
		graphql_deprecated: None,
	}
}

/// Field with record type kind and custom reference delete strategy
pub fn field_record_type() -> StoredFieldDefinition {
	StoredFieldDefinition {
		name: IdiomText::from_raw("author"),
		table: TableName::from("posts"),
		field_kind: Some(KindText::new(&Kind::Record(vec![TableName::from("users")]))),
		readonly: true,
		flexible: false,
		value: None,
		assert: None,
		computed: None,
		default: StoredDefineDefault::None,
		select_permission: StoredPermission::Full,
		create_permission: StoredPermission::Full,
		update_permission: StoredPermission::None,
		comment: Some("Author reference".to_string()),
		reference: Some(StoredReference {
			on_delete: StoredReferenceDeleteStrategy::Custom(ExprText::from_raw(
				"'DELETE $parent'",
			)),
		}),
		auth_limit: AuthLimit::new(
			AuthLevel::Database("test_ns".to_string(), "test_db".to_string()),
			Some("Editor".to_string()),
		),
		graphql_alias: None,
		graphql_deprecated: None,
	}
}

// ===========================================================================
// StoredFunctionDefinition fixtures
// ===========================================================================

/// Simple function
pub fn function_basic() -> StoredFunctionDefinition {
	StoredFunctionDefinition {
		name: "greet".into(),
		args: vec![],
		block: BlockText::from_raw("{ \"RETURN 'Hello, World!'\" }"),
		comment: None,
		permissions: StoredPermission::Full,
		returns: None,
		auth_limit: AuthLimit::new_no_limit(),
		graphql_alias: None,
		graphql_deprecated: None,
	}
}

/// Function with arguments and return type
pub fn function_with_args() -> StoredFunctionDefinition {
	StoredFunctionDefinition {
		name: "add_numbers".into(),
		args: vec![
			("a".to_string(), KindText::new(&Kind::Number)),
			("b".to_string(), KindText::new(&Kind::Number)),
		],
		block: BlockText::from_raw("{ 'RETURN $a + $b' }"),
		comment: Some("Add two numbers".to_string()),
		permissions: StoredPermission::Full,
		returns: Some(KindText::new(&Kind::Number)),
		auth_limit: AuthLimit::new_no_limit(),
		graphql_alias: None,
		graphql_deprecated: None,
	}
}

// ===========================================================================
// StoredIndexDefinition fixtures
// ===========================================================================

/// Basic index
pub fn index_basic() -> StoredIndexDefinition {
	StoredIndexDefinition {
		index_id: IndexId(1),
		name: "idx_name".into(),
		table_name: "users".into(),
		cols: vec![IdiomText::from_raw("name")],
		index: Index::Idx,
		comment: None,
		prepare_remove: false,
		format_version: 0,
	}
}

/// Unique index on multiple columns
pub fn index_unique() -> StoredIndexDefinition {
	StoredIndexDefinition {
		index_id: IndexId(2),
		name: "idx_email_unique".into(),
		table_name: "users".into(),
		cols: vec![IdiomText::from_raw("email")],
		index: Index::Uniq,
		comment: Some("Unique email constraint".to_string()),
		prepare_remove: false,
		format_version: 0,
	}
}

/// HNSW vector index
pub fn index_hnsw() -> StoredIndexDefinition {
	StoredIndexDefinition {
		index_id: IndexId(3),
		name: "idx_embedding_hnsw".into(),
		table_name: "documents".into(),
		cols: vec![IdiomText::from_raw("embedding")],
		index: Index::Hnsw(HnswParams {
			dimension: 1536,
			distance: Distance::Cosine,
			vector_type: VectorType::F32,
			m: 12,
			m0: 24,
			ml: Number::Float(1.0 / (12_f64).ln()),
			ef_construction: 150,
			extend_candidates: false,
			keep_pruned_connections: true,
			use_hashed_vector: false,
		}),
		comment: Some("Vector similarity search index".to_string()),
		prepare_remove: false,
		format_version: 0,
	}
}

/// Full-text search index with BM25 scoring
pub fn index_fulltext() -> StoredIndexDefinition {
	StoredIndexDefinition {
		index_id: IndexId(4),
		name: "idx_content_search".into(),
		table_name: "articles".into(),
		cols: vec![IdiomText::from_raw("title"), IdiomText::from_raw("body")],
		index: Index::FullText(FullTextParams {
			analyzer: "english".into(),
			highlight: true,
			scoring: Scoring::Bm {
				k1: 1.2,
				b: 0.75,
			},
		}),
		comment: Some("Full-text search on articles".to_string()),
		prepare_remove: false,
		format_version: 0,
	}
}

/// Count index with prepare_remove flag
pub fn index_count() -> StoredIndexDefinition {
	StoredIndexDefinition {
		index_id: IndexId(5),
		name: "idx_status_count".into(),
		table_name: "orders".into(),
		cols: vec![IdiomText::from_raw("status")],
		// `status = 'active'` contains a single quote, so the canonical
		// rendering of the old `Expr::Literal(Literal::String(..))` quotes it
		// with double quotes (QuoteStr, core/src/fmt/escape.rs).
		index: Index::Count(Some(CondText(SurqlText::from_raw("\"status = 'active'\"")))),
		comment: None,
		prepare_remove: true,
		format_version: 0,
	}
}

// ===========================================================================
// StoredMlModelDefinition fixtures
// ===========================================================================

/// ML model definition
pub fn model_basic() -> StoredMlModelDefinition {
	StoredMlModelDefinition {
		name: "sentiment".into(),
		hash: "sha256:abc123def456".into(),
		version: "1.0.0".into(),
		comment: Some("Sentiment analysis model".to_string()),
		permissions: StoredPermission::Full,
	}
}

// ===========================================================================
// StoredParamDefinition fixtures
// ===========================================================================

/// Boolean parameter
pub fn param_bool() -> StoredParamDefinition {
	StoredParamDefinition {
		name: "debug".into(),
		value: Value::Bool(true),
		comment: Some("Debug mode flag".to_string()),
		permissions: StoredPermission::Full,
	}
}

/// String parameter
pub fn param_string() -> StoredParamDefinition {
	StoredParamDefinition {
		name: "app_name".into(),
		value: Value::String(Strand::new_static("MyApp")),
		comment: None,
		permissions: StoredPermission::Full,
	}
}

// ===========================================================================
// SequenceDefinition fixtures
// ===========================================================================

/// Minimal sequence
pub fn sequence_basic() -> SequenceDefinition {
	SequenceDefinition {
		name: "order_id".into(),
		batch: 1,
		start: 1,
		timeout: None,
	}
}

/// Sequence with custom options
pub fn sequence_with_options() -> SequenceDefinition {
	SequenceDefinition {
		name: "invoice_number".into(),
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
		name: "admin".into(),
		hash: "$argon2id$v=19$m=65536,t=3,p=4$hash".to_string(),
		code: "TOTP_CODE".to_string(),
		roles: vec!["owner".to_string()],
		token_duration: None,
		session_duration: None,
		comment: None,
		base: Base::Root,
		scram: None,
	}
}

/// User with custom token/session durations
pub fn user_with_durations() -> UserDefinition {
	UserDefinition {
		name: "api_user".into(),
		hash: "$argon2id$v=19$m=65536,t=3,p=4$hash".to_string(),
		code: "".to_string(),
		roles: vec!["viewer".to_string(), "editor".to_string()],
		token_duration: Some(Duration::from_secs(3600)),
		session_duration: Some(Duration::from_secs(86400)),
		comment: Some("API service account".to_string()),
		base: Base::Ns,
		scram: None,
	}
}

/// User with database-level base
pub fn user_db_base() -> UserDefinition {
	UserDefinition {
		name: "db_user".into(),
		hash: "$argon2id$v=19$m=65536,t=3,p=4$hash".to_string(),
		code: "".to_string(),
		roles: vec!["editor".to_string()],
		token_duration: Some(Duration::from_secs(1800)),
		session_duration: None,
		comment: Some("Database-level user".to_string()),
		base: Base::Db,
		scram: None,
	}
}

/// User with SCRAM-SHA-256 verifier material (revision 2)
pub fn user_with_scram() -> UserDefinition {
	UserDefinition {
		name: "scram_user".into(),
		hash: "$argon2id$v=19$m=65536,t=3,p=4$hash".to_string(),
		code: "".to_string(),
		roles: vec!["owner".to_string()],
		token_duration: None,
		session_duration: None,
		comment: None,
		base: Base::Root,
		scram: Some(crate::catalog::ScramCredential::generate_with(
			"pencil",
			b"0123456789abcdef",
			4096,
		)),
	}
}

// ===========================================================================
// Record fixtures
// ===========================================================================

/// Record with None value
pub fn record_none() -> Record {
	Record::new(Value::None)
}

/// Record with Null value
pub fn record_null() -> Record {
	Record::new(Value::Null)
}

/// Record with boolean data
pub fn record_bool() -> Record {
	Record::new(Value::Bool(true))
}

/// Record with int number data
pub fn record_number_int() -> Record {
	Record::new(Value::Number(Number::Int(42)))
}

/// Record with float number data
pub fn record_number_float() -> Record {
	Record::new(Value::Number(Number::Float(42.0)))
}

/// Record with decimal number data
pub fn record_number_decimal() -> Record {
	Record::new(Value::Number(Number::Decimal(Decimal::from(42))))
}

/// Record with string data
pub fn record_string() -> Record {
	Record::new(Value::String(Strand::new_static("test data")))
}

/// Record with bytes data
pub fn record_bytes() -> Record {
	Record::new(Value::Bytes(Bytes::from(vec![0x01, 0x02, 0x03, 0x04])))
}

/// Record with duration data
pub fn record_duration() -> Record {
	Record::new(Value::Duration(ValDuration::from_secs(3600)))
}

/// Record with datetime data
pub fn record_datetime() -> Record {
	Record::new(Value::Datetime(Datetime::MIN_UTC))
}

/// Record with UUID data
pub fn record_uuid() -> Record {
	Record::new(Value::Uuid(Uuid(
		uuid::Uuid::from_str("123e4567-e89b-12d3-a456-426614174000").unwrap(),
	)))
}

/// Record with geometry data (point)
pub fn record_geometry_point() -> Record {
	Record::new(Value::Geometry(Geometry::Point(Point::new(1.0, 2.0))))
}

/// Record with geometry data (line)
pub fn record_geometry_line() -> Record {
	Record::new(Value::Geometry(Geometry::Line(LineString::new(vec![
		coord! { x: 1.0, y: 2.0 },
		coord! { x: 3.0, y: 4.0 },
	]))))
}

/// Record with geometry data (polygon)
pub fn record_geometry_polygon() -> Record {
	Record::new(Value::Geometry(Geometry::Polygon(Polygon::new(
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
	))))
}

/// Record with geometry data (multi point)
pub fn record_geometry_multi_point() -> Record {
	Record::new(Value::Geometry(Geometry::MultiPoint(MultiPoint::new(vec![
		Point::new(1.0, 2.0),
		Point::new(3.0, 4.0),
	]))))
}

/// Record with geometry data (multi line)
pub fn record_geometry_multi_line() -> Record {
	Record::new(Value::Geometry(Geometry::MultiLine(MultiLineString::new(vec![
		LineString::new(vec![coord! { x: 1.0, y: 2.0 }, coord! { x: 3.0, y: 4.0 }]),
		LineString::new(vec![coord! { x: 5.0, y: 6.0 }, coord! { x: 7.0, y: 8.0 }]),
	]))))
}

/// Record with geometry data (multi polygon)
pub fn record_geometry_multi_polygon() -> Record {
	Record::new(Value::Geometry(Geometry::MultiPolygon(MultiPolygon::new(vec![
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
	]))))
}

/// Record with geometry data (collection)
pub fn record_geometry_collection() -> Record {
	Record::new(Value::Geometry(Geometry::Collection(vec![
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
	])))
}

/// Record with table data
pub fn record_table() -> Record {
	Record::new(Value::Table(TableName::from("users")))
}

/// Record with record ID data
pub fn record_recordid() -> Record {
	Record::new(Value::RecordId(RecordId::new(TableName::from("users"), 123)))
}

/// Record with file data
pub fn record_file() -> Record {
	Record::new(Value::File(File::new("bucket".to_string(), "key".to_string())))
}

/// Record with range data
pub fn record_range_unbounded() -> Record {
	Record::new(Value::Range(Box::new(Range::unbounded())))
}

/// Record with range data
pub fn record_range_bounded() -> Record {
	Record::new(Value::Range(Box::new(Range {
		start: Bound::Included(Value::Number(Number::Int(123))),
		end: Bound::Excluded(Value::Number(Number::Int(456))),
	})))
}

/// Record with regex data
pub fn record_regex() -> Record {
	Record::new(Value::Regex(Regex::from_str("^test.*").unwrap()))
}

/// Record with array data
pub fn record_array() -> Record {
	Record::new(Value::Array(Array::from(vec![
		Value::String(Strand::new_static("item1")),
		Value::Number(Number::Int(123)),
		Value::Bool(true),
	])))
}

/// Sentinel `RecordId` used as the decode-time `KeyContext` for
/// `Record` compat tests. `Record::kv_decode_value` splices the rid
/// into the decoded `data` Object on the fast path; expected fixtures
/// for Object-data records must therefore include `id: test_record_rid()`
/// so `decoded == expected`. Encode strips the top-level `id` again, so
/// the byte constants in `v3_0_0_*.rs` are unaffected by this addition.
pub fn test_record_rid() -> RecordId {
	RecordId {
		table: TableName::from("compat_test"),
		key: RecordIdKey::String(Strand::new_static("fixture")),
	}
}

/// Record with object data
pub fn record_object() -> Record {
	let mut obj = Object::default();
	obj.insert("id".to_string(), Value::RecordId(test_record_rid()));
	obj.insert("name".to_string(), Value::String(Strand::new_static("Alice")));
	obj.insert("age".to_string(), Value::Number(Number::Int(30)));
	obj.insert("active".to_string(), Value::Bool(true));
	Record::new(Value::Object(obj))
}

/// Record with set data
pub fn record_set() -> Record {
	Record::new(Value::Set(Set::from(vec![
		Value::String(Strand::new_static("tag1")),
		Value::String(Strand::new_static("tag2")),
		Value::String(Strand::new_static("tag3")),
	])))
}

/// Record with metadata (Edge type)
pub fn record_with_metadata() -> Record {
	let mut obj = Object::default();
	obj.insert("id".to_string(), Value::RecordId(test_record_rid()));
	let mut record = Record::new(Value::Object(obj));
	record.set_record_type(RecordType::Edge {
		variant: 1,
	});
	record
}

/// Record with explicit Table metadata type
pub fn record_with_table_metadata() -> Record {
	let mut obj = Object::default();
	obj.insert("id".to_string(), Value::RecordId(test_record_rid()));
	obj.insert("name".to_string(), Value::String(Strand::new_static("Test Record")));
	let mut record = Record::new(Value::Object(obj));
	record.set_record_type(RecordType::Table);
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
// StoredApiActionDefinition fixtures
// ===========================================================================

/// Minimal API action definition
pub fn api_action_basic() -> StoredApiActionDefinition {
	StoredApiActionDefinition {
		methods: vec![ApiMethod::Get],
		action: ExprText::from_raw("'SELECT * FROM users'"),
		config: StoredApiConfigDefinition::default(),
	}
}

/// API action with multiple methods
pub fn api_action_multi_method() -> StoredApiActionDefinition {
	StoredApiActionDefinition {
		methods: vec![ApiMethod::Get, ApiMethod::Post, ApiMethod::Put],
		action: ExprText::from_raw("'CREATE users CONTENT $body'"),
		config: StoredApiConfigDefinition::default(),
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
// StoredModuleDefinition fixtures
// ===========================================================================

/// Module with Surrealism executable
pub fn module_surrealism() -> StoredModuleDefinition {
	StoredModuleDefinition {
		name: Some("my_module".to_string()),
		comment: Some("Custom module".to_string()),
		permissions: StoredPermission::Full,
		executable: ModuleExecutable::Surrealism(SurrealismExecutable {
			bucket: "my_bucket".to_string(),
			key: "module_key".to_string(),
		}),
		// Definitions serialized before revision 2 predate signing, so they
		// decode as unsigned.
		unsigned: true,
	}
}

/// Module with Silo executable
pub fn module_silo() -> StoredModuleDefinition {
	StoredModuleDefinition {
		name: Some("silo_module".to_string()),
		comment: None,
		permissions: StoredPermission::Full,
		executable: ModuleExecutable::Silo(SiloExecutable {
			organisation: "org".to_string(),
			package: "pkg".to_string(),
			major: 1,
			minor: 2,
			patch: 3,
		}),
		unsigned: true,
	}
}

/// Module with no name and Permission::None
pub fn module_no_name() -> StoredModuleDefinition {
	StoredModuleDefinition {
		name: None,
		comment: None,
		permissions: StoredPermission::None,
		executable: ModuleExecutable::Surrealism(SurrealismExecutable {
			bucket: "default_bucket".to_string(),
			key: "anonymous_module".to_string(),
		}),
		unsigned: true,
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
	obj.insert("name".to_string(), Value::String(Strand::new_static("Alice")));
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

/// Table mutations with Def operation
pub fn table_mutations_def() -> TableMutations {
	let mut mutations = TableMutations::new(TableName::from("users"));
	mutations.1.push(TableMutation::Def(Box::new(table_basic())));
	mutations
}

/// Table mutations with SetWithDiff operation
pub fn table_mutations_set_with_diff() -> TableMutations {
	let mut mutations = TableMutations::new(TableName::from("users"));
	let mut obj = Object::default();
	obj.insert("name".to_string(), Value::String(Strand::new_static("Bob")));
	obj.insert("age".to_string(), Value::Number(Number::Int(30)));
	mutations.1.push(TableMutation::SetWithDiff(
		RecordId::new(TableName::from("users"), 1),
		Value::Object(obj),
		vec![Operation::Replace {
			path: vec!["name".into()],
			value: Value::String(Strand::new_static("Alice")),
		}],
	));
	mutations
}

/// Table mutations with DelWithOriginal operation
pub fn table_mutations_del_with_original() -> TableMutations {
	let mut mutations = TableMutations::new(TableName::from("users"));
	let mut obj = Object::default();
	obj.insert("name".to_string(), Value::String(Strand::new_static("Charlie")));
	mutations.1.push(TableMutation::DelWithOriginal(
		RecordId::new(TableName::from("users"), 2),
		Value::Object(obj),
	));
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
	TermDocument {
		f: 123,
		o: vec![Offset::new(1, 2, 3, 4)],
	}
}

// ===========================================================================
// DocLengthAndCount fixtures
// ===========================================================================

/// Document length and count - basic default
pub fn doc_length_and_count_basic() -> DocLengthAndCount {
	DocLengthAndCount {
		total_docs_length: 123,
		doc_count: 456,
	}
}

// ===========================================================================
// Appending fixtures
// ===========================================================================

pub fn appending_none() -> Appending {
	Appending {
		old_values: None,
		new_values: None,
		id: RecordIdKey::Number(123),
		count_cond_match: None,
	}
}

pub fn appending_old_values() -> Appending {
	Appending {
		old_values: Some(vec![Value::String(Strand::new_static("old value"))]),
		new_values: None,
		id: RecordIdKey::Number(123),
		count_cond_match: None,
	}
}

pub fn appending_new_values() -> Appending {
	Appending {
		old_values: None,
		new_values: Some(vec![Value::String(Strand::new_static("new value"))]),
		id: RecordIdKey::Number(123),
		count_cond_match: None,
	}
}

pub fn appending_both() -> Appending {
	Appending {
		old_values: Some(vec![Value::String(Strand::new_static("old value"))]),
		new_values: Some(vec![Value::String(Strand::new_static("new value"))]),
		id: RecordIdKey::Number(123),
		count_cond_match: None,
	}
}

// ===========================================================================
// PrimaryAppending fixtures
// ===========================================================================

pub fn primary_appending_basic() -> PrimaryAppending {
	PrimaryAppending(123, 0)
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
	RecordIdKey::String(Strand::new_static("test_key"))
}

/// RecordIdKey with UUID
pub fn recordid_key_uuid() -> RecordIdKey {
	RecordIdKey::Uuid(Uuid(UuidExt::nil()))
}

/// RecordIdKey with array
pub fn recordid_key_array() -> RecordIdKey {
	RecordIdKey::Array(Array::from(vec![
		Value::Number(Number::Int(1)),
		Value::String(Strand::new_static("a")),
	]))
}

/// RecordIdKey with object
pub fn recordid_key_object() -> RecordIdKey {
	let mut obj = Object::default();
	obj.insert("id".to_string(), Value::Number(Number::Int(123)));
	RecordIdKey::Object(obj)
}

/// RecordIdKey with range
pub fn recordid_key_range() -> RecordIdKey {
	RecordIdKey::Range(Box::new(RecordIdKeyRange {
		start: Bound::Included(RecordIdKey::Number(1)),
		end: Bound::Excluded(RecordIdKey::Number(100)),
	}))
}
