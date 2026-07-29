use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;

use rstest::rstest;
use surrealdb_strand::Strand;
use uuid::Uuid;

use super::*;
use crate::catalog::auth::AuthLimit;
use crate::catalog::schema::base::Base;
use crate::expr::field::Selector;
use crate::expr::{
	ChangeFeed, Expr, Field, Fields, Filter, Groups, Idiom, Kind, Literal, Tokenizer,
};
use crate::iam::Auth;
use crate::key::KVValue;
use crate::kvs::version::MajorVersion;
use crate::val::{Datetime, TableName, Value};

/// This test is used to ensure that
#[rstest]
#[case::namespace(NamespaceDefinition {
	namespace_id: NamespaceId(123),
	name: "test".into(),
	comment: Some("comment".to_string()),
}, 16)]
#[case::database(DatabaseDefinition {
	namespace_id: NamespaceId(123),
	database_id: DatabaseId(456),
	name: "test".into(),
	strict: false,
	comment: Some("comment".to_string()),
	changefeed: Some(ChangeFeed {
		expiry: Duration::from_secs(123),
		store_diff: false,
	}),
}, 25)]
#[case::table(StoredTableDefinition {
	namespace_id: NamespaceId(123),
	database_id: DatabaseId(456),
	table_id: TableId(789),
	name: "test".into(),
	drop: false,
	schemafull: false,
	view: Some(ViewDefinition::Select {
			fields: Fields::Select(vec![Field::All, Field::Single (crate::expr::field::Selector{
				expr: Expr::Literal(Literal::String(Strand::new_static("expr"))),
				alias: Some(Idiom::from_str("field[0]").unwrap()),
			})]),
			tables: vec![TableName::from("what")],
			condition: Some(Expr::Literal(Literal::String(Strand::new_static("cond")))),
			groups: Some(Groups::default()),
		}.to_stored()),
	permissions: StoredPermissions::default(),
	changefeed: Some(ChangeFeed {
		expiry: Duration::from_secs(123),
		store_diff: false,
	}),
	comment: Some("comment".to_string()),
	table_type: TableType::Normal,
	cache_fields_ts: Uuid::default(),
	cache_events_ts: Uuid::default(),
	cache_tables_ts: Uuid::default(),
	cache_indexes_ts: Uuid::default(),
	cache_lives_ts: Uuid::default(),
	graphql_alias: None,
	graphql_deprecated: None,
}, 163)]
#[case::subscription(StoredSubscriptionDefinition {
	id: Uuid::default(),
	node: Uuid::default(),
	fields: StoredSubscriptionFields::Select(FieldsText::new(&Fields::Select(vec![Field::All, Field::Single(Selector{
		expr: Expr::Literal(Literal::String(Strand::new_static("expr"))),
		alias: Some(Idiom::from_str("field[0]").unwrap()),
	})]))),
	what: ExprText::new(&Expr::Literal(Literal::String(Strand::new_static("what")))),
	cond: Some(ExprText::new(&Expr::Literal(Literal::String(Strand::new_static("cond"))))),
	fetch: Some(vec![ExprText::new(&Expr::Literal(Literal::String(Strand::new_static("fetch"))))]),
	auth: Some(Auth::default()),
	session: Some(Value::default()),
	vars: BTreeMap::new(),
}, 96)]
#[case::access(StoredAccessDefinition {
	name: "access".into(),
	access_type: AccessType::Bearer(BearerAccess {
		kind: BearerAccessType::Bearer,
		subject: BearerAccessSubject::Record,
		jwt: JwtAccess {
			verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
				alg: Algorithm::EdDSA,
				key: "key".to_string(),
			}),
			issue: Some(JwtAccessIssue {
				alg: Algorithm::Hs512,
				key: "key".to_string(),
			}),
		},
	}),
	base: Base::Root,
	authenticate: Some(ExprText::from_raw("'expr'")),
	grant_duration: Some(Duration::from_secs(123)),
	token_duration: Some(Duration::from_secs(123)),
	session_duration: Some(Duration::from_secs(123)),
	comment: Some("comment".to_string()),
}, 61)]
#[case::access(AccessGrant {
	id: "access".to_string(),
	ac: "access".to_string(),
	creation: Datetime::MAX_UTC,
	expiration: Some(Datetime::MAX_UTC),
	revocation: Some(Datetime::MAX_UTC),
	subject: Subject::User("user".to_string()),
	grant: Grant::Jwt(GrantJwt {
		jti: Uuid::default(),
		token: Some("token".to_string()),
	}),
}, 95)]
#[case::analyzer(AnalyzerDefinition {
	name: "analyzer".into(),
	function: Some("function".into()),
	tokenizers: Some(vec![Tokenizer::Camel]),
	filters: Some(vec![Filter::Ascii]),
	comment: Some("comment".to_string()),
}, 37)]
#[case::api(StoredApiDefinition {
	path: PathText::from_raw("/test"),
	actions: vec![
		StoredApiActionDefinition {
			methods: vec![ApiMethod::Get],
			action: ExprText::from_raw("'action'"),
			config: StoredApiConfigDefinition::default(),
		},
	],
	fallback: None,
	config: StoredApiConfigDefinition {
		middleware: vec![
			MiddlewareDefinition {
				name: "middleware".into(),
				args: vec![],
			},
		],
		permissions: StoredPermission::Full,
	},
	comment: None,
	auth_limit: AuthLimit::default(),
}, 48)]
#[case::bucket(StoredBucketDefinition {
	id: Some(BucketId(123)),
	readonly: false,
	name: "bucket".into(),
	backend: Some("backend".into()),
	comment: Some("comment".to_string()),
	permissions: StoredPermission::Full,
}, 32)]
#[case::config(StoredConfigDefinition::GraphQL(GraphQLConfig {
	tables: GraphQLTablesConfig::default(),
	functions: GraphQLFunctionsConfig::default(),
	depth_limit: None,
	complexity_limit: None,
	introspection: GraphQLIntrospectionConfig::default(),
}), 11)]
#[case::event(StoredEventDefinition {
	name: "test".into(),
	target_table: "test".into(),
	when: ExprText::from_raw("'when'"),
	then: vec![ExprText::from_raw("'then'")],
	comment: Some("comment".to_string()),
	auth_limit: AuthLimit::default(),
    kind: EventKind::Async {
        retry: 1,
        max_depth: 5,
    },
}, 43)]
#[case::field(StoredFieldDefinition {
	name: IdiomText::from_raw("field[0]"),
	table: TableName::from("what"),
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
	comment: Some("comment".to_string()),
	reference: None,
	auth_limit: AuthLimit::default(),
	graphql_alias: None,
	graphql_deprecated: None,
}, 45)]
#[case::function(StoredFunctionDefinition {
	name: "function".into(),
	args: vec![],
	block: BlockText::from_raw("{ 'expr' }"),
	comment: Some("comment".to_string()),
	permissions: StoredPermission::Full,
	returns: Some(KindText::new(&Kind::Any)),
	auth_limit: AuthLimit::default(),
	graphql_alias: None,
	graphql_deprecated: None,
}, 44)]
#[case::index(StoredIndexDefinition {
	index_id: IndexId(123),
	name: "test".into(),
	table_name: "what".into(),
	cols: vec![IdiomText::from_raw("field[0]")],
	index: Index::Idx,
	comment: Some("comment".to_string()),
	prepare_remove: false,
	format_version: 1,
}, 35)]
#[case::model(StoredMlModelDefinition {
	name: "model".into(),
	hash: "hash".into(),
	version: "1.0.0".into(),
	comment: Some("comment".to_string()),
	permissions: StoredPermission::Full,
}, 29)]
#[case::param(StoredParamDefinition {
	name: "param".into(),
	value: Value::Bool(true),
	comment: Some("comment".to_string()),
	permissions: StoredPermission::Full,
}, 21)]
#[case::sequence(SequenceDefinition {
	name: "sequence".into(),
	batch: 123,
	start: 123,
	timeout: Some(Duration::from_secs(123)),
}, 15)]
#[case::version(MajorVersion::from(1), 2)]
#[case::user(UserDefinition {
	name: "tobie".into(),
	hash: "hash".into(),
	code: "code".to_string(),
	roles: vec!["role".to_string()],
	token_duration: Some(Duration::from_secs(123)),
	session_duration: Some(Duration::from_secs(123)),
	comment: Some("comment".to_string()),
	base: crate::catalog::schema::base::Base::Root,
	scram: None,
}, 41)]
fn test_serialize_deserialize<T>(#[case] original: T, #[case] expected_encoded_size: usize)
where
	T: KVValue<KeyContext = ()> + std::fmt::Debug + PartialEq,
{
	let encoded = original.kv_encode_value().unwrap();
	assert_eq!(encoded.len(), expected_encoded_size);

	let decoded = T::kv_decode_value(&encoded, ()).unwrap();
	assert_eq!(decoded, original);
}
