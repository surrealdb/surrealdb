use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;

use rstest::rstest;
use uuid::Uuid;

use super::*;
use crate::catalog::record::{Data, Record};
use crate::catalog::schema::base::Base;
use crate::expr::field::Selector;
use crate::expr::{
	Block, ChangeFeed, Expr, Fetch, Fetchs, Field, Fields, Filter, Groups, Idiom, Kind, Literal,
	Tokenizer,
};
use crate::iam::Auth;
use crate::kvs::KVValue;
use crate::kvs::version::MajorVersion;
use crate::val::{Datetime, Value};

/// This test is used to ensure that
#[rstest]
#[case::namespace(NamespaceDefinition {
	namespace_id: NamespaceId(123),
	name: "test".to_string(),
	comment: Some("comment".to_string()),
}, 16)]
#[case::database(DatabaseDefinition {
	namespace_id: NamespaceId(123),
	database_id: DatabaseId(456),
	name: "test".to_string(),
	strict: false,
	comment: Some("comment".to_string()),
	changefeed: Some(ChangeFeed {
		expiry: Duration::from_secs(123),
		store_diff: false,
	}),
}, 25)]
#[case::table(TableDefinition {
	namespace_id: NamespaceId(123),
	database_id: DatabaseId(456),
	table_id: TableId(789),
	name: "test".to_string(),
	drop: false,
	schemafull: false,
	view: Some(ViewDefinition::Select {
		fields: Fields::Select(vec![Field::All, Field::Single (crate::expr::field::Selector{
			expr: Expr::Literal(Literal::String("expr".to_string())),
			alias: Some(Idiom::from_str("field[0]").unwrap()),
		})]),
		tables: vec!["what".to_string()],
		condition: Some(Expr::Literal(Literal::String("cond".to_string()))),
		groups: Some(Groups::default()),
	}),
	permissions: Permissions::default(),
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
}, 149)]
#[case::subscription(SubscriptionDefinition {
	id: Uuid::default(),
	node: Uuid::default(),
	fields: Fields::Select(vec![Field::All, Field::Single(Selector{
		expr: Expr::Literal(Literal::String("expr".to_string())),
		alias: Some(Idiom::from_str("field[0]").unwrap()),
	})]),
	diff: false,
	what: Expr::Literal(Literal::String("what".to_string())),
	cond: Some(Expr::Literal(Literal::String("cond".to_string()))),
	fetch: Some(Fetchs(vec![Fetch(Expr::Literal(Literal::String("fetch".to_string())))])),
	auth: Some(Auth::default()),
	session: Some(Value::default()),
	vars: BTreeMap::new(),
}, 100)]
#[case::access(AccessDefinition {
	name: "access".to_string(),
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
	authenticate: Some(Expr::Literal(Literal::String("expr".to_string()))),
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
	name: "analyzer".to_string(),
	function: Some("function".to_string()),
	tokenizers: Some(vec![Tokenizer::Camel]),
	filters: Some(vec![Filter::Ascii]),
	comment: Some("comment".to_string()),
}, 37)]
#[case::api(ApiDefinition {
	path: "/test".parse().unwrap(),
	actions: vec![
		ApiActionDefinition {
			methods: vec![ApiMethod::Get],
			action: Expr::Literal(Literal::String("action".to_string())),
			config: ApiConfigDefinition::default(),
		},
	],
	fallback: None,
	config: ApiConfigDefinition {
		middleware: vec![
			MiddlewareDefinition {
				name: "middleware".to_string(),
				args: vec![],
			},
		],
		permissions: Permission::Full,
	},
	comment: None,
}, 44)]
#[case::bucket(BucketDefinition {
	id: Some(BucketId(123)),
	readonly: false,
	name: "bucket".to_string(),
	backend: Some("backend".to_string()),
	comment: Some("comment".to_string()),
	permissions: Permission::Full,
}, 32)]
#[case::config(ConfigDefinition::GraphQL(GraphQLConfig {
	tables: GraphQLTablesConfig::default(),
	functions: GraphQLFunctionsConfig::default(),
}), 7)]
#[case::event(EventDefinition {
	name: "test".to_string(),
	target_table: "test".to_string(),
	when: Expr::Literal(Literal::String("when".to_string())),
	then: vec![Expr::Literal(Literal::String("then".to_string()))],
	comment: Some("comment".to_string()),
}, 35)]
#[case::field(FieldDefinition {
	name: Idiom::from_str("field[0]").unwrap(),
	what: "what".to_string(),
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
	comment: Some("comment".to_string()),
	reference: None,
}, 39)]
#[case::function(FunctionDefinition {
	name: "function".to_string(),
	args: vec![],
	block: Block(vec![
		Expr::Literal(Literal::String("expr".to_string())),
	]),
	comment: Some("comment".to_string()),
	permissions: Permission::Full,
	returns: Some(Kind::Any),
}, 36)]
#[case::index(IndexDefinition {
	index_id: IndexId(123),
	name: "test".to_string(),
	table_id: TableId(456),
	table_name: "what".to_string(),
	cols: vec![Idiom::from_str("field[0]").unwrap()],
	index: Index::Idx,
	comment: Some("comment".to_string()),
}, 33)]
#[case::model(MlModelDefinition {
	name: "model".to_string(),
	hash: "hash".to_string(),
	version: "1.0.0".to_string(),
	comment: Some("comment".to_string()),
	permissions: Permission::Full,
}, 29)]
#[case::param(ParamDefinition {
	name: "param".to_string(),
	value: Value::Bool(true),
	comment: Some("comment".to_string()),
	permissions: Permission::Full,
}, 21)]
#[case::sequence(SequenceDefinition {
	name: "sequence".to_string(),
	batch: 123,
	start: 123,
	timeout: Some(Duration::from_secs(123)),
}, 15)]
#[case::version(MajorVersion::from(1), 2)]
#[case::user(UserDefinition {
	name: "tobie".to_string(),
	hash: "hash".to_string(),
	code: "code".to_string(),
	roles: vec!["role".to_string()],
	token_duration: Some(Duration::from_secs(123)),
	session_duration: Some(Duration::from_secs(123)),
	comment: Some("comment".to_string()),
	base: crate::catalog::schema::base::Base::Root,
}, 40)]
#[case::record(Record::new(Data::from(Value::Bool(true))), 5)]
fn test_serialize_deserialize<T>(#[case] original: T, #[case] expected_encoded_size: usize)
where
	T: KVValue + std::fmt::Debug + PartialEq,
{
	let encoded = original.kv_encode_value().unwrap();
	assert_eq!(encoded.len(), expected_encoded_size);

	let decoded = T::kv_decode_value(encoded).unwrap();
	assert_eq!(decoded, original);
}
