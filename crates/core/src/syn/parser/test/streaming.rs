use crate::{
	sql::{
		access::AccessDuration,
		access_type::{AccessType, JwtAccess, JwtAccessVerify, JwtAccessVerifyKey, RecordAccess},
		block::Entry,
		changefeed::ChangeFeed,
		filter::Filter,
		index::{Distance, MTreeParams, SearchParams, VectorType},
		language::Language,
		order::{OrderList, Ordering},
		statements::{
			analyze::AnalyzeStatement,
			show::{ShowSince, ShowStatement},
			sleep::SleepStatement,
			BeginStatement, BreakStatement, CancelStatement, CommitStatement, ContinueStatement,
			CreateStatement, DefineAccessStatement, DefineAnalyzerStatement,
			DefineDatabaseStatement, DefineEventStatement, DefineFieldStatement,
			DefineFunctionStatement, DefineIndexStatement, DefineNamespaceStatement,
			DefineParamStatement, DefineStatement, DefineTableStatement, DeleteStatement,
			ForeachStatement, IfelseStatement, InfoStatement, InsertStatement, KillStatement,
			OutputStatement, RelateStatement, RemoveFieldStatement, RemoveFunctionStatement,
			RemoveStatement, SelectStatement, SetStatement, ThrowStatement, UpdateStatement,
			UpsertStatement,
		},
		tokenizer::Tokenizer,
		Algorithm, Array, Base, Block, Cond, Data, Datetime, Dir, Duration, Edges, Explain,
		Expression, Fetch, Fetchs, Field, Fields, Future, Graph, Group, Groups, Id, Ident, Idiom,
		Idioms, Index, Kind, Limit, Number, Object, Operator, Order, Output, Param, Part,
		Permission, Permissions, Scoring, Split, Splits, Start, Statement, Strand, Subquery, Table,
		TableType, Tables, Thing, Timeout, Uuid, Value, Values, Version, With,
	},
	syn::parser::{Parser, PartialResult},
};
use chrono::{offset::TimeZone, NaiveDate, Offset, Utc};
use reblessive::Stack;

fn ident_field(name: &str) -> Value {
	Value::Idiom(Idiom(vec![Part::Field(Ident(name.to_string()))]))
}

static SOURCE: &str = r#"
	ANALYZE INDEX b on a;
	BEGIN;
	BEGIN TRANSACTION;
	BREAK;
	CANCEL;
	CANCEL TRANSACTION;
	COMMIT;
	COMMIT TRANSACTION;
	CONTINUE;
	CREATE ONLY foo SET bar = 3, foo +?= 4 RETURN VALUE foo AS bar TIMEOUT 1s PARALLEL;
	DEFINE NAMESPACE a COMMENT 'test';
	DEFINE NS a;
	DEFINE DATABASE a COMMENT 'test' CHANGEFEED 10m;
	DEFINE DB a;
	DEFINE FUNCTION fn::foo::bar($a: number, $b: array<bool,3>) {
		RETURN a
	} COMMENT 'test' PERMISSIONS FULL;
	DEFINE ACCESS a ON DATABASE TYPE RECORD WITH JWT ALGORITHM EDDSA KEY "foo" COMMENT "bar";
	DEFINE PARAM $a VALUE { a: 1, "b": 3 } PERMISSIONS WHERE null;
	DEFINE TABLE name DROP SCHEMAFUL CHANGEFEED 1s PERMISSIONS FOR SELECT WHERE a = 1 AS SELECT foo FROM bar GROUP BY foo;
	DEFINE EVENT event ON TABLE table WHEN null THEN null,none;
	DEFINE FIELD foo.*[*]... ON TABLE bar FLEX TYPE option<number | array<record<foo>,10>> VALUE null ASSERT true DEFAULT false PERMISSIONS FOR UPDATE NONE, FOR CREATE WHERE true;
	DEFINE INDEX index ON TABLE table FIELDS a,b[*] SEARCH ANALYZER ana BM25 (0.1,0.2)
			DOC_IDS_ORDER 1
			DOC_LENGTHS_ORDER 2
			POSTINGS_ORDER 3
			TERMS_ORDER 4
			DOC_IDS_CACHE 5
			DOC_LENGTHS_CACHE 6
			POSTINGS_CACHE 7
			TERMS_CACHE 8
			HIGHLIGHTS;
	DEFINE INDEX index ON TABLE table FIELDS a UNIQUE;
	DEFINE INDEX index ON TABLE table FIELDS a MTREE DIMENSION 4 DISTANCE MINKOWSKI 5 CAPACITY 6 DOC_IDS_ORDER 7 DOC_IDS_CACHE 8 MTREE_CACHE 9;
	DEFINE ANALYZER ana FILTERS ASCII, EDGENGRAM(1,2), NGRAM(3,4), LOWERCASE, SNOWBALL(NLD), UPPERCASE TOKENIZERS BLANK, CAMEL, CLASS, PUNCT FUNCTION fn::foo::bar;
	DELETE FROM ONLY |foo:32..64| Where 2 RETURN AFTER TIMEOUT 1s PARALLEL;
	DELETE FROM ONLY a:b->?[$][?true] WHERE null RETURN NULL TIMEOUT 1h PARALLEL;
	FOR $foo IN (SELECT foo FROM bar) * 2 {
		BREAK
	};
	IF foo THEN bar ELSE IF faz THEN baz ELSE baq END;
	IF foo { bar } ELSE IF faz { baz } ELSE { baq };
	INFO FOR ROOT;
	INFO FOR NAMESPACE;
	INFO FOR USER user ON namespace;
	SELECT bar as foo,[1,2],bar OMIT bar FROM ONLY a,1
		WITH INDEX index,index_2
		WHERE true
		SPLIT ON foo,bar
		GROUP foo,bar
		ORDER BY foo COLLATE NUMERIC ASC
		START AT { a: true }
		LIMIT BY a:b
		FETCH foo
		VERSION d"2012-04-23T18:25:43.0000511Z"
		EXPLAIN FULL;
	LET $param = 1;
	SHOW CHANGES FOR TABLE foo SINCE 1 LIMIT 10;
	SHOW CHANGES FOR DATABASE SINCE d"2012-04-23T18:25:43.0000511Z";
	SLEEP 1s;
	THROW 1s;
	INSERT IGNORE INTO $foo (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE a.b +?= null, c.d += none RETURN AFTER;
	KILL u"e72bee20-f49b-11ec-b939-0242ac120002";
	RETURN RETRUN FETCH RETURN;
	RELATE ONLY [1,2]->a:b->(CREATE foo) UNIQUE SET a += 1 RETURN NONE PARALLEL;
	REMOVE FUNCTION fn::foo::bar();
	REMOVE FIELD foo.bar[10] ON bar;
	UPDATE ONLY <future> { "text" }, a->b UNSET foo... , a->b, c[*] WHERE true RETURN DIFF TIMEOUT 1s PARALLEL;
	UPSERT ONLY <future> { "text" }, a->b UNSET foo... , a->b, c[*] WHERE true RETURN DIFF TIMEOUT 1s PARALLEL;
"#;

fn statements() -> Vec<Statement> {
	let offset = Utc.fix();
	let expected_datetime = offset
		.from_local_datetime(
			&NaiveDate::from_ymd_opt(2012, 4, 23)
				.unwrap()
				.and_hms_nano_opt(18, 25, 43, 51_100)
				.unwrap(),
		)
		.earliest()
		.unwrap()
		.with_timezone(&Utc);

	vec![
		Statement::Analyze(AnalyzeStatement::Idx(Ident("a".to_string()), Ident("b".to_string()))),
		Statement::Begin(BeginStatement),
		Statement::Begin(BeginStatement),
		Statement::Break(BreakStatement),
		Statement::Cancel(CancelStatement),
		Statement::Cancel(CancelStatement),
		Statement::Commit(CommitStatement),
		Statement::Commit(CommitStatement),
		Statement::Continue(ContinueStatement),
		Statement::Create(CreateStatement {
			only: true,
			what: Values(vec![Value::Table(Table("foo".to_owned()))]),
			data: Some(Data::SetExpression(vec![
				(
					Idiom(vec![Part::Field(Ident("bar".to_owned()))]),
					Operator::Equal,
					Value::Number(Number::Int(3)),
				),
				(
					Idiom(vec![Part::Field(Ident("foo".to_owned()))]),
					Operator::Ext,
					Value::Number(Number::Int(4)),
				),
			])),
			output: Some(Output::Fields(Fields(
				vec![Field::Single {
					expr: Value::Idiom(Idiom(vec![Part::Field(Ident("foo".to_owned()))])),
					alias: Some(Idiom(vec![Part::Field(Ident("bar".to_owned()))])),
				}],
				true,
			))),
			timeout: Some(Timeout(Duration(std::time::Duration::from_secs(1)))),
			parallel: true,
			version: None,
		}),
		Statement::Define(DefineStatement::Namespace(DefineNamespaceStatement {
			id: None,
			name: Ident("a".to_string()),
			comment: Some(Strand("test".to_string())),
			if_not_exists: false,
			overwrite: false,
		})),
		Statement::Define(DefineStatement::Namespace(DefineNamespaceStatement {
			id: None,
			name: Ident("a".to_string()),
			comment: None,
			if_not_exists: false,
			overwrite: false,
		})),
		Statement::Define(DefineStatement::Database(DefineDatabaseStatement {
			id: None,
			name: Ident("a".to_string()),
			comment: Some(Strand("test".to_string())),
			changefeed: Some(ChangeFeed {
				expiry: std::time::Duration::from_secs(60) * 10,
				store_diff: false,
			}),
			if_not_exists: false,
			overwrite: false,
		})),
		Statement::Define(DefineStatement::Database(DefineDatabaseStatement {
			id: None,
			name: Ident("a".to_string()),
			comment: None,
			changefeed: None,
			if_not_exists: false,
			overwrite: false,
		})),
		Statement::Define(DefineStatement::Function(DefineFunctionStatement {
			name: Ident("foo::bar".to_string()),
			args: vec![
				(Ident("a".to_string()), Kind::Number),
				(Ident("b".to_string()), Kind::Array(Box::new(Kind::Bool), Some(3))),
			],
			block: Block(vec![Entry::Output(OutputStatement {
				what: ident_field("a"),
				fetch: None,
			})]),
			comment: Some(Strand("test".to_string())),
			permissions: Permission::Full,
			if_not_exists: false,
			overwrite: false,
			returns: None,
		})),
		Statement::Define(DefineStatement::Access(DefineAccessStatement {
			name: Ident("a".to_string()),
			base: Base::Db,
			kind: AccessType::Record(RecordAccess {
				signup: None,
				signin: None,
				jwt: JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::EdDSA,
						key: "foo".to_string(),
					}),
					issue: None,
				},
				bearer: None,
			}),
			authenticate: None,
			// Default durations.
			duration: AccessDuration {
				grant: Some(Duration::from_days(30)),
				token: Some(Duration::from_hours(1)),
				session: None,
			},
			comment: Some(Strand("bar".to_string())),
			if_not_exists: false,
			overwrite: false,
		})),
		Statement::Define(DefineStatement::Param(DefineParamStatement {
			name: Ident("a".to_string()),
			value: Value::Object(Object(
				[
					("a".to_string(), Value::Number(Number::Int(1))),
					("b".to_string(), Value::Number(Number::Int(3))),
				]
				.into_iter()
				.collect(),
			)),
			comment: None,
			permissions: Permission::Specific(Value::Null),
			if_not_exists: false,
			overwrite: false,
		})),
		Statement::Define(DefineStatement::Table(DefineTableStatement {
			id: None,
			name: Ident("name".to_string()),
			drop: true,
			full: true,
			view: Some(crate::sql::View {
				expr: Fields(
					vec![Field::Single {
						expr: Value::Idiom(Idiom(vec![Part::Field(Ident("foo".to_owned()))])),
						alias: None,
					}],
					false,
				),
				what: Tables(vec![Table("bar".to_owned())]),
				cond: None,
				group: Some(Groups(vec![Group(Idiom(vec![Part::Field(Ident("foo".to_owned()))]))])),
			}),
			permissions: Permissions {
				select: Permission::Specific(Value::Expression(Box::new(
					crate::sql::Expression::Binary {
						l: Value::Idiom(Idiom(vec![Part::Field(Ident("a".to_owned()))])),
						o: Operator::Equal,
						r: Value::Number(Number::Int(1)),
					},
				))),
				create: Permission::None,
				update: Permission::None,
				delete: Permission::None,
			},
			changefeed: Some(ChangeFeed {
				expiry: std::time::Duration::from_secs(1),
				store_diff: false,
			}),
			comment: None,
			if_not_exists: false,
			overwrite: false,
			kind: TableType::Normal,
			cache_fields_ts: uuid::Uuid::default(),
			cache_events_ts: uuid::Uuid::default(),
			cache_tables_ts: uuid::Uuid::default(),
			cache_indexes_ts: uuid::Uuid::default(),
			cache_lives_ts: uuid::Uuid::default(),
		})),
		Statement::Define(DefineStatement::Event(DefineEventStatement {
			name: Ident("event".to_owned()),
			what: Ident("table".to_owned()),
			when: Value::Null,
			then: Values(vec![Value::Null, Value::None]),
			comment: None,
			if_not_exists: false,
			overwrite: false,
		})),
		Statement::Define(DefineStatement::Field(DefineFieldStatement {
			name: Idiom(vec![
				Part::Field(Ident("foo".to_owned())),
				Part::All,
				Part::All,
				Part::Flatten,
			]),
			what: Ident("bar".to_owned()),
			flex: true,
			kind: Some(Kind::Option(Box::new(Kind::Either(vec![
				Kind::Number,
				Kind::Array(Box::new(Kind::Record(vec![Table("foo".to_owned())])), Some(10)),
			])))),
			readonly: false,
			value: Some(Value::Null),
			assert: Some(Value::Bool(true)),
			default: Some(Value::Bool(false)),
			permissions: Permissions {
				delete: Permission::Full,
				update: Permission::None,
				create: Permission::Specific(Value::Bool(true)),
				select: Permission::Full,
			},
			comment: None,
			if_not_exists: false,
			overwrite: false,
		})),
		Statement::Define(DefineStatement::Index(DefineIndexStatement {
			name: Ident("index".to_owned()),
			what: Ident("table".to_owned()),
			cols: Idioms(vec![
				Idiom(vec![Part::Field(Ident("a".to_owned()))]),
				Idiom(vec![Part::Field(Ident("b".to_owned())), Part::All]),
			]),
			index: Index::Search(SearchParams {
				az: Ident("ana".to_owned()),
				hl: true,
				sc: Scoring::Bm {
					k1: 0.1,
					b: 0.2,
				},
				doc_ids_order: 1,
				doc_lengths_order: 2,
				postings_order: 3,
				terms_order: 4,
				doc_ids_cache: 5,
				doc_lengths_cache: 6,
				postings_cache: 7,
				terms_cache: 8,
			}),
			comment: None,
			if_not_exists: false,
			overwrite: false,
			concurrently: false,
		})),
		Statement::Define(DefineStatement::Index(DefineIndexStatement {
			name: Ident("index".to_owned()),
			what: Ident("table".to_owned()),
			cols: Idioms(vec![Idiom(vec![Part::Field(Ident("a".to_owned()))])]),
			index: Index::Uniq,
			comment: None,
			if_not_exists: false,
			overwrite: false,
			concurrently: false,
		})),
		Statement::Define(DefineStatement::Index(DefineIndexStatement {
			name: Ident("index".to_owned()),
			what: Ident("table".to_owned()),
			cols: Idioms(vec![Idiom(vec![Part::Field(Ident("a".to_owned()))])]),
			index: Index::MTree(MTreeParams {
				dimension: 4,
				distance: Distance::Minkowski(Number::Int(5)),
				capacity: 6,
				doc_ids_order: 7,
				doc_ids_cache: 8,
				mtree_cache: 9,
				vector_type: VectorType::F64,
			}),
			comment: None,
			if_not_exists: false,
			overwrite: false,
			concurrently: false,
		})),
		Statement::Define(DefineStatement::Analyzer(DefineAnalyzerStatement {
			name: Ident("ana".to_owned()),
			tokenizers: Some(vec![
				Tokenizer::Blank,
				Tokenizer::Camel,
				Tokenizer::Class,
				Tokenizer::Punct,
			]),
			filters: Some(vec![
				Filter::Ascii,
				Filter::EdgeNgram(1, 2),
				Filter::Ngram(3, 4),
				Filter::Lowercase,
				Filter::Snowball(Language::Dutch),
				Filter::Uppercase,
			]),
			function: Some(Ident("foo::bar".to_string())),
			comment: None,
			if_not_exists: false,
			overwrite: false,
		})),
		Statement::Delete(DeleteStatement {
			only: true,
			what: Values(vec![Value::Mock(crate::sql::Mock::Range("foo".to_string(), 32, 64))]),
			cond: Some(Cond(Value::Number(Number::Int(2)))),
			output: Some(Output::After),
			timeout: Some(Timeout(Duration(std::time::Duration::from_secs(1)))),
			parallel: true,
		}),
		Statement::Delete(DeleteStatement {
			only: true,
			what: Values(vec![Value::Idiom(Idiom(vec![
				Part::Start(Value::Edges(Box::new(Edges {
					dir: Dir::Out,
					from: Thing {
						tb: "a".to_owned(),
						id: Id::from("b"),
					},
					what: Tables::default(),
				}))),
				Part::Last,
				Part::Where(Value::Bool(true)),
			]))]),
			cond: Some(Cond(Value::Null)),
			output: Some(Output::Null),
			timeout: Some(Timeout(Duration(std::time::Duration::from_secs(60 * 60)))),
			parallel: true,
		}),
		Statement::Foreach(ForeachStatement {
			param: Param(Ident("foo".to_owned())),
			range: Value::Expression(Box::new(Expression::Binary {
				l: Value::Subquery(Box::new(Subquery::Select(SelectStatement {
					expr: Fields(
						vec![Field::Single {
							expr: Value::Idiom(Idiom(vec![Part::Field(Ident("foo".to_owned()))])),
							alias: None,
						}],
						false,
					),
					what: Values(vec![Value::Table(Table("bar".to_owned()))]),
					..Default::default()
				}))),
				o: Operator::Mul,
				r: Value::Number(Number::Int(2)),
			})),
			block: Block(vec![Entry::Break(BreakStatement)]),
		}),
		Statement::Ifelse(IfelseStatement {
			exprs: vec![
				(ident_field("foo"), ident_field("bar")),
				(ident_field("faz"), ident_field("baz")),
			],
			close: Some(ident_field("baq")),
		}),
		Statement::Ifelse(IfelseStatement {
			exprs: vec![
				(
					ident_field("foo"),
					Value::Block(Box::new(Block(vec![Entry::Value(ident_field("bar"))]))),
				),
				(
					ident_field("faz"),
					Value::Block(Box::new(Block(vec![Entry::Value(ident_field("baz"))]))),
				),
			],
			close: Some(Value::Block(Box::new(Block(vec![Entry::Value(ident_field("baq"))])))),
		}),
		Statement::Info(InfoStatement::Root(false)),
		Statement::Info(InfoStatement::Ns(false)),
		Statement::Info(InfoStatement::User(Ident("user".to_owned()), Some(Base::Ns), false)),
		Statement::Select(SelectStatement {
			expr: Fields(
				vec![
					Field::Single {
						expr: Value::Idiom(Idiom(vec![Part::Field(Ident("bar".to_owned()))])),
						alias: Some(Idiom(vec![Part::Field(Ident("foo".to_owned()))])),
					},
					Field::Single {
						expr: Value::Array(Array(vec![
							Value::Number(Number::Int(1)),
							Value::Number(Number::Int(2)),
						])),
						alias: None,
					},
					Field::Single {
						expr: Value::Idiom(Idiom(vec![Part::Field(Ident("bar".to_owned()))])),
						alias: None,
					},
				],
				false,
			),
			omit: Some(Idioms(vec![Idiom(vec![Part::Field(Ident("bar".to_owned()))])])),
			only: true,
			what: Values(vec![Value::Table(Table("a".to_owned())), Value::Number(Number::Int(1))]),
			with: Some(With::Index(vec!["index".to_owned(), "index_2".to_owned()])),
			cond: Some(Cond(Value::Bool(true))),
			split: Some(Splits(vec![
				Split(Idiom(vec![Part::Field(Ident("foo".to_owned()))])),
				Split(Idiom(vec![Part::Field(Ident("bar".to_owned()))])),
			])),
			group: Some(Groups(vec![
				Group(Idiom(vec![Part::Field(Ident("foo".to_owned()))])),
				Group(Idiom(vec![Part::Field(Ident("bar".to_owned()))])),
			])),
			order: Some(Ordering::Order(OrderList(vec![Order {
				value: Idiom(vec![Part::Field(Ident("foo".to_owned()))]),
				collate: true,
				numeric: true,
				direction: true,
			}]))),
			limit: Some(Limit(Value::Thing(Thing {
				tb: "a".to_owned(),
				id: Id::from("b"),
			}))),
			start: Some(Start(Value::Object(Object(
				[("a".to_owned(), Value::Bool(true))].into_iter().collect(),
			)))),
			fetch: Some(Fetchs(vec![Fetch(Value::Idiom(Idiom(vec![Part::Field(Ident(
				"foo".to_owned(),
			))])))])),
			version: Some(Version(Value::Datetime(Datetime(expected_datetime)))),
			timeout: None,
			parallel: false,
			tempfiles: false,
			explain: Some(Explain(true)),
		}),
		Statement::Set(SetStatement {
			name: "param".to_owned(),
			what: Value::Number(Number::Int(1)),
			kind: None,
		}),
		Statement::Show(ShowStatement {
			table: Some(Table("foo".to_owned())),
			since: ShowSince::Versionstamp(1),
			limit: Some(10),
		}),
		Statement::Show(ShowStatement {
			table: None,
			since: ShowSince::Timestamp(Datetime(expected_datetime)),
			limit: None,
		}),
		Statement::Sleep(SleepStatement {
			duration: Duration(std::time::Duration::from_secs(1)),
		}),
		Statement::Throw(ThrowStatement {
			error: Value::Duration(Duration(std::time::Duration::from_secs(1))),
		}),
		Statement::Insert(InsertStatement {
			into: Some(Value::Param(Param(Ident("foo".to_owned())))),
			data: Data::ValuesExpression(vec![
				vec![
					(
						Idiom(vec![Part::Field(Ident("a".to_owned()))]),
						Value::Number(Number::Int(1)),
					),
					(
						Idiom(vec![Part::Field(Ident("b".to_owned()))]),
						Value::Number(Number::Int(2)),
					),
					(
						Idiom(vec![Part::Field(Ident("c".to_owned()))]),
						Value::Number(Number::Int(3)),
					),
				],
				vec![
					(
						Idiom(vec![Part::Field(Ident("a".to_owned()))]),
						Value::Number(Number::Int(4)),
					),
					(
						Idiom(vec![Part::Field(Ident("b".to_owned()))]),
						Value::Number(Number::Int(5)),
					),
					(
						Idiom(vec![Part::Field(Ident("c".to_owned()))]),
						Value::Number(Number::Int(6)),
					),
				],
			]),
			ignore: true,
			update: Some(Data::UpdateExpression(vec![
				(
					Idiom(vec![
						Part::Field(Ident("a".to_owned())),
						Part::Field(Ident("b".to_owned())),
					]),
					Operator::Ext,
					Value::Null,
				),
				(
					Idiom(vec![
						Part::Field(Ident("c".to_owned())),
						Part::Field(Ident("d".to_owned())),
					]),
					Operator::Inc,
					Value::None,
				),
			])),
			output: Some(Output::After),
			version: None,
			timeout: None,
			parallel: false,
			relation: false,
		}),
		Statement::Kill(KillStatement {
			id: Value::Uuid(Uuid(uuid::uuid!("e72bee20-f49b-11ec-b939-0242ac120002"))),
		}),
		Statement::Output(OutputStatement {
			what: ident_field("RETRUN"),
			fetch: Some(Fetchs(vec![Fetch(ident_field("RETURN"))])),
		}),
		Statement::Relate(RelateStatement {
			only: true,
			kind: Value::Thing(Thing {
				tb: "a".to_owned(),
				id: Id::from("b"),
			}),
			from: Value::Array(Array(vec![
				Value::Number(Number::Int(1)),
				Value::Number(Number::Int(2)),
			])),
			with: Value::Subquery(Box::new(Subquery::Create(CreateStatement {
				only: false,
				what: Values(vec![Value::Table(Table("foo".to_owned()))]),
				data: None,
				output: None,
				timeout: None,
				parallel: false,
				version: None,
			}))),
			uniq: true,
			data: Some(Data::SetExpression(vec![(
				Idiom(vec![Part::Field(Ident("a".to_owned()))]),
				Operator::Inc,
				Value::Number(Number::Int(1)),
			)])),
			output: Some(Output::None),
			timeout: None,
			parallel: true,
		}),
		Statement::Remove(RemoveStatement::Function(RemoveFunctionStatement {
			name: Ident("foo::bar".to_owned()),
			if_exists: false,
		})),
		Statement::Remove(RemoveStatement::Field(RemoveFieldStatement {
			name: Idiom(vec![
				Part::Field(Ident("foo".to_owned())),
				Part::Field(Ident("bar".to_owned())),
				Part::Index(Number::Int(10)),
			]),
			what: Ident("bar".to_owned()),
			if_exists: false,
		})),
		Statement::Update(UpdateStatement {
			only: true,
			what: Values(vec![
				Value::Future(Box::new(Future(Block(vec![Entry::Value(Value::Strand(Strand(
					"text".to_string(),
				)))])))),
				Value::Idiom(Idiom(vec![
					Part::Field(Ident("a".to_string())),
					Part::Graph(Graph {
						dir: Dir::Out,
						what: Tables(vec![Table("b".to_string())]),
						expr: Fields::all(),
						..Default::default()
					}),
				])),
			]),
			cond: Some(Cond(Value::Bool(true))),
			data: Some(Data::UnsetExpression(vec![
				Idiom(vec![Part::Field(Ident("foo".to_string())), Part::Flatten]),
				Idiom(vec![
					Part::Field(Ident("a".to_string())),
					Part::Graph(Graph {
						dir: Dir::Out,
						what: Tables(vec![Table("b".to_string())]),
						expr: Fields::all(),
						..Default::default()
					}),
				]),
				Idiom(vec![Part::Field(Ident("c".to_string())), Part::All]),
			])),
			output: Some(Output::Diff),
			timeout: Some(Timeout(Duration(std::time::Duration::from_secs(1)))),
			parallel: true,
		}),
		Statement::Upsert(UpsertStatement {
			only: true,
			what: Values(vec![
				Value::Future(Box::new(Future(Block(vec![Entry::Value(Value::Strand(Strand(
					"text".to_string(),
				)))])))),
				Value::Idiom(Idiom(vec![
					Part::Field(Ident("a".to_string())),
					Part::Graph(Graph {
						dir: Dir::Out,
						what: Tables(vec![Table("b".to_string())]),
						expr: Fields::all(),
						..Default::default()
					}),
				])),
			]),
			cond: Some(Cond(Value::Bool(true))),
			data: Some(Data::UnsetExpression(vec![
				Idiom(vec![Part::Field(Ident("foo".to_string())), Part::Flatten]),
				Idiom(vec![
					Part::Field(Ident("a".to_string())),
					Part::Graph(Graph {
						dir: Dir::Out,
						what: Tables(vec![Table("b".to_string())]),
						expr: Fields::all(),
						..Default::default()
					}),
				]),
				Idiom(vec![Part::Field(Ident("c".to_string())), Part::All]),
			])),
			output: Some(Output::Diff),
			timeout: Some(Timeout(Duration(std::time::Duration::from_secs(1)))),
			parallel: true,
		}),
	]
}

#[test]
fn test_streaming() {
	let expected = statements();
	let mut current_stmt = 0;
	let source_bytes = SOURCE.as_bytes();
	let mut source_start = 0;
	let mut parser = Parser::new(&[]);
	let mut stack = Stack::new();

	for i in 0..(source_bytes.len() - 1) {
		let partial_source = &source_bytes[source_start..i];
		//let src = String::from_utf8_lossy(partial_source);
		//println!("{}:{}", i, src);
		parser = parser.change_source(partial_source);
		parser.reset();
		match stack
			.enter(|stk| parser.parse_partial_statement(i == source_bytes.len(), stk))
			.finish()
		{
			PartialResult::Empty {
				..
			} => continue,
			PartialResult::MoreData => continue,
			PartialResult::Ok {
				value,
				used,
			} => {
				assert_eq!(value, expected[current_stmt]);
				current_stmt += 1;
				source_start += used;
			}
			PartialResult::Err {
				err,
				..
			} => {
				panic!("Streaming test returned an error: {}", err.render_on_bytes(partial_source))
			}
		}
	}

	let partial_source = &source_bytes[source_start..];
	parser = parser.change_source(partial_source);
	parser.reset();
	match stack.enter(|stk| parser.parse_stmt(stk)).finish() {
		Ok(value) => {
			assert_eq!(value, expected[current_stmt]);
			current_stmt += 1;
		}
		Err(e) => {
			panic!("Streaming test returned an error: {}", e.render_on_bytes(partial_source))
		}
	}

	let src = String::from_utf8_lossy(&source_bytes[source_start..]);
	let range = src.char_indices().nth(100).map(|x| x.0).unwrap_or(src.len());
	let src = &src[..range];
	parser.reset();
	parser = parser.change_source(&source_bytes[source_start..]);
	assert_eq!(
		current_stmt,
		expected.len(),
		"failed to parse at {}\nAt statement {}\n\n{:?}",
		src,
		expected[current_stmt],
		stack.enter(|stk| parser.parse_partial_statement(true, stk)).finish()
	);
}
