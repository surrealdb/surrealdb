use crate::{
	sql::{
		block::Entry,
		changefeed::ChangeFeed,
		filter::Filter,
		index::{Distance, MTreeParams, SearchParams, VectorType},
		language::Language,
		statements::{
			analyze::AnalyzeStatement, show::ShowSince, show::ShowStatement, sleep::SleepStatement,
			BeginStatement, BreakStatement, CancelStatement, CommitStatement, ContinueStatement,
			CreateStatement, DefineAnalyzerStatement, DefineDatabaseStatement,
			DefineEventStatement, DefineFieldStatement, DefineFunctionStatement,
			DefineIndexStatement, DefineNamespaceStatement, DefineParamStatement, DefineStatement,
			DefineTableStatement, DefineTokenStatement, DeleteStatement, ForeachStatement,
			IfelseStatement, InfoStatement, InsertStatement, KillStatement, OptionStatement,
			OutputStatement, RelateStatement, RemoveAnalyzerStatement, RemoveDatabaseStatement,
			RemoveEventStatement, RemoveFieldStatement, RemoveFunctionStatement,
			RemoveIndexStatement, RemoveNamespaceStatement, RemoveParamStatement,
			RemoveScopeStatement, RemoveStatement, RemoveTableStatement, RemoveTokenStatement,
			RemoveUserStatement, SelectStatement, SetStatement, ThrowStatement, UpdateStatement,
			UseStatement,
		},
		tokenizer::Tokenizer,
		Algorithm, Array, Base, Block, Cond, Data, Datetime, Dir, Duration, Edges, Explain,
		Expression, Fetch, Fetchs, Field, Fields, Future, Graph, Group, Groups, Id, Ident, Idiom,
		Idioms, Index, Kind, Limit, Number, Object, Operator, Order, Orders, Output, Param, Part,
		Permission, Permissions, Scoring, Split, Splits, Start, Statement, Strand, Subquery, Table,
		TableType, Tables, Thing, Timeout, Uuid, Value, Values, Version, With,
	},
	syn::parser::mac::test_parse,
};
use chrono::{offset::TimeZone, NaiveDate, Offset, Utc};

#[test]
pub fn parse_analyze() {
	let res = test_parse!(parse_stmt, r#"ANALYZE INDEX b on a"#).unwrap();
	assert_eq!(
		res,
		Statement::Analyze(AnalyzeStatement::Idx(Ident("a".to_string()), Ident("b".to_string())))
	)
}

#[test]
pub fn parse_begin() {
	let res = test_parse!(parse_stmt, r#"BEGIN"#).unwrap();
	assert_eq!(res, Statement::Begin(BeginStatement));
	let res = test_parse!(parse_stmt, r#"BEGIN TRANSACTION"#).unwrap();
	assert_eq!(res, Statement::Begin(BeginStatement));
}

#[test]
pub fn parse_break() {
	let res = test_parse!(parse_stmt, r#"BREAK"#).unwrap();
	assert_eq!(res, Statement::Break(BreakStatement));
}

#[test]
pub fn parse_cancel() {
	let res = test_parse!(parse_stmt, r#"CANCEL"#).unwrap();
	assert_eq!(res, Statement::Cancel(CancelStatement));
	let res = test_parse!(parse_stmt, r#"CANCEL TRANSACTION"#).unwrap();
	assert_eq!(res, Statement::Cancel(CancelStatement));
}

#[test]
pub fn parse_commit() {
	let res = test_parse!(parse_stmt, r#"COMMIT"#).unwrap();
	assert_eq!(res, Statement::Commit(CommitStatement));
	let res = test_parse!(parse_stmt, r#"COMMIT TRANSACTION"#).unwrap();
	assert_eq!(res, Statement::Commit(CommitStatement));
}

#[test]
pub fn parse_continue() {
	let res = test_parse!(parse_stmt, r#"CONTINUE"#).unwrap();
	assert_eq!(res, Statement::Continue(ContinueStatement));
}

#[test]
fn parse_create() {
	let res = test_parse!(
		parse_stmt,
		"CREATE ONLY foo SET bar = 3, foo +?= 4 RETURN VALUE foo AS bar TIMEOUT 1s PARALLEL"
	)
	.unwrap();
	assert_eq!(
		res,
		Statement::Create(CreateStatement {
			only: true,
			what: Values(vec![Value::Table(Table("foo".to_owned()))]),
			data: Some(Data::SetExpression(vec![
				(
					Idiom(vec![Part::Field(Ident("bar".to_owned()))]),
					Operator::Equal,
					Value::Number(Number::Int(3))
				),
				(
					Idiom(vec![Part::Field(Ident("foo".to_owned()))]),
					Operator::Ext,
					Value::Number(Number::Int(4))
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
		}),
	);
}

#[test]
fn parse_define_namespace() {
	let res = test_parse!(parse_stmt, "DEFINE NAMESPACE a COMMENT 'test'").unwrap();
	assert_eq!(
		res,
		Statement::Define(DefineStatement::Namespace(DefineNamespaceStatement {
			id: None,
			name: Ident("a".to_string()),
			comment: Some(Strand("test".to_string())),
			if_not_exists: false,
		}))
	);

	let res = test_parse!(parse_stmt, "DEFINE NS a").unwrap();
	assert_eq!(
		res,
		Statement::Define(DefineStatement::Namespace(DefineNamespaceStatement {
			id: None,
			name: Ident("a".to_string()),
			comment: None,
			if_not_exists: false,
		}))
	)
}

#[test]
fn parse_define_database() {
	let res =
		test_parse!(parse_stmt, "DEFINE DATABASE a COMMENT 'test' CHANGEFEED 10m INCLUDE ORIGINAL")
			.unwrap();
	assert_eq!(
		res,
		Statement::Define(DefineStatement::Database(DefineDatabaseStatement {
			id: None,
			name: Ident("a".to_string()),
			comment: Some(Strand("test".to_string())),
			changefeed: Some(ChangeFeed {
				expiry: std::time::Duration::from_secs(60) * 10,
				store_original: true,
			}),
			if_not_exists: false,
		}))
	);

	let res = test_parse!(parse_stmt, "DEFINE DB a").unwrap();
	assert_eq!(
		res,
		Statement::Define(DefineStatement::Database(DefineDatabaseStatement {
			id: None,
			name: Ident("a".to_string()),
			comment: None,
			changefeed: None,
			if_not_exists: false,
		}))
	)
}

#[test]
fn parse_define_function() {
	let res = test_parse!(
		parse_stmt,
		r#"DEFINE FUNCTION fn::foo::bar($a: number, $b: array<bool,3>) {
			RETURN a
		} COMMENT 'test' PERMISSIONS FULL
		"#
	)
	.unwrap();

	assert_eq!(
		res,
		Statement::Define(DefineStatement::Function(DefineFunctionStatement {
			name: Ident("foo::bar".to_string()),
			args: vec![
				(Ident("a".to_string()), Kind::Number),
				(Ident("b".to_string()), Kind::Array(Box::new(Kind::Bool), Some(3)))
			],
			block: Block(vec![Entry::Output(OutputStatement {
				what: Value::Idiom(Idiom(vec![Part::Field(Ident("a".to_string()))])),
				fetch: None,
			})]),
			comment: Some(Strand("test".to_string())),
			permissions: Permission::Full,
			if_not_exists: false,
		}))
	)
}

#[test]
fn parse_define_user() {
	let res = test_parse!(
		parse_stmt,
		r#"DEFINE USER user ON ROOT COMMENT 'test' PASSHASH 'hunter2' ROLES foo, bar COMMENT "*******""#
	)
	.unwrap();

	let Statement::Define(DefineStatement::User(stmt)) = res else {
		panic!()
	};

	assert_eq!(stmt.name, Ident("user".to_string()));
	assert_eq!(stmt.base, Base::Root);
	assert_eq!(stmt.hash, "hunter2".to_owned());
	assert_eq!(stmt.roles, vec![Ident("foo".to_string()), Ident("bar".to_string())]);
	assert_eq!(stmt.comment, Some(Strand("*******".to_string())))
}

#[test]
fn parse_define_token() {
	let res = test_parse!(
		parse_stmt,
		r#"DEFINE TOKEN a ON SCOPE b TYPE EDDSA VALUE "foo" COMMENT "bar""#
	)
	.unwrap();
	assert_eq!(
		res,
		Statement::Define(DefineStatement::Token(DefineTokenStatement {
			name: Ident("a".to_string()),
			base: Base::Sc(Ident("b".to_string())),
			kind: Algorithm::EdDSA,
			code: "foo".to_string(),
			comment: Some(Strand("bar".to_string())),
			if_not_exists: false,
		}))
	)
}

#[test]
fn parse_define_scope() {
	let res = test_parse!(
		parse_stmt,
		r#"DEFINE SCOPE a SESSION 1s SIGNUP true SIGNIN false COMMENT "bar""#
	)
	.unwrap();

	// manually compare since DefineScopeStatement creates a random code in its parser.
	let Statement::Define(DefineStatement::Scope(stmt)) = res else {
		panic!()
	};

	assert_eq!(stmt.name, Ident("a".to_string()));
	assert_eq!(stmt.comment, Some(Strand("bar".to_string())));
	assert_eq!(stmt.session, Some(Duration(std::time::Duration::from_secs(1))));
	assert_eq!(stmt.signup, Some(Value::Bool(true)));
	assert_eq!(stmt.signin, Some(Value::Bool(false)));
}

#[test]
fn parse_define_param() {
	let res =
		test_parse!(parse_stmt, r#"DEFINE PARAM $a VALUE { a: 1, "b": 3 } PERMISSIONS WHERE null"#)
			.unwrap();

	assert_eq!(
		res,
		Statement::Define(DefineStatement::Param(DefineParamStatement {
			name: Ident("a".to_string()),
			value: Value::Object(Object(
				[
					("a".to_string(), Value::Number(Number::Int(1))),
					("b".to_string(), Value::Number(Number::Int(3))),
				]
				.into_iter()
				.collect()
			)),
			comment: None,
			permissions: Permission::Specific(Value::Null),
			if_not_exists: false,
		}))
	);
}

#[test]
fn parse_define_table() {
	let res =
		test_parse!(parse_stmt, r#"DEFINE TABLE name DROP SCHEMAFUL CHANGEFEED 1s INCLUDE ORIGINAL PERMISSIONS FOR SELECT WHERE a = 1 AS SELECT foo FROM bar GROUP BY foo"#)
			.unwrap();

	assert_eq!(
		res,
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
					false
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
						r: Value::Number(Number::Int(1))
					}
				))),
				create: Permission::None,
				update: Permission::None,
				delete: Permission::None,
			},
			changefeed: Some(ChangeFeed {
				expiry: std::time::Duration::from_secs(1),
				store_original: true,
			}),
			comment: None,
			if_not_exists: false,
			kind: TableType::Any,
		}))
	);
}

#[test]
fn parse_define_event() {
	let res =
		test_parse!(parse_stmt, r#"DEFINE EVENT event ON TABLE table WHEN null THEN null,none"#)
			.unwrap();

	assert_eq!(
		res,
		Statement::Define(DefineStatement::Event(DefineEventStatement {
			name: Ident("event".to_owned()),
			what: Ident("table".to_owned()),
			when: Value::Null,
			then: Values(vec![Value::Null, Value::None]),
			comment: None,
			if_not_exists: false,
		}))
	)
}

#[test]
fn parse_define_field() {
	let res = test_parse!(
		parse_stmt,
		r#"DEFINE FIELD foo.*[*]... ON TABLE bar FLEX TYPE option<number | array<record<foo>,10>> VALUE null ASSERT true DEFAULT false PERMISSIONS FOR DELETE, UPDATE NONE, FOR create WHERE true"#
	).unwrap();

	assert_eq!(
		res,
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
				Kind::Array(Box::new(Kind::Record(vec![Table("foo".to_owned())])), Some(10))
			])))),
			readonly: false,
			value: Some(Value::Null),
			assert: Some(Value::Bool(true)),
			default: Some(Value::Bool(false)),
			permissions: Permissions {
				delete: Permission::None,
				update: Permission::None,
				create: Permission::Specific(Value::Bool(true)),
				select: Permission::Full,
			},
			comment: None,
			if_not_exists: false,
		}))
	)
}

#[test]
fn parse_define_index() {
	let res = test_parse!(
		parse_stmt,
		r#"DEFINE INDEX index ON TABLE table FIELDS a,b[*] SEARCH ANALYZER ana BM25 (0.1,0.2)
			DOC_IDS_ORDER 1
			DOC_LENGTHS_ORDER 2
			POSTINGS_ORDER 3
			TERMS_ORDER 4
			DOC_IDS_CACHE 5
			DOC_LENGTHS_CACHE 6
			POSTINGS_CACHE 7
			TERMS_CACHE 8
			HIGHLIGHTS"#
	)
	.unwrap();

	assert_eq!(
		res,
		Statement::Define(DefineStatement::Index(DefineIndexStatement {
			name: Ident("index".to_owned()),
			what: Ident("table".to_owned()),
			cols: Idioms(vec![
				Idiom(vec![Part::Field(Ident("a".to_owned()))]),
				Idiom(vec![Part::Field(Ident("b".to_owned())), Part::All])
			]),
			index: Index::Search(SearchParams {
				az: Ident("ana".to_owned()),
				hl: true,
				sc: Scoring::Bm {
					k1: 0.1,
					b: 0.2
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
		}))
	);

	let res =
		test_parse!(parse_stmt, r#"DEFINE INDEX index ON TABLE table FIELDS a UNIQUE"#).unwrap();

	assert_eq!(
		res,
		Statement::Define(DefineStatement::Index(DefineIndexStatement {
			name: Ident("index".to_owned()),
			what: Ident("table".to_owned()),
			cols: Idioms(vec![Idiom(vec![Part::Field(Ident("a".to_owned()))]),]),
			index: Index::Uniq,
			comment: None,
			if_not_exists: false,
		}))
	);

	let res =
		test_parse!(parse_stmt, r#"DEFINE INDEX index ON TABLE table FIELDS a MTREE DIMENSION 4 DISTANCE MINKOWSKI 5 CAPACITY 6 DOC_IDS_ORDER 7 DOC_IDS_CACHE 8 MTREE_CACHE 9"#).unwrap();

	assert_eq!(
		res,
		Statement::Define(DefineStatement::Index(DefineIndexStatement {
			name: Ident("index".to_owned()),
			what: Ident("table".to_owned()),
			cols: Idioms(vec![Idiom(vec![Part::Field(Ident("a".to_owned()))]),]),
			index: Index::MTree(MTreeParams {
				dimension: 4,
				_distance: Default::default(),
				distance: Distance::Minkowski(Number::Int(5)),
				capacity: 6,
				doc_ids_order: 7,
				doc_ids_cache: 8,
				mtree_cache: 9,
				vector_type: VectorType::F64,
			}),
			comment: None,
			if_not_exists: false,
		}))
	);
}

#[test]
fn parse_define_analyzer() {
	let res = test_parse!(
		parse_stmt,
		r#"DEFINE ANALYZER ana FILTERS ASCII, EDGENGRAM(1,2), NGRAM(3,4), LOWERCASE, SNOWBALL(NLD), UPPERCASE TOKENIZERS BLANK, CAMEL, CLASS, PUNCT FUNCTION fn::foo::bar"#
	).unwrap();

	assert_eq!(
		res,
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
			comment: None,
			function: Some(Ident("foo::bar".to_string())),
			if_not_exists: false,
		})),
	)
}

#[test]
fn parse_delete() {
	let res = test_parse!(
		parse_statement,
		"DELETE FROM ONLY |foo:32..64| Where 2 RETURN AFTER TIMEOUT 1s PARALLEL"
	)
	.unwrap();
	assert_eq!(
		res,
		Statement::Delete(DeleteStatement {
			only: true,
			what: Values(vec![Value::Mock(crate::sql::Mock::Range("foo".to_string(), 32, 64))]),
			cond: Some(Cond(Value::Number(Number::Int(2)))),
			output: Some(Output::After),
			timeout: Some(Timeout(Duration(std::time::Duration::from_secs(1)))),
			parallel: true,
		})
	);
}

#[test]
fn parse_delete_2() {
	let res = test_parse!(
		parse_stmt,
		r#"DELETE FROM ONLY a:b->?[$][?true] WHERE null RETURN NULL TIMEOUT 1h PARALLEL"#
	)
	.unwrap();

	assert_eq!(
		res,
		Statement::Delete(DeleteStatement {
			only: true,
			what: Values(vec![Value::Idiom(Idiom(vec![
				Part::Start(Value::Edges(Box::new(Edges {
					dir: Dir::Out,
					from: Thing {
						tb: "a".to_owned(),
						id: Id::String("b".to_owned()),
					},
					what: Tables::default(),
				}))),
				Part::Last,
				Part::Where(Value::Bool(true)),
			]))]),
			cond: Some(Cond(Value::Null)),
			output: Some(Output::Null),
			timeout: Some(Timeout(Duration(std::time::Duration::from_secs(60 * 60)))),
			parallel: true
		})
	)
}

#[test]
pub fn parse_for() {
	let res = test_parse!(
		parse_stmt,
		r#"FOR $foo IN (SELECT foo FROM bar) * 2 {
			BREAK
		}"#
	)
	.unwrap();

	assert_eq!(
		res,
		Statement::Foreach(ForeachStatement {
			param: Param(Ident("foo".to_owned())),
			range: Value::Expression(Box::new(Expression::Binary {
				l: Value::Subquery(Box::new(Subquery::Select(SelectStatement {
					expr: Fields(
						vec![Field::Single {
							expr: Value::Idiom(Idiom(vec![Part::Field(Ident("foo".to_owned()))])),
							alias: None
						}],
						false
					),
					what: Values(vec![Value::Table(Table("bar".to_owned()))]),
					..Default::default()
				}))),
				o: Operator::Mul,
				r: Value::Number(Number::Int(2))
			})),
			block: Block(vec![Entry::Break(BreakStatement)])
		})
	)
}

#[test]
fn parse_if() {
	let res =
		test_parse!(parse_stmt, r#"IF foo THEN bar ELSE IF faz THEN baz ELSE baq END"#).unwrap();
	assert_eq!(
		res,
		Statement::Ifelse(IfelseStatement {
			exprs: vec![
				(
					Value::Idiom(Idiom(vec![Part::Field(Ident("foo".to_owned()))])),
					Value::Idiom(Idiom(vec![Part::Field(Ident("bar".to_owned()))]))
				),
				(
					Value::Idiom(Idiom(vec![Part::Field(Ident("faz".to_owned()))])),
					Value::Idiom(Idiom(vec![Part::Field(Ident("baz".to_owned()))]))
				)
			],
			close: Some(Value::Idiom(Idiom(vec![Part::Field(Ident("baq".to_owned()))])))
		})
	)
}

#[test]
fn parse_if_block() {
	let res =
		test_parse!(parse_stmt, r#"IF foo { bar } ELSE IF faz { baz } ELSE { baq }"#).unwrap();
	assert_eq!(
		res,
		Statement::Ifelse(IfelseStatement {
			exprs: vec![
				(
					Value::Idiom(Idiom(vec![Part::Field(Ident("foo".to_owned()))])),
					Value::Block(Box::new(Block(vec![Entry::Value(Value::Idiom(Idiom(vec![
						Part::Field(Ident("bar".to_owned()))
					])))]))),
				),
				(
					Value::Idiom(Idiom(vec![Part::Field(Ident("faz".to_owned()))])),
					Value::Block(Box::new(Block(vec![Entry::Value(Value::Idiom(Idiom(vec![
						Part::Field(Ident("baz".to_owned()))
					])))]))),
				)
			],
			close: Some(Value::Block(Box::new(Block(vec![Entry::Value(Value::Idiom(Idiom(
				vec![Part::Field(Ident("baq".to_owned()))]
			)))])))),
		})
	)
}

#[test]
fn parse_info() {
	let res = test_parse!(parse_stmt, "INFO FOR ROOT").unwrap();
	assert_eq!(res, Statement::Info(InfoStatement::Root));

	let res = test_parse!(parse_stmt, "INFO FOR KV").unwrap();
	assert_eq!(res, Statement::Info(InfoStatement::Root));

	let res = test_parse!(parse_stmt, "INFO FOR NAMESPACE").unwrap();
	assert_eq!(res, Statement::Info(InfoStatement::Ns));

	let res = test_parse!(parse_stmt, "INFO FOR NS").unwrap();
	assert_eq!(res, Statement::Info(InfoStatement::Ns));

	let res = test_parse!(parse_stmt, "INFO FOR SCOPE scope").unwrap();
	assert_eq!(res, Statement::Info(InfoStatement::Sc(Ident("scope".to_owned()))));

	let res = test_parse!(parse_stmt, "INFO FOR TABLE table").unwrap();
	assert_eq!(res, Statement::Info(InfoStatement::Tb(Ident("table".to_owned()))));

	let res = test_parse!(parse_stmt, "INFO FOR USER user").unwrap();
	assert_eq!(res, Statement::Info(InfoStatement::User(Ident("user".to_owned()), None)));

	let res = test_parse!(parse_stmt, "INFO FOR USER user ON namespace").unwrap();
	assert_eq!(res, Statement::Info(InfoStatement::User(Ident("user".to_owned()), Some(Base::Ns))));
}

#[test]
fn parse_select() {
	let res = test_parse!(
		parse_stmt,
		r#"
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
    EXPLAIN FULL
		"#
	)
	.unwrap();

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

	assert_eq!(
		res,
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
							Value::Number(Number::Int(2))
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
			order: Some(Orders(vec![Order {
				order: Idiom(vec![Part::Field(Ident("foo".to_owned()))]),
				random: false,
				collate: true,
				numeric: true,
				direction: true,
			}])),
			limit: Some(Limit(Value::Thing(Thing {
				tb: "a".to_owned(),
				id: Id::String("b".to_owned()),
			}))),
			start: Some(Start(Value::Object(Object(
				[("a".to_owned(), Value::Bool(true))].into_iter().collect()
			)))),
			fetch: Some(Fetchs(vec![Fetch(Idiom(vec![Part::Field(Ident("foo".to_owned()))]))])),
			version: Some(Version(Datetime(expected_datetime))),
			timeout: None,
			parallel: false,
			explain: Some(Explain(true)),
		}),
	);
}

#[test]
fn parse_let() {
	let res = test_parse!(parse_stmt, r#"LET $param = 1"#).unwrap();
	assert_eq!(
		res,
		Statement::Set(SetStatement {
			name: "param".to_owned(),
			what: Value::Number(Number::Int(1))
		})
	);

	let res = test_parse!(parse_stmt, r#"$param = 1"#).unwrap();
	assert_eq!(
		res,
		Statement::Set(SetStatement {
			name: "param".to_owned(),
			what: Value::Number(Number::Int(1))
		})
	);
}

#[test]
fn parse_show() {
	let res = test_parse!(parse_stmt, r#"SHOW CHANGES FOR TABLE foo SINCE 1 LIMIT 10"#).unwrap();

	assert_eq!(
		res,
		Statement::Show(ShowStatement {
			table: Some(Table("foo".to_owned())),
			since: ShowSince::Versionstamp(1),
			limit: Some(10)
		})
	);

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

	let res = test_parse!(
		parse_stmt,
		r#"SHOW CHANGES FOR DATABASE SINCE d"2012-04-23T18:25:43.0000511Z""#
	)
	.unwrap();
	assert_eq!(
		res,
		Statement::Show(ShowStatement {
			table: None,
			since: ShowSince::Timestamp(Datetime(expected_datetime)),
			limit: None
		})
	)
}

#[test]
fn parse_sleep() {
	let res = test_parse!(parse_stmt, r"SLEEP 1s").unwrap();

	let expect = Statement::Sleep(SleepStatement {
		duration: Duration(std::time::Duration::from_secs(1)),
	});
	assert_eq!(res, expect)
}

#[test]
fn parse_use() {
	let res = test_parse!(parse_stmt, r"USE NS foo").unwrap();
	let expect = Statement::Use(UseStatement {
		ns: Some("foo".to_owned()),
		db: None,
	});
	assert_eq!(res, expect);

	let res = test_parse!(parse_stmt, r"USE DB foo").unwrap();
	let expect = Statement::Use(UseStatement {
		ns: None,
		db: Some("foo".to_owned()),
	});
	assert_eq!(res, expect);

	let res = test_parse!(parse_stmt, r"USE NS bar DB foo").unwrap();
	let expect = Statement::Use(UseStatement {
		ns: Some("bar".to_owned()),
		db: Some("foo".to_owned()),
	});
	assert_eq!(res, expect);
}

#[test]
fn parse_value_stmt() {
	let res = test_parse!(parse_stmt, r"1s").unwrap();
	let expect = Statement::Value(Value::Duration(Duration(std::time::Duration::from_secs(1))));
	assert_eq!(res, expect);
}

#[test]
fn parse_throw() {
	let res = test_parse!(parse_stmt, r"THROW 1s").unwrap();

	let expect = Statement::Throw(ThrowStatement {
		error: Value::Duration(Duration(std::time::Duration::from_secs(1))),
	});
	assert_eq!(res, expect)
}

#[test]
fn parse_insert() {
	let res = test_parse!(
		parse_stmt,
	r#"INSERT IGNORE INTO $foo (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE a.b +?= null, c.d += none RETURN AFTER"#
	).unwrap();
	assert_eq!(
		res,
		Statement::Insert(InsertStatement {
			into: Value::Param(Param(Ident("foo".to_owned()))),
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
			timeout: None,
			parallel: false,
		}),
	)
}

#[test]
fn parse_kill() {
	let res = test_parse!(parse_stmt, r#"KILL $param"#).unwrap();
	assert_eq!(
		res,
		Statement::Kill(KillStatement {
			id: Value::Param(Param(Ident("param".to_owned())))
		})
	);

	let res = test_parse!(parse_stmt, r#"KILL u"e72bee20-f49b-11ec-b939-0242ac120002" "#).unwrap();
	assert_eq!(
		res,
		Statement::Kill(KillStatement {
			id: Value::Uuid(Uuid(uuid::uuid!("e72bee20-f49b-11ec-b939-0242ac120002")))
		})
	);
}

#[test]
fn parse_live() {
	let res = test_parse!(parse_stmt, r#"LIVE SELECT DIFF FROM $foo"#).unwrap();
	let Statement::Live(stmt) = res else {
		panic!()
	};
	assert_eq!(stmt.expr, Fields::default());
	assert_eq!(stmt.what, Value::Param(Param(Ident("foo".to_owned()))));

	let res =
		test_parse!(parse_stmt, r#"LIVE SELECT foo FROM table WHERE true FETCH a[where foo],b"#)
			.unwrap();
	let Statement::Live(stmt) = res else {
		panic!()
	};
	assert_eq!(
		stmt.expr,
		Fields(
			vec![Field::Single {
				expr: Value::Idiom(Idiom(vec![Part::Field(Ident("foo".to_owned()))])),
				alias: None,
			}],
			false,
		)
	);
	assert_eq!(stmt.what, Value::Table(Table("table".to_owned())));
	assert_eq!(stmt.cond, Some(Cond(Value::Bool(true))));
	assert_eq!(
		stmt.fetch,
		Some(Fetchs(vec![
			Fetch(Idiom(vec![
				Part::Field(Ident("a".to_owned())),
				Part::Where(Value::Idiom(Idiom(vec![Part::Field(Ident("foo".to_owned()))]))),
			])),
			Fetch(Idiom(vec![Part::Field(Ident("b".to_owned()))])),
		])),
	)
}

#[test]
fn parse_option() {
	let res = test_parse!(parse_stmt, r#"OPTION value = true"#).unwrap();
	assert_eq!(
		res,
		Statement::Option(OptionStatement {
			name: Ident("value".to_owned()),
			what: true
		})
	)
}

#[test]
fn parse_return() {
	let res = test_parse!(parse_stmt, r#"RETURN RETRUN FETCH RETURN"#).unwrap();
	assert_eq!(
		res,
		Statement::Output(OutputStatement {
			what: Value::Idiom(Idiom(vec![Part::Field(Ident("RETRUN".to_owned()))])),
			fetch: Some(Fetchs(vec![Fetch(Idiom(vec![Part::Field(
				Ident("RETURN".to_owned()).to_owned()
			)]))])),
		}),
	)
}

#[test]
fn parse_relate() {
	let res = test_parse!(
		parse_stmt,
		r#"RELATE ONLY [1,2]->a:b->(CREATE foo) UNIQUE SET a += 1 RETURN NONE PARALLEL"#
	)
	.unwrap();
	assert_eq!(
		res,
		Statement::Relate(RelateStatement {
			only: true,
			kind: Value::Thing(Thing {
				tb: "a".to_owned(),
				id: Id::String("b".to_owned()),
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
			}))),
			uniq: true,
			data: Some(Data::SetExpression(vec![(
				Idiom(vec![Part::Field(Ident("a".to_owned()))]),
				Operator::Inc,
				Value::Number(Number::Int(1))
			)])),
			output: Some(Output::None),
			timeout: None,
			parallel: true,
		}),
	)
}

#[test]
fn parse_remove() {
	let res = test_parse!(parse_stmt, r#"REMOVE NAMESPACE ns"#).unwrap();
	assert_eq!(
		res,
		Statement::Remove(RemoveStatement::Namespace(RemoveNamespaceStatement {
			name: Ident("ns".to_owned()),
			if_exists: false,
		}))
	);

	let res = test_parse!(parse_stmt, r#"REMOVE DB database"#).unwrap();
	assert_eq!(
		res,
		Statement::Remove(RemoveStatement::Database(RemoveDatabaseStatement {
			name: Ident("database".to_owned()),
			if_exists: false,
		}))
	);

	let res = test_parse!(parse_stmt, r#"REMOVE FUNCTION fn::foo::bar"#).unwrap();
	assert_eq!(
		res,
		Statement::Remove(RemoveStatement::Function(RemoveFunctionStatement {
			name: Ident("foo::bar".to_owned()),
			if_exists: false,
		}))
	);
	let res = test_parse!(parse_stmt, r#"REMOVE FUNCTION fn::foo::bar();"#).unwrap();
	assert_eq!(
		res,
		Statement::Remove(RemoveStatement::Function(RemoveFunctionStatement {
			name: Ident("foo::bar".to_owned()),
			if_exists: false,
		}))
	);

	let res = test_parse!(parse_stmt, r#"REMOVE TOKEN foo ON SCOPE bar"#).unwrap();
	assert_eq!(
		res,
		Statement::Remove(RemoveStatement::Token(RemoveTokenStatement {
			name: Ident("foo".to_owned()),
			base: Base::Sc(Ident("bar".to_owned())),
			if_exists: false,
		}))
	);

	let res = test_parse!(parse_stmt, r#"REMOVE SCOPE foo"#).unwrap();
	assert_eq!(
		res,
		Statement::Remove(RemoveStatement::Scope(RemoveScopeStatement {
			name: Ident("foo".to_owned()),
			if_exists: false,
		}))
	);

	let res = test_parse!(parse_stmt, r#"REMOVE PARAM $foo"#).unwrap();
	assert_eq!(
		res,
		Statement::Remove(RemoveStatement::Param(RemoveParamStatement {
			name: Ident("foo".to_owned()),
			if_exists: false,
		}))
	);

	let res = test_parse!(parse_stmt, r#"REMOVE TABLE foo"#).unwrap();
	assert_eq!(
		res,
		Statement::Remove(RemoveStatement::Table(RemoveTableStatement {
			name: Ident("foo".to_owned()),
			if_exists: false,
		}))
	);

	let res = test_parse!(parse_stmt, r#"REMOVE EVENT foo ON TABLE bar"#).unwrap();
	assert_eq!(
		res,
		Statement::Remove(RemoveStatement::Event(RemoveEventStatement {
			name: Ident("foo".to_owned()),
			what: Ident("bar".to_owned()),
			if_exists: false,
		}))
	);

	let res = test_parse!(parse_stmt, r#"REMOVE FIELD foo.bar[10] ON bar"#).unwrap();
	assert_eq!(
		res,
		Statement::Remove(RemoveStatement::Field(RemoveFieldStatement {
			name: Idiom(vec![
				Part::Field(Ident("foo".to_owned())),
				Part::Field(Ident("bar".to_owned())),
				Part::Index(Number::Int(10))
			]),
			what: Ident("bar".to_owned()),
			if_exists: false,
		}))
	);

	let res = test_parse!(parse_stmt, r#"REMOVE INDEX foo ON bar"#).unwrap();
	assert_eq!(
		res,
		Statement::Remove(RemoveStatement::Index(RemoveIndexStatement {
			name: Ident("foo".to_owned()),
			what: Ident("bar".to_owned()),
			if_exists: false,
		}))
	);

	let res = test_parse!(parse_stmt, r#"REMOVE ANALYZER foo"#).unwrap();
	assert_eq!(
		res,
		Statement::Remove(RemoveStatement::Analyzer(RemoveAnalyzerStatement {
			name: Ident("foo".to_owned()),
			if_exists: false,
		}))
	);

	let res = test_parse!(parse_stmt, r#"REMOVE user foo on database"#).unwrap();
	assert_eq!(
		res,
		Statement::Remove(RemoveStatement::User(RemoveUserStatement {
			name: Ident("foo".to_owned()),
			base: Base::Db,
			if_exists: false,
		}))
	);
}

#[test]
fn parse_update() {
	let res = test_parse!(
		parse_stmt,
		r#"UPDATE ONLY <future> { "text" }, a->b UNSET foo... , a->b, c[*] WHERE true RETURN DIFF TIMEOUT 1s PARALLEL"#
	)
	.unwrap();
	assert_eq!(
		res,
		Statement::Update(UpdateStatement {
			only: true,
			what: Values(vec![
				Value::Future(Box::new(Future(Block(vec![Entry::Value(Value::Strand(Strand(
					"text".to_string()
				)))])))),
				Value::Idiom(Idiom(vec![
					Part::Field(Ident("a".to_string())),
					Part::Graph(Graph {
						dir: Dir::Out,
						what: Tables(vec![Table("b".to_string())]),
						expr: Fields::all(),
						..Default::default()
					})
				]))
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
					})
				]),
				Idiom(vec![Part::Field(Ident("c".to_string())), Part::All])
			])),
			output: Some(Output::Diff),
			timeout: Some(Timeout(Duration(std::time::Duration::from_secs(1)))),
			parallel: true,
		})
	);
}
