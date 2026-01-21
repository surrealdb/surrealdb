use bytes::BytesMut;
use chrono::offset::TimeZone;
use chrono::{NaiveDate, Offset, Utc};

use crate::sql::access::AccessDuration;
use crate::sql::access_type::{
	AccessType, JwtAccess, JwtAccessVerify, JwtAccessVerifyKey, RecordAccess,
};
use crate::sql::changefeed::ChangeFeed;
use crate::sql::data::Assignment;
use crate::sql::field::Selector;
use crate::sql::filter::Filter;
use crate::sql::index::FullTextParams;
use crate::sql::language::Language;
use crate::sql::literal::ObjectEntry;
use crate::sql::lookup::{LookupKind, LookupSubject};
use crate::sql::order::{OrderList, Ordering};
use crate::sql::statements::define::{
	DefineAccessStatement, DefineAnalyzerStatement, DefineDatabaseStatement, DefineDefault,
	DefineEventStatement, DefineFieldStatement, DefineFunctionStatement, DefineIndexStatement,
	DefineKind, DefineNamespaceStatement, DefineParamStatement, DefineStatement,
	DefineTableStatement,
};
use crate::sql::statements::show::{ShowSince, ShowStatement};
use crate::sql::statements::sleep::SleepStatement;
use crate::sql::statements::{
	CreateStatement, DeleteStatement, ForeachStatement, IfelseStatement, InfoStatement,
	InsertStatement, KillStatement, OutputStatement, RelateStatement, RemoveFieldStatement,
	RemoveFunctionStatement, RemoveStatement, SelectStatement, SetStatement, UpdateStatement,
	UpsertStatement,
};
use crate::sql::tokenizer::Tokenizer;
use crate::sql::{
	Algorithm, AssignOperator, Base, BinaryOperator, Block, Cond, Data, Dir, Explain, Expr, Fetch,
	Fetchs, Field, Fields, Function, FunctionCall, Group, Groups, Idiom, Index, Kind, Limit,
	Literal, Lookup, Mock, Order, Output, Param, Part, Permission, Permissions, RecordIdKeyLit,
	RecordIdLit, Scoring, Script, Split, Splits, Start, TableType, TopLevelExpr, With,
};
use crate::syn::parser::StatementStream;
use crate::types::{PublicDatetime, PublicDuration, PublicUuid};
use crate::val::range::TypedRange;

fn ident_field(name: &str) -> Expr {
	Expr::Idiom(Idiom(vec![Part::Field(name.to_string())]))
}

static SOURCE: &str = r#"
	BEGIN;
	BEGIN TRANSACTION;
	BREAK;
	CANCEL;
	CANCEL TRANSACTION;
	COMMIT;
	COMMIT TRANSACTION;
	CONTINUE;
	CREATE ONLY foo SET bar = 3, foo +?= 4 RETURN VALUE foo AS bar TIMEOUT 1s;
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
	DEFINE FIELD foo.*[*]... ON TABLE bar TYPE option<number | array<record<foo>,10>> VALUE null ASSERT true DEFAULT false PERMISSIONS FOR UPDATE NONE, FOR CREATE WHERE true;
	DEFINE INDEX index ON TABLE table FIELDS a FULLTEXT ANALYZER ana BM25 (0.1,0.2) HIGHLIGHTS;
	DEFINE INDEX index ON TABLE table FIELDS a UNIQUE;
	DEFINE ANALYZER ana FILTERS ASCII, EDGENGRAM(1,2), NGRAM(3,4), LOWERCASE, SNOWBALL(NLD), UPPERCASE TOKENIZERS BLANK, CAMEL, CLASS, PUNCT FUNCTION fn::foo::bar;
	DELETE FROM ONLY |foo:32..64| WITH INDEX index,index_2 Where 2 RETURN AFTER TIMEOUT 1s EXPLAIN FULL;
	DELETE FROM ONLY a:b->?[$][?true] WITH INDEX index,index_2 WHERE null RETURN NULL TIMEOUT 1h EXPLAIN FULL;
	FOR $foo IN (SELECT foo FROM bar) * 2 {
		BREAK
	};
	IF foo THEN bar ELSE IF faz THEN baz ELSE baq END;
	IF foo { bar } ELSE IF faz { baz } ELSE { baq };
	INFO FOR ROOT;
	INFO FOR NAMESPACE;
	INFO FOR USER user ON namespace;
	SELECT bar as foo,[1,2],bar OMIT bar FROM ONLY a
		WITH INDEX index,index_2
		WHERE true
		GROUP foo,bar
		ORDER BY foo COLLATE NUMERIC ASC
		START AT { a: true }
		LIMIT BY a:b
		FETCH foo
		VERSION d"2012-04-23T18:25:43.0000511Z"
		EXPLAIN FULL;
	SELECT foo,bar FROM a SPLIT ON foo,bar;
	LET $param = 1;
	SHOW CHANGES FOR TABLE foo SINCE 1 LIMIT 10;
	SHOW CHANGES FOR DATABASE SINCE d"2012-04-23T18:25:43.0000511Z";
	SLEEP 1s;
	THROW 1s;
	INSERT IGNORE INTO $foo (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE a.b +?= null, c.d += none RETURN AFTER;
	KILL u"e72bee20-f49b-11ec-b939-0242ac120002";
	RETURN RETRUN FETCH RETURN;
	RELATE ONLY [1,2]->a:b->(CREATE foo) UNIQUE SET a += 1 RETURN NONE;
	REMOVE FUNCTION fn::foo::bar();
	REMOVE FIELD foo.bar[10] ON bar;
	UPDATE ONLY a->b WITH INDEX index,index_2 UNSET foo... , a->b, c[*] WHERE true RETURN DIFF TIMEOUT 1s EXPLAIN FULL;
	UPSERT ONLY a->b WITH INDEX index,index_2 UNSET foo... , a->b, c[*] WHERE true RETURN DIFF TIMEOUT 1s EXPLAIN FULL;
	function(){ ((1 + 1)) };
	"a b c d e f g h";
	u"ffffffff-ffff-ffff-ffff-ffffffffffff";
	r"a:[1,2,3,4,5,6,7,8,9,10]";
	/a b c d e f/;
	-123.456e10;
"#;

fn statements() -> Vec<TopLevelExpr> {
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
		TopLevelExpr::Begin,
		TopLevelExpr::Begin,
		TopLevelExpr::Expr(Expr::Break),
		TopLevelExpr::Cancel,
		TopLevelExpr::Cancel,
		TopLevelExpr::Commit,
		TopLevelExpr::Commit,
		TopLevelExpr::Expr(Expr::Continue),
		TopLevelExpr::Expr(Expr::Create(Box::new(CreateStatement {
			only: true,
			what: vec![Expr::Table("foo".to_owned())],
			data: Some(Data::SetExpression(vec![
				Assignment {
					place: Idiom(vec![Part::Field("bar".to_owned())]),
					operator: AssignOperator::Assign,
					value: Expr::Literal(Literal::Integer(3)),
				},
				Assignment {
					place: Idiom(vec![Part::Field("foo".to_owned())]),
					operator: AssignOperator::Extend,
					value: Expr::Literal(Literal::Integer(4)),
				},
			])),
			output: Some(Output::Fields(Fields::Value(Box::new(Selector {
				expr: ident_field("foo"),
				alias: Some(Idiom(vec![Part::Field("bar".to_owned())])),
			})))),
			timeout: Expr::Literal(Literal::Duration(PublicDuration::from_secs(1))),
			version: Expr::Literal(Literal::None),
		}))),
		TopLevelExpr::Expr(Expr::Define(Box::new(DefineStatement::Namespace(
			DefineNamespaceStatement {
				kind: DefineKind::Default,
				id: None,
				name: Expr::Idiom(Idiom::field("a".to_owned())),
				comment: Expr::Literal(Literal::String("test".to_owned())),
			},
		)))),
		TopLevelExpr::Expr(Expr::Define(Box::new(DefineStatement::Namespace(
			DefineNamespaceStatement {
				kind: DefineKind::Default,
				id: None,
				name: Expr::Idiom(Idiom::field("a".to_owned())),
				comment: Expr::Literal(Literal::None),
			},
		)))),
		TopLevelExpr::Expr(Expr::Define(Box::new(DefineStatement::Database(
			DefineDatabaseStatement {
				kind: DefineKind::Default,
				id: None,
				name: Expr::Idiom(Idiom::field("a".to_string())),
				strict: false,
				comment: Expr::Literal(Literal::String("test".to_string())),
				changefeed: Some(ChangeFeed {
					expiry: PublicDuration::from_secs(60 * 10),
					store_diff: false,
				}),
			},
		)))),
		TopLevelExpr::Expr(Expr::Define(Box::new(DefineStatement::Database(
			DefineDatabaseStatement {
				kind: DefineKind::Default,
				id: None,
				name: Expr::Idiom(Idiom::field("a".to_string())),
				strict: false,
				comment: Expr::Literal(Literal::None),
				changefeed: None,
			},
		)))),
		TopLevelExpr::Expr(Expr::Define(Box::new(DefineStatement::Function(
			DefineFunctionStatement {
				kind: DefineKind::Default,
				name: "foo::bar".to_owned(),
				args: vec![
					("a".to_owned(), Kind::Number),
					("b".to_owned(), Kind::Array(Box::new(Kind::Bool), Some(3))),
				],
				block: Block(vec![Expr::Return(Box::new(OutputStatement {
					what: ident_field("a"),
					fetch: None,
				}))]),
				comment: Expr::Literal(Literal::String("test".to_string())),
				permissions: Permission::Full,
				returns: None,
			},
		)))),
		TopLevelExpr::Expr(Expr::Define(Box::new(DefineStatement::Access(
			DefineAccessStatement {
				kind: DefineKind::Default,
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Record(RecordAccess {
					signup: None,
					signin: None,
					jwt: JwtAccess {
						verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
							alg: Algorithm::EdDSA,
							key: Expr::Literal(Literal::String("foo".to_string())),
						}),
						issue: None,
					},
					bearer: None,
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
					session: Expr::Literal(Literal::None),
				},
				comment: Expr::Literal(Literal::String("bar".to_string())),
			},
		)))),
		TopLevelExpr::Expr(Expr::Define(Box::new(DefineStatement::Param(DefineParamStatement {
			kind: DefineKind::Default,
			name: "a".to_owned(),
			value: Expr::Literal(Literal::Object(vec![
				ObjectEntry {
					key: "a".to_string(),
					value: Expr::Literal(Literal::Integer(1)),
				},
				ObjectEntry {
					key: "b".to_string(),
					value: Expr::Literal(Literal::Integer(3)),
				},
			])),
			comment: Expr::Literal(Literal::None),
			permissions: Permission::Specific(Expr::Literal(Literal::Null)),
		})))),
		TopLevelExpr::Expr(Expr::Define(Box::new(DefineStatement::Table(DefineTableStatement {
			kind: DefineKind::Default,
			id: None,
			name: Expr::Idiom(Idiom::field("name".to_string())),
			drop: true,
			full: true,
			view: Some(crate::sql::View {
				expr: Fields::Select(vec![Field::Single(Selector {
					expr: ident_field("foo"),
					alias: None,
				})]),
				what: vec!["bar".to_owned()],
				cond: None,
				group: Some(Groups(vec![Group(Idiom(vec![Part::Field("foo".to_owned())]))])),
			}),
			permissions: Permissions {
				select: Permission::Specific(Expr::Binary {
					left: Box::new(ident_field("a")),
					op: BinaryOperator::Equal,
					right: Box::new(Expr::Literal(Literal::Integer(1))),
				}),
				create: Permission::None,
				update: Permission::None,
				delete: Permission::None,
			},
			changefeed: Some(ChangeFeed {
				expiry: PublicDuration::from_secs(1),
				store_diff: false,
			}),
			comment: Expr::Literal(Literal::None),

			table_type: TableType::Normal,
		})))),
		TopLevelExpr::Expr(Expr::Define(Box::new(DefineStatement::Event(DefineEventStatement {
			kind: DefineKind::Default,
			name: Expr::Idiom(Idiom::field("event".to_string())),
			target_table: Expr::Idiom(Idiom::field("table".to_string())),
			when: Expr::Literal(Literal::Null),
			then: vec![Expr::Literal(Literal::Null), Expr::Literal(Literal::None)],
			comment: Expr::Literal(Literal::None),
		})))),
		TopLevelExpr::Expr(Expr::Define(Box::new(DefineStatement::Field(DefineFieldStatement {
			kind: DefineKind::Default,
			name: Expr::Idiom(Idiom(vec![
				Part::Field("foo".to_string()),
				Part::All,
				Part::All,
				Part::Flatten,
			])),
			what: Expr::Idiom(Idiom::field("bar".to_string())),
			field_kind: Some(Kind::Either(vec![
				Kind::None,
				Kind::Number,
				Kind::Array(Box::new(Kind::Record(vec!["foo".to_owned()])), Some(10)),
			])),
			flexible: false,
			readonly: false,
			value: Some(Expr::Literal(Literal::Null)),
			assert: Some(Expr::Literal(Literal::Bool(true))),
			default: DefineDefault::Set(Expr::Literal(Literal::Bool(false))),
			permissions: Permissions {
				delete: Permission::Full,
				update: Permission::None,
				create: Permission::Specific(Expr::Literal(Literal::Bool(true))),
				select: Permission::Full,
			},
			comment: Expr::Literal(Literal::None),
			reference: None,
			computed: None,
		})))),
		TopLevelExpr::Expr(Expr::Define(Box::new(DefineStatement::Index(DefineIndexStatement {
			kind: DefineKind::Default,
			name: Expr::Idiom(Idiom::field("index".to_string())),
			what: Expr::Idiom(Idiom::field("table".to_string())),
			cols: vec![Expr::Idiom(Idiom(vec![Part::Field("a".to_string())]))],
			index: Index::FullText(FullTextParams {
				az: "ana".to_owned(),
				hl: true,
				sc: Scoring::Bm {
					k1: 0.1,
					b: 0.2,
				},
			}),
			comment: Expr::Literal(Literal::None),
			concurrently: false,
		})))),
		TopLevelExpr::Expr(Expr::Define(Box::new(DefineStatement::Index(DefineIndexStatement {
			kind: DefineKind::Default,
			name: Expr::Idiom(Idiom::field("index".to_string())),
			what: Expr::Idiom(Idiom::field("table".to_string())),
			cols: vec![Expr::Idiom(Idiom(vec![Part::Field("a".to_string())]))],
			index: Index::Uniq,
			comment: Expr::Literal(Literal::None),
			concurrently: false,
		})))),
		TopLevelExpr::Expr(Expr::Define(Box::new(DefineStatement::Analyzer(
			DefineAnalyzerStatement {
				kind: DefineKind::Default,
				name: Expr::Idiom(Idiom::field("ana".to_string())),
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
				comment: Expr::Literal(Literal::None),
				function: Some("foo::bar".to_owned()),
			},
		)))),
		TopLevelExpr::Expr(Expr::Delete(Box::new(DeleteStatement {
			only: true,
			what: vec![Expr::Mock(Mock::Range("foo".to_string(), TypedRange::from_range(32..64)))],
			with: Some(With::Index(vec!["index".to_owned(), "index_2".to_owned()])),
			cond: Some(Cond(Expr::Literal(Literal::Integer(2)))),
			output: Some(Output::After),
			timeout: Expr::Literal(Literal::Duration(PublicDuration::from_secs(1))),
			explain: Some(Explain(true)),
		}))),
		TopLevelExpr::Expr(Expr::Delete(Box::new(DeleteStatement {
			only: true,
			what: vec![Expr::Idiom(Idiom(vec![
				Part::Start(Expr::Literal(Literal::RecordId(RecordIdLit {
					table: "a".to_owned(),
					key: RecordIdKeyLit::String("b".to_owned()),
				}))),
				Part::Graph(Lookup {
					kind: LookupKind::Graph(Dir::Out),
					..Default::default()
				}),
				Part::Last,
				Part::Where(Expr::Literal(Literal::Bool(true))),
			]))],
			with: Some(With::Index(vec!["index".to_owned(), "index_2".to_owned()])),
			cond: Some(Cond(Expr::Literal(Literal::Null))),
			output: Some(Output::Null),
			timeout: Expr::Literal(Literal::Duration(PublicDuration::from_secs(60 * 60))),
			explain: Some(Explain(true)),
		}))),
		TopLevelExpr::Expr(Expr::Foreach(Box::new(ForeachStatement {
			param: Param::new("foo".to_owned()),
			range: Expr::Binary {
				left: Box::new(Expr::Select(Box::new(SelectStatement {
					fields: Fields::Select(vec![Field::Single(Selector {
						expr: ident_field("foo"),
						alias: None,
					})]),
					what: vec![Expr::Table("bar".to_string())],
					omit: vec![],
					only: false,
					with: None,
					cond: None,
					split: None,
					group: None,
					order: None,
					limit: None,
					start: None,
					fetch: None,
					version: Expr::Literal(Literal::None),
					timeout: Expr::Literal(Literal::None),
					explain: None,
					tempfiles: false,
				}))),
				op: BinaryOperator::Multiply,
				right: Box::new(Expr::Literal(Literal::Integer(2))),
			},
			block: Block(vec![Expr::Break]),
		}))),
		TopLevelExpr::Expr(Expr::IfElse(Box::new(IfelseStatement {
			exprs: vec![
				(ident_field("foo"), ident_field("bar")),
				(ident_field("faz"), ident_field("baz")),
			],
			close: Some(ident_field("baq")),
		}))),
		TopLevelExpr::Expr(Expr::IfElse(Box::new(IfelseStatement {
			exprs: vec![
				(ident_field("foo"), Expr::Block(Box::new(Block(vec![ident_field("bar")])))),
				(ident_field("faz"), Expr::Block(Box::new(Block(vec![ident_field("baz")])))),
			],
			close: Some(Expr::Block(Box::new(Block(vec![ident_field("baq")])))),
		}))),
		TopLevelExpr::Expr(Expr::Info(Box::new(InfoStatement::Root(false)))),
		TopLevelExpr::Expr(Expr::Info(Box::new(InfoStatement::Ns(false)))),
		TopLevelExpr::Expr(Expr::Info(Box::new(InfoStatement::User(
			Expr::Idiom(Idiom::field("user".to_string())),
			Some(Base::Ns),
			false,
		)))),
		TopLevelExpr::Expr(Expr::Select(Box::new(SelectStatement {
			fields: Fields::Select(vec![
				Field::Single(Selector {
					expr: ident_field("bar"),
					alias: Some(Idiom(vec![Part::Field("foo".to_owned())])),
				}),
				Field::Single(Selector {
					expr: Expr::Literal(Literal::Array(vec![
						Expr::Literal(Literal::Integer(1)),
						Expr::Literal(Literal::Integer(2)),
					])),
					alias: None,
				}),
				Field::Single(Selector {
					expr: ident_field("bar"),
					alias: None,
				}),
			]),
			omit: vec![Expr::Idiom(Idiom(vec![Part::Field("bar".to_string())]))],
			only: true,
			what: vec![Expr::Table("a".to_owned())],
			with: Some(With::Index(vec!["index".to_owned(), "index_2".to_owned()])),
			cond: Some(Cond(Expr::Literal(Literal::Bool(true)))),
			split: None,
			group: Some(Groups(vec![
				Group(Idiom(vec![Part::Field("foo".to_owned())])),
				Group(Idiom(vec![Part::Field("bar".to_owned())])),
			])),
			order: Some(Ordering::Order(OrderList(vec![Order {
				value: Idiom(vec![Part::Field("foo".to_owned())]),
				collate: true,
				numeric: true,
				direction: true,
			}]))),
			limit: Some(Limit(Expr::Literal(Literal::RecordId(RecordIdLit {
				table: "a".to_owned(),
				key: RecordIdKeyLit::String("b".to_owned()),
			})))),
			start: Some(Start(Expr::Literal(Literal::Object(vec![ObjectEntry {
				key: "a".to_owned(),
				value: Expr::Literal(Literal::Bool(true)),
			}])))),
			fetch: Some(Fetchs(vec![Fetch(ident_field("foo"))])),
			version: Expr::Literal(Literal::Datetime(PublicDatetime::from(expected_datetime))),
			timeout: Expr::Literal(Literal::None),
			tempfiles: false,
			explain: Some(Explain(true)),
		}))),
		TopLevelExpr::Expr(Expr::Select(Box::new(SelectStatement {
			fields: Fields::Select(vec![
				Field::Single(Selector {
					expr: ident_field("foo"),
					alias: None,
				}),
				Field::Single(Selector {
					expr: ident_field("bar"),
					alias: None,
				}),
			]),
			omit: vec![],
			only: false,
			what: vec![Expr::Table("a".to_owned())],
			with: None,
			cond: None,
			split: Some(Splits(vec![
				Split(Idiom(vec![Part::Field("foo".to_owned())])),
				Split(Idiom(vec![Part::Field("bar".to_owned())])),
			])),
			group: None,
			order: None,
			limit: None,
			start: None,
			fetch: None,
			version: Expr::Literal(Literal::None),
			timeout: Expr::Literal(Literal::None),
			tempfiles: false,
			explain: None,
		}))),
		TopLevelExpr::Expr(Expr::Let(Box::new(SetStatement {
			name: "param".to_owned(),
			what: Expr::Literal(Literal::Integer(1)),
			kind: None,
		}))),
		TopLevelExpr::Show(ShowStatement {
			table: Some("foo".to_owned()),
			since: ShowSince::Versionstamp(1),
			limit: Some(10),
		}),
		TopLevelExpr::Show(ShowStatement {
			table: None,
			since: ShowSince::Timestamp(PublicDatetime::from(expected_datetime)),
			limit: None,
		}),
		TopLevelExpr::Expr(Expr::Sleep(Box::new(SleepStatement {
			duration: PublicDuration::from_secs(1),
		}))),
		TopLevelExpr::Expr(Expr::Throw(Box::new(Expr::Literal(Literal::Duration(
			PublicDuration::from_secs(1),
		))))),
		TopLevelExpr::Expr(Expr::Insert(Box::new(InsertStatement {
			into: Some(Expr::Param(Param::new("foo".to_owned()))),
			data: Data::ValuesExpression(vec![
				vec![
					(Idiom(vec![Part::Field("a".to_owned())]), Expr::Literal(Literal::Integer(1))),
					(Idiom(vec![Part::Field("b".to_owned())]), Expr::Literal(Literal::Integer(2))),
					(Idiom(vec![Part::Field("c".to_owned())]), Expr::Literal(Literal::Integer(3))),
				],
				vec![
					(Idiom(vec![Part::Field("a".to_owned())]), Expr::Literal(Literal::Integer(4))),
					(Idiom(vec![Part::Field("b".to_owned())]), Expr::Literal(Literal::Integer(5))),
					(Idiom(vec![Part::Field("c".to_owned())]), Expr::Literal(Literal::Integer(6))),
				],
			]),
			ignore: true,
			update: Some(Data::UpdateExpression(vec![
				Assignment {
					place: Idiom(vec![Part::Field("a".to_owned()), Part::Field("b".to_owned())]),
					operator: crate::sql::AssignOperator::Extend,
					value: Expr::Literal(Literal::Null),
				},
				Assignment {
					place: Idiom(vec![Part::Field("c".to_owned()), Part::Field("d".to_owned())]),
					operator: crate::sql::AssignOperator::Add,
					value: Expr::Literal(Literal::None),
				},
			])),
			output: Some(Output::After),
			version: Expr::Literal(Literal::None),
			timeout: Expr::Literal(Literal::None),
			relation: false,
		}))),
		TopLevelExpr::Kill(KillStatement {
			id: Expr::Literal(Literal::Uuid(PublicUuid::from(uuid::uuid!(
				"e72bee20-f49b-11ec-b939-0242ac120002"
			)))),
		}),
		TopLevelExpr::Expr(Expr::Return(Box::new(OutputStatement {
			what: ident_field("RETRUN"),
			fetch: Some(Fetchs(vec![Fetch(ident_field("RETURN"))])),
		}))),
		TopLevelExpr::Expr(Expr::Relate(Box::new(RelateStatement {
			only: true,
			through: Expr::Literal(Literal::RecordId(RecordIdLit {
				table: "a".to_owned(),
				key: RecordIdKeyLit::String("b".to_owned()),
			})),
			from: Expr::Literal(Literal::Array(vec![
				Expr::Literal(Literal::Integer(1)),
				Expr::Literal(Literal::Integer(2)),
			])),
			to: Expr::Create(Box::new(CreateStatement {
				only: false,
				what: vec![Expr::Table("foo".to_owned())],
				data: None,
				output: None,
				timeout: Expr::Literal(Literal::None),
				version: Expr::Literal(Literal::None),
			})),
			uniq: true,
			data: Some(Data::SetExpression(vec![Assignment {
				place: Idiom(vec![Part::Field("a".to_owned())]),
				operator: AssignOperator::Add,
				value: Expr::Literal(Literal::Integer(1)),
			}])),
			output: Some(Output::None),
			timeout: Expr::Literal(Literal::None),
		}))),
		TopLevelExpr::Expr(Expr::Remove(Box::new(RemoveStatement::Function(
			RemoveFunctionStatement {
				name: "foo::bar".to_owned(),
				if_exists: false,
			},
		)))),
		TopLevelExpr::Expr(Expr::Remove(Box::new(RemoveStatement::Field(RemoveFieldStatement {
			name: Expr::Idiom(Idiom(vec![
				Part::Field("foo".to_string()),
				Part::Field("bar".to_string()),
				Part::Value(Expr::Literal(Literal::Integer(10))),
			])),
			what: Expr::Idiom(Idiom(vec![Part::Field("bar".to_string())])),
			if_exists: false,
		})))),
		TopLevelExpr::Expr(Expr::Update(Box::new(UpdateStatement {
			only: true,
			what: vec![Expr::Idiom(Idiom(vec![
				Part::Field("a".to_owned()),
				Part::Graph(Lookup {
					kind: LookupKind::Graph(Dir::Out),
					what: vec![LookupSubject::Table {
						table: "b".to_owned(),
						referencing_field: None,
					}],
					..Default::default()
				}),
			]))],
			with: Some(With::Index(vec!["index".to_owned(), "index_2".to_owned()])),
			cond: Some(Cond(Expr::Literal(Literal::Bool(true)))),
			data: Some(Data::UnsetExpression(vec![
				Idiom(vec![Part::Field("foo".to_owned()), Part::Flatten]),
				Idiom(vec![
					Part::Field("a".to_owned()),
					Part::Graph(Lookup {
						kind: LookupKind::Graph(Dir::Out),
						what: vec![LookupSubject::Table {
							table: "b".to_owned(),
							referencing_field: None,
						}],
						..Default::default()
					}),
				]),
				Idiom(vec![Part::Field("c".to_owned()), Part::All]),
			])),
			output: Some(Output::Diff),
			timeout: Expr::Literal(Literal::Duration(PublicDuration::from_secs(1))),
			explain: Some(Explain(true)),
		}))),
		TopLevelExpr::Expr(Expr::Upsert(Box::new(UpsertStatement {
			only: true,
			what: vec![Expr::Idiom(Idiom(vec![
				Part::Field("a".to_owned()),
				Part::Graph(Lookup {
					kind: LookupKind::Graph(Dir::Out),
					what: vec![LookupSubject::Table {
						table: "b".to_owned(),
						referencing_field: None,
					}],
					..Default::default()
				}),
			]))],
			with: Some(With::Index(vec!["index".to_owned(), "index_2".to_owned()])),
			cond: Some(Cond(Expr::Literal(Literal::Bool(true)))),
			data: Some(Data::UnsetExpression(vec![
				Idiom(vec![Part::Field("foo".to_owned()), Part::Flatten]),
				Idiom(vec![
					Part::Field("a".to_owned()),
					Part::Graph(Lookup {
						kind: LookupKind::Graph(Dir::Out),
						what: vec![LookupSubject::Table {
							table: "b".to_owned(),
							referencing_field: None,
						}],
						..Default::default()
					}),
				]),
				Idiom(vec![Part::Field("c".to_owned()), Part::All]),
			])),
			output: Some(Output::Diff),
			timeout: Expr::Literal(Literal::Duration(PublicDuration::from_secs(1))),
			explain: Some(Explain(true)),
		}))),
		TopLevelExpr::Expr(Expr::FunctionCall(Box::new(FunctionCall {
			receiver: Function::Script(Script(" ((1 + 1)) ".to_owned())),
			arguments: Vec::new(),
		}))),
		TopLevelExpr::Expr(Expr::Literal(Literal::String("a b c d e f g h".to_owned()))),
		TopLevelExpr::Expr(Expr::Literal(Literal::Uuid(PublicUuid::from(uuid::Uuid::from_u128(
			0xffffffff_ffff_ffff_ffff_ffffffffffff,
		))))),
		TopLevelExpr::Expr(Expr::Literal(Literal::RecordId(RecordIdLit {
			table: "a".to_string(),
			key: RecordIdKeyLit::Array(
				[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
					.iter()
					.copied()
					.map(|x| Expr::Literal(Literal::Integer(x)))
					.collect(),
			),
		}))),
		TopLevelExpr::Expr(Expr::Literal(Literal::Regex("a b c d e f".parse().unwrap()))),
		TopLevelExpr::Expr(Expr::Literal(Literal::Float(-123.456e10))),
	]
}

#[test]
fn test_streaming() {
	let expected = statements();
	let mut statements = StatementStream::new();
	let mut buffer = BytesMut::new();
	let mut current_stmt = 0;

	for b in SOURCE.as_bytes() {
		match statements.parse_partial(&mut buffer) {
			Ok(Some(value)) => {
				assert_eq!(value, expected[current_stmt]);
				current_stmt += 1;
			}
			Ok(None) => {}
			Err(e) => {
				panic!(
					"Streaming test returned an error: {}\n\n buffer was {}",
					e,
					String::from_utf8_lossy(&buffer)
				)
			}
		}

		buffer.extend_from_slice(&[*b]);
	}

	loop {
		match statements.parse_complete(&mut buffer) {
			Ok(None) => break,
			Ok(Some(value)) => {
				assert_eq!(value, expected[current_stmt]);
				current_stmt += 1;
			}
			Err(e) => {
				panic!("Streaming test returned an error: {}", e)
			}
		}
	}

	if expected.len() != current_stmt {
		panic!("Not all statements parsed")
	}
}
