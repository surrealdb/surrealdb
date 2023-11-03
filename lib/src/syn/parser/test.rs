use crate::{
	sql::{
		block::Entry,
		changefeed::ChangeFeed,
		statements::{
			analyze::AnalyzeStatement, BeginStatement, BreakStatement, CancelStatement,
			CommitStatement, ContinueStatement, CreateStatement, DefineDatabaseStatement,
			DefineEventStatement, DefineFunctionStatement, DefineNamespaceStatement,
			DefineParamStatement, DefineStatement, DefineTableStatement, DefineTokenStatement,
			DefineUserStatement, DeleteStatement, OutputStatement, UpdateStatement,
		},
		Algorithm, Base, Block, Cond, Data, Dir, Duration, Field, Fields, Future, Graph, Group,
		Groups, Ident, Idiom, Kind, Number, Object, Operator, Output, Part, Permission,
		Permissions, Statement, Strand, Table, Tables, Timeout, Value, Values,
	},
	syn::parser::mac::test_parse,
};

#[test]
pub fn parse_analyze() {
	let res = test_parse!(parse_stmt, r#"ANALYZE INDEX a on b"#).unwrap();
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
		parse_create_stmt,
		"CREATE ONLY foo SET bar = 3, foo +?= 4 RETURN VALUE foo AS bar TIMEOUT 1s PARALLEL"
	)
	.unwrap();
	assert_eq!(
		res,
		CreateStatement {
			only: true,
			what: Values(vec![Value::Table(Table("foo".to_string()))]),
			data: Some(Data::SetExpression(vec![
				(
					Idiom(vec![Part::Field(Ident("bar".to_string()))]),
					Operator::Equal,
					Value::Number(Number::Int(3)),
				),
				(
					Idiom(vec![Part::Field(Ident("foo".to_string()))]),
					Operator::Ext,
					Value::Number(Number::Int(4)),
				),
			])),
			output: Some(Output::Fields(Fields(
				vec![Field::Single {
					expr: Value::Idiom(Idiom(vec![Part::Field(Ident("foo".to_string()))])),
					alias: Some(Idiom(vec![Part::Field(Ident("bar".to_string()))])),
				}],
				true,
			))),
			timeout: Some(Timeout(Duration(std::time::Duration::from_secs(1)))),
			parallel: true,
		}
	);
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
fn parse_define_namespace() {
	let res = test_parse!(parse_stmt, "DEFINE NAMESPACE a COMMENT 'test'").unwrap();
	assert_eq!(
		res,
		Statement::Define(DefineStatement::Namespace(DefineNamespaceStatement {
			id: None,
			name: Ident("a".to_string()),
			comment: Some(Strand("test".to_string()))
		}))
	);

	let res = test_parse!(parse_stmt, "DEFINE NS a").unwrap();
	assert_eq!(
		res,
		Statement::Define(DefineStatement::Namespace(DefineNamespaceStatement {
			id: None,
			name: Ident("a".to_string()),
			comment: None
		}))
	)
}

#[test]
fn parse_define_database() {
	let res = test_parse!(parse_stmt, "DEFINE DATABASE a COMMENT 'test' CHANGEFEED 10m").unwrap();
	assert_eq!(
		res,
		Statement::Define(DefineStatement::Database(DefineDatabaseStatement {
			id: None,
			name: Ident("a".to_string()),
			comment: Some(Strand("test".to_string())),
			changefeed: Some(ChangeFeed {
				expiry: std::time::Duration::from_secs(60) * 10
			})
		}))
	);

	let res = test_parse!(parse_stmt, "DEFINE DB a").unwrap();
	assert_eq!(
		res,
		Statement::Define(DefineStatement::Database(DefineDatabaseStatement {
			id: None,
			name: Ident("a".to_string()),
			comment: None,
			changefeed: None
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
				what: Value::Table(Table("a".to_string())),
				fetch: None,
			})]),
			comment: Some(Strand("test".to_string())),
			permissions: Permission::Full,
		}))
	)
}

#[test]
fn parse_define_user() {
	let res = test_parse!(
		parse_stmt,
		r#"DEFINE USER user ON ROOT COMMENT 'test' PASSWORD 'hunter2' PASSHASH 'r4' ROLES foo, bar COMMENT "*******""#
	)
	.unwrap();

	assert_eq!(
		res,
		Statement::Define(DefineStatement::User(DefineUserStatement {
			name: Ident("user".to_string()),
			base: Base::Root,
			hash: "r4".to_string(),
			code: "hunter2".to_string(),
			roles: vec![Ident("foo".to_string()), Ident("bar".to_string())],
			comment: Some(Strand("*******".to_string()))
		}))
	)
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
			comment: Some(Strand("bar".to_string()))
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
			permissions: Permission::Specific(Value::Null)
		}))
	);
}

#[test]
fn parse_define_table() {
	let res =
		test_parse!(parse_stmt, r#"DEFINE TABLE name DROP SCHEMAFUL CHANGEFEED 1s PERMISSIONS FOR SELECT WHERE a = 1 AS SELECT foo FROM bar GROUP BY foo"#)
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
				group: Some(Groups(
					vec![Group(Idiom(vec![Part::Field(Ident("foo".to_owned()))])),]
				)),
			}),
			permissions: Permissions {
				select: Permission::Specific(Value::Expression(Box::new(
					crate::sql::Expression::Binary {
						l: Value::Idiom(Idiom(vec![Part::Field(Ident("a".to_owned()))])),
						o: Operator::Equal,
						r: Value::Number(Number::Int(1))
					}
				))),
				create: Permission::Full,
				update: Permission::Full,
				delete: Permission::Full,
			},
			changefeed: Some(ChangeFeed {
				expiry: std::time::Duration::from_secs(1)
			}),
			comment: None,
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
		}))
	)
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
				))),])))),
				Value::Idiom(Idiom(vec![
					Part::Field(Ident("a".to_string())),
					Part::Graph(Graph {
						dir: Dir::Out,
						what: Tables(vec![Table("b".to_string())]),
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
						..Default::default()
					})
				]),
				Idiom(vec![Part::Field(Ident("c".to_string())), Part::All,])
			])),
			output: Some(Output::Diff),
			timeout: Some(Timeout(Duration(std::time::Duration::from_secs(1)))),
			parallel: true,
		})
	);
}
