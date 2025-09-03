use chrono::offset::TimeZone;
use chrono::{NaiveDate, Offset, Utc};

use crate::sql::access::AccessDuration;
use crate::sql::access_type::{
	AccessType, BearerAccess, BearerAccessSubject, BearerAccessType, JwtAccess, JwtAccessIssue,
	JwtAccessVerify, JwtAccessVerifyJwks, JwtAccessVerifyKey, RecordAccess,
};
use crate::sql::changefeed::ChangeFeed;
use crate::sql::data::Assignment;
use crate::sql::filter::Filter;
use crate::sql::index::{Distance, HnswParams, MTreeParams, SearchParams, VectorType};
use crate::sql::language::Language;
use crate::sql::literal::ObjectEntry;
use crate::sql::lookup::{LookupKind, LookupSubject};
use crate::sql::order::{OrderList, Ordering};
use crate::sql::statements::access::{
	self, AccessStatementGrant, AccessStatementPurge, AccessStatementRevoke, AccessStatementShow,
};
use crate::sql::statements::analyze::AnalyzeStatement;
use crate::sql::statements::define::user::PassType;
use crate::sql::statements::define::{DefineDefault, DefineKind};
use crate::sql::statements::show::{ShowSince, ShowStatement};
use crate::sql::statements::sleep::SleepStatement;
use crate::sql::statements::{
	AccessStatement, CreateStatement, DefineAccessStatement, DefineAnalyzerStatement,
	DefineDatabaseStatement, DefineEventStatement, DefineFieldStatement, DefineFunctionStatement,
	DefineIndexStatement, DefineNamespaceStatement, DefineParamStatement, DefineStatement,
	DefineTableStatement, DeleteStatement, ForeachStatement, IfelseStatement, InfoStatement,
	InsertStatement, KillStatement, OptionStatement, OutputStatement, RelateStatement,
	RemoveAccessStatement, RemoveAnalyzerStatement, RemoveDatabaseStatement, RemoveEventStatement,
	RemoveFieldStatement, RemoveFunctionStatement, RemoveIndexStatement, RemoveNamespaceStatement,
	RemoveParamStatement, RemoveStatement, RemoveTableStatement, RemoveUserStatement,
	SelectStatement, UpdateStatement, UpsertStatement, UseStatement,
};
use crate::sql::tokenizer::Tokenizer;
use crate::sql::{
	Algorithm, AssignOperator, Base, BinaryOperator, Block, Cond, Data, Dir, Explain, Expr, Fetch,
	Fetchs, Field, Fields, Group, Groups, Ident, Idiom, Idioms, Index, Kind, Limit, Literal,
	Lookup, Mock, Order, Output, Param, Part, Permission, Permissions, RecordIdKeyLit, RecordIdLit,
	Scoring, Split, Splits, Start, TableType, Timeout, TopLevelExpr, With,
};
use crate::syn;
use crate::syn::parser::ParserSettings;
use crate::val::{Datetime, Duration, Number, Strand, Uuid};

fn ident_field(name: &str) -> Expr {
	Expr::Idiom(Idiom(vec![Part::Field(Ident::new(name.to_string()).unwrap())]))
}

#[test]
pub fn parse_analyze() {
	let mut res = syn::parse_with(r#"ANALYZE INDEX b on a"#.as_bytes(), async |parser, stk| {
		parser.parse_query(stk).await
	})
	.unwrap();
	let res = res.expressions.pop().unwrap();
	assert_eq!(
		res,
		TopLevelExpr::Analyze(AnalyzeStatement::Idx(
			Ident::from_strand(strand!("a").to_owned()),
			Ident::from_strand(strand!("b").to_owned())
		))
	)
}

#[test]
pub fn parse_begin() {
	let res =
		syn::parse_with(r#"BEGIN;"#.as_bytes(), async |parser, stk| parser.parse_query(stk).await)
			.unwrap()
			.expressions
			.pop()
			.unwrap();
	assert_eq!(res, TopLevelExpr::Begin);
	let res = syn::parse_with(r#"BEGIN TRANSACTION;"#.as_bytes(), async |parser, stk| {
		parser.parse_query(stk).await
	})
	.unwrap()
	.expressions
	.pop()
	.unwrap();
	assert_eq!(res, TopLevelExpr::Begin);
}

#[test]
pub fn parse_break() {
	let res = syn::parse_with(r#"BREAK"#.as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(res, Expr::Break)
}

#[test]
pub fn parse_cancel() {
	let res = syn::parse_with(r#"CANCEL"#.as_bytes(), async |parser, stk| {
		parser.parse_top_level_expr(stk).await
	})
	.unwrap();
	assert_eq!(res, TopLevelExpr::Cancel);
	let res = syn::parse_with(r#"CANCEL TRANSACTION"#.as_bytes(), async |parser, stk| {
		parser.parse_top_level_expr(stk).await
	})
	.unwrap();
	assert_eq!(res, TopLevelExpr::Cancel);
}

#[test]
pub fn parse_commit() {
	let res = syn::parse_with(r#"COMMIT"#.as_bytes(), async |parser, stk| {
		parser.parse_top_level_expr(stk).await
	})
	.unwrap();
	assert_eq!(res, TopLevelExpr::Commit);
	let res = syn::parse_with(r#"COMMIT TRANSACTION"#.as_bytes(), async |parser, stk| {
		parser.parse_top_level_expr(stk).await
	})
	.unwrap();
	assert_eq!(res, TopLevelExpr::Commit);
}

#[test]
pub fn parse_continue() {
	let res = syn::parse_with(r#"CONTINUE"#.as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(res, Expr::Continue);
}

#[test]
fn parse_create() {
	let res = syn::parse_with(
		"CREATE ONLY foo SET bar = 3, foo +?= baz RETURN VALUE foo AS bar TIMEOUT 1s PARALLEL"
			.as_bytes(),
		async |parser, stk| parser.parse_expr_inherit(stk).await,
	)
	.unwrap();

	assert_eq!(
		res,
		Expr::Create(Box::new(CreateStatement {
			only: true,
			what: vec![Expr::Table(Ident::from_strand(strand!("foo").to_owned()))],
			data: Some(Data::SetExpression(vec![
				Assignment {
					place: Idiom(vec![Part::Field(Ident::from_strand(strand!("bar").to_owned()))]),
					operator: AssignOperator::Assign,
					value: Expr::Literal(Literal::Integer(3))
				},
				Assignment {
					place: Idiom(vec![Part::Field(Ident::from_strand(strand!("foo").to_owned()))]),
					operator: AssignOperator::Extend,
					value: ident_field("baz")
				},
			])),
			output: Some(Output::Fields(Fields::Value(Box::new(Field::Single {
				expr: ident_field("foo"),
				alias: Some(Idiom(vec![Part::Field(Ident::new("bar".to_string()).unwrap())])),
			})))),
			timeout: Some(Timeout(Duration(std::time::Duration::from_secs(1)))),
			parallel: true,
			version: None,
		})),
	);
}

#[test]
fn parse_define_namespace() {
	let res =
		syn::parse_with("DEFINE NAMESPACE a COMMENT 'test'".as_bytes(), async |parser, stk| {
			parser.parse_expr_inherit(stk).await
		})
		.unwrap();
	assert_eq!(
		res,
		Expr::Define(Box::new(DefineStatement::Namespace(DefineNamespaceStatement {
			kind: DefineKind::Default,
			id: None,
			name: Ident::from_strand(strand!("a").to_owned()),
			comment: Some(Strand::new("test".to_string()).unwrap()),
		})))
	);

	let res = syn::parse_with("DEFINE NS a".as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		Expr::Define(Box::new(DefineStatement::Namespace(DefineNamespaceStatement {
			kind: DefineKind::Default,
			id: None,
			name: Ident::from_strand(strand!("a").to_owned()),
			comment: None,
		})))
	)
}

#[test]
fn parse_define_database() {
	let res = syn::parse_with(
		"DEFINE DATABASE a COMMENT 'test' CHANGEFEED 10m INCLUDE ORIGINAL".as_bytes(),
		async |parser, stk| parser.parse_expr_inherit(stk).await,
	)
	.unwrap();
	assert_eq!(
		res,
		Expr::Define(Box::new(DefineStatement::Database(DefineDatabaseStatement {
			kind: DefineKind::Default,
			id: None,
			name: Ident::from_strand(strand!("a").to_owned()),
			comment: Some(strand!("test").to_owned()),
			changefeed: Some(ChangeFeed {
				expiry: std::time::Duration::from_secs(60) * 10,
				store_diff: true,
			}),
		})))
	);

	let res = syn::parse_with("DEFINE DB a".as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		Expr::Define(Box::new(DefineStatement::Database(DefineDatabaseStatement {
			kind: DefineKind::Default,
			id: None,
			name: Ident::from_strand(strand!("a").to_owned()),
			comment: None,
			changefeed: None,
		})))
	)
}

#[test]
fn parse_define_function() {
	let res = syn::parse_with(
		r#"DEFINE FUNCTION fn::foo::bar($a: number, $b: array<bool,3>) {
			RETURN a
		} COMMENT 'test' PERMISSIONS FULL
		"#
		.as_bytes(),
		async |parser, stk| parser.parse_expr_inherit(stk).await,
	)
	.unwrap();

	assert_eq!(
		res,
		Expr::Define(Box::new(DefineStatement::Function(DefineFunctionStatement {
			kind: DefineKind::Default,
			name: Ident::from_strand(strand!("foo::bar").to_owned()),
			args: vec![
				(Ident::from_strand(strand!("a").to_owned()), Kind::Number),
				(
					Ident::from_strand(strand!("b").to_owned()),
					Kind::Array(Box::new(Kind::Bool), Some(3))
				)
			],
			block: Block(vec![Expr::Return(Box::new(OutputStatement {
				what: ident_field("a"),
				fetch: None,
			}))]),
			comment: Some(strand!("test").to_owned()),
			permissions: Permission::Full,
			returns: None,
		})))
	)
}

#[test]
fn parse_define_user() {
	// Password.
	{
		let res = syn::parse_with(
			r#"DEFINE USER user ON ROOT COMMENT 'test' PASSWORD 'hunter2' COMMENT "*******""#
				.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap();

		let Expr::Define(res) = res else {
			panic!()
		};
		let DefineStatement::User(stmt) = *res else {
			panic!()
		};

		assert_eq!(stmt.name, Ident::from_strand(strand!("user").to_owned()));
		assert_eq!(stmt.base, Base::Root);
		assert_eq!(stmt.pass_type, PassType::Password("hunter2".to_owned()));
		assert_eq!(stmt.roles, vec![Ident::from_strand(strand!("Viewer").to_owned())]);
		assert_eq!(stmt.comment, Some(strand!("*******").to_owned()));
		assert_eq!(stmt.token_duration, Some(Duration::from_hours(1).unwrap()));
		assert_eq!(stmt.session_duration, None);
	}
	// Passhash.
	{
		let res = syn::parse_with(
			r#"DEFINE USER user ON ROOT COMMENT 'test' PASSHASH 'hunter2' COMMENT "*******""#
				.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap();

		let Expr::Define(res) = res else {
			panic!()
		};
		let DefineStatement::User(stmt) = *res else {
			panic!()
		};

		assert_eq!(stmt.name, Ident::from_strand(strand!("user").to_owned()));
		assert_eq!(stmt.base, Base::Root);
		assert_eq!(stmt.pass_type, PassType::Hash("hunter2".to_owned()));
		assert_eq!(stmt.roles, vec![Ident::from_strand(strand!("Viewer").to_owned())]);
		assert_eq!(stmt.comment, Some(strand!("*******").to_owned()));
		assert_eq!(stmt.token_duration, Some(Duration::from_hours(1).unwrap()));
		assert_eq!(stmt.session_duration, None);
	}
	// With roles.
	{
		let res = syn::parse_with(
			r#"DEFINE USER user ON ROOT COMMENT 'test' PASSHASH 'hunter2' ROLES editor, OWNER"#
				.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap();

		let Expr::Define(res) = res else {
			panic!()
		};
		let DefineStatement::User(stmt) = *res else {
			panic!()
		};

		assert_eq!(stmt.name, Ident::from_strand(strand!("user").to_owned()));
		assert_eq!(stmt.base, Base::Root);
		assert_eq!(stmt.pass_type, PassType::Hash("hunter2".to_owned()));
		assert_eq!(
			stmt.roles,
			vec![
				Ident::from_strand(strand!("editor").to_owned()),
				Ident::from_strand(strand!("OWNER").to_owned())
			]
		);
		assert_eq!(stmt.token_duration, Some(Duration::from_hours(1).unwrap()));
		assert_eq!(stmt.session_duration, None);
	}
	// With session duration.
	{
		let res = syn::parse_with(
			r#"DEFINE USER user ON ROOT COMMENT 'test' PASSHASH 'hunter2' DURATION FOR SESSION 6h"#
				.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap();

		let Expr::Define(res) = res else {
			panic!()
		};
		let DefineStatement::User(stmt) = *res else {
			panic!()
		};

		assert_eq!(stmt.name, Ident::from_strand(strand!("user").to_owned()));
		assert_eq!(stmt.base, Base::Root);
		assert_eq!(stmt.pass_type, PassType::Hash("hunter2".to_owned()));
		assert_eq!(stmt.roles, vec![Ident::from_strand(strand!("Viewer").to_owned())]);
		assert_eq!(stmt.token_duration, Some(Duration::from_hours(1).unwrap()));
		assert_eq!(stmt.session_duration, Some(Duration::from_hours(6).unwrap()));
	}
	// With session and token duration.
	{
		let res = syn::parse_with(
			r#"DEFINE USER user ON ROOT COMMENT 'test' PASSHASH 'hunter2' DURATION FOR TOKEN 15m, FOR SESSION 6h"#.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap();

		let Expr::Define(res) = res else {
			panic!()
		};
		let DefineStatement::User(stmt) = *res else {
			panic!()
		};

		assert_eq!(stmt.name, Ident::from_strand(strand!("user").to_owned()));
		assert_eq!(stmt.base, Base::Root);
		assert_eq!(stmt.pass_type, PassType::Hash("hunter2".to_owned()));
		assert_eq!(stmt.roles, vec![Ident::from_strand(strand!("Viewer").to_owned())]);
		assert_eq!(stmt.token_duration, Some(Duration::from_mins(15).unwrap()));
		assert_eq!(stmt.session_duration, Some(Duration::from_hours(6).unwrap()));
	}
	// With none token duration.
	{
		syn::parse_with(
			r#"DEFINE USER user ON ROOT COMMENT 'test' PASSHASH 'hunter2' DURATION FOR TOKEN NONE"#
				.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap_err();
	}
	// With nonexistent role.
	{
		syn::parse_with(
			r#"DEFINE USER user ON ROOT COMMENT 'test' PASSHASH 'hunter2' ROLES foo"#.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap_err();
	}
	// With existent and nonexistent roles.
	{
		syn::parse_with(
			r#"DEFINE USER user ON ROOT COMMENT 'test' PASSHASH 'hunter2' ROLES Viewer, foo"#
				.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap_err();
	}
}

#[test]
fn parse_define_access_jwt_key() {
	// With comment. Asymmetric verify only.
	{
		let res = syn::parse_with(
			r#"DEFINE ACCESS a ON DATABASE TYPE JWT ALGORITHM EDDSA KEY "foo" COMMENT "bar""#
				.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::EdDSA,
						key: "foo".to_string(),
					}),
					issue: None,
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Some(Duration::from_days(30).unwrap()),
					token: Some(Duration::from_hours(1).unwrap()),
					session: None,
				},
				comment: Some(strand!("bar").to_owned()),
			}))),
		)
	}
	// Asymmetric verify and issue.
	{
		let res = syn::parse_with( r#"DEFINE ACCESS a ON DATABASE TYPE JWT ALGORITHM EDDSA KEY "foo" WITH ISSUER KEY "bar""#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::EdDSA,
						key: "foo".to_string(),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::EdDSA,
						key: "bar".to_string(),
					}),
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Some(Duration::from_days(30).unwrap()),
					token: Some(Duration::from_hours(1).unwrap()),
					session: None,
				},
				comment: None,
			}))),
		)
	}
	// Asymmetric verify and issue with authenticate clause.
	{
		let res = syn::parse_with( r#"DEFINE ACCESS a ON DATABASE TYPE JWT ALGORITHM EDDSA KEY "foo" WITH ISSUER KEY "bar" AUTHENTICATE true"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::EdDSA,
						key: "foo".to_string(),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::EdDSA,
						key: "bar".to_string(),
					}),
				}),
				authenticate: Some(Expr::Literal(Literal::Bool(true))),
				// Default durations.
				duration: AccessDuration {
					grant: Some(Duration::from_days(30).unwrap()),
					token: Some(Duration::from_hours(1).unwrap()),
					session: None,
				},
				comment: None,
			}))),
		)
	}
	// Symmetric verify and implicit issue.
	{
		let res = syn::parse_with(
			r#"DEFINE ACCESS a ON DATABASE TYPE JWT ALGORITHM HS256 KEY "foo""#.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::Hs256,
						key: "foo".to_string(),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Hs256,
						key: "foo".to_string(),
					}),
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Some(Duration::from_days(30).unwrap()),
					token: Some(Duration::from_hours(1).unwrap()),
					session: None,
				},
				comment: None,
			}))),
		)
	}
	// Symmetric verify and explicit duration.
	{
		let res = syn::parse_with( r#"DEFINE ACCESS a ON DATABASE TYPE JWT ALGORITHM HS256 KEY "foo" DURATION FOR TOKEN 10s"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::Hs256,
						key: "foo".to_string(),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Hs256,
						key: "foo".to_string(),
					}),
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Some(Duration::from_days(30).unwrap()),
					token: Some(Duration::from_secs(10)),
					session: None,
				},
				comment: None,
			}))),
		)
	}
	// Symmetric verify and explicit issue matching data.
	{
		let res = syn::parse_with( r#"DEFINE ACCESS a ON DATABASE TYPE JWT ALGORITHM HS256 KEY "foo" WITH ISSUER ALGORITHM HS256 KEY "foo""#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::Hs256,
						key: "foo".to_string(),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Hs256,
						key: "foo".to_string(),
					}),
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Some(Duration::from_days(30).unwrap()),
					token: Some(Duration::from_hours(1).unwrap()),
					session: None,
				},
				comment: None,
			}))),
		)
	}
	// Symmetric verify and explicit issue non-matching data.
	{
		syn::parse_with(r#"DEFINE ACCESS a ON DATABASE TYPE JWT ALGORITHM HS256 KEY "foo" WITH ISSUER ALGORITHM HS384 KEY "bar" DURATION FOR TOKEN 10s"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap_err();
	}
	// Symmetric verify and explicit issue non-matching key.
	{
		syn::parse_with(r#"DEFINE ACCESS a ON DATABASE TYPE JWT ALGORITHM HS256 KEY "foo" WITH ISSUER KEY "bar" DURATION FOR TOKEN 10s"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap_err();
	}
	// Symmetric verify and explicit issue non-matching algorithm.
	{
		syn::parse_with(r#"DEFINE ACCESS a ON DATABASE TYPE JWT ALGORITHM HS256 KEY "foo" WITH ISSUER ALGORITHM HS384 DURATION FOR TOKEN 10s"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap_err();
	}
	// Symmetric verify and token duration is none.
	{
		syn::parse_with(
			r#"DEFINE ACCESS a ON DATABASE TYPE JWT ALGORITHM HS256 KEY "foo" DURATION FOR TOKEN NONE"#.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap_err();
	}
	// With comment. Asymmetric verify only. On namespace level.
	{
		let res = syn::parse_with(
			r#"DEFINE ACCESS a ON NAMESPACE TYPE JWT ALGORITHM EDDSA KEY "foo" COMMENT "bar""#
				.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Ns,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::EdDSA,
						key: "foo".to_string(),
					}),
					issue: None,
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Some(Duration::from_days(30).unwrap()),
					token: Some(Duration::from_hours(1).unwrap()),
					session: None,
				},
				comment: Some(strand!("bar").to_owned()),
			}))),
		)
	}
	// With comment. Asymmetric verify only. On root level.
	{
		let res = syn::parse_with(
			r#"DEFINE ACCESS a ON ROOT TYPE JWT ALGORITHM EDDSA KEY "foo" COMMENT "bar""#
				.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Root,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::EdDSA,
						key: "foo".to_string(),
					}),
					issue: None,
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Some(Duration::from_days(30).unwrap()),
					token: Some(Duration::from_hours(1).unwrap()),
					session: None,
				},
				comment: Some(strand!("bar").to_owned()),
			}))),
		)
	}
}

#[test]
fn parse_define_access_jwt_jwks() {
	// With comment. Verify only.
	{
		let res = syn::parse_with( r#"DEFINE ACCESS a ON DATABASE TYPE JWT URL "http://example.com/.well-known/jwks.json" COMMENT "bar""#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Jwks(JwtAccessVerifyJwks {
						url: "http://example.com/.well-known/jwks.json".to_string(),
					}),
					issue: None,
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Some(Duration::from_days(30).unwrap()),
					token: Some(Duration::from_hours(1).unwrap()),
					session: None,
				},
				comment: Some(strand!("bar").to_owned()),
			}))),
		)
	}
	// Verify and symmetric issuer.
	{
		let res = syn::parse_with( r#"DEFINE ACCESS a ON DATABASE TYPE JWT URL "http://example.com/.well-known/jwks.json" WITH ISSUER ALGORITHM HS384 KEY "foo""#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Jwks(JwtAccessVerifyJwks {
						url: "http://example.com/.well-known/jwks.json".to_string(),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Hs384,
						key: "foo".to_string(),
					}),
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Some(Duration::from_days(30).unwrap()),
					token: Some(Duration::from_hours(1).unwrap()),
					session: None,
				},
				comment: None,
			}))),
		)
	}
	// Verify and symmetric issuer with custom duration.
	{
		let res = syn::parse_with( r#"DEFINE ACCESS a ON DATABASE TYPE JWT URL "http://example.com/.well-known/jwks.json" WITH ISSUER ALGORITHM HS384 KEY "foo" DURATION FOR TOKEN 10s"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Jwks(JwtAccessVerifyJwks {
						url: "http://example.com/.well-known/jwks.json".to_string(),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Hs384,
						key: "foo".to_string(),
					}),
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Some(Duration::from_days(30).unwrap()),
					token: Some(Duration::from_secs(10)),
					session: None,
				},
				comment: None,
			}))),
		)
	}
	// Verify and asymmetric issuer.
	{
		let res = syn::parse_with( r#"DEFINE ACCESS a ON DATABASE TYPE JWT URL "http://example.com/.well-known/jwks.json" WITH ISSUER ALGORITHM PS256 KEY "foo""#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Jwks(JwtAccessVerifyJwks {
						url: "http://example.com/.well-known/jwks.json".to_string(),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Ps256,
						key: "foo".to_string(),
					}),
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Some(Duration::from_days(30).unwrap()),
					token: Some(Duration::from_hours(1).unwrap()),
					session: None,
				},
				comment: None,
			}))),
		)
	}
	// Verify and asymmetric issuer with custom duration.
	{
		let res = syn::parse_with(r#"DEFINE ACCESS a ON DATABASE TYPE JWT URL "http://example.com/.well-known/jwks.json" WITH ISSUER ALGORITHM PS256 KEY "foo" DURATION FOR TOKEN 10s, FOR SESSION 2d"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Jwks(JwtAccessVerifyJwks {
						url: "http://example.com/.well-known/jwks.json".to_string(),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Ps256,
						key: "foo".to_string(),
					}),
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Some(Duration::from_days(30).unwrap()),
					token: Some(Duration::from_secs(10)),
					session: Some(Duration::from_days(2).unwrap()),
				},
				comment: None,
			}))),
		)
	}
}

#[test]
fn parse_define_access_record() {
	// With comment. Nothing is explicitly defined.
	{
		let res = syn::parse_with(
			r#"DEFINE ACCESS a ON DB TYPE RECORD COMMENT "bar""#.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap();

		// Manually compare since DefineAccessStatement for record access
		// without explicit JWT will create a random signing key during parsing.
		let Expr::Define(res) = res else {
			panic!()
		};

		let DefineStatement::Access(stmt) = *res else {
			panic!()
		};

		assert_eq!(stmt.name, Ident::from_strand(strand!("a").to_owned()));
		assert_eq!(stmt.base, Base::Db);
		assert_eq!(stmt.authenticate, None);
		assert_eq!(
			stmt.duration,
			// Default durations.
			AccessDuration {
				grant: Some(Duration::from_days(30).unwrap()),
				token: Some(Duration::from_hours(1).unwrap()),
				session: None,
			}
		);
		assert_eq!(stmt.comment, Some(strand!("bar").to_owned()));
		match stmt.access_type {
			AccessType::Record(ac) => {
				assert_eq!(ac.signup, None);
				assert_eq!(ac.signin, None);
				match ac.jwt.verify {
					JwtAccessVerify::Key(key) => {
						assert_eq!(key.alg, Algorithm::Hs512);
					}
					_ => panic!(),
				}
				match ac.jwt.issue {
					Some(iss) => {
						assert_eq!(iss.alg, Algorithm::Hs512);
					}
					_ => panic!(),
				}
			}
			_ => panic!(),
		}
	}
	// With refresh token. Refresh token duration is set to 10 days.
	{
		let res = syn::parse_with_settings(
			r#"DEFINE ACCESS a ON DB TYPE RECORD WITH REFRESH DURATION FOR GRANT 10d"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |p, s| p.parse_expr_inherit(s).await,
		)
		.unwrap();

		// Manually compare since DefineAccessStatement for record access
		// without explicit JWT will create a random signing key during parsing.
		let Expr::Define(res) = res else {
			panic!()
		};

		let DefineStatement::Access(stmt) = *res else {
			panic!()
		};

		assert_eq!(stmt.name, Ident::from_strand(strand!("a").to_owned()));
		assert_eq!(stmt.base, Base::Db);
		assert_eq!(stmt.authenticate, None);
		assert_eq!(
			stmt.duration,
			// Default durations.
			AccessDuration {
				grant: Some(Duration::from_days(10).unwrap()),
				token: Some(Duration::from_hours(1).unwrap()),
				session: None,
			}
		);
		match stmt.access_type {
			AccessType::Record(ac) => {
				assert_eq!(ac.signup, None);
				assert_eq!(ac.signin, None);
				let jwt_verify_key = match ac.jwt.verify {
					JwtAccessVerify::Key(key) => {
						assert_eq!(key.alg, Algorithm::Hs512);
						key.key
					}
					_ => panic!(),
				};
				let jwt_issue_key = match ac.jwt.issue {
					Some(iss) => {
						assert_eq!(iss.alg, Algorithm::Hs512);
						iss.key
					}
					_ => panic!(),
				};
				// The JWT parameters should be the same as record authentication.
				match ac.bearer {
					Some(bearer) => {
						assert_eq!(bearer.kind, BearerAccessType::Refresh);
						assert_eq!(bearer.subject, BearerAccessSubject::Record);
						match bearer.jwt.verify {
							JwtAccessVerify::Key(key) => {
								assert_eq!(key.alg, Algorithm::Hs512);
								assert_eq!(key.key, jwt_verify_key);
							}
							_ => panic!(),
						}
						match bearer.jwt.issue {
							Some(iss) => {
								assert_eq!(iss.alg, Algorithm::Hs512);
								assert_eq!(iss.key, jwt_issue_key);
							}
							_ => panic!(),
						}
					}
					_ => panic!(),
				}
			}
			_ => panic!(),
		}
	}
	// Session duration, signing and authenticate clauses are explicitly defined.
	{
		let res = syn::parse_with(r#"DEFINE ACCESS a ON DB TYPE RECORD SIGNUP true SIGNIN false AUTHENTICATE true DURATION FOR SESSION 7d"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();

		// Manually compare since DefineAccessStatement for record access
		// without explicit JWT will create a random signing key during parsing.
		let Expr::Define(res) = res else {
			panic!()
		};

		let DefineStatement::Access(stmt) = *res else {
			panic!()
		};

		assert_eq!(stmt.name, Ident::from_strand(strand!("a").to_owned()));
		assert_eq!(stmt.base, Base::Db);
		assert_eq!(stmt.authenticate, Some(Expr::Literal(Literal::Bool(true))));
		assert_eq!(
			stmt.duration,
			AccessDuration {
				grant: Some(Duration::from_days(30).unwrap()),
				token: Some(Duration::from_hours(1).unwrap()),
				session: Some(Duration::from_days(7).unwrap()),
			}
		);
		assert_eq!(stmt.comment, None);
		match stmt.access_type {
			AccessType::Record(ac) => {
				assert_eq!(ac.signup, Some(Expr::Literal(Literal::Bool(true))));
				assert_eq!(ac.signin, Some(Expr::Literal(Literal::Bool(false))));
				match ac.jwt.verify {
					JwtAccessVerify::Key(key) => {
						assert_eq!(key.alg, Algorithm::Hs512);
					}
					_ => panic!(),
				}
				match ac.jwt.issue {
					Some(iss) => {
						assert_eq!(iss.alg, Algorithm::Hs512);
					}
					_ => panic!(),
				}
			}
			_ => panic!(),
		}
	}
	// Verification with JWT is explicitly defined only with symmetric key.
	{
		let res = syn::parse_with(r#"DEFINE ACCESS a ON DB TYPE RECORD WITH JWT ALGORITHM HS384 KEY "foo" DURATION FOR TOKEN 10s, FOR SESSION 15m"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Record(RecordAccess {
					signup: None,
					signin: None,
					jwt: JwtAccess {
						verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
							alg: Algorithm::Hs384,
							key: "foo".to_string(),
						}),
						issue: Some(JwtAccessIssue {
							alg: Algorithm::Hs384,
							// Issuer key matches verification key by default in symmetric
							// algorithms.
							key: "foo".to_string(),
						}),
					},
					bearer: None,
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Some(Duration::from_days(30).unwrap()),
					token: Some(Duration::from_secs(10)),
					session: Some(Duration::from_mins(15).unwrap()),
				},
				comment: None,
			}))),
		);
	}
	// Verification and issuing with JWT are explicitly defined with two different
	// keys.
	{
		let res = syn::parse_with(r#"DEFINE ACCESS a ON DB TYPE RECORD WITH JWT ALGORITHM PS512 KEY "foo" WITH ISSUER KEY "bar" DURATION FOR TOKEN 10s, FOR SESSION 15m"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Record(RecordAccess {
					signup: None,
					signin: None,
					jwt: JwtAccess {
						verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
							alg: Algorithm::Ps512,
							key: "foo".to_string(),
						}),
						issue: Some(JwtAccessIssue {
							alg: Algorithm::Ps512,
							key: "bar".to_string(),
						}),
					},
					bearer: None,
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Some(Duration::from_days(30).unwrap()),
					token: Some(Duration::from_secs(10)),
					session: Some(Duration::from_mins(15).unwrap()),
				},
				comment: None,
			}))),
		);
	}
	// Verification and issuing with JWT are explicitly defined with two different
	// keys. Refresh specified before JWT.
	{
		let res = syn::parse_with_settings(
			r#"DEFINE ACCESS a ON DB TYPE RECORD WITH REFRESH WITH JWT ALGORITHM PS512 KEY "foo" WITH ISSUER KEY "bar" DURATION FOR GRANT 10d, FOR TOKEN 10s, FOR SESSION 15m"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |p,s| p.parse_expr_inherit(s).await,
		)
			.unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Record(RecordAccess {
					signup: None,
					signin: None,
					jwt: JwtAccess {
						verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
							alg: Algorithm::Ps512,
							key: "foo".to_string(),
						}),
						issue: Some(JwtAccessIssue {
							alg: Algorithm::Ps512,
							key: "bar".to_string(),
						}),
					},
					bearer: Some(BearerAccess {
						kind: BearerAccessType::Refresh,
						subject: BearerAccessSubject::Record,
						jwt: JwtAccess {
							verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
								alg: Algorithm::Ps512,
								key: "foo".to_string(),
							}),
							issue: Some(JwtAccessIssue {
								alg: Algorithm::Ps512,
								key: "bar".to_string(),
							}),
						},
					}),
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Some(Duration::from_days(10).unwrap()),
					token: Some(Duration::from_secs(10)),
					session: Some(Duration::from_mins(15).unwrap()),
				},
				comment: None,
			}))),
		);
	}
	// Verification and issuing with JWT are explicitly defined with two different
	// keys. Refresh specified after JWT.
	{
		let res = syn::parse_with_settings(
			r#"DEFINE ACCESS a ON DB TYPE RECORD WITH JWT ALGORITHM PS512 KEY "foo" WITH ISSUER KEY "bar" WITH REFRESH DURATION FOR GRANT 10d, FOR TOKEN 10s, FOR SESSION 15m"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |p,s| p.parse_expr_inherit(s).await,
		).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Record(RecordAccess {
					signup: None,
					signin: None,
					jwt: JwtAccess {
						verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
							alg: Algorithm::Ps512,
							key: "foo".to_string(),
						}),
						issue: Some(JwtAccessIssue {
							alg: Algorithm::Ps512,
							key: "bar".to_string(),
						}),
					},
					bearer: Some(BearerAccess {
						kind: BearerAccessType::Refresh,
						subject: BearerAccessSubject::Record,
						jwt: JwtAccess {
							verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
								alg: Algorithm::Ps512,
								key: "foo".to_string(),
							}),
							issue: Some(JwtAccessIssue {
								alg: Algorithm::Ps512,
								key: "bar".to_string(),
							}),
						},
					}),
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Some(Duration::from_days(10).unwrap()),
					token: Some(Duration::from_secs(10)),
					session: Some(Duration::from_mins(15).unwrap()),
				},
				comment: None,
			}))),
		);
	}
	// Verification and issuing with JWT are explicitly defined with two different
	// keys. Token duration is explicitly defined.
	{
		let res = syn::parse_with(r#"DEFINE ACCESS a ON DB TYPE RECORD WITH JWT ALGORITHM RS256 KEY 'foo' WITH ISSUER KEY 'bar' DURATION FOR TOKEN 10s, FOR SESSION 15m"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Record(RecordAccess {
					signup: None,
					signin: None,
					jwt: JwtAccess {
						verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
							alg: Algorithm::Rs256,
							key: "foo".to_string(),
						}),
						issue: Some(JwtAccessIssue {
							alg: Algorithm::Rs256,
							key: "bar".to_string(),
						}),
					},
					bearer: None,
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Some(Duration::from_days(30).unwrap()),
					token: Some(Duration::from_secs(10)),
					session: Some(Duration::from_mins(15).unwrap()),
				},
				comment: None,
			}))),
		);
	}
	// kjjification with JWT is explicitly defined only with symmetric key. Token
	// duration is none.
	{
		syn::parse_with(
			r#"DEFINE ACCESS a ON DB TYPE RECORD DURATION FOR TOKEN NONE"#.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap_err();
	}
	// Attempt to define record access at the root level.
	{
		syn::parse_with(
			r#"DEFINE ACCESS a ON ROOT TYPE RECORD DURATION FOR TOKEN NONE"#.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap_err();
	}
	// Attempt to define record access at the namespace level.
	{
		syn::parse_with(
			r#"DEFINE ACCESS a ON NS TYPE RECORD DURATION FOR TOKEN NONE"#.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap_err();
	}
}

#[test]
fn parse_define_access_bearer() {
	// For user on database.
	{
		let res = syn::parse_with_settings(
			r#"DEFINE ACCESS a ON DB TYPE BEARER FOR USER COMMENT "foo""#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |p, s| p.parse_expr_inherit(s).await,
		)
		.unwrap();

		// Manually compare since DefineAccessStatement for bearer access
		// without explicit JWT will create a random signing key during parsing.
		let Expr::Define(res) = res else {
			panic!()
		};

		let DefineStatement::Access(stmt) = *res else {
			panic!()
		};

		assert_eq!(stmt.name, Ident::from_strand(strand!("a").to_owned()));
		assert_eq!(stmt.base, Base::Db);
		assert_eq!(stmt.authenticate, None);
		assert_eq!(
			stmt.duration,
			// Default durations.
			AccessDuration {
				grant: Some(Duration::from_days(30).unwrap()),
				token: Some(Duration::from_hours(1).unwrap()),
				session: None,
			}
		);
		assert_eq!(stmt.comment, Some(strand!("foo").to_owned()));
		match stmt.access_type {
			AccessType::Bearer(ac) => {
				assert_eq!(ac.subject, BearerAccessSubject::User);
			}
			_ => panic!(),
		}
	}
	// For user on namespace.
	{
		let res = syn::parse_with_settings(
			r#"DEFINE ACCESS a ON NS TYPE BEARER FOR USER COMMENT "foo""#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |p, s| p.parse_expr_inherit(s).await,
		)
		.unwrap();

		// Manually compare since DefineAccessStatement for bearer access
		// without explicit JWT will create a random signing key during parsing.
		let Expr::Define(res) = res else {
			panic!()
		};

		let DefineStatement::Access(stmt) = *res else {
			panic!()
		};

		assert_eq!(stmt.name, Ident::from_strand(strand!("a").to_owned()));
		assert_eq!(stmt.base, Base::Ns);
		assert_eq!(stmt.authenticate, None);
		assert_eq!(
			stmt.duration,
			// Default durations.
			AccessDuration {
				grant: Some(Duration::from_days(30).unwrap()),
				token: Some(Duration::from_hours(1).unwrap()),
				session: None,
			}
		);
		assert_eq!(stmt.comment, Some(strand!("foo").to_owned()));
		match stmt.access_type {
			AccessType::Bearer(ac) => {
				assert_eq!(ac.subject, BearerAccessSubject::User);
			}
			_ => panic!(),
		}
	}
	// For user on root.
	{
		let res = syn::parse_with_settings(
			r#"DEFINE ACCESS a ON ROOT TYPE BEARER FOR USER COMMENT "foo""#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |p, s| p.parse_expr_inherit(s).await,
		)
		.unwrap();

		// Manually compare since DefineAccessStatement for bearer access
		// without explicit JWT will create a random signing key during parsing.
		let Expr::Define(res) = res else {
			panic!()
		};

		let DefineStatement::Access(stmt) = *res else {
			panic!()
		};

		assert_eq!(stmt.name, Ident::from_strand(strand!("a").to_owned()));
		assert_eq!(stmt.base, Base::Root);
		assert_eq!(stmt.authenticate, None);
		assert_eq!(
			stmt.duration,
			// Default durations.
			AccessDuration {
				grant: Some(Duration::from_days(30).unwrap()),
				token: Some(Duration::from_hours(1).unwrap()),
				session: None,
			}
		);
		assert_eq!(stmt.comment, Some(strand!("foo").to_owned()));
		match stmt.access_type {
			AccessType::Bearer(ac) => {
				assert_eq!(ac.subject, BearerAccessSubject::User);
			}
			_ => panic!(),
		}
	}
	// For record on database.
	{
		let res = syn::parse_with_settings(
			r#"DEFINE ACCESS a ON DB TYPE BEARER FOR RECORD COMMENT "foo""#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |p, s| p.parse_expr_inherit(s).await,
		)
		.unwrap();

		// Manually compare since DefineAccessStatement for bearer access
		// without explicit JWT will create a random signing key during parsing.
		let Expr::Define(res) = res else {
			panic!()
		};

		let DefineStatement::Access(stmt) = *res else {
			panic!()
		};

		assert_eq!(stmt.name, Ident::from_strand(strand!("a").to_owned()));
		assert_eq!(stmt.base, Base::Db);
		assert_eq!(
			stmt.duration,
			// Default durations.
			AccessDuration {
				grant: Some(Duration::from_days(30).unwrap()),
				token: Some(Duration::from_hours(1).unwrap()),
				session: None,
			}
		);
		assert_eq!(stmt.comment, Some(strand!("foo").to_owned()));
		match stmt.access_type {
			AccessType::Bearer(ac) => {
				assert_eq!(ac.subject, BearerAccessSubject::Record);
			}
			_ => panic!(),
		}
	}
	// For record on namespace.
	{
		syn::parse_with_settings(
			r#"DEFINE ACCESS a ON NS TYPE BEARER FOR RECORD COMMENT "foo""#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |p, s| p.parse_expr_inherit(s).await,
		)
		.unwrap_err();
	}
	// For record on root.
	{
		syn::parse_with_settings(
			r#"DEFINE ACCESS a ON ROOT TYPE BEARER FOR RECORD COMMENT "foo""#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |p, s| p.parse_expr_inherit(s).await,
		)
		.unwrap_err();
	}
	// For user. Grant, session and token duration. With JWT.
	{
		let res = syn::parse_with_settings(
			r#"DEFINE ACCESS a ON DB TYPE BEARER FOR USER WITH JWT ALGORITHM HS384 KEY "foo" DURATION FOR GRANT 90d, FOR TOKEN 10s, FOR SESSION 15m"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |p,s| p.parse_expr_inherit(s).await,
		).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Bearer(BearerAccess {
					kind: BearerAccessType::Bearer,
					subject: BearerAccessSubject::User,
					jwt: JwtAccess {
						verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
							alg: Algorithm::Hs384,
							key: "foo".to_string(),
						}),
						issue: Some(JwtAccessIssue {
							alg: Algorithm::Hs384,
							// Issuer key matches verification key by default in symmetric
							// algorithms.
							key: "foo".to_string(),
						}),
					},
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Some(Duration::from_days(90).unwrap()),
					token: Some(Duration::from_secs(10)),
					session: Some(Duration::from_secs(900)),
				},
				comment: None,
			}))),
		)
	}
	// For record. Grant, session and token duration. With JWT.
	{
		let res = syn::parse_with_settings(
			r#"DEFINE ACCESS a ON DB TYPE BEARER FOR RECORD WITH JWT ALGORITHM HS384 KEY "foo" DURATION FOR GRANT 90d, FOR TOKEN 10s, FOR SESSION 15m"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |p,s| p.parse_expr_inherit(s).await,
		).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Ident::from_strand(strand!("a").to_owned()),
				base: Base::Db,
				access_type: AccessType::Bearer(BearerAccess {
					kind: BearerAccessType::Bearer,
					subject: BearerAccessSubject::Record,
					jwt: JwtAccess {
						verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
							alg: Algorithm::Hs384,
							key: "foo".to_string(),
						}),
						issue: Some(JwtAccessIssue {
							alg: Algorithm::Hs384,
							// Issuer key matches verification key by default in symmetric
							// algorithms.
							key: "foo".to_string(),
						}),
					},
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Some(Duration::from_days(90).unwrap()),
					token: Some(Duration::from_secs(10)),
					session: Some(Duration::from_secs(900)),
				},
				comment: None,
			}))),
		)
	}
}

#[test]
fn parse_define_param() {
	let res = syn::parse_with(
		r#"DEFINE PARAM $a VALUE { a: 1, "b": 3 } PERMISSIONS WHERE null"#.as_bytes(),
		async |parser, stk| parser.parse_expr_inherit(stk).await,
	)
	.unwrap();

	assert_eq!(
		res,
		Expr::Define(Box::new(DefineStatement::Param(DefineParamStatement {
			kind: DefineKind::Default,
			name: Ident::from_strand(strand!("a").to_owned()),
			value: Expr::Literal(Literal::Object(vec![
				ObjectEntry {
					key: "a".to_string(),
					value: Expr::Literal(Literal::Integer(1))
				},
				ObjectEntry {
					key: "b".to_string(),
					value: Expr::Literal(Literal::Integer(3))
				},
			])),
			comment: None,
			permissions: Permission::Specific(Expr::Literal(Literal::Null)),
		})))
	);
}

#[test]
fn parse_define_table() {
	let res =
		syn::parse_with(r#"DEFINE TABLE name DROP SCHEMAFUL CHANGEFEED 1s INCLUDE ORIGINAL PERMISSIONS FOR DELETE FULL, FOR SELECT WHERE a = 1 AS SELECT foo FROM bar GROUP BY foo"#.as_bytes(),async |parser,stk| parser.parse_expr_inherit(stk).await).unwrap();

	assert_eq!(
		res,
		Expr::Define(Box::new(DefineStatement::Table(DefineTableStatement {
			kind: DefineKind::Default,
			id: None,
			name: Ident::from_strand(strand!("name").to_owned()),
			drop: true,
			full: true,
			view: Some(crate::sql::View {
				expr: Fields::Select(vec![Field::Single {
					expr: ident_field("foo"),
					alias: None,
				}],),
				what: vec![Ident::from_strand(strand!("bar").to_owned())],
				cond: None,
				group: Some(Groups(vec![Group(Idiom(vec![Part::Field(Ident::from_strand(
					strand!("foo").to_owned()
				))]))])),
			}),
			permissions: Permissions {
				select: Permission::Specific(Expr::Binary {
					left: Box::new(ident_field("a")),
					op: BinaryOperator::Equal,
					right: Box::new(Expr::Literal(Literal::Integer(1)))
				}),
				create: Permission::None,
				update: Permission::None,
				delete: Permission::Full,
			},
			changefeed: Some(ChangeFeed {
				expiry: std::time::Duration::from_secs(1),
				store_diff: true,
			}),
			comment: None,

			table_type: TableType::Normal,
		})))
	);
}

#[test]
fn parse_define_event() {
	let res = syn::parse_with(
		r#"DEFINE EVENT event ON TABLE table WHEN null THEN null,none"#.as_bytes(),
		async |parser, stk| parser.parse_expr_inherit(stk).await,
	)
	.unwrap();

	assert_eq!(
		res,
		Expr::Define(Box::new(DefineStatement::Event(DefineEventStatement {
			kind: DefineKind::Default,
			name: Ident::from_strand(strand!("event").to_owned()),
			target_table: Ident::from_strand(strand!("table").to_owned()),
			when: Expr::Literal(Literal::Null),
			then: vec![Expr::Literal(Literal::Null), Expr::Literal(Literal::None)],
			comment: None,
		})))
	)
}

#[test]
fn parse_define_field() {
	// General
	{
		let res = syn::parse_with(r#"DEFINE FIELD foo.*[*]... ON TABLE bar FLEX TYPE option<number | array<record<foo>,10>> VALUE null ASSERT true DEFAULT false PERMISSIONS FOR UPDATE NONE, FOR CREATE WHERE true"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();

		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Field(DefineFieldStatement {
				kind: DefineKind::Default,
				name: Idiom(vec![
					Part::Field(Ident::from_strand(strand!("foo").to_owned())),
					Part::All,
					Part::All,
					Part::Flatten,
				]),
				what: Ident::from_strand(strand!("bar").to_owned()),
				flex: true,
				field_kind: Some(Kind::Option(Box::new(Kind::Either(vec![
					Kind::Number,
					Kind::Array(Box::new(Kind::Record(vec!["foo".to_owned()])), Some(10))
				])))),
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
				comment: None,
				reference: None,
				computed: None,
			})))
		)
	}

	// Invalid DELETE permission
	{
		// TODO(gguillemas): Providing the DELETE permission should return a parse error
		// in 3.0.0. Currently, the DELETE permission is just ignored to maintain
		// backward compatibility.
		let res = syn::parse_with(
			r#"DEFINE FIELD foo ON TABLE bar PERMISSIONS FOR DELETE NONE"#.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Field(DefineFieldStatement {
				kind: DefineKind::Default,
				name: Idiom(vec![Part::Field(Ident::from_strand(strand!("foo").to_owned())),]),
				what: Ident::from_strand(strand!("bar").to_owned()),
				flex: false,
				field_kind: None,
				readonly: false,
				value: None,
				assert: None,
				default: DefineDefault::None,
				permissions: Permissions {
					delete: Permission::Full,
					update: Permission::Full,
					create: Permission::Full,
					select: Permission::Full,
				},
				comment: None,
				reference: None,
				computed: None,
			})))
		)
	}
}

#[test]
fn parse_define_index() {
	let res = syn::parse_with(
		r#"DEFINE INDEX index ON TABLE table FIELDS a,b[*] SEARCH ANALYZER ana BM25 (0.1,0.2)
		DOC_IDS_ORDER 1
		DOC_LENGTHS_ORDER 2
		POSTINGS_ORDER 3
		TERMS_ORDER 4
		DOC_IDS_CACHE 5
		DOC_LENGTHS_CACHE 6
		POSTINGS_CACHE 7
		TERMS_CACHE 8
		HIGHLIGHTS
		"#
		.as_bytes(),
		async |parser, stk| parser.parse_expr_inherit(stk).await,
	)
	.unwrap();
	assert_eq!(
		res,
		Expr::Define(Box::new(DefineStatement::Index(DefineIndexStatement {
			kind: DefineKind::Default,
			name: Ident::from_strand(strand!("index").to_owned()),
			what: Ident::from_strand(strand!("table").to_owned()),
			cols: vec![
				Idiom(vec![Part::Field(Ident::from_strand(strand!("a").to_owned()))]),
				Idiom(vec![Part::Field(Ident::from_strand(strand!("b").to_owned())), Part::All])
			],
			index: Index::Search(SearchParams {
				az: Ident::from_strand(strand!("ana").to_owned()),
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
			concurrently: false
		})))
	);

	let res = syn::parse_with(
		r#"DEFINE INDEX index ON TABLE table FIELDS a UNIQUE"#.as_bytes(),
		async |parser, stk| parser.parse_expr_inherit(stk).await,
	)
	.unwrap();

	assert_eq!(
		res,
		Expr::Define(Box::new(DefineStatement::Index(DefineIndexStatement {
			kind: DefineKind::Default,
			name: Ident::from_strand(strand!("index").to_owned()),
			what: Ident::from_strand(strand!("table").to_owned()),
			cols: vec![Idiom(vec![Part::Field(Ident::from_strand(strand!("a").to_owned()))]),],
			index: Index::Uniq,
			comment: None,
			concurrently: false
		})))
	);

	let res =
		syn::parse_with( r#"DEFINE INDEX index ON TABLE table FIELDS a MTREE DIMENSION 4 DISTANCE MINKOWSKI 5 CAPACITY 6 TYPE I16 DOC_IDS_ORDER 7 DOC_IDS_CACHE 8 MTREE_CACHE 9"#.as_bytes(),async |parser,stk| parser.parse_expr_inherit(stk).await).unwrap();

	assert_eq!(
		res,
		Expr::Define(Box::new(DefineStatement::Index(DefineIndexStatement {
			kind: DefineKind::Default,
			name: Ident::from_strand(strand!("index").to_owned()),
			what: Ident::from_strand(strand!("table").to_owned()),
			cols: vec![Idiom(vec![Part::Field(Ident::from_strand(strand!("a").to_owned()))]),],
			index: Index::MTree(MTreeParams {
				dimension: 4,
				distance: Distance::Minkowski(Number::Int(5)),
				capacity: 6,
				doc_ids_order: 7,
				doc_ids_cache: 8,
				mtree_cache: 9,
				vector_type: VectorType::I16,
			}),
			comment: None,
			concurrently: false,
		})))
	);

	let res =
		syn::parse_with( r#"DEFINE INDEX index ON TABLE table FIELDS a HNSW DIMENSION 128 EFC 250 TYPE F32 DISTANCE MANHATTAN M 6 M0 12 LM 0.5 EXTEND_CANDIDATES KEEP_PRUNED_CONNECTIONS"#.as_bytes(),async |parser,stk| parser.parse_expr_inherit(stk).await).unwrap();
	assert_eq!(
		res,
		Expr::Define(Box::new(DefineStatement::Index(DefineIndexStatement {
			kind: DefineKind::Default,
			name: Ident::from_strand(strand!("index").to_owned()),
			what: Ident::from_strand(strand!("table").to_owned()),
			cols: vec![Idiom(vec![Part::Field(Ident::from_strand(strand!("a").to_owned()))]),],
			index: Index::Hnsw(HnswParams {
				dimension: 128,
				distance: Distance::Manhattan,
				vector_type: VectorType::F32,
				m: 6,
				m0: 12,
				ef_construction: 250,
				extend_candidates: true,
				keep_pruned_connections: true,
				ml: 0.5.into(),
			}),
			comment: None,
			concurrently: false
		})))
	);
}

#[test]
fn parse_define_analyzer() {
	let res = syn::parse_with(r#"DEFINE ANALYZER ana FILTERS ASCII, EDGENGRAM(1,2), NGRAM(3,4), LOWERCASE, SNOWBALL(NLD), UPPERCASE TOKENIZERS BLANK, CAMEL, CLASS, PUNCT FUNCTION fn::foo::bar"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
	assert_eq!(
		res,
		Expr::Define(Box::new(DefineStatement::Analyzer(DefineAnalyzerStatement {
			kind: DefineKind::Default,
			name: Ident::from_strand(strand!("ana").to_owned()),
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
			function: Some("foo::bar".to_owned()),
		}))),
	)
}

#[test]
fn parse_delete() {
	let res = syn::parse_with("DELETE FROM ONLY |foo:32..64| WITH INDEX index,index_2 Where 2 RETURN AFTER TIMEOUT 1s PARALLEL EXPLAIN FULL".as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
	assert_eq!(
		res,
		Expr::Delete(Box::new(DeleteStatement {
			only: true,
			what: vec![Expr::Mock(Mock::Range("foo".to_string(), 32, 64))],
			with: Some(With::Index(vec!["index".to_owned(), "index_2".to_owned()])),
			cond: Some(Cond(Expr::Literal(Literal::Integer(2)))),
			output: Some(Output::After),
			timeout: Some(Timeout(Duration(std::time::Duration::from_secs(1)))),
			parallel: true,
			explain: Some(Explain(true)),
		}))
	);
}

#[test]
fn parse_delete_2() {
	let res = syn::parse_with(r#"DELETE FROM ONLY a:b->?[$][?true] WITH INDEX index,index_2 WHERE null RETURN NULL TIMEOUT 1h PARALLEL EXPLAIN"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
	assert_eq!(
		res,
		Expr::Delete(Box::new(DeleteStatement {
			only: true,
			what: vec![Expr::Idiom(Idiom(vec![
				Part::Start(Expr::Literal(Literal::RecordId(RecordIdLit {
					table: "a".to_owned(),
					key: RecordIdKeyLit::String(strand!("b").to_owned()),
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
			timeout: Some(Timeout(Duration(std::time::Duration::from_secs(60 * 60)))),
			parallel: true,
			explain: Some(Explain(false)),
		}))
	)
}

#[test]
pub fn parse_for() {
	let res = syn::parse_with(
		r#"FOR $foo IN (SELECT foo FROM bar) * 2 { BREAK }"#.as_bytes(),
		async |parser, stk| parser.parse_expr_inherit(stk).await,
	)
	.unwrap();

	assert_eq!(
		res,
		Expr::Foreach(Box::new(ForeachStatement {
			param: Param::from_strand(strand!("foo").to_owned()),
			range: Expr::Binary {
				left: Box::new(Expr::Select(Box::new(SelectStatement {
					expr: Fields::Select(vec![Field::Single {
						expr: ident_field("foo"),
						alias: None
					}],),
					what: vec![Expr::Table(Ident::from_strand(strand!("bar").to_owned()))],
					omit: None,
					only: false,
					with: None,
					cond: None,
					split: None,
					group: None,
					order: None,
					limit: None,
					start: None,
					fetch: None,
					version: None,
					timeout: None,
					parallel: false,
					explain: None,
					tempfiles: false
				}))),
				op: BinaryOperator::Multiply,
				right: Box::new(Expr::Literal(Literal::Integer(2)))
			},
			block: Block(vec![Expr::Break])
		}))
	)
}

#[test]
fn parse_if() {
	let res = syn::parse_with(
		r#"IF foo THEN bar ELSE IF faz THEN baz ELSE baq END"#.as_bytes(),
		async |parser, stk| parser.parse_expr_inherit(stk).await,
	)
	.unwrap();
	assert_eq!(
		res,
		Expr::If(Box::new(IfelseStatement {
			exprs: vec![
				(ident_field("foo"), ident_field("bar")),
				(ident_field("faz"), ident_field("baz")),
			],
			close: Some(ident_field("baq"))
		}))
	)
}

#[test]
fn parse_if_block() {
	let res = syn::parse_with(
		r#"IF foo { bar } ELSE IF faz { baz } ELSE { baq }"#.as_bytes(),
		async |parser, stk| parser.parse_expr_inherit(stk).await,
	)
	.unwrap();
	assert_eq!(
		res,
		Expr::If(Box::new(IfelseStatement {
			exprs: vec![
				(ident_field("foo"), Expr::Block(Box::new(Block(vec![ident_field("bar")]))),),
				(ident_field("faz"), Expr::Block(Box::new(Block(vec![ident_field("baz")]))),)
			],
			close: Some(Expr::Block(Box::new(Block(vec![ident_field("baq")])))),
		}))
	)
}

#[test]
fn parse_info() {
	let res = syn::parse_with("INFO FOR ROOT".as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(res, Expr::Info(Box::new(InfoStatement::Root(false))));

	let res = syn::parse_with("INFO FOR KV".as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(res, Expr::Info(Box::new(InfoStatement::Root(false))));

	let res = syn::parse_with("INFO FOR NAMESPACE".as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(res, Expr::Info(Box::new(InfoStatement::Ns(false))));

	let res = syn::parse_with("INFO FOR NS".as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(res, Expr::Info(Box::new(InfoStatement::Ns(false))));

	let res = syn::parse_with("INFO FOR TABLE table".as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		Expr::Info(Box::new(InfoStatement::Tb(
			Ident::from_strand(strand!("table").to_owned()),
			false,
			None
		)))
	);

	let res = syn::parse_with("INFO FOR USER user".as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		Expr::Info(Box::new(InfoStatement::User(
			Ident::from_strand(strand!("user").to_owned()),
			None,
			false
		)))
	);

	let res = syn::parse_with("INFO FOR USER user ON namespace".as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		Expr::Info(Box::new(InfoStatement::User(
			Ident::from_strand(strand!("user").to_owned()),
			Some(Base::Ns),
			false
		)))
	);
}

#[test]
fn parse_select() {
	let res = syn::parse_with(
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
		.as_bytes(),
		async |p, s| p.parse_expr_inherit(s).await,
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
		Expr::Select(Box::new(SelectStatement {
			expr: Fields::Select(vec![
				Field::Single {
					expr: ident_field("bar"),
					alias: Some(Idiom(vec![Part::Field(Ident::from_strand(
						strand!("foo").to_owned()
					))])),
				},
				Field::Single {
					expr: Expr::Literal(Literal::Array(vec![
						Expr::Literal(Literal::Integer(1)),
						Expr::Literal(Literal::Integer(2))
					])),
					alias: None,
				},
				Field::Single {
					expr: ident_field("bar"),
					alias: None,
				},
			],),
			omit: Some(Idioms(vec![Idiom(vec![Part::Field(Ident::from_strand(
				strand!("bar").to_owned()
			))])])),
			only: true,
			what: vec![
				Expr::Table(Ident::from_strand(strand!("a").to_owned())),
				Expr::Literal(Literal::Integer(1))
			],
			with: Some(With::Index(vec!["index".to_owned(), "index_2".to_owned()])),
			cond: Some(Cond(Expr::Literal(Literal::Bool(true)))),
			split: Some(Splits(vec![
				Split(Idiom(vec![Part::Field(Ident::from_strand(strand!("foo").to_owned()))])),
				Split(Idiom(vec![Part::Field(Ident::from_strand(strand!("bar").to_owned()))])),
			])),
			group: Some(Groups(vec![
				Group(Idiom(vec![Part::Field(Ident::from_strand(strand!("foo").to_owned()))])),
				Group(Idiom(vec![Part::Field(Ident::from_strand(strand!("bar").to_owned()))])),
			])),
			order: Some(Ordering::Order(OrderList(vec![Order {
				value: Idiom(vec![Part::Field(Ident::from_strand(strand!("foo").to_owned()))]),
				collate: true,
				numeric: true,
				direction: true,
			}]))),
			limit: Some(Limit(Expr::Literal(Literal::RecordId(RecordIdLit {
				table: "a".to_owned(),
				key: RecordIdKeyLit::String(strand!("b").to_owned()),
			})))),
			start: Some(Start(Expr::Literal(Literal::Object(vec![ObjectEntry {
				key: "a".to_owned(),
				value: Expr::Literal(Literal::Bool(true))
			}])))),
			fetch: Some(Fetchs(vec![Fetch(ident_field("foo"))])),
			version: Some(Expr::Literal(Literal::Datetime(Datetime(expected_datetime)))),
			timeout: None,
			parallel: false,
			tempfiles: false,
			explain: Some(Explain(true)),
		})),
	);
}

#[test]
fn parse_show() {
	let res = syn::parse_with(
		r#"SHOW CHANGES FOR TABLE foo SINCE 1 LIMIT 10"#.as_bytes(),
		async |parser, stk| parser.parse_top_level_expr(stk).await,
	)
	.unwrap();

	assert_eq!(
		res,
		TopLevelExpr::Show(ShowStatement {
			table: Some(Ident::from_strand(strand!("foo").to_owned())),
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

	let res = syn::parse_with(
		r#"SHOW CHANGES FOR DATABASE SINCE d"2012-04-23T18:25:43.0000511Z""#.as_bytes(),
		async |parser, stk| parser.parse_query(stk).await,
	)
	.unwrap()
	.expressions
	.pop()
	.unwrap();
	assert_eq!(
		res,
		TopLevelExpr::Show(ShowStatement {
			table: None,
			since: ShowSince::Timestamp(Datetime(expected_datetime)),
			limit: None
		})
	)
}

#[test]
fn parse_sleep() {
	let res = syn::parse_with(r"SLEEP 1s".as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();

	let expect = Expr::Sleep(Box::new(SleepStatement {
		duration: Duration(std::time::Duration::from_secs(1)),
	}));
	assert_eq!(res, expect)
}

#[test]
fn parse_use() {
	let res = syn::parse_with(r"USE NS foo".as_bytes(), async |parser, stk| {
		parser.parse_query(stk).await
	})
	.unwrap()
	.expressions
	.pop()
	.unwrap();
	let expect = TopLevelExpr::Use(UseStatement {
		ns: Some(Ident::from_strand(strand!("foo").to_owned())),
		db: None,
	});
	assert_eq!(res, expect);

	let res = syn::parse_with(r"USE NS foo".as_bytes(), async |parser, stk| {
		parser.parse_query(stk).await
	})
	.unwrap()
	.expressions
	.pop()
	.unwrap();
	let expect = TopLevelExpr::Use(UseStatement {
		ns: Some(Ident::from_strand(strand!("foo").to_owned())),
		db: None,
	});
	assert_eq!(res, expect);

	let res = syn::parse_with(r"USE NS bar DB foo".as_bytes(), async |parser, stk| {
		parser.parse_query(stk).await
	})
	.unwrap()
	.expressions
	.pop()
	.unwrap();

	let expect = TopLevelExpr::Use(UseStatement {
		ns: Some(Ident::from_strand(strand!("bar").to_owned())),
		db: Some(Ident::from_strand(strand!("foo").to_owned())),
	});
	assert_eq!(res, expect);
}

#[test]
fn parse_use_lowercase() {
	let res = syn::parse_with(r"use ns foo".as_bytes(), async |parser, stk| {
		parser.parse_query(stk).await
	})
	.unwrap()
	.expressions
	.pop()
	.unwrap();
	let expect = TopLevelExpr::Use(UseStatement {
		ns: Some(Ident::from_strand(strand!("foo").to_owned())),
		db: None,
	});
	assert_eq!(res, expect);

	let res = syn::parse_with(r"use db foo".as_bytes(), async |parser, stk| {
		parser.parse_query(stk).await
	})
	.unwrap()
	.expressions
	.pop()
	.unwrap();

	let expect = TopLevelExpr::Use(UseStatement {
		ns: None,
		db: Some(Ident::from_strand(strand!("foo").to_owned())),
	});
	assert_eq!(res, expect);

	let res = syn::parse_with(r"use ns bar db foo".as_bytes(), async |parser, stk| {
		parser.parse_query(stk).await
	})
	.unwrap()
	.expressions
	.pop()
	.unwrap();

	let expect = TopLevelExpr::Use(UseStatement {
		ns: Some(Ident::from_strand(strand!("bar").to_owned())),
		db: Some(Ident::from_strand(strand!("foo").to_owned())),
	});
	assert_eq!(res, expect);
}

#[test]
fn parse_value_stmt() {
	let res =
		syn::parse_with(r"1s".as_bytes(), async |parser, stk| parser.parse_expr_inherit(stk).await)
			.unwrap();
	let expect = Expr::Literal(Literal::Duration(Duration(std::time::Duration::from_secs(1))));
	assert_eq!(res, expect);
}

#[test]
fn parse_throw() {
	let res = syn::parse_with(r"THROW 1s".as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();

	let expect = Expr::Throw(Box::new(Expr::Literal(Literal::Duration(Duration(
		std::time::Duration::from_secs(1),
	)))));
	assert_eq!(res, expect)
}

#[test]
fn parse_insert() {
	let res = syn::parse_with(
		r#"INSERT IGNORE INTO $foo (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE a.b +?= null, c.d += none RETURN AFTER"#.as_bytes(),
		async |parser,stk| parser.parse_expr_inherit(stk).await
	).unwrap();
	assert_eq!(
		res,
		Expr::Insert(Box::new(InsertStatement {
			into: Some(Expr::Param(Param::from_strand(strand!("foo").to_owned()))),
			data: Data::ValuesExpression(vec![
				vec![
					(
						Idiom(vec![Part::Field(Ident::from_strand(strand!("a").to_owned()))]),
						Expr::Literal(Literal::Integer(1)),
					),
					(
						Idiom(vec![Part::Field(Ident::from_strand(strand!("b").to_owned()))]),
						Expr::Literal(Literal::Integer(2)),
					),
					(
						Idiom(vec![Part::Field(Ident::from_strand(strand!("c").to_owned()))]),
						Expr::Literal(Literal::Integer(3)),
					),
				],
				vec![
					(
						Idiom(vec![Part::Field(Ident::from_strand(strand!("a").to_owned()))]),
						Expr::Literal(Literal::Integer(4)),
					),
					(
						Idiom(vec![Part::Field(Ident::from_strand(strand!("b").to_owned()))]),
						Expr::Literal(Literal::Integer(5)),
					),
					(
						Idiom(vec![Part::Field(Ident::from_strand(strand!("c").to_owned()))]),
						Expr::Literal(Literal::Integer(6)),
					),
				],
			]),
			ignore: true,
			update: Some(Data::UpdateExpression(vec![
				Assignment {
					place: Idiom(vec![
						Part::Field(Ident::from_strand(strand!("a").to_owned())),
						Part::Field(Ident::from_strand(strand!("b").to_owned())),
					]),
					operator: crate::sql::AssignOperator::Extend,
					value: Expr::Literal(Literal::Null)
				},
				Assignment {
					place: Idiom(vec![
						Part::Field(Ident::from_strand(strand!("c").to_owned())),
						Part::Field(Ident::from_strand(strand!("d").to_owned())),
					]),
					operator: crate::sql::AssignOperator::Add,
					value: Expr::Literal(Literal::None)
				},
			])),
			output: Some(Output::After),
			version: None,
			timeout: None,
			parallel: false,
			relation: false,
		})),
	)
}

#[test]
fn parse_insert_select() {
	let res = syn::parse_with(
		r#"INSERT IGNORE INTO bar (select foo from baz)"#.as_bytes(),
		async |parser, stk| parser.parse_expr_inherit(stk).await,
	)
	.unwrap();
	assert_eq!(
		res,
		Expr::Insert(Box::new(InsertStatement {
			into: Some(Expr::Table(Ident::from_strand(strand!("bar").to_owned()))),
			data: Data::SingleExpression(Expr::Select(Box::new(SelectStatement {
				expr: Fields::Select(vec![Field::Single {
					expr: Expr::Idiom(Idiom(vec![Part::Field(Ident::from_strand(
						strand!("foo").to_owned()
					))])),
					alias: None
				}],),
				omit: None,
				only: false,
				what: vec![Expr::Table(Ident::from_strand(strand!("baz").to_owned()))],
				with: None,
				cond: None,
				split: None,
				group: None,
				order: None,
				limit: None,
				start: None,
				fetch: None,
				version: None,
				timeout: None,
				parallel: false,
				explain: None,
				tempfiles: false
			}))),
			ignore: true,
			update: None,
			output: None,
			version: None,
			timeout: None,
			parallel: false,
			relation: false,
		})),
	)
}

#[test]
fn parse_kill() {
	let res = syn::parse_with(r#"KILL $param"#.as_bytes(), async |parser, stk| {
		parser.parse_query(stk).await
	})
	.unwrap()
	.expressions
	.pop()
	.unwrap();
	assert_eq!(
		res,
		TopLevelExpr::Kill(KillStatement {
			id: Expr::Param(Param::from_strand(strand!("param").to_owned()))
		})
	);

	let res = syn::parse_with(
		r#"KILL u"e72bee20-f49b-11ec-b939-0242ac120002" "#.as_bytes(),
		async |parser, stk| parser.parse_query(stk).await,
	)
	.unwrap()
	.expressions
	.pop()
	.unwrap();
	assert_eq!(
		res,
		TopLevelExpr::Kill(KillStatement {
			id: Expr::Literal(Literal::Uuid(Uuid(uuid::uuid!(
				"e72bee20-f49b-11ec-b939-0242ac120002"
			))))
		})
	);
}

#[test]
fn parse_live() {
	let res = syn::parse_with(r#"LIVE SELECT DIFF FROM $foo"#.as_bytes(), async |parser, stk| {
		parser.parse_top_level_expr(stk).await
	})
	.unwrap();
	let TopLevelExpr::Live(stmt) = res else {
		panic!()
	};
	assert_eq!(stmt.fields, Fields::Select(vec![Field::All]));
	assert_eq!(stmt.what, Expr::Param(Param::from_strand(strand!("foo").to_owned())));

	let res = syn::parse_with(
		r#"LIVE SELECT foo FROM table WHERE true FETCH a[where foo],b"#.as_bytes(),
		async |parser, stk| parser.parse_top_level_expr(stk).await,
	)
	.unwrap();
	let TopLevelExpr::Live(stmt) = res else {
		panic!()
	};
	assert_eq!(
		stmt.fields,
		Fields::Select(vec![Field::Single {
			expr: Expr::Idiom(Idiom(vec![Part::Field(Ident::from_strand(
				strand!("foo").to_owned()
			))])),
			alias: None,
		}],)
	);
	assert_eq!(stmt.what, Expr::Table(Ident::from_strand(strand!("table").to_owned())));
	assert_eq!(stmt.cond, Some(Cond(Expr::Literal(Literal::Bool(true)))));
	assert_eq!(
		stmt.fetch,
		Some(Fetchs(vec![
			Fetch(Expr::Idiom(Idiom(vec![
				Part::Field(Ident::from_strand(strand!("a").to_owned())),
				Part::Where(Expr::Idiom(Idiom(vec![Part::Field(Ident::from_strand(
					strand!("foo").to_owned()
				))]))),
			]))),
			Fetch(Expr::Idiom(Idiom(vec![Part::Field(Ident::from_strand(
				strand!("b").to_owned()
			))]))),
		])),
	)
}

#[test]
fn parse_option() {
	let res = syn::parse_with(r#"OPTION value = true"#.as_bytes(), async |parser, stk| {
		parser.parse_top_level_expr(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		TopLevelExpr::Option(OptionStatement {
			name: Ident::from_strand(strand!("value").to_owned()),
			what: true
		})
	)
}

#[test]
fn parse_return() {
	let res = syn::parse_with(r#"RETURN RETRUN FETCH RETURN"#.as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		Expr::Return(Box::new(OutputStatement {
			what: ident_field("RETRUN"),
			fetch: Some(Fetchs(vec![Fetch(ident_field("RETURN"))]))
		})),
	)
}

#[test]
fn parse_relate() {
	let res = syn::parse_with(
		r#"RELATE ONLY [1,2]->a:b->(CREATE foo) UNIQUE SET a += 1 RETURN NONE PARALLEL"#.as_bytes(),
		async |parser, stk| parser.parse_expr_inherit(stk).await,
	)
	.unwrap();
	assert_eq!(
		res,
		Expr::Relate(Box::new(RelateStatement {
			only: true,
			through: Expr::Literal(Literal::RecordId(RecordIdLit {
				table: "a".to_owned(),
				key: RecordIdKeyLit::String(strand!("b").to_owned()),
			})),
			from: Expr::Literal(Literal::Array(vec![
				Expr::Literal(Literal::Integer(1)),
				Expr::Literal(Literal::Integer(2)),
			])),
			to: Expr::Create(Box::new(CreateStatement {
				only: false,
				what: vec![Expr::Table(Ident::from_strand(strand!("foo").to_owned()))],
				data: None,
				output: None,
				timeout: None,
				parallel: false,
				version: None,
			})),
			uniq: true,
			data: Some(Data::SetExpression(vec![Assignment {
				place: Idiom(vec![Part::Field(Ident::from_strand(strand!("a").to_owned()))]),
				operator: AssignOperator::Add,
				value: Expr::Literal(Literal::Integer(1))
			}])),
			output: Some(Output::None),
			timeout: None,
			parallel: true,
		})),
	)
}

#[test]
fn parse_remove() {
	let res = syn::parse_with(r#"REMOVE NAMESPACE ns"#.as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		Expr::Remove(Box::new(RemoveStatement::Namespace(RemoveNamespaceStatement {
			name: Ident::from_strand(strand!("ns").to_owned()),
			if_exists: false,
			expunge: false,
		})))
	);

	let res = syn::parse_with(r#"REMOVE DB database"#.as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		Expr::Remove(Box::new(RemoveStatement::Database(RemoveDatabaseStatement {
			name: Ident::from_strand(strand!("database").to_owned()),
			if_exists: false,
			expunge: false,
		})))
	);

	let res = syn::parse_with(r#"REMOVE FUNCTION fn::foo::bar"#.as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		Expr::Remove(Box::new(RemoveStatement::Function(RemoveFunctionStatement {
			name: Ident::from_strand(strand!("foo::bar").to_owned()),
			if_exists: false,
		})))
	);
	let res =
		syn::parse_with(r#"REMOVE FUNCTION fn::foo::bar();"#.as_bytes(), async |parser, stk| {
			parser.parse_expr_inherit(stk).await
		})
		.unwrap();
	assert_eq!(
		res,
		Expr::Remove(Box::new(RemoveStatement::Function(RemoveFunctionStatement {
			name: Ident::from_strand(strand!("foo::bar").to_owned()),
			if_exists: false,
		})))
	);

	let res =
		syn::parse_with(r#"REMOVE ACCESS foo ON DATABASE"#.as_bytes(), async |parser, stk| {
			parser.parse_expr_inherit(stk).await
		})
		.unwrap();
	assert_eq!(
		res,
		Expr::Remove(Box::new(RemoveStatement::Access(RemoveAccessStatement {
			name: Ident::from_strand(strand!("foo").to_owned()),
			base: Base::Db,
			if_exists: false,
		})))
	);

	let res = syn::parse_with(r#"REMOVE PARAM $foo"#.as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		Expr::Remove(Box::new(RemoveStatement::Param(RemoveParamStatement {
			name: Ident::from_strand(strand!("foo").to_owned()),
			if_exists: false,
		})))
	);

	let res = syn::parse_with(r#"REMOVE TABLE foo"#.as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		Expr::Remove(Box::new(RemoveStatement::Table(RemoveTableStatement {
			name: Ident::from_strand(strand!("foo").to_owned()),
			if_exists: false,
			expunge: false,
		})))
	);

	let res =
		syn::parse_with(r#"REMOVE EVENT foo ON TABLE bar"#.as_bytes(), async |parser, stk| {
			parser.parse_expr_inherit(stk).await
		})
		.unwrap();
	assert_eq!(
		res,
		Expr::Remove(Box::new(RemoveStatement::Event(RemoveEventStatement {
			name: Ident::from_strand(strand!("foo").to_owned()),
			what: Ident::from_strand(strand!("bar").to_owned()),
			if_exists: false,
		})))
	);

	let res =
		syn::parse_with(r#"REMOVE FIELD foo.bar[10] ON bar"#.as_bytes(), async |parser, stk| {
			parser.parse_expr_inherit(stk).await
		})
		.unwrap();
	assert_eq!(
		res,
		Expr::Remove(Box::new(RemoveStatement::Field(RemoveFieldStatement {
			name: Idiom(vec![
				Part::Field(Ident::from_strand(strand!("foo").to_owned())),
				Part::Field(Ident::from_strand(strand!("bar").to_owned())),
				Part::Value(Expr::Literal(Literal::Integer(10)))
			]),
			what: Ident::from_strand(strand!("bar").to_owned()),
			if_exists: false,
		})))
	);

	let res = syn::parse_with(r#"REMOVE INDEX foo ON bar"#.as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		Expr::Remove(Box::new(RemoveStatement::Index(RemoveIndexStatement {
			name: Ident::from_strand(strand!("foo").to_owned()),
			what: Ident::from_strand(strand!("bar").to_owned()),
			if_exists: false,
		})))
	);

	let res = syn::parse_with(r#"REMOVE ANALYZER foo"#.as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		Expr::Remove(Box::new(RemoveStatement::Analyzer(RemoveAnalyzerStatement {
			name: Ident::from_strand(strand!("foo").to_owned()),
			if_exists: false,
		})))
	);

	let res = syn::parse_with(r#"REMOVE user foo on database"#.as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		Expr::Remove(Box::new(RemoveStatement::User(RemoveUserStatement {
			name: Ident::from_strand(strand!("foo").to_owned()),
			base: Base::Db,
			if_exists: false,
		})))
	);
}

#[test]
fn parse_update() {
	let res = syn::parse_with(r#"UPDATE ONLY a->b WITH INDEX index,index_2 UNSET foo... , a->b, c[*] WHERE true RETURN DIFF TIMEOUT 1s PARALLEL EXPLAIN FULL"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
	assert_eq!(
		res,
		Expr::Update(Box::new(UpdateStatement {
			only: true,
			what: vec![Expr::Idiom(Idiom(vec![
				Part::Field(Ident::from_strand(strand!("a").to_owned())),
				Part::Graph(Lookup {
					kind: LookupKind::Graph(Dir::Out),
					what: vec![LookupSubject::Table(Ident::from_strand(strand!("b").to_owned()))],
					..Default::default()
				})
			]))],
			with: Some(With::Index(vec!["index".to_owned(), "index_2".to_owned()])),
			cond: Some(Cond(Expr::Literal(Literal::Bool(true)))),
			data: Some(Data::UnsetExpression(vec![
				Idiom(vec![
					Part::Field(Ident::from_strand(strand!("foo").to_owned())),
					Part::Flatten
				]),
				Idiom(vec![
					Part::Field(Ident::from_strand(strand!("a").to_owned())),
					Part::Graph(Lookup {
						kind: LookupKind::Graph(Dir::Out),
						what: vec![LookupSubject::Table(Ident::from_strand(
							strand!("b").to_owned()
						))],
						..Default::default()
					})
				]),
				Idiom(vec![Part::Field(Ident::from_strand(strand!("c").to_owned())), Part::All])
			])),
			output: Some(Output::Diff),
			timeout: Some(Timeout(Duration(std::time::Duration::from_secs(1)))),
			parallel: true,
			explain: Some(Explain(true))
		}))
	);
}

#[test]
fn parse_upsert() {
	let res = syn::parse_with(r#"UPSERT ONLY a->b WITH INDEX index,index_2 UNSET foo... , a->b, c[*] WHERE true RETURN DIFF TIMEOUT 1s PARALLEL EXPLAIN"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();
	assert_eq!(
		res,
		Expr::Upsert(Box::new(UpsertStatement {
			only: true,
			what: vec![Expr::Idiom(Idiom(vec![
				Part::Field(Ident::from_strand(strand!("a").to_owned())),
				Part::Graph(Lookup {
					kind: LookupKind::Graph(Dir::Out),
					what: vec![LookupSubject::Table(Ident::from_strand(strand!("b").to_owned()))],
					..Default::default()
				})
			]))],
			with: Some(With::Index(vec!["index".to_owned(), "index_2".to_owned()])),
			cond: Some(Cond(Expr::Literal(Literal::Bool(true)))),
			data: Some(Data::UnsetExpression(vec![
				Idiom(vec![
					Part::Field(Ident::from_strand(strand!("foo").to_owned())),
					Part::Flatten
				]),
				Idiom(vec![
					Part::Field(Ident::from_strand(strand!("a").to_owned())),
					Part::Graph(Lookup {
						kind: LookupKind::Graph(Dir::Out),
						what: vec![LookupSubject::Table(Ident::from_strand(
							strand!("b").to_owned()
						))],
						..Default::default()
					})
				]),
				Idiom(vec![Part::Field(Ident::from_strand(strand!("c").to_owned())), Part::All])
			])),
			output: Some(Output::Diff),
			timeout: Some(Timeout(Duration(std::time::Duration::from_secs(1)))),
			parallel: true,
			explain: Some(Explain(false))
		}))
	);
}

#[test]
fn parse_access_grant() {
	// User
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON NAMESPACE GRANT FOR USER b"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Grant(AccessStatementGrant {
				ac: Ident::from_strand(strand!("a").to_owned()),
				base: Some(Base::Ns),
				subject: access::Subject::User(Ident::from_strand(strand!("b").to_owned())),
			})))
		);
	}
	// Record
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON NAMESPACE GRANT FOR RECORD b:c"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Grant(AccessStatementGrant {
				ac: Ident::from_strand(strand!("a").to_owned()),
				base: Some(Base::Ns),
				subject: access::Subject::Record(RecordIdLit {
					table: "b".to_owned(),
					key: RecordIdKeyLit::String(strand!("c").to_owned()),
				}),
			})))
		);
	}
}

#[test]
fn parse_access_show() {
	// All
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE SHOW ALL"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Show(AccessStatementShow {
				ac: Ident::from_strand(strand!("a").to_owned()),
				base: Some(Base::Db),
				gr: None,
				cond: None,
			})))
		);
	}
	// Grant
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE SHOW GRANT b"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Show(AccessStatementShow {
				ac: Ident::from_strand(strand!("a").to_owned()),
				base: Some(Base::Db),
				gr: Some(Ident::from_strand(strand!("b").to_owned())),
				cond: None,
			})))
		);
	}
	// Condition
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE SHOW WHERE true"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Show(AccessStatementShow {
				ac: Ident::from_strand(strand!("a").to_owned()),
				base: Some(Base::Db),
				gr: None,
				cond: Some(Cond(Expr::Literal(Literal::Bool(true)))),
			})))
		);
	}
}

#[test]
fn parse_access_revoke() {
	// All
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE REVOKE ALL"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Revoke(AccessStatementRevoke {
				ac: Ident::from_strand(strand!("a").to_owned()),
				base: Some(Base::Db),
				gr: None,
				cond: None,
			})))
		);
	}
	// Grant
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE REVOKE GRANT b"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Revoke(AccessStatementRevoke {
				ac: Ident::from_strand(strand!("a").to_owned()),
				base: Some(Base::Db),
				gr: Some(Ident::from_strand(strand!("b").to_owned())),
				cond: None,
			})))
		);
	}
	// Condition
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE REVOKE WHERE true"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Revoke(AccessStatementRevoke {
				ac: Ident::from_strand(strand!("a").to_owned()),
				base: Some(Base::Db),
				gr: None,
				cond: Some(Cond(Expr::Literal(Literal::Bool(true)))),
			})))
		);
	}
}

#[test]
fn parse_access_purge() {
	// All
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE PURGE EXPIRED, REVOKED"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Purge(AccessStatementPurge {
				ac: Ident::from_strand(strand!("a").to_owned()),
				base: Some(Base::Db),
				expired: true,
				revoked: true,
				grace: Duration::from_millis(0),
			})))
		);
	}
	// Expired
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE PURGE EXPIRED"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Purge(AccessStatementPurge {
				ac: Ident::from_strand(strand!("a").to_owned()),
				base: Some(Base::Db),
				expired: true,
				revoked: false,
				grace: Duration::from_millis(0),
			})))
		);
	}
	// Revoked
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE PURGE REVOKED"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Purge(AccessStatementPurge {
				ac: Ident::from_strand(strand!("a").to_owned()),
				base: Some(Base::Db),
				expired: false,
				revoked: true,
				grace: Duration::from_millis(0),
			})))
		);
	}
	// Expired for 90 days
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE PURGE EXPIRED FOR 90d"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Purge(AccessStatementPurge {
				ac: Ident::from_strand(strand!("a").to_owned()),
				base: Some(Base::Db),
				expired: true,
				revoked: false,
				grace: Duration::from_days(90).unwrap(),
			})))
		);
	}
	// Revoked for 90 days
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE PURGE REVOKED FOR 90d"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Purge(AccessStatementPurge {
				ac: Ident::from_strand(strand!("a").to_owned()),
				base: Some(Base::Db),
				expired: false,
				revoked: true,
				grace: Duration::from_days(90).unwrap(),
			})))
		);
	}
	// Invalid for 90 days
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE PURGE REVOKED, EXPIRED FOR 90d"#.as_bytes(),
			ParserSettings {
				bearer_access_enabled: true,
				..Default::default()
			},
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Purge(AccessStatementPurge {
				ac: Ident::from_strand(strand!("a").to_owned()),
				base: Some(Base::Db),
				expired: true,
				revoked: true,
				grace: Duration::from_days(90).unwrap(),
			})))
		);
	}
}
