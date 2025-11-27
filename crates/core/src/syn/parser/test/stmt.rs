use chrono::offset::TimeZone;
use chrono::{NaiveDate, Offset, Utc};

use crate::sql::access::AccessDuration;
use crate::sql::access_type::{
	AccessType, BearerAccess, BearerAccessSubject, BearerAccessType, JwtAccess, JwtAccessIssue,
	JwtAccessVerify, JwtAccessVerifyJwks, JwtAccessVerifyKey, RecordAccess,
};
use crate::sql::changefeed::ChangeFeed;
use crate::sql::data::Assignment;
use crate::sql::field::Selector;
use crate::sql::filter::Filter;
use crate::sql::index::{Distance, FullTextParams, HnswParams, VectorType};
use crate::sql::language::Language;
use crate::sql::literal::ObjectEntry;
use crate::sql::lookup::{LookupKind, LookupSubject};
use crate::sql::order::{OrderList, Ordering};
use crate::sql::statements::access::{
	self, AccessStatementGrant, AccessStatementPurge, AccessStatementRevoke, AccessStatementShow,
	PurgeKind,
};
use crate::sql::statements::define::user::PassType;
use crate::sql::statements::define::{
	DefineAccessStatement, DefineAnalyzerStatement, DefineDatabaseStatement, DefineDefault,
	DefineEventStatement, DefineFieldStatement, DefineFunctionStatement, DefineIndexStatement,
	DefineKind, DefineNamespaceStatement, DefineParamStatement, DefineStatement,
	DefineTableStatement,
};
use crate::sql::statements::live::LiveFields;
use crate::sql::statements::remove::RemoveAnalyzerStatement;
use crate::sql::statements::show::{ShowSince, ShowStatement};
use crate::sql::statements::sleep::SleepStatement;
use crate::sql::statements::{
	AccessStatement, CreateStatement, DeleteStatement, ForeachStatement, IfelseStatement,
	InfoStatement, InsertStatement, KillStatement, OptionStatement, OutputStatement,
	RelateStatement, RemoveAccessStatement, RemoveDatabaseStatement, RemoveEventStatement,
	RemoveFieldStatement, RemoveFunctionStatement, RemoveIndexStatement, RemoveNamespaceStatement,
	RemoveParamStatement, RemoveStatement, RemoveTableStatement, RemoveUserStatement,
	SelectStatement, UpdateStatement, UpsertStatement, UseStatement,
};
use crate::sql::tokenizer::Tokenizer;
use crate::sql::{
	Algorithm, AssignOperator, Base, BinaryOperator, Block, Cond, Data, Dir, Explain, Expr, Fetch,
	Fetchs, Field, Fields, Group, Groups, Idiom, Index, Kind, Limit, Literal, Lookup, Mock, Order,
	Output, Param, Part, Permission, Permissions, RecordIdKeyLit, RecordIdLit, Scoring, Split,
	Splits, Start, TableType, TopLevelExpr, With,
};
use crate::syn;
use crate::syn::parser::ParserSettings;
use crate::types::{PublicDatetime, PublicDuration, PublicUuid};
use crate::val::range::TypedRange;

fn ident_field(name: &str) -> Expr {
	Expr::Idiom(Idiom(vec![Part::Field(name.to_string())]))
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
			what: vec![Expr::Table("foo".to_owned())],
			data: Some(Data::SetExpression(vec![
				Assignment {
					place: Idiom(vec![Part::Field("bar".to_owned())]),
					operator: AssignOperator::Assign,
					value: Expr::Literal(Literal::Integer(3))
				},
				Assignment {
					place: Idiom(vec![Part::Field("foo".to_owned())]),
					operator: AssignOperator::Extend,
					value: ident_field("baz")
				},
			])),
			output: Some(Output::Fields(Fields::Value(Box::new(Selector {
				expr: ident_field("foo"),
				alias: Some(Idiom(vec![Part::Field("bar".to_string())])),
			})))),
			timeout: Expr::Literal(Literal::Duration(PublicDuration::from_secs(1))),
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
			name: Expr::Idiom(Idiom::field("a".to_string())),
			comment: Expr::Literal(Literal::String("test".to_string())),
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
			name: Expr::Idiom(Idiom::field("a".to_string())),
			comment: Expr::Literal(Literal::None),
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
			name: Expr::Idiom(Idiom::field("a".to_string())),
			strict: false,
			comment: Expr::Literal(Literal::String("test".to_string())),
			changefeed: Some(ChangeFeed {
				expiry: PublicDuration::from_secs(60 * 10),
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
			name: Expr::Idiom(Idiom::field("a".to_string())),
			strict: false,
			comment: Expr::Literal(Literal::None),
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
			name: "foo::bar".to_owned(),
			args: vec![
				("a".to_owned(), Kind::Number),
				("b".to_owned(), Kind::Array(Box::new(Kind::Bool), Some(3)))
			],
			block: Block(vec![Expr::Return(Box::new(OutputStatement {
				what: ident_field("a"),
				fetch: None,
			}))]),
			comment: Expr::Literal(Literal::String("test".to_string())),
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

		assert_eq!(stmt.name, Expr::Idiom(Idiom::field("user".to_string())));
		assert_eq!(stmt.base, Base::Root);
		assert_eq!(stmt.pass_type, PassType::Password("hunter2".to_owned()));
		assert_eq!(stmt.roles, vec!["Viewer".to_string()]);
		assert_eq!(stmt.comment, Expr::Literal(Literal::String("*******".to_string())));
		assert_eq!(
			stmt.token_duration,
			Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap()))
		);
		assert_eq!(stmt.session_duration, Expr::Literal(Literal::None));
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

		assert_eq!(stmt.name, Expr::Idiom(Idiom::field("user".to_string())));
		assert_eq!(stmt.base, Base::Root);
		assert_eq!(stmt.pass_type, PassType::Hash("hunter2".to_owned()));
		assert_eq!(stmt.roles, vec!["Viewer".to_string()]);
		assert_eq!(stmt.comment, Expr::Literal(Literal::String("*******".to_string())));
		assert_eq!(
			stmt.token_duration,
			Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap()))
		);
		assert_eq!(stmt.session_duration, Expr::Literal(Literal::None));
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

		assert_eq!(stmt.name, Expr::Idiom(Idiom::field("user".to_string())));
		assert_eq!(stmt.base, Base::Root);
		assert_eq!(stmt.pass_type, PassType::Hash("hunter2".to_owned()));
		assert_eq!(stmt.roles, vec!["editor".to_string(), "OWNER".to_string()]);
		assert_eq!(
			stmt.token_duration,
			Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap()))
		);
		assert_eq!(stmt.session_duration, Expr::Literal(Literal::None));
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

		assert_eq!(stmt.name, Expr::Idiom(Idiom::field("user".to_string())));
		assert_eq!(stmt.base, Base::Root);
		assert_eq!(stmt.pass_type, PassType::Hash("hunter2".to_owned()));
		assert_eq!(stmt.roles, vec!["Viewer".to_string()]);
		assert_eq!(
			stmt.token_duration,
			Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap()))
		);
		assert_eq!(
			stmt.session_duration,
			Expr::Literal(Literal::Duration(PublicDuration::from_hours(6).unwrap()))
		);
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

		assert_eq!(stmt.name, Expr::Idiom(Idiom::field("user".to_string())));
		assert_eq!(stmt.base, Base::Root);
		assert_eq!(stmt.pass_type, PassType::Hash("hunter2".to_owned()));
		assert_eq!(stmt.roles, vec!["Viewer".to_string()]);
		assert_eq!(
			stmt.token_duration,
			Expr::Literal(Literal::Duration(PublicDuration::from_mins(15).unwrap()))
		);
		assert_eq!(
			stmt.session_duration,
			Expr::Literal(Literal::Duration(PublicDuration::from_hours(6).unwrap()))
		);
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
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::EdDSA,
						key: Expr::Literal(Literal::String("foo".to_string())),
					}),
					issue: None,
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
					session: Expr::Literal(Literal::None),
				},
				comment: Expr::Literal(Literal::String("bar".to_string())),
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
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::EdDSA,
						key: Expr::Literal(Literal::String("foo".to_string())),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::EdDSA,
						key: Expr::Literal(Literal::String("bar".to_string())),
					}),
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
					session: Expr::Literal(Literal::None),
				},
				comment: Expr::Literal(Literal::None),
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
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::EdDSA,
						key: Expr::Literal(Literal::String("foo".to_string())),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::EdDSA,
						key: Expr::Literal(Literal::String("bar".to_string())),
					}),
				}),
				authenticate: Some(Expr::Literal(Literal::Bool(true))),
				// Default durations.
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
					session: Expr::Literal(Literal::None),
				},
				comment: Expr::Literal(Literal::None)
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
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::Hs256,
						key: Expr::Literal(Literal::String("foo".to_string())),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Hs256,
						key: Expr::Literal(Literal::String("foo".to_string())),
					}),
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
					session: Expr::Literal(Literal::None),
				},
				comment: Expr::Literal(Literal::None),
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
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::Hs256,
						key: Expr::Literal(Literal::String("foo".to_string())),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Hs256,
						key: Expr::Literal(Literal::String("foo".to_string())),
					}),
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_secs(10))),
					session: Expr::Literal(Literal::None),
				},
				comment: Expr::Literal(Literal::None),
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
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::Hs256,
						key: Expr::Literal(Literal::String("foo".to_string())),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Hs256,
						key: Expr::Literal(Literal::String("foo".to_string())),
					}),
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
					session: Expr::Literal(Literal::None),
				},
				comment: Expr::Literal(Literal::None),
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
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Ns,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::EdDSA,
						key: Expr::Literal(Literal::String("foo".to_string())),
					}),
					issue: None,
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
					session: Expr::Literal(Literal::None),
				},
				comment: Expr::Literal(Literal::String("bar".to_string())),
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
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Root,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::EdDSA,
						key: Expr::Literal(Literal::String("foo".to_string())),
					}),
					issue: None,
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
					session: Expr::Literal(Literal::None),
				},
				comment: Expr::Literal(Literal::String("bar".to_string())),
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
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Jwks(JwtAccessVerifyJwks {
						url: Expr::Literal(Literal::String(
							"http://example.com/.well-known/jwks.json".to_string()
						)),
					}),
					issue: None,
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
					session: Expr::Literal(Literal::None),
				},
				comment: Expr::Literal(Literal::String("bar".to_string())),
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
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Jwks(JwtAccessVerifyJwks {
						url: Expr::Literal(Literal::String(
							"http://example.com/.well-known/jwks.json".to_string()
						)),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Hs384,
						key: Expr::Literal(Literal::String("foo".to_string())),
					}),
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
					session: Expr::Literal(Literal::None),
				},
				comment: Expr::Literal(Literal::None),
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
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Jwks(JwtAccessVerifyJwks {
						url: Expr::Literal(Literal::String(
							"http://example.com/.well-known/jwks.json".to_string()
						)),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Hs384,
						key: Expr::Literal(Literal::String("foo".to_string())),
					}),
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_secs(10))),
					session: Expr::Literal(Literal::None),
				},
				comment: Expr::Literal(Literal::None),
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
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Jwks(JwtAccessVerifyJwks {
						url: Expr::Literal(Literal::String(
							"http://example.com/.well-known/jwks.json".to_string()
						)),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Ps256,
						key: Expr::Literal(Literal::String("foo".to_string())),
					}),
				}),
				authenticate: None,
				// Default durations.
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
					session: Expr::Literal(Literal::None),
				},
				comment: Expr::Literal(Literal::None),
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
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Jwt(JwtAccess {
					verify: JwtAccessVerify::Jwks(JwtAccessVerifyJwks {
						url: Expr::Literal(Literal::String(
							"http://example.com/.well-known/jwks.json".to_string()
						)),
					}),
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Ps256,
						key: Expr::Literal(Literal::String("foo".to_string())),
					}),
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_secs(10))),
					session: Expr::Literal(Literal::Duration(
						PublicDuration::from_days(2).unwrap()
					)),
				},
				comment: Expr::Literal(Literal::None),
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

		assert_eq!(stmt.name, Expr::Idiom(Idiom::field("a".to_string())));
		assert_eq!(stmt.base, Base::Db);
		assert_eq!(stmt.authenticate, None);
		assert_eq!(
			stmt.duration,
			// Default durations.
			AccessDuration {
				grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
				token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
				session: Expr::Literal(Literal::None),
			}
		);
		assert_eq!(stmt.comment, Expr::Literal(Literal::String("bar".to_string())));
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
			ParserSettings::default(),
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

		assert_eq!(stmt.name, Expr::Idiom(Idiom::field("a".to_string())));
		assert_eq!(stmt.base, Base::Db);
		assert_eq!(stmt.authenticate, None);
		assert_eq!(
			stmt.duration,
			// Default durations.
			AccessDuration {
				grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(10).unwrap())),
				token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
				session: Expr::Literal(Literal::None),
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

		assert_eq!(stmt.name, Expr::Idiom(Idiom::field("a".to_string())));
		assert_eq!(stmt.base, Base::Db);
		assert_eq!(stmt.authenticate, Some(Expr::Literal(Literal::Bool(true))));
		assert_eq!(
			stmt.duration,
			AccessDuration {
				grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
				token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
				session: Expr::Literal(Literal::Duration(PublicDuration::from_days(7).unwrap())),
			}
		);
		assert_eq!(stmt.comment, Expr::Literal(Literal::None));
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
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Record(RecordAccess {
					signup: None,
					signin: None,
					jwt: JwtAccess {
						verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
							alg: Algorithm::Hs384,
							key: Expr::Literal(Literal::String("foo".to_string())),
						}),
						issue: Some(JwtAccessIssue {
							alg: Algorithm::Hs384,
							// Issuer key matches verification key by default in symmetric
							// algorithms.
							key: Expr::Literal(Literal::String("foo".to_string())),
						}),
					},
					bearer: None,
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_secs(10))),
					session: Expr::Literal(Literal::Duration(
						PublicDuration::from_mins(15).unwrap()
					)),
				},
				comment: Expr::Literal(Literal::None),
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
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Record(RecordAccess {
					signup: None,
					signin: None,
					jwt: JwtAccess {
						verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
							alg: Algorithm::Ps512,
							key: Expr::Literal(Literal::String("foo".to_string())),
						}),
						issue: Some(JwtAccessIssue {
							alg: Algorithm::Ps512,
							key: Expr::Literal(Literal::String("bar".to_string())),
						}),
					},
					bearer: None,
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_secs(10))),
					session: Expr::Literal(Literal::Duration(
						PublicDuration::from_mins(15).unwrap()
					)),
				},
				comment: Expr::Literal(Literal::None),
			}))),
		);
	}
	// Verification and issuing with JWT are explicitly defined with two different
	// keys. Refresh specified before JWT.
	{
		let res = syn::parse_with_settings(
			r#"DEFINE ACCESS a ON DB TYPE RECORD WITH REFRESH WITH JWT ALGORITHM PS512 KEY "foo" WITH ISSUER KEY "bar" DURATION FOR GRANT 10d, FOR TOKEN 10s, FOR SESSION 15m"#.as_bytes(),
			ParserSettings::default(),
			async |p,s| p.parse_expr_inherit(s).await,
		)
			.unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Record(RecordAccess {
					signup: None,
					signin: None,
					jwt: JwtAccess {
						verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
							alg: Algorithm::Ps512,
							key: Expr::Literal(Literal::String("foo".to_string())),
						}),
						issue: Some(JwtAccessIssue {
							alg: Algorithm::Ps512,
							key: Expr::Literal(Literal::String("bar".to_string())),
						}),
					},
					bearer: Some(BearerAccess {
						kind: BearerAccessType::Refresh,
						subject: BearerAccessSubject::Record,
						jwt: JwtAccess {
							verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
								alg: Algorithm::Ps512,
								key: Expr::Literal(Literal::String("foo".to_string())),
							}),
							issue: Some(JwtAccessIssue {
								alg: Algorithm::Ps512,
								key: Expr::Literal(Literal::String("bar".to_string())),
							}),
						},
					}),
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(10).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_secs(10))),
					session: Expr::Literal(Literal::Duration(
						PublicDuration::from_mins(15).unwrap()
					)),
				},
				comment: Expr::Literal(Literal::None),
			}))),
		);
	}
	// Verification and issuing with JWT are explicitly defined with two different
	// keys. Refresh specified after JWT.
	{
		let res = syn::parse_with_settings(
			r#"DEFINE ACCESS a ON DB TYPE RECORD WITH JWT ALGORITHM PS512 KEY "foo" WITH ISSUER KEY "bar" WITH REFRESH DURATION FOR GRANT 10d, FOR TOKEN 10s, FOR SESSION 15m"#.as_bytes(),
			ParserSettings::default(),
			async |p,s| p.parse_expr_inherit(s).await,
		).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Record(RecordAccess {
					signup: None,
					signin: None,
					jwt: JwtAccess {
						verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
							alg: Algorithm::Ps512,
							key: Expr::Literal(Literal::String("foo".to_string())),
						}),
						issue: Some(JwtAccessIssue {
							alg: Algorithm::Ps512,
							key: Expr::Literal(Literal::String("bar".to_string())),
						}),
					},
					bearer: Some(BearerAccess {
						kind: BearerAccessType::Refresh,
						subject: BearerAccessSubject::Record,
						jwt: JwtAccess {
							verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
								alg: Algorithm::Ps512,
								key: Expr::Literal(Literal::String("foo".to_string())),
							}),
							issue: Some(JwtAccessIssue {
								alg: Algorithm::Ps512,
								key: Expr::Literal(Literal::String("bar".to_string())),
							}),
						},
					}),
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(10).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_secs(10))),
					session: Expr::Literal(Literal::Duration(
						PublicDuration::from_mins(15).unwrap()
					)),
				},
				comment: Expr::Literal(Literal::None),
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
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Record(RecordAccess {
					signup: None,
					signin: None,
					jwt: JwtAccess {
						verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
							alg: Algorithm::Rs256,
							key: Expr::Literal(Literal::String("foo".to_string())),
						}),
						issue: Some(JwtAccessIssue {
							alg: Algorithm::Rs256,
							key: Expr::Literal(Literal::String("bar".to_string())),
						}),
					},
					bearer: None,
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_secs(10))),
					session: Expr::Literal(Literal::Duration(
						PublicDuration::from_mins(15).unwrap()
					)),
				},
				comment: Expr::Literal(Literal::None),
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
			ParserSettings::default(),
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

		assert_eq!(stmt.name, Expr::Idiom(Idiom::field("a".to_string())));
		assert_eq!(stmt.base, Base::Db);
		assert_eq!(stmt.authenticate, None);
		assert_eq!(
			stmt.duration,
			// Default durations.
			AccessDuration {
				grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
				token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
				session: Expr::Literal(Literal::None),
			}
		);
		assert_eq!(stmt.comment, Expr::Literal(Literal::String("foo".to_string())));
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
			ParserSettings::default(),
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

		assert_eq!(stmt.name, Expr::Idiom(Idiom::field("a".to_string())));
		assert_eq!(stmt.base, Base::Ns);
		assert_eq!(stmt.authenticate, None);
		assert_eq!(
			stmt.duration,
			// Default durations.
			AccessDuration {
				grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
				token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
				session: Expr::Literal(Literal::None),
			}
		);
		assert_eq!(stmt.comment, Expr::Literal(Literal::String("foo".to_string())));
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
			ParserSettings::default(),
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

		assert_eq!(stmt.name, Expr::Idiom(Idiom::field("a".to_string())));
		assert_eq!(stmt.base, Base::Root);
		assert_eq!(stmt.authenticate, None);
		assert_eq!(
			stmt.duration,
			// Default durations.
			AccessDuration {
				grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
				token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
				session: Expr::Literal(Literal::None),
			}
		);
		assert_eq!(stmt.comment, Expr::Literal(Literal::String("foo".to_string())));
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
			ParserSettings::default(),
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

		assert_eq!(stmt.name, Expr::Idiom(Idiom::field("a".to_string())));
		assert_eq!(stmt.base, Base::Db);
		assert_eq!(
			stmt.duration,
			// Default durations.
			AccessDuration {
				grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(30).unwrap())),
				token: Expr::Literal(Literal::Duration(PublicDuration::from_hours(1).unwrap())),
				session: Expr::Literal(Literal::None),
			}
		);
		assert_eq!(stmt.comment, Expr::Literal(Literal::String("foo".to_string())));
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
			ParserSettings::default(),
			async |p, s| p.parse_expr_inherit(s).await,
		)
		.unwrap_err();
	}
	// For record on root.
	{
		syn::parse_with_settings(
			r#"DEFINE ACCESS a ON ROOT TYPE BEARER FOR RECORD COMMENT "foo""#.as_bytes(),
			ParserSettings::default(),
			async |p, s| p.parse_expr_inherit(s).await,
		)
		.unwrap_err();
	}
	// For user. Grant, session and token duration. With JWT.
	{
		let res = syn::parse_with_settings(
			r#"DEFINE ACCESS a ON DB TYPE BEARER FOR USER WITH JWT ALGORITHM HS384 KEY "foo" DURATION FOR GRANT 90d, FOR TOKEN 10s, FOR SESSION 15m"#.as_bytes(),
			ParserSettings::default(),
			async |p,s| p.parse_expr_inherit(s).await,
		).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Bearer(BearerAccess {
					kind: BearerAccessType::Bearer,
					subject: BearerAccessSubject::User,
					jwt: JwtAccess {
						verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
							alg: Algorithm::Hs384,
							key: Expr::Literal(Literal::String("foo".to_string())),
						}),
						issue: Some(JwtAccessIssue {
							alg: Algorithm::Hs384,
							// Issuer key matches verification key by default in symmetric
							// algorithms.
							key: Expr::Literal(Literal::String("foo".to_string())),
						}),
					},
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(90).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_secs(10))),
					session: Expr::Literal(Literal::Duration(PublicDuration::from_secs(900))),
				},
				comment: Expr::Literal(Literal::None),
			}))),
		)
	}
	// For record. Grant, session and token duration. With JWT.
	{
		let res = syn::parse_with_settings(
			r#"DEFINE ACCESS a ON DB TYPE BEARER FOR RECORD WITH JWT ALGORITHM HS384 KEY "foo" DURATION FOR GRANT 90d, FOR TOKEN 10s, FOR SESSION 15m"#.as_bytes(),
			ParserSettings::default(),
			async |p,s| p.parse_expr_inherit(s).await,
		).unwrap();
		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Access(DefineAccessStatement {
				kind: DefineKind::Default,
				name: Expr::Idiom(Idiom::field("a".to_string())),
				base: Base::Db,
				access_type: AccessType::Bearer(BearerAccess {
					kind: BearerAccessType::Bearer,
					subject: BearerAccessSubject::Record,
					jwt: JwtAccess {
						verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
							alg: Algorithm::Hs384,
							key: Expr::Literal(Literal::String("foo".to_string())),
						}),
						issue: Some(JwtAccessIssue {
							alg: Algorithm::Hs384,
							// Issuer key matches verification key by default in symmetric
							// algorithms.
							key: Expr::Literal(Literal::String("foo".to_string())),
						}),
					},
				}),
				authenticate: None,
				duration: AccessDuration {
					grant: Expr::Literal(Literal::Duration(PublicDuration::from_days(90).unwrap())),
					token: Expr::Literal(Literal::Duration(PublicDuration::from_secs(10))),
					session: Expr::Literal(Literal::Duration(PublicDuration::from_secs(900))),
				},
				comment: Expr::Literal(Literal::None),
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
			name: "a".to_owned(),
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
			comment: Expr::Literal(Literal::None),
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
			name: Expr::Idiom(Idiom::field("name".to_string())),
			drop: true,
			full: true,
			view: Some(crate::sql::View {
				expr: Fields::Select(vec![Field::Single(Selector {
					expr: ident_field("foo"),
					alias: None,
				})],),
				what: vec!["bar".to_string()],
				cond: None,
				group: Some(Groups(vec![Group(Idiom(vec![Part::Field("foo".to_string())]))]))
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
				expiry: PublicDuration::from_secs(1),
				store_diff: true,
			}),
			comment: Expr::Literal(Literal::None),

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
			name: Expr::Idiom(Idiom::field("event".to_string())),
			target_table: Expr::Idiom(Idiom::field("table".to_string())),
			when: Expr::Literal(Literal::Null),
			then: vec![Expr::Literal(Literal::Null), Expr::Literal(Literal::None)],
			comment: Expr::Literal(Literal::None),
		})))
	)
}

#[test]
fn parse_define_field() {
	// General
	{
		let res = syn::parse_with(r#"DEFINE FIELD foo.*[*]... ON TABLE bar TYPE option<number | array<record<foo>,10>> VALUE null ASSERT true DEFAULT false PERMISSIONS FOR UPDATE NONE, FOR CREATE WHERE true"#.as_bytes(),async |parser,stk| parser. parse_expr_inherit(stk).await).unwrap();

		assert_eq!(
			res,
			Expr::Define(Box::new(DefineStatement::Field(DefineFieldStatement {
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
					Kind::Array(Box::new(Kind::Record(vec!["foo".to_string()])), Some(10))
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
			})))
		)
	}

	// Invalid DELETE permission
	{
		syn::parse_with(
			r#"DEFINE FIELD foo ON TABLE bar PERMISSIONS FOR DELETE NONE"#.as_bytes(),
			async |parser, stk| parser.parse_expr_inherit(stk).await,
		)
		.unwrap_err();
	}
}

#[test]
fn parse_define_index() {
	let res = syn::parse_with(
		"DEFINE INDEX index ON TABLE table FIELDS a FULLTEXT ANALYZER ana BM25 (0.1,0.2) HIGHLIGHTS"
		.as_bytes(),
		async |parser, stk| parser.parse_expr_inherit(stk).await,
	)
	.unwrap();
	assert_eq!(
		res,
		Expr::Define(Box::new(DefineStatement::Index(DefineIndexStatement {
			kind: DefineKind::Default,
			name: Expr::Idiom(Idiom::field("index".to_string())),
			what: Expr::Idiom(Idiom::field("table".to_string())),
			cols: vec![Expr::Idiom(Idiom(vec![Part::Field("a".to_string())])),],
			index: Index::FullText(FullTextParams {
				az: "ana".to_owned(),
				hl: true,
				sc: Scoring::Bm {
					k1: 0.1,
					b: 0.2
				},
			}),
			comment: Expr::Literal(Literal::None),
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
			name: Expr::Idiom(Idiom::field("index".to_string())),
			what: Expr::Idiom(Idiom::field("table".to_string())),
			cols: vec![Expr::Idiom(Idiom(vec![Part::Field("a".to_string())]))],
			index: Index::Uniq,
			comment: Expr::Literal(Literal::None),
			concurrently: false
		})))
	);

	let res =
		syn::parse_with( r#"DEFINE INDEX index ON TABLE table FIELDS a HNSW DIMENSION 128 EFC 250 TYPE F32 DISTANCE MANHATTAN M 6 M0 12 LM 0.5 EXTEND_CANDIDATES KEEP_PRUNED_CONNECTIONS"#.as_bytes(),async |parser,stk| parser.parse_expr_inherit(stk).await).unwrap();
	assert_eq!(
		res,
		Expr::Define(Box::new(DefineStatement::Index(DefineIndexStatement {
			kind: DefineKind::Default,
			name: Expr::Idiom(Idiom::field("index".to_string())),
			what: Expr::Idiom(Idiom::field("table".to_string())),
			cols: vec![Expr::Idiom(Idiom(vec![Part::Field("a".to_string())]))],
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
			comment: Expr::Literal(Literal::None),
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
			what: vec![Expr::Mock(Mock::Range("foo".to_string(), TypedRange::from_range(32..64)))],
			with: Some(With::Index(vec!["index".to_string(), "index_2".to_string()])),
			cond: Some(Cond(Expr::Literal(Literal::Integer(2)))),
			output: Some(Output::After),
			timeout: Expr::Literal(Literal::Duration(PublicDuration::from_secs(1))),
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
			param: Param::new("foo".to_owned()),
			range: Expr::Binary {
				left: Box::new(Expr::Select(Box::new(SelectStatement {
					expr: Fields::Select(vec![Field::Single(Selector {
						expr: ident_field("foo"),
						alias: None
					})],),
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
					version: None,
					timeout: Expr::Literal(Literal::None),
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
		Expr::IfElse(Box::new(IfelseStatement {
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
		Expr::IfElse(Box::new(IfelseStatement {
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
			Expr::Idiom(Idiom::field("table".to_string())),
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
			Expr::Idiom(Idiom::field("user".to_string())),
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
			Expr::Idiom(Idiom::field("user".to_string())),
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
				Field::Single(Selector {
					expr: ident_field("bar"),
					alias: Some(Idiom(vec![Part::Field("foo".to_owned())])),
				}),
				Field::Single(Selector {
					expr: Expr::Literal(Literal::Array(vec![
						Expr::Literal(Literal::Integer(1)),
						Expr::Literal(Literal::Integer(2))
					])),
					alias: None,
				}),
				Field::Single(Selector {
					expr: ident_field("bar"),
					alias: None,
				}),
			],),
			omit: vec![Expr::Idiom(Idiom(vec![Part::Field("bar".to_string())]))],
			only: true,
			what: vec![Expr::Table("a".to_owned()), Expr::Literal(Literal::Integer(1))],
			with: Some(With::Index(vec!["index".to_owned(), "index_2".to_owned()])),
			cond: Some(Cond(Expr::Literal(Literal::Bool(true)))),
			split: Some(Splits(vec![
				Split(Idiom::field("foo".to_owned())),
				Split(Idiom::field("bar".to_owned())),
			])),
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
				value: Expr::Literal(Literal::Bool(true))
			}])))),
			fetch: Some(Fetchs(vec![Fetch(ident_field("foo"))])),
			version: Some(Expr::Literal(Literal::Datetime(PublicDatetime::from(
				expected_datetime
			)))),
			timeout: Expr::Literal(Literal::None),
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
			table: Some("foo".to_owned()),
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
			since: ShowSince::Timestamp(PublicDatetime::from(expected_datetime)),
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
		duration: PublicDuration::from_secs(1),
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
	let expect = TopLevelExpr::Use(UseStatement::Ns("foo".to_owned()));
	assert_eq!(res, expect);

	let res = syn::parse_with(r"USE NS foo".as_bytes(), async |parser, stk| {
		parser.parse_query(stk).await
	})
	.unwrap()
	.expressions
	.pop()
	.unwrap();
	let expect = TopLevelExpr::Use(UseStatement::Ns("foo".to_owned()));
	assert_eq!(res, expect);

	let res = syn::parse_with(r"USE NS bar DB foo".as_bytes(), async |parser, stk| {
		parser.parse_query(stk).await
	})
	.unwrap()
	.expressions
	.pop()
	.unwrap();

	let expect = TopLevelExpr::Use(UseStatement::NsDb("bar".to_owned(), "foo".to_owned()));
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
	let expect = TopLevelExpr::Use(UseStatement::Ns("foo".to_owned()));
	assert_eq!(res, expect);

	let res = syn::parse_with(r"use db foo".as_bytes(), async |parser, stk| {
		parser.parse_query(stk).await
	})
	.unwrap()
	.expressions
	.pop()
	.unwrap();

	let expect = TopLevelExpr::Use(UseStatement::Db("foo".to_owned()));
	assert_eq!(res, expect);

	let res = syn::parse_with(r"use ns bar db foo".as_bytes(), async |parser, stk| {
		parser.parse_query(stk).await
	})
	.unwrap()
	.expressions
	.pop()
	.unwrap();

	let expect = TopLevelExpr::Use(UseStatement::NsDb("bar".to_owned(), "foo".to_owned()));
	assert_eq!(res, expect);
}

#[test]
fn parse_value_stmt() {
	let res =
		syn::parse_with(r"1s".as_bytes(), async |parser, stk| parser.parse_expr_inherit(stk).await)
			.unwrap();
	let expect = Expr::Literal(Literal::Duration(PublicDuration::from_secs(1)));
	assert_eq!(res, expect);
}

#[test]
fn parse_throw() {
	let res = syn::parse_with(r"THROW 1s".as_bytes(), async |parser, stk| {
		parser.parse_expr_inherit(stk).await
	})
	.unwrap();

	let expect =
		Expr::Throw(Box::new(Expr::Literal(Literal::Duration(PublicDuration::from_secs(1)))));
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
			into: Some(Expr::Param(Param::new("foo".to_owned()))),
			data: Data::ValuesExpression(vec![
				vec![
					(Idiom::field("a".to_owned()), Expr::Literal(Literal::Integer(1)),),
					(Idiom::field("b".to_owned()), Expr::Literal(Literal::Integer(2)),),
					(Idiom::field("c".to_owned()), Expr::Literal(Literal::Integer(3)),),
				],
				vec![
					(Idiom::field("a".to_owned()), Expr::Literal(Literal::Integer(4)),),
					(Idiom::field("b".to_owned()), Expr::Literal(Literal::Integer(5)),),
					(Idiom::field("c".to_owned()), Expr::Literal(Literal::Integer(6)),),
				],
			]),
			ignore: true,
			update: Some(Data::UpdateExpression(vec![
				Assignment {
					place: Idiom(vec![Part::Field("a".to_owned()), Part::Field("b".to_owned()),]),
					operator: crate::sql::AssignOperator::Extend,
					value: Expr::Literal(Literal::Null)
				},
				Assignment {
					place: Idiom(vec![Part::Field("c".to_owned()), Part::Field("d".to_owned()),]),
					operator: crate::sql::AssignOperator::Add,
					value: Expr::Literal(Literal::None)
				},
			])),
			output: Some(Output::After),
			version: None,
			timeout: Expr::Literal(Literal::None),
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
			into: Some(Expr::Table("bar".to_owned())),
			data: Data::SingleExpression(Expr::Select(Box::new(SelectStatement {
				expr: Fields::Select(vec![Field::Single(Selector {
					expr: Expr::Idiom(Idiom(vec![Part::Field("foo".to_owned())])),
					alias: None
				})],),
				omit: vec![],
				only: false,
				what: vec![Expr::Table("baz".to_owned())],
				with: None,
				cond: None,
				split: None,
				group: None,
				order: None,
				limit: None,
				start: None,
				fetch: None,
				version: None,
				timeout: Expr::Literal(Literal::None),
				parallel: false,
				explain: None,
				tempfiles: false
			}))),
			ignore: true,
			update: None,
			output: None,
			version: None,
			timeout: Expr::Literal(Literal::None),
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
			id: Expr::Param(Param::new("param".to_owned()))
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
			id: Expr::Literal(Literal::Uuid(PublicUuid::from(uuid::uuid!(
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
	assert_eq!(stmt.fields, LiveFields::Diff);
	assert_eq!(stmt.what, Expr::Param(Param::new("foo".to_owned())));

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
		LiveFields::Select(Fields::Select(vec![Field::Single(Selector {
			expr: Expr::Idiom(Idiom(vec![Part::Field("foo".to_owned())])),
			alias: None,
		})],))
	);
	assert_eq!(stmt.what, Expr::Table("table".to_owned()));
	assert_eq!(stmt.cond, Some(Cond(Expr::Literal(Literal::Bool(true)))));
	assert_eq!(
		stmt.fetch,
		Some(Fetchs(vec![
			Fetch(Expr::Idiom(Idiom(vec![
				Part::Field("a".to_owned()),
				Part::Where(Expr::Idiom(Idiom(vec![Part::Field("foo".to_owned())]))),
			]))),
			Fetch(Expr::Idiom(Idiom(vec![Part::Field("b".to_owned())]))),
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
			name: "value".to_owned(),
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
				parallel: false,
				version: None,
			})),
			uniq: true,
			data: Some(Data::SetExpression(vec![Assignment {
				place: Idiom(vec![Part::Field("a".to_owned())]),
				operator: AssignOperator::Add,
				value: Expr::Literal(Literal::Integer(1))
			}])),
			output: Some(Output::None),
			timeout: Expr::Literal(Literal::None),
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
			name: Expr::Idiom(Idiom(vec![Part::Field("ns".to_string())])),
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
			name: Expr::Idiom(Idiom(vec![Part::Field("database".to_string())])),
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
			name: "foo::bar".to_owned(),
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
			name: "foo::bar".to_owned(),
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
			name: Expr::Idiom(Idiom(vec![Part::Field("foo".to_string())])),
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
			name: "foo".to_owned(),
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
			name: Expr::Idiom(Idiom(vec![Part::Field("foo".to_string())])),
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
			name: Expr::Idiom(Idiom(vec![Part::Field("foo".to_string())])),
			what: Expr::Idiom(Idiom(vec![Part::Field("bar".to_string())])),
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
			name: Expr::Idiom(Idiom(vec![
				Part::Field("foo".to_string()),
				Part::Field("bar".to_string()),
				Part::Value(Expr::Literal(Literal::Integer(10)))
			])),
			what: Expr::Idiom(Idiom(vec![Part::Field("bar".to_string())])),
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
			name: Expr::Idiom(Idiom(vec![Part::Field("foo".to_string())])),
			what: Expr::Idiom(Idiom(vec![Part::Field("bar".to_string())])),
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
			name: Expr::Idiom(Idiom(vec![Part::Field("foo".to_string())])),
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
			name: Expr::Idiom(Idiom(vec![Part::Field("foo".to_string())])),
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
				Part::Field("a".to_owned()),
				Part::Graph(Lookup {
					kind: LookupKind::Graph(Dir::Out),
					what: vec![LookupSubject::Table {
						table: "b".to_owned(),
						referencing_field: None
					}],
					..Default::default()
				})
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
							referencing_field: None
						}],
						..Default::default()
					})
				]),
				Idiom(vec![Part::Field("c".to_owned()), Part::All])
			])),
			output: Some(Output::Diff),
			timeout: Expr::Literal(Literal::Duration(PublicDuration::from_secs(1))),
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
				Part::Field("a".to_owned()),
				Part::Graph(Lookup {
					kind: LookupKind::Graph(Dir::Out),
					what: vec![LookupSubject::Table {
						table: "b".to_owned(),
						referencing_field: None
					}],
					..Default::default()
				})
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
							referencing_field: None
						}],
						..Default::default()
					})
				]),
				Idiom(vec![Part::Field("c".to_owned()), Part::All])
			])),
			output: Some(Output::Diff),
			timeout: Expr::Literal(Literal::Duration(PublicDuration::from_secs(1))),
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
			ParserSettings::default(),
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Grant(AccessStatementGrant {
				ac: "a".to_owned(),
				base: Some(Base::Ns),
				subject: access::Subject::User("b".to_owned()),
			})))
		);
	}
	// Record
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON NAMESPACE GRANT FOR RECORD b:c"#.as_bytes(),
			ParserSettings::default(),
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Grant(AccessStatementGrant {
				ac: "a".to_owned(),
				base: Some(Base::Ns),
				subject: access::Subject::Record(RecordIdLit {
					table: "b".to_owned(),
					key: RecordIdKeyLit::String("c".to_owned()),
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
			ParserSettings::default(),
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Show(AccessStatementShow {
				ac: "a".to_owned(),
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
			ParserSettings::default(),
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Show(AccessStatementShow {
				ac: "a".to_owned(),
				base: Some(Base::Db),
				gr: Some("b".to_owned()),
				cond: None,
			})))
		);
	}
	// Condition
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE SHOW WHERE true"#.as_bytes(),
			ParserSettings::default(),
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Show(AccessStatementShow {
				ac: "a".to_owned(),
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
			ParserSettings::default(),
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Revoke(AccessStatementRevoke {
				ac: "a".to_owned(),
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
			ParserSettings::default(),
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Revoke(AccessStatementRevoke {
				ac: "a".to_owned(),
				base: Some(Base::Db),
				gr: Some("b".to_owned()),
				cond: None,
			})))
		);
	}
	// Condition
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE REVOKE WHERE true"#.as_bytes(),
			ParserSettings::default(),
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Revoke(AccessStatementRevoke {
				ac: "a".to_owned(),
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
			ParserSettings::default(),
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Purge(AccessStatementPurge {
				ac: "a".to_owned(),
				base: Some(Base::Db),
				kind: PurgeKind::Both,
				grace: PublicDuration::from_millis(0),
			})))
		);
	}
	// Expired
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE PURGE EXPIRED"#.as_bytes(),
			ParserSettings::default(),
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Purge(AccessStatementPurge {
				ac: "a".to_owned(),
				base: Some(Base::Db),
				kind: PurgeKind::Expired,
				grace: PublicDuration::from_millis(0),
			})))
		);
	}
	// Revoked
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE PURGE REVOKED"#.as_bytes(),
			ParserSettings::default(),
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Purge(AccessStatementPurge {
				ac: "a".to_owned(),
				base: Some(Base::Db),
				kind: PurgeKind::Revoked,
				grace: PublicDuration::from_millis(0),
			})))
		);
	}
	// Expired for 90 days
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE PURGE EXPIRED FOR 90d"#.as_bytes(),
			ParserSettings::default(),
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Purge(AccessStatementPurge {
				ac: "a".to_owned(),
				base: Some(Base::Db),
				kind: PurgeKind::Expired,
				grace: PublicDuration::from_days(90).unwrap(),
			})))
		);
	}
	// Revoked for 90 days
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE PURGE REVOKED FOR 90d"#.as_bytes(),
			ParserSettings::default(),
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Purge(AccessStatementPurge {
				ac: "a".to_owned(),
				base: Some(Base::Db),
				kind: PurgeKind::Revoked,
				grace: PublicDuration::from_days(90).unwrap(),
			})))
		);
	}
	// Invalid for 90 days
	{
		let res = syn::parse_with_settings(
			r#"ACCESS a ON DATABASE PURGE REVOKED, EXPIRED FOR 90d"#.as_bytes(),
			ParserSettings::default(),
			async |parser, stk| parser.parse_top_level_expr(stk).await,
		)
		.unwrap();
		assert_eq!(
			res,
			TopLevelExpr::Access(Box::new(AccessStatement::Purge(AccessStatementPurge {
				ac: "a".to_owned(),
				base: Some(Base::Db),
				kind: PurgeKind::Both,
				grace: PublicDuration::from_days(90).unwrap(),
			})))
		);
	}
}
