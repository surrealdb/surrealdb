use revision::{DeserializeRevisioned, SerializeRevisioned};
use surrealdb_expr::expr::Expr;
use surrealdb_expr::expr::statements::{DefineStatement, DefineUserStatement};
use surrealdb_expr::parser::{self, TopLevelExpr};
use surrealdb_types::ToSql;

fn user(expr: &Expr) -> &DefineUserStatement {
	let Expr::Define(def) = expr else {
		panic!("expected definition")
	};
	let DefineStatement::User(user) = &**def else {
		panic!("expected user definition")
	};
	user
}

#[test]
fn request_credentials_are_inert_and_preparation_is_idempotent() {
	let query =
		parser::parse("DEFINE USER alice ON ROOT PASSWORD 'test secret' ROLES OWNER").unwrap();
	let TopLevelExpr::Expr(mut expr) = query.expressions.into_iter().next().unwrap() else {
		panic!("expected expression")
	};
	let raw = user(&expr);
	assert!(raw.pending_password.is_some());
	assert!(raw.hash.is_empty());
	assert!(raw.code.is_empty());
	assert!(raw.scram.is_none());
	let log = expr.to_redacted_sql();
	assert!(!log.contains("test secret"));
	assert!(!format!("{expr:?}").contains("test secret"));
	assert!(user(&expr).pending_password.is_some());
	expr.prepare_credentials();
	let prepared = user(&expr);
	assert!(prepared.pending_password.is_none());
	assert!(prepared.hash.starts_with("$argon2"));
	assert!(prepared.scram.is_some());
	assert_eq!(prepared.code.len(), 128);
	let saved = expr.clone();
	expr.prepare_credentials();
	assert_eq!(saved, expr);
}

#[test]
fn durable_text_derives_pending_credentials() {
	let expr =
		parser::expr("{ DEFINE USER alice ON ROOT PASSWORD 'durable secret'; RETURN 1 }").unwrap();
	let text = expr.to_stored_sql();
	assert!(!text.contains("durable secret"));
	assert!(text.contains("PASSHASH"));
	let mut bytes = Vec::new();
	expr.serialize_revisioned(&mut bytes).unwrap();
	assert!(!bytes.windows(b"durable secret".len()).any(|x| x == b"durable secret"));
	let restored = Expr::deserialize_revisioned(&mut bytes.as_slice()).unwrap();
	assert!(restored.to_sql().contains("PASSHASH"));
	assert!(!restored.to_sql().contains("durable secret"));
}

#[test]
fn live_identity_is_assigned_at_execution_boundary() {
	let query = parser::parse("LIVE SELECT * FROM person").unwrap();
	let mut stmt = query.expressions.into_iter().next().unwrap();
	let TopLevelExpr::Live(live) = &stmt else {
		panic!("expected live")
	};
	assert!(live.id.is_nil());
	assert!(live.node.is_nil());
	stmt.prepare_credentials();
	let TopLevelExpr::Live(live) = &stmt else {
		panic!("expected live")
	};
	assert!(!live.id.is_nil());
	assert!(!live.node.is_nil());
	let saved = stmt.clone();
	stmt.prepare_credentials();
	assert_eq!(saved, stmt);
}

#[test]
fn alter_user_preserves_password_and_scram_change_rules() {
	use surrealdb_expr::expr::statements::AlterStatement;
	for (clause, pending, clears_scram) in [
		("PASSWORD 'alter secret'", true, false),
		("PASSHASH 'fixed hash'", false, true),
		("PASSWORD 'alter secret' PASSHASH 'fixed hash'", false, true),
		("COMMENT 'unchanged credentials'", false, false),
	] {
		let query = parser::parse(&format!("ALTER USER alice ON ROOT {clause}")).unwrap();
		let TopLevelExpr::Expr(mut expr) = query.expressions.into_iter().next().unwrap() else {
			panic!("expected expression")
		};
		let Expr::Alter(def) = &expr else {
			panic!("expected alter")
		};
		let AlterStatement::User(user) = &**def else {
			panic!("expected user")
		};
		assert_eq!(user.pending_password.is_some(), pending);
		assert_eq!(user.scram == Some(None), clears_scram);
		expr.prepare_credentials();
		let Expr::Alter(def) = &expr else {
			panic!("expected alter")
		};
		let AlterStatement::User(user) = &**def else {
			panic!("expected user")
		};
		assert!(user.pending_password.is_none());
		if pending {
			assert!(user.hash.as_ref().unwrap().starts_with("$argon2"));
			assert!(matches!(user.scram, Some(Some(_))));
		} else if clears_scram {
			assert_eq!(user.hash.as_deref(), Some("fixed hash"));
			assert_eq!(user.scram, Some(None));
		} else {
			assert!(user.hash.is_none());
			assert!(user.scram.is_none());
		}
	}
}

#[test]
fn block_storage_and_path_serialization_derive_nested_credentials() {
	let Expr::Block(block) =
		parser::expr("{ DEFINE USER alice ON ROOT PASSWORD 'block secret'; RETURN 1 }").unwrap()
	else {
		panic!("expected block")
	};
	assert!(!block.to_stored_sql().contains("block secret"));
	assert!(block.to_stored_sql().contains("PASSHASH"));
	let path = parser::idiom("field[(DEFINE USER alice ON ROOT PASSWORD 'path secret')]").unwrap();
	let mut bytes = Vec::new();
	path.serialize_revisioned(&mut bytes).unwrap();
	assert!(!bytes.windows(b"path secret".len()).any(|x| x == b"path secret"));
	assert!(path.to_stored_sql().contains("PASSHASH"));
}

#[test]
fn user_metadata_redaction_does_not_prepare_or_reveal_credentials() {
	let query =
		parser::parse("DEFINE USER alice ON ROOT PASSWORD 'private secret' ROLES OWNER").unwrap();
	let TopLevelExpr::Expr(expr) = &query.expressions[0] else {
		panic!("expected expression")
	};
	let definition = user(expr);
	let saved = definition.clone();
	let mut output = String::new();
	definition.fmt_redacted_sql(&mut output, surrealdb_types::SqlFormat::SingleLine);
	assert!(output.contains("PASSHASH '[REDACTED]'"));
	assert!(!output.contains("private secret"));
	assert_eq!(definition, &saved);
}
