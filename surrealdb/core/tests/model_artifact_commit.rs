#![recursion_limit = "256"]
//! `REMOVE MODEL` deletes the model's `.surml` artifact only once the removal
//! of its definition has committed, so a transaction that is cancelled leaves
//! the catalog and the object store agreeing.
//!
//! The object store is a process-wide static read from the environment on first
//! use, so this binary pins it to memory before anything touches it and keeps
//! to a single test.

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::{Datastore, QueryRequest};
use surrealdb_core::obs;
use surrealdb_expr::expr::Expr;
use surrealdb_expr::expr::statements::{RemoveModelStatement, RemoveStatement};
use surrealdb_expr::parser::{Query, TopLevelExpr};

/// `REMOVE MODEL` has no SurrealQL spelling, so it runs as an AST.
fn remove_model() -> TopLevelExpr {
	TopLevelExpr::Expr(Expr::Remove(Box::new(RemoveStatement::Model(RemoveModelStatement {
		name: "m".to_string().into(),
		version: "1.0.0".to_string().into(),
		if_exists: false,
	}))))
}

async fn run_ast(
	ds: &Datastore,
	session: &Session,
	expressions: Vec<TopLevelExpr>,
) -> anyhow::Result<()> {
	let results = ds
		.run(QueryRequest::new(
			Query {
				expressions,
			},
			session,
		))
		.await?;
	for r in results {
		r.result?;
	}
	Ok(())
}

async fn run(ds: &Datastore, session: &Session, sql: &str) -> anyhow::Result<()> {
	for response in ds.execute(sql, session, None).await? {
		response.result?;
	}
	Ok(())
}

#[tokio::test]
async fn remove_model_deletes_the_artifact_only_on_commit() -> anyhow::Result<()> {
	// SAFETY: the only test of this binary, set before any other thread reads it.
	unsafe {
		std::env::set_var("SURREAL_OBJECT_STORE", "memory://");
		std::env::set_var("SURREAL_CACHE_STORE", "memory://");
	}
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
	let setup = Session::owner();
	run(&ds, &setup, "DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;").await?;
	let session = Session::owner().with_ns("test").with_db("test");

	let data = b"not a real model".to_vec();
	let hash = obs::hash(&data);
	ds.put_ml_model(&session, "m", "1.0.0", "a model", data).await?;
	let path = format!("ml/test/test/m-1.0.0-{hash}.surml");
	assert!(obs::get(&path).await.is_ok(), "the import stores the artifact");

	// A cancelled transaction rolls the definition's removal back, so the
	// artifact must survive it.
	let cancelled =
		run_ast(&ds, &session, vec![TopLevelExpr::Begin, remove_model(), TopLevelExpr::Cancel])
			.await;
	assert!(cancelled.is_err(), "the statements of a cancelled transaction report it");
	assert!(obs::get(&path).await.is_ok(), "a cancelled REMOVE MODEL keeps the artifact");

	run_ast(&ds, &session, vec![remove_model()]).await?;
	assert!(obs::get(&path).await.is_err(), "a committed REMOVE MODEL deletes the artifact");
	Ok(())
}
