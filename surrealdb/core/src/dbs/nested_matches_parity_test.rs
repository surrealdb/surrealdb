//! Engine parity for a MATCHES (`@@`) operator nested inside a composite WHERE
//! expression.
//!
//! A WHERE-clause MATCHES binds to a query executor only where the registration
//! walk reaches it: through `Expr::Binary` nodes, plus — on the streaming engine
//! only — the operand of a logical NOT. Outside that reach the expression names
//! no index, and the query is refused with `NoIndexFoundForMatch` on both
//! engines. The legacy engine refuses per row, as an unregistered expression is
//! evaluated on a row of a table it iterates; the streaming engine refuses the
//! plan, so its outcome does not depend on which rows the condition reaches.
//!
//! A registration is a property of the expression, not of where it is written:
//! wherever a registered MATCHES appears in the SELECT's own tree — a residual
//! filter, a projection, an IF/ELSE branch, a block statement planned at
//! evaluation time — it resolves against the same index. Three boundaries bound
//! that, and all three are pinned here:
//!
//! - A nested statement keeps its own scope. A MATCHES in a subquery's WHERE registers against that
//!   subquery's own executor and stays answerable.
//! - Registration does not reach outside the statement. A user-defined function or closure body is
//!   planned per call in its own scope, so it inherits nothing from the caller.
//! - The operand of a logical NOT is registered on the streaming engine so the bitmap-fusion
//!   planner's negated full-text branch stays evaluable in the residual filter. The legacy engine
//!   does not register it, so that one shape is refused there and answered here.

use anyhow::Result;
use surrealdb_types::Value;
use test_log::test;

use crate::catalog::providers::CatalogProvider;
use crate::dbs::{NewPlannerStrategy, Session};
use crate::kvs::{Datastore, TransactionType};

/// One full-text index and rows that reach every branch under test: `t:1`
/// matches the search term and takes the `flag = true` branch, so the legacy
/// engine's per-row refusal is actually triggered; `t:2` takes the other one.
const SETUP: &str = "
	DEFINE ANALYZER simple TOKENIZERS blank;
	DEFINE TABLE t SCHEMALESS;
	DEFINE INDEX ft ON t FIELDS content FULLTEXT ANALYZER simple BM25;
	CREATE t:1 SET flag = true,  content = 'alpha bravo';
	CREATE t:2 SET flag = false, content = 'charlie delta';
	CREATE t:3 SET flag = true,  content = 'echo foxtrot';
";

/// The refusal both engines raise for a MATCHES that binds to no executor.
const NO_INDEX: &str = "no suitable index supporting";

/// Run `query` under one engine against the shared fixture, reporting either
/// the record ids it selected or the refusal it produced, so the two engines
/// can be compared on both axes at once.
async fn select_ids(
	strategy: NewPlannerStrategy,
	query: &str,
) -> Result<Result<Vec<String>, String>> {
	let ds = Datastore::new("memory").await?;
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
	}
	let root = Session::owner().with_ns("test").with_db("test");
	for response in ds.execute(SETUP, &root, None).await? {
		response.result?;
	}

	let reader = Session::owner().with_ns("test").with_db("test").new_planner_strategy(strategy);
	let mut responses = ds.execute(query, &reader, None).await?;
	let value = match responses.remove(0).result {
		Ok(value) => value,
		Err(err) => return Ok(Err(err.to_string())),
	};
	let Value::Array(rows) = value else {
		return Ok(Err(format!("expected an array of rows, got {value:?}")));
	};
	let mut ids: Vec<String> =
		rows.into_iter().map(|row| surrealdb_types::ToSql::to_sql(&row)).collect();
	ids.sort();
	Ok(Ok(ids))
}

/// Run `query` under both engines.
async fn both_engines(
	query: &str,
) -> Result<(Result<Vec<String>, String>, Result<Vec<String>, String>)> {
	let legacy = select_ids(NewPlannerStrategy::ComputeOnly, query).await?;
	let streaming = select_ids(NewPlannerStrategy::AllReadOnlyStatements, query).await?;
	Ok((legacy, streaming))
}

/// Assert both engines refused `query` with the same message naming the
/// missing index. The refusal reaches clients verbatim, so the two engines
/// agreeing on the wording is part of the parity, not a detail of it.
fn assert_both_refuse(legacy: Result<Vec<String>, String>, streaming: Result<Vec<String>, String>) {
	let legacy = legacy.expect_err("the legacy engine must refuse a MATCHES that names no index");
	let streaming =
		streaming.expect_err("the streaming engine must refuse a MATCHES that names no index");
	assert!(legacy.contains(NO_INDEX), "legacy refusal does not name the missing index: {legacy}");
	assert_eq!(legacy, streaming, "the two engines word the same refusal differently");
}

/// An IF/ELSE branch is not a nested statement: its body is evaluated against
/// the enclosing SELECT's row, where a MATCHES the WHERE never registered names
/// no index.
#[test(tokio::test(flavor = "multi_thread"))]
async fn both_engines_refuse_a_matches_inside_an_if_else_branch() -> Result<()> {
	let (legacy, streaming) = both_engines(
		"SELECT VALUE id FROM t WHERE (IF flag = true { content @@ 'bravo' } ELSE { true })",
	)
	.await?;
	assert_both_refuse(legacy, streaming);
	Ok(())
}

/// A function argument is walked by neither engine's registration pass, so a
/// MATCHES there names no index on either.
#[test(tokio::test(flavor = "multi_thread"))]
async fn both_engines_refuse_a_matches_inside_a_function_argument() -> Result<()> {
	let (legacy, streaming) =
		both_engines("SELECT VALUE id FROM t WHERE array::any([content @@ 'bravo'])").await?;
	assert_both_refuse(legacy, streaming);
	Ok(())
}

/// The control: a MATCHES written as the whole WHERE registers on both engines
/// and selects the same rows.
#[test(tokio::test(flavor = "multi_thread"))]
async fn both_engines_answer_a_top_level_matches() -> Result<()> {
	let (legacy, streaming) =
		both_engines("SELECT VALUE id FROM t WHERE content @@ 'bravo'").await?;
	assert_eq!(legacy, streaming, "the two engines disagree on a top-level MATCHES");
	assert_eq!(legacy.expect("a top-level MATCHES is answerable"), vec!["t:1".to_string()]);
	Ok(())
}

/// A subquery plans in its own scope: its WHERE registers its own MATCHES, and
/// the outer statement's plan-time check does not reach into it.
#[test(tokio::test(flavor = "multi_thread"))]
async fn both_engines_answer_a_matches_inside_a_subquery() -> Result<()> {
	let (legacy, streaming) = both_engines(
		"SELECT VALUE id FROM t WHERE id IN (SELECT VALUE id FROM t WHERE content @@ 'bravo')",
	)
	.await?;
	assert_eq!(legacy, streaming, "the two engines disagree on a MATCHES inside a subquery");
	assert_eq!(legacy.expect("a subquery's own MATCHES is answerable"), vec!["t:1".to_string()]);
	Ok(())
}

/// Registration is by expression value, not by position: a MATCHES the WHERE
/// registers stays registered where the same expression is written again inside
/// an IF/ELSE branch of that WHERE, so the branch resolves it through the index
/// rather than answering `false`.
#[test(tokio::test(flavor = "multi_thread"))]
async fn both_engines_answer_a_registered_matches_duplicated_inside_a_branch() -> Result<()> {
	let (legacy, streaming) = both_engines(
		"SELECT VALUE id FROM t WHERE content @@ 'bravo' \
		 AND (IF flag = true { content @@ 'bravo' } ELSE { true })",
	)
	.await?;
	assert_eq!(legacy, streaming, "the two engines disagree on a duplicated registered MATCHES");
	assert_eq!(
		legacy.expect("a registered MATCHES resolves wherever it is written"),
		vec!["t:1".to_string()],
	);
	Ok(())
}

/// A projection is planned in the SELECT's own scope, so a block written there
/// resolves a registered MATCHES through the index like any other position in
/// the tree.
#[test(tokio::test(flavor = "multi_thread"))]
async fn both_engines_answer_a_registered_matches_inside_a_projection_block() -> Result<()> {
	let (legacy, streaming) =
		both_engines("SELECT VALUE { RETURN content @@ 'bravo'; } FROM t WHERE content @@ 'bravo'")
			.await?;
	assert_eq!(
		legacy, streaming,
		"the two engines disagree on a registered MATCHES in a projection block"
	);
	assert_eq!(
		legacy.expect("a registered MATCHES resolves in projection position"),
		vec!["true".to_string()],
	);
	Ok(())
}

/// Registration, not position, decides: a MATCHES no WHERE registered still
/// names no index in projection position, and both engines refuse the row it is
/// evaluated on.
#[test(tokio::test(flavor = "multi_thread"))]
async fn both_engines_refuse_an_unregistered_matches_inside_a_projection_block() -> Result<()> {
	let (legacy, streaming) =
		both_engines("SELECT VALUE { RETURN content @@ 'zulu'; } FROM t").await?;
	assert_both_refuse(legacy, streaming);
	Ok(())
}

/// The one shape the two engines answer differently, by design.
///
/// The streaming engine registers the operand of a logical NOT so the
/// bitmap-fusion planner can keep a negated full-text branch in its residual
/// filter; the legacy registration walk stops at the prefix and refuses the
/// query instead. Pinned per engine so the extension is not mistaken for a
/// regression on either side.
#[test(tokio::test(flavor = "multi_thread"))]
async fn engines_differ_on_a_negated_matches() -> Result<()> {
	let (legacy, streaming) =
		both_engines("SELECT VALUE id FROM t WHERE !(content @@ 'bravo')").await?;

	let legacy = legacy.expect_err("the legacy engine does not register the operand of a NOT");
	assert!(legacy.contains(NO_INDEX), "legacy refusal does not name the missing index: {legacy}");

	assert_eq!(
		streaming.expect("the streaming engine registers the operand of a NOT"),
		vec!["t:2".to_string(), "t:3".to_string()],
	);
	Ok(())
}
