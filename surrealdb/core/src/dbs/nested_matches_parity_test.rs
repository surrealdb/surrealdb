//! A MATCHES (`@@`) operator nested inside a composite WHERE expression.
//!
//! A WHERE-clause MATCHES binds to a query executor only where the registration
//! walk reaches it: through `Expr::Binary` nodes, plus the operand of a logical
//! NOT. Outside that reach the expression names no index, and the query is
//! refused with `NoIndexFoundForMatch`. The refusal is raised for the plan, so
//! its outcome does not depend on which rows the condition reaches.
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
//! - The operand of a logical NOT is registered so the bitmap-fusion planner's negated full-text
//!   branch stays evaluable in the residual filter.

use anyhow::Result;
use surrealdb_types::Value;
use test_log::test;

use crate::catalog::providers::CatalogProvider;
use crate::dbs::Session;
use crate::kvs::{Datastore, TransactionType};

/// One full-text index and rows that reach every branch under test: `t:1`
/// matches the search term and takes the `flag = true` branch; `t:2` takes the
/// other one.
const SETUP: &str = "
	DEFINE ANALYZER simple TOKENIZERS blank;
	DEFINE TABLE t SCHEMALESS;
	DEFINE INDEX ft ON t FIELDS content FULLTEXT ANALYZER simple BM25;
	CREATE t:1 SET flag = true,  content = 'alpha bravo';
	CREATE t:2 SET flag = false, content = 'charlie delta';
	CREATE t:3 SET flag = true,  content = 'echo foxtrot';
";

/// The refusal for a MATCHES that binds to no executor.
const NO_INDEX: &str = "no suitable index supporting";

/// Run `query` against the shared fixture, reporting either the record ids it
/// selected or the refusal it produced.
async fn select_ids(query: &str) -> Result<Result<Vec<String>, String>> {
	let ds = Datastore::new("memory").await?;
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
	}
	let reader = Session::owner().with_ns("test").with_db("test");
	for response in ds.execute(SETUP, &reader, None).await? {
		response.result?;
	}

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

/// Assert `query` was refused with a message naming the missing index. The
/// refusal reaches clients verbatim, so its wording is part of the contract.
fn assert_refused(outcome: Result<Vec<String>, String>) {
	let refusal = outcome.expect_err("a MATCHES that names no index must be refused");
	assert!(refusal.contains(NO_INDEX), "refusal does not name the missing index: {refusal}");
}

/// An IF/ELSE branch is not a nested statement: its body is evaluated against
/// the enclosing SELECT's row, where a MATCHES the WHERE never registered names
/// no index.
#[test(tokio::test(flavor = "multi_thread"))]
async fn a_matches_inside_an_if_else_branch_is_refused() -> Result<()> {
	let outcome = select_ids(
		"SELECT VALUE id FROM t WHERE (IF flag = true { content @@ 'bravo' } ELSE { true })",
	)
	.await?;
	assert_refused(outcome);
	Ok(())
}

/// A function argument is not walked by the registration pass, so a MATCHES
/// there names no index.
#[test(tokio::test(flavor = "multi_thread"))]
async fn a_matches_inside_a_function_argument_is_refused() -> Result<()> {
	let outcome =
		select_ids("SELECT VALUE id FROM t WHERE array::any([content @@ 'bravo'])").await?;
	assert_refused(outcome);
	Ok(())
}

/// The control: a MATCHES written as the whole WHERE registers and selects the
/// matching rows.
#[test(tokio::test(flavor = "multi_thread"))]
async fn a_top_level_matches_is_answered() -> Result<()> {
	let outcome = select_ids("SELECT VALUE id FROM t WHERE content @@ 'bravo'").await?;
	assert_eq!(outcome.expect("a top-level MATCHES is answerable"), vec!["t:1".to_string()]);
	Ok(())
}

/// A subquery plans in its own scope: its WHERE registers its own MATCHES, and
/// the outer statement's plan-time check does not reach into it.
#[test(tokio::test(flavor = "multi_thread"))]
async fn a_matches_inside_a_subquery_is_answered() -> Result<()> {
	let outcome = select_ids(
		"SELECT VALUE id FROM t WHERE id IN (SELECT VALUE id FROM t WHERE content @@ 'bravo')",
	)
	.await?;
	assert_eq!(outcome.expect("a subquery's own MATCHES is answerable"), vec!["t:1".to_string()]);
	Ok(())
}

/// Registration is by expression value, not by position: a MATCHES the WHERE
/// registers stays registered where the same expression is written again inside
/// an IF/ELSE branch of that WHERE, so the branch resolves it through the index
/// rather than answering `false`.
#[test(tokio::test(flavor = "multi_thread"))]
async fn a_registered_matches_duplicated_inside_a_branch_is_answered() -> Result<()> {
	let outcome = select_ids(
		"SELECT VALUE id FROM t WHERE content @@ 'bravo' \
		 AND (IF flag = true { content @@ 'bravo' } ELSE { true })",
	)
	.await?;
	assert_eq!(
		outcome.expect("a registered MATCHES resolves wherever it is written"),
		vec!["t:1".to_string()],
	);
	Ok(())
}

/// A projection is planned in the SELECT's own scope, so a block written there
/// resolves a registered MATCHES through the index like any other position in
/// the tree.
#[test(tokio::test(flavor = "multi_thread"))]
async fn a_registered_matches_inside_a_projection_block_is_answered() -> Result<()> {
	let outcome =
		select_ids("SELECT VALUE { RETURN content @@ 'bravo'; } FROM t WHERE content @@ 'bravo'")
			.await?;
	assert_eq!(
		outcome.expect("a registered MATCHES resolves in projection position"),
		vec!["true".to_string()],
	);
	Ok(())
}

/// Registration, not position, decides: a MATCHES no WHERE registered still
/// names no index in projection position, and the statement is refused.
#[test(tokio::test(flavor = "multi_thread"))]
async fn an_unregistered_matches_inside_a_projection_block_is_refused() -> Result<()> {
	let outcome = select_ids("SELECT VALUE { RETURN content @@ 'zulu'; } FROM t").await?;
	assert_refused(outcome);
	Ok(())
}

/// The operand of a logical NOT is registered, so the bitmap-fusion planner can
/// keep a negated full-text branch in its residual filter and the statement is
/// answered.
#[test(tokio::test(flavor = "multi_thread"))]
async fn a_negated_matches_is_answered() -> Result<()> {
	let outcome = select_ids("SELECT VALUE id FROM t WHERE !(content @@ 'bravo')").await?;
	assert_eq!(
		outcome.expect("the operand of a NOT is registered"),
		vec!["t:2".to_string(), "t:3".to_string()],
	);
	Ok(())
}
