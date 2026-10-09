//! A census of the lowering over every statement corpus the tests hold: how
//! many statements lower, and how many the VM declines and why. Run with
//! `--nocapture` to read the histogram.

use std::collections::BTreeMap;

use super::tests::{
	CONTROL_STATEMENTS, COVERED, DEFINED_SCHEMA, EVALUATED_TARGET_WRITES, FLAT_CONTROL_STATEMENTS,
	KILLING_STATEMENTS, SCHEMA, SCHEMA_STATEMENTS, VIEWED_SCHEMA, WRITE_SCHEMA, WRITES, planner,
};
use super::tests_write_data::{DATA_WRITES, ID_SCHEMA, ID_WRITES};
use super::tests_write_observe::{
	LIVE_REDEFINING_WRITES, LIVE_WRITES, LIVES_SCHEMA, OBSERVED_SCHEMA, OBSERVED_WRITES,
};
use super::tests_write_schema::{FIELD_SCHEMA, FIELD_WRITES, GUARDED_SCHEMA, GUARDED_WRITES};
use super::tests_write_storage::{
	CASCADE_SCHEMA, CASCADE_WRITES, DEEP_CASCADE_SCHEMA, DEEP_CASCADE_WRITES, INDEX_SCHEMA,
	INDEX_WRITES, STORAGE_SCHEMA, STORAGE_WRITES, VIEW_OF_VIEW_SCHEMA, VIEW_OF_VIEW_WRITES,
	VIEW_SCHEMA, VIEW_WRITES,
};
use crate::exec::operators::test_util::TestDb;

/// What lowering one statement came to.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Fate {
	/// The statement is not one expression statement the lowering accepts.
	Unparsed,
	/// Binding the statement against the catalog failed.
	Unbound,
	/// The VM declines the statement, for the reason.
	Declined(String),
	/// The statement lowers.
	Lowered,
}

/// How each statement of a corpus fared.
#[derive(Default)]
struct Census {
	fates: BTreeMap<Fate, usize>,
	statements: usize,
}

impl Census {
	fn add(&mut self, fate: Fate) {
		*self.fates.entry(fate).or_default() += 1;
		self.statements += 1;
	}

	fn merge(&mut self, other: &Census) {
		for (fate, count) in &other.fates {
			*self.fates.entry(fate.clone()).or_default() += count;
		}
		self.statements += other.statements;
	}

	fn declined(&self) -> usize {
		self.fates
			.iter()
			.filter(|(fate, _)| matches!(fate, Fate::Declined(_)))
			.map(|(_, count)| count)
			.sum()
	}
}

async fn fate_of(ctx: &crate::exec::ExecutionContext, sql: &str) -> Fate {
	let Ok(ast) = crate::syn::parse(sql) else {
		return Fate::Unparsed;
	};
	let Some(surrealdb_expr::parser::TopLevelExpr::Expr(expr)) = ast.expressions.into_iter().next()
	else {
		return Fate::Unparsed;
	};
	let Ok(op) = crate::expr::logical::lower(expr) else {
		return Fate::Unparsed;
	};
	let mut planner = planner(ctx);
	planner.speculate_with(ctx.options().map(|options| std::sync::Arc::new(options.clone())));
	let Ok(bound) = planner.bind(op).await else {
		return Fate::Unbound;
	};
	let physical = match super::map::map(&planner, &bound).await {
		Ok(physical) => super::optimize::optimize(physical),
		Err(declined) => return Fate::Declined(declined.reason),
	};
	match super::lower::lower(&planner, &physical).await {
		Ok(_) => Fate::Lowered,
		Err(declined) => Fate::Declined(declined.reason),
	}
}

async fn census_of<'a>(schema: &str, statements: impl IntoIterator<Item = &'a str>) -> Census {
	let db = TestDb::new_with_auth(schema).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let mut census = Census::default();
	for sql in statements {
		census.add(fate_of(&ctx, sql).await);
	}
	census
}

/// Lowers every statement of every corpus and prints the histogram of what
/// the VM declines.
#[tokio::test]
async fn the_census_lowers_every_corpus_statement() {
	let id_writes: Vec<&str> = ID_WRITES.iter().map(|(_, sql)| *sql).collect();
	let corpora: Vec<(&str, &str, Vec<&str>)> = vec![
		("COVERED", SCHEMA, COVERED.to_vec()),
		("WRITES", WRITE_SCHEMA, WRITES.to_vec()),
		("EVALUATED_TARGET_WRITES", VIEWED_SCHEMA, EVALUATED_TARGET_WRITES.to_vec()),
		("SCHEMA_STATEMENTS", DEFINED_SCHEMA, SCHEMA_STATEMENTS.to_vec()),
		("CONTROL_STATEMENTS", DEFINED_SCHEMA, CONTROL_STATEMENTS.to_vec()),
		("FLAT_CONTROL_STATEMENTS", DEFINED_SCHEMA, FLAT_CONTROL_STATEMENTS.to_vec()),
		(
			"KILLING_STATEMENTS",
			"DEFINE TABLE watched; DEFINE TABLE other",
			KILLING_STATEMENTS.to_vec(),
		),
		("DATA_WRITES", VIEWED_SCHEMA, DATA_WRITES.to_vec()),
		("ID_WRITES", ID_SCHEMA, id_writes),
		("FIELD_WRITES", FIELD_SCHEMA, FIELD_WRITES.to_vec()),
		("GUARDED_WRITES", GUARDED_SCHEMA, GUARDED_WRITES.to_vec()),
		("OBSERVED_WRITES", OBSERVED_SCHEMA, OBSERVED_WRITES.to_vec()),
		("LIVE_WRITES", LIVES_SCHEMA, LIVE_WRITES.to_vec()),
		("LIVE_REDEFINING_WRITES", LIVES_SCHEMA, LIVE_REDEFINING_WRITES.to_vec()),
		("STORAGE_WRITES", STORAGE_SCHEMA, STORAGE_WRITES.to_vec()),
		("INDEX_WRITES", INDEX_SCHEMA, INDEX_WRITES.to_vec()),
		("CASCADE_WRITES", CASCADE_SCHEMA, CASCADE_WRITES.to_vec()),
		("DEEP_CASCADE_WRITES", DEEP_CASCADE_SCHEMA, DEEP_CASCADE_WRITES.to_vec()),
		("VIEW_WRITES", VIEW_SCHEMA, VIEW_WRITES.to_vec()),
		("VIEW_OF_VIEW_WRITES", VIEW_OF_VIEW_SCHEMA, VIEW_OF_VIEW_WRITES.to_vec()),
	];
	let mut total = Census::default();
	for (name, schema, statements) in corpora {
		assert!(!statements.is_empty(), "{name} holds no statements");
		let census = census_of(schema, statements).await;
		println!("{name}: {} statements, {} declined", census.statements, census.declined());
		total.merge(&census);
	}
	println!("TOTAL: {} statements, {} declined", total.statements, total.declined());
	for (fate, count) in &total.fates {
		match fate {
			Fate::Declined(reason) => println!("  {count:>5} declined: {reason}"),
			Fate::Unparsed => println!("  {count:>5} unparsed"),
			Fate::Unbound => println!("  {count:>5} unbound"),
			Fate::Lowered => {}
		}
	}
	assert!(total.statements > 0);
}
