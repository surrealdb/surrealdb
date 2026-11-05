//! The index function calls of a run that name one full-text clause share one
//! context, so the index, the query's terms and the scorer are resolved once
//! per run however many calls, and rows, read them. In a run that writes, the
//! context judges each record as the writes before the call left the index.

use std::sync::Arc;

use super::interp::State;
use super::program::{CallSpec, IndexBinding};
use crate::exec::function::{FunctionRegistry, IndexContext, MatchContext};
use crate::expr::logical::FieldPath;

fn call(name: &str, field: &str, query: &str) -> CallSpec {
	CallSpec {
		name: name.to_owned(),
		plan_depth: 0,
		index: Some(IndexBinding::FullText {
			table: "t".into(),
			path: Some(FieldPath::field(field)),
			query: query.to_owned(),
			query_arg: false,
			extra: Vec::new(),
		}),
		closure_args: Vec::new(),
	}
}

fn context(state: &mut State, index: u32, spec: &CallSpec) -> Arc<MatchContext> {
	let registry = FunctionRegistry::with_builtins();
	match state.index_run(index, spec, &registry).expect("the call resolves").context {
		IndexContext::FullText(ctx) => ctx,
		IndexContext::Knn(_) => panic!("a full-text call resolves a full-text context"),
	}
}

#[test]
fn calls_naming_one_clause_share_its_context() {
	let mut state = State::vacant();
	state.read_only = true;
	let score = context(&mut state, 0, &call("search::score", "body", "hello"));
	let highlight = context(&mut state, 1, &call("search::highlight", "body", "hello"));
	let ordered = context(&mut state, 2, &call("search::score", "body", "hello"));
	assert!(Arc::ptr_eq(&score, &highlight), "score and highlight read one clause");
	assert!(Arc::ptr_eq(&score, &ordered), "a second score call reads the same clause");
}

#[test]
fn calls_of_a_run_that_writes_share_their_clauses_context() {
	let mut state = State::vacant();
	state.read_only = false;
	let score = context(&mut state, 0, &call("search::score", "body", "hello"));
	let highlight = context(&mut state, 1, &call("search::highlight", "body", "hello"));
	let next_row = context(&mut state, 0, &call("search::score", "body", "hello"));
	assert!(Arc::ptr_eq(&score, &highlight), "score and highlight read one clause");
	assert!(Arc::ptr_eq(&score, &next_row), "the next row's call reads the same clause");
}

#[test]
fn calls_naming_different_clauses_keep_their_own_contexts() {
	let mut state = State::vacant();
	let hello = context(&mut state, 0, &call("search::score", "body", "hello"));
	let world = context(&mut state, 1, &call("search::score", "body", "world"));
	let title = context(&mut state, 2, &call("search::score", "title", "hello"));
	assert!(!Arc::ptr_eq(&hello, &world), "another query is another clause");
	assert!(!Arc::ptr_eq(&hello, &title), "another field is another clause");
}

/// The context call `index` resolves for `query`, read when the call runs.
fn context_for(state: &mut State, index: u32, spec: &CallSpec, query: &str) -> Arc<MatchContext> {
	let registry = FunctionRegistry::with_builtins();
	let query = [query.to_owned()];
	match state
		.index_run_for(index, spec, &registry, Some(&query))
		.expect("the call resolves")
		.context
	{
		IndexContext::FullText(ctx) => ctx,
		IndexContext::Knn(_) => panic!("a full-text call resolves a full-text context"),
	}
}

#[test]
fn a_run_keeps_only_the_contexts_of_the_queries_its_calls_read() {
	for read_only in [true, false] {
		let mut state = State::vacant();
		state.read_only = read_only;
		let score = call("search::score", "body", "");
		let highlight = call("search::highlight", "body", "");
		for (row, query) in ["one", "two", "three"].into_iter().enumerate() {
			let scored = context_for(&mut state, 0, &score, query);
			let highlighted = context_for(&mut state, 1, &highlight, query);
			assert!(Arc::ptr_eq(&scored, &highlighted), "row {row}: both calls read one clause");
			drop((scored, highlighted));
			assert_eq!(state.match_contexts.len(), 1, "row {row}: the run keeps the clause in use");
		}
		let first = context_for(&mut state, 0, &score, "four");
		assert_eq!(state.match_contexts.len(), 2, "a call still reading the old query keeps it");
		let second = context_for(&mut state, 1, &highlight, "four");
		assert!(Arc::ptr_eq(&first, &second), "the calls share the new query's clause");
		drop((first, second));
		assert_eq!(state.match_contexts.len(), 1, "once no call reads it, the old one is let go");
	}
}
