//! The index function calls of a read-only run that name one full-text clause
//! share one context, so the index, the query's terms and the scorer are
//! resolved once per run however many calls read them. In a run that writes,
//! each call keeps its own, which reads the index as the writes before it left
//! it.

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
fn calls_of_a_run_that_writes_keep_their_own_contexts() {
	let mut state = State::vacant();
	state.read_only = false;
	let score = context(&mut state, 0, &call("search::score", "body", "hello"));
	let highlight = context(&mut state, 1, &call("search::highlight", "body", "hello"));
	assert!(!Arc::ptr_eq(&score, &highlight), "a call after a write reads the index anew");
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
