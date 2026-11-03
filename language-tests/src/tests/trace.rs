//! Query traces in language tests.
//!
//! A test asks for a trace with `[env] trace = ["relations", "links", …]`, the
//! kinds a `query` call's `trace` option names. The query then runs as a traced
//! request, and each statement's trace is graded beside its value: a result
//! written as a table states it under `trace`,
//!
//! ```toml
//! [[test.results]]
//! value = "[person:b]"
//! trace = "{ relations: [[person:a, knows:ab, person:b]] }"
//! ```
//!
//! and a result that states none expects the statement to send no trace at all,
//! so a traced test pins what every statement reports, the empty ones included.
//!
//! A trace is compared in a canonical form: the entries of `relations`,
//! `references`, `links` and `subscriptions` sorted, since the trace orders them
//! by when they were walked; and with uuids compared loosely, since a live
//! query's id differs per run. Duplicates are kept, so an entry reported twice is
//! a mismatch.

use anyhow::{Result, bail};
use surrealdb_rpc::TraceKinds;
use surrealdb_types::{ToSql, Value as SurValue};

use crate::tests::schema::{self, TestConfig, TestDetailsResults};

/// The kinds a test's `[env] trace` names; empty where it names none.
pub fn kinds(config: &TestConfig) -> Result<TraceKinds> {
	let mut kinds = TraceKinds::NONE;
	for name in &config.env.trace {
		let Some(kind) = TraceKinds::from_name(name) else {
			bail!("unknown trace kind `{name}` in `[env] trace`");
		};
		kinds = kinds.union(kind);
	}
	Ok(kinds)
}

/// The trace each statement of a traced test is expected to send, canonical,
/// `None` for none; `None` overall for a test that asks for no trace.
pub fn expected(config: &TestConfig) -> Option<Vec<Option<SurValue>>> {
	if config.env.trace.is_empty() {
		return None;
	}
	let Some(TestDetailsResults::QueryResult(results)) = &config.test.results else {
		return Some(Vec::new());
	};
	Some(
		results
			.iter()
			.map(|result| match result {
				schema::TestExpectation::Value(x) => x.trace.as_ref().map(|t| canonical(t.0.clone())),
				schema::TestExpectation::Match(x) => x.trace.as_ref().map(|t| canonical(t.0.clone())),
				schema::TestExpectation::Plain(_) | schema::TestExpectation::Error(_) => None,
			})
			.collect(),
	)
}

/// A statement's trace as a test states it: canonical, `None` where it sent
/// nothing.
pub fn got(trace: Option<surrealdb_rpc::Trace>) -> Option<SurValue> {
	trace.filter(|trace| !trace.is_empty()).map(|trace| canonical(trace.into_value()))
}

/// `trace` with the entries of each list sorted.
pub fn canonical(trace: SurValue) -> SurValue {
	let SurValue::Object(mut object) = trace else {
		return trace;
	};
	for key in ["relations", "references", "links", "subscriptions"] {
		if let Some(SurValue::Array(entries)) = object.remove(key) {
			let mut entries = entries.into_vec();
			entries.sort_by_cached_key(|entry| entry.to_sql());
			object.insert(key, SurValue::Array(entries.into()));
		}
	}
	SurValue::Object(object)
}

/// Whether two canonical traces agree, uuids compared loosely.
pub fn agrees(expected: Option<&SurValue>, got: Option<&SurValue>) -> bool {
	match (expected, got) {
		(None, None) => true,
		(Some(expected), Some(got)) => {
			let loosely = crate::tests::cmp::RoughlyEqConfig {
				uuid: false,
				..crate::tests::cmp::RoughlyEqConfig::all()
			};
			crate::tests::cmp::RoughlyEq::roughly_equal(expected, got, &loosely)
		}
		_ => false,
	}
}
