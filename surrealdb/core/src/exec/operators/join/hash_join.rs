//! The row conventions and in-memory build table of the VM's equi-joins: the
//! right side is built into a table keyed by the join key and the left side is
//! probed against it.
//!
//! Two row conventions are served:
//!
//! - **Binding rows**: the binding tables produced by two GQL `MATCH` sub-plans — two
//!   comma-separated patterns inside one `MATCH`, or two sequential `MATCH` clauses — that share
//!   zero or more *node* variables. Each shared node variable is a join key. Each row is a
//!   `Value::Object` keyed by binding name, and the output row is the union of a build row and a
//!   probe row.
//! - **Document rows**: the rows of a SurrealQL pipeline, joined by arbitrary key expressions. The
//!   output row is the probe row with the matched build row added under one top-level key, the
//!   join's *alias* ([`with_alias`], [`with_alias_owned`]).
//!
//! ## Join key — binding rows
//!
//! The join is keyed on `<binding>.id` for each shared binding name, in order
//! ([`join_key`]). A binding row holds a full record object, so `<binding>.id`
//! is its `id` field (a `Value::RecordId`); a bare `Value::RecordId` slot is
//! tolerated defensively. The key is the `Vec<Value>` of those id values, one
//! per key binding. No fetch happens: keys are read straight from the
//! already-bound objects. Because the shared keys are equal by construction on
//! every emitted match, the merged row ([`merge_rows`]) keeps the probe side's
//! copy of each shared binding.
//!
//! ### NULL / NONE keys (3VL)
//!
//! A `Value::Null` or `Value::None` in *any* key slot — or a missing /
//! non-extractable key binding — means the row has no join key (SQL 3VL: null
//! never equals anything, so it can never equi-join):
//!
//! - **Build side**: a null-keyed row is excluded from the build table entirely.
//! - **Probe side, [`Inner`](JoinType::Inner)**: a null-keyed row is dropped (it can match
//!   nothing).
//! - **Probe side, [`Left`](JoinType::Left)**: a null-keyed row passes through null-filled
//!   ([`null_filled`]: its null-template bindings set to `Value::Null`), exactly like a probe row
//!   whose key found no build match.
//!
//! ## Join key — document rows
//!
//! Each side's key is a list of expressions, one per key column. Two keys
//! match when they are equal by SurrealQL's `=` ([`keys_equal`]): `NULL`
//! matches `NULL`, `NONE` matches `NONE`, and a string matches a regex it
//! satisfies ([`key_has_regex`]). No key is excluded for being `NULL` or
//! `NONE`.
//!
//! ## Join types
//!
//! - [`Inner`](JoinType::Inner): emit the union of every probe row with each build row sharing its
//!   key.
//! - [`Left`](JoinType::Left): like `Inner`, but a probe row with no matching build row (including
//!   a null-keyed probe row) is still emitted once, null-filled. Used by GQL `OPTIONAL MATCH`.
//! - [`Cross`](JoinType::Cross): the join has no keys; emit the full cartesian product of build ×
//!   probe rows (the no-shared-variable case).
//!
//! ## Build budget & output bound
//!
//! The build side is held in memory ([`BuildTable`]). For binding rows, the
//! number of build rows is bounded by `SURREAL_GQL_MAX_JOIN_BUILD_ROWS` (the
//! shared GQL in-memory-build budget); exceeding it fails the query with an
//! error that names the knob ([`too_many_build_rows`]). Separately, the
//! cumulative number of rows *emitted* (a `Cross` product, or a high-fan-out
//! equi-join, can emit far more rows than either side holds) is bounded by
//! `SURREAL_GQL_MAX_OUTPUT_ROWS` ([`emit`]).

use std::collections::BTreeMap;

use crate::exec::FlowResult;
use crate::expr::ControlFlow;
use crate::val::{Object, Value};

/// The kind of join `HashJoin` performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinType {
	/// Emit only probe rows that match at least one build row.
	Inner,
	/// Emit every probe row; unmatched probe rows are null-filled with the
	/// build-introduced bindings (`null_template`) or the alias. Used by
	/// `OPTIONAL MATCH`.
	Left,
	/// Cartesian product (`keys` is empty): every build row × every probe row.
	Cross,
}

impl JoinType {
	/// EXPLAIN rendering of the join type.
	pub(crate) fn label(self) -> &'static str {
		match self {
			JoinType::Inner => "Inner",
			JoinType::Left => "Left",
			JoinType::Cross => "Cross",
		}
	}
}

/// A join key: one value per key column.
pub(crate) type JoinKey = Vec<Value>;

/// The probe row with `value` set under `alias`. A probe row that is not an
/// object — a bare value from a `VALUE` projection — has no field to hold
/// the alias, and is emitted as it is.
pub(crate) fn with_alias(probe_row: &Value, alias: &str, value: Value) -> Value {
	if let Value::Object(row) = probe_row {
		let mut row = row.clone();
		row.insert(alias.to_string(), value);
		Value::Object(row)
	} else {
		probe_row.clone()
	}
}

/// [`with_alias`] over a probe row that is given up: the row itself carries
/// the alias, with no copy of it.
pub(crate) fn with_alias_owned(probe_row: Value, alias: &str, value: Value) -> Value {
	if let Value::Object(mut row) = probe_row {
		row.insert(alias.to_string(), value);
		Value::Object(row)
	} else {
		probe_row
	}
}

/// Extract the join key for `row`: the `id` of each binding named in `keys`.
///
/// Returns `None` if any key slot is `Value::Null` / `Value::None`, missing, or
/// not a record (i.e. has no usable id) — such a row never equi-joins (3VL).
/// For a `Cross` join `keys` is empty; the planner uses [`BuildTable::all_rows`]
/// directly and never calls this with an empty `keys`, but an empty `keys`
/// here would yield `Some(vec![])` (every row shares the one empty key), which
/// is also correct.
pub(crate) fn join_key(row: &Value, keys: &[String]) -> Option<JoinKey> {
	let Value::Object(obj) = row else {
		return None;
	};
	let mut key = Vec::with_capacity(keys.len());
	for name in keys {
		let id = binding_id(obj.get(name))?;
		key.push(id);
	}
	Some(key)
}

/// The id `Value` of a binding slot, or `None` when it cannot equi-join.
///
/// - a full record object ⇒ its `id` field (normally a `Value::RecordId`);
/// - a bare `Value::RecordId` ⇒ itself (defensive: bindings normally hold full objects);
/// - `Value::Null` / `Value::None`, a missing binding, or any other value ⇒ `None` (no join key —
///   excluded per the 3VL rules).
fn binding_id(slot: Option<&Value>) -> Option<Value> {
	match slot {
		Some(Value::Object(node)) => match node.get("id") {
			Some(Value::Null) | Some(Value::None) | None => None,
			Some(id) => Some(id.clone()),
		},
		Some(Value::RecordId(rid)) => Some(Value::RecordId(rid.clone())),
		// Null / None / missing / any other value ⇒ no join key.
		_ => None,
	}
}

/// Merge a build row and a probe row into a single binding row.
///
/// The probe row's entries take precedence on the shared keys (which are equal
/// by construction, so the choice is immaterial); build-only bindings are
/// carried over. Both rows are objects in practice; a non-object row defaults
/// to empty so the merge never panics.
pub(crate) fn merge_rows(build_row: &Value, probe_row: &Value) -> Value {
	let mut merged = if let Value::Object(o) = build_row {
		o.clone()
	} else {
		Object::default()
	};
	if let Value::Object(probe) = probe_row {
		for (k, v) in probe.iter() {
			merged.insert(k.clone(), v.clone());
		}
	}
	Value::Object(merged)
}

/// Build a null-filled output for a `Left` miss: the probe row plus every
/// `null_template` binding set to `Value::Null`.
pub(crate) fn null_filled(probe_row: &Value, null_template: &[String]) -> Value {
	let mut row = if let Value::Object(o) = probe_row {
		o.clone()
	} else {
		Object::default()
	};
	for name in null_template {
		row.insert(name.clone(), Value::Null);
	}
	Value::Object(row)
}

/// Push `row` to the output batch, bumping the cumulative emitted-row counter and
/// failing when it exceeds the `SURREAL_GQL_MAX_OUTPUT_ROWS` ceiling. Counting at
/// the push (not per batch) is required: a single probe row crossed against a
/// large build side already fans out unboundedly, so the guard must trip inside
/// the inner loop.
pub(crate) fn emit(
	out: &mut Vec<Value>,
	emitted: &mut usize,
	max_rows: usize,
	row: Value,
) -> FlowResult<()> {
	*emitted += 1;
	if *emitted > max_rows {
		return Err(crate::exec::operators::gql_output_rows_exceeded(max_rows));
	}
	out.push(row);
	Ok(())
}

/// The error a join raises past `max_rows` build-side rows.
pub(crate) fn too_many_build_rows(max_rows: usize) -> ControlFlow {
	ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InvalidStatement(format!(
		"GQL MATCH join exceeded the maximum of {max_rows} build-side rows \
		 (configurable via SURREAL_GQL_MAX_JOIN_BUILD_ROWS)"
	))))
}

/// Whether two keys are equal by SurrealQL's `=`, column by column.
pub(crate) fn keys_equal(left: &[Value], right: &[Value]) -> bool {
	left.len() == right.len() && left.iter().zip(right).all(|(l, r)| l.equal(r))
}

/// Whether any column of `key` has an order that does not agree with `=`,
/// so it must be compared with [`Value::equal`].
pub(crate) fn key_has_regex(key: &[Value]) -> bool {
	key.iter().any(|v| !v.ord_agrees_with_equal() || !v.equal(v))
}

/// Build table keyed by `JoinKey: Ord`, with the rows sharing a key accumulating
/// in that key's `Vec` in insertion order. The stored row count is bounded by the
/// configured build-row budget.
///
/// **Ordered, not hashed.** A probe row joins a build row when their keys compare
/// equal, which holds every numeric variant of one value equal, including inside
/// a compound record id (`t:[1.5f]` against `t:[1.5dec]`). Ordering also makes
/// iteration deterministic, which is what [`Self::all_rows`] needs.
///
/// A key whose ordering does not agree with `=` (such as one holding a regex,
/// `NaN`, or a closure at any nesting depth) is kept apart: such keys are
/// compared one by one with [`Value::equal`] — on the build side against every
/// probe key, and on the probe side against every build row.
pub(crate) struct BuildTable {
	keyed: BTreeMap<JoinKey, Vec<Value>>,
	/// Build rows whose key holds a regex, with their keys.
	regex_keyed: Vec<(JoinKey, Value)>,
	rows: usize,
}

impl BuildTable {
	pub(crate) fn new() -> Self {
		Self {
			keyed: BTreeMap::new(),
			regex_keyed: Vec::new(),
			rows: 0,
		}
	}

	/// Insert a build `row` under `key`, failing when the total stored row count
	/// would exceed `max_rows`.
	pub(crate) fn insert(
		&mut self,
		key: JoinKey,
		row: Value,
		max_rows: usize,
	) -> Result<(), ControlFlow> {
		if self.rows >= max_rows {
			return Err(too_many_build_rows(max_rows));
		}
		if key_has_regex(&key) {
			self.regex_keyed.push((key, row));
		} else {
			self.keyed.entry(key).or_default().push(row);
		}
		self.rows += 1;
		Ok(())
	}

	/// The build rows stored under `key`, if any, by ordering equality alone.
	fn get(&self, key: &JoinKey) -> Option<&[Value]> {
		self.keyed.get(key).map(|rows| rows.as_slice())
	}

	/// The build rows whose key equals `key` by `=`: those stored under it,
	/// then those a regex or non-order-equivalent column on either side matches.
	pub(crate) fn matches<'a>(&'a self, key: &'a JoinKey) -> impl Iterator<Item = &'a Value> + 'a {
		let loose = key_has_regex(key);
		let exact = (!loose).then(|| self.get(key)).flatten().unwrap_or(&[]).iter();
		let build_regex =
			self.regex_keyed.iter().filter(move |(k, _)| keys_equal(k, key)).map(|(_, row)| row);
		let probe_regex = loose
			.then(|| {
				self.keyed
					.iter()
					.filter(move |(k, _)| keys_equal(k, key))
					.flat_map(|(_, rows)| rows.iter())
			})
			.into_iter()
			.flatten();
		exact.chain(build_regex).chain(probe_regex)
	}

	/// Every stored build row, used by `Cross`, in an order that is deterministic
	/// across processes: by key, then insertion order within a key. `Cross` has no
	/// join keys at all, so every row shares the one empty key and this is plain
	/// insertion order.
	pub(crate) fn all_rows(&self) -> impl Iterator<Item = &Value> {
		self.keyed.values().flatten().chain(self.regex_keyed.iter().map(|(_, row)| row))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::{RecordId, RecordIdKey, TableName};

	fn rid(table: &str, key: &str) -> RecordId {
		RecordId {
			table: TableName::new(table.to_string()),
			key: RecordIdKey::from(key.to_string()),
		}
	}

	/// A binding-row object: each `(binding, id)` pair becomes a full node
	/// object `{ id: table:key }` under `binding`.
	fn node_row(pairs: &[(&str, RecordId)]) -> Value {
		let mut row = Object::default();
		for (binding, id) in pairs {
			let mut node = Object::default();
			node.insert("id".to_string(), Value::RecordId(id.clone()));
			row.insert(binding.to_string(), Value::Object(node));
		}
		Value::Object(row)
	}

	/// A binding-row object with explicit slot values (for null/none cases).
	fn raw_row(pairs: &[(&str, Value)]) -> Value {
		let mut row = Object::default();
		for (binding, value) in pairs {
			row.insert(binding.to_string(), value.clone());
		}
		Value::Object(row)
	}

	#[test]
	fn build_table_guard_names_the_knob() {
		let mut table = BuildTable::new();
		// First insert under a budget of 1 succeeds.
		assert!(table.insert(vec![Value::from(1)], Value::from(10), 1).is_ok());
		// A second distinct row past the budget errors and names the knob.
		let err = table.insert(vec![Value::from(2)], Value::from(20), 1).unwrap_err();
		let ControlFlow::Err(e) = err else {
			panic!("expected error, got {err:?}");
		};
		let msg = e.to_string();
		assert!(
			msg.contains("SURREAL_GQL_MAX_JOIN_BUILD_ROWS"),
			"guard error must name the knob, got: {msg}"
		);
	}

	#[test]
	fn build_table_budget_counts_rows_not_keys() {
		// Two rows sharing one key still count as two rows against the budget.
		let mut table = BuildTable::new();
		assert!(table.insert(vec![Value::from(1)], Value::from(10), 2).is_ok());
		assert!(table.insert(vec![Value::from(1)], Value::from(11), 2).is_ok());
		let err = table.insert(vec![Value::from(1)], Value::from(12), 2).unwrap_err();
		assert!(matches!(err, ControlFlow::Err(_)));
		// The shared key holds both rows.
		assert_eq!(table.get(&vec![Value::from(1)]).map(|r| r.len()), Some(2));
	}

	#[test]
	fn binding_id_extraction_rules() {
		// Full object ⇒ its id.
		let node = node_row(&[("a", rid("t", "1"))]);
		let Value::Object(o) = &node else {
			panic!();
		};
		assert_eq!(binding_id(o.get("a")), Some(Value::RecordId(rid("t", "1"))));
		// Bare record id ⇒ itself.
		assert_eq!(
			binding_id(Some(&Value::RecordId(rid("t", "2")))),
			Some(Value::RecordId(rid("t", "2")))
		);
		// Null / None / missing / object-without-id / non-record ⇒ None.
		assert_eq!(binding_id(Some(&Value::Null)), None);
		assert_eq!(binding_id(Some(&Value::None)), None);
		assert_eq!(binding_id(None), None);
		assert_eq!(binding_id(Some(&Value::Bool(true))), None);
		let mut no_id = Object::default();
		no_id.insert("name".to_string(), Value::from("x"));
		assert_eq!(binding_id(Some(&Value::Object(no_id))), None);
		// Object whose id is itself Null ⇒ None.
		let mut null_id = Object::default();
		null_id.insert("id".to_string(), Value::Null);
		assert_eq!(binding_id(Some(&Value::Object(null_id))), None);
	}

	#[test]
	fn join_key_returns_none_on_any_null_slot() {
		let row = raw_row(&[("a", Value::RecordId(rid("t", "1"))), ("b", Value::Null)]);
		assert_eq!(join_key(&row, &["a".to_string(), "b".to_string()]), None);
	}

	#[test]
	fn build_table_nan_and_closure_keys_do_not_match() {
		let mut table = BuildTable::new();
		let nan_pt =
			Value::Geometry(crate::val::Geometry::Point(geo_types::Point::new(f64::NAN, 2.0)));
		let nested_nan = Value::Array(vec![nan_pt.clone()].into());
		let closure_a = Value::Closure(Box::new(crate::val::Closure::Expr {
			args: vec![(crate::expr::Param::from("a".to_owned()), crate::expr::Kind::Any)],
			returns: None,
			body: crate::expr::Expr::Literal(crate::expr::Literal::Integer(1)),
			captures: Default::default(),
		}));
		let closure_b = Value::Closure(Box::new(crate::val::Closure::Expr {
			args: vec![(crate::expr::Param::from("b".to_owned()), crate::expr::Kind::Any)],
			returns: None,
			body: crate::expr::Expr::Literal(crate::expr::Literal::Integer(2)),
			captures: Default::default(),
		}));
		assert!(table.insert(vec![nan_pt.clone()], Value::from(1), 10).is_ok());
		assert!(table.insert(vec![nested_nan.clone()], Value::from(2), 10).is_ok());
		assert!(table.insert(vec![closure_a], Value::from(3), 10).is_ok());

		assert_eq!(table.matches(&vec![nan_pt]).count(), 0);
		assert_eq!(table.matches(&vec![nested_nan]).count(), 0);
		assert_eq!(table.matches(&vec![closure_b]).count(), 0);
	}

	#[test]
	fn build_table_nested_regex_matches_either_side() {
		let re = Value::Regex("a.*".parse().unwrap());
		let composite_re = vec![Value::from(1), re];
		let composite_match = vec![Value::from(1), Value::from("abc")];
		let composite_miss = vec![Value::from(1), Value::from("xyz")];

		let mut build_re = BuildTable::new();
		assert!(build_re.insert(composite_re.clone(), Value::from(10), 10).is_ok());
		assert_eq!(
			build_re.matches(&composite_match).cloned().collect::<Vec<_>>(),
			vec![Value::from(10)]
		);
		assert_eq!(build_re.matches(&composite_miss).count(), 0);

		let mut probe_re = BuildTable::new();
		assert!(probe_re.insert(composite_match, Value::from(20), 10).is_ok());
		assert!(probe_re.insert(composite_miss, Value::from(30), 10).is_ok());
		assert_eq!(
			probe_re.matches(&composite_re).cloned().collect::<Vec<_>>(),
			vec![Value::from(20)]
		);
	}

	// ---- document rows -------------------------------------------------
}
