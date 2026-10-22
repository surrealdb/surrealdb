//! The names and nested-output helpers of the GROUP BY output row: the
//! `AggregateId` of an aggregate, the field names its group key and aggregate
//! values land under, and [`set_nested_value`], which writes a dotted output
//! path into the row.

use std::sync::Arc;

use crate::exec::function::{AggregateFunction, FunctionRegistry};
use crate::val::{Object, Value};

/// Whether a non-aggregate column of a group, which holds `stored`, still
/// needs evaluating: `None` means it has not been evaluated yet, and a stored
/// `NONE` is final for a column with one value for the whole group
/// (`constant`) and provisional for a first-value one.
pub(crate) fn first_value_needed(constant: bool, stored: &Option<Value>) -> bool {
	match stored {
		None => true,
		Some(value) => !constant && value.is_none(),
	}
}

/// An aggregate function named as data: what a plan holds in place of the
/// registry's function object, resolved against the registry when a run
/// starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateId {
	/// The registry's aggregate of this name, `count()` with no argument
	/// among them.
	Named(String),
	/// `count(expr)`, which counts the truthy values of its argument.
	CountField,
	/// The fold that collects each row's value of a projected expression
	/// carrying no aggregate of its own, duplicates kept.
	ImplicitCollect,
}

impl AggregateId {
	/// The id of the function the registry answers for the call `name`, which
	/// has an argument when `has_argument`.
	pub fn of_call(name: &str, has_argument: bool) -> Self {
		if name == "count" && has_argument {
			AggregateId::CountField
		} else {
			AggregateId::Named(name.to_owned())
		}
	}

	/// The function's name, as it is registered.
	pub fn name(&self) -> &str {
		match self {
			AggregateId::Named(name) => name,
			AggregateId::CountField => "count",
			AggregateId::ImplicitCollect => "array::collect",
		}
	}

	/// The function this id names in `registry`, or `None` for a name the
	/// registry does not hold.
	pub fn resolve(&self, registry: &FunctionRegistry) -> Option<Arc<dyn AggregateFunction>> {
		match self {
			AggregateId::Named(name) => registry.get_aggregate(name).cloned(),
			AggregateId::CountField => Some(registry.get_count_aggregate(true)),
			AggregateId::ImplicitCollect => Some(registry.get_implicit_collect_aggregate()),
		}
	}
}

/// Generate a synthetic field name for an aggregate at the given index.
/// These names are used in the transformed expression to reference aggregate results.
pub fn aggregate_field_name(idx: usize) -> String {
	format!("_a{}", idx)
}

/// [`aggregate_field_name`] as an object key, which takes no allocation for
/// the first [`PLACEHOLDER_KEYS`] indexes.
pub(crate) fn aggregate_field_key(idx: usize) -> surrealdb_strand::Strand {
	const NAMES: [&str; PLACEHOLDER_KEYS] = [
		"_a0", "_a1", "_a2", "_a3", "_a4", "_a5", "_a6", "_a7", "_a8", "_a9", "_a10", "_a11",
		"_a12", "_a13", "_a14", "_a15",
	];
	NAMES.get(idx).map_or_else(
		|| aggregate_field_name(idx).into(),
		|name| surrealdb_strand::Strand::new_static(name),
	)
}

/// [`group_key_field_name`] as an object key, which takes no allocation for
/// the first [`PLACEHOLDER_KEYS`] indexes.
pub(crate) fn group_key_field_key(idx: usize) -> surrealdb_strand::Strand {
	const NAMES: [&str; PLACEHOLDER_KEYS] = [
		"_g0", "_g1", "_g2", "_g3", "_g4", "_g5", "_g6", "_g7", "_g8", "_g9", "_g10", "_g11",
		"_g12", "_g13", "_g14", "_g15",
	];
	NAMES.get(idx).map_or_else(
		|| group_key_field_name(idx).into(),
		|name| surrealdb_strand::Strand::new_static(name),
	)
}

/// How many aggregate and grouping-key placeholder names exist as constants.
const PLACEHOLDER_KEYS: usize = 16;

/// Generate a synthetic field name for the grouping key at the given index.
///
/// A post-expression is evaluated against the group's answers rather than
/// against a row, so a key it names is placed in that document under this name
/// and the planner rewrites the read to match.
pub fn group_key_field_name(idx: usize) -> String {
	format!("_g{}", idx)
}

// ---------------------------------------------------------------------------
// Group map
// ---------------------------------------------------------------------------
//
// Two rows belong to the same group iff their group keys compare equal under
// `Value: Ord`, which holds every numeric variant of one value equal. The map
// is *ordered*: the group order is deterministic, and the drain feeds results
// downstream already sorted by key.
//
// `Vec<Value>: Borrow<[Value]>`, so a row probes with the borrowed slice its key
// values already live in and nothing is cloned unless the row opens a new group.
//
// The keys are held apart from the states, mapping each key to an index into
// `states`, because a `BTreeMap<GroupKey, GroupState>` cannot answer
// get-or-insert in one descent on stable Rust: returning the `get_mut` borrow
// from the hit branch keeps it live across the insert branch (E0499, NLL problem
// case 3), so a single map costs either two descents per row (`contains_key`
// then `get_mut`) or a key clone per row (`entry(key.to_vec())`) — the clone
// being the cost this layout exists to avoid. Copying the `usize` out ends the
// borrow, so the hit path is one descent plus one indexed load.
//
// Spill-to-disk for high-cardinality groups is a future change.

/// Set a value at a nested path in an object.
///
/// For a path like ["address", "city"], this creates or updates:
/// `{ address: { city: value } }`
pub(crate) fn set_nested_value(obj: &mut Object, path: &[String], value: Value) {
	if path.is_empty() {
		return;
	}

	if path.len() == 1 {
		// Simple case: just insert at this level
		obj.insert(path[0].as_str(), value);
		return;
	}

	// Need to traverse/create nested structure
	let key = &path[0];
	let rest = &path[1..];

	// Get or create the nested object
	let nested = obj.entry(key.as_str()).or_insert_with(|| Value::Object(Object::default()));

	if let Value::Object(nested_obj) = nested {
		set_nested_value(nested_obj, rest, value);
	} else {
		// Replace non-object with new object containing the nested path
		let mut new_obj = Object::default();
		set_nested_value(&mut new_obj, rest, value);
		*nested = Value::Object(new_obj);
	}
}

/// Lay per-expression group-key columns out row-major, so each row's key is a
/// contiguous slice the group probe can borrow.
///
/// `columns[expr_idx][row_idx]` is the evaluated value for group-by expression
/// `expr_idx` at row `row_idx`. The result holds `row_count` consecutive runs of
/// `columns.len()` values, so row `i`'s key is
/// `[i * columns.len() .. (i + 1) * columns.len()]` — the one caller-visible
/// invariant, and the reason this function owns the layout rather than the call
/// site. `columns` is consumed: values are moved out, and a single column is
/// taken whole because it is already the row-major form.
#[cfg(test)]
fn group_key_rows(columns: &mut Vec<Vec<Value>>, row_count: usize) -> Vec<Value> {
	if columns.len() == 1 {
		return columns.swap_remove(0);
	}
	let mut flat = Vec::with_capacity(row_count * columns.len());
	for row_idx in 0..row_count {
		for col in columns.iter_mut() {
			flat.push(std::mem::take(&mut col[row_idx]));
		}
	}
	flat
}

#[cfg(test)]
mod tests {
	use std::collections::hash_map::DefaultHasher;
	use std::hash::{Hash, Hasher};
	use std::str::FromStr;

	use rust_decimal::Decimal;

	use super::*;
	use crate::val::Number;

	// =========================================================================
	// Builders
	// =========================================================================

	// =========================================================================
	// Grouping
	// =========================================================================

	// =========================================================================
	// GROUP ALL
	// =========================================================================

	// =========================================================================
	// Group ordering
	// =========================================================================

	// =========================================================================
	// Field shapes
	// =========================================================================

	// =========================================================================
	// Batching
	// =========================================================================

	// =========================================================================
	// Control flow and errors
	// =========================================================================

	// =========================================================================
	// Planner-visible metadata
	// =========================================================================

	// =========================================================================
	// set_nested_value
	// =========================================================================

	#[test]
	fn set_nested_value_creates_missing_levels_and_replaces_non_objects() {
		let mut obj = Object::default();
		set_nested_value(&mut obj, &["a".to_string(), "b".to_string()], Value::from(1));
		assert_eq!(
			obj.get("a").cloned(),
			Some(Value::Object(Object::from_iter([("b".to_string(), Value::from(1))])))
		);

		// A scalar already sitting at a prefix of a later path is replaced by the
		// object that path needs.
		let mut obj = Object::default();
		set_nested_value(&mut obj, &["a".to_string()], Value::from(7));
		set_nested_value(&mut obj, &["a".to_string(), "b".to_string()], Value::from(1));
		assert_eq!(
			obj.get("a").cloned(),
			Some(Value::Object(Object::from_iter([("b".to_string(), Value::from(1))])))
		);

		// An empty path is a no-op.
		let mut obj = Object::default();
		set_nested_value(&mut obj, &[], Value::from(1));
		assert!(obj.is_empty());
	}

	// =========================================================================
	// GroupMap — group identity and ordering
	// =========================================================================

	fn dec(s: &str) -> Value {
		Value::Number(Number::Decimal(Decimal::from_str(s).unwrap()))
	}

	fn float(f: f64) -> Value {
		Value::Number(Number::Float(f))
	}

	/// Numeric keys hash as they group: keys that share a group hash alike,
	/// whatever their variants.
	#[test]
	fn hash_agrees_with_the_equality_that_decides_grouping() {
		fn hash(v: &Value) -> u64 {
			let mut hasher = DefaultHasher::new();
			v.hash(&mut hasher);
			hasher.finish()
		}

		for (a, b) in [(float(1.5), dec("1.50")), (Value::from(1i64), float(1.0))] {
			assert_eq!(a, b, "{a:?} and {b:?} are one group");
			assert_eq!(hash(&a), hash(&b), "{a:?} and {b:?} hash alike");
		}
		assert_ne!(float(0.1), dec("0.1"));
	}

	#[test]
	fn group_key_rows_takes_a_single_column_whole() {
		let mut columns = vec![vec![Value::from(1i64), dec("2")]];
		let flat = group_key_rows(&mut columns, 2);
		assert_eq!(flat, vec![Value::from(1i64), dec("2")]);
	}

	#[test]
	fn group_key_rows_lays_rows_out_contiguously() {
		let mut columns =
			vec![vec![Value::from(1i64), Value::from(2i64)], vec![Value::from(10i64), dec("20")]];
		let flat = group_key_rows(&mut columns, 2);
		assert_eq!(flat, vec![Value::from(1i64), Value::from(10i64), Value::from(2i64), dec("20")]);
	}
}
