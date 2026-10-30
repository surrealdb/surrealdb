//! Unit tests of the planning state under [`super`].
//!
//! The tests here need no datastore: the slot table, the shape digest, the name collector, the
//! value-dependence recorder and the source scans that hold the planning memo to its contract.
//! The scans read the crate's own sources, so a change that breaks the contract fails here before
//! it reaches a review.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use surrealdb_expr::parser::TopLevelExpr;
use surrealdb_strand::Strand;

use super::answer::PRINCIPAL_NAMES;
use super::deps::{
	BindingOrigin, DepRecorder, LiteralOrdinal, LiteralTable, MAX_CONSUMED_WALK, MAX_VALUE_DEPS,
	ValueDep, ValueDeps, ValueRef, ValueUse, context_names, reads_principal,
};
use super::names::{NameSet, collect};
use super::planning::PlanningCatalog;
use super::shape::{arm_key, digest};
use super::slots::{OccurrenceClass, SlotTable};
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::core::{
	FactCache, InsertPoint, Ir, IrScope, LocOrigin, Location, OpKind, OpSpec, PrintOpts, Rewriter,
	parse, print,
};
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::fold::FoldConsumer;
use crate::exec::ir::iface::ids::{FrameId, OpId, SnapshotId, ValueId};
use crate::exec::ir::iface::premise::SlotId;
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::registry;
use crate::expr::BinaryOperator;
use crate::val::{TableName, Value};

// ---------------------------------------------------------------------------------------------
// Support
// ---------------------------------------------------------------------------------------------

/// The first statement of `sql`.
fn statement(sql: &str) -> TopLevelExpr {
	let mut ast = crate::syn::parse(sql).unwrap_or_else(|e| panic!("`{sql}` parses: {e}"));
	ast.expressions.remove(0)
}

/// The slot table of the first statement of `sql`.
fn table_of(sql: &str) -> SlotTable {
	SlotTable::of(&statement(sql))
}

/// The class of every occurrence of the table, in order.
fn classes(table: &SlotTable) -> Vec<OccurrenceClass> {
	table.occurrences().iter().map(|occurrence| occurrence.class).collect()
}

/// The names of the free occurrences of the table, as a set.
fn free_names(table: &SlotTable) -> BTreeSet<String> {
	table
		.occurrences()
		.iter()
		.filter(|occurrence| matches!(occurrence.class, OccurrenceClass::Free(_)))
		.map(|occurrence| occurrence.name.as_str().to_owned())
		.collect()
}

fn dep(value: ValueRef, consumer: FoldConsumer) -> ValueDep {
	ValueDep {
		value,
		consumer,
		used: ValueUse::Exact,
	}
}

fn literal_dep(ordinal: u32) -> ValueDep {
	dep(ValueRef::Literal(LiteralOrdinal(ordinal)), FoldConsumer::IndexChoice)
}

/// The record of the only statement a recorder holds.
fn only(recorder: DepRecorder) -> ValueDeps {
	let mut records = recorder.finish();
	assert_eq!(records.len(), 1, "one statement is recorded");
	records.remove(0).1
}

// ---------------------------------------------------------------------------------------------
// Slot table
// ---------------------------------------------------------------------------------------------

/// A name read before a `LET` of the same name in one block is free at its first occurrence and
/// bound at its last.
#[test]
fn slot_table_partial_shadowing() {
	let table = table_of("RETURN { $x; LET $x = 1; $x }");
	let held = classes(&table);
	assert_eq!(held.first(), Some(&OccurrenceClass::Free(SlotId(0))));
	assert_eq!(held.last(), Some(&OccurrenceClass::Bound));
	assert_eq!(table.id_of("x"), Some(SlotId(0)));
	assert_eq!(table.name(SlotId(0)), Some("x"));
	assert_eq!(table.len(), 1);
}

/// Slots are numbered by name, in the order of each name's first free occurrence.
#[test]
fn slot_table_numbering_by_first_free_occurrence() {
	let table = table_of("SELECT * FROM t WHERE a = $b AND c = $a AND d = $b");
	assert_eq!(table.len(), 2);
	assert_eq!(table.id_of("b"), Some(SlotId(0)));
	assert_eq!(table.id_of("a"), Some(SlotId(1)));
	assert_eq!(table.name(SlotId(0)), Some("b"));
	assert_eq!(table.name(SlotId(1)), Some("a"));
	assert_eq!(table.name(SlotId(2)), None);
	assert_eq!(
		classes(&table),
		[
			OccurrenceClass::Free(SlotId(0)),
			OccurrenceClass::Free(SlotId(1)),
			OccurrenceClass::Free(SlotId(0)),
		]
	);
}

/// A principal name is never a slot. A row name outside a region that binds it is a free name
/// and has one.
#[test]
fn slot_table_slots_row_names_outside_a_row_region() {
	let table = table_of("RETURN [$this, $parent, $value, $auth.id, $session]");
	assert_eq!(table.len(), 3);
	for (at, name) in ["this", "parent", "value"].into_iter().enumerate() {
		assert_eq!(table.id_of(name), Some(SlotId(at as u16)), "{name}");
	}
	for name in ["auth", "session"] {
		assert_eq!(table.id_of(name), None, "{name}");
	}
	assert_eq!(
		classes(&table),
		[
			OccurrenceClass::Free(SlotId(0)),
			OccurrenceClass::Free(SlotId(1)),
			OccurrenceClass::Free(SlotId(2)),
			OccurrenceClass::Principal,
			OccurrenceClass::Principal,
		]
	);
	for name in PRINCIPAL_NAMES {
		assert_eq!(table_of(&format!("RETURN ${name}")).len(), 0, "{name}");
	}
}

/// A row name inside a region that declares it is a row; one the region does not declare is free.
#[test]
fn slot_table_row_names_follow_their_regions() {
	let free = OccurrenceClass::Free(SlotId(0));
	let row = OccurrenceClass::Row;
	let cases: [(&str, &[OccurrenceClass]); 8] = [
		(
			"SELECT * FROM t WHERE $this.a = $value AND b = $before",
			&[row, free, OccurrenceClass::Free(SlotId(1))],
		),
		("SELECT a, $value FROM t GROUP BY a", &[free]),
		("SELECT * FROM t LIMIT $this", &[free]),
		("UPDATE t SET a = $value", &[free]),
		("UPDATE t SET a = $input", &[free]),
		("UPDATE t SET a = 1 RETURN $before, $after", &[row, row]),
		("INSERT INTO t (a) VALUES ($input) ON DUPLICATE KEY UPDATE a = $input", &[free, row]),
		("SELECT * FROM t WHERE $parent = 1", &[free]),
	];
	for (sql, expected) in cases {
		assert_eq!(classes(&table_of(sql)), expected, "{sql}");
	}
}

/// `$parent` is declared where a row frame encloses the region, and only there.
#[test]
fn slot_table_parent_needs_an_enclosing_row() {
	let table = table_of("SELECT *, (SELECT * FROM u WHERE x = $parent.y) FROM t");
	assert_eq!(classes(&table), [OccurrenceClass::Row]);
	assert_eq!(table.len(), 0);
}

/// A closure and a function body see no row name of the region around them.
#[test]
fn slot_table_isolated_regions_see_no_enclosing_row() {
	let table = table_of("SELECT * FROM t WHERE array::any(a, |$x| $x = $this.b)");
	assert_eq!(classes(&table), [OccurrenceClass::Bound, OccurrenceClass::Free(SlotId(0))]);
	assert_eq!(table.id_of("this"), Some(SlotId(0)));
}

/// A `FOR` variable and a closure parameter are bound inside the statement, and the names they
/// read from outside are free.
#[test]
fn slot_table_for_and_closure_bind() {
	let looped = table_of("FOR $i IN $xs { $i; }");
	assert_eq!(looped.id_of("xs"), Some(SlotId(0)));
	assert_eq!(looped.id_of("i"), None);
	assert_eq!(looped.len(), 1);
	assert_eq!(classes(&looped), [OccurrenceClass::Free(SlotId(0)), OccurrenceClass::Bound]);

	let closure = table_of("RETURN array::map([1], |$y| $y + $z)");
	assert_eq!(closure.id_of("z"), Some(SlotId(0)));
	assert_eq!(closure.id_of("y"), None);
	assert_eq!(closure.len(), 1);
	assert_eq!(classes(&closure), [OccurrenceClass::Bound, OccurrenceClass::Free(SlotId(0))]);
}

/// A binding inside the statement is checked before the principal names, so the variable of a
/// `FOR` named `$session` reads the loop's value, and a `$session` outside the loop is the
/// principal's.
#[test]
fn session_shadowing_unchanged() {
	let looped = table_of("FOR $session IN [{ ns: 'x' }] { $session.ns }");
	assert_eq!(classes(&looped), [OccurrenceClass::Bound]);
	assert_eq!(looped.len(), 0);

	let mixed = table_of("RETURN { FOR $session IN [{ ns: 'x' }] { $session.ns }; $session }");
	assert_eq!(classes(&mixed), [OccurrenceClass::Bound, OccurrenceClass::Principal]);
	assert_eq!(mixed.len(), 0);
}

/// Two parses of one text give equal tables.
#[test]
fn slot_table_equal_for_equal_shape() {
	let text = "SELECT * FROM t WHERE a = $b AND c = $a AND d = 1";
	assert_eq!(table_of(text), table_of(text));
	assert_eq!(
		table_of(text),
		table_of("SELECT  *  FROM t /* a comment */ WHERE a = $b AND c = $a AND d = 1")
	);
	assert_ne!(table_of(text), table_of("SELECT * FROM t WHERE a = $a AND c = $b AND d = 1"));
}

/// The literals of a statement are counted in the walk that numbers them.
#[test]
fn literal_ordinals_follow_visit_order() {
	assert_eq!(table_of("SELECT * FROM t WHERE a = 1 AND b = 'x'").literal_count(), 2);
	assert_eq!(
		table_of("SELECT * FROM t WHERE a = 1   AND b = 'x' -- trailing\n").literal_count(),
		2
	);
	assert_eq!(table_of("RETURN $a").literal_count(), 0);
	assert_eq!(table_of("SELECT * FROM t LIMIT 10 START 5 TIMEOUT 5s").literal_count(), 3);
	assert_eq!(table_of("UPDATE t SET a = 1 TIMEOUT 5s").literal_count(), 2);
	assert_eq!(table_of("UPSERT t SET a = 1 TIMEOUT 5s").literal_count(), 2);
}

/// `START` and `TIMEOUT` clauses on `SELECT`, `UPDATE`, and `UPSERT` are visited outside any row
/// region.
#[test]
fn slot_table_visits_start_and_timeout_clauses() {
	let select = table_of("SELECT * FROM t LIMIT $lim START $off TIMEOUT $tm");
	assert_eq!(select.id_of("lim"), Some(SlotId(0)));
	assert_eq!(select.id_of("off"), Some(SlotId(1)));
	assert_eq!(select.id_of("tm"), Some(SlotId(2)));

	let update = table_of("UPDATE t SET a = $v TIMEOUT $tm");
	assert_eq!(update.id_of("v"), Some(SlotId(0)));
	assert_eq!(update.id_of("tm"), Some(SlotId(1)));

	let upsert = table_of("UPSERT t SET a = $v TIMEOUT $tm");
	assert_eq!(upsert.id_of("v"), Some(SlotId(0)));
	assert_eq!(upsert.id_of("tm"), Some(SlotId(1)));
}

// ---------------------------------------------------------------------------------------------
// Shape digest
// ---------------------------------------------------------------------------------------------

/// Whitespace and a comment leave the digest alone, and a different literal or table changes it.
#[test]
fn shape_digest_equal_iff_equal_ast() {
	let base = digest(&statement("SELECT * FROM t WHERE a = 1"));
	assert_eq!(base, digest(&statement("SELECT * FROM t WHERE a = 1")));
	assert_eq!(
		base,
		digest(&statement("SELECT  *\n FROM t /* a comment */ WHERE   a = 1 -- trailing\n"))
	);
	assert_ne!(base, digest(&statement("SELECT * FROM t WHERE a = 2")));
	assert_ne!(base, digest(&statement("SELECT * FROM u WHERE a = 1")));

	let key = arm_key(&base);
	assert_eq!(key, arm_key(&base));
	assert_eq!(key, u64::from_le_bytes(std::array::from_fn(|at| base.0[at])));
	assert_ne!(key, arm_key(&digest(&statement("SELECT * FROM u WHERE a = 1"))));
}

// ---------------------------------------------------------------------------------------------
// Names
// ---------------------------------------------------------------------------------------------

/// A statement reaches the tables of its sources, the functions it calls and its free names; a row
/// name is none of them.
#[test]
fn names_collect_reaches_tables_functions_params() {
	let names = collect(&statement("SELECT fn::f($p, $this) FROM t, u:1"));
	assert_eq!(names.tables, BTreeSet::from([TableName::from("t"), TableName::from("u")]));
	assert_eq!(names.functions, BTreeSet::from(["f".to_owned()]));
	assert_eq!(names.params, BTreeSet::from(["p".to_owned()]));
	assert!(!names.is_empty());

	let mut held = NameSet::default();
	assert!(held.is_empty());
	assert_eq!(names.missing_from(&held), names);
	held.extend(names.clone());
	assert!(names.missing_from(&held).is_empty());
}

/// A name bound inside the statement is not collected, nor an iteration name, nor a principal
/// name. A row name outside a region that binds it is.
#[test]
fn names_collect_only_free_names_of_the_text() {
	let names = collect(&statement(
		"RETURN { LET $x = 1; array::map([1], |$y| $y + $x + $z) + $auth.id + $this + $self }",
	));
	assert_eq!(names.params, BTreeSet::from(["z".to_owned()]));
	assert_eq!(
		collect(&statement("RETURN [$value, $input, $parent]")).params,
		BTreeSet::from(["value".to_owned(), "input".to_owned()])
	);
	assert!(collect(&statement("SELECT * FROM t WHERE a = $this.b")).params.is_empty());
	assert_eq!(
		collect(&statement("SELECT * FROM t WHERE a = $value")).params,
		BTreeSet::from(["value".to_owned()]),
		"`$value` is no row name of a statement's clause"
	);
	assert!(collect(&statement("RETURN { FOR $i IN [1] { $i }; 1 }")).params.is_empty());
}

/// A `record<t>` target is not a table the statement names.
#[test]
fn names_collect_ignores_record_kind_targets() {
	let names = collect(&statement("RETURN <record<u>> 'u:1'"));
	assert!(names.tables.is_empty());
}

// ---------------------------------------------------------------------------------------------
// Recorder
// ---------------------------------------------------------------------------------------------

/// A dependence recorded outside a statement is dropped, and the records come out in statement
/// order.
#[test]
fn recorder_records_only_inside_a_statement() {
	let recorder = DepRecorder::new();
	recorder.record(literal_dep(0));
	recorder.enter(2);
	recorder.record(literal_dep(1));
	recorder.leave();
	recorder.record(literal_dep(2));
	recorder.enter(1);
	recorder.leave();
	let records = recorder.finish();
	assert_eq!(records.iter().map(|(index, _)| *index).collect::<Vec<_>>(), [1, 2]);
	assert!(records[0].1.deps().is_empty());
	assert_eq!(records[1].1.deps(), [literal_dep(1)]);
}

/// A phase that could not record everything it consumed leaves the statement overflowed.
#[test]
fn absorb_incomplete_marks_overflow() {
	let mut deps = ValueDeps::new();
	deps.record(literal_dep(0));
	let recorder = DepRecorder::new();
	recorder.absorb(0, &deps, false);
	recorder.absorb(1, &deps, true);
	let records = recorder.finish();
	assert!(records[0].1.overflowed());
	assert_eq!(records[0].1.deps(), [literal_dep(0)]);
	assert!(!records[1].1.overflowed());
	assert_eq!(records[1].1.deps(), [literal_dep(0)]);
}

/// A record holds at most `MAX_VALUE_DEPS` distinct dependences and then reads as overflowed.
#[test]
fn value_deps_bounded() {
	let mut deps = ValueDeps::new();
	for ordinal in 0..300 {
		deps.record(literal_dep(ordinal));
	}
	assert!(deps.overflowed());
	assert_eq!(deps.deps().len(), MAX_VALUE_DEPS);
}

/// Once a statement is overflowed the recorder stores nothing further for it.
#[test]
fn record_after_overflow_stores_nothing() {
	let recorder = DepRecorder::new();
	recorder.enter(0);
	recorder.mark_overflowed();
	recorder.record(literal_dep(0));
	recorder.note_origin(SlotId(0), BindingOrigin::ClientParam);
	let deps = only(recorder);
	assert!(deps.overflowed());
	assert!(deps.deps().is_empty());
	assert_eq!(deps.origin(SlotId(0)), None);
}

/// A kept unit guards the slot, stored-parameter and absent names its record holds, and never a
/// principal or a literal; a principal read is reported apart.
#[test]
fn context_names_maps_slots_params_absent() {
	let slots = table_of("RETURN [$p]");
	let consumer = FoldConsumer::BranchChoice;
	let mut deps = ValueDeps::new();
	for value in [
		ValueRef::Slot(SlotId(0)),
		ValueRef::Param(Strand::from("q")),
		ValueRef::Absent(Strand::from("r")),
		ValueRef::Principal(Strand::from("auth")),
		ValueRef::Literal(LiteralOrdinal(0)),
	] {
		deps.record(dep(value, consumer));
	}
	assert_eq!(
		context_names(&deps, &slots),
		Some(BTreeSet::from(["p".to_owned(), "q".to_owned(), "r".to_owned()]))
	);
	assert!(reads_principal(&deps));

	let mut without = ValueDeps::new();
	without.record(dep(ValueRef::Param(Strand::from("q")), consumer));
	assert!(!reads_principal(&without));
	assert!(!reads_principal(&ValueDeps::new()));
}

// ---------------------------------------------------------------------------------------------
// The consumed walk, over a container read from its text form
// ---------------------------------------------------------------------------------------------

fn request() -> Ir {
	Ir::new(
		IrScope::Request {
			fragment: None,
		},
		RegionKind::Request,
	)
}

fn spec(kind: ExprOp, operands: &[ValueId]) -> OpSpec {
	OpSpec {
		kind: OpKind::Expr(kind),
		operands: operands.iter().copied().collect(),
		results: std::iter::once(TypeKnowledge::Unknown).collect(),
		frame: FrameId::new(0),
		snapshot: SnapshotId::new(0),
		loc: Location {
			stmt: 0,
			span: None,
			origin: LocOrigin::Synthesized,
		},
	}
}

/// Appends one `expr` op to the top block of `ir` and answers its result.
fn add(ir: &mut Ir, kind: ExprOp, operands: &[ValueId]) -> ValueId {
	let block = ir.region(ir.top()).expect("the top region exists").blocks[0];
	let mut facts = FactCache::new();
	let mut rw = Rewriter::new(ir, &mut facts);
	let op = rw.create(InsertPoint::End(block), spec(kind, operands)).expect("the op is built");
	rw.ir().op(op).expect("the op exists").results[0]
}

/// The container holding `1`, `$b`, `$auth` and an op combining them, read back from its text.
///
/// Answers the container, the literal's op and the combining op's result.
fn parsed_container() -> (Ir, OpId, ValueId) {
	let mut built = request();
	let one = add(&mut built, ExprOp::Literal(Box::new(Value::from(1i64))), &[]);
	let b = add(
		&mut built,
		ExprOp::FreeParam {
			name: Strand::from("b"),
		},
		&[],
	);
	let auth = add(
		&mut built,
		ExprOp::SessionParam {
			name: Strand::from("auth"),
		},
		&[],
	);
	let sum = add(
		&mut built,
		ExprOp::Binary {
			op: BinaryOperator::Add,
		},
		&[b, one],
	);
	add(&mut built, ExprOp::Array, &[sum, auth]);

	let text = print(&built, built.top(), PrintOpts::default());
	let ir = parse(
		&text,
		IrScope::Request {
			fragment: None,
		},
		registry::attr_syntaxes(),
	)
	.unwrap_or_else(|failure| panic!("{failure}\n{text}"));
	let ops: Vec<OpId> = ops_in(&ir, ir.top()).collect();
	let literal = ops
		.iter()
		.copied()
		.find(|op| {
			matches!(ir.op(*op).map(|data| &data.kind), Some(OpKind::Expr(ExprOp::Literal(..))))
		})
		.expect("the literal is read back");
	let combined = ir.op(*ops.last().expect("the container holds ops")).expect("the op exists");
	let result = combined.results[0];
	(ir, literal, result)
}

/// The walk from a combining op reaches the literal, the slot and the principal beneath it, and a
/// free name with no slot overflows the statement.
#[test]
fn consumed_walks_to_literal_slot_and_params() {
	let (ir, literal, value) = parsed_container();
	let slots = table_of("RETURN $b");
	let mut literals = LiteralTable::new();
	literals.insert(literal, LiteralOrdinal(0));

	let recorder = DepRecorder::new();
	recorder.enter(0);
	recorder.consumed(&ir, value, FoldConsumer::IndexChoice, &slots, &literals);
	let deps = only(recorder);
	let held: BTreeSet<ValueRef> = deps.deps().iter().map(|dep| dep.value.clone()).collect();
	assert_eq!(
		held,
		BTreeSet::from([
			ValueRef::Literal(LiteralOrdinal(0)),
			ValueRef::Slot(SlotId(0)),
			ValueRef::Principal(Strand::from("auth")),
		])
	);
	assert!(
		deps.deps()
			.iter()
			.all(|dep| dep.consumer == FoldConsumer::IndexChoice && dep.used == ValueUse::Exact)
	);
	assert!(reads_principal(&deps));
	assert!(!deps.overflowed());

	let recorder = DepRecorder::new();
	recorder.enter(0);
	recorder.consumed(&ir, value, FoldConsumer::IndexChoice, &SlotTable::default(), &literals);
	assert!(only(recorder).overflowed());

	// A walk over a chain longer than the bound stops and overflows.
	let mut chain = request();
	let mut tail = add(
		&mut chain,
		ExprOp::FreeParam {
			name: Strand::from("b"),
		},
		&[],
	);
	for _ in 0..MAX_CONSUMED_WALK {
		tail = add(&mut chain, ExprOp::Truthy, &[tail]);
	}
	let recorder = DepRecorder::new();
	recorder.enter(0);
	recorder.consumed(&chain, tail, FoldConsumer::BranchChoice, &slots, &LiteralTable::new());
	assert!(only(recorder).overflowed());
}

// ---------------------------------------------------------------------------------------------
// Agreement with the logical binder
// ---------------------------------------------------------------------------------------------

#[cfg(feature = "kv-mem")]
mod binding {
	use std::collections::BTreeSet;

	use super::{SlotTable, free_names, statement};
	use crate::exec::function::FunctionRegistry;
	use crate::exec::ir::build::logical::bind_request;
	use crate::exec::ir::core::ir::{Walk, WalkOrder};
	use crate::exec::ir::core::{IrScope, OpKind};
	use crate::exec::ir::expr::ops::ExprOp;
	use crate::exec::ir::iface::catalog::{EffectCx, SemanticCatalog};
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::ir::{RequestInput, RequestStatement};

	/// A semantic catalog that holds no definition.
	struct NoCatalog;

	impl SemanticCatalog for NoCatalog {}

	/// The statements the slot table and the binder are held to agree on: the forms the binder's
	/// own tests bind, with free names read at the top, in a closure, in a loop and after a
	/// `LET`.
	const CORPUS: [&str; 15] = [
		"RETURN { $x; LET $x = 1; $x }",
		"SELECT * FROM t WHERE a = $b AND c = $a AND d = $b",
		"SELECT * FROM $p",
		"UPDATE t SET a = $v WHERE b = $w",
		"FOR $i IN $xs { LET $j = $i; }",
		"RETURN array::map([1], |$y| $y + $z)",
		"RETURN { LET $k = 2; array::map([1], |$x| $x + $k) }",
		"RETURN { LET $k = $m; $k + $n }",
		"LET $a = $b + 1;",
		"RETURN $auth.id",
		"RETURN [$session, $token, $access]",
		"RETURN $value",
		"RETURN [$input, $before]",
		"SELECT * FROM t WHERE a = $before",
		"SELECT * FROM t WHERE $this.a = $value",
	];

	/// The names of the `expr.free_param` ops of the container `sql` binds to.
	async fn bound_free_names(sql: &str) -> BTreeSet<String> {
		let registry = FunctionRegistry::with_builtins();
		let frames = FrameTable::default();
		let cx = EffectCx {
			registry: &registry,
			catalog: &NoCatalog,
			frames: &frames,
		};
		let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
		let request_statements = RequestStatement::of(&parsed);
		let input = RequestInput::request(
			&request_statements,
			IrScope::Request {
				fragment: None,
			},
		);
		let ir = bind_request(&input, &cx).await.expect("the request binds");
		let mut names = BTreeSet::new();
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(OpKind::Expr(ExprOp::FreeParam {
				name,
			})) = ir.op(op).map(|data| &data.kind)
			{
				names.insert(name.as_str().to_owned());
			}
			Walk::Continue
		});
		names
	}

	/// The names the binder reads through `expr.free_param` are the names the slot table calls
	/// free, so a slot exists for every free read the binder builds.
	#[tokio::test]
	async fn slot_table_agrees_with_l3() {
		for sql in CORPUS {
			let table = SlotTable::of(&statement(sql));
			assert_eq!(bound_free_names(sql).await, free_names(&table), "{sql}");
		}
	}
}

// ---------------------------------------------------------------------------------------------
// Source scans
// ---------------------------------------------------------------------------------------------

/// The crate's source root.
fn src_root() -> PathBuf {
	Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) {
	let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display()));
	for entry in entries {
		let path = entry.expect("a directory entry reads").path();
		if path.is_dir() {
			collect_files(&path, out);
		} else if path.extension().is_some_and(|extension| extension == "rs") {
			out.push(path);
		}
	}
}

/// Every `.rs` file under the source root, as its path relative to the root with `/` separators
/// and its text, in path order.
fn sources() -> Vec<(String, String)> {
	let root = src_root();
	let mut files = Vec::new();
	collect_files(&root, &mut files);
	files.sort();
	files
		.into_iter()
		.map(|path| {
			let relative = path
				.strip_prefix(&root)
				.expect("the file is under the root")
				.to_string_lossy()
				.replace('\\', "/");
			let text = std::fs::read_to_string(&path)
				.unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
			(relative, text)
		})
		.collect()
}

const IR_DIR: &str = "exec/ir/";
const CACHE_DIR: &str = "exec/ir/cache/";
const THIS_FILE: &str = "exec/ir/cache/tests.rs";
const PLANNING_FILE: &str = "exec/ir/cache/planning.rs";

/// Whether a line of source is a comment.
fn is_comment(line: &str) -> bool {
	line.trim_start().starts_with("//")
}

/// Whether `line` declares process-wide state: a `static`, a `OnceLock`, a `LazyLock`, a
/// `thread_local!` or a `quick_cache`.
fn declares_standing_state(line: &str) -> bool {
	let item = line.trim_start();
	let item = item
		.strip_prefix("pub")
		.map_or(item, |rest| rest.trim_start_matches(|c: char| c != ' ' && c != '\t').trim_start());
	item.starts_with("static ")
		|| line.contains("OnceLock")
		|| line.contains("LazyLock")
		|| line.contains("thread_local!")
		|| line.contains("quick_cache")
}

/// The catalog types no standing state may hold.
const CATALOG_TYPES: [&str; 9] = [
	"TableDefinition",
	"FunctionDefinition",
	"ParamDefinition",
	"DatabaseDefinition",
	"IndexDefinition",
	"EventDefinition",
	"TableSnapshot",
	"CatalogSnapshot",
	"PlanningCatalog",
];

/// No catalog definition is held by anything that outlives one planning session, the memo is
/// built only where the pipeline is wired, and nothing outside the IR reaches into the planning
/// state except the files that consume it.
#[test]
fn no_standing_catalog_state() {
	/// The files outside `exec/ir/` that name this module.
	const CONSUMERS: [&str; 5] = [
		"dbs/executor.rs",
		"exec/vm/compile_cache.rs",
		"exec/vm/ir_catalog.rs",
		"exec/vm/units.rs",
		"exec/vm/serve.rs",
	];

	let mut standing = Vec::new();
	let mut constructed = Vec::new();
	let mut reached = Vec::new();
	for (path, text) in sources() {
		if path == THIS_FILE {
			continue;
		}
		let in_ir = path.starts_with(IR_DIR);
		for (number, line) in text.lines().enumerate() {
			if is_comment(line) {
				continue;
			}
			let at = format!("{path}:{}: {}", number + 1, line.trim());
			if in_ir
				&& declares_standing_state(line)
				&& CATALOG_TYPES.iter().any(|t| line.contains(t))
			{
				standing.push(at.clone());
			}
			if in_ir
				&& !path.starts_with(CACHE_DIR)
				&& path != "exec/ir/planning.rs"
				&& line.contains("PlanningCatalog::new(")
			{
				constructed.push(at.clone());
			}
			if !in_ir && !CONSUMERS.contains(&path.as_str()) && line.contains("ir::cache") {
				reached.push(at);
			}
		}
	}
	assert!(
		standing.is_empty(),
		"a catalog definition is held by process-wide state; the memo lives for one planning \
		 session only:\n{}",
		standing.join("\n")
	);
	assert!(
		constructed.is_empty(),
		"`PlanningCatalog::new` is called outside `exec/ir/planning.rs`; one memo is built per \
		 compile where the pipeline is wired:\n{}",
		constructed.join("\n")
	);
	assert!(
		reached.is_empty(),
		"the planning state is named outside `exec/ir/` by a file not on the consumer list:\n{}",
		reached.join("\n")
	);
}

/// The IR path resolves parameters and reads definitions only through the planning memo.
#[test]
fn ir_path_never_substitutes() {
	const SUBSTITUTION: [&str; 4] = [
		"bind_param",
		"resolve_param_value",
		"try_fold_to_literal",
		"resolve_projection_field_idioms",
	];
	const CATALOG_READS: [&str; 10] = [
		".get_tb(",
		".get_tb_by_name(",
		".all_tb_fields(",
		".all_tb_indexes(",
		".all_tb_events(",
		".all_tb_views(",
		".get_db_param(",
		".get_db_function(",
		".get_db_by_name(",
		"CatalogSnapshot::",
	];

	let mut found = Vec::new();
	for (path, text) in sources() {
		if !path.starts_with(IR_DIR) || path == PLANNING_FILE || path == THIS_FILE {
			continue;
		}
		for needle in SUBSTITUTION.iter().chain(CATALOG_READS.iter()) {
			if text.contains(needle) {
				found.push(format!("{path} names `{needle}`"));
			}
		}
	}
	assert!(
		found.is_empty(),
		"the IR path substitutes a parameter or reads a definition outside the planning memo:\n{}",
		found.join("\n")
	);
}

/// The calls of the four substitution functions of the planner, outside `exec/ir/`, are the ones
/// the scan allows and no others.
#[test]
fn substitution_sites_listed() {
	const FUNCTIONS: [&str; 4] = [
		"bind_param",
		"resolve_param_value",
		"try_fold_to_literal",
		"resolve_projection_field_idioms",
	];
	const LISTED: [(&str, usize); 8] = [
		("exec/index/access_path.rs", 1),
		("exec/planner/util/conditions.rs", 1),
		("exec/planner/util/literals.rs", 1),
		("exec/planner/util/params.rs", 2),
		("exec/planner/select/mod.rs", 1),
		("exec/planner.rs", 1),
		("exec/logical/resolve.rs", 1),
		("exec/logical/params.rs", 1),
	];

	/// The number of calls of `name` on `line`: an occurrence at a word boundary followed by an
	/// opening parenthesis, and not the name in a `fn` declaration.
	fn calls(line: &str, name: &str) -> usize {
		let mut count = 0;
		for (at, _) in line.match_indices(name) {
			let before = &line[..at];
			let bounded = !before.ends_with(|c: char| c.is_alphanumeric() || c == '_');
			let declared = before.trim_end().ends_with("fn");
			if bounded && !declared && line[at + name.len()..].starts_with('(') {
				count += 1;
			}
		}
		count
	}

	let mut found: BTreeMap<String, usize> = BTreeMap::new();
	for (path, text) in sources() {
		if path.starts_with(IR_DIR) {
			continue;
		}
		let held: usize = text
			.lines()
			.map(|line| FUNCTIONS.iter().map(|name| calls(line, name)).sum::<usize>())
			.sum();
		if held > 0 {
			found.insert(path, held);
		}
	}
	let listed: BTreeMap<String, usize> =
		LISTED.iter().map(|(path, count)| ((*path).to_owned(), *count)).collect();
	assert_eq!(
		found, listed,
		"a call of a parameter substitution function was added or removed; add the site to the \
		 substitution table of the parameter design (D5 section 5.4) and to `LISTED` here."
	);
	assert_eq!(listed.values().sum::<usize>(), 9);
}

// ---------------------------------------------------------------------------------------------
// Thread-safety of the planner's futures
// ---------------------------------------------------------------------------------------------

/// The memo is shared by reference across a planner's suspension points, and the futures that load
/// it are `Send`, so a planner future that awaits them is.
#[test]
fn planner_future_is_send() {
	fn assert_send<T: Send>(_: &T) {}
	fn assert_send_sync<T: Send + Sync>() {}

	fn futures(catalog: &PlanningCatalog, stmt: &TopLevelExpr) {
		assert_send(&catalog.preload(0, stmt));
		assert_send(&catalog.load_pending());
	}

	assert_send_sync::<PlanningCatalog>();
	assert_send_sync::<DepRecorder>();
	let _ = futures;
}

// ---------------------------------------------------------------------------------------------
// The memo over a datastore, end to end, and the record through the IR seam
// ---------------------------------------------------------------------------------------------

#[cfg(feature = "kv-mem")]
mod datastore {
	use std::collections::BTreeSet;
	use std::sync::Arc;

	use surrealdb_strand::Strand;

	use super::{
		BindingOrigin, FoldConsumer, FrameId, PlanningCatalog, SlotId, TableName, ValueDep,
		ValueDeps, ValueRef, ValueUse, context_names, reads_principal, statement, table_of,
	};
	use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::ctx::FrozenContext;
	use crate::exec::ir::cache::answer::{CatalogAnswer, ExactAnswer, FreeName, SessionValues};
	use crate::exec::ir::cache::planning::PlanBinding;
	use crate::exec::ir::core::ir::{Walk, WalkOrder};
	#[cfg(feature = "egraph")]
	use crate::exec::ir::egraph::{EgraphMode, set_mode};
	use crate::exec::ir::error::IrError;
	use crate::exec::ir::iface::IndexRef;
	use crate::exec::ir::iface::catalog::{BackendCaps, SourceKey};
	use crate::exec::ir::iface::choose::ValueKindClass;
	use crate::exec::ir::phys::access::{ReadVersion, TableAnswers};
	use crate::exec::operators::test_util::TestDb;
	use crate::expr::Idiom;
	use crate::kvs::{Datastore, Transaction, TransactionType};
	use crate::types::PublicVariables;
	use crate::val::Value;

	// -----------------------------------------------------------------------------------------
	// Support
	// -----------------------------------------------------------------------------------------

	/// The value `text` denotes.
	fn val(text: &str) -> Value {
		Value::from(crate::syn::value(text).unwrap_or_else(|e| panic!("`{text}` parses: {e}")))
	}

	/// The frame of a statement's own text.
	fn top() -> FrameId {
		FrameId::new(0)
	}

	async fn open(db: &TestDb, mode: TransactionType) -> Arc<Transaction> {
		Arc::new(db.datastore().transaction(mode).await.expect("a transaction opens"))
	}

	/// The ids of the `test` namespace and database.
	async fn ids(txn: &Transaction) -> (NamespaceId, DatabaseId) {
		let ns = txn.expect_ns_by_name("test").await.expect("namespace test");
		let db = txn.expect_db_by_name("test", "test").await.expect("database test");
		(ns.namespace_id, db.database_id)
	}

	/// A frozen context of the datastore over `txn`, binding each of `vars`.
	fn frozen(ds: &Datastore, txn: &Arc<Transaction>, vars: &[(&str, Value)]) -> FrozenContext {
		let mut ctx = ds.setup_ctx().expect("a context from the datastore");
		for (name, value) in vars {
			ctx.add_value(*name, Arc::new(value.clone()));
		}
		ctx.set_transaction(Arc::clone(txn));
		ctx.freeze()
	}

	/// A memo reading through `txn` under `session`, with `vars` bound in its context.
	async fn memo_over(
		db: &TestDb,
		txn: &Arc<Transaction>,
		session: SessionValues,
		vars: &[(&str, Value)],
	) -> PlanningCatalog {
		let (ns, dbid) = ids(txn).await;
		PlanningCatalog::new(
			Arc::clone(txn),
			frozen(db.datastore(), txn, vars),
			Some((ns, dbid)),
			session,
		)
		.with_database_names("test", "test")
	}

	/// The bindings that name each of `names` as a client parameter whose value the context holds.
	fn client<'a>(names: impl IntoIterator<Item = &'a str>) -> Vec<(String, PlanBinding)> {
		names
			.into_iter()
			.map(|name| (name.to_owned(), PlanBinding::context(BindingOrigin::ClientParam)))
			.collect()
	}

	/// A memo over a fresh read transaction, preloaded for `sql` as statement 0 and positioned
	/// inside it. Each of `vars` is a client parameter.
	async fn loaded(
		db: &TestDb,
		session: SessionValues,
		vars: &[(&str, Value)],
		sql: &str,
	) -> (PlanningCatalog, Arc<Transaction>) {
		let txn = open(db, TransactionType::Read).await;
		let memo = memo_over(db, &txn, session, vars).await;
		memo.preload(0, &statement(sql)).await.expect("the statement preloads");
		memo.enter_statement(0, top(), client(vars.iter().map(|(name, _)| *name)), false);
		(memo, txn)
	}

	/// The record of the first statement the memo recorded, empty when it recorded none.
	fn record_of(memo: PlanningCatalog) -> ValueDeps {
		memo.finish().into_iter().next().map_or_else(ValueDeps::new, |(_, deps)| deps)
	}

	fn dep_set(deps: &ValueDeps) -> BTreeSet<ValueDep> {
		deps.deps().iter().cloned().collect()
	}

	fn dep_of(value: ValueRef, consumer: FoldConsumer, used: ValueUse) -> ValueDep {
		ValueDep {
			value,
			consumer,
			used,
		}
	}

	/// Whether `deps` holds a dependence on `value`, whatever consumed it.
	fn depends_on(deps: &ValueDeps, value: &ValueRef) -> bool {
		deps.deps().iter().any(|dep| &dep.value == value)
	}

	/// Whether the memo's table `t` has a field named `name`.
	fn has_field(memo: &PlanningCatalog, name: &str) -> bool {
		memo.table(&TableName::from("t")).found().is_some_and(|snapshot| {
			snapshot.fields.iter().any(|field| field.name.to_raw_string() == name)
		})
	}

	const SETUP: &str = "
		DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
		DEFINE FUNCTION fn::f() { RETURN 1 } PERMISSIONS FULL;
		DEFINE PARAM $p VALUE 1 PERMISSIONS FULL;
	";

	// -----------------------------------------------------------------------------------------
	// Lookups
	// -----------------------------------------------------------------------------------------

	/// The `REFERENCE` fields that can name a record of a table are a database-wide read, so a
	/// delete of the table loads them with the statement and any other lookup of them is pending
	/// until `load_pending` reads them. After a catalog change the answer is unknown.
	#[tokio::test]
	async fn the_references_of_a_table_load_with_the_delete_that_purges_it() {
		let db = TestDb::new(
			"
			DEFINE TABLE author SCHEMALESS PERMISSIONS FULL;
			DEFINE TABLE post SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD by ON post TYPE record<author> REFERENCE ON DELETE CASCADE;
			",
		)
		.await;
		let author = TableName::from("author");
		let (memo, _txn) = loaded(&db, SessionValues::Fold, &[], "DELETE author:1").await;
		let CatalogAnswer::Found(held) = memo.referencing(&author) else {
			panic!("a delete loads the references of the table it purges");
		};
		let [(defining, fields)] = &held[..] else {
			panic!("one table defines a reference field: {held:?}");
		};
		assert_eq!(defining, &TableName::from("post"));
		assert_eq!(fields.len(), 1);

		let (memo, _txn) = loaded(&db, SessionValues::Fold, &[], "SELECT * FROM author").await;
		assert!(matches!(memo.referencing(&author), CatalogAnswer::NotLoaded));
		assert!(memo.load_pending().await.expect("the references load"));
		assert!(memo.referencing(&author).found().is_some());
		assert!(!memo.load_pending().await.expect("nothing is pending"));
		memo.mark_catalog_change();
		assert!(matches!(memo.referencing(&author), CatalogAnswer::Unknown));
	}

	/// Whether a table that can hold edges stores code a delete runs is a database-wide read: a
	/// delete loads it with the statement, any other lookup of it is pending until `load_pending`
	/// reads it, and after a catalog change it is unknown. An answer is noted as a read of the
	/// database's listing of tables, so code planned from it is not reused once a table is defined,
	/// removed or changed. A table declared `TYPE NORMAL` holds no edge, so its code does not
	/// count.
	#[tokio::test]
	async fn the_edge_code_loads_with_the_delete_and_reads_the_listing() {
		const BASE: &str = "
			DEFINE TABLE t TYPE NORMAL;
			DEFINE TABLE knows TYPE RELATION FROM t TO t;
			DEFINE TABLE log TYPE NORMAL;
			DEFINE EVENT logged ON t WHEN true THEN { CREATE log };
		";
		let db = TestDb::new(BASE).await;
		let (memo, txn) = loaded(&db, SessionValues::Fold, &[], "DELETE t").await;
		let (ns, dbid) = ids(&txn).await;
		let recording = txn.record_catalog_reads();
		assert!(matches!(memo.edge_code(), CatalogAnswer::Found(false)));
		assert!(recording.finish().listings.contains(&(ns, dbid)), "the listing is not read");

		for code in [
			"DEFINE EVENT e ON knows WHEN true THEN { CREATE log };",
			"DEFINE TABLE by_in AS SELECT count() FROM knows GROUP ALL;",
			"DEFINE FIELD r ON knows TYPE option<record<t>> REFERENCE;",
			"DEFINE FIELD k ON log TYPE option<record<knows>> REFERENCE;",
			"DEFINE TABLE other; DEFINE EVENT e ON other WHEN true THEN { CREATE log };",
		] {
			let db = TestDb::new(&format!("{BASE} {code}")).await;
			let (memo, _txn) = loaded(&db, SessionValues::Fold, &[], "DELETE t").await;
			assert!(matches!(memo.edge_code(), CatalogAnswer::Found(true)), "{code}");
		}

		let (memo, _txn) = loaded(&db, SessionValues::Fold, &[], "SELECT * FROM t").await;
		assert!(matches!(memo.edge_code(), CatalogAnswer::NotLoaded));
		assert!(memo.load_pending().await.expect("the edge code loads"));
		assert!(matches!(memo.edge_code(), CatalogAnswer::Found(false)));
		assert!(!memo.load_pending().await.expect("nothing is pending"));
		memo.mark_catalog_change();
		assert!(matches!(memo.edge_code(), CatalogAnswer::Unknown));
	}

	/// A field view is bound by the planner, which the memo does not hold: the first question is
	/// `NotLoaded` and leaves the table pending, the owner of the planner gives the view, and the
	/// view is unknown after a catalog change.
	#[tokio::test]
	async fn a_field_view_is_pending_until_the_planner_gives_it() {
		use crate::exec::ir::iface::catalog::FieldView;

		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let table = TableName::from("t");
		let (memo, _txn) = loaded(&db, SessionValues::Fold, &[], "UPDATE t SET a = 1").await;
		assert!(matches!(memo.field_view(&table), CatalogAnswer::NotLoaded));
		assert_eq!(memo.take_pending_field_views(), BTreeSet::from([table.clone()]));
		assert!(memo.take_pending_field_views().is_empty());
		memo.insert_field_view(table.clone(), FieldView::Enforced);
		assert!(matches!(
			memo.field_view(&table),
			CatalogAnswer::Found(view) if matches!(*view, FieldView::Enforced)
		));
		memo.mark_catalog_change();
		assert!(matches!(memo.field_view(&table), CatalogAnswer::Unknown));
	}

	/// A name the statement reaches is held after `preload`. A name it does not reach is not
	/// loaded until a lookup makes it pending and `load_pending` reads it, after which the
	/// transaction's catalog answers for it.
	#[tokio::test]
	async fn memo_answers_found_absent_not_loaded() {
		let db = TestDb::new(SETUP).await;
		let (memo, _txn) = loaded(&db, SessionValues::Fold, &[], "SELECT fn::f($p) FROM t").await;
		let (t, u) = (TableName::from("t"), TableName::from("u"));
		assert!(memo.table(&t).found().is_some());
		assert!(memo.function("f").found().is_some());
		assert!(memo.param("p").found().is_some());
		assert!(matches!(memo.table(&u), CatalogAnswer::NotLoaded));
		assert!(memo.load_pending().await.expect("the pending name loads"));
		assert!(matches!(memo.table(&u), CatalogAnswer::Absent));
		assert!(!memo.load_pending().await.expect("nothing is pending"));
	}

	/// A function the request's capabilities deny is answered `Absent` and its definition is
	/// so a consumer cannot inline it. A function the capabilities allow is answered from its
	/// definition.
	#[tokio::test]
	async fn denied_function_answers_absent() {
		use crate::dbs::capabilities::{Capabilities, Targets};

		let denied = Capabilities::all().with_functions(Targets::None);
		let db = TestDb::new_with_capabilities(SETUP, false, denied).await;
		let txn = open(&db, TransactionType::Read).await;
		let memo = memo_over(&db, &txn, SessionValues::Fold, &[]).await;
		let recording = txn.record_catalog_reads();
		memo.preload(0, &statement("SELECT fn::f($p) FROM t"))
			.await
			.expect("the statement preloads");
		assert!(matches!(memo.function("f"), CatalogAnswer::Absent));
		assert!(memo.param("p").found().is_some());
		assert!(recording.finish().functions.is_empty());
		assert!(!memo.load_pending().await.expect("nothing is pending"));
	}

	/// A function body and a field's `VALUE` clause reach the functions they call.
	#[tokio::test]
	async fn preload_reaches_callees_transitively() {
		let db = TestDb::new(
			"DEFINE FUNCTION fn::c() { RETURN 3 } PERMISSIONS FULL;
			 DEFINE FUNCTION fn::b() { RETURN 2 } PERMISSIONS FULL;
			 DEFINE FUNCTION fn::a() { RETURN fn::b() } PERMISSIONS FULL;
			 DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD v ON t TYPE int VALUE fn::c();",
		)
		.await;
		let (memo, _txn) = loaded(&db, SessionValues::Fold, &[], "SELECT fn::a() FROM t").await;
		assert!(memo.table(&TableName::from("t")).found().is_some());
		for name in ["a", "b", "c"] {
			assert!(memo.function(name).found().is_some(), "fn::{name}");
		}
		assert!(matches!(memo.function("d"), CatalogAnswer::NotLoaded));
	}

	/// A lookup answered from the memo is noted in a recording opened after the load.
	#[tokio::test]
	async fn memo_reads_are_recorded() {
		let db = TestDb::new(SETUP).await;
		let (memo, txn) = loaded(&db, SessionValues::Fold, &[], "SELECT fn::f($p) FROM t").await;
		let (ns, dbid) = ids(&txn).await;
		let t = TableName::from("t");
		let recording = txn.record_catalog_reads();
		assert!(memo.table(&t).found().is_some());
		assert!(memo.function("f").found().is_some());
		assert!(memo.param("p").found().is_some());
		let reads = recording.finish();
		assert!(reads.tables.contains(&(ns, dbid, t)));
		assert!(reads.functions.contains(&(ns, dbid, "f".to_owned())));
		assert!(reads.params.contains(&(ns, dbid, "p".to_owned())));
	}

	/// Preloading `SELECT fn::a($p) FROM t` holds exactly the tables, functions and stored
	/// parameters the statement shape needs, and the catalog reads the memo makes name no more:
	/// - the tables are `t` alone, since neither a `record<u>` field nor an unrelated table is
	///   reached by name;
	/// - the functions are `a` and what it and the field's `VALUE` call;
	/// - the parameters are `p` alone, since `$m` and `$n` are names a function body binds and `$x`
	///   is bound by a `LET` the statement does not hold.
	#[tokio::test]
	async fn memo_reads_no_more_than_main() {
		let db = TestDb::new(
			"DEFINE FUNCTION fn::c() { RETURN 3 } PERMISSIONS FULL;
			 DEFINE FUNCTION fn::b() { RETURN 2 } PERMISSIONS FULL;
			 DEFINE FUNCTION fn::a($n: int) { LET $m = $n; RETURN fn::b() + $m } PERMISSIONS FULL;
			 DEFINE FUNCTION fn::unrelated() { RETURN 4 } PERMISSIONS FULL;
			 DEFINE PARAM $p VALUE 1 PERMISSIONS FULL;
			 DEFINE PARAM $m VALUE 9 PERMISSIONS FULL;
			 DEFINE PARAM $n VALUE 8 PERMISSIONS FULL;
			 DEFINE PARAM $x VALUE 7 PERMISSIONS FULL;
			 DEFINE TABLE other SCHEMALESS PERMISSIONS FULL;
			 DEFINE TABLE u SCHEMALESS PERMISSIONS FULL;
			 DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			 DEFINE FIELD v ON t TYPE int VALUE fn::c();
			 DEFINE FIELD link ON t TYPE record<u>;",
		)
		.await;
		let txn = open(&db, TransactionType::Read).await;
		let memo = memo_over(&db, &txn, SessionValues::Fold, &[]).await;
		let recording = txn.record_catalog_reads();
		memo.preload(0, &statement("SELECT fn::a($p) FROM t"))
			.await
			.expect("the statement preloads");

		let t = TableName::from("t");
		assert!(memo.table(&t).found().is_some());
		for name in ["a", "b", "c"] {
			assert!(memo.function(name).found().is_some(), "fn::{name}");
		}
		assert!(memo.param("p").found().is_some());
		for table in ["u", "other"] {
			assert!(
				matches!(memo.table(&TableName::from(table)), CatalogAnswer::NotLoaded),
				"{table}"
			);
		}
		assert!(matches!(memo.function("unrelated"), CatalogAnswer::NotLoaded));
		for name in ["m", "n", "x"] {
			assert!(matches!(memo.param(name), CatalogAnswer::NotLoaded), "${name}");
		}

		let reads = recording.finish();
		assert!(!reads.opaque);
		let tables: BTreeSet<TableName> = reads.tables.iter().map(|(_, _, t)| t.clone()).collect();
		let functions: BTreeSet<&str> =
			reads.functions.iter().map(|(_, _, n)| n.as_str()).collect();
		let params: BTreeSet<&str> = reads.params.iter().map(|(_, _, n)| n.as_str()).collect();
		assert_eq!(tables, BTreeSet::from([t]));
		assert_eq!(functions, BTreeSet::from(["a", "b", "c"]));
		assert_eq!(params, BTreeSet::from(["p"]));
	}

	/// A stored parameter is read only for a free name the memo could answer from it: not in an
	/// `Isolated` compile, not for a name a binding answers, not for a name read at run time.
	#[tokio::test]
	async fn preload_reads_only_params_a_lookup_can_reach() {
		let db = TestDb::new(SETUP).await;
		let sql = "SELECT * FROM t WHERE a = $p";
		let (memo, _txn) = loaded(&db, SessionValues::Fold, &[], sql).await;
		assert!(memo.param("p").found().is_some());
		let (memo, _txn) = loaded(&db, SessionValues::Isolated, &[], sql).await;
		assert!(matches!(memo.param("p"), CatalogAnswer::NotLoaded));
		let (memo, _txn) = loaded(&db, SessionValues::Fold, &[("p", val("2"))], sql).await;
		assert!(matches!(memo.param("p"), CatalogAnswer::NotLoaded));
		let txn = open(&db, TransactionType::Read).await;
		let memo =
			memo_over(&db, &txn, SessionValues::RunTime, &[]).await.with_shadowed(["p".to_owned()]);
		memo.preload(0, &statement(sql)).await.expect("the statement preloads");
		assert!(matches!(memo.param("p"), CatalogAnswer::NotLoaded));
	}

	/// A definition a transaction has not committed is read by that transaction's memo and by no
	/// other.
	#[tokio::test]
	async fn uncommitted_ddl_is_private() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; DEFINE FIELD g ON t TYPE int;").await;
		let writer = open(&db, TransactionType::Write).await;
		let (ns, dbid) = ids(&writer).await;
		let t = TableName::from("t");
		let mut field = (*writer
			.get_tb_field(ns, dbid, &t, "g", None)
			.await
			.expect("the field reads")
			.expect("the field is defined"))
		.clone();
		field.name = Idiom::field("f");
		writer.put_tb_field(ns, dbid, &t, &field).await.expect("the field is stored");

		let stmt = statement("SELECT * FROM t");
		let own = memo_over(&db, &writer, SessionValues::Fold, &[]).await;
		own.preload(0, &stmt).await.expect("the writer's memo preloads");
		let reader = open(&db, TransactionType::Read).await;
		let other = memo_over(&db, &reader, SessionValues::Fold, &[]).await;
		other.preload(0, &stmt).await.expect("the reader's memo preloads");

		assert!(has_field(&own, "g"));
		assert!(has_field(&own, "f"), "the writer's memo reads its own uncommitted field");
		assert!(has_field(&other, "g"));
		assert!(!has_field(&other, "f"), "a reader's memo read an uncommitted field");
		writer.cancel().await.expect("the writer cancels");
	}

	/// A function a transaction has defined and not committed is not served to another
	/// transaction's memo through the datastore's cache, whether that transaction opens while the
	/// definition is pending or after it is cancelled.
	#[tokio::test]
	async fn uncommitted_definition_not_in_ds_cache() {
		let db = TestDb::new("DEFINE FUNCTION fn::f() { RETURN 1 } PERMISSIONS FULL;").await;
		let writer = open(&db, TransactionType::Write).await;
		let (ns, dbid) = ids(&writer).await;
		let mut def =
			(*writer.get_db_function(ns, dbid, "f", None).await.expect("the function reads"))
				.clone();
		def.name = Strand::from("g");
		writer.put_db_function(ns, dbid, &def).await.expect("the function is stored");

		let stmt = statement("RETURN fn::g()");
		let own = memo_over(&db, &writer, SessionValues::Fold, &[]).await;
		own.preload(0, &stmt).await.expect("the writer's memo preloads");
		assert!(own.function("g").found().is_some());

		let reader = open(&db, TransactionType::Read).await;
		let other = memo_over(&db, &reader, SessionValues::Fold, &[]).await;
		other.preload(0, &stmt).await.expect("the reader's memo preloads");
		assert!(matches!(other.function("g"), CatalogAnswer::Absent));

		writer.cancel().await.expect("the writer cancels");
		let after = open(&db, TransactionType::Read).await;
		let fresh = memo_over(&db, &after, SessionValues::Fold, &[]).await;
		fresh.preload(0, &stmt).await.expect("a later memo preloads");
		assert!(matches!(fresh.function("g"), CatalogAnswer::Absent));
	}

	// -----------------------------------------------------------------------------------------
	// Free names
	// -----------------------------------------------------------------------------------------

	/// A name the context binds shadows a stored parameter of the same name. With no binding the
	/// stored parameter answers, and a name bound nowhere is `NONE`.
	#[tokio::test]
	async fn free_names_follow_bind_param_order() {
		let db = TestDb::new(SETUP).await;
		let sql = "SELECT * FROM t WHERE a = $p AND b = $z";
		let consumer = FoldConsumer::IndexChoice;

		let (bound, _txn) = loaded(&db, SessionValues::Fold, &[("p", val("2"))], sql).await;
		assert_eq!(bound.free_value_exact("p", top(), consumer), ExactAnswer::Value(val("2")));
		let record = record_of(bound);
		assert_eq!(
			dep_set(&record),
			BTreeSet::from([dep_of(ValueRef::Slot(SlotId(0)), consumer, ValueUse::Exact)])
		);

		let (stored, _txn) = loaded(&db, SessionValues::Fold, &[], sql).await;
		assert_eq!(stored.free_value_exact("p", top(), consumer), ExactAnswer::Value(val("1")));
		assert_eq!(stored.free_value_exact("z", top(), consumer), ExactAnswer::Value(Value::None));
		let record = record_of(stored);
		assert_eq!(
			dep_set(&record),
			BTreeSet::from([
				dep_of(ValueRef::Param(Strand::from("p")), consumer, ValueUse::Exact),
				dep_of(ValueRef::Absent(Strand::from("z")), consumer, ValueUse::Exact),
			])
		);
	}

	/// A row name read outside any row region is answered from a client binding and recorded
	/// under its slot. Every name but the iteration names (`$this`, `$self`, `$parent`) is handled
	/// the same way. The iteration names are read when the statement runs.
	#[tokio::test]
	async fn row_names_outside_a_row_region_fold_a_binding() {
		let db = TestDb::new(SETUP).await;
		let (memo, _txn) = loaded(
			&db,
			SessionValues::Fold,
			&[("value", val("5")), ("this", val("6"))],
			"RETURN [$value, $this]",
		)
		.await;
		let consumer = FoldConsumer::BranchChoice;
		assert_eq!(memo.free_value_exact("value", top(), consumer), ExactAnswer::Value(val("5")));
		assert_eq!(memo.free_value_exact("this", top(), consumer), ExactAnswer::RunTime);
		let record = record_of(memo);
		assert_eq!(
			dep_set(&record),
			BTreeSet::from([dep_of(ValueRef::Slot(SlotId(0)), consumer, ValueUse::Exact)])
		);
		assert!(!record.overflowed());
	}

	/// A stored parameter whose permission is not `FULL` is read when the statement runs: it is
	/// never answered at plan time, and nothing is recorded for it.
	#[tokio::test]
	async fn define_param_permission_still_checked() {
		let db = TestDb::new("DEFINE PARAM $s VALUE 1 PERMISSIONS NONE;").await;
		let (memo, _txn) =
			loaded(&db, SessionValues::Fold, &[], "SELECT * FROM t WHERE a = $s").await;
		assert_eq!(memo.free_name("s", top()), FreeName::RunTime);
		assert_eq!(
			memo.free_value_exact("s", top(), FoldConsumer::IndexChoice),
			ExactAnswer::RunTime
		);
		assert_eq!(memo.free_kind("s", top(), FoldConsumer::IndexChoice), FreeName::RunTime);
		let record = record_of(memo);
		assert!(record.deps().is_empty());
		assert!(!record.overflowed());
	}

	/// A frame other than the statement's own text is definer code, which no caller binding and
	/// no stored parameter reaches at plan time.
	#[tokio::test]
	async fn definer_frame_names_record_nothing() {
		let db = TestDb::new(SETUP).await;
		let (memo, _txn) = loaded(
			&db,
			SessionValues::Fold,
			&[("x", val("1"))],
			"SELECT * FROM t WHERE a = $p AND b = $x",
		)
		.await;
		let definer = FrameId::new(1);
		for name in ["p", "x", "auth"] {
			assert_eq!(memo.free_name(name, definer), FreeName::RunTime, "{name}");
			assert_eq!(
				memo.free_value_exact(name, definer, FoldConsumer::IndexChoice),
				ExactAnswer::RunTime,
				"{name}"
			);
		}
		let record = record_of(memo);
		assert!(record.deps().is_empty());
		assert!(!record.overflowed());
	}

	/// After an op of the statement changed the catalog, free names answer `RunTime` until the
	/// statement ends. The next statement answers a client binding again, and a stored parameter
	/// at run time, since the definition it would read predates the change.
	#[tokio::test]
	async fn in_statement_ddl_not_folded() {
		let db = TestDb::new(SETUP).await;
		let (memo, _txn) = loaded(
			&db,
			SessionValues::Fold,
			&[("x", val("2"))],
			"SELECT * FROM t WHERE a = $p AND b = $x",
		)
		.await;
		let consumer = FoldConsumer::IndexChoice;
		assert_eq!(memo.free_value_exact("p", top(), consumer), ExactAnswer::Value(val("1")));
		assert_eq!(memo.free_value_exact("x", top(), consumer), ExactAnswer::Value(val("2")));
		memo.mark_catalog_change();
		assert_eq!(memo.free_value_exact("p", top(), consumer), ExactAnswer::RunTime);
		assert_eq!(memo.free_value_exact("x", top(), consumer), ExactAnswer::RunTime);
		assert_eq!(memo.free_name("p", top()), FreeName::RunTime);
		memo.leave_statement();
		assert_eq!(memo.free_value_exact("p", top(), consumer), ExactAnswer::RunTime);
		memo.enter_statement(1, top(), client(["x"]), false);
		assert_eq!(memo.free_value_exact("p", top(), consumer), ExactAnswer::RunTime);
		assert_eq!(memo.free_value_exact("x", top(), consumer), ExactAnswer::Value(val("2")));
	}

	/// From a catalog change to the end of the compile, a definition lookup answers `Unknown`:
	/// for a held definition, for a name the memo does not hold (which does not become pending),
	/// in the statement that changed the catalog and in every later one.
	#[tokio::test]
	async fn definitions_after_a_catalog_change_are_unknown() {
		let db = TestDb::new(SETUP).await;
		let (memo, _txn) = loaded(&db, SessionValues::Fold, &[], "SELECT fn::f($p) FROM t").await;
		let (t, u) = (TableName::from("t"), TableName::from("u"));
		assert!(memo.table(&t).found().is_some());
		assert!(memo.events(&t).found().is_some());
		assert!(memo.views(&t).found().is_some());
		assert!(memo.indexes(&t).found().is_some());
		assert!(memo.function("f").found().is_some());
		assert!(memo.param("p").found().is_some());

		memo.mark_catalog_change();
		for statement in [0, 1, 2] {
			if statement > 0 {
				memo.leave_statement();
				memo.enter_statement(statement, top(), Vec::new(), false);
			}
			assert!(
				matches!(memo.table(&t), CatalogAnswer::Unknown),
				"table, statement {statement}"
			);
			assert!(matches!(memo.events(&t), CatalogAnswer::Unknown), "events");
			assert!(matches!(memo.views(&t), CatalogAnswer::Unknown), "views");
			assert!(matches!(memo.indexes(&t), CatalogAnswer::Unknown), "indexes");
			assert!(matches!(memo.function("f"), CatalogAnswer::Unknown), "function");
			assert!(matches!(memo.param("p"), CatalogAnswer::Unknown), "param");
			assert!(matches!(memo.table(&u), CatalogAnswer::Unknown), "unheld table");
			assert!(matches!(memo.function("g"), CatalogAnswer::Unknown), "unheld function");
			assert!(matches!(memo.param("q"), CatalogAnswer::Unknown), "unheld param");
		}
		assert!(!memo.load_pending().await.expect("nothing is pending"));
	}

	/// The answers of a table read are held by the caller of the fill. A read the memo does not
	/// hold is pending until its answers are inserted, and a catalog change ends them.
	#[tokio::test]
	async fn table_answers_wait_for_the_fill_and_end_at_a_catalog_change() {
		let db = TestDb::new(SETUP).await;
		let (memo, _txn) = loaded(&db, SessionValues::Fold, &[], "SELECT * FROM t").await;
		let key: SourceKey = (TableName::from("t"), None, ReadVersion::Current);
		assert!(matches!(memo.table_answers(&key), CatalogAnswer::NotLoaded));
		assert_eq!(memo.take_pending_sources(), vec![key.clone()]);
		assert!(memo.take_pending_sources().is_empty());
		assert!(!memo.holds_answers(&key));

		memo.insert_answers(key.clone(), Arc::new(TableAnswers::plain("t".into(), true)));
		assert!(memo.holds_answers(&key));
		assert!(matches!(memo.table_answers(&key), CatalogAnswer::Found(_)));
		assert!(memo.take_pending_sources().is_empty());

		memo.mark_catalog_change();
		assert!(matches!(memo.table_answers(&key), CatalogAnswer::Unknown));
	}

	/// An index is ready when the answers of the table's current read hold it among the online
	/// indexes; the readiness of an index the table does not define is `Absent`.
	#[tokio::test]
	async fn an_index_is_ready_when_the_current_answers_hold_it() {
		let db = TestDb::new(&format!("{SETUP} DEFINE INDEX ix ON t FIELDS x;")).await;
		let (memo, _txn) = loaded(&db, SessionValues::Fold, &[], "SELECT * FROM t").await;
		let t = TableName::from("t");
		let ix = IndexRef {
			table: t.clone(),
			index: Strand::from("ix"),
			position: 0,
			scalar: Box::new([]),
		};
		assert!(matches!(memo.index_ready(&ix), CatalogAnswer::NotLoaded));
		let all = memo.indexes(&t).found().cloned().expect("the table's indexes");
		let key: SourceKey = (t.clone(), None, ReadVersion::Current);
		memo.insert_answers(
			key.clone(),
			Arc::new(TableAnswers::plain(t.clone(), true).with_indexes(all.to_vec())),
		);
		assert!(matches!(memo.index_ready(&ix), CatalogAnswer::Found(true)));
		memo.insert_answers(key, Arc::new(TableAnswers::plain(t.clone(), true)));
		assert!(matches!(memo.index_ready(&ix), CatalogAnswer::Found(false)));
		let missing = IndexRef {
			table: t,
			index: Strand::from("nosuch"),
			position: 0,
			scalar: Box::new([]),
		};
		assert!(matches!(memo.index_ready(&missing), CatalogAnswer::Absent));
	}

	/// The backend is what the compile's caller states, and the conservative default otherwise.
	#[tokio::test]
	async fn the_backend_is_what_the_caller_states() {
		let db = TestDb::new(SETUP).await;
		let txn = open(&db, TransactionType::Read).await;
		let memo = memo_over(&db, &txn, SessionValues::Fold, &[]).await;
		assert_eq!(memo.backend(), BackendCaps::default());
		let caps = BackendCaps {
			offers_read_views: true,
			reads_tracked: false,
			offers_write_views: false,
		};
		let memo = memo_over(&db, &txn, SessionValues::Fold, &[]).await.with_backend(caps);
		assert_eq!(memo.backend(), caps);
	}

	/// A catalog change marked between statements still ends the memo's definitions, and
	/// entering a statement does not reset it.
	#[tokio::test]
	async fn a_catalog_change_outside_a_statement_is_kept() {
		let db = TestDb::new(SETUP).await;
		let (memo, _txn) = loaded(&db, SessionValues::Fold, &[], "SELECT * FROM t").await;
		memo.leave_statement();
		memo.mark_catalog_change();
		memo.enter_statement(1, top(), Vec::new(), false);
		assert!(matches!(memo.table(&TableName::from("t")), CatalogAnswer::Unknown));
	}

	/// An `Isolated` compile answers no free name at plan time, whatever its context binds and
	/// whatever the database defines, and records nothing.
	#[tokio::test]
	async fn function_body_compile_folds_no_caller_binding() {
		let db = TestDb::new("DEFINE PARAM $p VALUE 2 PERMISSIONS FULL;").await;
		let (memo, _txn) = loaded(
			&db,
			SessionValues::Isolated,
			&[("x", val("1")), ("auth", val("'someone'"))],
			"RETURN [$x, $p, $auth]",
		)
		.await;
		for name in ["x", "p", "auth"] {
			assert_eq!(memo.free_name(name, top()), FreeName::RunTime, "{name}");
			assert_eq!(
				memo.free_value_exact(name, top(), FoldConsumer::BranchChoice),
				ExactAnswer::RunTime,
				"{name}"
			);
		}
		let finished = memo.finish();
		assert!(finished.iter().all(|(_, deps)| deps.deps().is_empty() && !deps.overflowed()));
	}

	/// A name the unit is told is shadowed is read when the unit runs, even where the context
	/// binds it.
	#[tokio::test]
	async fn shadowed_names_read_at_run_time() {
		let db = TestDb::new(SETUP).await;
		let txn = open(&db, TransactionType::Read).await;
		let memo = memo_over(&db, &txn, SessionValues::RunTime, &[("x", val("1"))])
			.await
			.with_shadowed(["x".to_owned()]);
		memo.preload(0, &statement("RETURN $x")).await.expect("the statement preloads");
		memo.enter_statement(0, top(), client(["x"]), false);
		assert_eq!(memo.free_name("x", top()), FreeName::RunTime);
		assert_eq!(
			memo.free_value_exact("x", top(), FoldConsumer::BranchChoice),
			ExactAnswer::RunTime
		);
		let record = record_of(memo);
		assert!(record.deps().is_empty());
		assert!(!record.overflowed());
	}

	/// A principal name is a plan-time value for a request's own statement and for no unit that
	/// is served to every principal of a class.
	#[tokio::test]
	async fn unit_compile_folds_no_principal() {
		let db = TestDb::new(SETUP).await;
		let vars = [("auth", val("{ id: user:alice }"))];
		let sql = "SELECT * FROM t WHERE owner = $auth.id";
		let consumer = FoldConsumer::IndexChoice;

		let (unit, _txn) = loaded(&db, SessionValues::RunTime, &vars, sql).await;
		assert_eq!(unit.free_name("auth", top()), FreeName::Principal);
		assert_eq!(unit.free_value_exact("auth", top(), consumer), ExactAnswer::RunTime);
		let record = record_of(unit);
		assert!(!reads_principal(&record));
		assert!(record.deps().is_empty());

		let (request, _txn) = loaded(&db, SessionValues::Fold, &vars, sql).await;
		assert_eq!(request.free_name("auth", top()), FreeName::Principal);
		assert_eq!(
			request.free_value_exact("auth", top(), consumer),
			ExactAnswer::Value(val("{ id: user:alice }"))
		);
		let record = record_of(request);
		assert!(reads_principal(&record));
		assert_eq!(
			dep_set(&record),
			BTreeSet::from([dep_of(
				ValueRef::Principal(Strand::from("auth")),
				consumer,
				ValueUse::Exact
			)])
		);
	}

	/// A bound name records the slot it is read through and where its value came from. A name
	/// whose value only the run computes is read at run time, as is a name the context binds that
	/// the caller lists no binding for: neither is answered from the context, and neither
	/// records a dependence.
	#[tokio::test]
	async fn computed_let_origin_recorded() {
		let db = TestDb::new("").await;
		let txn = open(&db, TransactionType::Read).await;
		let vars = [("t", val("'a'")), ("c", val("1")), ("u", val("2")), ("k", val("3"))];
		let memo = memo_over(&db, &txn, SessionValues::Fold, &vars).await;
		memo.preload(0, &statement("RETURN [$t, $c, $u, $k]"))
			.await
			.expect("the statement preloads");
		memo.enter_statement(
			0,
			top(),
			[
				("t".to_owned(), PlanBinding::computed()),
				("c".to_owned(), PlanBinding::context(BindingOrigin::ClientParam)),
				("k".to_owned(), PlanBinding::constant(val("4"))),
			],
			false,
		);
		let consumer = FoldConsumer::DynamicTargetTable;
		assert_eq!(memo.free_value_exact("t", top(), consumer), ExactAnswer::RunTime);
		assert_eq!(memo.free_name("t", top()), FreeName::RunTime);
		assert_eq!(memo.free_value_exact("u", top(), consumer), ExactAnswer::RunTime);
		assert_eq!(memo.free_value_exact("c", top(), consumer), ExactAnswer::Value(val("1")));
		assert_eq!(memo.free_value_exact("k", top(), consumer), ExactAnswer::Value(val("4")));
		let record = record_of(memo);
		assert_eq!(
			dep_set(&record),
			BTreeSet::from([
				dep_of(ValueRef::Slot(SlotId(1)), consumer, ValueUse::Exact),
				dep_of(ValueRef::Slot(SlotId(3)), consumer, ValueUse::Exact),
			])
		);
		assert_eq!(record.origin(SlotId(1)), Some(BindingOrigin::ClientParam));
		assert_eq!(record.origin(SlotId(3)), Some(BindingOrigin::LetConst));
	}

	/// A name a top-level `LET` computes is read at run time even where the catalog stores a
	/// parameter of that name, and a later statement that binds it as a constant answers that
	/// constant.
	#[tokio::test]
	async fn computed_let_does_not_fold_a_stored_parameter() {
		let db = TestDb::new(SETUP).await;
		let txn = open(&db, TransactionType::Read).await;
		let memo = memo_over(&db, &txn, SessionValues::Fold, &[]).await;
		let sql = "SELECT * FROM t WHERE a = $p";
		memo.preload(0, &statement(sql)).await.expect("the statement preloads");
		let consumer = FoldConsumer::IndexChoice;
		memo.enter_statement(0, top(), [("p".to_owned(), PlanBinding::computed())], false);
		assert_eq!(memo.free_name("p", top()), FreeName::RunTime);
		assert_eq!(memo.free_value_exact("p", top(), consumer), ExactAnswer::RunTime);
		assert_eq!(memo.free_kind("p", top(), consumer), FreeName::RunTime);
		memo.leave_statement();
		memo.enter_statement(1, top(), [("p".to_owned(), PlanBinding::constant(val("5")))], false);
		assert_eq!(memo.free_value_exact("p", top(), consumer), ExactAnswer::Value(val("5")));
		memo.leave_statement();
		memo.enter_statement(2, top(), Vec::new(), false);
		assert_eq!(memo.free_value_exact("p", top(), consumer), ExactAnswer::Value(val("1")));
	}

	/// A statement that reads at a `VERSION` stamp answers every definition `Unknown`, holds no
	/// name pending, and reads a stored parameter at run time, since the memo holds the current
	/// definitions and not the stamp's. The next statement answers them again.
	#[tokio::test]
	async fn versioned_statement_answers_definitions_unknown() {
		let db = TestDb::new(SETUP).await;
		let (memo, _txn) = loaded(&db, SessionValues::Fold, &[], "SELECT fn::f($p) FROM t").await;
		let t = TableName::from("t");
		assert!(memo.function("f").found().is_some());
		assert!(memo.database().is_some());
		memo.leave_statement();
		memo.enter_statement(1, top(), Vec::new(), true);
		assert!(matches!(memo.table(&t), CatalogAnswer::Unknown));
		assert!(matches!(memo.events(&t), CatalogAnswer::Unknown));
		assert!(matches!(memo.views(&t), CatalogAnswer::Unknown));
		assert!(matches!(memo.indexes(&t), CatalogAnswer::Unknown));
		assert!(matches!(memo.function("f"), CatalogAnswer::Unknown));
		assert!(matches!(memo.function("g"), CatalogAnswer::Unknown));
		assert!(matches!(memo.param("p"), CatalogAnswer::Unknown));
		assert!(memo.database().is_none());
		assert_eq!(memo.free_name("p", top()), FreeName::RunTime);
		assert!(!memo.load_pending().await.expect("nothing is pending"));
		memo.leave_statement();
		memo.enter_statement(2, top(), Vec::new(), false);
		assert!(memo.function("f").found().is_some());
		assert!(memo.table(&t).found().is_some());
	}

	/// A consumer that branches on the class of a value records the class, and a name bound
	/// nowhere records that it has no value.
	#[tokio::test]
	async fn free_kind_records_kind_class() {
		let db = TestDb::new("DEFINE PARAM $p VALUE 1 PERMISSIONS FULL;").await;
		let txn = open(&db, TransactionType::Read).await;
		let vars = [
			("r", val("t:1")),
			("tb", Value::Table(TableName::from("t"))),
			("arr", val("[1, 2]")),
			("s", val("'a'")),
		];
		let memo = memo_over(&db, &txn, SessionValues::Fold, &vars).await;
		memo.preload(0, &statement("RETURN [$r, $tb, $arr, $s, $p, $z]"))
			.await
			.expect("the statement preloads");
		memo.enter_statement(0, top(), client(["r", "tb", "arr", "s"]), false);
		let consumer = FoldConsumer::DynamicTargetTable;
		let expected = [
			("r", ValueKindClass::RecordId),
			("tb", ValueKindClass::Table),
			("arr", ValueKindClass::Array),
			("s", ValueKindClass::Other),
			("p", ValueKindClass::Other),
		];
		for (name, class) in expected {
			assert_eq!(memo.free_kind(name, top(), consumer), FreeName::Bound(class), "{name}");
		}
		assert_eq!(memo.free_kind("z", top(), consumer), FreeName::Unbound);
		let record = record_of(memo);
		assert_eq!(
			dep_set(&record),
			BTreeSet::from([
				dep_of(
					ValueRef::Slot(SlotId(0)),
					consumer,
					ValueUse::Kind(ValueKindClass::RecordId)
				),
				dep_of(ValueRef::Slot(SlotId(1)), consumer, ValueUse::Kind(ValueKindClass::Table)),
				dep_of(ValueRef::Slot(SlotId(2)), consumer, ValueUse::Kind(ValueKindClass::Array)),
				dep_of(ValueRef::Slot(SlotId(3)), consumer, ValueUse::Kind(ValueKindClass::Other)),
				dep_of(
					ValueRef::Param(Strand::from("p")),
					consumer,
					ValueUse::Kind(ValueKindClass::Other)
				),
				dep_of(ValueRef::Absent(Strand::from("z")), consumer, ValueUse::Unbound),
			])
		);
	}

	// -----------------------------------------------------------------------------------------
	// Requests through `execute`
	// -----------------------------------------------------------------------------------------

	/// Every statement's value of `sql` run as `session` with `vars`, an error rendered as its
	/// message.
	async fn answers(
		db: &TestDb,
		session: &crate::dbs::Session,
		vars: Option<PublicVariables>,
		sql: &str,
	) -> Vec<Value> {
		let results = db.datastore().execute(sql, session, vars).await.expect("the request runs");
		results
			.into_iter()
			.map(|r| r.result.map(Value::from).unwrap_or_else(|e| Value::from(e.to_string())))
			.collect()
	}

	async fn owner_answers(db: &TestDb, sql: &str) -> Vec<Value> {
		answers(db, &TestDb::owner(), None, sql).await
	}

	/// The value of the last statement of `sql`.
	async fn last_answer(db: &TestDb, sql: &str) -> Value {
		owner_answers(db, sql).await.pop().expect("the request answers")
	}

	/// A stored parameter redefined between two requests is the value the second request reads.
	#[tokio::test]
	async fn define_param_overwrite_across_requests() {
		let db = TestDb::new(
			"DEFINE TABLE t; CREATE t:1, t:2; DEFINE PARAM $p VALUE t:1 PERMISSIONS FULL;",
		)
		.await;
		let sql = "SELECT VALUE id FROM $p";
		assert_eq!(last_answer(&db, sql).await, val("[t:1]"));
		db.run("DEFINE PARAM OVERWRITE $p VALUE t:2 PERMISSIONS FULL").await;
		assert_eq!(last_answer(&db, sql).await, val("[t:2]"));
	}

	/// A function redefined between two requests is the body the second request runs.
	#[tokio::test]
	async fn define_function_overwrite_across_requests() {
		let db = TestDb::new(
			"DEFINE TABLE t; CREATE t:1, t:2;
			 DEFINE FUNCTION fn::f() { RETURN t:1 } PERMISSIONS FULL;",
		)
		.await;
		let sql = "SELECT VALUE id FROM fn::f()";
		assert_eq!(last_answer(&db, sql).await, val("[t:1]"));
		db.run("DEFINE FUNCTION OVERWRITE fn::f() { RETURN t:2 } PERMISSIONS FULL").await;
		assert_eq!(last_answer(&db, sql).await, val("[t:2]"));
	}

	/// A function a caller reaches through another function is read as of the second request
	/// when its definition changed between them.
	#[tokio::test]
	async fn callee_change_seen() {
		let db = TestDb::new(
			"DEFINE TABLE t; CREATE t:1, t:2;
			 DEFINE FUNCTION fn::b() { RETURN t:1 } PERMISSIONS FULL;
			 DEFINE FUNCTION fn::a() { RETURN fn::b() } PERMISSIONS FULL;",
		)
		.await;
		let sql = "SELECT VALUE id FROM fn::a()";
		assert_eq!(last_answer(&db, sql).await, val("[t:1]"));
		db.run("DEFINE FUNCTION OVERWRITE fn::b() { RETURN t:2 } PERMISSIONS FULL").await;
		assert_eq!(last_answer(&db, sql).await, val("[t:2]"));
	}

	const FIELD_SETUP: &str = "
		DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
		DEFINE FIELD v ON t VALUE 1;
	";

	/// A field's `VALUE` clause redefined between two requests is the clause the second
	/// request's write applies.
	async fn catalog_change_between_requests_on(db: &TestDb) {
		assert_eq!(last_answer(db, "CREATE t:1 RETURN VALUE v").await, val("[1]"));
		db.run("DEFINE FIELD OVERWRITE v ON t VALUE 2").await;
		assert_eq!(last_answer(db, "CREATE t:2 RETURN VALUE v").await, val("[2]"));
	}

	#[tokio::test]
	async fn catalog_change_between_requests() {
		let db = TestDb::new(FIELD_SETUP).await;
		catalog_change_between_requests_on(&db).await;
	}

	#[cfg(feature = "kv-surrealkv")]
	#[tokio::test]
	async fn catalog_change_between_requests_on_surrealkv() {
		let dir = tempfile::tempdir().expect("a temporary directory");
		let path = format!("surrealkv://{}", dir.path().to_string_lossy());
		let ds = Datastore::builder()
			.with_capabilities(crate::dbs::Capabilities::all())
			.build_with_path(&path)
			.await
			.expect("a surrealkv datastore");
		ds.execute(
			"DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test",
			&TestDb::owner(),
			None,
		)
		.await
		.expect("defines the database");
		let db = TestDb::over(ds);
		db.run(FIELD_SETUP).await;
		catalog_change_between_requests_on(&db).await;
	}

	/// A principal outside its tenant is refused before any table's clause is consulted, with
	/// the error the permission layer gives, and nothing is written.
	#[tokio::test]
	async fn foreign_tenant_refused() {
		let db = TestDb::new_with_auth(
			"DEFINE ACCESS user ON DATABASE TYPE RECORD;
			 DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
			 DEFINE TABLE shut SCHEMALESS PERMISSIONS NONE;
			 CREATE shut:1 SET v = 1;
			 CREATE user:alice;",
		)
		.await;
		let rid =
			crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "alice"));
		let mut outsider = crate::dbs::Session::for_record("test", "elsewhere", "user", rid);
		outsider.db = Some("test".to_owned());
		for sql in
			["UPDATE shut:1 SET v = 2", "UPDATE type::table('shut') SET v = 2", "DELETE shut"]
		{
			let results = db.datastore().execute(sql, &outsider, None).await.expect("runs");
			match &results[0].result {
				Err(e) => assert!(e.is_not_allowed(), "`{sql}`: expected NotAllowed, got {e}"),
				Ok(v) => panic!("`{sql}` answered {v:?} to a principal outside its tenant"),
			}
		}
		assert_eq!(last_answer(&db, "SELECT VALUE v FROM shut").await, val("[1]"));
	}

	/// A bind parameter shadows a stored parameter of the same name, request by request.
	#[tokio::test]
	async fn define_param_shadowed_by_bind() {
		let db = TestDb::new(
			"DEFINE TABLE t; CREATE t:1, t:2; DEFINE PARAM $p VALUE t:1 PERMISSIONS FULL;",
		)
		.await;
		let sql = "SELECT VALUE id FROM $p";
		let none = answers(&db, &TestDb::owner(), None, sql).await;
		assert_eq!(none.last(), Some(&val("[t:1]")));
		let mut vars = PublicVariables::default();
		vars.insert("p", crate::syn::value("t:2").expect("parses"));
		let bound = answers(&db, &TestDb::owner(), Some(vars), sql).await;
		assert_eq!(bound.last(), Some(&val("[t:2]")));
		let again = answers(&db, &TestDb::owner(), None, sql).await;
		assert_eq!(again.last(), Some(&val("[t:1]")));
	}

	/// A `LET` the block that bound it did not keep is read as its earlier value, or as no value
	/// where there was none, by every later statement of the request, and so by the same
	/// request run again.
	#[tokio::test]
	async fn let_reverted_by_cancel_not_folded() {
		let db = TestDb::new("DEFINE TABLE a; DEFINE TABLE b; CREATE a:1; CREATE b:1;").await;

		let rebound = "LET $t = 'b'; BEGIN; LET $t = 'a'; THROW 'x'; COMMIT;
			SELECT VALUE id FROM type::table($t);";
		for _ in 0..2 {
			let got = owner_answers(&db, rebound).await;
			assert_eq!(
				got.last(),
				Some(&val("[b:1]")),
				"the block's `$t` was read after it reverted"
			);
		}

		let unbound = "BEGIN; LET $t = 'a'; THROW 'x'; COMMIT;
			SELECT VALUE id FROM type::table($t);";
		let never_bound = last_answer(&db, "SELECT VALUE id FROM type::table($t);").await;
		assert_ne!(never_bound, val("[a:1]"));
		for _ in 0..2 {
			assert_eq!(last_answer(&db, unbound).await, never_bound);
		}
	}

	/// A table first written inside a transaction is read by a later statement of that
	/// transaction.
	#[tokio::test]
	async fn write_then_read_new_table_in_txn() {
		let db = TestDb::new("").await;
		let got = owner_answers(&db, "BEGIN; CREATE nt:1; SELECT * FROM nt; COMMIT;").await;
		assert_eq!(got.len(), 4);
		assert_eq!(got[2], val("[{ id: nt:1 }]"));
		assert_eq!(last_answer(&db, "SELECT * FROM nt").await, val("[{ id: nt:1 }]"));
	}

	// -----------------------------------------------------------------------------------------
	// The record through the IR seam
	// -----------------------------------------------------------------------------------------

	/// `sql` planned by `plan_request` as `session` with `vars` bound, in a read transaction
	/// opened after `setup` has been run and committed.
	async fn planned_as(
		session: &crate::dbs::Session,
		setup: &str,
		sql: &str,
		vars: &[(&str, Value)],
	) -> Result<crate::exec::ir::PlannedRequest, IrError> {
		use crate::dbs::{Capabilities, Options};
		use crate::exec::config::ExecConfig;
		use crate::exec::ir::core::ir::IrScope;
		use crate::exec::ir::{PlanningInput, RequestInput, RequestStatement, plan_request};
		use crate::exec::planner::Planner;
		use crate::exec::vm::PlanSession;

		let db = TestDb::new(setup).await;
		let txn = open(&db, TransactionType::Read).await;
		let ds = db.datastore();
		let mut ctx = ds.setup_ctx().expect("a context from the datastore");
		ctx.attach_session(session).expect("the session attaches");
		for (name, value) in vars {
			ctx.add_value(*name, Arc::new(value.clone()));
		}
		ctx.set_transaction(Arc::clone(&txn));
		let ctx = ctx.freeze();
		let registry = ctx.function_registry();
		let planner =
			Planner::with_txn(&ctx, registry, txn, session.ns.clone(), session.db.clone())
				.with_auth(Arc::clone(&session.au));
		let mut opt = Options::new(&ExecConfig::default());
		opt.set_ns(session.ns.as_deref().map(Arc::from));
		opt.set_db(session.db.as_deref().map(Arc::from));
		let opt = opt.with_auth(Arc::clone(&session.au));
		let plan_session =
			PlanSession::new(opt, ctx.shared_value("session").cloned().unwrap_or_default());
		let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
		let statements = RequestStatement::of(&parsed);
		let input = RequestInput::request(
			&statements,
			IrScope::Request {
				fragment: None,
			},
		);
		let planning = PlanningInput {
			planner: &planner,
			capabilities: Arc::new(Capabilities::all()),
			backend: BackendCaps::default(),
			held: Vec::new(),
			session: &plan_session,
		};
		plan_request(&input, &planning).await
	}

	/// `sql` planned as the owner over a database `setup` builds, with `vars` bound.
	async fn planned_on(
		setup: &str,
		sql: &str,
		vars: &[(&str, Value)],
	) -> crate::exec::ir::PlannedRequest {
		planned_as(&TestDb::owner(), setup, sql, vars).await.expect("the request plans")
	}

	/// The names `planned` reads when it runs: each `expr.free_param`, `expr.session_param` and
	/// `expr.catalog_param` of its container, in pre-order.
	fn run_time_reads(planned: &crate::exec::ir::PlannedRequest) -> Vec<String> {
		use crate::exec::ir::expr::ops::ExprOp;
		use crate::exec::ir::op_kind::OpKind;

		let ir = &planned.ir;
		let mut names = Vec::new();
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(OpKind::Expr(
				ExprOp::FreeParam {
					name,
				}
				| ExprOp::SessionParam {
					name,
				}
				| ExprOp::CatalogParam {
					name,
				},
			)) = ir.op(op).map(|data| &data.kind)
			{
				names.push(name.to_string());
			}
			Walk::Continue
		});
		names
	}

	/// Whether `planned` consumed a value at plan time: a statement kept a record, or the
	/// conversion's record names a value or is incomplete.
	fn consumed_a_plan_time_value(planned: &crate::exec::ir::PlannedRequest) -> bool {
		let (physical, complete) = &planned.outcome.physical_deps;
		!planned.outcome.deps.is_empty() || !physical.deps().is_empty() || !complete
	}

	const INDEXED: &str = "DEFINE TABLE t SCHEMALESS; DEFINE INDEX ia ON t FIELDS a;
		DEFINE FIELD at ON t TYPE datetime; DEFINE INDEX iat ON t FIELDS at;";

	/// One case per planning decision that may consume a value. A request plans every name it
	/// reads, a client binding, a stored parameter and a name bound nowhere alike, as a read
	/// when the statement runs: no decision consumes the value at plan time, so the plan keeps
	/// no record of one.
	#[tokio::test]
	async fn value_deps_per_row() {
		let stored = format!("{INDEXED} DEFINE PARAM $s VALUE 1 PERMISSIONS NONE;");
		for (row, setup, sql, vars, name) in [
			// A point lookup from a record id.
			(1, INDEXED, "SELECT * FROM t WHERE id = $p", vec![("p", val("t:1"))], "p"),
			// An index equality.
			(2, INDEXED, "SELECT * FROM t WHERE a = $p", vec![("p", val("1"))], "p"),
			// A `type::field` path.
			(3, INDEXED, "SELECT * FROM t WHERE type::field($f) = 1", vec![("f", val("'a'"))], "f"),
			// A source read from a record id.
			(4, INDEXED, "SELECT * FROM $p", vec![("p", val("t:1"))], "p"),
			// A version stamp.
			(
				5,
				INDEXED,
				"SELECT * FROM t VERSION $p",
				vec![("p", val("d'2024-01-01T00:00:00Z'"))],
				"p",
			),
			// A name bound nowhere.
			(6, INDEXED, "SELECT * FROM t WHERE a = $z", vec![], "z"),
			// A stored parameter.
			(7, stored.as_str(), "SELECT * FROM t WHERE a = $s", vec![], "s"),
			// A condition that combines a name with a literal.
			(8, INDEXED, "SELECT * FROM t WHERE a = $p + 1", vec![("p", val("1"))], "p"),
		] {
			let planned = planned_on(setup, sql, &vars).await;
			assert!(!consumed_a_plan_time_value(&planned), "row {row}");
			assert!(run_time_reads(&planned).contains(&name.to_owned()), "row {row}: {sql}");
		}
	}

	/// A record id compared with `id` through a bound variable is read when the statement runs,
	/// after the plan-time fold settles the comparison: the plan consumes no value of `$id`.
	#[tokio::test]
	async fn a_record_id_variable_compared_with_id_is_read_when_the_statement_runs() {
		let planned = planned_on(
			"DEFINE TABLE pk SCHEMALESS;",
			"SELECT * FROM pk WHERE id = $id",
			&[("id", val("pk:1"))],
		)
		.await;
		assert!(!consumed_a_plan_time_value(&planned));
		assert!(run_time_reads(&planned).contains(&"id".to_owned()));
	}

	/// A literal of the statement text is planned from where it stands, whatever the whitespace,
	/// the comments or the statement's place in the request, and the plan keeps no record of it.
	#[tokio::test]
	async fn literal_dep_recorded() {
		let mut texts = BTreeSet::new();
		for sql in [
			"SELECT * FROM t WHERE a = 1",
			"SELECT  *  FROM t\n WHERE   a = 1",
			"SELECT * FROM t /* a comment */ WHERE a = 1 -- trailing\n",
		] {
			let planned = planned_on(INDEXED, sql, &[]).await;
			assert!(!consumed_a_plan_time_value(&planned), "{sql}");
			texts.insert(crate::exec::ir::core::print::print(
				&planned.ir,
				planned.ir.top(),
				Default::default(),
			));
		}
		assert_eq!(texts.len(), 1, "every spelling plans to one container");
		let planned = planned_on(INDEXED, "RETURN 5; SELECT * FROM t WHERE a = 1", &[]).await;
		assert!(!consumed_a_plan_time_value(&planned));
		let planned = planned_on("", "RETURN 1 + 1", &[]).await;
		assert!(!consumed_a_plan_time_value(&planned));
	}

	/// The e-graph reorders and replaces ops. What the plan records of the values it consumed
	/// does not depend on whether the e-graph runs.
	#[cfg(feature = "egraph")]
	#[tokio::test]
	async fn value_deps_stable_across_rewrites() {
		struct Mode(EgraphMode);
		impl Mode {
			fn set(mode: EgraphMode) -> Self {
				Self(set_mode(mode))
			}
		}
		impl Drop for Mode {
			fn drop(&mut self) {
				set_mode(self.0);
			}
		}

		let sql = "SELECT * FROM t WHERE a = $p AND a > 0";
		let vars = [("p", val("1"))];
		let records = |planned: crate::exec::ir::PlannedRequest| {
			(planned.outcome.deps, planned.outcome.physical_deps)
		};
		let off = {
			let _mode = Mode::set(EgraphMode::ForceOff);
			records(planned_on(INDEXED, sql, &vars).await)
		};
		let seeded = {
			let _mode = Mode::set(EgraphMode::ForceOnSeeded);
			records(planned_on(INDEXED, sql, &vars).await)
		};
		assert_eq!(off, seeded);
	}

	/// A record user's own statement, indexed by `$auth.id`, reads the principal when it runs:
	/// the principal is never folded into a plan, so the plan names no principal value.
	#[tokio::test]
	async fn auth_id_index_parity() {
		let rid =
			crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "alice"));
		let session = crate::dbs::Session::for_record("test", "test", "user", rid);
		let setup = "DEFINE ACCESS user ON DATABASE TYPE RECORD;
			DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
			DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			DEFINE INDEX io ON t FIELDS owner;";
		let planned = planned_as(&session, setup, "SELECT * FROM t WHERE owner = $auth.id", &[])
			.await
			.expect("the request plans");
		assert!(run_time_reads(&planned).contains(&"auth".to_owned()));
		assert!(!consumed_a_plan_time_value(&planned));
		assert!(!reads_principal(&planned.outcome.physical_deps.0));
	}

	/// A unit kept for a statement whose index decision reads `$p` is guarded by no name: the
	/// plan reads `$p` when it runs, so no value of it is in the plan.
	#[tokio::test]
	async fn served_unit_guarded_by_context_deps() {
		let sql = "SELECT * FROM t WHERE a = $p";
		let planned = planned_on(INDEXED, sql, &[("p", val("1"))]).await;
		assert!(run_time_reads(&planned).contains(&"p".to_owned()));
		let guarded = context_names(&planned.outcome.physical_deps.0, &table_of(sql))
			.expect("the record is complete");
		assert!(guarded.is_empty(), "{guarded:?}");
	}

	/// The statement clock is read when the statement runs, never at plan time: the planned
	/// container bakes no instant in, and the only literal of the text is the duration.
	#[tokio::test]
	async fn now_is_a_statement_constant() {
		let sql = "SELECT * FROM t WHERE at > time::now() - 1d";
		assert_eq!(table_of(sql).literal_count(), 1);
		let planned =
			planned_as(&TestDb::owner(), INDEXED, sql, &[]).await.expect("the request plans");
		let ir = &planned.ir;
		let mut baked = Vec::new();
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(data) = ir.op(op) {
				let kind = format!("{:?}", data.kind);
				if kind.contains("Datetime(") {
					baked.push(kind);
				}
			}
			Walk::Continue
		});
		assert!(baked.is_empty(), "no instant is baked into the plan: {baked:?}");
	}
}
