//! Tests of the planning entry, over an in-memory datastore.
//!
//! Each test plans through [`plan`], which holds a read transaction of a database a setup script
//! builds, as the executor holds one when it plans a statement of a request.

use std::sync::Arc;

use surrealdb_expr::parser::TopLevelExpr;

use super::core::ir::{IrScope, Walk, WalkOrder};
use super::core::print::{PrintOpts, print};
use super::error::IrError;
use super::expr::ops::ExprOp;
use super::iface::catalog::BackendCaps;
use super::iface::conversion::Phase;
use super::op_kind::OpKind;
use super::{
	PlannedRequest, PlanningInput, RequestInput, RequestStatement, UnitKind, plan_request,
};
use crate::dbs::Options;
use crate::dbs::capabilities::Capabilities;
use crate::exec::config::ExecConfig;
use crate::exec::operators::test_util::TestDb;
use crate::exec::planner::Planner;
use crate::exec::vm::payload::Refusal;
use crate::exec::vm::{NotCovered, NotCoveredKind, PlanSession};
use crate::expr::logical::lower;
use crate::kvs::TransactionType;
use crate::val::Value;

fn request_scope() -> IrScope {
	IrScope::Request {
		fragment: None,
	}
}

/// The session of a test planner, which reads the `test` namespace and database.
pub(crate) fn test_session() -> PlanSession {
	let mut opt = Options::new(&ExecConfig::default());
	opt.set_ns(Some("test".into()));
	opt.set_db(Some("test".into()));
	PlanSession::new(opt, Value::None)
}

/// Plans `input` over a database `setup` builds, as the owner, in a read transaction.
pub(crate) async fn plan(setup: &str, input: &RequestInput<'_>) -> Result<PlannedRequest, IrError> {
	plan_as(setup, &TestDb::owner(), input).await
}

/// Plans `input` over a database `setup` builds, as the principal of `principal`, in a read
/// transaction.
pub(crate) async fn plan_as(
	setup: &str,
	principal: &crate::dbs::Session,
	input: &RequestInput<'_>,
) -> Result<PlannedRequest, IrError> {
	plan_on_as(&TestDb::new(setup).await, principal, input).await
}

/// Plans `input` over the database `db` holds, as the owner, in a read transaction.
pub(crate) async fn plan_on(
	db: &TestDb,
	input: &RequestInput<'_>,
) -> Result<PlannedRequest, IrError> {
	plan_on_as(db, &TestDb::owner(), input).await
}

/// Plans `input` over the database `db` holds, as the principal of `principal`, in a read
/// transaction.
pub(crate) async fn plan_on_as(
	db: &TestDb,
	principal: &crate::dbs::Session,
	input: &RequestInput<'_>,
) -> Result<PlannedRequest, IrError> {
	let ctx = db.exec_ctx_as(principal, TransactionType::Read).await;
	let txn = ctx.try_txn().expect("a transaction");
	let planner = Planner::with_txn(
		ctx.ctx(),
		ctx.function_registry(),
		txn,
		Some("test".to_owned()),
		Some("test".to_owned()),
	)
	.with_auth(Arc::clone(&ctx.root().auth));
	let session = test_session();
	let planning = PlanningInput {
		planner: &planner,
		capabilities: Arc::new(Capabilities::all()),
		backend: BackendCaps::default(),
		held: Vec::new(),
		session: &session,
	};
	plan_request(input, &planning).await
}

/// The text of the container `sql` plans to, planned as the statements of a request.
async fn planned_text(setup: &str, sql: &str) -> Result<String, IrError> {
	let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
	let statements = RequestStatement::of(&parsed);
	let input = RequestInput::request(&statements, request_scope());
	let planned = plan(setup, &input).await?;
	Ok(print(&planned.ir, planned.ir.top(), PrintOpts::default()))
}

const SCHEMA: &str = "
	DEFINE TABLE person SCHEMALESS;
	DEFINE FIELD name ON person TYPE string;
	DEFINE INDEX person_name ON person FIELDS name;
	DEFINE TABLE secret SCHEMALESS PERMISSIONS FOR select WHERE name = 'a';
	DEFINE TABLE c SCHEMALESS;
	DEFINE FIELD d ON c COMPUTED a + 1;
	DEFINE FUNCTION fn::double($x: int) { RETURN $x * 2 } PERMISSIONS FULL;
";

/// A statement handed over bound plans, past the physical boundary, over the tables it reads.
#[tokio::test]
async fn plan_request_plans_a_logical_statement() {
	let parsed = crate::syn::parse("SELECT * FROM person WHERE name = 'a'")
		.expect("the statement parses")
		.expressions;
	let [TopLevelExpr::Expr(expr)] = parsed.as_slice() else {
		panic!("one expression statement");
	};
	let op = lower(expr.clone()).expect("the statement has a logical form");
	let statements = [RequestStatement::Logical(&op)];
	let input = RequestInput::request(&statements, request_scope());
	let planned: PlannedRequest = plan(SCHEMA, &input).await.expect("the statement plans");
	assert_eq!(planned.ir.phase(), Phase::Physical);
	assert_eq!(planned.outcome.egraph.len(), 1, "one statement went to the e-graph planner");
	assert!(
		planned.outcome.answers.iter().any(|answers| answers.table.as_str() == "person"),
		"the table the statement reads was planned from its answers"
	);
}

/// A bound statement and the statement the parser wrote plan to the same container.
#[tokio::test]
async fn a_bound_statement_plans_as_the_parsed_one_does() {
	// The cases that consult the catalog while binding: a stored function, a table with a select
	// permission, a computed field and a definition with stored code.
	for sql in [
		"SELECT * FROM person",
		"SELECT name FROM person WHERE name = 'a' ORDER BY name LIMIT 2",
		"RETURN 1 + 2",
		"RETURN fn::double(2)",
		"SELECT * FROM secret",
		"SELECT d FROM c",
		"DEFINE FIELD e ON person VALUE name + '!'",
	] {
		let parsed = crate::syn::parse(sql).expect("the statement parses").expressions;
		let [TopLevelExpr::Expr(expr)] = parsed.as_slice() else {
			panic!("one expression statement");
		};
		let op = lower(expr.clone()).expect("the statement has a logical form");
		let bound = [RequestStatement::Logical(&op)];
		let from_bound = plan(SCHEMA, &RequestInput::request(&bound, request_scope()))
			.await
			.expect("the bound statement plans");
		let from_parsed = planned_text(SCHEMA, sql).await.expect("the parsed statement plans");
		assert_eq!(
			print(&from_bound.ir, from_bound.ir.top(), PrintOpts::default()),
			from_parsed,
			"{sql}"
		);
	}
}

/// The legality the logical plan states of its pipelines is read for the loops the binder made,
/// whether the statement was handed over bound or parsed.
#[tokio::test]
async fn the_legality_of_the_logical_pipelines_is_stated_for_the_loops_of_the_container() {
	use super::phys::ops::PhysOp;

	for sql in [
		"SELECT * FROM person WHERE name = 'a'",
		"SELECT name FROM person WHERE name = 'a' ORDER BY name LIMIT 2",
	] {
		let parsed = crate::syn::parse(sql).expect("the statement parses").expressions;
		let statements = RequestStatement::of(&parsed);
		let planned = plan(SCHEMA, &RequestInput::request(&statements, request_scope()))
			.await
			.expect("the statement plans");
		let stated = planned.facts.logical_legalities().expect("the legalities are stated");
		assert_eq!(stated.statements.len(), 1, "{sql}");
		assert!(!stated.pipelines.is_empty(), "{sql}");
		let mut listed = 0;
		let mut pipelines = 0;
		planned.ir.walk(planned.ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(OpKind::Phys(PhysOp::Pipeline(attrs))) =
				planned.ir.op(op).map(|data| &data.kind)
			{
				pipelines += 1;
				listed += usize::from(
					attrs.logical.is_some_and(|logical| stated.pipelines.contains_key(&logical)),
				);
			}
			Walk::Continue
		});
		assert!(pipelines > 0 && listed == pipelines, "{sql}: {listed} of {pipelines} listed");
	}
}

/// The placement of a pipeline reads the strategy the physical rules gave a statement in value
/// position, through the select loop that holds the statement: a statement that runs once, over a
/// probe or through a lookup that keeps one is evaluated once per run, and a statement applied for
/// every row is read for every row.
#[tokio::test]
async fn the_placement_reads_the_strategy_of_a_nested_statement() {
	use super::core::walk::ancestors;
	use super::expr::ops::NestedStrategy;
	use super::phys::facts::{NestedReading, facts_reading};
	use super::phys::ops::PhysOp;
	use crate::exec::planner::placement::NestedUse;

	let cases = [
		// A constant statement runs once.
		(
			"SELECT *, (SELECT * FROM person WHERE name = 'a') AS same FROM person",
			"once",
			NestedUse::OnceOrProbed,
		),
		// A join key with no index over it is hashed.
		(
			"SELECT *, (SELECT * FROM person WHERE age = $parent.age) AS same FROM person",
			"hash",
			NestedUse::OnceOrProbed,
		),
		// A join key over an indexed field is a lookup that keeps the probe plan.
		(
			"SELECT *, (SELECT * FROM person WHERE name = $parent.name) AS same FROM person",
			"lookup, then hash",
			NestedUse::OnceOrProbed,
		),
		// A correlated statement with no join key is applied for every row.
		(
			"SELECT *, (SELECT * FROM person WHERE age > $parent.age) AS later FROM person",
			"apply",
			NestedUse::PerRowReadOnly,
		),
		// A constant statement whose rows are only tested for membership keeps them as an index.
		(
			"SELECT *, age INSIDE (SELECT VALUE age FROM person WHERE name = 'a') AS member FROM person",
			"once",
			NestedUse::Other,
		),
	];
	for (sql, arm, expected) in cases {
		let parsed = crate::syn::parse(sql).expect("the statement parses").expressions;
		let statements = RequestStatement::of(&parsed);
		let mut planned = plan(SCHEMA, &RequestInput::request(&statements, request_scope()))
			.await
			.expect("the statement plans");
		let mut held = None;
		planned.ir.walk(planned.ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(OpKind::Expr(ExprOp::Subquery(strategy))) =
				planned.ir.op(op).map(|data| &data.kind)
			{
				held = Some((op, strategy.clone()));
			}
			Walk::Continue
		});
		let (subquery, strategy) = held.expect("the statement holds a statement in value position");
		let named = match &strategy {
			NestedStrategy::Apply => "apply",
			NestedStrategy::Once => "once",
			NestedStrategy::Hash {
				..
			} => "hash",
			NestedStrategy::Lookup {
				hashed: true,
				..
			} => "lookup, then hash",
			NestedStrategy::Lookup {
				hashed: false,
				..
			} => "lookup",
		};
		assert_eq!(named, arm, "{sql}");
		let pipeline = ancestors(&planned.ir, subquery)
			.find_map(|(_, owner)| {
				owner.filter(|owner| {
					matches!(
						planned.ir.op(*owner).map(|data| &data.kind),
						Some(OpKind::Phys(PhysOp::Pipeline(_)))
					)
				})
			})
			.expect("the statement stands in a pipeline");
		let physical =
			facts_reading(&planned.ir, &mut planned.facts, pipeline, NestedReading::Strategy)
				.expect("the pipeline has facts");
		assert_eq!(physical.nested, expected, "{sql}");
	}
}

/// A construct a region does not admit is the request's error, carrying the rejection's text; a
/// unit the request did not write is not judged.
#[tokio::test]
async fn a_rejection_is_the_error_of_the_request() {
	let sql = "SELECT VALUE id FROM person WHERE (CREATE w:1 RETURN NONE) = []";
	let error = planned_text(SCHEMA, sql).await.expect_err("the statement is rejected");
	let IrError::Rejected(refusal) = &error else {
		panic!("a rejection, not {error:?}");
	};
	let Refusal::InvalidStatement {
		message,
	} = &**refusal
	else {
		panic!("an invalid statement, not {refusal:?}");
	};
	assert!(message.contains("is not allowed in"), "{message}");
	assert!(message.ends_with("(statement 1)"), "{message}");
	let mapped = NotCovered::from(error);
	assert_eq!(mapped.kind, NotCoveredKind::Refused);
	assert!(mapped.refusal.is_some());
}

/// The error a statement that stands alone names is its position in its request.
#[tokio::test]
async fn a_statement_planned_alone_names_its_position() {
	let parsed =
		crate::syn::parse("SELECT VALUE id FROM person WHERE (CREATE w:1 RETURN NONE) = []")
			.expect("the statement parses")
			.expressions;
	let statements = RequestStatement::of(&parsed);
	let input = RequestInput {
		statements: &statements,
		scope: request_scope(),
		unit: UnitKind::RequestStatement {
			position: 4,
		},
		serves_late: false,
		late_bound: false,
	};
	match plan(SCHEMA, &input).await {
		Err(IrError::Rejected(refusal)) => {
			let Refusal::InvalidStatement {
				message,
			} = &*refusal
			else {
				panic!("an invalid statement, not {refusal:?}");
			};
			assert!(message.ends_with("(statement 5)"), "{message}");
		}
		other => panic!("a rejection, not {:?}", other.map(|planned| planned.ir.phase())),
	}
}

/// A guard, the predicate a called function's `PERMISSIONS` states, plans past the physical
/// boundary as a value over the row it is handed, under a permission frame.
#[tokio::test]
async fn a_guard_plans_as_a_value_over_its_row() {
	let parsed = crate::syn::parse("RETURN $this.name = 'a' AND $value.name != 'b'")
		.expect("the statement parses")
		.expressions;
	let statements = RequestStatement::of(&parsed);
	let input = RequestInput {
		statements: &statements,
		scope: request_scope(),
		unit: UnitKind::Guard,
		serves_late: false,
		late_bound: false,
	};
	let planned = plan(SCHEMA, &input).await.expect("the guard plans");
	assert_eq!(planned.ir.phase(), Phase::Physical);
	let top = planned.ir.region(planned.ir.top()).expect("the top region exists");
	assert_eq!(top.kind, crate::exec::ir::iface::region_kind::RegionKind::PermissionFrame);
}

/// The outcome an error of planning is reported as: a rejection is the refusal it carries, an
/// unknown catalog answer plans the statement again when it is reached, a construct whose planning
/// is not built is unsupported, and any other error is an internal outcome.
#[test]
fn an_error_of_planning_maps_to_the_outcome_a_caller_reports() {
	let rejected = NotCovered::from(IrError::Rejected(Box::new(Refusal::NamespaceEmpty)));
	assert_eq!(rejected.kind, NotCoveredKind::Refused);
	assert!(rejected.refusal.is_some());
	let late = NotCovered::from(IrError::LateBind {
		what: "a table the memo does not hold",
	});
	assert_eq!(late.kind, NotCoveredKind::LateBind);
	let not_yet = NotCovered::from(IrError::NotYet {
		what: "an entry that has not landed",
	});
	assert_eq!(not_yet.kind, NotCoveredKind::Unsupported);
	assert!(not_yet.refusal.is_none());
	let broken = NotCovered::from(IrError::Invariant {
		what: "a broken invariant",
	});
	assert_eq!(broken.kind, NotCoveredKind::Internal);
	assert!(broken.refusal.is_none());
}

/// Schema for the shapes below: an indexed table, a keyed table, an array-column index and a
/// foreign set.
const SHAPES_SCHEMA: &str = "
	DEFINE TABLE t SCHEMALESS;
	DEFINE FIELD a ON t TYPE any;
	DEFINE INDEX t_a ON t FIELDS a;
	DEFINE TABLE k SCHEMALESS;
	DEFINE FIELD id ON k TYPE [a: int, b: int];
	DEFINE TABLE u SCHEMALESS;
	DEFINE FIELD tags ON u TYPE array<string>;
	DEFINE INDEX u_tags ON u FIELDS tags;
	DEFINE TABLE holder SCHEMALESS;
	DEFINE FIELD items ON holder TYPE set<string>;
	DEFINE TABLE l SCHEMALESS;
	DEFINE FIELD link ON l TYPE record<holder>;
	DEFINE INDEX l_items ON l FIELDS link.items;
";

/// The statements whose plans the planner's mapping and optimiser fix. The planning entry plans
/// each to the shape its golden records, and each is named by the file of that golden.
const MAIN_SHAPES: [(&str, &str); 16] = [
	("scan", "SELECT * FROM t"),
	("sorted_limit", "SELECT a FROM t WHERE b = 1 ORDER BY a LIMIT 2"),
	("create", "CREATE t SET a = 1"),
	("update", "UPDATE t SET a += 1"),
	("delete", "DELETE t"),
	("block", "{ LET $x = 1; RETURN $x }"),
	("for_write", "FOR $i IN [1, 2] { CREATE t SET i = $i }"),
	("define_table", "DEFINE TABLE t"),
	("info", "INFO FOR DB"),
	("correlated", "SELECT *, (SELECT * FROM t WHERE a = 1) AS s FROM t"),
	("indexed", "SELECT * FROM t WHERE a = 1"),
	("key_range", "SELECT * FROM k WHERE id.a = 1"),
	("key_range_sorted", "SELECT * FROM k WHERE id.a = 1 AND id.b > $p ORDER BY id DESC LIMIT 3"),
	("key_range_in", "SELECT * FROM k WHERE id.a IN [1, 2, 3]"),
	("array_column", "SELECT * FROM u WHERE tags CONTAINS 'x'"),
	("foreign_set", "SELECT * FROM l WHERE link.items CONTAINS 'x'"),
];

/// The directory of the goldens, relative to the crate.
const GOLDENS: &str = "src/exec/ir/goldens/plan_request";

/// Every statement of [`MAIN_SHAPES`] plans, verifies at the physical boundary, and prints as its
/// golden does.
///
/// A golden is accepted only when each op in it matches the plan expected for the same statement;
/// the golden's header lists those ops in order. Run with `BLESS_GOLDENS=1` to write the goldens
/// for review against the planner's plans.
#[tokio::test]
#[ignore = "the goldens are written from a run and reviewed against the planner's own plans; none \
            was run"]
async fn plan_request_plans_main_shapes() {
	let bless = std::env::var_os("BLESS_GOLDENS").is_some();
	let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(GOLDENS);
	for (name, sql) in MAIN_SHAPES {
		let text = planned_text(SHAPES_SCHEMA, sql)
			.await
			.unwrap_or_else(|error| panic!("`{sql}` does not plan: {error}"));
		let path = root.join(format!("{name}.phys"));
		if bless {
			std::fs::create_dir_all(&root).expect("the goldens directory is created");
			std::fs::write(&path, &text).expect("the golden is written");
			continue;
		}
		let golden = std::fs::read_to_string(&path)
			.unwrap_or_else(|_| panic!("no golden for `{name}`: run with BLESS_GOLDENS=1"));
		assert_eq!(text, golden, "`{sql}` plans to another container than its golden");
	}
}

/// Stored code is planned as bound: the sums of a field's `VALUE` clause are not merged by the
/// loop optimiser and the physical boundary accepts the container.
#[tokio::test]
async fn stored_code_is_planned_as_bound() {
	let parsed = crate::syn::parse("DEFINE FIELD e ON person VALUE ($value + 1) + ($value + 1)")
		.expect("the statement parses")
		.expressions;
	let statements = RequestStatement::of(&parsed);
	let planned = plan(SCHEMA, &RequestInput::request(&statements, request_scope()))
		.await
		.expect("the definition plans");
	let mut sums = 0;
	planned.ir.walk(planned.ir.top(), WalkOrder::PreOrder, &mut |op| {
		if matches!(
			planned.ir.op(op).map(|data| &data.kind),
			Some(OpKind::Expr(ExprOp::Binary { .. }))
		) {
			sums += 1;
		}
		Walk::Continue
	});
	assert!(sums >= 3, "the clause keeps its three sums, found {sums}");
}

/// Tables in `held` are on the planner's cycle guard while the request plans and are released after
/// it. A table already on the guard before planning stays there and is neither entered twice nor
/// released.
#[tokio::test]
async fn a_held_table_is_released_when_the_request_has_planned() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
	let txn = ctx.try_txn().expect("a transaction");
	let planner = Planner::with_txn(
		ctx.ctx(),
		ctx.function_registry(),
		txn,
		Some("test".to_owned()),
		Some("test".to_owned()),
	)
	.with_auth(Arc::clone(&ctx.root().auth));
	let (ns, db_id) = planner.ns_db_ids().await.expect("the database exists");
	let person = surrealdb_strand::TableName::new("person");
	let secret = surrealdb_strand::TableName::new("secret");
	let outer =
		planner.cycle_guard.try_enter(ns, db_id, secret.clone()).expect("the table is free");
	let parsed =
		crate::syn::parse("SELECT * FROM person").expect("the statement parses").expressions;
	let statements = RequestStatement::of(&parsed);
	let input = RequestInput::request(&statements, request_scope());
	let session = test_session();
	let planning = PlanningInput {
		planner: &planner,
		capabilities: Arc::new(Capabilities::all()),
		backend: BackendCaps::default(),
		held: vec![person.clone(), secret.clone()],
		session: &session,
	};
	plan_request(&input, &planning).await.expect("the statement plans");
	assert!(!planner.cycle_guard.contains(ns, db_id, &person), "the table is released");
	assert!(planner.cycle_guard.contains(ns, db_id, &secret), "a table held before stays held");
	drop(outer);
	assert!(planner.cycle_guard.is_idle());
}

/// A session that selects a database other than the planner's is an invariant of the caller, who
/// builds the planner under the session.
#[tokio::test]
async fn a_session_naming_another_database_is_an_invariant() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
	let txn = ctx.try_txn().expect("a transaction");
	let planner = Planner::with_txn(
		ctx.ctx(),
		ctx.function_registry(),
		txn,
		Some("test".to_owned()),
		Some("test".to_owned()),
	)
	.with_auth(Arc::clone(&ctx.root().auth));
	let parsed =
		crate::syn::parse("SELECT * FROM person").expect("the statement parses").expressions;
	let statements = RequestStatement::of(&parsed);
	let input = RequestInput::request(&statements, request_scope());
	for (ns, db_name) in [("other", "test"), ("test", "other")] {
		let mut session = test_session();
		session.use_names(Some(ns), Some(db_name));
		let planning = PlanningInput {
			planner: &planner,
			capabilities: Arc::new(Capabilities::all()),
			backend: BackendCaps::default(),
			held: Vec::new(),
			session: &session,
		};
		let error = plan_request(&input, &planning).await.err().expect("the call is refused");
		assert!(matches!(error, IrError::Invariant { .. }), "{ns}/{db_name}: {error:?}");
	}
}

/// The session's `LET`s do not change the plan: a free name plans as read at run time whether the
/// session bound it or not, so the container is the one a session with no `LET` plans.
#[tokio::test]
async fn a_session_let_plans_as_a_name_read_at_run_time() {
	let sql = "SELECT * FROM person WHERE name = $x";
	let parsed = crate::syn::parse(sql).expect("the statement parses").expressions;
	let statements = RequestStatement::of(&parsed);
	let input = RequestInput::request(&statements, request_scope());
	let mut texts = Vec::new();
	for bound in [false, true] {
		let db = TestDb::new(SCHEMA).await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let txn = ctx.try_txn().expect("a transaction");
		let planner = Planner::with_txn(
			ctx.ctx(),
			ctx.function_registry(),
			txn,
			Some("test".to_owned()),
			Some("test".to_owned()),
		)
		.with_auth(Arc::clone(&ctx.root().auth));
		let mut session = test_session();
		if bound {
			session.bind_let("x", Some(Value::from("a")));
		}
		let planning = PlanningInput {
			planner: &planner,
			capabilities: Arc::new(Capabilities::all()),
			backend: BackendCaps::default(),
			held: Vec::new(),
			session: &session,
		};
		let planned = plan_request(&input, &planning).await.expect("the statement plans");
		texts.push(print(&planned.ir, planned.ir.top(), PrintOpts::default()));
	}
	assert_eq!(texts[0], texts[1]);
}

/// The attributes of the one write of `planned`, and the kind of each region its op hosts.
fn written(
	planned: &PlannedRequest,
) -> (crate::exec::ir::logical::ops::WriteAttrs, Vec<crate::exec::ir::iface::RegionKind>) {
	use crate::exec::ir::phys::ops::PhysOp;

	let mut found = None;
	planned.ir.walk(planned.ir.top(), WalkOrder::PreOrder, &mut |op| {
		let data = planned.ir.op(op).expect("the op exists");
		if let OpKind::Phys(PhysOp::Write(attrs)) = &data.kind {
			let kinds = data
				.regions
				.iter()
				.map(|region| planned.ir.region(*region).expect("the region exists").kind)
				.collect();
			assert!(found.replace((attrs.write.clone(), kinds)).is_none(), "one write");
		}
		Walk::Continue
	});
	found.expect("the container holds a write")
}

/// Tables with a `COMPUTED` field, and one with no field.
const VIEWED: &str = "
	DEFINE TABLE doc SCHEMALESS;
	DEFINE FIELD a ON doc TYPE int;
	DEFINE FIELD c ON doc COMPUTED a + 1;
	DEFINE TABLE plain SCHEMALESS;
";

/// A write to a table whose field view derives a value states the layers the planner binds over
/// the table, and hosts their regions on its op; a write to a table with a plain view states none.
#[tokio::test]
async fn a_write_states_the_layers_of_the_field_view_the_planner_binds() {
	use crate::exec::ir::iface::{ClauseRole, FieldRole, RegionKind};
	use crate::exec::ir::logical::ops::{ImageLayers, ImageView};

	let parsed = crate::syn::parse("CREATE doc SET a = 1").expect("the request parses").expressions;
	let statements = RequestStatement::of(&parsed);
	let input = RequestInput::request(&statements, request_scope());
	let planned = plan(VIEWED, &input).await.expect("the write plans");
	let (attrs, kinds) = written(&planned);
	assert_eq!(attrs.image.view, ImageView::Derived);
	let ImageLayers::Stack(layers) = &attrs.stored.image_layers else {
		panic!("the layers of a derived view are stated: {:?}", attrs.stored.image_layers);
	};
	assert!(
		layers.iter().any(|layer| layer.computed == [surrealdb_strand::Strand::new("c")]),
		"a layer carries the computed field its input declares: {layers:?}"
	);
	assert_eq!(kinds, attrs.region_kinds(), "the op hosts the regions the layers state");
	assert!(kinds.contains(&RegionKind::Clause(ClauseRole::Projection)));
	assert!(kinds.contains(&RegionKind::FieldFrame(FieldRole::Computed)));

	let parsed =
		crate::syn::parse("CREATE plain SET a = 1").expect("the request parses").expressions;
	let statements = RequestStatement::of(&parsed);
	let input = RequestInput::request(&statements, request_scope());
	let planned = plan(VIEWED, &input).await.expect("the write plans");
	let (attrs, kinds) = written(&planned);
	assert_eq!(attrs.image.view, ImageView::Plain);
	assert_eq!(attrs.stored.image_layers, ImageLayers::Unstated);
	assert!(kinds.is_empty());
}
