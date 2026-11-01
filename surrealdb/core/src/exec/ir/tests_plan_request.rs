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
use crate::dbs::capabilities::Capabilities;
use crate::dbs::{Options, Session};
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
	PlanSession::new(opt, Arc::new(Value::None))
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
	plan_on_in(db, principal, input, true).await
}

/// [`plan_as`] for a planner that plans a statement of a request where `request_root`, and a unit
/// compiled on its own otherwise.
async fn plan_in(
	setup: &str,
	session: &Session,
	input: &RequestInput<'_>,
	request_root: bool,
) -> Result<PlannedRequest, IrError> {
	plan_on_in(&TestDb::new(setup).await, session, input, request_root).await
}

/// [`plan_in`] over the database `db` holds.
async fn plan_on_in(
	db: &TestDb,
	session: &Session,
	input: &RequestInput<'_>,
	request_root: bool,
) -> Result<PlannedRequest, IrError> {
	let ctx = db.exec_ctx_as(session, TransactionType::Read).await;
	let txn = ctx.try_txn().expect("a transaction");
	let planner = Planner::with_txn(
		ctx.ctx(),
		ctx.function_registry(),
		txn,
		Some("test".to_owned()),
		Some("test".to_owned()),
	)
	.with_auth(Arc::clone(&ctx.root().auth))
	.trusting_index_shapes(true);
	let session = test_session();
	// The container is planned alone, with no compile to link a write's stages, so each write
	// binds its stages as ops of its own.
	let planner = if request_root {
		planner.at_request_root()
	} else {
		planner
	}
	.inlining_stages();
	let planning = PlanningInput {
		planner: &planner,
		capabilities: Arc::new(Capabilities::all()),
		backend: BackendCaps::default(),
		held: Vec::new(),
		session: &session,
	};
	plan_request(input, &planning).await
}

/// A container bound over a database, and the catalog memo its binder read.
pub(crate) struct Bound {
	pub(crate) ir: super::core::ir::Ir,
	pub(crate) catalog: super::cache::planning::PlanningCatalog,
}

impl Bound {
	/// The effect context of the container over the catalog its binder read and `registry`.
	pub(crate) fn cx<'a>(
		&'a self,
		registry: &'a crate::exec::function::FunctionRegistry,
	) -> super::iface::catalog::EffectCx<'a> {
		super::iface::catalog::EffectCx {
			registry,
			catalog: &self.catalog,
			frames: self.ir.frames(),
		}
	}
}

/// The container the statements of `sql` bind to over a database `setup` builds, as the owner, in
/// the logical dialect and planned no further. Each statement is resolved against the catalog as
/// a unit's statement is ([`Planner::reading_session_values`], [`Planner::bind`]) and handed over
/// bound: its plan holds the permission filters and the computed and guard layers the catalog
/// declares for its reads, and reads the session's values when it runs.
pub(crate) async fn bound(setup: &str, sql: &str) -> Bound {
	bound_as(&TestDb::owner(), setup, sql).await
}

/// [`bound`] as `session`, whose namespace and database are `test`.
pub(crate) async fn bound_as(session: &crate::dbs::Session, setup: &str, sql: &str) -> Bound {
	let db = TestDb::new(setup).await;
	let ctx = db.exec_ctx_as(session, TransactionType::Read).await;
	let txn = ctx.try_txn().expect("a transaction");
	let planner = Planner::with_txn(
		ctx.ctx(),
		ctx.function_registry(),
		txn,
		Some("test".to_owned()),
		Some("test".to_owned()),
	)
	.with_auth(Arc::clone(&ctx.root().auth));
	let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
	let mut resolved = Vec::new();
	for statement in &parsed {
		let TopLevelExpr::Expr(expr) = statement else {
			panic!("each statement is an expression");
		};
		let op = lower(expr.clone()).expect("the statement has a logical form");
		let unit = planner.reading_session_values();
		resolved.push(unit.bind(op).await.expect("the statement resolves"));
	}
	let statements: Vec<RequestStatement<'_>> =
		resolved.iter().map(RequestStatement::Logical).collect();
	let input = RequestInput::request(&statements, request_scope());
	let session = test_session();
	let planning = PlanningInput {
		planner: &planner,
		capabilities: Arc::new(Capabilities::all()),
		backend: BackendCaps::default(),
		held: Vec::new(),
		session: &session,
	};
	let (ir, catalog) =
		super::bind_request_over(&input, &planning).await.expect("the request binds");
	Bound {
		ir,
		catalog,
	}
}

/// The container `sql` plans to, planned as the statements of a request, over the database
/// `setup` builds.
pub(crate) async fn planned_request(setup: &str, sql: &str) -> PlannedRequest {
	planned_request_as(setup, &TestDb::owner(), sql).await
}

/// [`planned_request`] for the principal `session` names.
pub(crate) async fn planned_request_as(
	setup: &str,
	session: &Session,
	sql: &str,
) -> PlannedRequest {
	let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
	let statements = RequestStatement::of(&parsed);
	let input = RequestInput::request(&statements, request_scope());
	plan_as(setup, session, &input).await.expect("the request plans")
}

/// [`planned_request`] for a unit compiled on its own, which runs in a caller's frame.
pub(crate) async fn planned_unit(setup: &str, sql: &str) -> PlannedRequest {
	let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
	let statements = RequestStatement::of(&parsed);
	let input = RequestInput::request(&statements, request_scope());
	plan_in(setup, &TestDb::owner(), &input, false).await.expect("the unit plans")
}

/// [`planned_request`] for `sql` planned as a container of the kind `unit`, by a planner that
/// plans a statement of a request where `request_root`.
pub(crate) async fn planned_as_unit(
	setup: &str,
	sql: &str,
	unit: super::unit::UnitKind,
	request_root: bool,
) -> PlannedRequest {
	let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
	let statements = RequestStatement::of(&parsed);
	let mut input = RequestInput::request(&statements, request_scope());
	input.unit = unit;
	plan_in(setup, &TestDb::owner(), &input, request_root).await.expect("the unit plans")
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
		let physical = facts_reading(
			&planned.ir,
			&mut planned.facts,
			crate::exec::ir::analysis::cx::builtin_registry(),
			pipeline,
			NestedReading::Strategy,
		)
		.expect("the pipeline has facts");
		assert_eq!(physical.nested, expected, "{sql}");
	}
}

/// A statement in value position that holds another statement running once runs once itself, and
/// so does one whose rows come from a nearest-neighbour or full-text search: each answers the same
/// every time it is evaluated. Correlated through a join key, a search keeps every evaluation.
#[tokio::test]
async fn a_statement_holding_a_constant_statement_or_a_search_runs_once() {
	use super::expr::ops::NestedStrategy;

	let schema = "
		DEFINE TABLE person SCHEMALESS;
		DEFINE FIELD name ON person TYPE string;
		DEFINE INDEX person_name ON person FIELDS name;
		DEFINE TABLE emb SCHEMALESS;
		DEFINE INDEX emb_v ON emb FIELDS v HNSW DIMENSION 2 DIST EUCLIDEAN;
		DEFINE TABLE company SCHEMAFULL;
		DEFINE TABLE trial SCHEMAFULL;
		DEFINE FIELD kind ON trial TYPE option<string>;
		DEFINE TABLE sponsors TYPE RELATION FROM company TO trial SCHEMAFULL;
		DEFINE INDEX sponsors_in ON sponsors FIELDS in;
	";
	let cases = [
		// The inner statement runs once, so the outer one reads a constant and runs once too.
		(
			"SELECT * FROM person WHERE id IN (SELECT VALUE id FROM person WHERE age IN (SELECT VALUE age FROM person WHERE name = 'a'))",
			vec!["once", "once"],
		),
		// The same through a relation's endpoints, counted.
		(
			"SELECT count() FROM company WHERE id IN (SELECT VALUE in FROM sponsors WHERE out IN (SELECT VALUE id FROM trial WHERE kind = 'a')) GROUP ALL",
			vec!["once", "once"],
		),
		// A nearest-neighbour search the statement runs whole answers the same each time.
		(
			"SELECT * FROM person WHERE id IN (SELECT VALUE p FROM emb WHERE v <|2,10|> [0.0, 0.0])",
			vec!["once"],
		),
		// Correlated, the search ranks only the rows the outer row admits: applied per row.
		(
			"SELECT * FROM person WHERE id IN (SELECT VALUE p FROM emb WHERE p = $parent.id AND v <|2,10|> [0.0, 0.0])",
			vec!["apply"],
		),
		// An inner statement applied per row of the outer statement keeps the outer one applied.
		(
			"SELECT * FROM person WHERE id IN (SELECT VALUE id FROM person WHERE age IN (SELECT VALUE age FROM person WHERE name = rand::string()))",
			vec!["apply", "apply"],
		),
	];
	for (sql, arms) in cases {
		let parsed = crate::syn::parse(sql).expect("the statement parses").expressions;
		let statements = RequestStatement::of(&parsed);
		let planned = plan(schema, &RequestInput::request(&statements, request_scope()))
			.await
			.expect("the statement plans");
		let mut named = Vec::new();
		planned.ir.walk(planned.ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(OpKind::Expr(ExprOp::Subquery(strategy))) =
				planned.ir.op(op).map(|data| &data.kind)
			{
				named.push(match strategy {
					NestedStrategy::Apply => "apply",
					NestedStrategy::Once => "once",
					NestedStrategy::Hash {
						..
					} => "hash",
					NestedStrategy::Lookup {
						..
					} => "lookup",
				});
			}
			Walk::Continue
		});
		assert_eq!(named, arms, "{sql}");
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

/// Tables a record-id bound reads: plain ones, one that names the components of its ids, one a
/// permission governs, one a field permission governs, one with a computed field and a
/// relation.
const ID_BOUNDS: &str = "
	DEFINE TABLE t SCHEMALESS;
	DEFINE FIELD n ON t TYPE int;
	DEFINE TABLE indexed SCHEMALESS;
	DEFINE INDEX indexed_n ON indexed FIELDS n;
	DEFINE TABLE reading SCHEMALESS;
	DEFINE FIELD id ON reading TYPE [city: string, time: int];
	DEFINE TABLE owned SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth.id;
	DEFINE TABLE guarded SCHEMALESS PERMISSIONS FOR select FULL;
	DEFINE FIELD secret ON guarded PERMISSIONS FOR select WHERE owner = $auth.id;
	DEFINE TABLE derived SCHEMALESS;
	DEFINE FIELD a ON derived TYPE int;
	DEFINE FIELD c ON derived COMPUTED a + 1;
	DEFINE TABLE edge TYPE RELATION IN t OUT t LIGHTWEIGHT;
";

/// The record-id bounds of each table scan the container `sql` plans to holds, in the order the
/// container lists them, as the scan states them, with the number of operands each takes. The
/// request is planned as the owner, or as a record user of `user:alice`.
async fn scan_bounds_as(
	record_user: bool,
	sql: &str,
) -> Vec<(String, Option<crate::exec::ir::phys::ops::IdBoundsShape>, usize)> {
	use crate::exec::ir::phys::ops::PhysOp;

	let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
	let statements = RequestStatement::of(&parsed);
	let input = RequestInput::request(&statements, request_scope());
	let db = TestDb::new(ID_BOUNDS).await;
	let session = if record_user {
		let rid =
			crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "alice"));
		crate::dbs::Session::for_record("test", "test", "user", rid)
	} else {
		TestDb::owner()
	};
	let ctx = db.exec_ctx_as(&session, TransactionType::Read).await;
	let txn = ctx.try_txn().expect("a transaction");
	let planner = Planner::with_txn(
		ctx.ctx(),
		ctx.function_registry(),
		txn,
		Some("test".to_owned()),
		Some("test".to_owned()),
	)
	.with_auth(Arc::clone(&ctx.root().auth));
	let plan_session = test_session();
	let planning = PlanningInput {
		planner: &planner,
		capabilities: Arc::new(Capabilities::all()),
		backend: BackendCaps::default(),
		held: Vec::new(),
		session: &plan_session,
	};
	let planned = plan_request(&input, &planning).await.expect("the request plans");
	let mut scans = Vec::new();
	planned.ir.walk(planned.ir.top(), WalkOrder::PreOrder, &mut |op| {
		let data = planned.ir.op(op).expect("the op exists");
		if let OpKind::Phys(PhysOp::TableScan(attrs)) = &data.kind {
			scans.push((attrs.table.to_string(), attrs.bounds.clone(), data.operands.len()));
		}
		Walk::Continue
	});
	scans
}

/// [`scan_bounds_as`] for the owner.
async fn scan_bounds(
	sql: &str,
) -> Vec<(String, Option<crate::exec::ir::phys::ops::IdBoundsShape>, usize)> {
	scan_bounds_as(false, sql).await
}

/// The kind of source the placement reads of each pipeline of the container `sql` plans to, and
/// whether a table scan of `table` states record-id bounds.
async fn pipeline_sources(
	sql: &str,
	table: &str,
) -> (Vec<crate::exec::planner::placement::SourceKind>, bool) {
	use super::phys::facts::{NestedReading, facts_reading};
	use super::phys::ops::PhysOp;

	let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
	let statements = RequestStatement::of(&parsed);
	let mut planned = plan(ID_BOUNDS, &RequestInput::request(&statements, request_scope()))
		.await
		.expect("the request plans");
	let mut pipelines = Vec::new();
	let mut bounded = false;
	planned.ir.walk(planned.ir.top(), WalkOrder::PreOrder, &mut |op| {
		match planned.ir.op(op).map(|data| &data.kind) {
			Some(OpKind::Phys(PhysOp::Pipeline(_))) => pipelines.push(op),
			Some(OpKind::Phys(PhysOp::TableScan(attrs))) if attrs.table.as_str() == table => {
				bounded = attrs.bounds.is_some();
			}
			_ => {}
		}
		Walk::Continue
	});
	let sources = pipelines
		.into_iter()
		.map(|pipeline| {
			facts_reading(
				&planned.ir,
				&mut planned.facts,
				crate::exec::ir::analysis::cx::builtin_registry(),
				pipeline,
				NestedReading::Strategy,
			)
			.expect("the pipeline has facts")
			.source
		})
		.collect();
	(sources, bounded)
}

/// A table scan under record-id bounds is still a table scan to the placement: the pipeline that
/// reads it is placed as the pipeline of its unbounded twin is, and a table scan splits into
/// morsels over key ranges.
#[tokio::test]
async fn a_bounded_table_scan_places_as_a_table_scan() {
	use crate::exec::planner::placement::SourceKind;

	let bounded = "SELECT id FROM t WHERE id >= t:1 AND id < t:900000 AND n > 5";
	// The twin is not a bare conjunct, so its scan reads the whole table.
	let twin = "SELECT id FROM t WHERE (id >= t:1) = true AND (id < t:900000) = true AND n > 5";
	let (sources, bounds) = pipeline_sources(bounded, "t").await;
	assert!(bounds, "the scan states the bounds");
	let (twin_sources, twin_bounds) = pipeline_sources(twin, "t").await;
	assert!(!twin_bounds, "the twin's scan reads the whole table");
	assert!(sources.contains(&SourceKind::Scan), "{sources:?}");
	assert_eq!(sources, twin_sources);
}

/// A record id of `t` with the number key `n`, as a literal bound of a scan.
fn id_bound_literal(table: &str, n: i64) -> crate::exec::ir::phys::ops::KeyBoundShape {
	crate::exec::ir::phys::ops::KeyBoundShape::Literal(Value::RecordId(crate::val::RecordId {
		table: table.into(),
		key: crate::val::RecordIdKey::Number(n),
	}))
}

/// A table scan under a predicate that bounds `id` of its table with literal record ids states the
/// interval it reads, and takes no operand for it.
#[tokio::test]
async fn a_table_scan_states_the_literal_id_bounds_of_its_predicate() {
	use crate::exec::ir::phys::ops::{IdBoundsShape, KeyRangeBoundShape};
	let side = |n, inclusive| {
		Some(KeyRangeBoundShape {
			bound: id_bound_literal("t", n),
			inclusive,
		})
	};
	for (sql, low, high) in [
		("SELECT * FROM t WHERE id > t:2", side(2, false), None),
		("SELECT * FROM t WHERE id >= t:2", side(2, true), None),
		("SELECT * FROM t WHERE id < t:2", None, side(2, false)),
		("SELECT * FROM t WHERE t:2 >= id", None, side(2, true)),
		("SELECT * FROM t WHERE id > t:2 AND id <= t:9", side(2, false), side(9, true)),
		(
			"SELECT * FROM t WHERE id > t:2 AND id > t:4 AND id < t:9",
			side(4, false),
			side(9, false),
		),
		("SELECT * FROM t WHERE n = 1 AND id > t:2", side(2, false), None),
		("SELECT * FROM t WHERE id > t:2 AND n = 1 ORDER BY id DESC LIMIT 5", side(2, false), None),
		("SELECT * FROM t WHERE id > t:2 AND id < u:9", side(2, false), None),
		("SELECT * FROM t WHERE id > 5 AND id < t:9", None, side(9, false)),
		("SELECT * FROM t WHERE type::field('id') > t:2", side(2, false), None),
	] {
		let scans = scan_bounds(sql).await;
		assert_eq!(
			scans,
			vec![(
				"t".to_owned(),
				Some(IdBoundsShape {
					low,
					high
				}),
				0
			)],
			"{sql}"
		);
	}
}

/// Every form the bounds decline leaves the scan whole.
#[tokio::test]
async fn a_table_scan_whose_predicate_the_bounds_decline_states_none() {
	for sql in [
		// A bound that is no scalar-keyed record id of the table.
		"SELECT * FROM t WHERE id > u:1",
		"SELECT * FROM t WHERE id > 5",
		"SELECT * FROM t WHERE id > 'a'",
		"SELECT * FROM t WHERE id > t:[1]",
		"SELECT * FROM t WHERE id > t:{ a: 1 }",
		"SELECT * FROM t WHERE id > NONE",
		"SELECT * FROM t WHERE id > NULL",
		// Not a bare comparison of `id`.
		"SELECT * FROM t WHERE id != t:2",
		"SELECT * FROM t WHERE (id > t:2) = true",
		"SELECT * FROM t WHERE id > t:2 OR n = 1",
		"SELECT * FROM t WHERE NOT (id > t:2)",
		// A list with a composite key is no keyed read either.
		"SELECT * FROM t WHERE id IN [t:2, t:[3]]",
		"SELECT * FROM t WHERE n > 0",
		// Another conjunct that may raise.
		"SELECT * FROM t WHERE id > t:2 AND <int> n > 0",
		"SELECT * FROM t WHERE <int> n > 0 AND id > t:2",
		"SELECT * FROM t WHERE id > t:2 AND n > $p",
		"SELECT * FROM t WHERE id > t:2 AND math::abs(n) > 0",
		// A hint chooses the read.
		"SELECT * FROM t WITH NOINDEX WHERE id > t:2",
		// A write keeps its subjects.
		"UPDATE t SET n = 1 WHERE id > t:2",
		"DELETE t WHERE id > t:2",
		// The ids name components.
		"SELECT * FROM reading WHERE id > reading:['a', 1]",
		// A computed field is derived for every row.
		"SELECT * FROM derived WHERE id > derived:2",
		"SELECT * FROM derived WHERE id > derived:2 AND c > 0",
		// A relation stores no record range.
		"SELECT * FROM edge WHERE id > edge:2",
	] {
		let scans = scan_bounds(sql).await;
		assert!(!scans.is_empty(), "{sql} reads a table");
		assert!(
			scans.iter().all(|(_, bounds, operands)| bounds.is_none() && *operands == 0),
			"{sql}: {scans:?}"
		);
	}
}

/// A record user reads a table through its permission and its field permissions, which judge every
/// row the read lists, so the bounds never narrow such a read; the owner, whom they do not bind,
/// reads the same tables under bounds.
#[tokio::test]
async fn a_table_a_permission_governs_is_read_whole_by_the_user_it_governs() {
	for sql in [
		"SELECT * FROM owned WHERE id > owned:2",
		"SELECT * FROM owned WHERE id >= owned:2 AND id < owned:5",
		"SELECT * FROM guarded WHERE id > guarded:2",
	] {
		let scans = scan_bounds_as(true, sql).await;
		assert!(!scans.is_empty(), "{sql} reads a table");
		assert!(
			scans.iter().all(|(_, bounds, operands)| bounds.is_none() && *operands == 0),
			"{sql}: {scans:?}"
		);
		let scans = scan_bounds_as(false, sql).await;
		assert!(scans.iter().all(|(_, bounds, _)| bounds.is_some()), "{sql}: {scans:?}");
	}
}

/// Tables an existence read names: plain ones, ones a permission governs (one whose predicate
/// cannot raise, one that reads the caller, one that calls a function), and one with a computed
/// field.
const EXISTENCE: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE small SCHEMALESS;
	DEFINE TABLE big SCHEMALESS;
	DEFINE INDEX big_n ON big FIELDS n;
	DEFINE TABLE flagged SCHEMAFULL PERMISSIONS FOR select WHERE public = true;
	DEFINE FIELD public ON flagged TYPE bool;
	DEFINE FIELD n ON flagged TYPE int;
	DEFINE TABLE mine SCHEMAFULL PERMISSIONS FOR select WHERE owner = $auth.id;
	DEFINE FIELD owner ON mine TYPE record;
	DEFINE FIELD n ON mine TYPE int;
	DEFINE FUNCTION fn::check($x: any) { RETURN true; };
	DEFINE FUNCTION fn::raises($x: any) { IF $x = NONE { THROW 'none' }; RETURN true; };
	DEFINE TABLE checked SCHEMAFULL PERMISSIONS FOR select WHERE fn::check(id);
	DEFINE FIELD n ON checked TYPE int;
	DEFINE TABLE derived SCHEMALESS;
	DEFINE FIELD a ON derived TYPE int;
	DEFINE FIELD c ON derived COMPUTED a + 1;
";

/// For each subquery the container `sql` plans to holds, in pre-order, how many limits its
/// statement holds, and the strategy it runs under. The request is planned as the owner, or as a
/// record user of `user:alice`.
async fn subquery_limits(record_user: bool, sql: &str) -> Vec<(usize, String)> {
	subquery_shapes(record_user, sql)
		.await
		.into_iter()
		.map(|shape| (shape.limits, shape.strategy))
		.collect()
}

/// What a subquery's statement holds once planned.
struct SubqueryShape {
	/// The limits its statement holds.
	limits: usize,
	/// The strategy it runs under.
	strategy: String,
	/// Each sort of its first region, in pre-order: the rows it keeps and whether it publishes a
	/// top-k threshold.
	sorts: Vec<(Option<usize>, bool)>,
}

/// [`subquery_limits`] with the sorts of each subquery's statement.
async fn subquery_shapes(record_user: bool, sql: &str) -> Vec<SubqueryShape> {
	use crate::exec::ir::phys::ops::PhysOp;

	let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
	let statements = RequestStatement::of(&parsed);
	let input = RequestInput::request(&statements, request_scope());
	let db = TestDb::new(EXISTENCE).await;
	let session = if record_user {
		let rid =
			crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "alice"));
		crate::dbs::Session::for_record("test", "test", "user", rid)
	} else {
		TestDb::owner()
	};
	let ctx = db.exec_ctx_as(&session, TransactionType::Read).await;
	let txn = ctx.try_txn().expect("a transaction");
	let planner = Planner::with_txn(
		ctx.ctx(),
		ctx.function_registry(),
		txn,
		Some("test".to_owned()),
		Some("test".to_owned()),
	)
	.with_auth(Arc::clone(&ctx.root().auth));
	let plan_session = test_session();
	let planning = PlanningInput {
		planner: &planner,
		capabilities: Arc::new(Capabilities::all()),
		backend: BackendCaps::default(),
		held: Vec::new(),
		session: &plan_session,
	};
	let planned = plan_request(&input, &planning).await.expect("the request plans");
	let ir = &planned.ir;
	let mut subqueries = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		let data = ir.op(op).expect("the op exists");
		if let OpKind::Expr(ExprOp::Subquery(strategy)) = &data.kind {
			let mut limits = 0;
			for region in &data.regions {
				ir.walk(*region, WalkOrder::PreOrder, &mut |inner| {
					limits += usize::from(matches!(
						ir.op(inner).map(|data| &data.kind),
						Some(OpKind::Phys(PhysOp::Limit { .. }))
					));
					Walk::Continue
				});
			}
			let mut sorts = Vec::new();
			if let Some(region) = data.regions.first() {
				ir.walk(*region, WalkOrder::PreOrder, &mut |inner| {
					if let Some(OpKind::Phys(PhysOp::Sort(attrs))) =
						ir.op(inner).map(|data| &data.kind)
					{
						sorts.push((attrs.keep, attrs.pushdown));
					}
					Walk::Continue
				});
			}
			subqueries.push(SubqueryShape {
				limits,
				strategy: format!("{strategy:?}"),
				sorts,
			});
		}
		Walk::Continue
	});
	subqueries
}

/// A subquery only a truthiness test reads stops after its first row, as a written `LIMIT 1`
/// does, and runs under the strategy it would run under without it: the limit cuts each
/// evaluation of a correlated statement, not the one read a hash or a lookup shares.
#[tokio::test]
async fn a_subquery_read_only_for_existence_is_limited_to_a_row() {
	for (sql, strategy) in [
		("SELECT * FROM small WHERE (SELECT id FROM big WHERE s = 'a')", "Once"),
		("SELECT * FROM small WHERE (SELECT id FROM big)", "Once"),
		("SELECT * FROM small WHERE !(SELECT id FROM big WHERE s = 'a')", "Once"),
		("SELECT * FROM small WHERE (SELECT id FROM big WHERE s = $parent.s)", "Hash"),
		("SELECT * FROM small WHERE (SELECT id FROM big WHERE n > $parent.n)", "Apply"),
		("SELECT * FROM small WHERE (SELECT id FROM big WHERE n = $parent.n)", "Lookup"),
		("SELECT * FROM small WHERE n > 1 AND (SELECT id FROM big WHERE s = 'a')", "Once"),
		("SELECT * FROM small WHERE n > 1 OR (SELECT id FROM big WHERE s = 'a')", "Once"),
		("SELECT * FROM small WHERE (SELECT id FROM big WHERE s = 'a') AND n > 1", "Once"),
		("SELECT VALUE IF (SELECT id FROM big WHERE s = 'a') { 1 } ELSE { 2 } FROM small", "Once"),
	] {
		let found = subquery_limits(false, sql).await;
		assert_eq!(found.len(), 1, "{sql}: {found:?}");
		// A lookup that keeps a hashed plan beside its statement limits both.
		assert!(found[0].0 >= 1, "{sql}: {found:?}");
		assert!(found[0].1.starts_with(strategy), "{sql}: {found:?}");
		// The limit it holds is counted as a written `LIMIT 1` holds it. The layout of the op is
		// pinned by `the_ops_an_edit_makes_are_laid_out_as_the_binder_lays_them_out`.
		let start = sql.find("(SELECT").expect("a subquery");
		let end = start + sql[start..].find(')').expect("the subquery closes");
		let written = format!("{} LIMIT 1{}", &sql[..end], &sql[end..]);
		assert_eq!(subquery_limits(false, &written).await, found, "{written}");
	}
	// A call whose body is inlined as a literal leaves no work that may raise.
	let sql = "SELECT * FROM small WHERE (SELECT id FROM big WHERE fn::check(n))";
	let found = subquery_limits(false, sql).await;
	assert_eq!(found.len(), 1, "{sql}: {found:?}");
	assert!(found[0].0 >= 1 && found[0].1.starts_with("Once"), "{sql}: {found:?}");
	let written = "SELECT * FROM small WHERE (SELECT id FROM big WHERE fn::check(n) LIMIT 1)";
	assert_eq!(subquery_limits(false, written).await, found, "{written}");
}

/// A correlated statement that reads the first rows of an order through an equality of an indexed
/// field with `$parent` reads through that index, the bounded sort and the limit kept above the
/// read and the sort publishing no threshold, in a read and in a write; a `WITH` hint, a `VERSION`,
/// a descending table order and a field no index serves keep the table read.
#[tokio::test]
async fn a_correlated_top_k_statement_reads_through_the_join_index() {
	for (sql, lookup) in [
		(
			"SELECT (SELECT id FROM big WHERE n = $parent.n ORDER BY s LIMIT 1) AS v FROM small",
			true,
		),
		(
			"SELECT (SELECT id FROM big WHERE n = $parent.n ORDER BY s DESC LIMIT 2) AS v FROM small",
			true,
		),
		(
			"UPDATE small SET v = (SELECT VALUE s FROM big WHERE n = $parent.n ORDER BY s LIMIT 1)[0]",
			true,
		),
		(
			"SELECT (SELECT id FROM big WITH NOINDEX WHERE n = $parent.n ORDER BY s LIMIT 1) AS v \
			 FROM small",
			false,
		),
		(
			"SELECT (SELECT id FROM big WHERE n = $parent.n ORDER BY s LIMIT 1 VERSION \
			 d'2024-01-01') AS v FROM small",
			false,
		),
		(
			"SELECT (SELECT id FROM big WHERE n = $parent.n ORDER BY id DESC LIMIT 1) AS v FROM small",
			false,
		),
		(
			"SELECT (SELECT id FROM big WHERE s = $parent.s ORDER BY n LIMIT 1) AS v FROM small",
			false,
		),
	] {
		let found = subquery_shapes(false, sql).await;
		let [shape] = found.as_slice() else {
			panic!("{sql}: {} subqueries", found.len());
		};
		assert_eq!(shape.strategy.starts_with("Lookup"), lookup, "{sql}: {}", shape.strategy);
		if lookup {
			// The sort and the limit stay above the read; with no scan to take its key the sort
			// publishes no threshold.
			assert!(shape.limits >= 1, "{sql}: {} limits", shape.limits);
			assert!(
				matches!(shape.sorts.as_slice(), [(Some(_), false)]),
				"{sql}: {:?}",
				shape.sorts
			);
		}
	}
}

/// A subquery the shape or the work of which does not qualify keeps its whole read.
#[tokio::test]
async fn a_subquery_that_does_not_qualify_is_not_limited() {
	for sql in [
		// The value is read.
		"SELECT *, (SELECT id FROM big WHERE s = 'a') AS v FROM small",
		"SELECT * FROM small WHERE ((SELECT id FROM big WHERE s = 'a') OR 0) = 1",
		"SELECT * FROM small WHERE count((SELECT id FROM big WHERE s = 'a')) > 0",
		"SELECT * FROM small WHERE n IN (SELECT VALUE n FROM big)",
		"RETURN [(SELECT id FROM big)]",
		// Not a plain select.
		"SELECT * FROM small WHERE (SELECT id FROM big ORDER BY id)",
		"SELECT * FROM small WHERE (SELECT id FROM big LIMIT 3)",
		"SELECT * FROM small WHERE (SELECT id FROM big START 1)",
		"SELECT * FROM small WHERE (SELECT count() FROM big GROUP ALL)",
		"SELECT * FROM small WHERE (SELECT s FROM big SPLIT s)",
		"SELECT * FROM small WHERE (SELECT id FROM ONLY big:1)",
		"SELECT * FROM small WHERE (SELECT id FROM big TIMEOUT 5s)",
		"SELECT * FROM small WHERE (SELECT id FROM big WITH NOINDEX WHERE n = 1)",
		// Work that may raise.
		"SELECT * FROM small WHERE (SELECT <int> s FROM big)",
		"SELECT * FROM small WHERE (SELECT id FROM big WHERE <int> s > 0)",
		"SELECT * FROM small WHERE (SELECT id FROM big WHERE fn::raises(n))",
		"SELECT * FROM small WHERE (SELECT id FROM big WHERE s = $p)",
		"SELECT * FROM [small:1, small:2] WHERE (SELECT id FROM big WHERE s = $parent.s)",
		"SELECT * FROM small WHERE (SELECT id FROM big WHERE s = $parent.s.a)",
		// The table enforces something the statement does not state.
		"SELECT * FROM small WHERE (SELECT id FROM derived WHERE a = 1)",
		// A statement that is planned to render is left as written.
		"EXPLAIN SELECT * FROM small WHERE (SELECT id FROM big WHERE s = 'a')",
	] {
		let found = subquery_limits(false, sql).await;
		assert!(!found.is_empty(), "{sql}");
		// A limit the statement writes is the only one it holds.
		let written = usize::from(sql.contains("LIMIT") || sql.contains("START"));
		assert!(found.iter().all(|(limits, _)| *limits <= written), "{sql}: {found:?}");
	}
}

/// A table whose permission a record user is judged by is enforced as the read meets it, which the
/// statement's own ops do not state, so its read stays whole whatever the permission reads; the
/// owner, whom the permission does not govern, reads the same tables to a row.
#[tokio::test]
async fn a_subquery_over_a_table_the_read_enforces_is_not_limited() {
	for sql in [
		"SELECT * FROM small WHERE (SELECT id FROM flagged WHERE n = 1)",
		"SELECT * FROM small WHERE (SELECT id FROM flagged)",
		"SELECT * FROM small WHERE (SELECT id FROM mine WHERE n = 1)",
		"SELECT * FROM small WHERE (SELECT id FROM checked WHERE n = 1)",
	] {
		let found = subquery_limits(true, sql).await;
		assert_eq!(found.len(), 1, "{sql}: {found:?}");
		assert_eq!(found[0].0, 0, "{sql}: {found:?}");
		let found = subquery_limits(false, sql).await;
		assert_eq!(found.len(), 1, "{sql}: {found:?}");
		assert_eq!(found[0].0, 1, "{sql}: {found:?}");
	}
}

/// A bound that is a parameter the statement does not fold is the operand of the scan, read when
/// the scan opens; one a `LET` folds to a record id is a literal.
#[tokio::test]
async fn a_parameter_bound_is_an_operand_of_the_scan() {
	use crate::exec::ir::phys::ops::{IdBoundsShape, KeyBoundShape, KeyRangeBoundShape};

	let scans = scan_bounds("SELECT * FROM t WHERE id > $c").await;
	assert_eq!(
		scans,
		vec![(
			"t".to_owned(),
			Some(IdBoundsShape {
				low: Some(KeyRangeBoundShape {
					bound: KeyBoundShape::Operand(0),
					inclusive: false,
				}),
				high: None,
			}),
			1,
		)]
	);
	let scans = scan_bounds("SELECT * FROM t WHERE $c <= id AND id < $d").await;
	let [(_, Some(bounds), operands)] = scans.as_slice() else {
		panic!("a bounded scan: {scans:?}");
	};
	assert_eq!(*operands, 2);
	assert_eq!(bounds.low.as_ref().map(|side| &side.bound), Some(&KeyBoundShape::Operand(0)));
	assert_eq!(bounds.high.as_ref().map(|side| &side.bound), Some(&KeyBoundShape::Operand(1)));
	// A literal on a side beats a parameter on it.
	let scans = scan_bounds("SELECT * FROM t WHERE id > $c AND id > t:2").await;
	assert_eq!(scans[0].2, 0, "{scans:?}");
	let scans = scan_bounds("LET $c = t:2; SELECT * FROM t WHERE id > $c").await;
	assert!(scans[0].1.is_some(), "{scans:?}");
}

const COUNT_VALUE: &str = "
	DEFINE TABLE small SCHEMALESS;
	DEFINE TABLE big SCHEMALESS;
	DEFINE FIELD num ON big TYPE int;
	DEFINE FIELD tag ON big TYPE string;
	DEFINE INDEX big_num ON big FIELDS num;
";

/// An aggregate in value position, correlated with the row of the enclosing statement through an
/// equality, plans and passes verification, over an index, over a field no index lists, and
/// explained. Where an index serves the equality the plan is the lookup with the hashed plan kept
/// beside it.
#[tokio::test]
async fn a_correlated_aggregate_in_value_position_plans() {
	for (sql, lookup) in [
		(
			"SELECT id, (SELECT count() FROM big WHERE num = $parent.num GROUP ALL) AS c FROM small",
			Some("hashed: true"),
		),
		(
			"SELECT id, (SELECT VALUE count() FROM big WHERE num = $parent.num GROUP ALL)[0] AS c \
			 FROM small",
			Some("hashed: true"),
		),
		(
			"SELECT id, (SELECT math::sum(num) AS s FROM big WHERE num = $parent.num GROUP ALL) AS c \
			 FROM small",
			Some("hashed: true"),
		),
		(
			"SELECT id, (SELECT count() FROM big WHERE tag = $parent.tag GROUP ALL) AS c FROM small",
			None,
		),
		(
			"SELECT id, (SELECT count() FROM big WHERE num > $parent.num GROUP ALL) AS c FROM small",
			None,
		),
		(
			"EXPLAIN SELECT id, (SELECT count() FROM big WHERE num = $parent.num GROUP ALL) AS c \
			 FROM small",
			Some("hashed: true"),
		),
	] {
		let text = planned_text(COUNT_VALUE, sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
		assert_eq!(text.contains("hashed: true"), lookup.is_some(), "{sql}\n{text}");
		assert!(!text.contains("hashed: false"), "{sql}\n{text}");
	}
}

/// A correlated indexed equality count is the lookup and an ordinary aggregate, and the
/// uncorrelated twin is a count source over the index. An index equality lists the records of a
/// float or decimal key only approximately (`0.1` against `0.1dec`), so the lookup serves only the
/// keys `lookup_key` accepts. A key that is known only when the enclosing row is read and that
/// `lookup_key` refuses (float, decimal, array, composite record id) is served by the hashed
/// table read the lookup carries, and the aggregate counts the records either read yields.
#[tokio::test]
async fn a_correlated_count_is_a_lookup_and_an_aggregate() {
	let correlated =
		"SELECT id, (SELECT count() FROM big WHERE num = $parent.num GROUP ALL) AS c FROM small";
	let text = planned_text(COUNT_VALUE, correlated).await.expect("the statement plans");
	assert!(text.contains("phys.index_lookup"), "{text}");
	assert!(text.contains("phys.aggregate"), "{text}");
	assert!(!text.contains("phys.count"), "{text}");
	let constant = "SELECT id, (SELECT count() FROM big WHERE num = 3 GROUP ALL) AS c FROM small";
	let text = planned_text(COUNT_VALUE, constant).await.expect("the statement plans");
	assert!(text.contains("phys.count"), "{text}");
	assert!(text.contains("Equality"), "{text}");
	assert!(!text.contains("phys.aggregate"), "{text}");
}

/// A count correlated through a membership, a disjunction or an operand on the left plans and
/// passes verification. None of them is a count source: the membership and the disjunction have no
/// single index equality to count, and the operand on the left is the equality of the lookup.
#[tokio::test]
async fn a_correlated_membership_or_disjunction_count_plans_as_an_aggregate() {
	for (sql, lookup) in [
		(
			"SELECT id, (SELECT count() FROM big WHERE num IN $parent.nums GROUP ALL) AS c FROM small",
			false,
		),
		(
			"SELECT id, (SELECT count() FROM big WHERE num IN [$parent.num, 3] GROUP ALL) AS c \
			 FROM small",
			false,
		),
		(
			"SELECT id, (SELECT count() FROM big WHERE num = $parent.num OR num = 3 GROUP ALL) AS c \
			 FROM small",
			false,
		),
		(
			"SELECT id, (SELECT count() FROM big WHERE $parent.num = num GROUP ALL) AS c FROM small",
			true,
		),
		(
			"SELECT id, (SELECT count() FROM big WHERE num = $parent.num GROUP ALL) AS c \
			 FROM [small:1]",
			true,
		),
	] {
		let text = planned_text(COUNT_VALUE, sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
		assert!(text.contains("phys.aggregate"), "{sql}\n{text}");
		assert!(!text.contains("phys.count"), "{sql}\n{text}");
		if lookup {
			assert!(text.contains("phys.index_lookup"), "{sql}\n{text}");
			assert!(text.contains("hashed: true"), "{sql}\n{text}");
		}
	}
}

/// A correlated indexed statement keeps the hashed plan beside its lookup whatever its regions
/// hold: a sort, a fetch, a function call, an aggregate or a group key.
#[tokio::test]
async fn a_correlated_indexed_statement_keeps_its_hashed_plan() {
	for sql in [
		"SELECT id, (SELECT tag FROM big WHERE num = $parent.num) AS c FROM small",
		"SELECT id, (SELECT * FROM big WHERE num = $parent.num) AS c FROM small",
		"SELECT id, (SELECT tag, num + 1 AS m FROM big WHERE num = $parent.num ORDER BY tag) AS c \
		 FROM small",
		"SELECT id, (SELECT tag FROM big WHERE num = $parent.num FETCH tag) AS c FROM small",
		"SELECT id, (SELECT array::len(a) AS l FROM big WHERE num = $parent.num) AS c FROM small",
		"SELECT id, (SELECT tag, count() FROM big WHERE num = $parent.num GROUP BY tag) AS c \
		 FROM small",
		"SELECT id, (SELECT tag, math::max(num) AS m FROM big WHERE num = $parent.num GROUP BY tag) \
		 AS c FROM small",
	] {
		let text = planned_text(COUNT_VALUE, sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
		assert!(text.contains("hashed: true"), "{sql}\n{text}");
		assert!(!text.contains("hashed: false"), "{sql}\n{text}");
	}
}

const COUNT_VALUE_GUARDED: &str = "
	DEFINE TABLE small SCHEMALESS PERMISSIONS FULL;
	DEFINE TABLE guarded SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth.id;
	DEFINE FIELD num ON guarded TYPE int;
	DEFINE FIELD tag ON guarded TYPE string;
	DEFINE INDEX guarded_num ON guarded FIELDS num;
	DEFINE TABLE walled SCHEMALESS PERMISSIONS FULL;
	DEFINE FIELD num ON walled TYPE int;
	DEFINE FIELD tag ON walled TYPE string PERMISSIONS FOR select WHERE $auth.id = user:alice;
	DEFINE INDEX walled_num ON walled FIELDS num;
	DEFINE FUNCTION fn::bump($x: int) { RETURN $x + 1 } PERMISSIONS FULL;
	DEFINE TABLE open SCHEMALESS PERMISSIONS FOR select FULL;
	DEFINE FIELD num ON open TYPE int;
	DEFINE FIELD tag ON open TYPE string;
	DEFINE INDEX open_num ON open FIELDS num;
";

/// The text of the container `sql` plans to, planned as a record user of `user:alice`.
async fn planned_text_as_user(setup: &str, sql: &str) -> Result<String, IrError> {
	let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
	let statements = RequestStatement::of(&parsed);
	let input = RequestInput::request(&statements, request_scope());
	let db = TestDb::new(setup).await;
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "alice"));
	let session = crate::dbs::Session::for_record("test", "test", "user", rid);
	let ctx = db.exec_ctx_as(&session, TransactionType::Read).await;
	let txn = ctx.try_txn().expect("a transaction");
	let planner = Planner::with_txn(
		ctx.ctx(),
		ctx.function_registry(),
		txn,
		Some("test".to_owned()),
		Some("test".to_owned()),
	)
	.with_auth(Arc::clone(&ctx.root().auth));
	let plan_session = test_session();
	let planning = PlanningInput {
		planner: &planner,
		capabilities: Arc::new(Capabilities::all()),
		backend: BackendCaps::default(),
		held: Vec::new(),
		session: &plan_session,
	};
	let planned = plan_request(&input, &planning).await?;
	Ok(print(&planned.ir, planned.ir.top(), PrintOpts::default()))
}

/// A correlated indexed statement planned for a record user passes verification whatever it holds
/// that owns a nested region: a table permission, a field permission, an aggregate, a group key, a
/// method call and a function call.
///
/// Over a table whose select permission is `FULL` and that has no field permission, a statement
/// that calls only builtin functions keeps the hashed plan beside its lookup. A table with a table
/// permission or a field permission carries table enforcement in front of its read, and a probe is
/// not placed in front of enforcement, so those statements are looked up alone. A method call or a
/// call of a user function is not repeatable, so such a statement is applied with no lookup.
#[tokio::test]
async fn a_correlated_statement_for_a_record_user_plans() {
	for sql in [
		"SELECT id, (SELECT tag FROM open WHERE num = $parent.num) AS c FROM small",
		"SELECT id, (SELECT string::len(tag) AS l FROM open WHERE num = $parent.num) AS c FROM small",
		"SELECT id, (SELECT count() FROM open WHERE num = $parent.num GROUP ALL) AS c FROM small",
		"SELECT id, (SELECT tag, count() FROM open WHERE num = $parent.num GROUP BY tag) AS c \
		 FROM small",
		"SELECT id, (SELECT tag FROM open WHERE num = $parent.num ORDER BY tag) AS c FROM small",
	] {
		let text = planned_text_as_user(COUNT_VALUE_GUARDED, sql)
			.await
			.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
		assert!(text.contains("hashed: true"), "{sql}\n{text}");
		assert!(!text.contains("hashed: false"), "{sql}\n{text}");
	}
	for sql in [
		"SELECT id, (SELECT tag FROM guarded WHERE num = $parent.num) AS c FROM small",
		"SELECT id, (SELECT tag FROM walled WHERE num = $parent.num) AS c FROM small",
		"SELECT id, (SELECT count() FROM guarded WHERE num = $parent.num GROUP ALL) AS c \
		 FROM small",
		"SELECT id, (SELECT count() FROM walled WHERE num = $parent.num GROUP ALL) AS c \
		 FROM small",
		"SELECT id, (SELECT math::max(num) AS m FROM guarded WHERE num = $parent.num GROUP ALL) \
		 AS c FROM small",
		"SELECT id, (SELECT tag, count() FROM walled WHERE num = $parent.num GROUP BY tag) AS c \
		 FROM small",
	] {
		let text = planned_text_as_user(COUNT_VALUE_GUARDED, sql)
			.await
			.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
		assert!(text.contains("hashed: false"), "{sql}\n{text}");
		assert!(!text.contains("hashed: true"), "{sql}\n{text}");
	}
	for sql in [
		"SELECT id, (SELECT tag.uppercase() AS u FROM guarded WHERE num = $parent.num) AS c \
		 FROM small",
		"SELECT id, (SELECT tag.uppercase() AS u FROM open WHERE num = $parent.num) AS c FROM small",
		"SELECT id, (SELECT fn::bump(num) AS b FROM walled WHERE num = $parent.num) AS c FROM small",
		"SELECT id, (SELECT fn::bump(num) AS b FROM open WHERE num = $parent.num) AS c FROM small",
	] {
		let text = planned_text_as_user(COUNT_VALUE_GUARDED, sql)
			.await
			.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
		assert!(!text.contains("strategy:"), "{sql}\n{text}");
	}
}

const EXTREME: &str = "
	DEFINE TABLE t SCHEMAFULL;
	DEFINE FIELD n ON t TYPE int;
	DEFINE INDEX n ON t FIELDS n UNIQUE;
	DEFINE TABLE empty SCHEMAFULL;
	DEFINE FIELD n ON empty TYPE int;
	DEFINE INDEX n ON empty FIELDS n UNIQUE;
	DEFINE TABLE loose SCHEMAFULL;
	DEFINE FIELD n ON loose TYPE option<int>;
	DEFINE INDEX n ON loose FIELDS n UNIQUE;
	DEFINE TABLE plain SCHEMALESS;
";

/// How an extreme of an indexed field is read. `ORDER BY n LIMIT 1` is an ordered index scan under
/// a limit, in either direction, and an equality on a constant is an index equality scan.
#[tokio::test]
async fn an_extreme_is_an_ordered_limit_and_its_equality_is_an_index_read() {
	let count = |text: &str, what: &str| text.matches(what).count();
	for (order, direction) in [("DESC", "direction: Backward"), ("ASC", "direction: Forward")] {
		let sql = format!("SELECT VALUE n FROM t ORDER BY n {order} LIMIT 1");
		let text = planned_text(EXTREME, &sql).await.expect("the statement plans");
		assert_eq!(count(&text, "phys.index_scan {"), 1, "{sql}\n{text}");
		assert!(text.contains(direction), "{sql}\n{text}");
		assert!(text.contains("phys.limit"), "{sql}\n{text}");
		assert_eq!(count(&text, "phys.table_scan {"), 0, "{sql}\n{text}");
	}
	for sql in ["SELECT * FROM t WHERE n = 5", "SELECT * FROM t WHERE n = 5 LIMIT 1"] {
		let text = planned_text(EXTREME, sql).await.expect("the statement plans");
		assert_eq!(count(&text, "phys.index_scan {"), 1, "{sql}\n{text}");
		assert!(text.contains("Equality"), "{sql}\n{text}");
		assert_eq!(count(&text, "phys.table_scan {"), 0, "{sql}\n{text}");
	}
}

/// Whether the plan `text` answers an extreme from its index: a probe of two rows, the statement
/// over the first row when the probe shows no other row holds the extreme, and the aggregate as
/// written. Every read of the index is over the numbers: the probe and the statement over the first
/// row.
fn extreme_by_index(text: &str) -> bool {
	text.matches("Included(\\\"-Infinity\\\")").count() == 2
		&& text.matches("Included(\\\"NaN\\\")").count() == 2
		&& text.matches("phys.aggregate {").count() == 2
}

/// A `math::max` or `math::min` of an indexed `int` field over a whole table is a conditional: a
/// probe reads the first rows of the field's numbers in order, the statement runs over the rows the
/// probe read, and over the whole table where the probe did not settle the answer.
/// A statement at the top level carries the conditional as a statement `IF`, a statement in value
/// position as a value `IF`, and whatever pages the aggregate stays where it is.
#[tokio::test]
async fn an_extreme_of_an_indexed_int_reads_the_end_of_its_index() {
	for (function, direction) in [("max", "direction: Backward"), ("min", "direction: Forward")] {
		for (sql, conditional) in [
			(format!("SELECT math::{function}(n) AS m FROM t GROUP ALL"), "phys.if("),
			(format!("SELECT math::{function}(n) FROM t GROUP ALL"), "phys.if("),
			(format!("SELECT VALUE math::{function}(n) FROM t GROUP ALL"), "phys.if("),
			(format!("SELECT math::{function}(n) AS a.b FROM t GROUP ALL"), "phys.if("),
			(
				format!("SELECT math::{function}(n) AS m FROM t GROUP ALL LIMIT 1 START 0"),
				"phys.if(",
			),
			(format!("SELECT math::{function}(n) AS m FROM ONLY t GROUP ALL"), "phys.if("),
			// A filter that keeps every row is no filter.
			(format!("SELECT math::{function}(n) AS m FROM t WHERE true GROUP ALL"), "phys.if("),
			(format!("RETURN (SELECT VALUE math::{function}(n) FROM t GROUP ALL)[0]"), "expr.if("),
			(
				format!(
					"SELECT id, (SELECT math::{function}(n) AS m FROM t GROUP ALL) AS m FROM t"
				),
				"expr.if(",
			),
			(
				format!(
					"SELECT VALUE id FROM t WHERE n = (SELECT VALUE math::{function}(n) FROM t \
					 GROUP ALL)[0]"
				),
				"expr.if(",
			),
		] {
			let text = planned_text(EXTREME, &sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
			assert!(extreme_by_index(&text), "{sql}\n{text}");
			assert!(text.contains(conditional), "{sql}\n{text}");
			assert!(text.contains(direction), "{sql}\n{text}");
			// The probe reads two rows: its limit is the first literal of the plan. The test reads
			// the first and the second row of it.
			let probe = text.split("expr.index(").next().unwrap_or_default();
			assert!(probe.contains("expr.literal {value: 2}"), "{sql}\n{text}");
		}
	}
}

/// The probe of an extreme yields the value of the field of each row it reads and nothing else of
/// the row, and the test of the probe reads its values by position.
#[tokio::test]
async fn the_probe_of_an_extreme_keeps_only_the_field_of_its_rows() {
	for function in ["max", "min"] {
		let sql = format!("SELECT math::{function}(n) AS m FROM t GROUP ALL");
		let text = planned_text(EXTREME, &sql).await.expect("the statement plans");
		assert_eq!(text.matches("phys.project {").count(), 1, "{sql}\n{text}");
		let (probe, rest) = text.split_once("expr.index(").expect("the probe is tested");
		assert_eq!(probe.matches("phys.project { shape: Value }").count(), 1, "{sql}\n{text}");
		let tests = rest.split("phys.if(").next().unwrap_or_default();
		assert!(!tests.contains("expr.field"), "{sql}\n{text}");
	}
}

/// An extreme over a unique index reads nothing but that index and, where it does not settle the
/// answer, the table as written: the one table scan is the statement as written, there is no read
/// of the record ids and nothing is sorted, because the scans of the index order their rows.
#[tokio::test]
async fn an_extreme_reads_nothing_but_its_index() {
	for sql in [
		"SELECT math::max(n) AS m FROM t GROUP ALL",
		"SELECT math::min(n) AS m FROM t GROUP ALL",
		"RETURN (SELECT VALUE math::max(n) FROM t GROUP ALL)[0]",
	] {
		let text = planned_text(EXTREME, sql).await.expect("the statement plans");
		assert_eq!(text.matches("phys.table_scan {").count(), 1, "{sql}\n{text}");
		assert!(!text.contains("CONTAINSNOT"), "{sql}\n{text}");
		assert_eq!(text.matches("phys.sort {").count(), 0, "{sql}\n{text}");
		// The filter of each of the two reads of the numbers ends in an `and`.
		assert_eq!(text.matches("op: And").count(), 2, "{sql}\n{text}");
	}
}

const EXTREME_DECLINES: &str = "
	DEFINE TABLE t SCHEMAFULL;
	DEFINE FIELD n ON t TYPE int;
	DEFINE INDEX n ON t FIELDS n UNIQUE;
	DEFINE FIELD m ON t TYPE int;
	DEFINE TABLE loose SCHEMAFULL;
	DEFINE FIELD n ON loose TYPE option<int>;
	DEFINE INDEX n ON loose FIELDS n UNIQUE;
	DEFINE TABLE num SCHEMAFULL;
	DEFINE FIELD n ON num TYPE number;
	DEFINE INDEX n ON num FIELDS n UNIQUE;
	DEFINE TABLE flt SCHEMAFULL;
	DEFINE FIELD n ON flt TYPE float;
	DEFINE INDEX n ON flt FIELDS n UNIQUE;
	DEFINE TABLE dec SCHEMAFULL;
	DEFINE FIELD n ON dec TYPE decimal;
	DEFINE INDEX n ON dec FIELDS n UNIQUE;
	DEFINE TABLE untyped SCHEMALESS;
	DEFINE INDEX n ON untyped FIELDS n UNIQUE;
	DEFINE TABLE comp SCHEMAFULL;
	DEFINE FIELD a ON comp TYPE int;
	DEFINE FIELD n ON comp COMPUTED a + 1;
	DEFINE TABLE assigned SCHEMAFULL;
	DEFINE FIELD n ON assigned TYPE int VALUE $value + 1;
	DEFINE INDEX n ON assigned FIELDS n UNIQUE;
	DEFINE TABLE pair SCHEMAFULL;
	DEFINE FIELD n ON pair TYPE int;
	DEFINE FIELD m ON pair TYPE int;
	DEFINE INDEX n_m ON pair FIELDS n, m UNIQUE;
	DEFINE TABLE both SCHEMAFULL;
	DEFINE FIELD n ON both TYPE int;
	DEFINE FIELD m ON both TYPE int;
	DEFINE INDEX n ON both FIELDS n UNIQUE;
	DEFINE INDEX n_m ON both FIELDS n, m UNIQUE;
	DEFINE TABLE bare SCHEMAFULL;
	DEFINE FIELD n ON bare TYPE int;
	DEFINE TABLE arr SCHEMAFULL;
	DEFINE FIELD n ON arr TYPE array<int>;
	DEFINE INDEX n ON arr FIELDS n UNIQUE;
	DEFINE TABLE dup SCHEMAFULL;
	DEFINE FIELD n ON dup TYPE int;
	DEFINE INDEX n ON dup FIELDS n;
	DEFINE TABLE dupboth SCHEMAFULL;
	DEFINE FIELD n ON dupboth TYPE int;
	DEFINE INDEX n ON dupboth FIELDS n UNIQUE;
	DEFINE INDEX n_dup ON dupboth FIELDS n;
	DEFINE TABLE absent_table_marker SCHEMAFULL;
";

/// The statements an index does not answer plan as written: one aggregate over one read, and no
/// conditional.
#[tokio::test]
async fn an_extreme_the_index_does_not_answer_is_planned_as_written() {
	for sql in [
		// An index one key of which may be held by many rows, alone or beside a unique one.
		"SELECT math::max(n) AS m FROM dup GROUP ALL",
		"SELECT math::min(n) AS m FROM dup GROUP ALL",
		"RETURN (SELECT VALUE math::max(n) FROM dup GROUP ALL)[0]",
		"SELECT math::max(n) AS m FROM dupboth GROUP ALL",
		// The declared kind is not `int`.
		"SELECT math::max(n) AS m FROM loose GROUP ALL",
		"SELECT math::max(n) AS m FROM num GROUP ALL",
		"SELECT math::min(n) AS m FROM flt GROUP ALL",
		"SELECT math::min(n) AS m FROM dec GROUP ALL",
		"SELECT math::max(n) AS m FROM untyped GROUP ALL",
		"SELECT math::max(n) AS m FROM arr GROUP ALL",
		// The field is derived, or rewritten on write.
		"SELECT math::max(n) AS m FROM comp GROUP ALL",
		"SELECT math::max(n) AS m FROM assigned GROUP ALL",
		// The index is not over the field alone, or there is none.
		"SELECT math::max(n) AS m FROM pair GROUP ALL",
		"SELECT math::min(n) AS m FROM both GROUP ALL",
		"SELECT math::max(n) AS m FROM bare GROUP ALL",
		"SELECT math::max(n) AS m FROM absent GROUP ALL",
		// The statement is not the shape.
		"SELECT math::max(n) AS m FROM t WHERE n > 1 GROUP ALL",
		"SELECT math::max(n) AS m, count() AS c FROM t GROUP ALL",
		"SELECT math::max(n) AS m, math::min(n) AS l FROM t GROUP ALL",
		"SELECT n, math::max(m) AS x FROM t GROUP BY n",
		"SELECT math::max(n.x) AS m FROM t GROUP ALL",
		"SELECT math::max(n + 0) AS m FROM t GROUP ALL",
		"SELECT math::max(m) AS x FROM t GROUP ALL",
		"SELECT math::sum(n) AS m FROM t GROUP ALL",
		"SELECT math::max(n) AS m FROM t WITH NOINDEX GROUP ALL",
		"SELECT math::max(n) AS m FROM t GROUP ALL VERSION d'2024-01-01'",
		"SELECT math::max(n) AS m FROM t GROUP ALL TIMEOUT 1s",
		"SELECT math::max(n) AS m FROM t, loose GROUP ALL",
		"SELECT math::max(n) AS m FROM [1, 2] GROUP ALL",
		"SELECT math::max(n) AS m FROM t:1..9 GROUP ALL",
		"EXPLAIN SELECT math::max(n) AS m FROM t GROUP ALL",
		"EXPLAIN SELECT VALUE math::min(n) FROM t GROUP ALL",
		"RETURN (EXPLAIN SELECT VALUE math::max(n) FROM t GROUP ALL)",
		"SELECT id, (SELECT math::max(n) AS m FROM t WHERE m = $parent.m GROUP ALL) AS c FROM t",
	] {
		let text =
			planned_text(EXTREME_DECLINES, sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
		assert!(!text.contains("-Infinity"), "{sql}\n{text}");
		assert!(!text.contains("phys.if(") && !text.contains("expr.if("), "{sql}\n{text}");
		assert!(text.matches("phys.aggregate {").count() <= 1, "{sql}\n{text}");
	}
}

const EXTREME_PERMISSIONS: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
	DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
	DEFINE TABLE pub SCHEMAFULL PERMISSIONS FOR select WHERE public = true;
	DEFINE FIELD public ON pub TYPE bool;
	DEFINE FIELD n ON pub TYPE int;
	DEFINE INDEX n ON pub FIELDS n UNIQUE;
	DEFINE TABLE denied SCHEMAFULL PERMISSIONS FOR select NONE;
	DEFINE FIELD n ON denied TYPE int;
	DEFINE INDEX n ON denied FIELDS n UNIQUE;
	DEFINE TABLE open SCHEMAFULL PERMISSIONS FOR select FULL;
	DEFINE FIELD n ON open TYPE int;
	DEFINE INDEX n ON open FIELDS n UNIQUE;
	DEFINE TABLE owned SCHEMAFULL PERMISSIONS FOR select WHERE owner = $auth.id;
	DEFINE FIELD owner ON owned TYPE record;
	DEFINE FIELD n ON owned TYPE int;
	DEFINE INDEX n ON owned FIELDS n UNIQUE;
	DEFINE TABLE guarded SCHEMAFULL PERMISSIONS FOR select FULL;
	DEFINE FIELD owner ON guarded TYPE record;
	DEFINE FIELD n ON guarded TYPE int PERMISSIONS FOR select WHERE owner = $auth.id;
	DEFINE FIELD m ON guarded TYPE int;
	DEFINE INDEX n ON guarded FIELDS n UNIQUE;
	DEFINE INDEX m ON guarded FIELDS m UNIQUE;
";

/// For a record user, a table nothing governs keeps the conditional. A table whose permission or
/// field permission is enforced when its rows are read is read as written: the permission
/// predicate runs for every row the read meets, and a probe that stops after the first rows would
/// not run it for the rest.
#[tokio::test]
async fn an_extreme_for_a_record_user_declines_what_the_read_enforces() {
	for sql in [
		"SELECT math::max(n) AS m FROM open GROUP ALL",
		"SELECT math::min(n) AS m FROM open GROUP ALL",
		"RETURN (SELECT VALUE math::max(n) FROM open GROUP ALL)[0]",
	] {
		let text = planned_text_as_user(EXTREME_PERMISSIONS, sql)
			.await
			.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
		assert!(extreme_by_index(&text), "{sql}\n{text}");
	}
	for sql in [
		"SELECT math::max(n) AS m FROM pub GROUP ALL",
		"SELECT math::min(n) AS m FROM pub GROUP ALL",
		"SELECT math::max(n) AS m FROM denied GROUP ALL",
		"SELECT math::max(n) AS m FROM owned GROUP ALL",
		"SELECT math::max(n) AS m FROM guarded GROUP ALL",
		"SELECT math::max(m) AS m FROM guarded GROUP ALL",
		"RETURN (SELECT VALUE math::max(n) FROM pub GROUP ALL)[0]",
	] {
		let text = planned_text_as_user(EXTREME_PERMISSIONS, sql)
			.await
			.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
		assert!(text.contains("phys.enforced"), "{sql}\n{text}");
		assert!(!text.contains("-Infinity"), "{sql}\n{text}");
		assert!(text.matches("phys.aggregate {").count() == 1, "{sql}\n{text}");
	}
}

/// Every spelling of an extreme the language tests run plans and verifies, in value position,
/// paged, over an empty table, over a field no index serves, over a field that is not an `int`,
/// with a `WHERE`, with two columns, grouped, under `WITH NOINDEX` and explained.
#[tokio::test]
async fn the_extreme_statements_plan() {
	for sql in [
		"SELECT math::max(n) AS m FROM t GROUP ALL",
		"SELECT math::min(n) AS m FROM t GROUP ALL",
		"SELECT math::max(n) FROM t GROUP ALL",
		"SELECT VALUE math::min(n) FROM t GROUP ALL",
		"SELECT math::max(n) AS a.b FROM t GROUP ALL",
		"RETURN (SELECT VALUE math::max(n) FROM t GROUP ALL)[0]",
		"SELECT id, (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] AS m FROM t",
		"SELECT id, (SELECT math::min(n) AS lo FROM t GROUP ALL) AS m FROM t",
		"SELECT VALUE id FROM t WHERE n = (SELECT VALUE math::max(n) FROM t GROUP ALL)[0]",
		"SELECT math::max(n) AS m FROM t GROUP ALL LIMIT 1 START 0",
		"SELECT math::max(n) AS m FROM t GROUP ALL START 1",
		"SELECT math::min(n) AS m FROM t GROUP ALL LIMIT 1",
		"SELECT VALUE math::max(n) FROM ONLY t GROUP ALL",
		"SELECT id, (SELECT VALUE math::min(n) FROM t GROUP ALL)[0] AS m FROM t ORDER BY id",
		"SELECT id FROM t WHERE n = (SELECT VALUE math::min(n) FROM t GROUP ALL)[0] ORDER BY id",
		"SELECT math::max(n) AS m FROM ONLY t GROUP ALL",
		"SELECT math::max(n) AS m FROM empty GROUP ALL",
		"RETURN (SELECT VALUE math::min(n) FROM empty GROUP ALL)[0]",
		"SELECT math::max(n) AS m FROM t WHERE true GROUP ALL",
		"SELECT math::max(n + 0) AS m FROM t GROUP ALL",
		"SELECT math::max(n) AS m, count() AS c FROM t GROUP ALL",
		"SELECT n, math::max(n) AS m FROM t GROUP BY n",
		"SELECT math::max(n.x) AS m FROM t GROUP ALL",
		"SELECT math::max(n) AS m FROM loose GROUP ALL",
		"SELECT math::min(n) AS m FROM plain GROUP ALL",
		"SELECT math::max(n) AS m FROM t WITH NOINDEX GROUP ALL",
		"EXPLAIN SELECT math::max(n) AS m FROM t GROUP ALL",
		"EXPLAIN SELECT math::min(n) AS m FROM t GROUP ALL",
		"EXPLAIN SELECT VALUE math::max(n) FROM t GROUP ALL",
	] {
		planned_text(EXTREME, sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
	}
}

/// The printed ops of `text` whose op name starts with `head`, with the value ids erased, so that
/// the layout of an op (its operand count, its attributes) can be compared between two plans.
fn layout_of(text: &str, head: &str) -> Vec<String> {
	text.lines()
		.map(str::trim)
		.filter_map(|line| line.split_once(" = ").map_or(Some(line), |(_, op)| Some(op)))
		.filter(|op| op.starts_with(head))
		.map(|op| {
			let mut out = String::new();
			let mut chars = op.chars().peekable();
			while let Some(c) = chars.next() {
				out.push(c);
				if c == '%' {
					while chars.peek().is_some_and(char::is_ascii_digit) {
						chars.next();
					}
				}
			}
			out
		})
		.collect()
}

/// The ops of the clause of the first printed op of `text` whose name starts with `head`, up to
/// the op that follows the clause, with the value ids and the facts erased and the literals and
/// negations left out, so that the shape of a predicate can be compared between two plans.
fn clause_of(text: &str, head: &str) -> Vec<String> {
	let mut lines = text
		.lines()
		.map(str::trim)
		.skip_while(|line| !line.split_once(" = ").map_or(*line, |(_, op)| op).starts_with(head));
	lines.next();
	lines
		.take_while(|line| !line.starts_with("phys.yield"))
		.map(|line| line.split_once(" : fact(").map_or(line, |(op, _)| op))
		.map(|line| line.split_once(" = ").map_or(line, |(_, op)| op))
		.filter(|op| !op.starts_with("expr.literal") && !op.starts_with("expr.prefix"))
		.map(|op| {
			let mut out = String::new();
			let mut chars = op.chars().peekable();
			while let Some(c) = chars.next() {
				out.push(c);
				if c == '%' {
					while chars.peek().is_some_and(char::is_ascii_digit) {
						chars.next();
					}
				}
			}
			out
		})
		.collect()
}

/// The part of the printed index scan of `text` that states the order it delivers.
fn order_of(text: &str) -> Vec<String> {
	text.lines()
		.filter(|line| line.contains("phys.index_scan {"))
		.filter_map(|line| {
			let from = line.find("direction:")?;
			let to = line.find("rechecked:")?;
			Some(line[from..to].to_owned())
		})
		.collect()
}

/// The ops the edits before the conversion make are laid out as the binder lays them out for the
/// written statement: the limit of the existence short-circuit is the limit of a written `LIMIT 1`,
/// and the probe of an extreme reads, filters, limits and projects as the written probe does, with
/// its sort served by the index in the same direction. A change to the binder's layout of one of
/// these ops that the edits do not follow fails here.
#[tokio::test]
async fn the_ops_an_edit_makes_are_laid_out_as_the_binder_lays_them_out() {
	let edited = planned_text(EXISTENCE, "SELECT * FROM small WHERE (SELECT id FROM big)")
		.await
		.expect("the statement plans");
	let written = planned_text(EXISTENCE, "SELECT * FROM small WHERE (SELECT id FROM big LIMIT 1)")
		.await
		.expect("the statement plans");
	let limit = layout_of(&written, "phys.limit");
	assert_eq!(limit.len(), 1, "{written}");
	assert_eq!(layout_of(&edited, "phys.limit"), limit, "{edited}");

	for (function, order) in [("max", "DESC"), ("min", "ASC")] {
		let edited =
			planned_text(EXTREME, &format!("SELECT math::{function}(n) AS m FROM t GROUP ALL"))
				.await
				.expect("the statement plans");
		let written = planned_text(
			EXTREME,
			&format!(
				"SELECT VALUE n FROM t WHERE n >= -Infinity AND n <= NaN ORDER BY n {order} LIMIT 2"
			),
		)
		.await
		.expect("the statement plans");
		for head in ["phys.limit(", "phys.project("] {
			let [expected] =
				layout_of(&written, head).try_into().expect("one op of the written probe");
			let made = layout_of(&edited, head);
			assert!(!made.is_empty(), "{head}\n{edited}");
			// The probe is the first of the ops the edit made: the statement it guards limits too.
			assert_eq!(made[0], expected, "{head}\n{edited}");
		}
		assert_eq!(layout_of(&written, "phys.sort").len(), 0, "{written}");
		assert_eq!(layout_of(&edited, "phys.sort").len(), 0, "{edited}");
		let served = order_of(&written);
		assert_eq!(served.len(), 1, "{written}");
		assert!(order_of(&edited).iter().all(|made| *made == served[0]), "{edited}");
		// The clause of the probe's filter holds the ops of the written `WHERE`, in its order: the
		// two comparisons joined by a short-circuit `AND`. The literals differ by construction
		// (the edit states `-Infinity` as a literal, the written text as a negation).
		let made = clause_of(&edited, "phys.filter");
		assert!(!made.is_empty(), "{edited}");
		assert_eq!(made, clause_of(&written, "phys.filter"), "{edited}");
	}
}

/// The strategies of the statements in value position `sql` plans to over a database `setup`
/// builds, outermost first.
async fn strategies(setup: &str, sql: &str) -> Vec<super::expr::ops::NestedStrategy> {
	let parsed = crate::syn::parse(sql).expect("the statement parses").expressions;
	let statements = RequestStatement::of(&parsed);
	let planned = plan(setup, &RequestInput::request(&statements, request_scope()))
		.await
		.expect("the statement plans");
	let mut strategies = Vec::new();
	planned.ir.walk(planned.ir.top(), WalkOrder::PreOrder, &mut |op| {
		if let Some(OpKind::Expr(ExprOp::Subquery(strategy))) =
			planned.ir.op(op).map(|data| &data.kind)
		{
			strategies.push(strategy.clone());
		}
		Walk::Continue
	});
	strategies
}

/// A statement in value position that reads nothing of the row it is evaluated for, over an
/// indexed table, runs once whatever reads its value and whatever the enclosing statement orders,
/// limits or filters by, an order at random included: its one evaluation answers every row.
#[tokio::test]
async fn an_uncorrelated_statement_in_value_position_runs_once() {
	use super::expr::ops::NestedStrategy;

	const RECORDS: &str = "
		DEFINE TABLE record SCHEMALESS;
		DEFINE FIELD number ON record TYPE int;
		DEFINE INDEX record_number ON record FIELDS number;
	";
	for sql in [
		"SELECT id, (SELECT VALUE math::max(number) FROM record GROUP ALL)[0] AS m FROM record LIMIT 100",
		"SELECT id, (SELECT VALUE math::max(number) FROM record GROUP ALL) AS m FROM record LIMIT 100",
		"SELECT id, (SELECT VALUE number FROM record LIMIT 1)[0] AS m FROM record LIMIT 100",
		"SELECT id, (SELECT count() FROM record GROUP ALL) AS m FROM record LIMIT 100",
		"SELECT id, (SELECT VALUE math::sum(number) FROM record GROUP ALL)[0] AS m FROM record LIMIT 100",
		"SELECT id, (SELECT VALUE math::max(number) FROM record GROUP ALL)[0] AS m FROM record",
		"SELECT id, (SELECT VALUE math::max(number) FROM record GROUP ALL)[0] AS m FROM record ORDER BY id LIMIT 100",
		"SELECT id FROM record WHERE number = (SELECT VALUE math::max(number) FROM record GROUP ALL)[0]",
	] {
		assert_eq!(strategies(RECORDS, sql).await, vec![NestedStrategy::Once], "{sql}");
	}
	let sql =
		"SELECT id, (SELECT VALUE number FROM record ORDER BY RAND() LIMIT 1)[0] AS m FROM record";
	assert_eq!(strategies(RECORDS, sql).await, vec![NestedStrategy::Once], "{sql}");
}

/// The extreme of a uniquely indexed int field in value position, read beside every row of an
/// enclosing statement, is answered by a probe of the index and a conditional over two copies of
/// the statement, and each of those statements reads nothing of the row: every one runs once.
#[tokio::test]
async fn the_extreme_of_a_unique_index_in_value_position_runs_once() {
	use super::expr::ops::NestedStrategy;

	const RECORDS: &str = "
		DEFINE TABLE record SCHEMALESS;
		DEFINE FIELD integer ON record TYPE int;
		DEFINE INDEX record_integer ON record FIELDS integer UNIQUE;
	";
	for sql in [
		"SELECT id, (SELECT VALUE math::max(integer) FROM record GROUP ALL)[0] AS m FROM record LIMIT 100",
		"SELECT id, (SELECT VALUE math::min(integer) FROM record GROUP ALL)[0] AS m FROM record LIMIT 100",
	] {
		let made = strategies(RECORDS, sql).await;
		assert!(made.len() > 1, "the extreme is answered from its index: {sql}: {made:?}");
		assert!(made.iter().all(|strategy| *strategy == NestedStrategy::Once), "{sql}: {made:?}");
	}
}

/// A window binds over a sort of its rows on its partition keys, ascending, then its order keys:
/// the sort converts to `phys.sort` and the window to `phys.window` above it, which hosts one key
/// region per partition and order key. Nothing of the window moves below it, and no `LIMIT` above
/// it bounds the sort beneath.
#[tokio::test]
async fn a_window_binds_over_a_sort_of_its_keys() {
	use crate::expr::logical::{
		LogicalOp, LogicalOrdering, LogicalWindow, LogicalWindowCall, WindowFrame,
		WindowFrameBound, WindowFrameUnits, WindowFunction,
	};
	let parsed = crate::syn::parse("SELECT n, w FROM t ORDER BY s, n LIMIT 2")
		.expect("the statement parses")
		.expressions;
	let [TopLevelExpr::Expr(expr)] = parsed.as_slice() else {
		panic!("one expression statement");
	};
	let mut op = lower(expr.clone()).expect("the statement has a logical form");
	// The window takes the place of the ordering, keyed by its two keys, and the limit stays above
	// it.
	let LogicalOp::Project(project) = &mut op else {
		panic!("a projection: {op:?}");
	};
	let LogicalOp::Limit(limit) = &mut *project.input else {
		panic!("a limit: {:?}", project.input);
	};
	let LogicalOp::Sort(sort) = &*limit.input else {
		panic!("a sort: {:?}", limit.input);
	};
	let LogicalOrdering::Order(keys) = &sort.ordering else {
		panic!("an ordering by keys");
	};
	let [partition, order] = keys.as_slice() else {
		panic!("two keys");
	};
	let window = LogicalOp::Window(LogicalWindow {
		input: sort.input.clone(),
		partition: vec![partition.value.clone()],
		order: vec![order.clone()],
		calls: vec![LogicalWindowCall {
			output: "w".into(),
			function: WindowFunction::Rank,
			args: Vec::new(),
			frame: WindowFrame {
				units: WindowFrameUnits::Range,
				start: WindowFrameBound::UnboundedPreceding,
				end: WindowFrameBound::CurrentRow,
			},
		}],
	});
	*limit.input = window;
	let statements = [RequestStatement::Logical(&op)];
	let input = RequestInput::request(&statements, request_scope());
	let planned = plan("DEFINE TABLE t SCHEMALESS;", &input).await.expect("the statement plans");
	let text = print(&planned.ir, planned.ir.top(), PrintOpts::default());
	let sort = text.find("phys.sort").unwrap_or_else(|| panic!("a sort: {text}"));
	let window = text.find("phys.window").unwrap_or_else(|| panic!("a window: {text}"));
	assert!(sort < window, "the sort feeds the window: {text}");
	let mut sorts = Vec::new();
	planned.ir.walk(planned.ir.top(), WalkOrder::PreOrder, &mut |at| {
		if let Some(data) = planned.ir.op(at)
			&& let OpKind::Phys(super::phys::ops::PhysOp::Sort(attrs)) = &data.kind
		{
			sorts.push(attrs.keep);
		}
		Walk::Continue
	});
	assert_eq!(sorts, vec![None], "the limit above the window bounds no sort: {text}");
}
