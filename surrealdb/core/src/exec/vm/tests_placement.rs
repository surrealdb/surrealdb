//! The placement of every pipeline of the statements below, as the planner decides it and as the
//! compiled program runs it; the placement of each pipeline stays within what the logical plan
//! allows; and the policy declines each fact it must decline on its own.
//!
//! A statement is planned through [`plan_request`], whose container holds the placement of each of
//! its pipelines, and compiled through the production compile, whose program holds the placement
//! the VM runs: the planner's decision narrowed by the code lowering emitted.

use super::compile::compile;
use super::golden::Golden;
use super::morsel::{effective_input, effective_mode};
use super::program::{Pipeline, Program};
use super::tests_morsel::{
	FanOut, answered, bind, fans_out, planner, tiny, try_answered_fanning_out, viewless, whole,
};
use crate::exec::ExecutionContext;
use crate::exec::ir::core::ir::{IrScope, Walk, WalkOrder};
use crate::exec::ir::iface::catalog::BackendCaps;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::ir::phys::placement::{
	admission_gap, legality_inputs, merge_never_stronger, never_stronger,
};
use crate::exec::ir::phys::placement_attr::PlacementAttr;
use crate::exec::ir::{
	PlannedRequest, PlanningInput, RequestInput, RequestStatement, plan_request,
};
use crate::exec::logical::pipelines::{
	AggregateCombine, Arrival, ChainLegality, ErrorOrder, Legality, Paging, SinkCombine,
	SourceSplit, StatementLegality,
};
use crate::exec::operators::test_util::TestDb;
use crate::exec::planner::placement::{
	Combine, Decision, MergeOrder, MorselInput, MorselPlan, NestedUse, Parallel, PhysicalFacts,
	Policy, SinkKind, SourceKind,
};
use crate::expr::logical::{
	EvalFrame, JoinKind, LogicalBreak, LogicalDistinct, LogicalExpr, LogicalJoin, LogicalOp,
	RelationAlias,
};

/// The schema every corpus statement is bound against.
const SCHEMA: &str = "
	DEFINE TABLE person SCHEMAFULL;
	DEFINE FIELD name ON person TYPE string;
	DEFINE FIELD age ON person TYPE int;
	DEFINE FIELD city ON person TYPE option<string>;
	DEFINE INDEX person_age ON person FIELDS age;
	DEFINE TABLE company SCHEMAFULL;
	DEFINE FIELD name ON company TYPE string;
	DEFINE TABLE works_at TYPE RELATION IN person OUT company;
";

/// The statements whose placement the corpus goldens pin.
const CORPUS: &[&str] = &[
	// sources and projection
	"SELECT * FROM person",
	"SELECT name FROM person",
	"SELECT name, age FROM person",
	"SELECT * FROM person:tobie",
	// predicates the rules are expected to rewrite
	"SELECT * FROM person WHERE age > 18",
	"SELECT * FROM person WHERE age > 18 AND age > 3",
	"SELECT * FROM person WHERE true",
	"SELECT * FROM person WHERE false",
	"SELECT * FROM person WHERE age > 18 AND city = 'SF'",
	"SELECT * FROM person WHERE age > 18 OR age < 5",
	"SELECT * FROM person WHERE NOT (age > 18 AND city = 'SF')",
	"SELECT * FROM person WHERE age > 1 + 2",
	"SELECT * FROM person WHERE city = NONE",
	// ordering, limits, grouping
	"SELECT * FROM person ORDER BY age",
	"SELECT * FROM person ORDER BY age LIMIT 10",
	"SELECT * FROM person LIMIT 10 START 5",
	"SELECT count() FROM person GROUP ALL",
	"SELECT city, count() FROM person GROUP BY city",
	// subqueries and bindings
	"SELECT * FROM person WHERE id IN (SELECT VALUE id FROM person WHERE age > 18)",
	"SELECT (SELECT VALUE name FROM company) AS names FROM person",
	// graph
	"SELECT ->works_at->company FROM person",
	"SELECT * FROM person WHERE ->works_at->company",
	// effects that must survive rewriting unchanged
	"CREATE person:tobie SET name = 'Tobie', age = 30",
	"UPDATE person:tobie SET age = 31",
	"UPDATE person:tobie SET age = age + 1",
	"DELETE person:tobie",
	"SELECT id, (CREATE log:1) AS made FROM person",
	// subqueries in a SET clause. The first may be evaluated once, because nothing
	// the statement writes reaches `company`. The other three must be evaluated
	// per row.
	"UPDATE person SET city = (SELECT VALUE name FROM company)",
	"UPDATE person SET city = (SELECT VALUE name FROM company WHERE name = $this)",
	"UPDATE person SET city = (CREATE log:1).id",
	"UPDATE person SET city = (SELECT VALUE rand::uuid() FROM company)",
	// mock record sets, in the two positions that reach them
	"CREATE |person:3|",
	"CREATE |person:1..4|",
	"SELECT * FROM |person:3|",
	"RETURN |person:3|",
	// control flow the rules collapse
	"RETURN IF true THEN 1 ELSE 2 END",
	"RETURN IF false THEN (CREATE log:1) ELSE 2 END",
	"RETURN IF $c THEN 1 ELSE IF true THEN 2 ELSE 3 END",
	"SELECT * FROM person WHERE age > rand::int(1, 10)",
	"SELECT * FROM person WHERE age > time::now()",
];

/// A table of 300 rows with an index, a full-text index, a vector index, a computed field and a
/// relation.
const REGION_SETUP: &str = "
	DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
	DEFINE TABLE person SCHEMALESS;
	DEFINE FIELD twice ON person COMPUTED age * 2;
	DEFINE TABLE company SCHEMALESS;
	DEFINE TABLE works_at TYPE RELATION IN person OUT company;
	DEFINE INDEX by_age ON person FIELDS age;
	DEFINE INDEX by_name ON person FIELDS name FULLTEXT ANALYZER simple BM25;
	DEFINE INDEX by_emb ON person FIELDS emb HNSW DIMENSION 3 DISTANCE EUCLIDEAN;
	CREATE company:1 SET name = 'acme';
	FOR $i IN 0..300 {
		CREATE type::record('person', $i) SET
			name = 'name ' + <string> $i + ' a',
			age = $i % 40,
			city = ['x', 'y', 'z'][$i % 3],
			company = company:1,
			emb = [$i, $i % 7, $i % 5]
	};
	RELATE person:1->works_at->company:1;
";

/// Statements shaped for the admission of a region, over [`REGION_SETUP`].
const REGION_SHAPED: &[&str] = &[
	"SELECT * FROM person",
	"SELECT name, age FROM person WHERE age > 10",
	"SELECT ->works_at->company FROM person",
	"SELECT *, (SELECT VALUE name FROM company) AS c FROM person",
	"SELECT * FROM person FETCH company",
	"SELECT * FROM person WHERE name @@ 'a'",
	"SELECT * FROM person WHERE emb <|3, 40|> [1, 2, 3]",
	"SELECT math::sum(age) FROM person GROUP ALL",
	"SELECT city, count() FROM person GROUP BY city",
	"SELECT * FROM person LIMIT 5 START 2",
	"SELECT * FROM person ORDER BY age LIMIT 3",
	"SELECT time::now() FROM person",
	"SELECT rand() FROM person",
	"SELECT * FROM (SELECT * FROM person LIMIT 50) LIMIT 5",
	// Rows that decode through the computed field, so no body runs in the storage cursor:
	// an aggregate whose partial states combine exactly, and one that adds floats.
	"SELECT city, math::max(twice) AS top FROM person GROUP BY city",
	"SELECT city, math::sum(twice) AS total FROM person GROUP BY city",
	"SELECT twice FROM person WHERE twice > 10 LIMIT 7",
	"SELECT twice FROM person LIMIT 7",
];

/// Reads as GQL words them. A GQL `DISTINCT` is the match plan's own stage and lowers to no
/// distinct set of a pipeline.
#[cfg(feature = "gql")]
const GQL_SHAPED: &[&str] = &[
	"MATCH (n:person) RETURN DISTINCT n.city AS city ORDER BY city",
	"MATCH (n:person) RETURN n.name AS name SKIP 1 LIMIT 2",
	"MATCH (n:person) RETURN n.city AS city, count(*) AS c GROUP BY n.city ORDER BY city",
];

/// Statements whose physical plan reads a source the logical plan cannot see: a count the plan
/// answers from the count index or the key range, a membership test it probes, a mock set it
/// reads as values. Their physical pipelines may differ in number and in legality from the
/// logical plan's, and plan no more pipelines as batches than the logical plan allows to split.
const PHYSICAL_STRATEGIES: &[&str] = &[
	"SELECT count() FROM person GROUP ALL",
	"SELECT * FROM person WHERE id IN (SELECT VALUE id FROM person WHERE age > 18)",
	"SELECT * FROM |person:3|",
];

/// What the statements of [`FINAL_SHAPED`] and [`FINAL_WRITES`] read beyond [`REGION_SETUP`]: a
/// sequence, two tables joined on `k` (`r` with an index on it, `u` without) and a function whose
/// body scans a table.
const FINAL_SETUP: &str = "
	DEFINE SEQUENCE s;
	DEFINE TABLE l SCHEMALESS;
	DEFINE TABLE u SCHEMALESS;
	DEFINE TABLE r SCHEMALESS;
	DEFINE INDEX r_k ON r FIELDS k;
	DEFINE FUNCTION fn::scan() { RETURN (SELECT VALUE name FROM company) };
	FOR $i IN 0..30 {
		CREATE type::record('l', $i) SET k = $i % 10;
		CREATE type::record('u', $i) SET k = $i % 12, x = $i;
		CREATE type::record('r', $i) SET k = $i % 12, x = $i
	};
";

/// Reads whose placement the final golden pins where an advancing projection meets a breaker, a
/// probe, a source resolved when the statement is planned, and a nested statement.
const FINAL_SHAPED: &[&str] = &[
	"SELECT *, sequence::nextval('s') AS n FROM person ORDER BY age LIMIT 3",
	"SELECT sequence::nextval('s') FROM person WHERE emb <|3,40|> [1,2,3]",
	"SELECT * FROM type::table($t)",
	"SELECT id, (SELECT VALUE x FROM u WHERE k = $parent.k) AS xs FROM l",
	"SELECT id, (SELECT VALUE x FROM r WHERE k = $parent.k) AS xs FROM l",
	"SELECT * FROM person LIMIT rand::int(1, 5)",
	"SELECT *, (SELECT VALUE name FROM company) AS c FROM person LIMIT rand::int(1, 5)",
];

/// Statements that write, or that run a unit beside a write: every pipeline of each lowers beside
/// the statement's writes.
const FINAL_WRITES: &[&str] = &[
	"UPDATE person SET c = (SELECT VALUE count() FROM company GROUP ALL)",
	"UPDATE person SET city = (SELECT VALUE name FROM company)",
	"UPDATE person SET c = fn::scan()",
	"UPDATE l SET xs = (SELECT VALUE x FROM r WHERE k = $parent.k)",
	"{ CREATE a:1; SELECT * FROM person; }",
];

/// Two patterns that share a binding, which the match plan joins.
#[cfg(feature = "gql")]
const BINDING_JOIN: &str = "MATCH (a:person)-[:works_at]->(c:company), \
	 (b:person)-[:works_at]->(c:company) RETURN a.name AS a, b.name AS b";

/// The tables joined by hand: SurrealQL has no `JOIN`, and the SQL front ends that emit one are
/// not default features.
const FINAL_JOINS: &[(&str, JoinKind, &str, &str)] = &[
	("SELECT * FROM l JOIN u ON l.k = u.k", JoinKind::Inner, "l", "u"),
	("SELECT * FROM l LEFT JOIN r ON l.k = r.k", JoinKind::Left, "l", "r"),
];

/// `SELECT * FROM {left}` with its source replaced by `left` joined to `right` on their field
/// `k`, as the SQL front ends build a join.
async fn joined(ctx: &ExecutionContext, kind: JoinKind, left: &str, right: &str) -> LogicalOp {
	let source = |op: LogicalOp| {
		if let LogicalOp::Project(project) = op {
			*project.input
		} else {
			op
		}
	};
	let right = source(bind(ctx, &format!("SELECT * FROM {right}")).await);
	let mut statement = bind(ctx, &format!("SELECT * FROM {left}")).await;
	let LogicalOp::Project(project) = &mut statement else {
		panic!("`SELECT * FROM {left}` is not a projection");
	};
	let left = std::mem::replace(&mut *project.input, LogicalOp::Break(LogicalBreak));
	let alias = RelationAlias::new(0);
	*project.input = LogicalOp::Join(LogicalJoin::new(
		left,
		right,
		kind,
		vec![(LogicalExpr::Field("k".into()), alias.access("k"))],
		None,
		alias,
		EvalFrame::caller(),
	));
	statement
}

/// `sql`'s plan with a distinct set between its projection and its rows: a node of the plan that
/// no SurrealQL statement writes.
async fn distinct_of(ctx: &ExecutionContext, sql: &str) -> LogicalOp {
	let mut distinct = bind(ctx, sql).await;
	let rows = if let LogicalOp::Project(project) = &mut distinct {
		&mut *project.input
	} else {
		&mut distinct
	};
	let input = std::mem::replace(rows, LogicalOp::Break(LogicalBreak));
	*rows = LogicalOp::Distinct(LogicalDistinct {
		input: Box::new(input),
	});
	distinct
}

/// The reason the planner left the pipeline `op` of `planned` serial, or `None` where it runs as
/// morsels. A pipeline whose own judgement admits it, and that still runs serially, was narrowed
/// by the pipeline around it or by the backend.
fn refusal_of(
	planned: &mut PlannedRequest,
	op: crate::exec::ir::iface::ids::OpId,
) -> Option<String> {
	let attr = planned.ir.placement(op)?.clone();
	if !matches!(attr.decision(), Some(Decision::Serial)) {
		return None;
	}
	let (legality, statement, loop_facts) = legality_inputs(&planned.ir, &planned.facts, op);
	let facts = attr.facts()?;
	let judged = match &loop_facts {
		Some(loop_facts) => {
			let cut =
				crate::exec::ir::phys::write_cut::write_cut_of(&planned.ir, Some(loop_facts), op);
			Policy::judge_loop(
				&legality,
				statement,
				loop_facts,
				facts,
				cut.as_ref().map_err(|reason| *reason),
			)
		}
		None => Policy::judge(&legality, statement, facts),
	};
	Some(match judged {
		Err(reason) => reason.to_owned(),
		Ok(_) => "narrowed by the pipeline around it or by the backend".to_owned(),
	})
}

/// The placement the planner decided for one pipeline, and why it runs serially where it does.
pub(super) struct Decided {
	pub(super) attr: PlacementAttr,
	pub(super) refusal: Option<String>,
	pub(super) loop_facts: Option<crate::exec::ir::iface::policy::LoopFacts>,
}

/// The request `op` plans to, or `None` where the plan is declined.
async fn planned_of(ctx: &ExecutionContext, op: &LogicalOp) -> Option<PlannedRequest> {
	// The container is planned alone, with no compile to link a write's stages, so each write
	// binds its stages as ops of its own.
	let planner = planner(ctx).inlining_stages();
	let session = super::compile::session_of(&planner);
	let statements = [RequestStatement::Logical(op)];
	let input = RequestInput::request(
		&statements,
		IrScope::Request {
			fragment: None,
		},
	);
	let planning = PlanningInput {
		planner: &planner,
		capabilities: planner.frozen().get_capabilities(),
		backend: BackendCaps::default(),
		held: Vec::new(),
		session: &session,
	};
	plan_request(&input, &planning).await.ok()
}

/// The stages of each write loop the container `op` plans to holds, in the order of the loop's
/// body, each with the half the plan marks it as.
pub(super) async fn write_stages(
	ctx: &ExecutionContext,
	op: &LogicalOp,
) -> Vec<
	Vec<(crate::exec::ir::logical::ops::WriteStageKind, crate::exec::ir::logical::ops::WriteMark)>,
> {
	use crate::exec::ir::core::walk::ops_in;
	use crate::exec::ir::logical::ops::LoopKind;
	let planned = planned_of(ctx, op).await.expect("the statement plans");
	let mut loops = Vec::new();
	planned.ir.walk(planned.ir.top(), WalkOrder::PreOrder, &mut |op| {
		if matches!(
			planned.ir.op(op).map(|data| &data.kind),
			Some(OpKind::Phys(PhysOp::Loop {
				kind: LoopKind::Write,
				..
			}))
		) {
			loops.push(op);
		}
		Walk::Continue
	});
	loops
		.into_iter()
		.map(|loop_op| {
			let body = planned.ir.op(loop_op).and_then(|data| data.regions.first().copied());
			body.map(|body| {
				ops_in(&planned.ir, body)
					.filter_map(|op| match &planned.ir.op(op)?.kind {
						OpKind::Phys(PhysOp::WriteStage {
							stage,
							mark,
						}) => Some((*stage, *mark)),
						OpKind::Phys(_) | OpKind::Logical(_) | OpKind::Expr(_) => None,
					})
					.collect()
			})
			.unwrap_or_default()
		})
		.collect()
}

/// The pipelines of the container `sql` plans to, or `None` where the plan is declined.
pub(super) async fn decided(ctx: &ExecutionContext, op: &LogicalOp) -> Option<Vec<Decided>> {
	let mut planned = planned_of(ctx, op).await?;
	let mut pipelines = Vec::new();
	planned.ir.walk(planned.ir.top(), WalkOrder::PreOrder, &mut |op| {
		if matches!(
			planned.ir.op(op).map(|data| &data.kind),
			Some(OpKind::Phys(PhysOp::Pipeline(_)))
		) {
			pipelines.push(op);
		}
		Walk::Continue
	});
	let mut out = Vec::new();
	for op in pipelines {
		let attr = planned.ir.placement(op).cloned().expect("every pipeline is placed");
		let refusal = refusal_of(&mut planned, op);
		let (.., loop_facts) = legality_inputs(&planned.ir, &planned.facts, op);
		out.push(Decided {
			attr,
			refusal,
			loop_facts,
		});
	}
	Some(out)
}

/// The program `op` compiles to, or `None` where the VM declines it.
async fn compiled(ctx: &ExecutionContext, op: &LogicalOp) -> Option<Program> {
	Box::pin(compile(&planner(ctx), op)).await.ok().map(|compiled| compiled.program)
}

/// A statement of the corpus planned and compiled.
struct Statement {
	sql: String,
	/// The program the statement compiles to; `None` where the VM declines it.
	program: Option<Program>,
	/// The placement of each pipeline of the planned container, in the container's order.
	decided: Vec<Decided>,
}

/// `op` planned and compiled over `ctx`, recorded under `sql`.
async fn statement(ctx: &ExecutionContext, sql: &str, op: &LogicalOp) -> Statement {
	Statement {
		sql: sql.to_owned(),
		program: compiled(ctx, op).await,
		decided: decided(ctx, op).await.unwrap_or_default(),
	}
}

/// The statements of `statements`, bound and planned over `ctx`.
async fn statements_of(ctx: &ExecutionContext, statements: &[&str]) -> Vec<Statement> {
	let mut out = Vec::new();
	for sql in statements {
		out.push(statement(ctx, sql, &bind(ctx, sql).await).await);
	}
	out
}

/// The GQL `queries` over `db`, planned and compiled.
#[cfg(feature = "gql")]
async fn gql_statements_of(
	db: &TestDb,
	ctx: &ExecutionContext,
	queries: &[&str],
) -> Vec<Statement> {
	let mut out = Vec::new();
	for query in queries {
		let plan = crate::gql::parse_with_capabilities(
			query,
			&crate::dbs::Capabilities::all(),
			&db.datastore().parser_config(),
		)
		.unwrap_or_else(|e| panic!("`{query}` does not parse: {e:?}"))
		.0;
		out.push(statement(ctx, query, &plan).await);
	}
	out
}

/// One word for a pipeline's placement: `serial`, `batches` or `range`, and `in place` where its
/// body runs inside the storage cursor. The word is the mode the VM runs, which the body narrows
/// the decision to ([`effective_mode`]).
fn mode_word(parallel: &Parallel, in_place: bool) -> String {
	let how = match parallel {
		Parallel::Serial => "serial",
		Parallel::Morsels(plan) => match effective_mode(plan.input, in_place) {
			MorselInput::Batches => "batches",
			MorselInput::Range => "range",
		},
	};
	format!(
		"{how}{}",
		if in_place {
			" in place"
		} else {
			""
		}
	)
}

/// The pipelines of `program` and, recursively, of every program in its units, with the path of
/// the unit that holds each: `root[1]`, `root/2[0]`.
fn pipelines_of<'p>(program: &'p Program, path: &str, out: &mut Vec<(String, &'p Pipeline)>) {
	for (index, pipeline) in program.pipelines.iter().enumerate() {
		out.push((format!("{path}[{index}]"), pipeline));
	}
	for (unit, nested) in program.units.iter().enumerate() {
		pipelines_of(nested, &format!("{path}/{unit}"), out);
	}
}

/// What a statement is recorded as when the VM declines it.
const DECLINED: &str = "declined";

/// The planner's placement of every pipeline of a statement, as the placement golden records it:
/// the mode each pipeline of the compiled program runs in, and the reason each pipeline of the
/// planned container that runs serially does.
fn placement_of(statement: &Statement) -> Option<String> {
	let program = statement.program.as_ref()?;
	let mut pipelines = Vec::new();
	pipelines_of(program, "root", &mut pipelines);
	let modes: Vec<String> = pipelines
		.iter()
		.map(|(_, pipeline)| mode_word(&pipeline.parallel, pipeline.in_place))
		.collect();
	let mut refusals: Vec<&str> = Vec::new();
	for decided in &statement.decided {
		if let Some(reason) = &decided.refusal
			&& !refusals.contains(&reason.as_str())
		{
			refusals.push(reason);
		}
	}
	let mut body = modes.join(", ");
	for reason in refusals {
		body.push_str("\n  refused: ");
		body.push_str(reason);
	}
	Some(body)
}

/// Every pipeline of every statement the planner places as morsels is one the logical plan lists
/// and allows to split, and none is stronger than the logical plan states.
fn assert_within_legality(statements: &[Statement]) {
	for Statement {
		sql,
		decided,
		..
	} in statements
	{
		for Decided {
			attr,
			loop_facts,
			..
		} in decided
		{
			let decision = attr.decision().expect("every pipeline is placed");
			if matches!(decision, Decision::Morsels { .. }) {
				assert!(
					attr.legality().may_split(),
					"`{sql}` runs as morsels, which its legality refuses: {:?} over {:?}",
					attr.legality(),
					attr.facts()
				);
			}
			assert!(
				never_stronger(attr.legality(), decision).is_ok(),
				"`{sql}`: {decision:?} is stronger than {:?}",
				attr.legality()
			);
			assert!(
				merge_never_stronger(attr.legality(), attr.merge()).is_ok(),
				"`{sql}`: merges as {:?}, which is stronger than {:?}",
				attr.merge(),
				attr.legality()
			);
			assert_eq!(admission_gap(attr, loop_facts.as_ref()), None, "`{sql}`");
		}
	}
}

/// The statements of the corpora, planned and compiled over their own databases.
struct Corpus {
	all: Vec<Statement>,
	_dbs: Vec<TestDb>,
}

/// Plan and compile the statements of [`CORPUS`], [`REGION_SHAPED`], [`GQL_SHAPED`] and the
/// synthetic `DISTINCT`.
async fn planned_corpus() -> Corpus {
	let corpus = TestDb::new_with_config(SCHEMA, viewless()).await;
	let corpus_ctx = corpus.exec_ctx().await;
	let shaped = TestDb::new_with_config(REGION_SETUP, viewless()).await;
	let shaped_ctx = shaped.exec_ctx().await;
	let mut all = statements_of(&corpus_ctx, CORPUS).await;
	all.append(&mut statements_of(&shaped_ctx, REGION_SHAPED).await);
	#[cfg(feature = "gql")]
	all.append(&mut gql_statements_of(&shaped, &shaped_ctx, GQL_SHAPED).await);
	// A distinct set is a node of the plan that no SurrealQL statement writes; the SQL front ends
	// do.
	let distinct = distinct_of(&shaped_ctx, "SELECT city FROM person").await;
	all.push(statement(&shaped_ctx, "SELECT DISTINCT city FROM person", &distinct).await);
	Corpus {
		all,
		_dbs: vec![corpus, shaped],
	}
}

/// [`SCHEMA`] with the 300 people and 3 companies the statements of the corpus read.
fn seeded_schema() -> String {
	format!(
		"{SCHEMA}
		FOR $i IN 0..300 {{
			CREATE type::record('person', $i) SET
				name = 'name ' + <string> $i, age = $i % 40, city = ['x', 'y', 'z'][$i % 3]
		}};
		FOR $i IN 0..3 {{ CREATE type::record('company', $i) SET name = 'company ' + <string> $i }};"
	)
}

/// Every statement of `statements` over the database `setup` builds that plans morsels answers what
/// it answers over a datastore that never splits: its rows, in order where the statement orders or
/// pages them and as a multiset where it does not, or a failure the serial run's allows. Each runs
/// its morsels: a key range of it is split, or a batch of it is handed to a worker.
async fn fanned_statements_answer_as_the_serial_run(setup: &str, statements: &[&str]) {
	let serial = TestDb::new_with_config(setup, whole()).await;
	let wide = TestDb::new_with_config(setup, tiny()).await;
	let ctx = wide.exec_ctx().await;
	let mut fanned = 0;
	let mut failures = Vec::new();
	for sql in statements {
		// What draws entropy, or projects the clock, answers differently each time it runs; a
		// filter that compares with the clock answers the same.
		if sql.contains("SELECT time::now") || sql.contains("rand") {
			continue;
		}
		let bound = bind(&ctx, sql).await;
		if !compiled(&ctx, &bound).await.is_some_and(|program| fans_out(&program)) {
			continue;
		}
		let expected = answered(&serial, sql).await;
		// A vector search answers its few nearest rows, which fit one batch, and a subquery run
		// once for the statement reads the three companies, which its first chunk holds.
		let proof = !sql.contains("<|") && !sql.contains("(SELECT VALUE name FROM company)");
		match try_answered_fanning_out(&wide, sql, &expected, proof).await {
			Ok(FanOut::No) => failures.push(format!("`{sql}`: plans no morsels")),
			Ok(_) => fanned += 1,
			Err(why) => failures.push(format!("`{sql}`: {why}")),
		}
	}
	assert!(failures.is_empty(), "{failures:#?}");
	assert!(fanned > 0, "no statement plans morsels");
}

/// The planner places every pipeline of the corpus as the golden records, and what it places as
/// morsels the logical plan allows to split.
///
/// The golden records, for each statement the VM covers, the mode each pipeline of its program
/// runs in and the reason each serial pipeline of its plan is refused.
///
/// The corpus's `person` is SCHEMAFULL and has the index `person_age`, which is why it disagrees
/// with [`a_read_only_pipeline_is_admitted_by_its_shape`] on three shapes. A whole-row read with
/// no clause holds no loop, so the container holds no pipeline for it and it runs `serial in
/// place` with no refusal; a schemaless table keeps its projection and so a loop. A filter or an
/// `ORDER BY age` that the index answers is a `Batches` input whose body runs in the cursor, which
/// the VM runs serially. `count() GROUP ALL` is answered by the count and holds no pipeline.
#[tokio::test]
async fn placement_plans_the_pipelines_of_the_corpus() {
	let Corpus {
		all,
		..
	} = planned_corpus().await;
	let mut golden = Golden::new("placement_plans_the_pipelines_of_the_corpus");
	for statement in &all {
		if let Some(body) = placement_of(statement) {
			golden.record(&statement.sql, body);
		}
	}
	golden.check();
	assert_within_legality(&all);
	// Each statement the golden has run as morsels answers as its serial run does.
	fanned_statements_answer_as_the_serial_run(&seeded_schema(), CORPUS).await;
	fanned_statements_answer_as_the_serial_run(REGION_SETUP, REGION_SHAPED).await;
	let decisions: Vec<&Decided> = all.iter().flat_map(|s| &s.decided).collect();
	let planned = |d: &Decided| matches!(d.attr.decision(), Some(Decision::Morsels { .. }));
	let facts = |d: &Decided| d.attr.facts().cloned();
	assert!(decisions.iter().any(|d| planned(d)), "no pipeline runs as morsels");
	// A pipeline whose body runs in the storage cursor and is refused, by the program's own mark.
	assert!(
		all.iter().filter_map(|s| s.program.as_ref()).any(|program| {
			let mut pipelines = Vec::new();
			pipelines_of(program, "root", &mut pipelines);
			pipelines.iter().any(|(_, p)| p.in_place && matches!(p.parallel, Parallel::Serial))
		}),
		"no in-place pipeline is refused"
	);
	// A float fold is the serial fold's: a fold of one group over numbers runs as morsels that keep
	// their terms for the merge, in the order of the serial run, and any other is refused.
	let float = |d: &Decided| {
		matches!(&d.attr.legality().sink, SinkCombine::Aggregate(combines)
			if combines.contains(&AggregateCombine::InexactFloat))
	};
	assert!(decisions.iter().any(|d| float(d)), "no float aggregate is in the corpus");
	assert!(
		decisions.iter().filter(|d| float(d) && d.attr.legality().may_split()).all(|d| {
			if d.attr.legality().ordered_fold {
				planned(d)
					&& !d.attr.ordered_fold().is_empty()
					&& d.attr.merge() == MergeOrder::Serial
			} else {
				!planned(d)
					&& d.attr.ordered_fold().is_empty()
					&& d.refusal.as_deref()
						== Some("a float fold depends on how its terms are grouped")
			}
		}),
		"a float fold runs as morsels that keep their terms, or is refused for another reason"
	);
	assert!(
		decisions.iter().any(|d| float(d) && planned(d)),
		"no float fold runs as morsels that keep their terms"
	);
	assert!(
		decisions
			.iter()
			.any(|d| planned(d) && matches!(&d.attr.legality().sink, SinkCombine::Aggregate(_))),
		"no exact aggregate runs as morsels"
	);
	assert!(
		decisions.iter().any(|d| planned(d) && facts(d).is_some_and(|f| f.limits == 1)),
		"no paged pipeline runs as morsels"
	);
}

/// A pipeline that passes its rows through one distinct set runs as morsels, whose merge keeps the
/// first occurrence of each row.
#[tokio::test]
async fn a_distinct_pipeline_runs_as_morsels() {
	let Corpus {
		all,
		..
	} = planned_corpus().await;
	assert!(
		all.iter().flat_map(|s| &s.decided).any(|d| {
			matches!(d.attr.decision(), Some(Decision::Morsels { .. }))
				&& d.attr.facts().is_some_and(|f| f.distincts == 1)
		}),
		"no distinct pipeline runs as morsels"
	);
}

/// Every pipeline of every statement of the corpus, the statements that write, the joins, the
/// probes and the `MATCH` plans the planner runs as morsels is one the logical plan lists and
/// allows to split, and no decision is stronger than the logical plan states.
#[tokio::test]
async fn every_physical_pipeline_is_within_its_logical_legality() {
	let Corpus {
		all,
		..
	} = planned_corpus().await;
	assert_within_legality(&all);
	let placed = placed_statements().await;
	assert!(placed.statements.iter().any(|s| !s.decided.is_empty()), "no statement planned");
	assert_within_legality(&placed.statements);
}

/// An index scan is an ordinal source whether its columns hold scalars or arrays: the plan holds no
/// fact about the kinds an index's columns can hold, so every index scan is stated as one that may
/// reach a record through several entries, and the coordinator lists each record once, in the order
/// of its first entry. No index scan is a key range.
#[tokio::test]
async fn an_index_scan_is_an_ordinal_source() {
	let db = TestDb::new_with_config(
		"
		DEFINE TABLE item SCHEMAFULL;
		DEFINE FIELD n ON item TYPE int;
		DEFINE FIELD tags ON item TYPE array<string>;
		DEFINE INDEX by_n ON item FIELDS n;
		DEFINE TABLE bag SCHEMAFULL;
		DEFINE FIELD n ON bag TYPE int;
		DEFINE FIELD tags ON bag TYPE array<string>;
		DEFINE INDEX by_n_tags ON bag FIELDS n, tags;
		FOR $i IN 0..20 {
			CREATE type::record('item', $i) SET n = $i, tags = ['a', 'b'];
			CREATE type::record('bag', $i) SET n = $i, tags = ['a', 'b']
		};
		",
		viewless(),
	)
	.await;
	let ctx = db.exec_ctx().await;
	let planned =
		statements_of(&ctx, &["SELECT * FROM item WHERE n > 5", "SELECT * FROM bag WHERE n = 3"])
			.await;
	let sources: Vec<(SourceKind, SourceSplit)> = planned
		.iter()
		.flat_map(|s| &s.decided)
		.filter_map(|d| d.attr.facts())
		.map(|facts| (facts.source, facts.source.split()))
		.collect();
	assert!(!sources.is_empty(), "no statement plans a source");
	assert!(
		sources.iter().all(|source| *source
			== (
				SourceKind::Index {
					repeats: true
				},
				SourceSplit::Ordinal
			)),
		"{sources:?}"
	);
}

/// A ranked full-text scan runs serially only while it passes hits over by its sort's score
/// threshold. Where the pipeline may raise for a hit, a fallible cast here, the threshold is
/// dropped and the scan rejects nothing, so the pipeline is placed as the policy places any other:
/// the statement ranking the clause alone stays serial, and the one with the cast does not.
#[tokio::test]
async fn a_ranked_full_text_scan_is_serial_only_while_its_threshold_holds() {
	let db = TestDb::new_with_config(
		"
		DEFINE ANALYZER simple TOKENIZERS blank;
		DEFINE TABLE doc SCHEMALESS;
		DEFINE INDEX doc_body ON doc FIELDS body FULLTEXT ANALYZER simple BM25;
		FOR $i IN 0..20 { CREATE type::record('doc', $i) SET body = 'x y', n = '4' };
		",
		viewless(),
	)
	.await;
	let ctx = db.exec_ctx().await;
	let decisions = |statement: &Statement| -> Vec<Option<Decision>> {
		statement
			.decided
			.iter()
			.filter(|d| d.attr.facts().is_some_and(|facts| facts.source == SourceKind::FullText))
			.map(|d| d.attr.decision())
			.collect()
	};
	let planned = statements_of(
		&ctx,
		&[
			"SELECT id, search::score(1) AS s FROM doc WHERE body @1@ 'x' ORDER BY s DESC LIMIT 10",
			"SELECT id, search::score(1) AS s FROM doc WHERE body @1@ 'x' AND <int>n > 3 \
			 ORDER BY s DESC LIMIT 10",
		],
	)
	.await;
	assert_eq!(decisions(&planned[0]), vec![Some(Decision::Serial)], "{}", planned[0].sql);
	let cast = decisions(&planned[1]);
	assert_eq!(cast.len(), 1, "{}", planned[1].sql);
	assert_ne!(cast[0], Some(Decision::Serial), "{}", planned[1].sql);
}

/// The decision of the policy for a pipeline of `legality` and `facts`, in a statement that
/// refuses nothing, and for a pipeline that runs serially the reason.
fn judged(legality: &Legality, facts: &PhysicalFacts) -> Result<Decision, &'static str> {
	Policy::judge(
		legality,
		StatementLegality {
			refusal: None,
		},
		facts,
	)
}

/// Whether the policy plans a pipeline of `legality` and `facts` as morsels.
fn admits(legality: &Legality, facts: &PhysicalFacts) -> bool {
	matches!(judged(legality, facts), Ok(Decision::Morsels { .. }))
}

fn facts() -> PhysicalFacts {
	PhysicalFacts {
		source: SourceKind::Scan,
		enforce: false,
		writes: false,
		limits: 0,
		output: true,
		sink: SinkKind::Output,
		binds_row: false,
		fetches_row: false,
		distincts: 0,
		nested: NestedUse::None,
		host_blocks: false,
		paging_replayable: false,
		estimate: None,
	}
}

fn legality() -> Legality {
	Legality {
		source: SourceSplit::Range,
		chain: ChainLegality::RowLocal,
		sink: SinkCombine::Concat,
		paging: Paging::None,
		arrival: Arrival::Serial,
		ordered_fold: false,
		error_order: ErrorOrder::FirstByInputPosition,
		may_raise: false,
	}
}

/// What no statement of the corpus reaches: a pipeline of two paging clauses, and each fact the
/// policy refuses on its own, with the reason it names.
#[test]
fn the_policy_refuses_each_fact_on_its_own() {
	assert!(admits(&legality(), &facts()));
	type Fact = fn(&mut PhysicalFacts);
	let refused: &[(&str, Fact, &str)] = &[
		("enforced", |f| f.enforce = true, "rows are put through enforcement"),
		("writes", |f| f.writes = true, "the pipeline or its statement writes"),
		(
			"two limits",
			|f| {
				f.limits = 2;
				f.paging_replayable = true;
			},
			"the paging is not one a merge replays",
		),
		(
			"a limit that is not the output's",
			|f| {
				f.limits = 1;
				f.output = false;
				f.paging_replayable = true;
			},
			"the paging is not one a merge replays",
		),
		(
			"a limit the merge cannot replay",
			|f| f.limits = 1,
			"the paging is not one a merge replays",
		),
		("binds the row", |f| f.binds_row = true, "a binding step replaces the row"),
		(
			"reads the rows' records",
			|f| f.fetches_row = true,
			"the body reads the record each row names",
		),
		(
			"distinct",
			|f| f.distincts = 1,
			"the physical sink needs a combine the logical plan does not state",
		),
		(
			"nested statement evaluated once",
			|f| f.nested = NestedUse::OnceOrProbed,
			"a nested statement is evaluated once, probed, or writes",
		),
		(
			"nested statement that writes",
			|f| f.nested = NestedUse::Other,
			"a nested statement is evaluated once, probed, or writes",
		),
		("a block", |f| f.host_blocks = true, "an expression holds a block construct"),
	];
	for (what, set, reason) in refused {
		let mut facts = facts();
		set(&mut facts);
		assert_eq!(judged(&legality(), &facts), Err(*reason), "{what}");
	}
	let mut paged = facts();
	paged.limits = 1;
	paged.paging_replayable = true;
	assert!(matches!(
		judged(&legality(), &paged),
		Ok(Decision::Morsels {
			input: MorselInput::Range,
			paged: true,
			..
		})
	));
	// A source other than a table scan or a record range is read in batches.
	let mut lookup = facts();
	lookup.source = SourceKind::Lookup;
	assert!(matches!(
		judged(&legality(), &lookup),
		Ok(Decision::Morsels {
			input: MorselInput::Batches,
			..
		})
	));
	// A body that may raise runs every entry a top-k threshold would reject.
	for (may_raise, prunes) in [(false, true), (true, false)] {
		let mut raising = legality();
		raising.sink = SinkCombine::TopK;
		raising.may_raise = may_raise;
		let mut sorted = facts();
		sorted.sink = SinkKind::Sort {
			bounded: true,
		};
		sorted.output = false;
		assert!(matches!(
			judged(&raising, &sorted),
			Ok(Decision::Morsels {
				input: MorselInput::Range,
				prunes: planned,
				..
			}) if planned == prunes
		));
	}
	let mut nested = facts();
	nested.nested = NestedUse::PerRowReadOnly;
	assert!(admits(&legality(), &nested));
	let physical = |sink: &SinkCombine| match sink {
		SinkCombine::Aggregate(_) => SinkKind::Aggregate,
		SinkCombine::StableMerge => SinkKind::Sort {
			bounded: false,
		},
		SinkCombine::TopK => SinkKind::Sort {
			bounded: true,
		},
		SinkCombine::Concat
		| SinkCombine::Distinct
		| SinkCombine::NotSplittable
		| SinkCombine::OrderedApply => SinkKind::Split,
	};
	// A float fold's states combine and the logical plan says so, but the answer must equal the
	// serial fold bit for bit, so the policy declines it unless the plan states its terms may be
	// replayed in order.
	for (sink, expected) in [
		(SinkCombine::NotSplittable, Err("the logical plan does not allow the pipeline to split")),
		(
			SinkCombine::Aggregate(vec![AggregateCombine::Exact, AggregateCombine::Appending]),
			Ok(()),
		),
		(
			SinkCombine::Aggregate(vec![AggregateCombine::InexactFloat]),
			Err("a float fold depends on how its terms are grouped"),
		),
		(
			SinkCombine::Aggregate(vec![AggregateCombine::Exact, AggregateCombine::InexactFloat]),
			Err("a float fold depends on how its terms are grouped"),
		),
		(
			SinkCombine::Aggregate(vec![AggregateCombine::NotDecomposable]),
			Err("the logical plan does not allow the pipeline to split"),
		),
		(SinkCombine::StableMerge, Ok(())),
		(SinkCombine::TopK, Ok(())),
	] {
		let mut legality = legality();
		legality.sink = sink.clone();
		let mut facts = facts();
		facts.sink = physical(&sink);
		facts.output = false;
		assert_eq!(judged(&legality, &facts).map(|_| ()), expected, "{sink:?}");
	}
	// A fold the plan states may be replayed in order runs as morsels that keep their terms, and
	// the policy says so.
	let mut folded = legality();
	folded.sink =
		SinkCombine::Aggregate(vec![AggregateCombine::Exact, AggregateCombine::InexactFloat]);
	folded.ordered_fold = true;
	let mut folded_facts = facts();
	folded_facts.sink = SinkKind::Aggregate;
	folded_facts.output = false;
	let decision = judged(&folded, &folded_facts).expect("an ordered float fold fans out");
	assert_eq!(Policy::ordered_fold(&folded, decision, MergeOrder::Serial), [1]);
	// Terms kept for the merge are folded by a merge that takes the morsels in order.
	assert!(Policy::ordered_fold(&folded, decision, MergeOrder::Free).is_empty());
	assert_eq!(Policy::merge_order(&folded, &folded_facts, decision), MergeOrder::Serial);
	// Nothing keeps terms for a pipeline whose fold is not a float fold, or that runs serially.
	folded.sink = SinkCombine::Aggregate(vec![AggregateCombine::Exact]);
	assert!(Policy::ordered_fold(&folded, decision, MergeOrder::Serial).is_empty());
	folded.sink = SinkCombine::Aggregate(vec![AggregateCombine::InexactFloat]);
	assert!(Policy::ordered_fold(&folded, Decision::Serial, MergeOrder::Serial).is_empty());
	let mut whole = legality();
	whole.source = SourceSplit::Whole;
	assert!(!admits(&whole, &facts()));
}

/// The VM narrows the planner's decision by the body lowering emitted, one case for each row of
/// the table of `morsel::effective_mode`, and never turns a serial decision into morsels.
#[test]
fn the_effective_mode_narrows_by_the_emitted_body() {
	// A table scan or a record range, decided as `Range`: in the cursor it reads its ranges in
	// place, and over decoded rows it runs in batches.
	assert_eq!(effective_mode(MorselInput::Range, true), MorselInput::Range);
	assert_eq!(effective_mode(MorselInput::Range, false), MorselInput::Batches);
	// Any other source that splits, decided as `Batches`: batches over decoded rows, whatever the
	// body is, since only a key range is read in a cursor.
	assert_eq!(effective_mode(MorselInput::Batches, false), MorselInput::Batches);
	assert_eq!(effective_mode(MorselInput::Batches, true), MorselInput::Batches);
	// A serial decision is serial whatever the body is.
	for in_place in [true, false] {
		let pipeline = |parallel| Pipeline {
			source: super::program::Source::Once,
			code: Vec::new(),
			registers: 0,
			in_place,
			stored_rows: false,
			enforce: None,
			limits: Vec::new(),
			parallel,
			group: None,
			reads_row: false,
			deref_windows: false,
			frontier_rows: None,
			read_ahead: false,
			rows_ahead: None,
			unit: None,
		};
		assert_eq!(effective_input(&pipeline(Parallel::Serial)), None);
		let range = Parallel::Morsels(MorselPlan {
			input: MorselInput::Range,
			combine: Combine::Concat,
			paged: None,
			prunes: true,
			merge: MergeOrder::Serial,
			ordered_fold: Vec::new(),
		});
		assert_eq!(
			effective_input(&pipeline(range)),
			Some(effective_mode(MorselInput::Range, in_place))
		);
	}
}

/// A one-word account of a lowered pipeline's final placement: the mode the VM runs it in, which
/// its body narrows the copied decision to ([`effective_input`]), `in place` where its body runs
/// inside the storage cursor, and its group.
fn final_mode(pipeline: &Pipeline) -> String {
	let mut parts = vec![mode_word(&pipeline.parallel, pipeline.in_place)];
	if let (Some(_), Parallel::Morsels(plan)) = (effective_input(pipeline), &pipeline.parallel) {
		if plan.paged.is_some() {
			parts.push("paged".to_owned());
		}
		if plan.prunes {
			parts.push("prunes".to_owned());
		}
		parts.push(format!(
			"combine {}",
			match plan.combine {
				Combine::Concat => "concat",
				Combine::StableMerge => "stable merge",
				Combine::TopK => "top k",
				Combine::Aggregate => "aggregate",
				Combine::Distinct => "distinct",
				Combine::OrderedApply => "ordered apply",
			}
		));
	}
	if let Some(group) = pipeline.group {
		parts.push(format!("group {group}"));
	}
	parts.join(", ")
}

/// The final placement of every pipeline `program` lowered to: one line per pipeline of the
/// program and, recursively, of every program in its units, each prefixed with the path of the
/// unit that holds it.
fn final_placement_of(program: &Program) -> String {
	let mut pipelines = Vec::new();
	pipelines_of(program, "root", &mut pipelines);
	pipelines
		.iter()
		.map(|(path, pipeline)| format!("{path} {}", final_mode(pipeline)))
		.collect::<Vec<_>>()
		.join("\n")
}

/// The statements whose pipelines the planner places beside a write, a join's build or a probe,
/// and why a pipeline of each runs serially. The lines of each are pinned in the golden
/// `final_placement_documented`.
const DOCUMENTED: &[(&str, &str)] = &[
	(
		"UPDATE person SET city = (SELECT VALUE name FROM company)",
		"a read nested in a write is placed beside the write",
	),
	(
		"UPDATE person SET city = (SELECT VALUE name FROM company WHERE name = $this)",
		"a read nested in a write is placed beside the write",
	),
	(
		"UPDATE person SET city = (SELECT VALUE name FROM company) #2",
		"a read nested in a write is placed beside the write",
	),
	(
		"UPDATE l SET xs = (SELECT VALUE x FROM r WHERE k = $parent.k)",
		"a read nested in a write is placed beside the write",
	),
	(
		"SELECT * FROM l JOIN u ON l.k = u.k",
		"the build side of a join is a statement of its own, and runs serially",
	),
	(
		"SELECT * FROM l LEFT JOIN r ON l.k = r.k",
		"the build side of a join is a statement of its own, and runs serially",
	),
	(
		"SELECT id, (SELECT VALUE x FROM u WHERE k = $parent.k) AS xs FROM l",
		"the build of a probe is a statement of its own, and runs serially",
	),
	(
		"SELECT id, (SELECT VALUE x FROM r WHERE k = $parent.k) AS xs FROM l",
		"the build of the probe a lookup falls back to is a statement of its own",
	),
	(
		"SELECT *, (SELECT VALUE name FROM company) AS c FROM person LIMIT rand::int(1, 5)",
		"a statement nested in one that refuses its pipelines' splits is placed serial",
	),
];

/// The statements of the corpus in the order the final golden records them, planned and compiled
/// as the VM does, and the databases they run over.
struct PlacedStatements {
	statements: Vec<Statement>,
	/// The databases the statements are planned over, kept alive with the plans.
	_dbs: Vec<TestDb>,
}

/// Plan and compile every statement of [`CORPUS`], [`REGION_SHAPED`], [`PHYSICAL_STRATEGIES`],
/// [`GQL_SHAPED`], the synthetic `DISTINCT`, the joins, the probes, the binding join and the
/// statements that write: a read of the shapes the final placement must not lose.
async fn placed_statements() -> PlacedStatements {
	let corpus = TestDb::new_with_config(SCHEMA, viewless()).await;
	let corpus_ctx = corpus.exec_ctx().await;
	let shaped = TestDb::new_with_config(REGION_SETUP, viewless()).await;
	let shaped_ctx = shaped.exec_ctx().await;
	let extra = TestDb::new_with_config(&format!("{REGION_SETUP}{FINAL_SETUP}"), viewless()).await;
	let extra_ctx = extra.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound_ctx = extra_ctx.with_param("t", crate::val::Value::from("person"));
	let mut out: Vec<Statement> = Vec::new();
	out.extend(statements_of(&corpus_ctx, CORPUS).await);
	for sql in REGION_SHAPED.iter().chain(PHYSICAL_STRATEGIES) {
		out.extend(statements_of(&shaped_ctx, &[sql]).await);
	}
	#[cfg(feature = "gql")]
	out.extend(gql_statements_of(&shaped, &shaped_ctx, GQL_SHAPED).await);
	let distinct = distinct_of(&shaped_ctx, "SELECT city FROM person").await;
	out.push(statement(&shaped_ctx, "SELECT DISTINCT city FROM person", &distinct).await);
	// An advancing projection over each breaker, a join of either kind, a probe, a binding join,
	// and the statements that write.
	let distinct = distinct_of(&extra_ctx, "SELECT sequence::nextval('s') AS n FROM person").await;
	out.push(statement(&extra_ctx, ADVANCING, &distinct).await);
	for (sql, kind, left, right) in FINAL_JOINS {
		let join = joined(&extra_ctx, *kind, left, right).await;
		out.push(statement(&extra_ctx, sql, &join).await);
	}
	for sql in FINAL_SHAPED {
		let ctx = if sql.contains("$t") {
			&bound_ctx
		} else {
			&extra_ctx
		};
		out.extend(statements_of(ctx, &[sql]).await);
	}
	#[cfg(feature = "gql")]
	out.extend(gql_statements_of(&extra, &extra_ctx, &[BINDING_JOIN]).await);
	for sql in FINAL_WRITES {
		out.extend(statements_of(&extra_ctx, &[sql]).await);
	}
	PlacedStatements {
		statements: out,
		_dbs: vec![corpus, shaped, extra],
	}
}

/// The statement of [`placed_statements`] that is the distinct set over an advancing projection.
const ADVANCING: &str = "SELECT DISTINCT sequence::nextval('s') AS n FROM person";

/// The final placement of every pipeline of every program the corpus compiles to: the decision
/// lowering copies into `Program.pipelines`, with the body mode and the group, for each unit.
///
/// The mode is the one the VM runs: the planner's decision, narrowed by the body lowering emitted
/// ([`effective_input`]).
#[tokio::test]
async fn final_placement_of_the_corpus() {
	let PlacedStatements {
		statements,
		..
	} = placed_statements().await;
	let mut golden = Golden::new("final_placement_of_the_corpus");
	let mut recorded: Vec<(String, String)> = Vec::new();
	let mut seen: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
	for Statement {
		sql,
		program,
		..
	} in &statements
	{
		let body = program.as_ref().map_or_else(|| DECLINED.to_owned(), final_placement_of);
		golden.record(sql, &body);
		// A statement both corpora hold is recorded again under its ordinal.
		let times = seen.entry(sql.as_str()).or_default();
		*times += 1;
		let key = match times {
			1 => sql.clone(),
			n => format!("{sql} #{n}"),
		};
		recorded.push((key, body));
	}
	golden.check();
	// The statements whose pipelines the planner places beside a write, a join's build or a probe
	// are pinned, and each holds a pipeline that runs serially.
	let mut pinned = Golden::new("final_placement_documented");
	let base = Golden::recorded("final_placement_of_the_corpus");
	for (sql, why) in DOCUMENTED {
		let now =
			&recorded.iter().find(|(key, _)| key == sql).unwrap_or_else(|| panic!("`{sql}`")).1;
		let was = &base.iter().find(|(key, _)| key == sql).expect("a golden statement").1;
		assert_eq!(now, was, "`{sql}`: the golden holds the statement's lines");
		assert!(
			now.lines().any(|line| line
				.split_once(' ')
				.is_some_and(|(_, mode)| mode.starts_with("serial"))),
			"`{sql}` holds no serial pipeline, which `{why}`"
		);
		if sql.starts_with("UPDATE") {
			assert!(
				now.lines().all(|line| line
					.split_once(' ')
					.is_some_and(|(_, mode)| mode.starts_with("serial"))),
				"`{sql}` is a write beside which no pipeline runs as morsels: {now}"
			);
		}
		pinned.record(sql, now);
	}
	pinned.check();
}

/// Whether every pipeline of `program` and of the programs in its units is serial.
fn serial_throughout(program: &Program) -> bool {
	program.pipelines.iter().all(|pipeline| matches!(pipeline.parallel, Parallel::Serial))
		&& program.units.iter().all(|unit| serial_throughout(unit))
}

/// The program `sql` compiles to over `ctx`, panicking where the VM declines it.
pub(super) async fn program_of(ctx: &ExecutionContext, sql: &str) -> Program {
	let bound = bind(ctx, sql).await;
	compiled(ctx, &bound).await.unwrap_or_else(|| panic!("the VM declines `{sql}`"))
}

/// A user of the database's `user` scope, whose reads the tables' permissions judge.
fn record_user() -> crate::dbs::Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	crate::dbs::Session::for_record("test", "test", "user", rid)
}

/// A computed field that no answer reads, but a table's permission does, is derived for the
/// permission over a copy of the row. A computed field that draws entropy plans its table's scan
/// as serial; one that is deterministic leaves the scan to run as morsels.
#[tokio::test]
async fn a_computed_field_read_only_through_a_permission_plans_serial() {
	let db = TestDb::new_with_auth_and_config(
		"
		DEFINE TABLE vault PERMISSIONS FOR select WHERE r >= 0;
		DEFINE FIELD r ON vault COMPUTED rand();
		DEFINE TABLE token PERMISSIONS FOR select WHERE u != NONE;
		DEFINE FIELD u ON token COMPUTED rand::uuid::v7();
		DEFINE TABLE control PERMISSIONS FOR select WHERE d >= 0;
		DEFINE FIELD d ON control COMPUTED n * 2;
		FOR $i IN 0..300 {
			CREATE type::record('vault', $i) SET n = $i;
			CREATE type::record('token', $i) SET n = $i;
			CREATE type::record('control', $i) SET n = $i
		};
		",
		viewless(),
	)
	.await;
	let ctx = db.exec_ctx_as(&record_user(), crate::kvs::TransactionType::Read).await;
	for sql in ["SELECT id, n FROM vault", "SELECT id, n FROM token"] {
		let program = program_of(&ctx, sql).await;
		assert!(
			serial_throughout(&program),
			"`{sql}` plans morsels: {}",
			final_placement_of(&program)
		);
	}
	let sql = "SELECT id, n FROM control";
	let program = program_of(&ctx, sql).await;
	assert!(
		!serial_throughout(&program),
		"`{sql}` plans no morsels: {}",
		final_placement_of(&program)
	);
}

/// A computed field that calls code outside the database plans its table's scan as serial; a table
/// without one leaves the scan to run as morsels. Nothing runs: only the decision is read.
#[tokio::test]
async fn a_computed_field_calling_outside_the_database_plans_serial() {
	let db = TestDb::new_with_config(
		"
		DEFINE TABLE ext;
		DEFINE TABLE plain;
		FOR $i IN 0..300 {
			CREATE type::record('ext', $i) SET n = $i RETURN NONE;
			CREATE type::record('plain', $i) SET n = $i RETURN NONE
		};
		DEFINE FIELD e ON ext COMPUTED http::get('http://127.0.0.1:1/');
		",
		viewless(),
	)
	.await;
	let ctx = db.exec_ctx().await;
	let sql = "SELECT * FROM ext";
	let program = program_of(&ctx, sql).await;
	assert!(serial_throughout(&program), "`{sql}` plans morsels: {}", final_placement_of(&program));
	let sql = "SELECT * FROM plain";
	let program = program_of(&ctx, sql).await;
	assert!(
		!serial_throughout(&program),
		"`{sql}` plans no morsels: {}",
		final_placement_of(&program)
	);
}

/// Tables of the shapes the admission test plans over: a schemaless table with no index to read in
/// its place, a SCHEMAFULL table of the same shape, and a SCHEMAFULL table whose index answers a
/// filter on its field.
const ADMISSION_SETUP: &str = "
	DEFINE TABLE t;
	DEFINE TABLE u;
	DEFINE TABLE s SCHEMAFULL;
	DEFINE FIELD n ON s TYPE int;
	DEFINE FIELD k ON s TYPE int;
	DEFINE TABLE ix SCHEMAFULL;
	DEFINE FIELD n ON ix TYPE int;
	DEFINE INDEX ix_n ON ix FIELDS n;
	CREATE t:1 SET n = 1, k = 1, f = 0.5;
	CREATE u:1 SET n = 1;
	CREATE s:1 SET n = 1, k = 1;
	CREATE ix:1 SET n = 1;
";

/// What the planner decided for a pipeline, as a word: the input and combine of a fan-out, or
/// the reason a pipeline runs serially.
fn verdict_of(decided: &Decided) -> String {
	match decided.attr.decision() {
		Some(Decision::Morsels {
			input,
			combine,
			paged,
			prunes,
		}) => format!(
			"{input:?} {combine:?}{}{}{}",
			if paged {
				" paged"
			} else {
				""
			},
			if prunes {
				" prunes"
			} else {
				""
			},
			if !decided.attr.ordered_fold().is_empty() {
				" ordered fold"
			} else {
				""
			}
		),
		Some(Decision::Serial) => format!(
			"serial: {}",
			decided.refusal.as_deref().unwrap_or("narrowed by the pipeline around it")
		),
		None => "unplaced".to_owned(),
	}
}

/// The verdict of each pipeline of the statement `sql`, in the container's order, and whether the
/// facts of each state a write.
pub(super) async fn admission_of(ctx: &ExecutionContext, sql: &str) -> Vec<(String, bool)> {
	let op = bind(ctx, sql).await;
	let decided = decided(ctx, &op).await.expect("the statement plans");
	decided
		.iter()
		.map(|decided| {
			let writes = decided.attr.facts().expect("every pipeline records its facts").writes;
			(verdict_of(decided), writes)
		})
		.collect()
}

/// A pipeline of a read-only statement is judged on its own source and sink, under the verdict
/// of the loop it stands for: a scan, a filter, a record range, a grouping, a sort and a top-k
/// run as morsels, and the facts of none state a write. What is refused names its reason: a float
/// fold, a write, a read nested in a write, and a read that invokes a closure whose body writes.
///
/// A paged scan, and a scan whose sort its direction answers, are placed as one chain with the
/// pipeline that reads their rows: the placement is the first pipeline's, and the pipeline that
/// continues it is not placed apart, so what the continuation holds refuses the chain: entropy, a
/// write and a nested statement evaluated once in a projection after the paging keep the scan
/// serial. `count() GROUP ALL` over a whole table is answered by the count, which holds no
/// pipeline.
///
/// A top-k prunes only where nothing in its pipeline can raise: a filter that may raise leaves it
/// to run every row, as the serial scan does. A SCHEMAFULL table is placed as a schemaless one is,
/// except that a whole-row read with no clause holds no loop and so no pipeline. A read answered
/// by an index is a `Batches` input, which the VM runs serially where its body runs in the cursor
/// ([`effective_mode`](super::morsel::effective_mode)).
#[tokio::test]
async fn a_read_only_pipeline_is_admitted_by_its_shape() {
	let db = TestDb::new_with_config(ADMISSION_SETUP, viewless()).await;
	let ctx = db.exec_ctx().await;
	let consumer = ("serial: the loop's source cannot be split".to_owned(), false);
	let entropy = ("serial: the statement draws entropy".to_owned(), false);
	let continuation =
		("serial: the logical plan does not allow the pipeline to split".to_owned(), false);
	let cases: Vec<(&str, Vec<(String, bool)>)> = vec![
		("SELECT * FROM t", vec![("Range Concat".to_owned(), false)]),
		("SELECT n FROM t", vec![("Range Concat".to_owned(), false)]),
		("SELECT * FROM t WHERE n > 5", vec![("Range Concat".to_owned(), false)]),
		("SELECT * FROM t:1..5", vec![("Range Concat".to_owned(), false)]),
		("SELECT k, count() FROM t GROUP BY k", vec![("Range Aggregate".to_owned(), false)]),
		(
			"SELECT n, k FROM t ORDER BY k",
			vec![("Range StableMerge".to_owned(), false), consumer.clone()],
		),
		(
			"SELECT n, k FROM t ORDER BY k LIMIT 10",
			vec![("Range TopK prunes".to_owned(), false), consumer.clone()],
		),
		(
			"SELECT n, k FROM t ORDER BY k LIMIT 10 START 5",
			vec![("Range TopK prunes".to_owned(), false), consumer.clone()],
		),
		(
			"SELECT n, k FROM t WHERE <int> n >= 0 ORDER BY k LIMIT 10",
			vec![("Range TopK".to_owned(), false), consumer.clone()],
		),
		(
			"SELECT n, k FROM t WHERE (big ?? 0) + 1 >= 0 ORDER BY k LIMIT 3",
			vec![("Range TopK".to_owned(), false), consumer.clone()],
		),
		(
			"SELECT n, k FROM t WHERE v * 2 >= 0 ORDER BY k LIMIT 3",
			vec![("Range TopK".to_owned(), false), consumer.clone()],
		),
		(
			"SELECT n, k FROM t WHERE n = 700 OR <int> n >= 0 ORDER BY k DESC LIMIT 10 START 5",
			vec![("Range TopK".to_owned(), false), consumer.clone()],
		),
		(
			"SELECT n FROM t LIMIT 10 START 5",
			vec![("Range Concat paged".to_owned(), false), continuation.clone()],
		),
		(
			"SELECT * FROM t WHERE n > 5 LIMIT 3",
			vec![("Range Concat paged".to_owned(), false), continuation.clone()],
		),
		(
			"SELECT n FROM t ORDER BY id DESC",
			vec![("Range Concat".to_owned(), false), consumer.clone()],
		),
		(
			"SELECT count() FROM t WHERE n > 5 GROUP ALL",
			vec![("Range Aggregate".to_owned(), false)],
		),
		("SELECT count() FROM t GROUP ALL", vec![]),
		("SELECT n, rand::float() AS r FROM t LIMIT 5", vec![entropy.clone(), entropy.clone()]),
		(
			"SELECT n, (CREATE u SET n = 1) AS x FROM t LIMIT 5",
			vec![
				(
					"serial: the statement writes data or something outside the database"
						.to_owned(),
					true,
				),
				(
					"serial: the loop body, or the code its table stores, may write a record of the table its source reads at another key"
						.to_owned(),
					true,
				),
				("serial: two iterations may write one key".to_owned(), true),
			],
		),
		(
			"SELECT n, (SELECT VALUE n FROM u LIMIT 1) AS x FROM t LIMIT 5",
			vec![
				(
					"serial: a nested statement is evaluated once, probed, or writes".to_owned(),
					false,
				),
				continuation.clone(),
				("Range Concat paged".to_owned(), false),
				continuation.clone(),
			],
		),
		("SELECT * FROM s", vec![("Range Concat".to_owned(), false)]),
		("SELECT n FROM s", vec![("Range Concat".to_owned(), false)]),
		("SELECT * FROM s WHERE n > 5", vec![("Range Concat".to_owned(), false)]),
		(
			"SELECT n, k FROM s ORDER BY k",
			vec![("Range StableMerge".to_owned(), false), consumer.clone()],
		),
		(
			"SELECT n, k FROM s ORDER BY k LIMIT 10",
			vec![("Range TopK prunes".to_owned(), false), consumer.clone()],
		),
		("SELECT * FROM ix WHERE n > 5", vec![("Batches Concat".to_owned(), false)]),
		(
			"SELECT math::sum(f) FROM t GROUP ALL",
			vec![("Range Aggregate ordered fold".to_owned(), false)],
		),
		(
			"SELECT math::mean(f), math::stddev(f), math::variance(f), count() FROM t GROUP ALL",
			vec![("Range Aggregate ordered fold".to_owned(), false)],
		),
		(
			"SELECT k, math::sum(f) FROM t GROUP BY k",
			vec![("serial: a float fold depends on how its terms are grouped".to_owned(), false)],
		),
		(
			"SELECT vector::sum(v) FROM t GROUP ALL",
			vec![("serial: a float fold depends on how its terms are grouped".to_owned(), false)],
		),
		(
			"SELECT n, [n].find(|$x| { CREATE u SET n = $x; RETURN true }) AS f FROM t",
			vec![
				("serial: the statement runs a body the plan cannot see".to_owned(), true),
				(
					"serial: the loop body, or the code its table stores, may write a record of the table its source reads at another key"
						.to_owned(),
					true,
				),
			],
		),
		(
			"SELECT n, [n].map(|$x| { CREATE u SET n = $x; RETURN $x }) AS f FROM t",
			vec![
				("serial: the statement runs a body the plan cannot see".to_owned(), true),
				(
					"serial: the loop body, or the code its table stores, may write a record of the table its source reads at another key"
						.to_owned(),
					true,
				),
			],
		),
		("UPDATE t SET n = 2", vec![("Batches OrderedApply".to_owned(), true)]),
		(
			"UPDATE t SET flag = true WHERE n IN (SELECT VALUE n FROM u)",
			vec![
				(
					"serial: a stage the plan marks compute holds an op that only the apply half may run"
						.to_owned(),
					true,
				),
				("serial: the pipeline or its statement writes".to_owned(), true),
			],
		),
	];
	for (sql, expected) in cases {
		assert_eq!(admission_of(&ctx, sql).await, expected, "`{sql}`");
	}
}

/// On a datastore whose read-only transactions give no read view, a pipeline the policy
/// would read as range morsels plans serially while the policy cuts a range only for workers that
/// read through a view, and as range morsels where it cuts one without; morsels over batches are
/// kept either way.
#[tokio::test]
async fn a_range_read_plans_serially_where_the_datastore_gives_no_view() {
	let narrowed =
		("serial: narrowed by the pipeline around it or by the backend".to_owned(), false);
	let default = TestDb::new_viewless(ADMISSION_SETUP, surrealdb_cnf::ConfigMap::empty()).await;
	let default_ctx = default.exec_ctx().await;
	let cut = TestDb::new_viewless(ADMISSION_SETUP, viewless()).await;
	let cut_ctx = cut.exec_ctx().await;
	let sql = "SELECT * FROM t";
	assert_eq!(admission_of(&default_ctx, sql).await, vec![narrowed], "`{sql}`");
	assert_eq!(
		admission_of(&cut_ctx, sql).await,
		vec![("Range Concat".to_owned(), false)],
		"`{sql}`"
	);
	let sql = "UPDATE t SET n = 2";
	assert_eq!(admission_of(&default_ctx, sql).await, admission_of(&cut_ctx, sql).await);
}

/// Documents whose `body` a full-text index answers: 300 carry `x` in a body of a length of its
/// own, so no two of them score alike, a third of those carry `z` too, and as many again and a half
/// carry neither, so both terms weigh well above the scores' rounding. `n` numbers them.
const RANKED_SETUP: &str = "
	DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
	DEFINE TABLE doc SCHEMALESS;
	DEFINE INDEX doc_body ON doc FIELDS body FULLTEXT ANALYZER simple BM25;
	FOR $i IN 0..300 {
		CREATE type::record('doc', $i) SET
			body = 'x' + string::repeat(' y', $i) + ['', ' z', ''][$i % 3],
			n = $i
	};
	FOR $i IN 300..750 {
		CREATE type::record('doc', $i) SET body = 'filler', n = $i
	};
";

/// Whether the one full-text scan or index union the statement `op` plans holds a key that rejects
/// entries by a top-k threshold, or `None` where it holds no top-k key.
async fn score_key_of(ctx: &ExecutionContext, op: &LogicalOp) -> Option<bool> {
	let planned = planned_of(ctx, op).await.expect("the statement plans");
	let mut keys = Vec::new();
	planned.ir.walk(planned.ir.top(), WalkOrder::PreOrder, &mut |at| {
		match planned.ir.op(at).map(|data| &data.kind) {
			Some(OpKind::Phys(PhysOp::FullTextScan(scan))) => {
				keys.push(scan.topk.as_ref().map(|key| key.rejects_entries));
			}
			Some(OpKind::Phys(PhysOp::IndexUnion(union))) => {
				keys.push(union.topk.as_ref().map(|(key, _)| key.rejects_entries));
			}
			Some(_) | None => {}
		}
		Walk::Continue
	});
	let [key] = keys.as_slice() else {
		panic!("the plan holds {} full-text sources", keys.len());
	};
	*key
}

/// A ranked full-text read, ordered by a clause's score or by a sum of clauses' scores under a
/// `LIMIT`, and whether its scan or index union rejects hits by the sort's threshold. A read whose
/// pipeline may raise for a hit its scan would pass over, through a cast in its `WHERE` or a later
/// sort key, rejects none.
const RANKED: &[(&str, bool)] = &[
	("SELECT id, search::score(1) AS s FROM doc WHERE body @1@ 'x' ORDER BY s DESC LIMIT 10", true),
	(
		"SELECT id, search::score(1) AS s FROM doc WHERE body @1@ 'x' AND <int> n > 3 \
		 ORDER BY s DESC LIMIT 10",
		false,
	),
	(
		"SELECT id, search::score(1) + search::score(2) AS s FROM doc \
		 WHERE body @1@ 'x' OR body @2@ 'z' ORDER BY s DESC LIMIT 10",
		true,
	),
	(
		"SELECT id, search::score(1) + search::score(2) AS s, <int> n AS m FROM doc \
		 WHERE body @1@ 'x' OR body @2@ 'z' ORDER BY s DESC, m LIMIT 10",
		false,
	),
	(
		"SELECT id, search::score(1) + search::score(2) AS s FROM doc \
		 WHERE (body @1@ 'x' OR body @2@ 'z') AND <int> n > 3 ORDER BY s DESC LIMIT 10",
		false,
	),
];

/// A ranked full-text read whose scan rejects hits by the sort's threshold runs serially, so each
/// row reaches the sort as it is read and tightens the threshold at once: the policy admits the
/// pipeline as morsels, and the score rule narrows it. One whose scan holds the sort's key but
/// rejects no hit, because its pipeline may raise for a hit it would pass over, is placed as any
/// other read of its source and runs as morsels. The same holds of an index union of full-text
/// branches ordered by the sum of their clauses' scores. `EXPLAIN` shows the pushdown exactly where
/// the scan rejects hits, and whichever way the read runs it answers what it answers with no index.
#[tokio::test]
async fn a_ranked_full_text_read_runs_serially_only_where_its_scan_rejects_hits() {
	let db = TestDb::new_with_config(RANKED_SETUP, tiny()).await;
	let ctx = db.exec_ctx().await;
	for (sql, rejects) in RANKED {
		let op = bind(&ctx, sql).await;
		assert_eq!(score_key_of(&ctx, &op).await, Some(*rejects), "`{sql}`");
		let decided = decided(&ctx, &op).await.expect("the statement plans");
		let verdicts: Vec<String> = decided.iter().map(verdict_of).collect();
		let ranked: Vec<&Decided> = decided
			.iter()
			.filter(|decided| {
				decided.attr.facts().is_some_and(|facts| {
					matches!(facts.source, SourceKind::FullText | SourceKind::IndexUnion)
				})
			})
			.collect();
		let [scan] = ranked.as_slice() else {
			panic!("`{sql}` reads no one full-text source: {verdicts:?}");
		};
		assert_eq!(scan.attr.prunes() || scan.attr.prunes_by_score(), *rejects, "`{sql}`");
		let verdict = verdict_of(scan);
		if *rejects {
			assert_eq!(
				verdict, "serial: narrowed by the pipeline around it or by the backend",
				"`{sql}`: {verdicts:?}"
			);
		} else {
			assert!(
				matches!(scan.attr.decision(), Some(Decision::Morsels { .. })),
				"`{sql}`: {verdicts:?}"
			);
		}
		let program = program_of(&ctx, sql).await;
		assert_eq!(fans_out(&program), !rejects, "`{sql}`:\n{}", final_placement_of(&program));
		let plan = format!("{:?}", answered(&db, &format!("EXPLAIN {sql}")).await);
		assert_eq!(plan.contains("topk_pushdown: score"), *rejects, "`{sql}`\n{plan}");
		let answer = answered(&db, sql).await;
		assert_eq!(answer.as_ref().map(Vec::len), Ok(10), "`{sql}`: {answer:?}");
		let unindexed = sql.replacen("FROM doc", "FROM doc WITH NOINDEX", 1);
		assert_eq!(answer, answered(&db, &unindexed).await, "`{sql}`");
	}
}

/// The policy of a write that fans out refuses each fact it states on its own, with the reason it
/// names: the facts of a write over a table scan are admitted, and each of these alone is not.
#[test]
fn the_write_policy_refuses_each_fact_on_its_own() {
	use crate::exec::ir::phys::write_cut::WriteCut;

	let write_facts = || PhysicalFacts {
		sink: SinkKind::Write,
		output: false,
		..facts()
	};
	let write_legality = || Legality {
		sink: SinkCombine::OrderedApply,
		..legality()
	};
	let cut = WriteCut {
		stage: 3,
		compute_reads_data: false,
		encode_at_cut: true,
	};
	let judge = |legality: &Legality, facts: &PhysicalFacts, cut: Option<&WriteCut>| {
		Policy::judge_write(
			legality,
			StatementLegality {
				refusal: None,
			},
			facts,
			cut.ok_or("no cut"),
		)
	};
	assert!(matches!(
		judge(&write_legality(), &write_facts(), Some(&cut)),
		Ok(Decision::Morsels {
			combine: Combine::OrderedApply,
			..
		})
	));
	type Fact = fn(&mut PhysicalFacts);
	let refused: &[(&str, Fact, &str)] = &[
		(
			"not the write's sink",
			|f| f.sink = SinkKind::Output,
			"the pipeline's sink is not the write",
		),
		("enforced", |f| f.enforce = true, "rows are put through enforcement"),
		(
			"an index scan",
			|f| {
				f.source = SourceKind::Index {
					repeats: false,
				}
			},
			"the write reads its rows through an access path its own writes may move",
		),
		(
			"one record",
			|f| f.source = SourceKind::Record,
			"the write reads one record, which leaves no second batch to hand on",
		),
		("paged", |f| f.limits = 1, "the write pages its rows in the loop"),
		(
			"a nested statement evaluated once",
			|f| f.nested = NestedUse::OnceOrProbed,
			"a nested statement is evaluated once, probed, or writes",
		),
		("a block", |f| f.host_blocks = true, "an expression holds a block construct"),
		(
			"reads the rows' records",
			|f| f.fetches_row = true,
			"the body reads the record each row names",
		),
		("binds the row", |f| f.binds_row = true, "a binding step replaces the row"),
	];
	for (what, set, reason) in refused {
		let mut facts = write_facts();
		set(&mut facts);
		assert_eq!(
			judge(&write_legality(), &facts, Some(&cut)).map(|_| ()),
			Err(*reason),
			"{what}"
		);
	}
	assert_eq!(judge(&write_legality(), &write_facts(), None).map(|_| ()), Err("no cut"));
	let reads = WriteCut {
		compute_reads_data: true,
		..cut
	};
	assert_eq!(
		judge(&write_legality(), &write_facts(), Some(&reads)).map(|_| ()),
		Err("the compute half reads stored data")
	);
	assert_eq!(
		judge(&legality(), &write_facts(), Some(&cut)).map(|_| ()),
		Err("the logical plan does not allow the writes to apply in order")
	);
}

/// A placement decides through the policy of the configuration it is built from, so the datastore's
/// `morsel_*` settings are the ones its decisions read.
#[test]
fn a_placement_carries_the_policy_of_its_configuration() {
	use crate::exec::config::ExecConfig;
	use crate::exec::ir::iface::effects::{ElemPath, Multiplicity};
	use crate::exec::ir::iface::policy::LoopFacts;
	use crate::exec::ir::iface::{ApplyCut, Exit, LoopMode, SourceDelivery};
	use crate::exec::ir::logical::ops::LoopSink;
	use crate::exec::ir::phys::placement::Placement;

	let loop_facts = LoopFacts {
		mode: LoopMode::Independent,
		apply: ApplyCut::None,
		exit: Exit::None,
		sink: LoopSink::Output,
		delivery: SourceDelivery::ReadWhole,
		multiplicity: Multiplicity::Distinct,
		key_path: ElemPath::Id,
		compute_reads_data: false,
		cut_at_marks: true,
	};
	let config = ExecConfig {
		morsel_window_bytes: 4096,
		..ExecConfig::default()
	};
	assert_eq!(Placement::from_config(&config).loop_batch(&loop_facts, &facts()).max_bytes, 4096);
	assert_eq!(
		Placement::default().loop_batch(&loop_facts, &facts()).max_bytes,
		u32::try_from(ExecConfig::default().morsel_window_bytes).expect("the default window fits"),
	);
}
