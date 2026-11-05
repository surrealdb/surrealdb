//! `TopLevelExpr::read_only()` must over-approximate: it may call a read-only
//! statement writable, but never a writing statement read-only.
//!
//! The executor selects the transaction type from this predicate before the
//! statement is planned (`Executor::execute_plan_impl_inner`), so a clause it
//! fails to inspect lets a mutation reach a read-only transaction and fail
//! partway through. It also makes the same statement behave differently
//! depending on whether the caller wrapped it in `BEGIN`, which opens a write
//! transaction unconditionally.
//!
//! The predicate is judged against the engine that runs the statement. The
//! bytecode VM plans every statement to a program whose `access_mode()` is
//! `ReadWrite` when the program may write or has a side effect, and the
//! fixtures in the [`vm`] module plan each statement as the executor does,
//! in the transaction `read_only()` selects, and compare the two answers.

/// Parse one statement and check its top-level and expression predicates agree.
fn read_only(sql: &str) -> bool {
	let mut ast = crate::syn::parse(sql).expect("fixture should parse");
	assert_eq!(ast.expressions.len(), 1, "expected exactly one statement in {sql:?}");
	assert_agree(&ast.expressions.remove(0), sql)
}

/// Check the top-level transaction predicate against its expression payload.
fn assert_agree(runtime: &surrealdb_expr::parser::TopLevelExpr, context: &str) -> bool {
	let answer = runtime.read_only();
	let surrealdb_expr::parser::TopLevelExpr::Expr(expr) = runtime else {
		panic!("fixture is not a bare expression: {context}");
	};
	let converted = expr;
	assert_eq!(
		answer,
		converted.read_only(),
		"top-level and expression read-only predicates disagree on {context}"
	);
	answer
}

/// Assert that no case claims to be read-only, skipping shapes the grammar
/// rejects: a clause that cannot carry a mutation cannot be a hole.
///
/// Skips are reported rather than passed over silently, and at least one case
/// must run, so a grammar change that stops these fixtures parsing surfaces as
/// lost coverage instead of a still-green test.
fn assert_none_claim_read_only(cases: &[(&str, &str)]) {
	let mut exercised = 0;
	for (label, sql) in cases {
		if crate::syn::parse(sql).is_err() {
			println!("skipped (does not parse): {label}: {sql}");
			continue;
		}
		exercised += 1;
		assert!(!read_only(sql), "{label}: `{sql}` claimed to be read-only");
	}
	assert!(exercised > 0, "every fixture was skipped, so this test asserted nothing");
}

/// Every clause of a `SELECT` that can carry a user expression, each with a
/// mutation embedded in it. None of these may claim to be read-only.
const SELECT_CLAUSES: &[(&str, &str)] = &[
	("projection", "SELECT *, (CREATE log) AS w FROM person"),
	("what", "SELECT * FROM (CREATE log)"),
	("cond", "SELECT * FROM person WHERE (CREATE log).n > 0"),
	("omit", "SELECT * OMIT (CREATE log).n FROM person"),
	("limit", "SELECT * FROM person LIMIT (CREATE log).n"),
	("start", "SELECT * FROM person START (CREATE log).n"),
	("version", "SELECT * FROM person VERSION (CREATE log).n"),
	("timeout", "SELECT * FROM person TIMEOUT (CREATE log).n"),
	("fetch", "SELECT * FROM person FETCH tags[WHERE (CREATE log)]"),
	("split", "SELECT * FROM person SPLIT ON tags[WHERE (CREATE log)]"),
	("group", "SELECT tags FROM person GROUP BY tags[WHERE (CREATE log)]"),
	("order", "SELECT * FROM person ORDER BY tags[WHERE (CREATE log)]"),
];

/// The idiom parts that carry expressions, reached through a projection.
const IDIOM_PARTS: &[(&str, &str)] = &[
	("where filter", "SELECT tags[WHERE (CREATE log)] FROM person"),
	("value index", "SELECT tags[(CREATE log).n] FROM person"),
	("method argument", "SELECT tags.at((CREATE log).n) FROM person"),
	("destructure alias", "SELECT person.{ name, tag: tags[WHERE (CREATE log)] } FROM person"),
	("graph lookup cond", "SELECT ->likes[WHERE (CREATE log)]->thing FROM person"),
];

/// `INFO` names what it reports on with expressions, and takes a `VERSION`
/// expression, all of which the statement evaluates before it reads anything.
const INFO_POSITIONS: &[(&str, &str)] = &[
	("root version", "INFO FOR ROOT VERSION (CREATE log:1).id"),
	("ns version", "INFO FOR NS VERSION (CREATE log:1).id"),
	("db version", "INFO FOR DB VERSION (CREATE log:1).id"),
	("table version", "INFO FOR TABLE person VERSION (CREATE log:1).id"),
	("table name", "INFO FOR TABLE (CREATE log:1).id"),
	("user name", "INFO FOR USER (CREATE log:1).id"),
	("index name", "INFO FOR INDEX (CREATE log:1).id ON person"),
	("index table", "INFO FOR INDEX idx ON (CREATE log:1).id"),
];

/// A graph lookup's subject carries record-id range bounds, which are
/// record-id keys and so carry arbitrary expressions.
const LOOKUP_SUBJECTS: &[(&str, &str)] = &[
	("lookup range upper bound", "SELECT * FROM b:1->computed_edge:[6]..=[(CREATE log:1).id]"),
	("lookup range lower bound", "SELECT * FROM b:1->computed_edge:[(CREATE log:1).id]..=[6]"),
	("lookup subject in projection", "SELECT * FROM person:1->likes:[(CREATE log:2).id]..->post"),
];

/// `RETURN`'s fetch clause carries expressions just as a `SELECT`'s does.
const RETURN_FETCH: &[(&str, &str)] =
	&[("return fetch", "RETURN person FETCH tags[WHERE (CREATE log)]")];

/// Composite literals evaluate their element expressions in place.
const COMPOSITE_LITERALS: &[(&str, &str)] = &[
	("array literal", "RETURN [(CREATE log)]"),
	("set literal", "RETURN <set> [(CREATE log)]"),
	("object literal", "RETURN { w: (CREATE log) }"),
	("record id key", "RETURN r_thing:[(CREATE log).n]"),
	("record id range", "SELECT * FROM r_thing:[(CREATE log).n].."),
];

/// Closures execute bodies the statement text may not reveal. A call on
/// anything but a closure literal, a call argument, and a writing literal
/// body must all over-approximate to writable.
const CLOSURE_HOLES: &[(&str, &str)] = &[
	("call on a param", "RETURN $fn(1)"),
	("call argument", "RETURN (|$n| $n)((CREATE log).n)"),
	("writing literal body, called", "RETURN (|| { CREATE log })()"),
	("writing literal body, as a value", "RETURN [|| { CREATE log }]"),
	("writing literal body, as a builtin argument", "RETURN array::map([1], || { CREATE log })"),
	// A method call whose name is not a registered builtin resolves to a
	// closure stored on the receiver, whose body the statement text does
	// not reveal — through the idiom path and the postfix operator alike.
	("closure field invoked as a method", "RETURN $o.w()"),
	("closure field invoked as a postfix method", "RETURN ($o).w()"),
	("closure field invoked mid-idiom", "SELECT handlers.w().n FROM person"),
];

/// Statements that write wherever they appear: the baseline the clause
/// fixtures above extend.
const MUTATIONS: &[&str] =
	&["CREATE person", "UPDATE person SET a = 1", "DELETE person", "DEFINE TABLE person"];

/// Genuinely read-only statements, which must keep their read transaction.
/// Without these, "return false always" would satisfy every other test here
/// and cost every SELECT a write transaction.
const READ_ONLY_STATEMENTS: &[&str] = &[
	"SELECT * FROM person",
	"SELECT count() AS n FROM person GROUP ALL",
	"SELECT * FROM person WHERE age > 30 ORDER BY name LIMIT 10 START 5",
	"SELECT * FROM person FETCH friends",
	"SELECT * FROM person SPLIT ON tags",
	"SELECT name FROM person GROUP BY name",
	"SELECT * FROM person TIMEOUT 5s",
	"SELECT ->likes->thing FROM person",
	"SELECT tags[WHERE value > 1] FROM person",
	"RETURN person FETCH friends",
	"RETURN 1 + 2",
	// Registered builtin methods dispatch to builtins, so only their
	// receiver and arguments matter — including on receivers whose value
	// the plan cannot see, and with a pure literal closure argument.
	"RETURN $s.len()",
	"RETURN 'a'.len()",
	"SELECT tags.map(|$t| $t + 1) FROM person",
];

/// A call on a closure literal whose body is read-only keeps its read
/// transaction.
const PURE_CLOSURE_CALLS: &[&str] = &["RETURN (|$n: number| $n + 1)(1)", "SELECT * FROM (|| 1)()"];

#[test]
fn a_mutation_in_any_select_clause_requires_a_write_transaction() {
	assert_none_claim_read_only(SELECT_CLAUSES);
}

#[test]
fn a_mutation_in_any_idiom_part_requires_a_write_transaction() {
	assert_none_claim_read_only(IDIOM_PARTS);
}

#[test]
fn a_mutation_in_an_info_subject_or_version_requires_a_write_transaction() {
	assert_none_claim_read_only(INFO_POSITIONS);
}

#[test]
fn a_mutation_in_a_lookup_subject_requires_a_write_transaction() {
	assert_none_claim_read_only(LOOKUP_SUBJECTS);
}

#[test]
fn a_mutation_in_a_return_fetch_requires_a_write_transaction() {
	assert_none_claim_read_only(RETURN_FETCH);
}

#[test]
fn read_only_statements_keep_their_read_transaction() {
	for sql in READ_ONLY_STATEMENTS {
		assert!(read_only(sql), "`{sql}` should not need a write transaction");
	}
}

/// Mutations are writable wherever they appear, which is the baseline the
/// clause cases above extend.
#[test]
fn mutations_require_a_write_transaction() {
	for sql in MUTATIONS {
		assert!(!read_only(sql), "`{sql}` claimed to be read-only");
	}
}

/// Composite literals evaluate their element expressions in place, so a
/// mutation inside one needs a write transaction.
#[test]
fn a_mutation_in_a_composite_literal_requires_a_write_transaction() {
	assert_none_claim_read_only(COMPOSITE_LITERALS);
}

/// Closures execute bodies the statement text may not reveal. A call on
/// anything but a closure literal, a call argument, and a writing literal
/// body must all over-approximate to writable.
#[test]
fn closures_that_may_write_require_a_write_transaction() {
	assert_none_claim_read_only(CLOSURE_HOLES);
}

/// The precision that keeps the previous test honest: a call on a closure
/// literal whose body is read-only keeps its read transaction.
#[test]
fn a_pure_literal_closure_call_keeps_its_read_transaction() {
	for sql in PURE_CLOSURE_CALLS {
		assert!(read_only(sql), "`{sql}` should not need a write transaction");
	}
}

// ============================================================================
// The classification, checked against the programs the VM plans
// ============================================================================

/// What `read_only()` and the VM's access mode say of the same statement.
///
/// The executor picks the transaction from `read_only()` before it plans, so a
/// statement is planned here in the transaction that answer selects. Two
/// facts are asserted of every statement, and they are different questions:
///
/// - `read_only()` asks whether the statement writes storage. It may answer `false` for a statement
///   that only reads, and must never answer `true` for one that writes.
/// - `AccessMode::ReadWrite` on a program means it "may write **or have side effects**". A `SLEEP`
///   statement is the one construct whose program is `ReadWrite` while `read_only()` answers
///   `true`: the side effect is the pause, and it touches no key. Every other `ReadWrite` program
///   belongs to a statement `read_only()` calls writable.
///
/// The VM derives a program's mode while it lowers: a write, a deferred
/// statement, a nested statement, a call whose body may write, a closure
/// body and a closure invoked through a value each make it `ReadWrite`. The
/// derivation does not report which of them did, so the tests establish that
/// it reaches a position by pairing each statement with its twin that has
/// nothing in the position, and requiring that the two modes differ.
///
/// A statement the VM declines is a statement that fails to plan, so it
/// runs on no transaction at all and cannot reach a read-only one. A
/// position the VM declines is reported rather than counted as reached.
#[cfg(feature = "kv-mem")]
mod vm {
	use std::collections::BTreeSet;
	use std::ops::ControlFlow;
	use std::path::{Path, PathBuf};
	use std::sync::Arc;

	use crate::corpus_slices::{assert_slices_cover, slice, sliced_tests};
	use crate::err::Error;
	use crate::exec::operators::test_util::TestDb;
	use crate::exec::{AccessMode, ExecOperator, ExecutionContext};
	use crate::expr::logical::{LogicalExpr, LogicalOp};
	use crate::kvs::TransactionType;

	/// What the VM made of a statement.
	#[derive(Debug)]
	enum Outcome {
		/// The statement planned to a program of this access mode.
		Program(AccessMode),
		/// The statement failed to plan.
		Declined(Error),
	}

	impl Outcome {
		/// Whether this is the VM declining a statement it does not cover,
		/// as opposed to some other planning failure.
		fn is_vm_decline(&self) -> bool {
			matches!(
				self,
				Outcome::Declined(Error::Exec(crate::exec::Error::PlannerUnsupported { .. }))
			)
		}

		/// Whether this is the plan refusing a statement in a clause whose region does not admit
		/// it, such as a write in a `WHERE` clause: the request is refused before any statement
		/// of it runs, so no program holds the statement. The planner's own classifier
		/// ([`request_rejection`](crate::exec::ir::logical::constraints::request_rejection))
		/// tells a region rejection from any other invalid statement.
		fn is_region_refusal(&self) -> bool {
			match self {
				Outcome::Declined(error) => {
					crate::exec::ir::logical::constraints::request_rejection(error).is_some()
				}
				Outcome::Program(_) => false,
			}
		}
	}

	/// A statement as the executor plans it.
	struct Planned {
		/// `read_only()` of the statement: the answer that chose the transaction.
		read_only: bool,
		outcome: Outcome,
	}

	/// A datastore and one execution context per transaction type, so a
	/// statement is planned in the transaction `read_only()` selects.
	struct Harness {
		read: ExecutionContext,
		write: ExecutionContext,
		_db: TestDb,
	}

	impl Harness {
		/// A harness over a datastore that has run `setup`.
		async fn new(setup: &str) -> Self {
			let db = TestDb::new(setup).await;
			let read = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
			let write = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
			Self {
				read,
				write,
				_db: db,
			}
		}

		/// The context of the transaction a statement with this
		/// `read_only()` runs in.
		fn context(&self, read_only: bool) -> &ExecutionContext {
			if read_only {
				&self.read
			} else {
				&self.write
			}
		}

		/// Lower a bare expression as the executor does
		/// (`Executor::lower_bare_expression`).
		fn lower(expr: &crate::expr::Expr) -> Result<LogicalOp, Error> {
			if let crate::expr::Expr::Select(select) = expr {
				crate::dbs::validate_for_update(select)?;
			}
			crate::expr::logical::lower(expr.clone()).map_err(|reason| {
				crate::exec::Error::PlannerUnsupported {
					reason: reason.to_string(),
				}
				.into()
			})
		}

		/// Bind and plan `op` as the executor does
		/// (`Executor::plan_bare_expression`), in the transaction that
		/// `read_only` selects.
		async fn plan_op(
			&self,
			read_only: bool,
			op: LogicalOp,
		) -> Result<Arc<dyn ExecOperator>, Error> {
			let ctx = self.context(read_only);
			crate::exec::planner::plan_statement_inner(
				op,
				ctx.ctx(),
				ctx.function_registry(),
				ctx.try_txn().expect("the harness holds a transaction"),
				Some(Arc::clone(&ctx.root().auth)),
				0,
				ctx.options().map(|options| Arc::new(options.clone())),
			)
			.await
		}

		/// Parse and plan one bare expression statement.
		async fn plan(&self, sql: &str) -> Planned {
			let mut ast = crate::syn::parse(sql)
				.unwrap_or_else(|e| panic!("fixture should parse: {sql}: {e}"));
			assert_eq!(ast.expressions.len(), 1, "expected exactly one statement in {sql:?}");
			let top = ast.expressions.remove(0);
			let read_only = super::assert_agree(&top, sql);
			let surrealdb_expr::parser::TopLevelExpr::Expr(expr) = top else {
				panic!("fixture is not a bare expression: {sql}");
			};
			let outcome = match Self::lower(&expr) {
				Err(e) => Outcome::Declined(e),
				Ok(op) => match self.plan_op(read_only, op).await {
					Ok(plan) => Outcome::Program(plan.access_mode()),
					Err(e) => Outcome::Declined(e),
				},
			};
			Planned {
				read_only,
				outcome,
			}
		}
	}

	// ------------------------------------------------------------------------
	// The statements the surface fixtures name, planned
	// ------------------------------------------------------------------------

	/// Clause fixtures whose nested statement the VM never evaluates, so the
	/// program is `ReadOnly` because the statement does not write.
	///
	/// `OMIT` and `FETCH` name the fields they act on by path, and the VM
	/// resolves the path without evaluating the expression a path part
	/// holds. The classification stays writable, which is the safe direction;
	/// what the exception needs is evidence that nothing is written, which
	/// [`NOT_EVALUATED_RUNS`] gathers by running them.
	const NOT_EVALUATED: &[&str] = &[
		"SELECT * OMIT (CREATE log).n FROM person",
		"SELECT * FROM person FETCH tags[WHERE (CREATE log)]",
		"RETURN person FETCH tags[WHERE (CREATE log)]",
	];

	/// The statements run to show that [`NOT_EVALUATED`] writes nothing: each
	/// fixture, against a table that holds a row whose `tags` hold a record
	/// link, so a fetch has something to follow, and a by-id form of the
	/// `RETURN`.
	const NOT_EVALUATED_RUNS: &[&str] = &[
		"SELECT * OMIT (CREATE log).n FROM person",
		"SELECT * FROM person FETCH tags[WHERE (CREATE log)]",
		"RETURN person FETCH tags[WHERE (CREATE log)]",
		"RETURN person:1 FETCH tags[WHERE (CREATE log)]",
	];

	/// Every clause the surface fixtures put a mutation in, planned: a
	/// statement that plans is `ReadWrite`, except where the VM does not
	/// evaluate the clause ([`NOT_EVALUATED`]). These are the same statements
	/// the surface tests assert `read_only()` calls writable, so a pair of
	/// answers that disagrees in the unsafe direction — writable program,
	/// read-only classification — cannot hide in a clause only one side reads.
	#[tokio::test]
	async fn every_clause_that_requires_a_write_transaction_plans_read_write() {
		let h = Harness::new("").await;
		let mut planned = 0;
		let mut declined = Vec::new();
		let mut read_only_programs = Vec::new();
		for (list, cases) in [
			("select clauses", super::SELECT_CLAUSES),
			("idiom parts", super::IDIOM_PARTS),
			("info positions", super::INFO_POSITIONS),
			("lookup subjects", super::LOOKUP_SUBJECTS),
			("return fetch", super::RETURN_FETCH),
			("composite literals", super::COMPOSITE_LITERALS),
			("closure holes", super::CLOSURE_HOLES),
		] {
			for (label, sql) in cases {
				if crate::syn::parse(sql).is_err() {
					println!("skipped (does not parse): {label}: {sql}");
					continue;
				}
				let result = h.plan(sql).await;
				assert!(!result.read_only, "{list}: {label}: `{sql}` claimed to be read-only");
				match result.outcome {
					Outcome::Program(AccessMode::ReadWrite) => planned += 1,
					Outcome::Program(AccessMode::ReadOnly) => read_only_programs.push(*sql),
					Outcome::Declined(e) => declined.push(format!("{list}: {label}: {e:#}")),
				}
			}
		}
		println!("{planned} clause fixtures planned to ReadWrite, {} declined:", declined.len());
		for line in &declined {
			println!("  {line}");
		}
		assert!(planned > 0, "no clause fixture planned, so this test asserted nothing");

		let unreached: Vec<&&str> =
			read_only_programs.iter().filter(|sql| !NOT_EVALUATED.contains(sql)).collect();
		assert!(
			unreached.is_empty(),
			"these statements hold a write but planned to a ReadOnly program, so the derivation \
			 did not reach it: {unreached:#?}"
		);
		let stale: Vec<&&str> =
			NOT_EVALUATED.iter().filter(|sql| !read_only_programs.contains(sql)).collect();
		assert!(
			stale.is_empty(),
			"these planned to something other than ReadOnly, so drop them from the list: {stale:#?}"
		);

		// The exception holds only if running them writes nothing.
		let db = TestDb::new("CREATE person:1 SET tags = [person:1, 2, 'x'];").await;
		for sql in NOT_EVALUATED_RUNS {
			for response in db
				.datastore()
				.execute(sql, &TestDb::owner(), None)
				.await
				.expect("the statement should execute")
			{
				response.result.unwrap_or_else(|e| panic!("`{sql}` should run: {e}"));
			}
		}
		let log = db
			.datastore()
			.execute("SELECT * FROM log", &TestDb::owner(), None)
			.await
			.expect("the statement should execute")
			.remove(0)
			.result;
		let error =
			log.expect_err("the statements wrote to `log`, which they were to leave absent");
		assert!(
			error.to_string().contains("does not exist"),
			"`log` should be absent after the statements that do not evaluate their clause: {error}"
		);
	}

	/// The baseline mutations plan to `ReadWrite`, and the statements that
	/// keep their read transaction plan to `ReadOnly`: a program that is
	/// `ReadWrite` exactly when the statement writes.
	#[tokio::test]
	async fn the_program_is_read_write_exactly_when_the_statement_writes() {
		let h = Harness::new("").await;
		for sql in super::MUTATIONS {
			let result = h.plan(sql).await;
			assert!(!result.read_only, "`{sql}` claimed to be read-only");
			assert!(
				matches!(result.outcome, Outcome::Program(AccessMode::ReadWrite)),
				"`{sql}` writes, so it must plan to ReadWrite: {:?}",
				result.outcome
			);
		}
		for sql in super::READ_ONLY_STATEMENTS.iter().chain(super::PURE_CLOSURE_CALLS) {
			let result = h.plan(sql).await;
			assert!(
				matches!(result.outcome, Outcome::Program(AccessMode::ReadOnly)),
				"`{sql}` only reads, so it must plan to ReadOnly: {:?}",
				result.outcome
			);
		}
	}

	// ------------------------------------------------------------------------
	// A body the statement does not show
	// ------------------------------------------------------------------------

	/// A statement that runs a body its text does not contain — a stored
	/// function, a statement given as text, a closure held in a value — is
	/// `ReadWrite` as a program and writable as a classification, whatever
	/// the body turns out to do.
	///
	/// A stored function the program can read is not opaque: the VM takes its
	/// mode from the definition, so a function that only reads leaves the
	/// program `ReadOnly` while the classification, which cannot see it,
	/// stays writable.
	#[tokio::test]
	async fn a_body_the_statement_does_not_show_is_read_write_in_the_program() {
		let h = Harness::new(
			"DEFINE FUNCTION fn::writes() { CREATE log }; DEFINE FUNCTION fn::reads() { RETURN 1 };",
		)
		.await;
		for sql in [
			"RETURN fn::writes()",
			"RETURN [fn::writes()]",
			"RETURN array::map([1], |$x| fn::writes())",
			"RETURN eval::surql('CREATE log')",
			"RETURN $fn(1)",
			"RETURN $o.w()",
			"RETURN ($o).w()",
		] {
			let result = h.plan(sql).await;
			assert!(
				!result.read_only,
				"`{sql}` runs a body it does not show but claimed read-only"
			);
			assert!(
				matches!(result.outcome, Outcome::Program(AccessMode::ReadWrite)),
				"`{sql}` runs a body it does not show, so it must plan to ReadWrite: {:?}",
				result.outcome
			);
		}
		let reads = h.plan("RETURN fn::reads()").await;
		assert!(!reads.read_only, "a call the classifier cannot see into must be writable");
		assert!(
			matches!(reads.outcome, Outcome::Program(AccessMode::ReadOnly)),
			"a stored function that only reads leaves the program ReadOnly: {:?}",
			reads.outcome
		);
	}

	// ------------------------------------------------------------------------
	// Positions: where a nested statement can sit
	// ------------------------------------------------------------------------

	/// The mark in a template where a nested statement goes.
	const HOLE: &str = "%%";
	/// A statement that writes storage.
	const WRITES: &str = "CREATE log";
	/// A statement whose program is `ReadWrite` and whose classification is
	/// read-only: the documented over-approximation.
	const SLEEPS: &str = "SLEEP 1s";
	/// Nothing: an expression with no effect, the twin of the other two.
	const INERT: &str = "1";

	/// A statement with a hole where a nested statement goes.
	struct Position {
		label: &'static str,
		template: &'static str,
	}

	const fn at(label: &'static str, template: &'static str) -> Position {
		Position {
			label,
			template,
		}
	}

	/// The verdict on a position whose hole holds nothing: the statement
	/// classifies as read-only and plans to `ReadOnly`, so a difference in the
	/// statements with something in the hole is that statement's alone.
	fn judge_inert(label: &str, sql: &str, planned: &Planned) -> Result<(), String> {
		if !planned.read_only {
			return Err(format!("{label}: `{sql}` must claim read_only()"));
		}
		match planned.outcome {
			Outcome::Program(AccessMode::ReadOnly) => Ok(()),
			ref outcome @ (Outcome::Program(_) | Outcome::Declined(_)) => Err(format!(
				"{label}: `{sql}` has nothing nested, so it must plan to ReadOnly: {outcome:?}"
			)),
		}
	}

	/// What a position's statement came to once it held a nested statement.
	#[derive(Debug, PartialEq)]
	enum Reach {
		/// The program is `ReadWrite`: the derivation reached the position.
		Reached,
		/// The VM declined the statement, or the plan refused it in its region, so no program
		/// holds it.
		Declined,
	}

	/// The verdict on a position whose hole holds `nested`, which `writes` or
	/// sleeps.
	///
	/// - With a write the classification is writable and the program, if it plans, is `ReadWrite`.
	/// - With a sleep the classification is read-only, the documented over-approximation, and the
	///   program, if it plans, is `ReadWrite`.
	///
	/// A program that planned `ReadOnly` with a statement in the hole is the
	/// failure: the derivation did not reach the position. So is a write the
	/// classification calls read-only.
	fn judge_nested(
		label: &str,
		sql: &str,
		nested: &str,
		writes: bool,
		planned: &Planned,
	) -> Result<Reach, String> {
		if planned.read_only == writes {
			return Err(format!(
				"{label}: `{sql}` must classify as {}",
				if writes {
					"writable"
				} else {
					"read-only"
				}
			));
		}
		match &planned.outcome {
			Outcome::Program(AccessMode::ReadWrite) => Ok(Reach::Reached),
			Outcome::Program(AccessMode::ReadOnly) => Err(format!(
				"{label}: `{sql}` holds `{nested}` but planned to a ReadOnly program, so the \
				 derivation did not reach the position"
			)),
			outcome @ Outcome::Declined(_)
				if outcome.is_vm_decline() || outcome.is_region_refusal() =>
			{
				Ok(Reach::Declined)
			}
			outcome @ Outcome::Declined(_) => Err(format!(
				"{label}: `{sql}` failed to plan for a reason other than the VM declining it or \
				 its region refusing it: {outcome:?}"
			)),
		}
	}

	/// Fill the hole of every position three ways — with a write, with a
	/// sleep and with nothing — and judge the VM's answer to each
	/// ([`judge_inert`], [`judge_nested`]). A position the VM declines with a
	/// statement in it is reported rather than counted as reached.
	async fn assert_nested_statements_are_reached(h: &Harness, positions: &[Position]) {
		let mut reached = 0;
		for position in positions {
			let label = position.label;
			let inert_sql = position.template.replace(HOLE, INERT);
			let inert = h.plan(&inert_sql).await;
			judge_inert(label, &inert_sql, &inert).unwrap_or_else(|failure| panic!("{failure}"));
			for (nested, writes) in [(WRITES, true), (SLEEPS, false)] {
				let sql = position.template.replace(HOLE, nested);
				let planned = h.plan(&sql).await;
				match judge_nested(label, &sql, nested, writes, &planned) {
					Ok(Reach::Reached) => reached += 1,
					Ok(Reach::Declined) => println!(
						"declined by the VM or refused by its region (no program, so nothing \
						 runs): {label} with `{nested}`"
					),
					Err(failure) => panic!("{failure}"),
				}
			}
		}
		assert!(reached > 0, "no position planned, so these fixtures asserted nothing");
	}

	/// The verdicts reject what they are shaped to reject: a program that
	/// holds a statement and plans `ReadOnly`, a write the classification calls
	/// read-only, and a program that writes with nothing nested.
	///
	/// These run the verdict functions on the answers a defective VM or
	/// classifier would give, so a verdict that accepted everything fails here
	/// rather than leaving every position test green.
	#[test]
	fn the_verdicts_reject_the_answers_a_defective_engine_gives() {
		let planned = |read_only, mode| Planned {
			read_only,
			outcome: Outcome::Program(mode),
		};

		// The derivation missed the nested statement: a `ReadOnly` program.
		assert!(
			judge_nested("p", "s", WRITES, true, &planned(false, AccessMode::ReadOnly)).is_err()
		);
		assert!(
			judge_nested("p", "s", SLEEPS, false, &planned(true, AccessMode::ReadOnly)).is_err()
		);
		// The classification missed the write.
		assert!(
			judge_nested("p", "s", WRITES, true, &planned(true, AccessMode::ReadWrite)).is_err()
		);
		// The exception is a sleep the classification calls read-only; a sleep it
		// calls writable is not the shape the exception documents.
		assert!(
			judge_nested("p", "s", SLEEPS, false, &planned(false, AccessMode::ReadWrite)).is_err()
		);
		// The documented answers are accepted.
		assert_eq!(
			judge_nested("p", "s", WRITES, true, &planned(false, AccessMode::ReadWrite)),
			Ok(Reach::Reached)
		);
		assert_eq!(
			judge_nested("p", "s", SLEEPS, false, &planned(true, AccessMode::ReadWrite)),
			Ok(Reach::Reached)
		);
		// A program that writes with nothing nested, or a classification that
		// calls the empty twin writable, is not the twin the pairing needs.
		assert!(judge_inert("p", "s", &planned(true, AccessMode::ReadWrite)).is_err());
		assert!(judge_inert("p", "s", &planned(false, AccessMode::ReadOnly)).is_err());
		assert!(judge_inert("p", "s", &planned(true, AccessMode::ReadOnly)).is_ok());
	}

	/// Positions in a call, a closure or a branch.
	const CALL_AND_CLOSURE_POSITIONS: &[Position] = &[
		at("builtin call argument", "array::any([(%%)])"),
		at("method argument", "RETURN [1].at((%%))"),
		at("closure literal body", "array::map([1], |$x| { %% })"),
		at("closure literal body, invoked", "RETURN (|| (%%))()"),
		at("argument of an invoked closure literal", "RETURN (|$n| $n)((%%))"),
		at("conditional branch", "IF $c THEN (%%) ELSE 1 END"),
		at("conditional condition", "RETURN IF (%%) THEN 1 ELSE 2 END"),
	];

	/// Positions in a clause a plan node holds beside its input: the four
	/// clauses of a `SELECT` that take a bare expression, a recursion's body
	/// and base, and a KNN operand.
	const CLAUSE_POSITIONS: &[Position] = &[
		at("limit", "SELECT * FROM person LIMIT { %%; 1 }"),
		at("start", "SELECT * FROM person START { %%; 0 }"),
		at("version", "SELECT * FROM person VERSION { %%; d'2024-01-01T00:00:00Z' }"),
		at("timeout", "SELECT * FROM person TIMEOUT { %%; 1s }"),
		at("recursion base", "RETURN (%%).{1..2}(.b)"),
		at("recursion body", "RETURN a.{1..2}(.b[WHERE (%%)])"),
		// The operand is an array, so the comparison stays a KNN rather than a
		// filter; the vector is evaluated where the search opens.
		at(
			"KNN query vector",
			"RETURN (SELECT * FROM [{ embedding: [1, 2] }] \
			 WHERE embedding <|1,EUCLIDEAN|> [(%%), 2])",
		),
	];

	/// Positions in a container a value is built from, or a path's parts.
	const CONTAINER_AND_PATH_POSITIONS: &[Position] = &[
		at("array literal element", "RETURN [(%%)]"),
		at("set literal element", "RETURN { (%%), 1 }"),
		at("object literal value", "RETURN { w: (%%) }"),
		// A record-id key, in each of the three shapes that carry expressions.
		at("record id array key", "RETURN r_thing:[(%%)]"),
		at("record id object key", "RETURN r_thing:{ k: (%%) }"),
		at("record id range bound", "RETURN r_thing:[1]..=[(%%)]"),
		// A destructure body, at the top level and nested in another.
		at("destructure body", "RETURN { a: 1 }.{ b: (%%) }"),
		at("nested destructure body", "RETURN { a: { b: 1 } }.{ a.{ c: (%%) } }"),
		at("destructure base", "RETURN (%%).{ a }"),
		// The same, reached through an array continuation rather than a run.
		at("destructure through an array continuation", "RETURN [{ a: 1 }].*.{ b: (%%) }"),
		at("filter predicate", "RETURN [1, 2][WHERE (%%)]"),
		at("filter base", "RETURN (%%)[WHERE true]"),
		at("field access base", "RETURN (%%).n"),
		at("index base", "RETURN (%%)[1]"),
		at("index expression", "RETURN [1][(%%)]"),
		at("last element base", "RETURN (%%)[$]"),
		at("each base", "RETURN (%%).*"),
		at("flatten base", "RETURN (%%)..."),
		at("method receiver", "RETURN (%%).len()"),
		at("optional chain base", "RETURN (%%).?.x"),
		at("optional chain continuation", "RETURN a.?.b[WHERE (%%)]"),
		at("lookup base", "RETURN (%%)->likes->post"),
		at("lookup condition", "RETURN a->likes[WHERE (%%)]->post"),
		at("array continuation body", "RETURN [{ a: 1 }].a.b[WHERE (%%)]"),
		at("recursion marker base", "RETURN person:a.{..2}.{ name, r: (%%)->x->person.@ }"),
	];

	/// Positions in an expression a statement owns: its predicate, its
	/// projection, its operands and its sub-statements.
	const OWNED_EXPRESSION_POSITIONS: &[Position] = &[
		at("where predicate", "SELECT VALUE 1 FROM [1] WHERE (%%) = NONE"),
		at("projection field", "SELECT (%%) AS a FROM [1]"),
		at("projection value", "SELECT VALUE (%%) FROM [1]"),
		at("returned value", "RETURN (%%)"),
		at("thrown value", "THROW (%%)"),
		at("thrown value in value position", "RETURN THROW (%%)"),
		at("variable value", "{ LET $x = (%%); RETURN $x }"),
		at("loop source", "FOR $i IN (%%) { RETURN 1 }"),
		at("loop body", "FOR $i IN [1] { RETURN (%%) }"),
		at("prefix operand", "RETURN -(%%)"),
		at("negation operand", "RETURN !(%%)"),
		at("postfix range operand", "RETURN (%%).."),
		at("binary left operand", "RETURN (%%) + 1"),
		at("binary right operand", "RETURN 1 IN [(%%)]"),
		at("range end", "RETURN 1..(%%)"),
		at("coalesce operand", "RETURN (%%) ?? 1"),
		at("function argument", "RETURN type::record('a', (%%))"),
	];

	/// A nested statement is reached in each position of a call or a
	/// closure literal, whose arguments and bodies are expressions of their
	/// own rather than parts of the statement.
	#[tokio::test]
	async fn an_origin_nested_in_a_call_or_a_closure_is_reached() {
		let h = Harness::new("").await;
		assert_nested_statements_are_reached(&h, CALL_AND_CLOSURE_POSITIONS).await;
	}

	/// A nested statement is reached in a clause a plan node holds beside its
	/// input, where the program keeps the expression apart from the rows.
	///
	/// `limit`, `start`, `version` and `timeout` are the four clauses of a
	/// `SELECT` that take a bare expression, and a mutation in any of them
	/// must open a write transaction (`ADR 0001`, Decision 1). A recursion's
	/// body and a KNN operand are evaluated where the node opens rather than
	/// per row, so a derivation that read only the per-row instructions would
	/// miss them.
	#[tokio::test]
	async fn an_origin_an_operator_owns_but_does_not_print_is_reached() {
		let h = Harness::new("").await;
		assert_nested_statements_are_reached(&h, CLAUSE_POSITIONS).await;
	}

	/// A nested statement is reached when a container or a path's parts hold it.
	///
	/// A record-id key keeps its operands in a type of its own, and a path's
	/// parts are evaluated one step at a time, each over the value the step
	/// before produced; a destructure's field bodies and a filter's predicate
	/// are evaluated against an element rather than the row. A position the VM
	/// cannot compile an instruction for is declined, so no program holds a
	/// statement the derivation did not see.
	#[tokio::test]
	async fn an_origin_a_container_or_a_path_holds_is_reached() {
		let h = Harness::new("").await;
		assert_nested_statements_are_reached(&h, CONTAINER_AND_PATH_POSITIONS).await;
	}

	/// A nested statement is reached inside an expression a statement owns.
	///
	/// The mode of a statement is the join over every expression it owns, and
	/// an expression can wrap a whole statement. A predicate holding a `SLEEP`
	/// makes the program `ReadWrite`, and a predicate holding a write makes
	/// it `ReadWrite` and the statement writable.
	#[tokio::test]
	async fn an_origin_inside_an_owned_expression_is_reached() {
		let h = Harness::new("").await;
		assert_nested_statements_are_reached(&h, OWNED_EXPRESSION_POSITIONS).await;
	}

	/// Every origin of a `ReadWrite` program is inspected, not the first one
	/// found: a statement with two independent origins is `ReadWrite` with
	/// either of them alone, and `ReadOnly` with neither.
	///
	/// `{ F; SELECT (F) AS a FROM [1]; }` holds one origin as a statement of
	/// the block and another inside a projection's expression. Each is
	/// sufficient on its own, so a derivation that stopped at the first
	/// origin, or that ignored the owned expression, is `ReadOnly` for one of
	/// the three statements that hold an origin.
	#[tokio::test]
	async fn every_read_write_origin_is_inspected() {
		let h = Harness::new("").await;
		for (nested, writes) in [(WRITES, true), (SLEEPS, false)] {
			for (label, first, second, expected) in [
				("both origins", nested, nested, AccessMode::ReadWrite),
				("the block statement alone", nested, INERT, AccessMode::ReadWrite),
				("the projection alone", INERT, nested, AccessMode::ReadWrite),
				("neither origin", INERT, INERT, AccessMode::ReadOnly),
			] {
				let sql = format!("{{ {first}; SELECT ({second}) AS a FROM [1]; }}");
				let result = h.plan(&sql).await;
				let holds_origin = expected == AccessMode::ReadWrite;
				assert_eq!(
					result.read_only,
					!(writes && holds_origin),
					"{label}: `{sql}` classified {}",
					if result.read_only {
						"read-only"
					} else {
						"writable"
					}
				);
				match result.outcome {
					Outcome::Program(mode) => assert_eq!(
						mode, expected,
						"{label}: `{sql}` planned to {mode:?}, expected {expected:?}"
					),
					Outcome::Declined(e) => panic!("{label}: `{sql}` should plan: {e:#}"),
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	// Every expression form that nests another has a position
	// ------------------------------------------------------------------------

	/// The variants of `LogicalExpr` that are not leaves, read from the
	/// enum's declaration, each with whether it is spelled `Name(..)` or
	/// `Name { .. }` (only the name is used).
	///
	/// A variant is a leaf when it carries nothing or a payload that is none
	/// of the plan's expression holders; any other variant holds an
	/// expression, directly or through a type of its own, and is a position a
	/// nested statement can sit in.
	fn nesting_variants() -> Vec<String> {
		/// Payload types that hold no expression.
		const LEAF_PAYLOADS: &[&str] =
			&["", "LogicalLiteral", "Param", "TableName", "Mock", "Constant", "Strand"];

		let path: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
			.join("..")
			.join("expr")
			.join("src")
			.join("expr")
			.join("logical")
			.join("expr.rs");
		let src = std::fs::read_to_string(&path)
			.unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
		// Doc comments hold braces and commas; the declaration is read without them.
		let code = src
			.lines()
			.filter(|line| !line.trim_start().starts_with("//"))
			.collect::<Vec<_>>()
			.join("\n");
		let header = "pub enum LogicalExpr {";
		let start = code.find(header).expect("LogicalExpr is declared here") + header.len();
		let mut body = String::new();
		let mut depth = 1i32;
		for ch in code[start..].chars() {
			match ch {
				'{' => depth += 1,
				'}' => {
					depth -= 1;
					if depth == 0 {
						break;
					}
				}
				_ => {}
			}
			body.push(ch);
		}
		let mut variants = Vec::new();
		let (mut depth, mut current) = (0i32, String::new());
		for ch in body.chars() {
			match ch {
				'(' | '{' | '[' | '<' => depth += 1,
				')' | '}' | ']' | '>' => depth -= 1,
				',' if depth == 0 => {
					variants.push(std::mem::take(&mut current));
					continue;
				}
				_ => {}
			}
			current.push(ch);
		}
		variants.push(current);

		let mut nesting = Vec::new();
		for variant in variants {
			let variant = variant.trim();
			if variant.is_empty() {
				continue;
			}
			let name: String =
				variant.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
			let payload = variant[name.len()..].trim().trim_matches(|c| c == '(' || c == ')');
			if !LEAF_PAYLOADS.contains(&payload.trim()) {
				nesting.push(name);
			}
		}
		assert!(nesting.len() > 10, "the scan of LogicalExpr found {nesting:?}");
		nesting
	}

	/// The name of an expression's variant.
	fn variant_name(expr: &LogicalExpr) -> String {
		format!("{expr:?}").chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect()
	}

	/// The variants of `LogicalExpr` that no statement can reach a nested
	/// statement through, each with the reason.
	const UNREACHABLE_VARIANTS: &[(&str, &str)] = &[
		("First", "the parser writes `[0]` as an index, so no source text lowers to it"),
		("SetField", "a rebuilt value with no surface syntax"),
	];

	/// Every expression form that nests another expression is a position a
	/// fixture here puts a statement in, and the derivation of the program's
	/// mode is checked there.
	///
	/// The set of forms is read from the declaration of `LogicalExpr`, so a
	/// variant added to it surfaces here as a form with no fixture rather
	/// than as a position nobody tried. A form that no source text can
	/// express is listed in [`UNREACHABLE_VARIANTS`] with the reason, and the
	/// list is checked from the other side: an entry some fixture does reach
	/// is stale.
	#[test]
	fn every_expression_form_that_nests_another_has_a_fixture_position() {
		let mut fixtures: Vec<String> = [
			CALL_AND_CLOSURE_POSITIONS,
			CLAUSE_POSITIONS,
			CONTAINER_AND_PATH_POSITIONS,
			OWNED_EXPRESSION_POSITIONS,
		]
		.into_iter()
		.flatten()
		.map(|position| position.template.replace(HOLE, WRITES))
		.collect();
		for cases in [
			super::SELECT_CLAUSES,
			super::IDIOM_PARTS,
			super::INFO_POSITIONS,
			super::LOOKUP_SUBJECTS,
			super::RETURN_FETCH,
			super::COMPOSITE_LITERALS,
			super::CLOSURE_HOLES,
		] {
			fixtures.extend(cases.iter().map(|(_, sql)| (*sql).to_owned()));
		}

		let mut reached = BTreeSet::new();
		let mut lowered = 0;
		for sql in &fixtures {
			let Ok(mut ast) = crate::syn::parse(sql) else {
				continue;
			};
			let Some(surrealdb_expr::parser::TopLevelExpr::Expr(expr)) = ast.expressions.pop()
			else {
				continue;
			};
			let Ok(op) = Harness::lower(&expr) else {
				continue;
			};
			lowered += 1;
			let _ = op.visit_scalars_deep(&mut |scalar| {
				let _ = scalar.visit_deep(&mut |e| {
					reached.insert(variant_name(e));
					ControlFlow::<()>::Continue(())
				});
				Ok::<(), std::convert::Infallible>(())
			});
		}
		assert!(lowered > 0, "no fixture lowered, so this test asserted nothing");

		let unreachable: BTreeSet<&str> =
			UNREACHABLE_VARIANTS.iter().map(|(name, _)| *name).collect();
		let missing: Vec<String> = nesting_variants()
			.into_iter()
			.filter(|name| !reached.contains(name) && !unreachable.contains(name.as_str()))
			.collect();
		assert!(
			missing.is_empty(),
			"no fixture position puts a statement under these expression forms: {missing:?}"
		);
		let stale: Vec<&&str> =
			unreachable.iter().filter(|name| reached.contains(**name)).collect();
		assert!(stale.is_empty(), "a fixture reaches these, so drop them from the list: {stale:?}");
	}

	// ------------------------------------------------------------------------
	// The corpus sweep
	// ------------------------------------------------------------------------

	/// Walk every `.surql` file under `dir`, calling `f` with its contents.
	fn walk_surql(dir: &Path, f: &mut impl FnMut(&Path, &str)) {
		let entries =
			std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display()));
		for entry in entries {
			let path = entry.expect("directory entry should be readable").path();
			if path.is_dir() {
				walk_surql(&path, f);
			} else if path.extension().and_then(|x| x.to_str()) == Some("surql") {
				let source = std::fs::read_to_string(&path)
					.unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
				f(&path, &source);
			}
		}
	}

	/// Whether a statement sleeps: it holds a `SLEEP` statement, in its own
	/// operators, in a statement nested in one of its expressions, or in a
	/// closure body.
	fn holds_sleep(op: &LogicalOp) -> bool {
		let mut found = false;
		let _ = op.visit_ops_deep(&mut |node| {
			found |= matches!(node, LogicalOp::Sleep(_));
			ControlFlow::<()>::Continue(())
		});
		// `visit_ops_deep` does not enter a closure body, and `visit` does not
		// enter a subquery: a sleep behind either is found by following the
		// expression to the statements it holds.
		let _ = op.visit_scalars_deep(&mut |scalar| {
			let _ = scalar.visit(&mut |e| {
				if let LogicalExpr::Subquery(nested) = e {
					found |= holds_sleep(nested);
				}
				ControlFlow::<()>::Continue(())
			});
			Ok::<(), std::convert::Infallible>(())
		});
		found
	}

	/// Whether a program that is `ReadWrite` under a read-only classification
	/// is accounted for by the one documented over-approximation: the
	/// statement sleeps, and names no write.
	///
	/// "Names no write" is the logical plan's own footprint, derived from the
	/// plan rather than from the program or the surface syntax, so a write
	/// hidden beside a sleep is not excused by it.
	fn explained_by_sleep(op: &LogicalOp, h: &Harness) -> bool {
		holds_sleep(op)
			&& crate::exec::logical::footprint::of(op, h.read.function_registry()).writes.is_empty()
	}

	/// Whether a statement the VM planned violates the contract: its program is
	/// `ReadWrite`, its classification is read-only, and nothing but a sleep
	/// accounts for the difference.
	fn is_unexplained(read_only: bool, mode: AccessMode, explained_by_sleep: bool) -> bool {
		mode == AccessMode::ReadWrite && read_only && !explained_by_sleep
	}

	/// Counts from one corpus sweep, reported so a sweep that stops reaching
	/// the property shows up as lost coverage rather than as a still-green
	/// test.
	#[derive(Default)]
	struct Sweep {
		/// Statements that planned to a program.
		planned: usize,
		/// Of those, the ones whose program is `ReadWrite`, the half of the
		/// implication with something to prove.
		writing: usize,
		/// Of those, the ones `read_only()` called read-only, every one of which
		/// [`explained_by_sleep`] accounts for.
		over_approximated: usize,
		/// Statements that failed to plan. They run on no transaction, so
		/// they are not part of the property.
		declined: usize,
		/// Files the parser rejected, which the parser's own corpus test owns.
		unparsed: usize,
	}

	/// The exception excuses a sleep and nothing else: a statement that holds
	/// a sleep and also names a write is not explained by it, wherever the
	/// sleep sits, and the verdict flags a `ReadWrite` program under a
	/// read-only classification that nothing explains.
	#[tokio::test]
	async fn the_sleep_exception_excuses_only_a_sleep() {
		let h = Harness::new("").await;
		let explained = |sql: &'static str| {
			let mut ast = crate::syn::parse(sql).expect("fixture should parse");
			let Some(surrealdb_expr::parser::TopLevelExpr::Expr(expr)) = ast.expressions.pop()
			else {
				panic!("fixture is not a bare expression: {sql}");
			};
			let op = Harness::lower(&expr).expect("fixture should lower");
			explained_by_sleep(&op, &h)
		};
		for sql in [
			"SLEEP 1s",
			"RETURN (SLEEP 1s)",
			"{ SLEEP 1s; RETURN 1 }",
			"IF true { SLEEP 1s }",
			"SELECT (SLEEP 1s) AS a FROM [1]",
			"array::map([1], |$x| { SLEEP 1s })",
		] {
			assert!(explained(sql), "`{sql}` only sleeps, so the exception should excuse it");
		}
		for sql in [
			"RETURN 1",
			"CREATE log",
			"RETURN (CREATE log)",
			"{ SLEEP 1s; CREATE log }",
			"SELECT (SLEEP 1s) AS a, (CREATE log) AS b FROM [1]",
			"array::map([1], |$x| { SLEEP 1s; CREATE log })",
		] {
			assert!(
				!explained(sql),
				"`{sql}` is not only a sleep, so the exception must not excuse it"
			);
		}

		for (read_only, mode, sleeps, unexplained) in [
			(true, AccessMode::ReadWrite, false, true),
			(true, AccessMode::ReadWrite, true, false),
			(false, AccessMode::ReadWrite, false, false),
			(true, AccessMode::ReadOnly, false, false),
			(false, AccessMode::ReadOnly, false, false),
		] {
			assert_eq!(
				is_unexplained(read_only, mode, sleeps),
				unexplained,
				"read_only {read_only}, {mode:?}, sleeps {sleeps}"
			);
		}
	}

	/// The corpus form of the check the fixtures make position by position.
	///
	/// ADR 0001 Decision 1 asks for
	///
	/// > For every statement, `access_mode() == ReadWrite` implies `read_only() == false`
	///
	/// as a property over a parse corpus. Taken literally the implication is
	/// false for one construct: a `SLEEP` statement is `ReadWrite` because
	/// the pause is a side effect, and `read_only()` asks whether storage is
	/// written, so it answers `true`. Asserting the implication as stated
	/// would put `SLEEP` on a write transaction. The contract is stated over
	/// the origin of the mode instead:
	///
	/// > For every statement the VM plans, if the program is `ReadWrite` and
	/// > `read_only()` is `true`, the statement sleeps and its logical plan
	/// > names no write.
	///
	/// [`explained_by_sleep`] is the whole of the exception list. It may
	/// shrink and must not grow without a decision: an entry means "this
	/// construct's `ReadWrite` is known not to be a storage write", and a
	/// write appearing behind it is the failure it is shaped to expose.
	///
	/// A program's mode is a property of what the VM plans, so only statements
	/// that plan contribute. Each statement is planned alone, against a
	/// database that holds none of the definitions the corpus file around it
	/// makes, in the transaction `read_only()` selects. A statement that needs
	/// a definition only its file makes declines or plans against an absent
	/// table; the corpus therefore exercises the expression and statement
	/// forms, which is where a classifier under-approximates, and not a plan
	/// that depends on the catalog.
	///
	/// A decline is counted, not skipped silently, and the assertion at the
	/// end requires the sweep to have proved something in both halves: at
	/// least one statement planned, and at least one that planned to
	/// `ReadWrite`.
	///
	/// The sweep is split into slices that run as tests of their own, each
	/// taking the files whose position in path order is congruent to its
	/// number, so each slice holds the assertion over its files.
	fn access_mode_read_write_implies_not_read_only(k: usize) {
		tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.expect("a runtime")
			.block_on(access_mode_sweep(k));
	}

	sliced_tests! {
		access_mode_read_write_implies_not_read_only, ACCESS_MODE_SLICES;
		access_mode_read_write_implies_not_read_only_00 => 0,
		access_mode_read_write_implies_not_read_only_01 => 1,
		access_mode_read_write_implies_not_read_only_02 => 2,
		access_mode_read_write_implies_not_read_only_03 => 3,
		access_mode_read_write_implies_not_read_only_04 => 4,
		access_mode_read_write_implies_not_read_only_05 => 5,
		access_mode_read_write_implies_not_read_only_06 => 6,
		access_mode_read_write_implies_not_read_only_07 => 7,
		access_mode_read_write_implies_not_read_only_08 => 8,
		access_mode_read_write_implies_not_read_only_09 => 9,
		access_mode_read_write_implies_not_read_only_10 => 10,
		access_mode_read_write_implies_not_read_only_11 => 11,
	}

	/// The files of the language-test corpus, in path order, with the
	/// directory they are under.
	fn corpus_sources() -> (PathBuf, Vec<(PathBuf, String)>) {
		let corpus = Path::new(env!("CARGO_MANIFEST_DIR"))
			.join("..")
			.join("..")
			.join("language-tests")
			.join("tests");
		assert!(corpus.is_dir(), "language-test corpus not found at {}", corpus.display());
		let mut sources: Vec<(PathBuf, String)> = Vec::new();
		walk_surql(&corpus, &mut |path, source| {
			sources.push((path.to_path_buf(), source.to_string()));
		});
		sources.sort_by(|a, b| a.0.cmp(&b.0));
		(corpus, sources)
	}

	/// The slices of the corpus hold every file once between them and none
	/// is empty, so the sliced sweeps together walk the whole corpus.
	#[test]
	fn access_mode_slices_cover_the_corpus() {
		let (corpus, sources) = corpus_sources();
		let paths: Vec<String> = sources
			.iter()
			.map(|(path, _)| path.strip_prefix(&corpus).unwrap_or(path).display().to_string())
			.collect();
		assert!(paths.len() > 100, "the language tests number {}", paths.len());
		assert_slices_cover("language-test files", &paths);
	}

	/// The sweep over the files of slice `k`.
	async fn access_mode_sweep(k: usize) {
		let h = Harness::new("").await;
		let (corpus, sources) = corpus_sources();
		let sources = slice(sources, k);

		let mut sweep = Sweep::default();
		let mut unexplained: Vec<String> = Vec::new();
		for (path, source) in &sources {
			let Ok(ast) = crate::syn::parse(source) else {
				sweep.unparsed += 1;
				continue;
			};
			for top in ast.expressions {
				let read_only = top.read_only();
				let surrealdb_expr::parser::TopLevelExpr::Expr(expr) = top else {
					// A `BEGIN`/`COMMIT`/`USE` has no expression to lower; the
					// transaction-control statements are not part of the property.
					continue;
				};
				let Ok(op) = Harness::lower(&expr) else {
					sweep.declined += 1;
					continue;
				};
				let sleeps = read_only && explained_by_sleep(&op, &h);
				let Ok(plan) = h.plan_op(read_only, op).await else {
					sweep.declined += 1;
					continue;
				};
				sweep.planned += 1;
				let mode = plan.access_mode();
				if mode != AccessMode::ReadWrite {
					continue;
				}
				sweep.writing += 1;
				if is_unexplained(read_only, mode, sleeps) {
					unexplained.push(format!(
						"{}: `{}` claimed read_only(), planned ReadWrite, and is not a sleep",
						path.strip_prefix(&corpus).unwrap_or(path).display(),
						surrealdb_types::ToSql::to_sql(&expr),
					));
				} else if read_only {
					sweep.over_approximated += 1;
				}
			}
		}

		println!(
			"corpus sweep: {} statements planned, {} of them ReadWrite ({} of those explained by a \
			 sleep), {} statements declined, {} files unparsed",
			sweep.planned, sweep.writing, sweep.over_approximated, sweep.declined, sweep.unparsed
		);
		assert!(
			unexplained.is_empty(),
			"{} statement(s) claimed read_only() with a ReadWrite program that no documented \
			 over-approximation explains:\n{}",
			unexplained.len(),
			unexplained.join("\n")
		);
		assert!(sweep.planned > 0, "no statement planned, so this test asserted nothing");
		assert!(
			sweep.writing > 0,
			"no statement planned to ReadWrite, so the implication was never exercised"
		);
	}
}
