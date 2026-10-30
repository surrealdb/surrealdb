//! A definition's `PERMISSIONS` predicate judged once per statement.
//!
//! The predicate of a `DEFINE FUNCTION`, `PARAM`, `MODEL` or `MODULE` decides
//! whether the caller may use the definition. Where it reads only the
//! session's own values (`$auth`, `$session`, `$token`, `$access`), the
//! catalog and data the statement cannot change, and calls nothing whose
//! answer varies between calls, its verdict is the same at every call of the
//! statement: it is judged at the first call that reaches it and that
//! admission serves the statement's later calls ([`GuardVerdicts`]). The
//! first call judges it lazily, so a statement that never calls the
//! definition judges nothing, and a predicate that raises an error raises it
//! at that call exactly as a per-call judgement would. Only an admission is
//! kept: a refusal or an error is judged again by the next call.
//!
//! A predicate whose verdict may differ between calls is judged at every
//! call ([`GuardScope::PerCall`]): one that reads the row it is judged over
//! (`$this`, a field, the document), a parameter other than the session's
//! values, a clock, entropy or anything else not deterministic within the
//! statement, or
//! a body it cannot see. One that reads records — a nested statement, a graph
//! lookup, a record link — is judged once only in a transaction that cannot
//! write, since a statement that writes may change what it reads.
//!
//! The verdict is kept per principal: a call under a definition's `AUTH
//! LIMIT` is judged for the narrowed principal, apart from the caller's.

use std::sync::{Arc, Mutex, PoisonError};

use crate::exec::ExecutionContext;
use crate::exec::function::FunctionRegistry;
use crate::expr::FlowResult;
use crate::expr::logical::{EffectSet, LogicalExpr};

/// How often a predicate's verdict stands within one statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum GuardScope {
	/// Judged at every call.
	PerCall,
	/// Judged once per statement and principal; for a predicate that
	/// `reads_data`, only in a statement that only reads.
	Statement {
		reads_data: bool,
	},
}

/// The names a predicate may read and still be judged once per statement.
const SESSION_VALUES: &[&str] = surrealdb_cnf::PROTECTED_PARAM_NAMES;

impl GuardScope {
	/// The scope of predicate `clause`, from what it reads and the effects
	/// `registry` declares for what it calls.
	pub(crate) fn of(clause: &LogicalExpr, registry: &FunctionRegistry) -> Self {
		use std::ops::ControlFlow;
		let effects = crate::exec::logical::effects_of_body(clause, registry);
		if !effects.is_deterministic() || effects.intersects(EffectSet::READS_ROW) {
			return Self::PerCall;
		}
		if crate::exec::logical::effects_of_expr(clause, registry).reads_row_opaquely {
			return Self::PerCall;
		}
		// Every parameter it reads, at any depth, is one of the session's
		// values.
		let foreign_param = clause.visit_deep(&mut |node| {
			if let LogicalExpr::Param(name) = node
				&& !SESSION_VALUES.contains(&name.as_str())
			{
				return ControlFlow::Break(());
			}
			ControlFlow::Continue(())
		});
		if foreign_param.is_break() || reads_row(clause) {
			return Self::PerCall;
		}
		Self::Statement {
			reads_data: effects.intersects(EffectSet::READS_DATA),
		}
	}
}

/// Whether `expr` reads the row it is evaluated over: a field, the document
/// or the current value, outside any nested statement, whose rows are its own.
fn reads_row(expr: &LogicalExpr) -> bool {
	match expr {
		LogicalExpr::Field(_) | LogicalExpr::Doc | LogicalExpr::Current => true,
		LogicalExpr::Subquery(_) => false,
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue => {
			let mut found = false;
			expr.for_each_child(&mut |child| found = found || reads_row(child));
			found
		}
	}
}

/// What a predicate's verdict is kept under: the kind and name of the
/// definition and an identity of the clause judged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GuardKind {
	Function,
	Param,
	/// A model's or a module's: only a build that can call either has one.
	#[cfg(feature = "surrealism")]
	Model,
	#[cfg(feature = "surrealism")]
	Module,
}

/// The admissions one statement's predicates have answered.
pub(crate) struct GuardVerdicts {
	admitted: Mutex<Vec<Admitted>>,
	/// Whether the statement may write, which may change the records a
	/// predicate reads between two of its calls.
	may_write: bool,
}

/// The admissions of a statement not known to only read.
impl Default for GuardVerdicts {
	fn default() -> Self {
		Self::for_statement(true)
	}
}

/// One admission: of the principal, by the clause of the definition.
struct Admitted {
	kind: GuardKind,
	name: String,
	clause: u64,
	principal: Arc<crate::iam::Auth>,
}

impl std::fmt::Debug for GuardVerdicts {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("GuardVerdicts").finish_non_exhaustive()
	}
}

impl GuardVerdicts {
	/// The admissions of a statement that `may_write`, none held yet.
	pub(crate) fn for_statement(may_write: bool) -> Self {
		Self {
			admitted: Mutex::new(Vec::new()),
			may_write,
		}
	}

	fn admits(
		&self,
		kind: GuardKind,
		name: &str,
		clause: u64,
		principal: &Arc<crate::iam::Auth>,
	) -> bool {
		self.admitted.lock().unwrap_or_else(PoisonError::into_inner).iter().any(|held| {
			held.kind == kind
				&& held.clause == clause
				&& held.name == name
				&& (Arc::ptr_eq(&held.principal, principal) || held.principal == *principal)
		})
	}

	fn admit(&self, kind: GuardKind, name: &str, clause: u64, principal: &Arc<crate::iam::Auth>) {
		self.admitted.lock().unwrap_or_else(PoisonError::into_inner).push(Admitted {
			kind,
			name: name.to_owned(),
			clause,
			principal: Arc::clone(principal),
		});
	}
}

#[cfg(test)]
thread_local! {
	/// How many predicates this thread has judged, for tests that count them.
	static JUDGED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many predicates this thread has judged.
#[cfg(test)]
pub(crate) fn judged_count() -> usize {
	JUDGED.with(std::cell::Cell::get)
}

/// Whether a verdict of a predicate of `scope` stands for the whole statement `ctx` runs, which
/// [`judged`] keeps and answers every later read of the statement with, so the verdict is the same
/// whatever row a read is made for. A verdict that does not is judged on every read.
pub(crate) fn stands_for_statement(ctx: &ExecutionContext, scope: GuardScope) -> bool {
	match scope {
		GuardScope::PerCall => false,
		GuardScope::Statement {
			reads_data,
		} => !reads_data || !ctx.root().guard_verdicts.may_write,
	}
}

/// The predicate of definition `name` of `kind`, identified by `clause`, for
/// the principal `ctx` judges as: answered by an admission the statement
/// already holds where `scope` lets one stand, and otherwise by `judge`, whose
/// admission is kept where it may be.
pub(crate) async fn judged<Fut>(
	ctx: &ExecutionContext,
	scope: GuardScope,
	kind: GuardKind,
	name: &str,
	clause: u64,
	judge: impl FnOnce() -> Fut,
) -> FlowResult<()>
where
	Fut: std::future::Future<Output = FlowResult<()>>,
{
	let root = ctx.root();
	let kept = stands_for_statement(ctx, scope);
	if kept && root.guard_verdicts.admits(kind, name, clause, &root.auth) {
		return Ok(());
	}
	#[cfg(test)]
	JUDGED.with(|judged| judged.set(judged.get() + 1));
	judge().await?;
	if kept {
		root.guard_verdicts.admit(kind, name, clause, &root.auth);
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use crate::exec::operators::test_util::TestDb;
	use crate::val::Value;

	fn alice() -> crate::dbs::Session {
		let rid =
			crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "alice"));
		crate::dbs::Session::for_record("test", "test", "user", rid)
	}

	const SETUP: &str = "
		DEFINE ACCESS user ON DATABASE TYPE RECORD;
		DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
		CREATE user:alice SET admin = true;
		FOR $i IN 1..=10 { CREATE type::record('t', $i) SET n = $i };
		DEFINE FUNCTION fn::session($n: int) { RETURN $n * 2 } PERMISSIONS WHERE $auth = user:alice;
		DEFINE FUNCTION fn::linked($n: int) { RETURN $n * 2 } PERMISSIONS WHERE $auth.admin = true;
		DEFINE FUNCTION fn::clock($n: int) { RETURN $n * 2 }
			PERMISSIONS WHERE time::now() > d'2000-01-01T00:00:00Z';
		DEFINE FUNCTION fn::raising($n: int) { RETURN $n * 2 } PERMISSIONS WHERE <int> 'x' = 1;
		DEFINE PARAM $p VALUE 1 PERMISSIONS WHERE $auth = user:alice;
	";

	/// The outcome of `sql` run as alice, and how many predicates it judged.
	async fn judged(db: &TestDb, sql: &str) -> (Result<Value, String>, usize) {
		let before = super::judged_count();
		let mut results = db.datastore().execute(sql, &alice(), None).await.expect("runs");
		let outcome =
			results.pop().expect("a result").result.map(Value::from).map_err(|e| e.to_string());
		(outcome, super::judged_count() - before)
	}

	fn doubled() -> Value {
		Value::from(crate::syn::value("[2, 4, 6, 8, 10, 12, 14, 16, 18, 20]").expect("parses"))
	}

	/// A predicate that reads only the session's values is judged once for a
	/// statement that calls the function on every row.
	#[tokio::test]
	async fn a_predicate_on_session_values_is_judged_once_per_statement() {
		let db = TestDb::new_with_auth(SETUP).await;
		let sql = "SELECT VALUE fn::session(n) FROM t ORDER BY n";
		assert_eq!(judged(&db, sql).await, (Ok(doubled()), 1));
		// The next statement judges it again.
		assert_eq!(judged(&db, sql).await, (Ok(doubled()), 1));
		let params = "SELECT VALUE n + $p FROM t ORDER BY n";
		let (answer, count) = judged(&db, params).await;
		assert_eq!(
			answer,
			Ok(Value::from(crate::syn::value("[2, 3, 4, 5, 6, 7, 8, 9, 10, 11]").expect("parses")))
		);
		assert_eq!(count, 1, "a guarded parameter read on every row");
	}

	/// A predicate that reads the clock is judged at every call.
	#[tokio::test]
	async fn a_predicate_reading_the_clock_is_judged_per_call() {
		let db = TestDb::new_with_auth(SETUP).await;
		let sql = "SELECT VALUE fn::clock(n) FROM t ORDER BY n";
		assert_eq!(judged(&db, sql).await, (Ok(doubled()), 10));
	}

	/// A predicate that reads records is judged once in a statement that
	/// cannot write, and at every call in one that may.
	#[tokio::test]
	async fn a_predicate_reading_records_is_judged_once_only_where_nothing_writes() {
		let db = TestDb::new_with_auth(SETUP).await;
		let read = "SELECT VALUE fn::linked(n) FROM t ORDER BY n";
		assert_eq!(judged(&db, read).await, (Ok(doubled()), 1));
		let write = "UPDATE t SET m = fn::linked(n) RETURN NONE";
		let (answer, count) = judged(&db, write).await;
		assert!(answer.is_ok(), "{answer:?}");
		assert_eq!(count, 10, "a write's predicate that reads records");
	}

	/// A statement that never calls the function judges nothing and raises
	/// nothing, whatever the predicate would raise; one that calls it raises
	/// the predicate's error at the first call.
	#[tokio::test]
	async fn a_predicate_is_judged_at_the_first_call_and_raises_there() {
		let db = TestDb::new_with_auth(SETUP).await;
		let none = "SELECT VALUE fn::raising(n) FROM t WHERE n > 100";
		assert_eq!(
			judged(&db, none).await,
			(Ok(Value::from(crate::syn::value("[]").expect("parses"))), 0)
		);
		let (answer, count) = judged(&db, "SELECT VALUE fn::raising(n) FROM t").await;
		let error = answer.expect_err("the predicate raises");
		assert!(!error.contains("permission"), "the predicate's own error, not a refusal: {error}");
		assert_eq!(count, 1);
		let (direct, _) = judged(&db, "RETURN <int> 'x' = 1").await;
		assert_eq!(Err(error), direct, "the error the predicate raises on its own");
	}
}
