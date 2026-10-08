//! The executor's unit of work.

use surrealdb_types::{SqlFormat, ToSql};

use crate::dbs::QueryType;
use crate::expr::logical::LogicalOp;
use crate::expr::statements::OptionStatement;
use crate::observe::StatementType;
use crate::{expr, sql};

/// One top-level statement, in the form it arrived in.
///
/// SurrealQL reaches the executor as the parser produced it and is converted
/// to the expression layer one statement at a time, at the point an engine
/// needs it. A batch that ends early never converts the statements it did not
/// reach, and a streamed batch converts nothing before its first statement
/// runs.
///
/// A front end that emits a plan rather than a statement hands the plan in
/// directly: GQL lowers a `MATCH` query to a [`LogicalOp::Match`]. There is no
/// surface syntax behind a plan, so nothing converts it, and it runs on the
/// engine or fails.
pub(crate) enum TopLevelStatement {
	/// A statement in surface form, as the parser produced it.
	Ast(sql::TopLevelExpr),
	/// A plan that arrived already lowered.
	Plan(Box<LogicalOp>),
}

/// What the executor's drivers act on before a statement is converted.
///
/// A [`TopLevelStatement`] collapses to this, so a driver matches one small
/// enum rather than the surface statement's. Only what a driver has to
/// recognise itself appears here: the transaction-control statements, which it
/// runs rather than hands to an engine, and `OPTION`, which changes the
/// options every later statement in the batch runs under.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Dispatch {
	Begin,
	Cancel,
	Commit,
	Option(OptionStatement),
	/// Everything an engine runs.
	Runnable,
}

impl TopLevelStatement {
	/// What a driver acts on before anything is converted.
	pub(crate) fn dispatch(&self) -> Dispatch {
		match self {
			TopLevelStatement::Ast(stmt) => match stmt {
				sql::TopLevelExpr::Begin => Dispatch::Begin,
				sql::TopLevelExpr::Cancel => Dispatch::Cancel,
				sql::TopLevelExpr::Commit => Dispatch::Commit,
				sql::TopLevelExpr::Option(stmt) => Dispatch::Option(stmt.clone().into()),
				sql::TopLevelExpr::Access(_)
				| sql::TopLevelExpr::Kill(_)
				| sql::TopLevelExpr::Live(_)
				| sql::TopLevelExpr::Use(_)
				| sql::TopLevelExpr::Show(_)
				| sql::TopLevelExpr::Expr(_) => Dispatch::Runnable,
			},
			// A plan is something an engine runs; no driver acts on it first.
			TopLevelStatement::Plan(_) => Dispatch::Runnable,
		}
	}

	/// Whether this statement can run inside a read-only transaction. See
	/// [`sql::read_only`](crate::sql::read_only) for the one-sided contract
	/// the predicate upholds; [`LogicalOp::read_only`] upholds the same one
	/// for a plan.
	pub(crate) fn read_only(&self) -> bool {
		match self {
			TopLevelStatement::Ast(stmt) => stmt.read_only(),
			TopLevelStatement::Plan(op) => op.read_only(),
		}
	}

	/// Whether this statement's *kind* only reads.
	///
	/// Narrower than [`Self::read_only`], which walks the whole tree to
	/// choose a transaction mode. This reports the kind alone, and is what
	/// the `StatementEvent`'s `read_only` attribute carries on the path where
	/// the transaction is the caller's and its mode was not chosen from the
	/// statement.
	///
	/// A kind that can write answers `false` even when a particular statement
	/// of that kind would not — GQL's `MATCH` carries mutation stages, so it
	/// answers `false` whatever an individual query does. Reporting otherwise
	/// needs the statement's contents, which is what [`Self::read_only`] is
	/// for.
	pub(crate) fn kind_reads_only(&self) -> bool {
		kind_reads_only(self.statement_type())
	}

	/// This statement's bounded [`StatementType`] category.
	pub(crate) fn statement_type(&self) -> StatementType {
		match self {
			TopLevelStatement::Ast(stmt) => sql_statement_type(stmt),
			// A plan carries no statement kind to classify.
			TopLevelStatement::Plan(_) => StatementType::Other,
		}
	}

	/// The [`QueryType`] a result for this statement carries, which tells a
	/// transport whether the returned id names a new subscription.
	pub(crate) fn query_type(&self) -> QueryType {
		query_type(self.statement_type())
	}
}

/// Statement text describes what will run, not what was typed.
///
/// SECURITY: a surface statement is rendered through its lowered form, never
/// directly. Lowering `DEFINE USER ... PASSWORD 'secret'` replaces the
/// password with the Argon2 hash and SCRAM verifier derived from it, so the
/// lowered form carries no plaintext credential. Statement text reaches the
/// tracing span and any observer that asks for it; rendering the surface form
/// would put the plaintext password in both. The slow log prints a statement
/// the engine ran in this form: a statement holds either no schema statement
/// or is one, and the plan carries that statement in the same lowered form.
/// A `DEFINE USER`
/// nested inside a block, a function body or an `IF` branch is reached the
/// same way, which is why this is a property of the whole render rather than a
/// carve-out for one statement kind: every top-level kind that carries an
/// expression goes through its lowered form, whatever position the credential
/// sits in.
///
/// Rendering costs a lowering, and lowering a `PASSWORD` derives its Argon2
/// hash and SCRAM verifier under a fresh salt. So the text names a credential
/// equivalent to the stored one rather than the stored one itself, and a
/// statement rendered at several sites derives one per site. Both follow from
/// rendering through a form the surface statement does not itself hold; a
/// render that redacted the credential instead would need neither.
///
/// A plan has no surface form and holds no credential; it renders as the plan
/// it is.
impl ToSql for TopLevelStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			TopLevelStatement::Ast(stmt) => match stmt {
				// No payload, so nothing to lower.
				sql::TopLevelExpr::Begin
				| sql::TopLevelExpr::Cancel
				| sql::TopLevelExpr::Commit => stmt.fmt_sql(f, fmt),
				sql::TopLevelExpr::Access(stmt) => {
					render_lowered::<_, expr::statements::AccessStatement>(&**stmt, f, fmt)
				}
				sql::TopLevelExpr::Kill(stmt) => {
					render_lowered::<_, expr::statements::KillStatement>(stmt, f, fmt)
				}
				sql::TopLevelExpr::Live(stmt) => {
					render_lowered::<_, expr::statements::LiveStatement>(&**stmt, f, fmt)
				}
				sql::TopLevelExpr::Option(stmt) => {
					render_lowered::<_, OptionStatement>(stmt, f, fmt)
				}
				sql::TopLevelExpr::Use(stmt) => {
					render_lowered::<_, expr::statements::UseStatement>(stmt, f, fmt)
				}
				sql::TopLevelExpr::Show(stmt) => {
					render_lowered::<_, expr::statements::ShowStatement>(stmt, f, fmt)
				}
				// The expression layer renders a bare expression itself.
				sql::TopLevelExpr::Expr(expr) => expr::Expr::from(expr.clone()).fmt_sql(f, fmt),
			},
			TopLevelStatement::Plan(op) => op.fmt_sql(f, fmt),
		}
	}
}

/// Render a statement through its lowered form: convert it to the expression
/// layer and back, and render the surface form that returns. The lowering is
/// what replaces a plaintext credential with the derived hash and verifier,
/// and the expression layer renders these statement kinds by converting back
/// to the surface form, so the round trip yields the text the lowered form
/// renders.
fn render_lowered<S, L>(stmt: &S, f: &mut String, fmt: SqlFormat)
where
	S: Clone + Into<L> + ToSql,
	L: Into<S>,
{
	let lowered: L = stmt.clone().into();
	let surface: S = lowered.into();
	surface.fmt_sql(f, fmt);
}

/// The statement kinds that only read.
///
/// Derived from [`StatementType`] rather than matched on the statement, so
/// every kind here has exactly one classifier arm producing it.
fn kind_reads_only(kind: StatementType) -> bool {
	matches!(
		kind,
		StatementType::Use
			| StatementType::Show
			| StatementType::Select
			| StatementType::Info
			| StatementType::Explain
	)
}

/// The statement kinds whose result names a subscription. Derived for the same
/// reason as [`kind_reads_only`]; no `Expr` variant is a `LIVE` or `KILL`, so
/// these two kinds come only from the top level.
fn query_type(kind: StatementType) -> QueryType {
	match kind {
		StatementType::Live => QueryType::Live,
		StatementType::Kill => QueryType::Kill,
		_ => QueryType::Other,
	}
}

/// Classify a surface [`sql::TopLevelExpr`] into its bounded
/// [`StatementType`] category.
fn sql_statement_type(expr: &sql::TopLevelExpr) -> StatementType {
	match expr {
		sql::TopLevelExpr::Begin => StatementType::Begin,
		sql::TopLevelExpr::Cancel => StatementType::Cancel,
		sql::TopLevelExpr::Commit => StatementType::Commit,
		sql::TopLevelExpr::Access(_) => StatementType::Access,
		sql::TopLevelExpr::Kill(_) => StatementType::Kill,
		sql::TopLevelExpr::Live(_) => StatementType::Live,
		sql::TopLevelExpr::Option(_) => StatementType::Option,
		sql::TopLevelExpr::Use(_) => StatementType::Use,
		sql::TopLevelExpr::Show(_) => StatementType::Show,
		sql::TopLevelExpr::Expr(expr) => sql_expr_statement_type(expr),
	}
}

/// Classify a bare [`sql::Expr`] into its bounded [`StatementType`] category.
fn sql_expr_statement_type(expr: &sql::Expr) -> StatementType {
	match expr {
		sql::Expr::Select(_) => StatementType::Select,
		sql::Expr::Create(_) => StatementType::Create,
		sql::Expr::Update(_) => StatementType::Update,
		sql::Expr::Upsert(_) => StatementType::Upsert,
		sql::Expr::Delete(_) => StatementType::Delete,
		sql::Expr::Relate(_) => StatementType::Relate,
		sql::Expr::Insert(_) => StatementType::Insert,
		sql::Expr::Define(_) => StatementType::Define,
		sql::Expr::Remove(_) => StatementType::Remove,
		sql::Expr::Rebuild(_) => StatementType::Rebuild,
		sql::Expr::Alter(_) => StatementType::Alter,
		sql::Expr::Info(_) => StatementType::Info,
		sql::Expr::Foreach(_) => StatementType::Foreach,
		sql::Expr::IfElse(_) => StatementType::IfElse,
		sql::Expr::Sleep(_) => StatementType::Sleep,
		sql::Expr::Explain {
			..
		} => StatementType::Explain,
		sql::Expr::Let(_) => StatementType::Let,
		sql::Expr::Return(_) => StatementType::Return,
		sql::Expr::Break => StatementType::Break,
		sql::Expr::Continue => StatementType::Continue,
		sql::Expr::Throw(_) => StatementType::Throw,
		sql::Expr::Block(_) => StatementType::Block,
		// Anything that isn't a recognised statement-shaped expression
		// collapses to `Other`. Enumerated rather than using a wildcard so a
		// new `Expr` variant forces a classification decision at compile time.
		sql::Expr::Literal(_)
		| sql::Expr::Param(_)
		| sql::Expr::Idiom(_)
		| sql::Expr::Table(_)
		| sql::Expr::Mock(_)
		| sql::Expr::Constant(_)
		| sql::Expr::Prefix {
			..
		}
		| sql::Expr::Postfix {
			..
		}
		| sql::Expr::Binary {
			..
		}
		| sql::Expr::FunctionCall(_)
		| sql::Expr::Closure(_) => StatementType::Other,
	}
}
