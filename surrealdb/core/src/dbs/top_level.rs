//! The executor's unit of work.

use surrealdb_types::{SqlFormat, ToSql};

use crate::dbs::QueryType;
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
/// A caller that builds the expression tree itself hands in that form
/// directly: the GQL front end, the GraphQL bridge, and model upload all lower
/// their own statements. There is no surface syntax behind those, so nothing
/// converts and rendering them as text goes the long way round.
pub(crate) enum TopLevelStatement {
	/// A statement in surface form, as the parser produced it.
	Ast(sql::TopLevelExpr),
	/// A statement that arrived already lowered.
	Plan(expr::TopLevelExpr),
}

/// What the executor's drivers act on before a statement is converted.
///
/// Both forms of a [`TopLevelStatement`] collapse to this, so a driver matches
/// one small enum rather than two large ones. Only what a driver has to
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
			TopLevelStatement::Plan(stmt) => match stmt {
				expr::TopLevelExpr::Begin => Dispatch::Begin,
				expr::TopLevelExpr::Cancel => Dispatch::Cancel,
				expr::TopLevelExpr::Commit => Dispatch::Commit,
				expr::TopLevelExpr::Option(stmt) => Dispatch::Option(stmt.clone()),
				expr::TopLevelExpr::Access(_)
				| expr::TopLevelExpr::Kill(_)
				| expr::TopLevelExpr::Live(_)
				| expr::TopLevelExpr::Use(_)
				| expr::TopLevelExpr::Show(_)
				| expr::TopLevelExpr::Expr(_) => Dispatch::Runnable,
			},
		}
	}

	/// Whether this statement can run inside a read-only transaction. See
	/// [`sql::read_only`](crate::sql::read_only) for the one-sided contract
	/// the predicate upholds.
	pub(crate) fn read_only(&self) -> bool {
		match self {
			TopLevelStatement::Ast(stmt) => stmt.read_only(),
			TopLevelStatement::Plan(stmt) => stmt.read_only(),
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
			TopLevelStatement::Plan(stmt) => expr_statement_type(stmt),
		}
	}

	/// The [`QueryType`] a result for this statement carries, which tells a
	/// transport whether the returned id names a new subscription.
	pub(crate) fn query_type(&self) -> QueryType {
		query_type(self.statement_type())
	}

	/// The statement in the form the engines take.
	///
	/// This is the conversion the executor defers: it happens here, once, when
	/// a statement is about to run.
	pub(crate) fn into_expr(self) -> expr::TopLevelExpr {
		match self {
			TopLevelStatement::Ast(stmt) => stmt.into(),
			TopLevelStatement::Plan(stmt) => stmt,
		}
	}
}

/// Statement text describes what will run, not what was typed.
///
/// SECURITY: a surface statement is rendered through its lowered form, never
/// directly. Lowering `DEFINE USER ... PASSWORD 'secret'` replaces the
/// password with the Argon2 hash and SCRAM verifier derived from it, so the
/// lowered form carries no plaintext credential. Statement text reaches the
/// tracing span, the slow log and any observer that asks for it; rendering the
/// surface form would put the plaintext password in all three. A `DEFINE USER`
/// nested inside a block, a function body or an `IF` branch is reached the
/// same way, which is why this is a property of the whole render rather than a
/// carve-out for one statement kind.
///
/// Rendering costs a lowering, and lowering a `PASSWORD` derives its Argon2
/// hash and SCRAM verifier under a fresh salt. So the text names a credential
/// equivalent to the stored one rather than the stored one itself, and a
/// statement rendered at several sites derives one per site. Both follow from
/// rendering through a form the surface statement does not itself hold; a
/// render that redacted the credential instead would need neither.
impl ToSql for TopLevelStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			TopLevelStatement::Ast(stmt) => {
				let lowered: expr::TopLevelExpr = stmt.clone().into();
				lowered.fmt_sql(f, fmt);
			}
			TopLevelStatement::Plan(stmt) => stmt.fmt_sql(f, fmt),
		}
	}
}

/// The statement kinds that only read.
///
/// Derived from [`StatementType`] rather than matched on each form, so the
/// surface and lowered answers agree by construction instead of by a test.
/// Every kind here has exactly one classifier arm producing it, on each side.
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
/// reason as [`kind_reads_only`]; neither `Expr` enum has a `Live` or `Kill`
/// variant, so these two kinds come only from the top level.
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

/// Classify a lowered [`expr::TopLevelExpr`] into its bounded
/// [`StatementType`] category.
fn expr_statement_type(expr: &expr::TopLevelExpr) -> StatementType {
	match expr {
		expr::TopLevelExpr::Begin => StatementType::Begin,
		expr::TopLevelExpr::Cancel => StatementType::Cancel,
		expr::TopLevelExpr::Commit => StatementType::Commit,
		expr::TopLevelExpr::Access(_) => StatementType::Access,
		expr::TopLevelExpr::Kill(_) => StatementType::Kill,
		expr::TopLevelExpr::Live(_) => StatementType::Live,
		expr::TopLevelExpr::Option(_) => StatementType::Option,
		expr::TopLevelExpr::Use(_) => StatementType::Use,
		expr::TopLevelExpr::Show(_) => StatementType::Show,
		expr::TopLevelExpr::Expr(expr) => expr_expr_statement_type(expr),
	}
}

/// Classify a bare [`expr::Expr`] into its bounded [`StatementType`] category.
fn expr_expr_statement_type(expr: &expr::Expr) -> StatementType {
	match expr {
		expr::Expr::Select(_) => StatementType::Select,
		expr::Expr::Create(_) => StatementType::Create,
		expr::Expr::Update(_) => StatementType::Update,
		expr::Expr::Upsert(_) => StatementType::Upsert,
		expr::Expr::Delete(_) => StatementType::Delete,
		expr::Expr::Relate(_) => StatementType::Relate,
		expr::Expr::Insert(_) => StatementType::Insert,
		expr::Expr::Define(_) => StatementType::Define,
		expr::Expr::Remove(_) => StatementType::Remove,
		expr::Expr::Rebuild(_) => StatementType::Rebuild,
		expr::Expr::Alter(_) => StatementType::Alter,
		expr::Expr::Info(_) => StatementType::Info,
		expr::Expr::Foreach(_) => StatementType::Foreach,
		expr::Expr::IfElse(_) => StatementType::IfElse,
		expr::Expr::Sleep(_) => StatementType::Sleep,
		expr::Expr::Explain {
			..
		} => StatementType::Explain,
		expr::Expr::Let(_) => StatementType::Let,
		expr::Expr::Return(_) => StatementType::Return,
		expr::Expr::Break => StatementType::Break,
		expr::Expr::Continue => StatementType::Continue,
		expr::Expr::Throw(_) => StatementType::Throw,
		expr::Expr::Block(_) => StatementType::Block,
		// Enumerated rather than using a wildcard, as above.
		expr::Expr::Literal(_)
		| expr::Expr::Param(_)
		| expr::Expr::Idiom(_)
		| expr::Expr::Table(_)
		| expr::Expr::Mock(_)
		| expr::Expr::Constant(_)
		| expr::Expr::Prefix {
			..
		}
		| expr::Expr::Postfix {
			..
		}
		| expr::Expr::Binary {
			..
		}
		| expr::Expr::FunctionCall(_)
		| expr::Expr::Closure(_) => StatementType::Other,
		// GQL MATCH is not a SurrealQL statement-shaped expression.
		expr::Expr::Match(_) => StatementType::Other,
	}
}
