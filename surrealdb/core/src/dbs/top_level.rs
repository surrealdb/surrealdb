//! The executor's unit of work.

use surrealdb_expr::parser::{BeginOptions, TopLevelExpr};
use surrealdb_types::{SqlFormat, ToSql};

use crate::dbs::QueryType;
use crate::expr;
use crate::expr::logical::{LogicalOp, RequestStatementKind};
use crate::expr::statements::OptionStatement;
use crate::observe::StatementType;

/// One runtime statement or logical plan supplied by a frontend.
///
/// Parsed statements carry inert credentials and generated identities until
/// execution reaches them. Frontends may instead supply a logical plan, which
/// runs on the VM.
pub(crate) enum TopLevelStatement {
	/// A runtime statement, as the parser produced it.
	Ast(TopLevelExpr),
	/// A plan that arrived already lowered.
	Plan(Box<LogicalOp>),
	/// SurrealQL compiled directly from flat syntax, retaining observer metadata.
	Surreal(Box<surrealdb_expr::parser::LogicalStatement>),
}

/// What the executor's drivers act on before a statement runs.
///
/// A [`TopLevelStatement`] collapses to this, so a driver matches one small
/// enum rather than the surface statement's. Only what a driver has to
/// recognise itself appears here: the transaction-control statements, which it
/// runs rather than hands to an engine, and `OPTION`, which changes the
/// options every later statement in the batch runs under.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Dispatch {
	Begin(BeginOptions),
	Cancel,
	Commit,
	Option(OptionStatement),
	/// Everything an engine runs.
	Runnable,
}

impl TopLevelStatement {
	/// What a driver acts on before statement execution.
	pub(crate) fn dispatch(&self) -> Dispatch {
		match self {
			TopLevelStatement::Ast(stmt) => match stmt {
				TopLevelExpr::Begin(options) => Dispatch::Begin(options.clone()),
				TopLevelExpr::Cancel => Dispatch::Cancel,
				TopLevelExpr::Commit => Dispatch::Commit,
				TopLevelExpr::Option(stmt) => Dispatch::Option(stmt.clone()),
				TopLevelExpr::Access(_)
				| TopLevelExpr::Kill(_)
				| TopLevelExpr::Live(_)
				| TopLevelExpr::Use(_)
				| TopLevelExpr::Show(_)
				| TopLevelExpr::Expr(_) => Dispatch::Runnable,
			},
			// A plan is something an engine runs; no driver acts on it first.
			TopLevelStatement::Plan(_) | TopLevelStatement::Surreal(_) => Dispatch::Runnable,
		}
	}

	/// What request lowering groups this statement as. Matches the variants
	/// directly so an `OPTION` is not cloned.
	pub(crate) fn request_kind(&self) -> RequestStatementKind {
		match self {
			TopLevelStatement::Ast(stmt) => match stmt {
				TopLevelExpr::Begin(_) => RequestStatementKind::Begin,
				TopLevelExpr::Commit => RequestStatementKind::Commit,
				TopLevelExpr::Cancel => RequestStatementKind::Cancel,
				TopLevelExpr::Option(_) => RequestStatementKind::Option,
				TopLevelExpr::Access(_)
				| TopLevelExpr::Kill(_)
				| TopLevelExpr::Live(_)
				| TopLevelExpr::Use(_)
				| TopLevelExpr::Show(_)
				| TopLevelExpr::Expr(_) => RequestStatementKind::Run,
			},
			TopLevelStatement::Plan(_) | TopLevelStatement::Surreal(_) => RequestStatementKind::Run,
		}
	}

	/// Whether this statement can run inside a read-only transaction. See
	/// [`TopLevelExpr::read_only`] for the one-sided contract
	/// the predicate upholds; [`LogicalOp::read_only`] upholds the same one
	/// for a plan.
	pub(crate) fn read_only(&self) -> bool {
		match self {
			TopLevelStatement::Ast(stmt) => stmt.read_only(),
			TopLevelStatement::Plan(op) => op.read_only(),
			TopLevelStatement::Surreal(stmt) => stmt.plan.read_only(),
		}
	}

	/// Whether this statement is a blocking `DEFINE INDEX` or `REBUILD INDEX`
	/// whose build the transaction of its own awaits after committing.
	///
	/// A blocking build commits writes of its own in unbounded number, and a
	/// transaction held open across them ages past what an optimistic storage
	/// engine can still validate at commit. The statement alone qualifies, not
	/// one nested in a block, a function or an `IF`, nor one inside `BEGIN ...
	/// COMMIT`: awaiting after the commit means the rest of the transaction
	/// runs, and commits, before the build's outcome is known, so a build that
	/// then fails could no longer take back what those statements wrote. Only a
	/// transaction with nothing else in it stays atomic. This is decided here,
	/// from the statement, rather than by letting every transaction defer its
	/// builds: the instruction that queues a build cannot tell whether anything
	/// follows it. Every write transaction the executor owns accepts the later
	/// wait for an index to be materialised, which takes nothing back.
	///
	/// For the same reason as the deferral itself, a `DEFINE INDEX` whose
	/// name, table, fields or comment run anything when evaluated is excluded:
	/// whatever that writes would commit ahead of a build that may still fail.
	/// See
	/// [`evaluates_inertly`] for why that is decided on the operand's shape.
	/// Both statements always reach the executor in this surface form.
	///
	/// This only offers the statement the queue. Whether a `DEFINE INDEX`
	/// takes it is decided where the statement runs, from what it finds there
	/// (see `StoreInstr::BuildIndex`): one that replaces an existing index, or
	/// that creates its table, waits inside its transaction whatever its
	/// `OVERWRITE` or `IF NOT EXISTS`, and one that defines a new index on an
	/// existing table waits after the commit whatever them. A statement that
	/// queues nothing commits as any other does. The `COUNT` condition is not
	/// an operand here: the statement stores it unevaluated, and only the
	/// build and later writes run it.
	pub(crate) fn awaits_its_build_after_commit(&self) -> bool {
		if let TopLevelStatement::Ast(TopLevelExpr::Expr(expr::Expr::Define(stmt))) = self {
			if let expr::statements::DefineStatement::Index(ix) = &**stmt {
				!ix.concurrently
					&& evaluates_inertly(&ix.name)
					&& evaluates_inertly(&ix.what)
					&& evaluates_inertly(&ix.comment)
					&& ix.cols.iter().all(evaluates_inertly)
			} else {
				false
			}
		} else if let TopLevelStatement::Ast(TopLevelExpr::Expr(expr::Expr::Rebuild(stmt))) = self {
			matches!(&**stmt, expr::statements::RebuildStatement::Index(ix) if !ix.concurrently)
		} else {
			false
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
			TopLevelStatement::Ast(stmt) => statement_type(stmt),
			// A plan carries no statement kind to classify.
			TopLevelStatement::Plan(_) => StatementType::Other,
			TopLevelStatement::Surreal(stmt) => flat_statement_type(stmt.kind),
		}
	}

	/// The [`QueryType`] a result for this statement carries, which tells a
	/// transport whether the returned id names a new subscription.
	pub(crate) fn query_type(&self) -> QueryType {
		query_type(self.statement_type())
	}
}

/// Whether evaluating `operand` runs nothing that could write: a scalar literal,
/// a parameter, a constant, a table name, or a field path.
///
/// [`TopLevelExpr::read_only`] is not enough here. It takes a builtin call whose
/// arguments only read for a call that only reads, but a builtin that calls a
/// closure it is handed — `array::map([0], $f)` — runs a body the syntax
/// cannot see. So an operand that calls anything counts as one that may
/// write, and costs its statement only the wait inside its transaction.
fn evaluates_inertly(operand: &expr::Expr) -> bool {
	match operand {
		expr::Expr::Param(_) | expr::Expr::Table(_) | expr::Expr::Constant(_) => true,
		expr::Expr::Literal(literal) => matches!(
			literal,
			expr::Literal::None
				| expr::Literal::Null
				| expr::Literal::Bool(_)
				| expr::Literal::Float(_)
				| expr::Literal::Integer(_)
				| expr::Literal::Decimal(_)
				| expr::Literal::Duration(_)
				| expr::Literal::String(_)
				| expr::Literal::Datetime(_)
				| expr::Literal::Uuid(_)
		),
		expr::Expr::Idiom(idiom) => idiom.0.iter().all(|part| match part {
			expr::Part::Field(_)
			| expr::Part::All
			| expr::Part::Flatten
			| expr::Part::First
			| expr::Part::Last => true,
			expr::Part::Start(part) | expr::Part::Value(part) => evaluates_inertly(part),
			expr::Part::Where(_)
			| expr::Part::Lookup(_)
			| expr::Part::Method(..)
			| expr::Part::Destructure(_)
			| expr::Part::Optional
			| expr::Part::Recurse(..)
			| expr::Part::Doc
			| expr::Part::RepeatRecurse => false,
		}),
		expr::Expr::Mock(_)
		| expr::Expr::Block(_)
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
		| expr::Expr::Closure(_)
		| expr::Expr::Break
		| expr::Expr::Continue
		| expr::Expr::Return(_)
		| expr::Expr::Throw(_)
		| expr::Expr::IfElse(_)
		| expr::Expr::Select(_)
		| expr::Expr::Create(_)
		| expr::Expr::Update(_)
		| expr::Expr::Upsert(_)
		| expr::Expr::Delete(_)
		| expr::Expr::Relate(_)
		| expr::Expr::Insert(_)
		| expr::Expr::Define(_)
		| expr::Expr::Remove(_)
		| expr::Expr::Rebuild(_)
		| expr::Expr::Alter(_)
		| expr::Expr::Info(_)
		| expr::Expr::Foreach(_)
		| expr::Expr::Let(_)
		| expr::Expr::Sleep(_)
		| expr::Expr::Explain {
			..
		} => false,
	}
}

/// Observer text redacts password hashes, plaintext passwords and SCRAM
/// verifiers throughout nested expressions without preparing credentials.
impl ToSql for TopLevelStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Ast(stmt) => stmt.fmt_redacted_sql(f, fmt),
			Self::Plan(op) => op.fmt_sql(f, fmt),
			Self::Surreal(stmt) => stmt.text.fmt_sql(f, fmt),
		}
	}
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
	if matches!(kind, StatementType::Live) {
		QueryType::Live
	} else if matches!(kind, StatementType::Kill) {
		QueryType::Kill
	} else {
		QueryType::Other
	}
}

/// Classify a runtime [`TopLevelExpr`] into its bounded
/// [`StatementType`] category.
fn statement_type(expr: &TopLevelExpr) -> StatementType {
	match expr {
		TopLevelExpr::Begin(_) => StatementType::Begin,
		TopLevelExpr::Cancel => StatementType::Cancel,
		TopLevelExpr::Commit => StatementType::Commit,
		TopLevelExpr::Access(_) => StatementType::Access,
		TopLevelExpr::Kill(_) => StatementType::Kill,
		TopLevelExpr::Live(_) => StatementType::Live,
		TopLevelExpr::Option(_) => StatementType::Option,
		TopLevelExpr::Use(_) => StatementType::Use,
		TopLevelExpr::Show(_) => StatementType::Show,
		TopLevelExpr::Expr(expr) => expr_statement_type(expr),
	}
}

/// Classify a bare [`expr::Expr`] into its bounded [`StatementType`] category.
fn expr_statement_type(expr: &expr::Expr) -> StatementType {
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
		// Anything that isn't a recognised statement-shaped expression
		// collapses to `Other`. Enumerated rather than using a wildcard so a
		// new `Expr` variant forces a classification decision at compile time.
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
	}
}

/// Observer categories are derived from syntax independently of plan shape.
fn flat_statement_type(kind: surrealdb_expr::parser::StatementKind) -> StatementType {
	use surrealdb_expr::parser::StatementKind as Kind;
	match kind {
		Kind::Select => StatementType::Select,
		Kind::Create => StatementType::Create,
		Kind::Update => StatementType::Update,
		Kind::Upsert => StatementType::Upsert,
		Kind::Delete => StatementType::Delete,
		Kind::Relate => StatementType::Relate,
		Kind::Insert => StatementType::Insert,
		Kind::Info => StatementType::Info,
		Kind::Foreach => StatementType::Foreach,
		Kind::IfElse => StatementType::IfElse,
		Kind::Sleep => StatementType::Sleep,
		Kind::Explain => StatementType::Explain,
		Kind::Let => StatementType::Let,
		Kind::Return => StatementType::Return,
		Kind::Break => StatementType::Break,
		Kind::Continue => StatementType::Continue,
		Kind::Throw => StatementType::Throw,
		Kind::Block => StatementType::Block,
		Kind::Other => StatementType::Other,
	}
}
