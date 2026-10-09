//! Lowering from the surface AST to the logical plan.
//!
//! Pure and schema-free: no transaction, no catalog, no authority. A statement
//! lowers to the same tree for every caller, and everything that depends on
//! *who is asking* or *what the schema says* is added later by a pass over the
//! result.
//!
//! Lowering takes the surface AST only. A construct with no logical form is
//! reported as [`NoLogicalForm`] before any transaction is touched, and the
//! statement fails.

use std::collections::VecDeque;
use std::fmt::{self, Display, Formatter};
use std::ops::Bound;

use surrealdb_strand::{Strand, TableName};
use surrealdb_types::ToSql;

use super::expr::{
	LogicalCall, LogicalClosure, LogicalDestructureField, LogicalExpr, LogicalField, LogicalFields,
	LogicalIf, LogicalLiteral, LogicalLookup, LogicalLookupSubject, LogicalOrder, LogicalOrdering,
	LogicalPostfixOperator, LogicalRecordIdKey, LogicalRecordIdKeyRange, LogicalRecordIdLit,
	LogicalRecurse, LogicalRecurseInstruction, LogicalRecursePath, OutputPath, SourceRef,
};
use super::{
	LogicalAggregate, LogicalAssignment, LogicalBreak, LogicalContinue, LogicalData, LogicalDdl,
	LogicalDdlStatement, LogicalEval, LogicalExplain, LogicalFetch, LogicalFilter, LogicalForeach,
	LogicalIfOp, LogicalInfo, LogicalInsertRows, LogicalInserts, LogicalLet, LogicalLimit,
	LogicalOnly, LogicalOp, LogicalProject, LogicalReturn, LogicalSequence, LogicalSleep,
	LogicalSort, LogicalSource, LogicalSplit, LogicalSubquery, LogicalThrow, LogicalTimeout,
	LogicalUnionAll, LogicalValues, LogicalVersionScope, LogicalWrite, LogicalWriteSource,
	LogicalWriteStatement,
};
use crate::expr::part::Recurse;
use crate::sql;
use crate::sql::statements::{IfelseStatement, InfoStatement, SelectStatement};

/// Why a tree has no logical plan.
///
/// Every statement and expression the parser produces has a logical form,
/// schema statements and mutations included. What is reported here is a tree
/// no parser builds: a shape that only a caller constructing the surface
/// syntax by hand can write, such as an `INSERT` whose data clause names no
/// rows. It is a malformed input rather than a capability the engine lacks, so
/// a caller raises it as an error and has no other engine to hand the tree to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NoLogicalForm {
	/// A construct no parser produces.
	Unsupported(String),
}

impl Display for NoLogicalForm {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			NoLogicalForm::Unsupported(what) => write!(f, "{what} has no logical form"),
		}
	}
}

impl std::error::Error for NoLogicalForm {}

type Lowered<T> = Result<T, NoLogicalForm>;

/// Lower an expression in statement position to the operator that runs it.
///
/// A `SELECT` lowers to its pipeline; a mutation to a [`LogicalOp::Write`]; a
/// block to a [`LogicalOp::Sequence`]; `LET`, `RETURN`, `FOR`, `INFO`, `SLEEP`
/// and `EXPLAIN` to their operators; `IF`, `THROW`, `BREAK` and `CONTINUE` to
/// [`LogicalOp::If`], [`LogicalOp::Throw`], [`LogicalOp::Break`] and
/// [`LogicalOp::Continue`]. Any other expression — an arithmetic expression,
/// a function call — is a [`LogicalOp::Eval`] of its scalar form. A schema
/// statement is a [`LogicalOp::Ddl`] carrying it whole.
///
/// Statement position is the top level, a statement of a block (a `FOR` body
/// included), a `LET` value and an arm of a statement `IF`. The value of a
/// `RETURN` and the statement an `EXPLAIN` describes are lowered by
/// [`lower_value_statement`], which keeps an `IF`, `THROW`, `BREAK` or
/// `CONTINUE` there in its scalar form.
pub fn lower(expr: sql::Expr) -> Lowered<LogicalOp> {
	match expr {
		sql::Expr::IfElse(stmt) => lower_if_statement(*stmt),
		sql::Expr::Throw(value) => Ok(LogicalOp::Throw(LogicalThrow {
			value: lower_expr(*value)?,
		})),
		sql::Expr::Break => Ok(LogicalOp::Break(LogicalBreak)),
		sql::Expr::Continue => Ok(LogicalOp::Continue(LogicalContinue)),
		sql::Expr::Select(select) => lower_select(*select),
		sql::Expr::Block(block) => lower_block(*block),
		sql::Expr::Let(stmt) => Ok(LogicalOp::Let(LogicalLet {
			name: stmt.name.into_string(),
			kind: stmt.kind.map(Into::into),
			value: Box::new(lower(stmt.what)?),
		})),
		sql::Expr::Return(stmt) => Ok(LogicalOp::Return(LogicalReturn {
			what: Box::new(lower_value_statement(stmt.what)?),
			fetch: lower_fetchs(stmt.fetch)?,
		})),
		sql::Expr::Foreach(stmt) => Ok(LogicalOp::Foreach(LogicalForeach {
			param: stmt.param.into(),
			range: lower_expr(stmt.range)?,
			body: Box::new(lower_block(stmt.block)?),
		})),
		sql::Expr::Info(info) => Ok(LogicalOp::Info(Box::new(lower_info(*info)?))),
		sql::Expr::Sleep(stmt) => Ok(LogicalOp::Sleep(LogicalSleep {
			duration: LogicalExpr::Literal(LogicalLiteral::Duration(stmt.duration.into())),
		})),
		sql::Expr::Explain {
			format,
			analyze,
			statement,
		} => Ok(LogicalOp::Explain(LogicalExplain {
			input: Box::new(lower_value_statement(*statement)?),
			analyze,
			format: format.into(),
		})),
		sql::Expr::Create(stmt) => {
			// Targets, not rows read from storage. A creating statement's
			// subject does not exist yet, so the target is carried as the
			// value it evaluates to — a table to generate an id in, or a
			// record id to write at — the distinction `Value::Table` versus
			// `Value::RecordId` draws.
			let targets = lower_exprs(stmt.what.clone())?;
			let timeout = stmt.timeout.clone();
			with_timeout(
				LogicalOp::Write(LogicalWrite {
					data: stmt.data.clone().map(lower_data).transpose()?.flatten(),
					duplicate_update: None,
					cond: None,
					statement: LogicalWriteStatement::Create(Box::new((*stmt).into())),
					source: LogicalWriteSource::Targets(targets),
				}),
				timeout,
			)
		}
		sql::Expr::Update(stmt) => {
			// Rows: `UPDATE` writes records that already exist, so its targets
			// are read exactly as the same targets in a `FROM` clause are.
			let rows = lower_write_rows(stmt.what.clone(), stmt.with.clone())?;
			let timeout = stmt.timeout.clone();
			let explain = stmt.explain.as_ref().map(|sql::Explain(analyze)| *analyze);
			let op = with_timeout(
				LogicalOp::Write(LogicalWrite {
					data: stmt.data.clone().map(lower_data).transpose()?.flatten(),
					duplicate_update: None,
					cond: stmt.cond.clone().map(|c| lower_expr(c.0)).transpose()?,
					statement: LogicalWriteStatement::Update(Box::new((*stmt).into())),
					source: LogicalWriteSource::Rows(Box::new(rows)),
				}),
				timeout,
			)?;
			Ok(with_explain(op, explain))
		}
		sql::Expr::Upsert(stmt) => {
			// Targets, as for `CREATE`: an `UPSERT` subject need not exist, and
			// a reading source yields nothing for one that does not, which
			// would silently drop the write the statement exists to perform.
			// The write operator reconciles the two cases itself, retrying a
			// creating attempt that conflicts with an existing record as an
			// update.
			let targets = lower_exprs(stmt.what.clone())?;
			let timeout = stmt.timeout.clone();
			let explain = stmt.explain.as_ref().map(|sql::Explain(analyze)| *analyze);
			let op = with_timeout(
				LogicalOp::Write(LogicalWrite {
					data: stmt.data.clone().map(lower_data).transpose()?.flatten(),
					duplicate_update: None,
					cond: stmt.cond.clone().map(|c| lower_expr(c.0)).transpose()?,
					statement: LogicalWriteStatement::Upsert(Box::new((*stmt).into())),
					source: LogicalWriteSource::Targets(targets),
				}),
				timeout,
			)?;
			Ok(with_explain(op, explain))
		}
		sql::Expr::Delete(stmt) => {
			// Rows, as for `UPDATE`: a record must be read before it can be
			// removed.
			let rows = lower_write_rows(stmt.what.clone(), stmt.with.clone())?;
			let timeout = stmt.timeout.clone();
			let explain = stmt.explain.as_ref().map(|sql::Explain(analyze)| *analyze);
			let op = with_timeout(
				LogicalOp::Write(LogicalWrite {
					data: None,
					duplicate_update: None,
					cond: stmt.cond.clone().map(|c| lower_expr(c.0)).transpose()?,
					statement: LogicalWriteStatement::Delete(Box::new((*stmt).into())),
					source: LogicalWriteSource::Rows(Box::new(rows)),
				}),
				timeout,
			)?;
			Ok(with_explain(op, explain))
		}
		sql::Expr::Insert(stmt) => {
			// `INSERT` names no subjects to read: its rows *are* its subjects,
			// so they are the source. Nothing applies them to a record the
			// statement named elsewhere, which is what separates them from
			// every other form's data clause.
			let inserts = lower_inserts(stmt.into.clone(), stmt.data.clone())?;
			let timeout = stmt.timeout.clone();
			with_timeout(
				LogicalOp::Write(LogicalWrite {
					data: None,
					duplicate_update: stmt.update.clone().map(lower_data).transpose()?.flatten(),
					cond: None,
					statement: LogicalWriteStatement::Insert(Box::new((*stmt).into())),
					source: LogicalWriteSource::Inserts(inserts),
				}),
				timeout,
			)
		}
		sql::Expr::Relate(stmt) => {
			// Targets: the edge record `RELATE` writes does not exist yet, and
			// its endpoints are used as the ids they evaluate to rather than
			// read as documents. The list is `[from, through, to]`,
			// the order `LogicalWriteSource::Targets` documents for this form.
			let targets =
				lower_exprs(vec![stmt.from.clone(), stmt.through.clone(), stmt.to.clone()])?;
			let timeout = stmt.timeout.clone();
			with_timeout(
				LogicalOp::Write(LogicalWrite {
					data: stmt.data.clone().map(lower_data).transpose()?.flatten(),
					duplicate_update: None,
					cond: None,
					statement: LogicalWriteStatement::Relate(Box::new((*stmt).into())),
					source: LogicalWriteSource::Targets(targets),
				}),
				timeout,
			)
		}
		// A schema statement is carried whole. Its clauses stay surface
		// expressions, resolved where the catalog write is formed rather than
		// lowered here: a bare name in a DDL name position denotes itself, and
		// lowering it would make it a read of the row the statement runs
		// under.
		sql::Expr::Define(stmt) => Ok(LogicalOp::Ddl(LogicalDdl {
			statement: LogicalDdlStatement::Define(Box::new((*stmt).into())),
		})),
		sql::Expr::Remove(stmt) => Ok(LogicalOp::Ddl(LogicalDdl {
			statement: LogicalDdlStatement::Remove(Box::new((*stmt).into())),
		})),
		sql::Expr::Rebuild(stmt) => Ok(LogicalOp::Ddl(LogicalDdl {
			statement: LogicalDdlStatement::Rebuild(Box::new((*stmt).into())),
		})),
		sql::Expr::Alter(stmt) => Ok(LogicalOp::Ddl(LogicalDdl {
			statement: LogicalDdlStatement::Alter(Box::new((*stmt).into())),
		})),
		scalar @ (sql::Expr::Literal(_)
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
		| sql::Expr::Closure(_)) => Ok(LogicalOp::Eval(LogicalEval {
			expr: lower_expr(scalar)?,
		})),
	}
}

/// [`lower`] for a statement whose value is used where it stands — the value
/// of a `RETURN`, the statement an `EXPLAIN` describes: an `IF`, `THROW`,
/// `BREAK` or `CONTINUE` there is a [`LogicalOp::Eval`] of its scalar form,
/// and anything else lowers as [`lower`] lowers it.
pub fn lower_value_statement(expr: sql::Expr) -> Lowered<LogicalOp> {
	if matches!(
		expr,
		sql::Expr::IfElse(_) | sql::Expr::Throw(_) | sql::Expr::Break | sql::Expr::Continue
	) {
		return Ok(LogicalOp::Eval(LogicalEval {
			expr: lower_expr(expr)?,
		}));
	}
	lower(expr)
}

/// An `IF` in statement position: each arm lowered as a statement, so a block
/// arm is its [`LogicalOp::Sequence`] and a nested `IF` another
/// [`LogicalOp::If`].
///
/// An `IF` with an arm written as a `LET` — a `LET` as an expression, which
/// planning refuses whether or not the arm is taken — is a
/// [`LogicalOp::Eval`] of its scalar form, where that refusal is raised as
/// the statement is planned.
fn lower_if_statement(stmt: IfelseStatement) -> Lowered<LogicalOp> {
	if if_arm_is_a_let(&stmt) {
		return Ok(LogicalOp::Eval(LogicalEval {
			expr: lower_expr(sql::Expr::IfElse(Box::new(stmt)))?,
		}));
	}
	let branches = stmt
		.exprs
		.into_iter()
		.map(|(cond, then)| Ok((lower_expr(cond)?, lower(then)?)))
		.collect::<Lowered<Vec<_>>>()?;
	let otherwise = stmt.close.map(lower).transpose()?;
	Ok(LogicalOp::If(LogicalIfOp::new(branches, otherwise)))
}

/// Whether an arm of `stmt` is a `LET` written in place of a value, here or in
/// an `IF` written as an arm. An arm that is a block holding a `LET` is not:
/// the `LET` is a statement of the block.
fn if_arm_is_a_let(stmt: &IfelseStatement) -> bool {
	#[expect(clippy::wildcard_enum_match_arm, reason = "any other arm is a value or a statement")]
	fn is_a_let(arm: &sql::Expr) -> bool {
		match arm {
			sql::Expr::Let(_) => true,
			sql::Expr::IfElse(nested) => if_arm_is_a_let(nested),
			_ => false,
		}
	}
	stmt.exprs.iter().any(|(_, then)| is_a_let(then)) || stmt.close.as_ref().is_some_and(is_a_let)
}

/// Lower an expression in scalar position.
///
/// Scalars map to their [`LogicalExpr`] form. A statement in scalar position —
/// a `SELECT`, a mutation, a block, a `RETURN`, a `LET` — becomes
/// [`LogicalExpr::Subquery`] of the operator [`lower`] produces for it. DDL
/// statements have no logical form.
pub fn lower_expr(expr: sql::Expr) -> Lowered<LogicalExpr> {
	Ok(match expr {
		sql::Expr::Literal(literal) => lower_literal(literal)?,
		sql::Expr::Param(param) => LogicalExpr::Param(param.into()),
		sql::Expr::Idiom(idiom) => lower_idiom(idiom)?,
		sql::Expr::Table(table) => LogicalExpr::Table(table.into()),
		sql::Expr::Mock(mock) => LogicalExpr::Mock(mock.into()),
		sql::Expr::Constant(constant) => LogicalExpr::Constant(constant.into()),
		sql::Expr::Prefix {
			op,
			expr,
		} => LogicalExpr::Prefix {
			op: op.into(),
			expr: Box::new(lower_expr(*expr)?),
		},
		sql::Expr::Postfix {
			expr,
			op,
		} => lower_postfix(lower_expr(*expr)?, op)?,
		sql::Expr::Binary {
			left,
			op,
			right,
		} => LogicalExpr::Binary {
			left: Box::new(lower_expr(*left)?),
			op: op.into(),
			right: Box::new(lower_expr(*right)?),
		},
		sql::Expr::FunctionCall(call) => LogicalExpr::Call(Box::new(LogicalCall {
			receiver: call.receiver.into(),
			args: lower_exprs(call.arguments)?,
		})),
		sql::Expr::Closure(closure) => {
			let body_sql = std::sync::Arc::from(closure.body.to_sql());
			LogicalExpr::Closure(Box::new(LogicalClosure {
				args: closure.args.into_iter().map(|(p, k)| (p.into(), k.into())).collect(),
				returns: closure.returns.map(Into::into),
				body: lower_expr(closure.body)?,
				body_sql,
			}))
		}
		sql::Expr::Break => LogicalExpr::Break,
		sql::Expr::Continue => LogicalExpr::Continue,
		sql::Expr::Throw(expr) => LogicalExpr::Throw(Box::new(lower_expr(*expr)?)),
		sql::Expr::IfElse(stmt) => LogicalExpr::If(Box::new(lower_if(*stmt)?)),
		statement @ (sql::Expr::Block(_)
		| sql::Expr::Return(_)
		| sql::Expr::Select(_)
		| sql::Expr::Info(_)
		| sql::Expr::Foreach(_)
		| sql::Expr::Let(_)
		| sql::Expr::Sleep(_)
		| sql::Expr::Explain {
			..
		}
		| sql::Expr::Create(_)
		| sql::Expr::Update(_)
		| sql::Expr::Upsert(_)
		| sql::Expr::Delete(_)
		| sql::Expr::Insert(_)
		| sql::Expr::Relate(_)
		| sql::Expr::Define(_)
		| sql::Expr::Remove(_)
		| sql::Expr::Rebuild(_)
		| sql::Expr::Alter(_)) => LogicalExpr::Subquery(Box::new(lower(statement)?)),
	})
}

/// Lower a `SELECT` into its operator tree.
///
/// The operator order is the pipeline the physical planner builds:
///
/// ```text
/// Explain ← Only ← VersionScope ← Timeout ← Fetch ← Project ← Limit ← Sort
///         ← Aggregate ← Split ← Filter ← Source/Values/Subquery/UnionAll
/// ```
///
/// read outermost-first; the tree below is written with the leaves last.
/// Clauses that are absent contribute no operator, so a bare
/// `SELECT * FROM t` lowers to `Project` over `Source`.
pub fn lower_select(select: SelectStatement) -> Lowered<LogicalOp> {
	let SelectStatement {
		fields,
		omit,
		only,
		what,
		with,
		cond,
		split,
		group,
		order,
		limit,
		start,
		fetch,
		version,
		timeout,
		explain,
		tempfiles,
		for_update,
	} = select;

	// FROM.
	let mut op = lower_sources(what, with, for_update)?;

	// WHERE.
	if let Some(cond) = cond {
		op = LogicalOp::Filter(LogicalFilter {
			input: Box::new(op),
			predicate: lower_expr(cond.0)?,
		});
	}

	// SPLIT ON.
	if let Some(splits) = split {
		op = LogicalOp::Split(LogicalSplit {
			input: Box::new(op),
			exprs: splits.0.into_iter().map(|s| lower_idiom(s.0)).collect::<Lowered<_>>()?,
		});
	}

	// GROUP BY. The aggregation evaluates the projection itself, so the
	// projection below is reduced to a pass-through carrying only OMIT.
	let fields = lower_fields(fields)?;
	let grouped = group.is_some();
	if let Some(groups) = group {
		op = LogicalOp::Aggregate(LogicalAggregate {
			input: Box::new(op),
			group: groups.0.into_iter().map(|g| lower_idiom(g.0)).collect::<Lowered<_>>()?,
			fields: fields.clone(),
		});
	}

	// ORDER BY.
	if let Some(ordering) = order {
		op = LogicalOp::Sort(LogicalSort {
			input: Box::new(op),
			ordering: lower_ordering(ordering)?,
			tempfiles,
		});
	}

	// LIMIT / START.
	if limit.is_some() || start.is_some() {
		op = LogicalOp::Limit(LogicalLimit {
			input: Box::new(op),
			limit: limit.map(|l| lower_expr(l.0)).transpose()?,
			start: start.map(|s| lower_expr(s.0)).transpose()?,
		});
	}

	// The projection. A grouped query has already projected, so only OMIT is
	// left to apply; a grouped one with nothing to omit needs no operator.
	if !grouped || !omit.is_empty() {
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields: if grouped {
				LogicalFields::all()
			} else {
				fields
			},
			omit: lower_exprs(omit)?,
		});
	}

	// FETCH.
	let fetches = lower_fetchs(fetch)?;
	if !fetches.is_empty() {
		op = LogicalOp::Fetch(LogicalFetch {
			input: Box::new(op),
			fetches,
		});
	}

	// TIMEOUT, VERSION and ONLY. The first two are absent as a `NONE` literal
	// rather than as an `Option`.
	if !is_absent(&timeout) {
		op = LogicalOp::Timeout(LogicalTimeout {
			input: Box::new(op),
			timeout: lower_expr(timeout)?,
		});
	}
	if !is_absent(&version) {
		op = LogicalOp::VersionScope(LogicalVersionScope {
			input: Box::new(op),
			version: lower_expr(version)?,
		});
	}
	if only {
		op = LogicalOp::Only(LogicalOnly {
			input: Box::new(op),
		});
	}

	// EXPLAIN describes everything below it, so it wraps last. The clause form
	// has no FORMAT and answers with the structured plan; only the statement
	// form (`EXPLAIN [FORMAT ...] SELECT ...`) chooses text.
	if let Some(sql::Explain(analyze)) = explain {
		op = LogicalOp::Explain(LogicalExplain {
			input: Box::new(op),
			analyze,
			format: crate::expr::ExplainFormat::Json,
		});
	}

	Ok(op)
}

/// Lower a statement's list of targets to the subtree that reads them.
///
/// Several targets concatenate; the `WITH` hint and `FOR UPDATE` apply to each
/// stored source, matching how the physical planner resolves one access path
/// per source. The same list means the same subtree wherever it is written, so
/// a `SELECT`'s `FROM` and an updating mutation's targets share this lowering.
fn lower_sources(
	what: Vec<sql::Expr>,
	with: Option<sql::With>,
	for_update: bool,
) -> Lowered<LogicalOp> {
	let with = with.map(crate::expr::With::from);
	let leaves = what
		.into_iter()
		.map(|source| lower_source(source, with.as_ref(), for_update))
		.collect::<Lowered<Vec<_>>>()?;
	Ok(match leaves.len() {
		1 => leaves.into_iter().next().expect("length checked"),
		_ => LogicalOp::UnionAll(LogicalUnionAll {
			inputs: leaves,
		}),
	})
}

/// Lower an updating mutation's targets to the rows it writes: the same
/// subtree the same targets produce in a `FROM` clause. `FOR UPDATE` has no
/// spelling on these statements — the mutation's own write takes the lock.
///
/// The statement's `WHERE` is deliberately left out of that subtree, and must
/// stay out of it. On the updating shapes the per-record table permission gate
/// runs before the statement's condition, so that a condition with side
/// effects runs only for records the caller may write
/// (`doc/adr/0001-streaming-write-path.md`). Everything in this subtree runs
/// beneath the write operator and therefore ahead of every one of its stages,
/// so a condition lowered into a [`LogicalOp::Filter`] here — or pushed into
/// the scan — would be evaluated before that gate. The condition stays a
/// clause of the statement, which the write operator's `CheckWhereCondition`
/// stage evaluates in its place in the order.
fn lower_write_rows(what: Vec<sql::Expr>, with: Option<sql::With>) -> Lowered<LogicalOp> {
	lower_sources(what, with, false)
}

/// A mutation's data clause in logical form, so the rules can rewrite what it
/// evaluates.
///
/// The places stay as written: a place is a location rather than a value, and
/// what a path names is settled where the write is compiled. An empty clause
/// has no form — there is nothing to evaluate — and the `INSERT` spellings
/// that name rows rather than a clause are carried by
/// [`LogicalWriteSource::Inserts`] instead.
fn lower_data(data: sql::Data) -> Lowered<Option<LogicalData>> {
	Ok(Some(match data {
		sql::Data::EmptyExpression => return Ok(None),
		sql::Data::SetExpression(assignments) | sql::Data::UpdateExpression(assignments) => {
			LogicalData::Set(
				assignments
					.into_iter()
					.map(|a| {
						Ok(LogicalAssignment {
							place: a.place.into(),
							operator: a.operator.into(),
							value: lower_expr(a.value)?,
						})
					})
					.collect::<Lowered<Vec<_>>>()?,
			)
		}
		sql::Data::UnsetExpression(paths) => {
			LogicalData::Unset(paths.into_iter().map(Into::into).collect())
		}
		sql::Data::PatchExpression(expr) => LogicalData::Patch(lower_expr(expr)?),
		sql::Data::MergeExpression(expr) => LogicalData::Merge(lower_expr(expr)?),
		sql::Data::ReplaceExpression(expr) => LogicalData::Replace(lower_expr(expr)?),
		sql::Data::ContentExpression(expr) => LogicalData::Content(lower_expr(expr)?),
		sql::Data::SingleExpression(expr) => LogicalData::Single(lower_expr(expr)?),
		sql::Data::ValuesExpression(rows) => LogicalData::Values(
			rows.into_iter()
				.map(|row| {
					row.into_iter()
						.map(|(path, value)| Ok((path.into(), lower_expr(value)?)))
						.collect::<Lowered<Vec<_>>>()
				})
				.collect::<Lowered<Vec<_>>>()?,
		),
	}))
}

/// Lower `INSERT`'s subjects: its `INTO` table, and the rows it writes.
///
/// The rows are the source rather than a clause. Every other form applies its
/// data to a subject named elsewhere; `INSERT` has no such subject, so each row
/// *is* one — the record to write, with the table taken from `INTO` where the
/// statement has one and from the row's own record id where it does not.
///
/// Only the two spellings that name rows have a form here. Any other data
/// clause on an `INSERT` names none, and lowering declines rather than
/// silently writing records the statement did not describe.
fn lower_inserts(into: Option<sql::Expr>, data: sql::Data) -> Lowered<LogicalInserts> {
	let rows = match data {
		sql::Data::ValuesExpression(tuples) => LogicalInsertRows::Tuples(
			tuples
				.into_iter()
				.map(|tuple| {
					tuple
						.into_iter()
						.map(|(path, value)| Ok((path.into(), lower_expr(value)?)))
						.collect::<Lowered<Vec<_>>>()
				})
				.collect::<Lowered<Vec<_>>>()?,
		),
		sql::Data::SingleExpression(expr) => LogicalInsertRows::Value(lower_expr(expr)?),
		sql::Data::EmptyExpression
		| sql::Data::SetExpression(_)
		| sql::Data::UnsetExpression(_)
		| sql::Data::PatchExpression(_)
		| sql::Data::MergeExpression(_)
		| sql::Data::ReplaceExpression(_)
		| sql::Data::ContentExpression(_)
		| sql::Data::UpdateExpression(_) => {
			return Err(NoLogicalForm::Unsupported(
				"an INSERT whose data clause names no rows".to_string(),
			));
		}
	};
	Ok(LogicalInserts {
		into: into.map(lower_expr).transpose()?,
		rows,
	})
}

/// Lower one `FROM` expression to its leaf.
///
/// A nested `SELECT` is lowered eagerly. A literal that produces rows without
/// reading storage becomes [`LogicalOp::Values`]. Everything else is a
/// [`LogicalOp::Source`] whose [`SourceRef`] classifies it syntactically for
/// the physical planner to resolve.
///
/// `FOR UPDATE` is carried on each source of the statement, so the physical
/// planner judges every target against the clause's rules in one place. A
/// nested `SELECT` under the clause is therefore not a [`LogicalOp::Subquery`]
/// leaf, which has no flag to carry, but a [`LogicalOp::Source`] holding the
/// nested plan as a [`LogicalExpr::Subquery`] scalar — a
/// [`SourceRef::Dynamic`] — which the planner rejects as it rejects any other
/// target that is not a record.
fn lower_source(
	source: sql::Expr,
	with: Option<&crate::expr::With>,
	for_update: bool,
) -> Lowered<LogicalOp> {
	if let sql::Expr::Select(inner) = source {
		let inner = lower_select(*inner)?;
		return Ok(if for_update {
			LogicalOp::Source(LogicalSource {
				source: SourceRef::classify(LogicalExpr::Subquery(Box::new(inner))),
				schema: None,
				with: with.cloned(),
				for_update: true,
			})
		} else {
			LogicalOp::Subquery(LogicalSubquery {
				input: Box::new(inner),
			})
		});
	}
	let source = lower_expr(source)?;
	Ok(if is_value_literal(&source) {
		LogicalOp::Values(LogicalValues {
			written: matches!(source, LogicalExpr::Array(_)),
			expr: source,
			for_update,
		})
	} else {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::classify(source),
			schema: None,
			with: with.cloned(),
			for_update,
		})
	})
}

/// Whether a `FROM` expression is a literal that yields rows from its own
/// value rather than from storage. A record id is not one: it names stored
/// rows. Neither is an object literal, which is treated as a source so its
/// `ONLY` form errors when empty.
///
/// The classification is purely syntactic, so a pass that rewrites a
/// [`LogicalOp::Source`]'s expression into one of these forms re-applies it
/// to turn the node into a [`LogicalOp::Values`].
pub fn is_value_literal(expr: &LogicalExpr) -> bool {
	match expr {
		LogicalExpr::Array(_) => true,
		LogicalExpr::Literal(literal) => match literal {
			LogicalLiteral::String(_)
			| LogicalLiteral::Integer(_)
			| LogicalLiteral::Float(_)
			| LogicalLiteral::Decimal(_)
			| LogicalLiteral::Bool(_)
			| LogicalLiteral::None
			| LogicalLiteral::Null => true,
			// Treated as stored sources so that `ONLY` over one errors on an
			// empty result rather than yielding `NONE`. Matched exhaustively:
			// a new literal must be classified here deliberately.
			LogicalLiteral::Bytes(_)
			| LogicalLiteral::Regex(_)
			| LogicalLiteral::Geometry(_)
			| LogicalLiteral::File(_)
			| LogicalLiteral::Uuid(_)
			| LogicalLiteral::Datetime(_)
			| LogicalLiteral::Duration(_)
			| LogicalLiteral::UnboundedRange => false,
		},
		LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
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
		| LogicalExpr::Continue
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Subquery(_) => false,
	}
}

/// Whether an optional clause expression is the absent marker. `VERSION` and
/// `TIMEOUT` hold a bare expression and spell "not written" as `NONE`.
/// Wrap a mutation in its `EXPLAIN` clause, `analyze` being whether the clause
/// said `FULL`.
///
/// Outermost, as in a `SELECT`: what the plan describes is everything beneath
/// it, the timeout included. The clause form has no `FORMAT` and answers with
/// the structured plan. Only the three updating forms have the clause; the
/// creating ones have no grammar for it.
fn with_explain(op: LogicalOp, analyze: Option<bool>) -> LogicalOp {
	match analyze {
		Some(analyze) => LogicalOp::Explain(LogicalExplain {
			input: Box::new(op),
			analyze,
			format: crate::expr::ExplainFormat::Json,
		}),
		None => op,
	}
}

/// Wrap a mutation in its `TIMEOUT` clause.
///
/// A mutation carries the clause on the statement rather than on a source, so
/// it becomes the node above the write operator — the same node a `SELECT`'s
/// `TIMEOUT` lowers to, holding the same budget over everything beneath it.
fn with_timeout(op: LogicalOp, timeout: sql::Expr) -> Lowered<LogicalOp> {
	if is_absent(&timeout) {
		return Ok(op);
	}
	Ok(LogicalOp::Timeout(LogicalTimeout {
		input: Box::new(op),
		timeout: lower_expr(timeout)?,
	}))
}

fn is_absent(expr: &sql::Expr) -> bool {
	matches!(expr, sql::Expr::Literal(sql::Literal::None))
}

fn lower_block(block: sql::Block) -> Lowered<LogicalOp> {
	Ok(LogicalOp::Sequence(LogicalSequence {
		stmts: block.0.into_iter().map(lower).collect::<Lowered<_>>()?,
	}))
}

fn lower_exprs(exprs: Vec<sql::Expr>) -> Lowered<Vec<LogicalExpr>> {
	exprs.into_iter().map(lower_expr).collect()
}

fn lower_fetchs(fetch: Option<sql::Fetchs>) -> Lowered<Vec<LogicalExpr>> {
	fetch.map_or(Ok(Vec::new()), |fetchs| fetchs.0.into_iter().map(|f| lower_expr(f.0)).collect())
}

fn lower_if(stmt: IfelseStatement) -> Lowered<LogicalIf> {
	Ok(LogicalIf {
		branches: stmt
			.exprs
			.into_iter()
			.map(|(cond, then)| Ok((lower_expr(cond)?, lower_expr(then)?)))
			.collect::<Lowered<_>>()?,
		otherwise: stmt.close.map(lower_expr).transpose()?,
	})
}

fn lower_info(info: InfoStatement) -> Lowered<LogicalInfo> {
	let version = |v: Option<sql::Expr>| v.map(lower_expr).transpose();
	Ok(match info {
		InfoStatement::Root(structure, v) => LogicalInfo::Root {
			structure,
			version: version(v)?,
		},
		InfoStatement::Ns(structure, v) => LogicalInfo::Ns {
			structure,
			version: version(v)?,
		},
		InfoStatement::Db(structure, v) => LogicalInfo::Db {
			structure,
			version: version(v)?,
		},
		InfoStatement::Tb(table, structure, v) => LogicalInfo::Table {
			table: lower_expr(table)?,
			structure,
			version: version(v)?,
		},
		InfoStatement::User(user, base, structure) => LogicalInfo::User {
			user: lower_expr(user)?,
			base: base.map(Into::into),
			structure,
		},
		InfoStatement::Index(index, table, structure) => LogicalInfo::Index {
			index: lower_expr(index)?,
			table: lower_expr(table)?,
			structure,
		},
	})
}

/// Whether an object literal has the two-entry `{ type: "…", … }` shape a
/// GeoJSON geometry takes. Only such an object is offered to the geometry
/// conversion; every other object is an object.
fn looks_like_geometry(entries: &[sql::literal::ObjectEntry]) -> bool {
	entries.len() == 2
		&& entries.iter().any(|entry| {
			entry.key == "type"
				&& matches!(entry.value, sql::Expr::Literal(sql::Literal::String(_)))
		})
}

fn lower_literal(literal: sql::Literal) -> Lowered<LogicalExpr> {
	let leaf = |literal| Ok(LogicalExpr::Literal(literal));
	match literal {
		sql::Literal::None => leaf(LogicalLiteral::None),
		sql::Literal::Null => leaf(LogicalLiteral::Null),
		sql::Literal::UnboundedRange => leaf(LogicalLiteral::UnboundedRange),
		sql::Literal::Bool(x) => leaf(LogicalLiteral::Bool(x)),
		sql::Literal::Float(x) => leaf(LogicalLiteral::Float(x)),
		sql::Literal::Integer(x) => leaf(LogicalLiteral::Integer(x)),
		sql::Literal::Decimal(x) => leaf(LogicalLiteral::Decimal(x)),
		sql::Literal::Duration(x) => leaf(LogicalLiteral::Duration(x.into())),
		sql::Literal::String(x) => leaf(LogicalLiteral::String(x)),
		sql::Literal::Datetime(x) => leaf(LogicalLiteral::Datetime(x.into())),
		sql::Literal::Uuid(x) => leaf(LogicalLiteral::Uuid(x.into())),
		sql::Literal::Regex(x) => leaf(LogicalLiteral::Regex(x.into())),
		sql::Literal::Geometry(x) => leaf(LogicalLiteral::Geometry(x.into())),
		sql::Literal::File(x) => leaf(LogicalLiteral::File(x.into())),
		sql::Literal::Bytes(x) => leaf(LogicalLiteral::Bytes(x.into())),
		sql::Literal::RecordId(rid) => Ok(LogicalExpr::RecordId(Box::new(lower_record_id(rid)?))),
		sql::Literal::Array(items) => Ok(LogicalExpr::Array(lower_exprs(items)?)),
		sql::Literal::Set(items) => Ok(LogicalExpr::Set(lower_exprs(items)?)),
		sql::Literal::Object(entries) => {
			// An object literal in GeoJSON shape denotes a geometry; the
			// shared conversion decides.
			if looks_like_geometry(&entries)
				&& let crate::expr::Literal::Geometry(geometry) =
					crate::expr::Literal::from(sql::Literal::Object(entries.clone()))
			{
				return leaf(LogicalLiteral::Geometry(geometry));
			}
			Ok(LogicalExpr::Object(lower_entries(entries)?))
		}
	}
}

fn lower_entries(entries: Vec<sql::literal::ObjectEntry>) -> Lowered<Vec<(String, LogicalExpr)>> {
	entries.into_iter().map(|e| Ok((e.key.into_string(), lower_expr(e.value)?))).collect()
}

fn lower_record_id(rid: sql::RecordIdLit) -> Lowered<LogicalRecordIdLit> {
	Ok(LogicalRecordIdLit {
		table: rid.table.into(),
		key: lower_record_id_key(rid.key)?,
	})
}

fn lower_record_id_key(key: sql::RecordIdKeyLit) -> Lowered<LogicalRecordIdKey> {
	Ok(match key {
		sql::RecordIdKeyLit::Number(x) => LogicalRecordIdKey::Number(x),
		sql::RecordIdKeyLit::String(x) => LogicalRecordIdKey::String(x),
		sql::RecordIdKeyLit::Uuid(x) => LogicalRecordIdKey::Uuid(x.into()),
		sql::RecordIdKeyLit::Array(items) => LogicalRecordIdKey::Array(lower_exprs(items)?),
		sql::RecordIdKeyLit::Object(entries) => LogicalRecordIdKey::Object(lower_entries(entries)?),
		sql::RecordIdKeyLit::Generate(generator) => LogicalRecordIdKey::Generate(generator.into()),
		sql::RecordIdKeyLit::Range(range) => {
			LogicalRecordIdKey::Range(Box::new(lower_record_id_range(*range)?))
		}
	})
}

fn lower_record_id_range(range: sql::RecordIdKeyRangeLit) -> Lowered<LogicalRecordIdKeyRange> {
	let bound = |b: Bound<sql::RecordIdKeyLit>| {
		Ok(match b {
			Bound::Included(key) => Bound::Included(lower_record_id_key(key)?),
			Bound::Excluded(key) => Bound::Excluded(lower_record_id_key(key)?),
			Bound::Unbounded => Bound::Unbounded,
		})
	};
	Ok(LogicalRecordIdKeyRange {
		start: bound(range.start)?,
		end: bound(range.end)?,
	})
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

/// One step of a surface idiom, with the expressions it holds lowered.
enum Step {
	Access(Strand),
	Index(LogicalExpr),
	First,
	Last,
	Each,
	Flatten,
	Filter(LogicalExpr),
	Method(Strand, Vec<LogicalExpr>),
	Destructure(Vec<LogicalDestructureField>),
	Optional,
	Lookup(Box<LogicalLookup>),
	/// The body is the written `(…)` path, when there is one.
	Recurse(Recurse, Option<LogicalExpr>, Option<LogicalRecurseInstruction>),
	RepeatRecurse,
}

impl Step {
	/// Whether the step applies to each element of an array on its own:
	/// a field read, `.*` or a destructure.
	fn is_mapping(&self) -> bool {
		matches!(self, Step::Access(_) | Step::Each | Step::Destructure(_))
	}
}

/// Whether the array-continuation rule applies while folding a sequence of
/// steps.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PathMode {
	/// The path as written: a mapping step or a lookup over an array applies
	/// the rest of the path to each element ([`LogicalExpr::Broadcast`]).
	Continuation,
	/// A body — of an [`LogicalExpr::Optional`], a [`LogicalExpr::Recurse`]
	/// or a destructure alias — whose steps apply one after another to the
	/// value as it is.
	Plain,
}

/// The kind of the step folded last, as far as the lookup-continuation rule
/// distinguishes them.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Prev {
	Lookup,
	Flatten,
	Filter,
	Other,
}

/// Lower an idiom in expression position.
///
/// The first part is the root: a field is a [`LogicalExpr::Field`], `@` is
/// [`LogicalExpr::Doc`], a starting expression is itself, and a leading lookup
/// applies to [`LogicalExpr::Current`], the row. Every later part is a step
/// folded onto the root by [`fold_steps`] in [`PathMode::Continuation`].
///
/// A root written after the first part restarts the path ([`restart_at_root`]).
fn lower_idiom(idiom: sql::Idiom) -> Lowered<LogicalExpr> {
	let mut parts = restart_at_root(idiom.0).into_iter().peekable();
	let base = match parts.peek() {
		Some(sql::Part::Field(_)) => match parts.next() {
			Some(sql::Part::Field(name)) => LogicalExpr::Field(name),
			_ => unreachable!("peeked a field"),
		},
		Some(sql::Part::Start(_)) => match parts.next() {
			Some(sql::Part::Start(start)) => lower_expr(start)?,
			_ => unreachable!("peeked a start"),
		},
		Some(sql::Part::Doc) => {
			parts.next();
			LogicalExpr::Doc
		}
		Some(_) | None => LogicalExpr::Current,
	};
	let steps = lower_steps(parts)?;
	Ok(fold_steps(base, steps, PathMode::Continuation))
}

/// Lower an idiom that is a body over [`LogicalExpr::Current`]: the `(…)`
/// path of a recursion, or the path of a destructure alias. Every part is a
/// step, a leading field included; a leading starting expression or `@`
/// replaces `Current` as the base. Bodies fold in [`PathMode::Plain`].
fn lower_body(idiom: sql::Idiom) -> Lowered<LogicalExpr> {
	let mut parts = restart_at_root(idiom.0).into_iter().peekable();
	let base = match parts.peek() {
		Some(sql::Part::Start(_)) => match parts.next() {
			Some(sql::Part::Start(start)) => lower_expr(start)?,
			_ => unreachable!("peeked a start"),
		},
		Some(sql::Part::Doc) => {
			parts.next();
			LogicalExpr::Doc
		}
		Some(_) | None => LogicalExpr::Current,
	};
	let steps = lower_steps(parts)?;
	Ok(fold_steps(base, steps, PathMode::Plain))
}

/// The parts of a path from its last root on.
///
/// A starting expression or `@` is a root: it is the value the parts after it
/// apply to, wherever it is written. The parser writes one only first, so a
/// path holding one later is a tree built without it, and the parts before the
/// root are the value it replaces: they are not read, which is what a root
/// means rather than a step applied to what precedes it.
fn restart_at_root(mut parts: Vec<sql::Part>) -> Vec<sql::Part> {
	match parts.iter().rposition(|part| matches!(part, sql::Part::Start(_) | sql::Part::Doc)) {
		Some(at) if at > 0 => parts.split_off(at),
		_ => parts,
	}
}

fn lower_steps(parts: impl Iterator<Item = sql::Part>) -> Lowered<VecDeque<Step>> {
	parts
		.map(|part| {
			Ok(match part {
				sql::Part::All => Step::Each,
				sql::Part::Flatten => Step::Flatten,
				sql::Part::Last => Step::Last,
				sql::Part::First => Step::First,
				sql::Part::Field(name) => Step::Access(name),
				sql::Part::Where(expr) => Step::Filter(lower_expr(expr)?),
				sql::Part::Graph(lookup) => Step::Lookup(Box::new(lower_lookup(*lookup)?)),
				sql::Part::Value(expr) => Step::Index(lower_expr(expr)?),
				sql::Part::Method(name, args) => Step::Method(name, lower_exprs(args)?),
				sql::Part::Destructure(parts) => Step::Destructure(lower_destructure(parts)?),
				sql::Part::Optional => Step::Optional,
				sql::Part::Recurse(recurse, nested, instruction) => Step::Recurse(
					recurse.into(),
					nested.map(lower_body).transpose()?,
					instruction.map(lower_instruction).transpose()?,
				),
				sql::Part::RepeatRecurse => Step::RepeatRecurse,
				// A root is consumed before the steps are lowered
				// ([`restart_at_root`]): only the first part of a path can be
				// one by the time it gets here.
				sql::Part::Start(_) | sql::Part::Doc => {
					return Err(NoLogicalForm::Unsupported(
						"a root part that is not the first of its path".to_string(),
					));
				}
			})
		})
		.collect()
}

/// Fold `steps` onto `base`, one form per step, placing the forms that carry
/// a path's sequence semantics:
///
/// - **Array continuation** ([`PathMode::Continuation`] only). A mapping step (a field read, `.*`,
///   a destructure) followed by a step that is not a mapping step becomes `Broadcast { base, body
///   }`, where `body` is the step applied to `Current` followed by the rest of the path, folded on.
///   When every remaining step is a mapping step the steps nest plainly, since each maps over an
///   array base on its own. A lookup, or a run of consecutive lookups, whose previous step is not a
///   lookup or a flatten and which is followed by more steps or is a run of more than one hop
///   becomes a `Broadcast` the same way; its per-element results are flattened when the base is a
///   lookup or a flatten followed only by filters.
/// - **Auto-flatten.** A lookup (or run of lookups) followed by a filter has an implicit `Flatten`
///   between them, in every mode.
/// - **Optional.** `.?` takes the rest of the path as its body.
/// - **Recursion.** `.{n}` with no `(…)` takes the rest of the path as its body, up to and
///   including a `.@` at the body's top level; the steps after `.@` apply to the recursion's
///   result.
///
/// Bodies fold in [`PathMode::Plain`], except a `Broadcast` body, which
/// continues in the mode it was reached in: when the base is not an array,
/// the rest of the path is evaluated as the path it is, continuation
/// included.
fn fold_steps(base: LogicalExpr, steps: VecDeque<Step>, mode: PathMode) -> LogicalExpr {
	fold_from(base, steps, mode, Prev::Other, false)
}

/// [`fold_steps`] resumed after a step of kind `prev`, with
/// `array_from_lookup` recording whether the value so far is the result of a
/// lookup or a flatten narrowed only by filters.
fn fold_from(
	mut base: LogicalExpr,
	mut steps: VecDeque<Step>,
	mode: PathMode,
	mut prev: Prev,
	mut array_from_lookup: bool,
) -> LogicalExpr {
	while let Some(step) = steps.pop_front() {
		match step {
			Step::Optional => {
				let then = fold_steps(LogicalExpr::Current, steps, PathMode::Plain);
				return LogicalExpr::Optional {
					base: Box::new(base),
					then: Box::new(then),
				};
			}
			Step::Recurse(recurse, Some(nested), instruction) => {
				base = LogicalExpr::Recurse(Box::new(LogicalRecurse {
					base,
					recurse,
					path: Some(LogicalRecursePath::Nested(nested)),
					instruction,
				}));
				prev = Prev::Other;
				array_from_lookup = false;
			}
			Step::Recurse(recurse, None, instruction) => {
				// The body runs up to and including the first `.@` at this
				// level; what follows applies once to the result.
				let suffix = match steps.iter().position(|s| matches!(s, Step::RepeatRecurse)) {
					Some(pos) => steps.split_off(pos + 1),
					None => VecDeque::new(),
				};
				let path = if steps.is_empty() {
					None
				} else {
					Some(LogicalRecursePath::Trailing(fold_steps(
						LogicalExpr::Current,
						steps,
						PathMode::Plain,
					)))
				};
				base = LogicalExpr::Recurse(Box::new(LogicalRecurse {
					base,
					recurse,
					path,
					instruction,
				}));
				steps = suffix;
				prev = Prev::Other;
				array_from_lookup = false;
			}
			Step::Lookup(first) => {
				let mut lookups = vec![*first];
				while let Some(Step::Lookup(_)) = steps.front() {
					if let Some(Step::Lookup(lookup)) = steps.pop_front() {
						lookups.push(*lookup);
					}
				}
				let fused = lookups.len() > 1;
				let followed = !steps.is_empty();
				let continues = mode == PathMode::Continuation
					&& !matches!(prev, Prev::Lookup | Prev::Flatten)
					&& (followed || fused);
				if continues {
					let inner = apply_lookups(LogicalExpr::Current, lookups, &steps);
					let body = fold_from(inner, steps, mode, Prev::Lookup, true);
					return LogicalExpr::Broadcast {
						base: Box::new(base),
						body: Box::new(body),
						flatten: array_from_lookup,
					};
				}
				base = apply_lookups(base, lookups, &steps);
				prev = Prev::Lookup;
				array_from_lookup = true;
			}
			step @ (Step::Access(_) | Step::Each | Step::Destructure(_)) => {
				let broadcast = mode == PathMode::Continuation
					&& !steps.is_empty()
					&& !steps.iter().all(Step::is_mapping);
				if broadcast {
					let inner = apply_step(LogicalExpr::Current, step);
					let body = fold_from(inner, steps, mode, Prev::Other, false);
					return LogicalExpr::Broadcast {
						base: Box::new(base),
						body: Box::new(body),
						flatten: false,
					};
				}
				base = apply_step(base, step);
				prev = Prev::Other;
				array_from_lookup = false;
			}
			Step::Filter(predicate) => {
				base = LogicalExpr::Filter {
					base: Box::new(base),
					predicate: Box::new(predicate),
				};
				// A filter narrows the array without changing where it came
				// from.
				prev = Prev::Filter;
			}
			Step::Flatten => {
				base = LogicalExpr::Flatten {
					base: Box::new(base),
					explicit: true,
				};
				prev = Prev::Flatten;
				array_from_lookup = true;
			}
			step @ (Step::Index(_)
			| Step::First
			| Step::Last
			| Step::Method(..)
			| Step::RepeatRecurse) => {
				base = apply_step(base, step);
				prev = Prev::Other;
				array_from_lookup = false;
			}
		}
	}
	base
}

/// Nest a run of consecutive lookups onto `base`, with the implicit flatten
/// a following filter expects.
fn apply_lookups(
	mut base: LogicalExpr,
	lookups: Vec<LogicalLookup>,
	rest: &VecDeque<Step>,
) -> LogicalExpr {
	for lookup in lookups {
		base = LogicalExpr::Lookup {
			base: Box::new(base),
			lookup: Box::new(lookup),
		};
	}
	if let Some(Step::Filter(_)) = rest.front() {
		base = LogicalExpr::Flatten {
			base: Box::new(base),
			explicit: false,
		};
	}
	base
}

/// The form of one step applied to `base`. Steps with sequence semantics of
/// their own — optional, recursion, lookup runs — are placed by
/// [`fold_from`], never here.
fn apply_step(base: LogicalExpr, step: Step) -> LogicalExpr {
	let base = Box::new(base);
	match step {
		Step::Access(field) => LogicalExpr::Access {
			base,
			field,
		},
		Step::Index(index) => LogicalExpr::Index {
			base,
			index: Box::new(index),
		},
		Step::First => LogicalExpr::First(base),
		Step::Last => LogicalExpr::Last(base),
		Step::Each => LogicalExpr::Each(base),
		Step::Flatten => LogicalExpr::Flatten {
			base,
			explicit: true,
		},
		Step::Filter(predicate) => LogicalExpr::Filter {
			base,
			predicate: Box::new(predicate),
		},
		Step::Method(name, args) => LogicalExpr::Method {
			base,
			name,
			args,
		},
		Step::Destructure(fields) => LogicalExpr::Destructure {
			base,
			fields,
		},
		Step::RepeatRecurse => LogicalExpr::RepeatRecurse(base),
		Step::Lookup(lookup) => LogicalExpr::Lookup {
			base,
			lookup,
		},
		Step::Recurse(recurse, nested, instruction) => {
			LogicalExpr::Recurse(Box::new(LogicalRecurse {
				base: *base,
				recurse,
				path: nested.map(LogicalRecursePath::Nested),
				instruction,
			}))
		}
		Step::Optional => LogicalExpr::Optional {
			base,
			then: Box::new(LogicalExpr::Current),
		},
	}
}

/// Lower an idiom that names an output position: a projection alias, a
/// lookup alias.
fn lower_output_path(idiom: sql::Idiom) -> Lowered<OutputPath> {
	Ok(lower_idiom(idiom)?.output_path())
}

fn lower_destructure(
	parts: Vec<sql::part::DestructurePart>,
) -> Lowered<Vec<LogicalDestructureField>> {
	parts
		.into_iter()
		.map(|part| {
			Ok(match part {
				sql::part::DestructurePart::All(name) => LogicalDestructureField::All(name),
				sql::part::DestructurePart::Field(name) => LogicalDestructureField::Field(name),
				sql::part::DestructurePart::Aliased(name, idiom) => {
					LogicalDestructureField::Aliased(name, lower_body(idiom)?)
				}
				sql::part::DestructurePart::Destructure(name, parts) => {
					LogicalDestructureField::Destructure(name, lower_destructure(parts)?)
				}
				sql::part::DestructurePart::Rest => LogicalDestructureField::Rest,
			})
		})
		.collect()
}

fn lower_instruction(
	instruction: sql::part::RecurseInstruction,
) -> Lowered<LogicalRecurseInstruction> {
	Ok(match instruction {
		sql::part::RecurseInstruction::Path {
			inclusive,
		} => LogicalRecurseInstruction::Path {
			inclusive,
		},
		sql::part::RecurseInstruction::Collect {
			inclusive,
		} => LogicalRecurseInstruction::Collect {
			inclusive,
		},
		sql::part::RecurseInstruction::Shortest {
			expects,
			inclusive,
		} => LogicalRecurseInstruction::Shortest {
			expects: lower_expr(expects)?,
			inclusive,
		},
	})
}

fn lower_lookup(lookup: sql::lookup::Lookup) -> Lowered<LogicalLookup> {
	Ok(LogicalLookup {
		kind: lookup.kind.into(),
		expr: lookup.expr.map(lower_fields).transpose()?,
		only: lookup.only,
		what: lookup.what.into_iter().map(lower_lookup_subject).collect::<Lowered<_>>()?,
		cond: lookup.cond.map(|c| lower_expr(c.0)).transpose()?,
		split: lookup
			.split
			.map(|s| s.0.into_iter().map(|x| lower_idiom(x.0)).collect::<Lowered<_>>())
			.transpose()?,
		group: lookup
			.group
			.map(|g| g.0.into_iter().map(|x| lower_idiom(x.0)).collect::<Lowered<_>>())
			.transpose()?,
		order: lookup.order.map(lower_ordering).transpose()?,
		limit: lookup.limit.map(|l| lower_expr(l.0)).transpose()?,
		start: lookup.start.map(|s| lower_expr(s.0)).transpose()?,
		alias: lookup.alias.map(lower_output_path).transpose()?,
	})
}

fn lower_lookup_subject(subject: sql::lookup::LookupSubject) -> Lowered<LogicalLookupSubject> {
	Ok(match subject {
		sql::lookup::LookupSubject::Table {
			table,
			referencing_field,
		} => LogicalLookupSubject::Table {
			table: TableName::from(table),
			referencing_field,
		},
		sql::lookup::LookupSubject::Range {
			table,
			range,
			referencing_field,
		} => LogicalLookupSubject::Range {
			table: TableName::from(table),
			range: lower_record_id_range(range)?,
			referencing_field,
		},
	})
}

/// A projection's fields, as a `SELECT` or a `RETURN` clause writes them.
pub fn lower_fields(fields: sql::Fields) -> Lowered<LogicalFields> {
	Ok(match fields {
		sql::Fields::Value(selector) => LogicalFields::Value {
			expr: lower_expr(selector.expr)?,
			alias: selector.alias.map(lower_output_path).transpose()?,
		},
		sql::Fields::Select(fields) => LogicalFields::Select(
			fields
				.into_iter()
				.map(|field| {
					Ok(match field {
						sql::Field::All => LogicalField::All,
						sql::Field::Single(selector) => lower_field(selector)?,
					})
				})
				.collect::<Lowered<_>>()?,
		),
	})
}

/// A projected field. An unaliased field whose derived name would be spelled
/// from a nested plan — a statement in scalar position — is aliased to the
/// SurrealQL text of the expression as written, which is the key such a
/// projection takes; every other field keeps the alias the user
/// wrote, or none, and is named by [`LogicalExpr::output_path`].
fn lower_field(selector: sql::field::Selector) -> Lowered<LogicalField> {
	let written = match &selector.alias {
		Some(_) => None,
		None => Some(selector.expr.to_sql()),
	};
	let expr = lower_expr(selector.expr)?;
	let alias = match selector.alias {
		Some(alias) => Some(lower_output_path(alias)?),
		None if expr.output_name_renders_a_statement() => written.map(OutputPath::field),
		None => None,
	};
	Ok(LogicalField::user(expr, alias))
}

fn lower_ordering(ordering: sql::order::Ordering) -> Lowered<LogicalOrdering> {
	Ok(match ordering {
		sql::order::Ordering::Random => LogicalOrdering::Random,
		sql::order::Ordering::Order(list) => LogicalOrdering::Order(
			list.0
				.into_iter()
				.map(|order| {
					Ok(LogicalOrder {
						value: lower_idiom(order.value)?,
						collate: order.collate,
						numeric: order.numeric,
						direction: order.direction,
					})
				})
				.collect::<Lowered<_>>()?,
		),
	})
}

/// A postfix application of `op` to the lowered `expr`. The two range
/// operators stay operators. A method or closure call written after an
/// expression lowers to the node the idiom lowering builds for the same
/// construct — [`LogicalExpr::Method`] and [`LogicalExpr::Invoke`] — so every
/// pass meets one node per construct whichever way it was spelled.
fn lower_postfix(expr: LogicalExpr, op: sql::PostfixOperator) -> Lowered<LogicalExpr> {
	let expr = Box::new(expr);
	Ok(match op {
		sql::PostfixOperator::Range => LogicalExpr::Postfix {
			expr,
			op: LogicalPostfixOperator::Range,
		},
		sql::PostfixOperator::RangeSkip => LogicalExpr::Postfix {
			expr,
			op: LogicalPostfixOperator::RangeSkip,
		},
		sql::PostfixOperator::MethodCall(name, args) => LogicalExpr::Method {
			base: expr,
			name: name.into(),
			args: lower_exprs(args)?,
		},
		sql::PostfixOperator::Call(args) => LogicalExpr::Invoke {
			target: expr,
			args: lower_exprs(args)?,
		},
	})
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeSet;

	use surrealdb_types::ToSql;

	use super::*;
	use crate::expr::logical::LogicalRecursePath;

	/// The shape of a lowered tree, outermost operator first, so a test can
	/// state the pipeline it expects without matching on every field.
	fn shape(op: &LogicalOp) -> Vec<&'static str> {
		let mut out = vec![op.name()];
		let mut current = op;
		while let Some(child) = current.children().first() {
			out.push(child.name());
			current = child;
		}
		out
	}

	fn parse(sql: &str) -> sql::Expr {
		let ast = surrealdb_parser_next_lower::parse(sql).expect("test statement should parse");
		let top = ast.expressions.into_iter().next().expect("one statement");
		let sql::TopLevelExpr::Expr(expr) = top else {
			panic!("expected a bare expression");
		};
		expr
	}

	fn lower_sql(sql: &str) -> LogicalOp {
		lower(parse(sql)).expect("statement should lower")
	}

	/// An `IF`, `THROW`, `BREAK` and `CONTINUE` in statement position are
	/// operators of their own: at the top level, in a block, in a `FOR` body,
	/// as a `LET` value and as an arm of another `IF`.
	#[test]
	fn control_flow_in_statement_position_lowers_to_operators() {
		assert_eq!(
			lower_sql("IF $a { 1 } ELSE { 2 }").to_string(),
			"If[$a    ELSE]\n  Sequence\n    Eval[1]\n  Sequence\n    Eval[2]"
		);
		assert_eq!(lower_sql("THROW 'x'").to_string(), "Throw['x']");
		assert_eq!(lower_sql("BREAK").to_string(), "Break");
		assert_eq!(lower_sql("CONTINUE").to_string(), "Continue");
		assert_eq!(
			lower_sql("{ IF $a { BREAK }; CONTINUE; THROW 1 }").to_string(),
			"Sequence\n  If[$a]\n    Sequence\n      Break\n  Continue\n  Throw[1]"
		);
		assert_eq!(
			lower_sql("FOR $x IN [1] { IF $x { CONTINUE } }").to_string(),
			"Foreach[$x IN [1]]\n  Sequence\n    If[$x]\n      Sequence\n        Continue"
		);
		assert_eq!(
			lower_sql("LET $v = IF $a THEN 1 END").to_string(),
			"Let[$v]\n  If[$a]\n    Eval[1]"
		);
		assert_eq!(
			lower_sql("IF $a THEN IF $b THEN 1 ELSE 2 END END").to_string(),
			"If[$a]\n  If[$b    ELSE]\n    Eval[1]\n    Eval[2]"
		);
	}

	/// A statement `IF` with an arm written as a `LET`, at any depth of the
	/// `IF`s written as arms, keeps its scalar form; a block arm holding a
	/// `LET` does not.
	#[test]
	fn a_statement_if_with_a_let_arm_stays_an_expression() {
		assert!(matches!(lower_sql("IF $c THEN (LET $x = 1) ELSE 3 END"), LogicalOp::Eval(_)));
		assert!(matches!(lower_sql("IF $c THEN 1 ELSE (LET $x = 1) END"), LogicalOp::Eval(_)));
		assert!(matches!(
			lower_sql("IF $t THEN 1 ELSE IF $c THEN (LET $x = 1) ELSE 3 END"),
			LogicalOp::Eval(_)
		));
		assert!(matches!(lower_sql("IF $t { LET $x = 1; } ELSE { 3 }"), LogicalOp::If(_)));
	}

	/// The value of a `RETURN`, and an `IF` inside an expression, stay in
	/// value position.
	#[test]
	fn control_flow_in_value_position_stays_an_expression() {
		assert_eq!(
			lower_sql("RETURN IF $a THEN 1 END").to_string(),
			"Return\n  Eval[IF $a THEN 1 END]"
		);
		assert_eq!(lower_sql("RETURN THROW 'x'").to_string(), "Return\n  Eval[THROW 'x']");
		assert!(matches!(lower_sql("[IF $a THEN 1 END]"), LogicalOp::Eval(_)));
	}

	/// The scalar form of a statement `IF` evaluates to what the operator
	/// yields: a block arm becomes the subquery of its block.
	#[test]
	fn a_statement_if_has_its_value_position_form() {
		let LogicalOp::If(ifs) = lower_sql("IF $a { 1 } ELSE IF $b { 2 }") else {
			panic!("an IF operator");
		};
		let scalar = ifs.to_scalar();
		assert_eq!(scalar.branches.len(), 2);
		assert!(
			scalar.branches.iter().all(
				|(_, arm)| matches!(arm, LogicalExpr::Subquery(block) if block.name() == "Sequence")
			),
			"a block arm is the subquery of its block"
		);
		assert!(scalar.otherwise.is_none());
	}

	fn expr(sql: &str) -> LogicalExpr {
		let sql::Expr::Return(out) = parse(&format!("RETURN {sql}")) else {
			panic!("expected a RETURN");
		};
		lower_expr(out.what).expect("expression should lower")
	}

	/// A method or closure call written after an expression lowers to the
	/// node the idiom lowering builds for the same construct, so no pass can
	/// meet one construct under two spellings. The range operators stay
	/// postfix operators.
	#[test]
	fn a_postfix_call_lowers_to_the_node_of_its_construct() {
		let one = || {
			let sql::Expr::Return(out) = parse("RETURN 1") else {
				panic!("expected a RETURN");
			};
			out.what
		};
		let postfix = |op| {
			lower_expr(sql::Expr::Postfix {
				expr: Box::new(one()),
				op,
			})
			.expect("expression should lower")
		};

		let method = postfix(sql::PostfixOperator::MethodCall("len".to_owned(), vec![]));
		assert!(
			matches!(
				&method,
				LogicalExpr::Method { name, args, .. } if name.as_str() == "len" && args.is_empty()
			),
			"{method:?}"
		);
		assert!(method.to_sql().ends_with(".len()"), "{}", method.to_sql());

		let call = postfix(sql::PostfixOperator::Call(vec![one()]));
		assert!(matches!(&call, LogicalExpr::Invoke { args, .. } if args.len() == 1), "{call:?}");
		assert_eq!(call.to_sql(), "(1)(1)");

		let range = postfix(sql::PostfixOperator::Range);
		assert!(matches!(
			&range,
			LogicalExpr::Postfix {
				op: LogicalPostfixOperator::Range,
				..
			}
		));
		assert_eq!(range.to_sql(), "1..");
	}

	#[test]
	fn a_bare_select_is_a_projection_over_a_source() {
		assert_eq!(shape(&lower_sql("SELECT * FROM t")), ["Project", "Source"]);
	}

	#[test]
	fn a_where_clause_becomes_a_filter_under_the_projection() {
		assert_eq!(
			shape(&lower_sql("SELECT * FROM t WHERE a > 1")),
			["Project", "Filter", "Source"]
		);
	}

	#[test]
	fn the_pipeline_order_matches_the_physical_planner() {
		// SPLIT and GROUP are mutually exclusive in the grammar, so the full
		// pipeline is only reachable one of the two at a time.
		assert_eq!(
			shape(&lower_sql(
				"SELECT d FROM t WHERE b > 1 SPLIT d ORDER BY d LIMIT 1 FETCH f TIMEOUT 1s"
			)),
			["Timeout", "Fetch", "Project", "Limit", "Sort", "Split", "Filter", "Source"]
		);
	}

	#[test]
	fn a_grouped_pipeline_puts_the_aggregation_below_the_sort() {
		assert_eq!(
			shape(&lower_sql(
				"SELECT d FROM t WHERE b > 1 GROUP BY d ORDER BY d LIMIT 1 FETCH f TIMEOUT 1s"
			)),
			["Timeout", "Fetch", "Limit", "Sort", "Aggregate", "Filter", "Source"]
		);
	}

	#[test]
	fn several_sources_concatenate() {
		let op = lower_sql("SELECT * FROM a, b");
		assert_eq!(shape(&op), ["Project", "UnionAll", "Source"]);
		let LogicalOp::Project(LogicalProject {
			input,
			..
		}) = &op
		else {
			panic!("expected a projection");
		};
		assert_eq!(input.children().len(), 2);
	}

	#[test]
	fn a_literal_source_is_values_and_a_record_id_is_not() {
		assert_eq!(shape(&lower_sql("SELECT * FROM [1, 2, 3]")), ["Project", "Values"]);
		assert_eq!(shape(&lower_sql("SELECT * FROM 'x'")), ["Project", "Values"]);
		assert_eq!(shape(&lower_sql("SELECT * FROM t:1")), ["Project", "Source"]);
		let mixed = lower_sql("SELECT * FROM t, [1]");
		let names: Vec<_> = mixed.children()[0].children().iter().map(LogicalOp::name).collect();
		assert_eq!(names, ["Source", "Values"]);
	}

	#[test]
	fn a_source_is_classified_by_its_syntax() {
		fn source_of(sql: &str) -> SourceRef {
			let op = lower_sql(sql);
			let LogicalOp::Project(LogicalProject {
				input,
				..
			}) = op
			else {
				panic!("expected a projection");
			};
			let LogicalOp::Source(LogicalSource {
				source,
				..
			}) = *input
			else {
				panic!("expected a source");
			};
			source
		}
		assert!(matches!(source_of("SELECT * FROM t"), SourceRef::Table(_)));
		assert!(matches!(source_of("SELECT * FROM t:1"), SourceRef::RecordId(_)));
		assert!(matches!(source_of("SELECT * FROM t:1..5"), SourceRef::RecordId(_)));
		assert!(matches!(source_of("SELECT * FROM |t:3|"), SourceRef::Mock(_)));
		assert!(matches!(source_of("SELECT * FROM $p"), SourceRef::Param(_)));
		assert!(matches!(source_of("SELECT * FROM a->likes->b"), SourceRef::Dynamic(_)));
		assert!(matches!(source_of("SELECT * FROM { a: 1 }"), SourceRef::Dynamic(_)));
	}

	#[test]
	fn a_subquery_source_is_lowered_eagerly() {
		// The inner statement's own pipeline sits under the Subquery node, so
		// the outer WHERE can be pushed through it by a pass.
		assert_eq!(
			shape(&lower_sql("SELECT * FROM (SELECT * FROM t WHERE a) WHERE b")),
			["Project", "Filter", "Subquery", "Project", "Filter", "Source"]
		);
	}

	#[test]
	fn a_subquery_source_under_for_update_is_a_locked_dynamic_source() {
		// The flag has to reach the physical planner's target check, and a
		// `Subquery` leaf cannot carry it, so the nested plan is held as the
		// scalar of a `Source` that does. Only the flagged form takes this
		// shape; without the clause the nested statement stays a leaf.
		let op = lower_sql("SELECT * FROM (SELECT * FROM t:1) FOR UPDATE");
		assert_eq!(shape(&op), ["Project", "Source"]);
		let LogicalOp::Project(LogicalProject {
			input,
			..
		}) = op
		else {
			panic!("expected a projection");
		};
		let LogicalOp::Source(LogicalSource {
			source,
			for_update,
			..
		}) = *input
		else {
			panic!("expected a source");
		};
		assert!(for_update, "the clause is carried on the source");
		let SourceRef::Dynamic(LogicalExpr::Subquery(nested)) = source else {
			panic!("expected the nested plan as a dynamic scalar, got {source:?}");
		};
		assert_eq!(shape(&nested), ["Project", "Source"]);
	}

	#[test]
	fn the_with_hint_is_attached_to_every_stored_source() {
		let op = lower_sql("SELECT * FROM a, b, [1] WITH NOINDEX");
		let leaves = op.children()[0].children();
		let names: Vec<_> = leaves.iter().map(LogicalOp::name).collect();
		assert_eq!(names, ["Source", "Source", "Values"]);
		for leaf in leaves {
			if let LogicalOp::Source(LogicalSource {
				with,
				..
			}) = leaf
			{
				assert!(with.is_some(), "stored sources carry the hint");
			}
		}
	}

	#[test]
	fn a_grouped_query_projects_in_the_aggregation_and_omits_the_projection() {
		// The aggregation carries the fields, so no Project is emitted above
		// it unless OMIT has something to do.
		assert_eq!(shape(&lower_sql("SELECT count() FROM t GROUP ALL")), ["Aggregate", "Source"]);
		assert_eq!(
			shape(&lower_sql("SELECT count() OMIT x FROM t GROUP ALL")),
			["Project", "Aggregate", "Source"]
		);
	}

	#[test]
	fn an_absent_version_or_timeout_contributes_no_operator() {
		// Both are held as a bare expression whose absent form is a NONE
		// literal, so a naive lowering would wrap every statement in both.
		assert_eq!(shape(&lower_sql("SELECT * FROM t")), ["Project", "Source"]);
	}

	#[test]
	fn only_wraps_the_whole_pipeline() {
		assert_eq!(shape(&lower_sql("SELECT * FROM ONLY t:1")), ["Only", "Project", "Source"]);
	}

	#[test]
	fn a_where_clause_is_a_user_filter_never_a_permission_filter() {
		let op = lower_sql("SELECT * FROM t WHERE a > 1");
		assert!(op.any(|n| matches!(n, LogicalOp::Filter(_))));
		assert!(!op.any(|n| matches!(n, LogicalOp::PermissionFilter(_))));
	}

	#[test]
	fn every_scalar_in_a_rendered_plan_is_surrealql() {
		let rendered = lower_sql("SELECT a FROM t WHERE b > 1 LIMIT 2").to_string();
		assert!(rendered.contains("b > 1"), "predicate should render as SurrealQL: {rendered}");
		assert!(rendered.contains("LIMIT 2"), "limit should render: {rendered}");
		assert!(rendered.contains("Source[t]"), "source should name its table: {rendered}");
	}

	/// Scalars whose logical rendering must equal the expression layer's, so
	/// that a plan's scalars print the same whichever type holds them. A
	/// [`LogicalExpr::Subquery`] is the intentional exception: it renders as
	/// the plan it holds, so no statement appears here.
	const SCALAR_CORPUS: &[&str] = &[
		// Literals.
		"NONE",
		"NULL",
		"true",
		"1",
		"-1",
		"1.5",
		"1.5dec",
		"'x'",
		"d'2024-01-01T00:00:00Z'",
		"1h30m",
		"u'018e3b7a-0000-7000-8000-000000000000'",
		"/a+/",
		"..",
		// Collections and record ids.
		"[1, 2, 'x']",
		"[]",
		"{ a: 1, b: [2] }",
		"{ }",
		"{ type: 'Point', coordinates: [1.0, 2.0] }",
		"t:1",
		"t:['a', 1]",
		"t:{ a: 1, b: 'x' }",
		"t:1..5",
		"t:1>..=5",
		"t:..5",
		"t:uuid()",
		"t:⟨a b⟩",
		// Paths.
		"a.b[0].c[$]",
		"a[WHERE b > 1].c",
		"a.*",
		"a[*]",
		"a.{ b, c: d.e }",
		"a.{ b.{ c } }",
		"$p.name",
		"(1 + 2).b",
		"a.b.{2..}(.c)",
		"a.{1..3+collect}",
		"a.{..+path+inclusive}",
		"a.?.b",
		"->likes->person",
		"->(likes WHERE x > 1 AS l)",
		"<-knows<-person.name",
		"<->knows",
		"->likes:1..5",
		"<~refs",
		"->(likes, follows)",
		// Calls, methods, closures.
		"count(a, 'x')",
		"fn::custom(1)",
		"math::PI",
		"a.map(|$v| $v + 1)",
		"a.len()",
		"$f(1)",
		"|$x: int| $x + 1",
		"|$x: int, $y: string| $x",
		"|| 1",
		"|$x: int| a + $x",
		// Control flow as values.
		"IF a THEN 1 ELSE 2 END",
		"IF a THEN 1 ELSE IF b THEN 2 ELSE 3 END",
		"IF a THEN 1 END",
		"[IF a THEN 1 END]",
		"THROW 'x'",
		"BREAK",
		"CONTINUE",
		// Prefix and postfix operators.
		"!a",
		"-a",
		"+a",
		"-(1 + 2) * 3",
		"-(-1)",
		"<int> a",
		"<string> (a + b)",
		"..5",
		"..=5",
		"a..",
		"a>..",
		// Every binary operator.
		"a AND b",
		"a OR b",
		"a ?? b",
		"a ?: b",
		"a = b",
		"a == b",
		"a != b",
		"a ?= b",
		"a *= b",
		"a < b",
		"a <= b",
		"a > b",
		"a >= b",
		"a CONTAINS b",
		"a CONTAINSNOT b",
		"a CONTAINSALL b",
		"a CONTAINSANY b",
		"a CONTAINSNONE b",
		"a IN b",
		"a NOT IN b",
		"a ALLINSIDE b",
		"a ANYINSIDE b",
		"a NONEINSIDE b",
		"a OUTSIDE b",
		"a INTERSECTS b",
		"a @@ 'x'",
		"a @1@ 'x'",
		"a <|3|> b",
		"a <|3,COSINE|> b",
		"a + b",
		"a - b",
		"a * b",
		"a / b",
		"a % b",
		"a ** b",
		"a..b",
		"a..=b",
		"a>..b",
		"a>..=b",
		// Precedence and associativity.
		"a + b * c",
		"(a + b) * c",
		"a - (b - c)",
		"a ** (b ** c)",
		"(a ** b) ** c",
		"a AND (b OR c)",
		"(a = b) = c",
	];

	#[test]
	fn a_scalar_renders_as_the_expression_layer_renders_it() {
		for sql in SCALAR_CORPUS {
			let sql::Expr::Return(out) = parse(&format!("RETURN {sql}")) else {
				panic!("expected a RETURN");
			};
			let expected = crate::expr::Expr::from(out.what.clone()).to_sql();
			let lowered = lower_expr(out.what).expect("expression should lower");
			assert_eq!(
				lowered.to_sql(),
				expected,
				"rendering of `{sql}` differs from the expression layer"
			);
		}
	}

	/// Every part kind the idiom grammar produces, in the positions the
	/// lowering rules distinguish: roots (a field, `@`, a starting
	/// expression, a leading lookup), mapping steps before and after
	/// non-mapping steps, lookup runs with and without filters, optional
	/// chains, both recursion forms with and without `.@`, and destructure
	/// aliases written as paths and as expressions.
	const IDIOM_CORPUS: &[&str] = &[
		"a",
		"a.b",
		"a.b.c",
		"a[0]",
		"a[$]",
		"a[1]",
		"a[-1]",
		"a[1..3]",
		"a[b]",
		"a[$i]",
		"a.*",
		"a[*]",
		"a.*.b",
		"a.b.*",
		"a.*.*",
		"a.*[0]",
		"a…",
		"a...",
		"a.b…",
		"a[WHERE b > 1]",
		"a[WHERE b > 1].c",
		"a[WHERE b > 1][0].c",
		"a.b[0]",
		"a.b[0].c",
		"a.b[0].c[0]",
		"a.b.c[0]",
		"a.b.c.d[WHERE e][$]",
		"a.len()",
		"a.b.len()",
		"a.slice(1, 2).c",
		"a.map(|$v| $v.b)",
		"a.{ b }",
		"a.{ b, c }",
		"a.{ b.* }",
		"a.{ b: c }",
		"a.{ b: c.d }",
		"a.{ b: c[0].d }",
		"a.{ b: $this.c }",
		"a.{ b: c * 2 }",
		"a.{ b: (c + 1).d }",
		"a.{ b: [1, 2][0] }",
		"a.{ b: ->likes->person }",
		"a.{ b.{ c, d: e } }",
		"a.{ b, c: d.e[0] }.f",
		"a.{ b }[0]",
		"a.?",
		"a.?.b",
		"a.?.b.c",
		"a.?.b[0]",
		"a.?.b.?.c",
		"a.b.?.c[0]",
		"a[0].?.b",
		"->likes",
		"->likes->person",
		"->likes->person->follows",
		"->likes->person.name",
		"->likes->person.name[0]",
		"->likes[WHERE x > 1]",
		"->likes[WHERE x > 1]->person",
		"->likes[WHERE x > 1]->person.name",
		"->likes…->person",
		"->likes...->person",
		"->likes[0]",
		"->likes.out",
		"->likes.out->person",
		"<-knows",
		"<-knows<-person",
		"<->knows",
		"<->knows<->person",
		"<~refs",
		"<~refs<~more",
		"<~(refs FIELD a)",
		"->?",
		"->(likes, follows)",
		"->(likes WHERE x > 1)",
		"->(likes WHERE x > 1 AS l)",
		"->(likes AS l)->person",
		"->(likes AS l).name",
		"->likes:1..5",
		"->likes:1..",
		"->likes:..5",
		"->(likes:1..)",
		"->(SELECT * FROM likes)",
		"->(SELECT VALUE out FROM likes WHERE x > 1 ORDER BY y LIMIT 1 START 1)",
		"->(SELECT VALUE out FROM ONLY likes)",
		"->(SELECT * FROM likes SPLIT ON a)",
		"->(SELECT * FROM likes GROUP BY b)",
		"->(SELECT * FROM likes GROUP ALL)",
		"a->likes",
		"a->likes->person",
		"a->likes->person.name",
		"a.b->likes",
		"a.b->likes->person[0]",
		"a.{..}",
		"a.{2}",
		"a.{1..3}",
		"a.{..3}",
		"a.{2..}",
		"a.{..}(.b)",
		"a.{..}(.b.c)",
		"a.{..}(->knows->person)",
		"a.{..}(->knows->person).name",
		"a.{..}.b",
		"a.{..}.b.c",
		"a.{..}->knows->person",
		"a.{..}->knows->person.name",
		"a.{..}.b.@",
		"a.{..}.b.@.c",
		"a.{..}.{ name, friends: ->knows->person.@ }",
		"a.{1..3}(.{ name, friends: ->knows->person.@ })",
		"a.{..+path}(->knows->person)",
		"a.{..+path+inclusive}(->knows->person)",
		"a.{..+collect}(->knows->person)",
		"a.{..+collect+inclusive}(->knows->person)",
		"a.{..+shortest=person:1}(->knows->person)",
		"a.{..+shortest=person:1+inclusive}(->knows->person)",
		"a.{..+shortest=$target}(->knows->person)",
		"@.a",
		"@.a.b[0]",
		"$p.a",
		"$p.a.b",
		"$p.a[0]",
		"$p.a[0].b",
		"$p->likes->person",
		"$p[0]",
		"$p[WHERE x]",
		"$p.?.a",
		"$p.{..}(.a)",
		"(1 + 2).b",
		"(-1).abs()",
		"(-1.5).abs()",
		"(-1.5dec).abs()",
		"(!a).b",
		"(a..).len()",
		"(a.b).c[0]",
		"(a.b[0]).c[0]",
		"[1, 2][0]",
		"[1, 2].len()",
		"[{ a: [1, 2] }, { a: [3, 4] }].a[0]",
		"{ a: 1 }.a",
		"{ a: { b: 1 } }.a.b",
		"person:1.name",
		"person:1->likes->person",
		"person:1.{ name, age }",
		"'x'.len()",
		"count().a",
		"fn::f().a[0]",
		"`a.b`",
		"`a.b`.c",
		"⟨a b⟩.⟨c d⟩",
		"a.`b c`",
		"a.function",
		"a.b[WHERE c IN [1, 2]].d.e",
		"a.b[WHERE c][0].d[WHERE e][$]",
		"a[WHERE b].c[WHERE d]->e->f[WHERE g].h",
		// A grouped path start. Every step but a field access and `.*` reads as
		// the body of a broadcast over the access to its left, so these are all
		// different expressions from the same text without the parentheses.
		"(a.b)[0]",
		"(a.b)[$]",
		"(a.b)[WHERE c]",
		"(a.b).len()",
		"(a.b).*",
		"(a.b).?.c",
		"(a.b).{ x }",
		"(a.b)->likes->person",
		"(a.b.c)[0]",
		"(a.b)[0].c",
		"((a.b)[0].c)[1]",
		"($p.a)[0]",
		"((1 + 2).b)[0]",
		"(person:1.name)[0]",
	];

	/// Every idiom renders as an idiom that reads back as the same expression,
	/// in both the single-line and the pretty layout.
	///
	/// This is a round trip rather than a comparison against the surface text,
	/// because the two do not always agree and should not. The surface
	/// parenthesises a path start that is itself a path whenever it *might*
	/// matter, without proving that it does; lowering keeps only the grouping
	/// that changes the expression. `(a.b).c` and `a.b.c` lower to one tree, so
	/// no rendering of that tree can reproduce both spellings, and the
	/// canonical one is right.
	///
	/// What has to hold is that the rendering parses back to the expression it
	/// came from. That is the property the stored text depends on — a
	/// definition is re-read from its rendering — and it is the stronger claim:
	/// a grouping this layer drops but must not would reparse as a broadcast
	/// over the access to its left, which byte-for-byte-against-the-surface
	/// never checked, since the corpus is written in whatever spelling the
	/// surface happens to produce.
	#[test]
	fn a_path_renders_as_the_idiom_it_was_lowered_from() {
		let lower_of = |sql: &str| {
			let sql::Expr::Return(out) = parse(&format!("RETURN {sql}")) else {
				panic!("expected a RETURN for `{sql}`");
			};
			lower_expr(out.what).expect("expression should lower")
		};
		for sql in IDIOM_CORPUS {
			let lowered = lower_of(sql);
			for (layout, rendered) in [("", lowered.to_sql()), (" pretty", lowered.to_sql_pretty())]
			{
				assert_eq!(
					lower_of(&rendered),
					lowered,
					"the{layout} rendering of `{sql}` is `{rendered}`, which is a different \
					 expression"
				);
			}
		}
	}

	/// Grouping a path changes nothing when only mapping steps follow it.
	///
	/// `(a.b).c` and `a.b.c` apply `.c` to the same values, so both lower to
	/// one chain and the plan holds nothing that says which spelling produced
	/// it. The plan renders the unparenthesised form, which reads back as the
	/// same plan.
	///
	/// Grouping that does change the meaning is kept: a following step that
	/// broadcasts folds over the group as a unit, so the plan holds the group
	/// as the base of that fold and renders its parentheses. `(a.b).c[0]` in
	/// the corpus above covers that, and it renders byte for byte as written.
	#[test]
	fn grouping_a_path_before_a_mapping_step_is_canonicalised_away() {
		let grouped = lower_idiom("(a.b).c");
		assert_eq!(shape_of(&grouped), shape_of(&lower_idiom("a.b.c")));
		assert_eq!(grouped.to_sql(), "a.b.c");
	}

	/// Every idiom renders as a path that reads back as the plan it came from.
	///
	/// Two paths built from the same steps can differ only in where a fold
	/// sits, so a rendering that drops what separates them names a different
	/// plan: `(a.b).c[0]` folds over `a.b`, where `a.b.c[0]` folds over `a`
	/// and maps `.b` within it. Rendering is idempotent for the same reason —
	/// a plan that read back as itself renders the same way again.
	#[test]
	fn a_path_reads_back_as_the_plan_it_was_rendered_from() {
		for sql in IDIOM_CORPUS {
			let lowered = lower_idiom(sql);
			let rendered = lowered.to_sql();
			let reread = lower_idiom(&rendered);
			assert_eq!(
				shape_of(&reread),
				shape_of(&lowered),
				"`{sql}` rendered as `{rendered}`, which is a different plan"
			);
			assert_eq!(reread.to_sql(), rendered, "rendering `{sql}` is not idempotent");
		}
	}

	/// A root written after the first part of a path, which the parser never
	/// produces, is the value the rest of the path applies to: the parts
	/// before it are replaced, not read.
	#[test]
	fn a_root_inside_a_path_restarts_it() {
		let field = |name: &str| sql::Part::Field(name.into());
		let doc_then_b =
			super::lower_idiom(sql::Idiom(vec![field("a"), sql::Part::Doc, field("b")]))
				.expect("lowers");
		assert_eq!(shape_of(&doc_then_b), shape_of(&lower_idiom("@.b")));

		let start = sql::Expr::Param(sql::Param::new("p".to_owned()));
		let start_then_b =
			super::lower_idiom(sql::Idiom(vec![field("a"), sql::Part::Start(start), field("b")]))
				.expect("lowers");
		assert_eq!(shape_of(&start_then_b), shape_of(&lower_idiom("$p.b")));

		// The last root wins when several are written.
		let twice = super::lower_idiom(sql::Idiom(vec![
			sql::Part::Doc,
			field("a"),
			sql::Part::Doc,
			field("b"),
		]))
		.expect("lowers");
		assert_eq!(shape_of(&twice), shape_of(&lower_idiom("@.b")));

		// A recursion body takes a root the same way.
		let body = super::lower_body(sql::Idiom(vec![field("a"), sql::Part::Doc, field("b")]))
			.expect("lowers");
		assert_eq!(shape_of(&body), shape_of(&lower_idiom("@.b")));
	}

	/// Lowers an idiom written as surface SurrealQL.
	fn lower_idiom(sql: &str) -> LogicalExpr {
		let sql::Expr::Return(out) = parse(&format!("RETURN {sql}")) else {
			panic!("expected a RETURN");
		};
		lower_expr(out.what).expect("expression should lower")
	}

	/// A compact description of a path's structure: each form by its
	/// surface spelling over its base, with `B(base; body)` for a broadcast
	/// (`B*` when it flattens its results), `?(base; then)` for an optional,
	/// `R(base; body)` / `R(base; (body))` for a trailing / nested recursion,
	/// `~` for the implicit flatten, `…` for the written one and `_` for the
	/// current value.
	fn shape_of(e: &LogicalExpr) -> String {
		match e {
			LogicalExpr::Field(name) => name.to_string(),
			LogicalExpr::Doc => "@".to_string(),
			LogicalExpr::Current => "_".to_string(),
			LogicalExpr::Access {
				base,
				field,
			} => format!("{}.{field}", shape_of(base)),
			LogicalExpr::Index {
				base,
				..
			} => format!("{}[i]", shape_of(base)),
			LogicalExpr::First(base) => format!("{}[first]", shape_of(base)),
			LogicalExpr::Last(base) => format!("{}[$]", shape_of(base)),
			LogicalExpr::Each(base) => format!("{}.*", shape_of(base)),
			LogicalExpr::Flatten {
				base,
				explicit,
			} => format!(
				"{}{}",
				shape_of(base),
				if *explicit {
					"…"
				} else {
					"~"
				}
			),
			LogicalExpr::Filter {
				base,
				..
			} => format!("{}[W]", shape_of(base)),
			LogicalExpr::Method {
				base,
				name,
				..
			} => format!("{}.{name}()", shape_of(base)),
			LogicalExpr::Destructure {
				base,
				..
			} => format!("{}.{{}}", shape_of(base)),
			LogicalExpr::Optional {
				base,
				then,
			} => format!("?({}; {})", shape_of(base), shape_of(then)),
			LogicalExpr::Lookup {
				base,
				lookup,
			} => format!("{}{}", shape_of(base), lookup.to_sql()),
			LogicalExpr::Recurse(r) => match &r.path {
				None => format!("R({})", shape_of(&r.base)),
				Some(LogicalRecursePath::Nested(body)) => {
					format!("R({}; ({}))", shape_of(&r.base), shape_of(body))
				}
				Some(LogicalRecursePath::Trailing(body)) => {
					format!("R({}; {})", shape_of(&r.base), shape_of(body))
				}
			},
			LogicalExpr::RepeatRecurse(base) => format!("{}.@", shape_of(base)),
			LogicalExpr::Broadcast {
				base,
				body,
				flatten,
			} => format!(
				"B{}({}; {})",
				if *flatten {
					"*"
				} else {
					""
				},
				shape_of(base),
				shape_of(body)
			),
			other @ (LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Constant(_)
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
			| LogicalExpr::Continue
			| LogicalExpr::SetField {
				..
			}
			| LogicalExpr::Subquery(_)) => other.to_sql(),
		}
	}

	/// The placement of the sequence-semantics forms, one case per rule.
	#[test]
	fn lowering_places_the_sequence_forms_by_the_continuation_rules() {
		let cases = [
			// Mapping steps followed only by mapping steps nest plainly.
			("a", "a"),
			("a.b", "a.b"),
			("a.b.c", "a.b.c"),
			("a.*.b", "a.*.b"),
			("a.{ b }.c", "a.{}.c"),
			// A mapping step followed by a non-mapping step broadcasts, and
			// the body continues the fold so a later mapping step broadcasts
			// again for the case where the base is not an array.
			("a[0]", "a[i]"),
			("a.b[0]", "B(a; _.b[i])"),
			("a.b.c[0]", "B(a; B(_.b; _.c[i]))"),
			("a.b[1].c[0]", "B(a; B(_.b[i]; _.c[i]))"),
			("a.*[0]", "B(a; _.*[i])"),
			("a.{ b }[0]", "B(a; _.{}[i])"),
			("a.b.len()", "B(a; _.b.len())"),
			("a[0].b", "a[i].b"),
			("a[WHERE b].c", "a[W].c"),
			// A starting expression is the base of the first step.
			("$p.a", "$p.a"),
			("$p.a[0]", "B($p; _.a[i])"),
			("(a.b).c[0]", "B(a.b; _.c[i])"),
			("[1].a[0]", "B([1]; _.a[i])"),
			// A lookup run over anything but a lookup or a flatten, followed
			// by more steps or of more than one hop, broadcasts.
			("->likes", "_->likes"),
			("->likes->person", "B(_; _->likes->person)"),
			("->likes.out", "B(_; _->likes.out)"),
			("a->likes", "a->likes"),
			("a->likes->person", "B(a; _->likes->person)"),
			("a->likes.out", "B(a; _->likes.out)"),
			("a.b->likes", "B(a; _.b->likes)"),
			// A lookup followed by a filter has an implicit flatten; the
			// filter keeps the array's lookup origin, so a further lookup run
			// over it that continues broadcasts with flattening. A single
			// final hop does not continue.
			("->likes[WHERE x]", "B(_; _->likes~[W])"),
			("->likes[WHERE x]->person", "B(_; _->likes~[W]->person)"),
			("->likes[WHERE x]->person.name", "B(_; B*(_->likes~[W]; _->person.name))"),
			("a->likes[WHERE x]->person", "B(a; _->likes~[W]->person)"),
			// A written flatten blocks the broadcast of the lookup directly
			// after it, and marks the array as coming from a lookup.
			("->likes…->person", "B(_; _->likes…->person)"),
			("a…->likes->person", "a…->likes->person"),
			("a.b…->likes->person", "B(a; _.b…->likes->person)"),
			("a…[WHERE x]->likes.out", "B*(a…[W]; _->likes.out)"),
			// Optional takes the rest as a plain body.
			("a.?", "?(a; _)"),
			("a.?.b.c[0]", "?(a; _.b.c[i])"),
			("a.b.?.c[0]", "B(a; ?(_.b; _.c[i]))"),
			// A nested recursion is one step; a trailing one takes the rest up
			// to and including `.@`, and the suffix wraps it.
			("a.{..}", "R(a)"),
			("a.{..}(.b).c", "R(a; (_.b)).c"),
			("a.{..}.b.c[0]", "R(a; _.b.c[i])"),
			("a.{..}.b.@", "R(a; _.b.@)"),
			("a.{..}.b.@.c[0]", "B(R(a; _.b.@); _.c[i])"),
			("a.{..}->knows->person", "R(a; _->knows->person)"),
			("a.b.{..}(->knows)", "B(a; R(_.b; (_->knows)))"),
			// The row's record id.
			("@.a", "@.a"),
		];
		for (sql, want) in cases {
			assert_eq!(shape_of(&expr(sql)), want, "shape of `{sql}`");
		}
	}

	/// A body written as an idiom starts from the value it is applied to,
	/// even when its first part is a field, and a starting expression replaces
	/// that value.
	#[test]
	fn bodies_are_paths_over_the_current_value() {
		let LogicalExpr::Destructure {
			fields,
			..
		} = expr("a.{ b: c.d[1], e: $this.f, g: h * 2 }")
		else {
			panic!("expected a destructure");
		};
		let bodies: Vec<String> = fields
			.iter()
			.map(|field| match field {
				LogicalDestructureField::Aliased(_, body) => shape_of(body),
				other @ (LogicalDestructureField::All(_)
				| LogicalDestructureField::Field(_)
				| LogicalDestructureField::Destructure(..)
				| LogicalDestructureField::Rest) => {
					panic!("expected an alias, got {other:?}")
				}
			})
			.collect();
		// No broadcast inside a body: `c.d[1]` nests plainly.
		assert_eq!(bodies, ["_.c.d[i]", "$this.f", "h * 2"]);
	}

	#[test]
	fn a_block_is_a_sequence_as_a_statement_and_a_subquery_as_a_scalar() {
		let statement = lower_sql("{ LET $a = 1; RETURN $a + 1; }");
		assert_eq!(statement.name(), "Sequence");
		assert_eq!(
			statement.children().iter().map(LogicalOp::name).collect::<Vec<_>>(),
			["Let", "Return"]
		);
		let scalar = expr("1 + { LET $a = 1; RETURN $a + 1; }");
		let LogicalExpr::Binary {
			right,
			..
		} = scalar
		else {
			panic!("expected a binary expression");
		};
		let LogicalExpr::Subquery(nested) = *right else {
			panic!("expected a subquery");
		};
		assert_eq!(nested.name(), "Sequence");
	}

	#[test]
	fn control_flow_and_if_have_one_encoding_per_position() {
		// A top-level IF is the IF operator; one nested in an expression is
		// the scalar IF.
		assert!(matches!(lower_sql("IF true THEN 1 ELSE 2 END"), LogicalOp::If(_)));
		assert!(
			matches!(expr("[IF true THEN 1 END]"), LogicalExpr::Array(items) if matches!(items[0], LogicalExpr::If(_)))
		);
		assert!(matches!(lower_sql("BREAK"), LogicalOp::Break(_)));
		assert!(matches!(lower_sql("THROW 'x'"), LogicalOp::Throw(_)));
	}

	#[test]
	fn a_statement_in_scalar_position_is_a_subquery() {
		for sql in ["SELECT * FROM t", "RETURN 1", "LET $x = 1", "INFO FOR DB", "SLEEP 1ms"] {
			let lowered = expr(&format!("[{sql}]"));
			let LogicalExpr::Array(items) = lowered else {
				panic!("expected an array");
			};
			assert!(matches!(items[0], LogicalExpr::Subquery(_)), "`{sql}` should be a subquery");
		}
	}

	/// How lowering classifies a statement of each surface variant. Exhaustive
	/// on purpose: a variant added to the surface AST fails to compile here
	/// until it is classified, and the corpus below has one statement per
	/// variant so the classification is exercised rather than declared.
	///
	/// Total, because lowering is: every variant the parser produces has a
	/// logical form. A variant that does not would return a `Result` here
	/// again.
	fn expected(expr: &sql::Expr) -> &'static str {
		match expr {
			sql::Expr::Literal(_)
			| sql::Expr::Param(_)
			| sql::Expr::Idiom(_)
			| sql::Expr::Table(_)
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
			| sql::Expr::Closure(_)
			| sql::Expr::Mock(_) => "Eval",
			sql::Expr::Break => "Break",
			sql::Expr::Continue => "Continue",
			sql::Expr::Throw(_) => "Throw",
			sql::Expr::IfElse(_) => "If",
			sql::Expr::Block(_) => "Sequence",
			sql::Expr::Return(_) => "Return",
			sql::Expr::Select(_) => "Project",
			sql::Expr::Info(_) => "Info",
			sql::Expr::Foreach(_) => "Foreach",
			sql::Expr::Let(_) => "Let",
			sql::Expr::Sleep(_) => "Sleep",
			sql::Expr::Explain {
				..
			} => "Explain",
			sql::Expr::Create(_)
			| sql::Expr::Update(_)
			| sql::Expr::Delete(_)
			| sql::Expr::Relate(_)
			| sql::Expr::Insert(_)
			| sql::Expr::Upsert(_) => "Write",
			sql::Expr::Define(_)
			| sql::Expr::Remove(_)
			| sql::Expr::Rebuild(_)
			| sql::Expr::Alter(_) => "Ddl",
		}
	}

	fn variant_name(expr: &sql::Expr) -> &'static str {
		match expr {
			sql::Expr::Literal(_) => "Literal",
			sql::Expr::Param(_) => "Param",
			sql::Expr::Idiom(_) => "Idiom",
			sql::Expr::Table(_) => "Table",
			sql::Expr::Mock(_) => "Mock",
			sql::Expr::Block(_) => "Block",
			sql::Expr::Constant(_) => "Constant",
			sql::Expr::Prefix {
				..
			} => "Prefix",
			sql::Expr::Postfix {
				..
			} => "Postfix",
			sql::Expr::Binary {
				..
			} => "Binary",
			sql::Expr::FunctionCall(_) => "FunctionCall",
			sql::Expr::Closure(_) => "Closure",
			sql::Expr::Break => "Break",
			sql::Expr::Continue => "Continue",
			sql::Expr::Throw(_) => "Throw",
			sql::Expr::Return(_) => "Return",
			sql::Expr::IfElse(_) => "IfElse",
			sql::Expr::Select(_) => "Select",
			sql::Expr::Create(_) => "Create",
			sql::Expr::Update(_) => "Update",
			sql::Expr::Delete(_) => "Delete",
			sql::Expr::Relate(_) => "Relate",
			sql::Expr::Insert(_) => "Insert",
			sql::Expr::Define(_) => "Define",
			sql::Expr::Remove(_) => "Remove",
			sql::Expr::Rebuild(_) => "Rebuild",
			sql::Expr::Upsert(_) => "Upsert",
			sql::Expr::Alter(_) => "Alter",
			sql::Expr::Info(_) => "Info",
			sql::Expr::Foreach(_) => "Foreach",
			sql::Expr::Let(_) => "Let",
			sql::Expr::Sleep(_) => "Sleep",
			sql::Expr::Explain {
				..
			} => "Explain",
		}
	}

	/// One statement per surface variant. A bare table name only parses as
	/// one inside `FROM`, so that variant is built directly.
	fn corpus() -> Vec<sql::Expr> {
		let mut out: Vec<sql::Expr> = CORPUS.iter().map(|sql| parse(sql)).collect();
		out.push(sql::Expr::Table(sql::TableName::new("t")));
		out
	}

	const CORPUS: &[&str] = &[
		"1",
		"$p",
		"a.b",
		"|t:3|",
		"{ 1; 2 }",
		"math::PI",
		"!true",
		"1..",
		"1 + 2",
		"count()",
		"|| 1",
		"BREAK",
		"CONTINUE",
		"THROW 'x'",
		"RETURN 1",
		"IF true THEN 1 ELSE 2 END",
		"SELECT * FROM t",
		"CREATE t",
		"UPDATE t SET a = 1",
		"DELETE t",
		"RELATE a:1->r->b:1",
		"INSERT INTO t { a: 1 }",
		"DEFINE TABLE t",
		"REMOVE TABLE t",
		"REBUILD INDEX i ON t",
		"UPSERT t",
		"ALTER TABLE t SCHEMAFULL",
		"INFO FOR DB",
		"FOR $x IN [1] { }",
		"LET $x = 1",
		"SLEEP 1ms",
		"EXPLAIN SELECT * FROM t",
	];

	#[test]
	fn lowering_classifies_every_surface_variant() {
		let mut seen = BTreeSet::new();
		for statement in corpus() {
			let sql = statement.to_sql();
			seen.insert(variant_name(&statement));
			let want = expected(&statement);
			let got = lower(statement).expect("every surface variant lowers");
			assert_eq!(got.name(), want, "`{sql}`");
		}
		// The corpus must exercise every variant `expected` classifies.
		let all: BTreeSet<_> = [
			"Literal",
			"Param",
			"Idiom",
			"Table",
			"Mock",
			"Block",
			"Constant",
			"Prefix",
			"Postfix",
			"Binary",
			"FunctionCall",
			"Closure",
			"Break",
			"Continue",
			"Throw",
			"Return",
			"IfElse",
			"Select",
			"Create",
			"Update",
			"Delete",
			"Relate",
			"Insert",
			"Define",
			"Remove",
			"Rebuild",
			"Upsert",
			"Alter",
			"Info",
			"Foreach",
			"Let",
			"Sleep",
			"Explain",
		]
		.into_iter()
		.collect();
		assert_eq!(seen, all, "the corpus must cover every surface variant");
	}

	/// A schema statement in scalar position lowers with the statement that
	/// holds it, as a mutation does, so the block around it keeps its logical
	/// form.
	#[test]
	fn a_ddl_nested_in_a_scalar_keeps_the_statements_logical_form() {
		let statement = parse("RETURN { DEFINE TABLE x; 1 }");
		let lowered = lower(statement).expect("a block holding a schema statement lowers");
		assert!(
			lowered.any(|node| matches!(node, LogicalOp::Ddl(_))),
			"the block should carry the schema statement"
		);
	}

	/// A mock record set lowers wherever it sits, not only in `FROM`. A
	/// decline refuses the whole statement, so a mock that declined outside
	/// `FROM` would fail a query with one `|t:n|` in a predicate.
	#[test]
	fn a_mock_lowers_outside_from() {
		for sql in [
			"SELECT * FROM t WHERE a IN |u:3|",
			"CREATE |u:3|",
			"UPSERT |u:1..4|",
			"LET $x = |u:3|",
			"RETURN [|u:3|]",
		] {
			lower(parse(sql)).unwrap_or_else(|err| panic!("`{sql}` lowers: {err}"));
		}
	}

	/// A schema statement is not read-only, wherever it sits. The node owns no
	/// scalars, so a classification that reached for them would answer `true`
	/// over an empty set and put a catalog write on a read-only transaction.
	#[test]
	fn a_ddl_plan_is_not_read_only() {
		for sql in ["DEFINE TABLE x", "REMOVE TABLE x", "ALTER TABLE x", "REBUILD INDEX i ON x"] {
			let lowered = lower(parse(sql)).expect("a schema statement lowers");
			assert!(!lowered.read_only(), "`{sql}` writes the catalog");
		}
		// And through the statement that holds it, which reads the answer off
		// the nested plan.
		let lowered = lower(parse("RETURN { DEFINE TABLE x; 1 }")).expect("the block lowers");
		assert!(!lowered.read_only(), "a block holding a schema statement writes");
	}

	/// The node carries the statement and hands out no expressions: its
	/// clauses are resolved where the catalog write is formed, not by a pass
	/// over the plan.
	#[test]
	fn a_ddl_node_owns_no_scalars_and_no_inputs() {
		let lowered = lower(parse("DEFINE INDEX i ON t FIELDS a, b")).expect("lowers");
		assert!(matches!(lowered, LogicalOp::Ddl(_)));
		assert!(lowered.scalars().is_empty());
		assert!(lowered.children().is_empty());
		assert_eq!(lowered.name(), "Ddl");
	}

	/// A mutation in scalar position lowers with the statement that holds it,
	/// rather than declining it. The plan it produces still writes, so the
	/// whole statement is a writing plan and the planner is what decides
	/// whether it can compile one.
	#[test]
	fn a_lowered_write_nested_in_a_scalar_keeps_the_statement_writing() {
		let op = lower(parse("SELECT * FROM t WHERE a IN (CREATE u).id"))
			.expect("a nested CREATE should lower with its statement");
		assert!(!op.read_only(), "a statement containing a mutation must not claim read-only");

		// The control: the same statement without the mutation. Without it
		// the assertion above would also hold for a plan that had simply lost
		// the nested statement, which is the failure worth excluding.
		let control = lower(parse("SELECT * FROM t WHERE a IN u.id")).expect("should lower");
		assert!(control.read_only(), "the control must be read-only for the pair to mean anything");
	}

	/// The [`LogicalWrite`] a mutation lowers to.
	fn write_of(sql: &str) -> LogicalWrite {
		let op = lower_sql(sql);
		let name = op.name();
		let LogicalOp::Write(write) = op else {
			panic!("`{sql}` should lower to a write, got {name}");
		};
		write
	}

	fn targets_of(sql: &str) -> Vec<LogicalExpr> {
		match write_of(sql).source {
			LogicalWriteSource::Targets(targets) => targets,
			other @ (LogicalWriteSource::Inserts(_) | LogicalWriteSource::Rows(_)) => panic!(
				"`{sql}` writes subjects that need not exist, so it must not read them: {other:?}"
			),
		}
	}

	fn inserts_of(sql: &str) -> LogicalInserts {
		match write_of(sql).source {
			LogicalWriteSource::Inserts(inserts) => inserts,
			other @ (LogicalWriteSource::Targets(_) | LogicalWriteSource::Rows(_)) => {
				panic!("`{sql}` writes rows of its own: {other:?}")
			}
		}
	}

	fn rows_of(sql: &str) -> LogicalOp {
		match write_of(sql).source {
			LogicalWriteSource::Rows(rows) => *rows,
			other @ (LogicalWriteSource::Targets(_) | LogicalWriteSource::Inserts(_)) => {
				panic!("`{sql}` writes subjects that already exist, so it reads them: {other:?}")
			}
		}
	}

	/// Every mutation form has a logical form, and the node names the kind it
	/// performs.
	#[test]
	fn every_mutation_form_lowers_to_a_write() {
		for (sql, kind) in [
			("CREATE person SET a = 1", "CREATE"),
			("UPDATE person SET a = 1", "UPDATE"),
			("UPSERT person SET a = 1", "UPSERT"),
			("DELETE person", "DELETE"),
			("INSERT INTO person { a: 1 }", "INSERT"),
			("RELATE person:a->likes->person:b", "RELATE"),
		] {
			assert_eq!(write_of(sql).statement.kind(), kind, "`{sql}`");
		}
	}

	/// Which source arm a form takes is a correctness property, not a
	/// preference. A reading source resolves a record id to its stored
	/// document and yields nothing when there is none, which for a subject
	/// being created is always, so a creating form given one would have its
	/// write silently dropped. An updating form's subjects exist and are read.
	#[test]
	fn a_creating_form_names_targets_and_an_updating_form_reads_rows() {
		for sql in ["CREATE person", "UPSERT person", "RELATE person:a->likes->person:b"] {
			let _ = targets_of(sql);
			let op = lower_sql(sql);
			assert!(op.children().is_empty(), "`{sql}` has no plan subtree to read");
		}
		// `INSERT` names its subjects a third way — as the rows themselves —
		// and reads no more than the other two do.
		let _ = inserts_of("INSERT INTO person { a: 1 }");
		assert!(
			lower_sql("INSERT INTO person { a: 1 }").children().is_empty(),
			"an INSERT has no plan subtree to read"
		);
		for sql in ["UPDATE person SET a = 1", "DELETE person"] {
			assert_eq!(shape(&rows_of(sql)), ["Source"], "`{sql}`");
		}
	}

	/// `ONLY` is read from the statement. `INSERT` has no such keyword in the
	/// grammar, so it never asks for a single record.
	#[test]
	fn only_is_read_from_the_statement_and_insert_has_none() {
		for sql in [
			"CREATE ONLY person",
			"UPDATE ONLY person:1 SET a = 1",
			"UPSERT ONLY person:1",
			"DELETE ONLY person:1",
			"RELATE ONLY person:a->likes->person:b",
		] {
			assert!(write_of(sql).statement.only(), "`{sql}`");
		}
		assert!(!write_of("DELETE person:1").statement.only());
		assert!(!write_of("INSERT INTO person { a: 1 }").statement.only());
	}

	/// An updating form's targets lower to the subtree the same targets take
	/// in a `FROM` clause, index hint included, rather than to a second
	/// lowering of their own.
	#[test]
	fn an_updating_forms_targets_lower_as_a_from_clause_does() {
		let rows = rows_of("UPDATE a, b:1 WITH NOINDEX SET x = 1");
		assert_eq!(rows.name(), "UnionAll");
		let leaves: Vec<_> = rows.children().iter().map(LogicalOp::name).collect();
		assert_eq!(leaves, ["Source", "Source"]);
		for leaf in rows.children() {
			let LogicalOp::Source(LogicalSource {
				with,
				for_update,
				..
			}) = leaf
			else {
				panic!("expected a source");
			};
			assert!(with.is_some(), "the hint reaches each stored source");
			assert!(!for_update, "the mutation's own write takes the lock");
		}
	}

	/// The per-record table permission gate runs ahead of the statement's
	/// condition on the updating shapes, so that a condition with side effects
	/// runs only for records the caller may write
	/// (`doc/adr/0001-streaming-write-path.md`). The row subtree runs beneath
	/// the write operator and so ahead of every one of its stages, which is
	/// why the condition must not be lowered into it; it stays a clause of the
	/// statement, for the write operator's condition stage to evaluate.
	#[test]
	fn an_updating_forms_condition_stays_out_of_its_row_subtree() {
		for sql in ["UPDATE person SET a = 1 WHERE b > 1", "DELETE person WHERE b > 1"] {
			let rows = rows_of(sql);
			assert!(
				!rows.any(|node| matches!(node, LogicalOp::Filter(_))),
				"`{sql}` must not evaluate its condition below the write: {rows}"
			);
			assert!(
				!rows.to_string().contains("b > 1"),
				"`{sql}` must not push its condition into the scan: {rows}"
			);
		}
		// The condition is kept, not dropped: the write operator's stage reads
		// it from the statement.
		let LogicalWriteStatement::Update(stmt) =
			write_of("UPDATE person SET a = 1 WHERE b > 1").statement
		else {
			panic!("expected an UPDATE");
		};
		assert!(stmt.cond.is_some());
		let LogicalWriteStatement::Delete(stmt) = write_of("DELETE person WHERE b > 1").statement
		else {
			panic!("expected a DELETE");
		};
		assert!(stmt.cond.is_some());
		// The control: the same target and condition in a `FROM` clause do
		// build the filter, so the assertions above are about where a
		// mutation's condition goes rather than about lowering being unable to
		// place one.
		assert_eq!(
			shape(&lower_sql("SELECT * FROM person WHERE b > 1")),
			["Project", "Filter", "Source"]
		);
	}

	/// An `UPSERT` subject may or may not exist, and the write operator
	/// reconciles the two cases by retrying a conflicting create as an update.
	/// A condition does not change that: a reading source would still drop the
	/// creating case.
	#[test]
	fn upsert_names_targets_whether_or_not_it_has_a_condition() {
		assert_eq!(targets_of("UPSERT person:1 SET a = 1").len(), 1);
		assert_eq!(targets_of("UPSERT person:1 SET a = 1 WHERE b > 1").len(), 1);
		assert_eq!(targets_of("UPSERT a, b:1").len(), 2, "one target per thing written");
	}

	/// `INSERT`'s rows are its subjects: they reach the write operator as its
	/// source, so the operator has a record to write for each of them. The
	/// `INTO` table rides beside them and is absent where the statement names
	/// none, in which case each row names its table through the id it carries.
	#[test]
	fn insert_carries_its_rows_and_its_into_table_as_its_source() {
		let single = inserts_of("INSERT INTO person { a: 1 }");
		assert!(single.into.is_some());
		assert!(matches!(single.rows, LogicalInsertRows::Value(_)));

		let bare = inserts_of("INSERT { id: person:1, a: 1 }");
		assert!(
			bare.into.is_none(),
			"without INTO each record names its table through the id it carries"
		);

		// A `VALUES` tuple keeps one `(place, expression)` pair per column, in
		// the order written: the pairs build the row, so losing their order or
		// their paths loses what the row holds.
		let LogicalInsertRows::Tuples(tuples) =
			inserts_of("INSERT INTO person (a, b) VALUES (1, 2), (3, 4)").rows
		else {
			panic!("a VALUES list must lower to tuples");
		};
		assert_eq!(tuples.len(), 2);
		let rendered: Vec<_> =
			tuples[0].iter().map(|(path, value)| (path.to_sql(), value.to_sql())).collect();
		assert_eq!(rendered, [("a".to_string(), "1".to_string()), ("b".into(), "2".into())]);
	}

	/// The `INTO` table and every row expression are the node's own scalars,
	/// so a pass that rewrites expressions reaches them — the same property
	/// the target list has, and what makes a parameter in a row resolvable.
	#[test]
	fn an_inserts_rows_are_the_nodes_own_scalars() {
		let single = lower_sql("INSERT INTO person { a: $x }");
		assert_eq!(single.scalars().len(), 2, "the INTO table and the one row expression");
		assert!(single.children().is_empty(), "rows are not a plan subtree");

		let tuples = lower_sql("INSERT INTO person (a, b) VALUES (1, 2), (3, 4)");
		assert_eq!(tuples.scalars().len(), 5, "the INTO table and four values");

		let bare = lower_sql("INSERT { id: person:1 }");
		assert_eq!(bare.scalars().len(), 1, "no INTO table, one row expression");
	}

	/// A data clause that names no rows is not `INSERT`'s source, and lowering
	/// declines rather than writing records the statement never described.
	#[test]
	fn an_insert_whose_data_names_no_rows_has_no_logical_form() {
		let stmt = sql::Expr::Insert(Box::new(sql::statements::InsertStatement {
			into: Some(sql::Expr::Table("person".into())),
			data: sql::Data::ContentExpression(sql::Expr::Literal(sql::Literal::Integer(1))),
			ignore: false,
			update: None,
			output: None,
			timeout: sql::Expr::Literal(sql::Literal::None),
			relation: false,
		}));
		assert!(matches!(lower(stmt), Err(NoLogicalForm::Unsupported(_))));
	}

	/// `RELATE`'s targets are `[from, through, to]`, the order
	/// [`LogicalWriteSource::Targets`] documents for this form. None of the
	/// three is read: the endpoints are used as the ids they evaluate to, and
	/// the edge record does not exist yet.
	#[test]
	fn relate_names_its_endpoints_and_the_table_between_them() {
		let rendered: Vec<_> =
			targets_of("RELATE person:a->likes->person:b").iter().map(ToSql::to_sql).collect();
		assert_eq!(rendered, ["person:a", "likes", "person:b"]);
		// They are the node's own scalars, so a pass that rewrites expressions
		// reaches all three.
		let op = lower_sql("RELATE person:a->likes->person:b");
		assert_eq!(op.scalars().len(), 3);
	}

	#[test]
	fn an_explain_statement_carries_its_format() {
		let LogicalOp::Explain(LogicalExplain {
			analyze,
			format,
			input,
		}) = lower_sql("EXPLAIN ANALYZE FORMAT JSON SELECT * FROM t")
		else {
			panic!("expected an Explain");
		};
		assert!(analyze);
		assert_eq!(format, crate::expr::ExplainFormat::Json);
		assert_eq!(input.name(), "Project");
		// The SELECT clause form answers with the structured plan.
		let LogicalOp::Explain(LogicalExplain {
			format,
			analyze,
			..
		}) = lower_sql("SELECT * FROM t EXPLAIN FULL")
		else {
			panic!("expected an Explain");
		};
		assert!(analyze);
		assert_eq!(format, crate::expr::ExplainFormat::Json);
	}

	#[test]
	fn a_geojson_object_lowers_to_a_geometry() {
		assert!(matches!(
			expr("{ type: 'Point', coordinates: [1.0, 2.0] }"),
			LogicalExpr::Literal(LogicalLiteral::Geometry(_))
		));
		assert!(matches!(expr("{ type: 'Point', other: 1 }"), LogicalExpr::Object(_)));
	}
}
