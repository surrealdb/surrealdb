//! Building the statement families (definitions, access, live and match statements) as ops.
//!
//! A schema statement binds its authority checks and operands in the order its body performs
//! them (see [`guard_order`](crate::exec::ir::logical::ddl::guard_order)), then its op. The
//! clauses it stores and never runs are `StoredClause` regions, bound under the frame of the
//! statement that defines them; their ops are never executed, rewritten, folded, hoisted or merged
//! after bind, and a clause's digest equals the digest of the clause the statement's definition
//! template holds.
//!
//! `INFO` binds the operands of its subject, then the `VERSION`, in the order the logical plan
//! lists its expressions. `ACCESS`, `KILL`, `LIVE`, `SHOW`, `OPTION` and `USE` bind the operands
//! their attributes name, then the op. A secret a statement carries is an operand, never an
//! attribute. `MATCH` is one op with one `Subquery` region per stage of the plan, whose block
//! arguments are the bindings in scope; the region holds the stage's op and its clause regions.

#![deny(clippy::wildcard_enum_match_arm)]

use surrealdb_expr::parser::TopLevelExpr;
use surrealdb_strand::Strand;
use surrealdb_types::ToSql;

use super::logical::{
	Flow, Pos, Tail, bind_list, bind_projection, bind_value, enter_row_region, fetch_path_clause,
	first_result, owner_flow, raise_refused, row_clause, statements_of,
};
use super::match_fold;
use crate::exec::BoxFut;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::{OpId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind, RegionProperties, StoredRole};
use crate::exec::ir::iface::statements::{DdlGuard, InfoSubject};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ddl::{self, Body, DdlPlan, DdlStep, PlannedRegion, Src};
use crate::exec::ir::logical::ops::{LogOp, UseTarget};
use crate::exec::ir::logical::seam::{BindCx, Built, bind_expr, invariant};
use crate::exec::ir::logical::verify::timestamp_arg;
use crate::exec::ir::logical::{access, live};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::planner::Planner;
use crate::exec::vm::ir_catalog::fill_field_checks;
use crate::exec::vm::payload::Refusal;
use crate::expr::Expr;
use crate::expr::logical::{
	LogicalDdl, LogicalExpr, LogicalInfo, LogicalMatch, lower, lower_expr, lower_fields,
	lower_select,
};
use crate::expr::statements::UseStatement;
use crate::expr::statements::access::AccessStatement;
use crate::expr::statements::kill::KillStatement;
use crate::expr::statements::live::LiveStatement;
use crate::expr::statements::option::OptionStatement;
use crate::expr::statements::show::ShowStatement;
use crate::val::Value;

/// The logical form of the clause `expr`. A clause the parser produced always has one.
fn lowered(expr: &Expr) -> Result<LogicalExpr, IrError> {
	lower_expr(expr.clone())
		.map_err(|error| invariant(&format!("a clause without a logical form: {error}")))
}

/// A literal of `value` at the end of the current block.
fn literal(cx: &mut BindCx<'_>, value: Value) -> Result<ValueId, IrError> {
	cx.emit_value(OpKind::Expr(ExprOp::Literal(Box::new(value))), &[], TypeKnowledge::Unknown)
}

/// The values of `exprs`, bound in order; `None` when one ended the block.
fn bind_all<'a>(
	cx: &mut BindCx<'_>,
	exprs: impl IntoIterator<Item = &'a LogicalExpr>,
) -> Result<Option<Vec<ValueId>>, IrError> {
	let mut values = Vec::new();
	for expr in exprs {
		match bind_expr(cx, expr)? {
			Built::Value(value) => values.push(value),
			Built::Diverged => return Ok(None),
		}
	}
	Ok(Some(values))
}

// ---------------------------------------------------------------------------------------------
// The hook
// ---------------------------------------------------------------------------------------------

/// The refusal of a statement the entry's surface leaves out.
pub(crate) const NOT_SERVED_SCHEMA: &str = "ir build: a schema, INFO, LIVE, KILL, USE, OPTION, ACCESS or SHOW statement is not served by this entry";

/// Binds the statements that are not data statements: schema statements, `INFO`, `LIVE`, `KILL`,
/// `USE`, `OPTION`, `ACCESS` and `SHOW`.
///
/// The binder reaches them only through this hook, held by the binding context as an `Option`.
/// The context of an entry that serves them holds [`FULL_SCHEMA_BINDING`]; one that does not holds
/// nothing, names none of their binding, and refuses each of them with [`NOT_SERVED_SCHEMA`].
pub(crate) trait SchemaBinding {
	/// Binds the schema statement `ddl` ([`bind_ddl`]).
	fn ddl(&self, cx: &mut BindCx<'_>, ddl: &LogicalDdl) -> Result<Flow, IrError>;
	/// Binds `INFO` ([`bind_info`]).
	fn info(&self, cx: &mut BindCx<'_>, info: &LogicalInfo) -> Result<Flow, IrError>;
	/// Binds the top-level statement `statement`, an `OPTION`, `USE`, `ACCESS`, `KILL`, `LIVE` or
	/// `SHOW`. Any other statement is an invariant break.
	fn statement(&self, cx: &mut BindCx<'_>, statement: &TopLevelExpr) -> Result<Flow, IrError>;
	/// Gives the field statements of the bound container `ir` their catalog checks
	/// ([`fill_field_checks`]). An entry without this hook binds no field statement, so it has
	/// none to check.
	fn fill_field_checks<'a>(
		&'a self,
		ir: &'a mut Ir,
		facts: &'a mut FactCache,
		planner: &'a Planner<'_>,
	) -> BoxFut<'a, Result<(), IrError>>;
}

/// The binding of every statement family [`SchemaBinding`] covers.
pub(crate) struct FullSchemaBinding;

/// The one [`FullSchemaBinding`], named only by the surface that serves those statements.
pub(crate) static FULL_SCHEMA_BINDING: FullSchemaBinding = FullSchemaBinding;

impl SchemaBinding for FullSchemaBinding {
	fn ddl(&self, cx: &mut BindCx<'_>, ddl: &LogicalDdl) -> Result<Flow, IrError> {
		bind_ddl(cx, ddl)
	}

	fn info(&self, cx: &mut BindCx<'_>, info: &LogicalInfo) -> Result<Flow, IrError> {
		bind_info(cx, info)
	}

	fn statement(&self, cx: &mut BindCx<'_>, statement: &TopLevelExpr) -> Result<Flow, IrError> {
		match statement {
			TopLevelExpr::Option(statement) => bind_option(cx, statement),
			TopLevelExpr::Use(statement) => bind_use(cx, statement),
			TopLevelExpr::Access(statement) => bind_access(cx, statement),
			TopLevelExpr::Kill(statement) => bind_kill(cx, statement),
			TopLevelExpr::Live(statement) => bind_live(cx, statement),
			TopLevelExpr::Show(statement) => bind_show(cx, statement),
			TopLevelExpr::Expr(_)
			| TopLevelExpr::Begin(_)
			| TopLevelExpr::Commit
			| TopLevelExpr::Cancel => {
				Err(invariant("a data or transaction statement bound as a schema statement"))
			}
		}
	}

	fn fill_field_checks<'a>(
		&'a self,
		ir: &'a mut Ir,
		facts: &'a mut FactCache,
		planner: &'a Planner<'_>,
	) -> BoxFut<'a, Result<(), IrError>> {
		Box::pin(fill_field_checks(ir, facts, planner))
	}
}

/// The binding `cx` holds for the statements that are not data statements, or
/// [`NOT_SERVED_SCHEMA`] where its entry serves data statements alone.
pub(super) fn schema_binding(
	cx: &BindCx<'_>,
) -> Result<&'static (dyn SchemaBinding + Sync), IrError> {
	cx.schema_binding().ok_or(IrError::NotYet {
		what: NOT_SERVED_SCHEMA,
	})
}

// ---------------------------------------------------------------------------------------------
// Schema statements
// ---------------------------------------------------------------------------------------------

/// A schema statement: its checks and operands in the order of its form, then its op and the
/// regions of its stored clauses.
fn bind_ddl(cx: &mut BindCx<'_>, ddl: &LogicalDdl) -> Result<Flow, IrError> {
	let checks = ddl::checks_of(cx.catalog(), &ddl.statement);
	let plan = ddl::plan(&ddl.statement, checks, cx.codec()?)?;
	if let Some(refusal) = &plan.planning_refusal {
		return raise_refused(cx, refusal.clone());
	}
	if !ddl::operands_follow_table(&plan) {
		return Err(invariant("the operands of a schema statement stand out of its form's order"));
	}
	let mut values: Vec<Option<ValueId>> = vec![None; plan.operands.len()];
	let mut deferred = Vec::new();
	let mut guarded = false;
	for step in ddl::guard_order(plan.form) {
		match step {
			DdlStep::RefuseNoWrite => {
				emit_guard(cx, DdlGuard::RefuseNoWrite)?;
				guarded = true;
			}
			DdlStep::Allow => {
				let (action, resource, base) = plan.allow.clone();
				emit_guard(
					cx,
					DdlGuard::Allow {
						action,
						resource,
						base,
					},
				)?;
				for slot in deferred.drain(..) {
					if !bind_operand(cx, &plan, slot, &mut values)? {
						return Ok(Flow::Diverged);
					}
				}
				if let Some(refusal) = &plan.refusal {
					return raise_refused(cx, refusal.clone());
				}
			}
			DdlStep::Operand(role) => {
				for (slot, operand) in plan.operands.iter().enumerate() {
					if operand.role != *role {
						continue;
					}
					if !guarded && operand.src.is_written() {
						deferred.push(slot);
					} else if !bind_operand(cx, &plan, slot, &mut values)? {
						return Ok(Flow::Diverged);
					}
				}
			}
			DdlStep::Statement => {}
		}
	}
	let operands: Vec<ValueId> = values
		.into_iter()
		.collect::<Option<Vec<_>>>()
		.ok_or_else(|| invariant("an operand of a schema statement was never bound"))?;
	let op = cx.emit(OpKind::Logical(plan.op.clone()), &operands, &[TypeKnowledge::Unknown])?;
	let mut refusal = None;
	for region in &plan.regions {
		if let Some(found) = bind_region(cx, op, region)? {
			refusal.get_or_insert(found);
		}
	}
	owner_flow(cx, op, refusal)
}

fn emit_guard(cx: &mut BindCx<'_>, guard: DdlGuard) -> Result<(), IrError> {
	cx.emit(
		OpKind::Logical(LogOp::DdlGuard {
			guard,
		}),
		&[],
		&[],
	)
	.map(drop)
}

/// Binds the operand in `slot` of `plan` and records its value; `false` when it ended the block.
fn bind_operand(
	cx: &mut BindCx<'_>,
	plan: &DdlPlan<'_>,
	slot: usize,
	values: &mut [Option<ValueId>],
) -> Result<bool, IrError> {
	let operand = plan.operands.get(slot).ok_or(IrError::Stale("operand slot"))?;
	let value = match &operand.src {
		Src::Literal(value) => literal(cx, value.clone())?,
		Src::Expr(expr) => match bind_expr(cx, &lowered(expr)?)? {
			Built::Value(value) => value,
			Built::Diverged => return Ok(false),
		},
	};
	*values.get_mut(slot).ok_or(IrError::Stale("operand slot"))? = Some(value);
	Ok(true)
}

/// Binds one region of a schema statement under the frame of the statement. Returns the refusal
/// raised inside a stored function body, if any.
fn bind_region(
	cx: &mut BindCx<'_>,
	owner: OpId,
	region: &PlannedRegion<'_>,
) -> Result<Option<Refusal>, IrError> {
	match &region.body {
		Body::Clause(expr) => {
			row_clause(cx, owner, region.kind, &lowered(expr)?)?;
			Ok(None)
		}
		Body::Function {
			block,
			params,
		} => {
			let body = lower(block.as_ref().clone()).map_err(|error| {
				invariant(&format!("a function body without a logical form: {error}"))
			})?;
			// The timestamp, the parameters, then the row of the caller, which a field read or `@`
			// in the body reads.
			let mut args = vec![timestamp_arg()];
			args.extend(params.iter().map(|_| TypeKnowledge::Unknown));
			args.push(TypeKnowledge::Unknown);
			cx.within(owner, region.kind, &args, |cx, entry| {
				for (param, value) in params.iter().zip(entry.iter().skip(1)) {
					cx.declare(param.clone(), *value);
				}
				let row = entry.last().copied().ok_or(IrError::Stale("caller row"))?;
				cx.with_row(row, |cx| {
					bind_list(cx, statements_of(&body), Pos::Statement, Tail::Value)
				})
			})
		}
		Body::Select(select) => {
			let query = lower_select(select.as_ref().clone()).map_err(|error| {
				invariant(&format!("a stored query without a logical form: {error}"))
			})?;
			let args: &[TypeKnowledge] = if region.kind.isolated_from_above() {
				&[timestamp_arg()]
			} else {
				&[]
			};
			cx.within(owner, region.kind, args, |cx, _| {
				if let Some(value) = bind_value(cx, &query)? {
					cx.yield_logical(&[value])?;
				}
				Ok(None)
			})
		}
	}
}

// ---------------------------------------------------------------------------------------------
// INFO
// ---------------------------------------------------------------------------------------------

/// `INFO`: the operands of its subject, then its `VERSION`, in the order the plan lists them. A
/// name written bare (a table, a user or an index) is a string literal; any other name is the
/// value of its expression, which lowering reads as a string.
fn bind_info(cx: &mut BindCx<'_>, info: &LogicalInfo) -> Result<Flow, IrError> {
	let (subject, structure) = match info {
		LogicalInfo::Root {
			structure,
			..
		} => (InfoSubject::Root, *structure),
		LogicalInfo::Ns {
			structure,
			..
		} => (InfoSubject::Ns, *structure),
		LogicalInfo::Db {
			structure,
			..
		} => (InfoSubject::Db, *structure),
		LogicalInfo::Table {
			structure,
			..
		} => (InfoSubject::Table, *structure),
		LogicalInfo::User {
			base,
			structure,
			..
		} => (
			InfoSubject::User {
				base: *base,
			},
			*structure,
		),
		LogicalInfo::Index {
			structure,
			..
		} => (InfoSubject::Index, *structure),
	};
	let mut exprs = Vec::new();
	info.exprs(&mut exprs);
	// The subject's leading operands are names, and a bare name stands for itself: it reads no
	// row, so it binds as the string it spells.
	let names = match subject {
		InfoSubject::Root | InfoSubject::Ns | InfoSubject::Db => 0,
		InfoSubject::Table
		| InfoSubject::User {
			..
		} => 1,
		InfoSubject::Index => 2,
	};
	let mut operands = Vec::with_capacity(exprs.len());
	for (at, expr) in exprs.into_iter().enumerate() {
		let bare = if at >= names {
			None
		} else if let LogicalExpr::Field(name) = expr {
			Some(name.clone())
		} else if let LogicalExpr::Table(name) = expr {
			Some(Strand::new(name.as_str()))
		} else {
			None
		};
		let value = match bare {
			Some(name) => literal(cx, Value::String(name))?,
			None => match bind_expr(cx, expr)? {
				Built::Value(value) => value,
				Built::Diverged => return Ok(Flow::Diverged),
			},
		};
		operands.push(value);
	}
	cx.emit_value(
		OpKind::Logical(LogOp::Info {
			subject,
			structure,
		}),
		&operands,
		TypeKnowledge::Unknown,
	)
	.map(Flow::Value)
}

// ---------------------------------------------------------------------------------------------
// ACCESS, KILL, LIVE, SHOW, OPTION, USE
// ---------------------------------------------------------------------------------------------

/// `ACCESS`: the operands the statement evaluates, then the op and the filter of its grants.
fn bind_access(cx: &mut BindCx<'_>, statement: &AccessStatement) -> Result<Flow, IrError> {
	let plan = access::plan_access(statement);
	let mut operands = Vec::with_capacity(plan.operands.len());
	for expr in &plan.operands {
		match bind_expr(cx, &lowered(expr)?)? {
			Built::Value(value) => operands.push(value),
			Built::Diverged => return Ok(Flow::Diverged),
		}
	}
	let op = cx.emit(
		OpKind::Logical(LogOp::Access(Box::new(plan.attrs))),
		&operands,
		&[TypeKnowledge::Unknown],
	)?;
	if let Some(filter) = plan.filter {
		row_clause(cx, op, RegionKind::Clause(ClauseRole::Where), &lowered(filter)?)?;
	}
	first_result(cx, op).map(Flow::Value)
}

/// `KILL`: the id of the live query, then the op, which carries the id as the statement wrote it.
///
/// Errors that name the statement print the id as written. The op keeps that text as its
/// location's surface text. The digest ignores it, whether or not the statement is explained.
fn bind_kill(cx: &mut BindCx<'_>, statement: &KillStatement) -> Result<Flow, IrError> {
	let Built::Value(id) = bind_expr(cx, &lowered(&statement.id)?)? else {
		return Ok(Flow::Diverged);
	};
	let op = cx.emit(OpKind::Logical(LogOp::Kill), &[id], &[TypeKnowledge::Unknown])?;
	cx.write_text_on(op, statement.id.to_sql())?;
	first_result(cx, op).map(Flow::Value)
}

/// `LIVE`: the target, then the op and the stored clauses its subscription holds: the
/// projection, the filter and the fetch paths, in that order.
fn bind_live(cx: &mut BindCx<'_>, statement: &LiveStatement) -> Result<Flow, IrError> {
	let plan = live::plan_live(statement, cx.codec()?)?;
	let Built::Value(target) = bind_expr(cx, &lowered(plan.target)?)? else {
		return Ok(Flow::Diverged);
	};
	let op = cx.emit(
		OpKind::Logical(LogOp::Live(Box::new(plan.attrs))),
		&[target],
		&[TypeKnowledge::Unknown],
	)?;
	if let Some(fields) = plan.fields {
		let fields = lower_fields(fields.clone()).map_err(|error| {
			invariant(&format!("a live projection without a logical form: {error}"))
		})?;
		let kind = RegionKind::StoredClause(StoredRole::LiveFields);
		enter_row_region(cx, op, kind, |cx, row| {
			let projected =
				cx.with_row(row, |cx| bind_projection(cx, &fields, &[], false, Vec::new(), row))?;
			cx.yield_logical(&[projected])
		})?;
	}
	if let Some(cond) = plan.cond {
		row_clause(cx, op, RegionKind::StoredClause(StoredRole::LiveCond), &lowered(cond)?)?;
	}
	if !plan.fetch.is_empty() {
		let paths: Vec<LogicalExpr> =
			plan.fetch.iter().map(|path| lowered(path)).collect::<Result<_, _>>()?;
		let kind = RegionKind::StoredClause(StoredRole::LiveFetch);
		enter_row_region(cx, op, kind, |cx, row| {
			let fetch =
				cx.emit(OpKind::Logical(LogOp::Fetch), &[row], &[TypeKnowledge::Unknown])?;
			for path in &paths {
				fetch_path_clause(cx, fetch, path)?;
			}
			let fetched = first_result(cx, fetch)?;
			cx.yield_logical(&[fetched])
		})?;
	}
	first_result(cx, op).map(Flow::Value)
}

/// `SHOW CHANGES`: its attributes alone.
fn bind_show(cx: &mut BindCx<'_>, statement: &ShowStatement) -> Result<Flow, IrError> {
	cx.emit_value(
		OpKind::Logical(LogOp::Show(Box::new(access::show_attrs(statement)))),
		&[],
		TypeKnowledge::Unknown,
	)
	.map(Flow::Value)
}

/// `OPTION`: its attributes alone.
fn bind_option(cx: &mut BindCx<'_>, statement: &OptionStatement) -> Result<Flow, IrError> {
	cx.emit_value(
		OpKind::Logical(LogOp::Option {
			name: Strand::new(statement.name.as_str()),
			value: statement.what,
		}),
		&[],
		TypeKnowledge::Unknown,
	)
	.map(Flow::Value)
}

/// `USE`: the namespace, then the database, each when the target names it. A name written bare is
/// a string literal: it reads no row, and a `USE` whose names are all literals is one the request
/// carries as written.
fn bind_use(cx: &mut BindCx<'_>, statement: &UseStatement) -> Result<Flow, IrError> {
	let (target, exprs): (UseTarget, Vec<&Expr>) = match statement {
		UseStatement::Ns(ns) => (UseTarget::Ns, vec![ns]),
		UseStatement::Db(db) => (UseTarget::Db, vec![db]),
		UseStatement::NsDb(ns, db) => (UseTarget::NsDb, vec![ns, db]),
		UseStatement::Default => (UseTarget::Default, Vec::new()),
	};
	let mut operands = Vec::with_capacity(exprs.len());
	for expr in exprs {
		let value = match crate::ddl::scalar::written_name(expr) {
			Some(name) => literal(cx, Value::String(Strand::new(name.as_str())))?,
			None => match bind_expr(cx, &lowered(expr)?)? {
				Built::Value(value) => value,
				Built::Diverged => return Ok(Flow::Diverged),
			},
		};
		operands.push(value);
	}
	cx.emit_value(
		OpKind::Logical(LogOp::Use {
			target,
		}),
		&operands,
		TypeKnowledge::Unknown,
	)
	.map(Flow::Value)
}

// ---------------------------------------------------------------------------------------------
// MATCH
// ---------------------------------------------------------------------------------------------

/// `MATCH`: one op with one `Subquery` region holding the fold of the plan's binding table
/// ([`match_fold`]).
///
/// The fold is reached through the hook `cx` holds ([`match_fold::MatchBinding`]); where the entry
/// serves no `MATCH`, the statement is refused with [`match_fold::NOT_SERVED_MATCH`].
pub(super) fn bind_match(cx: &mut BindCx<'_>, statement: &LogicalMatch) -> Result<Flow, IrError> {
	match cx.match_binding() {
		Some(binding) => binding.bind(cx, statement),
		None => Err(IrError::NotYet {
			what: match_fold::NOT_SERVED_MATCH,
		}),
	}
}
