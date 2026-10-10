//! Lowering graph lookups into [`Instr::Lookup`] and the hops it takes.
//!
//! A run of consecutive lookups is one lookup, its hops grouped by the
//! planner: a `->edge->vertex` pair that planner
//! reads in one scan from the vertex each adjacency key names
//! (`Planner::try_fast_path_pair`) is one hop through the edge, and every
//! other lookup is a hop of its own. Each hop's records are read with the
//! permissions and field views of their tables as the catalog holds them at
//! lowering, which the program records it read, and its `WHERE` is a host
//! pipeline of the program over each record reached. A `WHERE` the planner
//! judges before the edge is read — from the adjacency key, or from the
//! inline fields the adjacency entry carries — is judged from them here too,
//! against the snapshot of each edge table's inline fields the planner took.
//!
//! A lookup taken here is a graph lookup of named or wildcard edge tables
//! with nothing but a `WHERE`, `ONLY` and a `*` projection, which shows its
//! records whole, evaluated in the caller's frame or, with no `WHERE`, in a
//! computed field's frame of the caller's own principal. The records of a
//! wildcard hop, and of every hop under a `VERSION`, are read as the scans'
//! own record resolution reads them. A hop whose records' view or permission
//! has no lowering leaves the lookup evaluated whole.

use std::ops::Bound;

use super::{Lowered, Lowering, NotCovered};
use crate::exec::vm::program::{
	CompareOp, EdgeSpec, FrameMode, HopPermission, HopRead, HopTable, HostScalar, Instr, KeyCheck,
	KeyCond, LookupHop, LookupPage, LookupSpec, NavOp, Operand, ReferenceSubject,
};
use crate::expr::Dir;
use crate::expr::logical::{
	GuardKind, LogicalExpr, LogicalFields, LogicalLiteral, LogicalLookup, LogicalLookupSubject,
	LogicalPermission,
};
use crate::expr::lookup::LookupKind;
use crate::val::{TableName, Value};

impl Lowering<'_, '_> {
	/// The lookups `expr` ends in, innermost first, and the expression they
	/// start from, when every one of them is of the shape lowering takes.
	pub(super) fn lookup_run<'e>(
		&self,
		expr: &'e LogicalExpr,
	) -> Option<(Vec<&'e LogicalLookup>, &'e LogicalExpr)> {
		// In a frame, only a computed field's of the statement's principal,
		// whose reads are checked as the statement's are, or one derived for
		// a permission predicate, or a permission predicate's, whose reads are
		// not checked. A hop's field views and `WHERE` run over the records
		// reached with the caller's own frame, so only hops that read no
		// record are taken in a frame whose reads differ from the caller's:
		// any but a computed field's.
		let framed = match self.eval_frame {
			None => false,
			Some(frame) => match self.permissions.get(frame as usize) {
				Some(spec) if spec.mode == FrameMode::Computed && spec.auth.is_none() => false,
				Some(spec) if spec.mode == FrameMode::Derived && spec.auth.is_none() => true,
				Some(spec) if matches!(spec.mode, FrameMode::Guard(_) | FrameMode::Gate(_)) => true,
				_ => return None,
			},
		};
		// A name a hop is given (`->(edge AS name)`) names the projected field
		// the path writes, and changes nothing the hop reaches.
		//
		// The last hop of a run may be paged, `LIMIT` and `START` written as
		// non-negative integers: the lookup answers the page of the records
		// its last hop reaches over all its origins ([`LookupSpec::page`]).
		// A hop that is paged shows ids alone, and is no `ONLY`.
		let plain = |lookup: &LogicalLookup, last: bool| {
			let paged = lookup.limit.is_some() || lookup.start.is_some();
			// `->(SELECT * FROM edge)` is the hop's records shown whole.
			(!framed || (lookup.cond.is_none() && lookup.expr.is_none()))
				&& (lookup.expr.is_none() || lookup.expr == Some(LogicalFields::all()))
				&& lookup.split.is_none()
				&& lookup.group.is_none()
				&& lookup.order.is_none()
				&& (!paged
					|| (last
						&& lookup.expr.is_none()
						&& lookup.cond.is_none()
						&& !lookup.only && page_of(lookup).is_some()))
				&& lookup.what.iter().all(|subject| match subject {
					LogicalLookupSubject::Table {
						referencing_field,
						..
					}
					| LogicalLookupSubject::Range {
						referencing_field,
						..
					} => referencing_field.is_none() || matches!(lookup.kind, LookupKind::Reference),
				})
		};
		// The run ends at a lookup with clauses of its own, which is the
		// base the run steps from.
		let mut run = Vec::new();
		let mut node = expr;
		while let LogicalExpr::Lookup {
			base,
			lookup,
		} = node && plain(lookup, run.is_empty())
		{
			run.push(lookup.as_ref());
			node = base;
		}
		run.reverse();
		if run.is_empty() {
			return None;
		}
		// A page is of what one path reaches, which `ONLY` anywhere on it
		// unwraps in its own way: the statement of the page answers that.
		if run.last().is_some_and(|last| page_of(last).is_some()) && only_in(expr) {
			return None;
		}
		Some((run, node))
	}

	/// The value of lookup `expr` as an [`Instr::Lookup`], or `None` when the
	/// lookup is evaluated whole.
	///
	/// `ONLY` on any hop of a path unwraps what the whole path answers: where
	/// a hop of the base the run steps from has it, the base is read without
	/// it and the run unwraps its answer instead.
	pub(super) async fn lookup(&mut self, expr: &LogicalExpr) -> Lowered<Option<Operand>> {
		let Some((run, base)) = self.lookup_run(expr) else {
			return Ok(None);
		};
		let Some(mut spec) = crate::exec::vm::host::out_of_line(|| self.lookup_spec(&run)).await?
		else {
			return Ok(None);
		};
		let src = if only_in(base) {
			spec.only_path = true;
			self.value(&without_only(base)).await?
		} else {
			crate::exec::vm::host::out_of_line(|| self.value(base)).await?
		};
		let lookup = u32::try_from(self.lookups.len())
			.map_err(|_| NotCovered::internal("more lookups than a program holds"))?;
		self.lookups.push(spec);
		let dst = self.register()?;
		self.emit(Instr::Lookup {
			lookup,
			src,
			dst,
			frame: self.eval_frame,
		});
		self.delegate_context = self.delegate_context.max(crate::exec::ContextLevel::Database);
		self.uses_eval = true;
		self.may_fail = true;
		Ok(Some(Operand::Reg(dst)))
	}

	/// A lookup whose last hop has clauses — a projection, `SPLIT`, `GROUP`,
	/// `ORDER`, `LIMIT` or `START` — as the statement those clauses make over
	/// the records the hop reaches without them, shown whole: `SELECT fields
	/// FROM base->(SELECT * FROM hop) WHERE … SPLIT … GROUP … ORDER … LIMIT …
	/// START …`, the
	/// records taken back to their ids where the hop answers ids, under `ONLY`
	/// where the lookup has it. The statement's `$parent` is the row the
	/// lookup is taken over, as the lookup's clauses read it, so the base is
	/// read from `$parent` where it steps from the row. Bound with the
	/// statement's planner; `None` where the lookup has no such form here.
	///
	/// The statement is mapped as a statement the expression holds, run per
	/// row it is taken over, so it lowers as a group of the program.
	pub(super) async fn lookup_statement(&mut self, expr: &LogicalExpr) -> Option<LogicalExpr> {
		use crate::expr::logical::{LogicalOp, LogicalSubquery};
		let op = self.lookup_statement_op(expr)?;
		// In a frame the statement is held unbound, as any statement a
		// clause holds: its sources are resolved where it runs, under the
		// frame's principal and its permission contract.
		if self.eval_frame.is_some() {
			let LogicalOp::Subquery(LogicalSubquery {
				input,
			}) = op
			else {
				return None;
			};
			return Some(LogicalExpr::Subquery(input));
		}
		let bound = crate::exec::planner::bind_statement(self.planner, op).await.ok()?;
		let LogicalOp::Subquery(LogicalSubquery {
			input,
		}) = bound
		else {
			return None;
		};
		if !self.nested_plans.iter().any(|nested| nested.op == *input)
			&& let Ok(plan) = Box::pin(super::super::map::map(self.planner, &input)).await
		{
			let plan = super::super::optimize::optimize(plan);
			self.nested_plans.push(std::sync::Arc::new(super::super::plan::Nested {
				op: (*input).clone(),
				plan,
				facts: super::super::plan::NestedFacts {
					correlated: true,
					repeatable: false,
					reads_rows: true,
					enclosing_writes: self.writes,
				},
				join: None,
				lookup: None,
				hashed: None,
				strategy: super::super::plan::Strategy::Apply,
				membership: false,
			}));
		}
		Some(LogicalExpr::Subquery(input))
	}

	/// The unbound statement of [`Self::lookup_statement`].
	pub(super) fn lookup_statement_op(
		&self,
		expr: &LogicalExpr,
	) -> Option<crate::expr::logical::LogicalOp> {
		use crate::expr::logical::{
			LogicalAggregate, LogicalFilter, LogicalLimit, LogicalOnly, LogicalOp, LogicalProject,
			LogicalSort, LogicalSplit, LogicalSubquery, LogicalValues,
		};
		// The statement reads its names from the frame, where no register the
		// pipeline binds a name to is. Inside an element body it is taken over
		// the element, its `$parent` ([`Self::statement_doc`]).
		if !self.inline_params.is_empty() || self.reads_versioned() {
			return None;
		}
		let LogicalExpr::Lookup {
			base,
			lookup,
		} = expr
		else {
			return None;
		};
		// In a frame whose reads differ from the caller's a hop's `WHERE` and
		// field views are the statement's too ([`Self::lookup_run`]).
		let framed = self.eval_frame.is_some() && self.lookup_run(expr).is_none();
		let clauses = (framed && (lookup.cond.is_some() || lookup.expr.is_some()))
			|| lookup.expr.as_ref().is_some_and(|fields| *fields != LogicalFields::all())
			|| lookup.split.is_some()
			|| lookup.group.is_some()
			|| lookup.order.is_some()
			|| lookup.limit.is_some()
			|| lookup.start.is_some();
		if !clauses {
			return None;
		}
		// `ONLY` on a hop of the base unwraps what the whole path answers,
		// which is what the statement answers: the base is read without it.
		let only = lookup.only || only_in(base);
		// A statement's value and its host scalars are evaluated over its
		// `$this`, which is its `$parent` too.
		let parent_is_row = self.document == super::DocumentRoot::RowElseReceiver;
		let base = reroot(&without_only(base), parent_is_row)?;
		// The records reached, shown whole, which the statement reads as its
		// rows.
		let plain = LogicalLookup {
			kind: lookup.kind.clone(),
			expr: Some(LogicalFields::all()),
			only: false,
			what: lookup.what.clone(),
			cond: None,
			split: None,
			group: None,
			order: None,
			limit: None,
			start: None,
			alias: None,
		};
		let mut op = LogicalOp::Values(LogicalValues {
			expr: LogicalExpr::Lookup {
				base: Box::new(base),
				lookup: Box::new(plain),
			},
			for_update: false,
			written: false,
		});
		if let Some(cond) = &lookup.cond {
			op = LogicalOp::Filter(LogicalFilter {
				input: Box::new(op),
				predicate: cond.clone(),
			});
		}
		if let Some(split) = &lookup.split {
			op = LogicalOp::Split(LogicalSplit {
				input: Box::new(op),
				exprs: split.clone(),
			});
		}
		// The hop answers the records' ids unless it projects or groups them.
		let fields = match &lookup.expr {
			Some(fields) => fields.clone(),
			None if lookup.group.is_some() => LogicalFields::all(),
			None => LogicalFields::Value {
				expr: LogicalExpr::Field("id".into()),
				alias: None,
			},
		};
		if let Some(group) = &lookup.group {
			op = LogicalOp::Aggregate(LogicalAggregate {
				input: Box::new(op),
				group: group.clone(),
				fields: fields.clone(),
			});
		}
		if let Some(ordering) = &lookup.order {
			op = LogicalOp::Sort(LogicalSort {
				input: Box::new(op),
				ordering: ordering.clone(),
				tempfiles: false,
			});
		}
		if lookup.limit.is_some() || lookup.start.is_some() {
			op = LogicalOp::Limit(LogicalLimit {
				input: Box::new(op),
				limit: lookup.limit.clone(),
				start: lookup.start.clone(),
			});
		}
		if lookup.group.is_none() {
			op = LogicalOp::Project(LogicalProject {
				input: Box::new(op),
				fields,
				omit: Vec::new(),
			});
		}
		if only {
			op = LogicalOp::Only(LogicalOnly {
				input: Box::new(op),
			});
		}
		Some(LogicalOp::Subquery(LogicalSubquery {
			input: Box::new(op),
		}))
	}

	/// Whether `call` is the `count(->edge)` the planner answers from the
	/// adjacency's cardinality ([`Self::degree_count`]).
	pub(super) fn counts_degree(&self, call: &crate::expr::logical::LogicalCall) -> bool {
		self.degree_shape(call).is_some()
	}

	/// The direction and edge tables of `call` when it is a `count(->edge)`
	/// the planner the call is compiled with answers from the adjacency's
	/// cardinality.
	fn degree_shape(
		&self,
		call: &crate::expr::logical::LogicalCall,
	) -> Option<(crate::exec::parts::LookupDirection, Vec<TableName>)> {
		let crate::expr::Function::Normal(name) = &call.receiver else {
			return None;
		};
		self.bare.as_ref().unwrap_or(self.planner).graph_degree_shape(name, &call.args)
	}

	/// `call`, a `count(->edge)` of builtin `name` the planner answers from
	/// the adjacency's cardinality: [`Instr::Degree`] over the current value, and where the
	/// storage leaves the count to the call, the call itself.
	pub(super) async fn degree_count(
		&mut self,
		call: &crate::expr::logical::LogicalCall,
		name: String,
	) -> Lowered<Operand> {
		let Some((direction, tables)) = self.degree_shape(call) else {
			return Err(NotCovered::internal("a degree count of a call that counts no degree"));
		};
		let degree = u32::try_from(self.degrees.len())
			.map_err(|_| NotCovered::internal("more degree counts than a program holds"))?;
		self.degrees.push(crate::exec::vm::program::DegreeSpec {
			direction,
			tables,
		});
		self.delegate_context = self.delegate_context.max(crate::exec::ContextLevel::Database);
		let src = crate::exec::vm::host::out_of_line(|| self.value(&LogicalExpr::Current)).await?;
		let dst = self.register()?;
		let counted = self.emit(Instr::Degree {
			degree,
			src,
			dst,
			done: 0,
		});
		let value = crate::exec::vm::host::out_of_line(|| self.builtin_call(call, name)).await?;
		self.emit(Instr::Load {
			dst,
			src: value,
		});
		let done = self.here();
		self.patch(counted, done);
		self.uses_eval = true;
		self.may_fail = true;
		Ok(Operand::Reg(dst))
	}

	/// Whether a broadcast of `body` over `base` is lowered to instructions:
	/// both are, and the body does not read the row in place of the element
	/// ([`Self::reads_frame_of_element`]). A
	/// destructure's aliased bodies read the value destructured, which is the
	/// element's.
	pub(super) fn is_native_broadcast(&self, base: &LogicalExpr, body: &LogicalExpr) -> bool {
		self.is_native(base) && self.is_native(body) && !self.reads_frame_of_element(body)
	}

	/// Whether `body`, evaluated with an element as its current value, reads
	/// the row in place of the element: in
	/// an index or a method's argument or a statement where the document root
	/// is not known to be the row, through a projection function, an
	/// invocation, or a closure that reads more than its parameters.
	pub(super) fn reads_frame_of_element(&self, body: &LogicalExpr) -> bool {
		body.any(|node| match node {
			// A statement is taken over the document root, which the body
			// cannot place where it is unknown; one that reads no row
			// answers alike over any.
			LogicalExpr::Subquery(op) => {
				self.document == super::DocumentRoot::Unknown
					&& crate::exec::planner::row_scope::reads_enclosing_row(op)
			}
			LogicalExpr::Invoke {
				..
			} => true,
			// A closure that reads only its parameters reads no frame.
			LogicalExpr::Closure(closure) => !super::path::reads_only_its_parameters(closure),
			// An index or a method's argument is evaluated against the
			// document root, which the body cannot place where it is
			// unknown.
			LogicalExpr::Index {
				index,
				..
			} => self.document == super::DocumentRoot::Unknown && reads_row(index),
			LogicalExpr::Method {
				args,
				..
			} => self.document == super::DocumentRoot::Unknown && args.iter().any(reads_row),
			// A projection function reads the current value.
			other => self.is_projection_call(other),
		})
	}

	/// A path's broadcast of `body` over `base`: over an array or a set, `body`
	/// once per element with the element as the current value, the results
	/// collected into the same kind of collection and, for an array, flattened
	/// one level when `flatten` is set; over anything else, `body` once with
	/// the value itself as the current value. A mapped body whose first step is `.*` takes
	/// that step only over an element that is a record id; any other element
	/// is its own first step's value.
	pub(super) async fn broadcast(
		&mut self,
		base: &LogicalExpr,
		body: &LogicalExpr,
		flatten: bool,
	) -> Lowered<Operand> {
		let mut stripped = body.clone();
		if strip_leading_each(&mut stripped) {
			return crate::exec::vm::host::out_of_line(|| {
				self.broadcast_each(base, body, &stripped, flatten)
			})
			.await;
		}
		// The body is lowered once, in the loop: a value that is not an
		// array or a set enters the body directly as its one element, and that one
		// result is the broadcast's. Lowering it once per branch would double
		// the program at each broadcast a path nests in another.
		let src = crate::exec::vm::host::out_of_line(|| self.value(base)).await?;
		let element = self.register()?;
		self.emit(Instr::Load {
			dst: element,
			src,
		});
		let single = self.register()?;
		let no = self.constant(Value::from(false));
		self.emit(Instr::Load {
			dst: single,
			src: no,
		});
		let items = self.register()?;
		let dst = self.register()?;
		let begin = self.emit(Instr::MapBegin {
			src: Operand::Reg(element),
			items,
			dst,
			other: 0,
			sets: true,
		});
		let pos = self.register()?;
		let zero = self.constant(Value::from(0i64));
		self.emit(Instr::Load {
			dst: pos,
			src: zero,
		});
		let index = self.register()?;
		let top = self.here();
		let next = self.emit(Instr::ElemNext {
			array: items,
			pos,
			elem: element,
			index,
			done: 0,
		});
		let body_start = self.here();
		let outer = self.element.replace(element);
		let mapped = crate::exec::vm::host::out_of_line(|| self.value(body)).await;
		self.element = outer;
		let mapped = mapped?;
		let once = self.emit(Instr::JumpIf {
			cond: Operand::Reg(single),
			truthy: true,
			target: 0,
		});
		self.emit(Instr::CollectPush {
			collection: dst,
			value: mapped,
		});
		self.emit(Instr::Jump {
			target: top,
		});
		let done = self.here();
		self.patch(next, done);
		if flatten {
			self.emit(Instr::Nav {
				op: NavOp::Flatten,
				src: Operand::Reg(dst),
				index: None,
				dst,
			});
		}
		let over = self.emit(Instr::Jump {
			target: 0,
		});
		// Anything but an array or a set: the body runs once, over the value
		// itself.
		let other = self.here();
		self.patch(begin, other);
		let yes = self.constant(Value::from(true));
		self.emit(Instr::Load {
			dst: single,
			src: yes,
		});
		self.emit(Instr::Jump {
			target: body_start,
		});
		let unwrapped = self.here();
		self.patch(once, unwrapped);
		self.emit(Instr::Load {
			dst,
			src: mapped,
		});
		let end = self.here();
		self.patch(over, end);
		self.uses_eval = true;
		Ok(Operand::Reg(dst))
	}

	/// [`Self::broadcast`] of a body whose first step is `.*`, `stripped` the
	/// body without it: over an array or a set, that step is taken only over an
	/// element that is a record id, and any other element is its own first
	/// step's value; over anything else, `body` runs once over the value.
	async fn broadcast_each(
		&mut self,
		base: &LogicalExpr,
		body: &LogicalExpr,
		stripped: &LogicalExpr,
		flatten: bool,
	) -> Lowered<Operand> {
		let src = crate::exec::vm::host::out_of_line(|| self.value(base)).await?;
		let held = self.register()?;
		self.emit(Instr::Load {
			dst: held,
			src,
		});
		let items = self.register()?;
		let dst = self.register()?;
		let begin = self.emit(Instr::MapBegin {
			src: Operand::Reg(held),
			items,
			dst,
			other: 0,
			sets: true,
		});
		let pos = self.register()?;
		let zero = self.constant(Value::from(0i64));
		self.emit(Instr::Load {
			dst: pos,
			src: zero,
		});
		let element = self.register()?;
		let index = self.register()?;
		let top = self.here();
		let next = self.emit(Instr::ElemNext {
			array: items,
			pos,
			elem: element,
			index,
			done: 0,
		});
		let outer = self.element;
		let plain = self.register()?;
		self.emit(Instr::Load {
			dst: plain,
			src: Operand::Reg(element),
		});
		let skip = self.emit(Instr::JumpIfNotRecordId {
			cond: Operand::Reg(element),
			target: 0,
		});
		self.emit(Instr::Each {
			src: Operand::Reg(element),
			dst: plain,
			frame: self.eval_frame,
		});
		let end = self.here();
		self.patch(skip, end);
		self.element = Some(plain);
		let mapped = crate::exec::vm::host::out_of_line(|| self.value(stripped)).await;
		self.element = outer;
		self.emit(Instr::CollectPush {
			collection: dst,
			value: mapped?,
		});
		self.emit(Instr::Jump {
			target: top,
		});
		let done = self.here();
		self.patch(next, done);
		if flatten {
			self.emit(Instr::Nav {
				op: NavOp::Flatten,
				src: Operand::Reg(dst),
				index: None,
				dst,
			});
		}
		let over = self.emit(Instr::Jump {
			target: 0,
		});
		let other = self.here();
		self.patch(begin, other);
		let outer = self.element.replace(held);
		let once = crate::exec::vm::host::out_of_line(|| self.value(body)).await;
		self.element = outer;
		self.emit(Instr::Load {
			dst,
			src: once?,
		});
		let end = self.here();
		self.patch(over, end);
		self.uses_eval = true;
		Ok(Operand::Reg(dst))
	}

	/// The collapse of the hop pair `edge`, `vertex` into one read of the
	/// edge's targets ([`Planner::fast_path_pair_shape`]), where its shape
	/// admits one and each edge table's `SELECT` permission, as the plan read
	/// it, is `Full` or the session's reads go unchecked.
	fn fast_path_pair(
		&self,
		edge: &LogicalLookup,
		vertex: &LogicalLookup,
	) -> Lowered<Option<crate::exec::planner::TargetVertexPlan>> {
		let Some(plan) = self.planner.fast_path_pair_shape(edge, vertex) else {
			return Ok(None);
		};
		let (Some(txn), Some(ns), Some(db)) =
			(self.planner.txn(), self.planner.ns(), self.planner.db())
		else {
			return Ok(None);
		};
		if self.planner.should_check_perms_for_view(ns, db) {
			for spec in &plan.edge_tables {
				let full = self.catalog.table(txn, &spec.table)?.is_some_and(|read| {
					read.definition.as_ref().is_some_and(|def| {
						matches!(def.permissions.select, crate::catalog::Permission::Full)
					})
				});
				if !full {
					return Ok(None);
				}
			}
		}
		Ok(Some(plan))
	}

	/// The definition and the fields of `table` as the plan read them; `None`
	/// for a table the catalog does not hold.
	fn table_defs(&self, table: &TableName) -> Option<crate::exec::planner::TableDefs> {
		let read = self.catalog.table(self.planner.txn()?, table).ok()??;
		Some((
			std::sync::Arc::clone(read.definition.as_ref()?),
			std::sync::Arc::clone(&read.fields),
		))
	}

	/// The hops of `run`, or `None` when one has no lowering. Each hop's host
	/// pipelines run over the records it reaches, not the element or the
	/// catch the lookup is lowered inside.
	async fn lookup_spec(&mut self, run: &[&LogicalLookup]) -> Lowered<Option<LookupSpec>> {
		let element = self.element.take();
		let catch = self.return_catch.take();
		let spec = crate::exec::vm::host::out_of_line(|| self.hops(run)).await;
		self.element = element;
		self.return_catch = catch;
		spec
	}

	/// The hops of `run`, or `None` when one has no lowering.
	async fn hops(&mut self, run: &[&LogicalLookup]) -> Lowered<Option<LookupSpec>> {
		let checked = !self.planner.perms_suppressed && self.planner.compiles_for_checked_caller();
		let mut hops = Vec::new();
		let mut at = 0;
		while at < run.len() {
			if at + 1 < run.len()
				&& let Some(plan) = self.fast_path_pair(run[at], run[at + 1])?
			{
				let Some(hop) =
					crate::exec::vm::host::out_of_line(|| self.through_hop(plan, run[at], checked))
						.await?
				else {
					return Ok(None);
				};
				hops.push(hop);
				at += 2;
				continue;
			}
			let Some(hop) =
				crate::exec::vm::host::out_of_line(|| self.edge_hop(run[at], checked)).await?
			else {
				return Ok(None);
			};
			hops.push(hop);
			at += 1;
		}
		let last = run.last().expect("a run holds a lookup");
		Ok(Some(LookupSpec {
			hops,
			extract_id: last.cond.is_some() && last.expr.is_none(),
			only: run.iter().any(|lookup| lookup.only),
			only_path: false,
			checked,
			page: last_page(run),
		}))
	}

	/// The hop through `edge`'s edges to the vertex their adjacency keys
	/// name that `plan` reads.
	async fn through_hop(
		&mut self,
		plan: crate::exec::planner::TargetVertexPlan,
		edge: &LogicalLookup,
		checked: bool,
	) -> Lowered<Option<LookupHop>> {
		use crate::exec::parts::LookupDirection;
		let directions = match plan.direction {
			LookupDirection::Out => vec![Dir::Out],
			LookupDirection::In => vec![Dir::In],
			_ => return Ok(None),
		};
		let mut tables = Vec::with_capacity(plan.target_tables.len());
		for table in &plan.target_tables {
			let Some(read) =
				crate::exec::vm::host::out_of_line(|| self.hop_table(table, checked, false))
					.await?
			else {
				return Ok(None);
			};
			tables.push((table.clone(), read));
		}
		let specs = crate::exec::vm::host::out_of_line(|| self.edge_specs(&edge.what)).await?;
		let Some(prefixes) = prefixes(&specs) else {
			return Ok(None);
		};
		Ok(Some(LookupHop {
			read: std::sync::Arc::new(HopRead::graph(
				directions,
				specs,
				prefixes,
				Some(plan.target_tables.clone()),
				unbounded(&plan.target_tables),
			)),
			full: false,
			tables,
			cond: None,
			key_cond: None,
		}))
	}

	/// The edge tables `what` names, each with the bounds a range written on
	/// it puts on its edges' keys.
	async fn edge_specs(&mut self, what: &[LogicalLookupSubject]) -> Lowered<Vec<EdgeSpec>> {
		let mut specs = Vec::with_capacity(what.len());
		for subject in what {
			specs.push(match subject {
				LogicalLookupSubject::Table {
					table,
					..
				} => EdgeSpec::whole(table.clone()),
				LogicalLookupSubject::Range {
					table,
					range,
					..
				} => {
					let (start, end) =
						crate::exec::vm::host::out_of_line(|| self.key_bounds(range)).await?;
					EdgeSpec {
						table: table.clone(),
						start,
						end,
					}
				}
			});
		}
		Ok(specs)
	}

	/// The bounds of key range `range`, each a host scalar the hop evaluates
	/// with no row.
	async fn key_bounds(
		&mut self,
		range: &crate::expr::logical::LogicalRecordIdKeyRange,
	) -> Lowered<(Bound<HostScalar>, Bound<HostScalar>)> {
		let start =
			crate::exec::vm::host::out_of_line(|| self.key_bound_scalar(&range.start)).await?;
		let end = crate::exec::vm::host::out_of_line(|| self.key_bound_scalar(&range.end)).await?;
		Ok((start, end))
	}

	/// One bound of a key range as a host scalar.
	async fn key_bound_scalar(
		&mut self,
		bound: &Bound<crate::expr::logical::LogicalRecordIdKey>,
	) -> Lowered<Bound<HostScalar>> {
		let (key, included) = match bound {
			Bound::Unbounded => return Ok(Bound::Unbounded),
			Bound::Included(key) => (key, true),
			Bound::Excluded(key) => (key, false),
		};
		let expr = crate::exec::planner::util::key_lit_to_expr(key).map_err(|e| {
			match super::super::plan::Refusal::of(&e) {
				// The planner refuses the bound as it plans the lookup.
				Some(refusal) => NotCovered::refused(refusal),
				None => NotCovered::internal(format!("a range bound with no expression: {e}")),
			}
		})?;
		let scalar = crate::exec::vm::host::out_of_line(|| self.host_scalar(&expr)).await?;
		Ok(if included {
			Bound::Included(scalar)
		} else {
			Bound::Excluded(scalar)
		})
	}

	/// The hop `lookup` takes to the edges of each record, or to the records
	/// referencing it.
	async fn edge_hop(
		&mut self,
		lookup: &LogicalLookup,
		checked: bool,
	) -> Lowered<Option<LookupHop>> {
		let named: Vec<TableName> = lookup
			.what
			.iter()
			.map(|subject| match subject {
				LogicalLookupSubject::Table {
					table,
					..
				}
				| LogicalLookupSubject::Range {
					table,
					..
				} => table.clone(),
			})
			.collect();
		// A `WHERE` the planner judges before the edge is read is judged from
		// its key, and the inline fields its entry carries.
		let (cond, key_cond) = match (&lookup.cond, &lookup.kind) {
			(None, _) => (None, None),
			(Some(cond), LookupKind::Reference) => {
				match crate::exec::vm::host::out_of_line(|| self.cond_pipeline(cond)).await {
					Some(pipeline) => (Some(pipeline), None),
					None => return Ok(None),
				}
			}
			(Some(cond), LookupKind::Graph(dir)) => {
				// Compiled only to ask the planner whether it pushes the
				// `WHERE` into the scan, as it compiles it to ask.
				let Ok(compiled) = self.planner.scalar_metadata(cond.clone()).await else {
					return Ok(None);
				};
				let pushed = self.planner.graph_predicate_payload_spec_from(
					cond,
					&compiled,
					&lookup.what,
					(*dir).into(),
					&|table| self.table_defs(table),
				);
				match pushed {
					Some(pushed) => {
						let Some(pipeline) =
							crate::exec::vm::host::out_of_line(|| self.cond_pipeline(cond)).await
						else {
							return Ok(None);
						};
						let payload = (!pushed.key_only).then(|| {
							let mut tables: Vec<_> = pushed.tables.into_iter().collect();
							tables.sort_by(|(a, _), (b, _)| a.cmp(b));
							tables
						});
						(
							None,
							Some(KeyCond {
								pipeline,
								needs_id: pushed.needs_id,
								needs_in: pushed.needs_in,
								needs_out: pushed.needs_out,
								payload,
								checks: key_checks(cond),
							}),
						)
					}
					None => {
						let Some(pipeline) =
							crate::exec::vm::host::out_of_line(|| self.cond_pipeline(cond)).await
						else {
							return Ok(None);
						};
						(Some(pipeline), None)
					}
				}
			}
		};
		let full = cond.is_some() || lookup.expr.is_some();
		let read = match &lookup.kind {
			LookupKind::Graph(dir) => {
				let specs =
					crate::exec::vm::host::out_of_line(|| self.edge_specs(&lookup.what)).await?;
				let Some(prefixes) = prefixes(&specs) else {
					return Ok(None);
				};
				HopRead::graph(
					match dir {
						Dir::Out => vec![Dir::Out],
						Dir::In => vec![Dir::In],
						Dir::Both => vec![Dir::In, Dir::Out],
					},
					specs,
					prefixes,
					None,
					Vec::new(),
				)
			}
			LookupKind::Reference => {
				let mut subjects = Vec::with_capacity(lookup.what.len().max(1));
				for subject in &lookup.what {
					let (table, referencing_field, (start, end)) = match subject {
						LogicalLookupSubject::Table {
							table,
							referencing_field,
						} => (table, referencing_field, (Bound::Unbounded, Bound::Unbounded)),
						LogicalLookupSubject::Range {
							table,
							referencing_field,
							range,
						} => (
							table,
							referencing_field,
							crate::exec::vm::host::out_of_line(|| self.key_bounds(range)).await?,
						),
					};
					let Some(prefix) = reference_prefix(table, referencing_field.as_deref()) else {
						return Ok(None);
					};
					subjects.push(ReferenceSubject {
						table: Some(table.clone()),
						field: referencing_field.clone(),
						start,
						end,
						prefix: Some(prefix),
					});
				}
				// `<~?` is every table that references the record.
				if subjects.is_empty() {
					subjects.push(ReferenceSubject {
						table: None,
						field: None,
						start: Bound::Unbounded,
						end: Bound::Unbounded,
						prefix: None,
					});
				}
				HopRead::References(subjects)
			}
		};
		// Under a `VERSION` the records are read as the scans read them, their
		// tables' permissions and views as they stood at the version, which
		// lowering does not read; so are a wildcard hop's, whose tables
		// lowering does not know.
		let mut tables = Vec::with_capacity(named.len());
		if !self.reads_versioned() {
			for table in &named {
				let Some(read) =
					crate::exec::vm::host::out_of_line(|| self.hop_table(table, checked, full))
						.await?
				else {
					return Ok(None);
				};
				tables.push((table.clone(), read));
			}
		}
		Ok(Some(LookupHop {
			read: std::sync::Arc::new(read),
			full,
			tables,
			cond,
			key_cond,
		}))
	}

	/// How a hop reads a record of `table`: its `PERMISSIONS FOR select` for
	/// a `checked` caller, and its field view when the record is shown
	/// `full`; `None` when either has no lowering.
	async fn hop_table(
		&mut self,
		table: &TableName,
		checked: bool,
		full: bool,
	) -> Lowered<Option<HopTable>> {
		let permission = if checked {
			let Some(txn) = self.planner.txn().cloned() else {
				return Ok(None);
			};
			let Some(read) = self.catalog.table(&txn, table)? else {
				return Ok(None);
			};
			match read.definition.as_deref().map(|def| def.select_permission_logical()) {
				None | Some(Ok(LogicalPermission::None)) => HopPermission::Deny,
				Some(Ok(LogicalPermission::Full)) => HopPermission::Allow,
				Some(Ok(LogicalPermission::Specific(predicate)))
					if crate::exec::logical::unplannable_write_in_expr(&predicate).is_none() =>
				{
					match crate::exec::vm::host::out_of_line(|| {
						self.permission_pipeline(table, &predicate)
					})
					.await
					{
						Some(pipeline) if reads_parent(&predicate) => {
							HopPermission::JudgedOverParent(pipeline)
						}
						Some(pipeline) => HopPermission::Judged(pipeline),
						None => return Ok(None),
					}
				}
				Some(_) => return Ok(None),
			}
		} else {
			HopPermission::Allow
		};
		let view = if full {
			match crate::exec::vm::host::out_of_line(|| self.image_view(table)).await {
				Ok(Some((view, _))) => view,
				_ => return Ok(None),
			}
		} else {
			None
		};
		Ok(Some(HopTable {
			permission,
			view,
		}))
	}

	/// A host pipeline over a record of `table` answering whether table
	/// permission `predicate` admits it, judged with the `COMPUTED` fields it
	/// reads derived.
	async fn permission_pipeline(
		&mut self,
		table: &TableName,
		predicate: &LogicalExpr,
	) -> Option<u16> {
		let derivations =
			crate::exec::vm::host::out_of_line(|| self.logical_guard_derivations(table, predicate))
				.await
				.ok()?;
		let aside = self.set_aside();
		let sorted = std::mem::replace(&mut self.reads_sorted_rows, false);
		let built = Box::pin(async {
			let dst = self.register()?;
			let rejected = if derivations.is_empty() {
				crate::exec::vm::host::out_of_line(|| {
					self.permission(predicate, &None, GuardKind::Read, None, None)
				})
				.await?
			} else {
				crate::exec::vm::host::out_of_line(|| {
					self.derived_permission(predicate, &None, GuardKind::Read, &derivations)
				})
				.await?
			};
			let admitted = self.constant(Value::Bool(true));
			self.emit(Instr::Load {
				dst,
				src: admitted,
			});
			let over = self.emit(Instr::Jump {
				target: 0,
			});
			let here = self.here();
			self.patch_all(rejected, here);
			let refused = self.constant(Value::Bool(false));
			self.emit(Instr::Load {
				dst,
				src: refused,
			});
			let here = self.here();
			self.patch(over, here);
			self.emit(Instr::Emit {
				src: dst,
			});
			self.emit(Instr::EndRow);
			Ok::<(), NotCovered>(())
		})
		.await;
		self.reads_sorted_rows = sorted;
		self.restore(aside, built).ok()
	}

	/// A host pipeline over a record reached answering `cond`'s value.
	async fn cond_pipeline(&mut self, cond: &LogicalExpr) -> Option<u16> {
		let aside = self.set_aside();
		let sorted = std::mem::replace(&mut self.reads_sorted_rows, false);
		let built = Box::pin(async {
			let value = crate::exec::vm::host::out_of_line(|| self.value(cond)).await?;
			let dst = self.register()?;
			self.emit(Instr::Load {
				dst,
				src: value,
			});
			self.emit(Instr::Emit {
				src: dst,
			});
			self.emit(Instr::EndRow);
			Ok::<(), NotCovered>(())
		})
		.await;
		self.reads_sorted_rows = sorted;
		self.restore(aside, built).ok()
	}
}

/// Whether a lookup of the run `expr` ends in has `ONLY`.
fn only_in(expr: &LogicalExpr) -> bool {
	let mut node = expr;
	while let LogicalExpr::Lookup {
		base,
		lookup,
	} = node
	{
		if lookup.only {
			return true;
		}
		node = base;
	}
	false
}

/// The page `lookup` asks for, where `LIMIT` and `START` are written as
/// non-negative integers; `None` for a lookup that pages nothing, or whose
/// clauses are evaluated when it runs.
fn page_of(lookup: &LogicalLookup) -> Option<LookupPage> {
	let integer = |expr: &LogicalExpr| match expr {
		LogicalExpr::Literal(LogicalLiteral::Integer(n)) => usize::try_from(*n).ok(),
		_ => None,
	};
	if lookup.limit.is_none() && lookup.start.is_none() {
		return None;
	}
	let limit = match &lookup.limit {
		Some(limit) => Some(integer(limit)?),
		None => None,
	};
	let start = match &lookup.start {
		Some(start) => integer(start)?,
		None => 0,
	};
	Some(LookupPage {
		start,
		limit,
	})
}

/// The page of the last lookup of `run`.
fn last_page(run: &[&LogicalLookup]) -> Option<LookupPage> {
	run.last().and_then(|last| page_of(last))
}

/// `expr` with `ONLY` taken off every lookup of the path it ends in.
fn without_only(expr: &LogicalExpr) -> LogicalExpr {
	match expr {
		LogicalExpr::Lookup {
			base,
			lookup,
		} => {
			let mut lookup = lookup.clone();
			lookup.only = false;
			LogicalExpr::Lookup {
				base: Box::new(without_only(base)),
				lookup,
			}
		}
		other => other.clone(),
	}
}

/// `base`, a lookup's base, read from `$parent` where it steps from the row:
/// its root the row itself or a field of it, or `$parent` where
/// `parent_is_row` says the row the lookup is taken over is `$parent` already.
/// `None` for a base with any other read of the row.
fn reroot(base: &LogicalExpr, parent_is_row: bool) -> Option<LogicalExpr> {
	let parent = || LogicalExpr::Param(crate::expr::Param::from("parent".to_owned()));
	Some(match base {
		LogicalExpr::Current => parent(),
		LogicalExpr::Field(name) => LogicalExpr::Access {
			base: Box::new(parent()),
			field: name.clone(),
		},
		LogicalExpr::Access {
			base,
			field,
		} => LogicalExpr::Access {
			base: Box::new(reroot(base, parent_is_row)?),
			field: field.clone(),
		},
		LogicalExpr::Lookup {
			base,
			lookup,
		} if lookup.cond.as_ref().is_none_or(|cond| !reads_parent(cond)) => LogicalExpr::Lookup {
			base: Box::new(reroot(base, parent_is_row)?),
			lookup: lookup.clone(),
		},
		LogicalExpr::First(base) => LogicalExpr::First(Box::new(reroot(base, parent_is_row)?)),
		LogicalExpr::Last(base) => LogicalExpr::Last(Box::new(reroot(base, parent_is_row)?)),
		LogicalExpr::Each(base) => LogicalExpr::Each(Box::new(reroot(base, parent_is_row)?)),
		LogicalExpr::Param(param) if parent_is_row && param.as_str() == "parent" => base.clone(),
		other
			if !reads_row(other) && !other.any(|node| matches!(node, LogicalExpr::Subquery(_))) =>
		{
			other.clone()
		}
		_ => return None,
	})
}

/// Replace the first step `body` applies to its current value with the
/// current value itself when that step is `.*`, looking through the bases of
/// the broadcasts it nests; whether it was.
fn strip_leading_each(body: &mut LogicalExpr) -> bool {
	let mut node = body;
	loop {
		if matches!(node, LogicalExpr::Each(base) if matches!(**base, LogicalExpr::Current)) {
			*node = LogicalExpr::Current;
			return true;
		}
		node = match node {
			LogicalExpr::Broadcast {
				base,
				..
			}
			| LogicalExpr::Access {
				base,
				..
			}
			| LogicalExpr::Index {
				base,
				..
			}
			| LogicalExpr::First(base)
			| LogicalExpr::Last(base)
			| LogicalExpr::Each(base)
			| LogicalExpr::Flatten {
				base,
				..
			}
			| LogicalExpr::Filter {
				base,
				..
			}
			| LogicalExpr::Method {
				base,
				..
			}
			| LogicalExpr::Destructure {
				base,
				..
			}
			| LogicalExpr::Optional {
				base,
				..
			}
			| LogicalExpr::Lookup {
				base,
				..
			}
			| LogicalExpr::RepeatRecurse(base) => base.as_mut(),
			LogicalExpr::Recurse(recurse) => &mut recurse.base,
			_ => return false,
		};
	}
}

/// Whether `expr` reads the value it is evaluated against: a field of it, it
/// itself, or a parameter that names a row.
pub(super) fn reads_row(expr: &LogicalExpr) -> bool {
	expr.any(|node| match node {
		LogicalExpr::Field(_) | LogicalExpr::Doc | LogicalExpr::Current => true,
		LogicalExpr::Param(param) => {
			crate::exec::planner::row_scope::RowScopeKind::from_param_name(param.as_str()).is_some()
		}
		_ => false,
	})
}

/// `cond`, a key-only `WHERE`, as the comparisons [`KeyCheck`] states joined
/// by `AND`, in the order the `WHERE` judges them; `None` for any other
/// expression.
fn key_checks(cond: &LogicalExpr) -> Option<Vec<KeyCheck>> {
	fn collect(expr: &LogicalExpr, out: &mut Vec<KeyCheck>) -> Option<()> {
		let LogicalExpr::Binary {
			left,
			op,
			right,
		} = expr
		else {
			return None;
		};
		if matches!(op, crate::expr::operator::BinaryOperator::And) {
			collect(left, out)?;
			return collect(right, out);
		}
		let op = CompareOp::of(op)?;
		let (field, value, swapped) = match (&**left, &**right) {
			(LogicalExpr::Field(name), other) => {
				(name, crate::exec::planner::static_value(other)?, false)
			}
			(other, LogicalExpr::Field(name)) => {
				(name, crate::exec::planner::static_value(other)?, true)
			}
			_ => return None,
		};
		out.push(KeyCheck {
			field: field.as_str().to_owned(),
			op,
			value: crate::exec::vm::plain::ValueData::new(value, "a key check").ok()?,
			swapped,
		});
		Some(())
	}
	let mut checks = Vec::new();
	collect(cond, &mut checks)?;
	Some(checks)
}

/// Whether `predicate` reads `$parent`, which a lookup binds
/// to the row it is taken over where a table's permission is judged.
fn reads_parent(predicate: &LogicalExpr) -> bool {
	predicate.any(|node| matches!(node, LogicalExpr::Param(param) if param.as_str() == "parent"))
}

/// `table`, and `field` when named, encoded as the leading segments of a
/// cached reference naming them.
fn reference_prefix(table: &TableName, field: Option<&str>) -> Option<Vec<u8>> {
	let mut prefix = storekey::encode_vec(table).ok()?;
	if let Some(field) = field {
		prefix.extend(storekey::encode_vec(field).ok()?);
	}
	Some(prefix)
}

/// `tables` as edge tables read whole.
fn unbounded(tables: &[TableName]) -> Vec<EdgeSpec> {
	tables.iter().map(|table| EdgeSpec::whole(table.clone())).collect()
}

/// Each of `specs`' tables encoded as the leading segment of a cached entry's
/// edge identity.
fn prefixes(specs: &[EdgeSpec]) -> Option<Vec<Vec<u8>>> {
	specs.iter().map(|spec| storekey::encode_vec(&spec.table).ok()).collect()
}
