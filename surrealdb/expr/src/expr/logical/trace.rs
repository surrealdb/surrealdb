//! Which lookups of a statement a query trace reports.
//!
//! A client may ask a streaming query to report the graph its statements walked
//! (`surrealdb_rpc::trace`). What is reported is what reaches the statement's
//! result as written: a lookup whose value is copied into the result
//! unchanged. [`mark`] gives each lookup of a statement its [`LookupTrace`]
//! from where it is written, before the statement is bound:
//!
//! - [`LookupTrace::Traced`]: a projection field, `SELECT VALUE`, a graph in `FROM`, a `RETURN` or
//!   bare value, a `LET` value, and anything nested in those through a path, an object or array
//!   literal, a recursion body or a subquery's own returned positions.
//! - [`LookupTrace::Partial`]: a lookup whose value reaches the result through something that
//!   changes it — a function or method call, an operator, an aggregate, a closure. It is not
//!   reported; running it tells the client the statement's trace is incomplete.
//! - [`LookupTrace::Inner`]: a lookup whose answer a traced lookup of the same path starts from, or
//!   whose answer a step cuts before it reaches the result: an index, `[0]`, `[$]` or a filter. Its
//!   walk is reported by that lookup, or by [`ANCHOR`] around the step, cut to what leads to the
//!   value that reaches the result.
//! - [`LookupTrace::None`]: a condition — `WHERE`, `ORDER BY`, `GROUP BY`, `SPLIT`, a filter part,
//!   an index — or anything else that does not feed the result. It is neither reported nor marks
//!   the trace incomplete.
//!
//! A record link the result reads through is traced where the read is in a traced position: the
//! value a field read, an index or a lookup is taken from, when that value is a field of a record
//! (`author` in `author.name`, `tags` in `tags.*.label`). [`mark`] writes the field's read as
//! [`LINK`]`(value, holder, field)`, which answers the value unchanged and records the link. In a
//! position that changes the value, as a [`LookupTrace::Partial`] lookup's, the holder is `NONE`:
//! a link read there is not recorded and marks the trace incomplete. An unaliased projection field
//! the marks rewrite takes as its alias the name the planner gives the field as written
//! ([`LogicalExpr::named_after_gates`]), so the rewrite never moves a value in the result.
//!
//! The marks are syntax, decided once per statement. A statement is marked only for a request that
//! asks for a trace, so a statement planned for any other request is the one it always was, and
//! the marks being part of the plan's identity keep the two from sharing a plan.

use super::expr::{
	LogicalCall, LogicalDestructureField, LogicalExpr, LogicalField, LogicalFields, LogicalLiteral,
	LogicalLookup, LogicalRecurseInstruction,
};
use super::{EvalFrame, LogicalOp};

/// The name of the function the marking pass writes around a record link the
/// result reads through: `link(value, holder, field)` answers `value` and
/// records the link. No query can write the name.
pub const LINK: &str = "\u{0}trace::link";

/// The name of the function the marking pass writes around a step that cuts
/// what a traced lookup answered, such as an index or a filter:
/// `anchor(value)` answers `value` and reports what the lookups before the
/// step walked, cut to what leads to `value`; `anchor(value, true)`, around a
/// step that cuts link reads, marks the trace partial. No query can write the
/// name.
pub const ANCHOR: &str = "\u{0}trace::anchor";

/// Whether `name` is the name of a function only the marking pass writes.
pub fn is_internal(name: &str) -> bool {
	name == LINK || name == ANCHOR
}

/// What a query trace does with one lookup. See the [module](self).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum LookupTrace {
	/// Not traced, and not a part of the result: a condition.
	#[default]
	None,
	/// Its edges are reported: its value is a part of the result.
	Traced,
	/// Not traced, though its value feeds the result: running it marks the
	/// statement's trace partial.
	Partial,
	/// Its value is where a traced lookup of the same path starts, through
	/// steps that keep records: a filter, an index, a flatten, a field read; or
	/// what a step cuts on its way into the result. What it walks is reported
	/// by that lookup, or by [`ANCHOR`] around the step, cut to what leads to
	/// the value that reaches the result.
	Inner,
}

impl LookupTrace {
	/// The role of what feeds this one through a change of value.
	fn changed(self) -> Self {
		match self {
			LookupTrace::Traced | LookupTrace::Partial | LookupTrace::Inner => LookupTrace::Partial,
			LookupTrace::None => LookupTrace::None,
		}
	}

	/// The role of the path a lookup in this role starts from.
	fn below(self) -> Self {
		match self {
			LookupTrace::Traced | LookupTrace::Inner => LookupTrace::Inner,
			LookupTrace::Partial | LookupTrace::None => self,
		}
	}
}

/// Mark every lookup of the statement `op` with its role in a query trace.
pub fn mark(op: &mut LogicalOp) {
	mark_op(op, LookupTrace::Traced);
}

/// Mark `fields`, the `RETURN` fields of a write whose role is `role`
/// ([`LogicalWrite::trace`](super::LogicalWrite::trace)), which are written
/// apart from its logical form.
pub fn mark_fields(fields: &mut LogicalFields, role: LookupTrace) {
	fields_in(fields, role);
}

fn mark_op(op: &mut LogicalOp, role: LookupTrace) {
	// A write's targets, rows and clauses are what it writes; its `RETURN`
	// fields are what it answers, and are marked where they are lowered.
	if let LogicalOp::Write(_) = op {
		for scalar in op.scalars_mut() {
			expr(scalar, LookupTrace::None);
		}
		if let LogicalOp::Write(write) = op {
			write.trace = role;
		}
		return;
	}
	match op {
		LogicalOp::Project(project) => {
			fields_in(&mut project.fields, role);
			for omit in &mut project.omit {
				expr(omit, LookupTrace::None);
			}
			mark_op(&mut project.input, role);
		}
		LogicalOp::Values(values) => expr(&mut values.expr, role),
		LogicalOp::Eval(eval) => expr(&mut eval.expr, role),
		// `FROM` a value or a graph: what it names are the rows.
		LogicalOp::Source(source) => {
			let mut exprs = Vec::new();
			source.source.exprs_mut(&mut exprs);
			for value in exprs {
				expr(value, role);
			}
		}
		LogicalOp::Aggregate(aggregate) => {
			for group in &mut aggregate.group {
				expr(group, LookupTrace::None);
			}
			// A group's row is built from many input rows: what an aggregate
			// answers is a change of value.
			fields_in(&mut aggregate.fields, role.changed());
			mark_op(&mut aggregate.input, role);
		}
		// A window function, like an aggregate, answers a value built from
		// many rows.
		LogicalOp::Window(_) => {
			let changed = role.changed();
			for scalar in op.scalars_mut() {
				expr(scalar, changed);
			}
			for child in op.children_mut() {
				mark_op(child, role);
			}
		}
		LogicalOp::Let(binding) => mark_op(&mut binding.value, role),
		LogicalOp::Return(ret) => {
			mark_op(&mut ret.what, role);
			for fetch in &mut ret.fetch {
				expr(fetch, LookupTrace::None);
			}
		}
		// A page, a `DISTINCT` or a filter drops rows its input already evaluated.
		// Where its input holds the projection (`DISTINCT` evaluates it below the
		// page) or the lookups are the rows themselves (a graph in `FROM`), those
		// lookups ran for rows the result does not hold, so they feed it through a
		// change. A page or a filter under a projection holds no projected lookup,
		// so this changes nothing there.
		LogicalOp::Limit(_) | LogicalOp::Distinct(_) | LogicalOp::Filter(_) => {
			for scalar in op.scalars_mut() {
				expr(scalar, LookupTrace::None);
			}
			let changed = role.changed();
			for child in op.children_mut() {
				mark_op(child, changed);
			}
		}
		// A loop answers nothing of its body.
		LogicalOp::Foreach(foreach) => {
			expr(&mut foreach.range, LookupTrace::None);
			mark_op(&mut foreach.body, LookupTrace::None);
		}
		// Everything else: its own expressions are clauses (a filter, an
		// ordering, a page, a write's targets and data), and its inputs answer
		// what it answers.
		other @ LogicalOp::Match(_)
		| other @ LogicalOp::Subquery(_)
		| other @ LogicalOp::Write(_)
		| other @ LogicalOp::Ddl(_)
		| other @ LogicalOp::UnionAll(_)
		| other @ LogicalOp::Join(_)
		| other @ LogicalOp::PermissionFilter(_)
		| other @ LogicalOp::Split(_)
		| other @ LogicalOp::Sort(_)
		| other @ LogicalOp::Fetch(_)
		| other @ LogicalOp::Timeout(_)
		| other @ LogicalOp::VersionScope(_)
		| other @ LogicalOp::Only(_)
		| other @ LogicalOp::Explain(_)
		| other @ LogicalOp::Sequence(_)
		| other @ LogicalOp::If(_)
		| other @ LogicalOp::Throw(_)
		| other @ LogicalOp::Break(_)
		| other @ LogicalOp::Continue(_)
		| other @ LogicalOp::Info(_)
		| other @ LogicalOp::Sleep(_) => {
			for scalar in other.scalars_mut() {
				expr(scalar, LookupTrace::None);
			}
			for child in other.children_mut() {
				mark_op(child, role);
			}
		}
	}
}

fn fields_in(fields: &mut LogicalFields, role: LookupTrace) {
	match fields {
		LogicalFields::Value {
			expr: value,
			..
		} => expr(value, role),
		LogicalFields::Select(fields) => {
			for field in fields {
				if let LogicalField::Single {
					expr: value,
					alias,
					frame,
					..
				} = field
				{
					// A column the binder adds runs in a frame of its own: a
					// computed field or a field guard, which is schema.
					let role = if *frame == EvalFrame::caller() {
						role
					} else {
						LookupTrace::None
					};
					// A field the marks rewrite keeps the name the planner gives
					// the field as written; any other field is named by the
					// planner as it always is.
					let unmarked = (alias.is_none() && !matches!(role, LookupTrace::None))
						.then(|| value.clone());
					expr(value, role);
					if let Some(unmarked) = unmarked
						&& unmarked != *value
					{
						*alias = Some(unmarked.named_after_gates().output_path());
					}
				}
			}
		}
	}
}

fn lookup_in(lookup: &mut LogicalLookup, role: LookupTrace) {
	lookup.trace = role;
	if let Some(fields) = &mut lookup.expr {
		fields_in(fields, role);
	}
	if let Some(cond) = &mut lookup.cond {
		expr(cond, LookupTrace::None);
	}
	for clause in [&mut lookup.split, &mut lookup.group].into_iter().flatten() {
		for value in clause {
			expr(value, LookupTrace::None);
		}
	}
	if let Some(order) = &mut lookup.order {
		let mut keys = Vec::new();
		order.exprs_mut(&mut keys);
		for key in keys {
			expr(key, LookupTrace::None);
		}
	}
	for page in [&mut lookup.limit, &mut lookup.start].into_iter().flatten() {
		expr(page, LookupTrace::None);
	}
}

fn destructure_in(fields: &mut [LogicalDestructureField], role: LookupTrace) {
	for field in fields {
		match field {
			LogicalDestructureField::Aliased(_, body) => expr(body, role),
			LogicalDestructureField::Destructure(_, nested) => destructure_in(nested, role),
			LogicalDestructureField::All(_)
			| LogicalDestructureField::Field(_)
			| LogicalDestructureField::Rest => {}
		}
	}
}

/// Where `read`, a value a step reads through, is a field of a record — the
/// row's own field, or a field of a value — write it as [`LINK`] over the field,
/// the record holding it and its name, so the link is recorded when it is read.
/// In a position that changes the value the holder is `NONE`, so a link read
/// there marks the trace partial instead.
fn link_read(read: &mut LogicalExpr, role: LookupTrace) {
	if matches!(role, LookupTrace::None) {
		return;
	}
	// A field read through, or a step that keeps part of one (`tags[0]`,
	// `tags[WHERE …]`): the step's value is what the link reaches, so it is
	// recorded for the elements kept.
	let Some((holder, field)) = field_read(cuts(read).unwrap_or(read)) else {
		return;
	};
	let holder = match role {
		LookupTrace::Partial => LogicalExpr::Literal(LogicalLiteral::None),
		LookupTrace::None | LookupTrace::Traced | LookupTrace::Inner => holder,
	};
	let value = std::mem::replace(read, LogicalExpr::Literal(LogicalLiteral::None));
	*read = LogicalExpr::Call(Box::new(LogicalCall {
		receiver: crate::expr::Function::Normal(LINK.to_owned()),
		args: vec![value, holder, LogicalExpr::Literal(LogicalLiteral::String(field))],
	}));
}

/// The record holding field read `read` and the field's name: the row's own
/// field, or a field of a value.
fn field_read(read: &LogicalExpr) -> Option<(LogicalExpr, surrealdb_strand::Strand)> {
	if let LogicalExpr::Field(field) = read {
		return Some((LogicalExpr::Doc, field.clone()));
	}
	let LogicalExpr::Access {
		base,
		field,
	} = read
	else {
		return None;
	};
	// The holder is evaluated a second time, beside the value read from it.
	// Only a read that answers the same and does nothing else when read again
	// may be; for any other, the link is not recorded and the trace is partial
	// (`NONE` as the holder says so).
	let holder = if rereads(base) {
		(**base).clone()
	} else {
		LogicalExpr::Literal(LogicalLiteral::None)
	};
	Some((holder, field.clone()))
}

/// Whether evaluating `e` again answers what it answered and does nothing
/// else: a read of the row, a parameter or a literal, and the steps that read
/// into such a value, a link read the marking wrote among them. Anything that
/// computes — a function, an operator, a lookup, a subquery — is not.
fn rereads(e: &LogicalExpr) -> bool {
	if matches!(
		e,
		LogicalExpr::Literal(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Field(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
	) {
		return true;
	}
	if let LogicalExpr::Access {
		base,
		..
	}
	| LogicalExpr::First(base)
	| LogicalExpr::Last(base)
	| LogicalExpr::Each(base)
	| LogicalExpr::Flatten {
		base,
		..
	} = e
	{
		return rereads(base);
	}
	if let LogicalExpr::Index {
		base,
		index,
	} = e
	{
		return rereads(base) && rereads(index);
	}
	if let LogicalExpr::RecordId(_) = e {
		return e.children().into_iter().all(rereads);
	}
	if let LogicalExpr::Call(call) = e
		&& matches!(&call.receiver, crate::expr::Function::Normal(name) if name == LINK)
	{
		return call.args.iter().all(rereads);
	}
	false
}

/// Whether `e` is a step that keeps only some of what it is handed: an index,
/// the first or the last element, or a filter.
fn cuts(e: &LogicalExpr) -> Option<&LogicalExpr> {
	if let LogicalExpr::Index {
		base,
		..
	}
	| LogicalExpr::First(base)
	| LogicalExpr::Last(base)
	| LogicalExpr::Filter {
		base,
		..
	} = e
	{
		return Some(base);
	}
	None
}

/// Whether `e` holds a lookup of its own path: one in a nested statement is
/// that statement's.
fn has_lookup(e: &LogicalExpr) -> bool {
	if let LogicalExpr::Lookup {
		..
	} = e
	{
		return true;
	}
	if let LogicalExpr::Subquery(_) = e {
		return false;
	}
	e.children().into_iter().any(has_lookup)
}

/// Whether `e` holds a subquery, whose statement reports what it walks into the
/// trace as it runs.
fn has_subquery(e: &LogicalExpr) -> bool {
	matches!(e, LogicalExpr::Subquery(_)) || e.children().into_iter().any(has_subquery)
}

/// Write `NONE` as the holder of every [`LINK`] the marks wrote into `e`, so
/// each link read there marks the trace partial rather than being recorded.
fn unname_link_holders(e: &mut LogicalExpr) {
	if let LogicalExpr::Call(call) = e
		&& matches!(&call.receiver, crate::expr::Function::Normal(name) if name == LINK)
		&& let Some(holder) = call.args.get_mut(1)
	{
		*holder = LogicalExpr::Literal(LogicalLiteral::None);
	}
	for child in e.children_mut() {
		unname_link_holders(child);
	}
}

/// Whether `e` reads through a record link a traced position would record: a
/// field read, an element step or a destructure over a field.
fn reads_through_links(e: &LogicalExpr) -> bool {
	if let LogicalExpr::Access {
		base,
		..
	}
	| LogicalExpr::Each(base)
	| LogicalExpr::Destructure {
		base,
		..
	} = e && field_read(base).is_some()
	{
		return true;
	}
	if let LogicalExpr::Subquery(_) = e {
		return false;
	}
	e.children().into_iter().any(reads_through_links)
}

fn expr(e: &mut LogicalExpr, role: LookupTrace) {
	// A step that cuts values a subquery answered: the subquery's statement
	// reports what it walks as it runs, whether or not the step keeps its value,
	// so what it walks reaches the result through a change.
	if role == LookupTrace::Traced && cuts(e).is_some_and(has_subquery) {
		expr(e, LookupTrace::Partial);
		return;
	}
	// A step that cuts what a traced lookup answered on its way into the result:
	// the lookups before it hold what they walk, and [`ANCHOR`] around the step
	// reports it against the value the step keeps. A link read beneath the step
	// is recorded when it is read and names no record to prune it against, so it
	// marks the trace partial instead.
	if role == LookupTrace::Traced && cuts(e).is_some_and(has_lookup) {
		expr(e, LookupTrace::Inner);
		unname_link_holders(e);
		let step = std::mem::replace(e, LogicalExpr::Literal(LogicalLiteral::None));
		*e = LogicalExpr::Call(Box::new(LogicalCall {
			receiver: crate::expr::Function::Normal(ANCHOR.to_owned()),
			args: vec![step],
		}));
		return;
	}
	// A step that cuts link reads holding no lookup: the value it keeps need
	// name no record to prune them against, so they are not recorded, and the
	// trace says it is partial.
	if role == LookupTrace::Traced && cuts(e).is_some_and(reads_through_links) {
		expr(e, LookupTrace::Partial);
		let step = std::mem::replace(e, LogicalExpr::Literal(LogicalLiteral::None));
		*e = LogicalExpr::Call(Box::new(LogicalCall {
			receiver: crate::expr::Function::Normal(ANCHOR.to_owned()),
			args: vec![step, LogicalExpr::Literal(LogicalLiteral::Bool(true))],
		}));
		return;
	}
	match e {
		LogicalExpr::Lookup {
			base,
			lookup,
		} => {
			lookup_in(lookup, role);
			expr(base, role.below());
			link_read(base, role.below());
		}
		// Steps that read through the value they are handed.
		LogicalExpr::Access {
			base,
			..
		}
		| LogicalExpr::Each(base) => {
			expr(base, role);
			link_read(base, role);
		}
		// Steps that keep the value.
		LogicalExpr::First(base)
		| LogicalExpr::Last(base)
		| LogicalExpr::Flatten {
			base,
			..
		}
		| LogicalExpr::RepeatRecurse(base) => expr(base, role),
		LogicalExpr::Index {
			base,
			index,
		} => {
			expr(base, role);
			expr(index, LookupTrace::None);
		}
		LogicalExpr::Filter {
			base,
			predicate,
		} => {
			expr(base, role);
			expr(predicate, LookupTrace::None);
		}
		LogicalExpr::Optional {
			base,
			then,
		}
		| LogicalExpr::Broadcast {
			base,
			body: then,
			..
		} => {
			expr(base, role);
			expr(then, role);
		}
		LogicalExpr::SetField {
			base,
			value,
			..
		} => {
			expr(base, role);
			expr(value, role);
		}
		LogicalExpr::Destructure {
			base,
			fields,
		} => {
			expr(base, role);
			link_read(base, role);
			destructure_in(fields, role);
		}
		LogicalExpr::Recurse(recurse) => {
			expr(&mut recurse.base, role);
			if let Some(path) = &mut recurse.path {
				expr(path.body_mut(), role);
			}
			if let Some(LogicalRecurseInstruction::Shortest {
				expects,
				..
			}) = &mut recurse.instruction
			{
				expr(expects, LookupTrace::None);
			}
		}
		LogicalExpr::Array(items) | LogicalExpr::Set(items) => {
			for item in items {
				expr(item, role);
			}
		}
		LogicalExpr::Object(entries) => {
			for (_, value) in entries {
				expr(value, role);
			}
		}
		LogicalExpr::If(branching) => {
			for (cond, value) in &mut branching.branches {
				expr(cond, LookupTrace::None);
				expr(value, role);
			}
			if let Some(otherwise) = &mut branching.otherwise {
				expr(otherwise, role);
			}
		}
		LogicalExpr::Subquery(op) => mark_op(op, role),
		LogicalExpr::RecordId(_) => {
			for key in e.children_mut() {
				expr(key, LookupTrace::None);
			}
		}
		// Anything that computes a new value from what it is handed.
		LogicalExpr::Method {
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
		| LogicalExpr::Throw(_) => {
			let role = role.changed();
			for child in e.children_mut() {
				expr(child, role);
			}
		}
		LogicalExpr::Literal(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Break
		| LogicalExpr::Continue => {}
	}
}
