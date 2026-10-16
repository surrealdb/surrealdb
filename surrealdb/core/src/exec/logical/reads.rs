//! What an expression reads off the row, as the binder's recogniser and the physical
//! planner both need to know it.
//!
//! Three questions are answered here. Which declared position a path reads
//! ([`declared_read_prefix`], [`reads_at_or_beneath`], [`reads_beneath`]),
//! which is how a conjunct that reads a layer's column is told from one
//! that reads only what the layer passes through. And whether an expression
//! reads the row other than by a field's name ([`reads_row_opaquely`]), in
//! which case what it reaches cannot be told from the expression and nothing
//! may move it past a node that sets columns on the row.
//!
//! A third concerns the layers of a table whose record id names its
//! components: the column that stands for a component is a read of one element
//! of the id ([`KeyColumn`]), so a read of the column is the same read made of
//! the id wherever it stands ([`substitute_record_key_parts`]).

use std::ops::ControlFlow;

use super::effects::Oracle;
use crate::exec::function::FunctionRegistry;
use crate::expr::logical::{
	FieldPath, FieldSegment, LogicalExpr, LogicalLiteral, effects_of, invokes_unseen_body,
};

/// Whether `expr` may read the row other than by a field's name: through a
/// row parameter — `$this`, `$self`, or `$parent`, which is this row at the
/// level of a statement no statement encloses — the document (`@`), a call
/// evaluated against the row, a nested plan, which may read the row as
/// `$parent`, or the invocation of a value — `e()`, `e.name(args)` — whose
/// body is not in the expression. What such a read reaches cannot be told
/// from the expression, so a node that sets columns on the row withholds it.
/// This is [`Effects::reads_row_opaquely`](crate::expr::logical::Effects).
///
/// SECURITY: [`invokes_an_unseen_body`] is what keeps a closure reached at
/// runtime above a field-permission layer; see the note there.
pub(crate) fn reads_row_opaquely(expr: &LogicalExpr, registry: &FunctionRegistry) -> bool {
	effects_of(expr, &Oracle::new(registry)).reads_row_opaquely
}

/// Whether `expr` itself invokes a value whose body is not in the
/// expression: `e(args)`; a method whose name the registry does not define,
/// which calls a closure stored under that name on the receiver; or a call
/// to a builtin that runs a closure it is handed — `array::any`,
/// `array::map`, `value::chain` and the rest declaring a closure position
/// through [`ScalarFunction::closure_args`](crate::exec::function::ScalarFunction::closure_args)
/// — with a value in that position that may be a closure not written in the
/// call.
///
/// SECURITY: what such a call reads off the row cannot be told from the call.
/// `LET $pred = || secret = 'yes'` puts the body nowhere in `WHERE $pred()`,
/// `LET $o = { check: || secret = 'yes' }` puts it nowhere in
/// `WHERE $o.check()`, and `LET $pred = |$x| secret = 'yes'` puts it nowhere
/// in `WHERE array::any([1], $pred)`, though the builtin runs it against the
/// row. Judged beneath a field-permission layer each would read the stored
/// `secret` and leak it through row membership, and the resolver would emit
/// no guard column for a field nothing appears to read. A closure written in
/// the call is not this: its body is in the expression, and its reads are
/// judged like any other child's. Nor is a value outside the closure
/// position: `array::any(tags, 'x')` reads `tags` by name and nothing else.
///
/// A registered name does not settle it either. A method with no
/// implementation for the receiver's type degrades to invoking a closure
/// stored in a field of that name, so `LET $o = { get: || secret = 'yes' }`
/// reaches a body through `$o.get()` — `get` being registered for files
/// alone. Only a name an *object* receiver dispatches for is exempt, an
/// object being the only value that can carry the field such a closure would
/// be stored in.
pub(crate) fn invokes_an_unseen_body(expr: &LogicalExpr, registry: &FunctionRegistry) -> bool {
	invokes_unseen_body(expr, &Oracle::new(registry))
}

/// Whether `expr` reads a field at or beneath one of `paths`. A nested
/// statement reads its own sources and is not entered.
pub(crate) fn reads_beneath(expr: &LogicalExpr, paths: &[FieldPath]) -> bool {
	expr.visit(&mut |e| {
		if paths.iter().any(|path| reads_at_or_beneath(e, path)) {
			ControlFlow::Break(())
		} else {
			ControlFlow::Continue(())
		}
	})
	.is_break()
}

/// Whether one of two paths lies at or beneath the other. A read or an index
/// column at `a` reaches every value at `b` when `b` extends `a`, and is
/// governed by `b` when `a` extends `b`; the restricted-field guards treat
/// either as touching.
pub(crate) fn paths_overlap<T: PartialEq>(a: &[T], b: &[T]) -> bool {
	a.starts_with(b) || b.starts_with(a)
}

/// Whether the path `expr` reads the declared field `field` or a position
/// beneath it: its [`declared_read_prefix`] starts with every segment of
/// `field`. `false` for anything that is not a path from the row's fields.
pub(crate) fn reads_at_or_beneath(expr: &LogicalExpr, field: &FieldPath) -> bool {
	declared_read_prefix(expr).is_some_and(|prefix| prefix.segments().starts_with(field.segments()))
}

/// The declared position a path reads before its first step that is not a
/// field read: the leading run of [`LogicalExpr::Field`],
/// [`LogicalExpr::Access`] and [`LogicalExpr::Each`] steps, followed through
/// a [`LogicalExpr::Broadcast`] into its body, where
/// [`LogicalExpr::Current`] stands for the position the base reached.
/// `a.b[0].c` and `a.b[WHERE x]` both read beneath `a.b`; the steps after the
/// index and the filter read positions the schema does not declare.
///
/// `None` when the path does not start from the row's fields — a parameter,
/// a start expression, `@`, the row itself under a leading lookup — or when
/// `expr` is not a path.
pub(crate) fn declared_read_prefix(expr: &LogicalExpr) -> Option<FieldPath> {
	read_prefix(expr, None).map(|(segments, _)| FieldPath::new(segments))
}

/// The segments a path has read so far and whether the next field read
/// extends them (`true` until a step that is not a field read closes the
/// prefix). `current` is the position [`LogicalExpr::Current`] stands for
/// inside a broadcast body; outside one the current value is unknown.
fn read_prefix(
	expr: &LogicalExpr,
	current: Option<&[FieldSegment]>,
) -> Option<(Vec<FieldSegment>, bool)> {
	match expr {
		LogicalExpr::Field(name) => Some((vec![FieldSegment::Field(name.clone())], true)),
		LogicalExpr::Current => current.map(|segments| (segments.to_vec(), true)),
		LogicalExpr::Access {
			base,
			field,
		} => {
			let (mut segments, open) = read_prefix(base, current)?;
			if open {
				segments.push(FieldSegment::Field(field.clone()));
			}
			Some((segments, open))
		}
		LogicalExpr::Each(base) => {
			let (mut segments, open) = read_prefix(base, current)?;
			if open {
				segments.push(FieldSegment::All);
			}
			Some((segments, open))
		}
		LogicalExpr::Broadcast {
			base,
			body,
			..
		} => {
			let (segments, open) = read_prefix(base, current)?;
			if !open {
				return Some((segments, false));
			}
			read_prefix(body, Some(&segments))
		}
		LogicalExpr::Index {
			base,
			..
		}
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
		} => read_prefix(base, current).map(|(segments, _)| (segments, false)),
		LogicalExpr::First(base) | LogicalExpr::Last(base) | LogicalExpr::RepeatRecurse(base) => {
			read_prefix(base, current).map(|(segments, _)| (segments, false))
		}
		LogicalExpr::Recurse(recurse) => {
			read_prefix(&recurse.base, current).map(|(segments, _)| (segments, false))
		}
		LogicalExpr::Doc
		| LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
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
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => None,
	}
}

/// A column of a layer that stands for one component of the record id.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct KeyColumn {
	/// The field the column lands under.
	pub(crate) name: String,
	/// What the column evaluates: a read of one element of the record id,
	/// which depends on the id alone.
	pub(crate) body: LogicalExpr,
}

/// `expr` with each read of a column in `columns` replaced by the expression the column
/// evaluates, for an expression evaluated against a record whose component fields are not
/// materialised, such as a `COUNT` index's guard: a bare read of a component, and a read of it
/// through the record (`$this.city`, `$self.city`, `@.city`, `$this['city']`, `$self['city']`), are
/// replaced. `@['city']` indexes the record id rather than reading the record, so it is left as
/// written.
pub(crate) fn substitute_record_key_parts(
	expr: &LogicalExpr,
	columns: &[KeyColumn],
) -> Option<LogicalExpr> {
	substitute(expr, columns, true)
}

fn substitute(
	expr: &LogicalExpr,
	columns: &[KeyColumn],
	through_record: bool,
) -> Option<LogicalExpr> {
	let is_the_record = |base: &LogicalExpr| {
		matches!(base, LogicalExpr::Doc)
			|| matches!(base, LogicalExpr::Param(param) if matches!(param.as_str(), "this" | "self"))
	};
	let column_of = |e: &LogicalExpr| -> Option<&KeyColumn> {
		let name = if let LogicalExpr::Field(name) = e {
			name.as_str()
		} else if let LogicalExpr::Access {
			base,
			field,
		} = e && through_record
			&& is_the_record(base)
		{
			field.as_str()
		} else if let LogicalExpr::Index {
			base,
			index,
		} = e && through_record
			&& matches!(base.as_ref(), LogicalExpr::Param(param) if matches!(param.as_str(), "this" | "self"))
			&& let LogicalExpr::Literal(LogicalLiteral::String(field)) = index.as_ref()
		{
			field.as_str()
		} else {
			return None;
		};
		columns.iter().find(|c| c.name == name)
	};
	if columns.is_empty() || !expr.any(|e| column_of(e).is_some()) {
		return None;
	}
	let mut out = expr.clone();
	let mut changed = false;
	rewrite_positions(&mut out, true, &mut |e, at_row| {
		if at_row && let Some(column) = column_of(e) {
			*e = column.body.clone();
			changed = true;
		}
	});
	changed.then_some(out)
}

/// The index of the first input of `expr` that is a *body*: an expression
/// evaluated against a value other than the one `expr` itself is evaluated
/// against — the element a broadcast maps over, the element a `[WHERE]`
/// filter judges, the value an optional continues from, each depth of a
/// recursion, the value a destructure alias reads, the edge rows a lookup's
/// clauses see, a closure's arguments. `None` when every input is evaluated
/// against the same value as `expr`. Indexes are into
/// [`LogicalExpr::children`].
fn body_inputs_from(expr: &LogicalExpr) -> Option<usize> {
	match expr {
		LogicalExpr::Filter {
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
		| LogicalExpr::Broadcast {
			..
		} => Some(1),
		LogicalExpr::Closure(_) => Some(0),
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
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
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::RepeatRecurse(_)
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
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => None,
	}
}

/// Rewrites `expr` in place, bottom-up, telling `f` at each position whether
/// it is evaluated against the same value as `expr` itself; see
/// [`visit_positions`]. A nested plan is not entered, and a closure body is
/// left as written.
fn rewrite_positions(
	expr: &mut LogicalExpr,
	at_row: bool,
	f: &mut impl FnMut(&mut LogicalExpr, bool),
) {
	if !matches!(expr, LogicalExpr::Closure(_)) {
		let bodies_from = body_inputs_from(expr);
		for (i, child) in expr.children_mut().into_iter().enumerate() {
			let child_at_row = at_row && bodies_from.is_none_or(|from| i < from);
			rewrite_positions(child, child_at_row, f);
		}
	}
	f(expr, at_row);
}
