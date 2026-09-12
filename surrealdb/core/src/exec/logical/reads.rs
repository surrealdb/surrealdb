//! What an expression reads off the row, as the rules and the physical
//! planner both need to know it.
//!
//! Two questions are answered here. Which declared position a path reads
//! ([`declared_read_prefix`], [`reads_at_or_beneath`], [`reads_beneath`]),
//! which is how a rule tells a conjunct that reads a layer's column from one
//! that reads only what the layer passes through. And whether an expression
//! reads the row other than by a field's name ([`reads_row_opaquely`]), in
//! which case what it reaches cannot be told from the expression and nothing
//! may move it past a node that sets columns on the row.

use std::ops::ControlFlow;

use super::effects::Oracle;
use crate::exec::function::FunctionRegistry;
use crate::expr::logical::{
	FieldPath, FieldSegment, LogicalExpr, LogicalFields, effects_of, invokes_unseen_body,
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
/// expression: `e(args)`, or a method whose name the registry does not
/// define, which calls a closure stored under that name on the receiver.
///
/// SECURITY: what such a call reads off the row cannot be told from the call.
/// `LET $pred = || secret = 'yes'` puts the body nowhere in `WHERE $pred()`,
/// and `LET $o = { check: || secret = 'yes' }` puts it nowhere in
/// `WHERE $o.check()`. Judged beneath a field-permission layer either would
/// read the stored `secret` and leak it through row membership, and the
/// resolver would emit no guard column for a field nothing appears to read.
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

/// The field each column of a layer's `fields` lands under, as a path of one
/// segment. Empty for a `VALUE` projection, which is never a layer.
pub(crate) fn layer_column_names(fields: &LogicalFields) -> Vec<FieldPath> {
	let LogicalFields::Select(fields) = fields else {
		return Vec::new();
	};
	fields
		.iter()
		.filter_map(|field| field.output_path())
		.map(|path| FieldPath::field(path.name()))
		.collect()
}
