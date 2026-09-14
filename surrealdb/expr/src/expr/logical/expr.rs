//! The scalar type of the logical plan.
//!
//! [`LogicalExpr`] is what an operator evaluates per row: a literal, a field
//! read, a step of a path, an operator application, a call, a closure, an `IF …
//! THEN … ELSE … END`, or a statement in scalar position. It holds scalars
//! only. Every statement kind is a [`LogicalOp`], and the one way a statement
//! appears inside a scalar is [`LogicalExpr::Subquery`]: a `SELECT` in a
//! predicate, a block in a projection, a `RETURN` as a value. Nothing
//! statement-shaped is re-encoded as a scalar variant, so there is exactly one
//! form for each construct and a pass that understands operators understands
//! every nested statement too.
//!
//! # Paths are expressions
//!
//! A surface idiom — `a.b[0].c`, `arr[WHERE x > 1]`, `a.len()`, `v.{ x, y }`,
//! `->likes->post` — is a composition of ordinary forms rather than a sequence
//! of parts walked by a dedicated evaluator. [`LogicalExpr::Field`] reads a
//! field of the row; every other step ([`LogicalExpr::Access`],
//! [`LogicalExpr::Index`], [`LogicalExpr::Filter`], [`LogicalExpr::Lookup`],
//! …) applies to the value of its `base`. The one rule a path carries that a
//! plain expression tree does not — a field read, `.*` or destructure applied
//! to an array applies the whole rest of the path to each element — is made
//! explicit by [`LogicalExpr::Broadcast`], whose `body` is applied to
//! [`LogicalExpr::Current`], the element. The rules that place a `Broadcast`
//! are static functions of the sequence of steps and live in lowering
//! (`lower.rs`); this module defines the forms, their traversal, their naming
//! and their rendering.
//!
//! The leaf types that carry no expression — [`Param`], [`TableName`],
//! [`Constant`], [`Kind`], the operator enums, [`Function`], [`LookupKind`],
//! [`Recurse`], [`RecordIdKeyGen`] and the `val` value types — are shared with
//! the expression layer. Everything that holds an expression in that layer has
//! a mirror here holding a `LogicalExpr`: lookups, record-id literals, field
//! lists, orderings, closures and calls.
//!
//! # Rendering
//!
//! A scalar renders as SurrealQL through the same helpers the expression layer
//! uses, so a predicate prints identically whichever type holds it, and a
//! lowered path renders as the idiom it came from: a body's `Current` renders
//! as nothing and the step above it renders with its normal prefix. A
//! [`LogicalExpr::Subquery`] renders as the plan it holds, in parentheses over
//! several lines; the enclosing operator re-indents those lines beneath itself.
//! A rendered plan is deterministic text for golden tests and caches; it is not
//! reparseable SurrealQL, and nothing needs it to be.
//!
//! # Traversal
//!
//! [`LogicalExpr::children`] lists every `LogicalExpr` nested directly in a
//! scalar — the base and the operands of a step, lookup clauses, calls,
//! closures, branches, array and object elements and record-id keys — and
//! [`LogicalExpr::visit`], [`LogicalExpr::visit_mut`] and
//! [`LogicalExpr::transform`] are built on it. A `Subquery` is a leaf to these:
//! the plan inside it is walked by [`LogicalOp`]'s own traversal, and a pass
//! that wants both descends explicitly when it meets the variant.

use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::{Bound, ControlFlow};

use common::fmt::{EscapeIdent, EscapeKwFreeIdent, EscapeObjectKey, EscapeRidKey, Fmt, QuoteStr};
use rust_decimal::Decimal;
use surrealdb_strand::{Strand, TableName};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::{EvalFrame, LogicalOp};
use crate::expr::lookup::LookupKind;
use crate::expr::method::is_builtin_method;
use crate::expr::part::Recurse;
use crate::expr::record_id::RecordIdKeyGen;
use crate::expr::{BinaryOperator, Constant, Function, Kind, Literal, Mock, Param, PrefixOperator};
use crate::sql::operator::BindingPower;
use crate::val::{Bytes, Datetime, Duration, File, Geometry, Regex, Uuid};

/// A leaf literal: a value that needs no evaluation and holds no expression.
///
/// Equality and hashing are by representation, as for the expression layer's
/// literal: two floats are equal when their bits are, so `NaN` equals itself
/// and `-0f` does not equal `0f`.
#[derive(Clone, Debug)]
pub enum LogicalLiteral {
	None,
	Null,
	/// `..` with neither bound.
	UnboundedRange,
	Bool(bool),
	Float(f64),
	Integer(i64),
	Decimal(Decimal),
	String(Strand),
	Bytes(Bytes),
	Regex(Regex),
	Duration(Duration),
	Datetime(Datetime),
	Uuid(Uuid),
	Geometry(Geometry),
	File(File),
}

impl LogicalLiteral {
	/// The expression-layer literal with the same value, for rendering through
	/// the shared formatter.
	fn to_expr_literal(&self) -> Literal {
		match self {
			LogicalLiteral::None => Literal::None,
			LogicalLiteral::Null => Literal::Null,
			LogicalLiteral::UnboundedRange => Literal::UnboundedRange,
			LogicalLiteral::Bool(x) => Literal::Bool(*x),
			LogicalLiteral::Float(x) => Literal::Float(*x),
			LogicalLiteral::Integer(x) => Literal::Integer(*x),
			LogicalLiteral::Decimal(x) => Literal::Decimal(*x),
			LogicalLiteral::String(x) => Literal::String(x.clone()),
			LogicalLiteral::Bytes(x) => Literal::Bytes(x.clone()),
			LogicalLiteral::Regex(x) => Literal::Regex(x.clone()),
			LogicalLiteral::Duration(x) => Literal::Duration(*x),
			LogicalLiteral::Datetime(x) => Literal::Datetime(*x),
			LogicalLiteral::Uuid(x) => Literal::Uuid(*x),
			LogicalLiteral::Geometry(x) => Literal::Geometry(x.clone()),
			LogicalLiteral::File(x) => Literal::File(x.clone()),
		}
	}
}

impl PartialEq for LogicalLiteral {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(LogicalLiteral::None, LogicalLiteral::None) => true,
			(LogicalLiteral::Null, LogicalLiteral::Null) => true,
			(LogicalLiteral::UnboundedRange, LogicalLiteral::UnboundedRange) => true,
			(LogicalLiteral::Bool(a), LogicalLiteral::Bool(b)) => a == b,
			(LogicalLiteral::Float(a), LogicalLiteral::Float(b)) => a.to_bits() == b.to_bits(),
			(LogicalLiteral::Integer(a), LogicalLiteral::Integer(b)) => a == b,
			(LogicalLiteral::Decimal(a), LogicalLiteral::Decimal(b)) => a == b,
			(LogicalLiteral::String(a), LogicalLiteral::String(b)) => a == b,
			(LogicalLiteral::Bytes(a), LogicalLiteral::Bytes(b)) => a == b,
			(LogicalLiteral::Regex(a), LogicalLiteral::Regex(b)) => a == b,
			(LogicalLiteral::Duration(a), LogicalLiteral::Duration(b)) => a == b,
			(LogicalLiteral::Datetime(a), LogicalLiteral::Datetime(b)) => a == b,
			(LogicalLiteral::Uuid(a), LogicalLiteral::Uuid(b)) => a == b,
			(LogicalLiteral::Geometry(a), LogicalLiteral::Geometry(b)) => a == b,
			(LogicalLiteral::File(a), LogicalLiteral::File(b)) => a == b,
			// Mismatched variants only. Every variant has an arm above; the
			// `every_literal_variant_equals_itself` test is what keeps that
			// true, since a variant that fell through here would compare
			// unequal to itself and silently break structural comparison.
			_ => false,
		}
	}
}

impl Eq for LogicalLiteral {}

impl Hash for LogicalLiteral {
	fn hash<H: Hasher>(&self, state: &mut H) {
		std::mem::discriminant(self).hash(state);
		match self {
			LogicalLiteral::None | LogicalLiteral::Null | LogicalLiteral::UnboundedRange => {}
			LogicalLiteral::Bool(x) => x.hash(state),
			LogicalLiteral::Float(x) => x.to_bits().hash(state),
			LogicalLiteral::Integer(x) => x.hash(state),
			LogicalLiteral::Decimal(x) => x.hash(state),
			LogicalLiteral::String(x) => x.hash(state),
			LogicalLiteral::Bytes(x) => x.hash(state),
			LogicalLiteral::Regex(x) => x.hash(state),
			LogicalLiteral::Duration(x) => x.hash(state),
			LogicalLiteral::Datetime(x) => x.hash(state),
			LogicalLiteral::Uuid(x) => x.hash(state),
			LogicalLiteral::Geometry(x) => x.hash(state),
			LogicalLiteral::File(x) => x.hash(state),
		}
	}
}

impl ToSql for LogicalLiteral {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_expr_literal().fmt_sql(f, fmt);
	}
}

/// A scalar expression of the logical plan. See the [module docs](self).
///
/// # Path forms
///
/// The forms from [`Self::Field`] to [`Self::Current`] compose a path. Every
/// *step* form holds the expression it applies to as `base`; a step whose base
/// is [`Self::Current`] is the innermost step of a body. Three forms hold a
/// body over `Current`: [`Self::Broadcast`], [`Self::Optional`] and
/// [`Self::Recurse`]; a destructure alias
/// ([`LogicalDestructureField::Aliased`]) is one as well.
///
/// The evaluation of each step is that of the corresponding physical part; the
/// contract each one upholds is stated on the variant. A path renders as the
/// idiom it was lowered from ([`ToSql`]), names its output through
/// [`Self::output_path`], and declares the position it reads through
/// [`Self::field_path`](super::schema).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalExpr {
	Literal(LogicalLiteral),
	/// `[a, b]`. Flattened out of the literal because its elements are
	/// expressions.
	Array(Vec<LogicalExpr>),
	/// `{a, b}`, a set literal.
	Set(Vec<LogicalExpr>),
	/// `{ k: v }`, in source order.
	Object(Vec<(String, LogicalExpr)>),
	/// `table:key`, whose key may hold expressions.
	RecordId(Box<LogicalRecordIdLit>),
	Param(Param),
	Table(TableName),
	Constant(Constant),
	/// `a`: a read of the row's field. The root of a path into the row, and
	/// the only form index analysis and schema typing need at the root.
	Field(Strand),
	/// `@`: the record id of the current row, as the leading part of an
	/// idiom. Evaluates as `Field("id")` does; kept distinct so that the path
	/// renders as written.
	Doc,
	/// The value a body is applied to: the element a [`Self::Broadcast`] maps
	/// over, the value an [`Self::Optional`] guards, the value at each depth
	/// of a [`Self::Recurse`], the value a destructure alias reads from. Also
	/// the row itself at the root of a path that starts with a lookup
	/// (`->likes->person`). Renders as nothing.
	Current,
	/// `v.f`: the field `f` of `base`. On an object, the field; on a record
	/// id, the field of the fetched record; on an array, the access mapped
	/// over the elements; on a geometry, its GeoJSON field. A mapping step.
	Access {
		base: Box<LogicalExpr>,
		field: Strand,
	},
	/// `v[e]`: `base` indexed by the value of `index`, which is evaluated
	/// against the row, not against `base`.
	Index {
		base: Box<LogicalExpr>,
		index: Box<LogicalExpr>,
	},
	/// `v[0]`: the first element of an array or set; any other value passes
	/// through.
	First(Box<LogicalExpr>),
	/// `v[$]`: the last element of an array or set; any other value passes
	/// through.
	Last(Box<LogicalExpr>),
	/// `v.*`: every element. A record id is fetched; an array has each
	/// record-id element fetched and every other element kept; an object
	/// passes through; anything else is `NONE`. A mapping step.
	Each(Box<LogicalExpr>),
	/// `v…`: one level of nesting removed from an array; any other value
	/// passes through. `explicit` is `false` for the flatten lowering places
	/// between a lookup and a following [`Self::Filter`], which renders as
	/// nothing; the written `…` renders as itself.
	Flatten {
		base: Box<LogicalExpr>,
		explicit: bool,
	},
	/// `v[WHERE p]`: the elements of an array or set for which `predicate` is
	/// truthy, evaluated with the element as the current value and the row as
	/// `$parent`. Anything that is not a collection yields `NONE`.
	Filter {
		base: Box<LogicalExpr>,
		predicate: Box<LogicalExpr>,
	},
	/// `v.name(args)`: a method on `base`. A registered name dispatches on
	/// the receiver's type; an unregistered one calls a closure stored under
	/// that name on the receiver. The arguments are evaluated against the row.
	Method {
		base: Box<LogicalExpr>,
		name: Strand,
		args: Vec<LogicalExpr>,
	},
	/// `v.{ … }`: an object of the listed fields of `base`. A record id is
	/// fetched first; an array has the destructure mapped over its elements.
	/// A mapping step.
	Destructure {
		base: Box<LogicalExpr>,
		fields: Vec<LogicalDestructureField>,
	},
	/// `v.?…`: `NONE` or `NULL` as `base` is the result; otherwise `then` is
	/// evaluated with `base` as [`Self::Current`]. `then` is the rest of the
	/// path, and is `Current` itself for a trailing `.?`.
	Optional {
		base: Box<LogicalExpr>,
		then: Box<LogicalExpr>,
	},
	/// `v->e`, `v<-e`, `v<~e`: one hop of a graph or reference lookup from the
	/// record ids in `base`. Consecutive hops nest, and the physical planner
	/// fuses a nested chain into one operator pipeline.
	Lookup {
		base: Box<LogicalExpr>,
		lookup: Box<LogicalLookup>,
	},
	/// `v.{n}(body)` or `v.{n}.body`: a fixpoint over a body. See
	/// [`LogicalRecurse`].
	Recurse(Box<LogicalRecurse>),
	/// `v.@`: the marker inside a recursion body at which the recursion
	/// re-enters. A step on the value the body has produced so far.
	RepeatRecurse(Box<LogicalExpr>),
	/// The array-continuation rule made explicit. When `base` is an array,
	/// `body` is evaluated once per element with the element as
	/// [`Self::Current`], and the results form the array; when `base` is
	/// anything else, `body` is evaluated once with `base` as `Current`.
	/// `flatten` asks for the per-element results to be flattened one level,
	/// and applies only when `base` is an array.
	///
	/// A `Broadcast` nested inside the body of a `Broadcast` that is mapping
	/// does not map again: it evaluates its base, then its body against that
	/// value. The rest of a path applied to one element runs as a plain
	/// sequence of steps, as the idiom evaluator's per-element loop did.
	/// Likewise an [`Self::Each`] directly on `Current` at the start of a
	/// mapped body fetches a record-id element and passes any other element
	/// through unchanged. No syntax renders: a `Broadcast` renders as its base
	/// followed by its body.
	Broadcast {
		base: Box<LogicalExpr>,
		body: Box<LogicalExpr>,
		flatten: bool,
	},
	/// The object `base` with `field` set to `value`. A `NONE` value removes
	/// the key; a `base` that is not an object is returned unchanged. Other
	/// keys and, in a value the form is applied to per element, array indices
	/// are untouched: the form replaces one key and never shifts anything.
	///
	/// Not a path form: it is a rebuilt value, not a read of one, so it has
	/// no [`Self::step_base`] and no declared position. It has no surface
	/// syntax; a plan renders it as `set_field(base, "field", value)`.
	SetField {
		base: Box<LogicalExpr>,
		field: Strand,
		value: Box<LogicalExpr>,
	},
	Prefix {
		op: PrefixOperator,
		expr: Box<LogicalExpr>,
	},
	/// `v..` or `v>..`: an open-ended range from `expr`. The one postfix
	/// operator form: a call written after an expression is [`Self::Invoke`],
	/// and a method written after one is [`Self::Method`].
	Postfix {
		expr: Box<LogicalExpr>,
		op: LogicalPostfixOperator,
	},
	/// `v(args)`: `target`, evaluated to a closure value, applied to `args`.
	/// The body is whatever the closure holds and is not in the expression
	/// unless `target` is a [`Self::Closure`] literal. Renders with the target
	/// parenthesised, `(v)(args)`.
	Invoke {
		target: Box<LogicalExpr>,
		args: Vec<LogicalExpr>,
	},
	Binary {
		left: Box<LogicalExpr>,
		op: BinaryOperator,
		right: Box<LogicalExpr>,
	},
	Call(Box<LogicalCall>),
	Closure(Box<LogicalClosure>),
	/// `IF … THEN … ELSE … END` as a value. The one encoding of `IF`: a
	/// top-level `IF` statement is [`LogicalOp::Eval`] of one of these.
	If(Box<LogicalIf>),
	Throw(Box<LogicalExpr>),
	Break,
	Continue,
	/// Any statement in scalar position.
	Subquery(Box<LogicalOp>),
}

/// A record-id literal. The key may hold expressions.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalRecordIdLit {
	pub table: TableName,
	pub key: LogicalRecordIdKey,
}

/// The key of a [`LogicalRecordIdLit`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalRecordIdKey {
	Number(i64),
	String(Strand),
	Uuid(Uuid),
	Array(Vec<LogicalExpr>),
	Object(Vec<(String, LogicalExpr)>),
	Generate(RecordIdKeyGen),
	Range(Box<LogicalRecordIdKeyRange>),
}

/// A record-id key range, `table:start..end`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalRecordIdKeyRange {
	pub start: Bound<LogicalRecordIdKey>,
	pub end: Bound<LogicalRecordIdKey>,
}

/// `v.{n}(body)`, `v.{n+instruction}.body`: a recursion.
///
/// The body is evaluated repeatedly from `base`, with the value of each depth
/// as [`LogicalExpr::Current`] for the next, between the depth bounds of
/// `recurse`; `instruction` chooses what is collected. The body itself is a
/// plain sequence of steps: no [`LogicalExpr::Broadcast`] appears inside it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalRecurse {
	pub base: LogicalExpr,
	pub recurse: Recurse,
	/// `None` for `v.{n}` with nothing after it: each depth is the value
	/// itself.
	pub path: Option<LogicalRecursePath>,
	pub instruction: Option<LogicalRecurseInstruction>,
}

/// The body of a [`LogicalRecurse`], and how it was written. Both evaluate
/// the same way; they render differently.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalRecursePath {
	/// `v.{n}(body)`: the body in parentheses. The steps after `)` apply once
	/// to the recursion's result, so they wrap the [`LogicalExpr::Recurse`].
	Nested(LogicalExpr),
	/// `v.{n}.body`: the rest of the path is the body, up to and including a
	/// [`LogicalExpr::RepeatRecurse`] (`.@`) at the body's top level. The
	/// steps after `.@` wrap the [`LogicalExpr::Recurse`].
	Trailing(LogicalExpr),
}

impl LogicalRecursePath {
	/// The body, however it was written.
	pub fn body(&self) -> &LogicalExpr {
		match self {
			LogicalRecursePath::Nested(body) | LogicalRecursePath::Trailing(body) => body,
		}
	}

	/// [`Self::body`], mutably.
	pub fn body_mut(&mut self) -> &mut LogicalExpr {
		match self {
			LogicalRecursePath::Nested(body) | LogicalRecursePath::Trailing(body) => body,
		}
	}
}

/// One entry of a `.{ … }` destructure.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalDestructureField {
	/// `f.*`: the field, with a record id fetched.
	All(Strand),
	/// `f`: the field.
	Field(Strand),
	/// `name: body`: `body` evaluated with the destructured value as
	/// [`LogicalExpr::Current`], under `name`. The body is a plain sequence of
	/// steps, or any expression when it was not written as a path.
	Aliased(Strand, LogicalExpr),
	/// `f.{ … }`: a nested destructure of the field.
	Destructure(Strand, Vec<LogicalDestructureField>),
}

/// The `+instruction` of a `.{n+instruction}` recursion.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalRecurseInstruction {
	Path {
		inclusive: bool,
	},
	Collect {
		inclusive: bool,
	},
	Shortest {
		expects: LogicalExpr,
		inclusive: bool,
	},
}

/// A graph or reference lookup, `->edge`, `<-edge`, `<~ref`, with its clauses.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalLookup {
	pub kind: LookupKind,
	pub expr: Option<LogicalFields>,
	pub only: bool,
	pub what: Vec<LogicalLookupSubject>,
	pub cond: Option<LogicalExpr>,
	pub split: Option<Vec<LogicalExpr>>,
	pub group: Option<Vec<LogicalExpr>>,
	pub order: Option<LogicalOrdering>,
	pub limit: Option<LogicalExpr>,
	pub start: Option<LogicalExpr>,
	/// `AS name`: where the lookup's value lands in the output, which is also
	/// the name of any path containing this lookup.
	pub alias: Option<OutputPath>,
}

/// What a [`LogicalLookup`] scans: a whole table or a key range on it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalLookupSubject {
	Table {
		table: TableName,
		referencing_field: Option<String>,
	},
	Range {
		table: TableName,
		range: LogicalRecordIdKeyRange,
		referencing_field: Option<String>,
	},
}

impl LogicalLookupSubject {
	fn referencing_field(&self) -> Option<&String> {
		match self {
			LogicalLookupSubject::Table {
				referencing_field,
				..
			}
			| LogicalLookupSubject::Range {
				referencing_field,
				..
			} => referencing_field.as_ref(),
		}
	}
}

/// A postfix operator: the two open-ended range forms. A method or closure
/// call written after an expression lowers to [`LogicalExpr::Method`] or
/// [`LogicalExpr::Invoke`] rather than to an operator, so every pass meets
/// one node per construct whichever way it was spelled.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalPostfixOperator {
	/// `a..`
	Range,
	/// `a>..`
	RangeSkip,
}

/// A function call. The receiver names a builtin, a user function, a script,
/// a model, a module or a silo package; none of those holds an expression.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalCall {
	pub receiver: Function,
	pub args: Vec<LogicalExpr>,
}

/// A closure literal, `|$a: int| -> int $a + 1`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalClosure {
	pub args: Vec<(Param, Kind)>,
	pub returns: Option<Kind>,
	pub body: LogicalExpr,
}

/// `IF c1 THEN v1 ELSE IF c2 THEN v2 ELSE v3 END`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalIf {
	/// `(condition, value)` pairs, in order.
	pub branches: Vec<(LogicalExpr, LogicalExpr)>,
	pub otherwise: Option<LogicalExpr>,
}

/// A projection list: `SELECT VALUE e` or `SELECT a, b AS c, *`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalFields {
	/// `VALUE e`: each row is the bare value. The alias names the value for
	/// `GROUP BY` and is not projected.
	Value {
		expr: LogicalExpr,
		alias: Option<OutputPath>,
	},
	/// An object per row with one entry per field.
	Select(Vec<LogicalField>),
}

impl LogicalFields {
	/// The `*` projection.
	pub fn all() -> Self {
		LogicalFields::Select(vec![LogicalField::All])
	}

	/// Whether the projection includes `*`.
	pub fn has_all_selection(&self) -> bool {
		match self {
			LogicalFields::Value {
				..
			} => false,
			LogicalFields::Select(fields) => fields.iter().any(|f| matches!(f, LogicalField::All)),
		}
	}
}

/// One entry of a [`LogicalFields::Select`].
///
/// `Single` is left unboxed: `*` is a marker a projection list holds at most a
/// few times among its fields, so the padding it costs is per `*`, while
/// boxing the payload would put an indirection on every read of every
/// projected field. The variant exceeds the lint's size threshold only with
/// 64-bit pointers, so the expectation is scoped to them.
#[cfg_attr(target_pointer_width = "64", expect(clippy::large_enum_variant))]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalField {
	/// `*`
	All,
	/// `expr` or `expr AS alias`.
	///
	/// The alias is where the value lands. Written by the user as `AS`, or
	/// set at lowering to the SurrealQL text of `expr` as written when the
	/// name [`LogicalExpr::output_path`] would derive is spelled from a
	/// nested plan ([`LogicalExpr::output_name_renders_a_statement`]). A
	/// consumer treats the two alike: an alias and a derived name are one
	/// thing to a projection.
	///
	/// `kind` and `frame` say how the value is produced. A field the user
	/// wrote has no kind and the caller's frame ([`LogicalField::user`]). A
	/// *column* of a projection layer (see [`LogicalOp::is_layer`]) carries
	/// a frame that is not the caller's: a `COMPUTED` field body evaluates in
	/// [`EvalMode::Computed`](super::EvalMode::Computed) under the definer's
	/// limit and carries the field's declared kind, to which the value is
	/// coerced wherever it is derived; a field `PERMISSIONS FOR select` guard
	/// evaluates in [`EvalMode::Permission`](super::EvalMode::Permission)
	/// and carries no kind, since the value it yields is a stored field as
	/// stored, or `NONE`, and a read never coerces a stored value.
	Single {
		expr: LogicalExpr,
		alias: Option<OutputPath>,
		/// The declared kind the value is coerced to; `None` leaves the value
		/// as `expr` produced it.
		kind: Option<Kind>,
		/// Who evaluates `expr`.
		frame: EvalFrame,
	},
}

impl LogicalField {
	/// A field the user wrote: `expr` or `expr AS alias`, evaluated as the
	/// caller with no coercion.
	pub fn user(expr: LogicalExpr, alias: Option<OutputPath>) -> Self {
		LogicalField::Single {
			expr,
			alias,
			kind: None,
			frame: EvalFrame::caller(),
		}
	}

	/// A column of a projection layer: `expr` landing on the row under
	/// `name`, coerced to `kind` when one is given, evaluated in `frame`.
	pub fn column(
		name: impl Into<String>,
		expr: LogicalExpr,
		kind: Option<Kind>,
		frame: EvalFrame,
	) -> Self {
		LogicalField::Single {
			expr,
			alias: Some(OutputPath::field(name)),
			kind,
			frame,
		}
	}

	/// The name a projected field lands under: its alias, or the name
	/// [`LogicalExpr::output_path`] derives. `None` for `*`.
	pub fn output_path(&self) -> Option<OutputPath> {
		match self {
			LogicalField::All => None,
			LogicalField::Single {
				expr,
				alias,
				..
			} => Some(alias.clone().unwrap_or_else(|| expr.output_path())),
		}
	}
}

/// `ORDER BY`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalOrdering {
	Random,
	Order(Vec<LogicalOrder>),
}

/// One `ORDER BY` key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalOrder {
	pub value: LogicalExpr,
	pub collate: bool,
	pub numeric: bool,
	/// `true` for ascending.
	pub direction: bool,
}

/// What a [`LogicalOp::Source`] reads.
///
/// A source is not a scalar: choosing an access path for it is the physical
/// planner's job, and the syntactic classification here is what it chooses
/// from. [`SourceRef::classify`] is the one place that classification lives.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SourceRef {
	/// A whole table.
	Table(TableName),
	/// One record, or a key range on a table.
	RecordId(Box<LogicalRecordIdLit>),
	/// A generated record set, `|t:10|`.
	Mock(Mock),
	/// A parameter resolved to a source when the statement runs.
	Param(Param),
	/// Any other expression, resolved to a source when the statement runs.
	Dynamic(LogicalExpr),
}

impl SourceRef {
	/// Classifies a `FROM` expression by its syntax. A pass that rewrites a
	/// source's expression re-applies this so the physical planner sees the
	/// rewritten form's access path rather than the original's.
	pub fn classify(expr: LogicalExpr) -> Self {
		match expr {
			LogicalExpr::Table(table) => SourceRef::Table(table),
			LogicalExpr::RecordId(rid) => SourceRef::RecordId(rid),
			LogicalExpr::Param(param) => SourceRef::Param(param),
			LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
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
			| LogicalExpr::Subquery(_) => SourceRef::Dynamic(expr),
		}
	}

	/// Every expression the source holds. A table, mock or parameter holds
	/// none; a record id holds those in its key; a dynamic source is one.
	pub fn for_each_expr<'a>(&'a self, out: &mut dyn FnMut(&'a LogicalExpr)) {
		match self {
			SourceRef::Table(_) | SourceRef::Mock(_) | SourceRef::Param(_) => {}
			SourceRef::RecordId(rid) => rid.key.for_each_expr(out),
			SourceRef::Dynamic(expr) => out(expr),
		}
	}

	/// [`Self::for_each_expr`], collected.
	pub fn exprs<'a>(&'a self, out: &mut Vec<&'a LogicalExpr>) {
		self.for_each_expr(&mut |expr| out.push(expr));
	}

	/// [`Self::exprs`], mutably.
	pub fn exprs_mut<'a>(&'a mut self, out: &mut Vec<&'a mut LogicalExpr>) {
		match self {
			SourceRef::Table(_) | SourceRef::Mock(_) | SourceRef::Param(_) => {}
			SourceRef::RecordId(rid) => rid.key.exprs_mut(out),
			SourceRef::Dynamic(expr) => out.push(expr),
		}
	}

	/// Whether resolving the source only reads.
	pub fn read_only(&self) -> bool {
		match self {
			SourceRef::Table(_) | SourceRef::Mock(_) | SourceRef::Param(_) => true,
			SourceRef::RecordId(rid) => rid.read_only(),
			SourceRef::Dynamic(expr) => expr.read_only(),
		}
	}
}

// ---------------------------------------------------------------------------
// Path structure
// ---------------------------------------------------------------------------

impl LogicalExpr {
	/// Whether this is a path form: a root ([`Self::Field`], [`Self::Doc`],
	/// [`Self::Current`]), a step, or a [`Self::Broadcast`].
	pub fn is_path(&self) -> bool {
		matches!(
			self,
			LogicalExpr::Field(_)
				| LogicalExpr::Doc
				| LogicalExpr::Current
				| LogicalExpr::Access { .. }
				| LogicalExpr::Index { .. }
				| LogicalExpr::First(_)
				| LogicalExpr::Last(_)
				| LogicalExpr::Each(_)
				| LogicalExpr::Flatten { .. }
				| LogicalExpr::Filter { .. }
				| LogicalExpr::Method { .. }
				| LogicalExpr::Destructure { .. }
				| LogicalExpr::Optional { .. }
				| LogicalExpr::Lookup { .. }
				| LogicalExpr::Recurse(_)
				| LogicalExpr::RepeatRecurse(_)
				| LogicalExpr::Broadcast { .. }
		)
	}

	/// The expression a step applies to: the `base` of every step form and of
	/// a [`Self::Broadcast`]. `None` for a path root and for anything that is
	/// not a path.
	pub fn step_base(&self) -> Option<&LogicalExpr> {
		match self {
			LogicalExpr::Access {
				base,
				..
			}
			| LogicalExpr::Index {
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
			}
			| LogicalExpr::Broadcast {
				base,
				..
			} => Some(base),
			LogicalExpr::First(base)
			| LogicalExpr::Last(base)
			| LogicalExpr::Each(base)
			| LogicalExpr::RepeatRecurse(base) => Some(base),
			LogicalExpr::Recurse(recurse) => Some(&recurse.base),
			LogicalExpr::Field(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
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
			| LogicalExpr::Subquery(_) => None,
		}
	}

	/// The expression a path starts from, following `base` through every step
	/// and [`Self::Broadcast`]: a [`Self::Field`], [`Self::Doc`] or
	/// [`Self::Current`] root, or the expression a `(e).rest` path began with
	/// (`$param.x` starts from the parameter). An expression that is not a
	/// step is its own root.
	pub fn path_root(&self) -> &LogicalExpr {
		let mut current = self;
		while let Some(base) = current.step_base() {
			current = base;
		}
		current
	}
}

// ---------------------------------------------------------------------------
// Traversal
// ---------------------------------------------------------------------------

impl LogicalExpr {
	/// Every `LogicalExpr` nested directly in this one, in source order,
	/// including the base and operands of a step, bodies, lookups, record-id
	/// keys, calls, closures, branches and collection elements. A
	/// [`Self::Subquery`] has no children here: its plan is walked by
	/// [`LogicalOp`].
	pub fn for_each_child<'a>(&'a self, out: &mut dyn FnMut(&'a LogicalExpr)) {
		match self {
			LogicalExpr::Literal(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Field(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_) => {}
			LogicalExpr::Array(items) | LogicalExpr::Set(items) => {
				for e in items {
					out(e);
				}
			}
			LogicalExpr::Object(entries) => {
				for e in entries.iter().map(|(_, v)| v) {
					out(e);
				}
			}
			LogicalExpr::RecordId(rid) => rid.key.for_each_expr(out),
			LogicalExpr::Access {
				base,
				..
			}
			| LogicalExpr::Flatten {
				base,
				..
			} => out(base),
			LogicalExpr::First(base)
			| LogicalExpr::Last(base)
			| LogicalExpr::Each(base)
			| LogicalExpr::RepeatRecurse(base) => out(base),
			LogicalExpr::Index {
				base,
				index,
			} => {
				out(base);
				out(index);
			}
			LogicalExpr::Filter {
				base,
				predicate,
			} => {
				out(base);
				out(predicate);
			}
			LogicalExpr::Method {
				base,
				args,
				..
			} => {
				out(base);
				for e in args {
					out(e);
				}
			}
			LogicalExpr::Destructure {
				base,
				fields,
			} => {
				out(base);
				for field in fields {
					field.for_each_expr(out);
				}
			}
			LogicalExpr::Optional {
				base,
				then,
			} => {
				out(base);
				out(then);
			}
			LogicalExpr::Lookup {
				base,
				lookup,
			} => {
				out(base);
				lookup.for_each_expr(out);
			}
			LogicalExpr::Recurse(recurse) => {
				out(&recurse.base);
				if let Some(path) = &recurse.path {
					out(path.body());
				}
				if let Some(LogicalRecurseInstruction::Shortest {
					expects,
					..
				}) = &recurse.instruction
				{
					out(expects);
				}
			}
			LogicalExpr::Broadcast {
				base,
				body,
				..
			} => {
				out(base);
				out(body);
			}
			LogicalExpr::SetField {
				base,
				value,
				..
			} => {
				out(base);
				out(value);
			}
			LogicalExpr::Prefix {
				expr,
				..
			} => out(expr),
			LogicalExpr::Postfix {
				expr,
				..
			} => out(expr),
			LogicalExpr::Invoke {
				target,
				args,
			} => {
				out(target);
				for e in args {
					out(e);
				}
			}
			LogicalExpr::Binary {
				left,
				right,
				..
			} => {
				out(left);
				out(right);
			}
			LogicalExpr::Call(call) => {
				for e in &call.args {
					out(e);
				}
			}
			LogicalExpr::Closure(closure) => out(&closure.body),
			LogicalExpr::If(branches) => {
				for (cond, then) in &branches.branches {
					out(cond);
					out(then);
				}
				if let Some(e) = &branches.otherwise {
					out(e);
				};
			}
			LogicalExpr::Throw(expr) => out(expr),
		}
	}

	/// [`Self::for_each_child`], collected. Allocates once per node, so a walk
	/// of a whole tree takes the callback form.
	pub fn children(&self) -> Vec<&LogicalExpr> {
		let mut out = Vec::new();
		self.for_each_child(&mut |child| out.push(child));
		out
	}

	/// [`Self::children`], mutably.
	pub fn children_mut(&mut self) -> Vec<&mut LogicalExpr> {
		let mut out = Vec::new();
		match self {
			LogicalExpr::Literal(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Field(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_) => {}
			LogicalExpr::Array(items) | LogicalExpr::Set(items) => out.extend(items),
			LogicalExpr::Object(entries) => out.extend(entries.iter_mut().map(|(_, v)| v)),
			LogicalExpr::RecordId(rid) => rid.key.exprs_mut(&mut out),
			LogicalExpr::Access {
				base,
				..
			}
			| LogicalExpr::Flatten {
				base,
				..
			} => out.push(base),
			LogicalExpr::First(base)
			| LogicalExpr::Last(base)
			| LogicalExpr::Each(base)
			| LogicalExpr::RepeatRecurse(base) => out.push(base),
			LogicalExpr::Index {
				base,
				index,
			} => {
				out.push(base);
				out.push(index);
			}
			LogicalExpr::Filter {
				base,
				predicate,
			} => {
				out.push(base);
				out.push(predicate);
			}
			LogicalExpr::Method {
				base,
				args,
				..
			} => {
				out.push(base);
				out.extend(args);
			}
			LogicalExpr::Destructure {
				base,
				fields,
			} => {
				out.push(base);
				fields.iter_mut().for_each(|field| field.exprs_mut(&mut out));
			}
			LogicalExpr::Optional {
				base,
				then,
			} => {
				out.push(base);
				out.push(then);
			}
			LogicalExpr::Lookup {
				base,
				lookup,
			} => {
				out.push(base);
				lookup.exprs_mut(&mut out);
			}
			LogicalExpr::Recurse(recurse) => {
				let LogicalRecurse {
					base,
					path,
					instruction,
					..
				} = &mut **recurse;
				out.push(base);
				if let Some(path) = path {
					out.push(path.body_mut());
				}
				if let Some(LogicalRecurseInstruction::Shortest {
					expects,
					..
				}) = instruction
				{
					out.push(expects);
				}
			}
			LogicalExpr::Broadcast {
				base,
				body,
				..
			} => {
				out.push(base);
				out.push(body);
			}
			LogicalExpr::SetField {
				base,
				value,
				..
			} => {
				out.push(base);
				out.push(value);
			}
			LogicalExpr::Prefix {
				expr,
				..
			} => out.push(expr),
			LogicalExpr::Postfix {
				expr,
				..
			} => out.push(expr),
			LogicalExpr::Invoke {
				target,
				args,
			} => {
				out.push(target);
				out.extend(args);
			}
			LogicalExpr::Binary {
				left,
				right,
				..
			} => {
				out.push(left);
				out.push(right);
			}
			LogicalExpr::Call(call) => out.extend(&mut call.args),
			LogicalExpr::Closure(closure) => out.push(&mut closure.body),
			LogicalExpr::If(branches) => {
				for (cond, then) in &mut branches.branches {
					out.push(cond);
					out.push(then);
				}
				out.extend(&mut branches.otherwise);
			}
			LogicalExpr::Throw(expr) => out.push(expr),
		}
		out
	}

	/// Walks the expression in pre-order, stopping at the first `Break`. Does
	/// not enter a [`Self::Subquery`].
	pub fn visit<B>(&self, f: &mut impl FnMut(&LogicalExpr) -> ControlFlow<B>) -> ControlFlow<B> {
		// The recursion is over `dyn FnMut` rather than the caller's closure
		// type: a generic function that handed itself a fresh closure at every
		// level would have no monomorphic form.
		fn walk<B>(
			expr: &LogicalExpr,
			f: &mut dyn FnMut(&LogicalExpr) -> ControlFlow<B>,
		) -> ControlFlow<B> {
			f(expr)?;
			let mut flow = ControlFlow::Continue(());
			expr.for_each_child(&mut |child| {
				if flow.is_continue() {
					flow = walk(child, &mut *f);
				}
			});
			flow
		}
		walk(self, f)
	}

	/// [`Self::visit`], entering every [`Self::Subquery`]: the scalars of a
	/// nested plan are walked in turn, so the walk reaches every expression
	/// evaluated within this one.
	pub fn visit_deep<B>(
		&self,
		f: &mut impl FnMut(&LogicalExpr) -> ControlFlow<B>,
	) -> ControlFlow<B> {
		fn walk<B>(
			expr: &LogicalExpr,
			f: &mut dyn FnMut(&LogicalExpr) -> ControlFlow<B>,
		) -> ControlFlow<B> {
			f(expr)?;
			if let LogicalExpr::Subquery(op) = expr {
				return match op.visit_scalars_deep(&mut |scalar| match walk(scalar, &mut *f) {
					ControlFlow::Continue(()) => Ok(()),
					ControlFlow::Break(b) => Err(b),
				}) {
					Ok(()) => ControlFlow::Continue(()),
					Err(b) => ControlFlow::Break(b),
				};
			}
			let mut flow = ControlFlow::Continue(());
			expr.for_each_child(&mut |child| {
				if flow.is_continue() {
					flow = walk(child, &mut *f);
				}
			});
			flow
		}
		walk(self, f)
	}

	/// Rewrites in place, bottom-up: every nested expression is rewritten
	/// before `f` sees the expression holding it. Does not enter a
	/// [`Self::Subquery`].
	pub fn visit_mut<E>(
		&mut self,
		f: &mut impl FnMut(&mut LogicalExpr) -> Result<(), E>,
	) -> Result<(), E> {
		for child in self.children_mut() {
			child.visit_mut(f)?;
		}
		f(self)
	}

	/// Rebuilds the expression bottom-up, replacing every node with `f(node)`.
	/// Does not enter a [`Self::Subquery`].
	pub fn transform<E>(
		mut self,
		f: &mut impl FnMut(LogicalExpr) -> Result<LogicalExpr, E>,
	) -> Result<LogicalExpr, E> {
		self.visit_mut(&mut |node| {
			let taken = std::mem::replace(node, LogicalExpr::Break);
			*node = f(taken)?;
			Ok(())
		})?;
		Ok(self)
	}

	/// Whether any nested expression, this one included, satisfies `pred`.
	pub fn any(&self, mut pred: impl FnMut(&LogicalExpr) -> bool) -> bool {
		self.visit(&mut |node| {
			if pred(node) {
				ControlFlow::Break(())
			} else {
				ControlFlow::Continue(())
			}
		})
		.is_break()
	}
}

impl LogicalDestructureField {
	fn for_each_expr<'a>(&'a self, out: &mut dyn FnMut(&'a LogicalExpr)) {
		match self {
			LogicalDestructureField::All(_) | LogicalDestructureField::Field(_) => {}
			LogicalDestructureField::Aliased(_, body) => out(body),
			LogicalDestructureField::Destructure(_, fields) => {
				for field in fields {
					field.for_each_expr(out);
				}
			}
		}
	}

	fn exprs_mut<'a>(&'a mut self, out: &mut Vec<&'a mut LogicalExpr>) {
		match self {
			LogicalDestructureField::All(_) | LogicalDestructureField::Field(_) => {}
			LogicalDestructureField::Aliased(_, body) => out.push(body),
			LogicalDestructureField::Destructure(_, fields) => {
				fields.iter_mut().for_each(|field| field.exprs_mut(out))
			}
		}
	}
}

impl LogicalLookup {
	fn for_each_expr<'a>(&'a self, out: &mut dyn FnMut(&'a LogicalExpr)) {
		if let Some(fields) = &self.expr {
			fields.for_each_expr(out);
		}
		for subject in &self.what {
			subject.for_each_expr(out);
		}
		if let Some(e) = &self.cond {
			out(e);
		};
		for e in self.split.iter().flatten() {
			out(e);
		}
		for e in self.group.iter().flatten() {
			out(e);
		}
		if let Some(order) = &self.order {
			order.for_each_expr(out);
		}
		if let Some(e) = &self.limit {
			out(e);
		};
		if let Some(e) = &self.start {
			out(e);
		};
	}

	fn exprs_mut<'a>(&'a mut self, out: &mut Vec<&'a mut LogicalExpr>) {
		if let Some(fields) = &mut self.expr {
			fields.exprs_mut(out);
		}
		for subject in &mut self.what {
			subject.exprs_mut(out);
		}
		out.extend(&mut self.cond);
		out.extend(self.split.iter_mut().flatten());
		out.extend(self.group.iter_mut().flatten());
		if let Some(order) = &mut self.order {
			order.exprs_mut(out);
		}
		out.extend(&mut self.limit);
		out.extend(&mut self.start);
	}
}

impl LogicalLookupSubject {
	fn for_each_expr<'a>(&'a self, out: &mut dyn FnMut(&'a LogicalExpr)) {
		match self {
			LogicalLookupSubject::Table {
				..
			} => {}
			LogicalLookupSubject::Range {
				range,
				..
			} => range.for_each_expr(out),
		}
	}

	fn exprs_mut<'a>(&'a mut self, out: &mut Vec<&'a mut LogicalExpr>) {
		match self {
			LogicalLookupSubject::Table {
				..
			} => {}
			LogicalLookupSubject::Range {
				range,
				..
			} => range.exprs_mut(out),
		}
	}
}

impl LogicalRecordIdKey {
	fn for_each_expr<'a>(&'a self, out: &mut dyn FnMut(&'a LogicalExpr)) {
		match self {
			LogicalRecordIdKey::Number(_)
			| LogicalRecordIdKey::String(_)
			| LogicalRecordIdKey::Uuid(_)
			| LogicalRecordIdKey::Generate(_) => {}
			LogicalRecordIdKey::Array(items) => {
				for e in items {
					out(e);
				}
			}
			LogicalRecordIdKey::Object(entries) => {
				for e in entries.iter().map(|(_, v)| v) {
					out(e);
				}
			}
			LogicalRecordIdKey::Range(range) => range.for_each_expr(out),
		}
	}

	fn exprs_mut<'a>(&'a mut self, out: &mut Vec<&'a mut LogicalExpr>) {
		match self {
			LogicalRecordIdKey::Number(_)
			| LogicalRecordIdKey::String(_)
			| LogicalRecordIdKey::Uuid(_)
			| LogicalRecordIdKey::Generate(_) => {}
			LogicalRecordIdKey::Array(items) => out.extend(items),
			LogicalRecordIdKey::Object(entries) => out.extend(entries.iter_mut().map(|(_, v)| v)),
			LogicalRecordIdKey::Range(range) => range.exprs_mut(out),
		}
	}
}

impl LogicalRecordIdKeyRange {
	fn for_each_expr<'a>(&'a self, out: &mut dyn FnMut(&'a LogicalExpr)) {
		for bound in [&self.start, &self.end] {
			if let Bound::Included(key) | Bound::Excluded(key) = bound {
				key.for_each_expr(out);
			}
		}
	}

	fn exprs_mut<'a>(&'a mut self, out: &mut Vec<&'a mut LogicalExpr>) {
		for bound in [&mut self.start, &mut self.end] {
			if let Bound::Included(key) | Bound::Excluded(key) = bound {
				key.exprs_mut(out);
			}
		}
	}
}

impl LogicalFields {
	/// Every expression the projection holds: each selector. An alias is a
	/// name, not an expression.
	pub fn for_each_expr<'a>(&'a self, out: &mut dyn FnMut(&'a LogicalExpr)) {
		match self {
			LogicalFields::Value {
				expr,
				..
			} => out(expr),
			LogicalFields::Select(fields) => {
				for field in fields {
					if let LogicalField::Single {
						expr,
						..
					} = field
					{
						out(expr);
					}
				}
			}
		}
	}

	/// [`Self::for_each_expr`], collected.
	pub fn exprs<'a>(&'a self, out: &mut Vec<&'a LogicalExpr>) {
		self.for_each_expr(&mut |expr| out.push(expr));
	}

	/// [`Self::exprs`], mutably.
	pub fn exprs_mut<'a>(&'a mut self, out: &mut Vec<&'a mut LogicalExpr>) {
		match self {
			LogicalFields::Value {
				expr,
				..
			} => out.push(expr),
			LogicalFields::Select(fields) => {
				for field in fields {
					if let LogicalField::Single {
						expr,
						..
					} = field
					{
						out.push(expr);
					}
				}
			}
		}
	}
}

impl LogicalOrdering {
	/// Every ordering key.
	pub fn for_each_expr<'a>(&'a self, out: &mut dyn FnMut(&'a LogicalExpr)) {
		match self {
			LogicalOrdering::Random => {}
			LogicalOrdering::Order(orders) => {
				for e in orders.iter().map(|o| &o.value) {
					out(e);
				}
			}
		}
	}

	/// [`Self::for_each_expr`], collected.
	pub fn exprs<'a>(&'a self, out: &mut Vec<&'a LogicalExpr>) {
		self.for_each_expr(&mut |expr| out.push(expr));
	}

	/// [`Self::exprs`], mutably.
	pub fn exprs_mut<'a>(&'a mut self, out: &mut Vec<&'a mut LogicalExpr>) {
		match self {
			LogicalOrdering::Random => {}
			LogicalOrdering::Order(orders) => out.extend(orders.iter_mut().map(|o| &mut o.value)),
		}
	}
}

// ---------------------------------------------------------------------------
// Read-only classification
// ---------------------------------------------------------------------------

impl LogicalExpr {
	/// Whether evaluating this expression only reads.
	///
	/// One-sided, as for the expression layer: it may call a reading
	/// expression writable, and never the reverse. Every construct whose
	/// effect is invisible from the syntax — a closure invoked through a value,
	/// an unregistered method, a custom or scripted function — answers `false`.
	pub fn read_only(&self) -> bool {
		match self {
			LogicalExpr::Literal(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Field(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Break
			| LogicalExpr::Continue => true,
			LogicalExpr::Array(items) | LogicalExpr::Set(items) => {
				items.iter().all(LogicalExpr::read_only)
			}
			LogicalExpr::Object(entries) => entries.iter().all(|(_, v)| v.read_only()),
			LogicalExpr::RecordId(rid) => rid.read_only(),
			// A builtin method writes nothing beyond what its receiver and
			// arguments carry; an unregistered name invokes a closure stored
			// on the receiver, whose body is invisible here.
			LogicalExpr::Method {
				base,
				name,
				args,
			} => {
				is_builtin_method(name)
					&& base.read_only()
					&& args.iter().all(LogicalExpr::read_only)
			}
			LogicalExpr::Lookup {
				base,
				lookup,
			} => base.read_only() && lookup.read_only(),
			LogicalExpr::SetField {
				base,
				value,
				..
			} => base.read_only() && value.read_only(),
			// Every other step reads whatever its base and operands read.
			LogicalExpr::Access {
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
			| LogicalExpr::Destructure {
				..
			}
			| LogicalExpr::Optional {
				..
			}
			| LogicalExpr::Recurse(_)
			| LogicalExpr::RepeatRecurse(_)
			| LogicalExpr::Broadcast {
				..
			} => {
				let mut reads = true;
				self.for_each_child(&mut |child| reads = reads && child.read_only());
				reads
			}
			LogicalExpr::Prefix {
				expr,
				..
			} => expr.read_only(),
			LogicalExpr::Postfix {
				expr,
				..
			} => expr.read_only(),
			// Applying a value runs a body this expression can only see when
			// the target is a closure literal; anything else must
			// over-approximate to writable.
			LogicalExpr::Invoke {
				target,
				args,
			} => {
				matches!(&**target, LogicalExpr::Closure(_))
					&& target.read_only()
					&& args.iter().all(LogicalExpr::read_only)
			}
			LogicalExpr::Binary {
				left,
				right,
				..
			} => left.read_only() && right.read_only(),
			LogicalExpr::Call(call) => {
				call.receiver.read_only() && call.args.iter().all(LogicalExpr::read_only)
			}
			// A closure literal is inert as a value, but wherever it can flow
			// it can also be invoked, so a writing body makes it writable.
			LogicalExpr::Closure(closure) => closure.body.read_only(),
			LogicalExpr::If(branches) => {
				branches.branches.iter().all(|(c, t)| c.read_only() && t.read_only())
					&& branches.otherwise.as_ref().is_none_or(LogicalExpr::read_only)
			}
			LogicalExpr::Throw(expr) => expr.read_only(),
			LogicalExpr::Subquery(op) => op.read_only(),
		}
	}
}

impl LogicalRecordIdLit {
	/// Whether evaluating the key only reads.
	pub fn read_only(&self) -> bool {
		self.key.read_only()
	}
}

impl LogicalRecordIdKey {
	fn read_only(&self) -> bool {
		match self {
			LogicalRecordIdKey::Number(_)
			| LogicalRecordIdKey::String(_)
			| LogicalRecordIdKey::Uuid(_)
			| LogicalRecordIdKey::Generate(_) => true,
			LogicalRecordIdKey::Array(items) => items.iter().all(LogicalExpr::read_only),
			LogicalRecordIdKey::Object(entries) => entries.iter().all(|(_, v)| v.read_only()),
			LogicalRecordIdKey::Range(range) => range.read_only(),
		}
	}
}

impl LogicalRecordIdKeyRange {
	fn read_only(&self) -> bool {
		[&self.start, &self.end].into_iter().all(|bound| match bound {
			Bound::Included(key) | Bound::Excluded(key) => key.read_only(),
			Bound::Unbounded => true,
		})
	}
}

impl LogicalLookup {
	fn read_only(&self) -> bool {
		self.what.iter().all(LogicalLookupSubject::read_only)
			&& self.expr.as_ref().is_none_or(LogicalFields::read_only)
			&& self.cond.as_ref().is_none_or(LogicalExpr::read_only)
			&& self.split.iter().flatten().all(LogicalExpr::read_only)
			&& self.group.iter().flatten().all(LogicalExpr::read_only)
			&& self.order.as_ref().is_none_or(LogicalOrdering::read_only)
			&& self.limit.as_ref().is_none_or(LogicalExpr::read_only)
			&& self.start.as_ref().is_none_or(LogicalExpr::read_only)
	}
}

impl LogicalLookupSubject {
	fn read_only(&self) -> bool {
		match self {
			LogicalLookupSubject::Table {
				..
			} => true,
			LogicalLookupSubject::Range {
				range,
				..
			} => range.read_only(),
		}
	}
}

impl LogicalFields {
	/// Whether evaluating every selector only reads.
	pub fn read_only(&self) -> bool {
		match self {
			LogicalFields::Value {
				expr,
				..
			} => expr.read_only(),
			LogicalFields::Select(fields) => fields.iter().all(|field| match field {
				LogicalField::All => true,
				LogicalField::Single {
					expr,
					..
				} => expr.read_only(),
			}),
		}
	}
}

impl LogicalOrdering {
	/// Whether evaluating every ordering key only reads.
	pub fn read_only(&self) -> bool {
		match self {
			LogicalOrdering::Random => true,
			LogicalOrdering::Order(orders) => orders.iter().all(|o| o.value.read_only()),
		}
	}
}

// ---------------------------------------------------------------------------
// Naming
// ---------------------------------------------------------------------------

/// Where a projected value lands in the result object.
///
/// A sequence of keys, each written into the object at the previous key:
/// `a.b` puts the value at `{ a: { b: … } }`, while the single key `a.b`
/// (an escaped identifier containing a dot) puts it at `{ "a.b": … }`. A
/// [`OutputPathPart::Lookup`] is a key spelled as a lookup renders,
/// `->likes`, which is how a graph traversal names itself in the output.
///
/// The keys are all a writer can address, but the path they were derived
/// from may have said more: `arr[0]`, `arr[$]`, `tags[WHERE …].value` and
/// `a.*` each contain a step that names no key. [`Self::exact`] records
/// whether the written path was exactly its keys, so that two paths landing
/// at the same keys still compare unequal when only one of them was written
/// as those keys, and so that a writer which can only address one bare key
/// can decline a path that asked for a position inside it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OutputPath {
	/// The keys, outermost first.
	pub parts: Vec<OutputPathPart>,
	/// Whether every step of the written path is one of `parts`.
	pub exact: bool,
}

/// One key of an [`OutputPath`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum OutputPathPart {
	/// A field name, unescaped.
	Field(String),
	/// A lookup, as it renders: `->likes`, `<-(knows WHERE x)`.
	Lookup(String),
}

impl OutputPath {
	/// The path of one key.
	pub fn field(name: impl Into<String>) -> Self {
		OutputPath::keys(vec![OutputPathPart::Field(name.into())])
	}

	/// A path written as exactly these keys.
	pub fn keys(parts: Vec<OutputPathPart>) -> Self {
		OutputPath {
			parts,
			exact: true,
		}
	}

	/// The keys, outermost first.
	pub fn parts(&self) -> &[OutputPathPart] {
		&self.parts
	}

	/// The one key, when the path lands at a single field.
	pub fn as_field(&self) -> Option<&str> {
		match self.parts.as_slice() {
			[OutputPathPart::Field(name)] => Some(name),
			_ => None,
		}
	}

	/// Whether the path lands at the single field `name`.
	pub fn is_field(&self, name: &str) -> bool {
		self.as_field() == Some(name)
	}

	/// The flat name of the path: a single field's name as written, and
	/// otherwise the path rendered as an idiom.
	pub fn name(&self) -> String {
		match self.as_field() {
			Some(name) => name.to_owned(),
			None => self.to_sql(),
		}
	}
}

impl ToSql for OutputPath {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		for (i, part) in self.parts.iter().enumerate() {
			match part {
				OutputPathPart::Field(name) if i == 0 => EscapeIdent(name).fmt_sql(f, fmt),
				OutputPathPart::Field(name) => write_sql!(f, fmt, ".{}", EscapeKwFreeIdent(name)),
				OutputPathPart::Lookup(lookup) => f.push_str(lookup),
			}
		}
	}
}

/// One naming-relevant piece of a linearised path.
enum Segment<'a> {
	/// A field read: the root `Field` or an `Access`.
	Field(&'a Strand),
	/// The expression a `(e).rest` path starts from.
	Start(&'a LogicalExpr),
	Lookup(&'a LogicalLookup),
	/// Any other step: contributes nothing to a name.
	Other,
}

impl LogicalExpr {
	/// The path in the order its steps apply, following a [`Self::Broadcast`]
	/// and an [`Self::Optional`] into their bodies and a
	/// [`LogicalRecursePath::Trailing`] body into its steps, since all of
	/// those are the continuation of the same written path. A
	/// [`LogicalRecursePath::Nested`] body and destructure alias bodies are
	/// not part of the path they sit in.
	fn segments<'a>(&'a self, out: &mut Vec<Segment<'a>>) {
		match self {
			LogicalExpr::Current => {}
			LogicalExpr::Field(name) => out.push(Segment::Field(name)),
			LogicalExpr::Doc => out.push(Segment::Other),
			LogicalExpr::Access {
				base,
				field,
			} => {
				base.segments(out);
				out.push(Segment::Field(field));
			}
			LogicalExpr::Lookup {
				base,
				lookup,
			} => {
				base.segments(out);
				out.push(Segment::Lookup(lookup));
			}
			LogicalExpr::Optional {
				base,
				then,
			} => {
				base.segments(out);
				out.push(Segment::Other);
				then.segments(out);
			}
			LogicalExpr::Recurse(recurse) => {
				recurse.base.segments(out);
				out.push(Segment::Other);
				if let Some(LogicalRecursePath::Trailing(body)) = &recurse.path {
					body.segments(out);
				}
			}
			LogicalExpr::Broadcast {
				base,
				body,
				..
			} => {
				base.segments(out);
				body.segments(out);
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
			} => {
				base.segments(out);
				out.push(Segment::Other);
			}
			LogicalExpr::First(base)
			| LogicalExpr::Last(base)
			| LogicalExpr::Each(base)
			| LogicalExpr::RepeatRecurse(base) => {
				base.segments(out);
				out.push(Segment::Other);
			}
			LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
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
			| LogicalExpr::Subquery(_) => out.push(Segment::Start(self)),
		}
	}

	/// Where a projected expression lands in the result when it has no alias.
	///
	/// A path names itself by its field reads, lookups and starting
	/// expression, in order; every other step (`[0]`, `[WHERE …]`, `.*`, a
	/// method, a destructure, `.?`, a recursion) contributes nothing, so
	/// `tags[WHERE type = 'x'][0].value` lands at `tags.value`. A lookup with
	/// an alias names the whole path by that alias, as a single key. A path
	/// starting from an expression (`$p.a`, `(SELECT …).x`) is a single key
	/// spelled as it renders, as is a path with nothing left to name it. A
	/// parameter is named by its name, a call by its function's name, a
	/// string literal by its contents, a datetime by its text, and anything
	/// else by its rendering. The result is [`OutputPath::exact`] when every
	/// step of the path became one of its keys; a non-path is always exact.
	///
	/// A rendering that spells a statement in scalar position is not a usable
	/// name — a [`Self::Subquery`] renders as the plan it holds — so a
	/// projection whose name would be one carries its SurrealQL text as its
	/// alias instead; see [`Self::output_name_renders_a_statement`].
	pub fn output_path(&self) -> OutputPath {
		if self.is_path() {
			let mut segments = Vec::new();
			self.segments(&mut segments);
			return path_output(&segments);
		}
		match self {
			LogicalExpr::Param(param) => OutputPath::field(param.as_str()),
			LogicalExpr::Call(call) => OutputPath::field(function_name(&call.receiver).as_str()),
			LogicalExpr::Literal(LogicalLiteral::String(s)) => OutputPath::field(s.as_str()),
			LogicalExpr::Literal(LogicalLiteral::Datetime(d)) => OutputPath::field(d.to_string()),
			// The path forms are named above; `is_path` covers all of them.
			LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Table(_)
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
			| LogicalExpr::Closure(_)
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_)
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
			| LogicalExpr::SetField {
				..
			} => OutputPath::field(self.to_sql()),
		}
	}

	/// Whether the name [`Self::output_path`] derives is spelled from the
	/// rendering of a statement in scalar position.
	///
	/// True for a [`Self::Subquery`] or [`Self::If`], for an expression named
	/// by its rendering that holds one anywhere, and for a path whose
	/// starting expression or whose lookups hold one, since those render into
	/// the path's name. False for the shapes whose name never renders a
	/// nested expression, whatever they hold: a parameter, a call, a string
	/// or datetime literal, a path named by its field reads alone, and a path
	/// named by a lookup alias.
	pub fn output_name_renders_a_statement(&self) -> bool {
		let is_statement =
			|expr: &LogicalExpr| matches!(expr, LogicalExpr::Subquery(_) | LogicalExpr::If(_));
		if self.is_path() {
			let mut segments = Vec::new();
			self.segments(&mut segments);
			if segments
				.iter()
				.any(|segment| matches!(segment, Segment::Lookup(lookup) if lookup.alias.is_some()))
			{
				return false;
			}
			return segments.iter().any(|segment| match segment {
				Segment::Start(start) => start.any(is_statement),
				Segment::Lookup(lookup) => {
					let mut found = false;
					lookup.for_each_expr(&mut |expr| found = found || expr.any(is_statement));
					found
				}
				Segment::Field(_) | Segment::Other => false,
			});
		}
		match self {
			LogicalExpr::Param(_)
			| LogicalExpr::Call(_)
			| LogicalExpr::Literal(LogicalLiteral::String(_) | LogicalLiteral::Datetime(_)) => false,
			LogicalExpr::Subquery(_) | LogicalExpr::If(_) => true,
			// The path forms were handled above; every other form is named by
			// its rendering.
			LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Table(_)
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
			| LogicalExpr::Closure(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
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
			| LogicalExpr::SetField {
				..
			} => self.any(is_statement),
		}
	}
}

/// [`LogicalExpr::output_path`] for a linearised path.
fn path_output(segments: &[Segment<'_>]) -> OutputPath {
	// The path is exact when every step became one of the keys.
	let exact_when = |parts: Vec<OutputPathPart>| OutputPath {
		exact: parts.len() == segments.len(),
		parts,
	};
	// A lookup with an alias names the whole path.
	for segment in segments {
		if let Segment::Lookup(lookup) = segment
			&& let Some(alias) = &lookup.alias
		{
			return exact_when(vec![OutputPathPart::Field(alias.name())]);
		}
	}
	let naming: Vec<&Segment<'_>> =
		segments.iter().filter(|s| !matches!(s, Segment::Other)).collect();
	let name = match naming.as_slice() {
		[Segment::Field(name)] => name.as_str().to_owned(),
		_ => {
			let mut out = String::new();
			for (i, segment) in naming.iter().enumerate() {
				match segment {
					Segment::Field(name) if i == 0 => {
						EscapeIdent(name.as_str()).fmt_sql(&mut out, SqlFormat::SingleLine)
					}
					Segment::Field(name) => {
						out.push('.');
						EscapeKwFreeIdent(name.as_str()).fmt_sql(&mut out, SqlFormat::SingleLine);
					}
					Segment::Start(start) => fmt_start(start, &mut out, SqlFormat::SingleLine),
					Segment::Lookup(lookup) => lookup.fmt_sql(&mut out, SqlFormat::SingleLine),
					Segment::Other => {}
				}
			}
			out
		}
	};
	if naming.is_empty() || naming.iter().any(|s| matches!(s, Segment::Start(_))) {
		return exact_when(vec![OutputPathPart::Field(name)]);
	}
	exact_when(
		naming
			.iter()
			.filter_map(|segment| match segment {
				Segment::Field(name) => Some(OutputPathPart::Field(name.as_str().to_owned())),
				Segment::Lookup(lookup) => Some(OutputPathPart::Lookup(lookup.to_sql())),
				Segment::Start(_) | Segment::Other => None,
			})
			.collect(),
	)
}

/// The name a function call projects as, `fn::name` for a user function and
/// the bare name for a builtin.
fn function_name(function: &Function) -> Strand {
	let mut parts = function.to_idiom().0.into_iter();
	if let Some(crate::expr::Part::Field(name)) = parts.next() {
		name
	} else {
		Strand::new("function")
	}
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Renders a scalar in a position where a statement-shaped one would be
/// ambiguous, parenthesising it. An `IF` is the one such shape; a
/// [`LogicalExpr::Subquery`] renders with its own parentheses.
struct Cover<'a>(&'a LogicalExpr);

impl ToSql for Cover<'_> {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		if let LogicalExpr::If(_) = self.0 {
			f.push('(');
			self.0.fmt_sql(f, fmt);
			f.push(')');
		} else {
			self.0.fmt_sql(f, fmt);
		}
	}
}

impl LogicalExpr {
	/// Whether the expression must be parenthesised when it is an operand.
	fn needs_parentheses(&self) -> bool {
		match self {
			LogicalExpr::Literal(LogicalLiteral::UnboundedRange)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Closure(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Throw(_)
			| LogicalExpr::If(_)
			| LogicalExpr::Postfix {
				..
			}
			| LogicalExpr::Invoke {
				..
			} => true,
			LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
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
			| LogicalExpr::Binary {
				..
			}
			| LogicalExpr::SetField {
				..
			}
			| LogicalExpr::Call(_)
			| LogicalExpr::Subquery(_) => false,
		}
	}

	/// The step that renders first in a path: the innermost step whose base
	/// is [`Self::Current`], looking through a [`Self::Broadcast`] over the
	/// row into its body. `None` for a path rooted in a field, `@` or a
	/// starting expression, since those render first themselves.
	fn leading_step(&self) -> Option<&LogicalExpr> {
		let mut current = self;
		loop {
			if let LogicalExpr::Broadcast {
				base,
				body,
				..
			} = current && matches!(**base, LogicalExpr::Current)
			{
				current = body;
				continue;
			}
			match current.step_base() {
				Some(LogicalExpr::Current) => return Some(current),
				Some(base) => current = base,
				None => return None,
			}
		}
	}

	/// Whether the leftmost token of the rendering is a minus sign, so that a
	/// negation in front of it must be parenthesised to avoid `--`.
	fn has_left_minus(&self) -> bool {
		match self {
			LogicalExpr::Prefix {
				op: PrefixOperator::Negate,
				..
			} => true,
			LogicalExpr::Postfix {
				expr,
				..
			}
			| LogicalExpr::Binary {
				left: expr,
				..
			}
			| LogicalExpr::Invoke {
				target: expr,
				..
			} => expr.has_left_minus(),
			LogicalExpr::Literal(LogicalLiteral::Integer(x)) => x.is_negative(),
			LogicalExpr::Literal(LogicalLiteral::Float(x)) => x.is_sign_negative(),
			LogicalExpr::Literal(LogicalLiteral::Decimal(x)) => x.is_sign_negative(),
			// A path starting with an outgoing lookup renders `->` first.
			LogicalExpr::Field(_)
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
			} => matches!(
				self.leading_step(),
				Some(LogicalExpr::Lookup { lookup, .. })
					if lookup.kind == LookupKind::Graph(crate::expr::Dir::Out)
			),
			LogicalExpr::Literal(_)
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
			| LogicalExpr::Call(_)
			| LogicalExpr::Closure(_)
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_) => false,
		}
	}

	/// Whether the rendering starts with a path, which a closure body must
	/// parenthesise so `->` is not read as a return-kind arrow.
	fn has_left_idiom(&self) -> bool {
		match self {
			LogicalExpr::Postfix {
				expr,
				..
			}
			| LogicalExpr::Binary {
				left: expr,
				..
			}
			| LogicalExpr::Invoke {
				target: expr,
				..
			} => expr.has_left_idiom(),
			LogicalExpr::Literal(_)
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
			| LogicalExpr::Call(_)
			| LogicalExpr::Closure(_)
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_) => false,
			LogicalExpr::Field(_)
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
			} => true,
		}
	}

	/// How tightly this expression binds as an operand; `Prime` for anything
	/// that is not itself an operator application.
	fn binding_power(&self) -> BindingPower {
		match self {
			LogicalExpr::Prefix {
				op,
				..
			} => {
				if let PrefixOperator::Range | PrefixOperator::RangeInclusive = op {
					BindingPower::Range
				} else {
					BindingPower::Prefix
				}
			}
			LogicalExpr::Postfix {
				op,
				..
			} => op.binding_power(),
			LogicalExpr::Invoke {
				..
			} => BindingPower::Call,
			LogicalExpr::Binary {
				op,
				..
			} => {
				let op: crate::sql::BinaryOperator = op.clone().into();
				BindingPower::for_binary_operator(&op)
			}
			LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
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
			| LogicalExpr::Call(_)
			| LogicalExpr::Closure(_)
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::SetField {
				..
			}
			| LogicalExpr::Subquery(_) => BindingPower::Prime,
		}
	}
}

impl LogicalPostfixOperator {
	fn binding_power(&self) -> BindingPower {
		match self {
			LogicalPostfixOperator::Range | LogicalPostfixOperator::RangeSkip => {
				BindingPower::Range
			}
		}
	}
}

/// Writes the indentation of the line closing a pretty-printed bracket: one
/// level less than the contents.
fn write_closing_indent(f: &mut String, fmt: SqlFormat) {
	if let SqlFormat::Indented(level) = fmt
		&& level > 0
	{
		for _ in 0..(level - 1) {
			f.push('\t');
		}
	}
}

fn fmt_object_entries(entries: &[(String, LogicalExpr)], f: &mut String, fmt: SqlFormat) {
	if fmt.is_pretty() {
		f.push('{');
	} else {
		f.push_str("{ ");
	}
	if !entries.is_empty() {
		let fmt = fmt.increment();
		if fmt.is_pretty() {
			f.push('\n');
			fmt.write_indent(f);
		}
		for (i, (key, value)) in entries.iter().enumerate() {
			if i > 0 {
				fmt.write_separator(f);
			}
			write_sql!(f, fmt, "{}: {}", EscapeObjectKey(key), Cover(value));
		}
		if fmt.is_pretty() {
			f.push('\n');
			write_closing_indent(f, fmt);
		}
	}
	if fmt.is_pretty() {
		f.push('}');
	} else {
		f.push_str(" }");
	}
}

/// How the innermost step of a body renders when its base is
/// [`LogicalExpr::Current`].
#[derive(Clone, Copy, PartialEq, Eq)]
enum BodyRoot {
	/// With its normal prefix: `.f`, `[e]`, `->e`. The body of a
	/// `Broadcast`, an `Optional` or a `Recurse`.
	Step,
	/// A leading field renders bare, as the first part of an idiom does. A
	/// destructure alias body, which is written as an idiom.
	Bare,
}

/// Renders the expression a path starts from, parenthesised whenever the
/// steps that follow could otherwise bind into it: an operator application,
/// a negative number, or anything that is parenthesised as an operand.
fn fmt_start(start: &LogicalExpr, f: &mut String, fmt: SqlFormat) {
	let negative_number = matches!(start, LogicalExpr::Literal(LogicalLiteral::Decimal(d)) if d.is_sign_negative())
		|| matches!(start, LogicalExpr::Literal(LogicalLiteral::Integer(i)) if i.is_negative())
		|| matches!(start, LogicalExpr::Literal(LogicalLiteral::Float(x)) if x.is_sign_negative());
	if start.needs_parentheses()
		|| matches!(
			start,
			LogicalExpr::Binary { .. }
				| LogicalExpr::Prefix { .. }
				| LogicalExpr::Postfix { .. }
				| LogicalExpr::Invoke { .. }
		) || negative_number
	{
		write_sql!(f, fmt, "({start})");
	} else {
		start.fmt_sql(f, fmt);
	}
}

/// Renders a path: its root, then each step with its prefix, then the bodies
/// it holds. The steps of a chain are rendered iteratively so that a long
/// idiom does not recurse once per step.
fn fmt_path(expr: &LogicalExpr, f: &mut String, fmt: SqlFormat, root: BodyRoot) {
	// Collect the chain from the outermost step down to its root.
	let mut steps = Vec::new();
	let mut current = expr;
	// A fold whose base is itself a path folds over what that whole path
	// produces, so the base is a group whose parentheses have to survive:
	// rendered flat, `(a.b).c[0]` reads back as `a.b.c[0]`, which folds over
	// `a` and maps `.b` inside it instead — a different plan over the same
	// steps.
	//
	// Only a base ending in a step that a following fold would take for its
	// own needs them. A mapping step is one, and so is a fold already placed;
	// every other step stays attached to the root it was written on when the
	// path is read back, and reads the same flat. Neither does a base rooted
	// at the element need them: it continues the path its body is already
	// inside rather than standing on its own.
	let mut grouped = false;
	let root_expr = loop {
		if let LogicalExpr::Broadcast {
			base,
			..
		} = current
			// A mapping step, which a following fold would take for its own,
			// or a fold already placed over a path. A fold whose body is not a
			// path renders as a plan form with no surface spelling, which no
			// parentheses make readable back.
			&& (matches!(
				base.as_ref(),
				LogicalExpr::Access { .. } | LogicalExpr::Destructure { .. }
			) || matches!(
				base.as_ref(),
				LogicalExpr::Broadcast { body, .. } if body.step_base().is_some()
			)) && !matches!(base.path_root(), LogicalExpr::Current)
		{
			steps.push(current);
			grouped = true;
			break base.as_ref();
		}
		match current.step_base() {
			Some(base) => {
				steps.push(current);
				current = base;
			}
			None => break current,
		}
	};
	// The root.
	if grouped {
		write_sql!(f, fmt, "({root_expr})");
	} else if let LogicalExpr::Field(name) = root_expr {
		EscapeIdent(name.as_str()).fmt_sql(f, fmt);
	} else if matches!(root_expr, LogicalExpr::Doc) {
		f.push('@');
	} else if !matches!(root_expr, LogicalExpr::Current) {
		fmt_start(root_expr, f, fmt);
	}
	// A bare leading field applies to the step directly on `Current`, which
	// is the innermost one; a broadcast over `Current` passes the mode on to
	// its body.
	let mut root = root;
	let on_current = matches!(root_expr, LogicalExpr::Current);
	for (i, step) in steps.iter().rev().enumerate() {
		let innermost = i == 0 && on_current;
		match step {
			LogicalExpr::Access {
				field,
				..
			} => {
				if innermost && root == BodyRoot::Bare {
					EscapeIdent(field.as_str()).fmt_sql(f, fmt);
				} else {
					write_sql!(f, fmt, ".{}", EscapeKwFreeIdent(field.as_str()));
				}
				root = BodyRoot::Step;
			}
			LogicalExpr::Index {
				index,
				..
			} => {
				write_sql!(f, fmt, "[{index}]");
				root = BodyRoot::Step;
			}
			LogicalExpr::First(_) => {
				f.push_str("[0]");
				root = BodyRoot::Step;
			}
			LogicalExpr::Last(_) => {
				f.push_str("[$]");
				root = BodyRoot::Step;
			}
			LogicalExpr::Each(_) => {
				f.push_str(".*");
				root = BodyRoot::Step;
			}
			LogicalExpr::Flatten {
				explicit,
				..
			} => {
				if *explicit {
					f.push('…');
				}
				root = BodyRoot::Step;
			}
			LogicalExpr::Filter {
				predicate,
				..
			} => {
				write_sql!(f, fmt, "[WHERE {predicate}]");
				root = BodyRoot::Step;
			}
			LogicalExpr::Method {
				name,
				args,
				..
			} => {
				write_sql!(
					f,
					fmt,
					".{}({})",
					EscapeKwFreeIdent(name.as_str()),
					Fmt::comma_separated(args.iter().map(Cover))
				);
				root = BodyRoot::Step;
			}
			LogicalExpr::Destructure {
				fields,
				..
			} => {
				fmt_destructure(fields, f, fmt);
				root = BodyRoot::Step;
			}
			LogicalExpr::Lookup {
				lookup,
				..
			} => {
				lookup.fmt_sql(f, fmt);
				root = BodyRoot::Step;
			}
			LogicalExpr::RepeatRecurse(_) => {
				f.push_str(".@");
				root = BodyRoot::Step;
			}
			LogicalExpr::Optional {
				then,
				..
			} => {
				f.push_str(".?");
				fmt_path(then, f, fmt, BodyRoot::Step);
				root = BodyRoot::Step;
			}
			LogicalExpr::Recurse(recurse) => {
				write_sql!(f, fmt, ".{{{}", recurse.recurse);
				if let Some(instruction) = &recurse.instruction {
					write_sql!(f, fmt, "+{instruction}");
				}
				f.push('}');
				match &recurse.path {
					Some(LogicalRecursePath::Nested(body)) => {
						f.push('(');
						fmt_path(body, f, fmt, BodyRoot::Step);
						f.push(')');
					}
					Some(LogicalRecursePath::Trailing(body)) => {
						fmt_path(body, f, fmt, BodyRoot::Step)
					}
					None => {}
				}
				root = BodyRoot::Step;
			}
			LogicalExpr::Broadcast {
				body,
				..
			} => {
				// Over the row, the body's leading step is the path's
				// leading step and keeps the mode; over anything else the
				// body continues a path already begun.
				let body_root = if innermost {
					root
				} else {
					BodyRoot::Step
				};
				fmt_path(body, f, fmt, body_root);
				root = BodyRoot::Step;
			}
			// `step_base` answers `None` for every other form, so none is
			// collected as a step.
			LogicalExpr::Field(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
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
			| LogicalExpr::Subquery(_) => {}
		}
	}
}

/// Renders `.{ … }`.
fn fmt_destructure(fields: &[LogicalDestructureField], f: &mut String, fmt: SqlFormat) {
	f.push_str(".{");
	if !fmt.is_pretty() {
		f.push(' ');
	}
	if !fields.is_empty() {
		let fmt = fmt.increment();
		write_sql!(f, fmt, "{}", Fmt::pretty_comma_separated(fields));
	}
	if fmt.is_pretty() {
		f.push('}');
	} else {
		f.push_str(" }");
	}
}

impl ToSql for LogicalExpr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			LogicalExpr::Literal(literal) => literal.fmt_sql(f, fmt),
			LogicalExpr::Array(items) => {
				f.push('[');
				if !items.is_empty() {
					let fmt = fmt.increment();
					if fmt.is_pretty() {
						f.push('\n');
						fmt.write_indent(f);
					}
					for (i, item) in items.iter().enumerate() {
						if i > 0 {
							fmt.write_separator(f);
						}
						Cover(item).fmt_sql(f, fmt);
					}
					if fmt.is_pretty() {
						f.push('\n');
						write_closing_indent(f, fmt);
					}
				}
				f.push(']');
			}
			LogicalExpr::Set(items) => {
				f.push('{');
				if !items.is_empty() {
					let fmt = fmt.increment();
					if fmt.is_pretty() {
						f.push('\n');
						fmt.write_indent(f);
					}
					for (i, item) in items.iter().enumerate() {
						if i > 0 {
							fmt.write_separator(f);
						} else if let LogicalExpr::RecordId(_) = item {
							// A leading record id would otherwise read as an
							// object key.
							f.push('(');
							item.fmt_sql(f, fmt);
							f.push(')');
							continue;
						}
						Cover(item).fmt_sql(f, fmt);
					}
					if items.len() == 1 {
						f.push(',');
					}
					if fmt.is_pretty() {
						f.push('\n');
						write_closing_indent(f, fmt);
					}
				} else {
					f.push(',');
				}
				f.push('}');
			}
			LogicalExpr::Object(entries) => fmt_object_entries(entries, f, fmt),
			LogicalExpr::RecordId(rid) => rid.fmt_sql(f, fmt),
			LogicalExpr::Param(param) => param.fmt_sql(f, fmt),
			LogicalExpr::Table(table) => EscapeIdent(table.as_str()).fmt_sql(f, fmt),
			LogicalExpr::Constant(constant) => constant.fmt_sql(f, fmt),
			LogicalExpr::Field(_)
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
			} => fmt_path(self, f, fmt, BodyRoot::Step),
			// No surface syntax; the rendering is plan text.
			LogicalExpr::SetField {
				base,
				field,
				value,
			} => write_sql!(
				f,
				fmt,
				"set_field({}, {}, {})",
				Cover(base),
				QuoteStr(field.as_str()),
				Cover(value)
			),
			LogicalExpr::Prefix {
				op,
				expr,
			} => {
				let expr: &LogicalExpr = expr;
				let expr_bp = expr.binding_power();
				let op_bp = if let PrefixOperator::Range | PrefixOperator::RangeInclusive = op {
					BindingPower::Range
				} else {
					BindingPower::Prefix
				};
				if expr.needs_parentheses()
					|| expr_bp < op_bp
					|| expr_bp == op_bp && matches!(expr_bp, BindingPower::Range)
					|| *op == PrefixOperator::Negate && expr.has_left_minus()
				{
					write_sql!(f, fmt, "{op}({expr})");
				} else {
					write_sql!(f, fmt, "{op}{expr}");
				}
			}
			LogicalExpr::Postfix {
				expr,
				op,
			} => {
				let expr: &LogicalExpr = expr;
				let expr_bp = expr.binding_power();
				let op_bp = op.binding_power();
				if expr.needs_parentheses()
					|| expr_bp < op_bp
					|| expr_bp == op_bp && matches!(expr_bp, BindingPower::Range)
				{
					write_sql!(f, fmt, "({expr}){op}");
				} else {
					write_sql!(f, fmt, "{expr}{op}");
				}
			}
			// The target is always parenthesised, so `$f(1)` and `(a.b)(1)`
			// both read back as an application of the whole target.
			LogicalExpr::Invoke {
				target,
				args,
			} => {
				let target: &LogicalExpr = target;
				write_sql!(f, fmt, "({target})");
				write_sql!(f, fmt, "({})", Fmt::comma_separated(args.iter().map(Cover)));
			}
			LogicalExpr::Binary {
				left,
				op,
				right,
			} => {
				let left: &LogicalExpr = left;
				let right: &LogicalExpr = right;
				let op_bp = self.binding_power();
				let left_bp = left.binding_power();
				let right_bp = right.binding_power();

				// A same-power left child under a left-associative operator
				// is the default nesting and needs no parentheses; under a
				// right-associative one (`**`), or a non-associative one, it
				// only exists because it was written parenthesised.
				if left.needs_parentheses()
					|| left_bp < op_bp
					|| left_bp == op_bp
						&& matches!(
							left_bp,
							BindingPower::Range
								| BindingPower::Relation | BindingPower::Equality
								| BindingPower::Power
						) {
					write_sql!(f, fmt, "({left})");
				} else {
					write_sql!(f, fmt, "{left}");
				}

				if matches!(
					op,
					BinaryOperator::Range
						| BinaryOperator::RangeSkip
						| BinaryOperator::RangeInclusive
						| BinaryOperator::RangeSkipInclusive
				) {
					op.fmt_sql(f, fmt);
				} else {
					f.push(' ');
					op.fmt_sql(f, fmt);
					f.push(' ');
				}

				// The mirror image: a same-power right child under a
				// left-associative operator was written parenthesised.
				if right.needs_parentheses()
					|| right_bp < op_bp
					|| right_bp == op_bp
						&& matches!(
							right_bp,
							BindingPower::Range
								| BindingPower::Relation | BindingPower::Equality
								| BindingPower::AddSub | BindingPower::MulDiv
						) {
					write_sql!(f, fmt, "({right})");
				} else {
					write_sql!(f, fmt, "{right}");
				}
			}
			LogicalExpr::Call(call) => call.fmt_sql(f, fmt),
			LogicalExpr::Closure(closure) => closure.fmt_sql(f, fmt),
			LogicalExpr::If(branches) => branches.fmt_sql(f, fmt),
			LogicalExpr::Throw(expr) => write_sql!(f, fmt, "THROW {}", Cover(expr)),
			LogicalExpr::Break => f.push_str("BREAK"),
			LogicalExpr::Continue => f.push_str("CONTINUE"),
			// The plan, parenthesised, one operator per line. The enclosing
			// operator's renderer re-indents the continuation lines beneath
			// itself, so the plan reads as a nested source does.
			LogicalExpr::Subquery(op) => {
				f.push_str("(\n");
				op.render(f, 1);
				f.push(')');
			}
		}
	}
}

impl Display for LogicalExpr {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.write_str(&self.to_sql())
	}
}

impl ToSql for LogicalPostfixOperator {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self {
			LogicalPostfixOperator::Range => f.push_str(".."),
			LogicalPostfixOperator::RangeSkip => f.push_str(">.."),
		}
	}
}

impl ToSql for LogicalRecordIdLit {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{}:{}", EscapeIdent(self.table.as_str()), self.key);
	}
}

impl ToSql for LogicalRecordIdKey {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			LogicalRecordIdKey::Number(v) => write_sql!(f, fmt, "{v}"),
			LogicalRecordIdKey::String(v) => EscapeRidKey(v).fmt_sql(f, fmt),
			LogicalRecordIdKey::Uuid(v) => write_sql!(f, fmt, "u{}", QuoteStr(&v.to_string())),
			LogicalRecordIdKey::Array(items) => {
				f.push('[');
				if !items.is_empty() {
					let fmt = fmt.increment();
					write_sql!(f, fmt, "{}", Fmt::pretty_comma_separated(items.iter().map(Cover)));
				}
				f.push(']');
			}
			LogicalRecordIdKey::Object(entries) => {
				if fmt.is_pretty() {
					f.push('{');
				} else {
					f.push_str("{ ");
				}
				if !entries.is_empty() {
					let fmt = fmt.increment();
					write_sql!(
						f,
						fmt,
						"{}",
						Fmt::pretty_comma_separated(entries.iter().map(|(key, value)| Fmt::new(
							(key, value),
							|(key, value), f, fmt| write_sql!(
								f,
								fmt,
								"{}: {}",
								EscapeObjectKey(key),
								Cover(value)
							)
						)))
					);
				}
				if fmt.is_pretty() {
					f.push('}');
				} else {
					f.push_str(" }");
				}
			}
			LogicalRecordIdKey::Generate(generator) => match generator {
				RecordIdKeyGen::Rand => f.push_str("rand()"),
				RecordIdKeyGen::Ulid => f.push_str("ulid()"),
				RecordIdKeyGen::Uuid => f.push_str("uuid()"),
			},
			LogicalRecordIdKey::Range(range) => range.fmt_sql(f, fmt),
		}
	}
}

impl ToSql for LogicalRecordIdKeyRange {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match &self.start {
			Bound::Unbounded => {}
			Bound::Included(v) => v.fmt_sql(f, fmt),
			Bound::Excluded(v) => {
				v.fmt_sql(f, fmt);
				f.push('>');
			}
		}
		match &self.end {
			Bound::Unbounded => f.push_str(".."),
			Bound::Excluded(v) => {
				f.push_str("..");
				v.fmt_sql(f, fmt);
			}
			Bound::Included(v) => {
				f.push_str("..=");
				v.fmt_sql(f, fmt);
			}
		}
	}
}

impl ToSql for LogicalDestructureField {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			LogicalDestructureField::All(name) => {
				write_sql!(f, fmt, "{}.*", EscapeKwFreeIdent(name.as_str()))
			}
			LogicalDestructureField::Field(name) => {
				write_sql!(f, fmt, "{}", EscapeKwFreeIdent(name.as_str()))
			}
			// The body is written as an idiom, so its leading field is bare.
			LogicalDestructureField::Aliased(name, body) => {
				write_sql!(f, fmt, "{}: ", EscapeKwFreeIdent(name.as_str()));
				fmt_path(body, f, fmt, BodyRoot::Bare);
			}
			LogicalDestructureField::Destructure(name, fields) => {
				write_sql!(f, fmt, "{}", EscapeKwFreeIdent(name.as_str()));
				fmt_destructure(fields, f, fmt);
			}
		}
	}
}

impl ToSql for LogicalRecurseInstruction {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			LogicalRecurseInstruction::Path {
				inclusive,
			} => {
				f.push_str("path");
				if *inclusive {
					f.push_str("+inclusive");
				}
			}
			LogicalRecurseInstruction::Collect {
				inclusive,
			} => {
				f.push_str("collect");
				if *inclusive {
					f.push_str("+inclusive");
				}
			}
			LogicalRecurseInstruction::Shortest {
				expects,
				inclusive,
			} => {
				write_sql!(f, fmt, "shortest={expects}");
				if *inclusive {
					f.push_str("+inclusive");
				}
			}
		}
	}
}

impl ToSql for LogicalLookup {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		// The short form `->edge` is only unambiguous for a single subject
		// with no clauses, no referencing field and no open-ended range.
		let short = self.what.len() <= 1
			&& self.what.iter().all(|subject| {
				subject.referencing_field().is_none()
					&& !matches!(
						subject,
						LogicalLookupSubject::Range {
							range: LogicalRecordIdKeyRange {
								end: Bound::Unbounded,
								..
							},
							..
						}
					)
			}) && self.cond.is_none()
			&& self.alias.is_none()
			&& self.expr.is_none();
		if short {
			self.kind.fmt_sql(f, fmt);
			if self.what.is_empty() {
				f.push('?');
			} else {
				write_sql!(f, fmt, "{}", Fmt::comma_separated(self.what.iter()));
			}
			return;
		}
		write_sql!(f, fmt, "{}(", self.kind);
		if let Some(fields) = &self.expr {
			write_sql!(f, fmt, "SELECT {} FROM ", fields);
			if self.only {
				f.push_str("ONLY ");
			}
		}
		if self.what.is_empty() {
			f.push('?');
		} else {
			write_sql!(f, fmt, "{}", Fmt::comma_separated(&self.what));
		}
		if let Some(cond) = &self.cond {
			write_sql!(f, fmt, " WHERE {}", Cover(cond));
		}
		if let Some(split) = &self.split {
			write_sql!(f, fmt, " SPLIT ON {}", Fmt::comma_separated(split));
		}
		if let Some(group) = &self.group {
			if group.is_empty() {
				f.push_str(" GROUP ALL");
			} else {
				write_sql!(f, fmt, " GROUP BY {}", Fmt::comma_separated(group));
			}
		}
		if let Some(order) = &self.order {
			write_sql!(f, fmt, " {order}");
		}
		if let Some(limit) = &self.limit {
			write_sql!(f, fmt, " LIMIT {}", Cover(limit));
		}
		if let Some(start) = &self.start {
			write_sql!(f, fmt, " START {}", Cover(start));
		}
		if let Some(alias) = &self.alias {
			write_sql!(f, fmt, " AS {alias}");
		}
		f.push(')');
	}
}

impl ToSql for LogicalLookupSubject {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			LogicalLookupSubject::Table {
				table,
				referencing_field,
			} => {
				write_sql!(f, fmt, "{}", EscapeKwFreeIdent(table.as_str()));
				if let Some(field) = referencing_field {
					write_sql!(f, fmt, " FIELD {}", EscapeKwFreeIdent(field));
				}
			}
			LogicalLookupSubject::Range {
				table,
				range,
				referencing_field,
			} => {
				write_sql!(f, fmt, "{}:{range}", EscapeKwFreeIdent(table.as_str()));
				if let Some(field) = referencing_field {
					write_sql!(f, fmt, " FIELD {}", EscapeKwFreeIdent(field));
				}
			}
		}
	}
}

impl ToSql for LogicalCall {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let args = || Fmt::comma_separated(self.args.iter().map(Cover));
		match &self.receiver {
			Function::Normal(name) => {
				for (i, segment) in name.split("::").enumerate() {
					if i == 0 {
						write_sql!(f, fmt, "{}", EscapeIdent(segment));
					} else {
						f.push_str("::");
						f.push_str(segment);
					}
				}
			}
			Function::Custom(name) => {
				f.push_str("fn");
				for segment in name.split("::") {
					write_sql!(f, fmt, "::{}", EscapeKwFreeIdent(segment));
				}
			}
			Function::Script(script) => {
				write_sql!(f, fmt, "function({}) {{{}}}", args(), script);
				return;
			}
			Function::Model(model) => write_sql!(f, fmt, "{model}"),
			Function::Module(module, sub) => {
				f.push_str("mod::");
				write_sql!(f, fmt, " {}", EscapeKwFreeIdent(module));
				if let Some(sub) = sub {
					for segment in sub.split("::") {
						write_sql!(f, fmt, "::{}", EscapeKwFreeIdent(segment));
					}
				}
			}
			Function::Silo {
				org,
				pkg,
				major,
				minor,
				patch,
				sub,
			} => {
				write_sql!(
					f,
					fmt,
					"silo::{}::{}::<{major}.{minor}.{patch}>",
					EscapeKwFreeIdent(org),
					EscapeKwFreeIdent(pkg),
				);
				if let Some(sub) = sub {
					for segment in sub.split("::") {
						write_sql!(f, fmt, "::{}", EscapeKwFreeIdent(segment));
					}
				}
			}
		}
		write_sql!(f, fmt, "({})", args());
	}
}

impl ToSql for LogicalClosure {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('|');
		for (i, (name, kind)) in self.args.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			write_sql!(f, fmt, "{name}: ");
			if let Kind::Either(_) = kind {
				write_sql!(f, fmt, "<{kind}>");
			} else {
				write_sql!(f, fmt, "{kind}");
			}
		}
		f.push('|');
		if let Some(returns) = &self.returns {
			write_sql!(f, fmt, " -> {returns}");
		}
		// A body starting with a path is parenthesised so a leading `->` is
		// not read as a return-kind arrow.
		if self.body.has_left_idiom() {
			write_sql!(f, fmt, " ({})", self.body);
		} else {
			write_sql!(f, fmt, " {}", Cover(&self.body));
		}
	}
}

impl ToSql for LogicalIf {
	/// Always the `IF … THEN … ELSE … END` form on one line: a plan has no
	/// bracketed-block form, since a block is a nested plan.
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		for (i, (cond, then)) in self.branches.iter().enumerate() {
			if i > 0 {
				f.push_str(" ELSE ");
			}
			write_sql!(f, fmt, "IF {} THEN {then}", Cover(cond));
		}
		if let Some(otherwise) = &self.otherwise {
			write_sql!(f, fmt, " ELSE {}", Cover(otherwise));
		}
		f.push_str(" END");
	}
}

impl ToSql for LogicalFields {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			LogicalFields::Value {
				expr,
				alias,
			} => {
				write_sql!(f, fmt, "VALUE {}", Cover(expr));
				if let Some(alias) = alias {
					write_sql!(f, fmt, " AS {alias}");
				}
			}
			LogicalFields::Select(fields) => {
				write_sql!(f, fmt, "{}", Fmt::comma_separated(fields))
			}
		}
	}
}

impl ToSql for LogicalField {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			LogicalField::All => f.push('*'),
			// A column's kind and frame are plan text, so a rendered layer
			// shows which values are coerced and which bodies run narrowed.
			LogicalField::Single {
				expr,
				alias,
				kind,
				frame,
			} => {
				write_sql!(f, fmt, "{}", Cover(expr));
				if let Some(alias) = alias {
					write_sql!(f, fmt, " AS {alias}");
				}
				if let Some(kind) = kind {
					write_sql!(f, fmt, " : {kind}");
				}
				if let Some(rendered) = super::render_frame(frame) {
					f.push(' ');
					f.push_str(&rendered);
				}
			}
		}
	}
}

impl ToSql for LogicalOrdering {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			LogicalOrdering::Random => f.push_str("ORDER BY RAND()"),
			LogicalOrdering::Order(orders) => {
				write_sql!(f, fmt, "ORDER BY {}", Fmt::comma_separated(orders))
			}
		}
	}
}

impl ToSql for LogicalOrder {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.value.fmt_sql(f, fmt);
		if self.collate {
			f.push_str(" COLLATE");
		}
		if self.numeric {
			f.push_str(" NUMERIC");
		}
		if !self.direction {
			f.push_str(" DESC");
		}
	}
}

impl ToSql for SourceRef {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			SourceRef::Table(table) => EscapeIdent(table.as_str()).fmt_sql(f, fmt),
			SourceRef::RecordId(rid) => rid.fmt_sql(f, fmt),
			SourceRef::Mock(mock) => mock.fmt_sql(f, fmt),
			SourceRef::Param(param) => param.fmt_sql(f, fmt),
			SourceRef::Dynamic(expr) => expr.fmt_sql(f, fmt),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::convert::Infallible;

	use super::*;

	fn expr(sql: &str) -> LogicalExpr {
		let ast = surrealdb_syn::parse(&format!("RETURN {sql}")).expect("expression should parse");
		let crate::sql::TopLevelExpr::Expr(crate::sql::Expr::Return(out)) =
			ast.expressions.into_iter().next().expect("one statement")
		else {
			panic!("expected a RETURN");
		};
		crate::expr::logical::lower_expr(out.what).expect("expression should lower")
	}

	/// Occurrences of `$p` the walk reaches.
	fn count_p(e: &LogicalExpr) -> usize {
		let mut n = 0;
		let _ = e.visit(&mut |x| {
			if matches!(x, LogicalExpr::Param(p) if p.as_str() == "p") {
				n += 1;
			}
			ControlFlow::<()>::Continue(())
		});
		n
	}

	/// `$p` in every position a scalar nests an expression, with the number of
	/// occurrences the walk must reach. A position the walk skips shows up as
	/// an uncounted parameter.
	fn nested_positions() -> Vec<(&'static str, LogicalExpr, usize)> {
		vec![
			("array elements", expr("[$p, $p]"), 2),
			("set elements", LogicalExpr::Set(vec![expr("$p"), expr("$p")]), 2),
			("object values", expr("{ a: $p, b: $p }"), 2),
			("record-id array key", expr("t:[$p, $p]"), 2),
			("record-id object key", expr("t:{ a: $p }"), 1),
			("record-id range key", expr("t:[$p]..=[$p]"), 2),
			("filter predicate", expr("a[WHERE b = $p]"), 1),
			("index", expr("a[$p]"), 1),
			("path start", expr("$p.a"), 1),
			("method arguments", expr("a.slice($p, $p)"), 2),
			("broadcast body", expr("a.b[$p]"), 1),
			("optional body", expr("a.?.b[$p]"), 1),
			("destructure alias", expr("a.{ b: c[$p] }"), 1),
			("nested destructure", expr("a.{ b.{ c: d[$p] } }"), 1),
			("lookup condition", expr("->(likes WHERE x = $p)"), 1),
			(
				"lookup projection and clauses",
				expr("->(SELECT a[$p] FROM likes WHERE x = $p LIMIT $p START $p)"),
				4,
			),
			("recurse nest", expr("a.{2..}(.b[$p])"), 1),
			("recurse trailing body", expr("a.{2..}.b[$p]"), 1),
			("recurse shortest", expr("a.{1..3+shortest=$p}"), 1),
			("prefix operand", expr("-$p"), 1),
			("cast operand", expr("<int> $p"), 1),
			("postfix operand", expr("$p.."), 1),
			("postfix call arguments", expr("$f($p, $p)"), 2),
			("binary operands", expr("$p + $p"), 2),
			("call arguments", expr("count($p, $p)"), 2),
			("closure body", expr("|$x: int| $x + $p"), 1),
			("if branches", expr("IF $p THEN $p ELSE IF $p THEN $p ELSE $p END"), 5),
			("throw", expr("THROW $p"), 1),
			("set_field base and value", set_field(expr("$p"), "f", expr("$p")), 2),
			("subquery, not entered", expr("[(SELECT * FROM t WHERE a = $p)]"), 0),
		]
	}

	fn set_field(base: LogicalExpr, field: &str, value: LogicalExpr) -> LogicalExpr {
		LogicalExpr::SetField {
			base: Box::new(base),
			field: Strand::new(field),
			value: Box::new(value),
		}
	}

	#[test]
	fn set_field_is_a_value_not_a_path() {
		let guard = set_field(expr("a"), "b", expr("IF p THEN a.b ELSE NONE END"));
		// Not a read of a position: no step base, no root but itself.
		assert!(!guard.is_path());
		assert_eq!(guard.step_base(), None);
		assert_eq!(guard.path_root(), &guard);
		assert!(guard.read_only());
		assert!(!set_field(expr("a"), "b", expr("fn::write()")).read_only());
		// Plan text, since the form has no surface syntax: the key is quoted
		// as a string literal is, so a name containing a quote or a space
		// reads back unambiguously, and the arguments render as a call's do.
		assert_eq!(guard.to_sql(), "set_field(a, 'b', (IF p THEN a.b ELSE NONE END))");
		assert_eq!(set_field(expr("a"), "b", expr("x + 1")).to_sql(), "set_field(a, 'b', x + 1)");
		assert_eq!(set_field(expr("a"), "it's", expr("1")).to_sql(), r#"set_field(a, "it's", 1)"#);
		// A projection names it by its rendering, as any non-path, so a
		// statement inside it renders into the name.
		assert_eq!(guard.output_path(), OutputPath::field(guard.to_sql()));
		assert!(guard.output_name_renders_a_statement());
		assert!(!set_field(expr("a"), "b", expr("x + 1")).output_name_renders_a_statement());
		// The base and the value are its children; the key is a name.
		assert_eq!(guard.children().len(), 2);
	}

	#[test]
	fn the_walk_reaches_every_nested_position_and_stops_at_a_subquery() {
		for (position, e, want) in nested_positions() {
			assert_eq!(count_p(&e), want, "{position}: {e}");
		}
	}

	#[test]
	fn visit_mut_rewrites_the_same_positions() {
		for (position, mut e, want) in nested_positions() {
			let mut rewritten = 0;
			let Ok(()) = e.visit_mut(&mut |x| {
				if matches!(x, LogicalExpr::Param(p) if p.as_str() == "p") {
					*x = LogicalExpr::Literal(LogicalLiteral::Integer(1));
					rewritten += 1;
				}
				Ok::<(), Infallible>(())
			});
			assert_eq!(rewritten, want, "{position}");
			assert_eq!(count_p(&e), 0, "{position}: {e}");
		}
	}

	#[test]
	fn an_identity_transform_rebuilds_the_same_expression() {
		for (position, e, _) in nested_positions() {
			let Ok(rebuilt) = e.clone().transform(&mut Ok::<LogicalExpr, Infallible>);
			assert_eq!(rebuilt, e, "{position}");
		}
	}

	#[test]
	fn a_path_names_its_output_by_its_naming_steps() {
		let field = |name: &str| OutputPathPart::Field(name.into());
		let lookup = |name: &str| OutputPathPart::Lookup(name.into());
		// (path, the keys it lands at, whether it was written as those keys)
		let cases: Vec<(&str, Vec<OutputPathPart>, bool)> = vec![
			("a", vec![field("a")], true),
			("a.b", vec![field("a"), field("b")], true),
			// Steps that select or transform do not name, and make the path
			// inexact.
			("tags[WHERE type = 'x'][0].value", vec![field("tags"), field("value")], false),
			("a[0]", vec![field("a")], false),
			("a[$]", vec![field("a")], false),
			("a.*", vec![field("a")], false),
			("a.?.b", vec![field("a"), field("b")], false),
			("a.len()", vec![field("a")], false),
			("a.{ b, c }", vec![field("a")], false),
			// A trailing recursion body is part of the path; a nested one is
			// not.
			("a.{..}.b", vec![field("a"), field("b")], false),
			("a.{..}(.b)", vec![field("a")], false),
			// Lookups name themselves as they render.
			("->likes->person", vec![lookup("->likes"), lookup("->person")], true),
			(
				"->likes->person.name",
				vec![lookup("->likes"), lookup("->person"), field("name")],
				true,
			),
			// A lookup alias names the whole path.
			("->(likes AS l)->person.name", vec![field("l")], false),
			("->(likes AS l)", vec![field("l")], true),
			// A path from an expression is one key spelled as it renders.
			("$p.a", vec![field("$p.a")], false),
			("(1 + 2).b", vec![field("(1 + 2).b")], false),
			// Non-paths.
			("$p", vec![field("p")], true),
			("count()", vec![field("count")], true),
			("fn::f()", vec![field("fn::f")], true),
			("'x'", vec![field("x")], true),
			("1 + 2", vec![field("1 + 2")], true),
		];
		for (sql, parts, exact) in cases {
			let got = expr(sql).output_path();
			assert_eq!(got.parts(), parts.as_slice(), "output path of `{sql}`");
			assert_eq!(got.exact, exact, "exactness of `{sql}`");
		}
		// The same keys written with and without a non-naming step are
		// distinct paths.
		assert_ne!(expr("a[0]").output_path(), expr("a").output_path());
		assert_eq!(expr("a.b").output_path().name(), "a.b");
		assert_eq!(expr("a").output_path().name(), "a");
		assert_eq!(expr("->likes->person").output_path().name(), "->likes->person");
		assert_eq!(expr("`a.b`").output_path().name(), "a.b");
		assert_eq!(expr("`a.b`").output_path(), OutputPath::field("a.b"));
		assert!(expr("id").output_path().is_field("id"));
	}

	#[test]
	fn a_path_root_is_the_expression_it_starts_from() {
		assert_eq!(expr("a.b[0].c").path_root(), &LogicalExpr::Field(Strand::new("a")));
		assert_eq!(expr("$p.a[0]").path_root(), &expr("$p"));
		assert_eq!(expr("->likes->person").path_root(), &LogicalExpr::Current);
		assert_eq!(expr("1 + 2").path_root(), &expr("1 + 2"));
		assert!(expr("a[0]").is_path());
		assert!(!expr("$p").is_path());
	}

	/// `Eq` is asserted for the literal, so every variant must equal itself.
	/// The hand-written `PartialEq` falls through to `false` for a variant it
	/// does not list, which would break structural comparison silently.
	#[test]
	fn every_literal_variant_equals_itself() {
		// Exhaustive so a new variant fails to compile until it is sampled.
		fn _forces_this_list_to_be_updated(l: &LogicalLiteral) {
			match l {
				LogicalLiteral::None
				| LogicalLiteral::Null
				| LogicalLiteral::UnboundedRange
				| LogicalLiteral::Bool(_)
				| LogicalLiteral::Float(_)
				| LogicalLiteral::Integer(_)
				| LogicalLiteral::Decimal(_)
				| LogicalLiteral::String(_)
				| LogicalLiteral::Bytes(_)
				| LogicalLiteral::Regex(_)
				| LogicalLiteral::Duration(_)
				| LogicalLiteral::Datetime(_)
				| LogicalLiteral::Uuid(_)
				| LogicalLiteral::Geometry(_)
				| LogicalLiteral::File(_) => {}
			}
		}
		let samples = [
			LogicalLiteral::None,
			LogicalLiteral::Null,
			LogicalLiteral::UnboundedRange,
			LogicalLiteral::Bool(true),
			LogicalLiteral::Float(1.5),
			LogicalLiteral::Float(f64::NAN),
			LogicalLiteral::Integer(7),
			LogicalLiteral::Decimal(Decimal::new(150, 2)),
			LogicalLiteral::String(Strand::new("s")),
			LogicalLiteral::Bytes(Bytes::from(vec![1u8])),
			LogicalLiteral::Regex("a".parse::<Regex>().expect("regex")),
			LogicalLiteral::Duration(Duration::from_secs(1)),
			LogicalLiteral::Datetime(Datetime::MIN_UTC),
			LogicalLiteral::Uuid(Uuid::nil()),
			LogicalLiteral::Geometry(Geometry::Point(geo::Point::new(1.0, 2.0))),
			LogicalLiteral::File(File::new("b".to_owned(), "p".to_owned())),
		];
		for sample in samples {
			assert_eq!(sample, sample.clone(), "{sample:?} must equal itself");
		}
		assert_ne!(LogicalLiteral::Float(-0.0), LogicalLiteral::Float(0.0));
	}
}
