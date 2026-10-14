//! The operations of the expression dialect.
//!
//! One [`ExprOp`] variant per scalar operation. A variant's payload is the op's attribute set:
//! plain data that is part of the op's identity. Sub-expressions are never payload; they are
//! operands, or regions where evaluation is lazy. A name that LET, FOR, an argument or a capture
//! binds is an SSA value, so no op reads it by name; a free name is [`ExprOp::FreeParam`].
//!
//! A payload larger than 24 bytes is boxed, so the op storage of a container stays dense.
//!
//! [`ExprTag`] is the payload-free twin of [`ExprOp`], and [`ExprOp::tag`] is total.

#![deny(clippy::wildcard_enum_match_arm)]

use std::ops::Bound;
use std::sync::Arc;

use surrealdb_strand::{Strand, TableName};

use crate::exec::function::FunctionRegistry;
use crate::exec::ir::iface::{CatalogObjectKey, IndexRef, Place};
use crate::exec::logical::pipelines::AggregateCombine;
use crate::exec::vm::compile_cache::ObjectVersion;
use crate::exec::vm::payload::Refusal;
use crate::expr::logical::LogicalPostfixOperator;
pub(crate) use crate::expr::logical::MisplacedControl;
use crate::expr::{BinaryOperator, Constant, Function, Kind, Mock, PrefixOperator, RecordIdKeyGen};
use crate::val::{Uuid, Value};

define_op_tags! {
	/// The payload-free identity of an [`ExprOp`].
	pub(crate) enum ExprTag {
		Literal => "expr.literal",
		Array => "expr.array",
		Set => "expr.set",
		Object => "expr.object",
		RecordId => "expr.record_id",
		FreeParam => "expr.free_param",
		SessionParam => "expr.session_param",
		CatalogParam => "expr.catalog_param",
		Table => "expr.table",
		Mock => "expr.mock",
		Constant => "expr.constant",
		Field => "expr.field",
		ComputedRead => "expr.computed_read",
		Doc => "expr.doc",
		Access => "expr.access",
		Deref => "expr.deref",
		Index => "expr.index",
		First => "expr.first",
		Last => "expr.last",
		Each => "expr.each",
		Flatten => "expr.flatten",
		Dispatch => "expr.dispatch",
		Method => "expr.method",
		Destructure => "expr.destructure",
		Optional => "expr.optional",
		RepeatRecurse => "expr.repeat_recurse",
		SetField => "expr.set_field",
		TupleRow => "expr.tuple_row",
		Prefix => "expr.prefix",
		Convert => "expr.convert",
		Postfix => "expr.postfix",
		Invoke => "expr.invoke",
		Binary => "expr.binary",
		ShortCircuit => "expr.short_circuit",
		Call => "expr.call",
		ClosureCreate => "expr.closure_create",
		If => "expr.if",
		Throw => "expr.throw",
		Subquery => "expr.subquery",
		Yield => "expr.yield",
		Truthy => "expr.truthy",
		Agg => "expr.agg",
		Now => "expr.now",
		Raise => "expr.raise",
		DepthCharge => "expr.depth_charge",
	}
}

/// One operation of the expression dialect.
///
/// Carries no `Eq` or `Hash`: some payloads embed types of the language that implement neither,
/// so identity is the structural digest of the attributes.
#[derive(Clone, Debug)]
pub(crate) enum ExprOp {
	/// A constant value. Collapsed literal aggregates are literals.
	Literal(Box<Value>),
	/// An array whose elements are the operands, in order. Built only when some element is not a
	/// literal.
	Array,
	/// A set whose elements are the operands, in order. Built only when some element is not a
	/// literal.
	Set,
	/// An object; the operands are the values, one per key, in source order.
	Object {
		keys: Box<[String]>,
	},
	/// A record id; the operands are the key's values in the order [`KeyShape`] names them. A
	/// generated key reads entropy.
	RecordId(Box<RecordIdAttrs>),
	/// A name no LET, FOR, argument or capture binds: it resolves through the request's input
	/// bindings and session variables, then a stored parameter at the bound version, then `NONE`.
	/// It never reads a caller's binding.
	FreeParam {
		name: Strand,
	},
	/// `$auth`, `$session`, `$token` and `$access` outside a subscriber frame.
	SessionParam {
		name: Strand,
	},
	/// A stored parameter, or `NONE` when none is defined.
	CatalogParam {
		name: Strand,
	},
	/// A table name used as a value.
	Table {
		name: TableName,
	},
	/// A generated set of record ids; reads nothing.
	Mock(Box<Mock>),
	/// A named constant.
	Constant {
		c: Constant,
	},
	/// The row's field `name`. Operand: the row.
	Field {
		name: Strand,
	},
	/// The row's lazily derived memo of a `COMPUTED` field: derived at the first read in a row and
	/// shared by every later read of that row. Operand: the row.
	ComputedRead(Box<ComputedReadAttrs>),
	/// The record id of the row. Operand: the row.
	Doc,
	/// The field of a value, which may be a record to fetch. Operand: the base.
	Access {
		field: Strand,
	},
	/// An explicit record read, never dropped at plan time. Operand: the record id.
	Deref {
		table_hint: Option<Box<TableName>>,
	},
	/// Operands: the base and the index.
	Index,
	/// The first element. Operand: the base.
	First,
	/// The last element. Operand: the base.
	Last,
	/// Every element, fetching record ids. Operand: the base.
	Each,
	/// One level of nesting removed. Operand: the base. `explicit` is false for the flatten
	/// placed between a lookup and a following filter.
	Flatten {
		explicit: bool,
	},
	/// The dispatch check of a method or invocation. Operands: the base, or the invocation target.
	Dispatch(Box<DispatchTarget>),
	/// The method call. Operands: the dispatch result, the base and the arguments.
	Method {
		name: Strand,
	},
	/// An object of the listed fields of the base. Operand: the base; one `LazyOperand` region
	/// per aliased entry, in order.
	Destructure {
		shape: Box<[DestructureEntry]>,
	},
	/// `NONE` or `NULL` as the base is the result; otherwise the `then` region runs with the base
	/// as its argument. Operand: the base; one `LazyOperand` region.
	Optional,
	/// The point at which a recursion re-enters. Operand: the value so far. Valid only inside the
	/// body of a `Recurse` loop.
	RepeatRecurse,
	/// The base object with one key set; a `NONE` value removes the key. Operands: the base and
	/// the value.
	SetField {
		field: Strand,
	},
	/// One tuple of an `INSERT ... VALUES`. The writes run in the order of `places`, starting from
	/// an empty object. A `NONE` value is written as a value, not as a removal.
	///
	/// Operands: one value per place, in the order of `places`, then the subscript operands the
	/// places name. Each `PlacePart::Dynamic` and `PlacePart::Index` reads its subscript by slot.
	/// Regions: one `Clause(Where)` region per filter part, in the order of the parts. Each
	/// `PlacePart::Filter` reads its region by slot. A subscript and a filter are evaluated as the
	/// write reaches the part. So the op is not a value of its operands alone: it appears only as
	/// an element of the rows of an `INSERT`, and no pass merges, hoists or folds it.
	TupleRow {
		places: Box<[Place]>,
	},
	/// A prefix operator. Operand: the operand. `PrefixOperator::Cast` never appears: a cast is
	/// [`ExprOp::Convert`].
	Prefix(Box<PrefixOperator>),
	/// A cast or coercion to a kind. Operand: the value.
	Convert(Box<ConvertAttrs>),
	/// An open-ended range. Operand: the operand.
	Postfix {
		op: LogicalPostfixOperator,
	},
	/// The application of a closure. Operands: the dispatch result and the arguments.
	Invoke,
	/// A strict binary operator. Operands: the left and the right. The short-circuiting operators
	/// are [`ExprOp::ShortCircuit`].
	Binary {
		op: BinaryOperator,
	},
	/// A lazily evaluated binary operator. Operand: the left; one `LazyOperand` region holds the
	/// right.
	ShortCircuit {
		op: ShortCircuitOp,
	},
	/// A function call. Operands: the arguments, then the row names the call passes
	/// ([`CallAttrs::row_names`]).
	Call(Box<CallAttrs>),
	/// A closure value. Operands: the captured values; one `ClosureBody` region.
	ClosureCreate(Box<ClosureAttrs>),
	/// A conditional in value position. Operand: the first condition; two `ValueIf` regions, the
	/// then arm and the else arm. A further condition nests an `If` in the else region.
	If,
	/// Raises its operand as an error. A terminator.
	Throw,
	/// A statement in value position, run as `NestedStrategy` says. One `Subquery` region, and a
	/// second holding the hashed plan when the strategy is a lookup that keeps one.
	Subquery(NestedStrategy),
	/// The terminator of a region that yields a value. Operands: the value.
	Yield,
	/// The truthiness coercion of a value. Operand: the value.
	Truthy,
	/// An aggregate call. Operands: the arguments, whose evaluation is in a
	/// `ErrorScope(ShapeToNone)` region.
	Agg {
		kind: &'static str,
		combine: AggregateCombine,
	},
	/// A clock read: the statement timestamp, which is one value per statement.
	Now {
		which: NowWhich,
	},
	/// Raises an error known at plan time. A terminator.
	Raise(Box<RaiseKind>),
	/// The depth charge of a call or triggered region that a rewrite removed.
	DepthCharge {
		levels: u32,
	},
}

impl ExprOp {
	/// The payload-free identity of the op. Total: no variant is left unnamed.
	pub(crate) fn tag(&self) -> ExprTag {
		match self {
			ExprOp::Literal(_) => ExprTag::Literal,
			ExprOp::Array => ExprTag::Array,
			ExprOp::Set => ExprTag::Set,
			ExprOp::Object {
				..
			} => ExprTag::Object,
			ExprOp::RecordId(_) => ExprTag::RecordId,
			ExprOp::FreeParam {
				..
			} => ExprTag::FreeParam,
			ExprOp::SessionParam {
				..
			} => ExprTag::SessionParam,
			ExprOp::CatalogParam {
				..
			} => ExprTag::CatalogParam,
			ExprOp::Table {
				..
			} => ExprTag::Table,
			ExprOp::Mock(_) => ExprTag::Mock,
			ExprOp::Constant {
				..
			} => ExprTag::Constant,
			ExprOp::Field {
				..
			} => ExprTag::Field,
			ExprOp::ComputedRead(_) => ExprTag::ComputedRead,
			ExprOp::Doc => ExprTag::Doc,
			ExprOp::Access {
				..
			} => ExprTag::Access,
			ExprOp::Deref {
				..
			} => ExprTag::Deref,
			ExprOp::Index => ExprTag::Index,
			ExprOp::First => ExprTag::First,
			ExprOp::Last => ExprTag::Last,
			ExprOp::Each => ExprTag::Each,
			ExprOp::Flatten {
				..
			} => ExprTag::Flatten,
			ExprOp::Dispatch(_) => ExprTag::Dispatch,
			ExprOp::Method {
				..
			} => ExprTag::Method,
			ExprOp::Destructure {
				..
			} => ExprTag::Destructure,
			ExprOp::Optional => ExprTag::Optional,
			ExprOp::RepeatRecurse => ExprTag::RepeatRecurse,
			ExprOp::SetField {
				..
			} => ExprTag::SetField,
			ExprOp::TupleRow {
				..
			} => ExprTag::TupleRow,
			ExprOp::Prefix(_) => ExprTag::Prefix,
			ExprOp::Convert(_) => ExprTag::Convert,
			ExprOp::Postfix {
				..
			} => ExprTag::Postfix,
			ExprOp::Invoke => ExprTag::Invoke,
			ExprOp::Binary {
				..
			} => ExprTag::Binary,
			ExprOp::ShortCircuit {
				..
			} => ExprTag::ShortCircuit,
			ExprOp::Call(_) => ExprTag::Call,
			ExprOp::ClosureCreate(_) => ExprTag::ClosureCreate,
			ExprOp::If => ExprTag::If,
			ExprOp::Throw => ExprTag::Throw,
			ExprOp::Subquery(_) => ExprTag::Subquery,
			ExprOp::Yield => ExprTag::Yield,
			ExprOp::Truthy => ExprTag::Truthy,
			ExprOp::Agg {
				..
			} => ExprTag::Agg,
			ExprOp::Now {
				..
			} => ExprTag::Now,
			ExprOp::Raise(_) => ExprTag::Raise,
			ExprOp::DepthCharge {
				..
			} => ExprTag::DepthCharge,
		}
	}
}

/// The attributes of a read of a computed field's memo.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ComputedReadAttrs {
	pub(crate) field: Strand,
	/// The definition of the field the read derives from, at the version the plan was bound.
	pub(crate) def: (CatalogObjectKey, ObjectVersion),
}

/// The attributes of a record-id op.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RecordIdAttrs {
	pub(crate) table: TableName,
	pub(crate) key_shape: KeyShape,
}

/// The shape of a record id's key, naming how the op's operands fill it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum KeyShape {
	/// A key with no operands.
	Number(i64),
	String(Strand),
	Uuid(Uuid),
	/// A generated key; reads entropy.
	Generate(RecordIdKeyGen),
	/// One operand per element.
	Array {
		len: u32,
	},
	/// One operand per key, in source order.
	Object {
		keys: Box<[String]>,
	},
	/// A key range. Each included or excluded bound contributes its operands, start before end.
	Range(Box<KeyRange>),
}

/// The bounds of a key range.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct KeyRange {
	pub(crate) start: Bound<KeyBound>,
	pub(crate) end: Bound<KeyBound>,
}

/// The key at one end of a range: any key that is not itself a range.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum KeyBound {
	Number(i64),
	String(Strand),
	Uuid(Uuid),
	Generate(RecordIdKeyGen),
	Array {
		len: u32,
	},
	Object {
		keys: Box<[String]>,
	},
}

/// What a dispatch check names.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DispatchTarget {
	/// A method of the base by name: a registered method on the receiver's type, or a closure
	/// stored under that name on the receiver.
	Method(Strand),
	/// The application of a closure value.
	Invoke,
}

/// One entry of a destructure.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DestructureEntry {
	/// `f.*`: the field, with a record id fetched.
	All(Strand),
	/// `f`: the field.
	Field(Strand),
	/// `name: body`: its body is the next `LazyOperand` region of the op.
	Aliased(Strand),
	/// `f.{ ... }`: a nested destructure of the field.
	Destructure(Strand, Box<[DestructureEntry]>),
	/// `...`: every field not named by another entry of the same pattern. Valid only last.
	Rest,
}

/// Whether a conversion is explicit or implicit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ConvertMode {
	/// `<kind> value`: converts, and fails when it cannot.
	Cast,
	/// A typed binding or declared field: admits a value of the kind and fails otherwise.
	Coerce,
}

/// How a statement in value position runs for the rows that evaluate it.
///
/// Every arm gives the same rows, order and errors as [`NestedStrategy::Apply`]. The other arms
/// change how often the statement's reads happen, never what an evaluation yields.
///
/// The physical phase picks a strategy other than `Apply` from the statement's facts. The regions
/// of the statement then already hold the plan that strategy runs. A binder emits `Apply` alone.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum NestedStrategy {
	/// Run for every evaluation, with the row it is evaluated for bound as its `$parent` and
	/// `$this`. This is the only strategy for a statement that writes, for a statement that reads
	/// the row other than through a join key, and for a statement written inside a statement
	/// that writes.
	Apply,
	/// Run at the first evaluation that reaches it. Its value answers every later evaluation until
	/// the enclosing statement runs again. The first evaluation runs the per-row form, so an error
	/// is raised at the same row, and a statement no row reaches never runs.
	Once,
	/// Run for every evaluation over a `phys.probe` beneath the innermost predicate. The rows
	/// beneath it are read once, at the first evaluation, and kept keyed by `field`. Each
	/// evaluation runs the rest of the statement over the kept rows its key may equal. The first
	/// conjunct of that predicate is `field = outer`, where `outer` is a path into `$parent`. The
	/// statement reads the row nowhere else.
	Hash {
		field: Strand,
	},
	/// Run for every evaluation. It reads the table beneath the predicate through `index`, a B-tree
	/// index over the join field alone, keyed by the row's value, so each evaluation reads only the
	/// records the index lists under its key. With `hashed`, the second region holds a plan over a
	/// probe. The evaluations after the first few in one run of the enclosing statement run that
	/// plan instead.
	Lookup {
		index: Box<IndexRef>,
		hashed: bool,
	},
}

/// The attributes of a function call.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CallAttrs {
	/// The function called.
	pub(crate) function: Function,
	/// How many of the op's operands, at the end, are the values of the row names the call passes
	/// rather than arguments. A function that reads the row it is called for, whether through its
	/// body or by its kind, makes the call depend on the row those names resolve to where it is
	/// written ([`passes_row_names`]). Zero for any other function.
	pub(crate) row_names: u8,
	/// What the physical phase read of the definition of a user-defined function. `None` before
	/// the physical phase, for any other function, and for a call in a stored-clause region, which
	/// is never run.
	pub(crate) callee: Option<CalleeFacts>,
}

/// Whether a call of `function` passes row names: a user-defined, module or silo function whose
/// body reads the row it is called for, and a builtin projection or index function, which reads
/// the row or the match state of the scan that yields it without taking either as an argument.
/// Without the row names such a call would take nothing from the loop it is evaluated in.
pub(crate) fn passes_row_names(function: &Function, registry: &FunctionRegistry) -> bool {
	match function {
		Function::Custom(_)
		| Function::Module(..)
		| Function::Silo {
			..
		} => true,
		Function::Normal(name) => registry.is_projection(name) || registry.is_index_function(name),
		Function::Script(_) | Function::Model(_) => false,
	}
}

impl CallAttrs {
	/// A call of `function` whose operands are all arguments, with nothing read of its definition.
	pub(crate) fn of(function: Function) -> Self {
		Self {
			function,
			row_names: 0,
			callee: None,
		}
	}
}

/// What the physical phase read of a user-defined function's definition.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CalleeFacts {
	/// The function is not defined, or the request's capabilities deny it. Entering a call raises
	/// the error that says which.
	Absent,
	/// The definition the call runs.
	Defined(DefinedCallee),
	/// The definition the plan read, in a statement that may change the catalog as it runs: the
	/// call runs the function's call unit, for the definition the transaction reads when the call
	/// is made, and the program holds no body for it.
	PerCall(DefinedCallee),
	/// The plan cannot know which definition the call meets. A schema statement before the call's
	/// statement may change it, or the statement reads at a `VERSION` stamp. The statement is
	/// planned again when it is reached.
	LateBound,
}

/// The facts of a user-defined function's definition that a call of it holds.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct DefinedCallee {
	/// The hash of the stored definition, which a call compares with the definition it reads when
	/// it runs.
	pub(crate) hash: u64,
	/// The kind of the definition's `PERMISSIONS` clause.
	pub(crate) permission: CalleePermission,
	/// Whether the body's text holds a statement that writes.
	pub(crate) writes: bool,
	/// The computation depth entering the body spends: the way to its shallowest write.
	pub(crate) body_depth: u8,
	/// The most computation depth the body spends at once anywhere in it.
	pub(crate) deepest: u8,
}

/// The kind of a user-defined function's `PERMISSIONS` clause.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CalleePermission {
	/// `FULL`: every caller may call it.
	Full,
	/// `NONE`: a caller whose permissions are checked may not.
	None,
	/// A predicate, judged as a call whose permissions are checked enters. `scope` is how often
	/// its verdict stands within a statement
	/// ([`GuardScope::of`](crate::exec::guard_verdicts::GuardScope::of) of the predicate). It is
	/// `None` where the run raises before it judges one: the clause has no logical form, or the
	/// definition's `AUTH LIMIT` does not parse.
	Predicate {
		scope: Option<crate::exec::guard_verdicts::GuardScope>,
	},
}

impl ExprOp {
	/// A call of `function` whose operands are all arguments.
	pub(crate) fn call(function: Function) -> Self {
		ExprOp::Call(Box::new(CallAttrs::of(function)))
	}
}

impl NestedStrategy {
	/// The number of regions an op that runs under this strategy owns.
	pub(crate) fn region_count(&self) -> usize {
		match self {
			NestedStrategy::Lookup {
				hashed: true,
				..
			} => 2,
			NestedStrategy::Lookup {
				hashed: false,
				..
			}
			| NestedStrategy::Apply
			| NestedStrategy::Once
			| NestedStrategy::Hash {
				..
			} => 1,
		}
	}
}

/// What a failed conversion does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum OnFail {
	Error,
	None,
}

/// The place a conversion was written, which is part of its identity because the error it
/// raises names it. Assigned by the binder; two conversions at different places are never equal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ConvertSite(pub(crate) u32);

/// The attributes of a conversion op.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ConvertAttrs {
	pub(crate) mode: ConvertMode,
	pub(crate) kind: Kind,
	pub(crate) site: ConvertSite,
	pub(crate) on_fail: OnFail,
}

/// The attributes of a closure value.
///
/// The op's operands are the values its body captures, one per name of `captures` in order. The
/// body region's block arguments are the statement timestamp, the parameters, one per operand,
/// then the row of the frame that invokes the closure where `reads_row` holds, then that frame's
/// document root where `reads_root` holds.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ClosureAttrs {
	pub(crate) params: Box<[(Strand, Kind)]>,
	pub(crate) returns: Option<Kind>,
	/// The body as written in canonical SurrealQL: the text a closure's content digest is taken
	/// over.
	pub(crate) body_sql: Arc<str>,
	/// The name each captured operand has where the closure is written, which the value carries
	/// so that the closure's unit reads it by name.
	pub(crate) captures: Box<[Strand]>,
	/// Whether the body reads the row it is invoked over, by a field, `@`, `$this` or a call that
	/// reads its caller's row. Its body region then takes that row as its last block argument.
	pub(crate) reads_row: bool,
	/// Whether the body reads the document root of the frame that invokes it through an index or a
	/// method argument, which is not the row it reads by a field where the frame is an element
	/// filter's body. Its body region then takes the root as the block argument after the row.
	/// Never set where `reads_row` is not.
	pub(crate) reads_root: bool,
	/// The closure as written. Every value the op makes carries it, so a host can compile the body
	/// when a value reaches it with no unit of its program. The IR never reads it. A container
	/// parsed from text holds none, because text does not carry it.
	pub(crate) literal: ClosureLiteral,
}

/// The written form of a closure literal, held beside the attributes that stand for it, or none
/// where a container was built from text.
///
/// It is not part of the op's identity: `body_sql` and the parameters state the same closure, so
/// two attribute sets that differ only in their literal are equal and hash alike.
#[derive(Clone, Default)]
pub(crate) struct ClosureLiteral(pub(crate) Option<Arc<crate::expr::logical::LogicalClosure>>);

impl ClosureLiteral {
	/// The literal `closure` is written as.
	pub(crate) fn of(closure: &crate::expr::logical::LogicalClosure) -> Self {
		Self(Some(Arc::new(closure.clone())))
	}
}

#[cfg(test)]
impl ClosureLiteral {
	/// The literal `|| NONE` with no parameters, written as `body_sql`.
	pub(crate) fn none_written_as(body_sql: &str) -> Self {
		use crate::expr::logical::{LogicalClosure, LogicalExpr, LogicalLiteral};
		Self(Some(Arc::new(LogicalClosure {
			args: Vec::new(),
			returns: None,
			body: LogicalExpr::Literal(LogicalLiteral::None),
			body_sql: Arc::from(body_sql),
		})))
	}
}

impl std::fmt::Debug for ClosureLiteral {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("ClosureLiteral(..)")
	}
}

impl PartialEq for ClosureLiteral {
	fn eq(&self, _: &Self) -> bool {
		true
	}
}

impl Eq for ClosureLiteral {}

impl std::hash::Hash for ClosureLiteral {
	fn hash<H: std::hash::Hasher>(&self, _: &mut H) {}
}

/// A lazily evaluated binary operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ShortCircuitOp {
	And,
	Or,
	NullCoalescing,
	/// The condition of `a ? b : c`.
	TernaryCondition,
}

/// Which reading of the statement clock an op yields.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum NowWhich {
	/// The instant as a datetime.
	Datetime,
	/// Whole seconds since the Unix epoch.
	UnixSeconds,
	UnixMillis,
	UnixMicros,
	UnixNanos,
}

/// An error known at plan time.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RaiseKind {
	/// The error a fold of a constant expression raised.
	Folded(FoldedError),
	/// `BREAK` or `CONTINUE` with no loop written around it.
	InvalidControlFlow,
	/// A transaction-control statement where none may stand.
	Misplaced(MisplacedControl),
	/// A statement the planner refuses to run.
	Refused(Box<Refusal>),
}

/// An error a plan-time fold raised, kept to be raised when the folded expression is reached.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FoldedError {
	pub(crate) text: String,
}

const _: () = {
	assert!(::core::mem::size_of::<ExprOp>() <= 32);
	assert!(::core::mem::size_of::<RaiseKind>() <= 32);
};

#[cfg(test)]
mod tests {
	use std::collections::HashSet;

	use super::ExprTag;

	#[test]
	fn mnemonics_are_unique_and_namespaced() {
		let mut seen = HashSet::new();
		for tag in ExprTag::ALL {
			let name = tag.mnemonic();
			assert!(name.starts_with("expr."), "{name} is outside the dialect");
			assert!(seen.insert(name), "{name} names two tags");
		}
	}

	#[test]
	fn tags_are_listed_in_order() {
		assert!(ExprTag::ALL.windows(2).all(|pair| pair[0] < pair[1]));
	}
}
