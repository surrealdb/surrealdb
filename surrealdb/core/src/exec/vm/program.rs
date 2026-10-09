//! The instruction set, and the program a physical plan lowers to.
//!
//! A program is a sequence of **pipelines**, run in order. A pipeline takes
//! rows from its source — the storage cursor, the rows a sort ordered, the
//! groups an aggregate finished — and runs its body once per row. The body is
//! the pipeline's streaming nodes, as instructions, ending in its sink: the
//! statement's output, a sort, or an aggregate. A sink that holds rows is the
//! source of a later pipeline.
//!
//! A body is a register machine over [`Scalar`]s. Register [`ROW`] denotes the
//! row, read through a [`RowReader`](super::row::RowReader): the stored bytes
//! for a pipeline that runs inside the storage cursor, an in-memory value for
//! any other. An instruction the interpreter cannot answer synchronously — an
//! expression it has no instruction for — hands control back to the host,
//! which evaluates it and resumes the body. A pipeline whose body holds none
//! runs inside the storage cursor.

use std::sync::Arc;

use surrealdb_strand::{Strand, TableName};

use super::codec::TypeId;
use super::codec::key::RouteId;
use super::row::FieldName;
use super::scalar::Scalar;
use crate::exec::field_path::FieldPath;
use crate::exec::{AccessMode, ContextLevel};
use crate::expr::operator::BinaryOperator;
use crate::fnc::operate;
use crate::kvs::Direction;
use crate::val::Value;

/// A register index.
pub(super) type Reg = u16;

/// An instruction index within a body.
pub(super) type Label = u32;

/// The register denoting the row.
pub(super) const ROW: Reg = 0;

/// Where a signal a nested statement raises is taken, by the expression at
/// whose root the statement sits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Catch {
	/// The register a `RETURN`'s value is stored in.
	pub(super) root: Reg,
	/// Where the body resumes once the value is stored.
	pub(super) label: Label,
	/// Whether a `BREAK` or `CONTINUE` is an error there, as it is in a
	/// record-id key element, which no loop encloses.
	pub(super) loops_fail: bool,
}

/// Where a `RETURN` a block statement raises is taken, by the `LET` or
/// `RETURN` whose value the statement is part of: the register the `RETURN`'s
/// value is stored in, and where the program resumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReturnCatch {
	pub(super) value: Reg,
	pub(super) label: Label,
}

assert_plain!(ReturnCatch);

/// The most registers a body's register file holds on the stack. A body's
/// registers may borrow from its row, so they live with the row's visit: on
/// the stack up to this many, and in one allocation made for the visit past
/// it.
pub(super) const STACK_REGISTERS: usize = 64;

/// Where an instruction reads a value from, without copying it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Operand {
	/// A register.
	Reg(Reg),
	/// An entry of [`Program::consts`].
	Const(u32),
	/// Field [`Program::fields`]`[name]` of the row, `NONE` when the row has
	/// no such field.
	Field(u32),
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum Instr {
	/// Coerce the value in `reg` to kind [`Program::kinds`]`[kind]`, as a
	/// field's declared kind coerces it: a value that does not coerce fails
	/// the row with an error naming the record and field
	/// [`Program::fields`]`[field]`.
	CoerceField {
		reg: Reg,
		kind: u32,
		field: u32,
	},
	/// Set the record the computed frames compute to the row's — the record
	/// id in its field [`Program::fields`]`[id]` — while the
	/// columns of a projection layer on its computed fields are evaluated. A
	/// self-dereference is answered from the row's stored fields as the
	/// caller may read them — the object in `doc` when set, the row whole when
	/// `doc` is [`ROW`], and otherwise the row when it carries a key under a
	/// computed name — with every name of
	/// [`Program::computed_names`]`[names]` removed; with neither, from
	/// storage. A row that is no record computes none.
	BeginComputing {
		id: u32,
		doc: Option<Reg>,
		names: u32,
	},
	/// Land a projection layer's columns on the row: remove every field the
	/// object in `cuts` names, then set every field of the object in `sets`.
	/// Hands back to the host; what registers held before it does not
	/// survive it.
	LandRow {
		sets: Reg,
		cuts: Reg,
	},
	/// Replace the row with an object holding it under the field `name`, the
	/// row of a binding table. Hands back to the host; what registers held
	/// before it does not survive it.
	BindRow {
		name: u32,
	},
	/// Jump to `target` when the statement's permissions are not checked,
	/// past a permission the body would otherwise apply.
	JumpIfPermissionsBypassed {
		target: Label,
	},
	/// `dst = field op lit`, or `dst = lit op field` when `reversed`, over a
	/// field read as an integer. A field holding anything but an integer is
	/// compared as `fallback` compares it.
	CompareFieldInt {
		field: u32,
		op: IntCmp,
		lit: i64,
		reversed: bool,
		fallback: CompareOp,
		dst: Reg,
	},
	/// Jump to `target` when the truthiness of [`Instr::CompareFieldInt`]'s
	/// answer for the same operands is `truthy`: the comparison and the branch
	/// on it as one instruction, with no register between them.
	JumpIfFieldInt {
		field: u32,
		op: IntCmp,
		lit: i64,
		reversed: bool,
		fallback: CompareOp,
		truthy: bool,
		target: Label,
	},
	/// Jump to `target` when the truthiness of `op(left, right)` is `truthy`:
	/// [`Instr::Compare`] and the branch on its answer as one instruction.
	JumpIfCompare {
		op: CompareOp,
		left: Operand,
		right: Operand,
		truthy: bool,
		target: Label,
	},
	/// `dst = op(left, right)`.
	Compare {
		op: CompareOp,
		left: Operand,
		right: Operand,
		dst: Reg,
	},
	/// `dst = op(left, right)` for an operator that computes a value from its
	/// two operands: arithmetic, or a range between them.
	Arith {
		op: ArithOp,
		left: Operand,
		right: Operand,
		dst: Reg,
	},
	/// `dst = op(src)` for a prefix or postfix operator.
	Unary {
		op: UnaryOp,
		src: Operand,
		dst: Reg,
	},
	/// `dst` = an empty array.
	NewArray {
		dst: Reg,
	},
	/// Append `value` to the array in `array`.
	ArrayPush {
		array: Reg,
		value: Operand,
	},
	/// `dst` = the value of parameter [`Program::params`]`[param]`, read as
	/// an expression reads it: in permission frame `frame` when set, and
	/// otherwise in the frame the body runs in.
	LoadParam {
		param: u32,
		dst: Reg,
		frame: Option<u16>,
	},
	/// `dst` = the value a block last bound in slot `slot` of the running
	/// state ([`BlockInstr::Bind`]): a `LET` or `FOR` name, resolved to the
	/// binding it reads when the program was lowered.
	LoadSlot {
		slot: u16,
		dst: Reg,
	},
	/// `dst` = key `key` of the sorted row the pipeline is running: a value a
	/// projection shares with the sort beneath it.
	SortKey {
		key: u16,
		dst: Reg,
	},
	/// `dst` = the value in `src` — [`ROW`] the row itself — with the records
	/// the paths of [`Program::fetches`]`[fetch]` reach fetched in place, as
	/// `FETCH` fetches them (`operators::fetch::fetch_fields`), by the host.
	Fetch {
		fetch: u16,
		src: Reg,
		dst: Reg,
	},
	/// `dst` = field [`Program::fields`]`[field]` of the value in `src`, read
	/// in permission frame `frame` when set, as a path step reads it: an object's field, `NONE`
	/// past anything that is not an object, record id, array or geometry; a record id fetched, an
	/// array read element by element, by the host.
	Access {
		src: Operand,
		field: u32,
		dst: Reg,
		frame: Option<u16>,
	},
	/// `dst` = every element of the value in `src` (`.*`), read in
	/// permission frame `frame` when set: an object as it is, an array with
	/// each record-id element fetched and every other kept, a record id
	/// fetched, anything else `NONE`; a fetch by the host.
	Each {
		src: Operand,
		dst: Reg,
		frame: Option<u16>,
	},
	/// `THROW` the value in `src`: fail with its raw string.
	Throw {
		src: Operand,
	},
	/// Begin `[WHERE …]` over the value in `src`: for an array, `dst` = an
	/// empty array and `items` = the array; for a set, `dst` = an empty set
	/// and `items` = the set's elements in order; for anything else,
	/// `dst` = `NONE` and a jump to `none`.
	FilterBegin {
		src: Operand,
		items: Reg,
		dst: Reg,
		none: Label,
	},
	/// Begin a path's broadcast over the value in `src`: for an array,
	/// `dst` = an empty array and `items` = the array; for anything else, a
	/// jump to `other`, where the body is applied to the value itself.
	MapBegin {
		src: Operand,
		items: Reg,
		dst: Reg,
		other: Label,
	},
	/// Add `value` to the collection in `collection`: pushed onto an array,
	/// inserted into a set.
	CollectPush {
		collection: Reg,
		value: Operand,
	},
	/// `dst = op(src)`, a path step over a value: `index` is the index an
	/// `[e]` step reads with, evaluated against the row.
	Nav {
		op: NavOp,
		src: Operand,
		index: Option<Operand>,
		dst: Reg,
	},
	/// Refuse builtin [`Program::calls`]`[call]` unless the session's
	/// capabilities allow it; checked before its arguments are evaluated.
	CallAllowed {
		call: u32,
		frame: Option<u16>,
	},
	/// `dst` = builtin [`Program::calls`]`[call]` applied to `args`, invoked
	/// by name through the function registry in permission frame `frame`
	/// when set, and otherwise in the frame the body runs in.
	Call {
		call: u32,
		args: Vec<Operand>,
		dst: Reg,
		frame: Option<u16>,
	},
	/// Enter user-defined function [`Program::functions`]`[function]`, called
	/// from permission or call frame `outer` when set: read its definition,
	/// check it is the one the program was lowered for, check it may be
	/// called and open call frame `frame`, under the definer's auth, for its
	/// arguments to be evaluated in.
	FunctionEnter {
		function: u32,
		frame: u16,
		outer: Option<u16>,
	},
	/// Bind `args`, the arguments of the function entered into call frame
	/// `frame`, as its definition declares them, into the registers from
	/// `params`, and consume the re-entry the call takes: for a body
	/// inlined.
	FunctionBind {
		function: u32,
		frame: u16,
		args: Vec<Operand>,
		params: Reg,
	},
	/// `dst` = `src`, what the inlined body of the function entered into call
	/// frame `frame` returned, coerced to its return kind; the frame closes.
	FunctionReturn {
		function: u32,
		frame: u16,
		src: Operand,
		dst: Reg,
	},
	/// `dst` = the function entered into call frame `frame`, applied to
	/// `args` and run as its body's subroutine; the frame closes.
	FunctionCall {
		function: u32,
		frame: u16,
		args: Vec<Operand>,
		dst: Reg,
		outer: Option<u16>,
	},
	/// `dst` = `src` converted to an array as builtin
	/// [`Program::calls`]`[call]` converts its argument at 1-based
	/// `position`, raising the error it raises for one that is not.
	ArrayArg {
		src: Operand,
		dst: Reg,
		call: u32,
		position: u8,
	},
	/// The next element of the array in `array`, from position `pos`: into
	/// `elem`, with its position into `index`, advancing `pos`; or a jump to
	/// `done` past the last.
	ElemNext {
		array: Reg,
		pos: Reg,
		elem: Reg,
		index: Reg,
		done: Label,
	},
	/// Bind `args` to the declared parameters of closure
	/// [`Program::closures`]`[closure]`, as a call of it binds them, into the
	/// registers from `params`.
	ClosureBind {
		closure: u32,
		args: Vec<Operand>,
		params: Reg,
	},
	/// `dst` = `src`, what closure [`Program::closures`]`[closure]`'s body
	/// returned, coerced to its return kind.
	ClosureReturn {
		closure: u32,
		src: Operand,
		dst: Reg,
	},
	/// Consume, and give back, the re-entry an invocation takes from the
	/// frame `frame` names: an error where none is left.
	Dive {
		frame: Option<u16>,
	},
	/// `dst` = the value of subquery [`Program::subqueries`]`[subquery]`,
	/// evaluated over the value in `doc` when set and the row otherwise, in
	/// permission frame `frame` when set. A `RETURN` the subquery signals is
	/// the value of the expression it sits at the root of, stored in
	/// `caught`'s register before jumping to its label, when the root takes
	/// one, and a `BREAK` or `CONTINUE` is an error where `caught` says so;
	/// otherwise the signal propagates, and out of a projection layer's
	/// column frame ([`PermissionSpec::column`]) any signal is an error.
	Subquery {
		subquery: u32,
		dst: Reg,
		frame: Option<u16>,
		caught: Option<Catch>,
		doc: Option<Reg>,
	},
	/// `dst` = the value closure literal [`Program::closure_values`]`[closure]`
	/// makes: its parameters bound where it is written captured, those the
	/// program holds itself from `slots`, in the spec's order, and the rest
	/// from the frame.
	MakeClosure {
		closure: u32,
		slots: Vec<Operand>,
		dst: Reg,
	},
	/// `dst` = the closure the object in `recv` holds in field `name`, raising
	/// the unknown-method error naming the receiver's type when it holds none,
	/// as a method call no builtin answers raises it.
	ClosureField {
		name: Strand,
		recv: Operand,
		dst: Reg,
	},
	/// Raise the error an invocation of the value in `src` raises when it is
	/// not a closure.
	RequireClosure {
		src: Operand,
	},
	/// `dst` = the closure in `target` invoked with `args`, from frame
	/// `frame` when set.
	Invoke {
		target: Operand,
		args: Vec<Operand>,
		dst: Reg,
		frame: Option<u16>,
	},
	/// `dst` = graph lookup [`Program::lookups`]`[lookup]` from the value
	/// in `src`: an array of what the hops reach, flattened over an array of origins, or `NONE`
	/// from a value that is neither a record id nor an object. Read in permission
	/// frame `frame` when set, a computed field's frame of the statement's
	/// own principal.
	Lookup {
		lookup: u32,
		src: Operand,
		dst: Reg,
		frame: Option<u16>,
	},
	/// Signal `BREAK`, or `CONTINUE` when not `brk`, out of the expression; in
	/// a permission frame `frame`, which has nowhere to send it, an error.
	Signal {
		brk: bool,
		frame: Option<u16>,
	},
	/// `dst` = the value at [`Program::paths`]`[path]` of the value in `src`,
	/// read structurally, as a sort extracts a key.
	Extract {
		src: Operand,
		path: u32,
		dst: Reg,
	},
	/// `dst` = the distance from the query vector of `rank` to the vector at
	/// [`Program::paths`]`[path]` of the value in `src`, as a brute-force
	/// KNN ranks a row. A row with no such vector, or one the distance does
	/// not apply to, takes no rank: control jumps to `none`.
	KnnRank {
		src: Operand,
		path: u32,
		rank: Arc<KnnRank>,
		dst: Reg,
		none: Label,
	},
	/// `dst` = the record ids mock `mock` generates: refused when they would
	/// take more than `limit` bytes.
	Mock {
		mock: Arc<crate::expr::mock::Mock>,
		limit: usize,
		dst: Reg,
	},
	/// `dst` = recursion `spec` from the value in `src`, the row as its
	/// document; a shortest path's target is the value in `target`.
	Recurse {
		src: Operand,
		spec: Arc<RecurseSpec>,
		target: Option<Operand>,
		dst: Reg,
	},
	/// `dst` = the destructure of the value in `src` into `fields`, in
	/// `frame` when set: the records it fetches are read as the frame reads
	/// them.
	Destructure {
		src: Operand,
		fields: Arc<[DestructureStep]>,
		dst: Reg,
		frame: Option<u16>,
	},
	/// `dst` = the record id [`Program::record_ids`]`[record]` describes,
	/// its computed key parts read from their operands.
	RecordId {
		record: u32,
		dst: Reg,
	},
	/// Resolve method [`Program::methods`]`[method]` on the receiver `recv`,
	/// before its arguments are evaluated: fall through when a builtin implements it for the
	/// receiver's type, jump to `closure` when there is one and the receiver is an object whose
	/// field of the method's name holds a closure, and raise the unknown-method
	/// error otherwise. `closure` is absent for a method whose builtins
	/// implement it for an object, which no closure field reaches.
	MethodResolve {
		method: u32,
		recv: Operand,
		closure: Option<Label>,
	},
	/// `dst` = the builtin method [`Program::methods`]`[method]` resolves to
	/// for `recv`, applied to `recv` then `args`, in permission frame `frame`
	/// when set.
	Method {
		method: u32,
		recv: Operand,
		args: Vec<Operand>,
		dst: Reg,
		frame: Option<u16>,
	},
	/// `dst` = the closure `recv` holds under method
	/// [`Program::methods`]`[method]`'s name, applied to `args`, in
	/// permission frame `frame` when set.
	MethodClosure {
		method: u32,
		recv: Operand,
		args: Vec<Operand>,
		dst: Reg,
		frame: Option<u16>,
	},
	/// Jump to `target` when `cond` is `NONE` or `NULL` — or, when `nullish`
	/// is false, when it is neither.
	JumpIfNullish {
		cond: Operand,
		nullish: bool,
		target: Label,
	},
	/// Jump to `target` when `cond` is `NONE` — or, when `none` is false,
	/// when it is not.
	JumpIfNone {
		cond: Operand,
		none: bool,
		target: Label,
	},
	/// Jump to `target` when the truthiness of `cond` is `truthy`.
	JumpIf {
		cond: Operand,
		truthy: bool,
		target: Label,
	},
	/// While rows remain for paging `limit` to skip, count one off and jump
	/// to `target`.
	Skip {
		limit: u16,
		target: Label,
	},
	/// Count one row against paging `limit`.
	Take {
		limit: u16,
	},
	/// Jump to `done` when paging `limit` admits no more rows.
	JumpIfLimitReached {
		limit: u16,
		done: Label,
	},
	/// `dst = {}`.
	NewObject {
		dst: Reg,
	},
	/// The object `dst` holds as a row to write fields onto: an object as it
	/// is, a geometry as the object of its `type` and `coordinates`, and any
	/// other value as an empty object.
	SeedObject {
		dst: Reg,
	},
	/// `dst` = a new object with each `(path, value)` of `fields` set in turn,
	/// the value at [`Program::paths`]`[path]`: [`Instr::NewObject`] and the
	/// [`Instr::SetField`]s that fill it as one instruction.
	BuildObject {
		dst: Reg,
		fields: Box<[(u32, Operand)]>,
	},
	/// `dst` = the value at [`Program::paths`]`[rest]` of field
	/// [`Program::fields`]`[field]` of the row, read structurally.
	LoadPath {
		dst: Reg,
		field: u32,
		rest: u32,
	},
	/// `dst` = the row, as a value of its own.
	LoadRow {
		dst: Reg,
	},
	/// `dst` = the row, and for a row that is a record id, the record it
	/// names, read by the host with the caller's permissions and computed
	/// fields; to `none` when no record answers.
	FetchRow {
		dst: Reg,
		none: Label,
	},
	/// Remove field [`Program::fields`]`[name]` from the object in `object`.
	RemoveField {
		object: Reg,
		name: u32,
	},
	/// Remove the value at path [`Program::paths`]`[path]` from the object in `object`.
	RemovePath {
		object: Reg,
		path: u32,
	},
	/// Remove from the value in `object` the locations `walk` names. A step
	/// known only once evaluated holds a host pipeline of
	/// [`Program::host_code`], run over the value it is applied to. Hands back
	/// to the host.
	Omit {
		object: Reg,
		walk: Arc<crate::exec::vm::plan::Walk<u16>>,
	},
	/// Set `value` at [`Program::paths`]`[path]` on the object in `object`.
	SetField {
		object: Reg,
		path: u32,
		value: Operand,
	},
	/// Write the bindings projection function [`Program::calls`]`[call]`
	/// answers for `args` and the row onto the object in `object`, as the
	/// projection operator writes a projection function at a projection's
	/// root: at [`Program::paths`]`[path]` when `aliased`, otherwise under the
	/// names the function produces. Hands back to the host.
	WriteProjection {
		call: u32,
		args: Vec<Operand>,
		object: Reg,
		path: u32,
		aliased: bool,
	},
	/// `dst = src`.
	Load {
		dst: Reg,
		src: Operand,
	},
	/// Hand `src` to the statement's output. [`ROW`] hands over the row.
	Emit {
		src: Reg,
	},
	/// [`Instr::Emit`] of `src`, then [`Instr::EndRow`].
	EmitEnd {
		src: Operand,
	},
	/// Build an object from `fields`, emit it to output, then end the row.
	BuildObjectEmit {
		fields: Box<[(u32, Operand)]>,
	},
	/// Drop the row immediately (`Step::Next`) when field `field`'s comparison
	/// against integer `lit` equals `reject`.
	FilterFieldInt {
		field: u32,
		op: IntCmp,
		lit: i64,
		reversed: bool,
		fallback: CompareOp,
		reject: bool,
	},
	/// Drop the row immediately (`Step::Next`) when `op(left, right).is_truthy() == reject`.
	FilterCompare {
		op: CompareOp,
		left: Operand,
		right: Operand,
		reject: bool,
	},
	/// Drop the row immediately (`Step::Next`) when `read(cond).is_truthy() == reject`.
	Filter {
		cond: Operand,
		reject: bool,
	},
	/// Hand the value in `src` — [`ROW`] the row itself — to split `split`,
	/// whose rows run through its pipeline once this body ends.
	SplitPush {
		split: u16,
		src: Reg,
	},
	/// Hand the row to sort `sort`, keyed by the `count` registers from
	/// `keys`.
	SortPush {
		sort: u16,
		keys: Reg,
		count: u16,
	},
	/// Continue at `target` when an earlier row of this run of distinct set
	/// `distinct` equals the row as a value; otherwise remember the row and
	/// continue after.
	Distinct {
		distinct: u16,
		target: Label,
	},
	/// Hand the row to join `join`, which looks up the rows its build table
	/// holds for the `count` registers from `keys` and runs each pairing
	/// through the join's pipeline once this body ends.
	JoinProbe {
		join: u16,
		keys: Reg,
		count: u16,
	},
	/// Jump on the verdict `cell` holds for the run of the statement: to
	/// `admitted` once a verdict that admits rows is held, to `refused` once
	/// one that refuses them is, and on to the next instruction, which
	/// judges the conjuncts the verdict is of, while none is.
	VerdictJump {
		cell: u16,
		admitted: Label,
		refused: Label,
	},
	/// Hold verdict `admits` in `cell` for the rest of the run of the
	/// statement.
	VerdictSet {
		cell: u16,
		admits: bool,
	},
	/// Hand the row, a row of the right input of join `join`, which holds its
	/// left row ([`JoinSpec::streams`]), to the join, which pairs it with the
	/// held row where their keys match and runs the pairing through the join's
	/// pipeline once this body ends.
	JoinPair {
		join: u16,
	},
	/// Set key `index` of the row's group in aggregate `aggregate`.
	GroupKey {
		aggregate: u16,
		index: u16,
		value: Operand,
	},
	/// Find the row's group in aggregate `aggregate` by its key, opening it
	/// if no earlier row did.
	GroupSelect {
		aggregate: u16,
	},
	/// Fold `value` into accumulator `agg` of field `field` of the row's
	/// group in aggregate `aggregate`. A value the accumulator cannot fold is
	/// left out, as the aggregate operator leaves it out.
	AccUpdate {
		aggregate: u16,
		field: u16,
		agg: u16,
		value: Operand,
	},
	/// Continue at `target`.
	Jump {
		target: Label,
	},
	/// Step through the array in `list`: `dst` = the element at the integer
	/// in `at`, and `at` moves past it; jump to `done` once `at` has passed
	/// the last.
	Next {
		list: Reg,
		at: Reg,
		dst: Reg,
		done: Label,
	},
	/// A step of the write body, serviced by the host against the record in
	/// flight. What registers held before it does not survive it.
	Write(WriteInstr),
	/// A step that reads or writes the store or the session, serviced by the
	/// host over the body's registers.
	Store(StoreInstr),
	/// A step of a block's control flow, serviced by the host over the frame
	/// the block runs in.
	Block(BlockInstr),
	/// End this row; the next row starts the body again.
	EndRow,
	/// End the pipeline: it takes no more rows.
	StopPipeline,
	/// Open a region whose instructions run up to the matching
	/// [`Instr::TryEnd`] and whose error that is only about the shape of the
	/// data ([`ControlFlow::is_ignorable`](crate::expr::ControlFlow::is_ignorable))
	/// does not fail the row: the registers are as the region left them, `dst`
	/// is `NONE` when set, and the body continues at `catch`. Any other
	/// error, and any signal, leaves the region as it would leave the body.
	/// Regions nest; an error is caught by the innermost.
	TryNone {
		catch: Label,
		dst: Option<Reg>,
	},
	/// Close the innermost region an [`Instr::TryNone`] or an
	/// [`Instr::MethodRegion`] opened.
	TryEnd,
	/// Open a region, up to the matching [`Instr::TryEnd`], over the body of
	/// method `method` run in place: outside a write's frame, an argument error
	/// raised in it and leaving it is the method's own
	/// ([`super::host::method_arguments_error`]), as when the method calls the
	/// body.
	MethodRegion {
		method: u32,
	},
	/// Open a region, up to the matching [`Instr::TryEnd`], over a
	/// permission predicate holding a statement that writes: an error raised
	/// in it and leaving it names the check that failed, as a scan that
	/// enforces such a permission names it.
	PermissionRegion {
		check: PermissionCheck,
	},
	/// Jump to `done` unless column `field` of the row's group in aggregate
	/// `aggregate` needs a value: it holds none yet, or holds `NONE` and the
	/// column reads the row. A column that does not read the row
	/// ([`AggSpec::fallback_is_constant`]) keeps its first value, `NONE`
	/// included.
	GroupFirstNeeded {
		aggregate: u16,
		field: u16,
		done: Label,
	},
	/// Set column `field` of the row's group in aggregate `aggregate` to the
	/// value of `src`.
	GroupFirstSet {
		aggregate: u16,
		field: u16,
		src: Operand,
	},
	/// `dst` = the index function [`Program::calls`]`[call]` names, bound to
	/// the clause or operator its [`CallSpec::index`] states, applied to
	/// `args` and to the row. The `MATCHES` reference or `KNN` reference
	/// argument is not among `args`.
	CallIndexFn {
		call: u32,
		args: Vec<Operand>,
		dst: Reg,
		frame: Option<u16>,
	},
	/// `dst` = whether the row satisfies `MATCHES` operator
	/// [`Program::matches`]`[matches]`.
	/// `query` holds the right operand's value for an operator whose probe
	/// reads its query when it runs
	/// ([`MatchProbe::query`](crate::exec::vm::matches::MatchProbe::query)), and is `None` for
	/// every other.
	Matches {
		matches: u32,
		query: Option<Operand>,
		dst: Reg,
		frame: Option<u16>,
	},
	/// Enter call [`Program::externals`]`[external]` before its arguments
	/// are evaluated, in permission frame `frame` when set: check the session
	/// may call it, and for a model or module read its definition and judge
	/// its `PERMISSIONS` clause, keeping the definition for
	/// [`Instr::ExternalCall`]. A build without the callee's runtime fails
	/// here.
	ExternalEnter {
		external: u32,
		frame: Option<u16>,
	},
	/// `dst` = call [`Program::externals`]`[external]`, entered by the
	/// [`Instr::ExternalEnter`] before it, made with `args` by the host, in
	/// permission frame `frame` when set: with the value in `this` as the
	/// current value when set, and otherwise the row.
	ExternalCall {
		external: u32,
		args: Vec<Operand>,
		this: Option<Operand>,
		dst: Reg,
		frame: Option<u16>,
	},
	/// `dst` = an empty set, which [`Instr::CollectPush`] inserts into.
	NewSet {
		dst: Reg,
	},
	/// `dst` = the degree [`Program::degrees`]`[degree]` counts of the
	/// record the value in `src` names, answered from the adjacency storage,
	/// and a jump to `done`; fall through, with `dst` untouched, where the
	/// value names no single record or the storage leaves the count to the
	/// call it stands for, which the instructions that follow evaluate.
	Degree {
		degree: u32,
		src: Operand,
		dst: Reg,
		done: Label,
	},
	/// `dst` = the `.@` marker over the value in `src`, in the phase of the
	/// `.@` recursion whose body the program runs ([`RecurseSpec::repeat`]):
	/// in discovery, the value's targets handed to the recursion and the value
	/// itself; in assembly, what the next depth assembled for them. Outside
	/// such a body, an error.
	RepeatRecurse {
		src: Operand,
		dst: Reg,
	},
	/// Jump to `target` when `cond` is not an object.
	JumpIfNotObject {
		cond: Operand,
		target: Label,
	},
	/// Fail with the error a `BREAK` or `CONTINUE` raises where no loop takes
	/// it.
	InvalidControlFlow,
	/// Jump to `target` when `cond` is not a record id.
	JumpIfNotRecordId {
		cond: Operand,
		target: Label,
	},
	/// The row the pipeline runs over into `dst`, or `or` when it runs over
	/// none.
	DocumentRoot {
		or: Operand,
		dst: Reg,
	},
	/// `dst` = what host pipeline [`Program::host_code`]`[pipeline]` emits
	/// run over a copy of the value in `src` as its row, in the current
	/// frame: a permission predicate judged against a record with the
	/// `COMPUTED` fields it reads derived onto the copy. Hands back to the
	/// host; the row the pipeline runs over is left as it is.
	JudgeCopy {
		pipeline: u16,
		src: Reg,
		dst: Reg,
	},
}

/// A step of a block's control flow: the statements it runs, the frame they
/// run in, and the signals that leave it.
///
/// Every statement a block runs is compiled when the query arrives, against
/// the catalog it meets when it runs: the block's schema statements are
/// modelled ahead of it (`vm::speculate`), and a block whose catalog changes
/// cannot all be known ahead is not lowered. Every statement is a group of
/// the program ([`Program::groups`]); a block holding a statement the VM does
/// not lower as one is not lowered at all. Running a block plans and lowers
/// nothing.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum BlockInstr {
	/// Run the group of statement [`Program::groups`]`[statement]`: its
	/// pipelines, which are this program's, over this program's state, in the
	/// current frame; `dst` = its value as `collect` gathers it. A `BREAK` or
	/// `CONTINUE` it raises jumps to `on_break` or `on_continue` when set, and
	/// otherwise leaves the program.
	RunEmbedded {
		/// The statement's group, [`Program::groups`]`[statement]`.
		statement: u16,
		dst: Reg,
		collect: Collect,
		on_break: Option<Label>,
		on_continue: Option<Label>,
		/// A `RETURN` the statement raises and its `collect` does not take is
		/// stored in `value`, and the program jumps to `label`; otherwise it
		/// leaves the program.
		on_return: Option<ReturnCatch>,
		/// For a late-bound statement, or one after a schema statement of the
		/// program, the context it requires: the session's namespace and
		/// database are resolved again as far as that before it runs, since a
		/// statement before it may have created, removed or replaced either.
		resolve: Option<ContextLevel>,
	},
	/// Bind parameter [`Program::fields`]`[name]` to the value in `value`:
	/// in slot `slot` of the running state, which the program's own reads of
	/// the name take ([`Instr::LoadSlot`]), and — when `frame`, because
	/// something the program does not lower reads its parameters — in the
	/// current frame.
	Bind {
		name: u32,
		value: Reg,
		slot: u16,
		frame: bool,
	},
	/// The value in `value` coerced to kind [`Program::kinds`]`[kind]`, as
	/// `LET $name` declares it; raises what the coercion refuses.
	CoerceKind {
		value: Reg,
		kind: u32,
		name: u32,
	},
	/// Raise when the value in `value` nests deeper than a value may.
	///
	/// A block threads one frame through every statement, so a `LET` that
	/// rebinds its own parameter to a value built from it grows the value
	/// without any storage write to bound it; the bound is checked where the
	/// binding is made.
	CheckDepth {
		value: Reg,
	},
	/// Save the current frame; [`Self::Restore`] returns to the last saved.
	Save,
	Restore,
	/// Resolve the session's namespace and database again, as far as the
	/// catalog allows, in the current frame and in every saved one: a schema
	/// statement just run may have created, removed or replaced either.
	Resolve,
	/// Raise `RETURN` with the value in `value`.
	Return {
		value: Reg,
	},
	/// Start iterating the value in `src` as a `FOR` range, into iterator
	/// `slot`: an array's elements, or a range's integers.
	IterStart {
		src: Reg,
		slot: u16,
	},
	/// `dst` = iterator `slot`'s next element; jump to `done` when it has
	/// none left.
	IterNext {
		slot: u16,
		dst: Reg,
		done: Label,
	},
	/// `SLEEP` for the duration in `duration`, cut short by the statement's
	/// remaining time or its cancellation.
	Sleep {
		duration: Reg,
	},
	/// `dst` = the value of nested statement [`Program::nested`]`[nested]`:
	/// its group run over this program's state, in the frame the statement
	/// is evaluated in — permission frame `frame` when set — with the row as
	/// its `$parent` and `$this`, and its rows gathered as a subquery gathers
	/// them: a scalar statement's last value or `NONE`, otherwise its rows as
	/// an array. A `RETURN` it signals is the value of the expression it sits
	/// at the root of, stored in `caught`'s register before jumping to its
	/// label, when the root takes one, and a `BREAK` or `CONTINUE` is an
	/// error where `caught` says so; otherwise the signal propagates.
	Nested {
		nested: u32,
		dst: Reg,
		frame: Option<u16>,
		caught: Option<Catch>,
	},
	/// `dst` = whether `probe` is a member (not a member when `negate`) of the
	/// rows of nested statement [`Program::nested`]`[nested]`, as the
	/// `INSIDE` (`NOT INSIDE`) of the probe over them answers, kept as a
	/// membership index from the first evaluation that reaches it, as
	/// [`Self::Nested`] keeps a `once` value. The statement runs as
	/// [`Self::Nested`] runs it, and its `caught` signals are taken as
	/// there.
	Member {
		nested: u32,
		probe: Reg,
		dst: Reg,
		negate: bool,
		frame: Option<u16>,
		caught: Option<Catch>,
	},
	/// Run late-bound statement [`Program::served`]`[served]`, written as the
	/// running operator's [`super::ServedStatements`] hold it: planned for
	/// the catalog the transaction reads when the block reaches it, served
	/// from the compile cache by its text and the frame it runs in, and run in
	/// the current frame; `dst` = its value as `collect` gathers it. A
	/// `BREAK` or `CONTINUE` it raises jumps to `on_break` or `on_continue`
	/// when set, and otherwise leaves the program.
	RunServed {
		served: u16,
		dst: Reg,
		collect: Collect,
		on_break: Option<Label>,
		on_continue: Option<Label>,
		/// A `RETURN` the statement raises and its `collect` does not take is
		/// stored in `value`, and the program jumps to `label`; otherwise it
		/// leaves the program.
		on_return: Option<ReturnCatch>,
		/// The context it requires: the session's namespace and database are
		/// resolved again as far as that before it runs, since a statement
		/// before it may have removed or replaced either.
		resolve: Option<ContextLevel>,
	},
	/// `EXPLAIN ANALYZE` of the group of statement
	/// [`Program::groups`]`[statement]`: the group run as
	/// [`Self::RunEmbedded`] runs it, its rows counted and timed, and `dst` =
	/// the rendering of analysis [`Program::analyses`]`[analysis]` over what
	/// was measured, or `NONE` where `collect` discards it. A `BREAK`,
	/// `CONTINUE` or `RETURN` the group raises ends it, and is not raised.
	RunAnalyzed {
		statement: u16,
		analysis: u16,
		dst: Reg,
		collect: Collect,
		/// For a late-bound statement, or one after a schema statement of the
		/// program, the context it requires, resolved again before it runs as
		/// [`Self::RunEmbedded`] resolves it.
		resolve: Option<ContextLevel>,
	},
}

assert_plain!(BlockInstr);

/// A late-bound statement a block runs ([`BlockInstr::RunServed`]): the names
/// the block binds around it, which it reads when it runs. The statement as
/// written is kept beside the program, at the same index, by the operator that
/// runs it ([`super::ServedStatements`]).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ServedSpec {
	pub(super) shadowed: Vec<String>,
	/// The re-entry depth the block's statements are planned at.
	pub(super) depth: u32,
}

/// What `EXPLAIN ANALYZE` of a statement a block runs renders, fixed when the
/// query arrives: the text of the statement's plan, cut where every operator
/// of it writes the one set of metrics the run of the statement records, or
/// its JSON with those metrics to fill, and whether the volatile metrics are
/// left out.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Analysis {
	/// The rendering of the plan between the places its metrics are written
	/// (`exec::operators::explain::analyze_text_pieces`).
	pub(super) pieces: Vec<String>,
	/// For `FORMAT JSON`, the plan's JSON whose `metrics` objects the run
	/// fills (`exec::operators::explain::analyze_json_template`).
	pub(super) json: Option<super::plain::ValueData<Value>>,
	/// Whether elapsed time and batch counts are left out.
	pub(super) redact: bool,
}

assert_plain!(Analysis);

/// How [`BlockInstr::RunEmbedded`] gathers a statement's value, as the operator it
/// stands in for gathers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Collect {
	/// A block's statement (`collect_shaped_value`): a scalar statement's
	/// last value or `NONE`, and otherwise its rows as an array. A signal
	/// leaves.
	Block,
	/// `RETURN`'s: a `RETURN` raised inside is its last value; a scalar
	/// statement's one value, `NONE`, or its values as an array, and
	/// otherwise its rows as an array.
	Return,
	/// A `LET` value's: a `RETURN` raised inside is its value; a scalar
	/// statement's first value or `NONE`, and otherwise its rows as an array.
	Let,
	/// A `FOR` body's: its values are dropped.
	Discard,
}

/// The refusals of a definition over a LIGHTWEIGHT relation, as plain data a
/// program carries in place of the message the refusal raises.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StoreRefusal {
	/// A field definition over the relation.
	Field,
	/// An index definition over the relation.
	Index,
	/// An event definition over the relation.
	Event,
}

impl StoreRefusal {
	/// The message the refusal raises.
	pub(super) fn message(self) -> &'static str {
		match self {
			Self::Field => {
				"a LIGHTWEIGHT relation cannot carry a field definition: its edges store no records"
			}
			Self::Index => {
				"a LIGHTWEIGHT relation cannot carry an index: its edges store no records"
			}
			Self::Event => {
				"a LIGHTWEIGHT relation cannot carry an event: its edges store no records"
			}
		}
	}
}

/// The clause a schema statement reads a name from, as plain data a program
/// carries in place of the label its coercion refusal names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NameClause {
	Namespace,
	Database,
	Table,
	Event,
	Index,
	IndexTable,
	What,
	Sequence,
	Analyzer,
	Access,
	User,
	Api,
	ApiPath,
	Bucket,
	TargetTable,
	ConfigNamespace,
	ConfigDatabase,
}

impl NameClause {
	/// The kind a coercion refusal names as the one wanted.
	pub(super) fn label(self) -> &'static str {
		match self {
			Self::Namespace => "namespace name",
			Self::Database => "database name",
			Self::Table => "table name",
			Self::Event => "event name",
			Self::Index => "index name",
			Self::IndexTable => "index table",
			Self::What => "what",
			Self::Sequence => "sequence name",
			Self::Analyzer => "analyzer name",
			Self::Access => "access name",
			Self::User => "user name",
			Self::Api => "api name",
			Self::ApiPath => "api path",
			Self::Bucket => "bucket name",
			Self::TargetTable => "target table",
			Self::ConfigNamespace => "namespace",
			Self::ConfigDatabase => "database",
		}
	}
}

/// A step over the store or the session: what a schema statement's program
/// is made of.
///
/// A key is named by its `keyspace!` route and built from the object in a
/// register holding its fields; a value is named by its codec type, and is
/// decoded into or encoded from a register.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum StoreInstr {
	/// Refuse to run in a frame that refuses writes.
	RefuseNoWrite,
	/// The permission check: raise unless the session may take `action` on
	/// `resource` at `base`.
	Allow {
		action: crate::iam::Action,
		resource: crate::iam::ResourceKind,
		base: crate::expr::Base,
	},
	/// `dst` = the value stored at the key, decoded as `ty`, or `NONE`.
	Get {
		route: RouteId,
		key: Reg,
		ty: TypeId,
		dst: Reg,
	},
	/// Stamp table `tb`'s catalog version, in namespace `ns` and database `db`,
	/// as every schema change of the table does: a fresh version, which the
	/// transaction counts so the plans it holds learn the catalog changed.
	StampCatalog {
		ns: Reg,
		db: Reg,
		tb: Reg,
		/// As [`StoreInstr::NextId`]'s `draw`: the fresh version is a draw.
		draw: u16,
		/// As [`StoreInstr::NextId`]'s `reserved`.
		reserved: Option<u32>,
	},
	/// Store `value`, encoded as `ty`, at the key.
	Put {
		route: RouteId,
		key: Reg,
		ty: TypeId,
		value: Reg,
	},
	/// [`Self::Put`] through the store's replace, which a versioned store
	/// applies to the key's latest version.
	Replace {
		route: RouteId,
		key: Reg,
		ty: TypeId,
		value: Reg,
	},
	/// `dst` = field [`Program::fields`]`[name]` of the object in `src`.
	GetField {
		src: Reg,
		name: u32,
		dst: Reg,
	},
	/// `dst` = the next id of `sequence` — within the namespace in `ns`, for
	/// a sequence scoped to one — which the sequence service leases outside the
	/// statement's transaction.
	NextId {
		sequence: IdSequence,
		ns: Option<Reg>,
		db: Option<Reg>,
		dst: Reg,
		/// Which of its statement's id draws this is, in the order lowering
		/// emits them.
		draw: u16,
		/// The id reserved for this draw when the program was planned, as an
		/// index into [`Program::reservations`]: the first run of the draw
		/// takes it rather than leasing another.
		reserved: Option<u32>,
	},
	/// `dst` = the name of the session's database; raises when none is
	/// selected.
	SessionDb {
		dst: Reg,
	},
	/// Raise when the statement runs speculatively (`vm::speculate`): what
	/// follows evaluates what a second evaluation may answer differently, or
	/// that acts outside the transaction, so only the statement's real run
	/// evaluates it, and the statements after it are not planned knowing
	/// what it defines.
	RealRunOnly,
	/// `dst` = a new version-7 uuid, from the clock.
	NewUuid {
		dst: Reg,
		/// As [`StoreInstr::NextId`]'s `draw`.
		draw: u16,
		/// As [`StoreInstr::NextId`]'s `reserved`.
		reserved: Option<u32>,
	},
	/// `dst` = the auth limit a definition made in this session records: the
	/// session's level and its highest role.
	SessionAuthLimit {
		dst: Reg,
	},
	/// Read the key, registering it for commit-time conflict detection, so a
	/// concurrent writer of it cannot also commit.
	Lock {
		route: RouteId,
		key: Reg,
	},
	/// Delete the key.
	Delete {
		route: RouteId,
		key: Reg,
	},
	/// Delete the model artifact from the underlying object store.
	DeleteModelArtifact {
		model: Reg,
	},
	/// Delete every key beneath the bound.
	DeleteRange {
		route: RouteId,
		key: Reg,
	},
	/// Record a table definition change in the transaction's changefeed
	/// buffer, written at commit.
	BufferTableChange {
		ns: Reg,
		db: Reg,
		tb: Reg,
		definition: Reg,
	},
	/// `dst` = the name of the session's namespace; raises when none is
	/// selected.
	SessionNs {
		dst: Reg,
	},
	/// Add one to the counter at the key, wrapping; an absent counter is 0.
	Increment {
		route: RouteId,
		key: Reg,
	},
	/// Evict every definition the datastore has cached.
	ClearDatastoreCache,
	/// Jump to `target` when the statement runs as part of an import.
	JumpIfImport {
		target: Label,
	},
	/// Raise the statement error `error`, naming the value in `arg`.
	Raise {
		error: StatementError,
		arg: Reg,
	},
	/// Raise what refuses `definition`, a table definition, in the database
	/// `ns`, `db`, as the table it redefines and that table's contents stand
	/// when the instruction runs: a LIGHTWEIGHT relation's edges, the fields,
	/// events, indexes and records of a table becoming one, and the INLINE
	/// fields of a relation changing its type.
	RefuseRedefiningTable {
		ns: Reg,
		db: Reg,
		definition: Arc<crate::catalog::TableDefinition>,
	},
	/// Raise what refuses the removal of table `tb` of the database `ns`,
	/// `db`: a non-empty LIGHTWEIGHT relation using it as an endpoint, or the
	/// table being a non-empty LIGHTWEIGHT relation, as the edges stand when
	/// the instruction runs.
	RefuseRemovingLightweight {
		ns: Reg,
		db: Reg,
		tb: Reg,
	},
	/// `dst` = the names of the tables the view `tb` of the database `ns`,
	/// `db` reads from, as the catalog holds its definition when the
	/// instruction runs; an empty array for a table that is no view.
	ViewSources {
		ns: Reg,
		db: Reg,
		tb: Reg,
		dst: Reg,
	},
	/// Raise the error the planner refuses a statement with: a late-bound
	/// statement the planner refuses when the block reaches it.
	Refuse {
		refusal: Box<super::plan::Refusal>,
	},
	/// Evict every definition the transaction has cached.
	ClearCache,
	/// `dst` = an array of the values stored strictly beneath the bound, in
	/// key order, each decoded as `ty`. When `unread` is set, a scan that does
	/// not read jumps there, logged, instead of raising.
	Scan {
		route: RouteId,
		key: Reg,
		ty: TypeId,
		dst: Reg,
		unread: Option<Label>,
	},
	/// [`StoreInstr::Scan`] of the catalog as it stood at the version stamp in
	/// `version` ([`StoreInstr::VersionStamp`]).
	ScanAt {
		route: RouteId,
		key: Reg,
		ty: TypeId,
		version: Reg,
		dst: Reg,
	},
	/// `dst` = the version stamp of the datetime `src` casts to, in the
	/// datastore's timestamps, for a read at a past version.
	VersionStamp {
		src: Reg,
		dst: Reg,
	},
	/// Delete the key and every earlier version of it.
	Clear {
		route: RouteId,
		key: Reg,
	},
	/// Delete every key beneath the bound, and every earlier version of each.
	ClearRange {
		route: RouteId,
		key: Reg,
	},
	/// Ask the store to compact every key beneath the bound.
	Compact {
		route: RouteId,
		key: Reg,
	},
	/// Ask the store to compact its whole keyspace.
	CompactAll,
	/// Set the process's query timeout to the duration in `timeout`, or clear
	/// it for `NONE`.
	SetQueryTimeout {
		timeout: Reg,
	},
	/// From each action in the array in `actions` (objects with a `methods`
	/// array), remove the methods the array in `methods` holds; an action left
	/// with none is removed by swapping the last action into its place.
	DropMethods {
		actions: Reg,
		methods: Reg,
	},
	/// Append the value in `value` to the array in `list`.
	Push {
		list: Reg,
		value: Reg,
	},
	/// `dst` = the definitions of kind `definitions` in the array in `list`,
	/// rendered for `INFO`: an array of each one's structure when
	/// `structured`, and otherwise an object of each one's SurrealQL keyed by
	/// its name, a later definition of a name replacing an earlier. A
	/// structured module reads the database in `scope`, and a structured
	/// sequence reports the bound it resumes from as of `version`, a stamp
	/// ([`StoreInstr::VersionStamp`]), when there is one.
	Render {
		definitions: Definitions,
		structured: bool,
		list: Reg,
		dst: Reg,
		scope: Option<(Reg, Reg)>,
		version: Option<Reg>,
	},
	/// `dst` = the definition of kind `definitions` in `src` rendered for
	/// `INFO`: its structure when `structured`, and otherwise its SurrealQL.
	RenderOne {
		definitions: Definitions,
		structured: bool,
		src: Reg,
		dst: Reg,
	},
	/// `dst` = the value in `src` read as a string, as a name an `INFO`
	/// evaluates is; raises what the coercion refuses.
	CoerceString {
		src: Reg,
		dst: Reg,
	},
	/// `dst` = the value in `src` read as the name a schema statement's clause
	/// `into` gives (`ddl::scalar::ident`), or when `optional`, as the name or
	/// `NONE` an optional clause gives (`ddl::scalar::optional_ident`); a value
	/// of another kind raises the coercion's refusal naming `into` as the kind
	/// wanted.
	CoerceName {
		src: Reg,
		dst: Reg,
		into: NameClause,
		optional: bool,
	},
	/// `dst` = the value in `src` converted by `clause`, as a schema
	/// statement's body converts an evaluated clause; raises what the
	/// conversion refuses.
	Convert {
		src: Reg,
		dst: Reg,
		clause: ClauseConversion,
	},
	/// Raise what `DEFINE ACCESS` raises when `check` fails.
	AccessCheck {
		check: AccessCheck,
	},
	/// The string in `src` parsed as an API path, as `DEFINE API` parses
	/// the path it resolves; raises what the parse refuses. `text` = the path
	/// as its definition stores it, `rendered` = the path the definition is
	/// written at.
	ParseApiPath {
		src: Reg,
		text: Reg,
		rendered: Reg,
	},
	/// `dst` = the value in `src` cast as a schema statement's `COMMENT` is
	/// (`Option<String>`): a string, or `NONE`; raises what the cast refuses.
	CastComment {
		src: Reg,
		dst: Reg,
	},
	/// `dst` = the process's report of `part`, for `INFO FOR ROOT`.
	ProcessInfo {
		part: ProcessPart,
		dst: Reg,
	},
	/// `dst` = cluster-visible build status for `index` on `table`.
	InfoIndex {
		table: Reg,
		index: Reg,
		dst: Reg,
	},
	/// Jump to `target` when the datastore runs without `service`.
	JumpIfNoService {
		service: Service,
		target: Label,
	},
	/// Raise what the body of a schema statement raises of its definition at
	/// this point, as its lowering decided from the catalog the statement was
	/// planned against.
	RefuseField {
		refusal: FieldRefusal,
	},
	/// Raise what `ALTER FIELD`'s body raises when `check` fails of the
	/// definition it has formed.
	FieldCheck {
		check: FieldCheck,
	},
	/// Delete the reference keys the field defined in `old`, of the table
	/// named in `table`, wrote under target tables the field defined in `new`
	/// no longer references, or under any when `new` is absent
	/// (`ddl::define::field::purge_dropped_reference_keys`). Does nothing when
	/// `old` holds no definition or one with no `REFERENCE`.
	PurgeDroppedReferences {
		ns: Reg,
		db: Reg,
		table: Reg,
		old: Reg,
		new: Option<Reg>,
	},
	/// Refuse the table defined in `table` when it is a LIGHTWEIGHT relation,
	/// whose edges store no records for a definition over it to apply to,
	/// raising `refusal`'s message as the definition's body raises it.
	RefuseLightweight {
		table: Reg,
		refusal: StoreRefusal,
	},
	/// Cancel the table's queued reclaim of its shared doc-id space, as a
	/// definition that consumes the space claims it
	/// (`claim_tb_doc_ids_reclaim`); raises while a reclaim is in progress.
	///
	/// With `upgrading`, the stored definition of an index about to be
	/// restamped at the current format: the claim is made only when that
	/// restamp moves the index from its private layout onto the shared space,
	/// and the refusal tells the caller to retry `REBUILD INDEX`.
	ClaimDocIdsReclaim {
		ns: Reg,
		db: Reg,
		tb: Reg,
		upgrading: Option<Reg>,
	},
	/// `dst` = a fresh id for an index of the table named in `tb`.
	NextIndexId {
		ns: Reg,
		db: Reg,
		tb: Reg,
		dst: Reg,
		/// As [`StoreInstr::NextId`]'s `draw`.
		draw: u16,
		/// As [`StoreInstr::NextId`]'s `reserved`.
		reserved: Option<u32>,
	},
	/// `dst` = whether an import replaying the index defined in `template`
	/// over the one defined in `existing` may keep the existing index's data
	/// (`import_replay_can_reuse_index`).
	IndexReplayReuses {
		existing: Reg,
		template: Reg,
		dst: Reg,
	},
	/// `dst` = whether the field defined in `field` admits an index on a
	/// path beneath it: a field of no type, or a type that permits sub-fields.
	FieldAdmitsSubFields {
		field: Reg,
		dst: Reg,
	},
	/// Build the index defined in `index` over the table defined in `table`
	/// with the index builder; when `blocking`, wait for the build to finish.
	/// When `fresh`, the index is created by this transaction, and its build
	/// state is removed if the transaction rolls back; a rebuilt index keeps
	/// the state of the generation it had.
	BuildIndex {
		table: Reg,
		index: Reg,
		blocking: bool,
		fresh: bool,
	},
	/// Evict the process's loaded state for the index defined in `index`, of
	/// the table defined in `table`, and abort a local build of it once the
	/// transaction commits.
	RetireIndexLocally {
		ns: Reg,
		db: Reg,
		table: Reg,
		index: Reg,
	},
	/// Tell the subscriber of the subscription defined in `subscription` that
	/// it is killed, once the transaction commits.
	KillOnCommit {
		subscription: Reg,
	},
	/// `dst` = whether the index defined in `index` reads and writes its
	/// table's shared doc-id space.
	SharesDocIds {
		index: Reg,
		dst: Reg,
	},
	/// Check the segmenter of the analyzer defined in `analyzer`, after
	/// loading the mapper files it names into the process, within the
	/// configured allowlist, when `mappers` is set.
	LoadAnalyzer {
		analyzer: Reg,
		mappers: bool,
	},
	/// Drop the process's analyzer mappers that none of the analyzer
	/// definitions in the array in `analyzers` names.
	CleanupMappers {
		analyzers: Reg,
	},
	/// Evict the process's loaded runtime for the module defined in `module`,
	/// of the database in `ns`/`db`.
	EvictModule {
		ns: Reg,
		db: Reg,
		module: Reg,
	},
	/// Load the process's runtime for the module defined in `module`, of the
	/// database in `ns`/`db`, logging rather than raising a failure.
	LoadModule {
		ns: Reg,
		db: Reg,
		module: Reg,
	},
	/// Register the storage backend of the bucket defined in `bucket`, of the
	/// database in `ns`/`db`, with the process's buckets; raise when the
	/// datastore runs without them.
	ConnectBucket {
		ns: Reg,
		db: Reg,
		bucket: Reg,
	},
	/// Refuse the value in `value` when it nests deeper than a value may be
	/// stored.
	CheckDepth {
		value: Reg,
	},
	/// Record on the transaction that the sequence named in `name` is defined
	/// by it, so a draw later in the transaction is served by the transaction
	/// itself, from the definition it wrote.
	SequenceDefinedHere {
		ns: Reg,
		db: Reg,
		name: Reg,
	},
	/// Evict the sequence service's state for the sequence named in `name`.
	EvictSequence {
		ns: Reg,
		db: Reg,
		name: Reg,
	},
	/// Jump to `target` when the statement runs speculatively
	/// (`vm::speculate`): a view's backfill is left to the statement's real
	/// run.
	JumpIfSpeculating {
		target: Label,
	},
	/// `dst` = the value that host pipeline `pipeline` answers with the value
	/// in `row` as the current value, in the statement's frame.
	EvaluateOver {
		pipeline: u16,
		row: Reg,
		dst: Reg,
	},
	/// Store the row of the materialized view named in `table` that `row`, a
	/// row of the view's backfill select (its source record's `id` and the
	/// view's fields), stands for under the source record's key, then run the
	/// view table's own order for a created row: unit `order`, lowered from the
	/// view table's definitions, or, for a view whose table is named only when
	/// the statement runs, the order lowered for that table when the backfill
	/// meets it.
	BackfillRow {
		table: Reg,
		row: Reg,
		order: Option<UnitId>,
	},
	/// `dst` = the document the fields of an aggregated view are evaluated
	/// over for the group that `row`, a row of the view's backfill select,
	/// answers: the group's keys and its aggregates' values. Jumps to `empty`
	/// when the row stands for no group.
	BackfillGroup {
		backfill: Backfill,
		row: Reg,
		dst: Reg,
		empty: Label,
	},
	/// Store the row of the aggregated view named in `table` that the group
	/// in `row` stands for: its stats as metadata, and as data the value of
	/// each of `fields` written at its path. Then run the view table's own
	/// order for a created row, as [`StoreInstr::BackfillRow`] runs it.
	BackfillGroupRow {
		backfill: Backfill,
		table: Reg,
		row: Reg,
		fields: Vec<(crate::expr::Idiom, Reg)>,
		order: Option<UnitId>,
	},
	/// Jump to `target` when the base the session selects is `base`: the
	/// root with no namespace, the namespace with a namespace and no
	/// database, the database with both. Raises when the session selects a
	/// database with no namespace.
	JumpIfSessionBase {
		base: crate::expr::Base,
		target: Label,
	},
	/// `dst` = the field path `source` names, as the text a stored definition
	/// keeps of it (`Idiom::to_raw_string`); `validated` refuses a part a
	/// stored field name cannot hold (`Idiom::validate_local`). Raises what
	/// resolving the path refuses.
	ResolvePath {
		source: PathSource,
		validated: bool,
		dst: Reg,
	},
	/// Append to the array in `list` the text of each field path `source`
	/// names, as a written list of index columns resolves it
	/// (`ddl::scalar::fields`). Raises what resolving the path refuses.
	ResolveColumns {
		source: ColumnSource,
		list: Reg,
	},
	/// Define the field the definition in `definition` describes, as
	/// `DEFINE FIELD` defines it once its definition is formed: its checks of
	/// the definition against the catalog, the field and its nested fields
	/// written, and the table refreshed. `kind` decides what a field that
	/// exists does.
	DefineField {
		definition: Reg,
		kind: crate::expr::statements::define::DefineKind,
	},
	/// Remove the field the text in `name` names from the table named in
	/// `table`, as `REMOVE FIELD` removes it once both are named.
	RemoveFieldDefinition {
		name: Reg,
		table: Reg,
		if_exists: bool,
	},
	/// Alter the field the text in `name` names of the table named in
	/// `table` by the clauses of `statement`, as `ALTER FIELD` alters it once
	/// both are named.
	AlterFieldDefinition {
		name: Reg,
		table: Reg,
		statement: Box<crate::expr::statements::alter::field::AlterFieldStatement>,
	},
	/// Raise what `DEFINE INDEX` raises of the index `index` of the table
	/// `table`, in the database `ns`/`db`, over the columns in the array
	/// `columns` of path texts: a column that names a computed field, or, when
	/// the boolean in `schemafull` is set, a field the table does not define.
	CheckIndexColumns {
		ns: Reg,
		db: Reg,
		table: Reg,
		index: Reg,
		columns: Reg,
		schemafull: Reg,
	},
	/// Raise what `ALTER TABLE` raises when the table definition in `updated`,
	/// formed from the one in `previous` by its clauses, is not a valid
	/// lightweight relation or leaves INLINE fields on a table that is no
	/// longer a relation. The definition in `updated` is replaced by the
	/// normalised one.
	CheckTableAlter {
		previous: Reg,
		updated: Reg,
	},
}

/// A field path a schema statement names, before it is resolved.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum PathSource {
	/// A path headed by a field. `holes` holds the register of each subscript
	/// that is not a literal, in the order the path holds them, each read as
	/// the integer or the key it stands for.
	Written {
		idiom: crate::expr::Idiom,
		holes: Vec<Reg>,
	},
	/// Any other expression: the value in the register is read as a string
	/// and parsed as a path.
	Evaluated(Reg),
}

/// An entry of a written list of index columns, before it is resolved.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum ColumnSource {
	/// A path.
	Path(PathSource),
	/// `type::fields(x)`, `x` in the register: the paths of its strings.
	TypeFields(Reg),
	/// `type::field(x)`, `x` in the register: the path of its string.
	TypeField(Reg),
	/// A `type::fields` or `type::field` call written with no argument,
	/// which raises the message.
	Refused(String),
}

/// What the backfill of an aggregated view computes per group: the view's
/// aggregations, and the values its select answers them from.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Backfill {
	pub(super) aggregations: Vec<crate::catalog::aggregation::Aggregation>,
	pub(super) values: Vec<crate::catalog::aggregation::BackfillValue>,
	/// How many keys the view groups by.
	pub(super) groups: usize,
}

assert_plain!(Backfill);
assert_plain!(StoreInstr);

/// A kind of catalog definition, as `INFO` renders it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum Definitions {
	Access,
	Api,
	Analyzer,
	Bucket,
	Function,
	Module,
	Model,
	Param,
	Table,
	User,
	Config,
	Sequence,
	Namespace,
	Database,
	Event,
	Field,
	Index,
	Live,
	Node,
	/// The root's default namespace and database, from its `default` config.
	Defaults,
}

/// What the process reports of itself to `INFO FOR ROOT`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum ProcessPart {
	/// Its resource usage.
	System,
	/// Its dynamic configuration.
	Config,
}

/// A process service a datastore may run without.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum Service {
	/// The notification broker.
	Broker,
	/// The sequence service.
	Sequences,
}

/// A catalog id the sequence service allocates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum IdSequence {
	Namespace,
	Database,
	Table,
}

/// A refusal `DEFINE FIELD` makes of its definition, as plain data a program
/// carries in place of the error it raises: each variant raises the error of
/// the same name, with the same fields.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FieldRefusal {
	InvalidStatement(String),
	Thrown(String),
	ComputedFieldCannotBeIndexed {
		field: String,
		index: String,
	},
	ComputedWrite(String),
	ComputedNestedField(String),
	ComputedKeywordConflict(String),
	ComputedNestedFieldConflict(String, String),
	ComputedParentFieldConflict(String, String),
	ComputedFieldCycle(String),
	ReferenceNestedField(String),
	ReferenceTypeConflict(String),
	MismatchedFieldTypes {
		name: String,
		kind: String,
		existing_name: String,
		existing_kind: String,
	},
	IdFieldKeywordConflict(String),
	IdFieldUnsupportedKind(String),
	/// The catalog's missing-table error.
	TbNotFound(crate::val::TableName),
	/// The catalog's refusal of a view's aggregation.
	InvalidAggregation(String),
	/// The catalog's refusal of a view's aggregate selector.
	InvalidAggregationSelector(String),
	/// The catalog's refusal of a view's select.
	CatalogQuery(String),
}

impl FieldRefusal {
	/// The refusal `error` raises, or `None` when it is no refusal of a
	/// field's definition.
	pub(super) fn of(error: &anyhow::Error) -> Option<Self> {
		use crate::exec::Error as E;
		match error.downcast_ref::<crate::catalog::Error>() {
			Some(crate::catalog::Error::TbNotFound {
				name,
			}) => return Some(Self::TbNotFound(name.clone())),
			Some(crate::catalog::Error::InvalidAggregation {
				message,
			}) => return Some(Self::InvalidAggregation(message.clone())),
			Some(crate::catalog::Error::InvalidAggregationSelector {
				expr,
			}) => return Some(Self::InvalidAggregationSelector(expr.clone())),
			Some(crate::catalog::Error::Query {
				message,
			}) => return Some(Self::CatalogQuery(message.clone())),
			_ => {}
		}
		Some(match error.downcast_ref::<E>()? {
			E::InvalidStatement(m) => Self::InvalidStatement(m.clone()),
			E::Thrown(m) => Self::Thrown(m.clone()),
			E::ComputedFieldCannotBeIndexed {
				field,
				index,
			} => Self::ComputedFieldCannotBeIndexed {
				field: field.clone(),
				index: index.clone(),
			},
			E::ComputedWrite(f) => Self::ComputedWrite(f.clone()),
			E::ComputedNestedField(f) => Self::ComputedNestedField(f.clone()),
			E::ComputedKeywordConflict(k) => Self::ComputedKeywordConflict(k.clone()),
			E::ComputedNestedFieldConflict(f, n) => {
				Self::ComputedNestedFieldConflict(f.clone(), n.clone())
			}
			E::ComputedParentFieldConflict(f, p) => {
				Self::ComputedParentFieldConflict(f.clone(), p.clone())
			}
			E::ComputedFieldCycle(c) => Self::ComputedFieldCycle(c.clone()),
			E::ReferenceNestedField(f) => Self::ReferenceNestedField(f.clone()),
			E::ReferenceTypeConflict(k) => Self::ReferenceTypeConflict(k.clone()),
			E::MismatchedFieldTypes {
				name,
				kind,
				existing_name,
				existing_kind,
			} => Self::MismatchedFieldTypes {
				name: name.clone(),
				kind: kind.clone(),
				existing_name: existing_name.clone(),
				existing_kind: existing_kind.clone(),
			},
			E::IdFieldKeywordConflict(k) => Self::IdFieldKeywordConflict(k.clone()),
			E::IdFieldUnsupportedKind(k) => Self::IdFieldUnsupportedKind(k.clone()),
			_ => return None,
		})
	}

	/// The error the refusal raises.
	pub(super) fn error(&self) -> anyhow::Error {
		use crate::exec::Error as E;
		let error = match self.clone() {
			Self::TbNotFound(name) => {
				return anyhow::Error::new(crate::catalog::Error::TbNotFound {
					name,
				});
			}
			Self::InvalidAggregation(message) => {
				return anyhow::Error::new(crate::catalog::Error::InvalidAggregation {
					message,
				});
			}
			Self::InvalidAggregationSelector(expr) => {
				return anyhow::Error::new(crate::catalog::Error::InvalidAggregationSelector {
					expr,
				});
			}
			Self::CatalogQuery(message) => {
				return anyhow::Error::new(crate::catalog::Error::Query {
					message,
				});
			}
			Self::InvalidStatement(m) => E::InvalidStatement(m),
			Self::Thrown(m) => E::Thrown(m),
			Self::ComputedFieldCannotBeIndexed {
				field,
				index,
			} => E::ComputedFieldCannotBeIndexed {
				field,
				index,
			},
			Self::ComputedWrite(f) => E::ComputedWrite(f),
			Self::ComputedNestedField(f) => E::ComputedNestedField(f),
			Self::ComputedKeywordConflict(k) => E::ComputedKeywordConflict(k),
			Self::ComputedNestedFieldConflict(f, n) => E::ComputedNestedFieldConflict(f, n),
			Self::ComputedParentFieldConflict(f, p) => E::ComputedParentFieldConflict(f, p),
			Self::ComputedFieldCycle(c) => E::ComputedFieldCycle(c),
			Self::ReferenceNestedField(f) => E::ReferenceNestedField(f),
			Self::ReferenceTypeConflict(k) => E::ReferenceTypeConflict(k),
			Self::MismatchedFieldTypes {
				name,
				kind,
				existing_name,
				existing_kind,
			} => E::MismatchedFieldTypes {
				name,
				kind,
				existing_name,
				existing_kind,
			},
			Self::IdFieldKeywordConflict(k) => E::IdFieldKeywordConflict(k),
			Self::IdFieldUnsupportedKind(k) => E::IdFieldUnsupportedKind(k),
		};
		anyhow::Error::new(error)
	}
}

/// A check `ALTER FIELD` makes of the stored field definition it forms by
/// merging its clauses into the one the statement reads, as its body makes
/// it of that definition before writing it.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum FieldCheck {
	/// The definition in `field` uses none of the clauses the `id` field
	/// refuses (`field_rules::id_field_restrictions`).
	IdField {
		field: Reg,
	},
	/// When the definition in `field` is `INLINE`, it can be embedded into
	/// the adjacency payloads of the table defined in `table`, read with its
	/// fields when the check runs (`field_rules::inline_restrictions`).
	Inline {
		field: Reg,
		table: Reg,
	},
	/// When the definition in `field` is `FLEXIBLE`, its kind contains an
	/// object and the table defined in `table` is `SCHEMAFULL`
	/// (`field_rules::flexible_restrictions`).
	Flexible {
		field: Reg,
		table: Reg,
	},
}

/// The conversion a schema statement's body applies to the value of a
/// clause it evaluates.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum ClauseConversion {
	/// A `DURATION FOR …`: cast to an optional duration.
	Duration,
	/// A verification `KEY`, or an `AUDIENCE` entry: coerced to a string.
	Coerced,
	/// A JWKS `URL`, or an issuer `KEY`: cast to a string.
	Cast,
	/// A sequence's `START`: cast to an integer.
	Integer,
	/// A sequence's `BATCH`: cast to an integer, which must be a batch size.
	Batch,
	/// `ALTER SYSTEM QUERY_TIMEOUT`: cast to a duration.
	RequiredDuration,
}

/// A check `DEFINE ACCESS` makes between the clauses it evaluates.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum AccessCheck {
	/// A record access method's token duration in `token` is set.
	RecordToken {
		token: Reg,
	},
	/// Under the symmetric algorithm `alg`, the keys in `verify` and `issue`
	/// are the same.
	SymmetricKeys {
		alg: crate::catalog::Algorithm,
		verify: Reg,
		issue: Reg,
	},
	/// The key in `key` is not the redaction placeholder, for the access
	/// method named in `name`.
	Unredacted {
		key: Reg,
		name: Reg,
	},
	/// Under the ES512 algorithm, reject unless importing or overwriting
	/// an existing ES512 access method.
	RejectEs512 {
		existing: Reg,
		overwrite: bool,
	},
	/// The access method defined in `access`, formed by an `ALTER ACCESS`
	/// from the one it altered, is valid: a record access method keeps its
	/// token duration, and a context clause is one its type admits.
	Altered {
		access: Reg,
	},
}

/// An error a schema statement raises, as the statement's body raises it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum StatementError {
	NsAlreadyExists,
	DbAlreadyExists,
	TbAlreadyExists,
	NsNotFound,
	DbNotFound,
	TbNotFound,
	FdAlreadyExists,
	PaAlreadyExists,
	EvAlreadyExists,
	FcAlreadyExists,
	AzAlreadyExists,
	SeqAlreadyExists,
	ApAlreadyExists,
	CgAlreadyExists,
	BuAlreadyExists,
	MdAlreadyExists,
	MlAlreadyExists,
	/// A user that exists; the argument is an object of the error's fields.
	UserRootAlreadyExists,
	AccessRootAlreadyExists,
	AccessNsAlreadyExists,
	AccessDbAlreadyExists,
	UserNsAlreadyExists,
	/// An index that exists; the argument is the index's name as written.
	IxAlreadyExists,
	/// A computed field an index names; the argument is an object of the
	/// `field` and the `index`.
	ComputedFieldCannotBeIndexed,
	UserDbAlreadyExists,
	CgNotFound,
	EvNotFound,
	FdNotFound,
	IxNotFound,
	PaNotFound,
	FcNotFound,
	SeqNotFound,
	AzNotFound,
	BuNotFound,
	MdNotFound,
	MlNotFound,
	/// The not-found errors of access methods, users and APIs; the argument
	/// is an object of the error's fields.
	AccessRootNotFound,
	AccessNsNotFound,
	AccessDbNotFound,
	ApNotFound,
	UserRootNotFound,
	UserNsNotFound,
	UserDbNotFound,
	/// An analyzer an index uses; the argument is an object of the
	/// analyzer's `name`, and the `table` and `index` using it.
	AzInUse,
	/// A table with views over it; the argument is an object of the
	/// `table`'s name and its `views`' definitions.
	TbHasViews,
	/// A method an API defines in more than one `FOR` clause; the argument is
	/// an object of the API's `value` (its path as given) and the `method`.
	ApMethodDuplicate,
}

/// A step of a write statement's body.
///
/// A write body runs one record at a time: the record in flight is a
/// [`WriteRow`](crate::exec::operators::write::row::WriteRow) the host holds,
/// never a register. The per-record order is a run of the instructions each
/// stage lowers to, and the branching between a form's orders — the
/// selection, the create attempt behind a savepoint, its classification and
/// retry — is the control flow around them. A failure raised inside a [`WriteInstr::TryBegin`]
/// region resumes the body at the region's handler with the failure held;
/// anywhere else it ends the statement.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum WriteInstr {
	/// Make the pipeline's row, a stored record, the record in flight.
	Record,
	/// Jump to `skip` when the statement gate refuses the record's table.
	Gate {
		skip: Label,
	},
	/// Resolve the record's table's write context and load its stored
	/// metadata, after checking the statement is neither cancelled nor out
	/// of time.
	Begin,
	/// Read the record a subject named, for a selection that asks whether it
	/// exists.
	LoadNamedSubject,
	/// Jump to `target` when the record does not exist yet.
	JumpIfNew {
		target: Label,
	},
	/// Jump to `target` when the record is one its source read rather than
	/// one the statement names.
	JumpIfNotIterationInitial {
		target: Label,
	},
	SavepointPush,
	SavepointRelease,
	SavepointRollback,
	/// Open a region whose failure resumes the body at `handler`.
	TryBegin {
		handler: Label,
	},
	/// Close the innermost region.
	TryEnd,
	/// The record's writes were rolled back: it counts as not written.
	ClearMutated,
	/// Propagate a held `BREAK`, `CONTINUE` or `RETURN`.
	RethrowSignal,
	/// Classify the held error of a create attempt under `policy`: raise it,
	/// jump to `skip`, or choose the record to retry the write as an update
	/// of, and continue.
	ClassifyCreateFailure {
		policy: crate::exec::operators::write::plan::RetryPolicy,
		skip: Label,
	},
	/// Jump to `target` when the statement is out of time.
	JumpIfDone {
		target: Label,
	},
	/// Read the record the classification chose and reset the record in
	/// flight to update it.
	ReloadForUpdate,
	/// Open the region in which a failure carried from the create attempt, if
	/// the classification carried one, replaces any failure raised.
	CarryBegin,
	/// Close the carrying region, dropping the carried failure.
	CarryEnd,
	/// Raise the carried failure, if there is one.
	RaiseCarried,
	/// Finish the record: count it as affected when it was written, as matched
	/// when its condition accepted it, and hand its output to the statement's
	/// unless it was `skipped`.
	End {
		skipped: bool,
	},
	/// Jump to `target` under `OPTION IMPORT`, where a write returns no row.
	JumpIfImport {
		target: Label,
	},
	/// Jump to `target` when the record has no post-image: `DELETE` emptied
	/// it.
	JumpIfNoPostImage {
		target: Label,
	},
	/// Forget the post-image's reduced counterpart, which the stages after
	/// its derivation have made stale.
	ClearCurrentReduced,
	/// Show `image` as the caller may see it, into the record's shown slot
	/// for that image: the image run through host pipeline
	/// [`Program::host_code`]`[view]` when set, the table's field view, and
	/// the image as it is otherwise. An image that is no record with an id
	/// is shown as it is, and so is an unchecked caller's post-image that
	/// carries its `COMPUTED` values already. For a `checked` caller the
	/// shown image is also the image's reduced counterpart.
	ShowImage {
		image: crate::exec::operators::write::stage::RecordImage,
		view: Option<u16>,
		checked: bool,
	},
	/// Set the row the statement returns for the record.
	SetOutput {
		output: WriteOutput,
	},
	/// Apply the stages of run `site` to the record, as they were lowered
	/// for its table: the unit [`WriteSpec::tables`] holds for the record's
	/// table has one pipeline per run, answering whether every stage kept the
	/// record. Jump to `skip` when a stage abandons it.
	DispatchTable {
		site: u16,
		skip: Label,
	},
	/// An instruction of the data stages.
	Data(super::write::DataInstr),
	/// An instruction of the schema stages.
	Schema(super::write::SchemaInstr),
	/// An instruction of the permission gates.
	Permissions(super::write::GateInstr),
	/// An instruction of the observer stages.
	Observe(super::write::ObserveInstr),
	/// An instruction of the storage stages and structural checks.
	Storage(super::write::StorageInstr),
}

impl Instr {
	/// Every code position the instruction may continue at other than the
	/// next, for a pass that moves instructions to rewrite.
	pub(super) fn labels_mut(&mut self) -> Vec<&mut Label> {
		match self {
			Instr::FilterBegin {
				none,
				..
			} => vec![none],
			Instr::MapBegin {
				other,
				..
			} => vec![other],
			Instr::JumpIfPermissionsBypassed {
				target,
			}
			| Instr::JumpIfFieldInt {
				target,
				..
			}
			| Instr::JumpIfCompare {
				target,
				..
			}
			| Instr::JumpIfNullish {
				target,
				..
			}
			| Instr::JumpIfNone {
				target,
				..
			}
			| Instr::JumpIfNotObject {
				target,
				..
			}
			| Instr::JumpIfNotRecordId {
				target,
				..
			}
			| Instr::JumpIf {
				target,
				..
			}
			| Instr::Skip {
				target,
				..
			}
			| Instr::Distinct {
				target,
				..
			}
			| Instr::Jump {
				target,
			} => vec![target],
			Instr::TryNone {
				catch,
				..
			} => vec![catch],
			Instr::JumpIfLimitReached {
				done,
				..
			}
			| Instr::GroupFirstNeeded {
				done,
				..
			}
			| Instr::ElemNext {
				done,
				..
			}
			| Instr::Next {
				done,
				..
			}
			| Instr::FetchRow {
				none: done,
				..
			}
			| Instr::KnnRank {
				none: done,
				..
			}
			| Instr::Degree {
				done,
				..
			} => vec![done],
			Instr::Subquery {
				caught,
				..
			}
			| Instr::Block(
				BlockInstr::Nested {
					caught,
					..
				}
				| BlockInstr::Member {
					caught,
					..
				},
			) => caught.iter_mut().map(|catch| &mut catch.label).collect(),
			Instr::MethodResolve {
				closure,
				..
			} => closure.iter_mut().collect(),
			Instr::Block(
				BlockInstr::RunEmbedded {
					on_break,
					on_continue,
					on_return,
					..
				}
				| BlockInstr::RunServed {
					on_break,
					on_continue,
					on_return,
					..
				},
			) => on_break
				.iter_mut()
				.chain(on_continue.iter_mut())
				.chain(on_return.iter_mut().map(|catch| &mut catch.label))
				.collect(),
			Instr::Block(BlockInstr::IterNext {
				done,
				..
			}) => vec![done],
			Instr::Store(
				StoreInstr::JumpIfImport {
					target,
				}
				| StoreInstr::JumpIfNoService {
					target,
					..
				}
				| StoreInstr::JumpIfSpeculating {
					target,
				}
				| StoreInstr::JumpIfSessionBase {
					target,
					..
				}
				| StoreInstr::BackfillGroup {
					empty: target,
					..
				},
			) => vec![target],
			Instr::Store(StoreInstr::Scan {
				unread,
				..
			}) => unread.iter_mut().collect(),
			Instr::Write(write) => match write {
				WriteInstr::Gate {
					skip,
				}
				| WriteInstr::ClassifyCreateFailure {
					skip,
					..
				}
				| WriteInstr::DispatchTable {
					skip,
					..
				} => vec![skip],
				WriteInstr::JumpIfNew {
					target,
				}
				| WriteInstr::JumpIfNotIterationInitial {
					target,
				}
				| WriteInstr::JumpIfDone {
					target,
				}
				| WriteInstr::JumpIfImport {
					target,
				}
				| WriteInstr::JumpIfNoPostImage {
					target,
				} => vec![target],
				WriteInstr::TryBegin {
					handler,
				} => vec![handler],
				WriteInstr::Data(instr) => instr.target_mut().into_iter().collect(),
				WriteInstr::Schema(instr) => instr.target_mut().into_iter().collect(),
				WriteInstr::Permissions(instr) => instr.target_mut().into_iter().collect(),
				WriteInstr::Observe(instr) => instr.target_mut().into_iter().collect(),
				WriteInstr::Storage(instr) => instr.target_mut().into_iter().collect(),
				_ => Vec::new(),
			},
			_ => Vec::new(),
		}
	}
}

/// What [`WriteInstr::SetOutput`] sets the record's output to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WriteOutput {
	/// No row.
	None,
	/// `NULL`.
	Null,
	/// The image in its shown slot.
	Shown(crate::exec::operators::write::stage::RecordImage),
	/// The JSON Patch from the shown pre-image to the shown post-image.
	Diff,
	/// Host pipeline [`Program::host_code`]`[pipeline]`'s answer over the
	/// shown post-image, with `$before` and `$after` the two shown images.
	Projection(u16),
}

/// A step of a place, its embedded expressions host pipelines of
/// [`Program::host_code`]: a subscript's pipeline answers the key or index
/// with no record in scope, and a `WHERE` step's judges one array element as
/// the row.
pub(super) type PlaceStepSpec = crate::exec::operators::write::place::PlaceStep<u16>;

/// One assignment of a `SET` clause, as the instructions that apply it read
/// it.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct AssignShape {
	/// The place as the statement wrote it.
	pub(super) path: crate::expr::Idiom,
	pub(super) operator: crate::expr::AssignOperator,
	/// The steps the place is walked by.
	pub(super) steps: Vec<PlaceStepSpec>,
}

/// A data clause, without the expressions that compute its values: those are
/// the host pipeline `DataInstr::ComputeInput` holds.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum DataShape {
	/// `SET a = 1, b += 2`, and `ON DUPLICATE KEY UPDATE`'s assignments, in
	/// written order.
	Set(Vec<AssignShape>),
	/// `UNSET a, b`: the paths to cut out.
	Unset(Vec<crate::expr::Idiom>),
	Content,
	Replace,
	Merge,
	Patch,
}

/// The clauses of a write statement, as the instructions of its body read
/// them.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct WriteShape {
	/// Whether the statement has a `WHERE` condition.
	pub(super) cond: bool,
	/// The data clause, for a form that takes one.
	pub(super) data: Option<DataShape>,
	/// `INSERT … ON DUPLICATE KEY UPDATE`'s clause.
	pub(super) duplicate_update: Option<DataShape>,
}

impl WriteShape {
	/// The clause the statement evaluates per record: the data clause, or
	/// `ON DUPLICATE KEY UPDATE` when there is none.
	pub(super) fn statement_data(&self) -> Option<&DataShape> {
		self.data.as_ref().or(self.duplicate_update.as_ref())
	}
}

assert_plain!(PlaceStepSpec);
assert_plain!(AssignShape);
assert_plain!(DataShape);
assert_plain!(WriteShape);

/// What a write statement's body is run with.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WriteSpec {
	pub(super) form: crate::exec::operators::write::plan::WriteForm,
	/// What the statement's clauses are, as the body's instructions read
	/// them.
	pub(super) shape: WriteShape,
	pub(super) output: Option<crate::expr::output::Output>,
	pub(super) only: bool,
	/// For a statement that names no single table, the units that hold the
	/// stages its body's [`WriteInstr::DispatchTable`]s apply, by the table a
	/// record is written to.
	pub(super) tables: Option<StageTables>,
}

/// The units of the stages a write applies to the records of each table it
/// can reach. A unit has one pipeline per run of stages the body takes, in
/// the body's order of sites.
///
/// A write that names its tables holds one unit per table the catalog holds
/// when the statement is lowered, and one for the tables it does not, which
/// carry no definitions: units of the statement's arena. A write that may
/// reach any table holds none: it names its stages ([`Self::by_name`]), and
/// the run loads the bundle of a table's stages by the table's name from the
/// compile cache the first time it writes a record of the table, compiled on
/// a miss ([`super::fault::serve_stage_bundle`]). Planning never lists the
/// database's tables for a write.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StageTables {
	/// The unit of each table the catalog holds, in table order.
	pub(super) tables: Vec<(surrealdb_strand::TableName, UnitId)>,
	/// The unit of a table the catalog does not hold, for a write that can
	/// reach one.
	pub(super) default: Option<UnitId>,
	/// The tables the write can reach; a record of any other is an error.
	pub(super) reach: super::dispatch::Reach,
	/// For a write that may reach any table, the stages its run loads for
	/// each table by name; such a write holds no `tables` or `default`.
	pub(super) by_name: Option<Arc<super::units::StageSetUnit>>,
	/// Whether the write is late-bound: its tables' catalog may change while
	/// it runs, so its run checks each write context it keeps, and each bundle
	/// of stages it loaded, against the catalog before taking it again.
	pub(super) revalidates: bool,
}

assert_plain!(StageTables);

impl StageTables {
	/// The unit that applies the stages to a record of `table`, or `None`
	/// where the write cannot reach the table.
	pub(super) fn unit(&self, table: &surrealdb_strand::TableName) -> Option<UnitId> {
		match self.tables.binary_search_by(|(name, _)| name.cmp(table)) {
			Ok(at) => Some(self.tables[at].1),
			Err(_) => self.reach.reaches(table).then_some(self.default).flatten(),
		}
	}
}

/// A comparison between two integers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IntCmp {
	Eq,
	Ne,
	Lt,
	Le,
	Gt,
	Ge,
}

impl IntCmp {
	/// The integer comparison `op` answers for two integers, for the
	/// operators whose answer on two integers is their numeric comparison.
	pub(super) fn of(op: CompareOp) -> Option<Self> {
		Some(match op {
			CompareOp::Equal | CompareOp::ExactEqual => IntCmp::Eq,
			CompareOp::NotEqual => IntCmp::Ne,
			CompareOp::LessThan => IntCmp::Lt,
			CompareOp::LessThanEqual => IntCmp::Le,
			CompareOp::MoreThan => IntCmp::Gt,
			CompareOp::MoreThanEqual => IntCmp::Ge,
			_ => return None,
		})
	}

	/// The comparison with its operands swapped: `a < b` is `b > a`.
	pub(super) fn flipped(self) -> Self {
		match self {
			IntCmp::Lt => IntCmp::Gt,
			IntCmp::Le => IntCmp::Ge,
			IntCmp::Gt => IntCmp::Lt,
			IntCmp::Ge => IntCmp::Le,
			same => same,
		}
	}

	pub(super) fn apply(self, a: i64, b: i64) -> bool {
		match self {
			IntCmp::Eq => a == b,
			IntCmp::Ne => a != b,
			IntCmp::Lt => a < b,
			IntCmp::Le => a <= b,
			IntCmp::Gt => a > b,
			IntCmp::Ge => a >= b,
		}
	}
}

/// A comparison between two values, named after the operator it evaluates.
///
/// The interpreter answers each with the function that evaluates the same
/// operator wherever it is written, so the answers cannot disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CompareOp {
	Equal,
	ExactEqual,
	NotEqual,
	AllEqual,
	AnyEqual,
	LessThan,
	LessThanEqual,
	MoreThan,
	MoreThanEqual,
	Contain,
	NotContain,
	ContainAll,
	ContainAny,
	ContainNone,
	Inside,
	NotInside,
	AllInside,
	AnyInside,
	NoneInside,
	Outside,
	Intersects,
}

/// An operator that computes a value from two operands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ArithOp {
	Add,
	Subtract,
	Multiply,
	Divide,
	Remainder,
	Power,
	/// `a..b`, `a..=b`, `a>..b`, `a>..=b`.
	Range {
		start_excluded: bool,
		end_included: bool,
	},
}

impl ArithOp {
	pub(super) fn of(op: &BinaryOperator) -> Option<Self> {
		Some(match op {
			BinaryOperator::Add => ArithOp::Add,
			BinaryOperator::Subtract => ArithOp::Subtract,
			BinaryOperator::Multiply => ArithOp::Multiply,
			BinaryOperator::Divide => ArithOp::Divide,
			BinaryOperator::Remainder => ArithOp::Remainder,
			BinaryOperator::Power => ArithOp::Power,
			BinaryOperator::Range => ArithOp::Range {
				start_excluded: false,
				end_included: false,
			},
			BinaryOperator::RangeInclusive => ArithOp::Range {
				start_excluded: false,
				end_included: true,
			},
			BinaryOperator::RangeSkip => ArithOp::Range {
				start_excluded: true,
				end_included: false,
			},
			BinaryOperator::RangeSkipInclusive => ArithOp::Range {
				start_excluded: true,
				end_included: true,
			},
			_ => return None,
		})
	}

	/// The operator applied to two values, raising what it refuses.
	pub(super) fn apply(self, left: Value, right: Value) -> anyhow::Result<Value> {
		use std::ops::Bound;
		Ok(match self {
			ArithOp::Add => operate::add(left, right)?,
			ArithOp::Subtract => operate::sub(left, right)?,
			ArithOp::Multiply => operate::mul(left, right)?,
			ArithOp::Divide => operate::div(left, right)?,
			ArithOp::Remainder => operate::rem(left, right)?,
			ArithOp::Power => operate::pow(left, right)?,
			ArithOp::Range {
				start_excluded,
				end_included,
			} => Value::Range(Box::new(crate::val::Range {
				start: if start_excluded {
					Bound::Excluded(left)
				} else {
					Bound::Included(left)
				},
				end: if end_included {
					Bound::Included(right)
				} else {
					Bound::Excluded(right)
				},
			})),
		})
	}
}

/// A prefix or postfix operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UnaryOp {
	Not,
	Negate,
	Positive,
	/// `..v`, `..=v`.
	RangeTo {
		included: bool,
	},
	/// `v..`, `v>..`.
	RangeFrom {
		excluded: bool,
	},
	/// `<kind> v`, to kind [`Program::kinds`]`[kind]`.
	Cast {
		kind: u32,
	},
}

impl UnaryOp {
	pub(super) fn apply(self, value: Value, kinds: &[crate::expr::Kind]) -> anyhow::Result<Value> {
		use std::ops::Bound;
		Ok(match self {
			UnaryOp::Not => operate::not(value)?,
			UnaryOp::Negate => operate::neg(value)?,
			UnaryOp::Positive => value,
			UnaryOp::RangeTo {
				included,
			} => Value::Range(Box::new(crate::val::Range {
				start: Bound::Unbounded,
				end: if included {
					Bound::Included(value)
				} else {
					Bound::Excluded(value)
				},
			})),
			UnaryOp::RangeFrom {
				excluded,
			} => Value::Range(Box::new(crate::val::Range {
				start: if excluded {
					Bound::Excluded(value)
				} else {
					Bound::Included(value)
				},
				end: Bound::Unbounded,
			})),
			UnaryOp::Cast {
				kind,
			} => value.cast_to_kind(&kinds[kind as usize]).map_err(|e| anyhow::anyhow!("{}", e))?,
		})
	}
}

impl CompareOp {
	/// The comparison `op` denotes, for the operators whose value depends on
	/// their two operands alone.
	pub(super) fn of(op: &BinaryOperator) -> Option<Self> {
		Some(match op {
			BinaryOperator::Equal => CompareOp::Equal,
			BinaryOperator::ExactEqual => CompareOp::ExactEqual,
			BinaryOperator::NotEqual => CompareOp::NotEqual,
			BinaryOperator::AllEqual => CompareOp::AllEqual,
			BinaryOperator::AnyEqual => CompareOp::AnyEqual,
			BinaryOperator::LessThan => CompareOp::LessThan,
			BinaryOperator::LessThanEqual => CompareOp::LessThanEqual,
			BinaryOperator::MoreThan => CompareOp::MoreThan,
			BinaryOperator::MoreThanEqual => CompareOp::MoreThanEqual,
			BinaryOperator::Contain => CompareOp::Contain,
			BinaryOperator::NotContain => CompareOp::NotContain,
			BinaryOperator::ContainAll => CompareOp::ContainAll,
			BinaryOperator::ContainAny => CompareOp::ContainAny,
			BinaryOperator::ContainNone => CompareOp::ContainNone,
			BinaryOperator::Inside => CompareOp::Inside,
			BinaryOperator::NotInside => CompareOp::NotInside,
			BinaryOperator::AllInside => CompareOp::AllInside,
			BinaryOperator::AnyInside => CompareOp::AnyInside,
			BinaryOperator::NoneInside => CompareOp::NoneInside,
			BinaryOperator::Outside => CompareOp::Outside,
			BinaryOperator::Intersects => CompareOp::Intersects,
			_ => return None,
		})
	}

	/// The comparison of two scalars. Kinds the scalar comparisons answer are
	/// compared in place; any other pair is compared as the values they denote.
	pub(super) fn apply_scalar<'r>(
		self,
		left: &Scalar<'_>,
		right: &Scalar<'_>,
	) -> anyhow::Result<Scalar<'r>> {
		use std::cmp::Ordering::{Equal, Greater, Less};
		let ordered = |accept: &[std::cmp::Ordering]| {
			left.partial_cmp(right).is_some_and(|o| accept.contains(&o))
		};
		let answer = match self {
			CompareOp::Equal => left.equal(right),
			CompareOp::NotEqual => !left.equal(right),
			CompareOp::ExactEqual => left.exact(right),
			CompareOp::LessThan => ordered(&[Less]),
			CompareOp::LessThanEqual => ordered(&[Less, Equal]),
			CompareOp::MoreThan => ordered(&[Greater]),
			CompareOp::MoreThanEqual => ordered(&[Greater, Equal]),
			CompareOp::AllEqual | CompareOp::AnyEqual if !left.is_collection() => left.equal(right),
			_ => {
				return Ok(match self.apply(&left.as_value(), &right.as_value())? {
					Value::Bool(v) => Scalar::Bool(v),
					other => Scalar::Value(std::borrow::Cow::Owned(other)),
				});
			}
		};
		Ok(Scalar::Bool(answer))
	}

	pub(super) fn apply(self, left: &Value, right: &Value) -> anyhow::Result<Value> {
		match self {
			CompareOp::Equal => operate::equal(left, right),
			CompareOp::ExactEqual => operate::exact(left, right),
			CompareOp::NotEqual => operate::not_equal(left, right),
			CompareOp::AllEqual => operate::all_equal(left, right),
			CompareOp::AnyEqual => operate::any_equal(left, right),
			CompareOp::LessThan => operate::less_than(left, right),
			CompareOp::LessThanEqual => operate::less_than_or_equal(left, right),
			CompareOp::MoreThan => operate::more_than(left, right),
			CompareOp::MoreThanEqual => operate::more_than_or_equal(left, right),
			CompareOp::Contain => operate::contain(left, right),
			CompareOp::NotContain => operate::not_contain(left, right),
			CompareOp::ContainAll => operate::contain_all(left, right),
			CompareOp::ContainAny => operate::contain_any(left, right),
			CompareOp::ContainNone => operate::contain_none(left, right),
			CompareOp::Inside => operate::inside(left, right),
			CompareOp::NotInside => operate::not_inside(left, right),
			CompareOp::AllInside => operate::inside_all(left, right),
			CompareOp::AnyInside => operate::inside_any(left, right),
			CompareOp::NoneInside => operate::inside_none(left, right),
			CompareOp::Outside => operate::outside(left, right),
			CompareOp::Intersects => operate::intersects(left, right),
		}
	}
}

/// The query of a brute-force KNN: the vector rows are ranked against and the
/// distance that ranks them.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct KnnRank {
	pub(super) vector: KnnVector,
	pub(super) distance: crate::catalog::Distance,
}

/// Where the vector a brute-force KNN ranks rows against comes from.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum KnnVector {
	Literal(Vec<crate::val::Number>),
	/// The query of [`SortSpec::knn_query`] of sort `0`, which the host
	/// evaluated when the statement started.
	Query(u16),
}

/// A query vector the host evaluates once, over no row, when the pipelines of
/// `group` start, and coerces to `array<number>`.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct KnnQuery {
	pub(super) scalar: HostScalar,
	pub(super) group: Option<u16>,
}

/// The ordering of one sort key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct KeyOrder {
	pub(super) ascending: bool,
	pub(super) collate: bool,
	pub(super) numeric: bool,
}

/// A split: what it splits each row on, and the pipeline its rows run
/// through.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct SplitSpec {
	pub(super) kind: SplitKind,
	/// The index in [`Program::pipelines`] of the pipeline whose source is
	/// this split.
	pub(super) pipeline: usize,
	/// The index in [`Program::pipelines`] of the pipeline a join's build side
	/// runs before, where the rows the join takes must read what the build
	/// side writes: the build side runs when that pipeline starts rather than
	/// when its first row reaches the join.
	pub(super) build_before: Option<usize>,
}

/// What turns a row into the rows a split's pipeline runs.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum SplitKind {
	/// `SPLIT`: one row per element of the collections the targets read.
	Targets(Vec<crate::exec::operators::SplitTarget>),
	/// A binding-table step, run by the host ([`super::binding`]).
	Step(super::binding::ProgramStep),
}

/// A join: how its build rows are matched to a probe row, and the pipeline
/// the pairings run through.
///
/// The build side is a group of the program, run when the first probe row
/// reaches the join and not before: a probe that yields no row never reads
/// it. Its rows are keyed by `build_keys`, each evaluated over the row set
/// under `alias` in an object of its own, and held in a table until the group
/// that owns the join starts again.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct JoinSpec {
	pub(super) kind: super::plan::JoinMatch,
	/// The field a build row is set under in a pairing, which the build keys
	/// and the residual read it through.
	pub(super) alias: String,
	/// What the build rows are keyed by, one scalar per key column, each
	/// evaluated over `{ alias: build row }`.
	pub(super) build_keys: Vec<HostScalar>,
	/// What decides, beside the keys, whether a build row matches a probe row:
	/// evaluated over the row the pairing would emit.
	pub(super) residual: Option<HostScalar>,
	/// The group that reads the build side; unread where the join holds its
	/// left row ([`Self::streams`]).
	pub(super) build: u16,
	/// The group whose start empties the build table.
	pub(super) owner: Option<u16>,
	/// The index in [`Program::pipelines`] of the pipeline whose source is
	/// this join.
	pub(super) pipeline: usize,
	/// For a join reading its right input through an index
	/// ([`super::plan::JoinLookup`]): the group that reads it for one left
	/// row's key, and the position of that key among the probe keys. The
	/// first [`super::plan::LOOKUP_EVALUATIONS`] left rows since the group
	/// that owns the join started run it in place of the build table.
	pub(super) lookup: Option<(u16, usize)>,
	/// For a join that holds its left input's one row
	/// ([`super::plan::JoinBuild::Left`]): the index in [`Program::pipelines`]
	/// of the pipeline that reads the right input and hands each row to the
	/// join ([`Instr::JoinPair`]). It runs after the left input's pipeline,
	/// and only where that pipeline handed the join a row.
	pub(super) streams: Option<usize>,
}

/// A sort: the order its rows drain in.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct SortSpec {
	pub(super) keys: Vec<KeyOrder>,
	/// Only this many rows of the order are ever read.
	pub(super) keep: Option<usize>,
	/// How the plan has the sort hold its rows: [`Spill::Spillable`] for a
	/// sort that keeps all of them, which the run charges to the query's
	/// lease and spills once the lease no longer covers them
	/// ([`Spill::Bounded`] for one bounded by `keep`).
	pub(super) held: super::plan::Spill,
	/// Whether the sort holds on disk what outgrows the rows it may keep in
	/// memory (`TEMPFILES`); never with `keep`, which bounds it already.
	pub(super) spill: bool,
	/// Whether the sort publishes the threshold of its first key to the
	/// scan that feeds it ([`TopKScan`]), through its cell of
	/// [`State::topk_cells`](super::interp::State).
	pub(super) pushdown: bool,
	pub(super) random: bool,
	/// Where the first key of each row the sort keeps is published for
	/// `vector::distance::knn()`: the sort of a brute-force KNN ranks rows by
	/// distance.
	pub(super) knn_slot: Option<u16>,
	/// For the sort of a brute-force KNN whose query vector is computed when
	/// the statement runs, the expression that computes it.
	pub(super) knn_query: Option<KnnQuery>,
}

/// An aggregate folded inside its input's pipeline: its fields, planned as the
/// aggregate operator plans them, and how many group keys it has — none for
/// `GROUP ALL`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AggregateSink {
	pub(super) fields: Vec<AggSpec>,
	pub(super) keys: usize,
	/// How the plan has the aggregate hold its groups: [`Spill::Spillable`]
	/// for `GROUP BY`'s, which the run charges to the query's lease and
	/// spills once the lease no longer covers them, [`Spill::Bounded`] for
	/// `GROUP ALL`'s one.
	pub(super) held: super::plan::Spill,
}

/// A projected field of an aggregate: what its group answers for it, in the
/// terms the body that folds the rows and the host that finishes the groups
/// share. Every expression it names is a pipeline of [`Program::host_code`] or
/// instructions of the body; the functions are names, resolved against the
/// registry when a run starts.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AggSpec {
	/// Where the field's answer lands in the group's object.
	pub(super) output_path: Vec<String>,
	/// Whether the field is a grouping key, answered by the group's key.
	pub(super) is_group_key: bool,
	/// For a grouping key, its position among the keys.
	pub(super) group_key_index: Option<usize>,
	/// The aggregates of the field; `None` for a field answered by the first
	/// value its group takes from its rows.
	pub(super) aggregate: Option<AggExpr>,
	/// Whether the field of a group answered by its first value has one value
	/// for every row of the group, which decides how many times it is
	/// evaluated and what a `NONE` from it means.
	pub(super) fallback_is_constant: bool,
	/// The pipeline that evaluates, once as the group finishes and over no
	/// row, a column of `GROUP ALL` the row cannot influence.
	pub(super) finish: Option<u16>,
}

/// The aggregate calls of one field and the expression over their answers.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AggExpr {
	pub(super) calls: Vec<AggCall>,
	/// The pipeline of [`Program::host_code`] that evaluates the expression
	/// over the group's answers, run over an object holding each call's
	/// answer as `_aN` and each key as `_gN`; `None` for a field that is
	/// exactly one call.
	pub(super) post: Option<u16>,
}

/// One aggregate call: the function and the pipelines of its arguments after
/// the first.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AggCall {
	pub(super) name: crate::exec::operators::AggregateId,
	/// The pipelines, each run once for the group over an object holding its
	/// keys as `_gN`, before the group finishes.
	pub(super) extra_args: Vec<u16>,
}

assert_plain!(AggregateSink);
assert_plain!(AggSpec);

/// What a frame below [`CALL_FRAME`] evaluates under: a permission predicate
/// or a computed field's body, and the authority its definition recorded.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct PermissionSpec {
	pub(super) mode: FrameMode,
	pub(super) auth: Option<surrealdb_iam::AuthLimit>,
	/// Whether the frame evaluates a column of a projection layer, as the
	/// projection layer evaluates it: a `BREAK` or `CONTINUE` out of it is an
	/// error naming the layer column, where one out of any other permission
	/// frame names the permission predicate.
	pub(super) column: bool,
}

/// What a [`PermissionSpec`] frame evaluates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FrameMode {
	/// A permission predicate of the given clause, as
	/// [`permission_frame`](crate::exec::operators::permission_frame)
	/// builds its context.
	Guard(crate::expr::logical::GuardKind),
	/// A `COMPUTED` field's body: writes are refused, reads stay checked.
	Computed,
	/// A table permission a write judges against one of the record's images,
	/// as the write gates judge it: a read guard refuses writes, a write
	/// guard does not, and what its host steps read is not permission-checked
	/// ([`EvalContext::skip_fetch_perms`](crate::exec::EvalContext)), while the
	/// statement's own context is left as it is.
	Gate(crate::expr::logical::GuardKind),
	/// An index's column evaluated against one image of the record, as the
	/// index stage evaluates it: in the statement's own context, with the
	/// record [`Instr::BeginComputing`] last set as the one a
	/// self-dereference answers from.
	IndexEntry,
	/// A `COMPUTED` field's body derived onto a copy of a record for a
	/// permission predicate that reads it, in the predicate's own frame:
	/// writes are refused, reads are not permission-checked, and the body
	/// runs under its field's `AUTH LIMIT`, computing the record
	/// [`Instr::BeginComputing`] last set.
	Derived,
}

impl PermissionSpec {
	/// The context this frame evaluates in, under the statement's `ctx`.
	///
	/// Every frame runs code a definer wrote, so each one is a privileged
	/// frame: it holds none of the caller's bindings
	/// ([`crate::exec::ExecutionContext::privileged_frame`]).
	pub(super) fn context(
		&self,
		ctx: &crate::exec::ExecutionContext,
	) -> crate::exec::ExecutionContext {
		match self.mode {
			// The frame every engine evaluates a permission predicate in.
			FrameMode::Guard(guard) => {
				crate::exec::operators::permission_frame(ctx, guard, self.auth.as_ref())
			}
			FrameMode::Gate(crate::expr::logical::GuardKind::Read) => ctx
				.clone()
				.privileged_frame()
				.refusing_writes(crate::dbs::NoWriteFrame::PermissionPredicate),
			FrameMode::Gate(crate::expr::logical::GuardKind::Write) | FrameMode::IndexEntry => {
				ctx.clone().privileged_frame()
			}
			FrameMode::Computed => {
				let frame = ctx.clone().privileged_frame().computing_field();
				match &self.auth {
					Some(limit) => frame.with_limited_auth(limit),
					None => frame,
				}
			}
			FrameMode::Derived => {
				let frame =
					ctx.clone().privileged_frame().with_skip_fetch_perms(true).computing_field();
				match &self.auth {
					Some(limit) => frame.with_limited_auth(limit),
					None => frame,
				}
			}
		}
	}
}

/// The permission check an [`Instr::PermissionRegion`] names in an error
/// leaving it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PermissionCheck {
	/// A table's permission.
	Table,
	/// A field's permission.
	Field,
}

impl PermissionCheck {
	/// The words an error leaving the region is prefixed with.
	pub(super) fn prefix(self) -> &'static str {
		match self {
			Self::Table => "Failed to check permission",
			Self::Field => "Failed to check field permission",
		}
	}
}

/// `START` and `LIMIT`, evaluated by the host once, `limit` first, before
/// any pipeline runs.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct LimitSpec {
	pub(super) limit: Option<HostScalar>,
	pub(super) start: Option<HostScalar>,
	/// The block statement whose paging this is, evaluated each time the block
	/// reaches it; `None` for the program's own, evaluated when it starts.
	pub(super) group: Option<u16>,
}

/// A path step that is a function of the value it steps from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NavOp {
	/// `v[e]` (`evaluate_index`).
	Index,
	/// `v[0]`: the first element of an array or set, `NONE` otherwise.
	First,
	/// `v[$]`: the last element of an array or set, `NONE` otherwise.
	Last,
	/// `v…` (`evaluate_flatten`).
	Flatten,
	/// `@`: the record identity the value carries (`DocPart`).
	Identity,
}

impl NavOp {
	pub(super) fn apply(self, value: &Value, index: Option<&Value>) -> anyhow::Result<Value> {
		Ok(match self {
			NavOp::Index => crate::exec::vm::access::index::evaluate_index(
				value,
				index.unwrap_or(&Value::None),
			)?,
			NavOp::First => match value {
				Value::Array(array) => array.first().cloned().unwrap_or(Value::None),
				Value::Set(set) => set.first().cloned().unwrap_or(Value::None),
				_ => Value::None,
			},
			NavOp::Last => match value {
				Value::Array(array) => array.last().cloned().unwrap_or(Value::None),
				Value::Set(set) => set.last().cloned().unwrap_or(Value::None),
				_ => Value::None,
			},
			NavOp::Flatten => crate::exec::vm::access::array_ops::evaluate_flatten(value)?,
			NavOp::Identity => crate::exec::vm::access::doc::record_identity(value),
		})
	}
}

/// The first frame index of a call frame. Below it, a frame is one of the
/// program's permission predicates ([`Program::permissions`]); from it, the
/// frame a user-defined function's arguments are evaluated in.
pub(super) const CALL_FRAME: u16 = 0x8000;

/// Whether `frame` is a permission predicate's, where a signal is an error
/// and the statement's `RETURN` is not taken.
pub(super) fn is_permission_frame(frame: Option<u16>) -> bool {
	frame.is_some_and(|frame| frame < CALL_FRAME)
}

/// A closure written in the call of a builtin that invokes it, whose body
/// runs in the caller's program: what binding its arguments and returning
/// from it take.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ClosureSpec {
	pub(super) args: Vec<(crate::expr::Param, crate::expr::Kind)>,
	pub(super) returns: Option<crate::expr::Kind>,
}

/// A recursion `v.{min..max+instruction}(body)`: its bounds, what it
/// collects, and its body, a unit of the root program over the value each
/// depth steps from (`None` for a recursion with no body, whose every depth is
/// the value itself).
///
/// With `repeat`, the body re-enters the recursion through `.@`
/// ([`Instr::RepeatRecurse`]): each depth's targets are discovered breadth
/// first and the result assembled from the deepest level back, whatever
/// `collect` says.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct RecurseSpec {
	pub(super) body: Option<UnitId>,
	pub(super) min: u32,
	pub(super) max: Option<u32>,
	pub(super) collect: RecurseCollect,
	pub(super) repeat: bool,
}

/// A nested program of the root program: an index into [`Program::units`] of
/// the root, whichever program holds the id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct UnitId(pub(super) u32);

/// What a unit takes when it runs.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UnitSig {
	/// The names the unit binds its arguments to, in order.
	pub(super) params: Vec<Strand>,
	/// Whether the unit runs over a row, the value its reads of the current
	/// value take; a unit that does not runs rowless.
	pub(super) takes_row: bool,
}

/// The frame a unit was lowered for. A unit runs only in a frame that
/// matches it, so that what lowering decided about the principal and the
/// permission contract holds for every run.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct UnitFrame {
	/// A digest of the class of the principal the unit was lowered for, its
	/// `AUTH LIMIT` cap included ([`PrincipalClass`]); `0` for a unit lowered
	/// for no principal, which assumes permissions apply and so serves any.
	pub(super) class_digest: u64,
	/// The namespace the unit was lowered in, `None` when it names none.
	pub(super) ns: Option<String>,
	/// The database the unit was lowered in, `None` when it names none.
	pub(super) db: Option<String>,
	/// Whether the unit was lowered for a frame that suppresses permission
	/// enforcement while still performing writes, which no frame that
	/// enforces them runs it in.
	pub(super) perms_suppressed: bool,
	/// Whether the unit was lowered for a frame whose reads are permission
	/// checked. A unit lowered for an unchecked frame runs in no checked one;
	/// one lowered for a checked frame also runs in an unchecked one, which
	/// waives checks it would otherwise apply.
	pub(super) checked: bool,
}

impl UnitFrame {
	/// A digest of the class of `auth` in the database `ns` and `db` name, on
	/// a datastore that enforces authentication where `auth_enabled`
	/// ([`PrincipalClass::of`]); `0` for no principal, and never `0` for one.
	pub(crate) fn digest(
		auth_enabled: bool,
		auth: Option<&crate::iam::Auth>,
		ns: Option<&str>,
		db: Option<&str>,
	) -> u64 {
		auth.map_or(0, |auth| PrincipalClass::of(auth_enabled, auth, ns, db).digest())
	}
}

/// What compiled code can tell of a principal in one database.
///
/// Planning consults the principal it compiles for only through whether its
/// reads of the database are permission checked
/// ([`crate::exec::permission::action_is_checked`] for `View`), for itself
/// and for itself capped by the `AUTH LIMIT` of each definition whose body or
/// clause it compiles. Two principals of one class answer every such question
/// alike, under every chain of caps, so code compiled for one is the code
/// compiled for the other, and a unit is shared by every principal of its
/// class ([`super::compile_cache`]). The class is the frame a unit admits: a
/// unit never runs for a principal of another class, and fails closed.
///
/// - A record or anonymous principal is left as it is by every cap, so whether its reads are
///   checked is all of it.
/// - A system user's level, where it covers the database, is fixed by the kind of the level: the
///   root, the namespace or the database. A cap narrows it to the cap's level, which covers the
///   database or not according to the cap alone, or leaves it, and leaves at least the viewer role.
///   So the kind, whether the level covers the database, and whether it holds a role decide every
///   answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PrincipalClass {
	/// A record or anonymous principal, whose reads are checked or not.
	Fixed {
		checked: bool,
	},
	/// A system user of a level of `kind`.
	System {
		kind: LevelKind,
		covers: bool,
		roled: bool,
	},
}

/// The kind of a system user's level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum LevelKind {
	Root,
	Namespace,
	Database,
}

impl PrincipalClass {
	/// The class of `auth` in the database `ns` and `db` name, on a datastore
	/// that enforces authentication where `auth_enabled`.
	pub(crate) fn of(
		auth_enabled: bool,
		auth: &crate::iam::Auth,
		ns: Option<&str>,
		db: Option<&str>,
	) -> Self {
		use crate::iam::Level;
		let kind = match auth.level() {
			Level::Root => LevelKind::Root,
			Level::Namespace(_) => LevelKind::Namespace,
			Level::Database(..) => LevelKind::Database,
			Level::No | Level::Record(..) => {
				return PrincipalClass::Fixed {
					checked: crate::exec::permission::action_is_checked(
						auth_enabled,
						auth,
						crate::iam::Action::View,
						ns,
						db,
					),
				};
			}
		};
		let covers = auth.is_root()
			|| match (ns, db) {
				(Some(ns), Some(db)) => auth.is_ns_check(ns) || auth.is_db_check(ns, db),
				(Some(ns), None) => auth.is_ns_check(ns),
				(None, _) => false,
			};
		PrincipalClass::System {
			kind,
			covers,
			roled: auth.has_viewer_role(),
		}
	}

	/// A digest of the class, never `0`.
	pub(crate) fn digest(self) -> u64 {
		use std::hash::{Hash, Hasher};
		let mut hasher = std::hash::DefaultHasher::new();
		self.hash(&mut hasher);
		hasher.finish() | 1
	}
}

/// What makes a program a unit: how it is called and the frame it runs in.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UnitHead {
	pub(super) sig: UnitSig,
	pub(super) frame: UnitFrame,
	/// For a unit that holds an arena of its own to run a function body from,
	/// the body's id in that arena; `None` for a unit that is itself what it
	/// is called to run.
	pub(super) entry: Option<UnitId>,
	/// For a unit that is a closure's body, what its values find it by.
	pub(super) closure: Option<ClosureHead>,
}

/// What a recursion collects.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum RecurseCollect {
	/// The value the last depth reached.
	Last,
	/// Every value reached.
	All {
		inclusive: bool,
	},
	/// Every path.
	Paths {
		inclusive: bool,
	},
	/// The shortest path to the target [`Instr::Recurse`] reads.
	Shortest {
		inclusive: bool,
	},
}

/// One field of a destructure.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum DestructureStep {
	/// `f.*`: the field, a record id fetched, kept only when it is an object.
	All(Strand),
	/// `f`: the field, `NONE` when absent.
	Field(Strand),
	/// `name: body`: [`Program::host_code`]`[pipeline]` over the value
	/// destructured.
	Aliased(Strand, u16),
	/// `f.{ … }`: the field destructured in turn.
	Nested(Strand, Arc<[DestructureStep]>),
	/// `...`: every field of the value that no earlier step has set, as it is.
	Rest,
}

/// A closure literal whose value a body makes: the literal as written, the
/// unit that is its body, and the parameters it captures from registers and
/// slots of the program, by name.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ClosureValueSpec {
	/// The literal as written, which every value it makes carries for a host
	/// that compiles the body on a fault.
	pub(super) closure: Arc<crate::expr::logical::LogicalClosure>,
	/// [`ClosureDigest::of`] the literal.
	pub(super) digest: ClosureDigest,
	/// The unit that is the literal's body, lowered with the program; `None`
	/// for a body the VM does not lower for this program's frame.
	pub(super) body: Option<UnitId>,
	pub(super) slots: Vec<String>,
	/// The parameters it captures from the frame the literal is evaluated
	/// in, when they are bound there.
	pub(super) framed: Vec<String>,
	/// The frame the literal is evaluated in.
	pub(super) frame: Option<u16>,
	/// What invoking the literal's body does to the transaction.
	pub(super) access_mode: AccessMode,
}

assert_plain!(ClosureValueSpec);

/// The version of the instruction set and of the layout of a [`Program`] a
/// unit is lowered to. A kept unit is keyed by it, so a unit lowered for
/// another version is never linked; it changes whenever an instruction, an
/// operand or a spec changes what it means.
pub(crate) const ISA_VERSION: u32 = 2;

/// The content digest of a closure as written: SHA-256 over the canonical
/// encoding [`Self::of`] states. It is the code reference a closure value
/// carries and the key its unit is found by. It names the closure's
/// canonical text, which two closures share where the printer writes them
/// alike (a negated literal and the negative literal), so wherever a unit is
/// found for a value the closure it is the body of is compared too
/// ([`ClosureHead::closure`]). The values a closure captured are not part of
/// it: they travel with the value.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ClosureDigest(pub(super) [u8; 32]);

assert_plain!(ClosureDigest);

/// A digest's bytes are already uniform, so a table hashes its first eight
/// and compares the whole digest on a match.
impl std::hash::Hash for ClosureDigest {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		let mut head = [0u8; 8];
		head.copy_from_slice(&self.0[..8]);
		state.write_u64(u64::from_le_bytes(head));
	}
}

impl ClosureDigest {
	/// The version of the encoding [`Self::of`] digests, which it digests
	/// first after the domain tag. It covers the layout and the canonical
	/// SurrealQL the kinds and the body print as, so it changes whenever
	/// either does.
	pub(crate) const ENCODING: u16 = 1;

	/// The digest of `closure`.
	///
	/// The encoding, at version [`Self::ENCODING`]: the domain tag
	/// `surrealdb.closure`, then the version as two little-endian bytes, then
	/// the number of declared parameters as four little-endian bytes and, for
	/// each in order, its name and the canonical SurrealQL of its kind; then a
	/// byte, `1` when a return kind is declared and `0` when none is, with the
	/// kind's canonical SurrealQL after a `1`; then the body as written
	/// ([`LogicalClosure::body_sql`]). Every text is its UTF-8 bytes preceded
	/// by their count as eight little-endian bytes, so no two encodings
	/// concatenate alike.
	///
	/// [`LogicalClosure::body_sql`]: crate::expr::logical::LogicalClosure::body_sql
	pub(crate) fn of(closure: &crate::expr::logical::LogicalClosure) -> Self {
		use sha2::{Digest, Sha256};
		use surrealdb_types::ToSql;
		fn text(hasher: &mut Sha256, text: &str) {
			hasher.update((text.len() as u64).to_le_bytes());
			hasher.update(text.as_bytes());
		}
		let mut hasher = Sha256::new();
		hasher.update(b"surrealdb.closure");
		hasher.update(Self::ENCODING.to_le_bytes());
		hasher.update((closure.args.len() as u32).to_le_bytes());
		for (param, kind) in &closure.args {
			text(&mut hasher, param.as_str());
			text(&mut hasher, &kind.to_sql());
		}
		match &closure.returns {
			Some(kind) => {
				hasher.update([1u8]);
				text(&mut hasher, &kind.to_sql());
			}
			None => hasher.update([0u8]),
		}
		text(&mut hasher, &closure.body_sql);
		Self(hasher.finalize().into())
	}
}

impl std::fmt::Debug for ClosureDigest {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		std::fmt::Display::fmt(self, f)
	}
}

impl std::fmt::Display for ClosureDigest {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for byte in &self.0 {
			write!(f, "{byte:02x}")?;
		}
		Ok(())
	}
}

/// What a unit that is a closure's body is found by: the digest of the
/// closure and the names its values capture. A closure value is run by the
/// unit whose head holds its digest and captured names, in a frame the unit
/// admits, while what its lowering read holds.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ClosureHead {
	/// [`ClosureDigest::of`] the closure the unit is the body of.
	pub(super) digest: ClosureDigest,
	/// The closure as written the unit is the body of. A digest names one
	/// closure's canonical text, and two closures that print alike share it:
	/// a run takes the unit for a value only when the value's closure is this
	/// one, by identity or compared whole.
	pub(super) closure: Arc<crate::expr::logical::LogicalClosure>,
	/// The [`ISA_VERSION`] the unit was lowered to: a run links no unit of
	/// another.
	pub(super) isa: u32,
	/// The names the closure value captured, sorted, which the unit reads
	/// from its slots after the arguments.
	pub(super) captured: Vec<String>,
	/// The names bound in the unit's slots, in slot order: the arguments,
	/// then the captures that are not also arguments.
	pub(super) slots: Vec<String>,
	/// Whether the unit reads nothing of its frame but its slots and
	/// re-enters nothing that could invoke a closure, so it runs in the
	/// invoker's frame with no frame of its own.
	pub(super) frameless: bool,
	/// What running the unit may do beyond computing its value — write
	/// through the transaction (a schema statement included), reach outside
	/// it, run a body it cannot see — as the effect fold over the bound body
	/// declares it. A run links the unit into a frame only where the frame
	/// grants every effect declared here ([`super::closure::grants`]).
	pub(super) effects: crate::expr::logical::EffectSet,
	/// How many table catalog versions the transaction had stamped when the
	/// unit was lowered. A unit the program lowered stands only while the
	/// invoking transaction has stamped no more; one linked into a run stands
	/// for the stamps of the transaction it was linked into
	/// ([`super::closure`]).
	pub(super) stamps: u64,
	/// What the lowering read of the catalog beyond the table versions the
	/// stamps count: a function, a parameter or a database, which must be as
	/// it was for each invocation. `None` when the lowering read nothing
	/// else, or a recording that cannot be told apart later.
	pub(super) reads: Option<super::closure::ClosureReads>,
	/// The `VERSION` stamp the body was lowered at: the unit runs only an
	/// invocation read at that stamp, and `None` only one read at none.
	pub(super) version: Option<u64>,
}

assert_plain!(ClosureHead);

/// A statement in scalar position, lowered with the program as unit
/// [`Self::unit`] of the arena that holds it: a program of its own that runs
/// over the context [`super::host::run_subprogram`] derives from the row.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct SubquerySpec {
	pub(super) unit: UnitId,
	/// Whether the statement reads the enclosing row as its document.
	pub(super) inherits_row: bool,
	/// What the statement does to the transaction: the unit's own access
	/// mode, which the program holding the subquery takes too.
	pub(super) access_mode: AccessMode,
	/// The context level the statement requires.
	pub(super) required_context: ContextLevel,
	/// Whether the statement keeps row order ([`Program::keeps_row_order`]),
	/// which the program holding the subquery keeps too.
	pub(super) keeps_row_order: bool,
}

assert_plain!(SubquerySpec);

/// The content hash of a stored definition as the program was lowered from
/// it, which the run compares with the definition it reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct DefHash(pub(super) u64);

impl DefHash {
	/// The hash of function `definition`
	/// ([`crate::kvs::cache::ds::FunctionSnapshot::hash_of`]).
	pub(super) fn of_function(definition: &crate::catalog::FunctionDefinition) -> Self {
		Self(crate::kvs::cache::ds::FunctionSnapshot::hash_of(definition))
	}

	/// The hash of the `PERMISSIONS` clause of parameter `definition`, the
	/// one part of a `DEFINE PARAM` a program lowers.
	pub(super) fn of_param(definition: &crate::catalog::ParamDefinition) -> Self {
		Self::of_permission(&definition.permissions)
	}

	/// The hash of a definition's `PERMISSIONS` clause `permission`, the one
	/// part of a model or module definition a program lowers.
	pub(super) fn of_permission(permission: &crate::catalog::Permission) -> Self {
		use std::hash::{Hash, Hasher};
		let mut hasher = std::hash::DefaultHasher::new();
		permission.hash(&mut hasher);
		Self(hasher.finish())
	}
}

/// A call of a user-defined function, by name: what the run checks of the
/// definition it reads, where the body is, and the guard that judges its
/// `PERMISSIONS` clause.
///
/// The body and the guard were lowered when the program was planned, from the
/// definition that hashes to [`Self::def_hash`]; a call that finds another
/// definition is an internal error, never a compilation.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct FunctionSpec {
	/// The function's name, without its `fn::` prefix.
	pub(super) name: String,
	/// The function's name as the language writes it, with its `fn::` prefix.
	pub(super) qualified: String,
	/// The planner's re-entry depth at the call; the body runs one deeper.
	pub(super) plan_depth: u32,
	/// The body's access mode, resolved through the call graph when the
	/// program was planned.
	pub(super) body_access_mode: AccessMode,
	/// Whether the function was defined and allowed when the program was
	/// lowered. A call of one that was not fails entering it, as the catalog
	/// or the capabilities say; the definition it then finds is an internal
	/// error.
	pub(super) defined: bool,
	/// The definition the body and the guard were lowered from.
	pub(super) def_hash: DefHash,
	/// The frame the call was lowered for.
	pub(super) frame: UnitFrame,
	/// Whether the body was bound for a principal whose reads are
	/// permission-checked, so that it applies table and field guards that a
	/// frame skipping permissions waives.
	pub(super) checked: bool,
	/// The body, a unit that is called; `None` for a call whose body the
	/// program inlines.
	pub(super) body: Option<UnitId>,
	/// The body lowered for the same principal in a frame that waives
	/// permission checks, which a call in a frame that skips permissions runs
	/// where [`Self::body`] is [`Self::checked`].
	pub(super) unchecked_body: Option<UnitId>,
	/// The `PERMISSIONS` predicate, judged while the call enters: the hash
	/// of the definition it was lowered from, the pipeline of
	/// [`Program::host_code`] that evaluates it, and how often its verdict
	/// stands within a statement. `None` for a clause that is not a
	/// predicate, and where the call's frame judges none.
	pub(super) guard: Option<(DefHash, u16, crate::exec::guard_verdicts::GuardScope)>,
	/// What the call spends of the computation-depth budget beyond its own
	/// re-entry ([`super::depth`]).
	pub(super) depth: CallDepth,
	/// Whether every body the call may run is light: a small leaf
	/// ([`Program::small_leaf`]) that reads its arguments from its slots and
	/// nothing else of its frame. The lighter calling convention runs it in
	/// the frame the call entered, one re-entry deeper, with the arguments in
	/// its slots, in place where it hands the host no step, rather than as a
	/// program of its own in a frame built for the call on the statement's
	/// frame stack.
	pub(super) light: bool,
}

/// What a call of a user-defined function spends of the computation-depth
/// budget beyond the one re-entry every call spends ([`super::depth`]).
///
/// Where the caller is evaluated one expression at a time, the call spends
/// [`Self::site`] and the body runs that way too. Elsewhere a body whose text
/// writes is entered that way, spending the call's planner depth and one
/// more for the body as a block; any other body is not. A body entered that
/// way spends [`Self::body`] as it is entered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct CallDepth {
	/// What the caller spends on the way to the call beyond what entering
	/// the caller spent.
	pub(super) site: u8,
	/// Whether the body's text writes.
	pub(super) writes: bool,
	/// What entering the body spends: the way to its shallowest write.
	pub(super) body: u8,
	/// The most the body spends at once anywhere in it, which must be left
	/// where it is entered.
	pub(super) deepest: u8,
}

assert_plain!(FunctionSpec);

/// What a call of code the host runs calls: code the statement or the
/// catalog holds as data, run by the engine's runtime for its kind.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum ExternalCallee {
	/// An embedded script, `function(…) { source }`, run by the script
	/// runtime with its arguments and the current value as `this`.
	Script {
		source: String,
	},
	/// Machine-learning model `name` at `version`, read from the catalog.
	Model {
		name: String,
		version: String,
	},
	/// An export of a module read from the catalog under its storage name
	/// `storage` (`mod::…`, or `silo::…` for a package when `silo`): `sub`,
	/// or its default export.
	Module {
		storage: String,
		sub: Option<String>,
		silo: bool,
	},
	/// A user-defined function `name`, without its `fn::` prefix, called by a
	/// late-bound statement: run through the call unit of the definition the
	/// transaction reads when the call runs ([`super::call_unit`]), which
	/// makes the checks, binds the arguments and runs the body.
	Function {
		name: String,
	},
	/// The body of user-defined function `name`, run by a call that entered
	/// it: a body that reads an argument where only a value answers, compiled
	/// for the values the call's arguments hold ([`super::call_unit`]).
	Specialised {
		name: String,
	},
}

/// A call of code the host runs ([`ExternalCallee`]), entered by
/// [`Instr::ExternalEnter`] before its arguments are evaluated and made by
/// [`Instr::ExternalCall`] with them.
///
/// A model or module call judges the definition's `PERMISSIONS` clause with
/// the predicate lowered when the program was planned from the clause that
/// hashes to the guard's [`DefHash`]; a definition whose clause hashes
/// otherwise is an internal error and the call fails closed.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ExternalSpec {
	pub(super) callee: ExternalCallee,
	/// The planner's re-entry depth at the call, from which code the callee
	/// runs continues the computation-depth count.
	pub(super) plan_depth: u32,
	/// The `PERMISSIONS` predicate of the definition the call reads: the hash
	/// of the clause it was lowered from, the pipeline of
	/// [`Program::host_code`] that evaluates it, and how often its verdict
	/// stands within a statement. `None` where the clause read when the
	/// program was planned was no predicate, or none was read.
	pub(super) guard: Option<(DefHash, u16, crate::exec::guard_verdicts::GuardScope)>,
}

assert_plain!(ExternalSpec);

/// A `count(->edge)`-shaped call the planner answers from the adjacency's
/// cardinality ([`Instr::Degree`]): the direction it walks and the edge
/// tables it counts, every table when empty.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct DegreeSpec {
	pub(super) direction: crate::exec::parts::LookupDirection,
	pub(super) tables: Vec<TableName>,
}

assert_plain!(DegreeSpec);

/// A read of a parameter by name, resolved at run time from the frame and
/// then from `DEFINE PARAM`, with the guard that judges the definition's
/// `PERMISSIONS` clause.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ParamSpec {
	pub(super) name: Strand,
	/// The predicate of the `DEFINE PARAM` found when the program was
	/// planned: the hash of the clause it was lowered from, the pipeline of
	/// [`Program::host_code`] that evaluates it, and how often its verdict
	/// stands within a statement.
	pub(super) guard: Option<(DefHash, u16, crate::exec::guard_verdicts::GuardScope)>,
}

assert_plain!(ParamSpec);

/// A statement in scalar position compiled into the program as group
/// [`Program::groups`]`[group]`, run as `strategy` says.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct NestedSpec {
	pub(super) group: u16,
	/// Whether the statement reads the row it is evaluated for as its
	/// document.
	pub(super) inherits_row: bool,
	/// Whether its value is kept, from the first evaluation that reaches it,
	/// until the group it is evaluated in — the program's own pipelines for
	/// `None` — starts again ([`super::plan::Strategy::Once`]).
	pub(super) once: bool,
	/// The group whose pipelines evaluate it.
	pub(super) owner: Option<u16>,
	/// For a statement kept across a loop's iterations, the loop, by its
	/// iterator slot ([`BlockInstr::IterStart`]): its value is forgotten when
	/// the loop starts rather than when its group does. Set only where no
	/// iteration can change what it reads: it reads no name the loop binds,
	/// and nothing in the loop's body writes.
	pub(super) hoist: Option<u16>,
	/// Whether the statement only reads, so a region's batch may run it over
	/// the batch's own state.
	pub(super) read_only: bool,
	/// Whether it reads through a probe ([`Source::Probe`]), whose kept rows
	/// are the running state's.
	pub(super) probed: bool,
	/// Whether its rows are kept as a membership index rather than as the
	/// array they gather ([`BlockInstr::Member`]), which only a statement kept
	/// across evaluations ([`Self::once`]) is.
	pub(super) members: bool,
	/// For a statement read through an index nested loop: the group that reads
	/// it through its probe, and the path into `$parent` of the key the
	/// lookup reads. The evaluations past the first
	/// [`super::plan::LOOKUP_EVALUATIONS`] since the group that owns it
	/// started run that group instead of `group`, as does an evaluation whose
	/// key the index may not list every equal record under, which a lookup
	/// would read the table whole for.
	pub(super) hashed: Option<(u16, Vec<surrealdb_strand::Strand>)>,
}

/// A graph lookup: a run of hops, each read from the records the one before
/// it reached, answered for one origin at a time.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LookupSpec {
	pub(super) hops: Vec<LookupHop>,
	/// Whether what the last hop reached is taken back to its record ids:
	/// its records were read for a `WHERE` alone.
	pub(super) extract_id: bool,
	/// Whether the answer is one value, `NONE` for none, rather than an
	/// array.
	pub(super) only: bool,
	/// Whether `ONLY` on a hop of the path before the lookup unwraps what the
	/// lookup answers over all its origins together: one value, `NONE` for
	/// none.
	pub(super) only_path: bool,
	/// Whether the caller's reads were checked when the hops were lowered,
	/// which decides the permissions and field views each hop holds.
	pub(super) checked: bool,
	/// The page of the records the last hop reaches, over all the origins
	/// together, that the lookup answers. The last hop reads no more of an
	/// adjacency than the page needs.
	pub(super) page: Option<LookupPage>,
}

/// `START` and `LIMIT` of a lookup's last hop, written as integers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LookupPage {
	pub(super) start: usize,
	pub(super) limit: Option<usize>,
}

impl LookupPage {
	/// How many records the page needs from the front of what the hop
	/// reaches; `None` when `START` alone leaves the rest of it open.
	pub(super) fn needs(&self) -> Option<usize> {
		self.limit.map(|limit| self.start.saturating_add(limit))
	}

	/// `records` cut to the page.
	pub(super) fn of(&self, records: Vec<Value>) -> Vec<Value> {
		let rest = records.into_iter().skip(self.start);
		match self.limit {
			Some(limit) => rest.take(limit).collect(),
			None => rest.collect(),
		}
	}
}

/// One hop of a [`LookupSpec`]: what `read` reaches from each record it is
/// handed, each record reached then judged and shown as a read of its table
/// is.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LookupHop {
	pub(super) read: Arc<HopRead>,
	/// Whether each record reached is read and shown whole, as a `WHERE`
	/// needs; otherwise it is its record id.
	pub(super) full: bool,
	/// How each table the hop reaches is read, when its tables are known.
	pub(super) tables: Vec<(TableName, HopTable)>,
	/// The `WHERE` the records reached are filtered by: host pipeline
	/// [`Program::host_code`]`[cond]` over each record as shown, with
	/// `$parent` bound.
	pub(super) cond: Option<u16>,
	/// A `WHERE` that reads only `id`, `in` and `out`, judged from each
	/// adjacency entry before its record is read.
	pub(super) key_cond: Option<KeyCond>,
}

/// What a hop reads to reach records from the records it is handed.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum HopRead {
	/// Each record's adjacency, along `directions` in order.
	Graph {
		directions: Vec<crate::expr::Dir>,
		/// The edge tables read, in written order; every edge table when
		/// empty.
		specs: Vec<EdgeSpec>,
		/// Each of `specs`' tables encoded, the prefix of a cached entry's
		/// edge.
		prefixes: Vec<Vec<u8>>,
		/// For a hop through an edge to the vertex beyond it, whose
		/// adjacency key names that vertex: the vertex tables admitted. The
		/// hop then reaches the vertices rather than the edges.
		targets: Option<Vec<TableName>>,
		/// `targets` as the edge tables of an edge's own adjacency, read for
		/// a key that names no vertex.
		target_specs: Vec<EdgeSpec>,
		/// The key ranges of `specs` and `target_specs`, where every bound is
		/// a constant: the same at every taking of the hop.
		fixed: Option<super::plain::ValueData<Arc<KeyRanges>>>,
	},
	/// The records referencing each record, subject by subject in written
	/// order, each subject over every record handed.
	References(Vec<ReferenceSubject>),
}

impl HopRead {
	/// A read of each record's adjacency, its key ranges fixed where every
	/// bound of `specs` and `target_specs` is a constant.
	pub(super) fn graph(
		directions: Vec<crate::expr::Dir>,
		specs: Vec<EdgeSpec>,
		prefixes: Vec<Vec<u8>>,
		targets: Option<Vec<TableName>>,
		target_specs: Vec<EdgeSpec>,
	) -> Self {
		// The bounds are constant host scalars, which hold no closure.
		let fixed = KeyRanges::fixed(&specs, &target_specs).and_then(|ranges| {
			super::plain::ValueData::new(Arc::new(ranges), "a hop's key ranges").ok()
		});
		HopRead::Graph {
			directions,
			specs,
			prefixes,
			targets,
			target_specs,
			fixed,
		}
	}
}

/// An edge table a hop reads, with the bounds on its edges' foreign key.
pub(crate) type KeyRange =
	(TableName, std::ops::Bound<crate::val::RecordIdKey>, std::ops::Bound<crate::val::RecordIdKey>);

/// A graph hop's edge tables with their bounds evaluated for one taking of
/// the hop: the edge tables read, the edge tables of an edge's own adjacency
/// read for a key naming no vertex, and whether every edge table is read
/// whole, which only the inline adjacency cache answers.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KeyRanges {
	pub(super) specs: Vec<KeyRange>,
	pub(super) targets: Vec<KeyRange>,
	pub(super) whole: bool,
}

impl KeyRanges {
	/// The key ranges of `specs` and `target_specs` where every bound is a
	/// constant, read as record keys.
	fn fixed(specs: &[EdgeSpec], target_specs: &[EdgeSpec]) -> Option<Self> {
		let bound = |bound: &std::ops::Bound<HostScalar>| {
			use std::ops::Bound;
			let key = |scalar: &HostScalar| match scalar {
				HostScalar::Const(value) => {
					Some(crate::exec::operators::scan::common::value_to_record_id_key(
						Value::clone(value),
					))
				}
				HostScalar::Code(_) => None,
			};
			Some(match bound {
				Bound::Unbounded => Bound::Unbounded,
				Bound::Included(scalar) => Bound::Included(key(scalar)?),
				Bound::Excluded(scalar) => Bound::Excluded(key(scalar)?),
			})
		};
		let ranges = |specs: &[EdgeSpec]| {
			specs
				.iter()
				.map(|spec| Some((spec.table.clone(), bound(&spec.start)?, bound(&spec.end)?)))
				.collect::<Option<Vec<_>>>()
		};
		Some(Self {
			specs: ranges(specs)?,
			targets: ranges(target_specs)?,
			whole: specs.iter().all(EdgeSpec::is_whole),
		})
	}
}

/// One edge table a graph hop reads: its edges whose foreign key the bounds
/// admit, each bound a host scalar evaluated with no row when the hop is
/// taken, its value read as a record key.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EdgeSpec {
	pub(super) table: TableName,
	pub(super) start: std::ops::Bound<HostScalar>,
	pub(super) end: std::ops::Bound<HostScalar>,
}

impl EdgeSpec {
	/// Every edge of `table`.
	pub(super) fn whole(table: TableName) -> Self {
		Self {
			table,
			start: std::ops::Bound::Unbounded,
			end: std::ops::Bound::Unbounded,
		}
	}

	/// Whether the spec admits every edge of its table.
	pub(super) fn is_whole(&self) -> bool {
		matches!(self.start, std::ops::Bound::Unbounded)
			&& matches!(self.end, std::ops::Bound::Unbounded)
	}
}

/// The referencing records one subject of `<~` names: those of `table`, or
/// of every table, holding the reference in `field`, or in any field, whose
/// keys the bounds admit (host scalars evaluated with no row when the hop is
/// taken, as [`EdgeSpec`]'s are).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReferenceSubject {
	pub(super) table: Option<TableName>,
	pub(super) field: Option<String>,
	pub(super) start: std::ops::Bound<HostScalar>,
	pub(super) end: std::ops::Bound<HostScalar>,
	/// `table` and `field` encoded, the prefix of a cached entry naming them;
	/// none without a table.
	pub(super) prefix: Option<Vec<u8>>,
}

/// A hop's `WHERE` over each adjacency entry, judged before the edge is read:
/// host pipeline [`Program::host_code`]`[pipeline]` over an object holding the
/// fields it reads, with `$parent` bound — the key's `id`, `in` and `out`, and
/// with `payload`, the inline fields the entry carries. An entry that cannot
/// answer them — its key naming no vertex, or its inline section absent or
/// written under another generation or field count than the snapshot of its
/// table in `payload` — is judged against its record as stored, and matches
/// nothing when there is none.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KeyCond {
	pub(super) pipeline: u16,
	pub(super) needs_id: bool,
	pub(super) needs_in: bool,
	pub(super) needs_out: bool,
	/// For a `WHERE` reading inline fields, each edge table's inline-field
	/// snapshot taken when the program was planned; `None` for one reading
	/// only what the key carries.
	pub(super) payload: Option<Vec<(TableName, crate::exec::operators::scan::PayloadTableSpec)>>,
	/// The `WHERE` as comparisons of the fields it reads with constants, an
	/// entry admitted when every one holds, judged in order over the fields
	/// with no pipeline run; `None` where it is any other expression.
	pub(super) checks: Option<Vec<KeyCheck>>,
}

/// One comparison of a key-only `WHERE`: field `field` of the object the
/// `WHERE` is judged over against constant `value` by `op`, the field the
/// left operand unless `swapped`. A field the object does not hold is
/// `NONE`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KeyCheck {
	pub(super) field: String,
	pub(super) op: CompareOp,
	pub(super) value: super::plain::ValueData<Value>,
	pub(super) swapped: bool,
}

/// How a hop reads a record of one table.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HopTable {
	/// The table's `PERMISSIONS FOR select`, for a checked caller.
	pub(super) permission: HopPermission,
	/// Host pipeline [`Program::host_code`]`[view]` showing a record read
	/// whole as the caller may see it; the record itself when `None`.
	pub(super) view: Option<u16>,
}

/// A table's `PERMISSIONS FOR select` as a hop judges it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum HopPermission {
	Allow,
	Deny,
	/// Host pipeline [`Program::host_code`]`[pipeline]` over the record,
	/// answering whether the caller may select it.
	Judged(u16),
	/// [`Self::Judged`], for a predicate that reads `$parent`: the pipeline
	/// runs with `$parent` the row the lookup is taken over, where there is
	/// one.
	JudgedOverParent(u16),
}

/// A record id a body builds, whose key holds computed parts.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct RecordIdSpec {
	pub(super) table: crate::val::TableName,
	pub(super) key: KeyShape,
}

impl RecordIdSpec {
	/// The record id the spec builds, its key read through `read`.
	pub(super) fn build(
		&self,
		read: &mut dyn FnMut(Operand) -> anyhow::Result<Value>,
	) -> anyhow::Result<Value> {
		Ok(Value::RecordId(crate::val::RecordId {
			table: self.table.clone(),
			key: self.key.build(read)?,
		}))
	}
}

/// The key of a [`RecordIdSpec`]: an array or object key is the value an
/// operand holds, built by the instructions before it.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum KeyShape {
	Fixed(super::plain::ValueData<crate::val::RecordIdKey>),
	Generate(crate::expr::record_id::RecordIdKeyGen),
	Array(Operand),
	Object(Operand),
	Range(Box<std::ops::Bound<KeyShape>>, Box<std::ops::Bound<KeyShape>>),
}

impl KeyShape {
	fn build(
		&self,
		read: &mut dyn FnMut(Operand) -> anyhow::Result<Value>,
	) -> anyhow::Result<crate::val::RecordIdKey> {
		use crate::val::RecordIdKey;
		let bound = |bound: &std::ops::Bound<KeyShape>,
		             read: &mut dyn FnMut(Operand) -> anyhow::Result<Value>| {
			anyhow::Ok(match bound {
				std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
				std::ops::Bound::Included(key) => std::ops::Bound::Included(key.build(read)?),
				std::ops::Bound::Excluded(key) => std::ops::Bound::Excluded(key.build(read)?),
			})
		};
		Ok(match self {
			KeyShape::Fixed(key) => crate::val::RecordIdKey::clone(key),
			KeyShape::Generate(generator) => generator.compute(),
			KeyShape::Array(operand) => match read(*operand)? {
				Value::Array(array) => RecordIdKey::Array(array),
				other => anyhow::bail!("an array key built as {}", other.kind_of()),
			},
			KeyShape::Object(operand) => match read(*operand)? {
				Value::Object(object) => RecordIdKey::Object(object),
				other => anyhow::bail!("an object key built as {}", other.kind_of()),
			},
			KeyShape::Range(start, end) => {
				let start = bound(start, read)?;
				let end = bound(end, read)?;
				RecordIdKey::Range(Box::new(crate::val::RecordIdKeyRange {
					start,
					end,
				}))
			}
		})
	}
}

/// A registered method a body calls, by name. The interpreter answers whether
/// the method resolves for a receiver from the mask alone; the host resolves
/// the name to its descriptor once per run.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct MethodSpec {
	pub(super) name: String,
	/// The kinds of receiver with a type-specific implementation, as a mask of
	/// [`ValueKind::bit`](crate::exec::function::ValueKind::bit)s.
	pub(super) kinds: u16,
	/// Whether the method has a generic implementation, which any receiver
	/// resolves to.
	pub(super) has_fallback: bool,
	/// The planner's re-entry depth at the call, as for [`CallSpec`].
	pub(super) plan_depth: u32,
	/// The positions among the method's arguments, the receiver not counted,
	/// at which an implementation the method resolves to invokes the closure
	/// it is handed: the operands [`Instr::Method`] links a unit for before
	/// the call.
	pub(super) closure_args: Vec<u16>,
	/// Whether the call is written in a schema statement's evaluated value,
	/// which names its argument errors as code a write reaches does
	/// ([`crate::exec::context::RootContext::writing`]).
	pub(super) writing: bool,
}

impl MethodSpec {
	/// Whether the method has an implementation for `value`: the answer
	/// [`MethodDescriptor::resolve`](crate::exec::function::MethodDescriptor::resolve)
	/// gives, from the mask alone.
	pub(super) fn resolves(&self, value: &Value) -> bool {
		self.has_fallback
			|| crate::exec::function::ValueKind::of(value)
				.is_some_and(|kind| self.kinds & kind.bit() != 0)
	}
}

assert_plain!(MethodSpec);

/// A builtin a body calls, by name.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct CallSpec {
	pub(super) name: String,
	/// The planner's re-entry depth at the call, which `eval::*` continues
	/// from when it plans its query string.
	pub(super) plan_depth: u32,
	/// For an index function, what it reports against.
	pub(super) index: Option<IndexBinding>,
	/// The positions among the call's arguments at which the builtin invokes
	/// the closure it is handed
	/// ([`ScalarFunction::closure_args`](crate::exec::function::ScalarFunction::closure_args)):
	/// the operands [`Instr::Call`] links a unit for before the call.
	pub(super) closure_args: Vec<u16>,
}

/// A `MATCHES` operator a body evaluates against the row: what it probes
/// and which rows an index answers for, as decided for the statement when it
/// is planned.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct MatchesSpec {
	pub(super) operator: crate::expr::operator::MatchesOperator,
	/// The field and query it probes an index with, when it denotes a probe.
	pub(super) probe: Option<crate::exec::vm::matches::MatchProbe>,
	/// Whether the statement's `WHERE` registers this expression.
	pub(super) registered: bool,
	/// The tables whose rows an index answers for.
	pub(super) executor_tables: crate::exec::vm::matches::ExecutorTables,
	/// The expression as an error names it.
	pub(super) sql: String,
	/// The pipeline of [`Program::host_code`] that reads the left operand off
	/// the row, for an operand that can cross a record link.
	pub(super) left: Option<u16>,
}

assert_plain!(MatchesSpec);

/// What an index function reports against: the `MATCHES` clause or the `KNN`
/// operator of the statement that calls it.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum IndexBinding {
	/// The full-text clause reading the declared position `path` of `table`
	/// for `query`; `None` for a field that reads no declared position.
	FullText {
		table: TableName,
		path: Option<crate::expr::logical::FieldPath>,
		query: String,
		/// Whether the query is the call's last argument, read when the call
		/// runs, rather than `query`.
		query_arg: bool,
	},
	/// The `KNN` operator of the statement whose scan publishes its distances
	/// in `slot` ([`Source::Knn::slot`]).
	Knn {
		slot: u16,
	},
}

assert_plain!(CallSpec);

/// A scalar the host evaluates outside a body — a paging clause, the record
/// a source names, a write's targets: the value it denotes, or a pipeline of
/// [`Program::host_code`] that computes it.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum HostScalar {
	Const(super::plain::ValueData<Value>),
	/// Host pipeline `pipeline`, whose one row emits the value.
	Code(u16),
}

/// An `INSERT`'s `INTO` clause: a table name or a string naming one.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct InsertInto {
	pub(super) scalar: HostScalar,
	/// The expression as written, which a refusal names rather than what it
	/// produced.
	pub(super) sql: String,
}

/// How an `INSERT` spells its rows.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum InsertRowsSpec {
	/// `INSERT INTO t (a, b) VALUES (1, 2), (3, 4)`: one tuple per row, each
	/// building a fresh empty object by writing its values through the places
	/// paired with them.
	Tuples(Vec<Vec<InsertFieldSpec>>),
	/// `INSERT INTO t { … }`, `[ … ]` or `(SELECT …)`: one value for the
	/// statement. An object is one row, an array one row per element.
	Value(HostScalar),
}

/// One `(place, value)` pair of a `VALUES` tuple.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct InsertFieldSpec {
	/// The place the value is written at, built out of the objects its path
	/// needs.
	pub(super) place: Vec<PlaceStepSpec>,
	pub(super) value: HostScalar,
}

/// A write target, as a write's source takes it.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Target {
	/// A scalar whose value names the subjects — a table, a record id, or an
	/// array of either.
	Scalar(HostScalar),
	/// `|t:n|`, whose ids are generated as the write consumes them.
	Mock(crate::expr::Mock),
}

assert_plain!(crate::expr::Mock);

/// A step of a `FETCH` path, as the walk takes it
/// ([`crate::exec::operators::fetch::fetch_fields_with`]).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FetchStepSpec {
	/// `.name`: the field of an object, or of every element of an array.
	Field(Strand),
	/// `.*`: every element of an array, every value of an object.
	All,
	/// The first element of an array.
	First,
	/// `[$]`: the last element of an array.
	Last,
	/// `[e]`: a computed index into an array or key into an object, the one
	/// value of host pipeline [`Program::host_code`]`[pipeline]`.
	Index {
		pipeline: u16,
	},
	/// A step the fetch does not walk through; the value reached so far is
	/// left as it is.
	Unsupported,
}

assert_plain!(FetchStepSpec);

/// The value that names what a count counts.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum CountedValue {
	/// Every record of the table.
	Table(TableName),
	/// The record id or record range that host pipeline
	/// [`Program::host_code`]`[pipeline]` emits.
	Code(u16),
}

/// How a count of the records of a table is answered when the caller's
/// reads are not permission checked, or the table's permission admits every
/// record.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum CountVia {
	/// The sum of the deltas of `COUNT` index `index`, and those the
	/// transaction has buffered.
	Deltas {
		index: crate::catalog::IndexId,
	},
	/// The entries of a B-tree index under an access, counted by their keys.
	BTreeKeys {
		index: crate::exec::index::access_path::IndexRef,
		access: super::plain::ValueData<crate::exec::index::access_path::BTreeAccess>,
	},
	/// The cardinality of an exact fused bitmap, with no record read.
	Bitmap(super::plain::ValueData<crate::exec::index::access_path::BitmapPlan>),
}

/// What a count source counts, as the count fast path's operators count it.
///
/// What each variant reads of the catalog — the permission, the `COUNT`
/// index, whether an ordinary pipeline stands beside it — is resolved when
/// the program is lowered; the run reads only the table's existence and its
/// lightweight relation kind, as of the statement's version.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum CountSource {
	/// Every record the evaluated `source` names: a table, a record id or a
	/// record range of `table`.
	All {
		table: TableName,
		source: CountedValue,
		select: super::plan::CountSelect,
		/// The unconditional `COUNT` index that answers a count of the whole
		/// table not read at a version.
		deltas: Option<crate::catalog::IndexId>,
		/// The unit that answers the statement as the ordinary pipeline
		/// does, for a conditional permission.
		ordinary: Option<UnitId>,
	},
	/// The records of `table` that the condition admits.
	Indexed {
		table: TableName,
		select: super::plan::CountSelect,
		via: CountVia,
		/// The unit that answers the statement as the ordinary pipeline
		/// does, for a conditional permission.
		ordinary: Option<UnitId>,
	},
}

// A count names an index's access and a bitmap plan, whose key values are
// plain exactly when `Value` is (see `Source`); the rest of the source is
// plain data.
assert_plain!(CountedValue);
assert_plain!(super::plan::CountSelect);
assert_plain!(crate::catalog::IndexId);

/// Where a pipeline's rows come from.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Source {
	/// Every record of `table`, in key order along `direction`.
	///
	/// `prefilter` is what the scan judges of a record's stored bytes before
	/// it decodes the record, when the body's first step is a filter whose
	/// whole predicate the bytes can judge. The filter still runs on every
	/// record the scan yields.
	Scan {
		table: TableName,
		direction: Direction,
		topk: Option<TopKScan>,
		prefilter: Option<super::plain::ValueData<crate::exec::pre_decode_filter::PreDecodeShape>>,
	},
	/// The records `record` names, evaluated once when the pipeline opens:
	/// one record for a record id, the records of its range in key order for
	/// a range.
	///
	/// A locked read (`for_update`) registers the record's key for
	/// commit-time conflict detection and refuses a range.
	Record {
		table: TableName,
		record: HostScalar,
		for_update: bool,
	},
	/// The records B-tree index `index` lists under `access`, in its key
	/// order along `direction`.
	Index {
		index: crate::exec::index::access_path::IndexRef,
		access: super::plain::ValueData<crate::exec::index::access_path::BTreeAccess>,
		direction: crate::kvs::Direction,
		/// The top-level fields read from each record; `None` reads whole
		/// records.
		decode: Option<crate::exec::operators::scan::common::DecodeFields>,
	},
	/// The records of `table` B-tree index `index` lists under an equality to
	/// a key read when the pipeline opens: the value at `outer`, a path of
	/// fields into `$parent` read through objects alone, or with no `outer`
	/// the key the join running the pipeline hands it
	/// ([`State::lookup_key`](super::interp::State)). Every record of
	/// `table`, in key order, where the key is one the index may not list
	/// every equal record under ([`super::plan::Plan::IndexLookup`]).
	Lookup {
		table: TableName,
		index: crate::exec::index::access_path::IndexRef,
		outer: Option<Vec<surrealdb_strand::Strand>>,
	},
	/// The records the B-tree scans `branches` list, each once: branch by
	/// branch, or merged by `merge`'s key.
	IndexUnion {
		branches: Vec<super::plan::IndexBranch>,
		merge: Option<crate::exec::operators::scan::MergeMode>,
		/// As for [`Source::Index`], for every branch.
		decode: Option<crate::exec::operators::scan::common::DecodeFields>,
	},
	/// The records of `table` whose doc-IDs survive fused bitmap plan `root`,
	/// in doc-ID order; `fallback` answers instead when an unanchored root
	/// overflows.
	Bitmap {
		table: TableName,
		root: super::plain::ValueData<crate::exec::index::access_path::BitmapPlan>,
		fallback: Option<Box<Source>>,
		/// As for [`Source::Index`].
		decode: Option<crate::exec::operators::scan::common::DecodeFields>,
	},
	/// The records full-text index `index` lists for `query`.
	FullText {
		table: TableName,
		index: crate::exec::index::access_path::IndexRef,
		query: String,
		operator: crate::expr::operator::MatchesOperator,
		/// As for [`Source::Index`].
		decode: Option<crate::exec::operators::scan::common::DecodeFields>,
	},
	/// The records KNN index `index` lists for nearest neighbors of `vector`.
	Knn {
		table: TableName,
		index: crate::exec::index::access_path::IndexRef,
		vector: Vec<crate::val::Number>,
		k: u32,
		ef: u32,
		/// Where the distance of each record found is published for
		/// `vector::distance::knn()`, when the statement reads them.
		slot: Option<u16>,
		/// What the search gates each candidate on.
		gate: Option<KnnGate>,
		/// Whether a read at the current version builds the rows from the
		/// ids the search returns, reading no record: nothing above the source
		/// reads more of a row than its `id`.
		ids_only: bool,
	},
	/// One row holding a count, shaped by `output`.
	Count {
		count: CountSource,
		output: crate::exec::operators::CountOutput,
	},
	/// The rows of each source in turn, in written order.
	Union(Vec<Source>),
	/// The rows a value in `FROM` yields: `value`, evaluated once when the
	/// pipeline opens, read as `read` says ([`super::plan::ValuesRead`]).
	/// A zero limit of the pipeline reads no record of a record id or table
	/// a dynamic read finds, and every other row is yielded and judged
	/// whatever the limit.
	Values {
		value: HostScalar,
		read: super::plan::ValuesRead,
	},
	/// The rows block statement [`Program::groups`]`[group]` yields — a
	/// statement in `FROM` — run once when the pipeline opens. A zero limit
	/// of the pipeline reads no record of the statement's own reads of
	/// storage, as it reads none of the pipeline's, unless the statement
	/// writes.
	Nested {
		group: u16,
	},
	/// The rows of block statement [`Program::groups`]`[build]` whose field
	/// `field` may equal the value at path `outer` of `$parent`
	/// ([`super::plan::Plan::Probe`]): the group run at the first opening of
	/// probe [`Program::probes`]`[probe]` and its rows kept, keyed by
	/// `field`, until the probe's owner starts again. At each opening where
	/// a row is kept, the key is read through objects alone; where the path
	/// meets any other value, every row is yielded. The rows are records,
	/// taken as a scan's are: a spent limit opens no probe, and rows past a
	/// limit are not read.
	Probe {
		build: u16,
		probe: u16,
		field: surrealdb_strand::Strand,
		outer: Vec<surrealdb_strand::Strand>,
	},
	/// The rows sort `sort` was handed, in its order.
	Sorted {
		sort: u16,
	},
	/// The groups aggregate `aggregate` folded, finished, in group-key order.
	Groups {
		aggregate: u16,
	},
	/// The rows split `split` turns each row of its input into, pushed
	/// through this pipeline as its input's pipeline settles each row, so a
	/// limit spent here stops the input's source. The host opens no source
	/// for this pipeline itself.
	Split {
		split: u16,
	},
	/// The pairings join `join` makes of the rows of its probe pipeline with
	/// the rows of its build table, pushed through this pipeline as the probe
	/// pipeline settles each row, so a limit spent here stops the probe
	/// pipeline's source. The host opens no source for this pipeline itself.
	Join {
		join: u16,
	},
	/// One row: the enclosing row of a statement in scalar position, and no
	/// row at all for a top-level one, whose body then evaluates against no
	/// document.
	Once,
	/// The subjects each of a write's `targets` names, evaluated one target
	/// at a time and each run as the record in flight. `deferable` says
	/// whether a table or record id names a record to create rather than
	/// records to read. `guarantees` says whether a table names only the
	/// record an `UPSERT` guarantees, its records being read by another
	/// pipeline of the write.
	WriteTargets {
		targets: Vec<Target>,
		deferable: bool,
		guarantees: bool,
	},
	/// An `INSERT`'s rows, all evaluated before the first is written, each
	/// run as the record in flight.
	WriteInserts {
		/// The `INTO` table, with the expression's text for the refusal of a
		/// value that names none.
		into: Option<InsertInto>,
		rows: InsertRowsSpec,
		/// `INSERT RELATION`: every row is an edge.
		relation: bool,
	},
	/// A `RELATE`'s edges, each run as the record in flight.
	WriteEdges {
		from: HostScalar,
		through: HostScalar,
		to: HostScalar,
	},
	/// The edges of lightweight relation `table`, which stores no record
	/// range: every edge, or those of the record `record` names — one edge
	/// for a record id, the edges its range holds for a range — read as
	/// stored records are, in canonical id order along `direction`. The
	/// table is a lightweight relation in the catalog the statement was
	/// planned against, and finding it otherwise when the source opens is an
	/// internal error.
	LightweightEdges {
		table: TableName,
		record: Option<HostScalar>,
		direction: Direction,
	},
}

/// What a KNN search gates each candidate record on, as pipelines of
/// [`Program::host_code`] that each judge one record: a pipeline emits a row
/// when the record passes the table's permission and then its condition, and
/// none when it does not.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct KnnGate {
	/// The table's permission alone, where the statement reads under one.
	pub(super) permission: Option<u16>,
	/// The permission, then the statement's conditions the search can judge
	/// with no query executor.
	pub(super) residual: Option<u16>,
	/// The permission, then every condition including the `MATCHES` clauses.
	pub(super) matches_condition: Option<u16>,
	/// The allow-list the search is restricted to.
	pub(super) prefilter: Option<KnnPrefilterSpec>,
}

/// A bitmap allow-list restricting a KNN search to the records that satisfy
/// the conjuncts it covers.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct KnnPrefilterSpec {
	pub(super) root: super::plain::ValueData<crate::exec::index::access_path::BitmapPlan>,
	/// The permission, then the conjuncts the bitmap does not cover.
	pub(super) residual: Option<u16>,
	/// Whether a `MATCHES` the bitmap leaves out is judged beside it.
	pub(super) uncovered_matches: bool,
}

/// The top-k threshold pushdown of a table scan: the first `ORDER BY` key of
/// the sort that feeds on the scan, which the scan reads from a record's
/// stored bytes to reject a record that cannot enter the sort's top k.
///
/// Sort `sort` of the program holding the scan publishes the threshold, in
/// its cell of the running state, and the scan arms a probe with that cell
/// each time it opens. A scan whose sort holds no cell reads no threshold.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct TopKScan {
	/// The first key's field path as pre-encoded wire segments.
	pub(super) path: Arc<[crate::exec::object_extract::PathSegment]>,
	pub(super) direction: crate::exec::operators::SortDirection,
	/// Whether the order has exactly one key: a tie with the threshold then
	/// rejects too.
	pub(super) single_key: bool,
	/// The index of the publishing sort in [`Program::sorts`].
	pub(super) sort: u16,
	/// The bound on how deep the walk over the stored bytes descends.
	pub(super) depth_limit: u32,
}

impl TopKScan {
	/// The scan of `key`, fed by sort `sort`.
	pub(super) fn new(key: &super::plan::TopKKey, sort: u16) -> Self {
		Self {
			path: Arc::clone(&key.path),
			direction: key.direction,
			single_key: key.single_key,
			sort,
			depth_limit: key.depth_limit,
		}
	}

	/// The probe armed with `cell`, the threshold cell of the sort for the
	/// run in progress.
	pub(super) fn probe(
		&self,
		cell: &Arc<crate::exec::topk_pushdown::TopKThresholdCell>,
	) -> Arc<crate::exec::topk_pushdown::TopKThresholdProbe> {
		crate::exec::topk_pushdown::TopKThresholdProbe::armed(
			Arc::clone(&self.path),
			self.direction,
			self.single_key,
			self.depth_limit,
			Arc::clone(cell),
		)
	}
}

// What a source that reads storage or an index holds is plain data: the
// engine resolves each into the structure it names when the source opens. The
// access of an index and the bitmap plan over it name key values, so they are
// plain exactly when `Value` is.
assert_plain!(TopKScan);
assert_plain!(super::plan::TopKKey);
assert_plain!(crate::exec::index::access_path::IndexRef);
assert_plain!(crate::exec::operators::scan::MergeMode);
assert_plain!(crate::exec::operators::scan::common::DecodeFields);
assert_plain!(crate::expr::operator::MatchesOperator);
assert_plain!(Vec<crate::val::Number>);

impl Source {
	/// The table this source reads, if it names one directly.
	pub(super) fn table(&self) -> Option<&TableName> {
		match self {
			Source::Scan {
				table,
				..
			}
			| Source::Record {
				table,
				..
			}
			| Source::Lookup {
				table,
				..
			}
			| Source::Bitmap {
				table,
				..
			}
			| Source::FullText {
				table,
				..
			}
			| Source::Knn {
				table,
				..
			}
			| Source::LightweightEdges {
				table,
				..
			} => Some(table),
			_ => None,
		}
	}

	/// What this scan judges of a record's stored bytes before it decodes the
	/// record, if anything.
	pub(super) fn prefilter(&self) -> Option<&crate::exec::pre_decode_filter::PreDecodeShape> {
		match self {
			Source::Scan {
				prefilter,
				..
			} => prefilter.as_deref(),
			_ => None,
		}
	}

	/// The top-k pushdown installed on this scan, if any.
	pub(super) fn topk(&self) -> Option<&TopKScan> {
		match self {
			Source::Scan {
				topk,
				..
			} => topk.as_ref(),
			_ => None,
		}
	}

	/// Whether every row comes from storage, so a body with no instruction
	/// that hands back to the host can run inside the storage cursor.
	pub(super) fn reads_storage(&self) -> bool {
		match self {
			Source::Scan {
				..
			}
			| Source::Record {
				..
			}
			| Source::Index {
				..
			}
			| Source::Lookup {
				..
			}
			| Source::IndexUnion {
				..
			}
			| Source::Bitmap {
				..
			}
			| Source::FullText {
				..
			}
			| Source::Knn {
				..
			}
			| Source::LightweightEdges {
				..
			} => true,
			Source::Union(sources) => sources.iter().all(Source::reads_storage),
			Source::Sorted {
				..
			}
			| Source::Groups {
				..
			}
			| Source::Split {
				..
			}
			| Source::Join {
				..
			}
			| Source::Values {
				..
			}
			| Source::Nested {
				..
			}
			| Source::Probe {
				..
			}
			| Source::Count {
				..
			}
			| Source::WriteTargets {
				..
			}
			| Source::WriteInserts {
				..
			}
			| Source::WriteEdges {
				..
			}
			| Source::Once => false,
		}
	}
}

/// The enforcement of a table's declarations a pipeline puts its rows through
/// at run time, as the table's scan does.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Enforce {
	/// The table's `PERMISSIONS FOR select`, its `COMPUTED` fields and its
	/// field permissions: a row the table's permission refuses is no row.
	Rows(TableName),
	/// The table's `COMPUTED` fields and field permissions alone, over an
	/// image of a record the statement wrote: the image is shown whatever the
	/// table's own permission says, as the output of a write is.
	Image(TableName),
	/// The table's field permissions alone, over an image of a record the
	/// statement wrote: nothing is derived, and each field the caller may not
	/// select is cut.
	Guards(TableName),
}

/// One pipeline: its source and the body run for each of its rows.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Pipeline {
	pub(super) source: Source,
	pub(super) code: Vec<Instr>,
	pub(super) registers: usize,
	/// Whether the body holds no instruction that hands back to the host, so
	/// a scan of a range runs inside the storage cursor, on the stored bytes.
	pub(super) in_place: bool,
	/// Whether the body holds no instruction that hands back to the host and
	/// reads rows a sort hands on, so a sorted record held as its stored bytes
	/// runs on those bytes, as [`Self::in_place`] does inside a cursor.
	pub(super) stored_rows: bool,
	/// What each row the source yields is put through before the body runs,
	/// where the body holds none of it. Such a pipeline runs on values, never
	/// on stored bytes or as a region.
	pub(super) enforce: Option<Enforce>,
	/// The paging clauses in the body. A pipeline one of whose limits admits no
	/// row reads no record: it does not open a source that reads storage, and
	/// reads no record of a value source's record ids or tables, nor of a
	/// statement in `FROM`.
	pub(super) limits: Vec<u16>,
	/// Whether the body may run its rows as a region: batch by batch, each
	/// batch inline or on a worker of its own, their outputs merged in batch
	/// order (see [`super::region`]). A region's body only reads, and pages
	/// only in the shape its merge replays, so every order of running its
	/// batches answers the rows the serial run answers.
	pub(super) region: bool,
	/// The statement of a block the pipeline belongs to, [`Program::groups`]
	/// `[group]`, run when the block reaches it; `None` for a pipeline of the
	/// program's own, run when the program runs.
	pub(super) group: Option<u16>,
}

impl Pipeline {
	/// Whether the body hands each row on as it is and does nothing else, so
	/// that its run over a row held in memory answers that row: no
	/// enforcement of the table's permissions, computed fields and field
	/// permissions is put on the row first ([`Self::enforce`]).
	pub(super) fn hands_row_on(&self) -> bool {
		self.enforce.is_none()
			&& matches!(
				self.code.as_slice(),
				[
					Instr::EmitEnd {
						src: Operand::Reg(ROW),
					},
					Instr::StopPipeline,
				] | [
					Instr::Emit {
						src: ROW,
					},
					Instr::EndRow,
					Instr::StopPipeline,
				]
			)
	}
}

impl Program {
	/// Whether the program is a small leaf: one pipeline of its own, run once,
	/// that pages, sorts, groups, joins, splits, fetches and writes nothing,
	/// reads no storage of its own, and runs no statement, user-defined
	/// function, closure, external call or index operator — a function body
	/// such as `{ LET $d = $x * 2; RETURN $d > 150 }`. It reaches no unit that
	/// could call back into the unit calling it.
	pub(super) fn small_leaf(&self) -> bool {
		matches!(
			self.pipelines.as_slice(),
			[Pipeline {
				source: Source::Once,
				group: None,
				..
			}]
		) && self.groups.is_empty()
			&& self.host_code.is_empty()
			&& self.limits.is_empty()
			&& self.sorts.is_empty()
			&& self.splits.is_empty()
			&& self.joins.is_empty()
			&& self.distincts.is_empty()
			&& self.fetches.is_empty()
			&& self.aggregates.is_empty()
			&& self.permissions.is_empty()
			&& self.write.is_none()
			&& self.tail.is_none()
			&& self.events.is_none()
			&& self.dispatch.is_none()
			&& self.matches.is_empty()
			&& self.externals.is_empty()
			&& self.degrees.is_empty()
			&& self.subqueries.is_empty()
			&& self.nested.is_empty()
			&& self.probes.is_empty()
			&& self.lookups.is_empty()
			&& self.functions.is_empty()
			&& self.closures.is_empty()
			&& self.closure_values.is_empty()
			&& self.units.is_empty()
	}
}

/// A statement of a block, compiled into the block's program: the pipelines,
/// paging clauses, sorts, aggregates and permission frames whose `group` is
/// its index, and the tables they share with the rest of the program.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupSpec {
	/// The statement's write, when it is one: the spec its write body is run
	/// with, as [`Program::write`] is a write program's.
	pub(super) write: Option<WriteSpec>,
	/// Whether the statement's value is its one row rather than its rows.
	pub(super) scalar: bool,
	/// Whether it reads no database: an expression statement.
	pub(super) rowless: bool,
	/// The sorts, aggregates and permission frames lowered for it, which each
	/// reach starts afresh: `start..end` of [`Program::sorts`],
	/// [`Program::aggregates`] and [`Program::permissions`].
	pub(super) sorts: std::ops::Range<u16>,
	pub(super) aggregates: std::ops::Range<u16>,
	pub(super) permissions: std::ops::Range<u16>,
	/// `VERSION`: the stamp every source of the statement reads at, evaluated
	/// in the block's frame when the block reaches it.
	pub(super) version: Option<HostScalar>,
	/// `TIMEOUT`: the duration the statement may run for, evaluated after
	/// its version; the host fails the statement once the deadline passes.
	pub(super) timeout: Option<HostScalar>,
	/// `ONLY`: the statement's value is its one row, `NONE` when it has none,
	/// and more than one row fails it.
	pub(super) only: bool,
}

/// A table a block's schema statement defines where there is none — a
/// `DEFINE FIELD`, `INDEX` or `EVENT` — whose speculative run defined it, and
/// which a write of the block before it may define first by writing to it
/// (`get_or_add_tb`): the [`Program::reservations`] of that run's definition
/// of the table. Whichever of the two defines the table takes them, so the
/// table holds the id, stamps and catalog version the statements after the
/// schema statement were planned against.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ImplicitTable {
	pub(super) table: TableName,
	/// The reservation of the table's id.
	pub(super) id: u32,
	/// The reservation of the stamp every one of its definition caches takes.
	pub(super) stamp: u32,
	/// The reservation of its catalog version.
	pub(super) version: u32,
}

assert_plain!(ImplicitTable);

/// A lowered physical plan.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Program {
	/// Run in order: a pipeline's source is a sink an earlier one filled.
	pub(super) pipelines: Vec<Pipeline>,
	pub(super) consts: super::plain::ValueData<Vec<Value>>,
	pub(super) fields: Vec<FieldName>,
	pub(super) paths: Vec<FieldPath>,
	pub(super) limits: Vec<LimitSpec>,
	pub(super) sorts: Vec<SortSpec>,
	pub(super) splits: Vec<SplitSpec>,
	pub(super) joins: Vec<JoinSpec>,
	/// For each distinct set, the group whose start empties it.
	pub(super) distincts: Vec<Option<u16>>,
	/// For each verdict a filter keeps on the conjuncts it judges once
	/// ([`Instr::VerdictJump`]), the group whose start forgets it.
	pub(super) verdicts: Vec<Option<u16>>,
	/// The paths each `FETCH` walks, as the planner resolves them.
	pub(super) fetches: Vec<Vec<Vec<FetchStepSpec>>>,
	pub(super) aggregates: Vec<AggregateSink>,
	/// The permission predicates the bodies evaluate: the frame each is
	/// evaluated in.
	pub(super) permissions: Vec<PermissionSpec>,
	/// The names each projection layer computes, which a self-dereference
	/// does not read from the row.
	pub(super) computed_names: Vec<Vec<surrealdb_strand::Strand>>,
	/// The table the statement reads, for `EXPLAIN`; none for a statement
	/// over a value.
	pub(super) table: Option<TableName>,
	/// For a write statement, what its body is run with.
	pub(super) write: Option<WriteSpec>,
	/// Whether the statement's value is its one row rather than its rows.
	pub(super) scalar: bool,
	/// Whether the statement reports rows whatever its value, as `RETURN`
	/// does, which leaves with a signal.
	pub(super) rows: bool,
	/// The statements a block runs and the statements nested in its
	/// expressions, each lowered into the program when the query arrives, run
	/// by [`BlockInstr::RunEmbedded`] and [`BlockInstr::Nested`].
	pub(super) groups: Vec<GroupSpec>,
	/// What `EXPLAIN ANALYZE` of a statement a block runs renders, by
	/// [`BlockInstr::RunAnalyzed`].
	pub(super) analyses: Vec<Analysis>,
	/// The late-bound statements a block runs, each served from the compile
	/// cache when the block reaches it, by [`BlockInstr::RunServed`].
	pub(super) served: Vec<ServedSpec>,
	/// The kinds `LET` coerces its values to and casts convert to.
	pub(super) kinds: Vec<crate::expr::Kind>,
	/// The parameters the bodies read, each keeping what it compiles of the
	/// `DEFINE PARAM` it finds.
	pub(super) params: Vec<ParamSpec>,
	/// The builtins the bodies call.
	pub(super) calls: Vec<CallSpec>,
	/// The `MATCHES` operators the bodies evaluate.
	pub(super) matches: Vec<MatchesSpec>,
	/// The calls of code the host runs that the bodies make.
	pub(super) externals: Vec<ExternalSpec>,
	/// The degree counts the bodies answer from the adjacency storage.
	pub(super) degrees: Vec<DegreeSpec>,
	/// The methods the bodies call.
	pub(super) methods: Vec<MethodSpec>,
	/// The record ids the bodies build.
	pub(super) record_ids: Vec<RecordIdSpec>,
	/// The statements the bodies evaluate in scalar position.
	pub(super) subqueries: Vec<SubquerySpec>,
	/// The statements the bodies evaluate in scalar position that are groups
	/// of the program, run by [`BlockInstr::Nested`].
	pub(super) nested: Vec<NestedSpec>,
	/// The probes of [`Source::Probe`], each with the group whose start
	/// drops the rows it keeps — the group evaluating the nested statement
	/// it reads for, the program's own pipelines for `None`.
	pub(super) probes: Vec<Option<u16>>,
	/// The graph lookups the bodies take.
	pub(super) lookups: Vec<LookupSpec>,
	/// The user-defined functions the bodies call, each with the subroutine
	/// its body is compiled to.
	pub(super) functions: Vec<FunctionSpec>,
	/// The closures whose bodies run in place of a builtin's invocations.
	pub(super) closures: Vec<ClosureSpec>,
	/// The closure literals whose values the bodies make.
	pub(super) closure_values: Vec<ClosureValueSpec>,
	/// The pipelines that compute host scalars, each run once with no row
	/// or the statement's `$this`.
	pub(super) host_code: Vec<Pipeline>,
	/// What was drawn while the program was planned against what its schema
	/// statements write (`vm::speculate`) — ids leased, uuids minted — which
	/// later statements were planned knowing: each is taken by the first run
	/// of the draw that reserved it.
	///
	/// A reservation is a plan-time lease belonging to the executor that
	/// lowered the program, consumed by that executor's first run. A program
	/// run anywhere else must be lowered there, so its draws are that
	/// executor's own. A reserved catalog id is held by the statement's
	/// planning until the operator running the program and its runs are
	/// dropped, and the draw that takes it confirms it; one no draw takes is
	/// released then (`vm::speculate::HeldIds`).
	pub(super) reservations: Vec<u128>,
	/// The tables whose definition the program's writes and its schema
	/// statements share reservations for, in the order the block's schema
	/// statements define them; a write takes the first whose reservations no
	/// run has taken ([`ImplicitTable`]).
	pub(super) implicit_tables: Vec<ImplicitTable>,
	/// The nested programs the root holds, addressed by [`UnitId`]; empty in
	/// a unit, whose ids index its root's, except in a unit that is a whole
	/// statement ([`SubquerySpec`]), which is the root of an arena of its own:
	/// the host pipelines it runs over states of their own find their units
	/// in it. A unit that holds the arena of a function bundle
	/// ([`UnitHead::entry`]) is the root of that arena: the body it names, and
	/// every body that body calls, index it, and run with it as the table their
	/// ids are read in.
	///
	/// A unit is shared by the one run that executes it as a program, which
	/// takes it by handle.
	pub(super) units: Arc<[Arc<Program>]>,
	/// The tables whose records the program reads by id where the table is
	/// known only from data, or `None` for a program that reads none: what
	/// [`Self::dispatch`] is built over.
	pub(super) reach: Option<super::dispatch::Reach>,
	/// The unit that reads each table the program's reads of records by id can
	/// reach, for a program that is the root of the reads it dispatches
	/// ([`super::dispatch`]); its ids index the table the program runs with.
	pub(super) dispatch: Option<Arc<super::dispatch::TableDispatch>>,
	/// For a program that is a unit, how it is called and the frame it was
	/// lowered for.
	pub(super) unit: Option<UnitHead>,
	/// The clauses of the statement's tail, which the run applies around the
	/// whole statement: see [`TailSpec`].
	pub(super) tail: Option<TailSpec>,
	/// For a unit that runs a table's events: the events, and the host
	/// pipelines of their clauses.
	pub(super) events: Option<Arc<super::write::LoweredEvents>>,
	pub(super) access_mode: AccessMode,
	pub(super) required_context: ContextLevel,
	/// Whether something the program calls answers differently for a call
	/// made after another, or leaves something behind, so its rows run one
	/// after another in the order read and no pipeline runs as a region.
	pub(super) keeps_row_order: bool,
	/// For the program of a whole request, the request: its steps in order
	/// and the programs of its statements, whose units are this program's.
	pub(super) request: Option<Box<RequestCode>>,
}

/// A request as one program: the steps the driver takes, in the order the
/// statements were written, and the program of each statement compiled when
/// the request arrived. A statement's program holds no units of its own: the
/// ids it calls index the units of the program holding the request.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RequestCode {
	pub(super) steps: Vec<RequestStep>,
	pub(super) statements: Vec<Arc<Program>>,
}

/// One step of a request, one per statement written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RequestStep {
	/// `BEGIN`: open the block's transaction.
	Begin,
	/// `COMMIT`: commit the block's transaction.
	Commit,
	/// `CANCEL`: cancel the block's transaction.
	Cancel,
	/// `OPTION name = what`.
	Option {
		name: String,
		what: bool,
	},
	/// `USE`: the namespace and database it names where they are written as
	/// names, `computed` where its run computes either.
	Use {
		ns: Option<String>,
		db: Option<String>,
		computed: bool,
	},
	/// A top-level `LET` of `name`, its value the program of statement
	/// `value` of [`RequestCode::statements`].
	Let {
		name: String,
		value: Option<u32>,
	},
	/// A statement the engine runs: the program of statement `statement`.
	Run {
		statement: Option<u32>,
	},
	/// A statement the driver runs from its surface form: `KILL`, `LIVE`,
	/// `SHOW` or `ACCESS`.
	Surface,
}

impl Program {
	/// The one pipeline of a unit that runs over a single value — a table's
	/// read unit, a recursion's body — which no block statement owns. The
	/// pipelines of the block statements the unit nests are lowered before
	/// it, so it is not necessarily the first. `None` where the program holds
	/// no such pipeline, or more than one.
	pub(super) fn own_pipeline(&self) -> Option<&Pipeline> {
		let mut own = self.pipelines.iter().filter(|pipeline| pipeline.group.is_none());
		match (own.next(), own.next()) {
			(Some(pipeline), None) => Some(pipeline),
			_ => None,
		}
	}
}

/// The tail of a statement as a program applies it: the `VERSION` its reads
/// are stamped with, the `TIMEOUT` that bounds its work, and whether `ONLY`
/// makes its value its one row.
///
/// The run evaluates `version`, then `timeout` in the frame the stamp makes,
/// before the statement starts; a statement still running at its deadline, or
/// finishing past it, fails with the timeout error, and `ONLY` over more than
/// one row fails with the single-output error.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct TailSpec {
	pub(super) version: Option<HostScalar>,
	pub(super) timeout: Option<HostScalar>,
	pub(super) only: bool,
}

// Every type a lowered program reaches is plain: this holds for the whole
// graph beneath the root, so a field anywhere in it that gives it a handle
// fails the build here.
assert_plain!(Program);
