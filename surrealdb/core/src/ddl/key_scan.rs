//! What naming the components of a record id is checked against.
//!
//! `DEFINE FIELD id ON reading TYPE [city: string, time: datetime]` gives the
//! elements of the record ids of `reading` names that every field, clause and
//! statement of the table can read. That reinterprets two things this module
//! reads:
//!
//! - the definitions of the table and of the views over it. A name that a definition already reads
//!   as a stored field changes meaning when the record id takes it ([`first_reader`]), and a name
//!   that a definition reads stops resolving when the record id drops or moves it. What a clause
//!   reads is judged by [`record_reads`]: the names it spells, or any name at all when it reads the
//!   record by names it computes or hands the record to a body that may read it. A view whose field
//!   list selects every field reads every name through the rows it holds, which keep the names they
//!   were written under.
//! - the records the table already holds ([`validate_record_keys`]). Every record id must be an
//!   array of the declared shape, and no record body may store a field under a component's name
//!   with a value other than the component's: such a field would be shadowed by the derived one and
//!   cut on the record's next write.
//! - whether the table may lose its structure at all ([`refuse_dropping_unordered_keys`]). A
//!   structure that does not order its keys exactly is what stops a scan being read as delivering
//!   `ORDER BY id`; the keys outlive the names, so the names go only while the records they key
//!   still walk in the order of their record ids.
//!
//! Both read through the transaction of the statement, so a table written or
//! defined earlier in the same transaction is seen as it stands.

use std::borrow::Cow;
use std::ops::ControlFlow as Step;
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::catalog::{
	self, DatabaseId, DefineDefault, KeyStructure, NamespaceId, Nonconformance, Permission,
	ViewDefinition,
};
use crate::exec::object_extract::{Extracted, PathSegment, extract_field_from_record_bytes};
use crate::exec::{Error as ExecError, ExecutionContext};
use crate::expr::computed_deps::extract_computed_deps;
use crate::expr::part::DestructurePart;
use crate::expr::reference::{Reference, ReferenceDeleteStrategy};
use crate::expr::visit::Visit;
use crate::expr::{
	BinaryOperator, Block, ControlFlow, Expr, Fields, Function, FunctionCall, Groups, Idiom,
	Literal, Part, PostfixOperator, PrefixOperator,
};
use crate::key::KVKeyDecode;
use crate::key::schema::{RecordKey, RecordPrefix};
use crate::kvs::{Direction, NORMAL_BATCH_SIZE, Transaction};
use crate::val::{RecordId, RecordIdKey, TableName, Value, try_encode_component};

/// Which definitions of a table count as readers of a component name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Readers {
	/// The indexes of the table and the views over it: what a name added to the
	/// record id would change the meaning of.
	Structural,
	/// Everything on the table that holds an expression: also its permissions,
	/// the clauses of its fields and its events. What a name removed from the
	/// record id would leave unresolved.
	All,
}

/// How a definition reads a component name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Via {
	/// By the name, which one of its clauses spells.
	Name,
	/// Through the rows of a view that selects every field and holds a row:
	/// each row holds every component under the name it was written with.
	Wildcard,
	/// By names a clause computes, or through a body it hands the record to:
	/// any name may be read ([`record_reads`]).
	Unresolved,
}

/// A definition that reads a component name as a field.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Reader {
	/// The component name that is read: for [`Via::Wildcard`] and
	/// [`Via::Unresolved`], the first of the names asked about.
	pub component: String,
	/// The kind of definition: `index`, `view`, `table permission`, `field` or
	/// `event`.
	pub what: &'static str,
	/// The name of the definition, or the operation of a table permission.
	pub name: String,
	/// How it reads the name.
	pub via: Via,
}

/// The component names two record id structures give, compared by position.
#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) struct NameChange {
	/// Names the new structure gives that the old one gave no component.
	pub added: Vec<String>,
	/// Names the old structure gave that the new one gives no component at the
	/// same position: a name that is dropped, renamed or moved.
	pub removed: Vec<String>,
}

/// The component names `new` gives against those `old` gave. A structure that
/// is `None`, or names nothing, gives no names.
pub(crate) fn name_change(old: Option<&KeyStructure>, new: Option<&KeyStructure>) -> NameChange {
	fn named(key: Option<&KeyStructure>) -> Vec<(u32, String)> {
		key.map(|key| {
			key.components
				.iter()
				.filter_map(|c| Some((c.position, c.name.as_ref()?.as_str().to_owned())))
				.collect()
		})
		.unwrap_or_default()
	}
	let (old, new) = (named(old), named(new));
	NameChange {
		added: new
			.iter()
			.filter(|(_, name)| !old.iter().any(|(_, before)| before == name))
			.map(|(_, name)| name.clone())
			.collect(),
		removed: old
			.iter()
			.filter(|entry| !new.contains(entry))
			.map(|(_, name)| name.clone())
			.collect(),
	}
}

/// The name of the field an idiom starts with.
pub(crate) fn root_name(idiom: &Idiom) -> Option<&str> {
	match idiom.0.first() {
		Some(Part::Field(root)) => Some(root.as_str()),
		_ => None,
	}
}

/// The key structure of the record ids of `table`, from the stored definition
/// of its `id` field.
pub(crate) async fn stored_key(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
) -> Result<Option<Arc<KeyStructure>>> {
	Ok(txn.get_tb_field(ns, db, table, "id", None).await?.and_then(|id| id.live_key().cloned()))
}

/// What the evaluation of a clause binds to the record of the table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadContext {
	/// A clause evaluated with the record as its document: the clauses of a
	/// field, a table permission, the clauses of a view and the `COUNT`
	/// condition of an index. `$this`, `$self` and `$parent` are the record.
	Record,
	/// The `WHEN` and `THEN` clauses of an event, whose document is the record
	/// the event fires for. `$after`, `$before` and `$value` are that record
	/// too, and `$input`, the data the statement wrote, holds its fields under
	/// the same names.
	Event,
}

/// What a clause reads of the record it is evaluated against.
#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) struct RecordReads {
	/// The fields it reads by a name it spells.
	pub names: Vec<String>,
	/// Whether it may also read fields by names it does not spell, so that
	/// any name may be one it reads.
	pub unresolved: bool,
}

impl RecordReads {
	fn extend(&mut self, other: RecordReads) {
		self.names.extend(other.names);
		self.unresolved |= other.unresolved;
	}
}

/// What `expr`, evaluated in `context`, reads of the record.
///
/// A name is read when the clause spells it: a field it names bare (`city`,
/// counted where [`extract_computed_deps`] counts it), a static path or
/// destructure through a parameter bound to the record (`$this.city`,
/// `$parent['city']`, `$this.{city}`) or through the document where the
/// document is the record (`@.city`), or a literal `type::field('city')`. A
/// bare name inside a nested write statement over another table
/// (`UPDATE other SET n += 1`) is counted as a read of the record's field of
/// that name too: it fails closed, and the refusal then names a read that is
/// another table's field.
///
/// The clause reads the record by names that cannot be determined
/// (`unresolved`) when, where the record is the document its evaluation reads
/// (outside a nested statement, unless that statement reads or writes the
/// record itself):
///
/// - it calls a user-defined, script, module or silo function, whose body reads the fields of the
///   document by names the call does not spell (`fn::city()` returning `city`). The body is not
///   read.
/// - it calls `type::field` or `type::fields` with anything but literal static paths.
///
/// and, wherever the record is reachable through a parameter bound to it or
/// the document:
///
/// - it reads the record through a name it computes (`$this['ci' + 'ty']`) or a part that is not a
///   static name, a destructure or a graph lookup (`$this.*.len()`, `$this[0]`).
/// - it hands the record itself to a function, a method, a closure or a cast as an argument or
///   receiver (`fn::city_of($this)`, `object::keys($after)`, `$this.keys()`, `object::keys(@)`),
///   directly or inside an array, object or operator that passes it on, or selects every field of
///   it (`SELECT * FROM $parent`). The builtins that only judge the type of a value or read a
///   record's id (`type::is_*`, `record::id`, `record::tb`, `record::table`, `meta::id`,
///   `meta::tb`) are handed it as values they cannot read a name of.
/// - it reads a part of a value that holds the record (`{ r: $this }.r.city`, `[$this][0]`), or a
///   nested statement reads or writes such a value (`SELECT * FROM [$this]`): the part reaches the
///   record, or a field of it, by a name that is not one of the record's.
///
/// Comparing the record (`$before != $after`), copying it (`SET af = $after`,
/// `RETURN $after`, `CONTENT $after`), holding it in a value that is not read
/// further (`{ copy: $this }`) and walking a graph from it read no name. A
/// variable bound to the record (`LET $r = $after`, `FOR $r IN [$after]`) is
/// the record from then on, and one bound to a value holding it
/// (`LET $o = { r: $this }`) holds it from then on.
///
/// Inside a `[WHERE ...]` filter `$this` and `$self` are the element filtered,
/// and inside a graph lookup's clauses they and the document are the edge or
/// the record looked up. Inside a nested statement they and the document are
/// the rows it reads or writes, which are the record only when the statement
/// targets a value bound to it; `$parent` stays bound to the record
/// everywhere, which over-approximates it below the first nested level.
///
/// What is not found: a read of a component from another record, through a
/// record link or a graph hop into the table (`prev.city`, `id.city`) or a
/// statement over another table that reads this one, whether by a path or by
/// a name passed as a string to a builtin (`object::get(prev.*, 'city')`); and
/// what a function body reads of a record handed to it as anything but the
/// record or the document (`fn::f(prev)`).
pub(crate) fn record_reads(expr: &Expr, context: ReadContext) -> RecordReads {
	let mut reach = RecordReach {
		context,
		names: extract_computed_deps(expr).fields,
		unresolved: false,
		this_is_record: true,
		doc_is_record: true,
		in_sink: false,
		aliases: Vec::new(),
	};
	reach.walk(expr);
	RecordReads {
		names: reach.names,
		unresolved: reach.unresolved,
	}
}

/// The builtins a value bound to the record can be handed without a name of
/// it being read: each judges only the type of its argument or reads the id
/// of a record.
fn reads_no_name_of_its_argument(name: &str) -> bool {
	name.starts_with("type::is_")
		|| name.starts_with("type::is::")
		|| matches!(name, "record::id" | "record::tb" | "record::table" | "meta::id" | "meta::tb")
}

/// Whether the operator's result is a boolean judged from its operands, so
/// that an operand is not passed on.
fn judges_its_operands(op: &BinaryOperator) -> bool {
	match op {
		BinaryOperator::Equal
		| BinaryOperator::ExactEqual
		| BinaryOperator::NotEqual
		| BinaryOperator::AllEqual
		| BinaryOperator::AnyEqual
		| BinaryOperator::LessThan
		| BinaryOperator::LessThanEqual
		| BinaryOperator::MoreThan
		| BinaryOperator::MoreThanEqual
		| BinaryOperator::Contain
		| BinaryOperator::NotContain
		| BinaryOperator::ContainAll
		| BinaryOperator::ContainAny
		| BinaryOperator::ContainNone
		| BinaryOperator::Inside
		| BinaryOperator::NotInside
		| BinaryOperator::AllInside
		| BinaryOperator::AnyInside
		| BinaryOperator::NoneInside
		| BinaryOperator::Outside
		| BinaryOperator::Intersects
		| BinaryOperator::Matches(_)
		| BinaryOperator::NearestNeighbor(_) => true,
		// The arithmetic and range operators build a value from their
		// operands, and the logical ones return one of them.
		BinaryOperator::Subtract
		| BinaryOperator::Add
		| BinaryOperator::Multiply
		| BinaryOperator::Divide
		| BinaryOperator::Remainder
		| BinaryOperator::Power
		| BinaryOperator::Or
		| BinaryOperator::And
		| BinaryOperator::NullCoalescing
		| BinaryOperator::TenaryCondition
		| BinaryOperator::Range
		| BinaryOperator::RangeInclusive
		| BinaryOperator::RangeSkip
		| BinaryOperator::RangeSkipInclusive => false,
	}
}

/// Whether the parts that follow a value bound to the record leave the record
/// itself, or every field of it: none, `.*`, or a destructure that keeps the
/// rest (`.{city, ..}`).
fn keeps_the_record(rest: &[Part]) -> bool {
	match rest.iter().find(|part| !matches!(part, Part::Optional)) {
		None | Some(Part::All) => true,
		Some(Part::Destructure(parts)) => parts.iter().any(|p| matches!(p, DestructurePart::Rest)),
		Some(_) => false,
	}
}

/// Whether a `type::field` or `type::fields` call names its paths as literals
/// whose roots are static field names.
fn names_static_paths(name: &str, arguments: &[Expr]) -> bool {
	let static_root = |path: &Expr| {
		if let Expr::Literal(Literal::String(path)) = path {
			crate::syn::idiom(path.as_str())
				.is_ok_and(|idiom| matches!(idiom.0.first(), Some(Part::Field(_))))
		} else {
			false
		}
	};
	match (name, arguments) {
		("type::field", [path]) => static_root(path),
		("type::fields", [Expr::Literal(Literal::Array(paths))]) => paths.iter().all(static_root),
		_ => false,
	}
}

/// The walk of [`record_reads`].
struct RecordReach {
	context: ReadContext,
	names: Vec<String>,
	unresolved: bool,
	/// Whether `$this` and `$self` are the record where the walk is.
	this_is_record: bool,
	/// Whether the document a function body or a projection reads is the
	/// record where the walk is.
	doc_is_record: bool,
	/// Whether the value the walk is at is handed to a body or a builtin that
	/// may read any of its fields.
	in_sink: bool,
	/// The variables bound where the walk is, innermost last, with what each
	/// holds of the record. A `LET` binds its name until the end of the block
	/// that holds it, and a `FOR` its variable for its body, as SurrealQL
	/// scopes them; a later binding of a name shadows an earlier one.
	aliases: Vec<(String, Carry)>,
}

/// What a value holds of the record a clause is evaluated against.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Carry {
	/// Nothing of it that the walk tracks: a field of it, a value unrelated
	/// to it, or a judgement of it.
	Nothing,
	/// The record itself, or every field of it: a part that follows reads a
	/// field of the record.
	Record,
	/// A value that holds the record inside it (`[$this]`, `{ r: $this }`),
	/// or may be it: a part that follows reaches the record, or a field of it,
	/// by a name that is not one of the record's.
	Container,
}

impl Carry {
	/// What a value that is either `self` or `other` holds of the record.
	fn either(self, other: Carry) -> Carry {
		match (self, other) {
			(Carry::Nothing, other) | (other, Carry::Nothing) => other,
			(Carry::Record, Carry::Record) => Carry::Record,
			_ => Carry::Container,
		}
	}

	/// What a value that holds an element carrying `self` holds of the record.
	fn held(self) -> Carry {
		match self {
			Carry::Nothing => Carry::Nothing,
			Carry::Record | Carry::Container => Carry::Container,
		}
	}
}

impl RecordReach {
	fn walk(&mut self, expr: &Expr) {
		let Ok(()) = crate::expr::visit::Visitor::visit_expr(self, expr);
	}

	/// What `param` holds of the record where the walk is.
	fn bound(&self, param: &str) -> Carry {
		let record = |is: bool| {
			if is {
				Carry::Record
			} else {
				Carry::Nothing
			}
		};
		match param {
			"this" | "self" => record(self.this_is_record),
			"parent" => Carry::Record,
			"after" | "before" | "value" | "input" => record(self.context == ReadContext::Event),
			other => self
				.aliases
				.iter()
				.rev()
				.find(|(alias, _)| alias == other)
				.map_or(Carry::Nothing, |(_, carry)| *carry),
		}
	}

	/// Run `walk` with the walk's bindings set as given, and restore them.
	fn scoped(&mut self, this: bool, doc: bool, sink: bool, walk: impl FnOnce(&mut Self)) {
		let saved = (self.this_is_record, self.doc_is_record, self.in_sink);
		(self.this_is_record, self.doc_is_record, self.in_sink) = (this, doc, sink);
		walk(self);
		(self.this_is_record, self.doc_is_record, self.in_sink) = saved;
	}

	/// Run `walk` with the values it reaches handed on (`sink`) or not.
	fn sinking(&mut self, sink: bool, walk: impl FnOnce(&mut Self)) {
		self.scoped(self.this_is_record, self.doc_is_record, sink, walk);
	}

	/// What the value of `expr` holds of the record.
	fn carry(&self, expr: &Expr) -> Carry {
		match expr {
			Expr::Param(param) => self.bound(param.as_str()),
			Expr::Idiom(idiom) => match idiom.0.as_slice() {
				[Part::Start(start), rest @ ..] => match self.carry(start) {
					Carry::Record if keeps_the_record(rest) => Carry::Record,
					Carry::Record | Carry::Nothing => Carry::Nothing,
					Carry::Container => Carry::Container,
				},
				[Part::Doc, rest @ ..] if self.doc_is_record && keeps_the_record(rest) => {
					Carry::Record
				}
				_ => Carry::Nothing,
			},
			Expr::Literal(Literal::Array(items) | Literal::Set(items)) => items
				.iter()
				.fold(Carry::Nothing, |carry, item| carry.either(self.carry(item).held())),
			Expr::Literal(Literal::Object(entries)) => entries
				.iter()
				.fold(Carry::Nothing, |carry, entry| carry.either(self.carry(&entry.value).held())),
			Expr::Binary {
				left,
				op,
				right,
			} if !judges_its_operands(op) => self.carry(left).either(self.carry(right)),
			Expr::Prefix {
				op,
				expr,
			} if !matches!(op, PrefixOperator::Not) => self.carry(expr),
			Expr::Block(block) => self.block_carry(block),
			Expr::Return(output) => self.carry(&output.what),
			// A closure whose body may yield the record holds it: what a call
			// of it yields is not followed, so the call is judged as handing
			// the record on.
			Expr::Closure(closure) => self.carry(&closure.body).held(),
			Expr::IfElse(branches) => branches
				.exprs
				.iter()
				.map(|(_, then)| then)
				.chain(branches.close.as_ref())
				.fold(Carry::Nothing, |carry, then| carry.either(self.carry(then))),
			Expr::Literal(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Constant(_)
			| Expr::Prefix {
				..
			}
			| Expr::Postfix {
				..
			}
			| Expr::Binary {
				..
			}
			| Expr::FunctionCall(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Throw(_)
			| Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Upsert(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Alter(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Sleep(_)
			| Expr::Explain {
				..
			} => Carry::Nothing,
		}
	}

	/// What the value of `block` holds of the record: what its last statement
	/// holds, or any `RETURN` among its statements or inside a branch or an
	/// inner block or `FOR` body of it, with the variables its `LET`s bind in
	/// scope.
	fn block_carry(&self, block: &Block) -> Carry {
		let mut inner = RecordReach {
			context: self.context,
			names: Vec::new(),
			unresolved: false,
			this_is_record: self.this_is_record,
			doc_is_record: self.doc_is_record,
			in_sink: self.in_sink,
			aliases: self.aliases.clone(),
		};
		let mut carry = Carry::Nothing;
		for (at, statement) in block.0.iter().enumerate() {
			match statement {
				Expr::Let(statement) => {
					let bound = inner.carry(&statement.what);
					inner.aliases.push((statement.name.as_str().to_owned(), bound));
				}
				// A `RETURN` ends the block with its value, and one inside a
				// branch, an inner block or a `FOR` body may; what a branch ends with
				// otherwise is counted too, which over-approximates.
				Expr::Return(_) | Expr::IfElse(_) | Expr::Block(_) => {
					carry = carry.either(inner.carry(statement))
				}
				Expr::Foreach(statement) => {
					let element = inner.element_carry(&statement.range);
					inner.aliases.push((statement.param.as_str().to_owned(), element));
					carry = carry.either(inner.block_carry(&statement.block));
					inner.aliases.pop();
				}
				Expr::Literal(_)
				| Expr::Param(_)
				| Expr::Idiom(_)
				| Expr::Table(_)
				| Expr::Mock(_)
				| Expr::Constant(_)
				| Expr::Prefix {
					..
				}
				| Expr::Postfix {
					..
				}
				| Expr::Binary {
					..
				}
				| Expr::FunctionCall(_)
				| Expr::Closure(_)
				| Expr::Break
				| Expr::Continue
				| Expr::Throw(_)
				| Expr::Select(_)
				| Expr::Create(_)
				| Expr::Update(_)
				| Expr::Upsert(_)
				| Expr::Delete(_)
				| Expr::Relate(_)
				| Expr::Insert(_)
				| Expr::Define(_)
				| Expr::Remove(_)
				| Expr::Rebuild(_)
				| Expr::Alter(_)
				| Expr::Info(_)
				| Expr::Sleep(_)
				| Expr::Explain {
					..
				} => {
					if at + 1 == block.0.len() {
						carry = carry.either(inner.carry(statement));
					}
				}
			}
		}
		carry
	}

	/// What each element a `FOR` iterates over `range` holds of the record:
	/// the elements of an array or set it spells, or else what the range
	/// holds.
	fn element_carry(&self, range: &Expr) -> Carry {
		if let Expr::Literal(Literal::Array(items) | Literal::Set(items)) = range {
			items.iter().fold(Carry::Nothing, |carry, item| carry.either(self.carry(item)))
		} else {
			self.carry(range)
		}
	}

	/// The parts that follow a parameter bound to the record.
	fn record_path(&mut self, rest: &[Part]) {
		let Some(at) = rest.iter().position(|part| !matches!(part, Part::Optional)) else {
			// The record itself.
			if self.in_sink {
				self.unresolved = true;
			}
			return;
		};
		let after = &rest[at + 1..];
		match &rest[at] {
			Part::Field(name) => self.names.push(name.as_str().to_owned()),
			Part::Value(Expr::Literal(Literal::String(name))) => {
				self.names.push(name.as_str().to_owned())
			}
			Part::Destructure(parts) => {
				let keeps_rest = parts.iter().any(|p| matches!(p, DestructurePart::Rest));
				if keeps_rest && (self.in_sink || !after.is_empty()) {
					self.unresolved = true;
				}
				self.destructured(parts);
			}
			// A graph lookup from the record reads its edges, not its fields.
			Part::Lookup(_) => {}
			// Every field of the record, passed on or read further.
			Part::All => {
				if self.in_sink || !after.is_empty() {
					self.unresolved = true;
				}
			}
			// A computed name, a method of the record, or a part that reads the
			// record as something other than an object of named fields.
			Part::Value(_)
			| Part::Method(..)
			| Part::Where(_)
			| Part::First
			| Part::Last
			| Part::Flatten
			| Part::Doc
			| Part::Recurse(..)
			| Part::RepeatRecurse
			| Part::Start(_)
			| Part::Optional => self.unresolved = true,
		}
		for part in &rest[at..] {
			let Ok(()) = crate::expr::visit::Visitor::visit_part(self, part);
		}
	}

	/// The names a destructure of the record reads.
	fn destructured(&mut self, parts: &[DestructurePart]) {
		for part in parts {
			match part {
				DestructurePart::All(name)
				| DestructurePart::Field(name)
				| DestructurePart::Destructure(name, _) => self.names.push(name.as_str().to_owned()),
				DestructurePart::Aliased(_, path) => match root_name(path) {
					Some(root) => self.names.push(root.to_owned()),
					None => self.unresolved = true,
				},
				DestructurePart::Rest => {}
			}
		}
	}

	fn call(&mut self, call: &FunctionCall) {
		let sink = match &call.receiver {
			Function::Normal(name) => {
				if matches!(name.as_str(), "type::field" | "type::fields")
					&& self.doc_is_record
					&& !names_static_paths(name, &call.arguments)
				{
					self.unresolved = true;
				}
				!reads_no_name_of_its_argument(name)
			}
			Function::Custom(_)
			| Function::Script(_)
			| Function::Module(..)
			| Function::Silo {
				..
			} => {
				if self.doc_is_record {
					self.unresolved = true;
				}
				true
			}
			// A model is applied to its arguments alone.
			Function::Model(_) => true,
		};
		self.sinking(sink, |reach| {
			for argument in &call.arguments {
				reach.walk(argument);
			}
		});
	}

	/// A statement nested in the clause: its targets are evaluated where it
	/// is written, and the rest against the rows it reads or writes.
	fn nested(&mut self, expr: &Expr) {
		let (targets, rows_are_targets): (Vec<&Expr>, bool) = match expr {
			Expr::Select(s) => (s.what.iter().collect(), true),
			Expr::Create(s) => (s.what.iter().collect(), true),
			Expr::Update(s) => (s.what.iter().collect(), true),
			Expr::Upsert(s) => (s.what.iter().collect(), true),
			Expr::Delete(s) => (s.what.iter().collect(), true),
			// The row of a `RELATE` is the edge it writes, and of an `INSERT`
			// the record it inserts.
			Expr::Relate(s) => (vec![&s.from, &s.to, &s.through], false),
			Expr::Insert(s) => (s.into.iter().collect(), false),
			Expr::Literal(_)
			| Expr::Param(_)
			| Expr::Idiom(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Block(_)
			| Expr::Constant(_)
			| Expr::Prefix {
				..
			}
			| Expr::Postfix {
				..
			}
			| Expr::Binary {
				..
			}
			| Expr::FunctionCall(_)
			| Expr::Closure(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Return(_)
			| Expr::Throw(_)
			| Expr::IfElse(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Alter(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Sleep(_)
			| Expr::Explain {
				..
			} => (Vec::new(), false),
		};
		let carried =
			targets.iter().fold(Carry::Nothing, |carry, target| carry.either(self.carry(target)));
		// A statement over a value that holds the record reads rows that may be
		// the record, by names the walk does not follow.
		if rows_are_targets && carried == Carry::Container {
			self.unresolved = true;
		}
		let on_record = rows_are_targets && carried == Carry::Record;
		self.sinking(false, |reach| {
			for target in &targets {
				reach.walk(target);
			}
		});
		if let Expr::Select(select) = expr
			&& on_record
			&& select.fields.has_all_selection()
		{
			// `SELECT * FROM $this` is every field of the record, read by no name.
			self.unresolved = true;
		}
		let sink = self.in_sink;
		self.scoped(on_record, on_record, sink, |reach| {
			let Ok(()) = expr.visit(reach);
		});
	}
}

impl crate::expr::visit::Visitor for RecordReach {
	type Error = std::convert::Infallible;

	fn visit_expr(&mut self, expr: &Expr) -> Result<(), Self::Error> {
		match expr {
			Expr::Param(param) => {
				if self.in_sink && self.bound(param.as_str()) != Carry::Nothing {
					self.unresolved = true;
				}
			}
			Expr::FunctionCall(call) => self.call(call),
			Expr::Binary {
				left,
				op,
				right,
			} => {
				let sink = self.in_sink && !judges_its_operands(op);
				self.sinking(sink, |reach| {
					reach.walk(left);
					reach.walk(right);
				});
			}
			Expr::Prefix {
				op,
				expr: operand,
			} => {
				// A cast is a builtin conversion, which may render the names of
				// the record it converts.
				let sink = match op {
					PrefixOperator::Not => false,
					PrefixOperator::Cast(_) => true,
					PrefixOperator::Positive
					| PrefixOperator::Negate
					| PrefixOperator::Range
					| PrefixOperator::RangeInclusive => self.in_sink,
				};
				self.sinking(sink, |reach| reach.walk(operand));
			}
			Expr::Postfix {
				expr: operand,
				op,
			} => match op {
				PostfixOperator::MethodCall(_, arguments) | PostfixOperator::Call(arguments) => {
					self.sinking(true, |reach| {
						reach.walk(operand);
						for argument in arguments {
							reach.walk(argument);
						}
					});
				}
				PostfixOperator::Range | PostfixOperator::RangeSkip => self.walk(operand),
			},
			Expr::Let(statement) => {
				self.walk(&statement.what);
				let carry = self.carry(&statement.what);
				self.aliases.push((statement.name.as_str().to_owned(), carry));
			}
			Expr::Block(_) => {
				let scope = self.aliases.len();
				expr.visit(self)?;
				self.aliases.truncate(scope);
			}
			// The range is evaluated in the scope around the `FOR`, before its
			// variable is bound; the variable is bound for the body only.
			Expr::Foreach(statement) => {
				self.walk(&statement.range);
				let scope = self.aliases.len();
				let carry = self.element_carry(&statement.range);
				self.aliases.push((statement.param.as_str().to_owned(), carry));
				crate::expr::visit::Visitor::visit_block(self, &statement.block)?;
				self.aliases.truncate(scope);
			}
			// A closure's parameters are bound for its body only, and shadow a
			// variable of the same name around it; what it captures stays bound.
			Expr::Closure(closure) => {
				let scope = self.aliases.len();
				for (param, _) in &closure.args {
					self.aliases.push((param.as_str().to_owned(), Carry::Nothing));
				}
				self.walk(&closure.body);
				self.aliases.truncate(scope);
			}
			Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Upsert(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_) => self.nested(expr),
			Expr::Literal(_)
			| Expr::Idiom(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Constant(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Return(_)
			| Expr::Throw(_)
			| Expr::IfElse(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Alter(_)
			| Expr::Info(_)
			| Expr::Sleep(_)
			| Expr::Explain {
				..
			} => expr.visit(self)?,
		}
		Ok(())
	}

	fn visit_idiom(&mut self, idiom: &Idiom) -> Result<(), Self::Error> {
		let reaches = |rest: &[Part]| rest.iter().any(|part| !matches!(part, Part::Optional));
		match idiom.0.as_slice() {
			[Part::Start(start), rest @ ..] if self.carry(start) == Carry::Record => {
				if !matches!(start, Expr::Param(_)) {
					self.sinking(false, |reach| reach.walk(start));
				}
				self.record_path(rest);
			}
			// A part of a value that holds the record reaches the record, or a
			// field of it, by a name that is not one of the record's.
			[Part::Start(start), rest @ ..]
				if reaches(rest) && self.carry(start) == Carry::Container =>
			{
				self.unresolved = true;
				self.sinking(false, |reach| reach.walk(start));
				for part in rest {
					self.visit_part(part)?;
				}
			}
			// The document, where it is the record.
			[Part::Doc, rest @ ..] if self.doc_is_record => self.record_path(rest),
			parts => {
				for part in parts {
					self.visit_part(part)?;
				}
			}
		}
		Ok(())
	}

	fn visit_part(&mut self, part: &Part) -> Result<(), Self::Error> {
		match part {
			// The element filtered.
			Part::Where(condition) => {
				let doc = self.doc_is_record;
				self.scoped(false, doc, false, |reach| reach.walk(condition));
			}
			// The edges and records looked up.
			Part::Lookup(lookup) => {
				self.scoped(false, false, false, |reach| {
					let Ok(()) = crate::expr::visit::Visitor::visit_lookup(reach, lookup);
				});
			}
			Part::Method(_, arguments) => {
				self.sinking(true, |reach| {
					for argument in arguments {
						reach.walk(argument);
					}
				});
			}
			Part::All
			| Part::Flatten
			| Part::Last
			| Part::First
			| Part::Field(_)
			| Part::Value(_)
			| Part::Start(_)
			| Part::Destructure(_)
			| Part::Optional
			| Part::Recurse(..)
			| Part::Doc
			| Part::RepeatRecurse => part.visit(self)?,
		}
		Ok(())
	}
}

/// The first of `names` that `reads` holds.
pub(crate) fn first_of<'a>(names: &'a [String], reads: &[String]) -> Option<&'a String> {
	names.iter().find(|name| reads.contains(name))
}

/// What the permission reads of the record.
fn permission_reads(permission: &Permission) -> RecordReads {
	match permission {
		Permission::Specific(expr) => record_reads(expr, ReadContext::Record),
		Permission::None | Permission::Full => RecordReads::default(),
	}
}

/// What the clauses of a view read of its source records: its field list,
/// its condition and its group expressions. The `*` of `SELECT *` is not
/// among them; [`selects_every_field`] tells it.
///
/// A group that spells the alias of a projected field groups by that field's
/// expression, as in `SELECT id[0] AS city ... GROUP BY city`, so it reads what
/// the expression reads (already counted with the field list) and not a stored
/// field of the alias's name.
pub(crate) fn view_reads(view: &ViewDefinition) -> RecordReads {
	let (fields, condition, groups) = view_clauses(view);
	let mut reads = RecordReads::default();
	for selector in fields.iter_non_all_fields() {
		reads.extend(record_reads(&selector.expr, ReadContext::Record));
	}
	if let Some(condition) = condition {
		reads.extend(record_reads(condition, ReadContext::Record));
	}
	for group in groups.into_iter().flat_map(|groups| groups.0.iter()) {
		let is_alias = fields
			.iter_non_all_fields()
			.any(|selector| selector.alias.as_ref().is_some_and(|alias| *alias == group.0));
		if !is_alias {
			reads.extend(record_reads(&Expr::Idiom(group.0.clone()), ReadContext::Record));
		}
	}
	reads
}

/// The field list, condition and groups of a view.
fn view_clauses(view: &ViewDefinition) -> (&Fields, Option<&Expr>, Option<&Groups>) {
	match view {
		ViewDefinition::Materialized {
			fields,
			condition,
			..
		} => (fields, condition.as_ref(), None),
		ViewDefinition::Aggregated {
			fields,
			condition,
			groups,
			..
		} => (fields, condition.as_ref(), Some(groups)),
		ViewDefinition::Select {
			fields,
			condition,
			groups,
			..
		} => (fields, condition.as_ref(), groups.as_ref()),
	}
}

/// Whether the field list of a view selects every field of its source records
/// (`SELECT *`, `SELECT *, temp * 2 AS x`). Its rows then hold every
/// component of the record id they were written under, by name.
fn selects_every_field(view: &ViewDefinition) -> bool {
	view_clauses(view).0.has_all_selection()
}

/// Whether the table `table`, a view or a table a view reads, holds a record.
/// Its graph edges and the references other records hold to it are not records
/// of it.
///
/// The answer is the defining transaction's snapshot. A write planned against
/// the catalog before the change committed can still add a row under the old
/// record id afterwards, to a table found empty here or to a view defined again
/// after the change; that is the stale-writer race of the record scan, which no
/// row count closes (see ADR 0008, *Operational notes*).
pub(crate) async fn holds_rows(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
) -> Result<bool> {
	let range = RecordPrefix {
		ns,
		db,
		tb: Cow::Borrowed(table),
	}
	.range()?;
	Ok(!txn.keys_raw(range, 1, 0, None).await?.is_empty())
}

/// The reader one definition is, given what its clauses read: the earliest of
/// `names` it reads by name, or else the first of `names` when it may read
/// any.
fn reader_of(
	names: &[String],
	reads: &RecordReads,
	what: &'static str,
	name: impl FnOnce() -> String,
) -> Option<Reader> {
	let (component, via) = match first_of(names, &reads.names) {
		Some(component) => (component, Via::Name),
		None if reads.unresolved => (names.first()?, Via::Unresolved),
		None => return None,
	};
	Some(Reader {
		component: component.clone(),
		what,
		name: name(),
		via,
	})
}

/// The first definition of `table`, in the order `readers` admits, that reads
/// one of `names` as a stored field.
///
/// Indexes come first, then the views over the table, and for
/// [`Readers::All`] the table's permissions, the clauses of its fields and its
/// events. A definition that reads one of `names` by name is reported with the
/// earliest of them ([`Via::Name`]). Within a view, the next reason is that it
/// selects every field and holds a row ([`Via::Wildcard`]): its rows hold the
/// components by the names they were written under, and are not rewritten
/// when the names change. A definition that may read any name
/// ([`record_reads`]) is reported last, with the first of `names`
/// ([`Via::Unresolved`]).
///
/// # Errors
///
/// The error of a catalog or storage read that fails.
pub(crate) async fn first_reader(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	names: &[String],
	readers: Readers,
) -> Result<Option<Reader>> {
	if names.is_empty() {
		return Ok(None);
	}
	for index in txn.all_tb_indexes(ns, db, table, None).await?.iter() {
		let mut read = RecordReads {
			names: index.cols.iter().filter_map(root_name).map(str::to_owned).collect(),
			unresolved: false,
		};
		if let Some(cond) = &index.count_cond {
			read.extend(record_reads(&cond.0, ReadContext::Record));
		}
		if let Some(reader) = reader_of(names, &read, "index", || index.name.to_string()) {
			return Ok(Some(reader));
		}
	}
	for view in txn.all_tb_views(ns, db, table, None).await?.iter() {
		let Some(definition) = &view.view else {
			continue;
		};
		let read = view_reads(definition);
		if let Some(component) = first_of(names, &read.names) {
			return Ok(Some(Reader {
				component: component.clone(),
				what: "view",
				name: view.name.to_string(),
				via: Via::Name,
			}));
		}
		if selects_every_field(definition) && holds_rows(txn, ns, db, &view.name).await? {
			return Ok(Some(Reader {
				component: names[0].clone(),
				what: "view",
				name: view.name.to_string(),
				via: Via::Wildcard,
			}));
		}
		if let Some(reader) = reader_of(names, &read, "view", || view.name.to_string()) {
			return Ok(Some(reader));
		}
	}
	if readers == Readers::Structural {
		return Ok(None);
	}
	if let Some(table_definition) = txn.get_tb(ns, db, table, None).await? {
		let permissions = &table_definition.permissions;
		let operations = [
			("select", &permissions.select),
			("create", &permissions.create),
			("update", &permissions.update),
			("delete", &permissions.delete),
		]
		.map(|(operation, permission)| (operation, permission_reads(permission)));
		let by_name = operations.iter().find(|(_, read)| first_of(names, &read.names).is_some());
		let by_any = || operations.iter().find(|(_, read)| read.unresolved);
		if let Some((operation, read)) = by_name.or_else(by_any)
			&& let Some(reader) =
				reader_of(names, read, "table permission", || (*operation).to_owned())
		{
			return Ok(Some(reader));
		}
	}
	for field in txn.all_tb_fields(ns, db, table, None).await?.iter() {
		// The id is the definition being changed, and a derived component is
		// the id read back.
		if field.is_key_part() || field.name.is_id() {
			continue;
		}
		let mut read = RecordReads::default();
		for expr in [&field.value, &field.assert, &field.computed].into_iter().flatten() {
			read.extend(record_reads(expr, ReadContext::Record));
		}
		if let DefineDefault::Set(expr) | DefineDefault::Always(expr) = &field.default {
			read.extend(record_reads(expr, ReadContext::Record));
		}
		// A `REFERENCE ON DELETE THEN` repair runs with the referencing record,
		// one of this table's, as `$this`.
		if let Some(Reference {
			on_delete,
			materialize,
		}) = &field.reference
		{
			if let ReferenceDeleteStrategy::Custom(expr) = on_delete {
				read.extend(record_reads(expr, ReadContext::Record));
			}
			// A `MATERIALIZE INTO … WHERE` filter is judged on this table's record.
			if let Some(expr) = materialize.as_ref().and_then(|m| m.filter.as_ref()) {
				read.extend(record_reads(expr, ReadContext::Record));
			}
		}
		for permission in
			[&field.select_permission, &field.create_permission, &field.update_permission]
		{
			read.extend(permission_reads(permission));
		}
		if let Some(reader) = reader_of(names, &read, "field", || field.name.to_raw_string()) {
			return Ok(Some(reader));
		}
	}
	for event in txn.all_tb_events(ns, db, table, None).await?.iter() {
		let mut read = record_reads(&event.when, ReadContext::Event);
		for then in &event.then {
			read.extend(record_reads(then, ReadContext::Event));
		}
		if let Some(reader) = reader_of(names, &read, "event", || event.name.to_string()) {
			return Ok(Some(reader));
		}
	}
	Ok(None)
}

/// The first clause of the `id` definition `id` that reads one of `names`: its
/// `ASSERT`, its `DEFAULT`, then its permissions. These clauses read the
/// components of the record id the definition itself declares, so a name the
/// definition's record id drops would leave them reading nothing.
///
/// A clause that reads one of `names` by name is reported before one that may
/// read any ([`record_reads`]), each in that order.
pub(crate) fn id_clause_reader(id: &catalog::FieldDefinition, names: &[String]) -> Option<Reader> {
	let default = match &id.default {
		DefineDefault::Set(expr) | DefineDefault::Always(expr) => Some(expr),
		DefineDefault::None => None,
	};
	let mut permissions = RecordReads::default();
	for permission in [&id.select_permission, &id.create_permission, &id.update_permission] {
		permissions.extend(permission_reads(permission));
	}
	let clauses = [
		("ASSERT of the field", id.assert.as_ref().map(|e| record_reads(e, ReadContext::Record))),
		("DEFAULT of the field", default.map(|e| record_reads(e, ReadContext::Record))),
		("PERMISSIONS clause of the field", Some(permissions)),
	];
	let clauses = clauses.iter().filter_map(|(what, read)| Some((*what, read.as_ref()?)));
	let by_name = clauses.clone().find(|(_, read)| first_of(names, &read.names).is_some());
	let (what, read) = by_name.or_else(|| clauses.clone().find(|(_, read)| read.unresolved))?;
	reader_of(names, read, what, || "id".to_owned())
}

/// Refuse the field `name` of `table` when it is a component of the record id,
/// which is a derived field with no definition of its own to remove or alter.
///
/// Names compare by their root, as every clash check does: `city.x` and
/// `city[*]` are `city`.
///
/// # Errors
///
/// [`ExecError::IdComponentNotAField`] for a component name; the error of a
/// catalog read that fails.
pub(crate) async fn refuse_component_name(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	name: &Idiom,
) -> Result<()> {
	let Some(root) = root_name(name) else {
		return Ok(());
	};
	if root == "id" {
		return Ok(());
	}
	let Some(key) = stored_key(txn, ns, db, table).await? else {
		return Ok(());
	};
	ensure!(
		key.names.as_ref().is_none_or(|names| names.position(root).is_none()),
		ExecError::IdComponentNotAField {
			name: root.to_owned(),
			table: table.as_str().to_owned(),
		}
	);
	Ok(())
}

/// Refuse leaving `table` with no record id structure while the keys it
/// already holds do not walk in the order of their record ids.
///
/// A component whose [`catalog::ComponentOrder`] is not `Exact` — a datetime,
/// whose key text orders by the whole second — is why a scan of such a table
/// is not taken to deliver `ORDER BY id`. That judgement reads the structure
/// the catalog holds now, while the keys it judges were written earlier and
/// are rewritten by no change of the names: a table left with records and no
/// structure reads as one whose keys walk in `id` order. The contract this
/// keeps is that dropping the names changes the answer to no query over the
/// records that exist.
///
/// The scan compares each record id with the one before it, in the order the
/// keys are stored, and refuses the first pair that is out of order. It
/// therefore refuses exactly the tables the judgement would be wrong about: a
/// coarse structure whose records happen to order exactly — one reading per
/// second — keeps its freedom to drop the names. Records written after the
/// names are gone are not covered, and are the ordinary case of a table whose
/// array record ids nothing describes.
///
/// Nothing is scanned unless the structure is lost: a structure that is
/// renamed, reordered or retyped keeps one, and its records are held to it by
/// [`validate_record_keys`] instead. A structure whose every component orders
/// exactly loses nothing when its names go.
///
/// The scan reads in pages sized to the fuel left and charges a unit per
/// record, as [`validate_record_keys`] does, so it honours the statement's
/// cancellation and deadline. The snapshot it reads is the statement's, with
/// the stale-writer race ADR 0008 describes under *Operational notes*.
///
/// # Errors
///
/// [`ExecError::IdComponentsOutOfOrderWithoutNames`] naming the first pair of
/// records whose keys and record ids disagree; the error of a storage read
/// that fails, or of the interruption of the statement.
pub(crate) async fn refuse_dropping_unordered_keys(
	exec: &ExecutionContext,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	old: Option<&KeyStructure>,
	new: Option<&KeyStructure>,
) -> Result<()> {
	if new.is_some() {
		return Ok(());
	}
	let Some(old) = old else {
		return Ok(());
	};
	if old.all_exactly_ordered() {
		return Ok(());
	}
	let range = RecordPrefix {
		ns,
		db,
		tb: Cow::Borrowed(table),
	}
	.range()?;
	// Only the keys are read: the order of the record ids is the whole
	// question, and no body decides it.
	let mut cursor = txn.open_keys_cursor_raw(range, Direction::Forward, 0, None).await?;
	let mut previous: Option<RecordIdKey> = None;
	loop {
		let mut refused = None;
		let chunk = crate::exec::vm::fuel::batch(NORMAL_BATCH_SIZE);
		let stats = cursor
			.for_each(chunk, &mut |raw_key| {
				// A key that cannot be read back is left to the scan that
				// admits records into a structure: it is not this statement's
				// to refuse, and it orders against nothing.
				let Ok(record_key) = RecordKey::decode_key(raw_key) else {
					return Ok(Step::Continue(()));
				};
				let current = record_key.id.as_ref().clone();
				if let Some(before) = &previous
					&& before > &current
				{
					refused = Some(ExecError::IdComponentsOutOfOrderWithoutNames {
						table: table.as_str().to_owned(),
						first: RecordId {
							table: table.clone(),
							key: before.clone(),
						}
						.to_sql(),
						second: RecordId {
							table: table.clone(),
							key: current,
						}
						.to_sql(),
					});
					return Ok(Step::Break(()));
				}
				previous = Some(current);
				Ok(Step::Continue(()))
			})
			.await?;
		if let Some(error) = refused {
			bail!(error);
		}
		if stats.rows == 0 {
			return Ok(());
		}
		crate::exec::vm::fuel::tick(exec, crate::exec::vm::fuel::units(stats.rows as usize))
			.await
			.map_err(interrupted)?;
	}
}

/// [`refuse_dropping_unordered_keys`] for the table `definition` defines a
/// field of, where that field is the `id`: the structure the stored `id`
/// gives is the one being left behind, and `definition` gives the one that
/// replaces it.
///
/// A definition of any other field changes no record id structure and is not
/// scanned. The caller runs this once the statement is known to store
/// `definition`, so a statement that leaves the stored `id` alone reads no
/// record.
///
/// # Errors
///
/// Those of [`refuse_dropping_unordered_keys`]; the error of a catalog read
/// that fails.
pub(crate) async fn refuse_dropping_unordered_keys_for(
	exec: &ExecutionContext,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	if !definition.name.is_id() {
		return Ok(());
	}
	let table = &definition.table;
	let existing_id = txn.get_tb_field(ns, db, table, "id", None).await?;
	let old = existing_id.as_deref().and_then(catalog::FieldDefinition::live_key);
	refuse_dropping_unordered_keys(
		exec,
		txn,
		ns,
		db,
		table,
		old.map(|key| key.as_ref()),
		definition.live_key().map(|key| key.as_ref()),
	)
	.await
}

/// What refuses `REMOVE FIELD name` on `table`: a component name, which has no
/// definition to remove, and the `id` while any definition of the table reads
/// one of its component names, or while the table holds records whose keys
/// only the structure orders ([`refuse_dropping_unordered_keys`]).
///
/// # Errors
///
/// [`ExecError::IdComponentNotAField`] for a component name;
/// [`ExecError::IdComponentInUse`], [`ExecError::IdComponentsUnderWildcardView`]
/// or [`ExecError::IdComponentReadUnresolved`] for the `id` of a record id some
/// definition reads the components of ([`first_reader`]);
/// [`ExecError::IdComponentsOutOfOrderWithoutNames`] for the `id` of a record
/// id whose keys the records still depend on; the error of a catalog or
/// storage read that fails.
pub(crate) async fn refuse_removing_field(
	exec: &ExecutionContext,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	name: &Idiom,
) -> Result<()> {
	refuse_component_name(txn, ns, db, table, name).await?;
	if !name.is_id() {
		return Ok(());
	}
	let Some(key) = stored_key(txn, ns, db, table).await? else {
		return Ok(());
	};
	let names: Vec<String> =
		key.names.iter().flat_map(|names| names.iter()).map(str::to_owned).collect();
	if let Some(reader) = first_reader(txn, ns, db, table, &names, Readers::All).await? {
		bail!(in_use(table, reader));
	}
	refuse_dropping_unordered_keys(exec, txn, ns, db, table, Some(&key), None).await
}

/// The error for a component name that `reader` still reads while the record id
/// drops or moves it.
pub(crate) fn in_use(table: &TableName, reader: Reader) -> ExecError {
	let table = table.as_str().to_owned();
	match reader.via {
		Via::Name => ExecError::IdComponentInUse {
			table,
			component: reader.component,
			what: reader.what.to_owned(),
			name: reader.name,
		},
		Via::Wildcard => ExecError::IdComponentsUnderWildcardView {
			table,
			view: reader.name,
		},
		Via::Unresolved => ExecError::IdComponentReadUnresolved {
			table,
			component: reader.component,
			what: reader.what.to_owned(),
			name: reader.name,
		},
	}
}

/// The error for a component name that `reader` reads as a stored field while
/// the record id takes the name.
pub(crate) fn read_by_existing(table: &TableName, reader: Reader) -> ExecError {
	let table = table.as_str().to_owned();
	match reader.via {
		Via::Name => ExecError::IdComponentReadByExisting {
			table,
			component: reader.component,
			what: reader.what.to_owned(),
			name: reader.name,
		},
		Via::Wildcard => ExecError::IdComponentsUnderWildcardView {
			table,
			view: reader.name,
		},
		Via::Unresolved => ExecError::IdComponentReadByExistingUnresolved {
			table,
			component: reader.component,
			what: reader.what.to_owned(),
			name: reader.name,
		},
	}
}

/// The type of a record id as the statement wrote it:
/// `[city: string, time: datetime]`.
fn declared_type(key: &KeyStructure) -> String {
	let kinds: Vec<_> = key.components.iter().filter_map(|c| c.declared.clone()).collect();
	match &key.names {
		Some(names) => names.fmt_keyed(&kinds).to_sql(),
		None => String::new(),
	}
}

/// How a record id differs from the structure, as a phrase for the message
/// that follows the record.
fn nonconformance_reason(why: &Nonconformance) -> String {
	if let Nonconformance::Component {
		name: Some(name),
		reason,
		..
	} = why
	{
		format!("`{name}` {reason}")
	} else {
		why.to_string()
	}
}

/// Whether the value a record body stores under a component's name is the
/// component: an absent field, or one that encodes as the key element does.
fn is_the_component(stored: &Value, component: &Value) -> bool {
	if matches!(stored, Value::None) {
		return true;
	}
	match (try_encode_component(stored), try_encode_component(component)) {
		(Some(stored), Some(component)) => stored == component,
		_ => false,
	}
}

/// Refuses a record whose generation the table's `layouts` do not admit, as a
/// read of the record refuses it. Neither way [`body_field`] reads a body
/// checks a document's generation, so the body is read only once this passes.
fn admit(value: &[u8], layouts: &catalog::TableLayouts) -> Result<()> {
	if let Some(stored) = catalog::StoredRecord::parse(value)? {
		let refusal =
			|error: catalog::RecordLayoutError| anyhow::Error::new(catalog::Error::Layout(error));
		match stored.generation() {
			Ok(None) => {}
			Ok(Some(generation)) => {
				layouts.admit(generation).map_err(refusal)?;
			}
			Err(stamp) => return Err(refusal(layouts.stamp_refusal(stamp))),
		}
	}
	Ok(())
}

/// The value the body of a stored record holds under `name`, or `None` when it
/// holds none.
///
/// The record is read in place where the stored bytes allow it and decoded
/// whole, through the table's `layouts`, where they do not.
fn body_field(
	segment: &PathSegment,
	value: &[u8],
	layouts: &catalog::TableLayouts,
	rid: impl FnOnce() -> RecordId,
) -> Result<Option<Value>> {
	match extract_field_from_record_bytes(value, std::slice::from_ref(segment), 1) {
		Extracted::Found(found) => Ok(Some(found)),
		Extracted::Missing => Ok(None),
		Extracted::Bail => {
			let record = catalog::Record::decode_admitted(value, Some(layouts), rid())?;
			Ok(if let Value::Object(object) = &record.data {
				object.get(segment.as_str()).cloned()
			} else {
				None
			})
		}
	}
}

/// One stored record against the structure: its id must conform, and its body
/// must hold no field under a component's name with another value.
///
/// A record whose stored key does not decode to a record id does not conform:
/// no component of an id that cannot be read can be read either. A record id
/// holding a datetime outside the years 0000 to 9999 is stored as text that
/// does not decode as a datetime, and is refused this way.
fn check_record(
	table: &TableName,
	key: &KeyStructure,
	probes: &[(u32, PathSegment)],
	raw_key: &[u8],
	value: &[u8],
	layouts: &catalog::TableLayouts,
) -> Result<()> {
	let ty = || declared_type(key);
	let Ok(record_key) = RecordKey::decode_key(raw_key) else {
		bail!(ExecError::IdComponentsNonConforming {
			table: table.as_str().to_owned(),
			ty: ty(),
			record: format!("in `{}` whose record id cannot be read", table.as_str()),
			reason: "a record id holding a datetime outside the years 0000 to 9999 cannot be \
			         read back"
				.to_owned(),
		});
	};
	let rid = || RecordId {
		table: table.clone(),
		key: record_key.id.as_ref().clone(),
	};
	if let Err(why) = key.conforms(&record_key.id) {
		bail!(ExecError::IdComponentsNonConforming {
			table: table.as_str().to_owned(),
			ty: ty(),
			record: rid().to_sql(),
			reason: nonconformance_reason(&why),
		});
	}
	let RecordIdKey::Array(elements) = record_key.id.as_ref() else {
		return Ok(());
	};
	if !probes.is_empty() {
		admit(value, layouts)?;
	}
	for (position, segment) in probes {
		let Some(component) = elements.0.get(*position as usize) else {
			continue;
		};
		let Some(stored) = body_field(segment, value, layouts, rid)? else {
			continue;
		};
		if !is_the_component(&stored, component) {
			bail!(ExecError::IdComponentsBodyConflict {
				table: table.as_str().to_owned(),
				ty: ty(),
				record: rid().to_sql(),
				component: segment.as_str().to_owned(),
			});
		}
	}
	Ok(())
}

/// The error of an interrupted scan, as the statement reports it.
fn interrupted(flow: ControlFlow) -> anyhow::Error {
	flow.into_error_or(|other| {
		anyhow::anyhow!("the scan of the records was interrupted by {other}")
	})
}

/// Refuse the record ids and bodies of `table` that `key` does not admit.
///
/// Every stored record id must be an array of the structure's shape
/// ([`KeyStructure::conforms`]), and no record body may store a field under a
/// component's name whose value differs from the component: an equal copy is
/// allowed, and is cut on the record's next write. The first record that fails
/// is reported, naming the record, the component and the remedy.
///
/// Skipped when the stored `id` definition already has `key`: the records were
/// validated against it when it was defined.
///
/// The scan reads in pages sized to the fuel left and charges a unit per
/// record, so it reaches the yield point between pages: it honours the
/// statement's cancellation and deadline like any other work of a program.
///
/// # Errors
///
/// [`ExecError::IdComponentsNonConforming`] or
/// [`ExecError::IdComponentsBodyConflict`] for the first offending record; the
/// error of a storage read that fails, or of the interruption of the statement.
pub(crate) async fn validate_record_keys(
	exec: &ExecutionContext,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	key: &Arc<KeyStructure>,
) -> Result<()> {
	if stored_key(txn, ns, db, table).await?.is_some_and(|stored| stored == *key) {
		return Ok(());
	}
	let probes: Vec<(u32, PathSegment)> = key
		.components
		.iter()
		.filter_map(|c| Some((c.position, PathSegment::new(c.name.clone()?))))
		.collect();
	let layouts = txn.table_layouts(ns, db, table, None).await?;
	let range = RecordPrefix {
		ns,
		db,
		tb: Cow::Borrowed(table),
	}
	.range()?;
	let mut cursor = txn.open_vals_cursor_raw(range, Direction::Forward, 0, None).await?;
	loop {
		let mut refused = None;
		let chunk = crate::exec::vm::fuel::batch(NORMAL_BATCH_SIZE);
		let stats = cursor
			.for_each(chunk, &mut |raw_key, value| match check_record(
				table, key, &probes, raw_key, value, &layouts,
			) {
				Ok(()) => Ok(Step::Continue(())),
				Err(error) => {
					refused = Some(error);
					Ok(Step::Break(()))
				}
			})
			.await?;
		if let Some(error) = refused {
			return Err(error);
		}
		if stats.rows == 0 {
			return Ok(());
		}
		crate::exec::vm::fuel::tick(exec, crate::exec::vm::fuel::units(stats.rows as usize))
			.await
			.map_err(interrupted)?;
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::expr::{KeyNames, Kind, KindLiteral};

	fn structure(names: &[&str], kinds: &[Kind]) -> KeyStructure {
		let strands: Vec<Strand> = names.iter().map(Strand::new).collect();
		let names = KeyNames::new(strands, kinds.len()).unwrap();
		KeyStructure::from_id_field(&Kind::Literal(KindLiteral::Array(kinds.to_vec())), &names)
			.unwrap()
	}

	fn pair(a: &str, b: &str) -> KeyStructure {
		structure(&[a, b], &[Kind::String, Kind::Datetime])
	}

	/// The `id` definition's own clauses are judged in the order ASSERT, DEFAULT,
	/// permissions, and only for the names asked about.
	#[test]
	fn the_clauses_of_the_id_that_read_a_name() {
		let id = |assert: Option<&str>, default: Option<&str>, create: Option<&str>| {
			catalog::FieldDefinition {
				name: Idiom::field(Strand::new("id")),
				assert: assert.map(|e| crate::syn::expr(e).unwrap()),
				default: default.map_or(DefineDefault::None, |e| {
					DefineDefault::Set(crate::syn::expr(e).unwrap())
				}),
				create_permission: create.map_or(Permission::Full, |e| {
					Permission::Specific(crate::syn::expr(e).unwrap())
				}),
				..Default::default()
			}
		};
		let names = |list: &[&str]| list.iter().map(|n| (*n).to_owned()).collect::<Vec<_>>();
		let reader = |id: &catalog::FieldDefinition, list: &[&str]| {
			id_clause_reader(id, &names(list)).map(|r| (r.what, r.component, r.name))
		};
		let all = id(Some("n > 0"), Some("['k', $this.m ?? 7]"), Some("p = 1"));
		assert_eq!(
			reader(&all, &["m", "n", "p"]),
			Some(("ASSERT of the field", "n".to_owned(), "id".to_owned()))
		);
		assert_eq!(
			reader(&all, &["m", "p"]),
			Some(("DEFAULT of the field", "m".to_owned(), "id".to_owned()))
		);
		assert_eq!(
			reader(&all, &["p"]),
			Some(("PERMISSIONS clause of the field", "p".to_owned(), "id".to_owned()))
		);
		assert_eq!(reader(&all, &["q"]), None);
		assert_eq!(reader(&all, &[]), None);
		assert_eq!(reader(&id(None, None, None), &["n"]), None);
	}

	#[test]
	fn no_names_to_names_adds_every_name() {
		let new = pair("city", "time");
		let change = name_change(None, Some(&new));
		assert_eq!(change.added, ["city", "time"]);
		assert!(change.removed.is_empty());
	}

	#[test]
	fn names_to_no_names_removes_every_name() {
		let old = pair("city", "time");
		let change = name_change(Some(&old), None);
		assert!(change.added.is_empty());
		assert_eq!(change.removed, ["city", "time"]);
	}

	#[test]
	fn a_retype_under_the_same_names_changes_none() {
		let old = pair("city", "time");
		let new = structure(&["city", "time"], &[Kind::String, Kind::Int]);
		assert_eq!(name_change(Some(&old), Some(&new)), NameChange::default());
	}

	#[test]
	fn a_renamed_component_is_removed_and_added() {
		let old = pair("city", "time");
		let new = pair("town", "time");
		let change = name_change(Some(&old), Some(&new));
		assert_eq!(change.added, ["town"]);
		assert_eq!(change.removed, ["city"]);
	}

	#[test]
	fn a_moved_component_is_removed_and_not_added() {
		let old = pair("city", "time");
		let new = pair("time", "city");
		let change = name_change(Some(&old), Some(&new));
		assert!(change.added.is_empty());
		assert_eq!(change.removed, ["city", "time"]);
	}

	#[test]
	fn a_stored_copy_equal_to_the_component_is_the_component() {
		let city = Value::from("London");
		assert!(is_the_component(&Value::None, &city));
		assert!(is_the_component(&Value::from("London"), &city));
		assert!(!is_the_component(&Value::from("Paris"), &city));
		assert!(!is_the_component(&Value::from(1), &city));
	}

	#[test]
	fn the_reasons_follow_the_component_name() {
		let key = pair("city", "time");
		let id =
			RecordIdKey::Array(crate::val::Array(vec![Value::from("London"), Value::from("x")]));
		let why = key.conforms(&id).unwrap_err();
		assert_eq!(nonconformance_reason(&why), "`time` is not a datetime");
		let id = RecordIdKey::Number(1);
		let why = key.conforms(&id).unwrap_err();
		assert_eq!(nonconformance_reason(&why), "the record id is not an array");
	}

	#[test]
	fn the_declared_type_reads_back_as_written() {
		assert_eq!(declared_type(&pair("city", "time")), "[city: string, time: datetime]");
	}

	/// What a clause, parsed from `sql`, reads in `context`: the names it
	/// spells, sorted, and whether it may read any name.
	fn reads_of(sql: &str, context: ReadContext) -> (Vec<String>, bool) {
		let expr = crate::syn::expr(sql).unwrap_or_else(|e| panic!("`{sql}` parses: {e}"));
		let mut reads = record_reads(&expr, context);
		reads.names.sort();
		reads.names.dedup();
		(reads.names, reads.unresolved)
	}

	/// Check each `(clause, names, unresolved)` in `context`.
	fn check(context: ReadContext, cases: &[(&str, &[&str], bool)]) {
		for (sql, names, unresolved) in cases {
			let expected: Vec<String> = names.iter().map(|n| (*n).to_owned()).collect();
			assert_eq!(reads_of(sql, context), (expected, *unresolved), "`{sql}` in {context:?}");
		}
	}

	/// A name a clause spells is read: bare, through a static path or a
	/// destructure of a parameter bound to the record, or as a literal path
	/// of a projection function.
	#[test]
	fn a_name_a_clause_spells_is_read() {
		check(
			ReadContext::Record,
			&[
				("city = 'London'", &["city"], false),
				("$this.city", &["city"], false),
				("$self['city']", &["city"], false),
				("$parent.city", &["city"], false),
				("$this.{city, time}", &["city", "time"], false),
				("$this.{city}.city", &["city"], false),
				("$this.{c: city.name}", &["city"], false),
				("type::field('city')", &["city"], false),
				("type::fields(['city', 'time.x'])", &["city", "time"], false),
				("string::uppercase(note)", &["note"], false),
				("@.city", &["city"], false),
				("@['city'] = 'x'", &["city"], false),
				("@.{city, time}", &["city", "time"], false),
				("(SELECT VALUE city FROM $this)", &["city"], false),
			],
		);
	}

	/// A clause that reads the record through a name it computes, or calls a
	/// body that reads the document, may read any name.
	#[test]
	fn a_computed_name_or_a_body_reading_the_document_may_read_any_name() {
		check(
			ReadContext::Record,
			&[
				("$this['ci' + 'ty']", &[], true),
				("$parent[$name]", &[], true),
				("$this[0]", &[], true),
				("type::field('ci' + 'ty')", &[], true),
				("type::fields(['city', $other])", &[], true),
				("fn::hidden()", &[], true),
				("fn::slug(note)", &["note"], true),
				("function() { return this.city; }", &[], true),
				("$this.{c: $x}", &[], true),
			],
		);
	}

	/// The record handed whole to a call, a method, a closure or a cast may
	/// have any of its fields read, unless the builtin only judges its type or
	/// reads its id.
	#[test]
	fn the_record_handed_to_a_body_may_have_any_name_read() {
		check(
			ReadContext::Record,
			&[
				("fn::city_of($this)", &[], true),
				("object::keys($this)", &[], true),
				("object::values($self)", &[], true),
				("$this.keys()", &[], true),
				("$this.*.len()", &[], true),
				("array::len([$this])", &[], true),
				("string::len(<string> $this)", &[], true),
				("<string> $parent", &[], true),
				("(|$r| $r.city)($this)", &[], true),
				("($this ?? {}).keys()", &[], true),
				("array::first(array::map([1], |$x| $this))", &[], true),
				("(SELECT * FROM $parent)", &[], true),
				("(SELECT VALUE fn::label() FROM $this)", &[], true),
				("{ LET $r = $this; object::keys($r) }", &[], true),
				("{ FOR $r IN [$this] { RETURN object::keys($r); } }", &[], true),
				("type::is_object($this) AND record::id(id) != NONE", &["id"], false),
				("record::tb($this)", &[], false),
				("object::keys(@.*)", &[], true),
				("fn::city_of(@.*)", &[], true),
				("@.{..}.keys()", &[], true),
			],
		);
	}

	/// A value that holds the record inside it is not the record: a part that
	/// follows it reaches the record, or a field of it, by a name that is not
	/// one of the record's, so it may read any name; so does a statement over
	/// it, and handing it to a body. Holding it, comparing it or copying it
	/// reads no name.
	#[test]
	fn a_value_that_holds_the_record_is_not_the_record() {
		check(
			ReadContext::Record,
			&[
				("{ r: $this }.r.city", &[], true),
				("object::keys({ r: $this }.r)", &[], true),
				("{ LET $o = { r: $this }; RETURN object::keys($o.r); }", &[], true),
				("{ LET $o = { r: $this }; RETURN $o.r.city; }", &[], true),
				("[$this][0].city", &[], true),
				("[$this, 1].len()", &[], true),
				("{ LET $o = [$this]; RETURN $o; }", &[], false),
				("{ LET $o = { r: $this }; RETURN $o = {}; }", &[], false),
				("{ r: $this }", &[], false),
				("(SELECT * FROM [$this])", &[], true),
				("{ FOR $r IN [{ r: $this }] { RETURN $r.r; } }", &[], true),
				("{ FOR $r IN [$this] { RETURN $r.city; } }", &["city"], false),
				("(IF true { $this } ELSE { {} }).city", &["city"], false),
				("(IF true { $this } ELSE { [$this] }).city", &[], true),
			],
		);
	}

	/// A `LET` binds its name until the end of its block and shadows an
	/// earlier binding there; a binding in an inner block or a `FOR` body ends
	/// with it, and the outer binding holds again.
	#[test]
	fn a_variable_holds_what_its_innermost_binding_holds() {
		check(
			ReadContext::Record,
			&[
				("{ LET $r = $this; LET $r = {}; RETURN $r.city; }", &[], false),
				("{ LET $r = {}; LET $r = $this; RETURN $r.city; }", &["city"], false),
				("{ LET $r = $this; IF true { LET $r = {}; }; RETURN $r.city; }", &["city"], false),
				("{ LET $r = $this; { LET $r = {}; }; RETURN $r.city; }", &["city"], false),
				(
					"{ LET $r = {}; IF true { LET $r = $this; RETURN $r.city; }; RETURN $r.x; }",
					&["city"],
					false,
				),
				(
					"{ LET $r = $this; FOR $r IN [{}] { RETURN $r.x; }; RETURN $r.city; }",
					&["city"],
					false,
				),
				("{ LET $r = $this; FOR $r IN object::values($r) { RETURN $r; }; }", &[], true),
				("{ LET $r = $this; RETURN (|$r: any| $r.city)({ city: 'x' }); }", &[], false),
				("{ LET $r = $this; RETURN (|$q: any| $r.city)(1); }", &["city"], false),
				("{ LET $f = || $this; RETURN $f().city; }", &[], true),
				("{ LET $f = || { r: $this }; RETURN $f().r.city; }", &[], true),
				("{ LET $f = || 1; RETURN $f(); }", &[], false),
				("{ LET $r = $this; FOR $r IN [$r.city] { RETURN $r; }; }", &["city"], false),
				("{ LET $o = { LET $q = $this; $q }; RETURN $o.city; }", &["city"], false),
				("{ LET $o = { LET $q = $this; RETURN $q; }; RETURN $o.city; }", &["city"], false),
				(
					"{ LET $o = { IF true { RETURN $this; }; {} }; RETURN $o.city; }",
					&["city"],
					false,
				),
			],
		);
	}

	/// Comparing the record, copying it, walking a graph from it and the
	/// values a clause reads that are not the record read no name.
	#[test]
	fn comparing_copying_and_unrelated_values_read_no_name() {
		check(
			ReadContext::Record,
			&[
				("$value > 0", &[], false),
				("@.* = $this", &[], false),
				("$value.len() > 0", &[], false),
				("note.len()", &["note"], false),
				("tags.map(|$t| $t + 1)", &["tags"], false),
				("tags[WHERE $this > 1]", &["tags"], false),
				("tags[WHERE string::len($this) > 1]", &["tags"], false),
				("$this = $parent", &[], false),
				("{ copy: $this }", &[], false),
				("$this->likes->post", &[], false),
				("$auth.id = owner", &["owner"], false),
				("$session.ns != NONE", &[], false),
				("$after.city", &[], false),
				("fn::x($after)", &[], true),
				("(SELECT VALUE name FROM other WHERE fn::ok() AND $this.n > 1)", &[], false),
				("(SELECT VALUE name FROM other WHERE ok = $parent.city)", &["city"], false),
				("string::lowercase((SELECT VALUE name FROM ONLY other LIMIT 1))", &[], false),
			],
		);
	}

	/// An event's record is `$after`, `$before`, `$value` and `$input` as well,
	/// and the document of its clauses outside a nested statement.
	#[test]
	fn an_event_reads_its_record_through_its_parameters() {
		check(
			ReadContext::Event,
			&[
				("$before != $after", &[], false),
				("$after.city", &["city"], false),
				("$input.city", &["city"], false),
				("object::keys($input)", &[], true),
				("$value['city']", &["city"], false),
				("$before.{city}", &["city"], false),
				("$after['ci' + 'ty']", &[], true),
				("CREATE audit SET af = $after, bf = $before", &["af", "bf"], false),
				("CREATE audit CONTENT $after", &[], false),
				("CREATE audit SET seen = fn::hidden()", &["seen"], false),
				("RELATE $after->seen->$before", &[], false),
				("CREATE audit SET seen = fn::city_of($after)", &["seen"], true),
				("CREATE audit SET keys = object::keys($value)", &["keys"], true),
				("fn::log('changed')", &[], true),
				(
					"{ LET $r = $after; CREATE audit SET n = array::len(object::values($r)) }",
					&["n"],
					true,
				),
				("(UPDATE $after SET seen = fn::hidden())", &["seen"], true),
			],
		);
	}
}

#[cfg(all(test, feature = "kv-mem"))]
mod scan_tests {
	use std::sync::Arc;

	use crate::catalog::Record;
	use crate::catalog::providers::{DatabaseProvider, TableProvider};
	use crate::exec::operators::test_util::TestDb;
	use crate::kvs::TransactionType::Write;
	use crate::val::{Array, RecordIdKey, TableName, Value};

	/// The error of each statement of `sql`, run as root Owner, in order;
	/// `None` for a statement that succeeds.
	async fn errors(db: &TestDb, sql: &str) -> Vec<Option<String>> {
		db.datastore()
			.execute(sql, &TestDb::owner(), None)
			.await
			.expect("the query runs")
			.into_iter()
			.map(|response| response.result.err().map(|error| error.to_string()))
			.collect()
	}

	/// A record id the named structure does not admit, written straight to
	/// storage: the write path refuses to write one through a statement.
	async fn store_nonconforming_record(db: &TestDb) {
		let txn = db.datastore().transaction(Write).await.expect("a transaction");
		let database = txn
			.get_db_by_name("test", "test", None)
			.await
			.expect("the catalog reads")
			.expect("the database exists");
		let table = TableName::new("reading");
		let id = RecordIdKey::Array(Array(vec![Value::from("London"), Value::from("x")]));
		let record = Record::new(Value::Object(Default::default()));
		txn.set_record(database.namespace_id, database.database_id, &table, &id, Arc::new(record))
			.await
			.expect("the record is written");
		txn.commit().await.expect("the transaction commits");
	}

	/// `count` records of `reading` whose ids fit `[city: string, n: int]`,
	/// written straight to storage in transactions of a few thousand, and when
	/// `offender` is set one more, `['zzz', 'x']`, which sorts after all of
	/// them.
	async fn store_records(db: &TestDb, count: i64, offender: bool) {
		let table = TableName::new("reading");
		let mut next = 0;
		while next < count {
			let txn = db.datastore().transaction(Write).await.expect("a transaction");
			let database = txn
				.get_db_by_name("test", "test", None)
				.await
				.expect("the catalog reads")
				.expect("the database exists");
			let (ns, dbid) = (database.namespace_id, database.database_id);
			for n in next..count.min(next + 5000) {
				let id = RecordIdKey::Array(Array(vec![Value::from("London"), Value::from(n)]));
				let record = Record::new(Value::Object(Default::default()));
				txn.set_record(ns, dbid, &table, &id, Arc::new(record))
					.await
					.expect("the record is written");
			}
			txn.commit().await.expect("the transaction commits");
			next += 5000;
		}
		if offender {
			let txn = db.datastore().transaction(Write).await.expect("a transaction");
			let database = txn
				.get_db_by_name("test", "test", None)
				.await
				.expect("the catalog reads")
				.expect("the database exists");
			let id = RecordIdKey::Array(Array(vec![Value::from("zzz"), Value::from("x")]));
			let record = Record::new(Value::Object(Default::default()));
			txn.set_record(
				database.namespace_id,
				database.database_id,
				&table,
				&id,
				Arc::new(record),
			)
			.await
			.expect("the record is written");
			txn.commit().await.expect("the transaction commits");
		}
	}

	/// The scan reads the table page by page and reaches every record: a table
	/// many pages long whose last record in key order does not fit is refused
	/// naming that record, and once it is gone the same statement passes and
	/// defines the field.
	#[tokio::test]
	async fn the_scan_reaches_the_last_record_of_a_table_many_pages_long() {
		let records = SCAN_RECORDS;
		let db = TestDb::new("DEFINE TABLE reading;").await;
		store_records(&db, records, true).await;
		let define = "DEFINE FIELD id ON reading TYPE [city: string, n: int];";
		let started = web_time::Instant::now();
		let refused = errors(&db, define).await;
		let elapsed = started.elapsed();
		let refusal = "the existing record reading:['zzz', 'x'] does not match it";
		assert!(
			refused[0].as_deref().is_some_and(|error| error.contains(refusal)),
			"expected `{refusal}`, got {refused:?}"
		);
		eprintln!("scanned {records} records and refused the last in {elapsed:?}");
		assert_eq!(
			errors(&db, "DELETE reading:['zzz', 'x']; INFO FOR TABLE reading;").await,
			[None, None]
		);
		assert_eq!(errors(&db, define).await, [None]);
	}

	/// More records than one page of the scan holds.
	const SCAN_RECORDS: i64 = 2_500;

	/// The first definition of `reading` that reads one of `names`, as
	/// `readers` admits them, in the database `setup` leaves.
	async fn first_reader_of(
		setup: &str,
		names: &[&str],
		readers: super::Readers,
	) -> Option<super::Reader> {
		let db = TestDb::new(setup).await;
		let txn =
			db.datastore().transaction(crate::kvs::TransactionType::Read).await.expect("a txn");
		let database = txn
			.get_db_by_name("test", "test", None)
			.await
			.expect("the catalog reads")
			.expect("the database exists");
		let names: Vec<String> = names.iter().map(|name| (*name).to_owned()).collect();
		let reader = super::first_reader(
			&txn,
			database.namespace_id,
			database.database_id,
			&TableName::new("reading"),
			&names,
			readers,
		)
		.await
		.expect("the definitions read");
		txn.cancel().await.expect("the transaction ends");
		reader
	}

	/// A view that selects every field reads every name through the rows it
	/// holds: it is a reader once it holds a row, in either direction, and
	/// only after its explicit reads, which are reported by name.
	#[tokio::test]
	async fn a_view_that_selects_every_field_reads_every_name_once_it_holds_a_row() {
		use super::{Readers, Via};
		let rows = "DEFINE TABLE reading; \
			 CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 10;";
		let wildcard = format!("{rows} DEFINE TABLE v AS SELECT * FROM reading;");
		for readers in [Readers::Structural, Readers::All] {
			let reader = first_reader_of(&wildcard, &["city", "time"], readers).await;
			let reader = reader.expect("the view reads every name");
			assert_eq!(
				(reader.what, reader.name.as_str(), reader.component.as_str(), reader.via),
				("view", "v", "city", Via::Wildcard)
			);
		}
		let explicit =
			format!("{rows} DEFINE TABLE v AS SELECT *, time AS at FROM reading WHERE temp > 5;");
		let reader = first_reader_of(&explicit, &["city", "time"], Readers::All).await;
		let reader = reader.expect("the view reads `time`");
		assert_eq!((reader.component.as_str(), reader.via), ("time", Via::Name));
		let empty = "DEFINE TABLE reading; DEFINE TABLE v AS SELECT * FROM reading;";
		assert_eq!(first_reader_of(empty, &["city"], Readers::All).await, None);
		let others = format!(
			"{rows} DEFINE TABLE agg AS SELECT temp, count() AS n FROM reading GROUP BY temp; \
			 DEFINE TABLE some AS SELECT temp FROM reading;"
		);
		assert_eq!(first_reader_of(&others, &["city"], Readers::All).await, None);
		let opaque = format!(
			"{rows} DEFINE FUNCTION fn::city_of($r: any) {{ RETURN $r.city; }}; \
			 DEFINE TABLE v AS SELECT fn::city_of($this) AS c FROM reading;"
		);
		let reader = first_reader_of(&opaque, &["city", "time"], Readers::Structural).await;
		let reader = reader.expect("the view may read any name");
		assert_eq!((reader.component.as_str(), reader.via), ("city", Via::Unresolved));
	}

	/// A field's `REFERENCE ON DELETE THEN` repair runs with the referencing
	/// record, one of the table's, as `$this`: a name it reads of it is read
	/// by the field.
	#[tokio::test]
	async fn a_reference_repair_reads_the_referencing_record() {
		use super::{Readers, Via};
		let setup = |then: &str| {
			format!(
				"DEFINE TABLE reading; \
				 DEFINE FIELD owner ON reading TYPE option<record<person>> \
				 REFERENCE ON DELETE THEN {then};"
			)
		};
		let cases = [
			("{ UPDATE $this SET gone = city }", Some(("city", Via::Name))),
			("{ UPDATE $this SET gone = $this.time }", Some(("time", Via::Name))),
			("{ UPDATE $this SET gone = fn::label($this) }", Some(("city", Via::Unresolved))),
			("{ UPDATE $this SET owner = NONE }", None),
		];
		for (then, expected) in cases {
			let reader = first_reader_of(&setup(then), &["city", "time"], Readers::All).await;
			let found = reader.map(|reader| {
				assert_eq!((reader.what, reader.name.as_str()), ("field", "owner"), "`{then}`");
				(reader.component, reader.via)
			});
			let expected = expected.map(|(component, via)| (component.to_owned(), via));
			assert_eq!(found, expected, "`{then}`");
		}
	}

	/// The records are checked against a structure only when the statement
	/// changes it: redefining the `id` with the structure it already has reads
	/// no record, while a structure that renames a component reads them all.
	#[tokio::test]
	async fn the_records_are_checked_only_against_a_changed_structure() {
		let db =
			TestDb::new("DEFINE FIELD id ON reading TYPE [city: string, time: datetime];").await;
		store_nonconforming_record(&db).await;
		let unchanged = errors(
			&db,
			"DEFINE FIELD OVERWRITE id ON reading TYPE [city: string, time: datetime] COMMENT 'c'; \
			 ALTER FIELD id ON reading COMMENT 'd'; \
			 ALTER FIELD id ON reading TYPE [city: string, time: datetime];",
		)
		.await;
		assert_eq!(unchanged, [None, None, None]);
		let changed = errors(
			&db,
			"DEFINE FIELD OVERWRITE id ON reading TYPE [place: string, time: datetime]; \
			 ALTER FIELD id ON reading TYPE [city: string, at: datetime];",
		)
		.await;
		let expected =
			[("[place: string, time: datetime]", "time"), ("[city: string, at: datetime]", "at")];
		assert_eq!(changed.len(), expected.len(), "{changed:?}");
		for (error, (ty, name)) in changed.iter().zip(expected) {
			let refusal = format!(
				"Cannot give `reading` the record id {ty}: the existing record \
				 reading:['London', 'x'] does not match it (`{name}` is not a datetime). Copy the \
				 records into a table with the new record id instead"
			);
			assert!(
				error.as_deref().is_some_and(|error| error.contains(&refusal)),
				"expected `{refusal}`, got {error:?}"
			);
		}
	}
}
