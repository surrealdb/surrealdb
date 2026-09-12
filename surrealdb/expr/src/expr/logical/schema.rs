//! The output schema of a plan node, and the partial typing of scalars.
//!
//! A [`Schema`] describes the rows a node produces: the fields the catalog
//! declares, each with its [`Kind`], and whether the row may carry fields the
//! schema does not list. A `SCHEMAFULL` table is closed — the declared fields
//! and nothing else — while a `SCHEMALESS` table, a literal value and every
//! statement kind are open. Openness is part of the schema so that an
//! optimisation needing a closed one declines on an open one rather than
//! assuming.
//!
//! Typing is partial. [`LogicalExpr::kind`] answers with the vocabulary of
//! `DEFINE FIELD … TYPE`, and [`Kind::Any`] is the honest answer whenever the
//! kind is not a property of the expression and the schema alone: a field on
//! an open schema, a parameter, a call, a subquery. A consumer fires on a known
//! kind and never on `Any`.
//!
//! A projection layer ([`LogicalOp::is_layer`](super::LogicalOp::is_layer))
//! keeps its input's fields and retypes each column it sets: a `COMPUTED`
//! column by its declared kind, a guard column as `option<kind of the field>`.
//! A read of `c` above the layer is therefore typed as the derived value,
//! and the same read beneath it as the stored declaration; nothing is
//! renamed. A parameter is never a property of the row and is always `Any`.
//!
//! An unbound plan carries no schema (`Source.schema` is `None`, which reads
//! as open); binding fills it from the catalog snapshot, and every other node
//! derives its own from its inputs through [`super::LogicalOp::schema`].
//!
//! A declared field is at a [`FieldPath`]: a position inside a value, named
//! by field segments and `*` (`arr.*.secret`), which is what `DEFINE FIELD`
//! declares. It is not an expression: [`LogicalExpr::field_path`] is the
//! partial map from an expression to the position it reads, and only a chain
//! of [`LogicalExpr::Field`], [`LogicalExpr::Access`] and [`LogicalExpr::Each`]
//! has one. Index analysis uses the same map to match an expression against
//! an indexed column.

use std::fmt::{self, Display, Formatter};

use surrealdb_iam::AuthLimit;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use super::expr::{LogicalExpr, LogicalLiteral, OutputPath};
use crate::expr::constant::ConstantValue;
use crate::expr::{BinaryOperator, Idiom, Kind, Part, PrefixOperator};

/// One segment of a [`FieldPath`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum FieldSegment {
	/// A field by name.
	Field(Strand),
	/// `*`: every element of an array, every value of an object.
	All,
}

/// A declared position inside a value: `a`, `a.b`, `arr.*.secret`.
///
/// The type of `DEFINE FIELD`'s name and of an index column, and the key a
/// [`Schema`] is looked up by. Built from the catalog through
/// [`FieldPath::from_idiom`] and from an expression through
/// [`LogicalExpr::field_path`]; the two agree, so a declared field and the
/// expression reading it compare equal.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FieldPath(Vec<FieldSegment>);

impl FieldPath {
	/// The path of `segments`, outermost first.
	pub fn new(segments: Vec<FieldSegment>) -> Self {
		FieldPath(segments)
	}

	/// The path of one field.
	pub fn field(name: impl Into<Strand>) -> Self {
		FieldPath(vec![FieldSegment::Field(name.into())])
	}

	/// The catalog's field name as a path. `None` when the idiom holds a
	/// part that is not a field or `*`: such a name declares no position an
	/// expression can be matched against.
	pub fn from_idiom(idiom: &Idiom) -> Option<Self> {
		idiom
			.0
			.iter()
			.map(|part| match part {
				Part::Field(name) => Some(FieldSegment::Field(name.clone())),
				Part::All => Some(FieldSegment::All),
				Part::Flatten
				| Part::First
				| Part::Last
				| Part::Value(_)
				| Part::Where(_)
				| Part::Lookup(_)
				| Part::Start(_)
				| Part::Method(..)
				| Part::Destructure(_)
				| Part::Optional
				| Part::Recurse(..)
				| Part::Doc
				| Part::RepeatRecurse => None,
			})
			.collect::<Option<Vec<_>>>()
			.map(FieldPath)
	}

	/// The segments, outermost first.
	pub fn segments(&self) -> &[FieldSegment] {
		&self.0
	}

	/// The field the path starts from. `None` for a path starting with `*`.
	pub fn root(&self) -> Option<&Strand> {
		match self.0.first() {
			Some(FieldSegment::Field(name)) => Some(name),
			Some(FieldSegment::All) | None => None,
		}
	}

	/// The one field, when the path is a single named field.
	pub fn as_field(&self) -> Option<&Strand> {
		match self.0.as_slice() {
			[FieldSegment::Field(name)] => Some(name),
			_ => None,
		}
	}

	/// The path with `segment` appended.
	pub fn child(mut self, segment: FieldSegment) -> Self {
		self.0.push(segment);
		self
	}
}

impl TryFrom<&Idiom> for FieldPath {
	type Error = ();

	fn try_from(idiom: &Idiom) -> Result<Self, ()> {
		FieldPath::from_idiom(idiom).ok_or(())
	}
}

impl From<&OutputPath> for FieldPath {
	/// Every key of an output path is a field of the result object, a lookup
	/// key included: `->likes` is the name of the field the lookup's value
	/// lands in.
	fn from(path: &OutputPath) -> Self {
		FieldPath(
			path.parts()
				.iter()
				.map(|part| match part {
					super::expr::OutputPathPart::Field(name)
					| super::expr::OutputPathPart::Lookup(name) => FieldSegment::Field(Strand::new(name)),
				})
				.collect(),
		)
	}
}

impl ToSql for FieldPath {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		for (i, segment) in self.0.iter().enumerate() {
			match segment {
				FieldSegment::Field(name) if i == 0 => {
					common::fmt::EscapeIdent(name.as_str()).fmt_sql(f, fmt)
				}
				FieldSegment::Field(name) => {
					f.push('.');
					common::fmt::EscapeKwFreeIdent(name.as_str()).fmt_sql(f, fmt);
				}
				FieldSegment::All => f.push_str(".*"),
			}
		}
	}
}

impl Display for FieldPath {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.write_str(&self.to_sql())
	}
}

impl LogicalExpr {
	/// The declared position this expression reads, when it reads one: a
	/// [`Self::Field`] is the field; an [`Self::Access`] is its base's
	/// position under the field; an [`Self::Each`] is its base's position
	/// under `*`. Anything else — an index, a filter, a method, a lookup, a
	/// broadcast, a path from an expression — reads no declared position, so
	/// the schema cannot type it and no index column matches it.
	pub fn field_path(&self) -> Option<FieldPath> {
		match self {
			LogicalExpr::Field(name) => Some(FieldPath::field(name.clone())),
			LogicalExpr::Access {
				base,
				field,
			} => Some(base.field_path()?.child(FieldSegment::Field(field.clone()))),
			LogicalExpr::Each(base) => Some(base.field_path()?.child(FieldSegment::All)),
			LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Index {
				..
			}
			| LogicalExpr::First(_)
			| LogicalExpr::Last(_)
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
}

/// Whether a row may carry fields the schema does not list.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Openness {
	/// The listed fields and nothing else.
	Closed,
	/// Any field may be present; an unlisted one has kind `Any`.
	Open,
}

/// What is known about the rows a node produces.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Schema {
	pub fields: Vec<FieldSchema>,
	pub openness: Openness,
}

impl Schema {
	/// Nothing declared, anything allowed.
	pub fn open() -> Self {
		Schema {
			fields: Vec::new(),
			openness: Openness::Open,
		}
	}

	/// Exactly `fields`, nothing else.
	pub fn closed(fields: Vec<FieldSchema>) -> Self {
		Schema {
			fields,
			openness: Openness::Closed,
		}
	}

	pub fn is_open(&self) -> bool {
		self.openness == Openness::Open
	}

	/// The declaration for exactly `path`.
	///
	/// A path beneath a declared field is not itself declared: for `a`
	/// declared `object` (or `FLEXIBLE`), `a.b` answers `None`, and the caller
	/// types it `Any`. That is the right answer, since the interior of an
	/// object field is open unless a field is declared at that path.
	pub fn field(&self, path: &FieldPath) -> Option<&FieldSchema> {
		self.fields.iter().find(|field| &field.path == path)
	}

	/// [`Self::field`], mutably.
	pub fn field_mut(&mut self, path: &FieldPath) -> Option<&mut FieldSchema> {
		self.fields.iter_mut().find(|field| &field.path == path)
	}
}

/// One declared field.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FieldSchema {
	pub path: FieldPath,
	/// `Kind::Any` when the declaration has no `TYPE`.
	pub kind: Kind,
	/// Whether a value read at `path` is known to be of `kind`, rather than
	/// declared to be. `true` for a column a layer coerces to its kind as it
	/// lands, and for a projected expression whose kind is checked
	/// ([`LogicalExpr::checked_kind`]). `false` for a stored field, whose
	/// declaration is enforced on write alone — a value stored before the
	/// declaration, or before it changed, holds what it held — and for a guard
	/// column, which passes the stored value through. A rewrite that treats a
	/// value as being of its kind ([`LogicalExpr::checked_kind`]) requires
	/// this; a rewrite that trusts the declaration reads `kind` alone.
	pub checked: bool,
	/// `FLEXIBLE`: the interior is open beneath a closed schema.
	pub flexible: bool,
	/// `COMPUTED e`, with the authority it runs under.
	pub computed: Option<ComputedBody>,
	pub select_permission: LogicalPermission,
}

impl FieldSchema {
	/// A plain stored field of `kind`, readable by everyone; its kind is
	/// declared, not checked.
	pub fn new(path: FieldPath, kind: Kind) -> Self {
		FieldSchema {
			path,
			kind,
			checked: false,
			flexible: false,
			computed: None,
			select_permission: LogicalPermission::Full,
		}
	}

	/// A field whose value is known to be of `kind`, readable by everyone.
	pub fn checked(path: FieldPath, kind: Kind) -> Self {
		FieldSchema {
			checked: true,
			..Self::new(path, kind)
		}
	}
}

/// The body of a `COMPUTED` field.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ComputedBody {
	pub expr: LogicalExpr,
	/// The declared kind the result is coerced to.
	pub kind: Option<Kind>,
	/// The `AUTH LIMIT` of the defining statement; `None` runs as the caller.
	pub auth: Option<AuthLimit>,
}

/// A `PERMISSIONS FOR select` clause.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalPermission {
	/// Nobody but a bypassing caller.
	None,
	/// Everyone.
	Full,
	/// Callers for whom the predicate is truthy.
	Specific(LogicalExpr),
}

/// A [`LogicalPermission`] whose guard is borrowed from the definition that
/// stores it.
///
/// What a caller judging a clause per evaluation needs, and all it needs: the
/// clause's kind, and the guard itself without a copy. A caller that puts the
/// guard into a plan it keeps takes [`LogicalPermission`] instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalPermissionRef<'a> {
	/// Nobody but a bypassing caller.
	None,
	/// Everyone.
	Full,
	/// Callers for whom the predicate is truthy.
	Specific(&'a LogicalExpr),
}

impl LogicalLiteral {
	/// The kind of the value the literal denotes.
	pub fn kind(&self) -> Kind {
		match self {
			LogicalLiteral::None => Kind::None,
			LogicalLiteral::Null => Kind::Null,
			LogicalLiteral::UnboundedRange => Kind::Range,
			LogicalLiteral::Bool(_) => Kind::Bool,
			LogicalLiteral::Float(_) => Kind::Float,
			LogicalLiteral::Integer(_) => Kind::Int,
			LogicalLiteral::Decimal(_) => Kind::Decimal,
			LogicalLiteral::String(_) => Kind::String,
			LogicalLiteral::Bytes(_) => Kind::Bytes,
			LogicalLiteral::Regex(_) => Kind::Regex,
			LogicalLiteral::Duration(_) => Kind::Duration,
			LogicalLiteral::Datetime(_) => Kind::Datetime,
			LogicalLiteral::Uuid(_) => Kind::Uuid,
			LogicalLiteral::Geometry(_) => Kind::Geometry(Vec::new()),
			LogicalLiteral::File(_) => Kind::File(Vec::new()),
		}
	}
}

/// Whether a kind admits only numbers.
pub fn is_numeric(kind: &Kind) -> bool {
	matches!(kind, Kind::Int | Kind::Float | Kind::Decimal | Kind::Number)
}

/// Whether an element of `kind` reaches the caller unchanged through `.*`:
/// the step dereferences a record id it finds among the elements, so an
/// element kind that admits a record — `any`, a record, an `either` or a
/// literal kind, which may name one — is not kept.
fn each_keeps_element_kind(kind: &Kind) -> bool {
	match kind {
		Kind::Any | Kind::Record(_) | Kind::Either(_) | Kind::Literal(_) => false,
		Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Decimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Geometry(_)
		| Kind::Function(..)
		| Kind::Range
		| Kind::File(_)
		| Kind::Array(..)
		| Kind::Set(..) => true,
	}
}

/// The kind every element of `kinds` shares, or `Any` when they differ or
/// there are none.
pub fn common_kind(kinds: impl IntoIterator<Item = Kind>) -> Kind {
	let mut kinds = kinds.into_iter();
	let Some(first) = kinds.next() else {
		return Kind::Any;
	};
	if kinds.all(|kind| kind == first) {
		first
	} else {
		Kind::Any
	}
}

impl LogicalExpr {
	/// The kind `self` is known to evaluate to, against the row `schema`
	/// describes, or `None` when its kind is declared or inferred rather than
	/// known: a literal's kind, a cast's target, a record id's, a table's or a
	/// constant's kind, the `bool` of a comparison, a containment test or a
	/// `NOT`, the `bool` of an `AND` or `OR` over operands checked to be
	/// `bool`, and the kind of a path to a field the schema marks checked
	/// ([`FieldSchema::checked`]). Anything else is `None`, a path to a stored
	/// field in particular: [`Self::kind`] answers with its declared kind,
	/// which the stored value need not have. A rewrite that drops a check or a
	/// conversion of a value requires this; a rewrite that only trusts the
	/// declaration may read [`Self::kind`].
	pub fn checked_kind(&self, schema: &Schema) -> Option<Kind> {
		match self {
			LogicalExpr::Literal(literal) => Some(literal.kind()),
			LogicalExpr::RecordId(rid) => Some(Kind::Record(vec![rid.table.clone()])),
			LogicalExpr::Table(table) => Some(Kind::Table(vec![table.clone()])),
			LogicalExpr::Constant(constant) => Some(match constant.value() {
				ConstantValue::Float(_) => Kind::Float,
				ConstantValue::Datetime(_) => Kind::Datetime,
				ConstantValue::Duration(_) => Kind::Duration,
			}),
			LogicalExpr::Field(_)
			| LogicalExpr::Access {
				..
			} => self
				.field_path()
				.and_then(|path| schema.field(&path))
				.filter(|field| field.checked)
				.map(|field| field.kind.clone()),
			LogicalExpr::Prefix {
				op,
				..
			} => match op {
				PrefixOperator::Cast(kind) => Some(kind.clone()),
				PrefixOperator::Not => Some(Kind::Bool),
				PrefixOperator::Positive
				| PrefixOperator::Negate
				| PrefixOperator::Range
				| PrefixOperator::RangeInclusive => None,
			},
			LogicalExpr::Binary {
				left,
				op,
				right,
			} => match op {
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
				| BinaryOperator::NearestNeighbor(_) => Some(Kind::Bool),
				// Short-circuit: the result is whichever operand decided it.
				BinaryOperator::And | BinaryOperator::Or => (left.checked_kind(schema)
					== Some(Kind::Bool)
					&& right.checked_kind(schema) == Some(Kind::Bool))
				.then_some(Kind::Bool),
				BinaryOperator::NullCoalescing
				| BinaryOperator::TenaryCondition
				| BinaryOperator::Add
				| BinaryOperator::Subtract
				| BinaryOperator::Multiply
				| BinaryOperator::Divide
				| BinaryOperator::Remainder
				| BinaryOperator::Power
				| BinaryOperator::Range
				| BinaryOperator::RangeInclusive
				| BinaryOperator::RangeSkip
				| BinaryOperator::RangeSkipInclusive => None,
			},
			LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
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
			}
			| LogicalExpr::Postfix {
				..
			}
			| LogicalExpr::Invoke {
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

	/// The kind of the value this expression evaluates to, against `schema`.
	///
	/// `Any` whenever the answer is not a property of the expression and the
	/// schema alone. A path reading a declared position
	/// ([`Self::field_path`]) is the declared kind or `Any`, and every other
	/// path form is `Any`; a `Param` is `Any`, since its value is not a
	/// property of the row; a [`Self::SetField`] is its base's kind, since it
	/// yields the base with one key replaced; a call, a method call, a
	/// subquery and control flow are `Any`. `AND` and `OR` yield one of their
	/// operands, so they are `Bool` only when both operands are; `!` always
	/// is. Arithmetic is `Number` when both operands are numeric and `Any`
	/// otherwise, since `+` on strings concatenates. `IF` yields one of its
	/// branches, so it is the union of their kinds with `NONE` first — a
	/// missing `ELSE` contributes `NONE` — which makes `IF p THEN f ELSE NONE
	/// END` an `option<kind of f>`; a union of one kind is that kind, and a
	/// union with `Any` is `Any`.
	pub fn kind(&self, schema: &Schema) -> Kind {
		match self {
			LogicalExpr::Literal(literal) => literal.kind(),
			LogicalExpr::Array(items) => {
				Kind::Array(Box::new(common_kind(items.iter().map(|e| e.kind(schema)))), None)
			}
			LogicalExpr::Set(items) => {
				Kind::Set(Box::new(common_kind(items.iter().map(|e| e.kind(schema)))), None)
			}
			LogicalExpr::Object(_) => Kind::Object,
			LogicalExpr::RecordId(rid) => Kind::Record(vec![rid.table.clone()]),
			LogicalExpr::Table(table) => Kind::Table(vec![table.clone()]),
			LogicalExpr::Constant(constant) => match constant.value() {
				ConstantValue::Float(_) => Kind::Float,
				ConstantValue::Datetime(_) => Kind::Datetime,
				ConstantValue::Duration(_) => Kind::Duration,
			},
			LogicalExpr::Field(_)
			| LogicalExpr::Access {
				..
			} => self
				.field_path()
				.and_then(|path| schema.field(&path))
				.map(|field| field.kind.clone())
				.unwrap_or(Kind::Any),
			// `base.*` over an array yields the array itself, its record-id
			// elements dereferenced, so it is typed as an array of the kind the
			// schema declares at `base.*` — when the base is declared an array
			// and the element kind is one no dereference can change. Over an
			// object it yields the object, and over anything else `NONE`.
			LogicalExpr::Each(base) => {
				let element = self
					.field_path()
					.and_then(|path| schema.field(&path))
					.map(|field| field.kind.clone());
				match (base.kind(schema), element) {
					(Kind::Array(_, len), Some(element)) if each_keeps_element_kind(&element) => {
						Kind::Array(Box::new(element), len)
					}
					_ => Kind::Any,
				}
			}
			LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Index {
				..
			}
			| LogicalExpr::First(_)
			| LogicalExpr::Last(_)
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
			} => Kind::Any,
			LogicalExpr::Prefix {
				op,
				expr,
			} => match op {
				PrefixOperator::Not => Kind::Bool,
				PrefixOperator::Positive | PrefixOperator::Negate => {
					let kind = expr.kind(schema);
					if is_numeric(&kind) {
						kind
					} else {
						Kind::Any
					}
				}
				PrefixOperator::Range | PrefixOperator::RangeInclusive => Kind::Range,
				PrefixOperator::Cast(kind) => kind.clone(),
			},
			LogicalExpr::Postfix {
				..
			} => Kind::Range,
			LogicalExpr::Invoke {
				..
			} => Kind::Any,
			LogicalExpr::Binary {
				left,
				op,
				right,
			} => binary_kind(op, &left.kind(schema), &right.kind(schema)),
			LogicalExpr::Closure(closure) => Kind::Function(
				Some(closure.args.iter().map(|(_, kind)| kind.clone()).collect()),
				closure.returns.clone().map(Box::new),
			),
			LogicalExpr::If(branches) => branch_kind(
				branches.branches.iter().map(|(_, then)| then.kind(schema)).chain(std::iter::once(
					branches.otherwise.as_ref().map_or(Kind::None, |e| e.kind(schema)),
				)),
			),
			LogicalExpr::SetField {
				base,
				..
			} => base.kind(schema),
			LogicalExpr::Param(_)
			| LogicalExpr::Call(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_) => Kind::Any,
		}
	}
}

/// The kind of a value that is one of `kinds`: their union, flattened and
/// with `None` first so that a value that is a kind or `NONE` is
/// `option<kind>`. `Any` when any branch is, since the union would say
/// nothing more.
fn branch_kind(kinds: impl IntoIterator<Item = Kind>) -> Kind {
	let flat: Vec<Kind> = kinds.into_iter().flat_map(Kind::flatten).collect();
	if flat.contains(&Kind::Any) {
		return Kind::Any;
	}
	let (none, rest): (Vec<Kind>, Vec<Kind>) = flat.into_iter().partition(|k| *k == Kind::None);
	Kind::either(none.into_iter().chain(rest).collect())
}

fn binary_kind(op: &BinaryOperator, left: &Kind, right: &Kind) -> Kind {
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
		| BinaryOperator::NearestNeighbor(_) => Kind::Bool,
		// Short-circuit: the result is whichever operand decided it.
		BinaryOperator::And | BinaryOperator::Or => {
			if *left == Kind::Bool && *right == Kind::Bool {
				Kind::Bool
			} else {
				Kind::Any
			}
		}
		BinaryOperator::NullCoalescing | BinaryOperator::TenaryCondition => Kind::Any,
		BinaryOperator::Add
		| BinaryOperator::Subtract
		| BinaryOperator::Multiply
		| BinaryOperator::Divide
		| BinaryOperator::Remainder
		| BinaryOperator::Power => {
			if is_numeric(left) && is_numeric(right) {
				Kind::Number
			} else {
				Kind::Any
			}
		}
		BinaryOperator::Range
		| BinaryOperator::RangeInclusive
		| BinaryOperator::RangeSkip
		| BinaryOperator::RangeSkipInclusive => Kind::Range,
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use surrealdb_strand::TableName;
	use surrealdb_types::ToSql;

	use super::*;
	use crate::expr::logical::{
		EvalFrame, JoinKind, LogicalDistinct, LogicalEval, LogicalExplain, LogicalFetch,
		LogicalField, LogicalFields, LogicalFilter, LogicalForeach, LogicalInfo, LogicalJoin,
		LogicalLet, LogicalLimit, LogicalOnly, LogicalOp, LogicalOrdering, LogicalPermissionFilter,
		LogicalProject, LogicalReturn, LogicalSequence, LogicalSleep, LogicalSort, LogicalSource,
		LogicalSplit, LogicalSubquery, LogicalTimeout, LogicalUnionAll, LogicalValues,
		LogicalVersionScope, RelationAlias, SourceRef, lower_expr,
	};
	use crate::expr::{ExplainFormat, Param};

	fn expr(sql: &str) -> LogicalExpr {
		let ast = surrealdb_syn::parse(&format!("RETURN {sql}")).expect("expression should parse");
		let crate::sql::TopLevelExpr::Expr(crate::sql::Expr::Return(out)) =
			ast.expressions.into_iter().next().expect("one statement")
		else {
			panic!("expected a RETURN");
		};
		lower_expr(out.what).expect("expression should lower")
	}

	fn idiom(name: &str) -> FieldPath {
		FieldPath::field(name)
	}

	fn table() -> TableName {
		TableName::from("t")
	}

	/// `DEFINE TABLE t SCHEMAFULL; DEFINE FIELD a ON t TYPE int; DEFINE FIELD b
	/// ON t TYPE option<string>; DEFINE FIELD c ON t FLEXIBLE TYPE object`, with
	/// the `id` every table carries.
	fn closed() -> Schema {
		let mut c = FieldSchema::new(idiom("c"), Kind::Object);
		c.flexible = true;
		Schema::closed(vec![
			FieldSchema::new(idiom("id"), Kind::Record(vec![table()])),
			FieldSchema::new(idiom("a"), Kind::Int),
			FieldSchema::new(idiom("b"), Kind::option(Kind::String)),
			c,
		])
	}

	/// A source the analyzer bound to `schema`.
	fn bound(schema: Schema) -> LogicalOp {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(table()),
			schema: Some(Arc::new(schema)),
			with: None,
			for_update: false,
		})
	}

	/// A source in an unbound plan.
	fn unbound() -> LogicalOp {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(table()),
			schema: None,
			with: None,
			for_update: false,
		})
	}

	fn kinds(schema: &Schema) -> Vec<(String, Kind)> {
		schema.fields.iter().map(|f| (f.path.to_sql(), f.kind.clone())).collect()
	}

	fn paths(schema: &Schema) -> Vec<String> {
		schema.fields.iter().map(|f| f.path.to_sql()).collect()
	}

	fn single(sql: &str, alias: Option<&str>) -> LogicalField {
		LogicalField::user(expr(sql), alias.map(OutputPath::field))
	}

	fn kind_of(schema: &Schema, name: &str) -> Option<Kind> {
		schema.field(&idiom(name)).map(|f| f.kind.clone())
	}

	#[test]
	fn kind_answers_the_documented_cases() {
		let schema = Schema::closed(vec![
			FieldSchema::new(idiom("n"), Kind::Int),
			FieldSchema::new(idiom("s"), Kind::String),
			FieldSchema::new(idiom("b"), Kind::Bool),
			FieldSchema::new(idiom("o"), Kind::Object),
		]);
		let cases = [
			("1", Kind::Int),
			("1.5", Kind::Float),
			("'x'", Kind::String),
			("true", Kind::Bool),
			("NONE", Kind::None),
			("n", Kind::Int),
			("s", Kind::String),
			("o.inner", Kind::Any),
			("missing", Kind::Any),
			("$p", Kind::Any),
			("n > 1", Kind::Bool),
			("s = 'x'", Kind::Bool),
			("n IN [1]", Kind::Bool),
			("!n", Kind::Bool),
			("b AND b", Kind::Bool),
			("n AND b", Kind::Any),
			("b OR $p", Kind::Any),
			("n + 1", Kind::Number),
			("n * 1.5", Kind::Number),
			("s + 'x'", Kind::Any),
			("n + $p", Kind::Any),
			("IF b THEN 1 ELSE 2 END", Kind::Int),
			("IF b THEN 1 ELSE 'x' END", Kind::either(vec![Kind::Int, Kind::String])),
			("IF b THEN 1 END", Kind::option(Kind::Int)),
			("IF b THEN s ELSE NONE END", Kind::option(Kind::String)),
			("IF b THEN NONE ELSE s END", Kind::option(Kind::String)),
			("IF b THEN NONE END", Kind::None),
			("IF b THEN $p ELSE 1 END", Kind::Any),
			("IF b THEN o.inner ELSE NONE END", Kind::Any),
			("count()", Kind::Any),
			("(SELECT * FROM t)", Kind::Any),
			("<string> n", Kind::String),
			("t:1", Kind::Record(vec![table()])),
			("[1, 2]", Kind::Array(Box::new(Kind::Int), None)),
			("[1, 'x']", Kind::Array(Box::new(Kind::Any), None)),
			(
				"|$x: int| -> int { $x }",
				Kind::Function(Some(vec![Kind::Int]), Some(Box::new(Kind::Int))),
			),
		];
		for (sql, want) in cases {
			assert_eq!(expr(sql).kind(&schema), want, "kind of `{sql}`");
		}
	}

	#[test]
	fn a_closed_schema_types_its_declared_fields_and_nothing_beneath_them() {
		let schema = closed();
		assert_eq!(expr("id").kind(&schema), Kind::Record(vec![table()]));
		assert_eq!(expr("a").kind(&schema), Kind::Int);
		assert_eq!(expr("b").kind(&schema), Kind::option(Kind::String));
		assert_eq!(expr("c").kind(&schema), Kind::Object);
		// The interior of a flexible object is open.
		assert_eq!(expr("c.x").kind(&schema), Kind::Any);
		// A closed schema says an undeclared field is absent; it does not know
		// what kind a value there would have.
		assert_eq!(expr("missing").kind(&schema), Kind::Any);
		assert_eq!(expr("a.b").kind(&schema), Kind::Any);
	}

	#[test]
	fn every_field_on_an_open_schema_is_any() {
		let open = Schema::open();
		for sql in ["id", "a", "b", "c", "c.x", "missing", "a.b[0]", "$p.a"] {
			assert_eq!(expr(sql).kind(&open), Kind::Any, "kind of `{sql}` on an open schema");
		}
		// A literal's kind is the literal's own, and an operator whose result
		// kind does not depend on its operands still answers.
		assert_eq!(expr("1").kind(&open), Kind::Int);
		assert_eq!(expr("a > 1").kind(&open), Kind::Bool);
		assert_eq!(expr("a + 1").kind(&open), Kind::Any);
		// An unbound source is open.
		assert_eq!(unbound().schema(), Schema::open());
	}

	#[test]
	fn a_projection_types_each_field_against_its_input() {
		let project = |input: LogicalOp| {
			LogicalOp::Project(LogicalProject {
				input: Box::new(input),
				fields: LogicalFields::Select(vec![
					single("id", None),
					single("a + 1", Some("sum")),
					single("b", None),
					single("c.x", Some("inner")),
					single("a > 1", Some("big")),
					single("<string> a", Some("text")),
				]),
				omit: vec![],
			})
		};
		let schema = project(bound(closed())).schema();
		assert_eq!(schema.openness, Openness::Closed);
		assert_eq!(
			kinds(&schema),
			[
				("id".to_string(), Kind::Record(vec![table()])),
				("sum".to_string(), Kind::Number),
				("b".to_string(), Kind::option(Kind::String)),
				("inner".to_string(), Kind::Any),
				("big".to_string(), Kind::Bool),
				("text".to_string(), Kind::String),
			]
		);
		// A projection over an open input is still closed — it produces
		// exactly these fields — but each is typed only where the expression
		// alone fixes the kind.
		let schema = project(unbound()).schema();
		assert_eq!(schema.openness, Openness::Closed);
		assert_eq!(
			kinds(&schema),
			[
				("id".to_string(), Kind::Any),
				("sum".to_string(), Kind::Any),
				("b".to_string(), Kind::Any),
				("inner".to_string(), Kind::Any),
				("big".to_string(), Kind::Bool),
				("text".to_string(), Kind::String),
			]
		);
		// `*` carries the input through, openness included, and OMIT removes.
		let star = LogicalOp::Project(LogicalProject {
			input: Box::new(bound(closed())),
			fields: LogicalFields::all(),
			omit: vec![expr("b")],
		});
		assert_eq!(star.schema().openness, Openness::Closed);
		assert_eq!(paths(&star.schema()), ["id", "a", "c"]);
		let star = LogicalOp::Project(LogicalProject {
			input: Box::new(unbound()),
			fields: LogicalFields::all(),
			omit: vec![],
		});
		assert!(star.schema().is_open());
		// `VALUE` rows are bare values, which a field list cannot describe.
		let value = LogicalOp::Project(LogicalProject {
			input: Box::new(bound(closed())),
			fields: LogicalFields::Value {
				expr: expr("a"),
				alias: None,
			},
			omit: vec![],
		});
		assert!(value.schema().is_open());
	}

	#[test]
	fn a_union_merges_fields_by_path_and_is_open_when_any_input_is() {
		let other = Schema::closed(vec![
			FieldSchema::new(idiom("id"), Kind::Record(vec![TableName::from("u")])),
			FieldSchema::new(idiom("a"), Kind::Int),
			FieldSchema::new(idiom("d"), Kind::Bool),
		]);
		let union = LogicalOp::UnionAll(LogicalUnionAll {
			inputs: vec![bound(closed()), bound(other)],
		});
		let schema = union.schema();
		assert_eq!(schema.openness, Openness::Closed);
		assert_eq!(paths(&schema), ["id", "a", "b", "c", "d"]);
		// Declared identically everywhere: the kind.
		assert_eq!(schema.field(&idiom("a")).map(|f| f.kind.clone()), Some(Kind::Int));
		// Declared differently: unknown.
		assert_eq!(schema.field(&idiom("id")).map(|f| f.kind.clone()), Some(Kind::Any));
		// Absent from a closed input: optional there, so optional in the union.
		assert_eq!(
			schema.field(&idiom("d")).map(|f| f.kind.clone()),
			Some(Kind::option(Kind::Bool))
		);
		assert_eq!(
			schema.field(&idiom("b")).map(|f| f.kind.clone()),
			Some(Kind::option(Kind::String))
		);
		// An open input opens the union and unknows every field it does not
		// declare.
		let union = LogicalOp::UnionAll(LogicalUnionAll {
			inputs: vec![bound(closed()), unbound()],
		});
		let schema = union.schema();
		assert!(schema.is_open());
		assert_eq!(paths(&schema), ["id", "a", "b", "c"]);
		assert!(schema.fields.iter().all(|f| f.kind == Kind::Any), "{:?}", kinds(&schema));
	}

	#[test]
	fn each_is_typed_as_the_array_it_yields() {
		let all = |name: &str| idiom(name).child(FieldSegment::All);
		// `DEFINE FIELD tags TYPE array<string>` declares `tags.*` as `string`;
		// `nums` likewise with a length; `refs` holds record ids, which `.*`
		// dereferences; `o` is an object whose values are declared, and `.*`
		// over an object yields the object.
		let schema = Schema::closed(vec![
			FieldSchema::new(idiom("tags"), Kind::Array(Box::new(Kind::String), None)),
			FieldSchema::new(all("tags"), Kind::String),
			FieldSchema::new(idiom("nums"), Kind::Array(Box::new(Kind::Int), Some(2))),
			FieldSchema::new(all("nums"), Kind::Int),
			FieldSchema::new(
				idiom("refs"),
				Kind::Array(Box::new(Kind::Record(vec![TableName::from("u")])), None),
			),
			FieldSchema::new(all("refs"), Kind::Record(vec![TableName::from("u")])),
			FieldSchema::new(idiom("o"), Kind::Object),
			FieldSchema::new(all("o"), Kind::String),
			FieldSchema::new(idiom("loose"), Kind::Any),
			FieldSchema::new(all("loose"), Kind::String),
		]);
		assert_eq!(expr("tags.*").kind(&schema), Kind::Array(Box::new(Kind::String), None));
		assert_eq!(expr("nums.*").kind(&schema), Kind::Array(Box::new(Kind::Int), Some(2)));
		assert_eq!(expr("<string> tags.*").kind(&schema), Kind::String);
		assert_eq!(expr("refs.*").kind(&schema), Kind::Any);
		assert_eq!(expr("o.*").kind(&schema), Kind::Any);
		assert_eq!(expr("loose.*").kind(&schema), Kind::Any);
		assert_eq!(expr("missing.*").kind(&schema), Kind::Any);
		// The position read is unchanged: `tags.*` still names the declared
		// element position, which is what index and demand analysis match.
		assert_eq!(expr("tags.*").field_path(), Some(all("tags")));
	}

	#[test]
	fn a_split_replaces_a_collection_kind_with_its_element_kind() {
		let schema = Schema::closed(vec![
			FieldSchema::new(idiom("arr"), Kind::Array(Box::new(Kind::Int), None)),
			FieldSchema::new(idiom("set"), Kind::Set(Box::new(Kind::String), None)),
			FieldSchema::new(idiom("opt"), Kind::option(Kind::Array(Box::new(Kind::Bool), None))),
			FieldSchema::new(idiom("a"), Kind::Int),
		]);
		let split = LogicalOp::Split(LogicalSplit {
			input: Box::new(bound(schema)),
			exprs: vec![expr("arr"), expr("set"), expr("opt"), expr("a"), expr("missing")],
		});
		let schema = split.schema();
		assert_eq!(schema.openness, Openness::Closed);
		assert_eq!(
			kinds(&schema),
			[
				("arr".to_string(), Kind::Int),
				("set".to_string(), Kind::String),
				("opt".to_string(), Kind::option(Kind::Bool)),
				// A value that is not a collection passes through a split.
				("a".to_string(), Kind::Int),
			]
		);
		assert!(
			LogicalOp::Split(LogicalSplit {
				input: Box::new(unbound()),
				exprs: vec![expr("arr")],
			})
			.schema()
			.is_open()
		);
	}

	/// The layer the resolver builds over `t` for `DEFINE FIELD c ON t TYPE
	/// int COMPUTED string::len(b)` and `DEFINE FIELD b ON t TYPE
	/// option<string> PERMISSIONS FOR select WHERE p`: the computed column
	/// carries the declared kind and coerces to it; the guard carries none,
	/// since a stored value is read as stored.
	fn resolved_layer() -> LogicalOp {
		LogicalOp::Project(LogicalProject {
			input: Box::new(bound(closed())),
			fields: LogicalFields::Select(vec![
				LogicalField::All,
				LogicalField::column(
					"c",
					expr("string::len(b)"),
					Some(Kind::Int),
					EvalFrame::computed(None),
				),
				LogicalField::column(
					"b",
					expr("IF p THEN b ELSE NONE END"),
					None,
					EvalFrame::permission(None),
				),
			]),
			omit: vec![],
		})
	}

	#[test]
	fn a_layer_retypes_its_columns_over_its_input() {
		let layer = resolved_layer();
		assert!(layer.is_layer());
		let schema = layer.schema();
		// The row keeps its shape: the same fields in the same order, the
		// same openness; only the columns' kinds change.
		assert_eq!(schema.openness, Openness::Closed);
		assert_eq!(paths(&schema), ["id", "a", "b", "c"]);
		assert_eq!(kind_of(&schema, "a"), Some(Kind::Int));
		// The computed body alone would be `Any`; the declared kind decides.
		assert_eq!(kind_of(&schema, "c"), Some(Kind::Int));
		// The guard yields the field or `NONE`: an option of the field's
		// declared kind.
		assert_eq!(kind_of(&schema, "b"), Some(Kind::option(Kind::String)));
		// The declaration's other properties survive the retyping.
		assert!(schema.field(&idiom("c")).is_some_and(|f| f.flexible));
	}

	#[test]
	fn a_column_the_input_does_not_declare_is_typed_by_its_expression() {
		// Over an open input nothing is declared: a guard column is typed by
		// its expression, and a column the input lacks is added to it.
		let layer = LogicalOp::Project(LogicalProject {
			input: Box::new(unbound()),
			fields: LogicalFields::Select(vec![
				LogicalField::All,
				LogicalField::column(
					"secret",
					expr("IF p THEN secret ELSE NONE END"),
					None,
					EvalFrame::permission(None),
				),
				LogicalField::column(
					"c",
					expr("a + 1"),
					Some(Kind::Int),
					EvalFrame::computed(None),
				),
				LogicalField::column("d", expr("1 > 0"), None, EvalFrame::computed(None)),
			]),
			omit: vec![],
		});
		assert!(layer.is_layer());
		let schema = layer.schema();
		assert!(schema.is_open());
		assert_eq!(
			kinds(&schema),
			[
				("secret".to_string(), Kind::Any),
				("c".to_string(), Kind::Int),
				("d".to_string(), Kind::Bool),
			]
		);
		// A guard over a declared field with no `TYPE` stays `Any` rather
		// than becoming `option<any>`.
		let layer = LogicalOp::Project(LogicalProject {
			input: Box::new(bound(Schema::closed(vec![FieldSchema::new(idiom("g"), Kind::Any)]))),
			fields: LogicalFields::Select(vec![
				LogicalField::All,
				LogicalField::column(
					"g",
					expr("IF p THEN g ELSE NONE END"),
					None,
					EvalFrame::permission(None),
				),
			]),
			omit: vec![],
		});
		assert_eq!(kind_of(&layer.schema(), "g"), Some(Kind::Any));
	}

	#[test]
	fn a_read_above_a_layer_is_typed_as_the_column() {
		let layer = resolved_layer();
		let schema = layer.schema();
		// Above the layer a plain field read sees the column: the derived
		// value under the field's own name.
		assert_eq!(expr("c").kind(&schema), Kind::Int);
		assert_eq!(expr("b").kind(&schema), Kind::option(Kind::String));
		assert_eq!(expr("c > 1").kind(&schema), Kind::Bool);
		assert_eq!(expr("c + 1").kind(&schema), Kind::Number);
		// Beneath the layer the same names are the stored declarations.
		assert_eq!(expr("c").kind(&closed()), Kind::Object);
		assert_eq!(expr("b").kind(&closed()), Kind::option(Kind::String));
		// A parameter is never a property of the row.
		assert_eq!(expr("$c").kind(&schema), Kind::Any);
		assert_eq!(expr("$_c0").kind(&schema), Kind::Any);
		// The user's projection above the stack is not a layer, and types
		// each output field as the column it reads, under the alias the
		// statement gave it.
		let project = LogicalOp::Project(LogicalProject {
			input: Box::new(layer),
			fields: LogicalFields::Select(vec![
				single("c", None),
				single("b", Some("guarded")),
				single("a", None),
			]),
			omit: vec![],
		});
		assert!(!project.is_layer());
		assert_eq!(
			kinds(&project.schema()),
			[
				("c".to_string(), Kind::Int),
				("guarded".to_string(), Kind::option(Kind::String)),
				("a".to_string(), Kind::Int),
			]
		);
	}

	#[test]
	fn set_field_has_the_kind_of_its_base() {
		let schema = closed();
		let guard = LogicalExpr::SetField {
			base: Box::new(expr("c")),
			field: Strand::new("inner"),
			value: Box::new(expr("IF p THEN c.inner ELSE NONE END")),
		};
		assert_eq!(guard.kind(&schema), Kind::Object);
		assert_eq!(guard.field_path(), None);
	}

	#[test]
	fn row_preserving_operators_pass_their_input_schema_through() {
		let leaf = || Box::new(bound(closed()));
		let ops = vec![
			LogicalOp::Subquery(LogicalSubquery {
				input: leaf(),
			}),
			LogicalOp::Filter(LogicalFilter {
				input: leaf(),
				predicate: expr("a > 1"),
			}),
			LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input: leaf(),
				predicate: expr("true"),
				auth: None,
			}),
			LogicalOp::Distinct(LogicalDistinct {
				input: leaf(),
			}),
			LogicalOp::Sort(LogicalSort {
				input: leaf(),
				ordering: LogicalOrdering::Random,
				tempfiles: false,
			}),
			LogicalOp::Limit(LogicalLimit {
				input: leaf(),
				limit: Some(expr("1")),
				start: None,
			}),
			LogicalOp::Timeout(LogicalTimeout {
				input: leaf(),
				timeout: expr("1s"),
			}),
			LogicalOp::VersionScope(LogicalVersionScope {
				input: leaf(),
				version: expr("d'2024-01-01T00:00:00Z'"),
			}),
			LogicalOp::Only(LogicalOnly {
				input: leaf(),
			}),
		];
		for op in ops {
			assert_eq!(op.schema(), closed(), "{} should pass its input through", op.name());
		}
	}

	#[test]
	fn a_join_adds_the_alias_typed_as_the_right_tables_record() {
		let join = |kind: JoinKind, right: LogicalOp| {
			LogicalOp::Join(LogicalJoin::new(
				bound(closed()),
				right,
				kind,
				vec![(expr("a"), RelationAlias::new(0).access("id"))],
				None,
				RelationAlias::new(0),
				EvalFrame::caller(),
			))
		};
		let other = || {
			LogicalOp::Filter(LogicalFilter {
				input: Box::new(LogicalOp::Source(LogicalSource {
					source: SourceRef::Table(TableName::from("u")),
					schema: None,
					with: None,
					for_update: false,
				})),
				predicate: expr("true"),
			})
		};
		// An inner join adds the matched record; a left join may hold NULL.
		let inner = join(JoinKind::Inner, other()).schema();
		assert_eq!(inner.openness, Openness::Closed);
		assert_eq!(kind_of(&inner, "_join0"), Some(Kind::Record(vec![TableName::from("u")])));
		assert_eq!(kind_of(&inner, "a"), Some(Kind::Int));
		let left = join(JoinKind::Left, other()).schema();
		assert_eq!(
			kind_of(&left, "_join0"),
			Some(Kind::either(vec![Kind::Null, Kind::Record(vec![TableName::from("u")])]))
		);
		// A right input that reads no single table types the alias as Any.
		let values = join(
			JoinKind::Inner,
			LogicalOp::Values(LogicalValues {
				expr: expr("[1]"),
				for_update: false,
			}),
		)
		.schema();
		assert_eq!(kind_of(&values, "_join0"), Some(Kind::Any));
		// A semi or anti join emits left rows unchanged.
		assert_eq!(join(JoinKind::Semi, other()).schema(), closed());
		assert_eq!(join(JoinKind::Anti, other()).schema(), closed());
	}

	#[test]
	fn a_fetch_retypes_the_fields_its_targets_start_from() {
		let fetch = |fetches: Vec<LogicalExpr>| {
			LogicalOp::Fetch(LogicalFetch {
				input: Box::new(bound(closed())),
				fetches,
			})
		};
		// The fetched field becomes the record it linked to, whose shape the
		// schema does not know; every other field is untouched.
		let schema = fetch(vec![expr("id")]).schema();
		assert_eq!(schema.openness, Openness::Closed);
		assert_eq!(
			kinds(&schema),
			[
				("id".to_string(), Kind::Any),
				("a".to_string(), Kind::Int),
				("b".to_string(), Kind::option(Kind::String)),
				("c".to_string(), Kind::Object),
			]
		);
		// A target beneath a field reaches into that field.
		let schema = fetch(vec![expr("c.link"), expr("b")]).schema();
		assert_eq!(schema.field(&idiom("c")).map(|f| f.kind.clone()), Some(Kind::Any));
		assert_eq!(schema.field(&idiom("b")).map(|f| f.kind.clone()), Some(Kind::Any));
		assert_eq!(schema.field(&idiom("a")).map(|f| f.kind.clone()), Some(Kind::Int));
		// A target that names no field of the row changes nothing.
		assert_eq!(fetch(vec![expr("missing")]).schema(), closed());
		// A target whose reach is decided at run time opens the schema.
		for target in ["$paths", "type::fields(['a'])", "$this.a"] {
			assert!(fetch(vec![expr(target)]).schema().is_open(), "FETCH {target}");
		}
		// An open input stays open.
		assert!(
			LogicalOp::Fetch(LogicalFetch {
				input: Box::new(unbound()),
				fetches: vec![expr("a")],
			})
			.schema()
			.is_open()
		);
	}

	#[test]
	fn values_explain_and_every_statement_operator_are_open() {
		let leaf = || Box::new(bound(closed()));
		let ops = vec![
			LogicalOp::Values(LogicalValues {
				expr: expr("[1, 2]"),
				for_update: false,
			}),
			LogicalOp::Explain(LogicalExplain {
				input: leaf(),
				analyze: false,
				format: ExplainFormat::Text,
			}),
			LogicalOp::Eval(LogicalEval {
				expr: expr("1"),
			}),
			LogicalOp::Sequence(LogicalSequence {
				stmts: vec![bound(closed())],
			}),
			LogicalOp::Let(LogicalLet {
				name: "x".to_string(),
				kind: Some(Kind::Int),
				value: leaf(),
			}),
			LogicalOp::Return(LogicalReturn {
				what: leaf(),
				fetch: vec![],
			}),
			LogicalOp::Foreach(LogicalForeach {
				param: Param::from("x".to_string()),
				range: expr("[1]"),
				body: leaf(),
			}),
			LogicalOp::Info(Box::new(LogicalInfo::Db {
				structure: false,
				version: None,
			})),
			LogicalOp::Sleep(LogicalSleep {
				duration: expr("1ms"),
			}),
		];
		for op in ops {
			assert_eq!(op.schema(), Schema::open(), "{} should be open", op.name());
		}
	}
}
