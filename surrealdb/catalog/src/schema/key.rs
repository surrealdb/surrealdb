//! The structure of an array record id.
//!
//! A table's record ids can be arrays whose elements mean something: the
//! `[city, time]` of `reading:['London', d'2025-01-01T10:00:00Z']`. A
//! [`KeyStructure`] says how many elements the key has, what each one holds, how
//! the stored encoding of each one orders, and which names address it. It is
//! built from one of two sources and read by one set of consumers:
//!
//! - the `TYPE` of the `id` field, when it names its components (`DEFINE FIELD id ON reading TYPE
//!   [city: string, time: datetime]`), and
//! - the group expressions of an aggregated view, whose rows are keyed by their group values.
//!
//! A structure is runtime-only. It is derived from stored definitions
//! ([`FieldDefinition::from_stored`], [`table_key`]) and never written.
//!
//! # Derived fields
//!
//! A named component is readable as a field. [`KeyStructure::derived_fields`]
//! yields one virtual definition per name: a read-only computed field whose
//! expression is `id[i]`. [`fields_from_stored`] places those definitions in the
//! field list straight after `id`. They have no stored form, so a consumer that
//! acts on definitions must decide what a definition with
//! [`FieldDefinition::key_part`] set means to it.

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use surrealdb_strand::Strand;
use surrealdb_types::ToSql;

use crate::catalog::aggregation::{AggregateFields, group_field_name};
use crate::catalog::auth::AuthLimit;
use crate::catalog::{
	FieldDefinition, LogicalBody, StoredFieldDefinition, TableDefinition, ViewDefinition,
	from_stored_all,
};
use crate::expr::{
	Expr, Idiom, KeyNames, KeyNamesError, Kind, KindLiteral, Literal, Part, PrefixOperator,
};
use crate::val::{Number, RecordIdKey, TableName, Value};

/// What a record id component holds.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ComponentKind {
	String,
	Int,
	Datetime,
	Uuid,
	Duration,
	Bool,
	/// A record id. An empty list admits a record of any table.
	Record(Box<[TableName]>),
	/// A type outside the set a key range can reason about: a stored component
	/// type that the allow-list ([`ComponentKind::classify`]) does not admit, or a
	/// component of an aggregated view whose group expression does not fix the
	/// type of the values it yields ([`group_component_kind`]).
	Unknown,
}

impl ComponentKind {
	/// The component kind of a declared type, or `None` when a record id
	/// component cannot have that type: only strings, ints, datetimes, uuids,
	/// durations, bools and records keep a storage order that a key range can use.
	pub fn classify(kind: &Kind) -> Option<Self> {
		Some(match kind {
			Kind::Named(_) => return None,
			Kind::String => Self::String,
			Kind::Int => Self::Int,
			Kind::Datetime => Self::Datetime,
			Kind::Uuid => Self::Uuid,
			Kind::Duration => Self::Duration,
			Kind::Bool => Self::Bool,
			Kind::Record(tables) => Self::Record(tables.clone().into_boxed_slice()),
			Kind::Any
			| Kind::None
			| Kind::Null
			| Kind::Float
			| Kind::Decimal
			| Kind::BigDecimal
			| Kind::Number
			| Kind::I8
			| Kind::U8
			| Kind::I16
			| Kind::U16
			| Kind::I32
			| Kind::U32
			| Kind::U64
			| Kind::F16
			| Kind::F32
			| Kind::Date
			| Kind::Time
			| Kind::Timestamp
			| Kind::Interval
			| Kind::Object
			| Kind::Array(..)
			| Kind::Geometry(..)
			| Kind::Bytes
			| Kind::Regex
			| Kind::Table(..)
			| Kind::Range
			| Kind::Set(..)
			| Kind::Either(..)
			| Kind::Function(..)
			| Kind::Literal(..)
			| Kind::File(..)
			| Kind::Enum(..) => return None,
		})
	}

	/// How the stored encoding of a component of this kind orders.
	pub fn order(&self) -> ComponentOrder {
		match self {
			Self::String | Self::Int | Self::Uuid | Self::Duration | Self::Bool => {
				ComponentOrder::Exact
			}
			Self::Datetime => ComponentOrder::CoarseSecond,
			Self::Record(_) => ComponentOrder::EqualityOnly,
			Self::Unknown => ComponentOrder::Unknown,
		}
	}

	/// Why `value` cannot be an element of a key at a component of this kind, as
	/// a phrase that follows the component's name ("`time` is not a datetime"),
	/// or `None` when it can.
	///
	/// An int component holds only a `Number::Int`: a float or a decimal that equals
	/// an integer (`5.0`, `5dec`) is refused, because its stored encoding is not
	/// that of the integer. A datetime component holds a datetime within the years
	/// 0000 to 9999 that is not a leap second.
	pub fn refusal(&self, value: &Value) -> Option<&'static str> {
		match (self, value) {
			(Self::Unknown, _) => None,
			(Self::String, Value::String(_)) => None,
			(Self::String, _) => Some("is not a string"),
			(Self::Int, Value::Number(n)) => match n {
				Number::Int(_) => None,
				Number::Float(_) if n.as_int_lossless().is_some() => {
					Some("is a float; an int component holds only ints")
				}
				Number::Decimal(_) if n.as_int_lossless().is_some() => {
					Some("is a decimal; an int component holds only ints")
				}
				Number::BigDecimal(_) if n.as_int_lossless().is_some() => {
					Some("is a bigdecimal; an int component holds only ints")
				}
				Number::Float(_)
				| Number::Decimal(_)
				| Number::BigDecimal(_)
				| Number::I8(_)
				| Number::I16(_)
				| Number::I32(_)
				| Number::U8(_)
				| Number::U16(_)
				| Number::U32(_)
				| Number::U64(_)
				| Number::F16(_)
				| Number::F32(_) => Some("is not an int"),
			},
			(Self::Int, _) => Some("is not an int"),
			(Self::Datetime, Value::Datetime(d)) => {
				if d.is_leap_second() {
					Some("is a leap second, which a record id cannot hold")
				} else if d.in_key_range() {
					None
				} else {
					Some("is a datetime outside the years 0000 to 9999")
				}
			}
			(Self::Datetime, _) => Some("is not a datetime"),
			(Self::Uuid, Value::Uuid(_)) => None,
			(Self::Uuid, _) => Some("is not a uuid"),
			(Self::Duration, Value::Duration(_)) => None,
			(Self::Duration, _) => Some("is not a duration"),
			(Self::Bool, Value::Bool(_)) => None,
			(Self::Bool, _) => Some("is not a bool"),
			(Self::Record(tables), Value::RecordId(rid)) => {
				if tables.is_empty() || tables.contains(&rid.table) {
					None
				} else {
					Some("is a record of another table")
				}
			}
			(Self::Record(_), _) => Some("is not a record"),
		}
	}
}

/// How the byte order of a component's stored encoding relates to the order of
/// its values. It bounds what a key range over the component may claim.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ComponentOrder {
	/// Byte order is value order, and two values encode alike exactly when they
	/// are equal: a bound is exact. Strings, ints, uuids, durations and bools. An
	/// int component holds only a `Number::Int`: the encoding of a float or a decimal
	/// carries another kind tag, so it would sort out of order against the elements
	/// that follow it.
	Exact,
	/// Only the whole-second prefix of the encoding orders chronologically; the
	/// sub-second digits and zone letter that follow it do not. A bound widens to
	/// the whole second. A datetime component.
	CoarseSecond,
	/// Equality is exact and no order is claimed. A record component.
	EqualityOnly,
	/// Nothing is known: the type is outside the set above, or the component
	/// belongs to an aggregated view whose group expression does not fix it.
	Unknown,
}

/// Where a [`KeyStructure`] comes from.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum KeyOrigin {
	/// The `TYPE` of the `id` field, with its components named.
	IdField,
	/// The group expressions of an aggregated view.
	AggregateView,
}

/// One element of a record id.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct KeyComponent {
	/// The index of the element in the key.
	pub position: u32,
	/// The name that addresses the element as a field. Some on every component
	/// of a [`KeyOrigin::IdField`] structure and on none of a
	/// [`KeyOrigin::AggregateView`] structure, whose components are addressed
	/// through [`KeyStructure::aliases`].
	pub name: Option<Strand>,
	pub kind: ComponentKind,
	/// The declared type, as written. None for an aggregated view.
	pub declared: Option<Kind>,
}

/// The shape of the array record ids of a table.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct KeyStructure {
	pub origin: KeyOrigin,
	/// One component per element of the key, in position order.
	pub components: Box<[KeyComponent]>,
	/// The idioms that address a component, with its position. For an `id`
	/// field these are the component names. For an aggregated view they are the
	/// output names of the select items that project a group value unchanged, so
	/// several idioms can address one position and a group that is not projected
	/// has none.
	pub aliases: Box<[(Idiom, u32)]>,
	/// The component names, as declared. Some exactly for [`KeyOrigin::IdField`].
	pub names: Option<KeyNames>,
}

/// Why a type cannot give a record id named components.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum KeyStructureError {
	/// A component has a type that a key range cannot reason about.
	#[error("the record id component `{name}` cannot have the type `{kind}`")]
	UnsupportedKind {
		name: String,
		kind: String,
	},
	/// A component name is not a plain identifier ([`KeyStructure::is_plain_name`]).
	#[error("the record id component name `{0}` is not a plain identifier")]
	NotPlainName(String),
	/// The names are not a valid list for the type.
	#[error(transparent)]
	Names(#[from] KeyNamesError),
	/// The type is not a tuple.
	#[error("a record id with named components must have a tuple type")]
	NotATuple,
}

/// Why a record id key does not fit a [`KeyStructure`].
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Nonconformance {
	/// The key is not an array.
	NotArray,
	/// The key has a different number of elements.
	Arity {
		expected: usize,
		found: usize,
	},
	/// An element is not a value its component can hold. `reason` is a phrase
	/// that follows the component's name, as [`ComponentKind::refusal`] gives it.
	Component {
		position: u32,
		name: Option<String>,
		reason: &'static str,
	},
}

impl Display for Nonconformance {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			Self::NotArray => f.write_str("the record id is not an array"),
			Self::Arity {
				expected,
				found,
			} => {
				let elements = if *found == 1 {
					"element"
				} else {
					"elements"
				};
				let are = if *expected == 1 {
					"is"
				} else {
					"are"
				};
				write!(f, "the record id has {found} {elements}, but {expected} {are} expected")
			}
			Self::Component {
				position,
				name: Some(name),
				reason,
			} => write!(f, "`{name}` {reason} (element {position})"),
			Self::Component {
				position,
				name: None,
				reason,
			} => write!(f, "element {position} {reason}"),
		}
	}
}

/// The elements of a tuple type.
fn tuple_kinds(kind: &Kind) -> Option<&[Kind]> {
	if let Kind::Literal(KindLiteral::Array(kinds)) = kind {
		Some(kinds)
	} else {
		None
	}
}

/// The name of the first part of an idiom, when that part is a field.
fn root_name(idiom: &Idiom) -> Option<&str> {
	if let Some(Part::Field(name)) = idiom.0.first() {
		Some(name.as_str())
	} else {
		None
	}
}

impl KeyStructure {
	/// The structure the `TYPE` of an `id` field declares for `names`.
	///
	/// Strict: this is the check `DEFINE FIELD` and `ALTER FIELD` apply. `kind`
	/// must be a tuple with one element per name, every name must be a plain
	/// identifier ([`Self::is_plain_name`]), and every element must be a type a
	/// key range can reason about ([`ComponentKind::classify`]).
	///
	/// # Errors
	///
	/// [`KeyStructureError`] naming the first fault, in position order; at one
	/// position the name is judged before the type.
	pub fn from_id_field(kind: &Kind, names: &KeyNames) -> Result<Self, KeyStructureError> {
		let kinds = tuple_kinds(kind).ok_or(KeyStructureError::NotATuple)?;
		if kinds.len() != names.len() {
			return Err(KeyStructureError::Names(KeyNamesError::Arity {
				names: names.len(),
				kinds: kinds.len(),
			}));
		}
		let mut classified = Vec::with_capacity(kinds.len());
		for (name, kind) in names.as_slice().iter().zip(kinds) {
			if !Self::is_plain_name(name.as_str()) {
				return Err(KeyStructureError::NotPlainName(name.as_str().to_owned()));
			}
			let Some(component) = ComponentKind::classify(kind) else {
				return Err(KeyStructureError::UnsupportedKind {
					name: name.as_str().to_owned(),
					kind: kind.to_sql(),
				});
			};
			classified.push(component);
		}
		Ok(Self::id_field(classified, kinds, names))
	}

	/// Whether `name` can name a component: it reads back unchanged as the raw
	/// name of the field it derives ([`Idiom::to_raw_string`]). That raw name is
	/// the key under which the record's fields are evaluated and handed to its
	/// events, views, `RETURN` images and `VALUE` and `ASSERT` clauses, and the
	/// rendering escapes every other name, so a component with another name would
	/// be invisible to them.
	///
	/// A plain identifier is non-empty, holds only ASCII letters, digits and
	/// underscores, does not start with a digit, and is neither `NaN` nor
	/// `Infinity`. A keyword is plain.
	pub fn is_plain_name(name: &str) -> bool {
		Idiom::field(Strand::new(name)).to_raw_string() == name
	}

	/// The structure the stored `TYPE` of an `id` field declares for `names`.
	///
	/// Lenient: reading the catalog never refuses a table over the types of its
	/// components. A type outside the set a key range can reason about gives a
	/// component of kind [`ComponentKind::Unknown`], whose order is
	/// [`ComponentOrder::Unknown`]. Returns `None` only when `kind` is not a
	/// tuple with one element per name.
	pub fn from_stored_id_field(kind: &Kind, names: &KeyNames) -> Option<Self> {
		let kinds = tuple_kinds(kind)?;
		if kinds.len() != names.len() {
			return None;
		}
		let classified =
			kinds.iter().map(|k| ComponentKind::classify(k).unwrap_or(ComponentKind::Unknown));
		Some(Self::id_field(classified.collect(), kinds, names))
	}

	/// The structure of an `id` field: components paired with their names and
	/// declared types, and each name an alias of its position.
	fn id_field(classified: Vec<ComponentKind>, kinds: &[Kind], names: &KeyNames) -> Self {
		let components: Vec<KeyComponent> = classified
			.into_iter()
			.zip(kinds)
			.zip(names.as_slice())
			.enumerate()
			.map(|(position, ((kind, declared), name))| KeyComponent {
				position: position as u32,
				name: Some(name.clone()),
				kind,
				declared: Some(declared.clone()),
			})
			.collect();
		let aliases = names
			.as_slice()
			.iter()
			.enumerate()
			.map(|(position, name)| (Idiom::field(name.clone()), position as u32))
			.collect();
		Self {
			origin: KeyOrigin::IdField,
			components: components.into_boxed_slice(),
			aliases,
			names: Some(names.clone()),
		}
	}

	/// The structure of the rows of an aggregated view: one component per
	/// `GROUP` expression, in group order.
	///
	/// A component is addressed by the output name of each select item that is
	/// exactly that group's value (`SELECT a, time::floor(ts, 1h) AS bucket ...
	/// GROUP BY a, bucket`); a group the view does not project has no alias. An
	/// item whose name another item also writes to is no alias either: the same
	/// name (`SELECT a AS x, b AS x`), a path beneath it or a path it lies beneath
	/// (`SELECT a AS x, b AS x.y`). The stored row then holds at that name what the
	/// last such item wrote, which need not be the group value the key holds. An
	/// item repeated exactly is the one alias. Nor is a name that is `id` or a
	/// path beneath it, since a read of the row's `id` reads its record id. A
	/// component's kind is the one its group expression fixes
	/// ([`group_component_kind`]), and [`ComponentKind::Unknown`] otherwise.
	///
	/// `None` for every other view, including `GROUP ALL`, whose single row has
	/// no group values.
	pub fn from_view(view: &ViewDefinition) -> Option<Self> {
		let ViewDefinition::Aggregated {
			analysis,
			..
		} = view
		else {
			return None;
		};
		let arity = analysis.group_expressions.len();
		if arity == 0 {
			return None;
		}
		let AggregateFields::Fields(items) = &analysis.fields else {
			return None;
		};
		let overlaps = |a: &Idiom, b: &Idiom| a.0.starts_with(&b.0) || b.0.starts_with(&a.0);
		let aliases: Vec<(Idiom, u32)> = items
			.iter()
			.filter(|(name, expr)| {
				!matches!(name.0.first(), Some(Part::Field(root)) if root.as_str() == "id")
					&& items.iter().all(|(other, other_expr)| {
						!overlaps(name, other) || (name == other && expr == other_expr)
					})
			})
			.filter_map(|(name, expr)| {
				group_reference(expr, arity).map(|group| (name.clone(), group as u32))
			})
			.collect();
		let components: Vec<KeyComponent> = analysis
			.group_expressions
			.iter()
			.enumerate()
			.map(|(position, expr)| KeyComponent {
				position: position as u32,
				name: None,
				kind: group_component_kind(expr),
				declared: None,
			})
			.collect();
		Some(Self {
			origin: KeyOrigin::AggregateView,
			components: components.into_boxed_slice(),
			aliases: aliases.into_boxed_slice(),
			names: None,
		})
	}

	/// The number of elements in a key.
	pub fn arity(&self) -> usize {
		self.components.len()
	}

	/// Whether `kind` is the tuple type this structure was built from: one
	/// element per component, each the component's declared type.
	pub fn declares(&self, kind: &Kind) -> bool {
		let Some(kinds) = tuple_kinds(kind) else {
			return false;
		};
		kinds.len() == self.components.len()
			&& kinds.iter().zip(self.components.iter()).all(|(k, c)| c.declared.as_ref() == Some(k))
	}

	/// How the component at `pos` orders. [`ComponentOrder::Unknown`] beyond the
	/// arity.
	pub fn order(&self, pos: u32) -> ComponentOrder {
		self.components.get(pos as usize).map_or(ComponentOrder::Unknown, |c| c.kind.order())
	}

	/// Whether every component's bytes order exactly as its values do. A key
	/// whose components are not all exact does not sort by `id` in storage order.
	pub fn all_exactly_ordered(&self) -> bool {
		self.components.iter().all(|c| c.kind.order() == ComponentOrder::Exact)
	}

	/// The name of the component at `pos`, for an `id` field.
	pub fn name_of(&self, pos: u32) -> Option<&str> {
		self.components.get(pos as usize)?.name.as_ref().map(Strand::as_str)
	}

	/// The idiom `id[position]`, which reads the element of the record's own id.
	pub fn component_idiom(position: u32) -> Idiom {
		Idiom(vec![Part::Field(Strand::new("id")), Part::index_int(i64::from(position))])
	}

	/// The position an idiom addresses: an alias of a component, or `id[i]` for
	/// an `i` below the arity. Only whole idioms match, so `city.x` and `id[0].x`
	/// address no component.
	pub fn component_of(&self, idiom: &Idiom) -> Option<u32> {
		if let Some(position) = self.index_reference(idiom) {
			return Some(position);
		}
		self.aliases.iter().find(|(alias, _)| alias == idiom).map(|(_, position)| *position)
	}

	/// `id[i]` as an element reference within the arity.
	fn index_reference(&self, idiom: &Idiom) -> Option<u32> {
		let [Part::Field(root), index] = idiom.0.as_slice() else {
			return None;
		};
		if root.as_str() != "id" {
			return None;
		}
		let position = if let Part::First = index {
			0
		} else if let Part::Value(Expr::Literal(Literal::Integer(i))) = index {
			u32::try_from(*i).ok()?
		} else {
			return None;
		};
		((position as usize) < self.arity()).then_some(position)
	}

	/// The `id[i]` idiom for a column that addresses a component by an alias or
	/// by position, or `None` for any other column. Two columns that address the
	/// same component have the same canonical column.
	pub fn canonical_column(&self, col: &Idiom) -> Option<Idiom> {
		self.component_of(col).map(Self::component_idiom)
	}

	/// Whether `key` is a record id this structure admits: an array with exactly
	/// one element per component, each a value its component can hold
	/// ([`ComponentKind::refusal`]). Reports the first fault, in position order.
	pub fn conforms(&self, key: &RecordIdKey) -> Result<(), Nonconformance> {
		let RecordIdKey::Array(array) = key else {
			return Err(Nonconformance::NotArray);
		};
		if array.0.len() != self.components.len() {
			return Err(Nonconformance::Arity {
				expected: self.components.len(),
				found: array.0.len(),
			});
		}
		for (component, value) in self.components.iter().zip(array.0.iter()) {
			if let Some(reason) = component.kind.refusal(value) {
				return Err(Nonconformance::Component {
					position: component.position,
					name: component.name.as_ref().map(|name| name.as_str().to_owned()),
					reason,
				});
			}
		}
		Ok(())
	}

	/// The virtual field definitions of the named components of `table`'s `id`,
	/// in position order. Empty for a structure that names none.
	///
	/// Each is a read-only computed field whose expression is `id[i]`, with
	/// [`FieldDefinition::key_part`] set to `i` and full select permission: the
	/// table's permissions govern a component as they govern the id. Its
	/// `AUTH LIMIT` is the inert root-owner stamp, so reading `id[i]` never
	/// narrows the reader's principal. It has no stored form.
	pub fn derived_fields(&self, table: &TableName) -> Vec<FieldDefinition> {
		self.components
			.iter()
			.filter_map(|component| {
				let name = component.name.as_ref()?;
				let computed = Expr::Idiom(Self::component_idiom(component.position));
				Some(FieldDefinition {
					name: Idiom::field(name.clone()),
					table: table.clone(),
					field_kind: component.declared.clone(),
					readonly: true,
					computed_body: Some(LogicalBody::of(&computed)),
					computed: Some(computed),
					key_part: Some(component.position),
					auth_limit: AuthLimit::new_no_limit(),
					..Default::default()
				})
			})
			.collect()
	}

	/// The components as `INFO ... STRUCTURE` reports them: a list of
	/// `{ name, position, kind }`, where `name` is omitted for a component that
	/// has none and `kind` is the type as declared.
	pub fn components_structure(&self) -> Value {
		Value::from(
			self.components
				.iter()
				.map(|component| {
					Value::from(map! {
						"name", if let Some(name) = &component.name => Value::from(name.as_str()),
						"position" => Value::from(i64::from(component.position)),
						"kind", if let Some(declared) = &component.declared => Value::from(declared.to_sql()),
					})
				})
				.collect::<Vec<Value>>(),
		)
	}
}

/// The kind of the key element a group expression writes, or
/// [`ComponentKind::Unknown`] where the expression does not fix it.
///
/// A view's rows are keyed by the values its group expressions evaluate to,
/// element for element and with no coercion on the way in, so the kind has to
/// be one every value the expression can yield holds. Only the expression
/// itself says that. A cast does: it yields a value of its target kind or
/// raises, so no row the view holds was keyed by anything else. A path to a
/// field does not: a declared field kind is enforced when a record is written,
/// so a record stored before the declaration, or before `DEFINE FIELD
/// OVERWRITE` retyped it, holds what it held, and the view row keyed from it
/// holds that.
///
/// Only a kind whose encoding orders exactly ([`ComponentOrder::Exact`]) is
/// resolved. The orders below it are read under rules a view's keys do not
/// keep: a key range over a datetime component takes the whole second its
/// bound lies in, which reaches every key of that second only for a datetime
/// within the years 0000 to 9999, and an element outside them is refused of an
/// `id` field ([`KeyStructure::conforms`]) but of no group value.
fn group_component_kind(expr: &Expr) -> ComponentKind {
	let Expr::Prefix {
		op: PrefixOperator::Cast(kind),
		..
	} = expr
	else {
		return ComponentKind::Unknown;
	};
	match kind {
		Kind::Named(_) => ComponentKind::Unknown,
		Kind::String => ComponentKind::String,
		Kind::Int => ComponentKind::Int,
		Kind::Uuid => ComponentKind::Uuid,
		Kind::Duration => ComponentKind::Duration,
		Kind::Bool => ComponentKind::Bool,
		Kind::Any
		| Kind::None
		| Kind::Null
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Float
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Number
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32
		| Kind::Object
		| Kind::Array(..)
		| Kind::Geometry(..)
		| Kind::Bytes
		| Kind::Regex
		| Kind::Table(..)
		| Kind::Range
		| Kind::Record(..)
		| Kind::Set(..)
		| Kind::Either(..)
		| Kind::Function(..)
		| Kind::Literal(..)
		| Kind::File(..)
		| Kind::Enum(..) => ComponentKind::Unknown,
	}
}

/// The group a select item's expression stands for, when it is exactly the
/// group's value: the `_g{i}` field the aggregation analysis substitutes for a
/// group expression.
fn group_reference(expr: &Expr, arity: usize) -> Option<usize> {
	let Expr::Idiom(idiom) = expr else {
		return None;
	};
	let [Part::Field(field)] = idiom.0.as_slice() else {
		return None;
	};
	let index: usize = field.as_str().strip_prefix("_g")?.parse().ok()?;
	(index < arity && group_field_name(index) == *field).then_some(index)
}

/// The field definitions of a table, with the virtual definitions of the
/// components its `id` names placed straight after `id`.
///
/// A table whose `id` names no components gets the list [`from_stored_all`]
/// builds, as the same allocation. Otherwise the result has one more definition
/// per component, each with [`FieldDefinition::key_part`] set. Every consumer of
/// the list decides what such a definition means to it: one that acts on
/// definitions (a write that stores a field, an export, a schema that is
/// declared to the user) skips them, and one that reads fields (a projection,
/// a filter, a permission) treats them as the computed fields they are.
///
/// # Errors
///
/// The error of the first stored definition that no longer compiles.
pub fn fields_from_stored(
	stored: &[StoredFieldDefinition],
) -> anyhow::Result<Arc<[FieldDefinition]>> {
	from_stored_all::<FieldDefinition>(stored).map(with_derived_fields)
}

/// `fields` with the virtual definitions of the `id` it holds, if that `id` has
/// a [`KeyStructure`] that still describes its type
/// ([`FieldDefinition::live_key`]); otherwise `fields` itself.
fn with_derived_fields(fields: Arc<[FieldDefinition]>) -> Arc<[FieldDefinition]> {
	let Some((at, key)) = fields.iter().enumerate().find_map(|(at, field)| {
		let key = field.live_key()?;
		(field.key_part.is_none() && field.name.is_id()).then(|| (at, Arc::clone(key)))
	}) else {
		return fields;
	};
	let derived = key.derived_fields(&fields[at].table);
	let mut all = Vec::with_capacity(fields.len() + derived.len());
	all.extend_from_slice(&fields[..=at]);
	all.extend(derived);
	all.extend_from_slice(&fields[at + 1..]);
	Arc::from(all)
}

/// The key structure of a table's record ids, from its definition and its field
/// list as [`fields_from_stored`] builds it.
///
/// - A table that is not a view has the structure its `id` field declares, if it names components.
/// - An aggregated view has the structure of its group expressions ([`KeyStructure::from_view`]),
///   unless the view table defines a field on a name a group is projected under, whichever way the
///   field is defined (`TYPE`, `VALUE`, `COMPUTED`, a permission): such a field changes what a read
///   of that name returns, so the name no longer says what the key holds.
/// - Everything else has none, including a table that is a view and whose `id` also names
///   components.
pub fn table_key(table: &TableDefinition, fields: &[FieldDefinition]) -> Option<Arc<KeyStructure>> {
	let id_key = fields.iter().find(|field| field.name.is_id()).and_then(FieldDefinition::live_key);
	match (&table.view, id_key) {
		(None, key) => key.cloned(),
		(Some(_), Some(_)) => None,
		(Some(view), None) => {
			let key = KeyStructure::from_view(view)?;
			let shadowed = fields.iter().any(|field| {
				root_name(&field.name).is_some_and(|root| {
					key.aliases.iter().any(|(alias, _)| root_name(alias) == Some(root))
				})
			});
			(!shadowed).then(|| Arc::new(key))
		}
	}
}

#[cfg(test)]
mod tests {
	use chrono::{TimeZone, Timelike, Utc};

	use super::*;
	use crate::catalog::view_to_definition;
	use crate::expr::{Group, Groups, View};
	use crate::val::{Array, Datetime, Duration, RecordId, Uuid};

	fn strands(list: &[&str]) -> Vec<Strand> {
		list.iter().map(Strand::new).collect()
	}

	fn names(list: &[&str]) -> KeyNames {
		KeyNames::new(strands(list), list.len()).unwrap()
	}

	fn tuple(kinds: Vec<Kind>) -> Kind {
		Kind::Literal(KindLiteral::Array(kinds))
	}

	fn reading() -> KeyStructure {
		KeyStructure::from_id_field(
			&tuple(vec![Kind::String, Kind::Datetime]),
			&names(&["city", "time"]),
		)
		.unwrap()
	}

	fn id_key(values: Vec<Value>) -> RecordIdKey {
		RecordIdKey::Array(Array(values))
	}

	/// A datetime on the first of January of `year`, `second` seconds into the
	/// day and `nanos` nanoseconds after that.
	fn at(year: i32, second: u32, nanos: u32) -> Value {
		let whole = Utc.with_ymd_and_hms(year, 1, 1, 0, 0, second).single().unwrap();
		Value::Datetime(Datetime(whole.with_nanosecond(nanos).unwrap()))
	}

	/// What a component type may be: strings, ints, datetimes, uuids, durations,
	/// bools and records. Everything else is refused with the component named.
	#[test]
	fn the_allow_list_for_a_declared_component_type() {
		let record = Kind::Record(vec![TableName::from("person")]);
		let accepted = [
			(Kind::String, ComponentKind::String),
			(Kind::Int, ComponentKind::Int),
			(Kind::Datetime, ComponentKind::Datetime),
			(Kind::Uuid, ComponentKind::Uuid),
			(Kind::Duration, ComponentKind::Duration),
			(Kind::Bool, ComponentKind::Bool),
			(record, ComponentKind::Record(vec![TableName::from("person")].into())),
			(Kind::Record(vec![]), ComponentKind::Record(Box::default())),
		];
		for (kind, expected) in accepted {
			let key = KeyStructure::from_id_field(&tuple(vec![kind.clone()]), &names(&["c"]))
				.unwrap_or_else(|e| panic!("{} was refused: {e}", kind.to_sql()));
			assert_eq!(key.components[0].kind, expected, "{}", kind.to_sql());
			assert_eq!(key.components[0].declared.as_ref(), Some(&kind));
		}

		let refused = [
			Kind::Float,
			Kind::Decimal,
			Kind::Number,
			Kind::Object,
			Kind::Bytes,
			Kind::Geometry(vec![]),
			Kind::Any,
			Kind::None,
			Kind::Array(Box::new(Kind::Int), None),
			Kind::Set(Box::new(Kind::Int), None),
			tuple(vec![Kind::Int]),
			Kind::Either(vec![Kind::String, Kind::None]),
			Kind::Either(vec![Kind::String, Kind::Int]),
			Kind::Literal(KindLiteral::String(Strand::new("a"))),
			Kind::Literal(KindLiteral::Integer(1)),
			Kind::Literal(KindLiteral::Bool(true)),
			Kind::Range,
			Kind::Regex,
			Kind::Table(vec![]),
		];
		for kind in refused {
			let err = KeyStructure::from_id_field(
				&tuple(vec![Kind::String, kind.clone()]),
				&names(&["a", "b"]),
			)
			.expect_err(&kind.to_sql());
			assert_eq!(
				err,
				KeyStructureError::UnsupportedKind {
					name: "b".to_owned(),
					kind: kind.to_sql()
				},
				"{}",
				kind.to_sql()
			);
		}
	}

	/// The first unsupported component is the one reported, in position order.
	#[test]
	fn the_first_unsupported_component_is_reported() {
		let err = KeyStructure::from_id_field(
			&tuple(vec![Kind::Float, Kind::Number]),
			&names(&["a", "b"]),
		)
		.unwrap_err();
		assert_eq!(
			err,
			KeyStructureError::UnsupportedKind {
				name: "a".to_owned(),
				kind: "float".to_owned()
			}
		);
		assert_eq!(err.to_string(), "the record id component `a` cannot have the type `float`");
	}

	/// A component name is accepted exactly when the raw rendering of the field
	/// it derives is the name itself: keywords are plain, anything the rendering
	/// escapes is refused.
	#[test]
	fn a_component_name_is_a_plain_identifier() {
		for name in
			["city", "_", "_x", "a1", "A_b_2", "time", "value", "select", "from", "none", "true"]
		{
			assert!(KeyStructure::is_plain_name(name), "{name}");
			assert_eq!(Idiom::field(Strand::new(name)).to_raw_string(), name);
			KeyStructure::from_id_field(&tuple(vec![Kind::String]), &names(&[name]))
				.unwrap_or_else(|e| panic!("{name}: {e}"));
		}
		for name in
			["the city", "a-b", "caf\u{e9}", "1x", "9", "a.b", "a`b", "", " a", "NaN", "Infinity"]
		{
			assert!(!KeyStructure::is_plain_name(name), "{name:?}");
			assert_ne!(Idiom::field(Strand::new(name)).to_raw_string(), name);
			if name.is_empty() {
				continue;
			}
			assert_eq!(
				KeyStructure::from_id_field(&tuple(vec![Kind::String]), &names(&[name])),
				Err(KeyStructureError::NotPlainName(name.to_owned())),
				"{name:?}"
			);
		}
		// Position order, and the name before the type at one position.
		assert_eq!(
			KeyStructure::from_id_field(
				&tuple(vec![Kind::Float, Kind::String]),
				&names(&["a", "b c"])
			),
			Err(KeyStructureError::UnsupportedKind {
				name: "a".to_owned(),
				kind: "float".to_owned()
			})
		);
		assert_eq!(
			KeyStructure::from_id_field(&tuple(vec![Kind::Float]), &names(&["b c"])),
			Err(KeyStructureError::NotPlainName("b c".to_owned()))
		);
		// Reading the catalog never refuses a name.
		assert!(
			KeyStructure::from_stored_id_field(&tuple(vec![Kind::String]), &names(&["b c"]))
				.is_some()
		);
	}

	#[test]
	fn a_type_that_is_not_a_tuple_of_the_names_is_refused() {
		for kind in [Kind::String, Kind::Array(Box::new(Kind::String), None), Kind::Any] {
			assert_eq!(
				KeyStructure::from_id_field(&kind, &names(&["a"])),
				Err(KeyStructureError::NotATuple),
				"{}",
				kind.to_sql()
			);
			assert_eq!(KeyStructure::from_stored_id_field(&kind, &names(&["a"])), None);
		}
		let kind = tuple(vec![Kind::String, Kind::Int]);
		assert_eq!(
			KeyStructure::from_id_field(&kind, &names(&["a"])),
			Err(KeyStructureError::Names(KeyNamesError::Arity {
				names: 1,
				kinds: 2
			}))
		);
		assert_eq!(KeyStructure::from_stored_id_field(&kind, &names(&["a"])), None);
	}

	/// A stored type that the allow-list no longer admits, or never did, still
	/// gives a structure: the component's order is unknown, which only declines key ranges.
	#[test]
	fn a_stored_type_outside_the_allow_list_gives_an_unknown_component() {
		let kind = tuple(vec![Kind::Float, Kind::Datetime, Kind::Any]);
		let key = KeyStructure::from_stored_id_field(&kind, &names(&["a", "b", "c"])).unwrap();
		assert_eq!(key.origin, KeyOrigin::IdField);
		assert_eq!(key.components[0].kind, ComponentKind::Unknown);
		assert_eq!(key.components[0].declared, Some(Kind::Float));
		assert_eq!(key.order(0), ComponentOrder::Unknown);
		assert_eq!(key.order(1), ComponentOrder::CoarseSecond);
		assert_eq!(key.order(2), ComponentOrder::Unknown);
		assert!(!key.all_exactly_ordered());
		// The strict constructor refuses the same type.
		assert!(KeyStructure::from_id_field(&kind, &names(&["a", "b", "c"])).is_err());
	}

	#[test]
	fn a_structure_declares_the_tuple_it_was_built_from() {
		let key = reading();
		assert!(key.declares(&tuple(vec![Kind::String, Kind::Datetime])));
		assert!(!key.declares(&tuple(vec![Kind::String, Kind::Int])));
		assert!(!key.declares(&tuple(vec![Kind::String])));
		assert!(!key.declares(&tuple(vec![Kind::String, Kind::Datetime, Kind::Int])));
		assert!(!key.declares(&Kind::String));
	}

	#[test]
	fn orders_follow_the_component_kinds() {
		let key = KeyStructure::from_id_field(
			&tuple(vec![
				Kind::String,
				Kind::Int,
				Kind::Datetime,
				Kind::Uuid,
				Kind::Duration,
				Kind::Bool,
				Kind::Record(vec![]),
			]),
			&names(&["s", "i", "d", "u", "du", "b", "r"]),
		)
		.unwrap();
		let orders: Vec<ComponentOrder> = (0..7).map(|p| key.order(p)).collect();
		assert_eq!(
			orders,
			[
				ComponentOrder::Exact,
				ComponentOrder::Exact,
				ComponentOrder::CoarseSecond,
				ComponentOrder::Exact,
				ComponentOrder::Exact,
				ComponentOrder::Exact,
				ComponentOrder::EqualityOnly,
			]
		);
		assert_eq!(key.order(7), ComponentOrder::Unknown);
		assert!(!key.all_exactly_ordered());

		let exact = KeyStructure::from_id_field(
			&tuple(vec![Kind::String, Kind::Int, Kind::Uuid, Kind::Duration, Kind::Bool]),
			&names(&["a", "b", "c", "d", "e"]),
		)
		.unwrap();
		assert!(exact.all_exactly_ordered(), "an int component is exact");

		// One component that is not exact is enough.
		for kind in [Kind::Datetime, Kind::Record(vec![])] {
			let mixed = KeyStructure::from_id_field(
				&tuple(vec![Kind::Int, kind.clone()]),
				&names(&["a", "b"]),
			)
			.unwrap();
			assert!(!mixed.all_exactly_ordered(), "{}", kind.to_sql());
		}
	}

	#[test]
	fn names_address_components_by_alias_or_position() {
		let key = reading();
		assert_eq!(key.arity(), 2);
		assert_eq!(key.name_of(0), Some("city"));
		assert_eq!(key.name_of(1), Some("time"));
		assert_eq!(key.name_of(2), None);

		let id = |i: i64| Idiom(vec![Part::Field(Strand::new("id")), Part::index_int(i)]);
		assert_eq!(key.component_of(&Idiom::field("city")), Some(0));
		assert_eq!(key.component_of(&Idiom::field("time")), Some(1));
		assert_eq!(key.component_of(&id(0)), Some(0));
		assert_eq!(key.component_of(&id(1)), Some(1));
		assert_eq!(
			key.component_of(&Idiom(vec![Part::Field(Strand::new("id")), Part::First])),
			Some(0)
		);

		// Beyond the arity, negative, not a literal index and not a whole component.
		assert_eq!(key.component_of(&id(2)), None);
		assert_eq!(key.component_of(&id(-1)), None);
		assert_eq!(
			key.component_of(&Idiom(vec![Part::Field(Strand::new("id")), Part::Last])),
			None
		);
		assert_eq!(key.component_of(&Idiom::field("id")), None);
		assert_eq!(key.component_of(&Idiom::field("value")), None);
		assert_eq!(
			key.component_of(&Idiom::field("city").push(Part::Field(Strand::new("x")))),
			None
		);
		assert_eq!(key.component_of(&id(0).push(Part::Field(Strand::new("x")))), None);
		assert_eq!(
			key.component_of(&Idiom(vec![Part::Field(Strand::new("other")), Part::index_int(0)])),
			None
		);
	}

	#[test]
	fn canonical_columns_map_aliases_and_positions_to_the_same_idiom() {
		let key = reading();
		let id1 = KeyStructure::component_idiom(1);
		assert_eq!(key.canonical_column(&Idiom::field("time")), Some(id1.clone()));
		assert_eq!(key.canonical_column(&id1), Some(id1));
		assert_eq!(key.canonical_column(&Idiom::field("value")), None);
		assert_eq!(KeyStructure::component_idiom(0).to_sql(), "id[0]");
	}

	/// The checks run in order: array, arity, then each element by position.
	#[test]
	fn conforms_reports_each_failure_class() {
		let key = reading();
		let ok = id_key(vec![Value::from("London"), at(2025, 10, 500_000_000)]);
		assert_eq!(key.conforms(&ok), Ok(()));
		// Extremes of the datetime range.
		for edge in [at(0, 0, 0), at(9999, 59, 999_999_999)] {
			assert_eq!(key.conforms(&id_key(vec![Value::from("a"), edge])), Ok(()));
		}

		assert_eq!(key.conforms(&RecordIdKey::String("a".into())), Err(Nonconformance::NotArray));
		assert_eq!(key.conforms(&RecordIdKey::Number(1)), Err(Nonconformance::NotArray));
		assert_eq!(
			key.conforms(&id_key(vec![Value::from("a")])),
			Err(Nonconformance::Arity {
				expected: 2,
				found: 1
			})
		);
		assert_eq!(
			key.conforms(&id_key(vec![Value::from("a"), at(2025, 0, 0), Value::from(1)])),
			Err(Nonconformance::Arity {
				expected: 2,
				found: 3
			})
		);
		assert_eq!(
			key.conforms(&id_key(vec![Value::from("a"), Value::from("not a date")])),
			Err(Nonconformance::Component {
				position: 1,
				name: Some("time".to_owned()),
				reason: "is not a datetime"
			})
		);
		assert_eq!(
			key.conforms(&id_key(vec![Value::from(1), Value::from("not a date")])),
			Err(Nonconformance::Component {
				position: 0,
				name: Some("city".to_owned()),
				reason: "is not a string"
			}),
			"the first failing element is reported"
		);
		// Years outside 0000 to 9999 have no fixed-width key prefix.
		for (year, value) in [(10000, at(10000, 0, 0)), (-1, at(-1, 0, 0))] {
			assert_eq!(
				key.conforms(&id_key(vec![Value::from("a"), value])),
				Err(Nonconformance::Component {
					position: 1,
					name: Some("time".to_owned()),
					reason: "is a datetime outside the years 0000 to 9999"
				}),
				"year {year}"
			);
		}
		// A leap second renders as second 60, which is not a year problem and says so.
		let leap = Value::Datetime(Datetime(
			Utc.timestamp_opt(1_483_228_799, 1_500_000_000).single().unwrap(),
		));
		assert_eq!(
			key.conforms(&id_key(vec![Value::from("a"), leap])),
			Err(Nonconformance::Component {
				position: 1,
				name: Some("time".to_owned()),
				reason: "is a leap second, which a record id cannot hold"
			})
		);
		assert_eq!(
			Nonconformance::Component {
				position: 1,
				name: Some("time".to_owned()),
				reason: "is not a datetime"
			}
			.to_string(),
			"`time` is not a datetime (element 1)"
		);
	}

	#[test]
	fn an_arity_reason_counts_in_the_singular_and_the_plural() {
		for (expected, found, text) in [
			(2, 1, "the record id has 1 element, but 2 are expected"),
			(1, 2, "the record id has 2 elements, but 1 is expected"),
			(3, 2, "the record id has 2 elements, but 3 are expected"),
			(1, 0, "the record id has 0 elements, but 1 is expected"),
		] {
			assert_eq!(
				Nonconformance::Arity {
					expected,
					found
				}
				.to_string(),
				text
			);
		}
	}

	#[test]
	fn conforms_checks_every_component_kind() {
		let key = KeyStructure::from_id_field(
			&tuple(vec![
				Kind::Int,
				Kind::Uuid,
				Kind::Duration,
				Kind::Bool,
				Kind::Record(vec![TableName::from("person")]),
			]),
			&names(&["i", "u", "d", "b", "r"]),
		)
		.unwrap();
		let person =
			Value::RecordId(RecordId::new(TableName::from("person"), String::from("tobie")));
		let uuid = Value::Uuid(Uuid::default());
		let duration = Value::Duration(Duration::default());
		let good = vec![Value::from(7), uuid, duration, Value::Bool(true), person.clone()];
		assert_eq!(key.conforms(&id_key(good.clone())), Ok(()));

		let cases: [(usize, Value, &str); 5] = [
			(0, Value::from("7"), "is not an int"),
			(1, Value::from("x"), "is not a uuid"),
			(2, Value::from(1), "is not a duration"),
			(3, Value::from(1), "is not a bool"),
			(4, Value::from("person:tobie"), "is not a record"),
		];
		for (at, bad, reason) in cases {
			let mut values = good.clone();
			values[at] = bad;
			assert_eq!(
				key.conforms(&id_key(values)),
				Err(Nonconformance::Component {
					position: at as u32,
					name: Some(["i", "u", "d", "b", "r"][at].to_owned()),
					reason
				})
			);
		}
		let mut other = good;
		other[4] = Value::RecordId(RecordId::new(TableName::from("company"), String::from("acme")));
		assert_eq!(
			key.conforms(&id_key(other)),
			Err(Nonconformance::Component {
				position: 4,
				name: Some("r".to_owned()),
				reason: "is a record of another table"
			})
		);

		// An untyped record component admits a record of any table.
		let any = KeyStructure::from_id_field(&tuple(vec![Kind::Record(vec![])]), &names(&["r"]))
			.unwrap();
		assert_eq!(any.conforms(&id_key(vec![person])), Ok(()));
	}

	/// An int component holds only a `Number::Int`. A float or a decimal is refused
	/// even when it equals an integer, because its stored encoding is not that of
	/// the integer: a range over the component would not reach it, and it would sort
	/// out of order against the elements that follow it.
	#[test]
	fn an_int_component_holds_only_ints() {
		let key = KeyStructure::from_id_field(&tuple(vec![Kind::Int]), &names(&["n"])).unwrap();
		let conform = |n: Number| key.conforms(&id_key(vec![Value::Number(n)]));
		let dec = |s: &str| Number::Decimal(s.parse().unwrap());

		for ok in [Number::Int(0), Number::Int(5), Number::Int(i64::MIN), Number::Int(i64::MAX)] {
			assert_eq!(conform(ok.clone()), Ok(()), "{ok:?}");
		}

		let float = "is a float; an int component holds only ints";
		let decimal = "is a decimal; an int component holds only ints";
		let not_an_int = "is not an int";
		for (bad, reason) in [
			// A whole number written as a float or a decimal, in every form.
			(Number::Float(5.0), float),
			(Number::Float(-0.0), float),
			(Number::Float(i64::MIN as f64), float),
			(dec("5"), decimal),
			(dec("0.00000"), decimal),
			(dec("5.00"), decimal),
			// Anything that is not a whole number that fits an i64.
			(Number::Float(5.5), not_an_int),
			(Number::Float(1e30), not_an_int),
			(Number::Float(i64::MAX as f64), not_an_int),
			(Number::Float(f64::NAN), not_an_int),
			(Number::Float(f64::INFINITY), not_an_int),
			(Number::Float(f64::NEG_INFINITY), not_an_int),
			(dec("5.5"), not_an_int),
			(dec("100000000000000000000"), not_an_int),
		] {
			assert_eq!(
				conform(bad.clone()),
				Err(Nonconformance::Component {
					position: 0,
					name: Some("n".to_owned()),
					reason
				}),
				"{bad:?}"
			);
		}
	}

	/// A component of unknown kind holds any value, so conformance holds its
	/// arity and nothing else.
	#[test]
	fn an_unknown_component_accepts_any_value() {
		let key = KeyStructure::from_stored_id_field(
			&tuple(vec![Kind::Float, Kind::String]),
			&names(&["a", "b"]),
		)
		.unwrap();
		assert_eq!(key.conforms(&id_key(vec![Value::from(1.5), Value::from("x")])), Ok(()));
		assert_eq!(
			key.conforms(&id_key(vec![Value::from("x"), Value::from(1)])),
			Err(Nonconformance::Component {
				position: 1,
				name: Some("b".to_owned()),
				reason: "is not a string"
			})
		);
	}

	#[test]
	fn derived_fields_are_read_only_computed_reads_of_the_id() {
		let table = TableName::from("reading");
		let derived = reading().derived_fields(&table);
		assert_eq!(derived.len(), 2);

		for (position, (name, kind)) in
			[("city", Kind::String), ("time", Kind::Datetime)].into_iter().enumerate()
		{
			let def = &derived[position];
			assert_eq!(def.name, Idiom::field(name));
			assert_eq!(def.table, table);
			assert_eq!(def.field_kind, Some(kind));
			assert_eq!(def.key_part, Some(position as u32));
			assert!(def.key.is_none());
			assert!(def.readonly);
			assert_eq!(def.select_permission, crate::catalog::Permission::Full);
			assert!(def.value.is_none() && def.assert.is_none());
			assert_eq!(def.default, crate::catalog::DefineDefault::None);
			assert_eq!(
				def.auth_limit,
				AuthLimit::new_no_limit(),
				"reading `id[i]` narrows nothing"
			);

			let computed = def.computed.as_ref().expect("a computed expression");
			assert_eq!(computed.to_sql(), format!("id[{position}]"));
			let body = def.computed_body.as_ref().expect("a computed body");
			assert_eq!(body.text().as_str(), format!("id[{position}]"));
			assert!(body.logical().is_ok(), "the body compiles to a logical scalar");
		}

		// A structure that names nothing has nothing to derive.
		let view = aggregated("a, count() AS n", &["a"]);
		assert!(KeyStructure::from_view(&view).unwrap().derived_fields(&table).is_empty());
	}

	fn aggregated(fields: &str, groups: &[&str]) -> ViewDefinition {
		let view = View {
			expr: crate::syn::fields_for_definition(fields).unwrap(),
			what: vec![TableName::from("t")],
			cond: None,
			group: Some(Groups(
				groups.iter().map(|g| Group(g.parse::<Idiom>().unwrap())).collect(),
			)),
		};
		view_to_definition(&view).unwrap()
	}

	fn alias(key: &KeyStructure, name: &str) -> Option<u32> {
		key.component_of(&name.parse::<Idiom>().unwrap())
	}

	#[test]
	fn a_view_key_is_addressed_by_the_names_its_groups_are_projected_under() {
		let key =
			KeyStructure::from_view(&aggregated("a, b AS renamed, count() AS n", &["a", "b"]))
				.unwrap();
		assert_eq!(key.origin, KeyOrigin::AggregateView);
		assert_eq!(key.arity(), 2);
		assert!(key.names.is_none());
		assert!(key.components.iter().all(|c| c.name.is_none() && c.declared.is_none()));
		assert_eq!(key.order(0), ComponentOrder::Unknown);
		assert!(!key.all_exactly_ordered());

		assert_eq!(alias(&key, "a"), Some(0));
		assert_eq!(alias(&key, "renamed"), Some(1));
		// `b` is projected as `renamed`, the count is not a group, and `id[i]` always addresses.
		assert_eq!(alias(&key, "b"), None);
		assert_eq!(alias(&key, "n"), None);
		assert_eq!(alias(&key, "id[1]"), Some(1));
		assert_eq!(
			key.canonical_column(&Idiom::field("renamed")),
			Some(KeyStructure::component_idiom(1))
		);
	}

	/// What a group expression fixes about its key element. A cast yields a
	/// value of its target kind or raises, so a component cast to a kind that
	/// orders exactly is that kind; everything else — a field read, whose
	/// declared kind binds only the writes that were checked against it, a
	/// call, a cast to a kind a record id cannot hold — is unknown. A cast to
	/// a datetime is unknown too: a group value may be a datetime outside the
	/// years 0000 to 9999, which an `id` field admits at no component.
	#[test]
	fn a_views_group_expression_fixes_its_component_kind_only_by_a_cast() {
		let exact = [
			("<string> a", ComponentKind::String),
			("<int> a", ComponentKind::Int),
			("<uuid> a", ComponentKind::Uuid),
			("<duration> a", ComponentKind::Duration),
			("<bool> a", ComponentKind::Bool),
		];
		for (expr, expected) in exact {
			let key =
				KeyStructure::from_view(&aggregated(&format!("{expr} AS k, count() AS n"), &["k"]))
					.unwrap();
			assert_eq!(key.components[0].kind, expected, "{expr}");
			assert_eq!(key.order(0), ComponentOrder::Exact, "{expr}");
			assert!(key.all_exactly_ordered(), "{expr}");
			// The kind is known; the type the view declares for the column is not.
			assert!(key.components[0].declared.is_none(), "{expr}");
		}

		let unknown = [
			"a",
			"a.b",
			"time::floor(a, 1h)",
			"<datetime> a",
			"<record<t>> a",
			"<float> a",
			"<number> a",
			"<any> a",
			"<option<string>> a",
		];
		for expr in unknown {
			let key =
				KeyStructure::from_view(&aggregated(&format!("{expr} AS k, count() AS n"), &["k"]))
					.unwrap();
			assert_eq!(key.components[0].kind, ComponentKind::Unknown, "{expr}");
			assert!(!key.all_exactly_ordered(), "{expr}");
		}
	}

	/// One unknown component is enough to take the exact order off the whole
	/// key, and a component's kind follows its own group.
	#[test]
	fn a_views_components_are_judged_one_by_one() {
		let key = KeyStructure::from_view(&aggregated(
			"<string> a AS s, b AS plain, <int> c AS i, count() AS n",
			&["s", "plain", "i"],
		))
		.unwrap();
		assert_eq!(key.components[0].kind, ComponentKind::String);
		assert_eq!(key.components[1].kind, ComponentKind::Unknown);
		assert_eq!(key.components[2].kind, ComponentKind::Int);
		assert!(!key.all_exactly_ordered());
	}

	#[test]
	fn a_view_group_expression_is_addressed_by_its_alias() {
		let key = KeyStructure::from_view(&aggregated(
			"time::floor(ts, 1h) AS bucket, count() AS n",
			&["bucket"],
		))
		.unwrap();
		assert_eq!(key.arity(), 1);
		assert_eq!(alias(&key, "bucket"), Some(0));
		assert_eq!(alias(&key, "ts"), None);
	}

	/// A group that is a multi-part path is addressed by the whole path, and
	/// by nothing shorter.
	#[test]
	fn a_view_group_that_is_a_path_is_addressed_by_the_whole_path() {
		let key = KeyStructure::from_view(&aggregated("a.b, count() AS n", &["a.b"])).unwrap();
		assert_eq!(key.arity(), 1);
		assert_eq!(alias(&key, "a.b"), Some(0));
		assert_eq!(alias(&key, "a"), None);
		assert_eq!(alias(&key, "a.b.c"), None);
	}

	/// A group that is not projected has a position and no name; one projected
	/// twice has two.
	#[test]
	fn a_view_group_that_is_not_projected_has_no_alias() {
		let key = KeyStructure::from_view(&aggregated("count() AS n", &["a"])).unwrap();
		assert_eq!(key.arity(), 1);
		assert!(key.aliases.is_empty());
		assert_eq!(alias(&key, "a"), None);
		assert_eq!(alias(&key, "id[0]"), Some(0));

		let twice =
			KeyStructure::from_view(&aggregated("a, a AS again, count() AS n", &["a"])).unwrap();
		assert_eq!(alias(&twice, "a"), Some(0));
		assert_eq!(alias(&twice, "again"), Some(0));
	}

	/// A name another item also writes to holds what the last of them wrote, so
	/// no item under such a name addresses a group; nor does a name that reads
	/// the row's record id.
	#[test]
	fn a_view_name_another_item_writes_to_is_no_alias() {
		let groups: &[&str] = &["a", "b"];
		for (fields, groups) in [
			("a AS x, b AS x, count() AS n", groups),
			("a AS x, b AS x.y, count() AS n", groups),
			("b AS x.y, a AS x, count() AS n", groups),
			("a AS x, { y: 1 } AS x, count() AS n", &["a"][..]),
			("a AS x, count() AS x.y", &["a"][..]),
		] {
			let key = KeyStructure::from_view(&aggregated(fields, groups)).unwrap();
			assert_eq!(alias(&key, "x"), None, "{fields}");
			assert_eq!(alias(&key, "x.y"), None, "{fields}");
			assert_eq!(alias(&key, "id[0]"), Some(0), "{fields}");
		}
		// Names that only share a leading part of a field name do not overlap,
		// and an item repeated exactly is one alias.
		let key =
			KeyStructure::from_view(&aggregated("a AS x, b AS xy, a AS x, count() AS n", groups))
				.unwrap();
		assert_eq!(alias(&key, "x"), Some(0));
		assert_eq!(alias(&key, "xy"), Some(1));
		// Siblings beneath one object are separate places.
		let key = KeyStructure::from_view(&aggregated("a AS o.p, b AS o.q, count() AS n", groups))
			.unwrap();
		assert_eq!(alias(&key, "o.p"), Some(0));
		assert_eq!(alias(&key, "o.q"), Some(1));

		for fields in ["a AS id, b, count() AS n", "a AS id.x, b, count() AS n"] {
			let key = KeyStructure::from_view(&aggregated(fields, groups)).unwrap();
			assert_eq!(alias(&key, "id"), None, "{fields}");
			assert_eq!(alias(&key, "id.x"), None, "{fields}");
			assert_eq!(alias(&key, "b"), Some(1), "{fields}");
		}
	}

	/// An item that is computed from a group is not the group.
	#[test]
	fn a_view_select_item_that_computes_from_a_group_is_not_an_alias() {
		let key =
			KeyStructure::from_view(&aggregated("a + 1 AS bumped, count() AS n", &["a"])).unwrap();
		assert!(key.aliases.is_empty());
	}

	#[test]
	fn only_an_aggregated_view_with_groups_has_a_view_key() {
		// GROUP ALL has no group values to key the one row by.
		let all = View {
			expr: crate::syn::fields_for_definition("count() AS n").unwrap(),
			what: vec![TableName::from("t")],
			cond: None,
			group: Some(Groups(Vec::new())),
		};
		assert_eq!(KeyStructure::from_view(&view_to_definition(&all).unwrap()), None);

		// No GROUP clause: a materialized view keyed by its source records.
		let plain = View {
			expr: crate::syn::fields_for_definition("a").unwrap(),
			what: vec![TableName::from("t")],
			cond: None,
			group: None,
		};
		assert_eq!(KeyStructure::from_view(&view_to_definition(&plain).unwrap()), None);
	}

	fn table(view: Option<ViewDefinition>) -> TableDefinition {
		use crate::catalog::{DatabaseId, LogicalPermissions, NamespaceId, TableId, TableType};
		let permissions = crate::catalog::Permissions::default();
		TableDefinition {
			namespace_id: NamespaceId(1),
			database_id: DatabaseId(1),
			table_id: TableId(1),
			name: TableName::from("v"),
			drop: false,
			schemafull: false,
			view,
			permissions_body: LogicalPermissions::of(&permissions),
			permissions,
			table_type: TableType::Any,
			changefeed: None,
			comment: None,
			cache_fields_ts: uuid::Uuid::nil(),
			cache_events_ts: uuid::Uuid::nil(),
			cache_tables_ts: uuid::Uuid::nil(),
			cache_indexes_ts: uuid::Uuid::nil(),
			cache_lives_ts: uuid::Uuid::nil(),
			graphql_alias: None,
			graphql_deprecated: None,
			graph_folded: false,
			graph_doc_ids: false,
			inline_edges_cap: None,
			inline_refs_cap: None,
			graph_inline_gen: 0,
		}
	}

	fn field(name: &str) -> FieldDefinition {
		FieldDefinition {
			name: name.parse().unwrap(),
			table: TableName::from("v"),
			..Default::default()
		}
	}

	fn named_id() -> FieldDefinition {
		let mut id = field("id");
		id.set_type(
			Some(tuple(vec![Kind::String, Kind::Datetime])),
			Some(names(&["city", "time"])),
		);
		id
	}

	#[test]
	fn table_key_reads_the_id_of_a_table_that_is_not_a_view() {
		let named = named_id();
		let key = table_key(&table(None), &[field("other"), named.clone()]).expect("a key");
		assert_eq!(key.origin, KeyOrigin::IdField);
		assert!(Arc::ptr_eq(&key, named.key.as_ref().unwrap()));

		assert_eq!(table_key(&table(None), &[]), None);
		assert_eq!(table_key(&table(None), &[field("id"), field("other")]), None);
	}

	#[test]
	fn table_key_reads_the_groups_of_an_aggregated_view() {
		let view = aggregated("a, b, count() AS n", &["a", "b"]);
		let key = table_key(&table(Some(view)), &[]).expect("a key");
		assert_eq!(key.origin, KeyOrigin::AggregateView);
		assert_eq!(key.arity(), 2);

		// A view with no groups, or one that is not aggregated, has none.
		let all = View {
			expr: crate::syn::fields_for_definition("count() AS n").unwrap(),
			what: vec![TableName::from("t")],
			cond: None,
			group: Some(Groups(Vec::new())),
		};
		assert_eq!(table_key(&table(Some(view_to_definition(&all).unwrap())), &[]), None);
	}

	/// A field of the view table on a name a group is projected under changes
	/// what a read of that name returns, so the name stops saying what the key
	/// holds. Every way of defining the field does that, and so does a field
	/// below the name.
	#[test]
	fn table_key_declines_a_view_with_a_field_on_a_group_alias() {
		let view = aggregated("a, b.c, count() AS n", &["a", "b.c"]);
		let with = |name: &str| table_key(&table(Some(view.clone())), &[field(name)]);
		assert!(with("n").is_some(), "a field on the aggregate is not on a group");
		assert!(with("other").is_some());
		for name in ["a", "a.x", "a[*]", "b", "b.c", "b.d"] {
			assert_eq!(with(name), None, "{name}");
		}
		// The key is judged against the whole list.
		assert_eq!(table_key(&table(Some(view)), &[field("n"), field("a.x")]), None);
	}

	/// A table cannot be both a view and have a named id; neither key is used.
	#[test]
	fn table_key_declines_a_view_whose_id_also_names_components() {
		let view = aggregated("a, count() AS n", &["a"]);
		assert_eq!(table_key(&table(Some(view)), &[named_id()]), None);
	}

	/// A type written to `field_kind` directly leaves the structure stale. The
	/// table's key and its field list act on the structure only while it still
	/// describes the type: a stale one names nothing and derives nothing.
	#[test]
	fn a_stale_structure_is_never_acted_on() {
		let mut stale = named_id();
		stale.field_kind = Some(tuple(vec![Kind::String, Kind::Int]));
		assert!(stale.key.is_some(), "the structure is stale, not removed");
		assert!(stale.live_key().is_none());

		assert_eq!(table_key(&table(None), std::slice::from_ref(&stale)), None);
		let all: Arc<[FieldDefinition]> = Arc::from(vec![stale, field("value")]);
		let same = with_derived_fields(Arc::clone(&all));
		assert!(
			Arc::ptr_eq(&all, &same),
			"no virtual definition is derived from a stale structure"
		);

		// The same definition with a type that agrees with its structure is acted on.
		let live = named_id();
		assert!(live.live_key().is_some());
		assert!(table_key(&table(None), std::slice::from_ref(&live)).is_some());
		let derived = with_derived_fields(Arc::from(vec![live, field("value")]));
		assert_eq!(derived.iter().filter(|f| f.key_part.is_some()).count(), 2);
	}

	fn stored(fields: &[FieldDefinition]) -> Vec<StoredFieldDefinition> {
		fields.iter().map(FieldDefinition::to_stored).collect()
	}

	/// A table that names nothing keeps the list as it was built, untouched.
	#[test]
	fn fields_without_names_are_not_copied() {
		let all: Arc<[FieldDefinition]> =
			Arc::from(vec![field("id"), field("value"), field("other")]);
		let same = with_derived_fields(Arc::clone(&all));
		assert!(Arc::ptr_eq(&all, &same));

		let from_stored = fields_from_stored(&stored(&all)).unwrap();
		assert_eq!(*from_stored, *all);
		assert!(from_stored.iter().all(|f| f.key.is_none() && f.key_part.is_none()));
	}

	/// The virtual definitions sit straight after `id`, whatever order the
	/// stored definitions come in.
	#[test]
	fn fields_with_names_gain_the_virtual_definitions_after_id() {
		let list = [field("alpha"), named_id(), field("value"), field("zeta")];
		let fields = fields_from_stored(&stored(&list)).unwrap();
		let order: Vec<String> = fields.iter().map(|f| f.name.to_raw_string()).collect();
		assert_eq!(order, ["alpha", "id", "city", "time", "value", "zeta"]);

		let id = &fields[1];
		assert_eq!(id.key_part, None);
		assert_eq!(id.key.as_deref(), Some(&reading()));
		assert_eq!(fields[2].key_part, Some(0));
		assert_eq!(fields[3].key_part, Some(1));
		assert!(
			fields.iter().enumerate().all(|(at, f)| (2..=3).contains(&at) == f.key_part.is_some())
		);

		// The structure the funnel produces is the one the table key reads.
		let key = table_key(&table(None), &fields).unwrap();
		assert_eq!(*key, reading());
	}

	#[test]
	fn the_id_comes_first_or_last_without_losing_a_neighbour() {
		for list in [vec![named_id()], vec![named_id(), field("z")], vec![field("a"), named_id()]] {
			let fields = fields_from_stored(&stored(&list)).unwrap();
			assert_eq!(fields.len(), list.len() + 2);
			let non_virtual: Vec<&FieldDefinition> =
				fields.iter().filter(|f| f.key_part.is_none()).collect();
			assert_eq!(non_virtual.len(), list.len());
		}
	}

	/// The lines of a corpus file in `testdata/`, without comments.
	fn corpus(text: &'static str) -> Vec<&'static str> {
		text.lines().filter(|line| !line.is_empty() && !line.starts_with('#')).collect()
	}

	/// A stored definition of the field `name` on `reading` whose type is the
	/// stored text `kind`, with a clause on each side of the type.
	fn stored_with_kind(name: &str, kind: &str) -> StoredFieldDefinition {
		let mut stored = FieldDefinition {
			name: Idiom::field(name),
			table: TableName::from("reading"),
			readonly: true,
			comment: Some("c".to_owned()),
			..Default::default()
		}
		.to_stored();
		stored.field_kind = Some(crate::catalog::KindText::from_raw(kind));
		stored
	}

	/// Every type a field definition is stored with today, on `id` and on any other
	/// field, is read, stored and rendered exactly as before component names
	/// existed: the type is the plain kind the stored text compiles to, nothing is
	/// derived, the stored bytes are the bytes read, and the `id` definition
	/// renders as the same definition on another field does.
	#[test]
	fn every_stored_field_type_reads_and_writes_back_unchanged() {
		use crate::catalog::{FromStored, KindText};
		use crate::expr::statements::define::DefineKind;

		let kinds = corpus(include_str!("testdata/stored_field_kinds.txt"));
		assert!(kinds.len() > 150, "the corpus is read whole");
		let mut stored_list = Vec::new();
		for text in kinds {
			let plain = KindText::from_raw(text)
				.compile()
				.unwrap_or_else(|e| panic!("`{text}` does not compile: {e}"));
			assert_eq!(KindText::new(&plain).as_str(), text, "`{text}` is not in stored form");
			assert_eq!(
				KindText::from_raw(text).compile_keyed().unwrap(),
				(plain.clone(), None),
				"`{text}` compiles differently as the type of `id`"
			);

			// `ix` is as long as `id`, so the two renderings align byte for byte.
			let mut rendered = Vec::new();
			let mut parsed = Vec::new();
			for name in ["id", "ix"] {
				let stored = stored_with_kind(name, text);
				let def = FieldDefinition::from_stored(&stored)
					.unwrap_or_else(|e| panic!("`{name}` of `{text}` does not load: {e}"));
				assert_eq!(def.field_kind.as_ref(), Some(&plain), "{name}: {text}");
				assert!(def.key.is_none() && def.live_key().is_none(), "{name}: {text}");
				assert!(!def.is_key_part(), "{name}: {text}");

				let back = def.to_stored();
				assert_eq!(back, stored, "{name}: `{text}` does not store back unchanged");
				assert_eq!(
					revision::to_vec(&back).unwrap(),
					revision::to_vec(&stored).unwrap(),
					"{name}: `{text}` does not store back to the same bytes"
				);
				let statement = def.to_runtime_definition(DefineKind::Default);
				assert_eq!(statement.key_names, None, "{name}: {text}");
				let sql = statement.to_sql();
				assert!(
					sql.starts_with(&format!("DEFINE FIELD {name} ON reading TYPE {text} ")),
					"{name}: {sql}"
				);
				assert_eq!(def.to_sql(), sql, "{name}: {text}");
				rendered.push(sql.replacen(name, "", 1));
				stored_list.push(stored);

				// The statement as a user writes it parses to the same tree, or
				// fails with the same error, on `id` as on any other field.
				let written = format!("DEFINE FIELD {name} ON reading TYPE {text}");
				parsed.push(match crate::syn::parse(&written) {
					Ok(ast) => format!("ok {}", ast.to_sql().replacen(name, "", 1)),
					Err(e) => format!("err {e}"),
				});
			}
			assert_eq!(rendered[0], rendered[1], "`{text}` renders differently on `id`");
			assert_eq!(parsed[0], parsed[1], "`{text}` parses differently on `id`");
		}

		// A field list that names no component is the list as built, one per
		// stored definition and none derived.
		let built = from_stored_all::<FieldDefinition>(&stored_list).unwrap();
		assert!(Arc::ptr_eq(&built, &with_derived_fields(Arc::clone(&built))));
		let fields = fields_from_stored(&stored_list).unwrap();
		assert_eq!(*fields, *built);
	}

	/// Every type that names the components of an `id` is read with its names,
	/// stored back as the same text, and rendered as the statement that wrote it.
	#[test]
	fn every_stored_keyed_id_type_reads_and_writes_back_unchanged() {
		use crate::catalog::FromStored;
		use crate::expr::statements::define::DefineKind;

		let kinds = corpus(include_str!("testdata/stored_keyed_id_kinds.txt"));
		assert!(kinds.len() > 30, "the corpus is read whole");
		for text in kinds {
			let stored = stored_with_kind("id", text);
			let def = FieldDefinition::from_stored(&stored)
				.unwrap_or_else(|e| panic!("`{text}` does not load: {e}"));
			let key = def.live_key().unwrap_or_else(|| panic!("`{text}` names its components"));
			assert_eq!(def.to_stored(), stored, "`{text}` does not store back unchanged");
			let statement = def.to_runtime_definition(DefineKind::Default);
			assert_eq!(statement.key_names, key.names, "{text}");
			let sql = statement.to_sql();
			assert!(sql.starts_with(&format!("DEFINE FIELD id ON reading TYPE {text} ")), "{sql}");
			assert_eq!(
				fields_from_stored(std::slice::from_ref(&stored)).unwrap().len(),
				1 + key.arity(),
				"{text}"
			);
		}
	}
}
