//! A value the VM holds in a register without owning heap memory.
//!
//! [`Scalar`] is either a scalar kind held inline — numbers, booleans,
//! durations, datetimes, uuids, and strings or bytes borrowed from the row —
//! or any other value, borrowed, owned or shared, in [`Scalar::Value`].
//! Comparing and testing scalars answers exactly what the same operation
//! answers on the [`Value`]s they denote: each scalar kind is compared by the
//! type the `Value` variant holds, and kinds are ranked against each other in
//! the declaration order of [`Value`]'s variants, which is what its derived
//! ordering ranks them by.

#![deny(clippy::wildcard_enum_match_arm)]

use std::borrow::Cow;
use std::cmp::Ordering;
use std::sync::Arc;

use crate::val::{Datetime, Duration, Number, Uuid, Value};

/// A value in a register.
///
/// [`Scalar::of`] holds a value of a scalar kind as that kind. A
/// [`Scalar::Value`] may still hold one, decoded where it could not be
/// borrowed; every comparison reads it through [`Scalar::view`].
#[derive(Debug, Clone)]
pub(crate) enum Scalar<'a> {
	None,
	Null,
	Bool(bool),
	Number(Number),
	Str(&'a str),
	Duration(Duration),
	Datetime(Datetime),
	Uuid(Uuid),
	Bytes(&'a [u8]),
	/// A value of any other kind: an array, set, object, record id, geometry
	/// and so on.
	Value(Held<'a>),
}

/// A value a register holds whole, and who owns it.
///
/// Reading a [`Held`] never copies the value, however it is held; cloning one
/// copies the value only where the register owns it. A shared value is the
/// allocation of the binding it was loaded from — a parameter, a block's
/// `LET`, a call's argument — so a register loaded from a large binding, and
/// every register a step into it reads it from, refer to that allocation
/// rather than to a copy. A register changes a value only once it owns it
/// ([`Held::to_mut`]), so a change never reaches the row, the program or the
/// binding the value was read from.
#[derive(Debug, Clone)]
pub(crate) enum Held<'a> {
	/// Borrowed from the row or the program.
	Borrowed(&'a Value),
	/// The register's own.
	Owned(Value),
	/// Shared with the binding it was read from.
	Shared(Arc<Value>),
}

impl Held<'_> {
	/// The value, owned: moved out where the register owns it or is the only
	/// holder of a shared one, and copied otherwise.
	pub(crate) fn into_owned(self) -> Value {
		match self {
			Held::Borrowed(value) => value.clone(),
			Held::Owned(value) => value,
			Held::Shared(value) => Arc::unwrap_or_clone(value),
		}
	}

	/// The value, to change: a borrowed value, or a shared one something else
	/// still holds, is first copied into the register's own, so the change
	/// reaches neither the row, the program nor the binding it was read from.
	pub(crate) fn to_mut(&mut self) -> &mut Value {
		if !matches!(self, Held::Owned(_)) {
			let owned = std::mem::replace(self, Held::Owned(Value::None)).into_owned();
			*self = Held::Owned(owned);
		}
		match self {
			Held::Owned(value) => value,
			Held::Borrowed(_) | Held::Shared(_) => {
				unreachable!("the value was made the register's own above")
			}
		}
	}
}

impl<'a> Held<'a> {
	/// The same value held for another reader, as a slot hands it to the
	/// register that loads it, or a register to the register an identity step
	/// answers into. A borrowed value is borrowed again. A value of a kind
	/// that is copied without a heap walk is copied; any other is moved into a
	/// shared allocation the first time it is read and shared from then on, so
	/// no read of it copies it, and `self` still holds the same value.
	pub(crate) fn share(&mut self) -> Held<'a> {
		match self {
			Held::Borrowed(value) => Held::Borrowed(value),
			Held::Shared(value) => Held::Shared(Arc::clone(value)),
			Held::Owned(
				value @ (Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::Number(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
				| Value::Uuid(_)),
			) => Held::Owned(value.clone()),
			Held::Owned(
				value @ (Value::String(_)
				| Value::Array(_)
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Regex(_)
				| Value::Range(_)
				| Value::Closure(_)),
			) => {
				let shared = Arc::new(std::mem::take(value));
				*self = Held::Shared(Arc::clone(&shared));
				Held::Shared(shared)
			}
		}
	}
}

impl std::ops::Deref for Held<'_> {
	type Target = Value;

	#[inline]
	fn deref(&self) -> &Value {
		match self {
			Held::Borrowed(value) => value,
			Held::Owned(value) => value,
			Held::Shared(value) => value,
		}
	}
}

impl AsRef<Value> for Held<'_> {
	#[inline]
	fn as_ref(&self) -> &Value {
		self
	}
}

impl<'a> Scalar<'a> {
	/// The value this scalar holds whole — an array, a set, an object and so
	/// on — borrowed; `None` for a scalar held unboxed.
	#[inline]
	pub(crate) fn as_whole(&self) -> Option<&Value> {
		if let Scalar::Value(value) = self {
			Some(value.as_ref())
		} else {
			None
		}
	}

	/// The scalar that denotes `value`, borrowing its heap parts.
	pub(crate) fn of(value: &'a Value) -> Self {
		match value {
			Value::None => Scalar::None,
			Value::Null => Scalar::Null,
			Value::Bool(v) => Scalar::Bool(*v),
			Value::Number(v) => Scalar::Number(v.clone()),
			Value::String(v) => Scalar::Str(v.as_str()),
			Value::Duration(v) => Scalar::Duration(*v),
			Value::Datetime(v) => Scalar::Datetime(*v),
			Value::Uuid(v) => Scalar::Uuid(*v),
			Value::Bytes(v) => Scalar::Bytes(v.0.as_ref()),
			other @ (Value::Array(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_)) => Scalar::Value(Held::Borrowed(other)),
		}
	}

	/// The [`Value`] this scalar denotes. Allocates for a string, bytes and a
	/// borrowed composite.
	pub(crate) fn to_value(&self) -> Value {
		match self {
			Scalar::None => Value::None,
			Scalar::Null => Value::Null,
			Scalar::Bool(v) => Value::Bool(*v),
			Scalar::Number(v) => Value::Number(v.clone()),
			Scalar::Str(v) => Value::String((*v).into()),
			Scalar::Duration(v) => Value::Duration(*v),
			Scalar::Datetime(v) => Value::Datetime(*v),
			Scalar::Uuid(v) => Value::Uuid(*v),
			Scalar::Bytes(v) => Value::Bytes(crate::val::Bytes(::bytes::Bytes::copy_from_slice(v))),
			Scalar::Value(v) => v.as_ref().clone(),
		}
	}

	/// Consumes this scalar and returns the [`Value`] it denotes without cloning
	/// an owned [`Scalar::Value`], or a shared one nothing else holds.
	#[inline]
	pub(crate) fn into_value(self) -> Value {
		if let Scalar::Value(held) = self {
			held.into_owned()
		} else {
			self.to_value()
		}
	}

	/// The same scalar for another register, as [`Instr::Load`] copies one:
	/// a held value is shared as [`Held::share`] shares it, so a register's
	/// own collection is moved into an allocation both registers then share
	/// rather than copied; any other scalar is copied.
	///
	/// [`Instr::Load`]: super::program::Instr::Load
	pub(crate) fn share(&mut self) -> Scalar<'a> {
		match self {
			Scalar::Value(held) => Scalar::Value(held.share()),
			Scalar::None
			| Scalar::Null
			| Scalar::Bool(_)
			| Scalar::Number(_)
			| Scalar::Str(_)
			| Scalar::Duration(_)
			| Scalar::Datetime(_)
			| Scalar::Uuid(_)
			| Scalar::Bytes(_) => self.clone(),
		}
	}

	/// The same scalar, borrowed from this one for as long as it is read: a
	/// held value is borrowed rather than copied, and any other scalar is
	/// copied, which never allocates.
	pub(crate) fn lend(&self) -> Scalar<'_> {
		match self {
			Scalar::Value(held) => Scalar::Value(Held::Borrowed(held)),
			Scalar::None => Scalar::None,
			Scalar::Null => Scalar::Null,
			Scalar::Bool(v) => Scalar::Bool(*v),
			Scalar::Number(v) => Scalar::Number(v.clone()),
			Scalar::Str(v) => Scalar::Str(v),
			Scalar::Duration(v) => Scalar::Duration(*v),
			Scalar::Datetime(v) => Scalar::Datetime(*v),
			Scalar::Uuid(v) => Scalar::Uuid(*v),
			Scalar::Bytes(v) => Scalar::Bytes(v),
		}
	}

	/// The [`Value`] this scalar denotes, borrowed where the scalar holds one.
	/// Allocates only for a string and bytes.
	pub(crate) fn as_value(&self) -> Cow<'_, Value> {
		if let Scalar::Value(v) = self {
			Cow::Borrowed(v.as_ref())
		} else {
			Cow::Owned(self.to_value())
		}
	}

	/// The same value, with a scalar kind held in [`Scalar::Value`] held as
	/// its kind.
	fn view(&self) -> Scalar<'_> {
		match self {
			Scalar::Value(v) => Scalar::of(v.as_ref()),
			Scalar::None => Scalar::None,
			Scalar::Null => Scalar::Null,
			Scalar::Bool(v) => Scalar::Bool(*v),
			Scalar::Number(v) => Scalar::Number(v.clone()),
			Scalar::Str(v) => Scalar::Str(v),
			Scalar::Duration(v) => Scalar::Duration(*v),
			Scalar::Datetime(v) => Scalar::Datetime(*v),
			Scalar::Uuid(v) => Scalar::Uuid(*v),
			Scalar::Bytes(v) => Scalar::Bytes(v),
		}
	}

	/// Whether this is an array or a set, which `*=` and `?=` compare element
	/// by element.
	pub(crate) fn is_collection(&self) -> bool {
		matches!(self.view(), Scalar::Value(v) if matches!(v.as_ref(), Value::Array(_) | Value::Set(_)))
	}

	/// Whether this is `NONE`.
	pub(crate) fn is_none(&self) -> bool {
		matches!(self.view(), Scalar::None)
	}

	/// [`Value::is_nullish`]: whether this is `NONE` or `NULL`.
	pub(crate) fn is_nullish(&self) -> bool {
		matches!(self.view(), Scalar::None | Scalar::Null)
	}

	/// [`Value::is_truthy`].
	pub(crate) fn is_truthy(&self) -> bool {
		match &self.view() {
			Scalar::None | Scalar::Null => false,
			Scalar::Bool(v) => *v,
			Scalar::Number(v) => v.is_truthy(),
			Scalar::Str(v) => !v.is_empty(),
			Scalar::Duration(v) => v.as_nanos() > 0,
			Scalar::Datetime(_) | Scalar::Uuid(_) => true,
			Scalar::Bytes(v) => !v.is_empty(),
			Scalar::Value(v) => v.is_truthy(),
		}
	}

	/// [`Value::equal`], the `=` operator.
	pub(crate) fn equal(&self, other: &Scalar<'_>) -> bool {
		let (this, other) = (self.view(), other.view());
		match (&this, &other) {
			(Scalar::Value(a), Scalar::Value(b)) => a.equal(b),
			// A string equals a regex that matches it, either way round.
			(Scalar::Str(s), Scalar::Value(v)) | (Scalar::Value(v), Scalar::Str(s)) => {
				matches!(v.as_ref(), Value::Regex(r) if r.inner().is_match(s))
			}
			_ => this.same_kind_eq(&other),
		}
	}

	/// The derived [`PartialEq`] of [`Value`], the `==` operator.
	pub(crate) fn exact(&self, other: &Scalar<'_>) -> bool {
		let (this, other) = (self.view(), other.view());
		match (&this, &other) {
			(Scalar::Value(a), Scalar::Value(b)) => a.as_ref() == b.as_ref(),
			_ => this.same_kind_eq(&other),
		}
	}

	/// Equality of two values of the same scalar kind; values of different
	/// kinds are unequal.
	fn same_kind_eq(&self, other: &Scalar<'_>) -> bool {
		match (self, other) {
			(Scalar::None, Scalar::None) | (Scalar::Null, Scalar::Null) => true,
			(Scalar::Bool(a), Scalar::Bool(b)) => a == b,
			(Scalar::Number(a), Scalar::Number(b)) => a == b,
			(Scalar::Str(a), Scalar::Str(b)) => a == b,
			(Scalar::Duration(a), Scalar::Duration(b)) => a == b,
			(Scalar::Datetime(a), Scalar::Datetime(b)) => a == b,
			(Scalar::Uuid(a), Scalar::Uuid(b)) => a == b,
			(Scalar::Bytes(a), Scalar::Bytes(b)) => a == b,
			_ => false,
		}
	}

	/// The derived [`PartialOrd`] of [`Value`], which `<`, `<=`, `>` and `>=`
	/// answer from.
	pub(crate) fn partial_cmp(&self, other: &Scalar<'_>) -> Option<Ordering> {
		let (this, other) = (self.view(), other.view());
		match (&this, &other) {
			(Scalar::Value(a), Scalar::Value(b)) => a.as_ref().partial_cmp(b.as_ref()),
			(Scalar::None, Scalar::None) | (Scalar::Null, Scalar::Null) => Some(Ordering::Equal),
			(Scalar::Bool(a), Scalar::Bool(b)) => a.partial_cmp(b),
			(Scalar::Number(a), Scalar::Number(b)) => a.partial_cmp(b),
			(Scalar::Str(a), Scalar::Str(b)) => a.partial_cmp(b),
			(Scalar::Duration(a), Scalar::Duration(b)) => a.partial_cmp(b),
			(Scalar::Datetime(a), Scalar::Datetime(b)) => a.partial_cmp(b),
			(Scalar::Uuid(a), Scalar::Uuid(b)) => a.partial_cmp(b),
			(Scalar::Bytes(a), Scalar::Bytes(b)) => a.partial_cmp(b),
			_ => this.rank().partial_cmp(&other.rank()),
		}
	}

	/// The position of this value's kind among [`Value`]'s variants.
	fn rank(&self) -> u8 {
		match self {
			Scalar::None => 0,
			Scalar::Null => 1,
			Scalar::Bool(_) => 2,
			Scalar::Number(_) => 3,
			Scalar::Str(_) => 4,
			Scalar::Duration(_) => 5,
			Scalar::Datetime(_) => 6,
			Scalar::Uuid(_) => 7,
			Scalar::Bytes(_) => 12,
			Scalar::Value(v) => value_rank(v),
		}
	}
}

/// The position of `value`'s variant in the declaration of [`Value`].
fn value_rank(value: &Value) -> u8 {
	match value {
		Value::None => 0,
		Value::Null => 1,
		Value::Bool(_) => 2,
		Value::Number(_) => 3,
		Value::String(_) => 4,
		Value::Duration(_) => 5,
		Value::Datetime(_) => 6,
		Value::Uuid(_) => 7,
		Value::Array(_) => 8,
		Value::Set(_) => 9,
		Value::Object(_) => 10,
		Value::Geometry(_) => 11,
		Value::Bytes(_) => 12,
		Value::Table(_) => 13,
		Value::RecordId(_) => 14,
		Value::File(_) => 15,
		Value::Regex(_) => 16,
		Value::Range(_) => 17,
		Value::Closure(_) => 18,
		Value::Date(_) => 19,
		Value::Time(_) => 20,
		Value::Timestamp(_) => 21,
		Value::Interval(_) => 22,
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::*;
	use crate::val::{Array, Object};

	fn array() -> Value {
		Value::Array(Array::from(vec![
			Value::Array(Array::from(vec![Value::from(1_i64), Value::from(2_i64)])),
			Value::Array(Array::from(vec![Value::from(3_i64), Value::from(4_i64)])),
		]))
	}

	/// A slot's value is moved into one shared allocation the first time it
	/// is read, and every read shares that allocation rather than copying the
	/// value; a value copied without a heap walk is copied.
	#[test]
	fn a_slot_shares_its_value_with_every_read() {
		let mut slot = Held::Owned(array());
		let first = slot.share();
		let second = slot.share();
		let (Held::Shared(held), Held::Shared(first), Held::Shared(second)) =
			(&slot, &first, &second)
		else {
			panic!("an array is shared by the slot and every read of it");
		};
		assert!(Arc::ptr_eq(held, first) && Arc::ptr_eq(held, second));
		assert_eq!(**held, array());

		let mut number = Held::Owned(Value::from(1_i64));
		assert!(matches!(number.share(), Held::Owned(Value::Number(_))));
		assert!(matches!(number, Held::Owned(_)), "a number stays the slot's own");
	}

	/// The address of the elements an array holds.
	fn elements(value: &Value) -> *const Value {
		let Value::Array(items) = value else {
			panic!("the value is an array");
		};
		items.0.as_ptr()
	}

	/// A shared value its holder is the only one to hold is moved out, not
	/// copied; one something else still holds is copied, and the other
	/// holder keeps the value it had.
	#[test]
	fn a_sole_holder_takes_a_shared_value_without_copying_it() {
		let shared = Arc::new(array());
		let at = elements(&shared);
		let owned = Held::Shared(shared).into_owned();
		assert_eq!(elements(&owned), at, "the sole holder's value is moved out");
		assert_eq!(owned, array());

		let shared = Arc::new(array());
		let other = Arc::clone(&shared);
		let mut owned = Held::Shared(shared).into_owned();
		assert_ne!(elements(&owned), elements(&other), "a value still held elsewhere is copied");
		if let Value::Array(items) = &mut owned {
			items.push(Value::from(5_i64));
		}
		assert_eq!(*other, array(), "the other holder keeps the value it had");
		assert_eq!(Arc::strong_count(&other), 1);
	}

	/// A register hands another the value it holds without copying it: a
	/// borrowed value is borrowed again, and the register's own collection
	/// is moved into one allocation both then share, which still reads as the
	/// value it was.
	#[test]
	fn a_register_shares_its_own_value_with_another() {
		let row = array();
		let mut borrowed = Held::Borrowed(&row);
		assert!(matches!(borrowed.share(), Held::Borrowed(value) if std::ptr::eq(value, &row)));

		let mut register = Held::Owned(array());
		let at = elements(&register);
		let answer = register.share();
		let (Held::Shared(held), Held::Shared(answer)) = (&register, &answer) else {
			panic!("an array is shared by the register and the answer");
		};
		assert!(Arc::ptr_eq(held, answer));
		assert_eq!(elements(held), at, "the elements are moved, not copied");
		assert_eq!(**held, array());
	}

	/// A register changes a shared value only once it is its own: the
	/// binding it was read from keeps the value it had.
	#[test]
	fn a_change_to_a_shared_value_never_reaches_the_binding() {
		let mut object = Object::default();
		object.insert("a".to_owned(), Value::from(1_i64));
		let original = Value::Object(object);
		let mut slot = Held::Owned(original.clone());
		let mut register = slot.share();
		let Value::Object(changed) = register.to_mut() else {
			panic!("the register holds an object");
		};
		changed.insert("b".to_owned(), Value::from(2_i64));
		assert!(matches!(register, Held::Owned(_)));
		assert_eq!(*slot, original, "the binding keeps the value it had");
		let Value::Object(changed) = &*register else {
			panic!("the register holds an object");
		};
		assert_eq!(changed.len(), 2);
	}

	/// A register changes a borrowed value only once it is its own: the row
	/// or the program it was borrowed from keeps the value it had.
	#[test]
	fn a_change_to_a_borrowed_value_never_reaches_the_row() {
		let row = array();
		let mut register = Held::Borrowed(&row);
		if let Value::Array(items) = register.to_mut() {
			items.push(Value::from(5_i64));
		}
		assert_eq!(row, array(), "the row keeps the value it had");
		assert!(matches!(&*register, Value::Array(items) if items.len() == 3));
	}
}
