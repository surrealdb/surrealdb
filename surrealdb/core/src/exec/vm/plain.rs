//! The plain-data witness for the types a lowered program is made of.
//!
//! A type is plain when it holds only data: no `dyn` object, function pointer
//! or closure, no interior-mutable cache, and no handle to a live engine
//! object. [`assert_plain!`] states that of one type at compile time, so a
//! change that gives it a handle fails the build where it is made. Each
//! module invokes it next to the types it keeps plain, and `program.rs`
//! invokes it on `Program`, which covers every type a program reaches.
//! Function pointers and closures satisfy the bounds, so the source scans of
//! `tests_plain.rs` cover those.
//!
//! The witness bounds are `PartialEq + Send + Sync + UnwindSafe +
//! RefUnwindSafe`. The two unwind-safety traits are auto traits that propagate
//! through every field, and a `dyn Trait` object is neither unless its trait
//! says so, so an `Arc<dyn Trait>` anywhere beneath the type is rejected.
//! `Mutex`, `RwLock` and the atomics are unwind-safe but not `PartialEq`, so
//! the equality bound rejects the caches those would hold.
//!
//! A `Value` is plain in every variant but `Value::Closure`, which can hold
//! an engine object, so the witness rejects a raw `Value`. A program holds
//! values only inside [`ValueData`], which the witness accepts because it can
//! be built only from data holding no closure.
//!
//! The witness and the scans reject an engine object a program type names;
//! they do not make one unnameable. That takes the program types in a crate
//! of their own, depending only on the data crates and checked against an
//! allow-list of `cargo metadata`, which is the shape a build of the VM for a
//! Worker takes.

use std::panic::{RefUnwindSafe, UnwindSafe};

use crate::exec::index::access_path::{BTreeAccess, BitmapPlan, BoundOperand};
use crate::val::{RecordIdKey, Value};

/// The bounds a plain type satisfies.
pub(crate) trait Plain: PartialEq + Send + Sync + UnwindSafe + RefUnwindSafe {}

impl<T: PartialEq + Send + Sync + UnwindSafe + RefUnwindSafe + ?Sized> Plain for T {}

/// Fails to compile unless `$ty` is plain.
macro_rules! assert_plain {
	($ty:ty) => {
		const _: () = {
			const fn witness<T: $crate::exec::vm::plain::Plain + ?Sized>() {}
			witness::<$ty>();
		};
	};
}

// The witness holds for plain data in every build. This assertion keeps the
// macro and its bounds in use even before any module invokes it.
assert_plain!(String);

/// Data a program holds that carries values — a constant, the constant pool,
/// an index access, a record key — proven to hold no closure when it is
/// built.
///
/// [`Self::new`] walks the data and refuses a closure anywhere in it, so a
/// `ValueData` holds only plain values and the witness accepts it. The
/// lowering turns that refusal into [`NotCovered::internal`]: a program never
/// holds a closure, and finding one is an internal error, never a decline.
///
/// [`NotCovered::internal`]: super::NotCovered::internal
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ValueData<T: Values>(T);

// Sound because `T` is one of the types `Values` is sealed to, whose only
// field that is not plain is a `Value`, and `ValueData::new` refuses every
// `Value::Closure`, the one variant holding an engine object.
impl<T: Values> UnwindSafe for ValueData<T> {}
impl<T: Values> RefUnwindSafe for ValueData<T> {}

impl<T: Values> ValueData<T> {
	/// `data`, or the internal error naming `what` when it holds a closure.
	pub(crate) fn new(data: T, what: &str) -> Result<Self, super::NotCovered> {
		let mut walk = Walk::default();
		data.values(&mut walk);
		match walk.closure() {
			false => Ok(Self(data)),
			true => Err(super::NotCovered::internal(format!(
				"{what} holds a closure value, which no program holds"
			))),
		}
	}
}

impl<T: Values> std::ops::Deref for ValueData<T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self.0
	}
}

mod sealed {
	pub trait Sealed {}
}

/// The types a [`ValueData`] holds, each listing the values inside it.
/// Sealed: every implementation is in this module, where each is checked to
/// hold nothing but plain fields and values.
pub(crate) trait Values: sealed::Sealed + PartialEq + Send + Sync {
	fn values<'a>(&'a self, walk: &mut Walk<'a>);
}

/// The values a [`Values`] lists, searched for a closure.
#[derive(Default)]
pub(crate) struct Walk<'a> {
	pending: Vec<&'a Value>,
	closure: bool,
}

impl<'a> Walk<'a> {
	fn value(&mut self, value: &'a Value) {
		self.pending.push(value);
	}

	fn bound(&mut self, bound: &'a std::ops::Bound<Value>) {
		if let std::ops::Bound::Included(value) | std::ops::Bound::Excluded(value) = bound {
			self.value(value);
		}
	}

	/// The value of a bound operand: a literal's, and none for the statement
	/// clock, which holds a duration and no value.
	fn operand(&mut self, operand: &'a BoundOperand) {
		if let Some(value) = operand.as_literal() {
			self.value(value);
		}
	}

	fn key(&mut self, key: &'a RecordIdKey) {
		match key {
			RecordIdKey::Array(items) => self.pending.extend(items.iter()),
			RecordIdKey::Object(fields) => self.pending.extend(fields.values()),
			RecordIdKey::Range(range) => {
				for bound in [&range.start, &range.end] {
					if let std::ops::Bound::Included(key) | std::ops::Bound::Excluded(key) = bound {
						self.key(key);
					}
				}
			}
			RecordIdKey::Number(_) | RecordIdKey::String(_) | RecordIdKey::Uuid(_) => {}
		}
	}

	/// Whether a value listed, or one nested inside it, is a closure.
	fn closure(&mut self) -> bool {
		while let Some(value) = self.pending.pop() {
			match value {
				Value::Closure(_) => self.closure = true,
				Value::Array(items) => self.pending.extend(items.iter()),
				Value::Set(items) => self.pending.extend(items.iter()),
				Value::Object(fields) => self.pending.extend(fields.values()),
				Value::Range(range) => {
					self.bound(&range.start);
					self.bound(&range.end);
				}
				Value::RecordId(rid) => self.key(&rid.key),
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::Number(_)
				| Value::String(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
				| Value::Uuid(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::File(_)
				| Value::Regex(_) => {}
			}
		}
		self.closure
	}
}

impl sealed::Sealed for Value {}
impl Values for Value {
	fn values<'a>(&'a self, walk: &mut Walk<'a>) {
		walk.value(self);
	}
}

impl sealed::Sealed for Vec<Value> {}
impl Values for Vec<Value> {
	fn values<'a>(&'a self, walk: &mut Walk<'a>) {
		walk.pending.extend(self.iter());
	}
}

impl sealed::Sealed for RecordIdKey {}
impl Values for RecordIdKey {
	fn values<'a>(&'a self, walk: &mut Walk<'a>) {
		walk.key(self);
	}
}

impl sealed::Sealed for BTreeAccess {}
impl Values for BTreeAccess {
	fn values<'a>(&'a self, walk: &mut Walk<'a>) {
		match self {
			BTreeAccess::Equality(operand) => walk.operand(operand),
			BTreeAccess::Range {
				range,
			} => range.operands().for_each(|operand| walk.operand(operand)),
			BTreeAccess::Compound {
				prefix,
				range,
				upper,
			} => {
				prefix.iter().for_each(|operand| walk.operand(operand));
				for (_, operand) in range.iter().chain(upper) {
					walk.operand(operand);
				}
			}
			BTreeAccess::FullText {
				..
			}
			| BTreeAccess::Knn {
				..
			} => {}
		}
	}
}

impl sealed::Sealed for BitmapPlan {}
impl Values for BitmapPlan {
	fn values<'a>(&'a self, walk: &mut Walk<'a>) {
		match self {
			BitmapPlan::BTree {
				access,
				..
			} => access.values(walk),
			BitmapPlan::Graph {
				source,
				..
			} => walk.key(&source.key),
			BitmapPlan::And(plans) | BitmapPlan::Or(plans) => {
				plans.iter().for_each(|plan| plan.values(walk));
			}
			BitmapPlan::AndNot {
				base,
				subtract,
			} => {
				base.values(walk);
				subtract.values(walk);
			}
			BitmapPlan::FullText {
				..
			} => {}
		}
	}
}

impl sealed::Sealed for crate::exec::pre_decode_filter::PreDecodeShape {}
impl Values for crate::exec::pre_decode_filter::PreDecodeShape {
	fn values<'a>(&'a self, walk: &mut Walk<'a>) {
		use crate::exec::pre_decode_filter::PreDecodeShape;
		match self {
			PreDecodeShape::And(parts) | PreDecodeShape::Or(parts) => {
				parts.iter().for_each(|part| part.values(walk));
			}
			PreDecodeShape::Not(inner) | PreDecodeShape::Selective(inner) => inner.values(walk),
			PreDecodeShape::Leaf {
				literal,
				..
			} => walk.value(literal),
		}
	}
}

impl sealed::Sealed for std::sync::Arc<super::program::KeyRanges> {}
impl Values for std::sync::Arc<super::program::KeyRanges> {
	fn values<'a>(&'a self, walk: &mut Walk<'a>) {
		for (_, start, end) in self.specs.iter().chain(&self.targets) {
			for bound in [start, end] {
				if let std::ops::Bound::Included(key) | std::ops::Bound::Excluded(key) = bound {
					walk.key(key);
				}
			}
		}
	}
}

// What the unwind-safety implementations above rely on: each type `Values`
// is sealed to is plain apart from the values it lists.
assert_plain!(crate::exec::index::access_path::IndexRef);
assert_plain!(crate::expr::operator::MatchesOperator);
assert_plain!(crate::expr::BinaryOperator);
assert_plain!(std::sync::Arc<[crate::exec::object_extract::PathSegment]>);
assert_plain!(crate::expr::Dir);
assert_plain!(Vec<crate::val::Number>);
assert_plain!(Vec<surrealdb_strand::TableName>);

#[cfg(test)]
mod tests {
	use std::marker::PhantomData;
	use std::sync::{Arc, Mutex};

	use super::Plain;

	/// Whether `T` satisfies [`Plain`], decided at the call site by method
	/// resolution: [`Accepted`] is found first and applies only when `T` is
	/// plain, and [`Rejected`], one auto-reference further out, otherwise.
	struct Probe<T: ?Sized>(PhantomData<T>);

	trait Accepted {
		fn is_plain(&self) -> bool {
			true
		}
	}

	impl<T: Plain + ?Sized> Accepted for Probe<T> {}

	trait Rejected {
		fn is_plain(&self) -> bool {
			false
		}
	}

	impl<T: ?Sized> Rejected for &Probe<T> {}

	/// Whether the concrete type `$ty` is plain.
	macro_rules! is_plain {
		($ty:ty) => {
			(&Probe::<$ty>(PhantomData)).is_plain()
		};
	}

	trait Engine {}

	#[derive(Debug, Clone, PartialEq)]
	struct Data {
		name: String,
		ids: Vec<u32>,
		nested: Option<Arc<[Data]>>,
	}

	assert_plain!(Data);
	assert_plain!(Option<Arc<Data>>);

	struct Holds {
		_engine: Arc<dyn Engine + Send + Sync>,
	}

	#[test]
	fn the_witness_accepts_data() {
		assert!(is_plain!(Data));
		assert!(is_plain!(Vec<Option<Arc<[u8]>>>));
		assert!(is_plain!(str));
	}

	#[test]
	fn the_witness_rejects_a_dyn_object() {
		assert!(!is_plain!(Arc<dyn Engine + Send + Sync>));
		assert!(!is_plain!(Box<dyn Engine>));
		assert!(!is_plain!(Vec<Option<Arc<dyn Engine + Send + Sync>>>));
		assert!(!is_plain!(Holds));
	}

	#[test]
	fn the_witness_rejects_a_cache() {
		assert!(!is_plain!(Mutex<u32>));
		assert!(!is_plain!(std::sync::atomic::AtomicU32));
		assert!(!is_plain!(std::cell::Cell<u32>));
	}
}
