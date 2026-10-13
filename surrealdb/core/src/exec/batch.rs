//! The unit of data operators exchange.
//!
//! [`ValueBatch`] lives in its own module so that its representation is private
//! to this file. Rust private fields are visible to descendant modules, so a
//! batch declared in `exec/mod.rs` would still be field-accessible from every
//! operator; declaring it here is what makes the accessors the only way in, and
//! therefore what lets a second representation be added without a change at
//! every use site.

use crate::val::Value;

/// A batch of values returned by an execution plan.
///
/// The row representation is private. Operators reach it only through the
/// accessors below, so that a columnar representation can be added as a variant
/// here without a change at every use site.
///
/// The accessors split into two groups, and that split is the contract a
/// columnar representation has to satisfy:
///
/// - [`len`](Self::len) and [`is_empty`](Self::is_empty) answer from metadata alone, so they stay
///   cheap for any representation.
/// - [`values`](Self::values), [`values_mut`](Self::values_mut) and
///   [`into_values`](Self::into_values) hand out rows. A non-row representation has to materialise
///   to answer them. That is the row fallback: correct everywhere, and the reason a columnar
///   operator wants to avoid asking.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ValueBatch {
	values: Vec<Value>,
}

impl ValueBatch {
	/// A batch holding these rows.
	pub(crate) fn new(values: Vec<Value>) -> Self {
		Self {
			values,
		}
	}

	/// A batch holding no values.
	pub(crate) fn empty() -> Self {
		Self::new(Vec::new())
	}

	/// The number of values in the batch.
	pub(crate) fn len(&self) -> usize {
		self.values.len()
	}

	/// Whether the batch carries no values.
	pub(crate) fn is_empty(&self) -> bool {
		self.values.is_empty()
	}

	/// The batch's rows.
	pub(crate) fn values(&self) -> &[Value] {
		&self.values
	}

	/// The batch's rows, for in-place filtering and rewriting.
	pub(crate) fn values_mut(&mut self) -> &mut Vec<Value> {
		&mut self.values
	}

	/// Consume the batch into its rows.
	pub(crate) fn into_values(self) -> Vec<Value> {
		self.values
	}
}

impl From<Vec<Value>> for ValueBatch {
	fn from(values: Vec<Value>) -> Self {
		Self::new(values)
	}
}

impl IntoIterator for ValueBatch {
	type Item = Value;
	type IntoIter = std::vec::IntoIter<Value>;

	fn into_iter(self) -> Self::IntoIter {
		self.values.into_iter()
	}
}

impl<'a> IntoIterator for &'a ValueBatch {
	type Item = &'a Value;
	type IntoIter = std::slice::Iter<'a, Value>;

	fn into_iter(self) -> Self::IntoIter {
		self.values.iter()
	}
}
