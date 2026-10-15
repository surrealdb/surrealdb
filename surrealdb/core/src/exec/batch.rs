//! The unit of data operators exchange.
//!
//! [`ValueBatch`] lives in its own module so that its representation is private
//! to this file. Rust private fields are visible to descendant modules, so a
//! batch declared in `exec/mod.rs` would still be field-accessible from every
//! operator; declaring it here is what makes the accessors the only way in, and
//! therefore what lets a second representation be added without a change at
//! every use site.

use crate::catalog::Generation;
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
	/// Rows already encoded for the wire, which precede `values`. Only a
	/// statement run with [`ExecOperator::execute_encoded`](super::ExecOperator::execute_encoded)
	/// produces them, for the consumer that asked; [`Self::values`] and the
	/// other row accessors do not see them.
	encoded: Option<surrealdb_rpc::EncodedRows>,
	/// The generation each row is stamped with, where the batch is a scan's
	/// records as stored and the table has migration steps: one per row, in
	/// row order. `None` once the rows may have been rewritten or reordered,
	/// and for a batch of a table with no migration.
	stamps: Option<Vec<Option<Generation>>>,
}

impl ValueBatch {
	/// A batch holding these rows.
	pub(crate) fn new(values: Vec<Value>) -> Self {
		Self {
			values,
			encoded: None,
			stamps: None,
		}
	}

	/// A batch of stored records, each stamped with the generation beside it.
	pub(crate) fn stamped(values: Vec<Value>, stamps: Vec<Option<Generation>>) -> Self {
		debug_assert_eq!(values.len(), stamps.len());
		Self {
			values,
			encoded: None,
			stamps: Some(stamps),
		}
	}

	/// The generation each row is stamped with, taken out of the batch, when
	/// the batch is a scan's records as stored. The rows are not changed.
	pub(crate) fn take_stamps(&mut self) -> Option<Vec<Option<Generation>>> {
		self.stamps.take()
	}

	/// Consume the batch into its rows and, for a batch of stored records, the
	/// generation each is stamped with.
	pub(crate) fn into_stamped(self) -> (Vec<Value>, Option<Vec<Option<Generation>>>) {
		(self.values, self.stamps)
	}

	/// A batch holding rows already encoded for the wire.
	pub(crate) fn encoded(rows: surrealdb_rpc::EncodedRows) -> Self {
		Self {
			values: Vec::new(),
			encoded: Some(rows),
			stamps: None,
		}
	}

	/// The rows already encoded for the wire, which precede the batch's
	/// values, taken out of it.
	pub(crate) fn take_encoded(&mut self) -> Option<surrealdb_rpc::EncodedRows> {
		self.encoded.take()
	}

	/// The number of values in the batch.
	pub(crate) fn len(&self) -> usize {
		self.values.len() + self.encoded.as_ref().map_or(0, surrealdb_rpc::EncodedRows::len)
	}

	/// Whether the batch carries no values.
	pub(crate) fn is_empty(&self) -> bool {
		self.len() == 0
	}

	/// The batch's rows.
	pub(crate) fn values(&self) -> &[Value] {
		&self.values
	}

	/// The batch's rows, for in-place filtering and rewriting.
	pub(crate) fn values_mut(&mut self) -> &mut Vec<Value> {
		self.stamps = None;
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
