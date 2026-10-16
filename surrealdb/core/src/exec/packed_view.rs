//! A stored record, admitted for the readers that judge or read it from its bytes.
//!
//! The pre-decode filter, the top-k probe and the in-place body each read columns of a packed row
//! where they lie. A packed row may be read that way only once it has been checked as a whole
//! ([`StoredRecord::checked_packed`]), which refuses every row the whole decode refuses, whichever
//! fields a reader asks of it. [`AdmittedRow`] is that check made once for a record, so the readers
//! of one record share it.

use crate::catalog::packed::CheckedPackedRow;
use crate::catalog::{StoredRecord, TableLayouts};
use crate::exec::operators::scan::pipeline::decode_record_value;
use crate::exec::pre_decode_filter::PreDecodeFilter;
use crate::val::Value;

/// A stored record as the readers of its bytes see it.
#[derive(Clone, Copy, Debug)]
pub(crate) enum AdmittedRow<'a> {
	/// The record's bytes: a record in the stored-value or `revision` encoding, or a packed row
	/// that has not been checked. A reader judges a record in these bytes as it reads them and
	/// takes a packed row among them as one it cannot read, which it neither judges nor rejects.
	Bytes(&'a [u8]),
	/// A packed row that the whole decode accepts, with the record's key, which names its table and
	/// its id.
	Packed {
		row: CheckedPackedRow<'a>,
		key: &'a [u8],
	},
	/// A packed row that the whole decode refuses: its key, its layouts' table or generation, or
	/// the row itself. No reader reads it: the whole decode raises the error.
	Refused,
}

impl<'a> AdmittedRow<'a> {
	/// The record `value` of the record `key` names, checked where it is a packed row against the
	/// descriptor of its generation in `layouts`, which admitted it. Any other record is its
	/// bytes.
	///
	/// A packed row is [`Self::Refused`] exactly where
	/// [`decode_record_value`](crate::exec::operators::scan::pipeline::decode_record_value) refuses
	/// it. The check allocates nothing for a row whose columns hold no value the check builds, and
	/// for a key that is a number, a string, a uuid, or an array or object of those.
	pub(crate) fn of(key: &'a [u8], value: &'a [u8], layouts: Option<&'a TableLayouts>) -> Self {
		if !StoredRecord::is_packed_bytes(value) {
			return Self::Bytes(value);
		}
		match crate::exec::vm::row_packed::admit(key, value, layouts) {
			Some(row) => Self::Packed {
				row,
				key,
			},
			None => Self::Refused,
		}
	}

	/// The record `value` of the record `key` names, as [`Self::of`] admits it where `filter`
	/// judges packed rows ([`PreDecodeFilter::judges_packed_rows`]), and otherwise its bytes: a
	/// packed row that the filter would leave unjudged is not checked for it.
	/// The record `value` of the record `key` names, whole: the value
	/// [`decode_record_value`] answers for the same bytes, or the error it raises. A packed row
	/// this admission checked is decoded from the checked row, which does not parse its envelope,
	/// admit its generation or prove its shape again; one it refused raises the whole decode's
	/// error; any other record is decoded from its bytes.
	pub(crate) fn decode(
		&self,
		key: &[u8],
		value: &[u8],
		layouts: Option<&TableLayouts>,
	) -> anyhow::Result<Value> {
		match self {
			Self::Packed {
				row,
				key,
			} => crate::exec::vm::row_packed::decode_checked(key, *row),
			Self::Refused => Err(crate::exec::vm::row_packed::refusal(key, value, layouts)),
			Self::Bytes(_) => decode_record_value(key, value, layouts),
		}
	}

	pub(crate) fn for_filter(
		filter: &PreDecodeFilter,
		key: &'a [u8],
		value: &'a [u8],
		layouts: Option<&'a TableLayouts>,
	) -> Self {
		if filter.judges_packed_rows() {
			Self::of(key, value, layouts)
		} else {
			Self::Bytes(value)
		}
	}
}
