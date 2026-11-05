//! The reference keys the record's fields declare.
//!
//! The keys a `REFERENCE` field maintains, and their release.
//!
//! The release runs on `DELETE` before the record is cleared, because it walks
//! the post-image the clear blanks.
//!
//! A reference key is the back-link a referenced record is found by: it lives
//! under the *target* record's range and names the record and field that point
//! at it. Both stages below maintain the same key set, and differ only in the
//! value they judge the record's fields against — the field's own value on a
//! write, and `NONE` on a delete, which releases every link the field holds.

#[cfg(test)]
thread_local! {
	/// How many times this thread ran a reference stage's body over a record, for
	/// tests that must know a write kept its reference keys as instructions.
	pub(crate) static REFERENCE_BODIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
