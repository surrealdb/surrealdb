//! A caller-owned buffer a [`RangeCursor`](crate::snapshot::RangeCursor) copies entries into.
//!
//! A [`Page`] holds the keys and the values of the entries it was filled with in two flat byte
//! buffers and a table of where each entry lies in them, so a page is three allocations whatever it
//! holds, and its owner reuses them across fills. Its capacity bounds the key and value bytes it
//! takes; an entry larger than the capacity is taken alone, the page growing for it, so no entry is
//! ever unreadable. The caller that owns the page owns that growth: [`Page::bytes`] reports what
//! the page holds and may exceed [`Page::capacity`] after such an entry.
//!
//! [`Page::batch`] is the same borrowed view a [`ScanCursorVals`](crate::api::ScanCursorVals)
//! batch is, so code that reads a batch reads a page.

use crate::api::{KeyValSpan, ValsBatch};

/// A reusable buffer of key and value pairs. See the [module docs](self).
#[derive(Debug, Default)]
pub struct Page {
	/// The keys of the entries held, concatenated in entry order.
	keys: Vec<u8>,
	/// The values of the entries held, concatenated in entry order.
	vals: Vec<u8>,
	/// Where each entry's key and value lie in `keys` and `vals`.
	spans: Vec<KeyValSpan>,
	/// The key and value bytes the page takes before it is full.
	capacity: usize,
}

impl Page {
	/// A page that takes `bytes` of keys and values before it is full.
	pub fn with_capacity(bytes: usize) -> Self {
		Self {
			keys: Vec::new(),
			vals: Vec::new(),
			spans: Vec::new(),
			capacity: bytes,
		}
	}

	/// Drop the entries held, keeping the buffers.
	pub fn clear(&mut self) {
		self.keys.clear();
		self.vals.clear();
		self.spans.clear();
	}

	/// The number of entries held.
	pub fn len(&self) -> usize {
		self.spans.len()
	}

	/// Whether the page holds no entry.
	pub fn is_empty(&self) -> bool {
		self.spans.is_empty()
	}

	/// The key and value bytes held.
	pub fn bytes(&self) -> usize {
		self.keys.len() + self.vals.len()
	}

	/// The key and value bytes the page takes before it is full.
	pub fn capacity(&self) -> usize {
		self.capacity
	}

	/// A borrowed view of the entries held, in the order they were pushed.
	pub fn batch(&self) -> ValsBatch<'_> {
		ValsBatch::from_parts(
			&self.keys,
			&self.vals,
			&self.spans,
			self.keys.len() as u64,
			self.vals.len() as u64,
		)
	}

	/// Whether an entry of `key` and `val` fits: it does while the page stays within its capacity,
	/// and always when the page is empty.
	pub fn fits(&self, key: &[u8], val: &[u8]) -> bool {
		self.is_empty() || self.bytes() + key.len() + val.len() <= self.capacity
	}

	/// Append one entry. `false`, with nothing appended, when it does not [fit](Self::fits).
	pub fn push(&mut self, key: &[u8], val: &[u8]) -> bool {
		if !self.fits(key, val) {
			return false;
		}
		self.spans.push(KeyValSpan {
			key_offset: self.keys.len(),
			key_len: key.len(),
			val_offset: self.vals.len(),
			val_len: val.len(),
		});
		self.keys.extend_from_slice(key);
		self.vals.extend_from_slice(val);
		true
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_page_takes_entries_up_to_its_capacity() {
		let mut page = Page::with_capacity(20);
		assert!(page.push(b"k1", b"0123456789"));
		assert!(page.push(b"k2", b"012345"));
		// Two more bytes would exceed 20.
		assert!(!page.push(b"k3", b"01"));
		assert_eq!((page.len(), page.bytes()), (2, 20));
		let batch = page.batch();
		assert_eq!(batch.get(0), Some((&b"k1"[..], &b"0123456789"[..])));
		assert_eq!(batch.get(1), Some((&b"k2"[..], &b"012345"[..])));
		assert_eq!((batch.key_bytes, batch.value_bytes), (4, 16));
	}

	#[test]
	fn an_entry_larger_than_the_capacity_is_taken_alone() {
		let mut page = Page::with_capacity(8);
		assert!(page.push(b"big", &[7u8; 100]));
		assert!(page.bytes() > page.capacity());
		assert!(!page.push(b"k", b"v"), "nothing joins an oversized entry");
		page.clear();
		assert!(page.is_empty());
		assert!(page.push(b"k", b"v"));
	}
}
