use std::borrow::Borrow;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::Arc;

use ahash::AHasher;
use bytes::Bytes;
use lru::LruCache;
use revision::{DeserializeRevisioned, Error, Revisioned, SerializeRevisioned};
use serde::{Deserialize, Serialize};
use surrealdb_types::{SqlFormat, ToSql};

thread_local! {
	/// Thread-local LRU cache for string interning
	/// Stores Arc<str> directly with automatic eviction of least recently used entries
	/// Size: 4096 entries provides good hit rate while keeping memory bounded (~64KB)
	static LOCAL_INTERNER: RefCell<LruCache<u64, Arc<str>>> =
		RefCell::new(LruCache::new(NonZeroUsize::new(4096).unwrap()));
}

/// Intern a string, returning a deduplicated Arc<str>
#[inline]
fn intern(s: String) -> Arc<str> {
	// Long strings are not interned because
	// they are more likely to be unique and
	// unlikely to be shared across documents.
	if s.len() >= 32 {
		return Arc::<str>::from(s);
	}
	// Hash the string for the cache
	let hash = {
		let mut hasher = AHasher::default();
		s.hash(&mut hasher);
		hasher.finish()
	};
	// Fetch or insert the string in the cache
	LOCAL_INTERNER.with(|cache| {
		// Borrow the cache mutably
		let mut cache = cache.borrow_mut();
		// Check if we have a cached entry for this hash
		if let Some(arc) = cache.get(&hash) {
			// Verify it's the same string
			if arc.as_ref() == s.as_str() {
				return arc.clone();
			}
		}
		// Cache miss: create new Arc for the string
		let arc: Arc<str> = Arc::<str>::from(s);
		// Insert the new entry into the cache
		cache.put(hash, arc.clone());
		// Return the new Arc
		arc
	})
}

/// Statistics about the Symbol interner cache
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
	/// Number of entries currently in the cache
	pub entries: usize,
	/// Maximum capacity of the cache
	pub capacity: usize,
}

/// An interned string for efficient storage of repeated string keys
///
/// Symbol wraps an `Arc<str>` and automatically interns strings through a
/// thread-local LRU cache. When multiple Symbol instances are created with
/// the same string value, they share the same underlying memory allocation.
///
/// This is particularly useful for object keys in documents where the same
/// field names appear repeatedly across many documents.
///
/// # Examples
///
/// ```
/// use surrealdb_core::val::Symbol;
///
/// let key1 = Symbol::from("name");
/// let key2 = Symbol::from("name");
///
/// // Both symbols share the same Arc<str>
/// assert!(std::sync::Arc::ptr_eq(&key1.into_inner(), &key2.into_inner()));
/// ```
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::Symbol")]
pub struct Symbol(Arc<str>);

impl Symbol {
	/// Create a new Symbol from a string
	///
	/// The string will be automatically interned through a thread-local cache.
	#[inline]
	pub fn new(s: impl Into<String>) -> Self {
		Self(intern(s.into()))
	}

	/// Get the underlying string as a slice
	#[inline]
	pub fn as_str(&self) -> &str {
		self.0.as_ref()
	}

	/// Get the underlying string as an Arc<str>
	#[inline]
	pub fn into_inner(self) -> Arc<str> {
		self.0
	}

	/// Convert the symbol string to a `bytes::Bytes`
	#[inline]
	pub fn into_bytes(self) -> Bytes {
		Bytes::from(self.0.as_ref().as_bytes().to_vec())
	}

	/// Get statistics about the interner cache for this thread
	pub fn cache_stats() -> CacheStats {
		LOCAL_INTERNER.with(|cache| {
			let cache = cache.borrow();
			CacheStats {
				entries: cache.len(),
				capacity: cache.cap().get(),
			}
		})
	}
}

impl Deref for Symbol {
	type Target = str;

	#[inline]
	fn deref(&self) -> &str {
		self.0.as_ref()
	}
}

impl AsRef<str> for Symbol {
	#[inline]
	fn as_ref(&self) -> &str {
		self.0.as_ref()
	}
}

impl Borrow<str> for Symbol {
	#[inline]
	fn borrow(&self) -> &str {
		self.0.as_ref()
	}
}

impl From<&str> for Symbol {
	#[inline]
	fn from(s: &str) -> Self {
		Self::new(s)
	}
}

impl From<String> for Symbol {
	#[inline]
	fn from(s: String) -> Self {
		Self::new(s)
	}
}

impl From<&String> for Symbol {
	#[inline]
	fn from(s: &String) -> Self {
		Self::new(s)
	}
}

impl From<Arc<str>> for Symbol {
	#[inline]
	fn from(arc: Arc<str>) -> Self {
		Self(arc)
	}
}

impl From<Symbol> for Arc<str> {
	#[inline]
	fn from(s: Symbol) -> Arc<str> {
		s.0
	}
}

impl From<Symbol> for String {
	#[inline]
	fn from(s: Symbol) -> String {
		s.0.to_string()
	}
}

impl From<&Symbol> for String {
	#[inline]
	fn from(s: &Symbol) -> String {
		s.0.to_string()
	}
}

impl Eq for Symbol {}

impl PartialEq for Symbol {
	#[inline]
	fn eq(&self, other: &Self) -> bool {
		// Fast path: check if pointers are equal
		if Arc::ptr_eq(&self.0, &other.0) {
			return true;
		}
		// Slow path: compare string contents
		self.0.as_ref() == other.0.as_ref()
	}
}

impl PartialEq<String> for Symbol {
	#[inline]
	fn eq(&self, other: &String) -> bool {
		self.0.as_ref() == other.as_str()
	}
}

impl PartialEq<&String> for Symbol {
	#[inline]
	fn eq(&self, other: &&String) -> bool {
		self.0.as_ref() == other.as_str()
	}
}

impl PartialEq<&str> for Symbol {
	#[inline]
	fn eq(&self, other: &&str) -> bool {
		self.0.as_ref() == *other
	}
}

impl Ord for Symbol {
	#[inline]
	fn cmp(&self, other: &Self) -> Ordering {
		// Fast path: check if pointers are equal
		if Arc::ptr_eq(&self.0, &other.0) {
			return Ordering::Equal;
		}
		// Slow path: compare string contents
		self.0.as_ref().cmp(other.0.as_ref())
	}
}

impl PartialOrd for Symbol {
	#[inline]
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		// Fast path: check if pointers are equal
		if Arc::ptr_eq(&self.0, &other.0) {
			return Some(Ordering::Equal);
		}
		// Slow path: compare string contents
		Some(self.0.as_ref().cmp(other.0.as_ref()))
	}
}

impl PartialOrd<String> for Symbol {
	#[inline]
	fn partial_cmp(&self, other: &String) -> Option<Ordering> {
		self.0.as_ref().partial_cmp(other.as_str())
	}
}

impl PartialOrd<&String> for Symbol {
	#[inline]
	fn partial_cmp(&self, other: &&String) -> Option<Ordering> {
		self.0.as_ref().partial_cmp(other.as_str())
	}
}

impl PartialOrd<&str> for Symbol {
	#[inline]
	fn partial_cmp(&self, other: &&str) -> Option<Ordering> {
		self.0.as_ref().partial_cmp(*other)
	}
}

impl Hash for Symbol {
	#[inline]
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.0.as_ref().hash(state)
	}
}

impl Debug for Symbol {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		std::fmt::Debug::fmt(&self.0, f)
	}
}

impl Display for Symbol {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		std::fmt::Display::fmt(&self.0, f)
	}
}

impl ToSql for Symbol {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(self.as_str());
	}
}

impl Revisioned for Symbol {
	#[inline]
	fn revision() -> u16 {
		1
	}
}

impl SerializeRevisioned for Symbol {
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(&self, writer: &mut W) -> Result<(), Error> {
		self.0.as_ref().serialize_revisioned(writer)
	}
}

impl DeserializeRevisioned for Symbol {
	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, Error> {
		Ok(Self::new(String::deserialize_revisioned(reader)?))
	}
}

impl<F> storekey::Encode<F> for Symbol {
	#[inline]
	fn encode<W: std::io::Write>(
		&self,
		writer: &mut storekey::Writer<W>,
	) -> Result<(), storekey::EncodeError> {
		<str as storekey::Encode<F>>::encode(self.0.as_ref(), writer)
	}
}

impl<'de, F> storekey::BorrowDecode<'de, F> for Symbol {
	#[inline]
	fn borrow_decode(
		reader: &mut storekey::BorrowReader<'de>,
	) -> Result<Self, storekey::DecodeError> {
		Ok(Self::new(<String as storekey::BorrowDecode<'de, F>>::borrow_decode(reader)?))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_basic_creation() {
		let s1 = Symbol::from("hello");
		let s2 = Symbol::from("hello");

		// Same string should share Arc
		assert!(Arc::ptr_eq(&s1.0, &s2.0));
		assert_eq!(s1.as_str(), "hello");
	}

	#[test]
	fn test_different_strings() {
		let s1 = Symbol::from("hello");
		let s2 = Symbol::from("world");

		// Different strings should have different Arcs
		assert!(!Arc::ptr_eq(&s1.0, &s2.0));
		assert_eq!(s1.as_str(), "hello");
		assert_eq!(s2.as_str(), "world");
	}

	#[test]
	fn test_equality() {
		let s1 = Symbol::from("test");
		let s2 = Symbol::from("test");
		let s3 = Symbol::from("other");

		assert_eq!(s1, s2);
		assert_ne!(s1, s3);
	}

	#[test]
	fn test_ordering() {
		let s1 = Symbol::from("apple");
		let s2 = Symbol::from("banana");
		let s3 = Symbol::from("cherry");

		assert!(s1 < s2);
		assert!(s2 < s3);
		assert!(s1 < s3);
	}

	#[test]
	fn test_hashing() {
		use std::collections::HashMap;

		let mut map = HashMap::new();
		let key = Symbol::from("key");
		map.insert(key.clone(), 42);

		assert_eq!(map.get(&Symbol::from("key")), Some(&42));
	}

	#[test]
	fn test_btreemap() {
		use std::collections::BTreeMap;

		let mut map = BTreeMap::new();
		map.insert(Symbol::from("z"), 1);
		map.insert(Symbol::from("a"), 2);
		map.insert(Symbol::from("m"), 3);

		let keys: Vec<_> = map.keys().map(|k| k.as_str()).collect();
		assert_eq!(keys, vec!["a", "m", "z"]);
	}

	#[test]
	fn test_serialization() {
		let s1 = Symbol::from("test_string");
		let mut bytes = Vec::new();
		s1.serialize_revisioned(&mut bytes).unwrap();

		let s2 = Symbol::deserialize_revisioned(&mut bytes.as_slice()).unwrap();
		assert_eq!(s1, s2);
	}

	#[test]
	fn test_cache_stats() {
		// Create some symbols
		for i in 0..10 {
			let _ = Symbol::from(format!("key_{}", i));
		}

		let stats = Symbol::cache_stats();
		assert!(stats.entries > 0);
		assert!(stats.entries <= stats.capacity);
		assert_eq!(stats.capacity, 4096);
	}

	#[test]
	fn test_lru_eviction() {
		// Fill cache beyond capacity
		for i in 0..5000 {
			let _ = Symbol::from(format!("key_{}", i));
		}

		// Cache should be at capacity
		let stats = Symbol::cache_stats();
		assert_eq!(stats.entries, stats.capacity);
	}

	#[test]
	fn test_deref() {
		let sym = Symbol::from("test");
		let s: &str = &sym;
		assert_eq!(s, "test");
		assert_eq!(sym.len(), 4);
		assert!(sym.starts_with("te"));
	}

	#[test]
	fn test_from_arc() {
		let arc: Arc<str> = Arc::from("from_arc");
		let sym = Symbol::from(arc.clone());
		assert!(Arc::ptr_eq(&sym.0, &arc));
	}
}
