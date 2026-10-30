//! Object store abstractions for bucket storage.
//!
//! This module defines the core traits and types for object storage operations:
//! - [`ObjectStore`] - The main trait that all storage backends implement
//! - [`ObjectKey`] - Normalized path representation for object keys
//! - [`ObjectMeta`] - Metadata about stored objects
//! - [`ListOptions`] - Options for listing objects in a bucket
//! - [`ListEntry`] - One entry of a listing: an object, or a group of them

use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;

use crate::expr::Error;
use crate::val::{CoerceError, Datetime, File, Number, Object, Value};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod cloud;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod file;
pub(crate) mod memory;
pub(crate) mod metered;
pub(crate) mod path;
pub(crate) mod prefixed;

// Expose type for external composers
pub use path::ObjectKey;

/// Metadata for a stored object.
///
/// Contains information about an object's size, last modification time, and key.
pub struct ObjectMeta {
	/// Size of the object in bytes
	pub size: u64,
	/// Last modification timestamp
	pub updated: DateTime<Utc>,
	/// The object's key (path)
	pub key: ObjectKey,
	/// The object's entity tag, when the backend has one.
	///
	/// Exposed by `file::head` and `file::list` so a client can tell whether an
	/// object changed since it last looked, and revalidate a download.
	///
	/// A `W/`-prefixed value is a *weak* validator. It is valid for
	/// `If-None-Match` revalidation and must not be used for `If-Match` or
	/// `If-Range`, which need byte-exact identity.
	pub etag: Option<String>,
	/// The media type stored with the object, when one was given.
	///
	/// Never inferred: an object stored without one has none, whatever its
	/// key's extension. Only a store whose
	/// [`supports_content_type`](ObjectStore::supports_content_type) holds can
	/// report one.
	pub content_type: Option<String>,
}

impl ObjectMeta {
	/// Converts the metadata into a SurrealDB `Value` for query results.
	///
	/// The returned value is an object with `updated`, `size` and `file`
	/// fields, `etag` when the backend supplies one, and `content_type` when
	/// one is stored.
	pub(crate) fn into_value(self, bucket: String) -> Value {
		Value::from(map! {
			"updated" => Value::from(Datetime(self.updated)),
			"size" => Value::from(self.size),
			"etag", if let Some(etag) = self.etag => Value::from(etag),
			"content_type", if let Some(content_type) = self.content_type => Value::from(content_type),
			"file" => Value::File(File {
				bucket,
				key: self.key.to_string(),
			})
		})
	}
}

/// A byte range requested from an object.
///
/// Mirrors the three forms an HTTP `Range` header can take, and maps directly
/// onto [`object_store::GetRange`] for the cloud backends.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ByteRange {
	/// `bytes=start-end`, with an exclusive end. An end past the object is
	/// clamped to it.
	Bounded(Range<u64>),
	/// `bytes=start-`. Everything from `start` to the end of the object.
	Offset(u64),
	/// `bytes=-n`. The last `n` bytes.
	Suffix(u64),
}

impl ByteRange {
	/// The concrete half-open range this resolves to for an object of `size`.
	///
	/// Returns `None` when the range cannot be satisfied, which an HTTP caller
	/// reports as `416 Range Not Satisfiable`. That covers a start at or past
	/// the end of the object, and any zero-length result. A `Suffix` longer
	/// than the object is not unsatisfiable: it clamps to the whole object,
	/// which is what RFC 9110 requires.
	pub fn resolve(&self, size: u64) -> Option<Range<u64>> {
		let range = match *self {
			ByteRange::Bounded(ref r) => {
				if r.start >= size {
					return None;
				}
				r.start..r.end.min(size)
			}
			ByteRange::Offset(start) => {
				if start >= size {
					return None;
				}
				start..size
			}
			ByteRange::Suffix(n) => size.saturating_sub(n)..size,
		};

		(range.start < range.end).then_some(range)
	}
}

/// An object opened for reading: its metadata, the range being returned, and
/// the bytes themselves.
///
/// The stream is `'static` so a caller can hand it to an HTTP body and return,
/// outliving its borrow of the store.
pub struct ObjectStream {
	/// Metadata for the whole object, not for the returned range.
	pub meta: ObjectMeta,
	/// The bytes this stream yields. Narrower than the object when a range was
	/// requested.
	pub range: Range<u64>,
	pub stream: BoxStream<'static, Result<Bytes, String>>,
}

/// Whether a streamed write may replace what is already at the key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutCondition {
	/// Replace any existing object.
	Overwrite,
	/// Commit only if the key is free, decided atomically with the commit.
	IfNotExists,
}

/// The media type that means "no type": what an object without one is served
/// as, and what HTTP clients send when they have nothing better to say.
pub const NO_CONTENT_TYPE: &str = "application/octet-stream";

/// Normalise a media type a caller asked to store.
///
/// [`NO_CONTENT_TYPE`] is the same as naming none, so it never reaches a
/// store, and a bucket on a backend that cannot hold a type still accepts it.
/// Anything else must be a `type/subtype` that is safe to send back as a
/// header: visible ASCII, spaces and tabs only, and no longer than 255 bytes.
/// Nothing is inferred and nothing is repaired; a value that fails is refused.
pub fn normalise_content_type(value: Option<String>) -> Result<Option<String>, String> {
	let Some(value) = value else {
		return Ok(None);
	};
	let trimmed = value.trim();
	let essence = trimmed.split(';').next().unwrap_or("").trim();
	if essence.eq_ignore_ascii_case(NO_CONTENT_TYPE) {
		return Ok(None);
	}
	let valid = !trimmed.is_empty()
		&& trimmed.len() <= 255
		&& trimmed.bytes().all(|b| b == b'\t' || (b' '..=b'~').contains(&b))
		&& essence.split_once('/').is_some_and(|(t, s)| {
			!t.is_empty() && !s.is_empty() && !t.contains(' ') && !s.contains(' ')
		});
	if !valid {
		return Err(format!("`{value}` is not a valid content type"));
	}
	Ok(Some(trimmed.to_owned()))
}

/// Read the options `file::put` and `file::put_if_not_exists` accept: only
/// `content_type`, normalised. Any other key is refused, so a misspelled
/// option is never silently ignored.
pub(crate) fn put_content_type(name: &str, opts: Option<Object>) -> Result<Option<String>, Error> {
	let invalid = |message: String| Error::InvalidFunctionArguments {
		name: name.to_owned(),
		message,
	};
	let Some(mut opts) = opts else {
		return Ok(None);
	};
	let content_type = match opts.remove("content_type") {
		None | Some(Value::None) => None,
		Some(v) => Some(v.coerce_to::<String>()?),
	};
	if let Some(unknown) = opts.keys().next() {
		return Err(invalid(format!("Unknown option `{unknown}`, expected: content_type")));
	}
	normalise_content_type(content_type).map_err(invalid)
}

/// Write `data` at `key` in one piece.
///
/// A write with a content type goes through [`ObjectStore::put_streamed`],
/// the one write that carries it; one without keeps the plain `put`. An
/// occupied key under [`PutCondition::IfNotExists`] is not an error, as for
/// `put_if_not_exists`.
pub(crate) async fn put_bytes(
	store: &dyn ObjectStore,
	key: &ObjectKey,
	data: Bytes,
	condition: PutCondition,
	content_type: Option<String>,
) -> Result<(), String> {
	if content_type.is_none() {
		return match condition {
			PutCondition::Overwrite => store.put(key, data).await,
			PutCondition::IfNotExists => store.put_if_not_exists(key, data).await,
		};
	}
	let opts = PutOptions {
		condition,
		content_type,
	};
	let body = Box::pin(futures::stream::once(async move { Ok(data) }));
	match store.put_streamed(key, body, opts).await {
		Ok(_) | Err(PutError::AlreadyExists) => Ok(()),
		Err(PutError::Failed(e)) => Err(e),
	}
}

/// How a streamed write commits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutOptions {
	pub condition: PutCondition,
	/// The media type to store with the object.
	///
	/// A store that cannot hold one refuses a write that carries one, rather
	/// than dropping it: a caller that names a type relies on it being served.
	pub content_type: Option<String>,
}

impl From<PutCondition> for PutOptions {
	fn from(condition: PutCondition) -> Self {
		Self {
			condition,
			content_type: None,
		}
	}
}

/// Why a streamed write did not commit.
///
/// Two variants rather than one string because the caller answers them
/// differently: an occupied key under [`PutCondition::IfNotExists`] is the
/// condition doing its job and is reported as `412`, while anything else is a
/// failure.
#[derive(Debug, thiserror::Error)]
pub enum PutError {
	/// The key was already occupied and the write was conditional.
	#[error("An object already exists at this key")]
	AlreadyExists,
	/// Any other failure, including an aborted stream.
	#[error("{0}")]
	Failed(String),
}

impl From<String> for PutError {
	fn from(e: String) -> Self {
		PutError::Failed(e)
	}
}

/// The outcome of a completed write.
#[derive(Debug)]
pub struct PutResult {
	/// Bytes actually written.
	pub size: u64,
	/// The stored object's entity tag, when the backend supplies one.
	pub etag: Option<String>,
}

/// The one delimiter a listing can group on.
///
/// `object_store` groups on `/` alone, and every key is a `/`-separated path,
/// so any other delimiter would be a promise only some backends could keep.
pub const LIST_DELIMITER: char = '/';

/// Options for listing objects in a bucket.
///
/// The semantics are the same on every backend, because each backend hands its
/// candidates to a [`Listing`], which applies them:
///
/// - `prefix` is a plain string prefix of the key, as in S3: `/b` matches `/b.txt` and `/b/c.txt`,
///   and `/b/` matches only the latter. It is not normalised, so a trailing `/` is kept.
/// - `delimiter` groups every key with a `/` after the prefix into one [`ListEntry::Prefix`], the
///   key up to and including that `/`. Without it, a listing is recursive.
/// - `start` is exclusive and compares against what an entry reports, so the last entry of one page
///   is the `start` of the next. When it names a group, every key in that group is skipped too.
/// - `limit` counts entries, groups included.
#[derive(Clone, Debug, Default)]
pub struct ListOptions {
	/// List the entries after this one (exclusive), used for pagination.
	pub start: Option<String>,
	/// List only keys starting with this string. Always begins with `/`.
	pub prefix: Option<String>,
	/// Group keys on [`LIST_DELIMITER`] after the prefix.
	pub delimiter: bool,
	/// Maximum number of entries to return.
	pub limit: Option<usize>,
}

impl ListOptions {
	/// The prefix to match, `/` when none was given.
	pub fn prefix(&self) -> &str {
		self.prefix.as_deref().unwrap_or("/")
	}

	/// The deepest directory that holds every key the prefix can match: the
	/// prefix up to and including its last `/`.
	///
	/// A backend lists beneath this and leaves the rest of the prefix to the
	/// [`Listing`], which is what lets a prefix end mid-name.
	pub fn directory(&self) -> &str {
		let prefix = self.prefix();
		match prefix.rfind(LIST_DELIMITER) {
			Some(i) => &prefix[..=i],
			None => "/",
		}
	}

	/// Accept a prefix or start key as written, adding the leading `/` every
	/// key carries.
	///
	/// Not normalised through [`ObjectKey`]: that drops a trailing `/`, which
	/// is the difference between `/b` and `/b/`. A `.` or `..` segment is
	/// refused, since the filesystem backend would resolve it.
	fn key_like(field: &str, value: String) -> Result<String, Error> {
		let value = if value.starts_with('/') {
			value
		} else {
			format!("/{value}")
		};
		if value.split(['/', '\\']).any(|s| s == ".." || s == ".") {
			return Err(Error::InvalidFunctionArguments {
				name: "file::list".to_owned(),
				message: format!("The `{field}` option cannot contain a `.` or `..` segment"),
			});
		}
		Ok(value)
	}
}

impl ListOptions {
	/// Build options from their parts, validated as `file::list` validates
	/// them. Shared by every caller that is not handed a SurrealQL object, so
	/// the HTTP route and the function accept exactly the same values.
	pub fn from_parts(
		prefix: Option<String>,
		delimiter: Option<&str>,
		start: Option<String>,
		limit: Option<usize>,
	) -> Result<Self, Error> {
		let delimiter = match delimiter {
			None => false,
			Some(d) if d.len() == 1 && d.starts_with(LIST_DELIMITER) => true,
			Some(d) => {
				return Err(Error::InvalidFunctionArguments {
					name: "file::list".to_owned(),
					message: format!(
						"The `delimiter` option must be \"{LIST_DELIMITER}\", not \"{d}\""
					),
				});
			}
		};
		Ok(Self {
			start: start.map(|s| Self::key_like("start", s)).transpose()?,
			prefix: prefix.map(|p| Self::key_like("prefix", p)).transpose()?,
			delimiter,
			limit,
		})
	}
}

impl TryFrom<Object> for ListOptions {
	type Error = Error;
	fn try_from(mut obj: Object) -> Result<Self, Self::Error> {
		// A file pointer is accepted as well as a string, so the `file` of the
		// last entry on a page can be passed back as it is.
		let start = obj
			.remove("start")
			.map(|start| {
				if let Value::File(file) = start {
					Ok(file.key)
				} else {
					start.coerce_to::<String>()
				}
			})
			.transpose()?;
		let prefix = obj.remove("prefix").map(|p| p.coerce_to::<String>()).transpose()?;
		let delimiter = obj.remove("delimiter").map(|d| d.coerce_to::<String>()).transpose()?;
		let limit = obj
			.remove("limit")
			.map(|limit| {
				let n = limit.coerce_to::<i64>()?;
				usize::try_from(n).map_err(|_| CoerceError::InvalidKind {
					from: Value::Number(Number::Int(n)),
					into: "non-negative int".into(),
				})
			})
			.transpose()?;
		Self::from_parts(prefix, delimiter.as_deref(), start, limit)
	}
}

/// One entry of a listing.
pub enum ListEntry {
	/// A stored object.
	Object(ObjectMeta),
	/// Every key that shares this prefix up to the next delimiter, which the
	/// prefix ends with: `/u/tobie/`. Only produced with a delimiter.
	Prefix(String),
}

impl ListEntry {
	/// The key the entry sorts and paginates by.
	pub fn key(&self) -> &str {
		match self {
			ListEntry::Object(meta) => meta.key.as_str(),
			ListEntry::Prefix(prefix) => prefix,
		}
	}

	/// Converts the entry into a SurrealDB `Value` for query results.
	///
	/// An object becomes what [`ObjectMeta::into_value`] makes; a group
	/// becomes `{ prefix }`, whose value can be passed back as the `prefix`
	/// of a listing one level down.
	pub(crate) fn into_value(self, bucket: String) -> Value {
		match self {
			ListEntry::Object(meta) => meta.into_value(bucket),
			ListEntry::Prefix(prefix) => Value::from(map! {
				"prefix" => Value::from(prefix),
			}),
		}
	}
}

/// Applies [`ListOptions`] to a backend's candidates.
///
/// A backend pushes what it found beneath [`ListOptions::directory`], in key
/// order, and stops when a push returns `false`. It may push an object that
/// does not match the prefix, or a group it found without reading the objects
/// in it; everything else is decided here, so no backend can disagree with
/// another about what a listing holds.
pub struct Listing<'a> {
	opts: &'a ListOptions,
	entries: Vec<ListEntry>,
}

impl<'a> Listing<'a> {
	pub fn new(opts: &'a ListOptions) -> Self {
		Self {
			opts,
			entries: Vec::new(),
		}
	}

	/// Whether the listing has all the entries it may return.
	pub fn is_full(&self) -> bool {
		self.opts.limit.is_some_and(|limit| self.entries.len() >= limit)
	}

	/// Offer an object. Returns `false` once the listing is full.
	pub fn push_object(&mut self, meta: ObjectMeta) -> bool {
		if self.is_full() {
			return false;
		}
		let prefix = self.opts.prefix();
		let key = meta.key.as_str();
		if !key.starts_with(prefix) {
			return true;
		}
		if self.opts.delimiter
			&& let Some(i) = key[prefix.len()..].find(LIST_DELIMITER)
		{
			let group = key[..prefix.len() + i + 1].to_owned();
			return self.push_prefix(group);
		}
		if self.before_start(key) {
			return true;
		}
		// A listing never reports a content type. S3, GCS and Azure listings
		// do not carry one, so reporting it from the stores that could would
		// make the answer depend on the backend.
		self.entries.push(ListEntry::Object(ObjectMeta {
			content_type: None,
			..meta
		}));
		!self.is_full()
	}

	/// Offer a group. Returns `false` once the listing is full.
	///
	/// A group is reported once however many objects it holds, which is why
	/// candidates must arrive in key order: every key in a group sorts after
	/// the group itself and before the first key outside it.
	pub fn push_prefix(&mut self, group: String) -> bool {
		if self.is_full() {
			return false;
		}
		if !self.opts.delimiter || !group.starts_with(self.opts.prefix()) {
			return true;
		}
		if self.entries.last().is_some_and(|last| last.key() == group) || self.before_start(&group)
		{
			return true;
		}
		self.entries.push(ListEntry::Prefix(group));
		!self.is_full()
	}

	/// Whether `key` is at or before `start`, or inside the group `start`
	/// names. The second is what stops the next page repeating a group.
	fn before_start(&self, key: &str) -> bool {
		self.opts.start.as_deref().is_some_and(|start| {
			key <= start
				|| (self.opts.delimiter
					&& start.ends_with(LIST_DELIMITER)
					&& key.starts_with(start))
		})
	}

	pub fn finish(self) -> Vec<ListEntry> {
		self.entries
	}
}

/// Trait for object storage backends.
///
/// This trait defines the core operations that all object storage implementations
/// must provide. Implementations include in-memory storage, local filesystem,
/// and cloud storage backends (S3, GCS, Azure Blob Storage, etc.).
///
/// All methods return boxed futures to allow for async operations and trait object usage.
pub trait ObjectStore: Send + Sync + 'static {
	/// Stores data at the specified key, overwriting any existing data.
	fn put<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	/// Stores data at the specified key only if the key does not already exist.
	fn put_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	/// Streams `data` to the specified key.
	///
	/// Implementations must not buffer the whole stream where the backend can
	/// avoid it, and must uphold two contracts.
	///
	/// **If the stream yields an error, leave no object at `key`.**
	///
	/// A caller aborts an upload by erroring the stream — that is how a client
	/// disconnect and a breached size limit both arrive here — so a partial
	/// object must never become visible, and a backend that holds interim state
	/// must release it. On S3 and GCS an unfinished multipart upload is billed
	/// until a lifecycle rule reaps it, and on a local filesystem a truncated
	/// file at a live key reads as a whole object.
	///
	/// **With [`PutCondition::IfNotExists`], do not replace an existing
	/// object, and decide that as part of the commit.**
	///
	/// A caller cannot uphold this itself: an `exists` check followed by a
	/// write is two operations, and two uploads that both find the key free
	/// will both take it. The condition exists because a presigned URL can
	/// carry `overwrite: false`, which is what makes it safe to hand to an
	/// untrusted third party, so the promise has to be kept where the commit
	/// happens. An implementation that finds the key occupied returns
	/// [`PutError::AlreadyExists`] and leaves the existing object untouched.
	///
	/// **With a content type, store it, or refuse before writing anything.**
	/// A store whose [`supports_content_type`](Self::supports_content_type)
	/// is `false` returns [`PutError::Failed`] without touching the key.
	fn put_streamed<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: BoxStream<'static, Result<Bytes, String>>,
		opts: PutOptions,
	) -> Pin<Box<dyn Future<Output = Result<PutResult, PutError>> + Send + 'a>>;

	/// Whether the store can keep a media type with an object.
	///
	/// A caller checks this before it accepts a body, so a write that names a
	/// type is refused before any bytes are read rather than after.
	fn supports_content_type(&self) -> bool;

	/// Retrieves data from the specified key.
	///
	/// Returns `Ok(None)` if the key does not exist.
	fn get<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>>;

	/// Opens the object at `key` for streamed reading.
	///
	/// Returns `Ok(None)` if the key does not exist, or if `range` cannot be
	/// satisfied for the object that is there.
	///
	/// Implementations must not buffer the whole object: the point of this
	/// method is that a caller can serve an object larger than the memory it
	/// has. The returned stream is `'static`, so it may be handed to an HTTP
	/// body and polled after the store reference is gone.
	///
	/// There is deliberately no default implementation. Every wrapper in this
	/// module forwards each method by hand, and a default body would compile in
	/// all of them while silently disabling streaming for the store underneath.
	fn get_streamed<'a>(
		&'a self,
		key: &'a ObjectKey,
		range: Option<ByteRange>,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectStream>, String>> + Send + 'a>>;

	/// Retrieves metadata for the specified key without fetching the data.
	///
	/// Returns `Ok(None)` if the key does not exist.
	fn head<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>>;

	/// Deletes the data at the specified key.
	///
	/// This operation is idempotent - deleting a non-existent key is not an error.
	fn delete<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	/// Checks whether data exists at the specified key.
	fn exists<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>>;

	/// Copies data from one key to another, overwriting the target if it exists.
	fn copy<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	/// Copies data from one key to another only if the target does not exist.
	fn copy_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	/// Moves data from one key to another, overwriting the target if it exists.
	fn rename<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	/// Moves data from one key to another only if the target does not exist.
	fn rename_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	/// Lists the entries matching the specified options.
	///
	/// Results are returned in lexicographical order by key. An implementation
	/// applies the options through a [`Listing`].
	fn list<'a>(
		&'a self,
		opts: &'a ListOptions,
	) -> Pin<Box<dyn Future<Output = Result<Vec<ListEntry>, String>> + Send + 'a>>;
}

impl ObjectStore for Arc<dyn ObjectStore> {
	fn put<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).put(key, data)
	}

	fn put_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).put_if_not_exists(key, data)
	}

	fn put_streamed<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: BoxStream<'static, Result<Bytes, String>>,
		opts: PutOptions,
	) -> Pin<Box<dyn Future<Output = Result<PutResult, PutError>> + Send + 'a>> {
		(**self).put_streamed(key, data, opts)
	}

	fn supports_content_type(&self) -> bool {
		(**self).supports_content_type()
	}

	fn get<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>> {
		(**self).get(key)
	}

	fn get_streamed<'a>(
		&'a self,
		key: &'a ObjectKey,
		range: Option<ByteRange>,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectStream>, String>> + Send + 'a>> {
		(**self).get_streamed(key, range)
	}

	fn head<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>> {
		(**self).head(key)
	}

	fn delete<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).delete(key)
	}

	fn exists<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>> {
		(**self).exists(key)
	}

	fn copy<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).copy(key, target)
	}

	fn copy_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).copy_if_not_exists(key, target)
	}

	fn rename<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).rename(key, target)
	}

	fn rename_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).rename_if_not_exists(key, target)
	}

	fn list<'a>(
		&'a self,
		opts: &'a ListOptions,
	) -> Pin<Box<dyn Future<Output = Result<Vec<ListEntry>, String>> + Send + 'a>> {
		(**self).list(opts)
	}
}

/// Drives every backend over the same [`ObjectStore::get_streamed`] contract.
///
/// The three stores reach the bytes in entirely different ways — a refcounted
/// slice, a seek and a bounded reader, a ranged HTTP request — so the contract
/// is asserted once here and called from each backend's own tests rather than
/// restated three times.
#[cfg(test)]
pub(crate) async fn assert_get_streamed_contract(store: &dyn ObjectStore) {
	use futures::TryStreamExt;

	async fn read(
		store: &dyn ObjectStore,
		key: &ObjectKey,
		range: Option<ByteRange>,
	) -> Option<(Vec<u8>, Range<u64>, u64)> {
		let res = store.get_streamed(key, range).await.expect("get_streamed failed")?;
		let range = res.range.clone();
		let size = res.meta.size;
		let chunks: Vec<Bytes> = res.stream.try_collect().await.expect("stream failed");
		Some((chunks.concat(), range, size))
	}

	let key = ObjectKey::new("/contract.bin");
	let body: Vec<u8> = (0u8..=255).collect();
	store.put(&key, Bytes::from(body.clone())).await.expect("put failed");

	let (got, range, size) = read(store, &key, None).await.expect("object is absent");
	assert_eq!(got, body, "a rangeless read must yield the whole object");
	assert_eq!(range, 0..256);
	assert_eq!(size, 256);

	let (got, range, _) = read(store, &key, Some(ByteRange::Bounded(10..20))).await.unwrap();
	assert_eq!(got, body[10..20], "Bounded must yield an exclusive end");
	assert_eq!(range, 10..20);

	let (got, range, _) = read(store, &key, Some(ByteRange::Offset(250))).await.unwrap();
	assert_eq!(got, body[250..], "Offset must run to the end of the object");
	assert_eq!(range, 250..256);

	let (got, range, _) = read(store, &key, Some(ByteRange::Suffix(6))).await.unwrap();
	assert_eq!(got, body[250..], "Suffix must yield the last n bytes");
	assert_eq!(range, 250..256);

	// RFC 9110: a suffix longer than the object clamps to the whole object
	// rather than failing.
	let (got, _, _) = read(store, &key, Some(ByteRange::Suffix(9999))).await.unwrap();
	assert_eq!(got, body, "an oversized Suffix must clamp, not fail");

	// An end past the object clamps; only a start past it is unsatisfiable.
	let (got, range, _) = read(store, &key, Some(ByteRange::Bounded(250..9999))).await.unwrap();
	assert_eq!(got, body[250..]);
	assert_eq!(range, 250..256);

	assert!(
		read(store, &key, Some(ByteRange::Bounded(256..300))).await.is_none(),
		"a start at the end of the object is unsatisfiable"
	);
	assert!(
		read(store, &key, Some(ByteRange::Offset(9999))).await.is_none(),
		"a start past the end of the object is unsatisfiable"
	);

	assert!(
		read(store, &ObjectKey::new("/absent.bin"), None).await.is_none(),
		"a missing key reads as absent, not as an error"
	);

	// An empty object has no satisfiable range, but still reads whole.
	let empty = ObjectKey::new("/empty.bin");
	store.put(&empty, Bytes::new()).await.expect("put failed");
	let (got, range, size) = read(store, &empty, None).await.expect("empty object is absent");
	assert!(got.is_empty());
	assert_eq!(range, 0..0);
	assert_eq!(size, 0);
	assert!(read(store, &empty, Some(ByteRange::Offset(0))).await.is_none());
}

/// Drives every backend over the same [`ObjectStore::put_streamed`] contract.
///
/// The abort case is the reason this is shared rather than written per backend:
/// each store fails differently without it — an un-aborted multipart upload on
/// S3, a truncated file at a live key on a local disk — and all three have to
/// end up in the same state.
#[cfg(test)]
pub(crate) async fn assert_put_streamed_contract(store: &dyn ObjectStore) {
	use futures::stream;

	fn chunks(parts: &[&[u8]]) -> BoxStream<'static, Result<Bytes, String>> {
		let owned: Vec<Result<Bytes, String>> =
			parts.iter().map(|p| Ok(Bytes::copy_from_slice(p))).collect();
		Box::pin(stream::iter(owned))
	}

	let key = ObjectKey::new("/put-contract.bin");

	let res = store
		.put_streamed(
			&key,
			chunks(&[b"hello ", b"streamed ", b"world"]),
			PutCondition::Overwrite.into(),
		)
		.await
		.expect("put_streamed failed");
	assert_eq!(res.size, 20, "size must count every byte of every chunk");
	assert_eq!(
		store.get(&key).await.unwrap().as_deref(),
		Some(&b"hello streamed world"[..]),
		"the chunks must be concatenated in order"
	);

	// An empty stream is a valid write of an empty object, not a no-op.
	let empty = ObjectKey::new("/put-empty.bin");
	let res = store
		.put_streamed(&empty, Box::pin(stream::empty()), PutCondition::Overwrite.into())
		.await
		.unwrap();
	assert_eq!(res.size, 0);
	assert!(store.exists(&empty).await.unwrap(), "an empty stream still creates the object");

	// The contract: a stream that errors leaves no object at the key, and
	// leaves any object already there untouched.
	let aborted: BoxStream<'static, Result<Bytes, String>> = Box::pin(stream::iter(vec![
		Ok(Bytes::from_static(b"partial")),
		Err("client went away".to_string()),
	]));
	store
		.put_streamed(&key, aborted, PutCondition::Overwrite.into())
		.await
		.expect_err("an errored stream must fail the write");
	assert_eq!(
		store.get(&key).await.unwrap().as_deref(),
		Some(&b"hello streamed world"[..]),
		"an aborted write must not replace the object that was there"
	);

	// A write changes the tag; a `head` afterwards reports the same tag the
	// write did. Without both, revalidation either never invalidates or never
	// hits.
	let tagged = ObjectKey::new("/put-tagged.bin");
	let first = store
		.put_streamed(&tagged, chunks(&[b"one"]), PutCondition::Overwrite.into())
		.await
		.unwrap();
	let head = store.head(&tagged).await.unwrap().expect("object is absent");
	assert!(first.etag.is_some(), "every backend supplies an etag");
	assert_eq!(first.etag, head.etag, "a write and a later head must agree");

	let second = store
		.put_streamed(&tagged, chunks(&[b"two"]), PutCondition::Overwrite.into())
		.await
		.unwrap();
	if first.etag.as_deref().is_some_and(|tag| !tag.starts_with("W/")) {
		// Only a strong validator promises this. A weak one may repeat: the
		// local store derives its tag from size and mtime, and these two writes
		// share a size, so a filesystem with coarse mtime granularity gives them
		// the same tag. Asserting it for every backend would be a flaky test
		// that contradicts what `W/` means.
		assert_ne!(first.etag, second.etag, "a strong etag must change with the object");
	}

	// `IfNotExists` refuses an occupied key and leaves it exactly as it was.
	// Every backend has to reach this answer as part of its commit rather than
	// through a check the caller makes first, which is the only way two
	// redemptions of one write-once URL cannot both succeed.
	let taken = ObjectKey::new("/put-conditional.bin");
	store
		.put_streamed(&taken, chunks(&[b"first"]), PutCondition::IfNotExists.into())
		.await
		.expect("a conditional write to a free key must succeed");
	let refused = store
		.put_streamed(&taken, chunks(&[b"second"]), PutCondition::IfNotExists.into())
		.await
		.expect_err("a conditional write to an occupied key must be refused");
	assert!(
		matches!(refused, PutError::AlreadyExists),
		"an occupied key is `AlreadyExists`, not a generic failure: {refused:?}"
	);
	assert_eq!(
		store.get(&taken).await.unwrap().as_deref(),
		Some(&b"first"[..]),
		"a refused conditional write must not touch the object that was there"
	);

	// An aborted conditional write must not leave the key claimed, or a
	// disconnect would permanently block the key it was reserving.
	let reserved = ObjectKey::new("/put-conditional-aborted.bin");
	let aborted: BoxStream<'static, Result<Bytes, String>> = Box::pin(stream::iter(vec![
		Ok(Bytes::from_static(b"partial")),
		Err("client went away".to_string()),
	]));
	store
		.put_streamed(&reserved, aborted, PutCondition::IfNotExists.into())
		.await
		.expect_err("an errored stream must fail");
	assert!(
		!store.exists(&reserved).await.unwrap(),
		"an aborted conditional write must leave the key free"
	);

	let fresh = ObjectKey::new("/put-aborted.bin");
	let aborted: BoxStream<'static, Result<Bytes, String>> = Box::pin(stream::iter(vec![
		Ok(Bytes::from_static(b"partial")),
		Err("client went away".to_string()),
	]));
	store
		.put_streamed(&fresh, aborted, PutCondition::Overwrite.into())
		.await
		.expect_err("an errored stream must fail");
	assert!(
		!store.exists(&fresh).await.unwrap(),
		"an aborted write must not leave a partial object at a fresh key"
	);

	// A content type is stored and read back by every read that reports
	// metadata, or the write is refused and leaves the key untouched.
	let typed = ObjectKey::new("/put-typed.png");
	let opts = PutOptions {
		condition: PutCondition::Overwrite,
		content_type: Some("image/png".to_owned()),
	};
	let res = store.put_streamed(&typed, chunks(&[b"png"]), opts).await;
	if store.supports_content_type() {
		res.expect("a store that supports content types must accept one");
		let head = store.head(&typed).await.unwrap().expect("the object exists");
		assert_eq!(head.content_type.as_deref(), Some("image/png"), "head must report the type");
		let read = store.get_streamed(&typed, None).await.unwrap().expect("the object exists");
		assert_eq!(read.meta.content_type.as_deref(), Some("image/png"), "a read must report it");
		store.copy(&typed, &ObjectKey::new("/put-typed-copy.png")).await.unwrap();
		let copy = store.head(&ObjectKey::new("/put-typed-copy.png")).await.unwrap().unwrap();
		assert_eq!(copy.content_type.as_deref(), Some("image/png"), "a copy keeps the type");
		// A write without one replaces the object, type and all.
		store
			.put_streamed(&typed, chunks(&[b"raw"]), PutCondition::Overwrite.into())
			.await
			.unwrap();
		assert_eq!(store.head(&typed).await.unwrap().unwrap().content_type, None);
	} else {
		assert!(
			matches!(res, Err(PutError::Failed(_))),
			"a store that cannot hold a content type must refuse one"
		);
		assert!(!store.exists(&typed).await.unwrap(), "a refused write must leave nothing");
	}
}

/// One set of listings every backend must answer alike.
///
/// Each backend's tests call [`list_conformance::check`] on an empty store of
/// their own. Agreement is the point: each backend hands its candidates to the
/// same [`Listing`], and this is what notices one that hands over the wrong ones.
#[cfg(test)]
pub(crate) mod list_conformance {
	use std::sync::Arc;

	use bytes::Bytes;

	use super::prefixed::PrefixedStore;
	use super::{ListEntry, ListOptions, ObjectKey, ObjectStore};

	const KEYS: [&str; 6] =
		["/a.txt", "/b.txt", "/b/c.txt", "/b/d/e.txt", "/bz.txt", "/u/tobie/x.png"];

	fn opts(
		prefix: Option<&str>,
		delimiter: bool,
		start: Option<&str>,
		limit: Option<usize>,
	) -> ListOptions {
		ListOptions {
			prefix: prefix.map(str::to_owned),
			delimiter,
			start: start.map(str::to_owned),
			limit,
		}
	}

	/// The options, and the keys they list. A group is spelled with its
	/// trailing `/`.
	fn cases() -> Vec<(ListOptions, Vec<&'static str>)> {
		vec![
			(opts(None, false, None, None), KEYS.to_vec()),
			(
				opts(Some("/b"), false, None, None),
				vec!["/b.txt", "/b/c.txt", "/b/d/e.txt", "/bz.txt"],
			),
			(opts(Some("/b/"), false, None, None), vec!["/b/c.txt", "/b/d/e.txt"]),
			(opts(None, true, None, None), vec!["/a.txt", "/b.txt", "/b/", "/bz.txt", "/u/"]),
			(opts(Some("/b"), true, None, None), vec!["/b.txt", "/b/", "/bz.txt"]),
			(opts(Some("/b/"), true, None, None), vec!["/b/c.txt", "/b/d/"]),
			(opts(Some("/u/to"), true, None, None), vec!["/u/tobie/"]),
			(opts(Some("/nothing"), true, None, None), vec![]),
			// Paging by the last entry of the previous page, including a group.
			(opts(None, true, None, Some(3)), vec!["/a.txt", "/b.txt", "/b/"]),
			(opts(None, true, Some("/b/"), Some(3)), vec!["/bz.txt", "/u/"]),
			(opts(None, false, Some("/b/c.txt"), Some(2)), vec!["/b/d/e.txt", "/bz.txt"]),
			(opts(None, false, None, Some(0)), vec![]),
		]
	}

	async fn fill(store: &dyn ObjectStore) {
		for key in KEYS {
			store.put(&ObjectKey::new(key), Bytes::from_static(b"x")).await.unwrap();
		}
	}

	async fn assert_cases(name: &str, store: &dyn ObjectStore) {
		for (opts, expected) in cases() {
			let listed = store.list(&opts).await.unwrap();
			let keys: Vec<&str> = listed.iter().map(ListEntry::key).collect();
			assert_eq!(keys, expected, "{name} disagrees for {opts:?}");
		}
	}

	/// Fill an empty `store` and check every case against it.
	pub(crate) async fn check(name: &str, store: &dyn ObjectStore) {
		fill(store).await;
		assert_cases(name, store).await;
	}

	/// As [`check`], through a [`PrefixedStore`] over an empty `shared`, the
	/// way a global bucket holds many buckets. A sibling whose name extends
	/// this bucket's is written alongside and must never appear.
	pub(crate) async fn check_prefixed(name: &str, shared: Arc<dyn ObjectStore>) {
		let sibling = ObjectKey::new("/1/1/logs-archive/leak.txt");
		shared.put(&sibling, Bytes::from_static(b"x")).await.unwrap();
		let store = PrefixedStore::new(shared, ObjectKey::new("/1/1/logs"));
		check(name, &store).await;
	}

	#[tokio::test]
	async fn memory() {
		check("memory", &super::memory::MemoryStore::new()).await;
		check_prefixed("prefixed memory", Arc::new(super::memory::MemoryStore::new())).await;
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use super::{ByteRange, ListOptions, Object, Value};

	#[test]
	fn resolve_clamps_an_end_past_the_object() {
		assert_eq!(ByteRange::Bounded(2..99).resolve(10), Some(2..10));
	}

	#[test]
	fn resolve_rejects_a_start_at_or_past_the_end() {
		assert_eq!(ByteRange::Bounded(10..20).resolve(10), None);
		assert_eq!(ByteRange::Offset(10).resolve(10), None);
		assert_eq!(ByteRange::Offset(11).resolve(10), None);
	}

	#[test]
	fn resolve_rejects_an_empty_range() {
		assert_eq!(ByteRange::Bounded(5..5).resolve(10), None);
		assert_eq!(ByteRange::Suffix(0).resolve(10), None);
	}

	#[test]
	fn resolve_clamps_an_oversized_suffix() {
		// RFC 9110 requires a suffix longer than the object to yield the whole
		// object, not `416`.
		assert_eq!(ByteRange::Suffix(99).resolve(10), Some(0..10));
	}

	#[test]
	fn resolve_finds_nothing_in_an_empty_object() {
		assert_eq!(ByteRange::Offset(0).resolve(0), None);
		assert_eq!(ByteRange::Suffix(1).resolve(0), None);
		assert_eq!(ByteRange::Bounded(0..1).resolve(0), None);
	}

	fn obj(entries: &[(&'static str, Value)]) -> Object {
		let map: BTreeMap<&str, Value> = entries.iter().cloned().collect();
		Object::from(map)
	}

	#[test]
	fn limit_accepts_zero() {
		let opts = ListOptions::try_from(obj(&[("limit", Value::from(0i64))]))
			.expect("limit=0 should be accepted");
		assert_eq!(opts.limit, Some(0));
	}

	#[test]
	fn limit_accepts_positive() {
		let opts = ListOptions::try_from(obj(&[("limit", Value::from(42i64))]))
			.expect("limit=42 should be accepted");
		assert_eq!(opts.limit, Some(42));
	}

	#[test]
	fn prefix_keeps_its_trailing_slash() {
		// `/b/` and `/b` are different prefixes: the first excludes `/b.txt`.
		let opts = ListOptions::try_from(obj(&[("prefix", Value::from("/b/"))])).unwrap();
		assert_eq!(opts.prefix.as_deref(), Some("/b/"));
		assert_eq!(opts.directory(), "/b/");

		let opts = ListOptions::try_from(obj(&[("prefix", Value::from("u/to"))])).unwrap();
		assert_eq!(opts.prefix.as_deref(), Some("/u/to"));
		assert_eq!(opts.directory(), "/u/");
	}

	#[test]
	fn prefix_and_start_refuse_traversal() {
		for (field, value) in [("prefix", "/a/../b"), ("start", "/./x"), ("prefix", "/a\\..\\b")] {
			let err = ListOptions::try_from(obj(&[(field, Value::from(value))])).unwrap_err();
			assert!(err.to_string().contains("segment"), "{field}={value}: {err}");
		}
	}

	#[test]
	fn start_accepts_a_file() {
		let file = Value::File(crate::val::File {
			bucket: "images".to_owned(),
			key: "/u/a.png".to_owned(),
		});
		let opts = ListOptions::try_from(obj(&[("start", file)])).unwrap();
		assert_eq!(opts.start.as_deref(), Some("/u/a.png"));
	}

	#[test]
	fn delimiter_accepts_only_a_slash() {
		let opts = ListOptions::try_from(obj(&[("delimiter", Value::from("/"))])).unwrap();
		assert!(opts.delimiter);
		for bad in ["", ",", "//"] {
			let err = ListOptions::try_from(obj(&[("delimiter", Value::from(bad))])).unwrap_err();
			assert!(err.to_string().contains("must be \"/\""), "{bad:?}: {err}");
		}
	}

	#[test]
	fn content_type_is_normalised_not_inferred() {
		use super::normalise_content_type as n;
		assert_eq!(n(None), Ok(None));
		// The "no type" type is the same as naming none, parameters included.
		assert_eq!(n(Some("application/octet-stream".into())), Ok(None));
		assert_eq!(n(Some("Application/Octet-Stream; x=1".into())), Ok(None));
		assert_eq!(n(Some(" image/png ".into())), Ok(Some("image/png".into())));
		assert_eq!(
			n(Some("text/plain; charset=utf-8".into())),
			Ok(Some("text/plain; charset=utf-8".into()))
		);
		for bad in
			["", "png", "/png", "image/", "image /png", "text/plain\r\nX-Evil: 1", "tëxt/plain"]
		{
			assert!(n(Some(bad.into())).is_err(), "{bad:?} must be refused");
		}
		assert!(n(Some(format!("a/{}", "b".repeat(300)))).is_err(), "an overlong type is refused");
	}

	#[test]
	fn limit_rejects_negative() {
		let err = ListOptions::try_from(obj(&[("limit", Value::from(-1i64))]))
			.expect_err("negative limit should be rejected");
		assert!(err.to_string().contains("non-negative int"), "unexpected error message: {err}");
	}
}
