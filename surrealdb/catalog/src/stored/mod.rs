//! Catalog entries in the stored-value grammar.
//!
//! A catalog entry is a keyed map from field number to value, read and written through the one
//! schema its type declares (see `surrealdb_expr::val::stored::schema`), inside an envelope that
//! records the oldest release able to read it ([`entry`]).
//!
//! # Dispatch
//!
//! The first byte tells the two grammars apart. `0xFE` ([`CATALOG_MARKER`]) starts a stored-grammar
//! entry, and no `revision` value can start with it, so every other first byte is read as
//! `revision` exactly as 3.x wrote it. A `revision` entry is read bounded and whole: its lengths
//! are proved against the bytes present and trailing bytes are refused.
//!
//! # Feature level
//!
//! A store records the newest release whose readers its writes must stay readable by (its feature
//! level), and a store with no record is at the Legacy level. At Legacy a write is `revision`, and
//! a value that `revision` cannot hold (a non-default field the `revision` form leaves out) is
//! refused with the release it needs, never written with the field dropped. Above Legacy a write is
//! a stored-grammar entry whose floor does not exceed the level. An entry is rewritten in the
//! stored grammar only by an ordinary write of that entry. Every write of an entry keeps the
//! unknown ignorable fields of the entry it replaces, and a level never writes `revision` over a
//! stored entry.
//!
//! # Refusals
//!
//! An entry whose floor is above this build's release is refused with [`EntryError::NeedsRelease`],
//! naming the release it needs. A field this build does not know is skipped when its number is odd
//! and refused when it is even.
//!
//! # Embedded entries
//!
//! Two `revision` containers hold a copy of a definition: a changefeed's table mutation holds a
//! table, and an async-event record holds an event. The copy is an [`EmbeddedEntry`], which is the
//! type's `revision` bytes when they hold it exactly and a length-framed entry otherwise.
//!
//! # Contents
//!
//! - [`entry`]: the envelope, the encoder and the two decoders.
//! - [`embedded`]: a catalog entry inside a `revision` container.
//! - [`error`]: the typed refusals.
//! - [`vtable`]: the type table the VM's catalog codec reads and writes through.
//! - [`lock`]: the schema lock that freezes field numbers, and its rules.
//! - `impl_kv_value_catalog!` and `impl_kv_value_stored!`: the `KVValue` impls.

pub mod embedded;
pub mod entry;
pub mod error;
pub mod lock;
mod macros;
pub mod vtable;

pub use embedded::EmbeddedEntry;
pub use entry::{
	CATALOG_MARKER, canonical, decode_entry, decode_entry_or_revision, encode_entry, encode_stored,
	parse_header, write_error,
};
pub use error::EntryError;
pub(crate) use macros::impl_stored_id;
pub use surrealdb_expr::val::stored::schema::{
	Floor, SchemaError, Stored, StoredField, StoredType, TypeSchema,
};
pub use vtable::{CATALOG_TYPES, CatalogVTable};

/// What the macros of this module name, so that the crates that use them need no further
/// dependency.
#[doc(hidden)]
pub mod private {
	pub use anyhow;
	pub use surrealdb_kvs::value::{EncodeContext, Encoded, KVValue};
}
