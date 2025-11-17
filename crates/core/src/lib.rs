// Temporary allow deprecated until the 3.0
#![allow(deprecated)]
// This triggers because we have regex's in or Value type which have a unsafecell inside.
#![allow(clippy::mutable_key_type)]

//! # Surrealdb Core
//!
//! This crate is the internal core library of SurrealDB.
//! It contains most of the database functionality on top of which the surreal
//! binary is implemented.
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to semver
//! and it's API is free to change and break code even between patch versions.
//! If you are looking for a stable interface to the Surrealdb library please have a look at <a href="https://crates.io/crates/surrealdb">the rust SDK</a>
//! </section>

#![doc(html_favicon_url = "https://surrealdb.s3.amazonaws.com/favicon.png")]
#![doc(html_logo_url = "https://surrealdb.s3.amazonaws.com/icon.png")]
// TODO: Remove
// This is added to keep the move anyhow PR somewhat smaller. This should be removed in a follow-up
// PR.
#![allow(clippy::large_enum_variant)]

#[macro_use]
extern crate tracing;

#[macro_use]
mod mac;

mod buc;
mod cf;
mod doc;
mod exe;
mod fmt;
mod fnc;
#[cfg(feature = "surrealism")]
mod surrealism;
mod sys;
mod val;

#[doc(hidden)]
pub mod str;

pub mod api;
pub mod catalog;
pub mod cnf;
pub mod ctx;
pub mod dbs;
pub mod env;
pub mod err;
pub mod expr;
pub mod gql;
pub mod iam;
pub mod idx;
pub mod key;
pub mod kvs;
pub mod mem;
pub mod obs;
pub mod options;
pub mod rpc;
pub mod sql;
pub mod syn;
pub mod vs;

pub(crate) mod types {
	//! Re-export the types from the types crate for internal use prefixed with Public.

	pub use surrealdb_types::{
		Action as PublicAction, Array as PublicArray, Bytes as PublicBytes,
		Datetime as PublicDatetime, Duration as PublicDuration, File as PublicFile,
		Geometry as PublicGeometry, GeometryKind as PublicGeometryKind, Kind as PublicKind,
		KindLiteral as PublicKindLiteral, Notification as PublicNotification,
		Number as PublicNumber, Object as PublicObject, Range as PublicRange,
		RecordId as PublicRecordId, RecordIdKey as PublicRecordIdKey,
		RecordIdKeyRange as PublicRecordIdKeyRange, Regex as PublicRegex, Set as PublicSet,
		SurrealValue, Table as PublicTable, Uuid as PublicUuid, Value as PublicValue,
		Variables as PublicVariables,
	};
}

#[cfg(feature = "ml")]
pub use surrealml as ml;

#[cfg(feature = "enterprise")]
#[rustfmt::skip]
pub mod ent;

/// Channels for receiving a SurrealQL database export
pub mod channel {
	pub use async_channel::{Receiver, Sender, bounded, unbounded};
}

/// Composer for the community edition of SurrealDB.
///
/// This struct implements the composer pattern for dependency injection, providing
/// default implementations of the traits required to initialize and run SurrealDB.
///
/// # Implemented Traits
/// - `TransactionBuilderFactory` - Selects and validates the datastore backend
/// - `RouterFactory` - Constructs the HTTP router with standard routes
/// - `ConfigCheck` - Validates configuration before initialization
///
/// # Usage
/// This is the default composer used by the `surreal` binary. Embedders can create
/// their own composer structs implementing these traits to customize behavior.
///
/// # Example
/// ```ignore
/// use surrealdb_core::CommunityComposer;
///
/// // Pass the composer to init functions
/// surreal::init(CommunityComposer())
/// ```
pub struct CommunityComposer();
