//! The SurrealDB catalog.
//!
//! Schema definitions in two forms per entity: a `Stored*` twin persisted
//! with canonical SurrealQL text for its expression-bearing fields, and a
//! runtime form holding compiled ASTs, bridged by `FromStored`/`to_stored`.
//! The provider traits here are the read/write contract the datastore layer
//! implements to serve definitions to the engine.
//!
//! Every persisted catalog entry is a keyed map in the stored-value grammar, declared once per type
//! by `#[derive(Stored)]` and frozen by `catalog-schema.lock`; entries a 3.x binary wrote in the
//! `revision` encoding stay readable. The envelope, the dispatch between the two grammars, the
//! feature-level rules and the embedded frame are described in [`stored`].
//!
//! This crate is an internal implementation detail of SurrealDB: its API is
//! unstable and changes without notice. Depend on the `surrealdb` SDK or
//! `surrealdb-core` instead.

#![allow(clippy::mutable_key_type)]
// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]

#[macro_use]
extern crate surrealdb_collections;
#[macro_use]
extern crate tracing;

// The module tree spells intra-layer paths as `crate::catalog::<name>` and
// names its neighbours as `crate::<layer>`; these aliases keep those paths
// valid for the layers this crate sits on.
pub use surrealdb_expr::{expr, parser as syn, val};
pub use surrealdb_iam as iam;

pub(crate) mod types {
	//! The public type surface, re-exported with `Public` prefixes so internal
	//! and public forms stay visually distinct at use sites.

	#[allow(unused_imports)]
	pub use surrealdb_types::{
		Action as PublicAction, Array as PublicArray, Bytes as PublicBytes,
		Datetime as PublicDatetime, Duration as PublicDuration, File as PublicFile,
		Geometry as PublicGeometry, GeometryKind as PublicGeometryKind, Kind as PublicKind,
		KindLiteral as PublicKindLiteral, Notification as PublicNotification,
		Number as PublicNumber, Object as PublicObject, Range as PublicRange,
		RecordId as PublicRecordId, RecordIdKey as PublicRecordIdKey,
		RecordIdKeyRange as PublicRecordIdKeyRange, Set as PublicSet, SurrealValue,
		Table as PublicTable, Uuid as PublicUuid, Value as PublicValue,
		Variables as PublicVariables,
	};
}

pub(crate) mod catalog {
	//! Self-alias so `crate::catalog::` paths written inside the module tree
	//! resolve against the crate root. Internal: the crate's own surface is its
	//! root, and core re-exports that root as `catalog`.
	pub(crate) use crate::*;
}

mod access;
pub mod aggregation;
/// The API route-path vocabulary persisted by API definitions.
pub mod api_path;
pub mod auth;
mod compiled;
mod conformance;
mod database;
mod error;
/// The store-wide feature level record.
pub mod feature;
mod generation;
/// Row layouts of table generations.
pub mod layout;
mod layout_derive;
mod module;
mod namespace;
/// Cluster-node registration and liveness bookkeeping.
pub mod node;
/// Packed rows: writing and reading the rows of a packed descriptor.
pub mod packed;
pub mod providers;
pub mod record;
pub mod schema;
/// Catalog entries in the stored-value grammar.
pub mod stored;
mod subscription;
mod table;
mod task;
mod text;
pub mod view;

pub use access::*;
pub use compiled::*;
pub use conformance::{
	Conformance, FieldCandidate, FieldClaim, TableConformance, birth_claims, birth_claims_of,
	definition_digest,
};
pub use database::*;
pub use error::{Error, extension_name};
pub use feature::StoredFeatureLevel;
pub use generation::{Generation, GenerationMark, GenerationRing, TableMigrations};
pub use layout::{
	Column, ColumnRole, GenerationLayout, LayoutDescriptor, LayoutRefusal, Location,
	MAX_PATH_DEPTH, PLANNED_PHYSICAL_KINDS, PackedDescriptor, PhysicalKind, Presence,
	RecordLayoutError, RowLayout, TableLayouts, decode_generation,
};
pub use layout_derive::{TableShape, derive_layout};
pub use module::*;
pub use namespace::*;
pub use record::*;
pub use schema::{
	ApiMethod, DiskAnnParams, Distance, FullTextParams, HnswParams, Scoring, StoredApiDefinition,
	VectorType, *,
};
pub use subscription::*;
pub use table::*;
pub use task::*;
pub use text::*;
pub use view::*;

impl From<Error> for crate::expr::ControlFlow {
	/// Boxes the catalog failure directly, so it stays the concrete type in the
	/// `anyhow` slot and the downcasts that steer `IF EXISTS` keep matching it.
	fn from(error: Error) -> Self {
		crate::expr::ControlFlow::Err(anyhow::Error::new(error))
	}
}
