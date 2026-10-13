//! The SurrealDB expression layer.
//!
//! `expr` holds the logical form of SurrealQL — [`expr::Expr`] and the
//! statement types the engine plans and evaluates — and `val` holds the value
//! model: [`val::Value`], its containers, and the scalar newtypes.
//!
//! This crate is an internal implementation detail of SurrealDB: its API is
//! unstable and changes without notice. Depend on the `surrealdb` SDK or
//! `surrealdb-core` instead.

#[macro_use]
extern crate surrealdb_collections;
#[macro_use]
extern crate tracing;

pub mod expr;
#[cfg(feature = "scripting")]
pub mod js;
pub use val::rnd;
pub mod val;

// The module tree spells intra-layer paths as `crate::<name>`; these aliases
// keep those paths valid for the layers this crate sits on.
pub(crate) use surrealdb_iam as iam;
pub(crate) use surrealdb_sql as sql;
pub(crate) use surrealdb_syn as syn;
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
