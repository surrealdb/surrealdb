//! Re-export of the value-codec macro.
//!
//! Key structs, their encoders and their range bounds are generated from the
//! declaration in [`crate::key::schema`], so nothing here writes a key. What
//! remains is the macro that gives a *value* type its revision-based codec, which
//! value types across the tree reach for through `crate::key`.

pub(crate) use surrealdb_kvs::impl_kv_value_revisioned;
