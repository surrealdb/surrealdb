//! Sorted `Vec`-backed map and set containers used inside `surrealdb-core`.
//!
//! This crate is **SurrealDB internal API** and does not promise stability.

#![forbid(unsafe_code)]

mod revision_impl;
mod search;
mod vec_map;
mod vec_set;

pub use vec_map::{Entry, IntoIter as VecMapIntoIter, OccupiedEntry, VacantEntry, VecMap};
pub use vec_set::{IntoIter as VecSetIntoIter, VecSet};

#[cfg(test)]
mod storekey_tests;
