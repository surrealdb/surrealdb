//! Catalog backwards compatibility test fixtures and tests.
//!
//! This module tests that serialized catalog types from previous versions
//! can still be deserialized by the current code AND that they deserialize
//! to the expected values. This catches both breaking changes to the
//! `revisioned` serialization format and subtle parsing behavior changes.
//!
//! ## Architecture
//!
//! - `fixtures.rs`: Defines expected values for each test case. These are the "source of truth"
//!   used both to generate byte arrays and to assert equality.
//! - `v3_0_0.rs`: Pre-serialized byte arrays from version 3.0.0. NEVER modify.
//! - `generator.rs`: Tool to generate byte arrays from fixtures.
//! - `tests.rs`: Tests that decode bytes and assert equality with fixtures.
//!
//! ## How it works
//!
//! 1. Fixtures define expected values as functions returning catalog types
//! 2. Generator serializes fixtures to byte arrays (frozen per version)
//! 3. Tests decode byte arrays and assert equality with fixture values
//!
//! ## Adding new version fixtures
//!
//! When releasing a new major version:
//! 1. Run: `cargo test -p surrealdb-core --lib catalog::compat::generator -- --ignored --nocapture`
//! 2. Copy the output to a new version file (e.g., `v3_1_0.rs`)
//! 3. Add the module to this file
//! 4. Add test cases for the new version
//!
//! ## IMPORTANT
//!
//! Once committed, version fixture files (e.g., `v3_0_0.rs`) must NEVER be modified.
//! They represent the exact serialization format from that version.
//!
//! The `fixtures.rs` file MAY be updated when types evolve to reflect how old
//! serialized data should be interpreted by the current code.

pub(super) mod fixtures;
mod generator;
mod tests;

#[rustfmt::skip]
mod v3_0_0;
