//! Index-engine tests that drive the engines through the datastore.
//!
//! The engines' own tests are white-box and live beside the code they exercise,
//! reaching an engine directly with nothing above it in scope. The tests here
//! are the opposite: they assert on engine behaviour reached through
//! `DEFINE INDEX` / `SELECT … <|k,ef|> …` and the compaction task, so a
//! `Datastore`, a `Session` and the query executor are the point rather than
//! scaffolding. They belong with the driver.

#[cfg(diskann)]
mod diskann_query_test;
mod hnsw_query_test;
