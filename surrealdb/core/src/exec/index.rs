//! Index support for the engine.
//!
//! This module provides index analysis, access path selection, and index iteration
//! for the new streaming query executor. It supports:
//!
//! - B-tree indexes (Idx, Uniq) with equality, range, and compound access patterns
//! - Full-text search indexes (FullText) with MATCHES operator
//! - Vector similarity indexes (Hnsw) with KNN search
//! - Count indexes for COUNT(*) optimization
//! - Key ranges over a table whose record ids name their components ([`key_range`]), which need no
//!   index
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        Planner Phase                            │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  IndexAnalyzer   →   IndexCandidate[]   →   select_access_path  │
//! │  (match WHERE)       (possible plans)       (choose best)       │
//! └─────────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                       Execution Phase                           │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  AccessPath  →  IndexScan/FullTextScan/KnnScan  →  ValueBatch   │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

pub(crate) mod access_path;
pub(crate) mod analysis;
pub(crate) mod derived_columns;
pub(crate) mod id_range;
pub(crate) mod iterator;
pub(crate) mod key_range;
pub(crate) mod probe;

pub use analysis::IndexCandidate;
