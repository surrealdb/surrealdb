//! Index support for the streaming executor.
//!
//! This module provides index analysis, access path selection, and index iteration
//! for the new streaming query executor. It supports:
//!
//! - B-tree indexes (Idx, Uniq) with equality, range, and compound access patterns
//! - Full-text search indexes (FullText) with MATCHES operator
//! - Vector similarity indexes (Hnsw) with KNN search
//! - Count indexes for COUNT(*) optimization
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
pub(crate) mod iterator;

pub use access_path::{AccessPath, BTreeAccess, RangeBound};
pub use analysis::{IndexAnalyzer, IndexCandidate};
