//! Scan operators — operators that perform storage I/O and check permissions.
//!
//! All operators in this module read data from the underlying key-value store
//! (table scans, index scans, full-text search, KNN, graph traversals, etc.)
//! and handle table/field-level permissions.

pub(crate) mod common;
mod count;
mod dynamic;
mod fulltext;
mod graph;
mod index;
mod index_count;
mod knn;
pub(crate) mod pipeline;
mod reference;
mod table;

pub use count::CountScan;
pub use dynamic::DynamicScan;
pub use fulltext::FullTextScan;
pub use graph::{EdgeTableSpec, GraphEdgeScan, GraphScanOutput};
pub use index::IndexScan;
pub use knn::KnnScan;
pub(crate) use pipeline::determine_scan_direction;
pub use reference::{ReferenceScan, ReferenceScanOutput};
pub use table::TableScan;
