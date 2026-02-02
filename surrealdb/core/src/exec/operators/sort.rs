//! Sort operators for ORDER BY processing.
//!
//! This module provides multiple sort operator implementations optimized for
//! different scenarios:
//!
//! - [`Sort`]: Full in-memory sort with parallel sorting support (evaluates expressions)
//! - [`SortByKey`]: In-memory sort by pre-computed field names (new consolidated approach)
//! - [`SortTopK`]: Heap-based top-k selection for ORDER BY + LIMIT queries
//! - [`RandomShuffle`]: Random ordering with reservoir sampling optimization
//! - [`ExternalSort`]: Disk-based external merge sort for large datasets (storage feature)

mod common;
#[cfg(storage)]
mod external;
mod full_sort;
mod shuffle;
mod topk;

pub use common::{OrderByField, SortDirection, SortKey};
#[cfg(storage)]
pub use external::ExternalSort;
pub use full_sort::{Sort, SortByKey};
pub use shuffle::RandomShuffle;
pub use topk::SortTopK;
