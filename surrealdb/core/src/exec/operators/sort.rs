//! The ordering the VM's sorts and ordered merges apply to values:
//! [`SortDirection`] and [`compare_values`], which honour `COLLATE` and
//! `NUMERIC`.

mod common;

pub(crate) use common::{SortDirection, compare_values};
