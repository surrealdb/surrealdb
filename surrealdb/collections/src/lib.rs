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

/// Creates a new [`VecMap`] of key-value pairs.
///
/// Clones items from the optional secondary map first, then applies conditional
/// entries. Duplicate keys keep the last value (same as repeated [`VecMap::insert`]).
/// Uses batch construction so key order in the source does not trigger quadratic cost.
#[macro_export]
macro_rules! map {
    ($($k:expr_2021 $(, if let $grant:pat = $check:expr_2021)? $(, if $guard:expr_2021)? => $v:expr_2021),* $(,)? $( => $x:expr_2021 )?) => {{
        let mut pairs: ::std::vec::Vec<_> = ::std::vec::Vec::new();
    	$(pairs.extend($x.iter().map(|(k, v)| (k.clone(), v.clone())));)?
		$( $(if let $grant = $check)? $(if $guard)? { pairs.push(($k, $v)); };)+
        pairs.into_iter().collect::<$crate::VecMap<_, _>>()
    }};
}

/// Maps an optional value to a new value if the optional value is some, otherwise returns none.
/// Useful when the computation is async
#[macro_export]
macro_rules! map_opt {
	($x:ident as $opt:expr => $exp:expr) => {
		match $opt {
			Some($x) => Some($exp),
			None => None,
		}
	};
}
