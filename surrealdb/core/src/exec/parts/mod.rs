//! What the VM's path lowering shares of the parts of an idiom: the direction of a
//! graph lookup ([`LookupDirection`]) and the recursion helpers of `recurse`. The
//! steps of a path themselves are VM instructions (`vm::access`).

// Re-export recursion utilities from the canonical definitions in `expr::idiom::recursion`.
// These are shared with the idiom recursion in `expr`.
pub(crate) use crate::expr::idiom::recursion::{clean_iteration, get_final, is_final};
pub(crate) mod lookup;
pub(crate) mod recurse;

pub(crate) use lookup::LookupDirection;
