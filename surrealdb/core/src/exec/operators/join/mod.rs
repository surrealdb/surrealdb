//! What the VM's joins share: the row conventions of a join ([`hash_join`]) and
//! the in-memory table the right side is built into.

pub(crate) mod hash_join;

pub use hash_join::JoinType;
