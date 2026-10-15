//! What every kind of extension shares: the archive a definition pins, the
//! signer it must come from, and what a definition lets an extension do.

pub(crate) mod archive;
#[allow(dead_code, reason = "the extension runner is what reads effective capabilities")]
pub(crate) mod capabilities;
#[cfg(feature = "surrealism")]
pub(crate) mod trust;
