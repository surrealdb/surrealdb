//! vs is a module to handle Versionstamps.
//! This module is supplemental to the kvs::tx module and is not intended to be used directly
//! by applications.
//! This module might be migrated into the kvs or kvs::tx module in the future.

/// Versionstamp is a 10-byte array used to identify a specific version of a key.
/// The first 8 bytes are significant (the u64), and the remaining 2 bytes are not significant, but used for extra precision.
/// To convert to and from this module, see the conv module in this same directory.
pub type Versionstamp = [u8; 10];

pub(crate) mod conv;
pub(crate) mod oracle;

pub use self::conv::*;
pub use self::oracle::*;
