//! vs is a module to handle Versionstamps.
//! This module is supplemental to the kvs::tx module and is not intended to be used directly
//! by applications.
//! This module might be migrated into the kvs or kvs::tx module in the future.

pub type Versionstamp = [u8; 10];

pub(crate) mod conv;

pub use self::conv::*;
