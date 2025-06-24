use hex;
use revision::revisioned;
use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Bytes(pub(crate) Vec<u8>);

impl From<Bytes> for crate::val::Bytes {
	fn from(v: Bytes) -> Self {
		crate::val::Bytes(v.0)
	}
}

impl From<crate::val::Bytes> for Bytes {
	fn from(v: crate::val::Bytes) -> Self {
		Bytes(v.0)
	}
}

impl Display for Bytes {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "b\"{}\"", hex::encode_upper(&self.0))
	}
}
