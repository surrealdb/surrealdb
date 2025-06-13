use hex;
use revision::revisioned;
use serde::de::SeqAccess;
use serde::{
	Deserialize, Serialize,
	de::{self, Visitor},
};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Bytes(pub(crate) Vec<u8>);

impl From<Bytes> for crate::expr::Bytes {
	fn from(v: Bytes) -> Self {
		crate::expr::Bytes(v.0)
	}
}

impl From<crate::expr::Bytes> for Bytes {
	fn from(v: crate::expr::Bytes) -> Self {
		Bytes(v.0)
	}
}

impl Display for Bytes {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "b\"{}\"", hex::encode_upper(&self.0))
	}
}
