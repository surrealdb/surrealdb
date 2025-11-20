use std::fmt;

use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	Serialize,
	PartialOrd,
	Deserialize,
	Hash,
	Encode,
	BorrowDecode,
)]
pub enum Dir {
	/// `<-`
	In,
	/// `->`
	Out,
	/// `<->`
	#[default]
	Both,
}

impl fmt::Display for Dir {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::In => "<-",
			Self::Out => "->",
			Self::Both => "<->",
		})
	}
}
