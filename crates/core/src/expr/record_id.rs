use serde::{Deserialize, Serialize};

use crate::expr::escape::EscapeRid;
use crate::expr::id::RecordIdKeyLit;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RecordIdLit {
	/// Table name
	pub tb: String,
	pub id: RecordIdKeyLit,
}

impl fmt::Display for RecordIdLit {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", EscapeRid(&self.tb), self.id)
	}
}
