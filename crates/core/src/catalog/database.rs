use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use revision::{revisioned, Revisioned};

use crate::expr::ChangeFeed;


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct DatabaseId(pub u16);

impl Revisioned for DatabaseId {
    fn revision() -> u16 {
        1
    }

    #[inline]
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
        self.0.serialize_revisioned(writer)
    }

	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
        Revisioned::deserialize_revisioned(reader).map(DatabaseId)
    }
}

impl Display for DatabaseId {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DatabaseDefinition {
	pub database_id: DatabaseId,
	pub name: String,
	pub comment: Option<String>,
	pub changefeed: Option<ChangeFeed>,
}