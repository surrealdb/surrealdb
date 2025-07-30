use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use revision::{revisioned, Revisioned};


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct NamespaceId(pub u16);

impl Revisioned for NamespaceId {
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
        Revisioned::deserialize_revisioned(reader).map(NamespaceId)
    }
}

impl Display for NamespaceId {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct NamespaceDefinition {
	pub id: NamespaceId,
	pub name: String,
	pub comment: Option<String>,
}
