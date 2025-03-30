use crate::sql::{Ident, Kind, Permission, Strand};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct TypeDefinition {
    pub name: Ident,
    pub kind: Kind,
    pub comment: Option<Strand>,
    pub permissions: Permission,
}

impl Display for TypeDefinition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DEFINE TYPE {} AS {}", self.name, self.kind)?;
        if let Some(ref v) = self.comment {
            write!(f, " COMMENT {v}")?
        }
        Ok(())
    }
} 