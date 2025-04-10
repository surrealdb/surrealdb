mod type_definition;

use crate::sql::statements::DefineStatement;

pub use type_definition::DefineTypeStatement;

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum DefineStatement {
    Type(DefineTypeStatement),
}
