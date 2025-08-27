use std::fmt::{self, Display, Formatter};

use crate::sql::Ident;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveBucketStatement {
	pub name: Ident,
	pub if_exists: bool,
}

impl Display for RemoveBucketStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE BUCKET")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}

impl From<RemoveBucketStatement> for crate::expr::statements::remove::RemoveBucketStatement {
	fn from(v: RemoveBucketStatement) -> Self {
		crate::expr::statements::remove::RemoveBucketStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::remove::RemoveBucketStatement> for RemoveBucketStatement {
	fn from(v: crate::expr::statements::remove::RemoveBucketStatement) -> Self {
		RemoveBucketStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}
