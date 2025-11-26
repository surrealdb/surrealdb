use std::fmt::{self, Display};

use crate::fmt::{EscapeKwIdent, QuoteStr};
use crate::sql::statements::alter::AlterKind;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AlterIndexStatement {
	pub name: String,
	pub table: String,
	pub if_exists: bool,
	pub prepare_remove: bool,
	pub comment: AlterKind<String>,
}

impl Display for AlterIndexStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ALTER INDEX")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, EscapeKwIdent(&self.table, &["IF"]))?;

		if self.prepare_remove {
			write!(f, " PREPARE REMOVE")?;
		}
		match self.comment {
			AlterKind::Set(ref x) => write!(f, " COMMENT {}", QuoteStr(x))?,
			AlterKind::Drop => write!(f, " DROP COMMENT")?,
			AlterKind::None => {}
		}
		Ok(())
	}
}

impl From<AlterIndexStatement> for crate::expr::statements::alter::AlterIndexStatement {
	fn from(v: AlterIndexStatement) -> Self {
		crate::expr::statements::alter::AlterIndexStatement {
			name: v.name,
			table: v.table,
			if_exists: v.if_exists,
			prepare_remove: v.prepare_remove,
			comment: v.comment.into(),
		}
	}
}
impl From<crate::expr::statements::alter::AlterIndexStatement> for AlterIndexStatement {
	fn from(v: crate::expr::statements::alter::AlterIndexStatement) -> Self {
		AlterIndexStatement {
			name: v.name,
			table: v.table,
			if_exists: v.if_exists,
			prepare_remove: v.prepare_remove,
			comment: v.comment.into(),
		}
	}
}
