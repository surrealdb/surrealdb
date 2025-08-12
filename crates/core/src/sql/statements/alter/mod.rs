pub mod field;
mod sequence;
mod table;

use std::fmt::{self, Display};

pub use field::AlterFieldStatement;
pub use sequence::AlterSequenceStatement;
pub use table::AlterTableStatement;

#[derive(Clone, Debug, Eq, PartialEq, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum AlterKind<T> {
	#[default]
	None,
	Set(T),
	Drop,
}

impl<A, B> From<AlterKind<A>> for crate::expr::statements::alter::AlterKind<B>
where
	B: From<A>,
{
	fn from(value: AlterKind<A>) -> Self {
		match value {
			AlterKind::Set(a) => crate::expr::statements::alter::AlterKind::Set(a.into()),
			AlterKind::Drop => crate::expr::statements::alter::AlterKind::Drop,
			AlterKind::None => crate::expr::statements::alter::AlterKind::None,
		}
	}
}

impl<A, B> From<crate::expr::statements::alter::AlterKind<A>> for AlterKind<B>
where
	B: From<A>,
{
	fn from(value: crate::expr::statements::alter::AlterKind<A>) -> Self {
		match value {
			crate::expr::statements::alter::AlterKind::Set(a) => AlterKind::Set(a.into()),
			crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
			crate::expr::statements::alter::AlterKind::None => AlterKind::None,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum AlterStatement {
	Table(AlterTableStatement),
	Sequence(AlterSequenceStatement),
	Field(AlterFieldStatement),
}

impl Display for AlterStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Table(v) => Display::fmt(v, f),
			Self::Sequence(v) => Display::fmt(v, f),
			Self::Field(v) => Display::fmt(v, f),
		}
	}
}

impl From<AlterStatement> for crate::expr::statements::AlterStatement {
	fn from(v: AlterStatement) -> Self {
		match v {
			AlterStatement::Table(v) => Self::Table(v.into()),
			AlterStatement::Sequence(v) => Self::Sequence(v.into()),
			AlterStatement::Field(v) => Self::Field(v.into()),
		}
	}
}

impl From<crate::expr::statements::AlterStatement> for AlterStatement {
	fn from(v: crate::expr::statements::AlterStatement) -> Self {
		match v {
			crate::expr::statements::AlterStatement::Table(v) => Self::Table(v.into()),
			crate::expr::statements::AlterStatement::Sequence(v) => Self::Sequence(v.into()),
			crate::expr::statements::AlterStatement::Field(v) => Self::Field(v.into()),
		}
	}
}
