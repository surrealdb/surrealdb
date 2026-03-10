pub mod field;
use surrealdb_types::{SqlFormat, ToSql};
mod database;
mod index;
mod sequence;

mod namespace;
mod system;
mod table;

pub use database::AlterDatabaseStatement;
pub use field::AlterFieldStatement;
pub use index::AlterIndexStatement;
pub use namespace::AlterNamespaceStatement;
pub use sequence::AlterSequenceStatement;
pub use system::AlterSystemStatement;
pub use table::AlterTableStatement;

#[derive(Clone, Debug, Eq, PartialEq, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// Tri‑state alteration helper used across `ALTER` AST nodes.
///
/// - `None`: leave the current value unchanged
/// - `Set(T)`: set/replace the current value to `T`
/// - `Drop`: remove/clear the current value
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
/// SQL AST for `ALTER` statements. Variants mirror specific resources.
pub enum AlterStatement {
	System(AlterSystemStatement),
	Namespace(AlterNamespaceStatement),
	Database(AlterDatabaseStatement),
	Table(AlterTableStatement),
	Index(AlterIndexStatement),
	Sequence(AlterSequenceStatement),
	Field(AlterFieldStatement),
}

impl ToSql for AlterStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::System(v) => v.fmt_sql(f, fmt),
			Self::Namespace(v) => v.fmt_sql(f, fmt),
			Self::Database(v) => v.fmt_sql(f, fmt),
			Self::Table(v) => v.fmt_sql(f, fmt),
			Self::Index(v) => v.fmt_sql(f, fmt),
			Self::Sequence(v) => v.fmt_sql(f, fmt),
			Self::Field(v) => v.fmt_sql(f, fmt),
		}
	}
}

impl From<AlterStatement> for crate::expr::statements::AlterStatement {
	fn from(v: AlterStatement) -> Self {
		match v {
			AlterStatement::System(v) => Self::System(v.into()),
			AlterStatement::Namespace(v) => Self::Namespace(v.into()),
			AlterStatement::Database(v) => Self::Database(v.into()),
			AlterStatement::Table(v) => Self::Table(v.into()),
			AlterStatement::Index(v) => Self::Index(v.into()),
			AlterStatement::Sequence(v) => Self::Sequence(v.into()),
			AlterStatement::Field(v) => Self::Field(v.into()),
		}
	}
}

impl From<crate::expr::statements::AlterStatement> for AlterStatement {
	fn from(v: crate::expr::statements::AlterStatement) -> Self {
		match v {
			crate::expr::statements::AlterStatement::System(v) => Self::System(v.into()),
			crate::expr::statements::AlterStatement::Namespace(v) => Self::Namespace(v.into()),
			crate::expr::statements::AlterStatement::Database(v) => Self::Database(v.into()),
			crate::expr::statements::AlterStatement::Table(v) => Self::Table(v.into()),
			crate::expr::statements::AlterStatement::Index(v) => Self::Index(v.into()),
			crate::expr::statements::AlterStatement::Sequence(v) => Self::Sequence(v.into()),
			crate::expr::statements::AlterStatement::Field(v) => Self::Field(v.into()),
		}
	}
}
