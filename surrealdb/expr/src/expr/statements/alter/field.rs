use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::expr::permission::Permissions;
use crate::expr::reference::Reference;
use crate::expr::{Expr, Kind, Literal};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum AlterDefault {
	#[default]
	None,
	Drop,
	Always(Expr),
	Set(Expr),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AlterFieldStatement {
	pub name: Expr,
	pub what: Expr,
	pub if_exists: bool,
	pub kind: AlterKind<Kind>,
	pub flexible: AlterKind<()>,
	pub readonly: AlterKind<()>,
	pub value: AlterKind<Expr>,
	pub assert: AlterKind<Expr>,
	pub default: AlterDefault,
	pub permissions: Option<Permissions>,
	pub comment: AlterKind<String>,
	pub reference: AlterKind<Reference>,
}

impl Default for AlterFieldStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			if_exists: false,
			kind: AlterKind::None,
			flexible: AlterKind::None,
			readonly: AlterKind::None,
			value: AlterKind::None,
			assert: AlterKind::None,
			default: AlterDefault::None,
			permissions: None,
			comment: AlterKind::None,
			reference: AlterKind::None,
		}
	}
}

impl ToSql for AlterFieldStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::field::AlterFieldStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
