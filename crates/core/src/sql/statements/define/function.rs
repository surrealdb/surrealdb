use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Base, Block, Ident, Kind, Permission, Strand, Value};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[revisioned(revision = 4)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineFunctionStatement {
	pub name: Ident,
	pub args: Vec<(Ident, Kind)>,
	pub block: Block,
	pub comment: Option<Strand>,
	pub permissions: Permission,
	#[revision(start = 2)]
	pub if_not_exists: bool,
	#[revision(start = 3)]
	pub overwrite: bool,
	#[revision(start = 4)]
	pub returns: Option<Kind>,
}

impl From<DefineFunctionStatement> for crate::expr::statements::DefineFunctionStatement {
	fn from(v: DefineFunctionStatement) -> Self {
		Self {
			name: v.name.into(),
			args: v.args.into_iter().map(|(i, k)| (i.into(), k.into())).collect(),
			block: v.block.into(),
			comment: v.comment.map(Into::into),
			permissions: v.permissions,
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
			returns: v.returns.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::DefineFunctionStatement> for DefineFunctionStatement {
	fn from(v: crate::expr::statements::DefineFunctionStatement) -> Self {
		Self {
			name: v.name.into(),
			args: v.args.into_iter().map(|(i, k)| (i.into(), k.into())).collect(),
			block: v.block.into(),
			comment: v.comment.map(Into::into),
			permissions: v.permissions,
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
			returns: v.returns.map(Into::into),
		}
	}
}

crate::sql::impl_display_from_sql!(DefineFunctionStatement);

impl crate::sql::DisplaySql for DefineFunctionStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE FUNCTION")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " fn::{}(", self.name.0)?;
		for (i, (name, kind)) in self.args.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			write!(f, "${name}: {kind}")?;
		}
		f.write_str(") ")?;
		if let Some(ref v) = self.returns {
			write!(f, "-> {v} ")?;
		}
		Display::fmt(&self.block, f)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		write!(f, "PERMISSIONS {}", self.permissions)?;
		Ok(())
	}
}

impl InfoStructure for DefineFunctionStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"args".to_string() => self.args
				.into_iter()
				.map(|(n, k)| vec![n.structure(), k.structure()].into())
				.collect::<Vec<Value>>()
				.into(),
			"block".to_string() => self.block.structure(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
			"returns".to_string(), if let Some(v) = self.returns => v.structure(),
		})
	}
}
