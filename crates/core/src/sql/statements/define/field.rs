use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::capabilities::ExperimentalTarget;
use crate::err::Error;
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::reference::Reference;

use crate::sql::{Ident, Idiom, Kind, Permissions, SqlValue, Strand};
use crate::sql::Part;
use anyhow::Result;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[revisioned(revision = 6)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineFieldStatement {
	pub name: Idiom,
	pub what: Ident,
	/// Whether the field is marked as flexible.
	/// Flexible allows the field to be schemaless even if the table is marked as schemafull.
	pub flex: bool,
	pub kind: Option<Kind>,
	#[revision(start = 2)]
	pub readonly: bool,
	pub value: Option<SqlValue>,
	pub assert: Option<SqlValue>,
	pub default: Option<SqlValue>,
	pub permissions: Permissions,
	pub comment: Option<Strand>,
	#[revision(start = 3)]
	pub if_not_exists: bool,
	#[revision(start = 4)]
	pub overwrite: bool,
	#[revision(start = 5)]
	pub reference: Option<Reference>,
	#[revision(start = 6)]
	pub default_always: bool,
}

impl DefineFieldStatement {
	/// Get the correct reference type if needed.
	pub(crate) async fn get_reference_kind(
		&self,
		ctx: &Context,
		opt: &Options,
	) -> Result<Option<Kind>> {
		if !ctx.get_capabilities().allows_experimental(&ExperimentalTarget::RecordReferences) {
			return Ok(None);
		}

		if let Some(Kind::References(Some(ft), Some(ff))) = &self.kind {
			// Obtain the field definition
			let (ns, db) = opt.ns_db()?;
			let fd = match ctx.tx().get_tb_field(ns, db, &ft.to_string(), &ff.to_string()).await {
				Ok(fd) => fd,
				// If the field does not exist, there is nothing to correct
				Err(e) => {
					if matches!(e.downcast_ref(), Some(Error::FdNotFound { .. })) {
						return Ok(None);
					} else {
						return Err(e);
					}
				}
			};

			// Check if the field is an array-like value and thus "containing" references
			let is_array_like = fd
				.kind
				.as_ref()
				.map(|kind| kind.get_optional_inner_kind().is_array_like())
				.unwrap_or_default();

			// If the field is an array-like value, add the `.*` part
			if is_array_like {
				let ff = ff.clone().push(Part::All);
				return Ok(Some(Kind::References(Some(ft.clone()), Some(ff))));
			}
		}

		Ok(None)
	}
}

impl Display for DefineFieldStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE FIELD")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		if self.flex {
			write!(f, " FLEXIBLE")?
		}
		if let Some(ref v) = self.kind {
			write!(f, " TYPE {v}")?
		}
		if let Some(ref v) = self.default {
			write!(f, " DEFAULT")?;
			if self.default_always {
				write!(f, " ALWAYS")?
			}

			write!(f, " {v}")?
		}
		if self.readonly {
			write!(f, " READONLY")?
		}
		if let Some(ref v) = self.value {
			write!(f, " VALUE {v}")?
		}
		if let Some(ref v) = self.assert {
			write!(f, " ASSERT {v}")?
		}
		if let Some(ref v) = self.reference {
			write!(f, " REFERENCE {v}")?
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		// Alternate permissions display implementation ignores delete permission
		// This display is used to show field permissions, where delete has no effect
		// Displaying the permission could mislead users into thinking it has an effect
		// Additionally, including the permission will cause a parsing error in 3.0.0
		write!(f, "{:#}", self.permissions)?;
		Ok(())
	}
}

impl From<DefineFieldStatement> for crate::expr::statements::DefineFieldStatement {
	fn from(v: DefineFieldStatement) -> Self {
		Self {
			name: v.name.into(),
			what: v.what.into(),
			flex: v.flex,
			readonly: v.readonly,
			kind: v.kind.map(Into::into),
			value: v.value.map(Into::into),
			assert: v.assert.map(Into::into),
			default: v.default.map(Into::into),
			permissions: v.permissions.into(),
			comment: v.comment.map(Into::into),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
			reference: v.reference.map(Into::into),
			default_always: v.default_always,
		}
	}
}

impl From<crate::expr::statements::DefineFieldStatement> for DefineFieldStatement {
	fn from(v: crate::expr::statements::DefineFieldStatement) -> Self {
		Self {
			name: v.name.into(),
			what: v.what.into(),
			flex: v.flex,
			readonly: v.readonly,
			kind: v.kind.map(Into::into),
			value: v.value.map(Into::into),
			assert: v.assert.map(Into::into),
			default: v.default.map(Into::into),
			permissions: v.permissions.into(),
			comment: v.comment.map(Into::into),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
			reference: v.reference.map(Into::into),
			default_always: v.default_always,
		}
	}
}
