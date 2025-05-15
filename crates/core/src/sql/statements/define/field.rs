use crate::ctx::Context;
use crate::dbs::capabilities::ExperimentalTarget;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::kvs::Transaction;
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::reference::Reference;
use crate::sql::statements::info::InfoStructure;
use crate::sql::statements::DefineTableStatement;
use crate::sql::{Base, Ident, Idiom, Kind, Permissions, Strand, Value};
use crate::sql::{Literal, Part};
use crate::sql::{Relation, TableType};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Write};
use std::sync::Arc;
use uuid::Uuid;

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
	pub value: Option<Value>,
	pub assert: Option<Value>,
	pub default: Option<Value>,
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
	pub(crate) fn validate_reference_options(&self, ctx: &Context) -> Result<(), Error> {
		if !ctx.get_capabilities().allows_experimental(&ExperimentalTarget::RecordReferences) {
			return Ok(());
		}

		if let Some(kind) = &self.kind {
			let kinds = match kind {
				Kind::Either(kinds) => kinds,
				kind => &vec![kind.to_owned()],
			};

			// Check if any of the kinds are references
			if kinds.iter().any(|k| matches!(k, Kind::References(_, _))) {
				// If any of the kinds are references, all of them must be
				if !kinds.iter().all(|k| matches!(k, Kind::References(_, _))) {
					return Err(Error::RefsMismatchingVariants);
				}

				// As the refs and dynrefs type essentially take over a field
				// they are not allowed to be mixed with most other clauses
				let typename = kind.to_string();

				if self.reference.is_some() {
					return Err(Error::RefsTypeConflict("REFERENCE".into(), typename));
				}

				if self.default.is_some() {
					return Err(Error::RefsTypeConflict("DEFAULT".into(), typename));
				}

				if self.value.is_some() {
					return Err(Error::RefsTypeConflict("VALUE".into(), typename));
				}

				if self.assert.is_some() {
					return Err(Error::RefsTypeConflict("ASSERT".into(), typename));
				}

				if self.flex {
					return Err(Error::RefsTypeConflict("FLEXIBLE".into(), typename));
				}

				if self.readonly {
					return Err(Error::RefsTypeConflict("READONLY".into(), typename));
				}
			}

			// If a reference is defined, the field must be a record
			if self.reference.is_some() {
				let kinds = match kind.get_optional_inner_kind() {
					Kind::Either(kinds) => kinds,
					Kind::Array(kind, _) | Kind::Set(kind, _) => match kind.as_ref() {
						Kind::Either(kinds) => kinds,
						kind => &vec![kind.to_owned()],
					},
					Kind::Literal(lit) => match lit {
						Literal::Array(kinds) => kinds,
						lit => &vec![Kind::Literal(lit.to_owned())],
					},
					kind => &vec![kind.to_owned()],
				};

				if !kinds.iter().all(|k| matches!(k, Kind::Record(_))) {
					return Err(Error::ReferenceTypeConflict(kind.to_string()));
				}
			}
		}

		Ok(())
	}

	/// Get the correct reference type if needed.
	pub(crate) async fn get_reference_kind(
		&self,
		ctx: &Context,
		opt: &Options,
	) -> Result<Option<Kind>, Error> {
		if !ctx.get_capabilities().allows_experimental(&ExperimentalTarget::RecordReferences) {
			return Ok(None);
		}

		if let Some(Kind::References(Some(ft), Some(ff))) = &self.kind {
			// Obtain the field definition
			let (ns, db) = opt.ns_db()?;
			let fd = match ctx.tx().get_tb_field(ns, db, &ft.to_string(), &ff.to_string()).await {
				Ok(fd) => fd,
				// If the field does not exist, there is nothing to correct
				Err(Error::FdNotFound {
					..
				}) => return Ok(None),
				Err(e) => return Err(e),
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

crate::sql::impl_display_from_sql!(DefineFieldStatement);

impl crate::sql::DisplaySql for DefineFieldStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl InfoStructure for DefineFieldStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"what".to_string() => self.what.structure(),
			"flex".to_string() => self.flex.into(),
			"kind".to_string(), if let Some(v) = self.kind => v.structure(),
			"value".to_string(), if let Some(v) = self.value => v.structure(),
			"assert".to_string(), if let Some(v) = self.assert => v.structure(),
			"default".to_string(), if let Some(v) = self.default => v.structure(),
			"reference".to_string(), if let Some(v) = self.reference => v.structure(),
			"readonly".to_string() => self.readonly.into(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
