use crate::catalog::TableDefinition;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::reference::Reference;
use crate::expr::{Base, Ident, Permissions, Strand, Value};
use crate::expr::{Idiom, Kind};
use crate::iam::{Action, ResourceKind};

use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::ops::Deref;
use uuid::Uuid;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AlterFieldStatement {
	pub name: Idiom,
	pub what: Ident,
	pub if_exists: bool,
	pub flex: Option<bool>,
	pub kind: Option<Option<Kind>>,
	pub readonly: Option<bool>,
	pub value: Option<Option<Value>>,
	pub assert: Option<Option<Value>>,
	pub default: Option<Option<Value>>,
	pub permissions: Option<Permissions>,
	pub comment: Option<Option<Strand>>,
	pub reference: Option<Option<Reference>>,
	pub default_always: Option<bool>,
}

impl AlterFieldStatement {
	pub(crate) async fn compute(
		&self,
		_stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Field, &Base::Db)?;
		// Get the NS and DB
		let (ns, db) = ctx.get_ns_db_ids_ro(opt).await?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Get the table definition
		let name = self.name.to_string();
		let mut df = match txn.get_tb_field(ns, db, &self.what, &name).await? {
			Some(tb) => tb.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}

				return Err(Error::FdNotFound {
					name,
				}
				.into());
			}
		};
		// Process the statement
		if let Some(flex) = &self.flex {
			df.flex = *flex;
		}
		if let Some(kind) = &self.kind {
			df.kind.clone_from(kind);
		}
		if let Some(readonly) = &self.readonly {
			df.readonly = *readonly;
		}
		if let Some(value) = &self.value {
			df.value.clone_from(value);
		}
		if let Some(assert) = &self.assert {
			df.assert.clone_from(assert);
		}
		if let Some(default) = &self.default {
			df.default.clone_from(default);
		}
		if let Some(permissions) = &self.permissions {
			df.permissions = permissions.clone();
		}
		if let Some(comment) = &self.comment {
			df.comment.clone_from(comment);
		}
		if let Some(reference) = &self.reference {
			df.reference.clone_from(reference);

			// Validate reference options
			if df.reference.is_some() {
				df.validate_reference_options(ctx)?;
			}
		}
		if let Some(default_always) = &self.default_always {
			df.default_always = *default_always;
		}

		// Validate reference options
		df.validate_reference_options(ctx)?;

		// Correct reference type
		if let Some(kind) = df.get_reference_kind(ctx, opt).await? {
			df.kind = Some(kind);
		}

		// Disallow mismatched types
		df.disallow_mismatched_types(ctx, ns, db).await?;

		// Set the table definition
		let key = crate::key::table::fd::new(ns, db, &self.what, &name);
		txn.set(&key, &df, None).await?;
		// Refresh the table cache
		let key = crate::key::database::tb::new(ns, db, &self.what);
		let Some(tb) = txn.get_tb(ns, db, &self.what).await? else {
			return Err(Error::TbNotFound {
				name: self.what.to_string(),
			}
			.into());
		};
		txn.set(
			&key,
			&TableDefinition {
				cache_fields_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
			None,
		)
		.await?;
		// Clear the cache
		txn.clear();
		// Process possible recursive defitions
		df.process_recursive_definitions(ns, db, txn.clone()).await?;
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for AlterFieldStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ALTER FIELD")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		if let Some(flex) = self.flex {
			if flex {
				write!(f, " FLEXIBLE")?;
			} else {
				write!(f, " DROP FLEXIBLE")?;
			}
		}
		if let Some(kind) = &self.kind {
			if let Some(kind) = kind {
				write!(f, " TYPE {kind}")?;
			} else {
				write!(f, " DROP TYPE")?;
			}
		}
		if let Some(readonly) = self.readonly {
			if readonly {
				write!(f, " READONLY")?;
			} else {
				write!(f, " DROP READONLY")?;
			}
		}
		if let Some(value) = &self.value {
			if let Some(value) = value {
				write!(f, " VALUE {value}")?;
			} else {
				write!(f, " DROP VALUE")?;
			}
		}
		if let Some(assert) = &self.assert {
			if let Some(assert) = assert {
				write!(f, " ASSERT {assert}")?;
			} else {
				write!(f, " DROP ASSERT")?;
			}
		}
		if let Some(default) = &self.default {
			if let Some(default) = default {
				write!(f, " DEFAULT")?;
				if self.default_always.is_some_and(|x| x) {
					write!(f, " ALWAYS")?;
				}

				write!(f, " {default}")?;
			} else {
				write!(f, " DROP DEFAULT")?;
			}
		}
		if let Some(permissions) = &self.permissions {
			write!(f, "{permissions}")?;
		}
		if let Some(comment) = &self.comment {
			if let Some(comment) = comment {
				write!(f, " COMMENT {comment}")?;
			} else {
				write!(f, " DROP COMMENT")?;
			}
		}
		if let Some(reference) = &self.reference {
			if let Some(reference) = reference {
				write!(f, " REFERENCE {reference}")?;
			} else {
				write!(f, " DROP REFERENCE")?;
			}
		}
		Ok(())
	}
}
