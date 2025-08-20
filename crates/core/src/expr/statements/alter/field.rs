use std::fmt::{self, Display};
use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::AlterKind;
use crate::catalog::TableDefinition;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::reference::Reference;
use crate::expr::statements::define::DefineDefault;
use crate::expr::{Base, Expr, Ident, Idiom, Kind, Permissions};
use crate::iam::{Action, ResourceKind};
use crate::val::{Strand, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum AlterDefault {
	#[default]
	None,
	Drop,
	Always(Expr),
	Set(Expr),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct AlterFieldStatement {
	pub name: Idiom,
	pub what: Ident,
	pub if_exists: bool,
	pub flex: AlterKind<()>,
	pub kind: AlterKind<Kind>,
	pub readonly: AlterKind<()>,
	pub value: AlterKind<Expr>,
	pub assert: AlterKind<Expr>,
	pub default: AlterDefault,
	pub permissions: Option<Permissions>,
	pub comment: AlterKind<Strand>,
	pub reference: AlterKind<Reference>,
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
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
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

		match self.flex {
			AlterKind::Set(_) => df.flex = true,
			AlterKind::Drop => df.flex = false,
			AlterKind::None => {}
		}

		match self.kind {
			AlterKind::Set(ref k) => df.field_kind = Some(k.clone()),
			AlterKind::Drop => df.field_kind = None,
			AlterKind::None => {}
		}

		match self.readonly {
			AlterKind::Set(_) => df.readonly = true,
			AlterKind::Drop => df.readonly = false,
			AlterKind::None => {}
		}

		match self.value {
			AlterKind::Set(ref k) => df.value = Some(k.clone()),
			AlterKind::Drop => df.value = None,
			AlterKind::None => {}
		}

		match self.assert {
			AlterKind::Set(ref k) => df.assert = Some(k.clone()),
			AlterKind::Drop => df.assert = None,
			AlterKind::None => {}
		}

		match self.default {
			AlterDefault::None => {}
			AlterDefault::Drop => df.default = DefineDefault::None,
			AlterDefault::Always(ref expr) => df.default = DefineDefault::Always(expr.clone()),
			AlterDefault::Set(ref expr) => df.default = DefineDefault::Set(expr.clone()),
		}

		if let Some(permissions) = &self.permissions {
			df.permissions = permissions.clone();
		}

		match self.comment {
			AlterKind::Set(ref k) => df.comment = Some(k.clone()),
			AlterKind::Drop => df.comment = None,
			AlterKind::None => {}
		}

		match self.reference {
			AlterKind::Set(ref k) => {
				df.reference = Some(k.clone());
			}
			AlterKind::Drop => df.reference = None,
			AlterKind::None => {}
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
		txn.clear_cache();
		// Process possible recursive defitions
		df.process_recursive_definitions(ns, db, txn.clone()).await?;
		// Clear the cache
		txn.clear_cache();
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

		match self.flex {
			AlterKind::Set(_) => write!(f, " FLEXIBLE")?,
			AlterKind::Drop => write!(f, " DROP FLEXIBLE")?,
			AlterKind::None => {}
		}

		match self.kind {
			AlterKind::Set(ref x) => write!(f, " TYPE {x}")?,
			AlterKind::Drop => write!(f, " DROP TYPE")?,
			AlterKind::None => {}
		}

		match self.readonly {
			AlterKind::Set(_) => write!(f, " READONLY")?,
			AlterKind::Drop => write!(f, " DROP READONLY")?,
			AlterKind::None => {}
		}

		match self.value {
			AlterKind::Set(ref v) => write!(f, " VALUE {v}")?,
			AlterKind::Drop => write!(f, " DROP VALUE")?,
			AlterKind::None => {}
		}

		match self.assert {
			AlterKind::Set(ref v) => write!(f, " ASSERT {v}")?,
			AlterKind::Drop => write!(f, " DROP ASSERT")?,
			AlterKind::None => {}
		}

		match self.default {
			AlterDefault::None => {}
			AlterDefault::Drop => write!(f, " DROP DEFAULT")?,
			AlterDefault::Always(ref expr) => write!(f, "DEFAULT ALWAYS {expr}")?,
			AlterDefault::Set(ref expr) => write!(f, "DEFAULT {expr}")?,
		}
		if let Some(permissions) = &self.permissions {
			write!(f, "{permissions}")?;
		}

		match self.comment {
			AlterKind::Set(ref v) => write!(f, " COMMENT {v}")?,
			AlterKind::Drop => write!(f, " DROP COMMENT")?,
			AlterKind::None => {}
		}

		match self.reference {
			AlterKind::Set(ref v) => write!(f, " REFERENCE {v}")?,
			AlterKind::Drop => write!(f, " DROP REFERENCE")?,
			AlterKind::None => {}
		}

		Ok(())
	}
}
