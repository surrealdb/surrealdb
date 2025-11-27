use std::fmt::{self, Display};
use std::ops::Deref;

use anyhow::Result;
use uuid::Uuid;

use super::AlterKind;
use crate::catalog::providers::TableProvider;
use crate::catalog::{self, Permission, Permissions, TableDefinition};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::reference::Reference;
use crate::expr::{Base, Expr, Idiom, Kind};
use crate::fmt::{EscapeKwIdent, QuoteStr};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) enum AlterDefault {
	#[default]
	None,
	Drop,
	Always(Expr),
	Set(Expr),
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct AlterFieldStatement {
	pub name: Idiom,
	pub what: String,
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

impl AlterFieldStatement {
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Field, &Base::Db)?;
		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
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

		match self.kind {
			AlterKind::Set(ref k) => df.field_kind = Some(k.clone()),
			AlterKind::Drop => df.field_kind = None,
			AlterKind::None => {}
		}
		match self.flexible {
			AlterKind::Set(_) => df.flexible = true,
			AlterKind::Drop => df.flexible = false,
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
			AlterDefault::Drop => df.default = catalog::DefineDefault::None,
			AlterDefault::Always(ref expr) => {
				df.default = catalog::DefineDefault::Always(expr.clone())
			}
			AlterDefault::Set(ref expr) => df.default = catalog::DefineDefault::Set(expr.clone()),
		}

		fn convert_permission(perm: &Permission) -> catalog::Permission {
			match perm {
				Permission::None => catalog::Permission::None,
				Permission::Full => catalog::Permission::Full,
				Permission::Specific(expr) => catalog::Permission::Specific(expr.clone()),
			}
		}

		if let Some(permissions) = &self.permissions {
			df.select_permission = convert_permission(&permissions.select);
			df.create_permission = convert_permission(&permissions.create);
			df.update_permission = convert_permission(&permissions.update);
		}

		match self.comment {
			AlterKind::Set(ref k) => df.comment = Some(k.clone()),
			AlterKind::Drop => df.comment = None,
			AlterKind::None => {}
		}

		match self.reference {
			AlterKind::Set(ref k) => df.reference = Some(k.clone()),
			AlterKind::Drop => df.reference = None,
			AlterKind::None => {}
		}

		// Disallow mismatched types
		//df.disallow_mismatched_types(ctx, ns, db).await?;

		// Set the table definition
		let key = crate::key::table::fd::new(ns, db, &self.what, &name);
		txn.set(&key, &df, None).await?;
		// Refresh the table cache
		let Some(tb) = txn.get_tb(ns, db, &self.what).await? else {
			return Err(Error::TbNotFound {
				name: self.what.clone(),
			}
			.into());
		};
		txn.put_tb(
			ns_name,
			db_name,
			&TableDefinition {
				cache_fields_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
		)
		.await?;
		// Process possible recursive defitions
		//df.process_recursive_definitions(ns, db, txn.clone()).await?;
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
		write!(f, " {} ON {}", self.name, EscapeKwIdent(&self.what, &["IF"]))?;

		match self.kind {
			AlterKind::Set(ref x) => write!(f, " TYPE {x}")?,
			AlterKind::Drop => write!(f, " DROP TYPE")?,
			AlterKind::None => {}
		}

		match self.flexible {
			AlterKind::Set(_) => write!(f, " FLEXIBLE")?,
			AlterKind::Drop => write!(f, " DROP FLEXIBLE")?,
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
			AlterKind::Set(ref x) => write!(f, " COMMENT {}", QuoteStr(x))?,
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
