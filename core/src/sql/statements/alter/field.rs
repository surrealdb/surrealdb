use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::{Base, Ident, Idiom, Kind, Permissions, Strand, Value};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};
use std::ops::Deref;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
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
}

impl AlterFieldStatement {
	pub(crate) async fn compute(
		&self,
		_stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
		// Get the NS and DB
		let ns = opt.ns()?;
		let db = opt.db()?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Get the name of the field
		let fd = self.name.to_string();
		// Get the table definition
		let mut df = match txn.get_tb_field(opt.ns()?, opt.db()?, &self.what, &fd).await {
			Ok(df) => df.deref().clone(),
			Err(Error::FdNotFound {
				..
			}) if self.if_exists => return Ok(Value::None),
			Err(v) => return Err(v),
		};
		// Process the statement
		let key = crate::key::table::fd::new(ns, db, &self.what, &fd);
		if let Some(ref flex) = &self.flex {
			df.flex = *flex;
		}
		if let Some(ref kind) = &self.kind {
			df.kind = kind.clone();
		}
		if let Some(ref readonly) = &self.readonly {
			df.readonly = *readonly;
		}
		if let Some(ref value) = &self.value {
			df.value = value.clone();
		}
		if let Some(ref assert) = &self.assert {
			df.assert = assert.clone();
		}
		if let Some(ref default) = &self.default {
			df.default = default.clone();
		}
		if let Some(ref permissions) = &self.permissions {
			df.permissions = permissions.clone();
		}
		if let Some(ref comment) = &self.comment {
			df.comment = comment.clone();
		}

		txn.set(key, &df).await?;

		// Process nested field definitions
		df.process_nested_fields(ctx, opt).await?;
		// Process in and out fields
		df.process_in_out_fields(ctx, opt).await?;
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
		write!(f, " {} ON {}", self.what, self.name)?;
		if let Some(flex) = self.flex {
			write!(f, " FLEXIBLE {flex}")?
		}
		if let Some(ref kind) = &self.kind {
			write!(f, " TYPE {}", kind.clone().map_or("UNSET".into(), |v| v.to_string()))?
		}
		if let Some(default) = &self.default {
			write!(f, " DEFAULT {}", default.clone().map_or("UNSET".into(), |v| v.to_string()))?
		}
		if let Some(readonly) = self.readonly {
			write!(f, " READONLY {readonly}")?
		}
		if let Some(ref value) = &self.value {
			write!(f, " VALUE {}", value.clone().map_or("UNSET".into(), |v| v.to_string()))?
		}
		if let Some(ref assert) = &self.assert {
			write!(f, " ASSERT {}", assert.clone().map_or("UNSET".into(), |v| v.to_string()))?
		}
		if let Some(ref comment) = &self.comment {
			write!(f, " COMMENT {}", comment.clone().map_or("UNSET".into(), |v| v.to_string()))?
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		if let Some(permissions) = &self.permissions {
			write!(f, "{permissions}")?;
		}
		Ok(())
	}
}
