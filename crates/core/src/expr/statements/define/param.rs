use std::fmt::{self, Display, Write};

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use super::DefineKind;
use crate::catalog::{ParamDefinition, Permission};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::fmt::{is_pretty, pretty_indent};
use crate::expr::{Base, Expr, FlowResultExt as _, Ident};
use crate::iam::{Action, ResourceKind};
use crate::val::{Strand, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineParamStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub value: Expr,
	pub comment: Option<Strand>,
	pub permissions: Permission,
}

impl DefineParamStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Parameter, &Base::Db)?;

		let value = stk.run(|stk| self.value.compute(stk, ctx, opt, doc)).await.catch_return()?;

		// Fetch the transaction
		let txn = ctx.tx();

		// Check if the definition exists
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		if txn.get_db_param(ns, db, &self.name).await.is_ok() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::PaAlreadyExists {
							name: self.name.to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => return Ok(Value::None),
			}
		}

		let db = {
			let (ns, db) = opt.ns_db()?;
			txn.get_or_add_db(ns, db, opt.strict).await?
		};

		// Process the statement
		let key = crate::key::database::pa::new(db.namespace_id, db.database_id, &self.name);
		txn.set(
			&key,
			&ParamDefinition {
				value,
				name: self.name.to_raw_string(),
				comment: self.comment.clone().map(|s| s.into_string()),
				permissions: self.permissions.clone(),
			},
			None,
		)
		.await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineParamStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE PARAM")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " ${} VALUE {}", self.name, self.value)?;
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
