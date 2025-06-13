use crate::{
	ctx::Context,
	dbs::Options,
	doc::CursorDoc,
	err::Error,
	expr::{Permission, ident::Ident},
	iam::Action,
};
use anyhow::{Result, bail};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::{fmt, ops::Deref, str};

use super::FlowResultExt as _;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Param";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Param")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Param(pub Ident);

impl From<Ident> for Param {
	fn from(v: Ident) -> Self {
		Self(v)
	}
}

impl From<String> for Param {
	fn from(v: String) -> Self {
		Self(v.into())
	}
}

impl From<&str> for Param {
	fn from(v: &str) -> Self {
		Self(v.into())
	}
}

impl Deref for Param {
	type Target = Ident;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Param {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Find the variable by name
		match self.as_str() {
			// This is a special param
			"this" | "self" => match doc {
				// The base document exists
				Some(v) => v.doc.as_ref().compute(stk, ctx, opt, doc).await.catch_return(),
				// The base document does not exist
				None => Ok(Value::None),
			},
			// This is a normal param
			v => match ctx.value(v) {
				// The param has been set locally
				Some(v) => v.compute(stk, ctx, opt, doc).await.catch_return(),
				// The param has not been set locally
				None => {
					// Ensure a database is set
					opt.valid_for_db()?;
					// Fetch a defined param if set
					let (ns, db) = opt.ns_db()?;
					let val = ctx.tx().get_db_param(ns, db, v).await;
					// Check if the param has been set globally
					let val = match val {
						Ok(x) => x,
						Err(e) => {
							if matches!(e.downcast_ref(), Some(Error::PaNotFound { .. })) {
								return Ok(Value::None);
							} else {
								return Err(e);
							}
						}
					};

					if opt.check_perms(Action::View)? {
						match &val.permissions {
							Permission::Full => (),
							Permission::None => {
								bail!(Error::ParamPermissions {
									name: v.to_owned(),
								})
							}
							Permission::Specific(e) => {
								// Disable permissions
								let opt = &opt.new_with_perms(false);
								// Process the PERMISSION clause
								if !e.compute(stk, ctx, opt, doc).await.catch_return()?.is_truthy()
								{
									bail!(Error::ParamPermissions {
										name: v.to_owned(),
									});
								}
							}
						}
					}
					// Return the computed value
					val.value.compute(stk, ctx, opt, doc).await.catch_return()
				}
			},
		}
	}
}

impl fmt::Display for Param {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "${}", &self.0.0)
	}
}
