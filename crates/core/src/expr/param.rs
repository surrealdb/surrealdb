use std::ops::Deref;
use std::{fmt, str};

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use super::FlowResultExt as _;
use crate::catalog::Permission;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::escape::EscapeKwFreeIdent;
use crate::expr::ident::Ident;
use crate::iam::Action;
use crate::val::{Strand, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Param(String);

impl Param {
	/// Create a new identifier
	///
	/// This function checks if the string has a null byte, returns None if it
	/// has.
	pub fn new(str: String) -> Option<Self> {
		if str.contains('\0') {
			return None;
		}
		Some(Self(str))
	}

	/// Create a new identifier
	///
	/// # Safety
	/// Caller should ensure that the string does not contain a null byte.
	pub unsafe fn new_unchecked(str: String) -> Self {
		Self(str)
	}

	/// returns the identifier section of the parameter,
	/// i.e. `$foo` without the `$` so: `foo`
	pub fn ident(self) -> Ident {
		// Safety: Param guarentees no null bytes within it's internal string.
		unsafe { Ident::new_unchecked(self.0) }
	}
}

impl From<Ident> for Param {
	fn from(v: Ident) -> Self {
		Self(v.to_string())
	}
}

impl From<Strand> for Param {
	fn from(v: Strand) -> Self {
		Self(v.into_string())
	}
}

impl Deref for Param {
	type Target = str;
	fn deref(&self) -> &Self::Target {
		self.0.as_str()
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
		match self.0.as_str() {
			// This is a special param
			"this" | "self" => match doc {
				// The base document exists
				Some(v) => Ok(v.doc.as_ref().clone()),
				// The base document does not exist
				None => Ok(Value::None),
			},
			// This is a normal param
			v => match ctx.value(v) {
				// The param has been set locally
				Some(v) => Ok(v.clone()),
				// The param has not been set locally
				None => {
					// Ensure a database is set
					opt.valid_for_db()?;
					// Fetch a defined param if set
					let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
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
								if !stk
									.run(|stk| e.compute(stk, ctx, opt, doc))
									.await
									.catch_return()?
									.is_truthy()
								{
									bail!(Error::ParamPermissions {
										name: v.to_owned(),
									});
								}
							}
						}
					}
					// Return the computed value
					Ok(val.value.clone())
				}
			},
		}
	}
}

impl fmt::Display for Param {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "${}", EscapeKwFreeIdent(&self.0))
	}
}
