use crate::{
	ctx::Context,
	dbs::Options,
	doc::CursorDoc,
	err::Error,
	iam::Action,
	sql::{ident::Ident, value::Value, Permission},
};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::{fmt, ops::Deref, str};

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
	) -> Result<Value, Error> {
		// Find the variable by name
		match self.as_str() {
			// This is a special param
			"this" | "self" => match doc {
				// The base document exists
				Some(v) => match &v.rid {
					Some(v) => Ok(Value::Thing(v.clone().deref().to_owned())),
					None => v.doc.as_ref().compute(stk, ctx, opt, doc).await,
				},
				// The base document does not exist
				None => Ok(Value::None),
			},
			// This is a normal param
			v => match ctx.value(v) {
				// The param has been set locally
				Some(v) => v.compute(stk, ctx, opt, doc).await,
				// The param has not been set locally
				None => {
					// Ensure a database is set
					opt.valid_for_db()?;
					// Fetch a defined param if set
					let val = ctx.tx().get_db_param(opt.ns()?, opt.db()?, v).await;
					// Check if the param has been set globally
					match val {
						// The param has been set globally
						Ok(val) => {
							// Check permissions
							if opt.check_perms(Action::View)? {
								match &val.permissions {
									Permission::Full => (),
									Permission::None => {
										return Err(Error::ParamPermissions {
											name: v.to_owned(),
										})
									}
									Permission::Specific(e) => {
										// Disable permissions
										let opt = &opt.new_with_perms(false);
										// Process the PERMISSION clause
										if !e.compute(stk, ctx, opt, doc).await?.is_truthy() {
											return Err(Error::ParamPermissions {
												name: v.to_owned(),
											});
										}
									}
								}
							}
							// Return the computed value
							val.value.compute(stk, ctx, opt, doc).await
						}
						// The param has not been set globally
						Err(Error::PaNotFound {
							..
						}) => Ok(Value::None),
						// There was another request error
						Err(e) => Err(e),
					}
				}
			},
		}
	}
}

impl fmt::Display for Param {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "${}", &self.0 .0)
	}
}
