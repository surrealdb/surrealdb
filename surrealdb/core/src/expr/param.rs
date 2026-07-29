use std::ops::Deref;
use std::str;

use anyhow::{Result, bail};
use common::fmt::EscapeKwFreeIdent;
use reblessive::tree::Stk;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use super::FlowResultExt as _;
use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{Error as CatalogError, Permission};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exec::Error as ExecError;
use crate::iam::Action;
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct Param(Strand);

impl Param {
	/// Convert into the underlying `Strand`.
	pub fn into_strand(self) -> Strand {
		self.0
	}

	/// returns the identifier section of the parameter,
	/// i.e. `$foo` without the `$` so: `foo`
	pub fn as_str(&self) -> &str {
		self.0.as_str()
	}
}

impl From<String> for Param {
	fn from(v: String) -> Self {
		Self(v.into())
	}
}

impl From<Strand> for Param {
	fn from(v: Strand) -> Self {
		Self(v)
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
	#[instrument(level = "trace", name = "Param::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Find the variable by name
		match self.as_str() {
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
					let Some((ns, db)) = ctx.try_ns_db_ids(opt).await? else {
						// If the database does not exist, then a defined param won't exist either
						// No need to create an ns/db for this, let's just return None
						return Ok(Value::None);
					};

					let val = ctx.tx().get_db_param(ns, db, v, opt.version).await;
					// Check if the param has been set globally
					let val = match val {
						Ok(x) => x,
						Err(e) => {
							if matches!(e.downcast_ref(), Some(CatalogError::PaNotFound { .. })) {
								return Ok(Value::None);
							} else {
								return Err(e);
							}
						}
					};

					if ctx.check_perms(opt, Action::View)? {
						match &val.permissions {
							Permission::Full => (),
							Permission::None => {
								bail!(ExecError::ParamPermissions {
									name: v.to_owned(),
								})
							}
							Permission::Specific(e) => {
								// Disable permission recursion and block side effects
								let opt = &opt.new_for_permission_predicate();
								// Process the PERMISSION clause
								if !stk
									.run(|stk| e.compute(stk, ctx, opt, doc))
									.await
									.catch_return()?
									.is_truthy()
								{
									bail!(ExecError::ParamPermissions {
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

impl ToSql for Param {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('$');
		EscapeKwFreeIdent(self.as_str()).fmt_sql(f, fmt);
	}
}
