use std::ops::Deref;
use std::str;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned};
use surrealdb_types::{SqlFormat, ToSql};

use super::FlowResultExt as _;
use crate::catalog::Permission;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::fmt::EscapeKwFreeIdent;
use crate::iam::Action;
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct Param(String);

impl Revisioned for Param {
	fn revision() -> u16 {
		String::revision()
	}
}

impl SerializeRevisioned for Param {
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		w: &mut W,
	) -> std::result::Result<(), revision::Error> {
		SerializeRevisioned::serialize_revisioned(&self.0, w)
	}
}

impl DeserializeRevisioned for Param {
	fn deserialize_revisioned<R: std::io::Read>(
		r: &mut R,
	) -> std::result::Result<Self, revision::Error>
	where
		Self: Sized,
	{
		DeserializeRevisioned::deserialize_revisioned(r).map(Param)
	}
}

impl Param {
	/// Create a new identifier
	///
	/// This function checks if the string has a null byte, returns None if it
	/// has.
	pub fn new(str: String) -> Self {
		Self(str)
	}

	// Convert into a string.
	pub fn into_string(self) -> String {
		self.0
	}

	/// returns the identifier section of the parameter,
	/// i.e. `$foo` without the `$` so: `foo`
	pub fn as_str(&self) -> &str {
		&self.0
	}
}

impl From<String> for Param {
	fn from(v: String) -> Self {
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
		match self.0.as_str() {
			// This is a special param
			"this" | "self" => match ctx.value("this") {
				// $this was explicitly set in context (e.g. field ASSERT/VALUE
				// expressions), so subqueries can still reference the original
				// document being validated via $this
				Some(v) => Ok(v.clone()),
				// Otherwise use the current cursor document
				None => match doc {
					Some(v) => Ok(v.doc.as_ref().clone()),
					None => Ok(Value::None),
				},
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

impl ToSql for Param {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('$');
		EscapeKwFreeIdent(&self.0).fmt_sql(f, fmt);
	}
}
