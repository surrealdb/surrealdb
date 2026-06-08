use std::ops::Deref;
use std::str;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned};
use surrealdb_strand::Strand;
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
pub(crate) struct Param(Strand);

impl Revisioned for Param {
	fn revision() -> u16 {
		Strand::revision()
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

impl revision::SkipRevisioned for Param {
	fn skip_revisioned<R: std::io::Read>(r: &mut R) -> std::result::Result<(), revision::Error> {
		<Strand as revision::SkipRevisioned>::skip_revisioned(r)
	}
}

impl revision::WalkRevisioned for Param {
	type Walker<'r, R: revision::BorrowedReader + 'r> = revision::LeafWalker<'r, Param, R>;

	fn walk_revisioned<'r, R: revision::BorrowedReader>(
		reader: &'r mut R,
	) -> std::result::Result<Self::Walker<'r, R>, revision::Error> {
		Ok(revision::LeafWalker::new(reader))
	}
}

impl revision::LengthPrefixedBytes for Param {}

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
							if matches!(e.downcast_ref(), Some(Error::PaNotFound { .. })) {
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
		EscapeKwFreeIdent(self.as_str()).fmt_sql(f, fmt);
	}
}

#[cfg(test)]
mod length_prefixed_bytes_tests {
	use revision::{DeserializeRevisioned, SerializeRevisioned, SkipRevisioned, WalkRevisioned};

	use super::Param;

	#[test]
	fn param_with_bytes_matches_serialize() {
		let param = Param::from("my_var".to_string());
		let mut bytes = Vec::new();
		param.serialize_revisioned(&mut bytes).unwrap();
		let mut r = bytes.as_slice();
		let walker = Param::walk_revisioned(&mut r).unwrap();
		let observed = walker.with_bytes(|raw| raw.to_vec()).unwrap();
		assert_eq!(observed.as_slice(), b"my_var");
		assert!(r.is_empty());
	}

	#[test]
	fn param_skip_leaves_reader_consumed_like_decode() {
		let param = Param::from("skip_me".to_string());
		let mut bytes = Vec::new();
		param.serialize_revisioned(&mut bytes).unwrap();
		let mut skip_reader = bytes.as_slice();
		Param::skip_revisioned(&mut skip_reader).unwrap();
		assert!(skip_reader.is_empty());
		let roundtrip = Param::deserialize_revisioned(&mut bytes.as_slice()).unwrap();
		assert_eq!(roundtrip, param);
	}
}
