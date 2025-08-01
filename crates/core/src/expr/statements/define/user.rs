use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Base, Ident, Strand, Value, escape::QuoteStr, fmt::Fmt, user::UserDuration};
use crate::iam::{Action, ResourceKind};
use crate::kvs::impl_kv_value_revisioned;
use anyhow::{Result, bail};
use argon2::{
	Argon2,
	password_hash::{PasswordHasher, SaltString},
};

use rand::{Rng, distributions::Alphanumeric, rngs::OsRng};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 4)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineUserStatement {
	pub name: Ident,
	pub base: Base,
	pub hash: String,
	pub code: String,
	pub roles: Vec<Ident>,
	#[revision(start = 3)]
	pub duration: UserDuration,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
	#[revision(start = 4)]
	pub overwrite: bool,
}

impl_kv_value_revisioned!(DefineUserStatement);

#[expect(clippy::fallible_impl_from)]
impl From<(Base, &str, &str, &str)> for DefineUserStatement {
	fn from((base, user, pass, role): (Base, &str, &str, &str)) -> Self {
		DefineUserStatement {
			base,
			name: user.into(),
			hash: Argon2::default()
				.hash_password(pass.as_ref(), &SaltString::generate(&mut OsRng))
				.unwrap()
				.to_string(),
			code: rand::thread_rng()
				.sample_iter(&Alphanumeric)
				.take(128)
				.map(char::from)
				.collect::<String>(),
			roles: vec![role.into()],
			duration: UserDuration::default(),
			comment: None,
			if_not_exists: false,
			overwrite: false,
		}
	}
}

impl DefineUserStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;
		// Check the statement type
		match self.base {
			Base::Root => {
				// Fetch the transaction
				let txn = ctx.tx();
				// Check if the definition exists
				if txn.get_root_user(&self.name).await.is_ok() {
					if self.if_not_exists {
						return Ok(Value::None);
					} else if !self.overwrite && !opt.import {
						bail!(Error::UserRootAlreadyExists {
							name: self.name.to_string(),
						});
					}
				}
				// Process the statement
				let key = crate::key::root::us::new(&self.name);
				txn.set(
					&key,
					&DefineUserStatement {
						// Don't persist the `IF NOT EXISTS` clause to schema
						if_not_exists: false,
						overwrite: false,
						..self.clone()
					},
					None,
				)
				.await?;
				// Clear the cache
				txn.clear();
				// Ok all good
				Ok(Value::None)
			}
			Base::Ns => {
				// Fetch the transaction
				let txn = ctx.tx();
				let ns = ctx.get_ns_id(opt).await?;
				// Check if the definition exists
				if txn.get_ns_user(ns, &self.name).await.is_ok() {
					if self.if_not_exists {
						return Ok(Value::None);
					} else if !self.overwrite && !opt.import {
						bail!(Error::UserNsAlreadyExists {
							name: self.name.to_string(),
							ns: opt.ns()?.into(),
						});
					}
				}

				let ns = {
					let ns = opt.ns()?;
					txn.get_or_add_ns(ns, opt.strict).await?
				};

				// Process the statement
				let key = crate::key::namespace::us::new(ns.namespace_id, &self.name);
				txn.set(
					&key,
					&DefineUserStatement {
						// Don't persist the `IF NOT EXISTS` clause to schema
						if_not_exists: false,
						overwrite: false,
						..self.clone()
					},
					None,
				)
				.await?;
				// Clear the cache
				txn.clear();
				// Ok all good
				Ok(Value::None)
			}
			Base::Db => {
				// Fetch the transaction
				let txn = ctx.tx();
				// Check if the definition exists
				let (ns, db) = ctx.get_ns_db_ids(opt).await?;
				if txn.get_db_user(ns, db, &self.name).await.is_ok() {
					if self.if_not_exists {
						return Ok(Value::None);
					} else if !self.overwrite && !opt.import {
						let (ns, db) = opt.ns_db()?;
						bail!(Error::UserDbAlreadyExists {
							name: self.name.to_string(),
							ns: ns.to_string(),
							db: db.to_string(),
						});
					}
				}

				let db = {
					let (ns, db) = opt.ns_db()?;
					txn.get_or_add_db(ns, db, opt.strict).await?
				};

				// Process the statement
				let key =
					crate::key::database::us::new(db.namespace_id, db.database_id, &self.name);
				txn.set(
					&key,
					&DefineUserStatement {
						// Don't persist the `IF NOT EXISTS` clause to schema
						if_not_exists: false,
						overwrite: false,
						..self.clone()
					},
					None,
				)
				.await?;
				// Clear the cache
				txn.clear();
				// Ok all good
				Ok(Value::None)
			}
			// Other levels are not supported
			_ => Err(anyhow::Error::new(Error::InvalidLevel(self.base.to_string()))),
		}
	}
}

impl Display for DefineUserStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE USER")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(
			f,
			" {} ON {} PASSHASH {} ROLES {}",
			self.name,
			self.base,
			QuoteStr(&self.hash),
			Fmt::comma_separated(
				&self.roles.iter().map(|r| r.to_string().to_uppercase()).collect::<Vec<String>>()
			),
		)?;
		// Always print relevant durations so defaults can be changed in the future
		// If default values were not printed, exports would not be forward compatible
		// None values need to be printed, as they are different from the default values
		write!(f, " DURATION")?;
		write!(
			f,
			" FOR TOKEN {},",
			match self.duration.token {
				Some(dur) => format!("{}", dur),
				None => "NONE".to_string(),
			}
		)?;
		write!(
			f,
			" FOR SESSION {}",
			match self.duration.session {
				Some(dur) => format!("{}", dur),
				None => "NONE".to_string(),
			}
		)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

impl InfoStructure for DefineUserStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"base".to_string() => self.base.structure(),
			"hash".to_string() => self.hash.into(),
			"roles".to_string() => self.roles.into_iter().map(Ident::structure).collect(),
			"duration".to_string() => Value::from(map! {
				"token".to_string() => self.duration.token.into(),
				"session".to_string() => self.duration.session.into(),
			}),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
