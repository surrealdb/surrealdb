use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{escape::quote_str, fmt::Fmt, Base, Ident, Object, Strand, Value};
use argon2::{
	password_hash::{PasswordHasher, SaltString},
	Argon2,
};
use derive::Store;
use rand::{distributions::Alphanumeric, rngs::OsRng, Rng};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineUserStatement {
	pub name: Ident,
	pub base: Base,
	pub hash: String,
	pub code: String,
	pub roles: Vec<Ident>,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
}

impl From<(Base, &str, &str)> for DefineUserStatement {
	fn from((base, user, pass): (Base, &str, &str)) -> Self {
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
			roles: vec!["owner".into()],
			comment: None,
			if_not_exists: false,
		}
	}
}

impl DefineUserStatement {
	pub(crate) fn from_parsed_values(name: Ident, base: Base, roles: Vec<Ident>) -> Self {
		DefineUserStatement {
			name,
			base,
			roles, // New users get the viewer role by default
			code: rand::thread_rng()
				.sample_iter(&Alphanumeric)
				.take(128)
				.map(char::from)
				.collect::<String>(),
			..Default::default()
		}
	}

	pub(crate) fn set_password(&mut self, password: &str) {
		self.hash = Argon2::default()
			.hash_password(password.as_bytes(), &SaltString::generate(&mut OsRng))
			.unwrap()
			.to_string()
	}

	pub(crate) fn set_passhash(&mut self, passhash: String) {
		self.hash = passhash;
	}

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;

		match self.base {
			Base::Root => {
				// Claim transaction
				let mut run = ctx.tx_lock().await;
				// Clear the cache
				run.clear_cache();
				// Check if user already exists
				if self.if_not_exists && run.get_root_user(&self.name).await.is_ok() {
					return Err(Error::UserRootAlreadyExists {
						value: self.name.to_string(),
					});
				}
				// Process the statement
				let key = crate::key::root::us::new(&self.name);
				run.set(
					key,
					DefineUserStatement {
						// Don't persist the "IF NOT EXISTS" clause to schema
						if_not_exists: false,
						..self.clone()
					},
				)
				.await?;
				// Ok all good
				Ok(Value::None)
			}
			Base::Ns => {
				// Claim transaction
				let mut run = ctx.tx_lock().await;
				// Clear the cache
				run.clear_cache();
				// Check if user already exists
				if self.if_not_exists && run.get_ns_user(opt.ns(), &self.name).await.is_ok() {
					return Err(Error::UserNsAlreadyExists {
						value: self.name.to_string(),
						ns: opt.ns().into(),
					});
				}
				// Process the statement
				let key = crate::key::namespace::us::new(opt.ns(), &self.name);
				run.add_ns(opt.ns(), opt.strict).await?;
				run.set(
					key,
					DefineUserStatement {
						// Don't persist the "IF NOT EXISTS" clause to schema
						if_not_exists: false,
						..self.clone()
					},
				)
				.await?;
				// Ok all good
				Ok(Value::None)
			}
			Base::Db => {
				// Claim transaction
				let mut run = ctx.tx_lock().await;
				// Clear the cache
				run.clear_cache();
				// Check if user already exists
				if self.if_not_exists
					&& run.get_db_user(opt.ns(), opt.db(), &self.name).await.is_ok()
				{
					return Err(Error::UserDbAlreadyExists {
						value: self.name.to_string(),
						ns: opt.ns().into(),
						db: opt.db().into(),
					});
				}
				// Process the statement
				let key = crate::key::database::us::new(opt.ns(), opt.db(), &self.name);
				run.add_ns(opt.ns(), opt.strict).await?;
				run.add_db(opt.ns(), opt.db(), opt.strict).await?;
				run.set(
					key,
					DefineUserStatement {
						// Don't persist the "IF NOT EXISTS" clause to schema
						if_not_exists: false,
						..self.clone()
					},
				)
				.await?;
				// Ok all good
				Ok(Value::None)
			}
			// Other levels are not supported
			_ => Err(Error::InvalidLevel(self.base.to_string())),
		}
	}
}

impl Display for DefineUserStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE USER")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		write!(
			f,
			" {} ON {} PASSHASH {} ROLES {}",
			self.name,
			self.base,
			quote_str(&self.hash),
			Fmt::comma_separated(
				&self.roles.iter().map(|r| r.to_string().to_uppercase()).collect::<Vec<String>>()
			)
		)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

impl InfoStructure for DefineUserStatement {
	fn structure(self) -> Value {
		let Self {
			name,
			base,
			hash,
			roles,
			comment,
			..
		} = self;
		let mut acc = Object::default();

		acc.insert("name".to_string(), name.structure());

		acc.insert("base".to_string(), base.structure());

		acc.insert("passhash".to_string(), hash.into());

		acc.insert(
			"roles".to_string(),
			Value::Array(roles.into_iter().map(|r| r.structure()).collect()),
		);

		if let Some(comment) = comment {
			acc.insert("comment".to_string(), comment.into());
		}

		Value::Object(acc)
	}
}
