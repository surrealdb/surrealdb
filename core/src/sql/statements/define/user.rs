use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{
	escape::quote_str, fmt::Fmt, user::UserDuration, Base, Duration, Ident, Object, Strand, Value,
};
use argon2::{
	password_hash::{PasswordHasher, SaltString},
	Argon2,
};
use derive::Store;
use rand::{distributions::Alphanumeric, rngs::OsRng, Rng};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
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
}

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
		}
	}
}

impl DefineUserStatement {
	pub(crate) fn from_parsed_values(
		name: Ident,
		base: Base,
		roles: Vec<Ident>,
		duration: UserDuration,
	) -> Self {
		DefineUserStatement {
			name,
			base,
			roles,
			duration,
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

	pub(crate) fn set_token_duration(&mut self, duration: Option<Duration>) {
		self.duration.token = duration;
	}

	pub(crate) fn set_session_duration(&mut self, duration: Option<Duration>) {
		self.duration.session = duration;
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
				if run.get_root_user(&self.name).await.is_ok() {
					if self.if_not_exists {
						return Ok(Value::None);
					} else {
						return Err(Error::UserRootAlreadyExists {
							value: self.name.to_string(),
						});
					}
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
				if run.get_ns_user(opt.ns()?, &self.name).await.is_ok() {
					if self.if_not_exists {
						return Ok(Value::None);
					} else {
						return Err(Error::UserNsAlreadyExists {
							value: self.name.to_string(),
							ns: opt.ns()?.into(),
						});
					}
				}
				// Process the statement
				let key = crate::key::namespace::us::new(opt.ns()?, &self.name);
				run.add_ns(opt.ns()?, opt.strict).await?;
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
				if run.get_db_user(opt.ns()?, opt.db()?, &self.name).await.is_ok() {
					if self.if_not_exists {
						return Ok(Value::None);
					} else {
						return Err(Error::UserDbAlreadyExists {
							value: self.name.to_string(),
							ns: opt.ns()?.into(),
							db: opt.db()?.into(),
						});
					}
				}
				// Process the statement
				let key = crate::key::database::us::new(opt.ns()?, opt.db()?, &self.name);
				run.add_ns(opt.ns()?, opt.strict).await?;
				run.add_db(opt.ns()?, opt.db()?, opt.strict).await?;
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
		let Self {
			name,
			base,
			hash,
			roles,
			duration,
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

		let mut dur = Object::default();
		dur.insert("token".to_string(), duration.token.into());
		dur.insert("session".to_string(), duration.session.into());
		acc.insert("duration".to_string(), dur.to_string().into());

		if let Some(comment) = comment {
			acc.insert("comment".to_string(), comment.into());
		}

		Value::Object(acc)
	}
}
