use std::fmt::{self, Display};

use anyhow::{Result, bail};
use argon2::Argon2;
use argon2::password_hash::{PasswordHasher, SaltString};
use rand::Rng as _;
use rand::distributions::Alphanumeric;
use rand::rngs::OsRng;

use super::DefineKind;
use crate::catalog::{self, UserDefinition};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::escape::QuoteStr;
use crate::expr::fmt::Fmt;
use crate::expr::statements::info::InfoStructure;
use crate::expr::user::UserDuration;
use crate::expr::{Base, Ident};
use crate::iam::{Action, ResourceKind};
use crate::val::{self, Strand, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct DefineUserStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub base: Base,
	pub hash: String,
	pub code: String,
	pub roles: Vec<Ident>,
	pub duration: UserDuration,
	pub comment: Option<Strand>,
}

impl DefineUserStatement {
	pub fn new_with_password(base: Base, user: Strand, pass: &str, role: Ident) -> Self {
		DefineUserStatement {
			kind: DefineKind::Default,
			base,
			name: Ident::from_strand(user),
			hash: Argon2::default()
				.hash_password(pass.as_ref(), &SaltString::generate(&mut OsRng))
				.unwrap()
				.to_string(),
			code: rand::thread_rng()
				.sample_iter(&Alphanumeric)
				.take(128)
				.map(char::from)
				.collect::<String>(),
			roles: vec![role],
			duration: UserDuration::default(),
			comment: None,
		}
	}

	pub fn into_definition(&self) -> catalog::UserDefinition {
		UserDefinition {
			name: self.name.clone().to_string(),
			hash: self.hash.clone(),
			code: self.code.clone(),
			roles: self.roles.iter().map(|x| x.clone().into_string()).collect(),
			token_duration: self.duration.token.map(|x| x.0),
			session_duration: self.duration.session.map(|x| x.0),
			comment: self.comment.as_ref().map(|x| x.clone().into_string()),
		}
	}

	pub fn from_definition(base: Base, def: &catalog::UserDefinition) -> Self {
		Self {
			kind: DefineKind::Default,
			base,
			name: Ident::new(def.name.clone()).unwrap(),
			hash: def.hash.clone(),
			code: def.code.clone(),
			roles: def.roles.iter().map(|x| Ident::new(x.clone()).unwrap()).collect(),
			duration: UserDuration {
				token: def.token_duration.map(val::Duration),
				session: def.session_duration.map(val::Duration),
			},
			comment: def.comment.as_ref().map(|x| Strand::new(x.clone()).unwrap()),
		}
	}

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
				if let Some(user) = txn.get_root_user(&self.name).await? {
					match self.kind {
						DefineKind::Default => {
							if !opt.import {
								bail!(Error::UserRootAlreadyExists {
									name: user.name.to_string(),
								});
							}
						}
						DefineKind::Overwrite => {}
						DefineKind::IfNotExists => return Ok(Value::None),
					}
				}
				// Process the statement
				let key = crate::key::root::us::new(&self.name);
				txn.set(&key, &self.into_definition(), None).await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
			Base::Ns => {
				// Fetch the transaction
				let txn = ctx.tx();
				let ns = ctx.get_ns_id(opt).await?;
				// Check if the definition exists
				if let Some(user) = txn.get_ns_user(ns, &self.name).await? {
					match self.kind {
						DefineKind::Default => {
							if !opt.import {
								bail!(Error::UserNsAlreadyExists {
									name: user.name.to_string(),
									ns: opt.ns()?.into(),
								});
							}
						}
						DefineKind::Overwrite => {}
						DefineKind::IfNotExists => return Ok(Value::None),
					}
				}

				let ns = {
					let ns = opt.ns()?;
					txn.get_or_add_ns(ns, opt.strict).await?
				};

				// Process the statement
				let key = crate::key::namespace::us::new(ns.namespace_id, &self.name);
				txn.set(&key, &self.into_definition(), None).await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
			Base::Db => {
				// Fetch the transaction
				let txn = ctx.tx();
				// Check if the definition exists
				let (ns, db) = ctx.get_ns_db_ids(opt).await?;
				if let Some(user) = txn.get_db_user(ns, db, &self.name).await? {
					match self.kind {
						DefineKind::Default => {
							if !opt.import {
								bail!(Error::UserDbAlreadyExists {
									name: user.name.to_string(),
									ns: opt.ns()?.to_string(),
									db: opt.db()?.to_string(),
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
				let key =
					crate::key::database::us::new(db.namespace_id, db.database_id, &self.name);
				txn.set(&key, &self.into_definition(), None).await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
		}
	}
}

impl Display for DefineUserStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE USER")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
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
		if let Some(ref comment) = self.comment {
			write!(f, " COMMENT {comment}")?
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
				"token".to_string() => self.duration.token.map(Value::from).unwrap_or(Value::None),
				"session".to_string() => self.duration.session.map(Value::from).unwrap_or(Value::None),
			}),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
