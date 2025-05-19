use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::kvs::KeyEncode;
use crate::sql::fmt::{is_pretty, pretty_indent, Fmt};
use crate::sql::function::FunctionVersion;
use crate::sql::statements::info::InfoStructure;
use crate::sql::{
	Base, Block, CustomFunctionName, File, FlowResultExt, Ident, Kind, Permission, Strand, Value,
};

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[revisioned(revision = 5)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineFunctionStatement {
	pub name: Ident,
	#[revision(end = 5, convert_fn = "convert_args")]
	pub args: Vec<(Ident, Kind)>,
	#[revision(end = 5, convert_fn = "convert_block")]
	pub block: Block,
	pub comment: Option<Strand>,
	pub permissions: Permission,
	#[revision(start = 2)]
	pub if_not_exists: bool,
	#[revision(start = 3)]
	pub overwrite: bool,
	#[revision(start = 4, end = 5, convert_fn = "convert_returns")]
	pub returns: Option<Kind>,
	#[revision(start = 5)]
	pub executable: Executable,
}

impl DefineFunctionStatement {
	fn convert_args(
		&mut self,
		_revision: u16,
		old_args: Vec<(Ident, Kind)>,
	) -> Result<(), revision::Error> {
		if let Executable::Block {
			args,
			..
		} = &mut self.executable
		{
			*args = old_args;
		}

		Ok(())
	}
	fn convert_block(&mut self, _revision: u16, old_block: Block) -> Result<(), revision::Error> {
		if let Executable::Block {
			block,
			..
		} = &mut self.executable
		{
			*block = old_block;
		}

		Ok(())
	}
	fn convert_returns(
		&mut self,
		_revision: u16,
		old_returns: Option<Kind>,
	) -> Result<(), revision::Error> {
		if let Executable::Block {
			returns,
			..
		} = &mut self.executable
		{
			*returns = old_returns;
		}

		Ok(())
	}

	pub(crate) async fn args<'a>(&'a self) -> Result<&'a Vec<(Ident, Kind)>, Error> {
		match &self.executable {
			Executable::Block {
				args,
				..
			} => Ok(args),
			_ => Err(fail!("TODO compute args")),
		}
	}

	pub(crate) async fn returns<'a>(&'a self) -> Result<Option<&'a Kind>, Error> {
		match &self.executable {
			Executable::Block {
				returns,
				..
			} => Ok(returns.as_ref()),
			_ => Err(fail!("TODO compute returns")),
		}
	}

	pub(crate) async fn execute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		version: Option<&FunctionVersion>,
		submodule: Option<&Ident>,
	) -> Result<Value, Error> {
		match &self.executable {
			Executable::Block {
				block,
				..
			} => {
				if version.is_some() || submodule.is_some() {
					let name = CustomFunctionName {
						name: self.name.clone(),
						version: version.cloned(),
						submodule: submodule.cloned(),
					};

					Err(Error::FcNotFound {
						name: name.to_string(),
					})
				} else {
					block.compute(stk, ctx, opt, doc).await.catch_return()
				}
			}
			Executable::SurrealismPackage(_) => {
				let name = CustomFunctionName {
					name: self.name.clone(),
					version: version.cloned(),
					submodule: submodule.cloned(),
				};

				Err(Error::FcNotFound {
					name: name.to_string(),
				})
			}
			Executable::SiloPackage {
				organisation,
				package,
				..
			} => {
				if self.name.is_empty() {
					let name = CustomFunctionName {
						name: self.name.clone(),
						version: version.cloned(),
						submodule: submodule.cloned(),
					};

					Err(Error::FcNotFound {
						name: name.to_string(),
					})
				} else {
					let name = CustomFunctionName {
						name: format!("{}::{}", organisation.0, package.0).into(),
						version: version.cloned(),
						submodule: submodule.cloned(),
					};

					Err(Error::SiNotFound {
						name: name.to_string(),
					})
				}
			}
		}
	}
}

impl DefineFunctionStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Function, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		let (ns, db) = opt.ns_db()?;
		if txn.get_db_function(ns, db, &self.name).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else if !self.overwrite {
				return Err(Error::FcAlreadyExists {
					name: self.name.to_string(),
				});
			}
		}
		// Process the statement
		let key = match &self.executable {
			Executable::SiloPackage {
				organisation,
				package,
				..
			} if self.name.is_empty() => {
				let name = format!("{organisation}::{package}");
				crate::key::database::si::new(ns, db, &name).encode()?
			}
			_ => crate::key::database::fc::new(ns, db, &self.name).encode()?,
		};
		txn.get_or_add_ns(ns, opt.strict).await?;
		txn.get_or_add_db(ns, db, opt.strict).await?;
		txn.set(
			key,
			revision::to_vec(&DefineFunctionStatement {
				// Don't persist the `IF NOT EXISTS` clause to schema
				if_not_exists: false,
				overwrite: false,
				..self.clone()
			})?,
			None,
		)
		.await?;
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
}

impl fmt::Display for DefineFunctionStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE FUNCTION")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		match &self.executable {
			Executable::Block {
				block,
				args,
				returns,
			} => {
				write!(f, " fn::{}(", self.name.0)?;
				for (i, (name, kind)) in args.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					write!(f, "${name}: {kind}")?;
				}
				f.write_str(") ")?;
				if let Some(ref v) = returns {
					write!(f, "-> {v} ")?;
				}
				Display::fmt(&block, f)?;
			}
			Executable::SurrealismPackage(file) => {
				write!(f, " fn::{} AS {}", self.name.0, file)?;
			}
			Executable::SiloPackage {
				organisation,
				package,
				versions,
			} => {
				if !self.name.is_empty() {
					write!(f, " fn::{} AS", self.name.0)?;
				}

				write!(
					f,
					" silo::{}::{}::<{}>",
					organisation,
					package,
					Fmt::verbar_separated(versions)
				)?;
			}
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		write!(f, "PERMISSIONS {}", self.permissions)?;
		Ok(())
	}
}

impl InfoStructure for DefineFunctionStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"executable".to_string() => self.executable.structure(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Executable {
	Block {
		block: Block,
		args: Vec<(Ident, Kind)>,
		returns: Option<Kind>,
	},
	SurrealismPackage(File),
	SiloPackage {
		organisation: Ident,
		package: Ident,
		versions: Vec<String>,
	},
}

impl Default for Executable {
	fn default() -> Self {
		Self::Block {
			block: Default::default(),
			args: Vec::new(),
			returns: None,
		}
	}
}

impl InfoStructure for Executable {
	fn structure(self) -> Value {
		match self {
			Self::Block {
				block,
				args,
				returns,
			} => Value::from(map! {
				"executable".to_string() => Value::from("block"),
				"block".to_string() => block.structure(),
				"args".to_string() => args
					.into_iter()
					.map(|(n, k)| vec![n.structure(), k.structure()].into())
					.collect::<Vec<Value>>()
					.into(),
				"returns".to_string(), if let Some(v) = returns => v.structure(),
			}),
			Self::SurrealismPackage(f) => Value::from(map! {
				"executable".to_string() => Value::from("surrealism-package"),
				"file".to_string() => f.structure(),
			}),
			Self::SiloPackage {
				organisation,
				package,
				versions,
			} => Value::from(map! {
				"executable".to_string() => Value::from("silo-package"),
				"organisation".to_string() => Value::from(organisation.to_string()),
				"package".to_string() => Value::from(package.to_string()),
				"versions".to_string() => Value::from(versions),
			}),
		}
	}
}
