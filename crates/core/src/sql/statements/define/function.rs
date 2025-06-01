use crate::sql::fmt::{Fmt, is_pretty, pretty_indent};

use crate::sql::{Block, File, Ident, Kind, Permission, Strand};

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
				if let Some(v) = returns {
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

impl From<DefineFunctionStatement> for crate::expr::statements::DefineFunctionStatement {
	fn from(v: DefineFunctionStatement) -> Self {
		Self {
			name: v.name.into(),
			comment: v.comment.map(Into::into),
			permissions: v.permissions.into(),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
			executable: v.executable.into(),
		}
	}
}

impl From<crate::expr::statements::DefineFunctionStatement> for DefineFunctionStatement {
	fn from(v: crate::expr::statements::DefineFunctionStatement) -> Self {
		Self {
			name: v.name.into(),
			comment: v.comment.map(Into::into),
			permissions: v.permissions.into(),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
			executable: v.executable.into(),
		}
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

impl From<Executable> for crate::expr::statements::define::Executable {
	fn from(v: Executable) -> Self {
		match v {
			Executable::Block {
				block,
				args,
				returns,
			} => Self::Block {
				block: block.into(),
				args: args.into_iter().map(|(i, k)| (i.into(), k.into())).collect(),
				returns: returns.map(Into::into),
			},
			Executable::SurrealismPackage(file) => Self::SurrealismPackage(file.into()),
			Executable::SiloPackage {
				organisation,
				package,
				versions,
			} => Self::SiloPackage {
				organisation: organisation.into(),
				package: package.into(),
				versions,
			},
		}
	}
}

impl From<crate::expr::statements::define::Executable> for Executable {
	fn from(v: crate::expr::statements::define::Executable) -> Self {
		match v {
			crate::expr::statements::define::Executable::Block {
				block,
				args,
				returns,
			} => Self::Block {
				block: block.into(),
				args: args.into_iter().map(|(i, k)| (i.into(), k.into())).collect(),
				returns: returns.map(Into::into),
			},
			crate::expr::statements::define::Executable::SurrealismPackage(file) => {
				Self::SurrealismPackage(file.into())
			}
			crate::expr::statements::define::Executable::SiloPackage {
				organisation,
				package,
				versions,
			} => Self::SiloPackage {
				organisation: organisation.into(),
				package: package.into(),
				versions,
			},
		}
	}
}
