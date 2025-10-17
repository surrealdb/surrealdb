use crate::expr::{Block, Kind};
use crate::expr;
use crate::fmt::EscapeKwFreeIdent;
use crate::val::File;
use std::fmt::{self, Display};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum Executable {
	Block(BlockExecutable),
	Surrealism(SurrealismExecutable),
	Silo(SiloExecutable),
}

impl From<expr::Executable> for Executable {
	fn from(executable: expr::Executable) -> Self {
		match executable {
			expr::Executable::Block(block) => Executable::Block(block.into()),
			expr::Executable::Surrealism(surrealism) => Executable::Surrealism(surrealism.into()),
			expr::Executable::Silo(silo) => Executable::Silo(silo.into()),
		}
	}
}

impl From<Executable> for expr::Executable {
	fn from(executable: Executable) -> Self {
		match executable {
			Executable::Block(block) => expr::Executable::Block(block.into()),
			Executable::Surrealism(surrealism) => expr::Executable::Surrealism(surrealism.into()),
			Executable::Silo(silo) => expr::Executable::Silo(silo.into()),
		}
	}
}

impl fmt::Display for Executable {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Executable::Block(block) => block.fmt(f),
			Executable::Surrealism(surrealism) => surrealism.fmt(f),
			Executable::Silo(silo) => silo.fmt(f),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct BlockExecutable {
	pub args: Vec<(String, Kind)>,
	pub returns: Option<Kind>,
	pub block: Block,
}

impl From<expr::BlockExecutable> for BlockExecutable {
	fn from(executable: expr::BlockExecutable) -> Self {
		Self {
			args: executable.args,
			returns: executable.returns,
			block: executable.block,
		}
	}
}

impl From<BlockExecutable> for expr::BlockExecutable {
	fn from(executable: BlockExecutable) -> Self {
		Self {
			args: executable.args,
			returns: executable.returns,
			block: executable.block,
		}
	}
}

impl fmt::Display for BlockExecutable {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "(")?;
		for (i, (name, kind)) in self.args.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			write!(f, "${}: {kind}", EscapeKwFreeIdent(name))?;
		}
		f.write_str(") ")?;
		if let Some(ref v) = self.returns {
			write!(f, "-> {v} ")?;
		}
		Display::fmt(&self.block, f)?;
        Ok(())
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct SurrealismExecutable(File);

impl From<expr::SurrealismExecutable> for SurrealismExecutable {
	fn from(executable: expr::SurrealismExecutable) -> Self {
		Self(executable.0)
	}
}

impl From<SurrealismExecutable> for expr::SurrealismExecutable {
	fn from(executable: SurrealismExecutable) -> Self {
		expr::SurrealismExecutable(executable.0)
	}
}

impl fmt::Display for SurrealismExecutable {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, " AS {}", self.0)
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct SiloExecutable {
	pub organisation: String,
	pub package: String,
	pub major: u32,
	pub minor: u32,
	pub patch: u32,
}

impl From<expr::SiloExecutable> for SiloExecutable {
	fn from(executable: expr::SiloExecutable) -> Self {
		Self {
			organisation: executable.organisation,
			package: executable.package,
			major: executable.major,
			minor: executable.minor,
			patch: executable.patch,
		}
	}
}

impl From<SiloExecutable> for expr::SiloExecutable {
	fn from(executable: SiloExecutable) -> Self {
		Self {
			organisation: executable.organisation,
			package: executable.package,
			major: executable.major,
			minor: executable.minor,
			patch: executable.patch,
		}
	}
}

impl fmt::Display for SiloExecutable {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, " AS silo::{}::{}<{}.{}.{}>", self.organisation, self.package, self.major, self.minor, self.patch)
	}
}