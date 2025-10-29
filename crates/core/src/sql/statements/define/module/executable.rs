use std::fmt::{self, Debug, Display};

use crate::val::File;
use crate::{catalog, expr};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ModuleExecutable {
	Surrealism(SurrealismExecutable),
	Silo(SiloExecutable),
}

impl From<expr::ModuleExecutable> for ModuleExecutable {
	fn from(executable: expr::ModuleExecutable) -> Self {
		match executable {
			expr::ModuleExecutable::Surrealism(surrealism) => {
				ModuleExecutable::Surrealism(surrealism.into())
			}
			expr::ModuleExecutable::Silo(silo) => ModuleExecutable::Silo(silo.into()),
		}
	}
}

impl From<catalog::ModuleExecutable> for ModuleExecutable {
	fn from(executable: catalog::ModuleExecutable) -> Self {
		match executable {
			catalog::ModuleExecutable::Surrealism(surrealism) => {
				ModuleExecutable::Surrealism(surrealism.into())
			}
			catalog::ModuleExecutable::Silo(silo) => ModuleExecutable::Silo(silo.into()),
		}
	}
}

impl From<ModuleExecutable> for expr::ModuleExecutable {
	fn from(executable: ModuleExecutable) -> Self {
		match executable {
			ModuleExecutable::Surrealism(surrealism) => {
				expr::ModuleExecutable::Surrealism(surrealism.into())
			}
			ModuleExecutable::Silo(silo) => expr::ModuleExecutable::Silo(silo.into()),
		}
	}
}

impl fmt::Display for ModuleExecutable {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			ModuleExecutable::Surrealism(surrealism) => Display::fmt(surrealism, f),
			ModuleExecutable::Silo(silo) => Display::fmt(silo, f),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SurrealismExecutable(pub File);

impl From<expr::SurrealismExecutable> for SurrealismExecutable {
	fn from(executable: expr::SurrealismExecutable) -> Self {
		Self(executable.0)
	}
}

impl From<catalog::SurrealismExecutable> for SurrealismExecutable {
	fn from(executable: catalog::SurrealismExecutable) -> Self {
		Self(File::new(executable.bucket, executable.key))
	}
}

impl From<SurrealismExecutable> for expr::SurrealismExecutable {
	fn from(executable: SurrealismExecutable) -> Self {
		expr::SurrealismExecutable(executable.0)
	}
}

impl fmt::Display for SurrealismExecutable {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

impl From<catalog::SiloExecutable> for SiloExecutable {
	fn from(executable: catalog::SiloExecutable) -> Self {
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
		write!(
			f,
			"silo::{}::{}<{}.{}.{}>",
			self.organisation, self.package, self.major, self.minor, self.patch
		)
	}
}
