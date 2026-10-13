//! `sql` -> `expr` conversions for [`crate::sql::module`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::expr;
use crate::sql::module::*;

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

impl From<expr::SurrealismExecutable> for SurrealismExecutable {
	fn from(executable: expr::SurrealismExecutable) -> Self {
		Self(executable.0.into())
	}
}

impl From<SurrealismExecutable> for expr::SurrealismExecutable {
	fn from(executable: SurrealismExecutable) -> Self {
		expr::SurrealismExecutable(executable.0.into())
	}
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
