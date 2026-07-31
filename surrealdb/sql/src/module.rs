use common::fmt::EscapeKwFreeIdent;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::file::File;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum ModuleName {
	Module(String),
	Silo(String, String, u32, u32, u32),
}

impl ToSql for ModuleName {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			ModuleName::Module(name) => write_sql!(f, fmt, "mod::{}", EscapeKwFreeIdent(name)),
			ModuleName::Silo(org, pkg, major, minor, patch) => {
				write_sql!(
					f,
					fmt,
					"silo::{}::{}::<{major}.{minor}.{patch}>",
					EscapeKwFreeIdent(org),
					EscapeKwFreeIdent(pkg)
				);
			}
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum ModuleExecutable {
	Surrealism(SurrealismExecutable),
	Silo(SiloExecutable),
}

impl ToSql for ModuleExecutable {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			ModuleExecutable::Surrealism(surrealism) => surrealism.fmt_sql(f, fmt),
			ModuleExecutable::Silo(silo) => silo.fmt_sql(f, fmt),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct SurrealismExecutable(pub File);

impl ToSql for SurrealismExecutable {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.0.fmt_sql(f, fmt);
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct SiloExecutable {
	pub organisation: String,
	pub package: String,
	pub major: u32,
	pub minor: u32,
	pub patch: u32,
}

impl ToSql for SiloExecutable {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(
			f,
			fmt,
			"silo::{}::{}::<{}.{}.{}>",
			self.organisation,
			self.package,
			self.major,
			self.minor,
			self.patch
		)
	}
}
