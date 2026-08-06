use std::fmt::{self, Display};

use anyhow::Result;
use common::fail;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::Kind;
use crate::sql;
use crate::val::File;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ModuleExecutable {
	Surrealism(SurrealismExecutable),
	Silo(SiloExecutable),
}

impl ToSql for ModuleExecutable {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let module_executable: crate::sql::ModuleExecutable = self.clone().into();
		module_executable.fmt_sql(f, sql_fmt);
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Signature {
	pub args: Vec<Kind>,
	pub returns: Option<Kind>,
	pub writeable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SurrealismExecutable(pub File);

impl ToSql for SurrealismExecutable {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let surrealism_executable: crate::sql::SurrealismExecutable = self.clone().into();
		surrealism_executable.fmt_sql(f, sql_fmt);
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SiloExecutable {
	pub organisation: String,
	pub package: String,
	pub major: u32,
	pub minor: u32,
	pub patch: u32,
}

impl ToSql for SiloExecutable {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let silo_executable: crate::sql::SiloExecutable = self.clone().into();
		silo_executable.fmt_sql(f, sql_fmt);
	}
}

/// The parsed name of a module: a plain module or a silo package release.
///
/// Not persisted itself; [`ModuleName::get_storage_name`] is the single point
/// that derives the storage name both the statements and the catalog use.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ModuleName {
	Module(String),
	Silo(String, String, u32, u32, u32),
}

impl ModuleName {
	pub fn get_storage_name(&self) -> String {
		match self {
			ModuleName::Module(name) => format!("mod::{}", name),
			ModuleName::Silo(org, pkg, major, minor, patch) => {
				format!("silo::{org}::{pkg}::<{major}.{minor}.{patch}>")
			}
		}
	}
}

impl From<sql::module::ModuleName> for ModuleName {
	fn from(v: sql::module::ModuleName) -> Self {
		match v {
			sql::module::ModuleName::Module(name) => ModuleName::Module(name),
			sql::module::ModuleName::Silo(org, pkg, major, minor, patch) => {
				ModuleName::Silo(org, pkg, major, minor, patch)
			}
		}
	}
}

impl From<ModuleName> for sql::module::ModuleName {
	fn from(v: ModuleName) -> Self {
		match v {
			ModuleName::Module(name) => sql::module::ModuleName::Module(name),
			ModuleName::Silo(org, pkg, major, minor, patch) => {
				sql::module::ModuleName::Silo(org, pkg, major, minor, patch)
			}
		}
	}
}

impl<'a> TryFrom<&'a crate::expr::statements::DefineModuleStatement> for ModuleName {
	type Error = anyhow::Error;
	fn try_from(
		value: &'a crate::expr::statements::DefineModuleStatement,
	) -> Result<Self, Self::Error> {
		if let Some(name) = &value.name {
			Ok(ModuleName::Module(name.clone()))
		} else if let crate::expr::ModuleExecutable::Silo(silo) = &value.executable {
			Ok(ModuleName::Silo(
				silo.organisation.clone(),
				silo.package.clone(),
				silo.major,
				silo.minor,
				silo.patch,
			))
		} else {
			// It should not be possible to get to this point as in the parser
			// we validate that the module has a name or is a silo module
			fail!("A module without a name cannot be stored")
		}
	}
}

impl Display for ModuleName {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			ModuleName::Module(name) => write!(f, "mod::{}", name),
			ModuleName::Silo(org, pkg, major, minor, patch) => {
				write!(f, "silo::{org}::{pkg}::<{major}.{minor}.{patch}>")
			}
		}
	}
}
