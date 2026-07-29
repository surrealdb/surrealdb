use std::fmt::{self, Display};

use revision::revisioned;

use crate::catalog::StoredModuleDefinition;
use crate::expr::statements::info::InfoStructure;
use crate::sql;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum ModuleExecutable {
	Surrealism(SurrealismExecutable),
	Silo(SiloExecutable),
}

impl InfoStructure for ModuleExecutable {
	fn structure(self) -> Value {
		match self {
			ModuleExecutable::Surrealism(surrealism) => surrealism.structure(),
			ModuleExecutable::Silo(silo) => silo.structure(),
		}
	}
}

impl From<ModuleExecutable> for sql::module::ModuleExecutable {
	fn from(executable: ModuleExecutable) -> Self {
		match executable {
			ModuleExecutable::Surrealism(surrealism) => {
				sql::module::ModuleExecutable::Surrealism(surrealism.into())
			}
			ModuleExecutable::Silo(silo) => sql::module::ModuleExecutable::Silo(silo.into()),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct SurrealismExecutable {
	pub bucket: String,
	pub key: String,
}

impl InfoStructure for SurrealismExecutable {
	fn structure(self) -> Value {
		Value::from(map! {
			"type" => Value::from("surrealism"),
			"bucket" => self.bucket.into(),
			"key" => self.key.into(),
		})
	}
}

impl From<SurrealismExecutable> for sql::module::SurrealismExecutable {
	fn from(executable: SurrealismExecutable) -> Self {
		Self(sql::file::File {
			bucket: executable.bucket,
			key: executable.key,
		})
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct SiloExecutable {
	pub organisation: String,
	pub package: String,
	pub major: u32,
	pub minor: u32,
	pub patch: u32,
}

impl InfoStructure for SiloExecutable {
	fn structure(self) -> Value {
		Value::from(map! {
			"type" => Value::from("silo"),
			"organisation" => self.organisation.into(),
			"package" => self.package.into(),
			"major" => self.major.into(),
			"minor" => self.minor.into(),
			"patch" => self.patch.into(),
		})
	}
}

impl From<SiloExecutable> for sql::module::SiloExecutable {
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

// This enum is not actually stored, but is used to generate the storage name of a module
// Therefor I found it to fit better inside catalog, and to then let expr use this enum aswell,
// to have a single point where the storage name is generated.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum ModuleName {
	Module(String),
	Silo(String, String, u32, u32, u32),
}

impl ModuleName {
	pub(crate) fn get_storage_name(&self) -> String {
		match self {
			ModuleName::Module(name) => format!("mod::{}", name),
			ModuleName::Silo(org, pkg, major, minor, patch) => {
				format!("silo::{org}::{pkg}<{major}.{minor}.{patch}>")
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

impl TryFrom<&StoredModuleDefinition> for ModuleName {
	type Error = anyhow::Error;
	fn try_from(value: &StoredModuleDefinition) -> Result<Self, Self::Error> {
		if let Some(name) = &value.name {
			Ok(ModuleName::Module(name.clone()))
		} else if let ModuleExecutable::Silo(silo) = &value.executable {
			Ok(ModuleName::Silo(
				silo.organisation.clone(),
				silo.package.clone(),
				silo.major,
				silo.minor,
				silo.patch,
			))
		} else {
			fail!("A module without a name cannot be stored")
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
				write!(f, "silo::{org}::{pkg}<{major}.{minor}.{patch}>")
			}
		}
	}
}
