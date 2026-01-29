use std::fmt::{self, Display};

use priority_lfu::DeepSizeOf;
use revision::revisioned;

use crate::catalog::ModuleDefinition;
use crate::expr::statements::info::InfoStructure;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
pub(crate) struct SurrealismExecutable {
	pub bucket: String,
	pub key: String,
}

impl InfoStructure for SurrealismExecutable {
	fn structure(self) -> Value {
		Value::from(map! {
			"type".to_string() => Value::from("surrealism"),
			"bucket".to_string() => self.bucket.into(),
			"key".to_string() => self.key.into(),
		})
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
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
			"type".to_string() => Value::from("silo"),
			"organisation".to_string() => self.organisation.into(),
			"package".to_string() => self.package.into(),
			"major".to_string() => self.major.into(),
			"minor".to_string() => self.minor.into(),
			"patch".to_string() => self.patch.into(),
		})
	}
}

// This enum is not actually stored, but is used to generate the storage name of a module
// Therefor I found it to fit better inside catalog, and to then let expr use this enum aswell,
// to have a single point where the storage name is generated.
#[derive(Clone, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
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

impl TryFrom<&ModuleDefinition> for ModuleName {
	type Error = anyhow::Error;
	fn try_from(value: &ModuleDefinition) -> Result<Self, Self::Error> {
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
