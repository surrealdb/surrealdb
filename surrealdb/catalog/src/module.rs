use revision::revisioned;

use crate::catalog::StoredModuleDefinition;
pub use crate::expr::module::ModuleName;
use crate::expr::statements::info::InfoStructure;
use crate::stored::Stored;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub enum ModuleExecutable {
	#[stored(n = 1)]
	Surrealism(SurrealismExecutable),
	#[stored(n = 2)]
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
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct SurrealismExecutable {
	#[stored(n = 2)]
	pub bucket: String,
	#[stored(n = 4)]
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct SiloExecutable {
	#[stored(n = 2)]
	pub organisation: String,
	#[stored(n = 4)]
	pub package: String,
	#[stored(n = 6)]
	pub major: u32,
	#[stored(n = 8)]
	pub minor: u32,
	#[stored(n = 10)]
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

use common::fail;

use crate::expr::module::{
	ModuleExecutable as ExprModuleExecutable, SiloExecutable as ExprSiloExecutable,
	SurrealismExecutable as ExprSurrealismExecutable,
};

impl From<ModuleExecutable> for ExprModuleExecutable {
	fn from(executable: ModuleExecutable) -> Self {
		match executable {
			ModuleExecutable::Surrealism(surrealism) => {
				ExprModuleExecutable::Surrealism(surrealism.into())
			}
			ModuleExecutable::Silo(silo) => ExprModuleExecutable::Silo(silo.into()),
		}
	}
}

impl From<ExprModuleExecutable> for ModuleExecutable {
	fn from(executable: ExprModuleExecutable) -> Self {
		match executable {
			ExprModuleExecutable::Surrealism(surrealism) => {
				ModuleExecutable::Surrealism(surrealism.into())
			}
			ExprModuleExecutable::Silo(silo) => ModuleExecutable::Silo(silo.into()),
		}
	}
}

impl From<SurrealismExecutable> for ExprSurrealismExecutable {
	fn from(executable: SurrealismExecutable) -> Self {
		Self(crate::val::File::new(executable.bucket, executable.key))
	}
}

impl From<ExprSurrealismExecutable> for SurrealismExecutable {
	fn from(executable: ExprSurrealismExecutable) -> Self {
		Self {
			bucket: executable.0.bucket,
			key: executable.0.key,
		}
	}
}

impl From<SiloExecutable> for ExprSiloExecutable {
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

impl From<ExprSiloExecutable> for SiloExecutable {
	fn from(executable: ExprSiloExecutable) -> Self {
		Self {
			organisation: executable.organisation,
			package: executable.package,
			major: executable.major,
			minor: executable.minor,
			patch: executable.patch,
		}
	}
}
