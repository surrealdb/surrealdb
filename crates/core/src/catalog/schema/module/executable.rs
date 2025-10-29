use revision::revisioned;

use crate::expr::statements::info::InfoStructure;
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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
			"type".to_string() => Value::from("silo"),
			"organisation".to_string() => self.organisation.into(),
			"package".to_string() => self.package.into(),
			"major".to_string() => self.major.into(),
			"minor".to_string() => self.minor.into(),
			"patch".to_string() => self.patch.into(),
		})
	}
}
