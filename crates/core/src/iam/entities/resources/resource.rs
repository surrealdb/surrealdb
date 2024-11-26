use revision::revisioned;
use std::{
	collections::{HashMap, HashSet},
	str::FromStr,
};

use super::Level;

use cedar_policy::{Entity, EntityId, EntityTypeName, EntityUid, RestrictedExpression};
use serde::{Deserialize, Serialize};

#[revisioned(revision = 2)]
#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum ResourceKind {
	#[default]
	Any,
	Namespace,
	Database,
	Record,
	Table,
	Document,
	Option,
	Function,
	Analyzer,
	Parameter,
	Model,
	Event,
	Field,
	Index,
	Access,
	#[revision(start = 2)]
	Config(ConfigKind),

	// IAM
	Actor,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum ConfigKind {
	GraphQL,
}

impl std::fmt::Display for ResourceKind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ResourceKind::Any => write!(f, "Any"),
			ResourceKind::Namespace => write!(f, "Namespace"),
			ResourceKind::Database => write!(f, "Database"),
			ResourceKind::Record => write!(f, "Record"),
			ResourceKind::Table => write!(f, "Table"),
			ResourceKind::Document => write!(f, "Document"),
			ResourceKind::Option => write!(f, "Option"),
			ResourceKind::Function => write!(f, "Function"),
			ResourceKind::Analyzer => write!(f, "Analyzer"),
			ResourceKind::Parameter => write!(f, "Parameter"),
			ResourceKind::Model => write!(f, "Model"),
			ResourceKind::Event => write!(f, "Event"),
			ResourceKind::Field => write!(f, "Field"),
			ResourceKind::Index => write!(f, "Index"),
			ResourceKind::Access => write!(f, "Access"),
			ResourceKind::Actor => write!(f, "Actor"),
			ResourceKind::Config(c) => write!(f, "Config::{c}"),
		}
	}
}

impl std::fmt::Display for ConfigKind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ConfigKind::GraphQL => write!(f, "GraphQL"),
		}
	}
}

impl ResourceKind {
	// Helpers for building default resources for specific levels. Useful for authorization checks.
	pub fn on_level(self, level: Level) -> Resource {
		Resource::new("".into(), self, level)
	}

	pub fn on_root(self) -> Resource {
		self.on_level(Level::Root)
	}

	pub fn on_ns(self, ns: &str) -> Resource {
		self.on_level((ns,).into())
	}

	pub fn on_db(self, ns: &str, db: &str) -> Resource {
		self.on_level((ns, db).into())
	}

	pub fn on_record(self, ns: &str, db: &str, rid: &str) -> Resource {
		self.on_level((ns, db, rid).into())
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Resource(String, ResourceKind, Level);

impl std::fmt::Display for Resource {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let Resource(id, kind, level) = self;
		write!(f, "{}{}:\"{}\"", level, kind, id)
	}
}

impl Resource {
	pub fn new(id: String, kind: ResourceKind, level: Level) -> Self {
		Self(id, kind, level)
	}

	pub fn id(&self) -> &str {
		&self.0
	}

	pub fn kind(&self) -> &ResourceKind {
		&self.1
	}

	pub fn level(&self) -> &Level {
		&self.2
	}

	// Cedar policy helpers
	pub fn cedar_attrs(&self) -> HashMap<String, RestrictedExpression> {
		[("type", self.kind().into()), ("level", self.level().into())]
			.into_iter()
			.map(|(x, v)| (x.into(), v))
			.collect()
	}

	pub fn cedar_parents(&self) -> HashSet<EntityUid> {
		HashSet::from([self.level().into()])
	}

	pub fn cedar_entities(&self) -> Vec<Entity> {
		let mut entities = Vec::new();

		entities.push(self.into());
		entities.extend(self.level().cedar_entities());

		entities
	}
}

impl std::convert::From<&Resource> for EntityUid {
	fn from(res: &Resource) -> Self {
		EntityUid::from_type_name_and_id(
			EntityTypeName::from_str(&res.kind().to_string()).unwrap(),
			EntityId::from_str(res.id()).unwrap(),
		)
	}
}

impl std::convert::From<&Resource> for Entity {
	fn from(res: &Resource) -> Self {
		Entity::new(res.into(), res.cedar_attrs(), res.cedar_parents())
	}
}

impl std::convert::From<&Resource> for RestrictedExpression {
	fn from(res: &Resource) -> Self {
		format!("{}", EntityUid::from(res)).parse().unwrap()
	}
}

impl std::convert::From<&ResourceKind> for RestrictedExpression {
	fn from(kind: &ResourceKind) -> Self {
		RestrictedExpression::new_string(kind.to_string())
	}
}
