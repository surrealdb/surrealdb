use revision::revisioned;
use serde::{Deserialize, Serialize};

use super::Level;

#[revisioned(revision = 5)]
#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
	#[revision(start = 3)]
	Api,
	#[revision(start = 4)]
	Bucket,
	#[revision(start = 5)]
	Sequence,
	// IAM
	Actor,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum ConfigKind {
	GraphQL,
	Api,
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
			ResourceKind::Api => write!(f, "Api"),
			ResourceKind::Analyzer => write!(f, "Analyzer"),
			ResourceKind::Parameter => write!(f, "Parameter"),
			ResourceKind::Model => write!(f, "Model"),
			ResourceKind::Event => write!(f, "Event"),
			ResourceKind::Field => write!(f, "Field"),
			ResourceKind::Index => write!(f, "Index"),
			ResourceKind::Access => write!(f, "Access"),
			ResourceKind::Actor => write!(f, "Actor"),
			ResourceKind::Config(c) => write!(f, "Config::{c}"),
			ResourceKind::Bucket => write!(f, "Bucket"),
			ResourceKind::Sequence => write!(f, "Sequence"),
		}
	}
}

impl std::fmt::Display for ConfigKind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ConfigKind::GraphQL => write!(f, "GraphQL"),
			ConfigKind::Api => write!(f, "API"),
		}
	}
}

impl ResourceKind {
	// Helpers for building default resources for specific levels. Useful for
	// authorization checks.
	pub fn on_level(self, level: Level) -> Resource {
		Resource::new("".into(), self, level)
	}

	pub fn on_root(self) -> Resource {
		self.on_level(Level::Root)
	}

	pub fn on_ns(self, ns: &str) -> Resource {
		self.on_level(Level::Namespace(ns.to_owned()))
	}

	pub fn on_db(self, ns: &str, db: &str) -> Resource {
		self.on_level(Level::Database(ns.to_owned(), db.to_owned()))
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Resource {
	id: String,
	kind: ResourceKind,
	level: Level,
}

impl std::fmt::Display for Resource {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}{}:\"{}\"", self.level, self.kind, self.id)
	}
}

impl Resource {
	pub(crate) fn new(id: String, kind: ResourceKind, level: Level) -> Self {
		Self {
			id,
			kind,
			level,
		}
	}

	pub(crate) fn id(&self) -> &str {
		&self.id
	}

	pub(crate) fn kind(&self) -> &ResourceKind {
		&self.kind
	}

	pub(crate) fn level(&self) -> &Level {
		&self.level
	}
}
