//! Namespace data model.
//!
//! This module defines the namespace data model, which is used to
//! represent a namespace in the database. The namespace is a
//! collection of databases, and is used to group related databases
//! together.

use crate::sql::{
	statements::{info::InfoStructure, DefineNamespaceStatement},
	Value,
};
use newtype::NewType;
use revision::revisioned;
use serde::{Deserialize, Serialize};

/// Namespace identifier.
#[revisioned(revision = 1)]
#[derive(
	Copy, Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash, NewType,
)]
#[repr(transparent)]
pub struct NamespaceId(pub u32);

/// Namespace data model.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Namespace {
	/// Namespace identifier.
	pub id: Option<NamespaceId>,
	/// Namespace name.
	pub name: String,
	/// Optional comment.
	pub comment: Option<String>,
	/// The SQL definition of the namespace.
	pub definition: String,
}

impl From<&DefineNamespaceStatement> for Namespace {
	fn from(value: &DefineNamespaceStatement) -> Self {
		Self {
			id: value.id.map(NamespaceId::from),
			name: value.name.to_string(),
			comment: value.comment.as_ref().map(|s| s.to_string()),
			definition: value.to_string(),
		}
	}
}

impl InfoStructure for Namespace {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
