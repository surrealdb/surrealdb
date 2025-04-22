//! Namespace data model.
//!
//! This module defines the namespace data model, which is used to
//! represent a namespace in the database. The namespace is a
//! collection of databases, and is used to group related databases
//! together.

use crate::kvs::Transaction;
use crate::sql::{
	statements::{info::InfoStructure, DefineNamespaceStatement},
	Value,
};
use newtype::NewType;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Namespace identifier.
#[revisioned(revision = 1)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(
	Copy, Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash, NewType,
)]
#[repr(transparent)]
pub struct NamespaceId(pub u32);

/// Namespace data model.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Namespace {
	/// Namespace identifier.
	pub id: NamespaceId,
	/// Namespace name.
	pub name: String,
	/// Optional comment.
	pub comment: Option<String>,
	/// The SQL definition of the namespace.
	pub definition: String,
}

impl Namespace {
	/// Create a new namespace.
	pub fn new(id: NamespaceId, name: String) -> Self {
		let definition = format!("DEFINE NAMESPACE {name}");
		Self {
			id,
			name,
			comment: None,
			definition,
		}
	}

	/// Convert a [`DefineNamespaceStatement`] to a [`Namespace`].
	pub async fn try_from_statement(
		tx: &Arc<Transaction>,
		statement: &DefineNamespaceStatement,
	) -> Result<Self, crate::err::Error> {
		let id = match statement.id {
			Some(id) => NamespaceId::from(id),
			None => {
				// Generate a new ID if not provided
				tx.lock().await.get_next_ns_id().await?
			}
		};

		Ok(Self {
			id: NamespaceId::from(id),
			name: statement.name.to_string(),
			comment: statement.comment.as_ref().map(|s| s.to_string()),
			definition: statement.to_string(),
		})
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
