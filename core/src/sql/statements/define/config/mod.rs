mod graphql;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Base, Ident, Strand, Value};
use derive::Store;
use graphql::GraphQLConfig;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum DefineConfigStatement {
	GraphQL(GraphQLConfig),
}

impl DefineConfigStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Namespace, &Base::Root)?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		// if txn.get_ns(&self.name).await.is_ok() {
		// 	if self.if_not_exists {
		// 		return Ok(Value::None);
		// 	} else if !self.overwrite {
		// 		return Err(Error::NsAlreadyExists {
		// 			value: self.name.to_string(),
		// 		});
		// 	}
		// }
		// // Process the statement
		// let key = crate::key::root::ns::new(&self.name);
		// txn.set(
		// 	key,
		// 	DefineNamespaceStatement {
		// 		id: if self.id.is_none() {
		// 			Some(txn.lock().await.get_next_ns_id().await?)
		// 		} else {
		// 			None
		// 		},
		// 		// Don't persist the `IF NOT EXISTS` clause to schema
		// 		if_not_exists: false,
		// 		overwrite: false,
		// 		..self.clone()
		// 	},
		// )
		// .await?;
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
}

// impl InfoStructure for DefineNamespaceStatement {
// 	fn structure(self) -> Value {
// 		Value::from(map! {
// 			"name".to_string() => self.name.structure(),
// 			"comment".to_string(), if let Some(v) = self.comment => v.into(),
// 		})
// 	}
// }

impl Display for DefineConfigStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::GraphQL(v) => Display::fmt(v, f),
		}
	}
}
