pub mod graphql;

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
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		match self {
			DefineConfigStatement::GraphQL(g) => g.compute(ctx, opt, doc),
		}
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
