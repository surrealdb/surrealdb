use crate::sql::value::SqlValue;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[cfg(feature = "ml")]
use crate::iam::Action;
#[cfg(feature = "ml")]
use crate::sql::Permission;
#[cfg(feature = "ml")]
use futures::future::try_join_all;
#[cfg(feature = "ml")]
use std::collections::HashMap;
#[cfg(feature = "ml")]
use surrealml::errors::error::SurrealError;
#[cfg(feature = "ml")]
use surrealml::execution::compute::ModelComputation;
#[cfg(feature = "ml")]
use surrealml::ndarray as mlNdarray;
#[cfg(feature = "ml")]
use surrealml::storage::surml_file::SurMlFile;

#[cfg(feature = "ml")]
const ARGUMENTS: &str = "The model expects 1 argument. The argument can be either a number, an object, or an array of numbers.";

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Model";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Model")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Model {
	pub name: String,
	pub version: String,
	pub args: Vec<SqlValue>,
}

impl fmt::Display for Model {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ml::{}<{}>(", self.name, self.version)?;
		for (idx, p) in self.args.iter().enumerate() {
			if idx != 0 {
				write!(f, ",")?;
			}
			write!(f, "{}", p)?;
		}
		write!(f, ")")
	}
}

impl From<Model> for crate::expr::Model {
	fn from(v: Model) -> Self {
		Self {
			name: v.name,
			version: v.version,
			args: v.args.into_iter().map(Into::into).collect(),
		}
	}
}
impl From<crate::expr::Model> for Model {
	fn from(v: crate::expr::Model) -> Self {
		Self {
			name: v.name,
			version: v.version,
			args: v.args.into_iter().map(Into::into).collect(),
		}
	}
}
