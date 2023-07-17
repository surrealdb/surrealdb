use js::{prelude::Opt, Result};

use crate::sql::Value as SurValue;

#[allow(clippy::module_inception)]
mod classes;
//mod txn;

pub use classes::Query;

#[js::function]
pub fn query(query: String, variables: Opt<class::QueryVariables>) -> Result<SurValue> {
	todo!()
}
