use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::base::Base;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use crate::sql::value::Value;
use derive::Store;
use nom::bytes::complete::tag_no_case;
use nom::bytes::streaming::tag;
use nom::combinator::cut;
use nom::multi::separated_list1;
use nom::sequence::pair;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct RemoveTableStatement {
	pub names: Vec<Ident>,
}

impl RemoveTableStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		for name in &self.names {
			// Get the defined table
			let tb = run.get_tb(opt.ns(), opt.db(), name).await?;
			// Delete the definition
			let key = crate::key::database::tb::new(opt.ns(), opt.db(), name);
			run.del(key).await?;
			// Remove the resource data
			let key = crate::key::table::all::new(opt.ns(), opt.db(), name);
			run.delp(key, u32::MAX).await?;
			// Check if this is a foreign table
			if let Some(view) = &tb.view {
				// Process each foreign table
				for v in view.what.0.iter() {
					// Save the view config
					let key = crate::key::table::ft::new(opt.ns(), opt.db(), v, name);
					run.del(key).await?;
				}
			}
		}
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveTableStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(
			f,
			"REMOVE TABLE {}",
			self.names.iter().map(|v| v.clone().0).collect::<Vec<String>>().join(", ")
		)
	}
}

pub fn table(i: &str) -> IResult<&str, RemoveTableStatement> {
	let (i, _) = tag_no_case("TABLE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, names) = cut(separated_list1(pair(tag(","), shouldbespace), ident))(i)?;
	Ok((
		i,
		RemoveTableStatement {
			names,
		},
	))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_table() {
		let (rem, res) = table("TABLE foo").unwrap();
		assert_eq!(rem, "");
		assert_eq!(res.names, vec![Ident("foo".to_string())]);
	}

	#[test]
	fn test_table_multi() {
		let (rem, res) = table("TABLE foo, bar").unwrap();
		assert_eq!(rem, "");
		assert_eq!(res.names, vec![Ident("foo".to_string()), Ident("bar".to_string())]);
	}
}
