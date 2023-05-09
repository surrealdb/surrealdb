mod tree;

use crate::dbs::{Iterable, Options, Transaction};
use crate::err::Error;
use crate::idx::planner::tree::TreeBuilder;
use crate::sql::{Cond, Table};

pub(crate) struct QueryPlanner<'a> {
	opt: &'a Options,
	cond: &'a Option<Cond>,
}

impl<'a> QueryPlanner<'a> {
	pub(crate) fn new(opt: &'a Options, cond: &'a Option<Cond>) -> Self {
		Self {
			cond,
			opt,
		}
	}

	pub(crate) async fn get_iterable(
		&self,
		txn: &Transaction,
		t: Table,
	) -> Result<Iterable, Error> {
		match TreeBuilder::parse(self.opt, txn, &t, self.cond).await {
			Ok(Some(_)) => {
				return Ok(Iterable::Table(t));
			}
			Ok(None) => Ok(Iterable::Table(t)),
			Err(Error::BypassQueryPlanner) => Ok(Iterable::Table(t)),
			Err(e) => Err(e),
		}
	}
}
