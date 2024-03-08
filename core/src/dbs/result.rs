use crate::ctx::Context;
use crate::dbs::group::GroupsCollector;
use crate::dbs::store::StoreCollector;
use crate::dbs::{Options, Statement, Transaction};
use crate::err::Error;
use crate::sql::Value;
use std::cmp::Ordering;
use std::slice::IterMut;

pub(super) enum Results {
	None,
	Store(StoreCollector),
	Groups(GroupsCollector),
}

impl Default for Results {
	fn default() -> Self {
		Self::None
	}
}

impl Results {
	pub(super) fn prepare(&mut self, stm: &Statement<'_>) -> Self {
		if stm.expr().is_some() && stm.group().is_some() {
			Self::Groups(GroupsCollector::new(stm))
		} else {
			Self::Store(StoreCollector::default())
		}
	}
	pub(super) async fn push(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		val: Value,
	) -> Result<(), Error> {
		match self {
			Results::None => {}
			Results::Store(s) => {
				s.push(val);
			}
			Results::Groups(g) => {
				g.push(ctx, opt, txn, stm, val).await?;
			}
		}
		Ok(())
	}

	pub(super) fn sort_by<F>(&mut self, compare: F)
	where
		F: FnMut(&Value, &Value) -> Ordering,
	{
		if let Results::Store(s) = self {
			s.sort_by(compare)
		}
	}

	pub(super) fn start_limit(&mut self, start: Option<&usize>, limit: Option<&usize>) {
		if let Results::Store(s) = self {
			if let Some(&start) = start {
				s.start(start);
			}
			if let Some(&limit) = limit {
				s.limit(limit);
			}
		}
	}

	pub(super) fn len(&self) -> usize {
		match self {
			Results::None => 0,
			Results::Store(s) => s.len(),
			Results::Groups(g) => g.len(),
		}
	}

	pub(super) async fn group(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<Self, Error> {
		Ok(match self {
			Self::None => Self::None,
			Self::Store(s) => Self::Store(s.take_store()),
			Self::Groups(g) => Self::Store(g.output(ctx, opt, txn, stm).await?),
		})
	}

	pub(super) fn take(&mut self) -> Vec<Value> {
		if let Self::Store(s) = self {
			s.take_vec()
		} else {
			vec![]
		}
	}
}

impl<'a> IntoIterator for &'a mut Results {
	type Item = &'a mut Value;
	type IntoIter = IterMut<'a, Value>;

	fn into_iter(self) -> Self::IntoIter {
		if let Results::Store(s) = self {
			s.into_iter()
		} else {
			[].iter_mut()
		}
	}
}
