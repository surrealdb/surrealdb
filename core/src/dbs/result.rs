use crate::ctx::Context;
use crate::dbs::group::GroupsCollector;
use crate::dbs::plan::Explanation;
use crate::dbs::store::{FileCollector, MemoryCollector};
use crate::dbs::{Options, Statement, Transaction};
use crate::err::Error;
use crate::sql::{Orders, Value};
use std::slice::IterMut;

pub(super) enum Results {
	None,
	Memory(MemoryCollector),
	File(Box<FileCollector>),
	Groups(GroupsCollector),
}

impl Default for Results {
	fn default() -> Self {
		Self::None
	}
}

impl Results {
	pub(super) fn prepare(
		&mut self,
		ctx: &Context<'_>,
		stm: &Statement<'_>,
	) -> Result<Self, Error> {
		Ok(if stm.expr().is_some() && stm.group().is_some() {
			Self::Groups(GroupsCollector::new(stm))
		} else if ctx.is_memory() {
			Self::Memory(Default::default())
		} else {
			Self::File(Box::new(FileCollector::new()?))
		})
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
			Results::Memory(s) => {
				s.push(val);
			}
			Results::File(e) => {
				e.push(val)?;
			}
			Results::Groups(g) => {
				g.push(ctx, opt, txn, stm, val).await?;
			}
		}
		Ok(())
	}

	pub(super) fn sort(&mut self, orders: &Orders) -> Result<(), Error> {
		match self {
			Results::Memory(m) => m.sort(orders),
			Results::File(f) => f.sort(orders),
			_ => Ok(()),
		}
	}

	pub(super) fn start_limit(&mut self, start: Option<&usize>, limit: Option<&usize>) {
		match self {
			Results::None => {}
			Results::Memory(m) => m.start_limit(start, limit),
			Results::File(f) => f.start_limit(start, limit),
			Results::Groups(_) => {}
		}
	}

	pub(super) fn len(&self) -> usize {
		match self {
			Results::None => 0,
			Results::Memory(s) => s.len(),
			Results::File(e) => e.len(),
			Results::Groups(g) => g.len(),
		}
	}

	pub(super) fn try_into_iter(&mut self) -> Result<IterMut<'_, Value>, Error> {
		match self {
			Results::Memory(s) => s.try_iter_mut(),
			Results::File(f) => f.try_iter_mut(),
			_ => Ok([].iter_mut()),
		}
	}

	pub(super) fn take(&mut self) -> Result<Vec<Value>, Error> {
		Ok(match self {
			Results::Memory(m) => m.take_vec(),
			Results::File(f) => f.take_vec()?,
			_ => vec![],
		})
	}

	pub(super) fn explain(&self, exp: &mut Explanation) {
		match self {
			Results::None => exp.add_collector("None", vec![]),
			Results::Memory(s) => {
				s.explain(exp);
			}
			Results::File(e) => {
				e.explain(exp);
			}
			Results::Groups(g) => {
				g.explain(exp);
			}
		}
	}
}
