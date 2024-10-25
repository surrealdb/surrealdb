use crate::ctx::Context;
use crate::dbs::group::GroupsCollector;
use crate::dbs::plan::Explanation;
#[cfg(storage)]
use crate::dbs::store::file_store::FileCollector;
use crate::dbs::store::parallel_ordered::OrderedParallelCollector;
use crate::dbs::store::MemoryCollector;
use crate::dbs::{Options, Statement};
use crate::err::Error;
use crate::sql::order::Ordering;
use crate::sql::Value;
use reblessive::tree::Stk;

pub(super) enum Results {
	None,
	Memory(MemoryCollector),
	OrderedParallel(OrderedParallelCollector),
	#[cfg(storage)]
	File(Box<FileCollector>),
	Groups(GroupsCollector),
}

impl Results {
	pub(super) fn prepare(
		&mut self,
		#[cfg(storage)] ctx: &Context,
		stm: &Statement<'_>,
	) -> Result<Self, Error> {
		if stm.expr().is_some() && stm.group().is_some() {
			return Ok(Self::Groups(GroupsCollector::new(stm)));
		}
		#[cfg(storage)]
		if stm.tempfiles() {
			if let Some(temp_dir) = ctx.temporary_directory() {
				return Ok(Self::File(Box::new(FileCollector::new(temp_dir)?)));
			}
		}
		#[cfg(not(target_arch = "wasm32"))]
		if stm.parallel() {
			if let Some(order) = stm.order() {
				return Ok(Self::OrderedParallel(OrderedParallelCollector::new(order)));
			}
		}
		Ok(Self::Memory(Default::default()))
	}

	pub(super) async fn push(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		val: Value,
	) -> Result<(), Error> {
		match self {
			Self::None => {}
			Self::Memory(s) => {
				s.push(val);
			}
			Self::OrderedParallel(c) => {
				c.push(val).await?;
			}
			#[cfg(storage)]
			Self::File(e) => {
				e.push(val).await?;
			}
			Self::Groups(g) => {
				g.push(stk, ctx, opt, stm, val).await?;
			}
		}
		Ok(())
	}

	#[cfg(not(target_arch = "wasm32"))]
	pub(super) async fn async_sort(&mut self, orders: &Ordering) -> Result<(), Error> {
		match self {
			Self::Memory(m) => m.sort(orders).await?,
			#[cfg(storage)]
			Self::File(f) => f.sort(orders),
			_ => {}
		}
		Ok(())
	}

	#[cfg(target_arch = "wasm32")]
	pub(super) fn sort(&mut self, orders: &Ordering) {
		match self {
			Self::Memory(m) => m.small_sort(orders),
			#[cfg(storage)]
			Self::File(f) => f.sort(orders),
			_ => {}
		}
	}

	pub(super) async fn start_limit(
		&mut self,
		start: Option<u32>,
		limit: Option<u32>,
	) -> Result<(), Error> {
		match self {
			Self::None => {}
			Self::Memory(m) => m.start_limit(start, limit),
			Self::OrderedParallel(c) => c.start_limit(start, limit).await?,
			#[cfg(storage)]
			Self::File(f) => f.start_limit(start, limit),
			Self::Groups(_) => {}
		}
		Ok(())
	}

	pub(super) fn len(&self) -> usize {
		match self {
			Self::None => 0,
			Self::Memory(s) => s.len(),
			Self::OrderedParallel(s) => s.len(),
			#[cfg(storage)]
			Self::File(e) => e.len(),
			Self::Groups(g) => g.len(),
		}
	}

	pub(super) async fn take(&mut self) -> Result<Vec<Value>, Error> {
		Ok(match self {
			Self::Memory(m) => m.take_vec(),
			Self::OrderedParallel(c) => c.take_vec().await?,
			#[cfg(storage)]
			Self::File(f) => f.take_vec().await?,
			_ => vec![],
		})
	}

	pub(super) fn explain(&self, exp: &mut Explanation) {
		match self {
			Self::None => exp.add_collector("None", vec![]),
			Self::Memory(s) => {
				s.explain(exp);
			}
			Self::OrderedParallel(c) => c.explain(exp),
			#[cfg(storage)]
			Self::File(e) => {
				e.explain(exp);
			}
			Self::Groups(g) => {
				g.explain(exp);
			}
		}
	}
}

impl Default for Results {
	fn default() -> Self {
		Self::None
	}
}

impl From<Vec<Value>> for Results {
	fn from(value: Vec<Value>) -> Self {
		Results::Memory(value.into())
	}
}
