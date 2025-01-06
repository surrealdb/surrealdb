use crate::ctx::Context;
#[cfg(storage)]
use crate::dbs::file::FileCollector;
use crate::dbs::group::GroupsCollector;
use crate::dbs::plan::Explanation;
use crate::dbs::store::{MemoryCollector, MemoryOrdered, MemoryRandom};
use crate::dbs::{Options, Statement};
use crate::err::Error;
use crate::sql::order::Ordering;
use crate::sql::Value;
use reblessive::tree::Stk;

pub(super) enum Results {
	None,
	Memory(MemoryCollector),
	MemoryRandom(MemoryRandom),
	MemoryOrdered(MemoryOrdered),
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
		if let Some(ordering) = stm.order() {
			return match ordering {
				Ordering::Random => Ok(Self::MemoryRandom(MemoryRandom::new(None))),
				Ordering::Order(orders) => {
					Ok(Self::MemoryOrdered(MemoryOrdered::new(orders.clone(), None)))
				}
			};
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
			Self::MemoryOrdered(c) => {
				c.push(val);
			}
			Self::MemoryRandom(c) => {
				c.push(val);
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
	pub(super) async fn sort(&mut self, orders: &Ordering) -> Result<(), Error> {
		match self {
			#[cfg(storage)]
			Self::File(f) => f.sort(orders),
			Self::MemoryOrdered(c) => c.sort().await?,
			Self::MemoryRandom(c) => c.sort(),
			Self::None | Self::Memory(_) | Self::Groups(_) => {}
		}
		Ok(())
	}

	#[cfg(target_arch = "wasm32")]
	pub(super) fn sort(&mut self, orders: &Ordering) {
		match self {
			Self::MemoryOrdered(c) => c.sort(),
			Self::MemoryRandom(c) => c.sort(),
			#[cfg(storage)]
			Self::File(f) => f.sort(orders),
			Self::None | Self::Groups(_) | Self::Memory(_) => {}
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
			Self::MemoryOrdered(m) => m.start_limit(start, limit),
			Self::MemoryRandom(c) => c.start_limit(start, limit),
			#[cfg(storage)]
			Self::File(f) => f.start_limit(start, limit),
			Self::Groups(_) => {}
		}
		Ok(())
	}

	pub(super) fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub(super) fn len(&self) -> usize {
		match self {
			Self::None => 0,
			Self::Memory(s) => s.len(),
			Self::MemoryOrdered(s) => s.len(),
			Self::MemoryRandom(s) => s.len(),
			#[cfg(storage)]
			Self::File(e) => e.len(),
			Self::Groups(g) => g.len(),
		}
	}

	pub(super) async fn take(&mut self) -> Result<Vec<Value>, Error> {
		Ok(match self {
			Self::Memory(m) => m.take_vec(),
			Self::MemoryOrdered(c) => c.take_vec(),
			Self::MemoryRandom(c) => c.take_vec(),
			#[cfg(storage)]
			Self::File(f) => f.take_vec().await?,
			Self::None | Self::Groups(_) => vec![],
		})
	}

	pub(super) fn explain(&self, exp: &mut Explanation) {
		match self {
			Self::None => exp.add_collector("None", vec![]),
			Self::Memory(s) => {
				s.explain(exp);
			}
			Self::MemoryOrdered(c) => c.explain(exp),
			Self::MemoryRandom(c) => c.explain(exp),
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
