use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
#[cfg(storage)]
use crate::dbs::file::FileCollector;
use crate::dbs::group::GroupCollector;
use crate::dbs::plan::Explanation;
use crate::dbs::store::{MemoryCollector, MemoryOrdered, MemoryOrderedLimit, MemoryRandom};
use crate::dbs::{Options, Statement};
use crate::expr::order::Ordering;
use crate::idx::planner::RecordStrategy;
use crate::val::Value;

#[derive(Default)]
pub(super) enum Results {
	#[default]
	None,
	Memory(MemoryCollector),
	MemoryRandom(MemoryRandom),
	MemoryOrdered(MemoryOrdered),
	MemoryOrderedLimit(MemoryOrderedLimit),
	#[cfg(storage)]
	File(Box<FileCollector>),
	Groups(GroupCollector),
}

impl Results {
	pub(super) fn prepare(
		&mut self,
		ctx: &FrozenContext,
		stm: &Statement<'_>,
		start: Option<u32>,
		limit: Option<u32>,
	) -> Result<Self> {
		if stm.expr().is_some() && stm.group().is_some() {
			return Ok(Self::Groups(GroupCollector::new(stm)?));
		}
		#[cfg(storage)]
		if stm.tempfiles()
			&& let Some(temp_dir) = ctx.temporary_directory()
		{
			return Ok(Self::File(Box::new(FileCollector::new(
				temp_dir,
				stm.order().cloned(),
				ctx.config().limits.external_sorting_buffer_limit,
			)?)));
		}
		if let Some(ordering) = stm.order() {
			return match ordering {
				Ordering::Random => Ok(Self::MemoryRandom(MemoryRandom::new(None))),
				Ordering::Order(orders) => {
					if let Some(limit) = limit {
						let limit = start.unwrap_or(0) + limit;
						// Use the priority-queue optimization only when both conditions hold:
						// - the effective limit (start + limit) does not exceed
						//   MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE
						// - there is no SPLIT clause (SPLIT can change the number of produced
						//   records)
						// Otherwise, fall back to full in-memory ordering.
						if stm.split().is_none()
							&& limit <= ctx.config().limits.max_order_limit_priority_queue_size
						{
							return Ok(Self::MemoryOrderedLimit(MemoryOrderedLimit::new(
								limit as usize,
								orders.clone(),
							)));
						}
					}
					Ok(Self::MemoryOrdered(MemoryOrdered::new(orders.clone(), None)))
				}
			};
		}
		Ok(Self::Memory(Default::default()))
	}

	pub(super) async fn push(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		rs: RecordStrategy,
		val: Value,
	) -> Result<()> {
		match self {
			Self::None => {}
			Self::Memory(s) => {
				s.push(val);
			}
			Self::MemoryOrdered(c) => {
				c.push(val);
			}
			Self::MemoryOrderedLimit(c) => {
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
				g.push(stk, ctx, opt, rs, val).await?;
			}
		}
		Ok(())
	}

	pub(super) async fn sort(&mut self) -> Result<()> {
		match self {
			Self::MemoryOrdered(c) => {
				#[cfg(not(target_family = "wasm"))]
				c.sort().await?;
				#[cfg(target_family = "wasm")]
				c.sort();
			}
			Self::MemoryOrderedLimit(c) => c.sort(),
			Self::MemoryRandom(c) => c.sort(),
			Self::None | Self::Memory(_) | Self::Groups(_) => {}
			#[cfg(storage)]
			Self::File(_) => {
				// File is sorted when it is taken.
			}
		}
		Ok(())
	}

	pub(super) async fn start_limit(
		&mut self,
		skip: Option<usize>,
		start: Option<u32>,
		limit: Option<u32>,
	) -> Result<()> {
		let start = if skip.is_some() {
			None
		} else {
			start
		};
		match self {
			Self::Memory(m) => m.start_limit(start, limit),
			Self::MemoryOrdered(m) => m.start_limit(start, limit),
			Self::MemoryOrderedLimit(m) => m.start_limit(start, limit),
			Self::MemoryRandom(c) => c.start_limit(start, limit),
			#[cfg(storage)]
			Self::File(f) => f.start_limit(start, limit),
			Self::None | Self::Groups(_) => {}
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
			Self::MemoryOrderedLimit(s) => s.len(),
			Self::MemoryRandom(s) => s.len(),
			#[cfg(storage)]
			Self::File(e) => e.len(),
			Self::Groups(g) => g.len(),
		}
	}

	pub(super) async fn take(&mut self) -> Result<Vec<Value>> {
		Ok(match self {
			Self::Memory(m) => m.take_vec(),
			Self::MemoryOrdered(c) => c.take_vec(),
			Self::MemoryOrderedLimit(c) => c.take_vec(),
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
			Self::MemoryOrderedLimit(c) => c.explain(exp),
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

impl From<Vec<Value>> for Results {
	fn from(value: Vec<Value>) -> Self {
		Results::Memory(value.into())
	}
}
