//! Lowering a block's control flow — a block, `LET`, `FOR`, `RETURN`,
//! `BREAK`, `CONTINUE` and `SLEEP` ([`Control`]) — into one program.
//!
//! The control flow is the program's own instructions: a loop is a jump back
//! to its head, `BREAK` and `CONTINUE` are jumps to its exit and to its next
//! element — or, where no loop of the program encloses them, signals that
//! leave it — and `LET` and `FOR` bind their names in a slot of the running
//! state, which the program's own reads of the name take, and — when
//! something in the name's scope that the program does not lower may read it
//! — in the block's frame, which each loop iteration saves and restores.
//!
//! A block is compiled in full when the query arrives: each statement it
//! runs is a group of the program ([`super::Lowering::group`]), run in the
//! frame the block has when it reaches it, and a loop's body is lowered once
//! for all its iterations. A statement that does not lower as a group fails
//! the lowering of the whole statement holding the block, and that statement
//! fails with the reason. No statement is planned while the program runs.
//!
//! Every statement is lowered against the catalog it meets when it runs: the
//! block's schema statements modelled ahead of it (`vm::speculate`) and the
//! tables its writes define, which bind as the open table no definition
//! holds; a write defining a table a schema statement after it extends takes
//! the ids that statement's speculative run drew for it
//! ([`super::program::ImplicitTable`]). A block whose catalog changes cannot
//! all be known ahead, and that runs a statement reading what they change, is
//! declined whole, and is lowered again with each statement after a schema
//! statement late-bound
//! ([`crate::exec::planner::Planner::binding_late_after_schema`]): it loads
//! what it names when it runs.
//!
//! A program declares the context level and access mode of the statements
//! it runs.

use super::{Lowered, Lowering, NotCovered};
use crate::exec::AccessMode;
use crate::exec::vm::plan::{Control, PhysicalPlan};
use crate::exec::vm::program::{BlockInstr, Collect, Instr, Label, Operand, Reg, Source};
use crate::expr::logical::LogicalOp;
use crate::val::Value;

/// What a block statement was planned knowing: the catalog it was mapped
/// against, for a schema statement whose effect the statements after it were
/// planned knowing, its speculative run, and for `EXPLAIN ANALYZE` of the
/// statement, what it renders.
#[derive(Clone, Copy, Default)]
pub(super) struct Planned<'a> {
	over: Option<&'a crate::kvs::speculation::Overlay>,
	late_bound: bool,
	within: crate::exec::planner::CatalogWithin,
	raises_when_reached: bool,
	modelled: Option<&'a crate::exec::vm::speculate::Speculated>,
	analysis: Option<&'a super::super::program::Analysis>,
}

/// A loop being lowered: the jumps and the statements run inside it that
/// leave it or move to its next element.
pub(super) struct LoopTargets {
	/// How many frames are saved when its body starts.
	depth: usize,
	breaks: Vec<Label>,
	continues: Vec<Label>,
	/// The runs of statements inside the loop, with the frames saved when
	/// each runs, whose `BREAK` and `CONTINUE` signals the loop takes.
	runs: Vec<(Label, usize)>,
	/// How many of [`Lowering::block_bound`] the blocks around the loop bind:
	/// the names after them are the loop's own.
	bound_from: usize,
	/// Whether anything in the body may write what its footprint cannot
	/// name: code the program does not see.
	writes: bool,
	/// What the body's statements write, with what the catalog says they
	/// trigger.
	written: crate::exec::logical::footprint::Footprint,
	/// The statements nested in the body's statements whose value is kept
	/// for one run of their statement ([`super::plan::Strategy::Once`]) and
	/// that read no name the loop binds, by [`Lowering::nested`] index, with
	/// what they read: kept across the loop's iterations when the body's
	/// writes have no `FLOW` edge into them.
	invariant: Vec<(usize, crate::exec::logical::footprint::Footprint)>,
}

/// A block name's binding whose scope is being lowered.
pub(super) struct SlotScope {
	/// The [`BlockInstr::Bind`] that binds it.
	bind: Label,
	slot: u16,
	/// Where the program's tables and the pipeline in flight stood when it was
	/// bound.
	pipelines: usize,
	host_code: usize,
	code: usize,
	groups: usize,
	frame_reads: u32,
	tables: [usize; 11],
}

impl Lowering<'_, '_> {
	/// The lengths of the tables whose entries evaluate what the program does
	/// not lower: subqueries, functions and closures, calls and methods,
	/// permissions, fetches, aggregates, splits and record ids.
	fn frame_tables(&self) -> [usize; 11] {
		[
			self.call_needs.len(),
			self.subqueries.len(),
			self.functions.len(),
			self.closures.len(),
			self.calls.len(),
			self.methods.len(),
			self.permissions.len(),
			self.fetches.len(),
			self.aggregates.len(),
			self.splits.len(),
			self.record_ids.len(),
		]
	}

	/// Bind the value in `value` to block name [`Self::fields`]`[name]` in a
	/// slot of its own, whose scope the caller opens with
	/// [`Self::slot_names`] and closes with [`Self::close_slot`].
	fn bind_slot(&mut self, name: u32, value: Reg) -> Lowered<(u16, SlotScope)> {
		let slot = self.slots;
		self.slots = self
			.slots
			.checked_add(1)
			.ok_or_else(|| NotCovered::internal("more names than a block binds"))?;
		let bind = self.block(BlockInstr::Bind {
			name,
			value,
			slot,
			frame: true,
		});
		Ok((
			slot,
			SlotScope {
				bind,
				slot,
				pipelines: self.pipelines.len(),
				host_code: self.host_code.len(),
				code: self.code.len(),
				groups: self.groups.len(),
				frame_reads: self.frame_reads,
				tables: self.frame_tables(),
			},
		))
	}

	/// Close the scope of `scope`'s binding: when nothing lowered in it reads
	/// the frame's parameters but through the program's own instructions, the
	/// name is bound in its slot alone.
	fn close_slot(&mut self, scope: &SlotScope) {
		let pipelines = &self.pipelines[scope.pipelines..];
		let host_code = &self.host_code[scope.host_code..];
		let frame_free = self.frame_reads == scope.frame_reads
			&& self.frame_tables() == scope.tables
			&& self.groups[scope.groups..].iter().all(|group| group.write.is_none())
			&& pipelines.iter().chain(host_code).all(|pipeline| {
				reads_no_frame(&pipeline.source) && pipeline.code.iter().all(lowered_whole)
			}) && self.code[scope.code..].iter().all(lowered_whole);
		if !frame_free {
			return;
		}
		if let Instr::Block(BlockInstr::Bind {
			frame,
			..
		}) = &mut self.code[scope.bind as usize]
		{
			*frame = false;
		}
	}

	/// Lower `control` into the program, its statements' sources being
	/// `sources`.
	///
	/// A block whose catalog changes are not all modelled ahead of it is
	/// declined by its mapping ([`data_dependent_ddl`]), so every statement
	/// the program runs was lowered against the catalog it meets when it
	/// runs.
	pub(super) async fn control(
		&mut self,
		control: &Control,
		sources: &[(LogicalOp, Vec<String>)],
	) -> Lowered<()> {
		self.sources = sources.to_vec();
		let result = self.block_constant(Value::None)?;
		crate::exec::vm::host::out_of_line(|| self.node(control, result, Collect::Block)).await?;
		// The program's dispatch reads each table as the catalog held it when
		// the query arrived, as the operator tree binds a value written in
		// `FROM`; a value in `FROM` only the data names, and a `FETCH`, the
		// tree resolves against the catalog the block's schema statements
		// leave. A block whose schema statements change what such a read of
		// the block's own may find is not covered.
		// A block planned late-bound after a change loads every table a
		// statement after it reads when the statement runs.
		let late = self.late_reach.clone().unwrap_or_else(crate::exec::vm::dispatch::Reach::none);
		if !self.planner.binds_late_after_schema()
			&& late.meets(&redefined_tables(control, sources, self.planner.function_registry()))
		{
			return Err(NotCovered::new(
				"a block that reads by data the tables its schema statements change",
			));
		}
		self.emit(Instr::Emit {
			src: result,
		});
		self.emit(Instr::EndRow);
		self.push_write_pipeline(Source::Once)?;
		self.scalar = true;
		// `RETURN` reports rows, as its operator does, and leaves with its
		// value.
		if matches!(control, Control::Return { .. }) {
			self.rows = true;
		}
		Ok(())
	}

	/// Lower `control`, leaving its value in `dst`; a statement's value is
	/// gathered as `collect` says.
	async fn node(&mut self, control: &Control, dst: Reg, collect: Collect) -> Lowered<()> {
		match control {
			Control::Seq(stmts) => {
				// A block nests what it holds, except the block that is an
				// `IF` branch, which its branch nests.
				let branch = std::mem::take(&mut self.branch_seq);
				let outer_nested = self.control_nested;
				self.control_nested |= !branch;
				let outer = self.block_bound.len();
				let outer_slots = self.slot_names.len();
				let outer_scopes = self.slot_scopes.len();
				self.load_none(dst);
				for statement in stmts {
					// A block inside a block binds its names in a frame of its
					// own, which ends with it.
					let scoped = matches!(statement, Control::Seq(_));
					if scoped {
						self.block(BlockInstr::Save);
						self.saved += 1;
					}
					let lowered = crate::exec::vm::host::out_of_line(|| {
						self.node(statement, dst, Collect::Block)
					})
					.await;
					if scoped {
						self.saved -= 1;
						self.block(BlockInstr::Restore);
					}
					lowered?;
					// A `LET` binds its name for the statements after it.
					if let Control::Let {
						name,
						..
					} = statement
					{
						self.block_bound.push(name.clone());
						let slot = self.slot_scopes.last().expect("a LET opens a scope").slot;
						self.slot_names.push((name.clone(), slot));
					}
				}
				self.block_bound.truncate(outer);
				self.slot_names.truncate(outer_slots);
				// Innermost first, so an inner name's reads are counted before
				// the outer name's scope closes.
				while self.slot_scopes.len() > outer_scopes {
					let scope = self.slot_scopes.pop().expect("checked above");
					self.close_slot(&scope);
				}
				self.control_nested = outer_nested;
			}
			Control::Let {
				name,
				kind,
				value,
			} => {
				let bound = self.register()?;
				crate::exec::vm::host::out_of_line(|| self.node(value, bound, Collect::Let))
					.await?;
				self.fields.push(name.clone());
				let name = (self.fields.len() - 1) as u32;
				if let Some(kind) = kind {
					self.kinds.push(kind.clone());
					self.block(BlockInstr::CoerceKind {
						value: bound,
						kind: (self.kinds.len() - 1) as u32,
						name,
					});
				}
				self.block(BlockInstr::CheckDepth {
					value: bound,
				});
				let (_, scope) = self.bind_slot(name, bound)?;
				self.slot_scopes.push(scope);
				self.release(bound);
				self.load_none(dst);
			}
			Control::For {
				param,
				range,
				first,
				body,
			} => {
				crate::exec::vm::host::out_of_line(|| {
					self.for_loop(param, range, first.as_deref(), body, dst)
				})
				.await?
			}
			Control::Return {
				value,
			} => {
				// What a `RETURN` evaluates is nested in it.
				let outer_nested = std::mem::replace(&mut self.control_nested, true);
				let returned = match value.as_ref() {
					// An expression's value is its one value, a `RETURN` inside
					// it answering it.
					Control::Value(expr) => {
						crate::exec::vm::host::out_of_line(|| self.once_value(expr, true)).await
					}
					other => match self.register() {
						Ok(returned) => crate::exec::vm::host::out_of_line(|| {
							self.node(other, returned, Collect::Return)
						})
						.await
						.map(|()| returned),
						Err(e) => Err(e),
					},
				};
				self.control_nested = outer_nested;
				let returned = returned?;
				self.block(BlockInstr::Return {
					value: returned,
				});
			}
			Control::Value(expr) => {
				let value =
					crate::exec::vm::host::out_of_line(|| self.once_value(expr, false)).await?;
				self.emit(Instr::Load {
					dst,
					src: Operand::Reg(value),
				});
				self.release(value);
			}
			Control::Sleep {
				duration,
			} => {
				let duration = self.block_constant(Value::Duration(*duration))?;
				self.block(BlockInstr::Sleep {
					duration,
				});
				self.release(duration);
				// Sleeping is a side effect: the operator reports that it
				// writes.
				self.writes = true;
				self.load_none(dst);
			}
			Control::If {
				branches,
				otherwise,
			} => {
				// The leading run of conditions that are expressions is one
				// evaluation region: each is evaluated only when the ones before
				// it did not hold, so a value an earlier condition computed is
				// read by a later one. A condition that is a statement ends the
				// run. The pins outlive the branch bodies, which are lowered
				// beneath scopes of their own.
				let roots = branches
					.iter()
					.map_while(|(condition, _)| match condition {
						Control::Value(expr) => Some(crate::expr::logical::dag::Root {
							expr,
							frame: crate::expr::logical::dag::Frame::new(0),
						}),
						_ => None,
					})
					.collect::<Vec<_>>();
				let scope = self.shared_scope(&roots);
				self.shared.push(scope);
				let lowered: Lowered<()> = async {
					let mut done = Vec::with_capacity(branches.len());
					for (condition, branch) in branches {
						let cond = self.register()?;
						crate::exec::vm::host::out_of_line(|| {
							self.node(condition, cond, Collect::Block)
						})
						.await?;
						let skip = self.emit(Instr::JumpIf {
							cond: Operand::Reg(cond),
							truthy: false,
							target: 0,
						});
						self.release(cond);
						crate::exec::vm::host::out_of_line(|| self.if_branch(branch, dst, collect))
							.await?;
						done.push(self.emit(Instr::Jump {
							target: 0,
						}));
						let next = self.here();
						self.patch(skip, next);
					}
					match otherwise {
						Some(branch) => {
							crate::exec::vm::host::out_of_line(|| {
								self.if_branch(branch, dst, collect)
							})
							.await?
						}
						None => self.load_none(dst),
					}
					let end = self.here();
					self.patch_all(done, end);
					Ok(())
				}
				.await;
				self.shared.pop();
				lowered?;
			}
			Control::Throw {
				value,
			} => {
				let thrown = self.register()?;
				crate::exec::vm::host::out_of_line(|| self.node(value, thrown, Collect::Block))
					.await?;
				self.emit(Instr::Throw {
					src: Operand::Reg(thrown),
				});
				self.release(thrown);
				self.load_none(dst);
			}
			Control::Statement {
				plan,
				source,
				changes_catalog,
				planned_over,
				modelled,
				analysis,
				late_bound,
				within,
				raises_when_reached,
			} => {
				let planned = Planned {
					over: planned_over.as_deref(),
					late_bound: *late_bound,
					within: *within,
					raises_when_reached: *raises_when_reached,
					modelled: modelled.as_deref(),
					analysis: analysis.as_deref(),
				};
				crate::exec::vm::host::out_of_line(|| {
					self.statement(plan, *source, *changes_catalog, planned, dst, collect)
				})
				.await?;
			}
			Control::Break | Control::Continue => {
				let leaves = matches!(control, Control::Break);
				if self.loops.is_empty() {
					// No loop of this program encloses it: the signal leaves
					// the program for its caller, as the statement raises it.
					self.emit(Instr::Signal {
						brk: leaves,
						frame: None,
					});
					self.load_none(dst);
				} else {
					// The frames the blocks inside the loop saved are restored
					// on the way out of them.
					let depth = self.loops.last().expect("checked above").depth;
					for _ in depth..self.saved {
						self.block(BlockInstr::Restore);
					}
					let jump = self.emit(Instr::Jump {
						target: 0,
					});
					let targets = self.loops.last_mut().expect("checked above");
					if leaves {
						targets.breaks.push(jump);
					} else {
						targets.continues.push(jump);
					}
				}
			}
		}
		Ok(())
	}

	/// A branch of an `IF` in a statement's place, its value left in `dst`:
	/// statements run in a frame of their own, which ends with the branch.
	async fn if_branch(&mut self, branch: &Control, dst: Reg, collect: Collect) -> Lowered<()> {
		if let Control::Value(_) = branch {
			self.control_ifs += 1;
			let lowered =
				crate::exec::vm::host::out_of_line(|| self.node(branch, dst, collect)).await;
			self.control_ifs -= 1;
			return lowered;
		}
		self.block(BlockInstr::Save);
		self.saved += 1;
		let outer_scopes = self.slot_scopes.len();
		self.control_ifs += 1;
		self.branch_seq = true;
		let lowered = crate::exec::vm::host::out_of_line(|| self.node(branch, dst, collect)).await;
		self.branch_seq = false;
		self.control_ifs -= 1;
		// A `LET` the branch is binds its name for nothing after it.
		while self.slot_scopes.len() > outer_scopes {
			let scope = self.slot_scopes.pop().expect("checked above");
			self.close_slot(&scope);
		}
		self.saved -= 1;
		self.block(BlockInstr::Restore);
		lowered
	}

	/// `FOR param IN range { body }` — `exec/operators/foreach.rs`: `range`
	/// evaluated once, then `body` for each element with `param` bound to it,
	/// the frame saved before and restored after each iteration; `CONTINUE`
	/// moves to the next element and `BREAK` leaves. Its value is `NONE`.
	///
	/// A loop with a `first` body runs it for its first element and `body`
	/// for the rest; a register notes that the first has run. Everything
	/// after the loop was planned against the catalog the first iteration
	/// leaves, which a loop that runs none leaves undone: a block reading what
	/// it defines after such a loop is not lowered ([`data_dependent_ddl`]).
	async fn for_loop(
		&mut self,
		param: &crate::expr::Param,
		range: &Control,
		first: Option<&Control>,
		body: &Control,
		dst: Reg,
	) -> Lowered<()> {
		let evaluated = self.register()?;
		// The range is evaluated once, before the body: one evaluation region.
		let roots = match range {
			Control::Value(expr) => vec![crate::expr::logical::dag::Root {
				expr,
				frame: crate::expr::logical::dag::Frame::new(0),
			}],
			_ => Vec::new(),
		};
		let scope = self.shared_scope(&roots);
		self.shared.push(scope);
		let ranged =
			crate::exec::vm::host::out_of_line(|| self.node(range, evaluated, Collect::Block))
				.await;
		self.shared.pop();
		ranged?;
		let range = evaluated;
		let peeled = match first {
			Some(_) => {
				let peeled = self.register()?;
				self.consts.push(Value::Bool(false));
				self.emit(Instr::Load {
					dst: peeled,
					src: Operand::Const((self.consts.len() - 1) as u32),
				});
				Some(peeled)
			}
			None => None,
		};
		let slot = self.iter_slots;
		self.iter_slots += 1;
		self.block(BlockInstr::IterStart {
			src: range,
			slot,
		});
		self.release(range);
		let element = self.register()?;
		let scratch = self.register()?;
		self.fields.push(param.as_str().to_owned());
		let name = (self.fields.len() - 1) as u32;
		let head = self.block(BlockInstr::IterNext {
			slot,
			dst: element,
			done: 0,
		});
		self.block(BlockInstr::Save);
		self.saved += 1;
		let (bound, scope) = self.bind_slot(name, element)?;
		let unseen = self.unseen_code();
		self.loops.push(LoopTargets {
			depth: self.saved,
			breaks: Vec::new(),
			continues: Vec::new(),
			runs: Vec::new(),
			bound_from: self.block_bound.len(),
			writes: false,
			written: Default::default(),
			invariant: Vec::new(),
		});
		self.block_bound.push(param.as_str().to_owned());
		self.slot_names.push((param.as_str().to_owned(), bound));
		let lowered = match (first, peeled) {
			(Some(first), Some(peeled)) => {
				crate::exec::vm::host::out_of_line(|| {
					self.peeled_body(first, body, peeled, scratch)
				})
				.await
			}
			_ => {
				crate::exec::vm::host::out_of_line(|| self.node(body, scratch, Collect::Discard))
					.await
			}
		};
		self.slot_names.pop();
		self.block_bound.pop();
		if lowered.is_ok() {
			self.close_slot(&scope);
		}
		let mut targets = self.loops.pop().expect("pushed above");
		self.saved -= 1;
		lowered?;
		targets.writes |= self.writes_unseen(&unseen);
		if let Some(outer) = self.loops.last_mut() {
			outer.writes |= targets.writes;
			outer.written.absorb(targets.written.clone());
		}
		if !targets.writes {
			// A statement kept once for the loop is run where an iteration first
			// reaches it unless what an iteration writes flows into what it
			// reads: a `FLOW` edge from the body's writes to its reads.
			for (at, reads) in targets.invariant {
				if crate::exec::logical::edges::data(&targets.written, &reads)
					.contains(crate::exec::logical::edges::Edges::FLOW)
				{
					continue;
				}
				self.nested[at].owner = None;
				self.nested[at].hoist = Some(slot);
			}
		}
		self.block(BlockInstr::Restore);
		self.emit(Instr::Jump {
			target: head,
		});
		let continued = self.block(BlockInstr::Restore);
		self.emit(Instr::Jump {
			target: head,
		});
		let broken = self.block(BlockInstr::Restore);
		let exit = self.here();
		self.patch(head, exit);
		self.patch_all(targets.breaks, broken);
		self.patch_all(targets.continues, continued);
		// A statement signalling from inside blocks within the body lands on
		// a pad that restores their frames first.
		let mut pads: Vec<(usize, Label, Label)> = Vec::new();
		let mut over = Vec::new();
		for (run, saved) in targets.runs {
			let (on_break_at, on_continue_at) = if saved == targets.depth {
				(broken, continued)
			} else if let Some((_, b, c)) = pads.iter().find(|(depth, _, _)| *depth == saved) {
				(*b, *c)
			} else {
				if over.is_empty() {
					over.push(self.emit(Instr::Jump {
						target: 0,
					}));
				}
				let b = self.here();
				for _ in targets.depth..saved {
					self.block(BlockInstr::Restore);
				}
				self.emit(Instr::Jump {
					target: broken,
				});
				let c = self.here();
				for _ in targets.depth..saved {
					self.block(BlockInstr::Restore);
				}
				self.emit(Instr::Jump {
					target: continued,
				});
				pads.push((saved, b, c));
				(b, c)
			};
			if let Instr::Block(
				BlockInstr::RunEmbedded {
					on_break,
					on_continue,
					..
				}
				| BlockInstr::RunServed {
					on_break,
					on_continue,
					..
				},
			) = &mut self.code[run as usize]
			{
				*on_break = Some(on_break_at);
				*on_continue = Some(on_continue_at);
			}
		}
		let past = self.here();
		self.patch_all(over, past);
		if let Some(peeled) = peeled {
			self.release(peeled);
		}
		self.release(element);
		self.release(scratch);
		self.load_none(dst);
		Ok(())
	}

	/// A peeled loop's body: `first` while `peeled` is false, which it sets,
	/// and `rest` once it is true.
	async fn peeled_body(
		&mut self,
		first: &Control,
		rest: &Control,
		peeled: Reg,
		scratch: Reg,
	) -> Lowered<()> {
		let to_rest = self.emit(Instr::JumpIf {
			cond: Operand::Reg(peeled),
			truthy: true,
			target: 0,
		});
		self.consts.push(Value::Bool(true));
		self.emit(Instr::Load {
			dst: peeled,
			src: Operand::Const((self.consts.len() - 1) as u32),
		});
		crate::exec::vm::host::out_of_line(|| self.node(first, scratch, Collect::Discard)).await?;
		let done = self.emit(Instr::Jump {
			target: 0,
		});
		let at = self.here();
		self.patch(to_rest, at);
		crate::exec::vm::host::out_of_line(|| self.node(rest, scratch, Collect::Discard)).await?;
		let at = self.here();
		self.patch(done, at);
		Ok(())
	}

	/// A statement compiled into the program: `plan`, lowered here as a group
	/// of the program's own pipelines ([`super::Lowering::group`]), run by
	/// [`BlockInstr::RunEmbedded`] over the program's state, or by
	/// [`BlockInstr::RunAnalyzed`] for `EXPLAIN ANALYZE` of it. Its tail
	/// (`TIMEOUT`, `VERSION`, `ONLY`) is the group's, applied by the host. A
	/// statement that does not lower as a group is not covered, and neither is
	/// the statement holding the block.
	async fn statement(
		&mut self,
		plan: &PhysicalPlan,
		source: u16,
		changes_catalog: bool,
		planned: Planned<'_>,
		dst: Reg,
		collect: Collect,
	) -> Lowered<()> {
		let (_, shadows) = self.sources[source as usize].clone();
		let planner = self
			.planner
			.nested_statement(self.control_ifs, self.control_nested)
			.shadowing(shadows.iter().cloned());
		// A late-bound statement is lowered as it was mapped, and may write
		// whatever the catalog it meets lets it, which no footprint read now
		// names.
		let planner = match planned.late_bound {
			true => {
				for targets in &mut self.loops {
					targets.writes = true;
				}
				planner.late_binding()
			}
			false => planner.reaching(planned.within),
		};
		// Lowered, and its footprint and loop invariants judged, against the
		// catalog it was mapped against, as the block's schema statements
		// before it leave it: through a speculation's view, held until the
		// statement is lowered.
		let (planner, _speculation, viewed) = match planned.over {
			Some(over) => match crate::exec::vm::speculate::view(&planner, over) {
				Some((speculation, over)) => (over, Some(speculation), true),
				None => (planner, None, false),
			},
			None => (planner, None, true),
		};
		let nested_from = self.nested.len();
		// A schema statement's draws take the ids its speculative run leased,
		// which the statements after it were planned knowing.
		self.reserving = planned.modelled.map(|run| run.draws.clone()).unwrap_or_default();
		let reservations = self.reservations.len();
		let implicit_tables = self.implicit_tables.len();
		self.next_group_refuses = planned.raises_when_reached;
		let lowered = match viewed {
			true => crate::exec::vm::host::out_of_line(|| self.group(&planner, plan)).await,
			false => {
				Err(NotCovered::new("a block statement over a catalog the speculation cannot view"))
			}
		};
		self.reserving.clear();
		// The statements after a schema statement were lowered knowing its
		// speculative run: one whose run here would lease ids they do not know
		// is not the statement they know. Nor is one defining a table a write
		// before it may define first, unless that write takes the reservations
		// of its definition.
		let shares_unknown =
			planned.modelled.and_then(|run| run.shared.as_ref()).is_some_and(|table| {
				!self.implicit_tables[implicit_tables..].iter().any(|shared| shared.table == *table)
			});
		if shares_unknown
			|| planned.modelled.is_some_and(|run| {
				lowered.is_err() || self.reservations.len() - reservations != run.draws.len()
			}) {
			#[cfg(test)]
			crate::exec::vm::speculate::DECLINED.with(|count| count.set(count.get() + 1));
			return Err(NotCovered::new(crate::exec::vm::speculate::DATA_DEPENDENT_DDL));
		}
		let (statement, access_mode, required_context) = lowered?;
		let (statement_op, _) = self.sources[source as usize].clone();
		let writes =
			self.groups[statement as usize].write.is_some() || access_mode != AccessMode::ReadOnly;
		if (writes || changes_catalog) && !self.loops.is_empty() {
			let catalog = plan.catalog.as_ref().unwrap_or(&self.catalog);
			let footprint =
				crate::exec::vm::plan_catalog::footprint(&planner, catalog, &statement_op, false)
					.await;
			for targets in &mut self.loops {
				targets.written.absorb(footprint.clone());
			}
		}
		crate::exec::vm::host::out_of_line(|| {
			self.loop_invariants(&planner, plan, statement, nested_from)
		})
		.await;
		// The statement's context level and access mode are the program's too.
		self.delegate_context = self.delegate_context.max(required_context);
		// A statement after a schema statement of the program resolves the
		// session's namespace and database when it runs, as far as it
		// requires: the schema statement may have created, removed or
		// replaced either.
		let resolve = match self.catalog_changed.is_some() || planned.late_bound {
			true => Some(required_context),
			false => None,
		};
		if access_mode != AccessMode::ReadOnly {
			self.writes = true;
		}
		if let Some(analysis) = planned.analysis {
			// The analysis ends the statement at the signals it raises, so no
			// loop takes them.
			let at = u16::try_from(self.analyses.len())
				.map_err(|_| NotCovered::internal("more analyses than a program holds"))?;
			self.analyses.push(analysis.clone());
			self.block(BlockInstr::RunAnalyzed {
				statement,
				analysis: at,
				dst,
				collect,
				resolve,
			});
			return Ok(());
		}
		// A late-bound statement runs as planned for the catalog its
		// transaction reads when the block reaches it — the access path it
		// takes, the definitions it inlines — served from the compile cache by
		// its text; its group, lowered late-bound, stands for what the block
		// knows of it here. It reads the block's names through the frame.
		let at = if planned.late_bound && self.serves_late {
			let (op, shadows) = self.sources[source as usize].clone();
			let served = u16::try_from(self.served.len()).map_err(|_| {
				NotCovered::internal("more late-bound statements than a program holds")
			})?;
			self.served.push(crate::exec::vm::program::ServedSpec {
				shadowed: shadows,
				depth: self.planner.current_depth(),
			});
			self.served_ops.push(std::sync::Arc::new(op));
			self.frame_reads += 1;
			self.block(BlockInstr::RunServed {
				served,
				dst,
				collect,
				on_break: None,
				on_continue: None,
				resolve: Some(required_context),
			})
		} else {
			self.block(BlockInstr::RunEmbedded {
				statement,
				dst,
				collect,
				on_break: None,
				on_continue: None,
				resolve,
			})
		};
		// What runs after a schema statement runs in the frame the catalog it
		// leaves resolves the session's names to.
		if changes_catalog {
			self.block(BlockInstr::Resolve);
			if self.catalog_changed.is_none() {
				self.catalog_changed = Some((self.delegate_context, self.call_needs.len()));
			}
		}
		let saved = self.saved;
		if let Some(targets) = self.loops.last_mut() {
			targets.runs.push((at, saved));
		}
		Ok(())
	}

	/// Note, for the innermost loop being lowered, the statements nested in
	/// statement `plan` — lowered as group `group`, its nested statements
	/// from [`Self::nested`]`[from..]` — whose value is kept for a run of the
	/// statement, when none of them reads a name the loop binds.
	async fn loop_invariants(
		&mut self,
		planner: &crate::exec::planner::Planner<'_>,
		plan: &PhysicalPlan,
		group: u16,
		from: usize,
	) {
		let Some(targets) = self.loops.last() else {
			return;
		};
		let names = &self.block_bound[targets.bound_from..];
		let reads_loop_name = plan.nested.iter().any(|nested| {
			nested.strategy == crate::exec::vm::plan::Strategy::Once
				&& crate::exec::vm::map::names_param(&nested.op, &|name| {
					names.iter().any(|bound| bound == name)
				})
		});
		if reads_loop_name {
			return;
		}
		let kept: Vec<usize> = (from..self.nested.len())
			.filter(|at| self.nested[*at].once && self.nested[*at].owner == Some(group))
			.collect();
		if kept.is_empty() {
			return;
		}
		// What any of the statement's nested reads kept once reads: each kept
		// statement is judged against the union, which over-approximates it.
		let mut reads = crate::exec::logical::footprint::Footprint::default();
		for nested in &plan.nested {
			if nested.strategy == crate::exec::vm::plan::Strategy::Once {
				let catalog = std::sync::Arc::clone(
					nested.plan.catalog.as_ref().or(plan.catalog.as_ref()).unwrap_or(&self.catalog),
				);
				reads.absorb(
					crate::exec::vm::plan_catalog::footprint(planner, &catalog, &nested.op, true)
						.await,
				);
			}
		}
		let invariant = &mut self.loops.last_mut().expect("checked above").invariant;
		invariant.extend(kept.into_iter().map(|at| (at, reads.clone())));
	}

	/// How far the tables of what the program does not see reached: custom
	/// functions, closures and their calls, calls and subqueries.
	fn unseen_code(&self) -> [usize; 5] {
		[
			self.functions.len(),
			self.closures.len(),
			self.invokes,
			self.call_needs.len(),
			self.subqueries.len(),
		]
	}

	/// Whether what was lowered since `from` ([`Self::unseen_code`]) holds
	/// code that may write: a custom function, a closure, or a call or
	/// subquery whose access mode writes.
	fn writes_unseen(&self, from: &[usize; 5]) -> bool {
		let writes = |mode: AccessMode| mode != AccessMode::ReadOnly;
		self.functions.len() > from[0]
			|| self.closures.len() > from[1]
			|| self.invokes > from[2]
			|| self.call_needs[from[3]..].iter().any(|needs| writes(needs.access_mode))
			|| self.subqueries[from[4]..].iter().any(|spec| writes(spec.access_mode))
	}

	fn block_constant(&mut self, value: Value) -> Lowered<Reg> {
		self.consts.push(value);
		let src = Operand::Const((self.consts.len() - 1) as u32);
		let dst = self.register()?;
		self.emit(Instr::Load {
			dst,
			src,
		});
		Ok(dst)
	}

	fn load_none(&mut self, dst: Reg) {
		self.consts.push(Value::None);
		self.emit(Instr::Load {
			dst,
			src: Operand::Const((self.consts.len() - 1) as u32),
		});
	}

	fn block(&mut self, instr: BlockInstr) -> u32 {
		self.emit(Instr::Block(instr))
	}
}

/// Whether a pipeline over `source` reads nothing of the frame's parameters
/// through its source: one that evaluates no compiled expression.
fn reads_no_frame(source: &Source) -> bool {
	matches!(
		source,
		Source::Once
			| Source::Scan { .. }
			| Source::LightweightEdges {
				record: None,
				..
			} | Source::Sorted { .. }
			| Source::Groups { .. }
			| Source::Split { .. }
	)
}

/// Whether `instr` is the program's own work, which reads a block name only
/// through its slot: no compiled expression, host call or record fetch.
fn lowered_whole(instr: &Instr) -> bool {
	matches!(
		instr,
		Instr::Block(_)
			| Instr::CoerceField { .. }
			| Instr::CompareFieldInt { .. }
			| Instr::JumpIfFieldInt { .. }
			| Instr::JumpIfCompare { .. }
			| Instr::Compare { .. }
			| Instr::Arith { .. }
			| Instr::Unary { .. }
			| Instr::NewArray { .. }
			| Instr::ArrayPush { .. }
			| Instr::LoadParam { .. }
			| Instr::LoadSlot { .. }
			| Instr::SortKey { .. }
			| Instr::Nav { .. }
			| Instr::ElemNext { .. }
			| Instr::JumpIfNullish { .. }
			| Instr::JumpIfNone { .. }
			| Instr::JumpIf { .. }
			| Instr::Skip { .. }
			| Instr::Take { .. }
			| Instr::JumpIfLimitReached { .. }
			| Instr::NewObject { .. }
			| Instr::SeedObject { .. }
			| Instr::BuildObject { .. }
			| Instr::LoadPath { .. }
			| Instr::LoadRow { .. }
			| Instr::RemoveField { .. }
			| Instr::SetField { .. }
			| Instr::Load { .. }
			| Instr::Emit { .. }
			| Instr::EmitEnd { .. }
			| Instr::BuildObjectEmit { .. }
			| Instr::FilterFieldInt { .. }
			| Instr::FilterCompare { .. }
			| Instr::Filter { .. }
			| Instr::SplitPush { .. }
			| Instr::SortPush { .. }
			| Instr::GroupKey { .. }
			| Instr::GroupSelect { .. }
			| Instr::AccUpdate { .. }
			| Instr::Jump { .. }
			| Instr::Next { .. }
			| Instr::EndRow
			| Instr::StopPipeline
	)
}

/// A step of a block, in the order a run of it reaches them.
enum Step {
	/// A statement or expression lowered or planned with the program: its
	/// source, the catalog it is lowered against, and whether code it
	/// reaches may change the catalog unseen
	/// ([`super::super::speculate::runs_unseen_schema`]).
	Lowered {
		op: Box<LogicalOp>,
		over: Option<std::sync::Arc<crate::kvs::speculation::Overlay>>,
		may_hide_schema: bool,
	},
	/// A change to the catalog the statements after it were not lowered
	/// knowing: what it writes, `None` when that is not known.
	Changes(Option<crate::exec::logical::footprint::Footprint>),
	/// The start and the end of a loop body, which runs again after the
	/// steps inside it.
	LoopStart,
	LoopEnd,
}

/// The steps of `control`, `over` being the catalog the steps from here on
/// are lowered against as the block's modelled schema statements leave it.
fn steps(
	control: &Control,
	sources: &[(LogicalOp, Vec<String>)],
	registry: &crate::exec::function::FunctionRegistry,
	over: &mut Option<std::sync::Arc<crate::kvs::speculation::Overlay>>,
	out: &mut Vec<Step>,
) {
	let value =
		|expr: &crate::expr::logical::LogicalExpr, over: &Option<_>, out: &mut Vec<Step>| {
			out.push(Step::Lowered {
				op: Box::new(LogicalOp::Eval(crate::expr::logical::LogicalEval {
					expr: expr.clone(),
				})),
				over: Option::clone(over),
				may_hide_schema: true,
			});
		};
	match control {
		Control::Seq(stmts) => stmts.iter().for_each(|s| steps(s, sources, registry, over, out)),
		Control::Let {
			value: inner,
			..
		}
		| Control::Return {
			value: inner,
		}
		| Control::Throw {
			value: inner,
		} => steps(inner, sources, registry, over, out),
		Control::For {
			range,
			first,
			body,
			..
		} => {
			steps(range, sources, registry, over, out);
			out.push(Step::LoopStart);
			if let Some(first) = first {
				steps(first, sources, registry, over, out);
			}
			steps(body, sources, registry, over, out);
			out.push(Step::LoopEnd);
			// A peeled loop's later statements were lowered knowing what its
			// first iteration defines, which a loop that runs none leaves
			// undone.
			if let Some(first) = first
				&& !runs_once_at_least(range)
			{
				let mut defined = crate::exec::logical::footprint::Footprint::default();
				modelled_schema(first, sources, registry, &mut defined);
				out.push(Step::Changes(Some(defined)));
			}
		}
		Control::Value(expr) => value(expr, over, out),
		Control::If {
			branches,
			otherwise,
		} => {
			for (condition, branch) in branches {
				steps(condition, sources, registry, over, out);
				steps(branch, sources, registry, over, out);
			}
			if let Some(otherwise) = otherwise {
				steps(otherwise, sources, registry, over, out);
			}
		}
		Control::Statement {
			source,
			changes_catalog,
			planned_over,
			modelled,
			..
		} => {
			let (op, _) = &sources[*source as usize];
			out.push(Step::Lowered {
				op: Box::new(op.clone()),
				over: planned_over.clone(),
				may_hide_schema: !changes_catalog,
			});
			match modelled {
				Some(run) => *over = Some(std::sync::Arc::new(run.overlay.clone())),
				None if *changes_catalog => {
					out.push(Step::Changes(Some(crate::exec::logical::footprint::of(op, registry))))
				}
				None => {}
			}
		}
		Control::Sleep {
			..
		}
		| Control::Break
		| Control::Continue => {}
	}
}

/// The names `control` binds: its `LET`s and loop parameters.
fn bound_names(control: &Control, out: &mut Vec<String>) {
	match control {
		Control::Seq(stmts) => stmts.iter().for_each(|s| bound_names(s, out)),
		Control::Let {
			name,
			value,
			..
		} => {
			out.push(name.clone());
			bound_names(value, out);
		}
		Control::Return {
			value,
		}
		| Control::Throw {
			value,
		} => bound_names(value, out),
		Control::For {
			param,
			first,
			body,
			..
		} => {
			out.push(param.as_str().to_owned());
			first.iter().for_each(|first| bound_names(first, out));
			bound_names(body, out);
		}
		Control::If {
			branches,
			otherwise,
		} => {
			for (_, branch) in branches {
				bound_names(branch, out);
			}
			otherwise.iter().for_each(|branch| bound_names(branch, out));
		}
		_ => {}
	}
}

/// The tables whose reads a schema statement of `control`, whose statements
/// are `sources`, may change: those whose definition, fields or view it
/// defines, alters or removes. A definition of anything else — an index, an
/// event, a function — changes no row a read of a table yields.
fn redefined_tables(
	control: &Control,
	sources: &[(LogicalOp, Vec<String>)],
	registry: &crate::exec::function::FunctionRegistry,
) -> crate::exec::vm::dispatch::Reach {
	use crate::exec::logical::footprint::{CatalogKind, Resource};
	use crate::exec::vm::dispatch::Reach;
	use crate::expr::logical::LogicalDdlStatement;
	use crate::expr::statements::{AlterStatement, DefineStatement, RemoveStatement};
	let redefined = |control: &Control| redefined_tables(control, sources, registry);
	let all = |controls: &mut dyn Iterator<Item = &Control>| {
		controls.fold(Reach::none(), |reach, control| reach.union(redefined(control)))
	};
	match control {
		Control::Seq(stmts) => all(&mut stmts.iter()),
		Control::Let {
			value,
			..
		}
		| Control::Return {
			value,
		}
		| Control::Throw {
			value,
		} => redefined(value),
		Control::For {
			range,
			first,
			body,
			..
		} => {
			all(&mut std::iter::once(range.as_ref()).chain(first.as_deref()).chain([body.as_ref()]))
		}
		Control::If {
			branches,
			otherwise,
		} => all(&mut branches
			.iter()
			.flat_map(|(condition, branch)| [condition, branch])
			.chain(otherwise.as_deref())),
		Control::Statement {
			source,
			changes_catalog: true,
			..
		} => {
			let op = &sources[*source as usize].0;
			let redefines = match op {
				LogicalOp::Ddl(ddl) => match &ddl.statement {
					LogicalDdlStatement::Define(define) => {
						matches!(
							define.as_ref(),
							DefineStatement::Table(_) | DefineStatement::Field(_)
						)
					}
					LogicalDdlStatement::Alter(alter) => {
						matches!(
							alter.as_ref(),
							AlterStatement::Table(_) | AlterStatement::Field(_)
						)
					}
					LogicalDdlStatement::Remove(remove) => {
						matches!(
							remove.as_ref(),
							RemoveStatement::Table(_) | RemoveStatement::Field(_)
						)
					}
					LogicalDdlStatement::Rebuild(_) => false,
				},
				_ => true,
			};
			if !redefines {
				return Reach::none();
			}
			crate::exec::logical::footprint::of(op, registry).writes.iter().fold(
				Reach::none(),
				|reach, written| match written {
					Resource::Unknown => Reach::Any,
					Resource::Catalog {
						kind: CatalogKind::Table | CatalogKind::Field | CatalogKind::View,
						table,
						..
					} => reach.union(match table {
						Some(table) => {
							Reach::Tables(std::collections::BTreeSet::from([table.clone()]))
						}
						None => Reach::Any,
					}),
					Resource::Catalog {
						..
					}
					| Resource::Data {
						..
					} => reach,
				},
			)
		}
		Control::Statement {
			..
		}
		| Control::Value(_)
		| Control::Sleep {
			..
		}
		| Control::Break
		| Control::Continue => Reach::none(),
	}
}

/// What the schema statements of `control` modelled ahead of the block write.
fn modelled_schema(
	control: &Control,
	sources: &[(LogicalOp, Vec<String>)],
	registry: &crate::exec::function::FunctionRegistry,
	out: &mut crate::exec::logical::footprint::Footprint,
) {
	match control {
		Control::Seq(stmts) => {
			stmts.iter().for_each(|s| modelled_schema(s, sources, registry, out))
		}
		Control::Let {
			value,
			..
		}
		| Control::Return {
			value,
		}
		| Control::Throw {
			value,
		} => modelled_schema(value, sources, registry, out),
		Control::For {
			first,
			body,
			..
		} => {
			first.iter().for_each(|first| modelled_schema(first, sources, registry, out));
			modelled_schema(body, sources, registry, out);
		}
		Control::If {
			branches,
			otherwise,
		} => {
			for (_, branch) in branches {
				modelled_schema(branch, sources, registry, out);
			}
			otherwise.iter().for_each(|branch| modelled_schema(branch, sources, registry, out));
		}
		Control::Statement {
			source,
			modelled: Some(_),
			..
		} => out.absorb(crate::exec::logical::footprint::of(&sources[*source as usize].0, registry)),
		_ => {}
	}
}

/// Whether a loop over `range` runs its body at least once whatever the data:
/// a written array or range that is not empty.
fn runs_once_at_least(range: &Control) -> bool {
	use crate::expr::BinaryOperator;
	use crate::expr::logical::{LogicalExpr, LogicalLiteral};
	let Control::Value(expr) = range else {
		return false;
	};
	if let LogicalExpr::Binary {
		left,
		op,
		right,
	} = expr
		&& let (
			LogicalExpr::Literal(LogicalLiteral::Integer(from)),
			LogicalExpr::Literal(LogicalLiteral::Integer(to)),
		) = (left.as_ref(), right.as_ref())
	{
		let first = match op {
			BinaryOperator::Range | BinaryOperator::RangeInclusive => Some(*from),
			BinaryOperator::RangeSkip | BinaryOperator::RangeSkipInclusive => from.checked_add(1),
			_ => None,
		};
		return match (op, first) {
			(BinaryOperator::Range | BinaryOperator::RangeSkip, Some(first)) => first < *to,
			(_, Some(first)) => first <= *to,
			(_, None) => false,
		};
	}
	match crate::exec::planner::util::try_literal_to_value(expr) {
		Some(Value::Array(items)) => !items.is_empty(),
		Some(Value::Range(range)) => {
			range.coerce_to_typed::<i64>().is_ok_and(|range| range.iter().next().is_some())
		}
		_ => false,
	}
}

/// Whether the lowering of a step with footprint `step` reads what a catalog
/// change writing `change` may change; a change of unknown footprint, or one
/// to definitions no table or name scopes, may change what any step reading
/// or writing anything reads.
fn depends(
	change: Option<&crate::exec::logical::footprint::Footprint>,
	step: &crate::exec::logical::footprint::Footprint,
) -> bool {
	use crate::exec::logical::footprint::{CatalogKind, Resource};
	let touched = || step.reads.iter().chain(&step.writes);
	let Some(change) = change else {
		return touched().next().is_some();
	};
	change.writes.iter().any(|written| match written {
		Resource::Data {
			..
		} => false,
		Resource::Unknown
		| Resource::Catalog {
			kind: CatalogKind::Other | CatalogKind::Access,
			..
		} => touched().next().is_some(),
		Resource::Catalog {
			kind,
			table,
			..
		} => touched().any(|read| {
			written.overlaps(read)
				// A record read or written is read as its table's definitions
				// say: its permissions, fields and computed values.
				|| matches!(kind, CatalogKind::Table | CatalogKind::Field | CatalogKind::Index
					| CatalogKind::Event | CatalogKind::View)
					&& matches!(read, Resource::Data { table: read } if table.is_none() || read.is_none() || read == table)
		}),
	})
}

/// Whether block `control` runs a statement after a catalog change it was not
/// lowered knowing, whose lowering reads what that change may change: the
/// statements after the change, and every statement of a loop body the change
/// is in, which the loop runs again.
///
/// A change is a schema statement no speculative run modelled — one in a loop
/// or under a condition, one after a change the block could not model, one
/// whose speculative run did not end — a peeled loop that may run no
/// iteration, and a statement that reaches code that may change the catalog
/// ([`super::super::speculate::runs_unseen_schema`]). A write defining the
/// table it writes to is none: a table no definition holds binds as the open,
/// permission-`NONE` table the write defines.
pub(in crate::exec::vm) async fn data_dependent_ddl(
	planner: &crate::exec::planner::Planner<'_>,
	control: &Control,
	sources: &[(LogicalOp, Vec<String>)],
) -> bool {
	let registry = planner.function_registry();
	let mut found = Vec::new();
	steps(control, sources, registry, &mut None, &mut found);
	// The first step each step is followed by, in a run of the block: the
	// step after it, or the start of the outermost loop it is in.
	let mut follows = Vec::with_capacity(found.len());
	let mut loops: Vec<usize> = Vec::new();
	for (at, step) in found.iter().enumerate() {
		match step {
			Step::LoopStart => loops.push(at),
			Step::LoopEnd => {
				loops.pop();
			}
			_ => {}
		}
		follows.push(loops.first().copied().unwrap_or(at + 1));
	}
	let mut bound = Vec::new();
	bound_names(control, &mut bound);
	// A name the block or the frame around it binds, or the engine binds, is
	// no parameter the catalog defines.
	let catalog_param = |name: &str| {
		!bound.iter().any(|bound| bound == name)
			&& !planner.param_is_shadowed(name)
			&& !crate::expr::logical::ITERATION_PARAMS.contains(&name)
			&& !surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&name)
	};
	let footprints: Vec<Option<crate::exec::logical::footprint::Footprint>> = found
		.iter()
		.map(|step| match step {
			Step::Lowered {
				op,
				..
			} => {
				let mut footprint = crate::exec::logical::footprint::of(op, registry);
				footprint.reads.retain(|read| match read {
					crate::exec::logical::footprint::Resource::Catalog {
						kind: crate::exec::logical::footprint::CatalogKind::Param,
						name: Some(name),
						..
					} => catalog_param(name),
					_ => true,
				});
				Some(footprint)
			}
			_ => None,
		})
		.collect();
	let dependent = |change: Option<&crate::exec::logical::footprint::Footprint>, from: usize| {
		footprints[from..].iter().flatten().any(|step| depends(change, step))
	};
	let outside = crate::exec::vm::speculate::closures_from_outside(
		planner,
		found.iter().filter_map(|step| match step {
			Step::Lowered {
				op,
				..
			} => Some(op.as_ref()),
			_ => None,
		}),
		&bound,
	);
	for (at, step) in found.iter().enumerate() {
		match step {
			Step::Changes(change) if dependent(change.as_ref(), follows[at]) => return true,
			Step::Lowered {
				op,
				over,
				may_hide_schema: true,
			} if dependent(None, follows[at]) => {
				let hides = match over.as_deref().filter(|over| !over.is_empty()) {
					Some(over) => match crate::exec::vm::speculate::view(planner, over) {
						Some((_speculation, viewed)) => {
							crate::exec::vm::speculate::runs_unseen_schema(&viewed, op, outside)
								.await
						}
						None => true,
					},
					None => {
						crate::exec::vm::speculate::runs_unseen_schema(planner, op, outside).await
					}
				};
				if hides {
					return true;
				}
			}
			_ => {}
		}
	}
	false
}
