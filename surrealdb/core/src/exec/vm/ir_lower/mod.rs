//! Lowering from the IR's physical dialect to the VM's programs.
//!
//! Lowering plans nothing. Every decision it carries out (placement, region shape, the method of
//! each operation) is already an attribute of the physical operation it reads, and it never
//! chooses between alternatives. It only allocates registers, labels, pool indices, cell numbers
//! and site numbers. It is total over the physical dialect: every operation either lowers to
//! instructions or returns a typed refusal. No operation is skipped silently.
//!
//! # The direct entry
//!
//! [`lower`] is exhaustive. Every `PhysTag` and `ExprTag` has its own arm. Each arm either lowers
//! natively, or is a tag whose [`Coverage`] is `NotYet` and whose refusal names `ir_lower::lower`
//! and the operation's mnemonic, or (for an operation only its parent lowers) returns an invariant
//! break. There is no catch-all arm. A container that is not past the physical boundary is refused
//! the same way.
//!
//! The operations that lower natively are: the `expr` operand subset; `phys.eval` of such a value
//! (one scalar pipeline); `phys.info` (one scalar pipeline of store instructions, or a group of one
//! such pipeline in a block); `phys.let` of a session parameter (one scalar pipeline of its value);
//! `phys.yield`; the `phys.loop`s that stand in value position (an element filter and a broadcast),
//! which lower where an expression reads their result; and the read pipelines of [`pipe`]: the
//! source ops of [`read`], the streaming ops `phys.enforced`, `phys.filter`, `phys.limit`,
//! `phys.distinct` and `phys.project`, and the `phys.only` and `phys.version_scope` that wrap the
//! statement's rows as its tail.
//!
//! The control flow of a statement (`phys.block`, `phys.if`, a `FOR` `phys.loop`, `phys.sleep`,
//! `phys.let` of a local name, `phys.return`, `phys.break`, `phys.next` and `expr.throw`) lowers as
//! one statement program ([`block`]). A schema statement (`phys.ddl_guard` and the `phys.define`,
//! `phys.remove`, `phys.alter` or `phys.rebuild` it ends in) is one program of [`ddl`] at the top
//! of a container, and a group of one pipeline in a block. `phys.prefetch` is
//! [`Coverage::ViaLoop`]: it lowers as part of the `phys.loop` arm, so a prefetch reached outside a
//! lowered loop is `NotYet`.
//!
//! A `phys.write` is one statement program too ([`write_stmt`]): its source ([`write_source`]), its
//! body of stages ([`stage`]), and the clauses its run reads ([`write_place`]). The ops only a
//! write lowers are [`Coverage::ViaWrite`]: a row of an `INSERT` (`expr.tuple_row`), the stage ops
//! of its subject loop, and the ops of the clauses a stage hosts. Reaching one anywhere else is an
//! invariant break.
//!
//! A container whose statement is a `phys.match` is a GQL `MATCH`. [`matching`] lowers its region
//! as the pipelines of one program: a binding step that wraps each row streams in a pipeline's
//! body, and any other step is a split between two pipelines.
//!
//! A container whose top region holds `phys.pipeline` ops is a read statement, and
//! [`lower_indexed`] lowers it. A source opens an index through the reference an [`IndexResolver`]
//! returns, because an op names an index and the scan opens it by the reference planning resolved.
//! [`lower`] is [`lower_indexed`] with a resolver that knows no index.
//!
//! A request is not one program. The container that holds its statements and the transactions that
//! group them is [`Coverage::ViaRequest`]. [`request_steps`] reads the steps its driver takes from
//! it, and `lower` builds the program of one statement. Every statement, `OPTION`, `USE` and a
//! session `LET` among them, has a program of its own. The three that change the request's session
//! record what the executor applies to it when the program ends.
//!
//! `coverage_report` walks every operation of a container without stopping, and reports each one
//! whose [`Coverage`] is not `Native`. It measures what the direct entry still declines. `lower`
//! stops at the first refusal, so it cannot serve that purpose.
//!
//! This module builds a [`Program`] with a struct literal from the physical dialect alone. It
//! takes no plan type of the planner and calls no other lowering.
//!
//! # What lowering keeps
//!
//! - A literal or a constant may become a `Program::consts` entry. A bind parameter and a `LET`
//!   value are slots, read by `LoadParam` and `LoadSlot`. `expr.now` is evaluated per execution and
//!   is never a constant: it lowers as a call to the clock builtin, which reads the statement's
//!   instant from the context.
//! - No `static`, `OnceLock`, `thread_local` or lazy value holds lowered output or anything derived
//!   from a catalog.
//! - No emitted instruction compiles code, except the unit loads the VM already emits: a
//!   `LoadParam` with no guard loads the parameter's unit by name.
//! - Placement is copied from the physical plan. A pipeline the plan did not place is an internal
//!   error.
//!
//! [`Program`]: super::program::Program

mod access;
mod batch_read;
mod block;
mod closure;
mod control;
mod ddl;
mod dispatch;
mod emit;
mod events;
mod explain;
mod explain_hook;
mod expr;
mod fold;
mod graph;
mod info;
mod kernel;
mod matching;
mod memo;
mod nested;
mod path;
mod pipe;
mod place;
mod read;
pub(crate) mod reg;
mod schema;
mod select;
mod stage;
mod subscription;
#[cfg(test)]
mod tests;
mod units;
mod write;
mod write_place;
mod write_source;
mod write_stmt;

// The crate-visible entry points, re-exported for callers outside `ir_lower`.
#[allow(unused_imports)]
pub(crate) use control::{Slot, UseOperand, UseOperands, request_slots, request_steps};
#[allow(unused_imports)]
pub(crate) use dispatch::{Coverage, coverage_report};
#[cfg(test)]
pub(crate) use dispatch::{lower, lower_indexed};
pub(crate) use explain_hook::{ExplainLowering, FULL_EXPLAIN_LOWERING};
#[allow(unused_imports)]
pub(crate) use expr::LowerLimits;
#[allow(unused_imports)]
pub(crate) use fold::{IrExprLowering, lower_expr_parts};
#[allow(unused_imports)]
pub(crate) use place::{Lowered, Provenance};
#[allow(unused_imports)]
pub(crate) use read::{IndexResolver, NoIndexes};
pub(crate) use schema::{FULL_SCHEMA_LOWERING, NOT_SERVED, SchemaLowering};
#[allow(unused_imports)]
pub(crate) use subscription::LoweredLive;
#[cfg(test)]
pub(crate) use units::empty_lowered;
#[allow(unused_imports)]
pub(crate) use units::{
	CascadeKind, ClosureTemplate, ContainerId, LowerCx, Mark, Needed, UnitRequest, lower_cascade,
	lower_closure_body, lower_program, lower_request_statement, lower_serving, lower_subscription,
	lower_unit, request_program,
};
#[allow(unused_imports)]
pub(in crate::exec::vm) use write::{
	WriteCutEmit, emit_cut, emit_off_path, emit_resume, emit_skip, guard_writing_morsels,
};
pub(crate) use write_stmt::linked_stage_sets;
