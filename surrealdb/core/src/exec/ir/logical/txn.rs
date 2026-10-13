//! Transaction and statement regions.
//!
//! An explicit or session transaction is one `logical.txn` region holding its statements. A
//! statement outside a transaction is a `logical.stmt` of its own, committed by itself, and is
//! never a transaction. A top-level `RETURN` is a value statement: it ends the statement that
//! holds it and neither the transaction nor the request, and the statements after it are still
//! planned.
//!
//! Every statement region carries the statement timestamp as its block argument. A call of
//! `time::now` reads that argument, one value per statement, only where the request's
//! capabilities allow the function. The binder gets no capability set, so it leaves the call as a
//! call. That call reads the clock itself and keeps the run-time capability check.
//!
//! [`regions_of`] maps the grouping of a request into transactions onto the top-level regions the
//! binder builds. The request driver makes the grouping from the statement kinds alone. Every
//! statement of a group has a region of its own, including a `BEGIN`, `COMMIT` or `CANCEL`, so
//! every statement has exactly one slot.

use super::ops::{TxnRegionEnd, TxnRegionMode, TxnStart};
use crate::exec::ir::iface::region_kind::StatementCommit;
use crate::expr::logical::{LogicalTransaction, MisplacedControl, TxnEnd, TxnMode};

/// What one statement region of a request holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StatementBody {
	/// The statement's own text: any statement that is not a transaction control, `OPTION`
	/// included.
	Run,
	/// The `BEGIN` that opens the transaction. Its body yields `NONE`.
	Begin,
	/// The `COMMIT` that ends the transaction. Its body yields `NONE`.
	Commit,
	/// The `CANCEL` that ends the transaction. Its body yields `NONE`.
	Cancel,
	/// A transaction control that cannot act where it stands: its body raises the error the
	/// control names.
	Misplaced(MisplacedControl),
}

/// One statement region: the statement's position in the request, how it is committed and what
/// its body holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StatementSlot {
	/// The position of the statement in the request, counting every statement.
	pub(crate) index: usize,
	pub(crate) commit: StatementCommit,
	pub(crate) body: StatementBody,
}

/// One top-level region of a request, in request order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TopRegion {
	/// A statement outside any transaction: a `logical.stmt` of its own.
	Statement(StatementSlot),
	/// A `logical.txn` of `mode` holding `statements` in order, each committed with the
	/// transaction.
	Transaction {
		mode: TxnRegionMode,
		statements: Vec<StatementSlot>,
	},
}

/// The top-level regions of the request whose grouping is `groups`, in order.
///
/// An auto-commit group is one statement region with no transaction. An explicit group is one
/// transaction holding a statement region per statement, the first being its `BEGIN` and, when
/// the group ends in `COMMIT` or `CANCEL`, the last being that control. A statement the grouping
/// marks misplaced holds the error of its mark, wherever it stands.
///
/// `fragment` is the index of the fragment in its stream, `None` for a whole request. A
/// transaction with no terminator ends a whole request `Unterminated`; in a fragment it stays
/// `Open` for the next one. A transaction that a previous fragment began cannot be told from the
/// grouping, which sees the statements of this fragment only, so none is `Continued`.
///
/// `session` names the transaction a client holds across requests, when the request runs in one;
/// a group of the session mode is that transaction. The grouping must have been made with the
/// same `session`. A session group with no id names transaction 0.
pub(crate) fn regions_of(
	groups: &[LogicalTransaction],
	fragment: Option<u32>,
	session: Option<u32>,
) -> Vec<TopRegion> {
	let mut out = Vec::with_capacity(groups.len());
	for group in groups {
		match group.mode {
			TxnMode::AutoCommit => {
				out.extend(
					statements_of(group, StatementCommit::Auto)
						.into_iter()
						.map(TopRegion::Statement),
				);
			}
			TxnMode::Explicit {
				end,
			} => out.push(TopRegion::Transaction {
				mode: TxnRegionMode::Explicit {
					start: TxnStart::Begin,
					end: region_end(end, fragment),
					emission: group.emission,
				},
				statements: statements_of(group, StatementCommit::InTxn),
			}),
			TxnMode::Session => out.push(TopRegion::Transaction {
				mode: TxnRegionMode::Session {
					id: session.unwrap_or(0),
				},
				statements: statements_of(group, StatementCommit::InTxn),
			}),
		}
	}
	out
}

/// How an explicit transaction ends in a whole request or in a fragment of a stream.
fn region_end(end: TxnEnd, fragment: Option<u32>) -> TxnRegionEnd {
	match end {
		TxnEnd::Commit => TxnRegionEnd::Commit,
		TxnEnd::Cancel => TxnRegionEnd::Cancel,
		TxnEnd::Unterminated => match fragment {
			None => TxnRegionEnd::Unterminated,
			Some(_) => TxnRegionEnd::Open,
		},
	}
}

/// The statement regions of `group`, one per statement, committed as `commit`.
fn statements_of(group: &LogicalTransaction, commit: StatementCommit) -> Vec<StatementSlot> {
	let last = group.statements.last().copied();
	// Both lists are in request order, so one cursor over the marks serves every statement.
	let mut marks = group.misplaced.iter().peekable();
	let mut out = Vec::with_capacity(group.statements.len());
	for index in group.statements.iter().copied() {
		while marks.next_if(|(at, _)| *at < index).is_some() {}
		let body = if let Some((_, mark)) = marks.next_if(|(at, _)| *at == index) {
			StatementBody::Misplaced(*mark)
		} else {
			match group.mode {
				TxnMode::Explicit {
					end,
				} => {
					if index == group.first_index {
						StatementBody::Begin
					} else if Some(index) == last {
						match end {
							TxnEnd::Commit => StatementBody::Commit,
							TxnEnd::Cancel => StatementBody::Cancel,
							TxnEnd::Unterminated => StatementBody::Run,
						}
					} else {
						StatementBody::Run
					}
				}
				TxnMode::AutoCommit | TxnMode::Session => StatementBody::Run,
			}
		};
		out.push(StatementSlot {
			index,
			commit,
			body,
		});
	}
	out
}
