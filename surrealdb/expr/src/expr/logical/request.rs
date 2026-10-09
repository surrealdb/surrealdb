//! A request's statements grouped into the transactions they run in.
//!
//! [`lower_request`] reads the statements of a request in order and groups
//! them as the request driver runs them: each statement outside `BEGIN …`
//! is a transaction of its own, the statements from a `BEGIN` up to the
//! `COMMIT` or `CANCEL` that ends it are one, and a request that ends inside a
//! block leaves that block unterminated. The grouping is fixed by the text
//! alone, so it is known before any statement runs.
//!
//! Every statement keeps its position in the request, which is the index of
//! its result: `BEGIN`, `COMMIT` and `CANCEL` keep their own result slots.
//!
//! What the driver does with a misplaced transaction statement is part of the
//! grouping. A `COMMIT` or `CANCEL` outside a block is a transaction of its
//! own, which fails. A `BEGIN` inside a block does not start another: the
//! block it is in is aborted there, and the block still ends at the next
//! `COMMIT` or `CANCEL` ([`LogicalTransaction::aborted_at`]).

/// How a transaction of a request is opened and ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TxnMode {
	/// One statement, in a transaction of its own, committed when it
	/// succeeds.
	AutoCommit,
	/// The statements of a `BEGIN …` block, ended as `end` says.
	Explicit {
		end: TxnEnd,
	},
	/// Every statement of a request run inside a transaction the client
	/// holds across requests, which a later request commits or cancels.
	Session,
}

/// How an explicit transaction ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TxnEnd {
	Commit,
	Cancel,
	/// The request ends before a `COMMIT` or `CANCEL`: the block is
	/// cancelled.
	Unterminated,
}

/// When a transaction's results reach a streaming consumer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Emission {
	/// Rows are sent as they are produced, provisional until their
	/// statement's `Finished`.
	#[default]
	Stream,
	/// Rows are held and sent once the transaction has committed.
	Hold,
}

/// What one statement of a request is, as the grouping reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RequestStatementKind {
	Begin,
	Commit,
	Cancel,
	/// `OPTION`, which opens no transaction.
	Option,
	/// Any other statement, run in a transaction.
	Run,
}

/// One transaction of a request.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalTransaction {
	pub mode: TxnMode,
	/// When the transaction's results reach a streaming consumer.
	pub emission: Emission,
	/// The position in the request of its first statement: its `BEGIN`, for
	/// an explicit transaction.
	pub first_index: usize,
	/// The positions of its statements, in order, its `BEGIN` and the
	/// `COMMIT` or `CANCEL` that ends it included.
	pub statements: Vec<usize>,
	/// For an explicit transaction, the position of a `BEGIN` inside it,
	/// which aborts it there.
	pub aborted_at: Option<usize>,
}

/// Group the statements of a request, each given by its kind, into the
/// transactions they run in, in order. `session` says the request runs in a
/// transaction the client holds across requests, which every statement of it
/// shares.
pub fn lower_request(kinds: &[RequestStatementKind], session: bool) -> Vec<LogicalTransaction> {
	if session {
		return match kinds.is_empty() {
			true => Vec::new(),
			false => vec![LogicalTransaction {
				mode: TxnMode::Session,
				emission: Emission::Stream,
				first_index: 0,
				statements: (0..kinds.len()).collect(),
				aborted_at: None,
			}],
		};
	}
	let mut out = Vec::new();
	let mut block: Option<LogicalTransaction> = None;
	for (at, kind) in kinds.iter().enumerate() {
		match (&mut block, kind) {
			(Some(open), RequestStatementKind::Commit | RequestStatementKind::Cancel) => {
				open.statements.push(at);
				let end = if matches!(kind, RequestStatementKind::Commit) {
					TxnEnd::Commit
				} else {
					TxnEnd::Cancel
				};
				open.mode = TxnMode::Explicit {
					end,
				};
				if let Some(closed) = block.take() {
					out.push(closed);
				}
			}
			(Some(open), RequestStatementKind::Begin) => {
				open.statements.push(at);
				open.aborted_at.get_or_insert(at);
			}
			(Some(open), RequestStatementKind::Option | RequestStatementKind::Run) => {
				open.statements.push(at);
			}
			(None, RequestStatementKind::Begin) => {
				block = Some(LogicalTransaction {
					mode: TxnMode::Explicit {
						end: TxnEnd::Unterminated,
					},
					emission: Emission::Stream,
					first_index: at,
					statements: vec![at],
					aborted_at: None,
				});
			}
			(
				None,
				RequestStatementKind::Commit
				| RequestStatementKind::Cancel
				| RequestStatementKind::Option
				| RequestStatementKind::Run,
			) => out.push(LogicalTransaction {
				mode: TxnMode::AutoCommit,
				emission: Emission::Stream,
				first_index: at,
				statements: vec![at],
				aborted_at: None,
			}),
		}
	}
	out.extend(block);
	out
}

#[cfg(test)]
mod tests {
	use super::RequestStatementKind::{Begin, Cancel, Commit, Run};
	use super::*;

	fn modes(kinds: &[RequestStatementKind]) -> Vec<(TxnMode, Vec<usize>, Option<usize>)> {
		lower_request(kinds, false)
			.into_iter()
			.map(|txn| (txn.mode, txn.statements, txn.aborted_at))
			.collect()
	}

	const AUTO: TxnMode = TxnMode::AutoCommit;

	fn explicit(end: TxnEnd) -> TxnMode {
		TxnMode::Explicit {
			end,
		}
	}

	#[test]
	fn each_statement_outside_a_block_is_a_transaction_of_its_own() {
		assert_eq!(
			modes(&[Run, RequestStatementKind::Option, Run]),
			vec![(AUTO, vec![0], None), (AUTO, vec![1], None), (AUTO, vec![2], None)]
		);
	}

	#[test]
	fn a_block_runs_from_its_begin_to_the_statement_that_ends_it() {
		assert_eq!(
			modes(&[Run, Begin, Run, Run, Commit, Run]),
			vec![
				(AUTO, vec![0], None),
				(explicit(TxnEnd::Commit), vec![1, 2, 3, 4], None),
				(AUTO, vec![5], None),
			]
		);
		assert_eq!(
			modes(&[Begin, Run, Cancel]),
			vec![(explicit(TxnEnd::Cancel), vec![0, 1, 2], None)]
		);
		assert_eq!(modes(&[Begin, Run]), vec![(explicit(TxnEnd::Unterminated), vec![0, 1], None)]);
	}

	#[test]
	fn a_misplaced_transaction_statement_keeps_its_place() {
		// A COMMIT or CANCEL outside a block is a transaction of its own.
		assert_eq!(
			modes(&[Commit, Cancel, Run]),
			vec![(AUTO, vec![0], None), (AUTO, vec![1], None), (AUTO, vec![2], None)]
		);
		// A BEGIN inside a block aborts it, and the block still ends at its
		// COMMIT.
		assert_eq!(
			modes(&[Begin, Run, Begin, Run, Commit, Run]),
			vec![(explicit(TxnEnd::Commit), vec![0, 1, 2, 3, 4], Some(2)), (AUTO, vec![5], None)]
		);
	}

	#[test]
	fn a_session_transaction_holds_every_statement() {
		let txns = lower_request(&[Run, Begin, Run], true);
		assert_eq!(txns.len(), 1);
		assert_eq!(txns[0].mode, TxnMode::Session);
		assert_eq!(txns[0].statements, vec![0, 1, 2]);
	}

	#[test]
	fn every_transaction_streams_by_default() {
		for txn in lower_request(&[Run, Begin, Run, Cancel, Begin], false) {
			assert_eq!(txn.emission, Emission::Stream);
		}
	}
}
