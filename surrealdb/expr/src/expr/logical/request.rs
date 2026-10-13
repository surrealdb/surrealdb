//! A request's statements grouped into the transactions they run in.
//!
//! [`lower_request`] reads the statements of a request in order and groups
//! them as the request driver runs them: each statement outside `BEGIN …`
//! is a transaction of its own, the statements from a `BEGIN` up to the
//! `COMMIT` or `CANCEL` that ends it are one, and a request that ends inside a
//! block leaves that block unterminated. The grouping is fixed by the text
//! alone, so it is known before any statement runs.
//!
//! Every statement keeps its position in the request, and every statement
//! gets one response slot, except the successful results an import suppresses
//! (`skip_success_results`). A position counts the statements the grouper has
//! been given; it is not a slot index, so a driver keeps counting its own
//! slots.
//!
//! A transaction statement that cannot do what it says is marked, not
//! rejected. A `COMMIT` or `CANCEL` outside a block is a transaction of its
//! own, marked [`MisplacedControl::CommitOutsideBlock`] or
//! [`MisplacedControl::CancelOutsideBlock`]. A `BEGIN` inside a block does not
//! start another: the block it is in is aborted there, and the block still ends
//! at the next `COMMIT` or `CANCEL` ([`LogicalTransaction::aborted_at`]); it is
//! marked [`MisplacedControl::NestedBegin`]. In a session transaction, which
//! the client already holds, every `BEGIN`, `COMMIT` and `CANCEL` is marked.
//! Marks are recorded in [`LogicalTransaction::misplaced`] and handed out per
//! statement as [`StatementRole::Misplaced`]. They say where and which, never
//! how: the driver raises each error when it reaches the statement, after the
//! earlier statements have run, and decides how a block ends.
//!
//! [`RequestGrouper`] places one statement at a time, for a driver that pulls
//! statements from a stream it cannot read ahead of time. [`lower_request`] is
//! a fold over it, so the two agree on every request.
//!
//! The emission of an explicit transaction is fixed when its `BEGIN` is placed,
//! because a streaming driver cannot read ahead to learn how the block ends.
//! Auto-commit and session transactions always stream.
//!
//! A driver that reads its input as fragments of one stream groups each
//! fragment with [`RequestGrouper::finish_fragment`], which returns a block
//! still open as an [`OpenBlock`] instead of ending it, and continues it in the
//! next fragment with [`RequestGrouper::resume`]. A fragment never knows it is
//! the last: the driver ends an open block as unterminated when the stream
//! ends. A resumed grouper numbers its statements on from the fragment that
//! left the block open; a grouper that continues no block numbers from 0, and
//! the driver keeps the count across fragments of one stream.

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

/// Why a transaction-control statement cannot do what it says.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MisplacedControl {
	/// A `BEGIN` inside an explicit block, which aborts the block.
	NestedBegin,
	/// A `BEGIN` in a request that runs in a session transaction.
	BeginInSession,
	/// A `COMMIT` with no block to commit.
	CommitOutsideBlock,
	/// A `CANCEL` with no block to cancel.
	CancelOutsideBlock,
}

impl MisplacedControl {
	/// The message only; the caller picks the error type.
	pub fn message(self) -> &'static str {
		match self {
			MisplacedControl::NestedBegin => {
				"Tried to start a transaction while another transaction was open"
			}
			MisplacedControl::BeginInSession => "Cannot BEGIN a transaction within a transaction",
			MisplacedControl::CommitOutsideBlock => "Cannot COMMIT without starting a transaction",
			MisplacedControl::CancelOutsideBlock => "Cannot CANCEL without starting a transaction",
		}
	}
}

/// What the grouping makes of one statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StatementRole {
	/// A statement an engine runs.
	Run,
	/// `OPTION`.
	Option,
	/// A `BEGIN` that opens a block.
	Begin,
	/// A `COMMIT` that ends the open block. Produced only while a block is
	/// open.
	Commit,
	/// A `CANCEL` that ends the open block. Produced only while a block is
	/// open.
	Cancel,
	/// A transaction-control statement that cannot act where it stands.
	Misplaced(MisplacedControl),
}

/// Where one statement of a request sits in the grouping.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StatementPlace {
	/// The statement's position in the request, counting every statement the
	/// grouper has been given. It is not the statement's response slot.
	pub index: usize,
	/// The position, in the output of [`lower_request`] or in the
	/// `transactions` of the fragment's [`FragmentGrouping`], of the transaction
	/// that holds the statement.
	pub transaction: usize,
	/// What the grouping makes of the statement.
	pub role: StatementRole,
	/// When the results of the transaction that holds the statement reach a
	/// streaming consumer.
	pub emission: Emission,
}

/// One transaction of a request.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalTransaction {
	pub mode: TxnMode,
	/// When the transaction's results reach a streaming consumer.
	pub emission: Emission,
	/// The position in the request of its first statement: its `BEGIN`, for
	/// an explicit transaction that starts with [`BlockStart::Begin`].
	pub first_index: usize,
	/// The positions of its statements, in order, its `BEGIN` and the
	/// `COMMIT` or `CANCEL` that ends it included.
	pub statements: Vec<usize>,
	/// For an explicit transaction, the position of the first `BEGIN` inside
	/// it, which aborts it there. A continued block records only the `BEGIN`s
	/// of its own fragment.
	pub aborted_at: Option<usize>,
	/// The positions of its misplaced transaction-control statements, in
	/// order, with why each is misplaced.
	pub misplaced: Vec<(usize, MisplacedControl)>,
	/// How the transaction opens in its fragment. Read through
	/// [`LogicalTransaction::start`]; the private field keeps the struct
	/// constructible only by the grouper.
	start: BlockStart,
}

impl LogicalTransaction {
	fn new(mode: TxnMode, first_index: usize) -> Self {
		LogicalTransaction {
			mode,
			emission: Emission::Stream,
			first_index,
			statements: vec![first_index],
			aborted_at: None,
			misplaced: Vec::new(),
			start: BlockStart::Begin,
		}
	}

	/// How the transaction opens: [`BlockStart::Begin`] when its first
	/// statement is the `BEGIN` of an explicit block, an auto-commit statement
	/// or the first statement of a session transaction;
	/// [`BlockStart::Continued`] when it continues a block that an earlier
	/// fragment left open, in which case [`LogicalTransaction::first_index`] and
	/// [`LogicalTransaction::statements`] cover only this fragment's part of
	/// the block.
	pub fn start(&self) -> BlockStart {
		self.start
	}
}

/// How an explicit block of a fragment opens. Maps one to one to the IR's
/// `TxnStart`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BlockStart {
	/// The block's own `BEGIN` is in this fragment.
	Begin,
	/// The block was opened by an earlier fragment of the same stream.
	Continued,
}

/// An explicit block a fragment left open: it continues in the next fragment of
/// the same stream.
///
/// Only [`RequestGrouper::finish_fragment`] makes one, and only
/// [`RequestGrouper::resume`] consumes it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OpenBlock {
	/// The block's part in the fragment that left it open. Its mode is
	/// `Explicit { end: Unterminated }`; that the block is open, not
	/// unterminated, is carried by this type.
	block: LogicalTransaction,
	/// The position of the next statement of the stream.
	next: usize,
}

impl OpenBlock {
	/// The block's part in the fragment that left it open.
	pub fn transaction(&self) -> &LogicalTransaction {
		&self.block
	}
}

/// The grouping of one fragment: its closed transactions, and the block it
/// leaves open.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FragmentGrouping {
	/// The fragment's transactions in order, none of them open. A block that
	/// continued from an earlier fragment and ended here is among them with
	/// [`BlockStart::Continued`].
	pub transactions: Vec<LogicalTransaction>,
	/// The explicit block the fragment ends inside, if any. It is never in
	/// `transactions`.
	pub open: Option<OpenBlock>,
}

/// The role of a statement that is not transaction control: `OPTION` or a
/// statement an engine runs. Transaction-control kinds are given their roles
/// by the grouper, which knows the block they stand in.
fn plain_role(kind: RequestStatementKind) -> StatementRole {
	match kind {
		RequestStatementKind::Option => StatementRole::Option,
		RequestStatementKind::Run
		| RequestStatementKind::Begin
		| RequestStatementKind::Commit
		| RequestStatementKind::Cancel => StatementRole::Run,
	}
}

/// Groups the statements of a request into transactions one statement at a
/// time, in the order the driver reads them.
///
/// Every statement the driver reads must be given to [`RequestGrouper::place`]
/// exactly once and in order, or the grouper's positions fall out of step with
/// the request. The grouping depends on the kinds of the statements and where
/// they stand, never on how any statement runs.
#[derive(Debug)]
pub struct RequestGrouper {
	session: bool,
	explicit_emission: Emission,
	next: usize,
	out: Vec<LogicalTransaction>,
	/// The explicit block that is open, which is pushed to `out` when it ends.
	block: Option<LogicalTransaction>,
	/// The one transaction of a session request.
	held: Option<LogicalTransaction>,
}

impl RequestGrouper {
	/// `session` says the request runs in a transaction the client holds
	/// across requests, which every statement of it shares.
	pub fn new(session: bool) -> Self {
		RequestGrouper {
			session,
			explicit_emission: Emission::Stream,
			next: 0,
			out: Vec::new(),
			block: None,
			held: None,
		}
	}

	/// A grouper for the next fragment of a stream whose previous fragment left
	/// `open`. Its first `COMMIT` or `CANCEL` ends that block, the statements it
	/// is given are numbered from where the previous fragment stopped, and the
	/// block's transaction has [`BlockStart::Continued`]. Explicit blocks opened
	/// later in this fragment take the emission of `open`.
	///
	/// `session` and `open` come from the same stream. A session request holds
	/// no explicit block, so a session grouper keeps only the numbering of
	/// `open` and never continues it.
	pub fn resume(session: bool, open: OpenBlock) -> Self {
		let OpenBlock {
			block,
			next,
		} = open;
		let mut grouper = RequestGrouper::new(session).with_explicit_emission(block.emission);
		grouper.next = next;
		if !session {
			grouper.block = Some(LogicalTransaction {
				statements: Vec::new(),
				first_index: next,
				aborted_at: None,
				misplaced: Vec::new(),
				start: BlockStart::Continued,
				..block
			});
		}
		grouper
	}

	/// The emission given to every explicit transaction this grouper opens.
	/// Auto-commit and session transactions always stream.
	pub fn with_explicit_emission(mut self, emission: Emission) -> Self {
		self.explicit_emission = emission;
		self
	}

	/// Place the next statement of the request.
	pub fn place(&mut self, kind: RequestStatementKind) -> StatementPlace {
		let index = self.next;
		self.next += 1;
		if self.session {
			let role = match kind {
				RequestStatementKind::Begin => {
					StatementRole::Misplaced(MisplacedControl::BeginInSession)
				}
				RequestStatementKind::Commit => {
					StatementRole::Misplaced(MisplacedControl::CommitOutsideBlock)
				}
				RequestStatementKind::Cancel => {
					StatementRole::Misplaced(MisplacedControl::CancelOutsideBlock)
				}
				RequestStatementKind::Option => StatementRole::Option,
				RequestStatementKind::Run => StatementRole::Run,
			};
			let held = match &mut self.held {
				Some(held) => {
					held.statements.push(index);
					held
				}
				none => none.insert(LogicalTransaction::new(TxnMode::Session, index)),
			};
			if let StatementRole::Misplaced(m) = role {
				held.misplaced.push((index, m));
			}
			return StatementPlace {
				index,
				transaction: 0,
				role,
				emission: Emission::Stream,
			};
		}
		match (&mut self.block, kind) {
			(Some(open), RequestStatementKind::Commit | RequestStatementKind::Cancel) => {
				open.statements.push(index);
				let (end, role) = if matches!(kind, RequestStatementKind::Commit) {
					(TxnEnd::Commit, StatementRole::Commit)
				} else {
					(TxnEnd::Cancel, StatementRole::Cancel)
				};
				open.mode = TxnMode::Explicit {
					end,
				};
				let transaction = self.out.len();
				let emission = open.emission;
				self.out.extend(self.block.take());
				StatementPlace {
					index,
					transaction,
					role,
					emission,
				}
			}
			(Some(open), RequestStatementKind::Begin) => {
				open.statements.push(index);
				open.aborted_at.get_or_insert(index);
				open.misplaced.push((index, MisplacedControl::NestedBegin));
				StatementPlace {
					index,
					transaction: self.out.len(),
					role: StatementRole::Misplaced(MisplacedControl::NestedBegin),
					emission: open.emission,
				}
			}
			(Some(open), RequestStatementKind::Option | RequestStatementKind::Run) => {
				open.statements.push(index);
				StatementPlace {
					index,
					transaction: self.out.len(),
					role: plain_role(kind),
					emission: open.emission,
				}
			}
			(None, RequestStatementKind::Begin) => {
				let mut block = LogicalTransaction::new(
					TxnMode::Explicit {
						end: TxnEnd::Unterminated,
					},
					index,
				);
				block.emission = self.explicit_emission;
				self.block = Some(block);
				StatementPlace {
					index,
					transaction: self.out.len(),
					role: StatementRole::Begin,
					emission: self.explicit_emission,
				}
			}
			(
				None,
				RequestStatementKind::Commit
				| RequestStatementKind::Cancel
				| RequestStatementKind::Option
				| RequestStatementKind::Run,
			) => {
				let mut txn = LogicalTransaction::new(TxnMode::AutoCommit, index);
				let role = match kind {
					RequestStatementKind::Commit => {
						StatementRole::Misplaced(MisplacedControl::CommitOutsideBlock)
					}
					RequestStatementKind::Cancel => {
						StatementRole::Misplaced(MisplacedControl::CancelOutsideBlock)
					}
					// A `BEGIN` is handled by its own arm above.
					RequestStatementKind::Option
					| RequestStatementKind::Run
					| RequestStatementKind::Begin => plain_role(kind),
				};
				if let StatementRole::Misplaced(m) = role {
					txn.misplaced.push((index, m));
				}
				let transaction = self.out.len();
				self.out.push(txn);
				StatementPlace {
					index,
					transaction,
					role,
					emission: Emission::Stream,
				}
			}
		}
	}

	/// The transactions of the statements placed so far, in order. A block
	/// still open ends unterminated; an empty session request has none.
	pub fn finish(mut self) -> Vec<LogicalTransaction> {
		self.out.extend(self.block.take());
		self.out.extend(self.held.take());
		self.out
	}

	/// End a fragment of fragmented input. The transactions of the statements
	/// placed so far are `transactions`; a block still open is `open`, never an
	/// unterminated transaction, so the next fragment can continue it with
	/// [`RequestGrouper::resume`]. A fragment never knows it is the last: the
	/// driver ends an open block as unterminated when the stream ends. A session
	/// grouper never returns an open block.
	pub fn finish_fragment(mut self) -> FragmentGrouping {
		let open = self.block.take().map(|block| OpenBlock {
			block,
			next: self.next,
		});
		self.out.extend(self.held.take());
		FragmentGrouping {
			transactions: self.out,
			open,
		}
	}
}

/// Group the statements of a request, each given by its kind, into the
/// transactions they run in, in order. `session` says the request runs in a
/// transaction the client holds across requests, which every statement of it
/// shares.
pub fn lower_request(kinds: &[RequestStatementKind], session: bool) -> Vec<LogicalTransaction> {
	lower_request_with(kinds, session, Emission::Stream)
}

/// [`lower_request`], giving every explicit transaction `explicit` as its
/// emission. Auto-commit and session transactions always stream.
pub fn lower_request_with(
	kinds: &[RequestStatementKind],
	session: bool,
	explicit: Emission,
) -> Vec<LogicalTransaction> {
	let mut grouper = RequestGrouper::new(session).with_explicit_emission(explicit);
	for kind in kinds {
		grouper.place(*kind);
	}
	grouper.finish()
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

	#[test]
	fn an_explicit_transaction_holds_when_asked() {
		let kinds = [Run, Begin, Run, Commit, Begin, Run, Cancel, Begin, Run];
		let txns = lower_request_with(&kinds, false, Emission::Hold);
		assert_eq!(
			txns.iter().map(|txn| txn.emission).collect::<Vec<_>>(),
			vec![Emission::Stream, Emission::Hold, Emission::Hold, Emission::Hold]
		);
		assert_eq!(txns[0].mode, TxnMode::AutoCommit);
		assert_eq!(txns[3].mode, explicit(TxnEnd::Unterminated));
	}

	#[test]
	fn auto_commit_and_session_transactions_always_stream() {
		for txn in lower_request_with(&[Run, Commit, Run], false, Emission::Hold) {
			assert_eq!(txn.emission, Emission::Stream);
		}
		let txns = lower_request_with(&[Run, Begin, Run, Commit], true, Emission::Hold);
		assert_eq!(txns.len(), 1);
		assert_eq!(txns[0].mode, TxnMode::Session);
		assert_eq!(txns[0].emission, Emission::Stream);
	}

	#[test]
	fn a_place_carries_its_transactions_emission() {
		let mut grouper = RequestGrouper::new(false).with_explicit_emission(Emission::Hold);
		let emissions: Vec<_> = [Run, Begin, Run, Begin, Commit, Commit]
			.iter()
			.map(|kind| grouper.place(*kind).emission)
			.collect();
		assert_eq!(
			emissions,
			vec![
				Emission::Stream,
				Emission::Hold,
				Emission::Hold,
				Emission::Hold,
				Emission::Hold,
				Emission::Stream
			]
		);
	}

	fn roles(kinds: &[RequestStatementKind], session: bool) -> Vec<StatementRole> {
		let mut grouper = RequestGrouper::new(session);
		kinds.iter().map(|kind| grouper.place(*kind).role).collect()
	}

	const NESTED: StatementRole = StatementRole::Misplaced(MisplacedControl::NestedBegin);
	const STRAY_COMMIT: StatementRole =
		StatementRole::Misplaced(MisplacedControl::CommitOutsideBlock);
	const STRAY_CANCEL: StatementRole =
		StatementRole::Misplaced(MisplacedControl::CancelOutsideBlock);

	#[test]
	fn a_statement_outside_a_block_has_its_own_role() {
		assert_eq!(
			roles(&[Run, RequestStatementKind::Option, Commit, Cancel], false),
			vec![StatementRole::Run, StatementRole::Option, STRAY_COMMIT, STRAY_CANCEL]
		);
	}

	#[test]
	fn a_block_is_opened_and_ended_by_its_own_statements() {
		assert_eq!(
			roles(&[Begin, Run, RequestStatementKind::Option, Commit], false),
			vec![
				StatementRole::Begin,
				StatementRole::Run,
				StatementRole::Option,
				StatementRole::Commit
			]
		);
		assert_eq!(
			roles(&[Begin, Cancel, Begin, Commit], false),
			vec![
				StatementRole::Begin,
				StatementRole::Cancel,
				StatementRole::Begin,
				StatementRole::Commit
			]
		);
	}

	#[test]
	fn a_begin_inside_a_block_is_nested() {
		assert_eq!(
			roles(&[Begin, Begin, Commit], false),
			vec![StatementRole::Begin, NESTED, StatementRole::Commit]
		);
	}

	#[test]
	fn a_session_marks_every_transaction_statement() {
		assert_eq!(
			roles(&[Begin, Run, Commit, Cancel, RequestStatementKind::Option], true),
			vec![
				StatementRole::Misplaced(MisplacedControl::BeginInSession),
				StatementRole::Run,
				STRAY_COMMIT,
				STRAY_CANCEL,
				StatementRole::Option,
			]
		);
		let mut grouper = RequestGrouper::new(true);
		for kind in [Run, Begin, Commit, Cancel] {
			grouper.place(kind);
		}
		let txns = grouper.finish();
		assert_eq!(txns.len(), 1);
		assert_eq!(txns[0].mode, TxnMode::Session);
		assert_eq!(txns[0].aborted_at, None);
		assert_eq!(
			txns[0].misplaced,
			vec![
				(1, MisplacedControl::BeginInSession),
				(2, MisplacedControl::CommitOutsideBlock),
				(3, MisplacedControl::CancelOutsideBlock),
			]
		);
	}

	#[test]
	fn an_empty_session_request_has_no_transaction() {
		assert!(RequestGrouper::new(true).finish().is_empty());
		assert!(RequestGrouper::new(false).finish().is_empty());
	}

	#[test]
	fn misplaced_statements_are_recorded_on_their_transaction() {
		let txns = lower_request(&[Commit, Begin, Begin, Run, Commit, Cancel], false);
		assert_eq!(txns.len(), 3);
		assert_eq!(txns[0].misplaced, vec![(0, MisplacedControl::CommitOutsideBlock)]);
		assert_eq!(txns[1].misplaced, vec![(2, MisplacedControl::NestedBegin)]);
		assert_eq!(txns[2].misplaced, vec![(5, MisplacedControl::CancelOutsideBlock)]);
	}

	#[test]
	fn a_second_nested_begin_is_marked_while_the_abort_keeps_the_first() {
		let txns = lower_request(&[Begin, Begin, Run, Begin, Commit], false);
		assert_eq!(txns.len(), 1);
		assert_eq!(txns[0].aborted_at, Some(1));
		assert_eq!(
			txns[0].misplaced,
			vec![(1, MisplacedControl::NestedBegin), (3, MisplacedControl::NestedBegin)]
		);
	}

	#[test]
	fn places_are_numbered_without_gaps_and_point_at_their_transaction() {
		let kinds = [Run, Begin, Run, Begin, Commit, Commit, Begin, Run];
		let mut grouper = RequestGrouper::new(false);
		let places: Vec<_> = kinds.iter().map(|kind| grouper.place(*kind)).collect();
		let txns = grouper.finish();
		assert_eq!(
			places.iter().map(|p| p.index).collect::<Vec<_>>(),
			(0..kinds.len()).collect::<Vec<_>>()
		);
		for place in &places {
			assert!(txns[place.transaction].statements.contains(&place.index));
		}
		assert_eq!(
			places.iter().map(|p| p.transaction).collect::<Vec<_>>(),
			vec![0, 1, 1, 1, 1, 2, 3, 3]
		);
	}

	#[test]
	fn lowering_is_the_fold_of_the_grouper() {
		let sequences: [&[RequestStatementKind]; 5] = [
			&[],
			&[Run, Begin, Run, Commit, Cancel],
			&[Begin, Begin, Run, Begin],
			&[Commit, Begin, Cancel, Begin, Run, Commit],
			&[RequestStatementKind::Option, Begin, RequestStatementKind::Option, Cancel],
		];
		for kinds in sequences {
			for session in [false, true] {
				let mut grouper = RequestGrouper::new(session);
				for kind in kinds {
					grouper.place(*kind);
				}
				assert_eq!(lower_request(kinds, session), grouper.finish());
			}
		}
	}

	/// One fragment cut of `kinds` after each position in `cuts`, grouped in
	/// order. Returns every statement's `(position, role, emission)`, the
	/// transactions with the parts of a spanning block merged, and whether any
	/// block spanned a cut. A grouper that continues no block numbers from 0, so
	/// its positions are shifted by the statements the earlier fragments held. A
	/// resumed grouper numbers on from the grouper it continues, so it keeps that
	/// grouper's shift.
	fn fold_fragments(
		kinds: &[RequestStatementKind],
		cuts: &[usize],
	) -> (Vec<(usize, StatementRole, Emission)>, Vec<LogicalTransaction>, bool) {
		let mut bounds = vec![0];
		bounds.extend_from_slice(cuts);
		bounds.push(kinds.len());
		let mut places = Vec::new();
		let mut merged: Vec<LogicalTransaction> = Vec::new();
		let mut spanned = false;
		let mut open: Option<OpenBlock> = None;
		let mut base = 0;
		for window in bounds.windows(2) {
			if open.is_none() {
				base = window[0];
			}
			let mut grouper = match open.take() {
				Some(block) => RequestGrouper::resume(false, block),
				None => RequestGrouper::new(false),
			};
			for kind in &kinds[window[0]..window[1]] {
				let place = grouper.place(*kind);
				places.push((place.index + base, place.role, place.emission));
			}
			let FragmentGrouping {
				transactions,
				open: left,
			} = grouper.finish_fragment();
			let pieces =
				transactions.into_iter().chain(left.as_ref().map(|o| o.transaction().clone()));
			for mut piece in pieces {
				piece.first_index += base;
				piece.statements.iter_mut().for_each(|i| *i += base);
				piece.aborted_at = piece.aborted_at.map(|i| i + base);
				piece.misplaced.iter_mut().for_each(|(i, _)| *i += base);
				match (piece.start(), merged.last_mut()) {
					(BlockStart::Continued, Some(head)) => {
						spanned = true;
						head.mode = piece.mode;
						head.statements.extend(piece.statements);
						head.misplaced.extend(piece.misplaced);
						head.aborted_at = head.aborted_at.or(piece.aborted_at);
					}
					(BlockStart::Continued, None) => {
						panic!("a continued block with no block to continue")
					}
					(BlockStart::Begin, _) => merged.push(piece),
				}
			}
			open = left;
		}
		(places, merged, spanned)
	}

	fn whole_places(kinds: &[RequestStatementKind]) -> Vec<(usize, StatementRole, Emission)> {
		let mut grouper = RequestGrouper::new(false);
		kinds
			.iter()
			.map(|kind| {
				let place = grouper.place(*kind);
				(place.index, place.role, place.emission)
			})
			.collect()
	}

	#[test]
	fn a_fragment_ending_inside_a_block_returns_it_open() {
		let mut grouper = RequestGrouper::new(false);
		for kind in [Run, Begin, Run] {
			grouper.place(kind);
		}
		let fragment = grouper.finish_fragment();
		assert_eq!(fragment.transactions.len(), 1);
		assert_eq!(fragment.transactions[0].mode, AUTO);
		let open = fragment.open.expect("the block is still open");
		assert_eq!(open.transaction().statements, vec![1, 2]);
		assert_eq!(open.transaction().start(), BlockStart::Begin);
		assert_eq!(open.transaction().mode, explicit(TxnEnd::Unterminated));
		// A fragment that ends between transactions leaves nothing open.
		let mut grouper = RequestGrouper::new(false);
		for kind in [Begin, Commit, Run] {
			grouper.place(kind);
		}
		let fragment = grouper.finish_fragment();
		assert_eq!(fragment.transactions.len(), 2);
		assert!(fragment.open.is_none());
	}

	#[test]
	fn a_resumed_block_ends_at_the_next_terminator_and_is_continued() {
		let mut first = RequestGrouper::new(false);
		for kind in [Begin, Run] {
			first.place(kind);
		}
		let open = first.finish_fragment().open.expect("open");
		let mut second = RequestGrouper::resume(false, open);
		let places: Vec<_> = [Run, Commit, Run].iter().map(|kind| second.place(*kind)).collect();
		assert_eq!(places.iter().map(|p| p.index).collect::<Vec<_>>(), vec![2, 3, 4]);
		assert_eq!(
			places.iter().map(|p| p.role).collect::<Vec<_>>(),
			vec![StatementRole::Run, StatementRole::Commit, StatementRole::Run]
		);
		assert_eq!(places.iter().map(|p| p.transaction).collect::<Vec<_>>(), vec![0, 0, 1]);
		let txns = second.finish();
		assert_eq!(txns.len(), 2);
		assert_eq!(txns[0].start(), BlockStart::Continued);
		assert_eq!(txns[0].mode, explicit(TxnEnd::Commit));
		assert_eq!(txns[0].statements, vec![2, 3]);
		assert_eq!(txns[1].start(), BlockStart::Begin);
		assert_eq!(txns[1].mode, AUTO);
	}

	#[test]
	fn a_resumed_block_without_a_terminator_is_unterminated_by_finish_and_open_by_finish_fragment()
	{
		let mut first = RequestGrouper::new(false);
		first.place(Begin);
		let open = first.finish_fragment().open.expect("open");
		let mut second = RequestGrouper::resume(false, open.clone());
		second.place(Run);
		let txns = second.finish();
		assert_eq!(txns.len(), 1);
		assert_eq!(txns[0].start(), BlockStart::Continued);
		assert_eq!(txns[0].mode, explicit(TxnEnd::Unterminated));
		let mut second = RequestGrouper::resume(false, open);
		second.place(Run);
		let again = second.finish_fragment();
		assert!(again.transactions.is_empty());
		let open = again.open.expect("still open");
		assert_eq!(open.transaction().start(), BlockStart::Continued);
		assert_eq!(open.transaction().statements, vec![1]);
		// A fragment with no statement keeps the block and the numbering.
		let mut third = RequestGrouper::resume(false, open);
		assert_eq!(third.place(Commit).index, 2);
	}

	#[test]
	fn a_resumed_grouper_keeps_the_emission_of_the_block() {
		let mut first = RequestGrouper::new(false).with_explicit_emission(Emission::Hold);
		first.place(Begin);
		let open = first.finish_fragment().open.expect("open");
		assert_eq!(open.transaction().emission, Emission::Hold);
		let mut second = RequestGrouper::resume(false, open);
		let emissions: Vec<_> =
			[Run, Commit, Begin, Run].iter().map(|kind| second.place(*kind).emission).collect();
		assert_eq!(emissions, vec![Emission::Hold; 4]);
	}

	#[test]
	fn a_nested_begin_in_a_resumed_block_is_marked() {
		let mut first = RequestGrouper::new(false);
		first.place(Begin);
		let open = first.finish_fragment().open.expect("open");
		let mut second = RequestGrouper::resume(false, open);
		assert_eq!(second.place(Begin).role, NESTED);
		let txns = second.finish();
		assert_eq!(txns[0].aborted_at, Some(1));
		assert_eq!(txns[0].misplaced, vec![(1, MisplacedControl::NestedBegin)]);
	}

	#[test]
	fn a_split_request_folds_to_the_grouping_of_the_whole_request() {
		let sequences: [&[RequestStatementKind]; 6] = [
			&[Run, Begin, Run, Commit, Run],
			&[Begin, Run, Cancel, Begin, Run, Commit],
			&[Begin, Begin, Run, Begin, Commit, Cancel],
			&[Commit, Begin, RequestStatementKind::Option, Run, Cancel, Run],
			&[Begin, Run, Run],
			&[Run, Begin, Run, Begin, Commit, Commit, Begin, Run],
		];
		for kinds in sequences {
			let whole = lower_request(kinds, false);
			let places = whole_places(kinds);
			for mask in 0u32..(1 << (kinds.len() - 1)) {
				let cuts: Vec<usize> = (0..kinds.len() - 1)
					.filter(|bit| mask >> bit & 1 == 1)
					.map(|bit| bit + 1)
					.collect();
				let (folded_places, folded, spanned) = fold_fragments(kinds, &cuts);
				assert_eq!(folded_places, places, "{kinds:?} cut at {cuts:?}");
				assert_eq!(folded, whole, "{kinds:?} cut at {cuts:?} (spanned: {spanned})");
			}
		}
	}

	#[test]
	fn a_cut_inside_a_block_continues_it_and_a_cut_between_blocks_does_not() {
		let kinds = [Run, Begin, Run, Commit, Run];
		let (_, inside, spanned) = fold_fragments(&kinds, &[3]);
		assert!(spanned);
		assert_eq!(inside, lower_request(&kinds, false));
		let (_, between, spanned) = fold_fragments(&kinds, &[1, 4]);
		assert!(!spanned);
		assert_eq!(between, lower_request(&kinds, false));
	}

	#[test]
	fn a_session_grouper_never_returns_an_open_block() {
		for kinds in [&[Begin, Run][..], &[Run, Begin, Commit, Cancel], &[Begin]] {
			let mut grouper = RequestGrouper::new(true);
			for kind in kinds {
				grouper.place(*kind);
			}
			let fragment = grouper.finish_fragment();
			assert!(fragment.open.is_none());
			assert_eq!(fragment.transactions.len(), 1);
			assert_eq!(fragment.transactions[0].mode, TxnMode::Session);
			assert_eq!(fragment.transactions[0].start(), BlockStart::Begin);
		}
		// Resumed as a session, an open block's numbering carries and nothing stays open.
		let mut first = RequestGrouper::new(false);
		first.place(Begin);
		let open = first.finish_fragment().open.expect("open");
		let mut session = RequestGrouper::resume(true, open);
		assert_eq!(
			session.place(Begin).role,
			StatementRole::Misplaced(MisplacedControl::BeginInSession)
		);
		let fragment = session.finish_fragment();
		assert!(fragment.open.is_none());
		assert_eq!(fragment.transactions[0].statements, vec![1]);
	}

	#[test]
	fn finish_still_leaves_an_open_block_unterminated() {
		let kinds = [Begin, Run];
		let mut grouper = RequestGrouper::new(false);
		for kind in kinds {
			grouper.place(kind);
		}
		let txns = grouper.finish();
		assert_eq!(txns, lower_request(&kinds, false));
		assert_eq!(txns[0].mode, explicit(TxnEnd::Unterminated));
		assert_eq!(txns[0].start(), BlockStart::Begin);
	}
}
