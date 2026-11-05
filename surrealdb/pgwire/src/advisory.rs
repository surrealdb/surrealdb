//! Session-level advisory locks: `pg_advisory_lock`, `pg_try_advisory_lock`,
//! `pg_advisory_unlock` and `pg_advisory_unlock_all`.
//!
//! The lock table is shared by every connection of one listener, so it is per
//! server process: a lock held through one node of a cluster is invisible to a
//! client of another. Nothing here touches the datastore, and a connection that
//! waits for a lock holds no datastore transaction of its own.
//!
//! A lock belongs to the session (connection) that took it and survives the
//! end of any transaction block. A session may take the same key again; each
//! `pg_advisory_lock` needs its own `pg_advisory_unlock`. Whatever a session
//! still holds is released when its [`AdvisorySession`] is dropped, which
//! covers a clean close, an abrupt disconnect and an aborted task alike.
//!
//! A key is either one `bigint` or a pair of `int4`s; as in PostgreSQL the two
//! forms are different key spaces even when their values coincide.
//!
//! The functions run only as a whole statement, `SELECT pg_advisory_lock(key)`
//! (see [`parse_call`]): taking a lock waits on other sessions, which the query
//! executor cannot do, so they are refused inside any larger query. There is no
//! deadlock detection; a session blocked in a cycle waits until it is cancelled.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::Notify;

use super::error::PgError;

/// Upper bound on the distinct keys one session may hold, so a client cannot
/// grow the shared table without limit.
pub(crate) const MAX_LOCKS_PER_SESSION: usize = 1024;

/// A lock key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AdvisoryKey {
	/// `pg_advisory_lock(bigint)`.
	Big(i64),
	/// `pg_advisory_lock(int4, int4)`.
	Pair(i32, i32),
}

/// The session that holds a key and how many times it took it.
struct Holder {
	session: i32,
	count: u32,
}

#[derive(Default)]
struct State {
	held: HashMap<AdvisoryKey, Holder>,
	/// The distinct keys each session holds.
	sessions: HashMap<i32, HashSet<AdvisoryKey>>,
}

/// Why a wait for a lock ended without the lock.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Interrupted {
	/// The abort future given to [`AdvisoryLocks::lock`] completed first.
	Aborted,
}

/// The advisory locks of one listener.
#[derive(Default)]
pub(crate) struct AdvisoryLocks {
	state: Mutex<State>,
	/// Woken whenever a key becomes free.
	released: Notify,
}

impl AdvisoryLocks {
	fn state(&self) -> std::sync::MutexGuard<'_, State> {
		// Every operation leaves the maps consistent before it can panic, so a
		// poisoned lock still guards valid state.
		self.state.lock().unwrap_or_else(PoisonError::into_inner)
	}

	/// Take `key` for `session` if it is free or already the session's own.
	/// Returns whether the session now holds it.
	pub(crate) fn try_lock(&self, session: i32, key: AdvisoryKey) -> Result<bool, PgError> {
		let mut state = self.state();
		if let Some(holder) = state.held.get_mut(&key) {
			if holder.session != session {
				return Ok(false);
			}
			holder.count = holder.count.checked_add(1).ok_or_else(too_many_locks)?;
			return Ok(true);
		}
		let owned = state.sessions.entry(session).or_default();
		if owned.len() >= MAX_LOCKS_PER_SESSION {
			return Err(too_many_locks());
		}
		owned.insert(key);
		state.held.insert(
			key,
			Holder {
				session,
				count: 1,
			},
		);
		Ok(true)
	}

	/// Take `key` for `session`, waiting while another session holds it. The
	/// wait ends early, without the lock, when `abort` completes. No lock of the
	/// table is held while waiting.
	pub(crate) async fn lock(
		&self,
		session: i32,
		key: AdvisoryKey,
		abort: impl Future<Output = ()>,
	) -> Result<Result<(), Interrupted>, PgError> {
		tokio::pin!(abort);
		loop {
			// Register for the next release before looking at the table, so a
			// release between the look and the wait is not missed.
			let released = self.released.notified();
			tokio::pin!(released);
			released.as_mut().enable();
			if self.try_lock(session, key)? {
				return Ok(Ok(()));
			}
			tokio::select! {
				biased;
				() = &mut abort => return Ok(Err(Interrupted::Aborted)),
				() = &mut released => {}
			}
		}
	}

	/// Release one hold of `key` by `session`. Returns whether the session held it.
	pub(crate) fn unlock(&self, session: i32, key: AdvisoryKey) -> bool {
		let mut state = self.state();
		let Some(holder) = state.held.get_mut(&key) else {
			return false;
		};
		if holder.session != session {
			return false;
		}
		holder.count -= 1;
		if holder.count == 0 {
			state.held.remove(&key);
			if let Some(owned) = state.sessions.get_mut(&session) {
				owned.remove(&key);
				if owned.is_empty() {
					state.sessions.remove(&session);
				}
			}
			drop(state);
			self.released.notify_waiters();
		}
		true
	}

	/// Release every lock `session` holds, however many times it took each.
	pub(crate) fn unlock_all(&self, session: i32) {
		let mut state = self.state();
		let Some(owned) = state.sessions.remove(&session) else {
			return;
		};
		for key in owned {
			state.held.remove(&key);
		}
		drop(state);
		self.released.notify_waiters();
	}

	/// The number of distinct keys `session` holds.
	#[cfg(test)]
	fn held_by(&self, session: i32) -> usize {
		self.state().sessions.get(&session).map_or(0, HashSet::len)
	}
}

fn too_many_locks() -> PgError {
	PgError::new("53200", "out of shared memory")
		.with_hint("A session may hold a bounded number of advisory locks; release some first.")
}

/// A connection's claim on the lock table: its locks are released when it is
/// dropped.
pub(crate) struct AdvisorySession {
	locks: Arc<AdvisoryLocks>,
	id: i32,
}

impl AdvisorySession {
	pub(crate) fn new(locks: Arc<AdvisoryLocks>, id: i32) -> Self {
		Self {
			locks,
			id,
		}
	}

	pub(crate) fn id(&self) -> i32 {
		self.id
	}

	pub(crate) fn locks(&self) -> Arc<AdvisoryLocks> {
		Arc::clone(&self.locks)
	}
}

impl Drop for AdvisorySession {
	fn drop(&mut self) {
		self.locks.unlock_all(self.id);
	}
}

/// The advisory function a statement calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdvisoryOp {
	Lock,
	TryLock,
	Unlock,
	UnlockAll,
}

impl AdvisoryOp {
	/// The function's name, which is also its result column's default name.
	pub(crate) fn name(self) -> &'static str {
		match self {
			Self::Lock => "pg_advisory_lock",
			Self::TryLock => "pg_try_advisory_lock",
			Self::Unlock => "pg_advisory_unlock",
			Self::UnlockAll => "pg_advisory_unlock_all",
		}
	}

	/// Whether the function returns `void` rather than `boolean`.
	pub(crate) fn returns_void(self) -> bool {
		matches!(self, Self::Lock | Self::UnlockAll)
	}
}

/// One argument of an advisory call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdvisoryArg {
	Literal(i64),
	/// `NULL`, which a call answers with `NULL` and takes no lock for.
	Null,
	/// `$n`, numbered from one.
	Param(usize),
}

/// A statement that is one call of an advisory function.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AdvisoryCall {
	pub(crate) op: AdvisoryOp,
	pub(crate) args: Vec<AdvisoryArg>,
	/// The name the result column carries.
	pub(crate) column: String,
}

impl AdvisoryCall {
	/// The highest `$n` the call reads, or zero.
	pub(crate) fn param_count(&self) -> usize {
		self.args
			.iter()
			.map(|arg| match arg {
				AdvisoryArg::Param(n) => *n,
				AdvisoryArg::Literal(_) | AdvisoryArg::Null => 0,
			})
			.max()
			.unwrap_or(0)
	}

	/// The PostgreSQL type oid of each argument position: `int8` for the
	/// one-key form and `int4` for each half of the two-key form.
	pub(crate) fn arg_oid(&self) -> i32 {
		if self.args.len() == 2 {
			23
		} else {
			20
		}
	}
}

/// Recognise a statement that is exactly one call of an advisory function,
/// `SELECT pg_advisory_lock(<key>)` with an optional `AS name` and a trailing
/// `;`, where each key argument is an integer literal or a `$n` parameter,
/// optionally cast to an integer type. `sql` has its comments removed. Any
/// other statement, including one that merely contains such a call, is not
/// recognised.
pub(crate) fn parse_call(sql: &str) -> Option<AdvisoryCall> {
	let rest = sql.trim().trim_end_matches(';').trim();
	let rest = strip_keyword(rest, "select")?;
	let open = rest.find('(')?;
	let name = rest[..open].trim();
	let name = name
		.get(.."pg_catalog.".len())
		.filter(|prefix| prefix.eq_ignore_ascii_case("pg_catalog."))
		.map_or(name, |_| name["pg_catalog.".len()..].trim_start());
	let op = [AdvisoryOp::Lock, AdvisoryOp::TryLock, AdvisoryOp::Unlock, AdvisoryOp::UnlockAll]
		.into_iter()
		.find(|op| op.name().eq_ignore_ascii_case(name))?;
	let close = rest[open..].find(')')? + open;
	let inner = rest[open + 1..close].trim();
	let args = if inner.is_empty() {
		Vec::new()
	} else {
		inner.split(',').map(parse_arg).collect::<Option<Vec<_>>>()?
	};
	let arity_ok = match op {
		AdvisoryOp::UnlockAll => args.is_empty(),
		AdvisoryOp::Lock | AdvisoryOp::TryLock | AdvisoryOp::Unlock => {
			args.len() == 1 || args.len() == 2
		}
	};
	if !arity_ok {
		return None;
	}
	let tail = rest[close + 1..].trim();
	let column = if tail.is_empty() {
		op.name().to_owned()
	} else {
		match strip_keyword(tail, "as") {
			Some(alias) => parse_alias(alias.trim())?,
			// Without `AS` a keyword starts another clause, which this is not a call alone for.
			None if CLAUSE_KEYWORDS.iter().any(|k| k.eq_ignore_ascii_case(tail)) => return None,
			None => parse_alias(tail)?,
		}
	};
	Some(AdvisoryCall {
		op,
		args,
		column,
	})
}

/// Keywords that may follow a select list, which are not a column's name.
const CLAUSE_KEYWORDS: &[&str] = &[
	"from",
	"where",
	"group",
	"having",
	"window",
	"order",
	"limit",
	"offset",
	"fetch",
	"for",
	"union",
	"intersect",
	"except",
];

/// `text` without a leading `keyword` that is followed by whitespace, matched
/// without regard to case.
fn strip_keyword<'a>(text: &'a str, keyword: &str) -> Option<&'a str> {
	let head = text.get(..keyword.len())?;
	let rest = &text[keyword.len()..];
	(head.eq_ignore_ascii_case(keyword) && rest.starts_with(char::is_whitespace))
		.then(|| rest.trim_start())
}

fn parse_alias(alias: &str) -> Option<String> {
	if let Some(quoted) = alias.strip_prefix('"').and_then(|rest| rest.strip_suffix('"')) {
		return (!quoted.is_empty() && !quoted.contains('"')).then(|| quoted.to_owned());
	}
	let mut chars = alias.chars();
	let first = chars.next()?;
	(first.is_ascii_alphabetic() || first == '_')
		.then_some(())
		.filter(|()| chars.all(|c| c.is_ascii_alphanumeric() || c == '_'))
		.map(|()| alias.to_ascii_lowercase())
}

fn parse_arg(arg: &str) -> Option<AdvisoryArg> {
	let arg = arg.trim();
	let (value, cast) = match arg.split_once("::") {
		Some((value, cast)) => (value.trim(), Some(cast.trim())),
		None => (arg, None),
	};
	if let Some(cast) = cast
		&& !["bigint", "int8", "int", "int4", "integer", "smallint", "int2"]
			.iter()
			.any(|ty| ty.eq_ignore_ascii_case(cast))
	{
		return None;
	}
	if let Some(index) = value.strip_prefix('$') {
		let index: usize = index.parse().ok()?;
		return (index >= 1).then_some(AdvisoryArg::Param(index));
	}
	if value.eq_ignore_ascii_case("null") {
		return Some(AdvisoryArg::Null);
	}
	let digits = value.strip_prefix(['+', '-']).unwrap_or(value);
	if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
		return None;
	}
	value.parse::<i64>().ok().map(AdvisoryArg::Literal)
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use tokio::time::{sleep, timeout};

	use super::*;

	fn locks() -> Arc<AdvisoryLocks> {
		Arc::new(AdvisoryLocks::default())
	}

	const KEY: AdvisoryKey = AdvisoryKey::Big(72707369);

	#[test]
	fn a_session_may_take_a_key_again_and_each_take_needs_its_own_unlock() {
		let locks = locks();
		assert!(locks.try_lock(1, KEY).unwrap());
		assert!(locks.try_lock(1, KEY).unwrap());
		assert!(!locks.try_lock(2, KEY).unwrap(), "another session is refused while held");
		assert!(locks.unlock(1, KEY));
		assert!(!locks.try_lock(2, KEY).unwrap(), "one hold is left");
		assert!(locks.unlock(1, KEY));
		assert!(!locks.unlock(1, KEY), "a third unlock finds nothing held");
		assert!(locks.try_lock(2, KEY).unwrap(), "the key is free after the last unlock");
	}

	#[test]
	fn unlocking_a_key_the_session_does_not_hold_is_false_and_changes_nothing() {
		let locks = locks();
		assert!(!locks.unlock(1, KEY));
		assert!(locks.try_lock(1, KEY).unwrap());
		assert!(!locks.unlock(2, KEY), "a key another session holds is not released");
		assert!(!locks.try_lock(2, KEY).unwrap());
	}

	#[test]
	fn the_one_key_and_two_key_forms_are_different_key_spaces() {
		let locks = locks();
		assert!(locks.try_lock(1, AdvisoryKey::Big(1)).unwrap());
		assert!(locks.try_lock(2, AdvisoryKey::Pair(0, 1)).unwrap());
		assert!(locks.try_lock(3, AdvisoryKey::Pair(1, 0)).unwrap());
	}

	#[test]
	fn unlock_all_releases_every_hold_of_the_session_only() {
		let locks = locks();
		assert!(locks.try_lock(1, KEY).unwrap());
		assert!(locks.try_lock(1, KEY).unwrap());
		assert!(locks.try_lock(1, AdvisoryKey::Big(2)).unwrap());
		assert!(locks.try_lock(2, AdvisoryKey::Big(3)).unwrap());
		locks.unlock_all(1);
		assert_eq!(locks.held_by(1), 0);
		assert!(locks.try_lock(3, KEY).unwrap());
		assert!(!locks.try_lock(3, AdvisoryKey::Big(3)).unwrap());
	}

	#[test]
	fn dropping_the_session_releases_what_it_holds() {
		let locks = locks();
		let session = AdvisorySession::new(Arc::clone(&locks), 7);
		assert!(locks.try_lock(session.id(), KEY).unwrap());
		assert!(!locks.try_lock(8, KEY).unwrap());
		drop(session);
		assert!(locks.try_lock(8, KEY).unwrap());
	}

	#[test]
	fn a_session_holds_a_bounded_number_of_keys() {
		let locks = locks();
		for key in 0..MAX_LOCKS_PER_SESSION as i64 {
			assert!(locks.try_lock(1, AdvisoryKey::Big(key)).unwrap());
		}
		let err = locks.try_lock(1, AdvisoryKey::Big(i64::MAX)).unwrap_err();
		assert_eq!(err.to_string(), "53200: out of shared memory");
		// A key the session already holds needs no room.
		assert!(locks.try_lock(1, AdvisoryKey::Big(0)).unwrap());
		assert!(locks.try_lock(2, AdvisoryKey::Big(i64::MAX)).unwrap());
	}

	#[tokio::test]
	async fn a_waiter_takes_the_key_when_its_holder_releases_it() {
		let locks = locks();
		assert!(locks.try_lock(1, KEY).unwrap());
		let waiter = {
			let locks = Arc::clone(&locks);
			tokio::spawn(async move {
				locks.lock(2, KEY, std::future::pending()).await.unwrap().unwrap();
			})
		};
		sleep(Duration::from_millis(50)).await;
		assert!(!waiter.is_finished(), "the waiter blocks while the key is held");
		assert!(locks.unlock(1, KEY));
		timeout(Duration::from_secs(5), waiter).await.unwrap().unwrap();
		assert!(!locks.try_lock(3, KEY).unwrap(), "the waiter now holds the key");
	}

	#[tokio::test]
	async fn a_waiter_takes_the_key_when_its_holder_is_dropped() {
		let locks = locks();
		let holder = AdvisorySession::new(Arc::clone(&locks), 1);
		assert!(locks.try_lock(holder.id(), KEY).unwrap());
		let waiter = {
			let locks = Arc::clone(&locks);
			tokio::spawn(async move {
				locks.lock(2, KEY, std::future::pending()).await.unwrap().unwrap();
			})
		};
		sleep(Duration::from_millis(50)).await;
		assert!(!waiter.is_finished());
		drop(holder);
		timeout(Duration::from_secs(5), waiter).await.unwrap().unwrap();
	}

	#[tokio::test]
	async fn a_wait_ends_without_the_lock_when_it_is_aborted() {
		let locks = locks();
		assert!(locks.try_lock(1, KEY).unwrap());
		let outcome = locks.lock(2, KEY, sleep(Duration::from_millis(20))).await;
		assert_eq!(outcome.unwrap(), Err(Interrupted::Aborted));
		assert!(locks.unlock(1, KEY), "the holder is untouched");
		assert!(locks.try_lock(3, KEY).unwrap(), "the aborted waiter took nothing");
	}

	#[tokio::test]
	async fn a_free_key_is_taken_without_waiting() {
		let locks = locks();
		let outcome = locks.lock(1, KEY, std::future::pending()).await;
		assert_eq!(outcome.unwrap(), Ok(()));
	}

	#[test]
	fn recognises_a_whole_statement_call() {
		let call = parse_call("SELECT pg_advisory_lock(72707369)").unwrap();
		assert_eq!(call.op, AdvisoryOp::Lock);
		assert_eq!(call.args, [AdvisoryArg::Literal(72707369)]);
		assert_eq!(call.column, "pg_advisory_lock");
		let call = parse_call(" select PG_TRY_ADVISORY_LOCK ( $1::bigint ) as got ; ").unwrap();
		assert_eq!(call.op, AdvisoryOp::TryLock);
		assert_eq!(call.args, [AdvisoryArg::Param(1)]);
		assert_eq!(call.column, "got");
		let call = parse_call("SELECT pg_catalog.pg_advisory_unlock(1, $2)").unwrap();
		assert_eq!(call.op, AdvisoryOp::Unlock);
		assert_eq!(call.args, [AdvisoryArg::Literal(1), AdvisoryArg::Param(2)]);
		assert_eq!(call.param_count(), 2);
		assert_eq!(call.arg_oid(), 23);
		let call = parse_call("SELECT pg_advisory_unlock_all()").unwrap();
		assert_eq!(call.op, AdvisoryOp::UnlockAll);
		assert!(call.args.is_empty());
		assert_eq!(
			parse_call("SELECT pg_advisory_lock(-5)").unwrap().args,
			[AdvisoryArg::Literal(-5)]
		);
		assert_eq!(
			parse_call("SELECT pg_try_advisory_lock(NULL)").unwrap().args,
			[AdvisoryArg::Null]
		);
		assert_eq!(
			parse_call("SELECT pg_try_advisory_lock(1, null::int)").unwrap().args,
			[AdvisoryArg::Literal(1), AdvisoryArg::Null]
		);
	}

	#[test]
	fn leaves_everything_else_to_the_planner() {
		for sql in [
			"SELECT pg_advisory_lock(1) FROM t",
			"SELECT pg_advisory_lock(1) from",
			"SELECT pg_advisory_lock(1) union SELECT 1",
			"SELECT pg_advisory_lock(1), 2",
			"SELECT 1 WHERE pg_try_advisory_lock(1)",
			"SELECT pg_advisory_lock(id)",
			"SELECT pg_advisory_lock(1::text)",
			"SELECT pg_advisory_lock()",
			"SELECT pg_advisory_unlock_all(1)",
			"SELECT pg_advisory_lock(1,2,3)",
			"SELECT pg_advisory_lock(99999999999999999999)",
			"SELECT pg_advisory_lock_shared(1)",
			"SELECT foo(1)",
			"SELECTpg_advisory_lock(1)",
			"INSERT INTO t VALUES (1)",
		] {
			assert_eq!(parse_call(sql), None, "{sql}");
		}
	}
}
