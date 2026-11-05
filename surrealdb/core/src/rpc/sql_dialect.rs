//! Request options, control statements and response shaping for the `pgsql` and `mysql` RPC
//! methods.
//!
//! A call answers with one object, `{ statements, params?, transaction }`. `statements` holds
//! one entry per statement with its status, time and rows, or its error with a dialect error
//! code. `transaction.status` is `idle`, `active` or `failed`.

mod codes;
mod typed;

use surrealdb_rpc::QueryResult;
use surrealdb_rpc::error::invalid_params;

use crate::dbs::Session;
use crate::kvs::{Datastore, Dialect};
use crate::types::{PublicArray, PublicValue};

// A union type's array type takes the OID `ENUM_ARRAY_OID_OFFSET` past the type's own, so the
// catalog draws no type id at or past that distance.
#[cfg(feature = "postgres")]
const _: () =
	assert!(crate::catalog::TypeId::LIMIT == surrealdb_sqlplan::descriptor::ENUM_ARRAY_OID_OFFSET);
// Each enum's labels take a block of `ENUM_LABEL_OID_STRIDE` `pg_enum` OIDs, so the catalog draws
// no label identity at or past it, and the last block ends below the 32-bit limit.
#[cfg(feature = "postgres")]
const _: () = assert!(
	crate::catalog::ENUM_LABEL_IDENT_LIMIT == surrealdb_sqlplan::descriptor::ENUM_LABEL_OID_STRIDE
		&& (surrealdb_sqlplan::descriptor::ENUM_LABEL_OID_BASE as u64)
			+ (crate::catalog::TypeId::LIMIT as u64)
				* (surrealdb_sqlplan::descriptor::ENUM_LABEL_OID_STRIDE as u64)
			<= u32::MAX as u64
);

/// The session variable that names the dialect transaction the session holds open.
pub(super) const TXN_VAR: &str = "sql_txn";
/// The session variable that is `true` once a statement has failed in the open transaction.
pub(super) const TXN_FAILED_VAR: &str = "sql_txn_failed";
/// The session variable that is `true` while the open transaction has run no statement, so that
/// `SET TRANSACTION` can still choose its isolation level and access mode.
pub(super) const TXN_FRESH_VAR: &str = "sql_txn_fresh";
/// The isolation level of the open transaction, as the string `snapshot` or `serializable`.
const TXN_ISOLATION_VAR: &str = "sql_txn_isolation";
/// Whether the open transaction is read-only.
const TXN_READ_ONLY_VAR: &str = "sql_txn_read_only";
/// The modes the session's later transactions open with.
const DEFAULT_ISOLATION_VAR: &str = "sql_txn_default_isolation";
const DEFAULT_READ_ONLY_VAR: &str = "sql_txn_default_read_only";
/// The modes the next transaction alone opens with.
const NEXT_ISOLATION_VAR: &str = "sql_txn_next_isolation";
const NEXT_READ_ONLY_VAR: &str = "sql_txn_next_read_only";

/// Refuse a client write to the variables that record the session's dialect transaction: only the
/// `pgsql` and `mysql` methods may name the transaction a session holds.
pub(super) fn check_not_transaction_state(key: &str) -> Result<(), surrealdb_types::Error> {
	if key.starts_with("sql_txn") {
		return Err(invalid_params(format!("Cannot set protected variable: {key}")));
	}
	Ok(())
}

/// The isolation level and access mode a statement asks a transaction to have. A field that is
/// `None` leaves the one the session defaults to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct TxnModes {
	pub isolation: Option<crate::kvs::Isolation>,
	pub read_only: Option<bool>,
}

fn isolation_name(isolation: crate::kvs::Isolation) -> &'static str {
	match isolation {
		crate::kvs::Isolation::Snapshot => "snapshot",
		crate::kvs::Isolation::Serializable => "serializable",
	}
}

/// The isolation level a name asks for. The levels below `REPEATABLE READ` run as the engine's
/// snapshot isolation, which the SQL standard allows because a transaction may be given a stronger
/// level than the one it names.
pub(super) fn isolation_named(name: &str) -> Option<crate::kvs::Isolation> {
	let words: Vec<String> =
		name.trim().trim_matches('\'').split_whitespace().map(str::to_ascii_lowercase).collect();
	let words: Vec<&str> = words.iter().map(String::as_str).collect();
	match words.as_slice() {
		["serializable"] => Some(crate::kvs::Isolation::Serializable),
		["snapshot"] | ["repeatable", "read"] | ["read", "committed" | "uncommitted"] => {
			Some(crate::kvs::Isolation::Snapshot)
		}
		_ => None,
	}
}

impl TxnModes {
	/// The options object of the `begin` method that opens a transaction with these modes, or no
	/// parameters when it asks for none.
	pub(super) fn begin_params(self) -> PublicArray {
		if self.isolation.is_none() && self.read_only != Some(true) {
			return PublicArray::new();
		}
		let mut options = surrealdb_types::Object::new();
		if let Some(isolation) = self.isolation {
			options.insert("isolation", PublicValue::String(isolation_name(isolation).to_owned()));
		}
		if self.read_only == Some(true) {
			options.insert("read_only", PublicValue::Object(surrealdb_types::Object::new()));
		}
		vec![PublicValue::Object(options)].into()
	}

	/// These modes with the one-shot modes of a `SET TRANSACTION` outside a transaction and the
	/// session's defaults filling what they leave open, in that order.
	pub(super) fn resolve(self, session: &Session) -> Self {
		let isolation_of = |value: Option<&PublicValue>| match value {
			Some(PublicValue::String(name)) => isolation_named(name),
			_ => None,
		};
		let read_only_of = |value: Option<&PublicValue>| match value {
			Some(PublicValue::Bool(on)) => Some(*on),
			Some(PublicValue::String(text)) => {
				Some(matches!(text.to_ascii_lowercase().as_str(), "on" | "true" | "yes" | "1"))
			}
			_ => None,
		};
		let next_isolation = isolation_of(session.variables.get(NEXT_ISOLATION_VAR));
		let next_read_only = read_only_of(session.variables.get(NEXT_READ_ONLY_VAR));
		Self {
			isolation: self
				.isolation
				.or(next_isolation)
				.or_else(|| isolation_of(session.variables.get(DEFAULT_ISOLATION_VAR)))
				.or_else(|| {
					isolation_of(
						session
							.variables
							.get(setting_var("default_transaction_isolation").as_str()),
					)
				}),
			read_only: self
				.read_only
				.or(next_read_only)
				.or_else(|| read_only_of(session.variables.get(DEFAULT_READ_ONLY_VAR)))
				.or_else(|| {
					read_only_of(
						session
							.variables
							.get(setting_var("default_transaction_read_only").as_str()),
					)
				}),
		}
	}

	/// Record these as the modes of the transaction just opened, which has used up the one-shot
	/// modes of a `SET TRANSACTION`.
	pub(super) fn record(self, session: &mut Session) {
		session.variables.remove(NEXT_ISOLATION_VAR);
		session.variables.remove(NEXT_READ_ONLY_VAR);
		session.variables.insert(
			TXN_ISOLATION_VAR.to_owned(),
			PublicValue::String(
				isolation_name(self.isolation.unwrap_or(crate::kvs::Isolation::Snapshot))
					.to_owned(),
			),
		);
		session
			.variables
			.insert(TXN_READ_ONLY_VAR.to_owned(), PublicValue::Bool(self.read_only == Some(true)));
		session.variables.insert(TXN_FRESH_VAR.to_owned(), PublicValue::Bool(true));
	}

	/// The modes the open transaction was opened with, with `self` laid over them.
	pub(super) fn over_open(self, session: &Session) -> Self {
		let open = Self {
			isolation: match session.variables.get(TXN_ISOLATION_VAR) {
				Some(PublicValue::String(name)) => isolation_named(name),
				_ => None,
			},
			read_only: match session.variables.get(TXN_READ_ONLY_VAR) {
				Some(PublicValue::Bool(on)) => Some(*on),
				_ => None,
			},
		};
		Self {
			isolation: self.isolation.or(open.isolation),
			read_only: self.read_only.or(open.read_only),
		}
	}

	/// The modes the open transaction has, as `SET TRANSACTION` compares them.
	pub(super) fn of_open(session: &Session) -> Self {
		Self::default().over_open(session)
	}

	/// Make these the modes the session's later transactions open with, or the next one's alone.
	pub(super) fn store(self, session: &mut Session, for_session: bool) {
		let (isolation_var, read_only_var) = if for_session {
			(DEFAULT_ISOLATION_VAR, DEFAULT_READ_ONLY_VAR)
		} else {
			(NEXT_ISOLATION_VAR, NEXT_READ_ONLY_VAR)
		};
		if let Some(isolation) = self.isolation {
			session.variables.insert(
				isolation_var.to_owned(),
				PublicValue::String(isolation_name(isolation).to_owned()),
			);
		}
		if let Some(read_only) = self.read_only {
			session.variables.insert(read_only_var.to_owned(), PublicValue::Bool(read_only));
		}
	}
}

/// Whether the open transaction has run no statement.
pub(super) fn txn_is_fresh(session: &Session) -> bool {
	matches!(session.variables.get(TXN_FRESH_VAR), Some(PublicValue::Bool(true)))
}

/// Note that the open transaction has run a statement.
pub(super) async fn mark_used(session_lock: &tokio::sync::RwLock<Session>) {
	if txn_is_fresh(&*session_lock.read().await) {
		session_lock.write().await.variables.remove(TXN_FRESH_VAR);
	}
}

/// What the call asks of its statements.
#[derive(Clone, Copy, Default)]
pub(super) struct Options {
	/// Describe the last statement's columns and parameters instead of running it.
	pub describe: bool,
	/// Rows as arrays in column order instead of objects keyed by column name.
	pub row_array: bool,
	/// Run every statement of the call in one transaction that commits only if all succeed.
	pub atomic: bool,
	/// Every value as its text-protocol form, `null` for SQL NULL.
	pub value_text: bool,
	/// The isolation level of an atomic call's transaction.
	pub isolation: Option<crate::kvs::Isolation>,
	/// Whether an atomic call's transaction is read-only.
	pub read_only: bool,
}

/// Read the `options` object. A field that asks for behaviour these methods do not provide is
/// refused, never ignored.
pub(super) fn parse_options(
	options: Option<PublicValue>,
) -> Result<Options, surrealdb_types::Error> {
	let mut parsed = Options::default();
	let object = match options {
		None | Some(PublicValue::None | PublicValue::Null) => return Ok(parsed),
		Some(PublicValue::Object(object)) => object,
		Some(unexpected) => {
			return Err(invalid_params(format!(
				"Expected options to be object, got {unexpected:?}"
			)));
		}
	};
	for (name, value) in object.iter() {
		match (name.as_str(), value) {
			("describe", PublicValue::Bool(on)) => parsed.describe = *on,
			("rowMode", PublicValue::String(mode)) if mode == "object" => {}
			("rowMode", PublicValue::String(mode)) if mode == "array" => parsed.row_array = true,
			("valueMode", PublicValue::String(mode)) if mode == "native" => {}
			("valueMode", PublicValue::String(mode)) if mode == "text" => parsed.value_text = true,
			("atomic", PublicValue::Bool(on)) => parsed.atomic = *on,
			("isolation", PublicValue::String(level)) => {
				parsed.isolation = Some(isolation_named(level).ok_or_else(|| {
					invalid_params(format!(
						"Expected isolation to be `snapshot`, `serializable` or a SQL isolation level, got `{level}`"
					))
				})?);
			}
			("isolation" | "readOnly", PublicValue::None | PublicValue::Null) => {}
			("readOnly", PublicValue::Bool(on)) => parsed.read_only = *on,
			("valueMode", _) => {
				return Err(invalid_params(format!(
					"The option `{name}` is not supported with this value"
				)));
			}
			("describe" | "rowMode" | "isolation" | "readOnly", _) => {
				return Err(invalid_params(format!("Invalid value for the option `{name}`")));
			}
			_ => return Err(invalid_params(format!("Unknown option `{name}`"))),
		}
	}
	if (parsed.isolation.is_some() || parsed.read_only) && !parsed.atomic {
		return Err(invalid_params(
			"The options `isolation` and `readOnly` apply to atomic calls only".to_string(),
		));
	}
	Ok(parsed)
}

/// A statement that is the session's to carry out rather than the engine's.
pub(super) enum Control {
	Begin(TxnModes),
	/// `SET TRANSACTION modes` for the open or next transaction, or with `session` for every
	/// later one (`SET SESSION CHARACTERISTICS AS TRANSACTION`, MySQL `SET SESSION TRANSACTION`).
	SetTransaction {
		modes: TxnModes,
		session: bool,
	},
	Commit,
	Rollback,
	/// MySQL `USE database`.
	#[cfg_attr(not(feature = "mysql"), allow(dead_code))]
	Use(String),
	/// PostgreSQL `SET [SESSION | LOCAL] name = value`.
	#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
	Set(String, String),
	/// PostgreSQL `SHOW name`.
	#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
	Show(String),
	/// PostgreSQL `RESET name` or `RESET ALL`.
	#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
	Reset(String),
}

/// `text` without the whitespace and comments that precede its first token; `None` when a block
/// comment is never closed.
fn without_leading_comments(text: &str) -> Option<&str> {
	let mut rest = text;
	loop {
		rest = rest.trim_start();
		if let Some(after) = rest.strip_prefix("/*") {
			rest = after.split_once("*/")?.1;
		} else if rest.starts_with("--") || rest.starts_with('#') {
			rest = rest.split_once('\n').map_or("", |(_, tail)| tail);
		} else {
			return Some(rest);
		}
	}
}

/// The first keyword of `text`, lower-cased, after whitespace and comments. It is how a statement
/// is told apart cheaply from a session statement before it is parsed.
fn first_keyword(text: &str) -> Option<String> {
	let rest = without_leading_comments(text)?;
	let word: String = rest.chars().take_while(|c| c.is_ascii_alphabetic()).collect();
	(!word.is_empty()).then(|| word.to_ascii_lowercase())
}

/// Classify an entry whose whole text is a transaction-control statement, a MySQL `USE` or a
/// PostgreSQL setting statement. These act on the RPC session, so they are recognised only as an
/// entry of their own, and from the parsed statement, so comments around one do not hide it.
#[cfg_attr(not(any(feature = "postgres", feature = "mysql")), allow(unused_variables))]
pub(super) fn control(text: &str, dialect: Dialect) -> Option<Control> {
	let keyword = first_keyword(text)?;
	if !matches!(
		keyword.as_str(),
		"begin"
			| "start" | "commit"
			| "end" | "rollback"
			| "abort" | "use"
			| "set" | "show"
			| "reset"
	) {
		return None;
	}
	if keyword == "set"
		&& let Some(control) = without_leading_comments(text).and_then(set_transaction)
	{
		return Some(control);
	}
	#[cfg(feature = "postgres")]
	if dialect == Dialect::PgSql
		&& let Ok(statements) = crate::pgsql::parse(text)
		&& let [statement] = statements.as_slice()
	{
		use surrealdb_pgsql::ast::stmt::{IsolationLevel, Statement, TransactionStmt};
		if let Statement::Transaction(transaction) = statement {
			if let TransactionStmt::Begin {
				read_only,
				isolation,
				..
			} = transaction
			{
				return Some(Control::Begin(TxnModes {
					isolation: isolation.map(|level| match level {
						IsolationLevel::Serializable => crate::kvs::Isolation::Serializable,
						IsolationLevel::RepeatableRead
						| IsolationLevel::ReadCommitted
						| IsolationLevel::ReadUncommitted => crate::kvs::Isolation::Snapshot,
					}),
					read_only: read_only.then_some(true),
				}));
			}
			if let TransactionStmt::Commit {
				..
			} = transaction
			{
				return Some(Control::Commit);
			}
			if let TransactionStmt::Rollback {
				..
			} = transaction
			{
				return Some(Control::Rollback);
			}
		}
		if let Statement::Set(set) = statement {
			return Some(Control::Set(set.name.clone(), unquote(&set.value)));
		}
		if let Statement::Show(show) = statement {
			return Some(Control::Show(show.name.clone()));
		}
		if let Statement::Reset(reset) = statement {
			return Some(Control::Reset(reset.name.clone()));
		}
	}
	#[cfg(feature = "mysql")]
	if dialect == Dialect::MySql
		&& let Ok(statements) = crate::mysql::parse(text)
		&& let [statement] = statements.as_slice()
	{
		use surrealdb_mysql::ast::stmt::{Statement, TransactionStmt};
		if let Statement::Use(use_stmt) = statement {
			return Some(Control::Use(use_stmt.database.clone()));
		}
		if let Statement::Transaction(transaction) = statement {
			if let TransactionStmt::Begin(_) = transaction {
				return Some(Control::Begin(TxnModes::default()));
			}
			if let TransactionStmt::StartTransaction {
				read_only,
				..
			} = transaction
			{
				return Some(Control::Begin(TxnModes {
					isolation: None,
					read_only: read_only.then_some(true),
				}));
			}
			if let TransactionStmt::Commit(_) = transaction {
				return Some(Control::Commit);
			}
			if let TransactionStmt::Rollback(_) = transaction {
				return Some(Control::Rollback);
			}
		}
	}
	// Forms the parser may not take as statements (`END`, `BEGIN WORK`): matched as text, which
	// has no comments to see through since the first keyword already skipped them.
	let normalised = text
		.trim()
		.trim_end_matches(';')
		.split_whitespace()
		.collect::<Vec<_>>()
		.join(" ")
		.to_ascii_uppercase();
	match normalised.as_str() {
		"BEGIN" | "BEGIN WORK" | "BEGIN TRANSACTION" | "START TRANSACTION" => {
			return Some(Control::Begin(TxnModes::default()));
		}
		"COMMIT" | "COMMIT WORK" | "COMMIT TRANSACTION" | "END" | "END TRANSACTION" => {
			return Some(Control::Commit);
		}
		"ROLLBACK" | "ROLLBACK WORK" | "ROLLBACK TRANSACTION" | "ABORT" => {
			return Some(Control::Rollback);
		}
		_ => {}
	}
	None
}

/// A `SET [SESSION | LOCAL | GLOBAL] TRANSACTION modes` or `SET SESSION CHARACTERISTICS AS
/// TRANSACTION modes` statement, where `modes` are `ISOLATION LEVEL level` and `READ ONLY | READ
/// WRITE`, separated by commas or spaces. Any other form is not one.
fn set_transaction(text: &str) -> Option<Control> {
	let normalised = text
		.trim()
		.trim_end_matches(';')
		.replace(',', " ")
		.split_whitespace()
		.map(str::to_ascii_lowercase)
		.collect::<Vec<_>>();
	let mut words = normalised.iter().map(String::as_str).peekable();
	words.next().filter(|word| *word == "set")?;
	let mut session = false;
	match words.peek().copied() {
		Some("session") => {
			words.next();
			session = true;
			if words.peek().copied() == Some("characteristics") {
				words.next();
				words.next().filter(|word| *word == "as")?;
			}
		}
		Some("global") => {
			words.next();
			session = true;
		}
		Some("local") => {
			words.next();
		}
		_ => {}
	}
	words.next().filter(|word| *word == "transaction")?;
	let mut modes = TxnModes::default();
	while let Some(word) = words.next() {
		match word {
			"isolation" => {
				words.next().filter(|word| *word == "level")?;
				let level = match words.next()? {
					"serializable" => "serializable".to_owned(),
					"repeatable" => {
						words.next().filter(|word| *word == "read")?;
						"repeatable read".to_owned()
					}
					"read" => format!("read {}", words.next()?),
					_ => return None,
				};
				modes.isolation = Some(isolation_named(&level)?);
			}
			"read" => match words.next()? {
				"only" => modes.read_only = Some(true),
				"write" => modes.read_only = Some(false),
				_ => return None,
			},
			_ => return None,
		}
	}
	(modes != TxnModes::default()).then_some(Control::SetTransaction {
		modes,
		session,
	})
}

/// A setting value without the quotes of a single quoted literal (`'it''s'` is `it's`); a list or
/// an unquoted value is kept as written.
#[cfg(feature = "postgres")]
fn unquote(value: &str) -> String {
	let value = value.trim();
	if let Some(inner) = value.strip_prefix('\'').and_then(|rest| rest.strip_suffix('\''))
		&& !inner.replace("''", "").contains('\'')
	{
		return inner.replace("''", "'");
	}
	value.to_owned()
}

/// Whether `text` is a statement that acts on the session rather than the data: a transaction
/// control, `USE` or a setting in either dialect. Not allowed inside an atomic call, where it would
/// have no effect.
#[cfg_attr(not(feature = "mysql"), allow(unused_variables))]
pub(super) fn is_session_statement(text: &str, dialect: Dialect) -> bool {
	if control(text, dialect).is_some() {
		return true;
	}
	#[cfg(feature = "mysql")]
	if dialect == Dialect::MySql
		&& let Ok(statements) = crate::mysql::parse(text)
	{
		use surrealdb_mysql::ast::stmt::Statement;
		return statements.iter().any(|statement| {
			matches!(
				statement,
				Statement::Set(_)
					| Statement::Use(_)
					| Statement::Transaction(_)
					| Statement::LockTables(_)
					| Statement::UnlockTables(_)
			)
		});
	}
	false
}

/// The session variable that holds the PostgreSQL setting `name`.
fn setting_var(name: &str) -> String {
	let normal: String = name
		.trim()
		.chars()
		.map(|c| {
			if c.is_alphanumeric() {
				c.to_ascii_lowercase()
			} else {
				'_'
			}
		})
		.collect();
	format!("{SETTING_PREFIX}{normal}")
}

/// The prefix of the session variables that hold PostgreSQL settings.
const SETTING_PREFIX: &str = "sql_setting_";

/// The value PostgreSQL reports for a setting no `SET` has changed.
fn setting_default(name: &str) -> Option<&'static str> {
	Some(match name {
		"timezone" => "UTC",
		"application_name" => "",
		"search_path" => "\"$user\", public",
		"client_encoding" => "UTF8",
		"datestyle" => "ISO, MDY",
		"standard_conforming_strings" => "on",
		"extra_float_digits" => "1",
		"intervalstyle" => "postgres",
		"bytea_output" => "hex",
		"statement_timeout" => "0",
		"client_min_messages" => "notice",
		"default_transaction_isolation" => "read committed",
		"default_transaction_read_only" => "off",
		"integer_datetimes" => "on",
		"server_encoding" => "UTF8",
		_ => return None,
	})
}

/// Carry out a `SET`, `SHOW` or `RESET` on the session's settings. Settings are session
/// variables; they last for the session, and `SET LOCAL` is treated as `SET`.
pub(super) async fn apply_setting(
	session_lock: &tokio::sync::RwLock<Session>,
	control: &Control,
	gate: impl Fn(&Session) -> Result<(), surrealdb_types::Error>,
) -> Result<PublicValue, surrealdb_types::Error> {
	if let Control::Set(name, value) = control {
		let mut session = session_lock.write().await;
		gate(&session)?;
		session.variables.insert(setting_var(name), PublicValue::String(value.clone()));
		return Ok(tagged_entry("SET"));
	}
	if let Control::Reset(name) = control {
		let mut session = session_lock.write().await;
		gate(&session)?;
		if name.eq_ignore_ascii_case("all") {
			let held: Vec<String> = session
				.variables
				.iter()
				.map(|(key, _)| key.clone())
				.filter(|key| key.starts_with(SETTING_PREFIX))
				.collect();
			for key in held {
				session.variables.remove(key.as_str());
			}
		} else {
			session.variables.remove(setting_var(name).as_str());
		}
		return Ok(tagged_entry("RESET"));
	}
	if let Control::Show(name) = control {
		let session = session_lock.read().await;
		gate(&session)?;
		let key = name.trim().to_ascii_lowercase();
		let value = match session.variables.get(setting_var(name).as_str()) {
			Some(PublicValue::String(value)) => Some(value.clone()),
			_ => setting_default(&key).map(str::to_owned),
		};
		let Some(value) = value else {
			return Ok(error_entry(
				&format!("unrecognized configuration parameter \"{name}\""),
				"42704",
				None,
			));
		};
		let column = typed::ColumnMeta::text(&key);
		let (columns, rows) = typed::columns_and_rows(
			std::slice::from_ref(&column),
			vec![vec![PublicValue::String(value)]],
			Dialect::PgSql,
			Options::default(),
		);
		let mut entry = surrealdb_types::Object::new();
		entry.insert("status", PublicValue::String("OK".to_owned()));
		entry.insert("command", typed::command_tag("SHOW", None));
		entry.insert("columns", columns);
		entry.insert("rows", rows);
		return Ok(PublicValue::Object(entry));
	}
	Ok(error_entry("not a setting statement", "XX000", None))
}

/// Whether a call runs a transaction-control entry or is atomic. Such a call finishes or opens a
/// transaction the client must learn the outcome of, so the method-level wall-clock guard must not
/// drop it; the call's other statements are bounded by a deadline the handler measures itself. A
/// `describe` call runs nothing and is never exempt.
pub(super) fn has_transaction_control(params: &PublicArray, dialect: Dialect) -> bool {
	if matches!(
		params.get(2),
		Some(PublicValue::Object(options)) if matches!(options.get("describe"), Some(PublicValue::Bool(true)))
	) {
		return false;
	}
	// An atomic call finishes a transaction it opened itself, which must not be dropped midway.
	if matches!(
		params.get(2),
		Some(PublicValue::Object(options)) if matches!(options.get("atomic"), Some(PublicValue::Bool(true)))
	) {
		return true;
	}
	let is_control = |text: &str| {
		matches!(
			control(text, dialect),
			Some(
				Control::Begin(_)
					| Control::Commit
					| Control::Rollback
					| Control::SetTransaction { .. }
			)
		)
	};
	let entry_is_control = |entry: &PublicValue| {
		if let PublicValue::String(text) = entry {
			is_control(text)
		} else if let PublicValue::Object(object) = entry {
			matches!(object.get("text"), Some(PublicValue::String(text)) if is_control(text))
		} else {
			false
		}
	};
	match params.first() {
		Some(PublicValue::Array(entries)) => entries.iter().any(entry_is_control),
		Some(entry) => entry_is_control(entry),
		None => false,
	}
}

fn number(n: i64) -> PublicValue {
	PublicValue::Number(surrealdb_types::Number::Int(n))
}

/// The dialect error code of an engine error: the SQLSTATE, and for MySQL the error number.
fn classify(err: &surrealdb_types::Error, dialect: Dialect) -> (&'static str, Option<i64>) {
	codes::classify(err, dialect)
}

/// The 1-based character position a parse error names, from its `at START..END` span, which is a
/// byte offset into the statement text.
fn position_in(message: &str) -> Option<i64> {
	let rest = &message[message.find(" at ")? + 4..];
	let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
	if digits.is_empty() || !rest[digits.len()..].starts_with("..") {
		return None;
	}
	digits.parse::<i64>().ok().map(|offset| offset + 1)
}

/// An `ERR` entry whose `position` is a byte offset into `text`, with that position as the
/// 1-based character position clients are promised.
pub(super) fn char_position(entry: PublicValue, text: &str) -> PublicValue {
	let PublicValue::Object(mut object) = entry else {
		return entry;
	};
	let Some(PublicValue::Object(mut error)) = object.remove("error") else {
		return PublicValue::Object(object);
	};
	if let Some(PublicValue::Number(surrealdb_types::Number::Int(position))) = error.get("position")
		&& let Ok(byte) = usize::try_from(position - 1)
	{
		let mut end = byte.min(text.len());
		while !text.is_char_boundary(end) {
			end -= 1;
		}
		error.insert(
			"position",
			number(i64::try_from(text[..end].chars().count()).unwrap_or(0) + 1),
		);
	}
	object.insert("error", PublicValue::Object(error));
	PublicValue::Object(object)
}

/// The `ERR` entry of a statement that was cancelled before it finished.
pub(super) fn cancelled_entry(dialect: Dialect) -> PublicValue {
	let message = "canceling statement due to user request";
	if dialect == Dialect::MySql {
		error_entry(message, "70100", Some(1317))
	} else {
		error_entry(message, "57014", None)
	}
}

/// An `ERR` statement entry.
pub(super) fn error_entry(message: &str, sqlstate: &str, errno: Option<i64>) -> PublicValue {
	let mut error = surrealdb_types::Object::new();
	error.insert("message", PublicValue::String(message.to_owned()));
	error.insert("severity", PublicValue::String("ERROR".to_owned()));
	error.insert("sqlstate", PublicValue::String(sqlstate.to_owned()));
	error.insert("errno", errno.map_or(PublicValue::Null, number));
	error.insert("position", position_in(message).map_or(PublicValue::Null, number));
	let mut entry = surrealdb_types::Object::new();
	entry.insert("status", PublicValue::String("ERR".to_owned()));
	entry.insert("error", PublicValue::Object(error));
	PublicValue::Object(entry)
}

/// The `ERR` entry for an engine error.
pub(super) fn engine_error_entry(err: &surrealdb_types::Error, dialect: Dialect) -> PublicValue {
	let (sqlstate, errno) = classify(err, dialect);
	error_entry(err.message(), sqlstate, errno)
}

/// The `ERR` entry of a statement refused because the transaction already failed.
pub(super) fn aborted_entry() -> PublicValue {
	error_entry(
		"current transaction is aborted, commands ignored until end of transaction block",
		"25P02",
		None,
	)
}

/// An `OK` entry with no rows, for a statement the session carried out.
pub(super) fn ok_entry() -> PublicValue {
	let mut entry = surrealdb_types::Object::new();
	entry.insert("status", PublicValue::String("OK".to_owned()));
	entry.insert("columns", PublicValue::Array(PublicArray::new()));
	entry.insert("rows", PublicValue::Array(PublicArray::new()));
	PublicValue::Object(entry)
}

/// An `OK` entry with no rows and the command tag of the control statement the session carried
/// out.
pub(super) fn tagged_entry(tag: &str) -> PublicValue {
	let mut entry = surrealdb_types::Object::new();
	entry.insert("status", PublicValue::String("OK".to_owned()));
	entry.insert("command", typed::command_tag(tag, None));
	entry.insert("columns", PublicValue::Array(PublicArray::new()));
	entry.insert("rows", PublicValue::Array(PublicArray::new()));
	PublicValue::Object(entry)
}

/// The planner analysis of one statement, `None` when it could not be made. A failed analysis
/// never decides what runs; the statement's own run reports its error.
pub(super) async fn analyse(
	ds: &Datastore,
	session: &Session,
	transaction: Option<&std::sync::Arc<crate::kvs::Transaction>>,
	dialect: Dialect,
	text: &str,
) -> Option<typed::Described> {
	typed::analyse(ds, session, transaction, dialect, text).await.ok()
}

/// A result as the SQL dialects show it: a record id is its bare key, since SQL values carry no
/// table.
#[cfg(any(feature = "postgres", feature = "mysql"))]
fn show_record_ids_as_keys(value: PublicValue) -> PublicValue {
	surrealdb_sqlplan::show_record_ids_as_keys(value)
}

#[cfg(not(any(feature = "postgres", feature = "mysql")))]
fn show_record_ids_as_keys(value: PublicValue) -> PublicValue {
	value
}

/// The statement entry of one engine result, typed and tagged from the statement's `described`
/// analysis when there is one.
pub(super) fn result_entry(
	result: QueryResult,
	dialect: Dialect,
	options: Options,
	described: Option<&typed::Described>,
) -> PublicValue {
	let time = format!("{:?}", result.time);
	let rows = match result.result.map(show_record_ids_as_keys) {
		Ok(PublicValue::Array(rows)) => rows.into_vec(),
		Ok(PublicValue::None | PublicValue::Null) => Vec::new(),
		Ok(other) => vec![other],
		Err(err) => return engine_error_entry(&err, dialect),
	};
	let kind = typed::kind_of(described);
	let count = rows.len();
	let (columns, shaped) = if typed::answers_with_rows(described) {
		typed::shape(
			rows,
			described.and_then(|described| described.columns.as_deref()),
			described.map_or(&[][..], |described| described.returning_order.as_slice()),
		)
	} else {
		(Vec::new(), Vec::new())
	};
	let (columns, rows) = typed::columns_and_rows(&columns, shaped, dialect, options);
	let mut entry = surrealdb_types::Object::new();
	entry.insert("status", PublicValue::String("OK".to_owned()));
	entry.insert("time", PublicValue::String(time));
	entry.insert("command", typed::command(&kind, dialect, count));
	entry.insert("columns", columns);
	entry.insert("rows", rows);
	PublicValue::Object(entry)
}

/// The response object of a call.
pub(super) fn response(
	statements: Vec<PublicValue>,
	params: Option<PublicValue>,
	session: &Session,
) -> PublicValue {
	let status = if session.variables.get(TXN_VAR).is_none() {
		"idle"
	} else if matches!(session.variables.get(TXN_FAILED_VAR), Some(PublicValue::Bool(true))) {
		"failed"
	} else {
		"active"
	};
	let mut transaction = surrealdb_types::Object::new();
	transaction.insert("status", PublicValue::String(status.to_owned()));
	let mut object = surrealdb_types::Object::new();
	object.insert("statements", PublicValue::Array(statements.into()));
	if let Some(params) = params {
		object.insert("params", params);
	}
	object.insert("transaction", PublicValue::Object(transaction));
	PublicValue::Object(object)
}

/// The response object of an atomic call: `committed` says whether its transaction committed,
/// which is not so when a statement failed: the entries before the failure keep their rows and
/// carry `rolled_back: true`.
pub(super) fn response_atomic(statements: Vec<PublicValue>, committed: bool) -> PublicValue {
	let statements: Vec<PublicValue> = if committed {
		statements
	} else {
		statements
			.into_iter()
			.map(|mut entry| {
				if let PublicValue::Object(object) = &mut entry
					&& matches!(object.get("status"), Some(PublicValue::String(status)) if status == "OK")
				{
					object.insert("rolled_back", PublicValue::Bool(true));
				}
				entry
			})
			.collect()
	};
	let mut transaction = surrealdb_types::Object::new();
	transaction.insert("status", PublicValue::String("idle".to_owned()));
	transaction.insert("committed", PublicValue::Bool(committed));
	let mut object = surrealdb_types::Object::new();
	object.insert("statements", PublicValue::Array(statements.into()));
	object.insert("transaction", PublicValue::Object(transaction));
	PublicValue::Object(object)
}

/// The statement entry and the parameter descriptors of `text` described without running it.
///
/// The dialect's capability is checked here because no execution path does it for a describe.
/// The catalog is read through the session, and through its open transaction when it has one, so a
/// subject is told only what it may read and sees its own uncommitted schema. Only a `SELECT` has
/// columns to describe; any other statement describes as having none.
pub(super) async fn describe(
	ds: &Datastore,
	session: &Session,
	transaction: Option<&std::sync::Arc<crate::kvs::Transaction>>,
	dialect: Dialect,
	text: &str,
) -> Result<(PublicValue, PublicValue), surrealdb_types::Error> {
	ds.check_sql_dialect(dialect)?;
	let analysed = typed::analyse(ds, session, transaction, dialect, text).await?;
	let (mut entry, params) = typed::described_entry(&analysed, dialect);
	if analysed.failure.is_some() {
		entry = char_position(entry, text);
	}
	if analysed.failure.is_none()
		&& let PublicValue::Object(object) = &mut entry
		&& matches!(object.get("columns"), Some(PublicValue::Null))
		&& !matches!(analysed.kind, typed::Kind::Select)
	{
		object.insert("columns", PublicValue::Array(PublicArray::new()));
	}
	Ok((entry, params))
}

/// The statements of `text` as separate texts, or `None` when it holds fewer than two or cannot
/// be split by the parsed spans. A text is run statement by statement so each SQL statement
/// answers with one entry and a failure stops the statements after it.
#[cfg(not(any(feature = "postgres", feature = "mysql")))]
pub(super) fn split_statements(_text: &str, _dialect: Dialect) -> Option<Vec<String>> {
	None
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
pub(super) fn split_statements(text: &str, dialect: Dialect) -> Option<Vec<String>> {
	let mut spans: Vec<(usize, usize)> = Vec::new();
	match dialect {
		#[cfg(feature = "postgres")]
		Dialect::PgSql => {
			for statement in crate::pgsql::parse(text).ok()? {
				let span = statement.span();
				spans.push((span.start as usize, span.end as usize));
			}
		}
		#[cfg(feature = "mysql")]
		Dialect::MySql => {
			for statement in crate::mysql::parse(text).ok()? {
				let span = statement.span();
				spans.push((span.start, span.end));
			}
		}
		_ => return None,
	}
	if spans.len() < 2 {
		return None;
	}
	let mut parts = Vec::with_capacity(spans.len());
	let mut end = 0usize;
	for (start, stop) in spans {
		if stop <= start || start < end || stop > text.len() {
			return None;
		}
		parts.push(text.get(start..stop)?.to_owned());
		end = stop;
	}
	Some(parts)
}

/// The one entry of a SQL statement that lowered to `results`: the first error if any, else the
/// last result (a statement that lowers to several engine statements reports the last).
pub(super) fn statement_entry(
	results: Vec<QueryResult>,
	dialect: Dialect,
	options: Options,
	described: Option<&typed::Described>,
) -> (PublicValue, bool) {
	let errored = results.iter().any(|result| result.result.is_err());
	let chosen = if errored {
		results.into_iter().find(|result| result.result.is_err())
	} else {
		results.into_iter().last()
	};
	match chosen {
		Some(result) => (result_entry(result, dialect, options, described), errored),
		None => (ok_entry(), false),
	}
}

/// The entry of a statement that was not run because an earlier statement of its text failed.
pub(super) fn skipped_entry(dialect: Dialect) -> PublicValue {
	let message = "not run: an earlier statement of the text failed";
	if dialect == Dialect::MySql {
		error_entry(message, "HY000", Some(1105))
	} else {
		error_entry(message, "XX000", None)
	}
}

/// Whether a statement entry is an `ERR` entry.
pub(super) fn is_error_entry(entry: &PublicValue) -> bool {
	matches!(
		entry,
		PublicValue::Object(object) if matches!(object.get("status"), Some(PublicValue::String(status)) if status == "ERR")
	)
}

/// Mark the session's open dialect transaction failed, if it holds one. A call that was dropped
/// at its deadline may have left a statement of it half run.
pub(super) async fn fail_open_transaction(session_lock: &tokio::sync::RwLock<Session>) {
	let mut session = session_lock.write().await;
	if session.variables.get(TXN_VAR).is_some() {
		session.variables.insert(TXN_FAILED_VAR.to_owned(), PublicValue::Bool(true));
	}
}

/// Record that the transaction `open` has failed, unless the session has since moved to another
/// transaction or none: the flag always belongs to the transaction named by `sql_txn`.
pub(super) async fn mark_failed(session_lock: &tokio::sync::RwLock<Session>, open: uuid::Uuid) {
	let mut session = session_lock.write().await;
	let current = matches!(
		session.variables.get(TXN_VAR),
		Some(PublicValue::Uuid(id)) if id.into_inner() == open
	);
	if current {
		session.variables.insert(TXN_FAILED_VAR.to_owned(), PublicValue::Bool(true));
	}
}

/// The session's open dialect transaction and whether it has failed, read afresh. A transaction
/// that no longer exists (ended through the ordinary `commit` or `cancel` methods, or by the
/// server) is cleared from the session, so the session reports `idle` rather than naming a
/// transaction nothing can reach.
pub(super) async fn live_transaction<T>(
	this: &T,
	session_lock: &tokio::sync::RwLock<Session>,
) -> (Option<uuid::Uuid>, bool)
where
	T: super::RpcProtocol + ?Sized,
{
	let (open, failed) = {
		let session = session_lock.read().await;
		let open = match session.variables.get(TXN_VAR) {
			Some(PublicValue::Uuid(id)) => Some(id.into_inner()),
			_ => None,
		};
		let failed = matches!(session.variables.get(TXN_FAILED_VAR), Some(PublicValue::Bool(true)));
		(open, failed)
	};
	let Some(id) = open else {
		// A failed flag with no transaction is orphaned state, cleared so it cannot outlive it.
		if failed {
			let mut session = session_lock.write().await;
			if session.variables.get(TXN_VAR).is_none() {
				session.variables.remove(TXN_FAILED_VAR);
			}
		}
		return (None, false);
	};
	if this.get_tx(id).await.is_ok() {
		return (Some(id), failed);
	}
	let mut session = session_lock.write().await;
	// Only the transaction that was checked is cleared; a concurrent call may have opened another.
	let still_stale = matches!(
		session.variables.get(TXN_VAR),
		Some(PublicValue::Uuid(current)) if current.into_inner() == id
	);
	if still_stale {
		session.variables.remove(TXN_VAR);
		session.variables.remove(TXN_FAILED_VAR);
		return (None, false);
	}
	let open = match session.variables.get(TXN_VAR) {
		Some(PublicValue::Uuid(current)) => Some(current.into_inner()),
		_ => None,
	};
	let failed = matches!(session.variables.get(TXN_FAILED_VAR), Some(PublicValue::Bool(true)));
	(open, failed)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn byte_positions_become_character_positions() {
		let entry = error_entry("parse error: at 6..8: unexpected", "42601", None);
		let PublicValue::Object(object) = char_position(entry, "\u{e9}\u{e9} SELEC ((") else {
			panic!("not an entry");
		};
		let Some(PublicValue::Object(error)) = object.get("error") else {
			panic!("no error");
		};
		assert_eq!(error.get("position"), Some(&number(5)));
	}

	#[test]
	fn control_sees_through_comments() {
		for text in ["/* client */ BEGIN", "-- setup\nCOMMIT", "/* a */ /* b */ ROLLBACK;"] {
			assert!(control(text, Dialect::PgSql).is_some(), "{text}");
		}
		assert!(control("/* x */ SELECT 1", Dialect::PgSql).is_none());
	}

	#[test]
	fn parse_errors_name_their_position() {
		assert_eq!(position_in("PostgreSQL parse error: at 7..12: unexpected token"), Some(8));
		assert_eq!(position_in("The table 'widget' already exists"), None);
		assert_eq!(position_in("looks at 5 things"), None);
	}

	#[test]
	fn control_recognises_transaction_statements() {
		for text in
			["BEGIN", " begin work; ", "START TRANSACTION", "BEGIN\tWORK", "start\ntransaction"]
		{
			assert!(matches!(control(text, Dialect::PgSql), Some(Control::Begin(_))), "{text}");
		}
		for text in ["COMMIT", "end"] {
			assert!(matches!(control(text, Dialect::MySql), Some(Control::Commit)), "{text}");
		}
		for text in ["ROLLBACK;", "abort"] {
			assert!(matches!(control(text, Dialect::PgSql), Some(Control::Rollback)), "{text}");
		}
		assert!(control("SELECT 1", Dialect::PgSql).is_none());
		assert!(control("BEGIN; SELECT 1", Dialect::PgSql).is_none());
	}

	#[cfg(any(feature = "postgres", feature = "mysql"))]
	fn modes(control: Option<Control>) -> TxnModes {
		match control {
			Some(Control::Begin(modes)) => modes,
			other => panic!("not a BEGIN: {}", other.is_some()),
		}
	}

	#[cfg(feature = "postgres")]
	#[test]
	fn pgsql_begin_carries_its_isolation_level_and_access_mode() {
		use crate::kvs::Isolation;
		let begin = |text: &str| modes(control(text, Dialect::PgSql));
		assert_eq!(begin("BEGIN"), TxnModes::default());
		assert_eq!(
			begin("BEGIN ISOLATION LEVEL SERIALIZABLE"),
			TxnModes {
				isolation: Some(Isolation::Serializable),
				read_only: None
			}
		);
		for level in ["REPEATABLE READ", "READ COMMITTED", "READ UNCOMMITTED"] {
			assert_eq!(
				begin(&format!("BEGIN ISOLATION LEVEL {level}")).isolation,
				Some(Isolation::Snapshot),
				"{level}"
			);
		}
		assert_eq!(
			begin("START TRANSACTION READ ONLY"),
			TxnModes {
				isolation: None,
				read_only: Some(true)
			}
		);
		assert_eq!(
			begin("begin transaction isolation level serializable read only"),
			TxnModes {
				isolation: Some(Isolation::Serializable),
				read_only: Some(true)
			}
		);
	}

	#[cfg(feature = "mysql")]
	#[test]
	fn mysql_start_transaction_carries_its_access_mode() {
		let begin = |text: &str| modes(control(text, Dialect::MySql));
		assert_eq!(begin("START TRANSACTION"), TxnModes::default());
		assert_eq!(begin("START TRANSACTION WITH CONSISTENT SNAPSHOT"), TxnModes::default());
		for text in [
			"START TRANSACTION READ ONLY",
			"START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT",
			"START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY",
		] {
			assert_eq!(begin(text).read_only, Some(true), "{text}");
		}
		assert_eq!(begin("START TRANSACTION READ WRITE").read_only, None);
	}

	#[test]
	fn set_transaction_chooses_modes_for_the_open_next_or_every_later_transaction() {
		use crate::kvs::Isolation;
		let set = |text: &str| match control(text, Dialect::PgSql) {
			Some(Control::SetTransaction {
				modes,
				session,
			}) => (modes, session),
			_ => panic!("{text} is not a SET TRANSACTION"),
		};
		assert_eq!(
			set("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
			(
				TxnModes {
					isolation: Some(Isolation::Serializable),
					read_only: None
				},
				false
			)
		);
		assert_eq!(
			set("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"),
			(
				TxnModes {
					isolation: None,
					read_only: Some(true)
				},
				true
			)
		);
		assert_eq!(
			set("set session transaction isolation level read committed, read write;"),
			(
				TxnModes {
					isolation: Some(Isolation::Snapshot),
					read_only: Some(false)
				},
				true
			)
		);
		assert!(control("SET TRANSACTION SNAPSHOT '000-1'", Dialect::PgSql).is_none());
		assert!(control("SET TRANSACTION", Dialect::PgSql).is_none());
		assert!(matches!(
			control("/* client */ SET TRANSACTION READ ONLY", Dialect::PgSql),
			Some(Control::SetTransaction { .. })
		));
		assert!(control("SET timezone = 'UTC'", Dialect::PgSql).is_some());
	}

	#[test]
	fn the_sql_isolation_levels_name_the_engines_two() {
		use crate::kvs::Isolation;
		for (name, level) in [
			("snapshot", Isolation::Snapshot),
			("Repeatable  Read", Isolation::Snapshot),
			("READ COMMITTED", Isolation::Snapshot),
			("read uncommitted", Isolation::Snapshot),
			("'serializable'", Isolation::Serializable),
		] {
			assert_eq!(isolation_named(name), Some(level), "{name}");
		}
		assert_eq!(isolation_named("repeatable"), None);
		assert_eq!(isolation_named("linearizable"), None);
	}

	#[test]
	fn an_atomic_call_takes_isolation_and_read_only_and_no_other_call_does() {
		let parse = |isolation: Option<&str>, read_only: Option<bool>, atomic: bool| {
			let mut options = surrealdb_types::Object::new();
			options.insert("atomic", PublicValue::Bool(atomic));
			if let Some(isolation) = isolation {
				options.insert("isolation", PublicValue::String(isolation.to_owned()));
			}
			if let Some(read_only) = read_only {
				options.insert("readOnly", PublicValue::Bool(read_only));
			}
			parse_options(Some(PublicValue::Object(options)))
		};
		let parsed = parse(Some("serializable"), Some(true), true).unwrap();
		assert_eq!(parsed.isolation, Some(crate::kvs::Isolation::Serializable));
		assert!(parsed.read_only);
		assert!(parse(Some("serializable"), None, false).is_err());
		assert!(parse(None, Some(true), false).is_err());
		assert!(parse(Some("bogus"), None, true).is_err());
		assert!(parse(None, Some(false), false).is_ok());
	}

	#[test]
	fn a_response_marks_the_entries_an_uncommitted_call_rolled_back() {
		let ok = || PublicValue::Object(surrealdb_types::object! { status: "OK" });
		let err = || PublicValue::Object(surrealdb_types::object! { status: "ERR" });
		let response = |committed| {
			let PublicValue::Object(object) = response_atomic(vec![ok(), err()], committed) else {
				panic!("not an object")
			};
			let Some(PublicValue::Array(statements)) = object.get("statements").cloned() else {
				panic!("no statements")
			};
			statements.into_vec()
		};
		let rolled_back = |entry: &PublicValue| matches!(entry, PublicValue::Object(o) if o.get("rolled_back") == Some(&PublicValue::Bool(true)));
		let uncommitted = response(false);
		assert!(rolled_back(&uncommitted[0]));
		assert!(!rolled_back(&uncommitted[1]));
		assert!(!response(true).iter().any(rolled_back));
	}

	#[test]
	fn returning_columns_follow_the_list_with_a_star_at_its_place() {
		let keys = |names: &[&str]| names.iter().map(|name| (*name).to_owned()).collect::<Vec<_>>();
		let order = |entries: &[Option<&str>]| {
			entries.iter().map(|entry| entry.map(str::to_owned)).collect::<Vec<_>>()
		};
		let engine = keys(&["age", "id", "name"]);
		assert_eq!(typed::ordered(engine.clone(), &[]), engine);
		assert_eq!(
			typed::ordered(engine.clone(), &order(&[Some("name"), Some("id"), Some("age")])),
			keys(&["name", "id", "age"])
		);
		assert_eq!(
			typed::ordered(engine.clone(), &order(&[Some("name"), None])),
			keys(&["name", "age", "id"])
		);
		assert_eq!(
			typed::ordered(engine, &order(&[None, Some("name")])),
			keys(&["age", "id", "name"])
		);
	}

	#[cfg(feature = "mysql")]
	#[test]
	fn control_parses_mysql_use_forms() {
		for (text, expected) in
			[("USE other", "other"), ("use\tother;", "other"), ("USE `my db`", "my db")]
		{
			match control(text, Dialect::MySql) {
				Some(Control::Use(database)) => assert_eq!(database, expected, "{text}"),
				_ => panic!("{text} is not a USE"),
			}
		}
		assert!(control("USE other; SELECT 1", Dialect::MySql).is_none());
		assert!(control("USE other", Dialect::PgSql).is_none());
	}

	#[cfg(any(feature = "postgres", feature = "mysql"))]
	fn assert_splits(dialect: Dialect) {
		let parts = split_statements("SELECT 1; SELECT 2;", dialect).expect("two statements");
		assert_eq!(parts.len(), 2, "{dialect:?}");
		assert!(split_statements("SELECT 1", dialect).is_none());
		assert!(split_statements("SELEC ((", dialect).is_none());
	}

	#[cfg(feature = "postgres")]
	#[test]
	fn pgsql_text_splits_into_its_statements() {
		assert_splits(Dialect::PgSql);
	}

	#[cfg(feature = "mysql")]
	#[test]
	fn mysql_text_splits_into_its_statements() {
		assert_splits(Dialect::MySql);
	}

	#[test]
	fn transaction_control_calls_are_recognised() {
		let call = |value: PublicValue| -> PublicArray { vec![value].into() };
		let plain = call(PublicValue::String("SELECT 1".into()));
		assert!(!has_transaction_control(&plain, Dialect::PgSql));
		let commit = call(PublicValue::String("COMMIT".into()));
		assert!(has_transaction_control(&commit, Dialect::PgSql));
		let batch = call(PublicValue::Array(surrealdb_types::array![
			"INSERT 1",
			surrealdb_types::object! { text: "COMMIT" }
		]));
		assert!(has_transaction_control(&batch, Dialect::MySql));
		assert!(!has_transaction_control(&PublicArray::new(), Dialect::MySql));
		let described: PublicArray = vec![
			PublicValue::String("COMMIT".into()),
			PublicValue::None,
			PublicValue::Object(surrealdb_types::object! { describe: true }),
		]
		.into();
		assert!(!has_transaction_control(&described, Dialect::PgSql));
	}
}
