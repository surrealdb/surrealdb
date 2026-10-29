//! Request options, control statements and response shaping for the `pgsql` and `mysql` RPC
//! methods.
//!
//! A call answers with one object, `{ statements, params?, transaction }`. `statements` holds
//! one entry per statement with its status, time and rows, or its error with a dialect error
//! code. `transaction.status` is `idle`, `active` or `failed`.

mod typed;

use surrealdb_rpc::QueryResult;
use surrealdb_rpc::error::invalid_params;

use crate::dbs::Session;
use crate::kvs::{Datastore, Dialect};
use crate::types::{PublicArray, PublicValue};

/// The session variable that names the dialect transaction the session holds open.
pub(super) const TXN_VAR: &str = "sql_txn";
/// The session variable that is `true` once a statement has failed in the open transaction.
pub(super) const TXN_FAILED_VAR: &str = "sql_txn_failed";

/// Refuse a client write to the variables that record the session's dialect transaction: only the
/// `pgsql` and `mysql` methods may name the transaction a session holds.
pub(super) fn check_not_transaction_state(key: &str) -> Result<(), surrealdb_types::Error> {
	if key == TXN_VAR || key == TXN_FAILED_VAR {
		return Err(invalid_params(format!("Cannot set protected variable: {key}")));
	}
	Ok(())
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
			("valueMode", _) => {
				return Err(invalid_params(format!(
					"The option `{name}` is not supported with this value"
				)));
			}
			("describe" | "rowMode", _) => {
				return Err(invalid_params(format!("Invalid value for the option `{name}`")));
			}
			_ => return Err(invalid_params(format!("Unknown option `{name}`"))),
		}
	}
	Ok(parsed)
}

/// A statement that is the session's to carry out rather than the engine's.
pub(super) enum Control {
	Begin,
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

/// The first keyword of `text`, lower-cased, after whitespace and comments. It is how a statement
/// is told apart cheaply from a session statement before it is parsed.
fn first_keyword(text: &str) -> Option<String> {
	let mut rest = text;
	loop {
		rest = rest.trim_start();
		if let Some(after) = rest.strip_prefix("/*") {
			rest = after.split_once("*/")?.1;
		} else if rest.starts_with("--") || rest.starts_with('#') {
			rest = rest.split_once('\n').map_or("", |(_, tail)| tail);
		} else {
			break;
		}
	}
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
	#[cfg(feature = "postgres")]
	if dialect == Dialect::PgSql
		&& let Ok(statements) = crate::pgsql::parse(text)
		&& let [statement] = statements.as_slice()
	{
		use surrealdb_pgsql::ast::stmt::{Statement, TransactionStmt};
		if let Statement::Transaction(transaction) = statement {
			if let TransactionStmt::Begin {
				read_only: false,
				isolation: None,
				..
			} = transaction
			{
				return Some(Control::Begin);
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
			if let TransactionStmt::Begin(_)
			| TransactionStmt::StartTransaction {
				read_only: false,
				..
			} = transaction
			{
				return Some(Control::Begin);
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
			return Some(Control::Begin);
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
		matches!(control(text, dialect), Some(Control::Begin | Control::Commit | Control::Rollback))
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
	if matches!(err.query_details(), Some(surrealdb_types::QueryError::Cancelled)) {
		return if dialect == Dialect::MySql {
			("70100", Some(1317))
		} else {
			("57014", None)
		};
	}
	match (dialect == Dialect::MySql, err.is_validation(), err.is_not_allowed()) {
		(false, true, _) => ("42601", None),
		(false, _, true) => ("42501", None),
		(false, _, _) => ("XX000", None),
		(true, true, _) => ("42000", Some(1064)),
		(true, _, true) => ("42000", Some(1142)),
		(true, _, _) => ("HY000", Some(1105)),
	}
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

/// The statement entry of one engine result, typed and tagged from the statement's `described`
/// analysis when there is one.
pub(super) fn result_entry(
	result: QueryResult,
	dialect: Dialect,
	options: Options,
	described: Option<&typed::Described>,
) -> PublicValue {
	let time = format!("{:?}", result.time);
	let rows = match result.result {
		Ok(PublicValue::Array(rows)) => rows.into_vec(),
		Ok(PublicValue::None | PublicValue::Null) => Vec::new(),
		Ok(other) => vec![other],
		Err(err) => return engine_error_entry(&err, dialect),
	};
	let kind = typed::kind_of(described);
	let count = rows.len();
	let (columns, shaped) = if typed::answers_with_rows(described) {
		typed::shape(rows, described.and_then(|described| described.columns.as_deref()))
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
/// which is not so when a statement failed (the entries before the failure were rolled back).
pub(super) fn response_atomic(statements: Vec<PublicValue>, committed: bool) -> PublicValue {
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
			assert!(matches!(control(text, Dialect::PgSql), Some(Control::Begin)), "{text}");
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
