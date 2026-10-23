//! Connection-scoped Grand Unified Configuration (GUC) parameter store.
//!
//! Implements PostgreSQL runtime configuration parameters, `SET [LOCAL|SESSION]`,
//! `SHOW`, `RESET [ALL]`, savepoint snapshotting, transaction rollback, and
//! status parameter change reporting.

use std::collections::HashMap;

use super::error::PgError;

/// A single GUC parameter entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GucEntry {
	pub name: String,
	pub default_value: String,
	pub current_value: String,
	pub description: String,
	pub is_status_param: bool,
	pub is_readonly: bool,
	pub is_ignored: bool,
}

/// Result of evaluating a `SHOW` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShowResult {
	Single {
		name: String,
		value: String,
	},
	All(Vec<GucEntry>),
}

#[derive(Debug, Clone)]
struct SavepointSnapshot {
	name: String,
	locals: Option<HashMap<String, String>>,
	entries: HashMap<String, GucEntry>,
	session_overrides: HashMap<String, String>,
}

/// Canonical GUC parameter store for a client connection.
#[derive(Debug, Clone)]
pub(crate) struct GucStore {
	/// Known configuration parameters (keyed by canonical lowercase name).
	entries: HashMap<String, GucEntry>,
	/// Session-level overrides for custom placeholder parameters (e.g. `myapp.user_id`).
	session_overrides: HashMap<String, String>,
	/// Transaction-local overrides (active during an interactive transaction block).
	txn_locals: Option<HashMap<String, String>>,
	/// Savepoint snapshots: stack of `SavepointSnapshot`.
	savepoint_snapshots: Vec<SavepointSnapshot>,
	/// Snapshot of session state taken at transaction begin.
	txn_begin_snapshot: Option<(HashMap<String, GucEntry>, HashMap<String, String>)>,
}

impl Default for GucStore {
	fn default() -> Self {
		Self::new()
	}
}

impl GucStore {
	/// Create a new `GucStore` preloaded with standard PostgreSQL configuration parameters.
	pub(crate) fn new() -> Self {
		let mut store = Self {
			entries: HashMap::new(),
			session_overrides: HashMap::new(),
			txn_locals: None,
			savepoint_snapshots: Vec::new(),
			txn_begin_snapshot: None,
		};
		store.populate_defaults();
		store
	}

	fn register_full(
		&mut self,
		name: &str,
		default: &str,
		description: &str,
		is_status: bool,
		is_readonly: bool,
		is_ignored: bool,
	) {
		let norm = normalize_name(name);
		self.entries.insert(
			norm,
			GucEntry {
				name: name.to_string(),
				default_value: default.to_string(),
				current_value: default.to_string(),
				description: description.to_string(),
				is_status_param: is_status,
				is_readonly,
				is_ignored,
			},
		);
	}

	fn register(
		&mut self,
		name: &str,
		default: &str,
		description: &str,
		is_status: bool,
		is_readonly: bool,
	) {
		self.register_full(name, default, description, is_status, is_readonly, false);
	}

	fn register_ignored(&mut self, name: &str, default: &str, description: &str) {
		self.register_full(name, default, description, false, false, true);
	}

	fn populate_defaults(&mut self) {
		// Read-only parameters (55P02 on attempt to change)
		self.register("server_version", "16.0-surrealdb", "Shows the server version.", true, true);
		self.register(
			"server_version_num",
			"160000",
			"Shows the numeric server version.",
			false,
			true,
		);
		self.register(
			"server_encoding",
			"UTF8",
			"Shows the server (database) character set encoding.",
			true,
			true,
		);
		self.register(
			"integer_datetimes",
			"on",
			"Shows whether datetimes are 64-bit integer based.",
			true,
			true,
		);
		self.register(
			"is_superuser",
			"on",
			"Shows whether current user is a superuser.",
			true,
			true,
		);

		// Status parameters reported during startup and upon changes
		self.register(
			"client_encoding",
			"UTF8",
			"Sets the client's character set encoding.",
			true,
			false,
		);
		self.register(
			"DateStyle",
			"ISO, MDY",
			"Sets the display format for date and time values.",
			true,
			false,
		);
		self.register(
			"standard_conforming_strings",
			"on",
			"Causes '...' to treat backslashes literally in strings.",
			true,
			false,
		);
		self.register(
			"TimeZone",
			"UTC",
			"Sets the time zone for displaying and interpreting time stamps.",
			true,
			false,
		);
		self.register(
			"application_name",
			"",
			"Sets the application name to be reported in statistics and logs.",
			true,
			false,
		);
		self.register("session_authorization", "root", "Sets the session user name.", true, false);

		// Other common standard PostgreSQL parameters
		self.register(
			"search_path",
			"\"$user\", public",
			"Sets the schema search order for names that are not schema-qualified.",
			false,
			false,
		);
		self.register(
			"extra_float_digits",
			"1",
			"Sets the number of digits displayed for floating-point values.",
			false,
			false,
		);
		self.register(
			"statement_timeout",
			"0",
			"Sets the maximum allowed duration of any statement.",
			false,
			false,
		);
		self.register(
			"client_min_messages",
			"notice",
			"Sets the message levels that are sent to the client.",
			false,
			false,
		);
		self.register("bytea_output", "hex", "Sets the output format for bytea.", false, false);
		self.register(
			"intervalstyle",
			"postgres",
			"Sets the display format for interval values.",
			false,
			false,
		);
		self.register(
			"max_connections",
			"100",
			"Sets the maximum number of concurrent connections.",
			false,
			true,
		);
		self.register(
			"transaction_isolation",
			"repeatable read",
			"Sets the current transaction's isolation level.",
			false,
			false,
		);
		self.register(
			"transaction_read_only",
			"off",
			"Sets the current transaction's read-only status.",
			false,
			false,
		);
		self.register(
			"default_transaction_isolation",
			"repeatable read",
			"Sets the default isolation level for transactions.",
			false,
			false,
		);
		self.register(
			"default_transaction_read_only",
			"off",
			"Sets the default read-only status for transactions.",
			false,
			false,
		);
		self.register(
			"session_replication_role",
			"origin",
			"Sets the session's behavior for triggers and rewrite rules.",
			false,
			false,
		);
		self.register(
			"idle_in_transaction_session_timeout",
			"0",
			"Sets the maximum allowed duration of any idling transaction.",
			false,
			false,
		);
		self.register(
			"lock_timeout",
			"0",
			"Sets the maximum allowed duration of any wait for a lock.",
			false,
			false,
		);
		self.register("row_security", "on", "Enables row security.", false, false);
		self.register_ignored(
			"check_function_bodies",
			"on",
			"Check function bodies during CREATE FUNCTION.",
		);
		self.register_ignored(
			"default_tablespace",
			"",
			"Sets the default tablespace to create tables and indexes in.",
		);
		self.register_ignored(
			"gin_fuzzy_search_limit",
			"0",
			"Sets the maximum allowed result for exact match by GIN index.",
		);
		self.register_ignored(
			"session_replication_role",
			"origin",
			"Sets the session's behavior for triggers and rewrite rules.",
		);
		self.register_ignored(
			"synchronize_seqscans",
			"on",
			"Enable synchronized sequential scans.",
		);
		self.register_ignored("geqo", "on", "Enable genetic query optimization.");
		self.register_ignored(
			"cursor_tuple_fraction",
			"0.1",
			"Sets the planner's estimate of the fraction of a cursor's rows that will be retrieved.",
		);
		self.register("role", "none", "Sets the current role.", false, false);
	}

	/// Return whether the parameter is recognised as an ignored compatibility parameter.
	pub fn is_ignored(&self, name: &str) -> bool {
		let norm = normalize_name(name);
		self.entries.get(&norm).is_some_and(|e| e.is_ignored)
	}

	/// Retrieve the active effective value for a parameter (taking transaction-local
	/// overrides into account).
	pub fn get(&self, name: &str) -> Option<&str> {
		let norm = normalize_name(name);
		if let Some(locals) = &self.txn_locals
			&& let Some(val) = locals.get(&norm)
		{
			return Some(val.as_str());
		}
		if let Some(val) = self.session_overrides.get(&norm) {
			return Some(val.as_str());
		}
		if let Some(entry) = self.entries.get(&norm) {
			return Some(entry.current_value.as_str());
		}
		None
	}

	/// Set a configuration parameter.
	///
	/// If the parameter is a status parameter and its effective value changes, returns
	/// `Some((canonical_name, new_value))` so the caller can emit a `ParameterStatus` packet.
	pub fn set(
		&mut self,
		name: &str,
		value: &str,
		is_local: bool,
		in_transaction: bool,
	) -> Result<Option<(&'static str, String)>, PgError> {
		let norm = normalize_name(name);
		let clean_val = strip_quotes(value.trim());

		// Previous effective value for detecting status packet emission
		let prev_val = self.get(&norm).map(str::to_string);

		// Handle DEFAULT value reset
		if clean_val.eq_ignore_ascii_case("DEFAULT") {
			if is_local && in_transaction {
				if let Some(locals) = &mut self.txn_locals {
					locals.remove(&norm);
				}
			} else {
				self.session_overrides.remove(&norm);
				if let Some(entry) = self.entries.get_mut(&norm) {
					entry.current_value = entry.default_value.clone();
				}
			}
			let new_val = self.get(&norm).unwrap_or("").to_string();
			if let Some(canonical) = canonical_status_param(&norm)
				&& prev_val.as_deref() != Some(&new_val)
			{
				return Ok(Some((canonical, new_val)));
			}
			return Ok(None);
		}

		// Validate parameter and value
		if let Some(entry) = self.entries.get(&norm) {
			if entry.is_readonly {
				return Err(PgError::cant_change_runtime_param(format!(
					"parameter \"{}\" cannot be changed",
					entry.name
				)));
			}
			// Value validation
			match norm.as_str() {
				"bytea_output"
					if !clean_val.eq_ignore_ascii_case("hex")
						&& !clean_val.eq_ignore_ascii_case("escape") =>
				{
					return Err(PgError::invalid_parameter_value(format!(
						"invalid value for parameter \"bytea_output\": \"{clean_val}\""
					)));
				}
				"standard_conforming_strings" | "row_security" | "check_function_bodies"
					if !is_boolean_str(clean_val) =>
				{
					return Err(PgError::invalid_parameter_value(format!(
						"parameter \"{}\" requires a Boolean value",
						entry.name
					)));
				}
				_ => {}
			}
		} else if !norm.contains('.') {
			// Unrecognized parameter that is not a placeholder
			return Err(PgError::undefined_object(format!(
				"unrecognized configuration parameter \"{name}\""
			)));
		}

		let normalized_val = normalize_boolean_if_bool(clean_val);

		// Apply value according to scope
		if is_local {
			if in_transaction {
				let locals = self.txn_locals.get_or_insert_with(HashMap::new);
				locals.insert(norm.clone(), normalized_val);
			}
			// When run outside a transaction, SET LOCAL has no lasting effect in PostgreSQL.
		} else {
			// Session-wide setting clears transaction-local override for this parameter
			if let Some(locals) = &mut self.txn_locals {
				locals.remove(&norm);
			}
			if let Some(entry) = self.entries.get_mut(&norm) {
				entry.current_value = normalized_val;
			} else {
				self.session_overrides.insert(norm.clone(), normalized_val);
			}
		}

		let new_val = self.get(&norm).unwrap_or("").to_string();
		if let Some(canonical) = canonical_status_param(&norm)
			&& prev_val.as_deref() != Some(&new_val)
		{
			return Ok(Some((canonical, new_val)));
		}

		Ok(None)
	}

	/// Reset a configuration parameter to its default value, or reset all parameters.
	///
	/// Returns any status parameter updates that occurred as a result.
	pub fn reset(&mut self, name: &str) -> Result<Vec<(&'static str, String)>, PgError> {
		if name.eq_ignore_ascii_case("ALL") {
			let mut changed = Vec::new();
			let previous_status: Vec<(&'static str, Option<String>)> = STATUS_PARAMS
				.iter()
				.map(|&(canon, norm)| (canon, self.get(norm).map(str::to_string)))
				.collect();

			self.session_overrides.clear();
			if let Some(locals) = &mut self.txn_locals {
				locals.clear();
			}
			for entry in self.entries.values_mut() {
				if !entry.is_readonly {
					entry.current_value = entry.default_value.clone();
				}
			}

			for (canon, old_val) in previous_status {
				let new_val = self.get(canon).unwrap_or("");
				if old_val.as_deref() != Some(new_val) {
					changed.push((canon, new_val.to_string()));
				}
			}
			return Ok(changed);
		}

		let norm = normalize_name(name);
		let prev_val = self.get(&norm).map(str::to_string);

		if let Some(entry) = self.entries.get_mut(&norm) {
			if entry.is_readonly {
				return Err(PgError::cant_change_runtime_param(format!(
					"parameter \"{}\" cannot be changed",
					entry.name
				)));
			}
			if let Some(locals) = &mut self.txn_locals {
				locals.remove(&norm);
			}
			self.session_overrides.remove(&norm);
			entry.current_value = entry.default_value.clone();

			let new_val = entry.current_value.clone();
			if let Some(canonical) = canonical_status_param(&norm)
				&& prev_val.as_deref() != Some(&new_val)
			{
				return Ok(vec![(canonical, new_val)]);
			}
			return Ok(vec![]);
		}

		if norm.contains('.') {
			if let Some(locals) = &mut self.txn_locals {
				locals.remove(&norm);
			}
			self.session_overrides.remove(&norm);
			return Ok(vec![]);
		}

		Err(PgError::undefined_object(format!("unrecognized configuration parameter \"{name}\"")))
	}

	/// Show current setting for a parameter, or all parameters (`SHOW ALL`).
	pub fn show(&self, name: &str) -> Result<ShowResult, PgError> {
		if name.eq_ignore_ascii_case("ALL") {
			let mut list = Vec::new();
			for entry in self.entries.values() {
				let current = self.get(&entry.name).unwrap_or(&entry.current_value);
				let mut e = entry.clone();
				e.current_value = current.to_string();
				list.push(e);
			}
			for (k, v) in &self.session_overrides {
				if !self.entries.contains_key(k) {
					list.push(GucEntry {
						name: k.clone(),
						default_value: String::new(),
						current_value: self.get(k).unwrap_or(v).to_string(),
						description: "Custom session variable.".to_string(),
						is_status_param: false,
						is_readonly: false,
						is_ignored: false,
					});
				}
			}
			list.sort_by(|a, b| a.name.cmp(&b.name));
			Ok(ShowResult::All(list))
		} else {
			let norm = normalize_name(name);
			if let Some(val) = self.get(&norm) {
				Ok(ShowResult::Single {
					name: norm,
					value: val.to_string(),
				})
			} else {
				Err(PgError::undefined_object(format!(
					"unrecognized configuration parameter \"{name}\""
				)))
			}
		}
	}

	/// Notify that an interactive transaction block has begun (`BEGIN`).
	pub(crate) fn begin_transaction(&mut self) {
		self.txn_locals = Some(HashMap::new());
		self.savepoint_snapshots.clear();
		self.txn_begin_snapshot = Some((self.entries.clone(), self.session_overrides.clone()));
	}

	/// Notify that an interactive transaction block has committed (`COMMIT`).
	pub(crate) fn commit_transaction(&mut self) {
		self.txn_locals = None;
		self.savepoint_snapshots.clear();
		self.txn_begin_snapshot = None;
	}

	/// Notify that an interactive transaction block has aborted (`ROLLBACK`).
	///
	/// Reverts all transaction-local GUC overrides and session changes made inside
	/// the transaction block, and returns any status parameters that reverted.
	pub(crate) fn rollback_transaction(&mut self) -> Vec<(&'static str, String)> {
		let prev_status: Vec<(&'static str, Option<String>)> = STATUS_PARAMS
			.iter()
			.map(|&(canon, norm)| (canon, self.get(norm).map(str::to_string)))
			.collect();

		self.txn_locals = None;
		self.savepoint_snapshots.clear();
		if let Some((entries_snap, session_snap)) = self.txn_begin_snapshot.take() {
			self.entries = entries_snap;
			self.session_overrides = session_snap;
		}

		let mut changed = Vec::new();
		for (canon, old_val) in prev_status {
			let new_val = self.get(canon).unwrap_or("").to_string();
			if old_val.as_deref() != Some(&new_val) {
				changed.push((canon, new_val));
			}
		}

		changed
	}

	/// Snapshot current transaction-local GUC overrides and session entries at savepoint creation.
	pub(crate) fn savepoint(&mut self, name: &str) {
		let snap_locals = self.txn_locals.clone();
		self.savepoint_snapshots.push(SavepointSnapshot {
			name: name.to_lowercase(),
			locals: snap_locals,
			entries: self.entries.clone(),
			session_overrides: self.session_overrides.clone(),
		});
	}

	/// Rollback transaction-local GUC overrides and session changes to a previous savepoint
	/// snapshot.
	pub(crate) fn rollback_to_savepoint(
		&mut self,
		name: &str,
	) -> Result<Vec<(&'static str, String)>, PgError> {
		let name_lower = name.to_lowercase();
		if let Some(pos) = self.savepoint_snapshots.iter().rposition(|s| s.name == name_lower) {
			let prev_status: Vec<(&'static str, Option<String>)> = STATUS_PARAMS
				.iter()
				.map(|&(canon, norm)| (canon, self.get(norm).map(str::to_string)))
				.collect();

			let snap = self.savepoint_snapshots[pos].clone();
			self.savepoint_snapshots.truncate(pos + 1);

			self.txn_locals = snap.locals;
			self.entries = snap.entries;
			self.session_overrides = snap.session_overrides;

			let mut changed = Vec::new();
			for (canon, old_val) in prev_status {
				let new_val = self.get(canon).unwrap_or("").to_string();
				if old_val.as_deref() != Some(&new_val) {
					changed.push((canon, new_val));
				}
			}

			Ok(changed)
		} else {
			Err(PgError::invalid_savepoint(format!("savepoint \"{name}\" does not exist")))
		}
	}

	/// Release a savepoint and discard its snapshot.
	pub(crate) fn release_savepoint(&mut self, name: &str) -> Result<(), PgError> {
		let name_lower = name.to_lowercase();
		if let Some(pos) = self.savepoint_snapshots.iter().rposition(|s| s.name == name_lower) {
			self.savepoint_snapshots.truncate(pos);
			Ok(())
		} else {
			Err(PgError::invalid_savepoint(format!("savepoint \"{name}\" does not exist")))
		}
	}

	/// List of standard status parameters to emit during connection startup.
	pub fn startup_status_params(&self) -> Vec<(&'static str, &str)> {
		STATUS_PARAMS.iter().map(|&(canon, norm)| (canon, self.get(norm).unwrap_or(""))).collect()
	}
}

/// Normalize parameter names (lowercase, space to underscore, aliases).
pub fn normalize_name(name: &str) -> String {
	let lower = name.trim().to_ascii_lowercase();
	match lower.as_str() {
		"timezone" | "time zone" => "timezone".to_string(),
		"names" => "client_encoding".to_string(),
		other => other.to_string(),
	}
}

fn strip_quotes(s: &str) -> &str {
	if (s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2)
		|| (s.starts_with('"') && s.ends_with('"') && s.len() >= 2)
	{
		&s[1..s.len() - 1]
	} else {
		s
	}
}

fn is_boolean_str(s: &str) -> bool {
	matches!(
		s.to_ascii_lowercase().as_str(),
		"on" | "off" | "true" | "false" | "yes" | "no" | "1" | "0"
	)
}

fn normalize_boolean_if_bool(s: &str) -> String {
	match s.to_ascii_lowercase().as_str() {
		"on" | "true" | "yes" | "1" => "on".to_string(),
		"off" | "false" | "no" | "0" => "off".to_string(),
		_ => s.to_string(),
	}
}

const STATUS_PARAMS: &[(&str, &str)] = &[
	("server_version", "server_version"),
	("server_encoding", "server_encoding"),
	("client_encoding", "client_encoding"),
	("DateStyle", "datestyle"),
	("integer_datetimes", "integer_datetimes"),
	("standard_conforming_strings", "standard_conforming_strings"),
	("TimeZone", "timezone"),
	("application_name", "application_name"),
	("is_superuser", "is_superuser"),
	("session_authorization", "session_authorization"),
];

fn canonical_status_param(norm: &str) -> Option<&'static str> {
	STATUS_PARAMS.iter().find(|&&(_, n)| n == norm).map(|&(canon, _)| canon)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_guc_defaults_and_get() {
		let store = GucStore::new();
		assert_eq!(store.get("timezone"), Some("UTC"));
		assert_eq!(store.get("TIME ZONE"), Some("UTC"));
		assert_eq!(store.get("client_encoding"), Some("UTF8"));
		assert_eq!(store.get("server_version"), Some("16.0-surrealdb"));
	}

	#[test]
	fn test_guc_set_and_readonly() {
		let mut store = GucStore::new();
		let changed = store.set("timezone", "'Asia/Tokyo'", false, false).unwrap();
		assert_eq!(changed, Some(("TimeZone", "Asia/Tokyo".to_string())));
		assert_eq!(store.get("timezone"), Some("Asia/Tokyo"));

		// Readonly rejection
		let err = store.set("server_version", "15.0", false, false).unwrap_err();
		assert_eq!(err.code, "55P02");
	}

	#[test]
	fn test_guc_transaction_local_rollback() {
		let mut store = GucStore::new();
		store.set("timezone", "UTC", false, false).unwrap();

		// Begin transaction
		store.begin_transaction();
		let changed = store.set("timezone", "America/New_York", true, true).unwrap();
		assert_eq!(changed, Some(("TimeZone", "America/New_York".to_string())));
		assert_eq!(store.get("timezone"), Some("America/New_York"));

		// Rollback transaction restores session value and reports status param update
		let reverted = store.rollback_transaction();
		assert_eq!(reverted, vec![("TimeZone", "UTC".to_string())]);
		assert_eq!(store.get("timezone"), Some("UTC"));
	}

	#[test]
	fn test_guc_savepoint_rollback() {
		let mut store = GucStore::new();
		store.set("timezone", "UTC", false, false).unwrap();

		store.begin_transaction();
		store.savepoint("sp1");

		store.set("timezone", "Europe/London", true, true).unwrap();
		assert_eq!(store.get("timezone"), Some("Europe/London"));

		let reverted = store.rollback_to_savepoint("sp1").unwrap();
		assert_eq!(reverted, vec![("TimeZone", "UTC".to_string())]);
		assert_eq!(store.get("timezone"), Some("UTC"));

		store.commit_transaction();
	}

	#[test]
	fn test_guc_show_and_reset() {
		let mut store = GucStore::new();
		match store.show("timezone").unwrap() {
			ShowResult::Single {
				name,
				value,
			} => {
				assert_eq!(name, "timezone");
				assert_eq!(value, "UTC");
			}
			_ => panic!("expected single show result"),
		}

		match store.show("ALL").unwrap() {
			ShowResult::All(entries) => {
				assert!(entries.len() > 10);
			}
			_ => panic!("expected all show result"),
		}

		store.set("timezone", "Asia/Tokyo", false, false).unwrap();
		let reset_status = store.reset("timezone").unwrap();
		assert_eq!(reset_status, vec![("TimeZone", "UTC".to_string())]);
		assert_eq!(store.get("timezone"), Some("UTC"));
	}
}
