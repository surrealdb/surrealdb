//! MySQL SQL Modes representation and parsing.

use std::fmt;
use std::str::FromStr;

/// Set of MySQL SQL modes that control syntax and semantic behaviors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlMode {
	pub ansi_quotes: bool,
	pub no_backslash_escapes: bool,
	pub pipes_as_concat: bool,
	pub ignore_space: bool,
	pub only_full_group_by: bool,
	pub strict_trans_tables: bool,
	pub strict_all_tables: bool,
	pub no_zero_in_date: bool,
	pub no_zero_date: bool,
	pub error_for_division_by_zero: bool,
	pub no_engine_substitution: bool,
	pub pad_char_to_full_length: bool,
	pub real_as_float: bool,
}

impl Default for SqlMode {
	fn default() -> Self {
		Self::default_mysql_8()
	}
}

impl SqlMode {
	/// Empty SQL mode with no flags enabled.
	pub const fn empty() -> Self {
		Self {
			ansi_quotes: false,
			no_backslash_escapes: false,
			pipes_as_concat: false,
			ignore_space: false,
			only_full_group_by: false,
			strict_trans_tables: false,
			strict_all_tables: false,
			no_zero_in_date: false,
			no_zero_date: false,
			error_for_division_by_zero: false,
			no_engine_substitution: false,
			pad_char_to_full_length: false,
			real_as_float: false,
		}
	}

	/// Default MySQL 8.0+ SQL mode:
	/// ONLY_FULL_GROUP_BY, STRICT_TRANS_TABLES, NO_ZERO_IN_DATE, NO_ZERO_DATE,
	/// ERROR_FOR_DIVISION_BY_ZERO, NO_ENGINE_SUBSTITUTION
	pub const fn default_mysql_8() -> Self {
		Self {
			ansi_quotes: false,
			no_backslash_escapes: false,
			pipes_as_concat: false,
			ignore_space: false,
			only_full_group_by: true,
			strict_trans_tables: true,
			strict_all_tables: false,
			no_zero_in_date: true,
			no_zero_date: true,
			error_for_division_by_zero: true,
			no_engine_substitution: true,
			pad_char_to_full_length: false,
			real_as_float: false,
		}
	}

	/// MySQL ANSI mode:
	/// REAL_AS_FLOAT, PIPES_AS_CONCAT, ANSI_QUOTES, IGNORE_SPACE, ONLY_FULL_GROUP_BY
	pub const fn ansi() -> Self {
		Self {
			ansi_quotes: true,
			no_backslash_escapes: false,
			pipes_as_concat: true,
			ignore_space: true,
			only_full_group_by: true,
			strict_trans_tables: false,
			strict_all_tables: false,
			no_zero_in_date: false,
			no_zero_date: false,
			error_for_division_by_zero: false,
			no_engine_substitution: false,
			pad_char_to_full_length: false,
			real_as_float: true,
		}
	}

	/// MySQL TRADITIONAL mode:
	/// STRICT_TRANS_TABLES, STRICT_ALL_TABLES, NO_ZERO_IN_DATE, NO_ZERO_DATE,
	/// ERROR_FOR_DIVISION_BY_ZERO, NO_ENGINE_SUBSTITUTION
	pub const fn traditional() -> Self {
		Self {
			ansi_quotes: false,
			no_backslash_escapes: false,
			pipes_as_concat: false,
			ignore_space: false,
			only_full_group_by: false,
			strict_trans_tables: true,
			strict_all_tables: true,
			no_zero_in_date: true,
			no_zero_date: true,
			error_for_division_by_zero: true,
			no_engine_substitution: true,
			pad_char_to_full_length: false,
			real_as_float: false,
		}
	}

	pub const fn with_ansi_quotes(mut self, val: bool) -> Self {
		self.ansi_quotes = val;
		self
	}

	pub const fn with_no_backslash_escapes(mut self, val: bool) -> Self {
		self.no_backslash_escapes = val;
		self
	}

	pub const fn with_pipes_as_concat(mut self, val: bool) -> Self {
		self.pipes_as_concat = val;
		self
	}

	pub const fn with_ignore_space(mut self, val: bool) -> Self {
		self.ignore_space = val;
		self
	}

	/// Parse comma-separated MySQL mode string.
	pub fn from_mode_str(s: &str) -> Self {
		let mut mode = Self::empty();
		for part in s.split(',') {
			let trimmed = part.trim();
			if trimmed.is_empty() {
				continue;
			}
			match trimmed.to_ascii_uppercase().as_str() {
				"ANSI" => {
					mode.ansi_quotes = true;
					mode.pipes_as_concat = true;
					mode.ignore_space = true;
					mode.only_full_group_by = true;
					mode.real_as_float = true;
				}
				"TRADITIONAL" => {
					mode.strict_trans_tables = true;
					mode.strict_all_tables = true;
					mode.no_zero_in_date = true;
					mode.no_zero_date = true;
					mode.error_for_division_by_zero = true;
					mode.no_engine_substitution = true;
				}
				"ANSI_QUOTES" => mode.ansi_quotes = true,
				"NO_BACKSLASH_ESCAPES" => mode.no_backslash_escapes = true,
				"PIPES_AS_CONCAT" => mode.pipes_as_concat = true,
				"IGNORE_SPACE" => mode.ignore_space = true,
				"ONLY_FULL_GROUP_BY" => mode.only_full_group_by = true,
				"STRICT_TRANS_TABLES" => mode.strict_trans_tables = true,
				"STRICT_ALL_TABLES" => mode.strict_all_tables = true,
				"NO_ZERO_IN_DATE" => mode.no_zero_in_date = true,
				"NO_ZERO_DATE" => mode.no_zero_date = true,
				"ERROR_FOR_DIVISION_BY_ZERO" => mode.error_for_division_by_zero = true,
				"NO_ENGINE_SUBSTITUTION" => mode.no_engine_substitution = true,
				"PAD_CHAR_TO_FULL_LENGTH" => mode.pad_char_to_full_length = true,
				"REAL_AS_FLOAT" => mode.real_as_float = true,
				_ => {}
			}
		}
		mode
	}

	/// Format active modes as a standard comma-separated MySQL string.
	pub fn format(&self) -> String {
		let mut parts = Vec::new();
		if self.only_full_group_by {
			parts.push("ONLY_FULL_GROUP_BY");
		}
		if self.strict_trans_tables {
			parts.push("STRICT_TRANS_TABLES");
		}
		if self.strict_all_tables {
			parts.push("STRICT_ALL_TABLES");
		}
		if self.no_zero_in_date {
			parts.push("NO_ZERO_IN_DATE");
		}
		if self.no_zero_date {
			parts.push("NO_ZERO_DATE");
		}
		if self.error_for_division_by_zero {
			parts.push("ERROR_FOR_DIVISION_BY_ZERO");
		}
		if self.no_engine_substitution {
			parts.push("NO_ENGINE_SUBSTITUTION");
		}
		if self.ansi_quotes {
			parts.push("ANSI_QUOTES");
		}
		if self.no_backslash_escapes {
			parts.push("NO_BACKSLASH_ESCAPES");
		}
		if self.pipes_as_concat {
			parts.push("PIPES_AS_CONCAT");
		}
		if self.ignore_space {
			parts.push("IGNORE_SPACE");
		}
		if self.pad_char_to_full_length {
			parts.push("PAD_CHAR_TO_FULL_LENGTH");
		}
		if self.real_as_float {
			parts.push("REAL_AS_FLOAT");
		}
		parts.join(",")
	}
}

impl FromStr for SqlMode {
	type Err = std::convert::Infallible;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Ok(Self::from_mode_str(s))
	}
}

impl fmt::Display for SqlMode {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.format())
	}
}
