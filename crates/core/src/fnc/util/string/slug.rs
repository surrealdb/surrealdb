use std::sync::LazyLock;

use deunicode::deunicode;
use regex::Regex;

static ALLOWED: LazyLock<Regex> = LazyLock::new(|| Regex::new("[^a-z0-9-_]").unwrap());
static HYPHENS: LazyLock<Regex> = LazyLock::new(|| Regex::new("-+").unwrap());

pub fn slug<S: AsRef<str>>(s: S) -> String {
	// Get a reference
	let s = s.as_ref();
	// Convert unicode to ascii
	let mut s = deunicode(s);
	// Convert string to lowercase
	s.make_ascii_lowercase();
	// Replace any non-simple characters
	let s = ALLOWED.replace_all(s.as_ref(), "-");
	// Replace any duplicated hyphens
	let s = HYPHENS.replace_all(s.as_ref(), "-");
	// Remove any surrounding hyphens
	let s = s.trim_matches('-');
	// Return the string
	s.to_owned()
}
