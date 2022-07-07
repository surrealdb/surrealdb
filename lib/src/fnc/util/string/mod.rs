use deunicode::deunicode;
use once_cell::sync::Lazy;
use regex::Regex;

static SIMPLES: Lazy<Regex> = Lazy::new(|| Regex::new("[^a-z0-9-_]").unwrap());
static HYPHENS: Lazy<Regex> = Lazy::new(|| Regex::new("-+").unwrap());

pub fn slug<S: AsRef<str>>(s: S) -> String {
	// Get a reference
	let s = s.as_ref();
	// Convert unicode to ascii
	let s = deunicode(s);
	// Convert string to lowercase
	let s = s.to_ascii_lowercase();
	// Replace any non-simple characters
	let s = SIMPLES.replace_all(s.as_ref(), "-");
	// Replace any duplicated hyphens
	let s = HYPHENS.replace_all(s.as_ref(), "-");
	// Remove any surrounding hyphens
	let s = s.trim_matches('-');
	// Return the string
	s.to_owned()
}
