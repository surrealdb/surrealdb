//! SurrealQL reserved-keyword predicate.
//!
//! Lives here, below both the parser and the formatter, because each needs it
//! for opposite reasons: the lexer classifies these words as keywords, and the
//! formatter must quote an identifier that collides with one so the rendered
//! query reparses to the same tree.

use phf::phf_set;
use unicase::UniCase;

/// A set of keywords which might in some contexts are dissallowed as an
/// identifier.
pub static RESERVED_KEYWORD: phf::Set<UniCase<&'static str>> = phf_set! {
	UniCase::ascii("ALTER"),
	UniCase::ascii("BEGIN"),
	UniCase::ascii("BREAK"),
	UniCase::ascii("CANCEL"),
	UniCase::ascii("COMMIT"),
	UniCase::ascii("CONTINUE"),
	UniCase::ascii("CREATE"),
	UniCase::ascii("DEFINE"),
	UniCase::ascii("DELETE"),
	UniCase::ascii("FOR"),
	UniCase::ascii("IF"),
	UniCase::ascii("INFO"),
	UniCase::ascii("INSERT"),
	UniCase::ascii("KILL"),
	UniCase::ascii("LIVE"),
	UniCase::ascii("OPTION"),
	UniCase::ascii("REBUILD"),
	UniCase::ascii("RETURN"),
	UniCase::ascii("RELATE"),
	UniCase::ascii("REMOVE"),
	UniCase::ascii("SELECT"),
	UniCase::ascii("LET"),
	UniCase::ascii("SHOW"),
	UniCase::ascii("SLEEP"),
	UniCase::ascii("THROW"),
	UniCase::ascii("UPDATE"),
	UniCase::ascii("UPSERT"),
	UniCase::ascii("USE"),
	UniCase::ascii("DIFF"),
	UniCase::ascii("RAND"),
	UniCase::ascii("NONE"),
	UniCase::ascii("NULL"),
	UniCase::ascii("AFTER"),
	UniCase::ascii("BEFORE"),
	UniCase::ascii("VALUE"),
	UniCase::ascii("BY"),
	UniCase::ascii("ALL"),
	UniCase::ascii("TRUE"),
	UniCase::ascii("FALSE"),
	UniCase::ascii("WHERE"),
	UniCase::ascii("TABLE"),
	UniCase::ascii("TB"),
	UniCase::ascii("SEQUENCE"),
	UniCase::ascii("FUNCTION"),
};

pub fn could_be_reserved(s: &str) -> bool {
	RESERVED_KEYWORD.contains(&UniCase::ascii(s))
}
