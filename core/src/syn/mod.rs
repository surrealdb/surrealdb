//! Module containing the implementation of the surrealql tokens, lexer, and parser.

pub mod common;
pub mod error;

// #[cfg(not(feature = "experimental-parser"))]
// pub mod v1;
// #[cfg(not(feature = "experimental-parser"))]
// pub use v1::{datetime_raw, duration, idiom, json, parse, range, subquery, thing, value};

#[cfg(feature = "experimental-parser")]
pub mod v2;
#[cfg(feature = "experimental-parser")]
pub use v2::{
	datetime_raw, duration, idiom, json, json_legacy_strand, parse, range, subquery, thing, value,
	value_legacy_strand,
};

#[cfg(test)]
pub trait Parse<T> {
	fn parse(val: &str) -> T;
}

#[cfg(test)]
mod test {
	use super::parse;

	#[test]
	fn test_error_in_lineterminator() {
		let q = r#"
select * from person
CREATE person CONTENT { foo:'bar'};
"#;
		parse(q).unwrap_err();
	}
}
