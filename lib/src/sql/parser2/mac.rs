macro_rules! unexpected {
	($parser:expr, $found:expr, $expected:expr) => {
		return Err($crate::sql::parser2::ParseError {
			kind: $crate::sql::parser2::ParseErrorKind::Unexpected {
				found: $found,
				expected: $expected,
			},
			at: $parser.last_span(),
		});
	};
}

macro_rules! expected {
	($parser:expr, $kind:tt) => {
		match $parser.next_token().kind {
			t!($kind) => {}
			x => {
				let expected = $crate::sql::parser2::Expected::from(t!($kind));
				unexpected!($parser, x, expected);
			}
		}
	};
}

macro_rules! to_do {
	($parser:expr) => {
		return Err($crate::sql::parser2::ParseError {
			kind: $crate::sql::parser2::ParseErrorKind::Todo,
			at: $parser.last_span(),
		})
	};
}

pub(super) use expected;
pub(super) use to_do;
pub(super) use unexpected;
