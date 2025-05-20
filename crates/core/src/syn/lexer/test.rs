use crate::syn::token::{TokenKind, t};

macro_rules! test_case(
	($source:expr_2021 => [$($token:expr_2021),*$(,)?]) => {
		let mut lexer = crate::syn::lexer::Lexer::new($source.as_bytes());
		let mut i = 0;
		$(
			let next = lexer.next();
			if let Some(next) = next {
				let span = std::str::from_utf8(lexer.reader.span(next.span)).unwrap_or("invalid utf8");
				if let TokenKind::Invalid = next.kind{
					let error = lexer.error.take().unwrap();
					assert_eq!(next.kind, $token, "{} = {}:{} => {:?}",span, i, stringify!($token), error);
				}else{
					assert_eq!(next.kind, $token, "{} = {}:{}", span, i, stringify!($token));
				}
			}else{
				assert_eq!(next,None);
			}
			i += 1;
		)*
		let _ = i;
		assert_eq!(lexer.next(),None)
	};
);

#[test]
fn operators() {
	test_case! {
		r#"- + / * ! **
           < > <= >= <- <-> ->
           = == -= += != +?=
           ? ?? ?: ?~ ?=
           { } [ ] ( )
           ; , | || & &&
		   $
           . .. ...

           ^
    "# => [
			t!("-"), t!(" "), t!("+"),t!(" "),  t!("/"),t!(" "),  t!("*"),t!(" "),  t!("!"),t!(" "),  t!("**"), t!(" "),

			t!("<"), t!(" "), t!(">"), t!(" "), t!("<="), t!(" "), t!(">="), t!(" "), t!("<"), t!("-"), t!(" "), t!("<"), t!("->"), t!(" "), t!("->"), t!(" "),

			t!("="), t!(" "), t!("=="), t!(" "), t!("-="), t!(" "), t!("+="), t!(" "), t!("!="), t!(" "), t!("+?="), t!(" "),

			t!("?"), t!(" "), t!("??"), t!(" "), t!("?:"), t!(" "), t!("?~"), t!(" "), t!("?="), t!(" "),

			t!("{"), t!(" "), t!("}"), t!(" "), t!("["), t!(" "), t!("]"), t!(" "), t!("("), t!(" "), t!(")"), t!(" "),

			t!(";"), t!(" "), t!(","), t!(" "), t!("|"), t!(" "), t!("||"), t!(" "), TokenKind::Invalid, t!(" "), t!("&&"), t!(" "),

			t!("$"), t!(" "),

			t!("."), t!(" "), t!(".."), t!(" "), t!("..."), t!(" "),

			TokenKind::Invalid, t!(" ")
		]
	}
}

#[test]
fn comments() {
	test_case! {
		r"
			+ /* some comment */
			- // another comment
			+ -- a third comment
			-
		" => [
			t!(" "), t!("+"), t!(" "), t!(" "), t!(" "),
			t!("-"), t!(" "), t!(" "), t!(" "),
			t!("+"), t!(" "), t!(" "), t!(" "),
			t!("-"), t!(" ")
		]
	}
}

#[test]
fn whitespace() {
	test_case! {
		"+= \t\n\r -=" => [
			t!("+="), t!(" "),
			t!("-="),
		]
	}
}

#[test]
fn identifiers() {
	test_case! {
		r#"
			123123adwad+akdwkj+akdwkj1231312313123+_a_k_d_wkj1231312313123+____wdw____+
		"#
			=> [
			t!(" "),
			TokenKind::Digits, // 123123
			TokenKind::Identifier, // adwad
			t!("+"),
			TokenKind::Identifier, // akdwkj
			t!("+"),
			TokenKind::Identifier, // akdwkj1231312313123
			t!("+"),
			TokenKind::Identifier, // _a_k_d_wkj1231312313123
			t!("+"),
			TokenKind::Identifier, // ____wdw____
			t!("+"),
			t!(" "),
		]
	}
}

#[test]
fn keyword() {
	test_case! {
		r#"select SELECT sElEcT"# => [
			t!("SELECT"),t!(" "),
			t!("SELECT"),t!(" "),
			t!("SELECT"),
		]
	}
}

#[test]
fn ident_angle_with_escape_char() {
	test_case! {
		r#"⟨⟨something\⟩⟩"# => [
			TokenKind::Identifier,
		]
	}
}
