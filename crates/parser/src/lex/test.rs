use super::*;
use logos::Logos;

fn assert_tokens(source: &str, expect: &[BaseTokenKind]) {
	let tokens: Vec<_> = BaseTokenKind::lexer(source.as_bytes()).map(|x| x.unwrap()).collect();
	assert_eq!(tokens, expect)
}

macro_rules! impl_test {
	($name:ident: $source:expr => $expected:expr) => {
		#[test]
		fn $name() {
			assert_tokens($source, $expected);
		}
	};
}

impl_test!(brace: "{" => &[BaseTokenKind::OpenBrace]);
impl_test!(plus: "+" => &[BaseTokenKind::Plus]);

impl_test!(basic_ident: "hello_world" => &[BaseTokenKind::Ident]);

impl_test!(kw_select_lower: "select" => &[BaseTokenKind::KwSelect]);
impl_test!(kw_select_upper: "SELECT" => &[BaseTokenKind::KwSelect]);
impl_test!(kw_select_mixed: "SeLeCt" => &[BaseTokenKind::KwSelect]);

impl_test!(basic_number: "1" => &[BaseTokenKind::Int]);

impl_test!(basic_float: "1.0" => &[BaseTokenKind::Float]);
impl_test!(float_exponent: "1.0e2" => &[BaseTokenKind::Float]);
impl_test!(float_exponent_neg: "1.0e-2" => &[BaseTokenKind::Float]);
impl_test!(float_exponent_pos: "1.0e+3" => &[BaseTokenKind::Float]);
impl_test!(float_suffix: "2f" => &[BaseTokenKind::Float]);

impl_test!(basic_decimal: "2dec" => &[BaseTokenKind::Decimal]);
impl_test!(decimal_mantissa: "2.0dec" => &[BaseTokenKind::Decimal]);
impl_test!(decimal_exponent: "2e1dec" => &[BaseTokenKind::Decimal]);
impl_test!(decimal_mantissa_exponent: "2.0e1dec" => &[BaseTokenKind::Decimal]);
impl_test!(decimal_mantissa_exponent_neg: "2.0e-1dec" => &[BaseTokenKind::Decimal]);
impl_test!(decimal_mantissa_exponent_pos: "2.0e+1dec" => &[BaseTokenKind::Decimal]);

impl_test!(basic_add: "1 + 2" => &[BaseTokenKind::Int, BaseTokenKind::Plus, BaseTokenKind::Int]);

impl_test!(whitespace: r#"

/* bla */
// foo
# bla
-- bla
"# => &[]);

impl_test!(whitespace_intersperse: r#"

/* bla */ 1
// foo
# bla
-- bla
"# => &[BaseTokenKind::Int]);
impl_test!(whitespace_intersperse_2: r#"

/* bla */
// foo
# bla
1
-- bla
"# => &[BaseTokenKind::Int]);

fn assert_returns_utf8_error(source: &[u8]) {
	assert!(std::str::from_utf8(source).is_err());
	let mut tokens = Vec::new();
	for t in BaseTokenKind::lexer(source) {
		match t {
			Err(LexError::InvalidUtf8(..)) => return,
			t => {
				tokens.push(t);
			}
		}
	}
	panic!("Source did not return a utf8 error: {:?}", tokens)
}

#[test]
fn no_invalid_utf8() {
	assert_returns_utf8_error(&[b'"', 129, b'"']);
	assert_returns_utf8_error(&[b'h', b'e', b'l', b'l', b'o', 0b11101111]);
}
