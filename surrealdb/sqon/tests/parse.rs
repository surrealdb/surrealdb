//! Tests parsing SQON source text into `surrealdb_types::Value` and other typed values
//! through the public `surrealdb_sqon::from_str` entry point.

use std::ops::Bound;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use surrealdb_sqon::{Error, Kind, ValueKind};
use surrealdb_types::{
	Array, Bytes, File, Number, Object, Range, RecordId, RecordIdKey, RecordIdKeyRange, Set, Table,
	Value,
};

#[track_caller]
fn parse(source: &str) -> Value {
	surrealdb_sqon::from_str::<Value>(source)
		.unwrap_or_else(|e| panic!("failed to parse `{source}`: {e}"))
}

#[track_caller]
fn parse_err(source: &str) -> Error {
	match surrealdb_sqon::from_str::<Value>(source) {
		Ok(x) => panic!("expected `{source}` to fail to parse, got {x:?}"),
		Err(e) => e,
	}
}

fn int(i: i64) -> Value {
	Value::Number(Number::Int(i))
}

fn float(f: f64) -> Value {
	Value::Number(Number::Float(f))
}

fn string(s: &str) -> Value {
	Value::String(s.to_string())
}

fn array<I: IntoIterator<Item = Value>>(items: I) -> Value {
	Value::Array(Array::from(items.into_iter().collect::<Vec<_>>()))
}

fn object<'a, I: IntoIterator<Item = (&'a str, Value)>>(items: I) -> Value {
	Value::Object(Object::from_iter(items.into_iter().map(|(k, v)| (k.to_string(), v))))
}

fn set<I: IntoIterator<Item = Value>>(items: I) -> Value {
	let mut res = Set::new();
	for item in items {
		res.insert(item);
	}
	Value::Set(res)
}

fn record_id(table: &str, key: RecordIdKey) -> Value {
	Value::RecordId(RecordId {
		table: Table::new(table),
		key,
	})
}

fn range(start: Bound<Value>, end: Bound<Value>) -> Value {
	Value::Range(Box::new(Range {
		start,
		end,
	}))
}

fn datetime(s: &str) -> Value {
	Value::Datetime(
		DateTime::parse_from_rfc3339(s)
			.expect("test datetime should be valid rfc3339")
			.with_timezone(&Utc)
			.into(),
	)
}

#[test]
fn keywords() {
	assert_eq!(parse("null"), Value::Null);
	assert_eq!(parse("NULL"), Value::Null);
	assert_eq!(parse("NuLl"), Value::Null);
	assert_eq!(parse("none"), Value::None);
	assert_eq!(parse("NONE"), Value::None);
	assert_eq!(parse("true"), Value::Bool(true));
	assert_eq!(parse("TRUE"), Value::Bool(true));
	assert_eq!(parse("false"), Value::Bool(false));
	assert_eq!(parse("FaLsE"), Value::Bool(false));
}

#[test]
fn integers() {
	assert_eq!(parse("0"), int(0));
	assert_eq!(parse("123"), int(123));
	assert_eq!(parse("+123"), int(123));
	assert_eq!(parse("-123"), int(-123));
	assert_eq!(parse("1_000_000"), int(1_000_000));
	assert_eq!(parse("9223372036854775807"), int(i64::MAX));
	assert_eq!(parse("-9223372036854775808"), int(i64::MIN));

	parse_err("9223372036854775808");
	parse_err("-9223372036854775809");
}

#[test]
fn floats() {
	assert_eq!(parse("1.5"), float(1.5));
	assert_eq!(parse("-2.5"), float(-2.5));
	assert_eq!(parse("1f"), float(1.0));
	assert_eq!(parse("1.5f"), float(1.5));
	assert_eq!(parse("1e5"), float(1e5));
	assert_eq!(parse("1.5e-3"), float(1.5e-3));
	assert_eq!(parse("1.5E2f"), float(150.0));
	assert_eq!(parse("1_000.5"), float(1000.5));

	assert_eq!(parse("Infinity"), float(f64::INFINITY));
	assert_eq!(parse("+Infinity"), float(f64::INFINITY));
	assert_eq!(parse("-Infinity"), float(f64::NEG_INFINITY));

	let Value::Number(Number::Float(nan)) = parse("NaN") else {
		panic!("expected NaN to parse to a float");
	};
	assert!(nan.is_nan());
}

#[test]
fn decimals() {
	let dec = |s: &str| Value::Number(Number::Decimal(s.parse::<Decimal>().unwrap()));

	assert_eq!(parse("1.5dec"), dec("1.5"));
	assert_eq!(parse("-1.5dec"), dec("-1.5"));
	assert_eq!(parse("42dec"), dec("42"));
	assert_eq!(parse("1_000dec"), dec("1000"));
	assert_eq!(parse("1.5e2dec"), dec("150"));
	assert_eq!(parse("0.30000000000000004dec"), dec("0.30000000000000004"));
}

#[test]
fn durations() {
	let dur = |d: StdDuration| Value::Duration(d.into());

	assert_eq!(parse("1ns"), dur(StdDuration::from_nanos(1)));
	assert_eq!(parse("1us"), dur(StdDuration::from_micros(1)));
	assert_eq!(parse("1µs"), dur(StdDuration::from_micros(1)));
	assert_eq!(parse("1ms"), dur(StdDuration::from_millis(1)));
	assert_eq!(parse("1s"), dur(StdDuration::from_secs(1)));
	assert_eq!(parse("1m"), dur(StdDuration::from_secs(60)));
	assert_eq!(parse("1h"), dur(StdDuration::from_secs(60 * 60)));
	assert_eq!(parse("1d"), dur(StdDuration::from_secs(24 * 60 * 60)));
	assert_eq!(parse("1w"), dur(StdDuration::from_secs(7 * 24 * 60 * 60)));
	assert_eq!(parse("1y"), dur(StdDuration::from_secs(365 * 24 * 60 * 60)));
	assert_eq!(parse("1h30m"), dur(StdDuration::from_secs(90 * 60)));
	assert_eq!(parse("1m1s"), dur(StdDuration::from_secs(61)));

	// Total duration overflows the seconds counter.
	parse_err("600000000000y");
}

#[test]
fn strings() {
	assert_eq!(parse(r#""hello""#), string("hello"));
	assert_eq!(parse("'hello'"), string("hello"));
	assert_eq!(parse(r#"s"hello""#), string("hello"));
	assert_eq!(parse("s'hello'"), string("hello"));
	assert_eq!(parse(r#""""#), string(""));
	assert_eq!(parse("''"), string(""));
	assert_eq!(parse(r#""it's""#), string("it's"));
	assert_eq!(parse(r#"'say "hi"'"#), string(r#"say "hi""#));
	assert_eq!(parse("\"multi\nline\""), string("multi\nline"));
}

#[test]
fn string_escapes() {
	assert_eq!(parse(r#""a\nb""#), string("a\nb"));
	assert_eq!(parse(r#""a\tb""#), string("a\tb"));
	assert_eq!(parse(r#""a\\b""#), string("a\\b"));
	assert_eq!(parse(r#""a\"b""#), string("a\"b"));
	assert_eq!(parse(r#"'a\'b'"#), string("a'b"));
	assert_eq!(parse(r#""\u0041""#), string("A"));
	assert_eq!(parse(r#""\u{41}""#), string("A"));
	assert_eq!(parse(r#""\u{1F600}""#), string("😀"));
	// A UTF-16 surrogate pair.
	assert_eq!(parse(r#""\uD83D\uDE00""#), string("😀"));

	// A lone surrogate is invalid.
	parse_err(r#""\uD800""#);
	parse_err(r#""\q""#);
}

#[test]
fn unterminated_string() {
	parse_err(r#""hello"#);
	parse_err("'hello");
	parse_err(r#"s"hello"#);
	parse_err(r#"d"2020-01-01"#);
}

#[test]
fn datetimes() {
	assert_eq!(parse(r#"d"2020-01-01T00:00:00Z""#), datetime("2020-01-01T00:00:00Z"));
	assert_eq!(parse("d'2023-11-28T11:41:20.262Z'"), datetime("2023-11-28T11:41:20.262Z"));
	assert_eq!(parse(r#"d"2020-01-01""#), datetime("2020-01-01T00:00:00Z"));
	assert_eq!(parse(r#"d"2020-01-01T10:00:00+02:00""#), datetime("2020-01-01T10:00:00+02:00"));

	parse_err(r#"d"2020-13-01""#);
	parse_err(r#"d"not a datetime""#);
}

#[test]
fn uuids() {
	let expected =
		Value::Uuid(uuid::Uuid::parse_str("8f7f8b21-2f77-4f0a-9b6a-8d6f9a8c8f21").unwrap().into());
	assert_eq!(parse(r#"u"8f7f8b21-2f77-4f0a-9b6a-8d6f9a8c8f21""#), expected);
	assert_eq!(parse("u'8f7f8b21-2f77-4f0a-9b6a-8d6f9a8c8f21'"), expected);

	parse_err(r#"u"8f7f8b21-2f77-4f0a-9b6a""#);
	parse_err(r#"u"not a uuid""#);
}

#[test]
fn bytes() {
	assert_eq!(parse(r#"b"""#), Value::Bytes(Bytes::from(Vec::new())));
	assert_eq!(parse(r#"b"CAFE01""#), Value::Bytes(Bytes::from(vec![0xCA, 0xFE, 0x01])));
	assert_eq!(parse("b'cafe01'"), Value::Bytes(Bytes::from(vec![0xCA, 0xFE, 0x01])));

	// Odd number of hex digits.
	parse_err(r#"b"CAF""#);
	parse_err(r#"b"ZZ""#);
}

#[test]
fn files() {
	assert_eq!(
		parse(r#"f"bucket:/path/to.txt""#),
		Value::File(File::new("bucket", "/path/to.txt"))
	);
	assert_eq!(parse("f'my-bucket.prod:/x'"), Value::File(File::new("my-bucket.prod", "/x")));
	assert_eq!(parse(r#"f"bucket:/""#), Value::File(File::new("bucket", "/")));

	// Missing the `:/` separator.
	parse_err(r#"f"bucket""#);
	parse_err(r#"f"bucket:x""#);
	// Invalid character in the bucket name.
	parse_err(r#"f"bu ck:/x""#);
	// Invalid character in the key.
	parse_err(r#"f"bucket:/a b""#);
}

#[test]
fn arrays() {
	assert_eq!(parse("[]"), array([]));
	assert_eq!(parse("[ ]"), array([]));
	assert_eq!(parse("[1]"), array([int(1)]));
	assert_eq!(parse("[1, 2, 3]"), array([int(1), int(2), int(3)]));
	assert_eq!(parse("[1, 2, 3,]"), array([int(1), int(2), int(3)]));
	assert_eq!(parse("[[1], [2]]"), array([array([int(1)]), array([int(2)])]));
	assert_eq!(parse("[[], 1]"), array([array([]), int(1)]));
	assert_eq!(parse("[1, 'a', true]"), array([int(1), string("a"), Value::Bool(true)]));

	parse_err("[1 2]");
	parse_err("[1,");
	parse_err("[");
}

#[test]
fn objects() {
	assert_eq!(parse("{}"), object([]));
	assert_eq!(parse("{ }"), object([]));
	assert_eq!(parse("{a: 1}"), object([("a", int(1))]));
	assert_eq!(parse("{a: 1,}"), object([("a", int(1))]));
	assert_eq!(parse("{a: 1, b: 2}"), object([("a", int(1)), ("b", int(2))]));
	assert_eq!(parse(r#"{"a b": 1}"#), object([("a b", int(1))]));
	assert_eq!(parse("{'a': 1}"), object([("a", int(1))]));
	assert_eq!(parse("{`weird key`: 1}"), object([("weird key", int(1))]));
	assert_eq!(parse("{⟨another key⟩: 1}"), object([("another key", int(1))]));
	assert_eq!(parse("{a: {b: 1}}"), object([("a", object([("b", int(1))]))]));
	assert_eq!(parse("{a: [1]}"), object([("a", array([int(1)]))]));
	// A later duplicate key overwrites the earlier one.
	assert_eq!(parse("{a: 1, a: 2}"), object([("a", int(2))]));

	parse_err("{a: 1 b: 2}");
	parse_err("{a: }");
	parse_err("{a}");
	parse_err("{");
}

#[test]
fn object_number_and_keyword_keys() {
	assert_eq!(parse("{00: 0}"), object([("00", int(0))]));
	assert_eq!(parse("{123: 1}"), object([("123", int(1))]));
	assert_eq!(parse("{true: 1}"), object([("true", int(1))]));
	assert_eq!(parse("{null: 1}"), object([("null", int(1))]));
	assert_eq!(parse("{none: 1}"), object([("none", int(1))]));
	assert_eq!(parse("{NaN: 1}"), object([("NaN", int(1))]));
	assert_eq!(parse("{Infinity: 1}"), object([("Infinity", int(1))]));
}

#[test]
fn sets() {
	assert_eq!(parse("{,}"), set([]));
	assert_eq!(parse("{1,}"), set([int(1)]));
	assert_eq!(parse("{1, 2, 3}"), set([int(1), int(2), int(3)]));
	assert_eq!(parse("{1, 2, 3,}"), set([int(1), int(2), int(3)]));
	assert_eq!(parse("{'a', 'b'}"), set([string("a"), string("b")]));
	assert_eq!(parse(r#"{"a",}"#), set([string("a")]));
	assert_eq!(parse("{s'a', s'b'}"), set([string("a"), string("b")]));
	assert_eq!(parse("{[1], [2]}"), set([array([int(1)]), array([int(2)])]));
	assert_eq!(parse("{{,}, {a: 1}}"), set([set([]), object([("a", int(1))])]));
	assert_eq!(parse("{1h, 2h}"), set([parse("1h"), parse("2h")]));
	assert_eq!(parse("{d'2020-01-01', 1}"), set([datetime("2020-01-01T00:00:00Z"), int(1)]));
	// A duplicate element is only included once.
	assert_eq!(parse("{1, 1}"), set([int(1)]));

	parse_err("{,");
	parse_err("{, 1}");
	parse_err("{1, 2");
}

#[test]
fn record_ids() {
	assert_eq!(parse("person:tobie"), record_id("person", RecordIdKey::String("tobie".into())));
	assert_eq!(parse("person:123"), record_id("person", RecordIdKey::Number(123)));
	assert_eq!(parse("person:-123"), record_id("person", RecordIdKey::Number(-123)));
	assert_eq!(
		parse("person:⟨complex id⟩"),
		record_id("person", RecordIdKey::String("complex id".into()))
	);
	assert_eq!(
		parse("person:`complex id`"),
		record_id("person", RecordIdKey::String("complex id".into()))
	);
	assert_eq!(parse("⟨weird table⟩:1"), record_id("weird table", RecordIdKey::Number(1)));
	assert_eq!(
		parse(r#"person:u"8f7f8b21-2f77-4f0a-9b6a-8d6f9a8c8f21""#),
		record_id(
			"person",
			RecordIdKey::Uuid(
				uuid::Uuid::parse_str("8f7f8b21-2f77-4f0a-9b6a-8d6f9a8c8f21").unwrap().into()
			)
		)
	);
	assert_eq!(
		parse(r#"person:'string key'"#),
		record_id("person", RecordIdKey::String("string key".into()))
	);
	assert_eq!(parse("person:true"), record_id("person", RecordIdKey::String("true".into())));
	assert_eq!(
		parse("person:[1, 'a']"),
		record_id("person", RecordIdKey::Array(Array::from(vec![int(1), string("a")])))
	);
	assert_eq!(
		parse("person:{a: 1}"),
		record_id("person", RecordIdKey::Object(Object::from_iter([("a".to_string(), int(1))])))
	);

	parse_err("person:");
	parse_err("person:1.5");
}

#[test]
fn record_id_flexible_keys() {
	let key = |s: &str| record_id("a", RecordIdKey::String(s.into()));

	assert_eq!(parse("a:0123456789abcdef"), key("0123456789abcdef"));
	assert_eq!(parse("a:12345_5678abcdef"), key("12345_5678abcdef"));
	assert_eq!(parse("a:0NaN"), key("0NaN"));
	assert_eq!(parse("a:0Infinity"), key("0Infinity"));
	assert_eq!(parse("a:NaN"), key("NaN"));
	assert_eq!(parse("a:Infinity"), key("Infinity"));
	assert_eq!(parse("a:1h"), key("1h"));
	assert_eq!(parse("a:222j"), key("222j"));
	assert_eq!(parse("a:1e5"), key("1e5"));
	// These lex as error tokens due to the ident-after-number guards.
	assert_eq!(parse("a:4fO"), key("4fO"));
	assert_eq!(parse("a:1h2x"), key("1h2x"));
	assert_eq!(parse(r#"r"a:4fO""#), key("4fO"));
	// Integer keys overflowing an i64 fall back to string keys, keeping the sign.
	assert_eq!(parse("a:10000000000000000000000"), key("10000000000000000000000"));
	assert_eq!(parse("a:+10000000000000000000000"), key("+10000000000000000000000"));
	assert_eq!(parse("a:-10000000000000000000000"), key("-10000000000000000000000"));

	// A signed number with a trailing ident is not a valid flexible ident.
	parse_err("a:-10n");
	parse_err("a:+1e5x");
}

#[test]
fn record_id_strings() {
	assert_eq!(
		parse(r#"r"person:tobie""#),
		record_id("person", RecordIdKey::String("tobie".into()))
	);
	assert_eq!(parse("r'person:123'"), record_id("person", RecordIdKey::Number(123)));
	assert_eq!(parse(r#"r"person:⟨x y⟩""#), record_id("person", RecordIdKey::String("x y".into())));

	parse_err(r#"r"person""#);
	parse_err(r#"r"person:""#);
	parse_err(r#"r"person:1 garbage""#);
	parse_err(r#"r"1:1""#);
}

#[test]
fn record_id_ranges() {
	let key_range = |table: &str, start: Bound<RecordIdKey>, end: Bound<RecordIdKey>| {
		record_id(
			table,
			RecordIdKey::Range(Box::new(RecordIdKeyRange {
				start,
				end,
			})),
		)
	};

	assert_eq!(
		parse("person:1..5"),
		key_range(
			"person",
			Bound::Included(RecordIdKey::Number(1)),
			Bound::Excluded(RecordIdKey::Number(5))
		)
	);
	assert_eq!(
		parse("person:1..=5"),
		key_range(
			"person",
			Bound::Included(RecordIdKey::Number(1)),
			Bound::Included(RecordIdKey::Number(5))
		)
	);
	assert_eq!(
		parse("person:1>..5"),
		key_range(
			"person",
			Bound::Excluded(RecordIdKey::Number(1)),
			Bound::Excluded(RecordIdKey::Number(5))
		)
	);
	assert_eq!(
		parse("person:1>..=5"),
		key_range(
			"person",
			Bound::Excluded(RecordIdKey::Number(1)),
			Bound::Included(RecordIdKey::Number(5))
		)
	);
	assert_eq!(parse("person:.."), key_range("person", Bound::Unbounded, Bound::Unbounded));
	assert_eq!(
		parse("person:..5"),
		key_range("person", Bound::Unbounded, Bound::Excluded(RecordIdKey::Number(5)))
	);
	assert_eq!(
		parse("person:..=5"),
		key_range("person", Bound::Unbounded, Bound::Included(RecordIdKey::Number(5)))
	);
	assert_eq!(
		parse("person:1.."),
		key_range("person", Bound::Included(RecordIdKey::Number(1)), Bound::Unbounded)
	);
	assert_eq!(
		parse("person:tobie..=zoe"),
		key_range(
			"person",
			Bound::Included(RecordIdKey::String("tobie".into())),
			Bound::Included(RecordIdKey::String("zoe".into()))
		)
	);
}

#[test]
fn ranges() {
	assert_eq!(parse(".."), range(Bound::Unbounded, Bound::Unbounded));
	assert_eq!(parse("1.."), range(Bound::Included(int(1)), Bound::Unbounded));
	assert_eq!(parse("..5"), range(Bound::Unbounded, Bound::Excluded(int(5))));
	assert_eq!(parse("..=5"), range(Bound::Unbounded, Bound::Included(int(5))));
	assert_eq!(parse("1..5"), range(Bound::Included(int(1)), Bound::Excluded(int(5))));
	assert_eq!(parse("1..=5"), range(Bound::Included(int(1)), Bound::Included(int(5))));
	assert_eq!(parse("1>..5"), range(Bound::Excluded(int(1)), Bound::Excluded(int(5))));
	assert_eq!(parse("1>..=5"), range(Bound::Excluded(int(1)), Bound::Included(int(5))));
	assert_eq!(
		parse("'a'..'b'"),
		range(Bound::Included(string("a")), Bound::Excluded(string("b")))
	);
}

#[test]
fn nested_ranges() {
	assert_eq!(
		parse("[1..2, ..]"),
		array([
			range(Bound::Included(int(1)), Bound::Excluded(int(2))),
			range(Bound::Unbounded, Bound::Unbounded),
		])
	);
	assert_eq!(
		parse("{a: 1..}"),
		object([("a", range(Bound::Included(int(1)), Bound::Unbounded))])
	);
	assert_eq!(parse("{1..2,}"), set([range(Bound::Included(int(1)), Bound::Excluded(int(2)))]));
	assert_eq!(parse("{..2, 1}"), set([range(Bound::Unbounded, Bound::Excluded(int(2))), int(1)]));
}

#[test]
fn comments_and_whitespace() {
	assert_eq!(parse("// comment\n1"), int(1));
	assert_eq!(parse("1 // trailing"), int(1));
	assert_eq!(parse("/* before */ 1 /* after */"), int(1));
	assert_eq!(parse("/* body ends in a star **/ 1"), int(1));
	assert_eq!(parse("/*** stars everywhere ***/ 1"), int(1));
	assert_eq!(parse("# comment\ntrue"), Value::Bool(true));
	assert_eq!(parse("-- comment\nnull"), Value::Null);
	assert_eq!(parse("[1, # comment\n2]"), array([int(1), int(2)]));
	assert_eq!(parse("\u{00A0}1\u{3000}"), int(1));
	assert_eq!(parse("\t\r\n 1 \n"), int(1));
	assert_eq!(parse("\u{000B}\u{000C}1"), int(1));
}

#[test]
fn trailing_tokens() {
	parse_err("1 2");
	parse_err("[1] 2");
	parse_err("{} {}");
	parse_err("1..2 3");
	parse_err("null null");
}

#[test]
fn unexpected_tokens() {
	parse_err("");
	parse_err("   ");
	parse_err(":");
	parse_err("}");
	parse_err(",");
	parse_err("]");
	parse_err("..=");
	parse_err("foo");
}

#[test]
fn recursion_limit() {
	let mut deep = "[".repeat(500);
	deep.push('1');
	deep.push_str(&"]".repeat(500));
	assert_eq!(parse_err(&deep), Error::RecursionLimit);

	let mut ok = "[".repeat(100);
	ok.push('1');
	ok.push_str(&"]".repeat(100));
	parse(&ok);
}

#[test]
fn recursion_limit_record_id_keys() {
	// Chained record id key range bounds.
	let mut deep = String::from("a:");
	deep.push_str(&"..=".repeat(500));
	deep.push('1');
	assert_eq!(parse_err(&deep), Error::RecursionLimit);

	// Record ids nested through array keys.
	let mut deep = "a:[".repeat(500);
	deep.push('1');
	deep.push_str(&"]".repeat(500));
	assert_eq!(parse_err(&deep), Error::RecursionLimit);

	// Record ids nested through object keys.
	let mut deep = "a:{x: ".repeat(500);
	deep.push('1');
	deep.push_str(&"}".repeat(500));
	assert_eq!(parse_err(&deep), Error::RecursionLimit);
}

#[test]
fn typed_deserialization() {
	assert_eq!(surrealdb_sqon::from_str::<i64>("42").unwrap(), 42);
	assert_eq!(surrealdb_sqon::from_str::<f64>("1.5").unwrap(), 1.5);
	assert!(surrealdb_sqon::from_str::<bool>("true").unwrap());
	assert_eq!(surrealdb_sqon::from_str::<String>("'hi'").unwrap(), "hi");
	assert_eq!(surrealdb_sqon::from_str::<Vec<i64>>("[1, 2, 3]").unwrap(), vec![1, 2, 3]);
	assert_eq!(
		surrealdb_sqon::from_str::<Vec<Vec<i64>>>("[[1], [2]]").unwrap(),
		vec![vec![1], vec![2]]
	);
	assert_eq!(surrealdb_sqon::from_str::<StdDuration>("90s").unwrap(), StdDuration::from_secs(90));
	assert_eq!(
		surrealdb_sqon::from_str::<Decimal>("1.5dec").unwrap(),
		"1.5".parse::<Decimal>().unwrap()
	);
	assert_eq!(
		surrealdb_sqon::from_str::<DateTime<Utc>>(r#"d"2020-01-01T00:00:00Z""#).unwrap(),
		DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z").unwrap().with_timezone(&Utc)
	);
}

#[test]
fn typed_bounds() {
	let bounds = |s: &str| surrealdb_sqon::from_str::<(Bound<i64>, Bound<i64>)>(s);

	assert_eq!(bounds("1..5").unwrap(), (Bound::Included(1), Bound::Excluded(5)));
	assert_eq!(bounds("1..=5").unwrap(), (Bound::Included(1), Bound::Included(5)));
	assert_eq!(bounds("1>..5").unwrap(), (Bound::Excluded(1), Bound::Excluded(5)));
	assert_eq!(bounds("1>..=5").unwrap(), (Bound::Excluded(1), Bound::Included(5)));
	assert_eq!(bounds("..").unwrap(), (Bound::Unbounded, Bound::Unbounded));
	assert_eq!(bounds("..=3").unwrap(), (Bound::Unbounded, Bound::Included(3)));
	assert_eq!(bounds("1..").unwrap(), (Bound::Included(1), Bound::Unbounded));

	assert_eq!(
		surrealdb_sqon::from_str::<(Bound<String>, Bound<String>)>("'a'..'b'").unwrap(),
		(Bound::Included("a".to_string()), Bound::Excluded("b".to_string()))
	);

	// Composes with other generic impls.
	assert_eq!(
		surrealdb_sqon::from_str::<Vec<(Bound<i64>, Bound<i64>)>>("[1..2, ..]").unwrap(),
		vec![(Bound::Included(1), Bound::Excluded(2)), (Bound::Unbounded, Bound::Unbounded)]
	);

	// A plain value is not a range.
	bounds("1").unwrap_err();
	// The start bound is parsed with the element type's visitor.
	let Error::UnexpectedType {
		found,
		expected,
	} = bounds("'a'..2").unwrap_err()
	else {
		panic!("expected an UnexpectedType error");
	};
	assert_eq!(found, Kind::Value(ValueKind::String));
	assert_eq!(expected, "an integer");
}

#[test]
fn typed_mismatches() {
	let Error::UnexpectedType {
		found,
		expected,
	} = surrealdb_sqon::from_str::<i64>("'x'").unwrap_err()
	else {
		panic!("expected an UnexpectedType error");
	};
	assert_eq!(found, Kind::Value(ValueKind::String));
	assert_eq!(expected, "an integer");

	// The start bound parses as a bool before the range operator is discovered.
	let Error::UnexpectedType {
		found,
		..
	} = surrealdb_sqon::from_str::<bool>("true..false").unwrap_err()
	else {
		panic!("expected an UnexpectedType error");
	};
	assert_eq!(found, Kind::Value(ValueKind::Range));

	let Error::UnexpectedType {
		found,
		..
	} = surrealdb_sqon::from_str::<Vec<i64>>("{1,}").unwrap_err()
	else {
		panic!("expected an UnexpectedType error");
	};
	assert_eq!(found, Kind::Value(ValueKind::Set));
}
