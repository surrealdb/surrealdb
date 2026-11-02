//! The text form of the expression dialect's attributes.
//!
//! An op prints as its mnemonic, then, for a tag with attributes, one space and
//! `{key: value, ...}` with the keys in payload field order. Printing, parsing and printing again
//! yields the text printed first, for every tag and every payload variant. Parsing accepts the
//! attribute text with or without the leading mnemonic, and answers a [`ParseError`], never a
//! default op, for text it cannot read.
//!
//! A value is one of:
//! - a string, written as a SurrealQL string literal and read back with the language's parser;
//! - an integer;
//! - a list, `[value, ...]`;
//! - a map, `{key: value, ...}`;
//! - a word, the name of an enum variant, or `true`, `false` and `none`;
//! - a word followed by one value, a variant that carries data: `Number 1`, `some "t"`.
//!
//! A [`Kind`] is written as the string literal of its canonical text. A binary operator is written
//! as the string literal of its SurrealQL spelling, except the two that carry fields. A literal's
//! value is the text of the value when the language's parser reads that text back to a value with
//! the same `revision` encoding; otherwise it is `bytes:` and the hex of the encoding. Whether a
//! value takes the `bytes:` form is decided by that round trip alone, never by its variant.

#![deny(clippy::wildcard_enum_match_arm)]

use std::ops::Bound;
use std::sync::{Arc, OnceLock};

use surrealdb_strand::{Strand, TableName};
use surrealdb_types::ToSql;

use super::ops::{
	CallAttrs, CalleeFacts, CalleePermission, ClosureAttrs, ComputedReadAttrs, ConvertAttrs,
	ConvertMode, ConvertSite, DefinedCallee, DestructureEntry, DispatchTarget, ExprOp, ExprTag,
	FoldedError, KeyBound, KeyRange, KeyShape, MisplacedControl, NestedStrategy, NowWhich, OnFail,
	RaiseKind, RecordIdAttrs, ShortCircuitOp,
};
use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::function::FunctionRegistry;
use crate::exec::guard_verdicts::GuardScope;
use crate::exec::ir::core::parse::ParseError;
use crate::exec::ir::core::print::AttrSyntax;
use crate::exec::ir::iface::statements::{Place, PlacePart, UnaddressedPart};
use crate::exec::ir::iface::{CatalogObjectKey, IndexRef};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::logical::footprint::CatalogKind;
use crate::exec::logical::pipelines::AggregateCombine;
use crate::exec::vm::compile_cache::ObjectVersion;
use crate::exec::vm::payload::Refusal;
use crate::expr::index::Distance;
use crate::expr::logical::LogicalPostfixOperator;
use crate::expr::operator::{BooleanOperator, MatchesOperator, NearestNeighbor};
use crate::expr::{
	BinaryOperator, Constant, Function, Kind, Mock, Model, PrefixOperator, RecordIdKeyGen, Script,
};
use crate::types::{PublicNumber, PublicValue};
use crate::val::convert_public::convert_public_value_to_internal;
use crate::val::range::TypedRange;
use crate::val::{Number, Uuid, Value};

/// The expression dialect's attribute syntax.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ExprSyntax;

impl AttrSyntax for ExprSyntax {
	fn print_attrs(&self, kind: &OpKind, out: &mut String) {
		out.push_str(kind.tag().mnemonic());
		if let OpKind::Expr(op) = kind {
			print_op(op, out);
		}
	}

	fn parse_attrs(&self, tag: OpTag, text: &str) -> Result<OpKind, ParseError> {
		let OpTag::Expr(tag) = tag else {
			return Err(error(format!("{} is not an expression tag", tag.mnemonic())));
		};
		let body = strip_mnemonic(tag, text)?;
		parse_op(tag, body).map(OpKind::Expr)
	}
}

fn error(detail: impl std::fmt::Display) -> ParseError {
	ParseError {
		line: 0,
		detail: format!("expr::parse_attrs: {detail}"),
	}
}

/// The registry that names the aggregates an `agg` op may hold. An aggregate's name is a
/// `&'static str` owned by its registered function, so parsing resolves the text there.
fn aggregates() -> &'static FunctionRegistry {
	static REGISTRY: OnceLock<FunctionRegistry> = OnceLock::new();
	REGISTRY.get_or_init(FunctionRegistry::with_aggregates)
}

// ---------------------------------------------------------------------------------------------
// Enum tables. One row per variant, from a match that names every variant, so a variant added to
// an enum fails to compile here until it has a row. The parse direction is built from the same
// rows.
// ---------------------------------------------------------------------------------------------

/// A table for an enum whose variants carry no data.
macro_rules! word_table {
	($print:ident, $parse:ident, $ty:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
		fn $print(value: &$ty) -> &'static str {
			match value {
				$($ty::$variant => $text),+
			}
		}

		fn $parse(text: &str) -> Option<$ty> {
			match text {
				$($text => Some($ty::$variant),)+
				_ => None,
			}
		}
	};
}

/// A table for an enum some of whose variants carry data. The `data` patterns name those
/// variants, which have no row: the print answers `None` for them.
macro_rules! token_table {
	(
		$print:ident, $parse:ident, $ty:ident {
			$($variant:ident => $text:literal),+ $(,)?
		} data $($data:pat),+ $(,)?
	) => {
		fn $print(value: &$ty) -> Option<&'static str> {
			match value {
				$($ty::$variant => Some($text),)+
				$($data => None,)+
			}
		}

		fn $parse(text: &str) -> Option<$ty> {
			match text {
				$($text => Some($ty::$variant),)+
				_ => None,
			}
		}
	};
}

word_table!(convert_mode_text, convert_mode_from, ConvertMode {
	Cast => "Cast",
	Coerce => "Coerce",
});

word_table!(unaddressed_text, unaddressed_from, UnaddressedPart {
	Lookup => "Lookup",
	Destructure => "Destructure",
	Recurse => "Recurse",
	Start => "Start",
	Method => "Method",
	Filter => "Filter",
});

word_table!(on_fail_text, on_fail_from, OnFail {
	Error => "Error",
	None => "None",
});

word_table!(short_circuit_text, short_circuit_from, ShortCircuitOp {
	And => "And",
	Or => "Or",
	NullCoalescing => "NullCoalescing",
	TernaryCondition => "TernaryCondition",
});

word_table!(now_which_text, now_which_from, NowWhich {
	Datetime => "Datetime",
	UnixSeconds => "UnixSeconds",
	UnixMillis => "UnixMillis",
	UnixMicros => "UnixMicros",
	UnixNanos => "UnixNanos",
});

word_table!(combine_text, combine_from, AggregateCombine {
	Exact => "Exact",
	Appending => "Appending",
	InexactFloat => "InexactFloat",
	NotDecomposable => "NotDecomposable",
});

word_table!(postfix_text, postfix_from, LogicalPostfixOperator {
	Range => "Range",
	RangeSkip => "RangeSkip",
});

word_table!(misplaced_text, misplaced_from, MisplacedControl {
	NestedBegin => "NestedBegin",
	BeginInSession => "BeginInSession",
	CommitOutsideBlock => "CommitOutsideBlock",
	CancelOutsideBlock => "CancelOutsideBlock",
});

word_table!(key_gen_text, key_gen_from, RecordIdKeyGen {
	Rand => "Rand",
	Ulid => "Ulid",
	Uuid => "Uuid",
});

word_table!(boolean_text, boolean_from, BooleanOperator {
	And => "And",
	Or => "Or",
});

word_table!(catalog_kind_text, catalog_kind_from, CatalogKind {
	Table => "Table",
	Field => "Field",
	Index => "Index",
	Event => "Event",
	View => "View",
	Function => "Function",
	Param => "Param",
	Access => "Access",
	Other => "Other",
});

word_table!(constant_text, constant_from, Constant {
	MathE => "MathE",
	MathFrac1Pi => "MathFrac1Pi",
	MathFrac1Sqrt2 => "MathFrac1Sqrt2",
	MathFrac2Pi => "MathFrac2Pi",
	MathFrac2SqrtPi => "MathFrac2SqrtPi",
	MathFracPi2 => "MathFracPi2",
	MathFracPi3 => "MathFracPi3",
	MathFracPi4 => "MathFracPi4",
	MathFracPi6 => "MathFracPi6",
	MathFracPi8 => "MathFracPi8",
	MathInfinity => "MathInfinity",
	MathLn10 => "MathLn10",
	MathLn2 => "MathLn2",
	MathLog102 => "MathLog102",
	MathLog10E => "MathLog10E",
	MathLog210 => "MathLog210",
	MathLog2E => "MathLog2E",
	MathNegInfinity => "MathNegInfinity",
	MathPi => "MathPi",
	MathSqrt2 => "MathSqrt2",
	MathTau => "MathTau",
	TimeEpoch => "TimeEpoch",
	TimeMin => "TimeMin",
	TimeMax => "TimeMax",
	DurationMax => "DurationMax",
});

token_table!(prefix_text, prefix_from, PrefixOperator {
	Not => "Not",
	Positive => "Positive",
	Negate => "Negate",
	Range => "Range",
	RangeInclusive => "RangeInclusive",
} data PrefixOperator::Cast(_));

token_table!(distance_text, distance_from, Distance {
	Chebyshev => "Chebyshev",
	Cosine => "Cosine",
	Euclidean => "Euclidean",
	Hamming => "Hamming",
	Jaccard => "Jaccard",
	Manhattan => "Manhattan",
	Pearson => "Pearson",
	CosineNormalized => "CosineNormalized",
	InnerProduct => "InnerProduct",
} data Distance::Minkowski(_));

token_table!(binary_text, binary_from, BinaryOperator {
	Subtract => "-",
	Add => "+",
	Multiply => "*",
	Divide => "/",
	Remainder => "%",
	Power => "**",
	Equal => "=",
	ExactEqual => "==",
	NotEqual => "!=",
	AllEqual => "*=",
	AnyEqual => "?=",
	Or => "OR",
	And => "AND",
	NullCoalescing => "??",
	TenaryCondition => "?:",
	LessThan => "<",
	LessThanEqual => "<=",
	MoreThan => ">",
	MoreThanEqual => ">=",
	Contain => "CONTAINS",
	NotContain => "CONTAINSNOT",
	ContainAll => "CONTAINSALL",
	ContainAny => "CONTAINSANY",
	ContainNone => "CONTAINSNONE",
	Inside => "INSIDE",
	NotInside => "NOTINSIDE",
	AllInside => "ALLINSIDE",
	AnyInside => "ANYINSIDE",
	NoneInside => "NONEINSIDE",
	Outside => "OUTSIDE",
	Intersects => "INTERSECTS",
	Range => "..",
	RangeInclusive => "..=",
	RangeSkip => ">..",
	RangeSkipInclusive => ">..=",
} data BinaryOperator::Matches(_), BinaryOperator::NearestNeighbor(_));

// ---------------------------------------------------------------------------------------------
// Printing
// ---------------------------------------------------------------------------------------------

/// A map being written: `{key: value, ...}`.
struct Map<'a> {
	out: &'a mut String,
	first: bool,
}

impl<'a> Map<'a> {
	fn open(out: &'a mut String) -> Self {
		out.push('{');
		Self {
			out,
			first: true,
		}
	}

	/// Writes the separator and `key: `, and answers the buffer the value goes into.
	fn key(&mut self, key: &str) -> &mut String {
		if !self.first {
			self.out.push_str(", ");
		}
		self.first = false;
		self.out.push_str(key);
		self.out.push_str(": ");
		self.out
	}

	fn close(self) {
		self.out.push('}');
	}
}

fn print_string(out: &mut String, text: &str) {
	out.push_str(&Value::String(Strand::new(text)).to_sql());
}

fn print_int(out: &mut String, value: impl std::fmt::Display) {
	out.push_str(&value.to_string());
}

fn print_bool(out: &mut String, value: bool) {
	out.push_str(if value {
		"true"
	} else {
		"false"
	});
}

fn print_kind(out: &mut String, kind: &Kind) {
	print_string(out, &kind.to_sql());
}

fn print_list<T>(out: &mut String, items: &[T], mut each: impl FnMut(&mut String, &T)) {
	out.push('[');
	for (at, item) in items.iter().enumerate() {
		if at > 0 {
			out.push_str(", ");
		}
		each(out, item);
	}
	out.push(']');
}

fn print_names(out: &mut String, names: &[String]) {
	print_list(out, names, |out, name| print_string(out, name));
}

fn print_option<T>(out: &mut String, value: Option<&T>, each: impl FnOnce(&mut String, &T)) {
	match value {
		None => out.push_str("none"),
		Some(value) => {
			out.push_str("some ");
			each(out, value);
		}
	}
}

fn print_bound<T>(out: &mut String, bound: &Bound<T>, each: impl FnOnce(&mut String, &T)) {
	match bound {
		Bound::Included(value) => {
			out.push_str("Included ");
			each(out, value);
		}
		Bound::Excluded(value) => {
			out.push_str("Excluded ");
			each(out, value);
		}
		Bound::Unbounded => out.push_str("Unbounded"),
	}
}

/// The prefix of a value written as its `revision` encoding in hex.
const BYTES: &str = "bytes:";

/// The text a reader gets `value` back from: its SurrealQL text where that round-trips, otherwise
/// its `revision` encoding. A value neither form holds, a closure, is written as its SurrealQL
/// text, which no reader accepts. Every dialect's printer writes a value through this, and reads
/// it back through [`read_value`].
pub(crate) fn value_text(value: &Value) -> String {
	let text = value.to_sql();
	if text_round_trips(value, &text) {
		return text;
	}
	match revision::to_vec(value) {
		Ok(bytes) => format!("{BYTES}{}", hex::encode(bytes)),
		Err(_) => text,
	}
}

/// Whether `text` reads back to a value with the same `revision` encoding as `value`.
fn text_round_trips(value: &Value, text: &str) -> bool {
	let Ok(want) = revision::to_vec(value) else {
		return false;
	};
	match crate::syn::value(text) {
		Ok(read) => {
			revision::to_vec(&convert_public_value_to_internal(read)).is_ok_and(|got| got == want)
		}
		Err(_) => false,
	}
}

fn print_key_shape(out: &mut String, shape: &KeyShape) {
	match shape {
		KeyShape::Number(n) => {
			out.push_str("Number ");
			print_int(out, n);
		}
		KeyShape::String(s) => {
			out.push_str("String ");
			print_string(out, s.as_str());
		}
		KeyShape::Uuid(u) => {
			out.push_str("Uuid ");
			print_string(out, &u.0.to_string());
		}
		KeyShape::Generate(g) => {
			out.push_str("Generate ");
			out.push_str(key_gen_text(g));
		}
		KeyShape::Array {
			len,
		} => {
			out.push_str("Array ");
			print_int(out, len);
		}
		KeyShape::Object {
			keys,
		} => {
			out.push_str("Object ");
			print_names(out, keys);
		}
		KeyShape::Range(range) => {
			out.push_str("Range ");
			let mut map = Map::open(out);
			print_bound(map.key("start"), &range.start, print_key_bound);
			print_bound(map.key("end"), &range.end, print_key_bound);
			map.close();
		}
	}
}

fn print_key_bound(out: &mut String, bound: &KeyBound) {
	match bound {
		KeyBound::Number(n) => {
			out.push_str("Number ");
			print_int(out, n);
		}
		KeyBound::String(s) => {
			out.push_str("String ");
			print_string(out, s.as_str());
		}
		KeyBound::Uuid(u) => {
			out.push_str("Uuid ");
			print_string(out, &u.0.to_string());
		}
		KeyBound::Generate(g) => {
			out.push_str("Generate ");
			out.push_str(key_gen_text(g));
		}
		KeyBound::Array {
			len,
		} => {
			out.push_str("Array ");
			print_int(out, len);
		}
		KeyBound::Object {
			keys,
		} => {
			out.push_str("Object ");
			print_names(out, keys);
		}
	}
}

fn print_mock(out: &mut String, mock: &Mock) {
	match mock {
		Mock::Count(table, count) => {
			out.push_str("Count ");
			let mut map = Map::open(out);
			print_string(map.key("table"), table.as_str());
			print_int(map.key("count"), count);
			map.close();
		}
		Mock::Range(table, range) => {
			out.push_str("Range ");
			let mut map = Map::open(out);
			print_string(map.key("table"), table.as_str());
			print_bound(map.key("start"), &range.start, |out, n| print_int(out, n));
			print_bound(map.key("end"), &range.end, |out, n| print_int(out, n));
			map.close();
		}
	}
}

fn print_entry(out: &mut String, entry: &DestructureEntry) {
	match entry {
		DestructureEntry::All(field) => {
			out.push_str("All ");
			print_string(out, field.as_str());
		}
		DestructureEntry::Field(field) => {
			out.push_str("Field ");
			print_string(out, field.as_str());
		}
		DestructureEntry::Aliased(field) => {
			out.push_str("Aliased ");
			print_string(out, field.as_str());
		}
		DestructureEntry::Destructure(field, entries) => {
			out.push_str("Destructure ");
			let mut map = Map::open(out);
			print_string(map.key("field"), field.as_str());
			print_list(map.key("entries"), entries, print_entry);
			map.close();
		}
		DestructureEntry::Rest => out.push_str("Rest"),
	}
}

fn print_place(out: &mut String, place: &Place) {
	print_list(out, &place.parts, |out, part| match part {
		PlacePart::All => out.push_str("All"),
		PlacePart::Flatten => out.push_str("Flatten"),
		PlacePart::Last => out.push_str("Last"),
		PlacePart::First => out.push_str("First"),
		PlacePart::Optional => out.push_str("Optional"),
		PlacePart::Doc => out.push_str("Doc"),
		PlacePart::RepeatRecurse => out.push_str("RepeatRecurse"),
		PlacePart::Field(name) => {
			out.push_str("Field ");
			print_string(out, name.as_str());
		}
		PlacePart::Filter(slot) => {
			out.push_str("Filter ");
			print_int(out, slot);
		}
		PlacePart::Dynamic(slot) => {
			out.push_str("Dynamic ");
			print_int(out, slot);
		}
		PlacePart::Index(slot) => {
			out.push_str("Index ");
			print_int(out, slot);
		}
		PlacePart::Unaddressed(kind, text) => {
			out.push_str("Unaddressed ");
			let mut map = Map::open(out);
			map.key("kind").push_str(unaddressed_text(kind));
			print_string(map.key("text"), text);
			map.close();
		}
	});
}

fn print_function(out: &mut String, function: &Function) {
	match function {
		Function::Normal(name) => {
			out.push_str("normal ");
			print_string(out, name);
		}
		Function::Custom(name) => {
			out.push_str("custom ");
			print_string(out, name);
		}
		Function::Script(Script(source)) => {
			out.push_str("script ");
			print_string(out, source);
		}
		Function::Model(model) => {
			out.push_str("model ");
			let mut map = Map::open(out);
			print_string(map.key("name"), model.name.as_str());
			print_string(map.key("version"), model.version.as_str());
			map.close();
		}
		Function::Module(name, sub, prefix) => {
			out.push_str("module ");
			let mut map = Map::open(out);
			print_string(map.key("name"), name);
			print_option(map.key("sub"), sub.as_ref(), |out, sub| print_string(out, sub));
			print_string(map.key("prefix"), prefix.as_str());
			map.close();
		}
		Function::Silo {
			org,
			pkg,
			major,
			minor,
			patch,
			sub,
		} => {
			out.push_str("silo ");
			let mut map = Map::open(out);
			print_string(map.key("org"), org);
			print_string(map.key("pkg"), pkg);
			print_int(map.key("major"), major);
			print_int(map.key("minor"), minor);
			print_int(map.key("patch"), patch);
			print_option(map.key("sub"), sub.as_ref(), |out, sub| print_string(out, sub));
			map.close();
		}
	}
}

fn print_binary(out: &mut String, op: &BinaryOperator) {
	if let Some(token) = binary_text(op) {
		print_string(out, token);
		return;
	}
	match op {
		BinaryOperator::Matches(matches) => {
			out.push_str("Matches ");
			let mut map = Map::open(out);
			print_option(map.key("rf"), matches.rf.as_ref(), |out, rf| print_int(out, rf));
			map.key("operator").push_str(boolean_text(&matches.operator));
			map.close();
		}
		BinaryOperator::NearestNeighbor(nearest) => match &**nearest {
			NearestNeighbor::K(k, distance) => {
				out.push_str("K ");
				let mut map = Map::open(out);
				print_int(map.key("k"), k);
				print_distance(map.key("distance"), distance);
				map.close();
			}
			NearestNeighbor::KTree(k) => {
				out.push_str("KTree ");
				print_int(out, k);
			}
			NearestNeighbor::Approximate(k, ef) => {
				out.push_str("Approximate ");
				let mut map = Map::open(out);
				print_int(map.key("k"), k);
				print_int(map.key("ef"), ef);
				map.close();
			}
		},
		BinaryOperator::Subtract
		| BinaryOperator::Add
		| BinaryOperator::Multiply
		| BinaryOperator::Divide
		| BinaryOperator::Remainder
		| BinaryOperator::Power
		| BinaryOperator::Equal
		| BinaryOperator::ExactEqual
		| BinaryOperator::NotEqual
		| BinaryOperator::AllEqual
		| BinaryOperator::AnyEqual
		| BinaryOperator::Or
		| BinaryOperator::And
		| BinaryOperator::NullCoalescing
		| BinaryOperator::TenaryCondition
		| BinaryOperator::LessThan
		| BinaryOperator::LessThanEqual
		| BinaryOperator::MoreThan
		| BinaryOperator::MoreThanEqual
		| BinaryOperator::Contain
		| BinaryOperator::NotContain
		| BinaryOperator::ContainAll
		| BinaryOperator::ContainAny
		| BinaryOperator::ContainNone
		| BinaryOperator::Inside
		| BinaryOperator::NotInside
		| BinaryOperator::AllInside
		| BinaryOperator::AnyInside
		| BinaryOperator::NoneInside
		| BinaryOperator::Outside
		| BinaryOperator::Intersects
		| BinaryOperator::Range
		| BinaryOperator::RangeInclusive
		| BinaryOperator::RangeSkip
		| BinaryOperator::RangeSkipInclusive => {}
	}
}

/// The distance of a `K` nearest-neighbour operator. A Minkowski order is the string literal of
/// its number's [`value_text`].
fn print_distance(out: &mut String, distance: &Distance) {
	if let Some(word) = distance_text(distance) {
		out.push_str(word);
		return;
	}
	match distance {
		Distance::Minkowski(order) => {
			out.push_str("Minkowski ");
			print_string(out, &value_text(&Value::Number(number_from_public(order))));
		}
		Distance::Chebyshev
		| Distance::Cosine
		| Distance::Euclidean
		| Distance::Hamming
		| Distance::Jaccard
		| Distance::Manhattan
		| Distance::Pearson
		| Distance::CosineNormalized
		| Distance::InnerProduct => {}
	}
}

fn number_from_public(number: &PublicNumber) -> Number {
	match number {
		PublicNumber::Int(i) => Number::Int(*i),
		PublicNumber::Float(f) => Number::Float(*f),
		PublicNumber::Decimal(d) => Number::Decimal(*d),
		PublicNumber::BigDecimal(d) => Number::BigDecimal(d.clone()),
		PublicNumber::I8(i) => Number::I8(*i),
		PublicNumber::U8(u) => Number::U8(*u),
		PublicNumber::I16(i) => Number::I16(*i),
		PublicNumber::U16(u) => Number::U16(*u),
		PublicNumber::I32(i) => Number::I32(*i),
		PublicNumber::U32(u) => Number::U32(*u),
		PublicNumber::U64(u) => Number::U64(*u),
		PublicNumber::F16(f) => Number::F16(*f),
		PublicNumber::F32(f) => Number::F32(*f),
	}
}

fn number_to_public(number: &Number) -> PublicNumber {
	match number {
		Number::Int(i) => PublicNumber::Int(*i),
		Number::Float(f) => PublicNumber::Float(*f),
		Number::Decimal(d) => PublicNumber::Decimal(*d),
		Number::BigDecimal(d) => PublicNumber::BigDecimal(d.clone()),
		Number::I8(i) => PublicNumber::I8(*i),
		Number::U8(u) => PublicNumber::U8(*u),
		Number::I16(i) => PublicNumber::I16(*i),
		Number::U16(u) => PublicNumber::U16(*u),
		Number::I32(i) => PublicNumber::I32(*i),
		Number::U32(u) => PublicNumber::U32(*u),
		Number::U64(u) => PublicNumber::U64(*u),
		Number::F16(f) => PublicNumber::F16(*f),
		Number::F32(f) => PublicNumber::F32(*f),
	}
}

fn print_prefix(out: &mut String, op: &PrefixOperator) {
	if let Some(word) = prefix_text(op) {
		out.push_str(word);
		return;
	}
	match op {
		PrefixOperator::Cast(kind) => {
			out.push_str("Cast ");
			print_kind(out, kind);
		}
		PrefixOperator::Not
		| PrefixOperator::Positive
		| PrefixOperator::Negate
		| PrefixOperator::Range
		| PrefixOperator::RangeInclusive => {}
	}
}

fn print_refusal(out: &mut String, refusal: &Refusal) {
	match refusal {
		Refusal::ProtectedParam {
			name,
		} => {
			out.push_str("ProtectedParam ");
			print_string(out, name);
		}
		Refusal::Query {
			message,
		} => {
			out.push_str("Query ");
			print_string(out, message);
		}
		Refusal::InvalidKnnVector {
			vector,
		} => {
			out.push_str("InvalidKnnVector ");
			print_string(out, vector);
		}
		Refusal::UncoercibleKnnVector {
			message,
		} => {
			out.push_str("UncoercibleKnnVector ");
			print_string(out, message);
		}
		Refusal::InvalidStatement {
			message,
		} => {
			out.push_str("InvalidStatement ");
			print_string(out, message);
		}
		Refusal::InvalidFetch {
			value,
		} => {
			out.push_str("InvalidFetch ");
			print_string(out, value);
		}
		Refusal::AccessContextNotReadOnly => out.push_str("AccessContextNotReadOnly"),
		Refusal::AccessContextRequiresRecord => out.push_str("AccessContextRequiresRecord"),
		Refusal::NoIndexForMatch {
			exp,
		} => {
			out.push_str("NoIndexForMatch ");
			print_string(out, exp);
		}
		Refusal::InvalidBound {
			found,
			expected,
		} => {
			out.push_str("InvalidBound ");
			let mut map = Map::open(out);
			print_string(map.key("found"), found);
			print_string(map.key("expected"), expected);
			map.close();
		}
		Refusal::SchemaClauseEffect {
			clause,
			construct,
		} => {
			out.push_str("SchemaClauseEffect ");
			let mut map = Map::open(out);
			print_string(map.key("clause"), clause);
			print_string(map.key("construct"), construct);
			map.close();
		}
		Refusal::RecursionInstructionPlanConflict => {
			out.push_str("RecursionInstructionPlanConflict");
		}
		Refusal::UnsupportedRepeatRecurse => out.push_str("UnsupportedRepeatRecurse"),
		Refusal::ComputationDepthExceeded => out.push_str("ComputationDepthExceeded"),
		Refusal::AccessRedactedKey {
			ac,
		} => {
			out.push_str("AccessRedactedKey ");
			print_string(out, ac);
		}
		Refusal::NamespaceEmpty => out.push_str("NamespaceEmpty"),
		Refusal::DatabaseEmpty => out.push_str("DatabaseEmpty"),
	}
}

fn print_raise(out: &mut String, kind: &RaiseKind) {
	match kind {
		RaiseKind::Folded(error) => {
			out.push_str("Folded ");
			let mut map = Map::open(out);
			print_string(map.key("text"), &error.text);
			map.close();
		}
		RaiseKind::InvalidControlFlow => out.push_str("InvalidControlFlow"),
		RaiseKind::Misplaced(control) => {
			out.push_str("Misplaced ");
			out.push_str(misplaced_text(control));
		}
		RaiseKind::Refused(refusal) => {
			out.push_str("Refused ");
			print_refusal(out, refusal);
		}
	}
}

fn print_callee(out: &mut String, callee: &CalleeFacts) {
	match callee {
		CalleeFacts::Absent => out.push_str("Absent"),
		CalleeFacts::LateBound => out.push_str("LateBound"),
		CalleeFacts::Defined(defined) | CalleeFacts::PerCall(defined) => {
			let word = match callee {
				CalleeFacts::PerCall(_) => "PerCall ",
				CalleeFacts::Absent | CalleeFacts::LateBound | CalleeFacts::Defined(_) => {
					"Defined "
				}
			};
			out.push_str(word);
			let mut map = Map::open(out);
			print_int(map.key("hash"), defined.hash);
			print_callee_permission(map.key("permission"), defined.permission);
			print_bool(map.key("writes"), defined.writes);
			print_int(map.key("body_depth"), defined.body_depth);
			print_int(map.key("deepest"), defined.deepest);
			map.close();
		}
	}
}

fn print_callee_permission(out: &mut String, permission: CalleePermission) {
	match permission {
		CalleePermission::Full => out.push_str("Full"),
		CalleePermission::None => out.push_str("None"),
		CalleePermission::Predicate {
			scope: None,
		} => out.push_str("Predicate"),
		CalleePermission::Predicate {
			scope: Some(scope),
		} => {
			out.push_str("Predicate ");
			let mut map = Map::open(out);
			match scope {
				GuardScope::PerCall => map.key("scope").push_str("PerCall"),
				GuardScope::Statement {
					reads_data,
				} => {
					let out = map.key("scope");
					out.push_str("Statement ");
					let mut inner = Map::open(out);
					print_bool(inner.key("reads_data"), reads_data);
					inner.close();
				}
			}
			map.close();
		}
	}
}

fn print_strategy(out: &mut String, strategy: &NestedStrategy) {
	match strategy {
		NestedStrategy::Apply => out.push_str("Apply"),
		NestedStrategy::Once => out.push_str("Once"),
		NestedStrategy::Hash {
			field,
		} => {
			out.push_str("Hash ");
			let mut map = Map::open(out);
			print_string(map.key("field"), field.as_str());
			map.close();
		}
		NestedStrategy::Lookup {
			index,
			hashed,
		} => {
			out.push_str("Lookup ");
			let mut map = Map::open(out);
			let mut shape = Map::open(map.key("index"));
			print_string(shape.key("table"), index.table.as_str());
			print_string(shape.key("index"), index.index.as_str());
			print_int(shape.key("position"), index.position);
			print_list(shape.key("scalar"), &index.scalar, |out, scalar| print_bool(out, *scalar));
			shape.close();
			print_bool(map.key("hashed"), *hashed);
			map.close();
		}
	}
}

fn print_catalog_key(out: &mut String, key: &CatalogObjectKey) {
	let mut map = Map::open(out);
	map.key("kind").push_str(catalog_kind_text(&key.kind));
	print_option(map.key("ns"), key.ns.as_ref(), |out, ns| print_int(out, ns.0));
	print_option(map.key("db"), key.db.as_ref(), |out, db| print_int(out, db.0));
	print_option(map.key("table"), key.table.as_ref(), |out, table| {
		print_string(out, table.as_str());
	});
	print_option(map.key("name"), key.name.as_ref(), |out, name| print_string(out, name.as_str()));
	map.close();
}

fn print_version(out: &mut String, version: &ObjectVersion) {
	match version {
		ObjectVersion::Stamped(stamp) => {
			out.push_str("Stamped ");
			print_string(out, &stamp.to_string());
		}
		ObjectVersion::Content(hash) => {
			out.push_str("Content ");
			print_int(out, hash);
		}
		ObjectVersion::Inline => out.push_str("Inline"),
	}
}

/// Appends the attributes of `op`: a space and a map for an op that has attributes, nothing for
/// an op that has none.
fn print_op(op: &ExprOp, out: &mut String) {
	match op {
		ExprOp::Array
		| ExprOp::Set
		| ExprOp::Doc
		| ExprOp::Index
		| ExprOp::First
		| ExprOp::Last
		| ExprOp::Each
		| ExprOp::Optional
		| ExprOp::RepeatRecurse
		| ExprOp::Invoke
		| ExprOp::If
		| ExprOp::Throw
		| ExprOp::Subquery(NestedStrategy::Apply)
		| ExprOp::Yield
		| ExprOp::Truthy => return,
		ExprOp::Literal(value) => {
			out.push_str(" {value: ");
			out.push_str(&value_text(value));
			out.push('}');
			return;
		}
		ExprOp::Object {
			..
		}
		| ExprOp::RecordId(_)
		| ExprOp::FreeParam {
			..
		}
		| ExprOp::SessionParam {
			..
		}
		| ExprOp::CatalogParam {
			..
		}
		| ExprOp::Table {
			..
		}
		| ExprOp::Mock(_)
		| ExprOp::Constant {
			..
		}
		| ExprOp::Field {
			..
		}
		| ExprOp::ComputedRead(_)
		| ExprOp::Access {
			..
		}
		| ExprOp::Deref {
			..
		}
		| ExprOp::Flatten {
			..
		}
		| ExprOp::Dispatch(_)
		| ExprOp::Method {
			..
		}
		| ExprOp::Destructure {
			..
		}
		| ExprOp::SetField {
			..
		}
		| ExprOp::TupleRow {
			..
		}
		| ExprOp::Prefix(_)
		| ExprOp::Convert(_)
		| ExprOp::Postfix {
			..
		}
		| ExprOp::Binary {
			..
		}
		| ExprOp::ShortCircuit {
			..
		}
		| ExprOp::Call(_)
		| ExprOp::ClosureCreate(_)
		| ExprOp::Agg {
			..
		}
		| ExprOp::Now {
			..
		}
		| ExprOp::Raise(_)
		| ExprOp::Subquery(_)
		| ExprOp::DepthCharge {
			..
		} => {}
	}
	out.push(' ');
	let mut map = Map::open(out);
	match op {
		ExprOp::Object {
			keys,
		} => print_names(map.key("keys"), keys),
		ExprOp::RecordId(attrs) => {
			print_string(map.key("table"), attrs.table.as_str());
			print_key_shape(map.key("key_shape"), &attrs.key_shape);
		}
		ExprOp::FreeParam {
			name,
		}
		| ExprOp::SessionParam {
			name,
		}
		| ExprOp::CatalogParam {
			name,
		}
		| ExprOp::Field {
			name,
		}
		| ExprOp::Method {
			name,
		} => print_string(map.key("name"), name.as_str()),
		ExprOp::Table {
			name,
		} => print_string(map.key("name"), name.as_str()),
		ExprOp::Mock(mock) => print_mock(map.key("mock"), mock),
		ExprOp::Constant {
			c,
		} => map.key("c").push_str(constant_text(c)),
		ExprOp::ComputedRead(attrs) => {
			print_string(map.key("field"), attrs.field.as_str());
			let mut def = Map::open(map.key("def"));
			print_catalog_key(def.key("key"), &attrs.def.0);
			print_version(def.key("version"), &attrs.def.1);
			def.close();
		}
		ExprOp::Access {
			field,
		}
		| ExprOp::SetField {
			field,
		} => print_string(map.key("field"), field.as_str()),
		ExprOp::Deref {
			table_hint,
		} => print_option(map.key("table_hint"), table_hint.as_deref(), |out, table| {
			print_string(out, table.as_str());
		}),
		ExprOp::Flatten {
			explicit,
		} => print_bool(map.key("explicit"), *explicit),
		ExprOp::Dispatch(target) => {
			let target_out = map.key("target");
			match &**target {
				DispatchTarget::Method(name) => {
					target_out.push_str("Method ");
					print_string(target_out, name.as_str());
				}
				DispatchTarget::Invoke => target_out.push_str("Invoke"),
			}
		}
		ExprOp::Destructure {
			shape,
		} => print_list(map.key("shape"), shape, print_entry),
		ExprOp::TupleRow {
			places,
		} => print_list(map.key("places"), places, print_place),
		ExprOp::Prefix(op) => print_prefix(map.key("op"), op),
		ExprOp::Convert(attrs) => {
			map.key("mode").push_str(convert_mode_text(&attrs.mode));
			print_kind(map.key("kind"), &attrs.kind);
			print_int(map.key("site"), attrs.site.0);
			map.key("on_fail").push_str(on_fail_text(&attrs.on_fail));
		}
		ExprOp::Postfix {
			op,
		} => map.key("op").push_str(postfix_text(op)),
		ExprOp::Binary {
			op,
		} => print_binary(map.key("op"), op),
		ExprOp::ShortCircuit {
			op,
		} => map.key("op").push_str(short_circuit_text(op)),
		ExprOp::Call(call) => {
			print_function(map.key("function"), &call.function);
			print_int(map.key("row_names"), call.row_names);
			print_option(map.key("callee"), call.callee.as_ref(), print_callee);
		}
		ExprOp::ClosureCreate(attrs) => {
			print_list(map.key("params"), &attrs.params, |out, (name, kind)| {
				let mut param = Map::open(out);
				print_string(param.key("name"), name.as_str());
				print_kind(param.key("kind"), kind);
				param.close();
			});
			print_option(map.key("returns"), attrs.returns.as_ref(), print_kind);
			print_string(map.key("body_sql"), &attrs.body_sql);
			print_list(map.key("captures"), &attrs.captures, |out, name| {
				print_string(out, name.as_str());
			});
			print_bool(map.key("reads_row"), attrs.reads_row);
			print_bool(map.key("reads_root"), attrs.reads_root);
		}
		ExprOp::Agg {
			kind,
			combine,
		} => {
			print_string(map.key("kind"), kind);
			map.key("combine").push_str(combine_text(combine));
		}
		ExprOp::Now {
			which,
		} => map.key("which").push_str(now_which_text(which)),
		ExprOp::Raise(kind) => print_raise(map.key("kind"), kind),
		ExprOp::DepthCharge {
			levels,
		} => print_int(map.key("levels"), levels),
		ExprOp::Subquery(strategy) => print_strategy(map.key("strategy"), strategy),
		ExprOp::Literal(_)
		| ExprOp::Array
		| ExprOp::Set
		| ExprOp::Doc
		| ExprOp::Index
		| ExprOp::First
		| ExprOp::Last
		| ExprOp::Each
		| ExprOp::Optional
		| ExprOp::RepeatRecurse
		| ExprOp::Invoke
		| ExprOp::If
		| ExprOp::Throw
		| ExprOp::Yield
		| ExprOp::Truthy => {}
	}
	map.close();
}

// ---------------------------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------------------------

/// The deepest nesting of lists, maps and variants a text may hold.
const MAX_DEPTH: usize = 64;

/// A value of the text form, before it is read as a payload type.
#[derive(Debug)]
enum Node {
	Str(String),
	Int(i128),
	List(Vec<Node>),
	Map(Vec<(String, Node)>),
	/// A word and the value it carries, if any.
	Word(String, Option<Box<Node>>),
}

impl Node {
	fn describe(&self) -> &'static str {
		match self {
			Node::Str(_) => "a string",
			Node::Int(_) => "an integer",
			Node::List(_) => "a list",
			Node::Map(_) => "a map",
			Node::Word(..) => "a word",
		}
	}

	fn string(self) -> Result<String, ParseError> {
		match self {
			Node::Str(text) => Ok(text),
			other @ (Node::Int(_) | Node::List(_) | Node::Map(_) | Node::Word(..)) => {
				Err(error(format!("expected a string, found {}", other.describe())))
			}
		}
	}

	fn int<T: TryFrom<i128>>(self) -> Result<T, ParseError> {
		match self {
			Node::Int(n) => {
				T::try_from(n).map_err(|_| error(format!("integer {n} is out of range")))
			}
			other @ (Node::Str(_) | Node::List(_) | Node::Map(_) | Node::Word(..)) => {
				Err(error(format!("expected an integer, found {}", other.describe())))
			}
		}
	}

	fn boolean(self) -> Result<bool, ParseError> {
		match self.word()? {
			(word, None) if word == "true" => Ok(true),
			(word, None) if word == "false" => Ok(false),
			(word, _) => Err(error(format!("expected true or false, found {word}"))),
		}
	}

	fn list(self) -> Result<Vec<Node>, ParseError> {
		match self {
			Node::List(items) => Ok(items),
			other @ (Node::Str(_) | Node::Int(_) | Node::Map(_) | Node::Word(..)) => {
				Err(error(format!("expected a list, found {}", other.describe())))
			}
		}
	}

	fn map(self) -> Result<Fields, ParseError> {
		match self {
			Node::Map(items) => Ok(Fields(items.into_iter())),
			other @ (Node::Str(_) | Node::Int(_) | Node::List(_) | Node::Word(..)) => {
				Err(error(format!("expected a map, found {}", other.describe())))
			}
		}
	}

	fn word(self) -> Result<(String, Option<Node>), ParseError> {
		match self {
			Node::Word(word, arg) => Ok((word, arg.map(|arg| *arg))),
			other @ (Node::Str(_) | Node::Int(_) | Node::List(_) | Node::Map(_)) => {
				Err(error(format!("expected a word, found {}", other.describe())))
			}
		}
	}

	/// A word that carries nothing.
	fn unit(self) -> Result<String, ParseError> {
		match self.word()? {
			(word, None) => Ok(word),
			(word, Some(_)) => Err(error(format!("{word} carries no value"))),
		}
	}

	/// `none` or `some <value>`.
	fn option<T>(
		self,
		each: impl FnOnce(Node) -> Result<T, ParseError>,
	) -> Result<Option<T>, ParseError> {
		match self.word()? {
			(word, None) if word == "none" => Ok(None),
			(word, Some(arg)) if word == "some" => each(arg).map(Some),
			(word, _) => Err(error(format!("expected none or some, found {word}"))),
		}
	}

	/// `Included <value>`, `Excluded <value>` or `Unbounded`.
	fn bound<T>(
		self,
		each: impl FnOnce(Node) -> Result<T, ParseError>,
	) -> Result<Bound<T>, ParseError> {
		match self.word()? {
			(word, Some(arg)) if word == "Included" => each(arg).map(Bound::Included),
			(word, Some(arg)) if word == "Excluded" => each(arg).map(Bound::Excluded),
			(word, None) if word == "Unbounded" => Ok(Bound::Unbounded),
			(word, _) => Err(error(format!("expected a bound, found {word}"))),
		}
	}
}

/// The entries of a map, read in the order they were written.
struct Fields(std::vec::IntoIter<(String, Node)>);

impl Fields {
	/// The value of the next entry, which must be named `key`.
	fn take(&mut self, key: &str) -> Result<Node, ParseError> {
		match self.0.next() {
			Some((found, value)) if found == key => Ok(value),
			Some((found, _)) => Err(error(format!("expected key {key}, found {found}"))),
			None => Err(error(format!("expected key {key}, found the end of the map"))),
		}
	}

	/// Fails when an entry was not taken.
	fn done(mut self) -> Result<(), ParseError> {
		match self.0.next() {
			Some((found, _)) => Err(error(format!("unknown key {found}"))),
			None => Ok(()),
		}
	}
}

/// A position in the text being read.
struct Cursor<'a> {
	src: &'a str,
	at: usize,
	depth: usize,
}

impl<'a> Cursor<'a> {
	fn new(src: &'a str) -> Self {
		Self {
			src,
			at: 0,
			depth: 0,
		}
	}

	fn rest(&self) -> &'a str {
		&self.src[self.at..]
	}

	fn peek(&self) -> Option<char> {
		self.rest().chars().next()
	}

	fn bump(&mut self, c: char) {
		self.at += c.len_utf8();
	}

	fn skip_space(&mut self) {
		while let Some(c) = self.peek() {
			if !c.is_whitespace() {
				break;
			}
			self.bump(c);
		}
	}

	fn expect(&mut self, want: char) -> Result<(), ParseError> {
		self.skip_space();
		match self.peek() {
			Some(c) if c == want => {
				self.bump(c);
				Ok(())
			}
			Some(c) => Err(error(format!("expected {want}, found {c}"))),
			None => Err(error(format!("expected {want}, found the end of the text"))),
		}
	}

	/// Fails when anything but space follows.
	fn finish(&mut self) -> Result<(), ParseError> {
		self.skip_space();
		match self.peek() {
			Some(_) => Err(error(format!("unexpected text after the attributes: {}", self.rest()))),
			None => Ok(()),
		}
	}

	fn identifier(&mut self) -> Result<String, ParseError> {
		let start = self.at;
		while let Some(c) = self.peek() {
			if !(c.is_ascii_alphanumeric() || c == '_') {
				break;
			}
			self.bump(c);
		}
		if self.at == start {
			return Err(error(match self.peek() {
				Some(c) => format!("expected a name, found {c}"),
				None => "expected a name, found the end of the text".to_owned(),
			}));
		}
		Ok(self.src[start..self.at].to_owned())
	}

	fn node(&mut self) -> Result<Node, ParseError> {
		self.depth += 1;
		if self.depth > MAX_DEPTH {
			return Err(error("the attributes nest too deeply"));
		}
		let node = self.node_at_depth();
		self.depth -= 1;
		node
	}

	fn node_at_depth(&mut self) -> Result<Node, ParseError> {
		self.skip_space();
		match self.peek() {
			Some('{') => self.map(),
			Some('[') => self.list(),
			Some(quote @ ('\'' | '"')) => self.string(quote).map(Node::Str),
			Some(c) if c.is_ascii_digit() || c == '-' => self.int(),
			Some(c) if c.is_ascii_alphabetic() || c == '_' => {
				let word = self.identifier()?;
				self.skip_space();
				let carries = self.peek().is_some_and(|c| {
					c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '\'' | '"' | '{' | '[')
				});
				let arg = if carries {
					Some(Box::new(self.node()?))
				} else {
					None
				};
				Ok(Node::Word(word, arg))
			}
			Some(c) => Err(error(format!("unexpected {c}"))),
			None => Err(error("unexpected end of the text")),
		}
	}

	fn int(&mut self) -> Result<Node, ParseError> {
		let start = self.at;
		if self.peek() == Some('-') {
			self.bump('-');
		}
		while let Some(c) = self.peek() {
			if !c.is_ascii_digit() {
				break;
			}
			self.bump(c);
		}
		let text = &self.src[start..self.at];
		text.parse::<i128>().map(Node::Int).map_err(|_| error(format!("{text} is not an integer")))
	}

	/// A string literal, read with the language's parser.
	fn string(&mut self, quote: char) -> Result<String, ParseError> {
		let start = self.at;
		self.bump(quote);
		loop {
			match self.peek() {
				None => return Err(error("a string is not closed")),
				Some('\\') => {
					self.bump('\\');
					if let Some(escaped) = self.peek() {
						self.bump(escaped);
					}
				}
				Some(c) if c == quote => {
					self.bump(c);
					break;
				}
				Some(c) => self.bump(c),
			}
		}
		match crate::syn::value(&self.src[start..self.at]) {
			Ok(PublicValue::String(text)) => Ok(text),
			Ok(_) => Err(error("a string literal did not read as a string")),
			Err(e) => Err(error(format!("a string literal does not parse: {e}"))),
		}
	}

	fn list(&mut self) -> Result<Node, ParseError> {
		self.expect('[')?;
		let mut items = Vec::new();
		loop {
			self.skip_space();
			if self.peek() == Some(']') {
				self.bump(']');
				return Ok(Node::List(items));
			}
			if !items.is_empty() {
				self.expect(',')?;
			}
			items.push(self.node()?);
		}
	}

	fn map(&mut self) -> Result<Node, ParseError> {
		self.expect('{')?;
		let mut items = Vec::new();
		loop {
			self.skip_space();
			if self.peek() == Some('}') {
				self.bump('}');
				return Ok(Node::Map(items));
			}
			if !items.is_empty() {
				self.expect(',')?;
				self.skip_space();
			}
			let key = self.identifier()?;
			self.expect(':')?;
			items.push((key, self.node()?));
		}
	}
}

/// `text` without a leading mnemonic: the text itself when it is empty or starts with an
/// attribute map, otherwise the text after its first word, which must be the mnemonic of `tag`.
fn strip_mnemonic(tag: ExprTag, text: &str) -> Result<&str, ParseError> {
	let text = text.trim();
	if text.is_empty() || text.starts_with('{') {
		return Ok(text);
	}
	let end = text.find(char::is_whitespace).unwrap_or(text.len());
	let (head, rest) = text.split_at(end);
	if head == tag.mnemonic() {
		Ok(rest.trim_start())
	} else {
		Err(error(format!("the text of {head} is not the text of {}", tag.mnemonic())))
	}
}

/// The attributes of an op that has none: `text` must be empty.
fn no_attrs(op: ExprOp, text: &str) -> Result<ExprOp, ParseError> {
	if text.is_empty() {
		Ok(op)
	} else {
		Err(error(format!("{} has no attributes, found {text}", op.tag().mnemonic())))
	}
}

fn fields(text: &str) -> Result<Fields, ParseError> {
	let mut cursor = Cursor::new(text);
	let node = cursor.node()?;
	cursor.finish()?;
	node.map()
}

fn read_strand(node: Node) -> Result<Strand, ParseError> {
	node.string().map(Strand::from)
}

fn read_table(node: Node) -> Result<TableName, ParseError> {
	node.string().map(TableName::new)
}

fn read_kind(node: Node) -> Result<Kind, ParseError> {
	read_kind_text(&node.string()?)
}

/// Reads a kind from its canonical text only. The language's kind parser stops once it has read a
/// kind and ignores what follows, so the text is also required to equal the canonical text of the
/// kind it read. Every dialect's printer writes a kind as `to_sql` and reads it through this.
pub(crate) fn read_kind_text(text: &str) -> Result<Kind, ParseError> {
	let kind = crate::syn::kind(text).map_err(|e| error(format!("a kind does not parse: {e}")))?;
	if kind.to_sql() != text {
		return Err(error(format!("{text} is not the canonical text of a kind")));
	}
	Ok(kind)
}

/// The text of `f`, which [`read_f64`] reads back to the same bits: the shortest decimal that
/// round-trips, `-0.0` and `inf` included, and a NaN as `NaN:` and its bits in hex.
pub(crate) fn f64_text(f: f64) -> String {
	if f.is_nan() {
		format!("NaN:{:016x}", f.to_bits())
	} else {
		format!("{f:?}")
	}
}

/// The float [`f64_text`] printed as `text`.
pub(crate) fn read_f64(text: &str) -> Option<f64> {
	match text.strip_prefix("NaN:") {
		Some(bits) => u64::from_str_radix(bits, 16).ok().map(f64::from_bits).filter(|f| f.is_nan()),
		None => text.parse::<f64>().ok().filter(|f| !f.is_nan()),
	}
}

/// As [`f64_text`], for an `f32`.
pub(crate) fn f32_text(f: f32) -> String {
	if f.is_nan() {
		format!("NaN:{:08x}", f.to_bits())
	} else {
		format!("{f:?}")
	}
}

/// The float [`f32_text`] printed as `text`.
pub(crate) fn read_f32(text: &str) -> Option<f32> {
	match text.strip_prefix("NaN:") {
		Some(bits) => u32::from_str_radix(bits, 16).ok().map(f32::from_bits).filter(|f| f.is_nan()),
		None => text.parse::<f32>().ok().filter(|f| !f.is_nan()),
	}
}

fn read_names(node: Node) -> Result<Box<[String]>, ParseError> {
	node.list()?.into_iter().map(Node::string).collect()
}

fn read_uuid(node: Node) -> Result<uuid::Uuid, ParseError> {
	let text = node.string()?;
	uuid::Uuid::parse_str(&text).map_err(|e| error(format!("{text} is not a uuid: {e}")))
}

/// The unknown-variant failure of the enum `ty`.
fn unknown<T>(ty: &str, word: &str) -> Result<T, ParseError> {
	Err(error(format!("{word} is not a variant of {ty}")))
}

fn read_word<T>(
	node: Node,
	ty: &str,
	from: impl FnOnce(&str) -> Option<T>,
) -> Result<T, ParseError> {
	let word = node.unit()?;
	match from(&word) {
		Some(value) => Ok(value),
		None => unknown(ty, &word),
	}
}

fn read_key_shape(node: Node) -> Result<KeyShape, ParseError> {
	let (word, arg) = node.word()?;
	match (word.as_str(), arg) {
		("Number", Some(arg)) => arg.int().map(KeyShape::Number),
		("String", Some(arg)) => read_strand(arg).map(KeyShape::String),
		("Uuid", Some(arg)) => read_uuid(arg).map(|u| KeyShape::Uuid(Uuid(u))),
		("Generate", Some(arg)) => {
			read_word(arg, "RecordIdKeyGen", key_gen_from).map(KeyShape::Generate)
		}
		("Array", Some(arg)) => arg.int().map(|len| KeyShape::Array {
			len,
		}),
		("Object", Some(arg)) => read_names(arg).map(|keys| KeyShape::Object {
			keys,
		}),
		("Range", Some(arg)) => {
			let mut map = arg.map()?;
			let start = map.take("start")?.bound(read_key_bound)?;
			let end = map.take("end")?.bound(read_key_bound)?;
			map.done()?;
			Ok(KeyShape::Range(Box::new(KeyRange {
				start,
				end,
			})))
		}
		(other, _) => unknown("KeyShape", other),
	}
}

fn read_key_bound(node: Node) -> Result<KeyBound, ParseError> {
	let (word, arg) = node.word()?;
	match (word.as_str(), arg) {
		("Number", Some(arg)) => arg.int().map(KeyBound::Number),
		("String", Some(arg)) => read_strand(arg).map(KeyBound::String),
		("Uuid", Some(arg)) => read_uuid(arg).map(|u| KeyBound::Uuid(Uuid(u))),
		("Generate", Some(arg)) => {
			read_word(arg, "RecordIdKeyGen", key_gen_from).map(KeyBound::Generate)
		}
		("Array", Some(arg)) => arg.int().map(|len| KeyBound::Array {
			len,
		}),
		("Object", Some(arg)) => read_names(arg).map(|keys| KeyBound::Object {
			keys,
		}),
		(other, _) => unknown("KeyBound", other),
	}
}

fn read_mock(node: Node) -> Result<Mock, ParseError> {
	let (word, arg) = node.word()?;
	match (word.as_str(), arg) {
		("Count", Some(arg)) => {
			let mut map = arg.map()?;
			let table = read_table(map.take("table")?)?;
			let count: i64 = map.take("count")?.int()?;
			map.done()?;
			Ok(Mock::Count(table, count))
		}
		("Range", Some(arg)) => {
			let mut map = arg.map()?;
			let table = read_table(map.take("table")?)?;
			let start: Bound<i64> = map.take("start")?.bound(Node::int)?;
			let end: Bound<i64> = map.take("end")?.bound(Node::int)?;
			map.done()?;
			Ok(Mock::Range(
				table,
				TypedRange {
					start,
					end,
				},
			))
		}
		(other, _) => unknown("Mock", other),
	}
}

fn read_places(node: Node) -> Result<Box<[Place]>, ParseError> {
	node.list()?.into_iter().map(read_place).collect()
}

fn read_place(node: Node) -> Result<Place, ParseError> {
	let parts = node.list()?.into_iter().map(read_place_part).collect::<Result<Box<[_]>, _>>()?;
	Ok(Place {
		parts,
	})
}

fn read_place_part(node: Node) -> Result<PlacePart, ParseError> {
	let (word, arg) = node.word()?;
	match (word.as_str(), arg) {
		("All", None) => Ok(PlacePart::All),
		("Flatten", None) => Ok(PlacePart::Flatten),
		("Last", None) => Ok(PlacePart::Last),
		("First", None) => Ok(PlacePart::First),
		("Optional", None) => Ok(PlacePart::Optional),
		("Doc", None) => Ok(PlacePart::Doc),
		("RepeatRecurse", None) => Ok(PlacePart::RepeatRecurse),
		("Field", Some(arg)) => read_strand(arg).map(PlacePart::Field),
		("Filter", Some(arg)) => arg.int().map(PlacePart::Filter),
		("Dynamic", Some(arg)) => arg.int().map(PlacePart::Dynamic),
		("Index", Some(arg)) => arg.int().map(PlacePart::Index),
		("Unaddressed", Some(arg)) => {
			let mut map = arg.map()?;
			let kind = read_word(map.take("kind")?, "UnaddressedPart", unaddressed_from)?;
			let text = map.take("text")?.string()?;
			map.done()?;
			Ok(PlacePart::Unaddressed(kind, text.into_boxed_str()))
		}
		(other, _) => unknown("PlacePart", other),
	}
}

fn read_entries(node: Node) -> Result<Box<[DestructureEntry]>, ParseError> {
	node.list()?.into_iter().map(read_entry).collect()
}

fn read_entry(node: Node) -> Result<DestructureEntry, ParseError> {
	let (word, arg) = node.word()?;
	match (word.as_str(), arg) {
		("All", Some(arg)) => read_strand(arg).map(DestructureEntry::All),
		("Field", Some(arg)) => read_strand(arg).map(DestructureEntry::Field),
		("Aliased", Some(arg)) => read_strand(arg).map(DestructureEntry::Aliased),
		("Destructure", Some(arg)) => {
			let mut map = arg.map()?;
			let field = read_strand(map.take("field")?)?;
			let entries = read_entries(map.take("entries")?)?;
			map.done()?;
			Ok(DestructureEntry::Destructure(field, entries))
		}
		("Rest", None) => Ok(DestructureEntry::Rest),
		(other, _) => unknown("DestructureEntry", other),
	}
}

fn read_function(node: Node) -> Result<Function, ParseError> {
	let (word, arg) = node.word()?;
	match (word.as_str(), arg) {
		("normal", Some(arg)) => arg.string().map(Function::Normal),
		("custom", Some(arg)) => arg.string().map(Function::Custom),
		("script", Some(arg)) => arg.string().map(|source| Function::Script(Script(source))),
		("model", Some(arg)) => {
			let mut map = arg.map()?;
			let name = read_strand(map.take("name")?)?;
			let version = read_strand(map.take("version")?)?;
			map.done()?;
			Ok(Function::Model(Model {
				name,
				version,
			}))
		}
		("module", Some(arg)) => {
			let mut map = arg.map()?;
			let name = map.take("name")?.string()?;
			let sub = map.take("sub")?.option(Node::string)?;
			let prefix = match map.take("prefix")?.string()?.as_str() {
				"ext" => crate::expr::ExtPrefix::Ext,
				"mod" => crate::expr::ExtPrefix::Mod,
				other => return Err(error(format!("unknown extension prefix `{other}`"))),
			};
			map.done()?;
			Ok(Function::Module(name, sub, prefix))
		}
		("silo", Some(arg)) => {
			let mut map = arg.map()?;
			let org = map.take("org")?.string()?;
			let pkg = map.take("pkg")?.string()?;
			let major = map.take("major")?.int()?;
			let minor = map.take("minor")?.int()?;
			let patch = map.take("patch")?.int()?;
			let sub = map.take("sub")?.option(Node::string)?;
			map.done()?;
			Ok(Function::Silo {
				org,
				pkg,
				major,
				minor,
				patch,
				sub,
			})
		}
		(other, _) => unknown("Function", other),
	}
}

fn read_distance(node: Node) -> Result<Distance, ParseError> {
	let (word, arg) = node.word()?;
	match (word.as_str(), arg) {
		("Minkowski", Some(arg)) => {
			let Value::Number(order) = read_value(&arg.string()?)? else {
				return Err(error("a Minkowski order is not a number"));
			};
			Ok(Distance::Minkowski(number_to_public(&order)))
		}
		(other, None) => match distance_from(other) {
			Some(distance) => Ok(distance),
			None => unknown("Distance", other),
		},
		(other, Some(_)) => unknown("Distance", other),
	}
}

fn read_binary(node: Node) -> Result<BinaryOperator, ParseError> {
	if let Node::Str(token) = &node {
		return match binary_from(token) {
			Some(op) => Ok(op),
			None => unknown("BinaryOperator", token),
		};
	}
	let (word, arg) = node.word()?;
	match (word.as_str(), arg) {
		("Matches", Some(arg)) => {
			let mut map = arg.map()?;
			let rf: Option<u8> = map.take("rf")?.option(Node::int)?;
			let operator = read_word(map.take("operator")?, "BooleanOperator", boolean_from)?;
			map.done()?;
			Ok(BinaryOperator::Matches(MatchesOperator {
				rf,
				operator,
			}))
		}
		("K", Some(arg)) => {
			let mut map = arg.map()?;
			let k: u32 = map.take("k")?.int()?;
			let distance = read_distance(map.take("distance")?)?;
			map.done()?;
			Ok(BinaryOperator::NearestNeighbor(Box::new(NearestNeighbor::K(k, distance))))
		}
		("KTree", Some(arg)) => {
			let k: u32 = arg.int()?;
			Ok(BinaryOperator::NearestNeighbor(Box::new(NearestNeighbor::KTree(k))))
		}
		("Approximate", Some(arg)) => {
			let mut map = arg.map()?;
			let k: u32 = map.take("k")?.int()?;
			let ef: u32 = map.take("ef")?.int()?;
			map.done()?;
			Ok(BinaryOperator::NearestNeighbor(Box::new(NearestNeighbor::Approximate(k, ef))))
		}
		(other, _) => unknown("BinaryOperator", other),
	}
}

fn read_prefix(node: Node) -> Result<PrefixOperator, ParseError> {
	let (word, arg) = node.word()?;
	match (word.as_str(), arg) {
		("Cast", Some(arg)) => read_kind(arg).map(PrefixOperator::Cast),
		(other, None) => match prefix_from(other) {
			Some(op) => Ok(op),
			None => unknown("PrefixOperator", other),
		},
		(other, Some(_)) => unknown("PrefixOperator", other),
	}
}

fn read_refusal(node: Node) -> Result<Refusal, ParseError> {
	let (word, arg) = node.word()?;
	match (word.as_str(), arg) {
		("ProtectedParam", Some(arg)) => arg.string().map(|name| Refusal::ProtectedParam {
			name,
		}),
		("Query", Some(arg)) => arg.string().map(|message| Refusal::Query {
			message,
		}),
		("InvalidKnnVector", Some(arg)) => arg.string().map(|vector| Refusal::InvalidKnnVector {
			vector,
		}),
		("UncoercibleKnnVector", Some(arg)) => {
			arg.string().map(|message| Refusal::UncoercibleKnnVector {
				message,
			})
		}
		("InvalidStatement", Some(arg)) => arg.string().map(|message| Refusal::InvalidStatement {
			message,
		}),
		("InvalidFetch", Some(arg)) => arg.string().map(|value| Refusal::InvalidFetch {
			value,
		}),
		("AccessContextNotReadOnly", None) => Ok(Refusal::AccessContextNotReadOnly),
		("AccessContextRequiresRecord", None) => Ok(Refusal::AccessContextRequiresRecord),
		("NoIndexForMatch", Some(arg)) => arg.string().map(|exp| Refusal::NoIndexForMatch {
			exp,
		}),
		("InvalidBound", Some(arg)) => {
			let mut map = arg.map()?;
			let found = map.take("found")?.string()?;
			let expected = map.take("expected")?.string()?;
			map.done()?;
			Ok(Refusal::InvalidBound {
				found,
				expected,
			})
		}
		("SchemaClauseEffect", Some(arg)) => {
			let mut map = arg.map()?;
			let clause = map.take("clause")?.string()?;
			let construct = map.take("construct")?.string()?;
			map.done()?;
			Ok(Refusal::SchemaClauseEffect {
				clause,
				construct,
			})
		}
		("RecursionInstructionPlanConflict", None) => Ok(Refusal::RecursionInstructionPlanConflict),
		("UnsupportedRepeatRecurse", None) => Ok(Refusal::UnsupportedRepeatRecurse),
		("ComputationDepthExceeded", None) => Ok(Refusal::ComputationDepthExceeded),
		("AccessRedactedKey", Some(arg)) => arg.string().map(|ac| Refusal::AccessRedactedKey {
			ac,
		}),
		("NamespaceEmpty", None) => Ok(Refusal::NamespaceEmpty),
		("DatabaseEmpty", None) => Ok(Refusal::DatabaseEmpty),
		(other, _) => unknown("Refusal", other),
	}
}

fn read_raise(node: Node) -> Result<RaiseKind, ParseError> {
	let (word, arg) = node.word()?;
	match (word.as_str(), arg) {
		("Folded", Some(arg)) => {
			let mut map = arg.map()?;
			let text = map.take("text")?.string()?;
			map.done()?;
			Ok(RaiseKind::Folded(FoldedError {
				text,
			}))
		}
		("InvalidControlFlow", None) => Ok(RaiseKind::InvalidControlFlow),
		("Misplaced", Some(arg)) => {
			read_word(arg, "MisplacedControl", misplaced_from).map(RaiseKind::Misplaced)
		}
		("Refused", Some(arg)) => {
			read_refusal(arg).map(|refusal| RaiseKind::Refused(Box::new(refusal)))
		}
		(other, _) => unknown("RaiseKind", other),
	}
}

fn read_catalog_key(node: Node) -> Result<CatalogObjectKey, ParseError> {
	let mut map = node.map()?;
	let kind = read_word(map.take("kind")?, "CatalogKind", catalog_kind_from)?;
	let ns = map.take("ns")?.option(|n| n.int().map(NamespaceId))?;
	let db = map.take("db")?.option(|n| n.int().map(DatabaseId))?;
	let table = map.take("table")?.option(read_table)?;
	let name = map.take("name")?.option(read_strand)?;
	map.done()?;
	Ok(CatalogObjectKey {
		kind,
		ns,
		db,
		table,
		name,
	})
}

fn read_callee(node: Node) -> Result<CalleeFacts, ParseError> {
	let (word, arg) = node.word()?;
	match (word.as_str(), arg) {
		("Absent", None) => Ok(CalleeFacts::Absent),
		("LateBound", None) => Ok(CalleeFacts::LateBound),
		(word @ ("Defined" | "PerCall"), Some(arg)) => {
			let mut map = arg.map()?;
			let hash = map.take("hash")?.int()?;
			let permission = read_callee_permission(map.take("permission")?)?;
			let writes = map.take("writes")?.boolean()?;
			let body_depth = map.take("body_depth")?.int()?;
			let deepest = map.take("deepest")?.int()?;
			map.done()?;
			let defined = DefinedCallee {
				hash,
				permission,
				writes,
				body_depth,
				deepest,
			};
			Ok(match word {
				"PerCall" => CalleeFacts::PerCall(defined),
				_ => CalleeFacts::Defined(defined),
			})
		}
		(other, _) => unknown("CalleeFacts", other),
	}
}

fn read_callee_permission(node: Node) -> Result<CalleePermission, ParseError> {
	let (word, arg) = node.word()?;
	match (word.as_str(), arg) {
		("Full", None) => Ok(CalleePermission::Full),
		("None", None) => Ok(CalleePermission::None),
		("Predicate", None) => Ok(CalleePermission::Predicate {
			scope: None,
		}),
		("Predicate", Some(arg)) => {
			let mut map = arg.map()?;
			let scope = match map.take("scope")?.word()? {
				(word, None) if word == "PerCall" => GuardScope::PerCall,
				(word, Some(arg)) if word == "Statement" => {
					let mut inner = arg.map()?;
					let reads_data = inner.take("reads_data")?.boolean()?;
					inner.done()?;
					GuardScope::Statement {
						reads_data,
					}
				}
				(other, _) => return unknown("GuardScope", &other),
			};
			map.done()?;
			Ok(CalleePermission::Predicate {
				scope: Some(scope),
			})
		}
		(other, _) => unknown("CalleePermission", other),
	}
}

fn read_strategy(node: Node) -> Result<NestedStrategy, ParseError> {
	let (word, arg) = node.word()?;
	match (word.as_str(), arg) {
		("Apply", None) => Ok(NestedStrategy::Apply),
		("Once", None) => Ok(NestedStrategy::Once),
		("Hash", Some(arg)) => {
			let mut map = arg.map()?;
			let field = read_strand(map.take("field")?)?;
			map.done()?;
			Ok(NestedStrategy::Hash {
				field,
			})
		}
		("Lookup", Some(arg)) => {
			let mut map = arg.map()?;
			let mut shape = map.take("index")?.map()?;
			let table = read_table(shape.take("table")?)?;
			let index = read_strand(shape.take("index")?)?;
			let position = shape.take("position")?.int()?;
			let scalar = shape
				.take("scalar")?
				.list()?
				.into_iter()
				.map(Node::boolean)
				.collect::<Result<Box<[bool]>, ParseError>>()?;
			shape.done()?;
			let hashed = map.take("hashed")?.boolean()?;
			map.done()?;
			Ok(NestedStrategy::Lookup {
				index: Box::new(IndexRef {
					table,
					index,
					position,
					scalar,
				}),
				hashed,
			})
		}
		(other, _) => unknown("NestedStrategy", other),
	}
}

fn read_version(node: Node) -> Result<ObjectVersion, ParseError> {
	let (word, arg) = node.word()?;
	match (word.as_str(), arg) {
		("Stamped", Some(arg)) => read_uuid(arg).map(ObjectVersion::Stamped),
		("Content", Some(arg)) => arg.int().map(ObjectVersion::Content),
		("Inline", None) => Ok(ObjectVersion::Inline),
		(other, _) => unknown("ObjectVersion", other),
	}
}

/// Reads a value's text: the `bytes:` form, or SurrealQL.
pub(crate) fn read_value(text: &str) -> Result<Value, ParseError> {
	let text = text.trim();
	if let Some(digits) = text.strip_prefix(BYTES) {
		let bytes = hex::decode(digits).map_err(|e| error(format!("bytes are not hex: {e}")))?;
		return revision::from_slice::<Value>(&bytes)
			.map_err(|e| error(format!("bytes do not decode to a value: {e}")));
	}
	crate::syn::value(text)
		.map(convert_public_value_to_internal)
		.map_err(|e| error(format!("a value does not parse: {e}")))
}

/// `{value: <value text>}`. The value text runs to the closing brace, so the grammar of the other
/// attributes does not delimit it.
fn parse_literal(text: &str) -> Result<ExprOp, ParseError> {
	let inner = text
		.strip_prefix('{')
		.and_then(|text| text.strip_suffix('}'))
		.ok_or_else(|| error("a literal's attributes are not a map"))?;
	let rest = inner
		.trim_start()
		.strip_prefix("value")
		.and_then(|rest| rest.trim_start().strip_prefix(':'))
		.ok_or_else(|| error("a literal's attributes do not start with value"))?;
	read_value(rest).map(|value| ExprOp::Literal(Box::new(value)))
}

/// Reads the attributes of the op `tag` names from `text`, which has no mnemonic.
fn parse_op(tag: ExprTag, text: &str) -> Result<ExprOp, ParseError> {
	match tag {
		ExprTag::Array => return no_attrs(ExprOp::Array, text),
		ExprTag::Set => return no_attrs(ExprOp::Set, text),
		ExprTag::Doc => return no_attrs(ExprOp::Doc, text),
		ExprTag::Index => return no_attrs(ExprOp::Index, text),
		ExprTag::First => return no_attrs(ExprOp::First, text),
		ExprTag::Last => return no_attrs(ExprOp::Last, text),
		ExprTag::Each => return no_attrs(ExprOp::Each, text),
		ExprTag::Optional => return no_attrs(ExprOp::Optional, text),
		ExprTag::RepeatRecurse => return no_attrs(ExprOp::RepeatRecurse, text),
		ExprTag::Invoke => return no_attrs(ExprOp::Invoke, text),
		ExprTag::If => return no_attrs(ExprOp::If, text),
		ExprTag::Throw => return no_attrs(ExprOp::Throw, text),
		ExprTag::Subquery if text.is_empty() => return Ok(ExprOp::Subquery(NestedStrategy::Apply)),
		ExprTag::Yield => return no_attrs(ExprOp::Yield, text),
		ExprTag::Truthy => return no_attrs(ExprOp::Truthy, text),
		ExprTag::Literal => return parse_literal(text),
		ExprTag::Object
		| ExprTag::RecordId
		| ExprTag::FreeParam
		| ExprTag::SessionParam
		| ExprTag::CatalogParam
		| ExprTag::Table
		| ExprTag::Mock
		| ExprTag::Constant
		| ExprTag::Field
		| ExprTag::ComputedRead
		| ExprTag::Access
		| ExprTag::Deref
		| ExprTag::Flatten
		| ExprTag::Dispatch
		| ExprTag::Method
		| ExprTag::Destructure
		| ExprTag::SetField
		| ExprTag::TupleRow
		| ExprTag::Prefix
		| ExprTag::Convert
		| ExprTag::Postfix
		| ExprTag::Binary
		| ExprTag::ShortCircuit
		| ExprTag::Call
		| ExprTag::ClosureCreate
		| ExprTag::Agg
		| ExprTag::Now
		| ExprTag::Raise
		| ExprTag::Subquery
		| ExprTag::DepthCharge => {}
	}
	let mut map = fields(text)?;
	let op = match tag {
		ExprTag::Object => ExprOp::Object {
			keys: read_names(map.take("keys")?)?,
		},
		ExprTag::RecordId => {
			let table = read_table(map.take("table")?)?;
			let key_shape = read_key_shape(map.take("key_shape")?)?;
			ExprOp::RecordId(Box::new(RecordIdAttrs {
				table,
				key_shape,
			}))
		}
		ExprTag::FreeParam => ExprOp::FreeParam {
			name: read_strand(map.take("name")?)?,
		},
		ExprTag::SessionParam => ExprOp::SessionParam {
			name: read_strand(map.take("name")?)?,
		},
		ExprTag::CatalogParam => ExprOp::CatalogParam {
			name: read_strand(map.take("name")?)?,
		},
		ExprTag::Table => ExprOp::Table {
			name: read_table(map.take("name")?)?,
		},
		ExprTag::Mock => ExprOp::Mock(Box::new(read_mock(map.take("mock")?)?)),
		ExprTag::Constant => ExprOp::Constant {
			c: read_word(map.take("c")?, "Constant", constant_from)?,
		},
		ExprTag::Field => ExprOp::Field {
			name: read_strand(map.take("name")?)?,
		},
		ExprTag::ComputedRead => {
			let field = read_strand(map.take("field")?)?;
			let mut def = map.take("def")?.map()?;
			let key = read_catalog_key(def.take("key")?)?;
			let version = read_version(def.take("version")?)?;
			def.done()?;
			ExprOp::ComputedRead(Box::new(ComputedReadAttrs {
				field,
				def: (key, version),
			}))
		}
		ExprTag::Access => ExprOp::Access {
			field: read_strand(map.take("field")?)?,
		},
		ExprTag::Deref => ExprOp::Deref {
			table_hint: map.take("table_hint")?.option(read_table)?.map(Box::new),
		},
		ExprTag::Flatten => ExprOp::Flatten {
			explicit: map.take("explicit")?.boolean()?,
		},
		ExprTag::Dispatch => {
			let (word, arg) = map.take("target")?.word()?;
			ExprOp::Dispatch(Box::new(match (word.as_str(), arg) {
				("Method", Some(arg)) => DispatchTarget::Method(read_strand(arg)?),
				("Invoke", None) => DispatchTarget::Invoke,
				(other, _) => return unknown("DispatchTarget", other),
			}))
		}
		ExprTag::Method => ExprOp::Method {
			name: read_strand(map.take("name")?)?,
		},
		ExprTag::Destructure => ExprOp::Destructure {
			shape: read_entries(map.take("shape")?)?,
		},
		ExprTag::SetField => ExprOp::SetField {
			field: read_strand(map.take("field")?)?,
		},
		ExprTag::TupleRow => ExprOp::TupleRow {
			places: read_places(map.take("places")?)?,
		},
		ExprTag::Prefix => ExprOp::Prefix(Box::new(read_prefix(map.take("op")?)?)),
		ExprTag::Convert => {
			let mode = read_word(map.take("mode")?, "ConvertMode", convert_mode_from)?;
			let kind = read_kind(map.take("kind")?)?;
			let site = ConvertSite(map.take("site")?.int()?);
			let on_fail = read_word(map.take("on_fail")?, "OnFail", on_fail_from)?;
			ExprOp::Convert(Box::new(ConvertAttrs {
				mode,
				kind,
				site,
				on_fail,
			}))
		}
		ExprTag::Postfix => ExprOp::Postfix {
			op: read_word(map.take("op")?, "LogicalPostfixOperator", postfix_from)?,
		},
		ExprTag::Binary => ExprOp::Binary {
			op: read_binary(map.take("op")?)?,
		},
		ExprTag::ShortCircuit => ExprOp::ShortCircuit {
			op: read_word(map.take("op")?, "ShortCircuitOp", short_circuit_from)?,
		},
		ExprTag::Call => {
			let function = read_function(map.take("function")?)?;
			let row_names = map.take("row_names")?.int()?;
			let callee = map.take("callee")?.option(read_callee)?;
			ExprOp::Call(Box::new(CallAttrs {
				function,
				row_names,
				callee,
			}))
		}
		ExprTag::ClosureCreate => {
			let params = map
				.take("params")?
				.list()?
				.into_iter()
				.map(|param| {
					let mut param = param.map()?;
					let name = read_strand(param.take("name")?)?;
					let kind = read_kind(param.take("kind")?)?;
					param.done()?;
					Ok((name, kind))
				})
				.collect::<Result<Box<[(Strand, Kind)]>, ParseError>>()?;
			let returns = map.take("returns")?.option(read_kind)?;
			let body_sql: Arc<str> = map.take("body_sql")?.string()?.into();
			let captures = map
				.take("captures")?
				.list()?
				.into_iter()
				.map(read_strand)
				.collect::<Result<Box<[Strand]>, ParseError>>()?;
			let reads_row = map.take("reads_row")?.boolean()?;
			let reads_root = map.take("reads_root")?.boolean()?;
			ExprOp::ClosureCreate(Box::new(ClosureAttrs {
				params,
				returns,
				body_sql,
				captures,
				reads_row,
				reads_root,
				literal: Default::default(),
			}))
		}
		ExprTag::Agg => {
			let name = map.take("kind")?.string()?;
			let combine = read_word(map.take("combine")?, "AggregateCombine", combine_from)?;
			let Some(registered) = aggregates().get_aggregate(&name) else {
				return Err(error(format!("{name} is not a registered aggregate")));
			};
			ExprOp::Agg {
				kind: registered.name(),
				combine,
			}
		}
		ExprTag::Now => ExprOp::Now {
			which: read_word(map.take("which")?, "NowWhich", now_which_from)?,
		},
		ExprTag::Raise => ExprOp::Raise(Box::new(read_raise(map.take("kind")?)?)),
		ExprTag::DepthCharge => ExprOp::DepthCharge {
			levels: map.take("levels")?.int()?,
		},
		ExprTag::Subquery => ExprOp::Subquery(read_strategy(map.take("strategy")?)?),
		ExprTag::Literal
		| ExprTag::Array
		| ExprTag::Set
		| ExprTag::Doc
		| ExprTag::Index
		| ExprTag::First
		| ExprTag::Last
		| ExprTag::Each
		| ExprTag::Optional
		| ExprTag::RepeatRecurse
		| ExprTag::Invoke
		| ExprTag::If
		| ExprTag::Throw
		| ExprTag::Yield
		| ExprTag::Truthy => {
			return Err(error(format!("{} has no attribute map", tag.mnemonic())));
		}
	};
	map.done()?;
	Ok(op)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn strand_of(text: &str) -> Strand {
		Strand::new(text)
	}

	fn table_of(text: &str) -> TableName {
		TableName::new(strand_of(text))
	}

	fn value(text: &str) -> Value {
		convert_public_value_to_internal(crate::syn::value(text).expect("a value"))
	}

	fn kind_of(text: &str) -> Kind {
		crate::syn::kind(text).expect("a kind")
	}

	fn float(f: f64) -> Value {
		Value::Number(Number::Float(f))
	}

	fn printed(op: &ExprOp) -> String {
		let mut text = String::new();
		ExprSyntax.print_attrs(&OpKind::Expr(op.clone()), &mut text);
		text
	}

	/// Prints `op`, parses the text and prints the result: the second text equals the first, and
	/// the parsed op has the same tag and attribute digest.
	fn assert_round_trips(op: &ExprOp) {
		let first = printed(op);
		let parsed = ExprSyntax
			.parse_attrs(OpTag::Expr(op.tag()), &first)
			.unwrap_or_else(|e| panic!("{first} does not parse: {e}"));
		let OpKind::Expr(parsed) = parsed else {
			panic!("{first} parsed to another dialect");
		};
		assert_eq!(printed(&parsed), first, "{first} changes when parsed");
		assert_eq!(parsed.tag(), op.tag(), "{first} changes its tag");
		assert_eq!(parsed.attr_digest(), op.attr_digest(), "{first} changes its attributes");
	}

	fn parse(tag: ExprTag, text: &str) -> Result<OpKind, ParseError> {
		ExprSyntax.parse_attrs(OpTag::Expr(tag), text)
	}

	/// The simplest op of each tag.
	fn fixture(tag: ExprTag) -> ExprOp {
		match tag {
			ExprTag::Literal => ExprOp::Literal(Box::new(Value::from(1i64))),
			ExprTag::Array => ExprOp::Array,
			ExprTag::Set => ExprOp::Set,
			ExprTag::Object => ExprOp::Object {
				keys: Box::new(["a".to_owned(), "b c".to_owned()]),
			},
			ExprTag::RecordId => ExprOp::RecordId(Box::new(RecordIdAttrs {
				table: table_of("t"),
				key_shape: KeyShape::Number(1),
			})),
			ExprTag::FreeParam => ExprOp::FreeParam {
				name: strand_of("q"),
			},
			ExprTag::SessionParam => ExprOp::SessionParam {
				name: strand_of("auth"),
			},
			ExprTag::CatalogParam => ExprOp::CatalogParam {
				name: strand_of("p"),
			},
			ExprTag::Table => ExprOp::Table {
				name: table_of("t"),
			},
			ExprTag::Mock => ExprOp::Mock(Box::new(Mock::Count(table_of("t"), 3))),
			ExprTag::Constant => ExprOp::Constant {
				c: Constant::MathE,
			},
			ExprTag::Field => ExprOp::Field {
				name: strand_of("a"),
			},
			ExprTag::ComputedRead => ExprOp::ComputedRead(Box::new(ComputedReadAttrs {
				field: strand_of("a"),
				def: (
					CatalogObjectKey {
						kind: CatalogKind::Field,
						ns: Some(NamespaceId(1)),
						db: Some(DatabaseId(2)),
						table: Some(table_of("t")),
						name: Some(strand_of("a")),
					},
					ObjectVersion::Content(u64::MAX),
				),
			})),
			ExprTag::Doc => ExprOp::Doc,
			ExprTag::Access => ExprOp::Access {
				field: strand_of("a"),
			},
			ExprTag::Deref => ExprOp::Deref {
				table_hint: Some(Box::new(table_of("t"))),
			},
			ExprTag::Index => ExprOp::Index,
			ExprTag::First => ExprOp::First,
			ExprTag::Last => ExprOp::Last,
			ExprTag::Each => ExprOp::Each,
			ExprTag::Flatten => ExprOp::Flatten {
				explicit: true,
			},
			ExprTag::Dispatch => {
				ExprOp::Dispatch(Box::new(DispatchTarget::Method(strand_of("len"))))
			}
			ExprTag::Method => ExprOp::Method {
				name: strand_of("len"),
			},
			ExprTag::Destructure => ExprOp::Destructure {
				shape: Box::new([DestructureEntry::Field(strand_of("a"))]),
			},
			ExprTag::Optional => ExprOp::Optional,
			ExprTag::RepeatRecurse => ExprOp::RepeatRecurse,
			ExprTag::SetField => ExprOp::SetField {
				field: strand_of("a"),
			},
			ExprTag::TupleRow => ExprOp::TupleRow {
				places: Box::new([Place {
					parts: Box::new([PlacePart::Field(strand_of("a"))]),
				}]),
			},
			ExprTag::Prefix => ExprOp::Prefix(Box::new(PrefixOperator::Not)),
			ExprTag::Convert => ExprOp::Convert(Box::new(ConvertAttrs {
				mode: ConvertMode::Cast,
				kind: Kind::Int,
				site: ConvertSite(7),
				on_fail: OnFail::Error,
			})),
			ExprTag::Postfix => ExprOp::Postfix {
				op: LogicalPostfixOperator::Range,
			},
			ExprTag::Invoke => ExprOp::Invoke,
			ExprTag::Binary => ExprOp::Binary {
				op: BinaryOperator::Add,
			},
			ExprTag::ShortCircuit => ExprOp::ShortCircuit {
				op: ShortCircuitOp::And,
			},
			ExprTag::Call => ExprOp::call(Function::Normal("math::abs".to_owned())),
			ExprTag::ClosureCreate => ExprOp::ClosureCreate(Box::new(ClosureAttrs {
				params: Box::new([(strand_of("a"), Kind::Int)]),
				returns: Some(Kind::String),
				body_sql: "$a + 'x'".into(),
				captures: Box::new([]),
				reads_row: false,
				reads_root: false,
				literal: Default::default(),
			})),
			ExprTag::If => ExprOp::If,
			ExprTag::Throw => ExprOp::Throw,
			ExprTag::Subquery => ExprOp::Subquery(NestedStrategy::Apply),
			ExprTag::Yield => ExprOp::Yield,
			ExprTag::Truthy => ExprOp::Truthy,
			ExprTag::Agg => ExprOp::Agg {
				kind: "math::mean",
				combine: AggregateCombine::InexactFloat,
			},
			ExprTag::Now => ExprOp::Now {
				which: NowWhich::Datetime,
			},
			ExprTag::Raise => ExprOp::Raise(Box::new(RaiseKind::InvalidControlFlow)),
			ExprTag::DepthCharge => ExprOp::DepthCharge {
				levels: 1,
			},
		}
	}

	fn binary_operators() -> Vec<BinaryOperator> {
		vec![
			BinaryOperator::Subtract,
			BinaryOperator::Add,
			BinaryOperator::Multiply,
			BinaryOperator::Divide,
			BinaryOperator::Remainder,
			BinaryOperator::Power,
			BinaryOperator::Equal,
			BinaryOperator::ExactEqual,
			BinaryOperator::NotEqual,
			BinaryOperator::AllEqual,
			BinaryOperator::AnyEqual,
			BinaryOperator::Or,
			BinaryOperator::And,
			BinaryOperator::NullCoalescing,
			BinaryOperator::TenaryCondition,
			BinaryOperator::LessThan,
			BinaryOperator::LessThanEqual,
			BinaryOperator::MoreThan,
			BinaryOperator::MoreThanEqual,
			BinaryOperator::Contain,
			BinaryOperator::NotContain,
			BinaryOperator::ContainAll,
			BinaryOperator::ContainAny,
			BinaryOperator::ContainNone,
			BinaryOperator::Inside,
			BinaryOperator::NotInside,
			BinaryOperator::AllInside,
			BinaryOperator::AnyInside,
			BinaryOperator::NoneInside,
			BinaryOperator::Outside,
			BinaryOperator::Intersects,
			BinaryOperator::Range,
			BinaryOperator::RangeInclusive,
			BinaryOperator::RangeSkip,
			BinaryOperator::RangeSkipInclusive,
			BinaryOperator::Matches(MatchesOperator {
				rf: None,
				operator: BooleanOperator::And,
			}),
			BinaryOperator::Matches(MatchesOperator {
				rf: Some(3),
				operator: BooleanOperator::Or,
			}),
			BinaryOperator::NearestNeighbor(Box::new(NearestNeighbor::KTree(5))),
			BinaryOperator::NearestNeighbor(Box::new(NearestNeighbor::Approximate(5, 40))),
		]
	}

	fn distances() -> Vec<Distance> {
		vec![
			Distance::Chebyshev,
			Distance::Cosine,
			Distance::Euclidean,
			Distance::Hamming,
			Distance::Jaccard,
			Distance::Manhattan,
			Distance::Pearson,
			Distance::CosineNormalized,
			Distance::InnerProduct,
			Distance::Minkowski(PublicNumber::Int(3)),
			Distance::Minkowski(PublicNumber::Float(2.5)),
			Distance::Minkowski(PublicNumber::Float(f64::NAN)),
		]
	}

	fn prefix_operators() -> Vec<PrefixOperator> {
		vec![
			PrefixOperator::Not,
			PrefixOperator::Positive,
			PrefixOperator::Negate,
			PrefixOperator::Range,
			PrefixOperator::RangeInclusive,
			PrefixOperator::Cast(Kind::Int),
			PrefixOperator::Cast(kind_of("array<int, 3>")),
		]
	}

	fn key_shapes() -> Vec<KeyShape> {
		let range = |start, end| {
			KeyShape::Range(Box::new(KeyRange {
				start,
				end,
			}))
		};
		vec![
			KeyShape::Number(i64::MIN),
			KeyShape::String(strand_of("it's \"quoted\"")),
			KeyShape::Uuid(Uuid::nil()),
			KeyShape::Generate(RecordIdKeyGen::Rand),
			KeyShape::Generate(RecordIdKeyGen::Ulid),
			KeyShape::Generate(RecordIdKeyGen::Uuid),
			KeyShape::Array {
				len: 2,
			},
			KeyShape::Object {
				keys: Box::new(["a".to_owned(), "b".to_owned()]),
			},
			range(Bound::Included(KeyBound::Number(1)), Bound::Unbounded),
			range(Bound::Unbounded, Bound::Excluded(KeyBound::String(strand_of("z")))),
			range(
				Bound::Excluded(KeyBound::Uuid(Uuid::nil())),
				Bound::Included(KeyBound::Generate(RecordIdKeyGen::Ulid)),
			),
			range(
				Bound::Included(KeyBound::Array {
					len: 1,
				}),
				Bound::Included(KeyBound::Object {
					keys: Box::new(["k".to_owned()]),
				}),
			),
			range(Bound::Unbounded, Bound::Unbounded),
		]
	}

	fn destructure_entries() -> Vec<DestructureEntry> {
		vec![
			DestructureEntry::All(strand_of("a")),
			DestructureEntry::Field(strand_of("b")),
			DestructureEntry::Aliased(strand_of("c")),
			DestructureEntry::Destructure(
				strand_of("d"),
				Box::new([
					DestructureEntry::Field(strand_of("e")),
					DestructureEntry::Destructure(
						strand_of("f"),
						Box::new([DestructureEntry::Aliased(strand_of("g"))]),
					),
					DestructureEntry::Rest,
				]),
			),
			DestructureEntry::Rest,
		]
	}

	fn functions() -> Vec<Function> {
		vec![
			Function::Normal("math::abs".to_owned()),
			Function::Custom("my::fn".to_owned()),
			Function::Script(Script("return 'a' + \"b\";\n".to_owned())),
			Function::Model(Model {
				name: strand_of("ml::m"),
				version: strand_of("1.0.0"),
			}),
			Function::Module("m".to_owned(), None, crate::expr::ExtPrefix::Ext),
			Function::Module("m".to_owned(), Some("sub".to_owned()), crate::expr::ExtPrefix::Mod),
			Function::Silo {
				org: "o".to_owned(),
				pkg: "p".to_owned(),
				major: 1,
				minor: 2,
				patch: u32::MAX,
				sub: None,
			},
			Function::Silo {
				org: "o".to_owned(),
				pkg: "p".to_owned(),
				major: 0,
				minor: 0,
				patch: 0,
				sub: Some("s".to_owned()),
			},
		]
	}

	fn refusals() -> Vec<Refusal> {
		vec![
			Refusal::ProtectedParam {
				name: "auth".to_owned(),
			},
			Refusal::Query {
				message: "m".to_owned(),
			},
			Refusal::InvalidKnnVector {
				vector: "v".to_owned(),
			},
			Refusal::UncoercibleKnnVector {
				message: "m".to_owned(),
			},
			Refusal::InvalidStatement {
				message: "m".to_owned(),
			},
			Refusal::InvalidFetch {
				value: "v".to_owned(),
			},
			Refusal::SchemaClauseEffect {
				clause: "VALUE".to_owned(),
				construct: "`CREATE` writes data".to_owned(),
			},
			Refusal::AccessContextNotReadOnly,
			Refusal::AccessContextRequiresRecord,
			Refusal::NoIndexForMatch {
				exp: "a @@ 'b'".to_owned(),
			},
			Refusal::InvalidBound {
				found: "0".to_owned(),
				expected: "1..=256".to_owned(),
			},
			Refusal::RecursionInstructionPlanConflict,
			Refusal::UnsupportedRepeatRecurse,
			Refusal::ComputationDepthExceeded,
			Refusal::AccessRedactedKey {
				ac: "a".to_owned(),
			},
			Refusal::NamespaceEmpty,
			Refusal::DatabaseEmpty,
		]
	}

	fn raise_kinds() -> Vec<RaiseKind> {
		let mut kinds = vec![
			RaiseKind::Folded(FoldedError {
				text: "An error: 'x' \"y\"".to_owned(),
			}),
			RaiseKind::InvalidControlFlow,
			RaiseKind::Misplaced(MisplacedControl::NestedBegin),
			RaiseKind::Misplaced(MisplacedControl::BeginInSession),
			RaiseKind::Misplaced(MisplacedControl::CommitOutsideBlock),
			RaiseKind::Misplaced(MisplacedControl::CancelOutsideBlock),
		];
		kinds.extend(refusals().into_iter().map(|r| RaiseKind::Refused(Box::new(r))));
		kinds
	}

	fn catalog_kinds() -> Vec<CatalogKind> {
		vec![
			CatalogKind::Table,
			CatalogKind::Field,
			CatalogKind::Index,
			CatalogKind::Event,
			CatalogKind::View,
			CatalogKind::Function,
			CatalogKind::Param,
			CatalogKind::Access,
			CatalogKind::Other,
		]
	}

	fn constants() -> Vec<Constant> {
		vec![
			Constant::MathE,
			Constant::MathFrac1Pi,
			Constant::MathFrac1Sqrt2,
			Constant::MathFrac2Pi,
			Constant::MathFrac2SqrtPi,
			Constant::MathFracPi2,
			Constant::MathFracPi3,
			Constant::MathFracPi4,
			Constant::MathFracPi6,
			Constant::MathFracPi8,
			Constant::MathInfinity,
			Constant::MathLn10,
			Constant::MathLn2,
			Constant::MathLog102,
			Constant::MathLog10E,
			Constant::MathLog210,
			Constant::MathLog2E,
			Constant::MathNegInfinity,
			Constant::MathPi,
			Constant::MathSqrt2,
			Constant::MathTau,
			Constant::TimeEpoch,
			Constant::TimeMin,
			Constant::TimeMax,
			Constant::DurationMax,
		]
	}

	/// Values whose text round-trips and values whose text does not.
	fn literals() -> Vec<Value> {
		vec![
			Value::None,
			Value::Null,
			Value::Bool(true),
			Value::from(0i64),
			Value::from(i64::MAX),
			Value::from(i64::MIN),
			float(1.5),
			float(0.0),
			float(-0.0),
			float(f64::NAN),
			float(f64::INFINITY),
			float(f64::NEG_INFINITY),
			value("1dec"),
			value("0dec"),
			value("''"),
			value("'a'"),
			value("\"it's\""),
			value("'both \\' and \"'"),
			value("'line\\nbreak\\ttab'"),
			value("1h30m"),
			value("d'2020-01-02T03:04:05Z'"),
			value("u'00000000-0000-0000-0000-000000000000'"),
			value("[1, 'a', [2, 3]]"),
			value("{ a: 1, b: { c: [true, NONE, NULL] }, 'd e': 'f' }"),
			value("{}"),
			value("[]"),
		]
	}

	#[test]
	fn round_trip_every_payload_variant() {
		let mut ops: Vec<ExprOp> = ExprTag::ALL.iter().map(|tag| fixture(*tag)).collect();
		ops.extend(literals().into_iter().map(|v| ExprOp::Literal(Box::new(v))));
		ops.extend(binary_operators().into_iter().map(|op| ExprOp::Binary {
			op,
		}));
		ops.extend(distances().into_iter().map(|distance| ExprOp::Binary {
			op: BinaryOperator::NearestNeighbor(Box::new(NearestNeighbor::K(2, distance))),
		}));
		ops.extend(prefix_operators().into_iter().map(|op| ExprOp::Prefix(Box::new(op))));
		ops.extend(key_shapes().into_iter().map(|key_shape| {
			ExprOp::RecordId(Box::new(RecordIdAttrs {
				table: table_of("t"),
				key_shape,
			}))
		}));
		ops.extend(raise_kinds().into_iter().map(|kind| ExprOp::Raise(Box::new(kind))));
		ops.extend(functions().into_iter().map(ExprOp::call));
		for callee in [
			None,
			Some(CalleeFacts::Absent),
			Some(CalleeFacts::LateBound),
			Some(CalleeFacts::Defined(DefinedCallee {
				hash: u64::MAX,
				permission: CalleePermission::Predicate {
					scope: None,
				},
				writes: false,
				body_depth: 0,
				deepest: 0,
			})),
			Some(CalleeFacts::Defined(DefinedCallee {
				hash: 1,
				permission: CalleePermission::Predicate {
					scope: Some(GuardScope::PerCall),
				},
				writes: false,
				body_depth: 0,
				deepest: 0,
			})),
			Some(CalleeFacts::Defined(DefinedCallee {
				hash: 2,
				permission: CalleePermission::Predicate {
					scope: Some(GuardScope::Statement {
						reads_data: true,
					}),
				},
				writes: true,
				body_depth: 3,
				deepest: u8::MAX,
			})),
			Some(CalleeFacts::Defined(DefinedCallee {
				hash: 0,
				permission: CalleePermission::None,
				writes: false,
				body_depth: 0,
				deepest: 0,
			})),
		] {
			ops.push(ExprOp::Call(Box::new(CallAttrs {
				function: Function::Custom("my::fn".to_owned()),
				row_names: 2,
				callee,
			})));
		}
		ops.extend(constants().into_iter().map(|c| ExprOp::Constant {
			c,
		}));
		ops.push(ExprOp::Destructure {
			shape: destructure_entries().into_boxed_slice(),
		});
		ops.push(ExprOp::Destructure {
			shape: Box::new([]),
		});
		ops.push(ExprOp::Object {
			keys: Box::new([]),
		});
		ops.push(ExprOp::TupleRow {
			places: Box::new([]),
		});
		ops.push(ExprOp::TupleRow {
			places: Box::new([
				Place {
					parts: Box::new([
						PlacePart::Field(strand_of("a")),
						PlacePart::Dynamic(3),
						PlacePart::Filter(0),
						PlacePart::Index(4),
						PlacePart::All,
						PlacePart::First,
						PlacePart::Last,
						PlacePart::Flatten,
						PlacePart::Optional,
						PlacePart::Doc,
						PlacePart::RepeatRecurse,
					]),
				},
				Place {
					parts: Box::new([
						PlacePart::Field(strand_of("b c")),
						PlacePart::Unaddressed(UnaddressedPart::Lookup, "->edge".into()),
						PlacePart::Unaddressed(UnaddressedPart::Destructure, "{a, b}".into()),
						PlacePart::Unaddressed(UnaddressedPart::Recurse, "{1..2}".into()),
						PlacePart::Unaddressed(UnaddressedPart::Start, "[START 1]".into()),
						PlacePart::Unaddressed(UnaddressedPart::Method, "x(\"q\")".into()),
					]),
				},
				Place {
					parts: Box::new([]),
				},
			]),
		});
		for mock in [
			Mock::Count(table_of("t"), -1),
			Mock::Range(
				table_of("t"),
				TypedRange {
					start: Bound::Included(1),
					end: Bound::Excluded(9),
				},
			),
			Mock::Range(
				table_of("t"),
				TypedRange {
					start: Bound::Unbounded,
					end: Bound::Unbounded,
				},
			),
		] {
			ops.push(ExprOp::Mock(Box::new(mock)));
		}
		for kind in catalog_kinds() {
			ops.push(ExprOp::ComputedRead(Box::new(ComputedReadAttrs {
				field: strand_of("f"),
				def: (
					CatalogObjectKey {
						kind,
						ns: None,
						db: None,
						table: None,
						name: None,
					},
					ObjectVersion::Inline,
				),
			})));
		}
		ops.push(ExprOp::ComputedRead(Box::new(ComputedReadAttrs {
			field: strand_of("f"),
			def: (
				CatalogObjectKey {
					kind: CatalogKind::Field,
					ns: Some(NamespaceId(0)),
					db: Some(DatabaseId(u32::MAX)),
					table: Some(table_of("t")),
					name: None,
				},
				ObjectVersion::Stamped(uuid::Uuid::nil()),
			),
		})));
		ops.push(ExprOp::Deref {
			table_hint: None,
		});
		ops.push(ExprOp::Flatten {
			explicit: false,
		});
		ops.push(ExprOp::Dispatch(Box::new(DispatchTarget::Invoke)));
		ops.push(ExprOp::ClosureCreate(Box::new(ClosureAttrs {
			params: Box::new([]),
			returns: None,
			body_sql: "".into(),
			captures: Box::new([]),
			reads_row: false,
			reads_root: false,
			literal: Default::default(),
		})));
		ops.push(ExprOp::Postfix {
			op: LogicalPostfixOperator::RangeSkip,
		});
		for op in [
			ShortCircuitOp::And,
			ShortCircuitOp::Or,
			ShortCircuitOp::NullCoalescing,
			ShortCircuitOp::TernaryCondition,
		] {
			ops.push(ExprOp::ShortCircuit {
				op,
			});
		}
		for which in [
			NowWhich::Datetime,
			NowWhich::UnixSeconds,
			NowWhich::UnixMillis,
			NowWhich::UnixMicros,
			NowWhich::UnixNanos,
		] {
			ops.push(ExprOp::Now {
				which,
			});
		}
		for combine in [
			AggregateCombine::Exact,
			AggregateCombine::Appending,
			AggregateCombine::InexactFloat,
			AggregateCombine::NotDecomposable,
		] {
			ops.push(ExprOp::Agg {
				kind: "math::mean",
				combine,
			});
		}
		for (mode, on_fail) in
			[(ConvertMode::Cast, OnFail::Error), (ConvertMode::Coerce, OnFail::None)]
		{
			ops.push(ExprOp::Convert(Box::new(ConvertAttrs {
				mode,
				kind: kind_of("option<array<int>>"),
				site: ConvertSite(u32::MAX),
				on_fail,
			})));
		}
		ops.push(ExprOp::DepthCharge {
			levels: u32::MAX,
		});
		for strategy in [
			NestedStrategy::Apply,
			NestedStrategy::Once,
			NestedStrategy::Hash {
				field: strand_of("a\"b"),
			},
			NestedStrategy::Lookup {
				index: Box::new(IndexRef {
					table: table_of("t"),
					index: strand_of("ix"),
					position: u32::MAX,
					scalar: Box::new([true, false]),
				}),
				hashed: false,
			},
			NestedStrategy::Lookup {
				index: Box::new(IndexRef {
					table: table_of("t"),
					index: strand_of("ix"),
					position: 0,
					scalar: Box::new([]),
				}),
				hashed: true,
			},
		] {
			ops.push(ExprOp::Subquery(strategy));
		}
		for op in &ops {
			assert_round_trips(op);
		}
	}

	#[test]
	fn floats_with_no_plain_text_read_back_to_the_same_value() {
		for v in [float(f64::NAN), float(-0.0), float(f64::INFINITY), float(f64::NEG_INFINITY)] {
			let text = value_text(&v);
			let read = read_value(&text).expect("the printed text reads");
			assert_eq!(
				revision::to_vec(&read).expect("encodes"),
				revision::to_vec(&v).expect("encodes"),
				"{text} reads back to another value"
			);
		}
		for v in [float(f64::NAN), float(-0.0), float(f64::INFINITY), float(f64::NEG_INFINITY)] {
			let text = value_text(&v);
			assert!(text == v.to_sql() || text.starts_with(BYTES), "{text}");
		}
		assert_eq!(value_text(&Value::from(1i64)), "1");
	}

	#[test]
	fn the_enum_tables_name_every_variant_once() {
		for c in constants() {
			assert_eq!(constant_from(constant_text(&c)), Some(c));
		}
		for k in catalog_kinds() {
			assert_eq!(catalog_kind_from(catalog_kind_text(&k)), Some(k));
		}
		for op in binary_operators() {
			if let Some(token) = binary_text(&op) {
				assert_eq!(binary_from(token), Some(op));
			}
		}
		for d in distances() {
			if let Some(word) = distance_text(&d) {
				assert_eq!(distance_from(word), Some(d));
			}
		}
		for op in prefix_operators() {
			if let Some(word) = prefix_text(&op) {
				assert_eq!(prefix_from(word), Some(op));
			}
		}
	}

	#[test]
	fn parse_accepts_with_or_without_mnemonic() {
		for tag in ExprTag::ALL {
			let op = fixture(*tag);
			let with = printed(&op);
			let without = with
				.strip_prefix(tag.mnemonic())
				.expect("a print starts with the mnemonic")
				.trim_start()
				.to_owned();
			let a = parse(*tag, &with).expect("with the mnemonic");
			let b = parse(*tag, &without).expect("without it");
			let (OpKind::Expr(a), OpKind::Expr(b)) = (a, b) else {
				panic!("{with} parsed to another dialect");
			};
			assert_eq!(printed(&a), with);
			assert_eq!(printed(&b), with);
		}
	}

	#[test]
	fn a_tag_without_attributes_prints_only_its_mnemonic() {
		assert_eq!(printed(&ExprOp::Array), "expr.array");
		assert_eq!(printed(&ExprOp::Yield), "expr.yield");
	}

	#[test]
	fn malformed_text_is_a_parse_error() {
		// An unknown key, a missing key and an extra key.
		assert!(parse(ExprTag::Field, "{nam: 'a'}").is_err());
		assert!(parse(ExprTag::Field, "{}").is_err());
		assert!(parse(ExprTag::Field, "{name: 'a', extra: 1}").is_err());
		// The text of another tag, with and without its mnemonic.
		assert!(parse(ExprTag::Field, "expr.access {field: 'a'}").is_err());
		assert!(parse(ExprTag::Field, "{field: 'a'}").is_err());
		// Truncated text.
		assert!(parse(ExprTag::Field, "{name: 'a'").is_err());
		assert!(parse(ExprTag::Field, "{name: 'a").is_err());
		assert!(parse(ExprTag::Field, "{name:").is_err());
		assert!(parse(ExprTag::Field, "").is_err());
		assert!(parse(ExprTag::Field, "expr.field").is_err());
		// Attributes on a tag that has none, and text after the attributes.
		assert!(parse(ExprTag::Array, "{name: 'a'}").is_err());
		assert!(parse(ExprTag::Field, "{name: 'a'} {name: 'b'}").is_err());
		// An aggregate no registry holds.
		assert!(parse(ExprTag::Agg, "{kind: 'no::such::aggregate', combine: Exact}").is_err());
		assert!(parse(ExprTag::Agg, "{kind: 'math::mean', combine: Exact}").is_ok());
		// A variant no enum has, and a value of the wrong shape.
		assert!(parse(ExprTag::Now, "{which: Sometime}").is_err());
		assert!(parse(ExprTag::Now, "{which: 'Datetime'}").is_err());
		assert!(parse(ExprTag::DepthCharge, "{levels: -1}").is_err());
		assert!(parse(ExprTag::DepthCharge, "{levels: 4294967296}").is_err());
		assert!(parse(ExprTag::Binary, "{op: '<=>'}").is_err());
		assert!(parse(ExprTag::Flatten, "{explicit: maybe}").is_err());
		assert!(parse(ExprTag::Literal, "{values: 1}").is_err());
		assert!(parse(ExprTag::Literal, "{value: bytes:zz}").is_err());
		assert!(parse(ExprTag::Literal, "{value: (}").is_err());
		assert!(
			parse(ExprTag::Convert, "{mode: Cast, kind: 'int(', site: 0, on_fail: Error}").is_err()
		);
		// A tag of another dialect.
		let other = OpTag::ALL
			.iter()
			.find(|tag| !matches!(tag, OpTag::Expr(_)))
			.expect("another dialect has tags");
		assert!(ExprSyntax.parse_attrs(*other, "").is_err());
	}

	#[test]
	fn nesting_is_bounded() {
		let deep = format!("{{shape: {}{}}}", "[".repeat(200), "]".repeat(200));
		assert!(parse(ExprTag::Destructure, &deep).is_err());
	}
}
