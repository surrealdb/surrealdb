mod de;
mod ser;

use std::borrow::Cow;

pub use de::{from_value, FromValueError};
pub use ser::to_value;

#[derive(Clone, Debug)]
enum Number {
	I8(i8),
	U8(u8),

	I16(i16),
	U16(u16),

	I32(i32),
	U32(u32),
	F32(f32),

	I64(i64),
	U64(u64),
	F64(f64),

	I128(i128),
	U128(u128),
}

#[derive(Clone, Debug)]
enum Struct<'a> {
	Unit {
		name: &'static str,
	},
	NewType {
		name: &'static str,
		value: Box<Content<'a>>,
	},
	Tuple {
		name: &'static str,
		values: Vec<Content<'a>>,
	},
	Object {
		name: &'static str,
		fields: Vec<(&'static str, Content<'a>)>,
	},
}

#[derive(Clone, Debug)]
enum Enum<'a> {
	Unit {
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
	},
	NewType {
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		value: Box<Content<'a>>,
	},
	Tuple {
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		values: Vec<Content<'a>>,
	},
	Struct {
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		fields: Vec<(&'static str, Content<'a>)>,
	},
}

#[derive(Clone, Debug)]
enum Content<'a> {
	Unit,
	Bool(bool),
	Number(Number),
	Char(char),
	String(Cow<'a, str>),
	Bytes(Cow<'a, [u8]>),
	Seq(Vec<Content<'a>>),
	Map(Vec<(Content<'a>, Content<'a>)>),
	Option(Option<Box<Content<'a>>>),
	Struct(Struct<'a>),
	Enum(Enum<'a>),
	Tuple(Vec<Content<'a>>),
}
