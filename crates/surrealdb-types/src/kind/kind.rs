use crate::{KindGeometry, KindLiteral, Strand};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Kind {
	/// The most generic type, can be anything.
	Any,
    /// None type.
    None,
	/// Null type.
	Null,
	/// Boolean type.
	Bool,
	/// Bytes type.
	Bytes,
	/// Datetime type.
	Datetime,
	/// Decimal type.
	Decimal,
	/// Duration type.
	Duration,
	/// 64-bit floating point type.
	Float,
	/// 64-bit signed integer type.
	Int,
	/// Number type, can be either a float, int or decimal.
	/// This is the most generic type for numbers.
	Number,
	/// Object type.
	Object,
	/// Geometric 2D point type with longitude *then* latitude coordinates.
	/// This follows the GeoJSON spec.
	Point,
	/// String type.
	String,
	/// UUID type.
	Uuid,
	/// Regular expression type.
	Regex,
	/// A record type.
	Record(Vec<Strand>),
	/// A geometry type.
	/// The vec contains the geometry types as strings, for example `"point"` or
	/// `"polygon"`. TODO(3.0): Change to use an enum
	Geometry(Vec<KindGeometry>),
	/// An optional type.
	Option(Box<Kind>),
	/// An either type.
	/// Can be any of the kinds in the vec.
	Either(Vec<Kind>),
	/// A set type.
	Set(Box<Kind>, Option<u64>),
	/// An array type.
	Array(Box<Kind>, Option<u64>),
	/// A function type.
	/// The first option is the argument types, the second is the optional
	/// return type.
	Function(Option<Vec<Kind>>, Option<Box<Kind>>),
	/// A range type.
	Range,
	/// A literal type.
	/// The literal type is used to represent a type that can only be a single
	/// value. For example, `"a"` is a literal type which can only ever be
	/// `"a"`. This can be used in the `Kind::Either` type to represent an
	/// enum.
	Literal(KindLiteral),
	/// A file type.
	/// If the kind was specified without a bucket the vec will be empty.
	/// So `<file>` is just `Kind::File(Vec::new())`
	File(Vec<Strand>),
}

impl Default for Kind {
	fn default() -> Self {
		Self::Any
	}
}