use thiserror::Error;

use super::types::TypeName;

#[derive(Debug, Error)]
pub enum Error {
	#[error("Tried to read byte {byte}, but can only read up to {len}")]
	OutOfBounds {
		byte: usize,
		len: usize,
	},

	#[error("Encountered invalid major `{0}`, expected a u8 between 0 and 7")]
	InvalidMajor(u8),

	#[error("Expected a major length of up to 27, instead found `{0}`")]
	InvalidMajorLength(u8),

	#[error(
		"Expected a chunk of major {expected}, but found major {found} instead while decoding an infinite value"
	)]
	InvalidChunkMajor {
		found: u8,
		expected: u8,
	},

	#[error("Unexpected infinite value while already processing an infinite value")]
	UnexpectedInfiniteValue,

	#[error("Found invalid UTF-8 characters while decoding text")]
	InvalidText,

	#[error("Found an invalid UUID value")]
	InvalidUuid,

	#[error("Found an invalid Decimal value")]
	InvalidDecimal,

	#[error("Found an invalid Datetime value")]
	InvalidDatetime,

	#[error("Found an invalid Duration value")]
	InvalidDuration,

	#[error("Found an invalid Future value")]
	InvalidFuture,

	#[error("Found an invalid Record ID value")]
	InvalidRecordId,

	#[error("Found an invalid Range bound")]
	InvalidBound,

	#[error("Cannot decode a bound which is not part of a range")]
	UnexpectedBound,

	#[error("Expected a geometry polygon to contain at least one geometry line")]
	GeometryPolygonEmpty,

	#[error("Expected to find {0}")]
	ExpectedValue(String),

	#[error("Encountered a CBOR break where this was not expected")]
	UnexpectedBreak,

	#[error("Encountered an invalid simple value under tag 7, with length {0}")]
	InvalidSimpleValue(u8),

	#[error("Encountered unsupported tagged value with tag `{0}`")]
	UnsupportedTag(u64),

	#[error("Encountered a value which cannot be encoded")]
	UnsupportedEncodingValue,

	#[error("An error occured: {0}")]
	Thrown(String),
}

impl serde::ser::Error for Error {
	fn custom<T: std::fmt::Display>(msg: T) -> Self {
		Error::Thrown(msg.to_string())
	}
}

pub trait ExpectDecoded<T> {
	fn expect_decoded(self) -> Result<T, Error>
	where
		T: TypeName;
}

impl<T> ExpectDecoded<T> for Option<T> {
	fn expect_decoded(self) -> Result<T, Error>
	where
		T: TypeName,
	{
		self.ok_or_else(|| Error::ExpectedValue(T::type_name()))
	}
}
