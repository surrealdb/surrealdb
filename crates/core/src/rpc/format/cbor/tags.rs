use super::{
	decode::Decoder,
	err::{Error, ExpectDecoded},
	major::Major,
};
use crate::sql::{Datetime, Duration, Future, Geometry, Range, SqlValue, Table, Thing, Uuid};
use rust_decimal::Decimal;

macro_rules! create_tags {
    // Main entrypoint
    ($(
        $name:ident($tag:expr) => $handler:tt
    ),* $(,)?) => {
        #[derive(PartialEq, Eq, Clone, Copy, Debug)]
        pub enum Tag {
            $(#[allow(non_camel_case_types)] $name,)*
        }

        impl Tag {
            pub fn decode(&self, dec: &mut Decoder) -> Result<SqlValue, Error> {
                match self {
                    $(
                        Tag::$name => create_tags!(@decode_body self dec $handler),
                    )*
                }
            }
        }

        impl TryFrom<u64> for Tag {
            type Error = Error;
            fn try_from(tag: u64) -> Result<Tag, Error> {
                match tag {
                    $($tag => Ok(Tag::$name),)*
                    _ => Err(Error::UnsupportedTag(tag))
                }
            }
        }

        impl From<Tag> for u64 {
            fn from(tag: Tag) -> u64 {
                match tag {
                    $(Tag::$name => $tag,)*
                }
            }
        }
    };

    // Special handling
    (@decode_body $self:ident $dec:ident None) => {{
        let _ = $dec.decode::<SqlValue>()?;
        Ok(SqlValue::None)
    }};

    (@decode_body $self:ident $dec:ident UnexpectedBound) => {{
        Err(Error::UnexpectedBound)
    }};

    // If it's a type
    (@decode_body $self:ident $dec:ident $typ:ty) => {
        Ok($dec.try_decode::<$typ>(&Major::Tagged(*$self))?.expect_decoded()?.into())
    };
}

create_tags!(
	// Tags from the spec - https://www.iana.org/assignments/cbor-tags/cbor-tags.xhtml
	SPEC_DATETIME(0) => Datetime,
	SPEC_UUID(37) => Uuid,

	// Custom tags (6->15 is unassigned)
	NONE(6) => None,
	TABLE(7) => Table,
	RECORDID(8) => Thing,
	STRING_UUID(9) => Uuid,
	STRING_DECIMAL(10) => Decimal,
	// pub const TAG_BINARY_DECIMAL: u64 = 11;
	CUSTOM_DATETIME(12) => Datetime,
	STRING_DURATION(13) => Duration,
	CUSTOM_DURATION(14) => Duration,
	FUTURE(15) => Future,

	// Ranges (49->51 is unassigned)
	RANGE(49) => Range,
	BOUND_INCLUDED(50) => UnexpectedBound,
	BOUND_EXCLUDED(51) => UnexpectedBound,

	// Custom tags (55->60 is unassigned)
	// TODO(kearfy): implement for 3.x
	// FILE(55) => File,

	// Custom Geometries (88->95 is unassigned)
	GEOMETRY_POINT(88) => Geometry,
	GEOMETRY_LINE(89) => Geometry,
	GEOMETRY_POLYGON(90) => Geometry,
	GEOMETRY_MULTIPOINT(91) => Geometry,
	GEOMETRY_MULTILINE(92) => Geometry,
	GEOMETRY_MULTIPOLYGON(93) => Geometry,
	GEOMETRY_COLLECTION(94) => Geometry,
);
