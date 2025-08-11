mod flatbuffers;
// mod protobuffers;

pub use surrealdb_protocol::{TryFromValue, TryIntoValue};

/// Trait for converting a type to a flatbuffers builder type.
pub trait ToFlatbuffers {
	type Output<'bldr>;

	/// Convert the type to a flatbuffers builder type.
	fn to_fb<'bldr>(
		&self,
		builder: &mut ::flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>>;
}

/// Trait for converting a flatbuffers builder type to a type.
pub trait FromFlatbuffers {
	type Input<'a>;

	/// Convert a flatbuffers builder type to a type.
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
	where
		Self: Sized;
}
