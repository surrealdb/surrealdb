mod collections;
mod geometry;
mod kind;
mod misc;
mod primitives;
mod record;
mod value;

/// Trait for converting a type to a flatbuffers builder type.
pub trait ToFlatbuffers {
	/// The output type for the flatbuffers builder
	type Output<'bldr>;

	/// Convert the type to a flatbuffers builder type.
	fn to_fb<'bldr>(
		&self,
		builder: &mut ::flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>>;
}

/// Trait for converting a flatbuffers builder type to a type.
pub trait FromFlatbuffers {
	/// The input type from the flatbuffers builder
	type Input<'a>;

	/// Convert a flatbuffers builder type to a type.
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
	where
		Self: Sized;
}
