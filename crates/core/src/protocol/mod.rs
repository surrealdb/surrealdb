
mod expr;
mod rpc;
mod value;

pub trait ToFlatbuffers {
	type Output<'bldr>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut ::flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr>;
}

pub trait FromFlatbuffers {
	type Input<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self>
	where
		Self: Sized;
}
