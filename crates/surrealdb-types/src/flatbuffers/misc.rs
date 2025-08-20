use surrealdb_protocol::fb::v1 as proto_fb;

use super::{FromFlatbuffers, ToFlatbuffers};
use crate::File;

impl ToFlatbuffers for File {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::File<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let bucket = builder.create_string(&self.bucket);
		let key = builder.create_string(&self.key);
		Ok(proto_fb::File::create(
			builder,
			&proto_fb::FileArgs {
				bucket: Some(bucket),
				key: Some(key),
			},
		))
	}
}

impl FromFlatbuffers for File {
	type Input<'a> = proto_fb::File<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let bucket = input.bucket().ok_or_else(|| anyhow::anyhow!("Missing bucket in File"))?;
		let key = input.key().ok_or_else(|| anyhow::anyhow!("Missing key in File"))?;
		Ok(File {
			bucket: bucket.to_string(),
			key: key.to_string(),
		})
	}
}