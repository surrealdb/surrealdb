use surrealdb_protocol::fb::v1 as proto_fb;

use super::{FromFlatbuffers, ToFlatbuffers};
use crate::Table;

impl ToFlatbuffers for Table {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::StringValue<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let table = builder.create_string(self);
		Ok(proto_fb::StringValue::create(
			builder,
			&proto_fb::StringValueArgs {
				value: Some(table),
			},
		))
	}
}

impl FromFlatbuffers for Table {
	type Input<'a> = proto_fb::StringValue<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		Ok(Table::new(
			input.value().ok_or_else(|| anyhow::anyhow!("Missing table value"))?.to_string(),
		))
	}
}
