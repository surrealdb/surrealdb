use crate::protocol::{FromFlatbuffers, ToFlatbuffers};
use crate::val::File;
use crate::val::Table;

use surrealdb_protocol::fb::v1 as proto_fb;

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

impl ToFlatbuffers for Table {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::TableName<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let name = builder.create_string(self.as_str());
		Ok(proto_fb::TableName::create(
			builder,
			&proto_fb::TableNameArgs {
				name: Some(name),
			},
		))
	}
}

impl FromFlatbuffers for Table {
	type Input<'a> = proto_fb::TableName<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let name = input.name().ok_or_else(|| anyhow::anyhow!("Missing name in Table"))?;
		Ok(Table::new(name.to_string()).ok_or_else(|| anyhow::anyhow!("Invalid table name"))?)
	}
}

// impl ToFlatbuffers for Fetch {
// 	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Value<'bldr>>;

// 	#[inline]
// 	fn to_fb<'bldr>(
// 		&self,
// 		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
// 	) -> anyhow::Result<Self::Output<'bldr>> {
// 		self.0.to_fb(builder)
// 	}
// }

// impl FromFlatbuffers for Fetch {
// 	type Input<'a> = proto_fb::Value<'a>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
// 		let value = Value::from_fb(input)?;
// 		Ok(Fetch(value))
// 	}
// }

// impl ToFlatbuffers for Fetchs {
// 	type Output<'bldr> = flatbuffers::WIPOffset<
// 		::flatbuffers::Vector<'bldr, ::flatbuffers::ForwardsUOffset<proto_fb::Value<'bldr>>>,
// 	>;

// 	#[inline]
// 	fn to_fb<'bldr>(
// 		&self,
// 		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
// 	) -> anyhow::Result<Self::Output<'bldr>> {
// 		let mut values = Vec::with_capacity(self.0.len());
// 		for value in &self.0 {
// 			values.push(value.to_fb(builder)?);
// 		}
// 		Ok(builder.create_vector(&values))
// 	}
// }

// impl FromFlatbuffers for Fetchs {
// 	type Input<'a> = flatbuffers::Vector<'a, ::flatbuffers::ForwardsUOffset<proto_fb::Value<'a>>>;

// 	#[inline]
// 	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
// 		let mut fetchs = Vec::new();
// 		for value in input {
// 			fetchs.push(Fetch(Value::from_fb(value)?));
// 		}
// 		Ok(Fetchs(fetchs))
// 	}
// }
