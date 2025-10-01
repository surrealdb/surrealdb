use std::ops::Bound;
use std::str::FromStr;

use surrealdb_protocol::fb::v1::{self as proto_fb, ValueBound};

use super::{FromFlatbuffers, ToFlatbuffers};
use crate::{File, Range, Regex, Value};

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

impl ToFlatbuffers for Regex {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::StringValue<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let value = builder.create_string(self.0.as_str());
		Ok(proto_fb::StringValue::create(
			builder,
			&proto_fb::StringValueArgs {
				value: Some(value),
			},
		))
	}
}

impl FromFlatbuffers for Regex {
	type Input<'a> = proto_fb::StringValue<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let pattern = input.value().ok_or_else(|| anyhow::anyhow!("Missing regex value"))?;
		Ok(Regex::from_str(pattern)?)
	}
}

impl ToFlatbuffers for Range {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Range<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let (start_type, start) = self.start.to_fb(builder)?;
		let (end_type, end) = self.end.to_fb(builder)?;
		Ok(proto_fb::Range::create(
			builder,
			&proto_fb::RangeArgs {
				start_type,
				start,
				end_type,
				end,
			},
		))
	}
}

impl FromFlatbuffers for Range {
	type Input<'a> = proto_fb::Range<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let start = match input.start_type() {
			ValueBound::Unbounded => {
				input
					.start_as_unbounded()
					.ok_or_else(|| anyhow::anyhow!("Missing start in Range"))?;
				Bound::Unbounded
			}
			ValueBound::Inclusive => {
				let start = input
					.start_as_inclusive()
					.ok_or_else(|| anyhow::anyhow!("Missing start in Range"))?;
				Bound::Included(Value::from_fb(start)?)
			}
			ValueBound::Exclusive => {
				let start = input
					.start_as_exclusive()
					.ok_or_else(|| anyhow::anyhow!("Missing start in Range"))?;
				Bound::Excluded(Value::from_fb(start)?)
			}
			_ => return Err(anyhow::anyhow!("Invalid start type in Range")),
		};

		let end = match input.end_type() {
			ValueBound::Unbounded => {
				input.end_as_unbounded().ok_or_else(|| anyhow::anyhow!("Missing end in Range"))?;
				Bound::Unbounded
			}
			ValueBound::Inclusive => {
				let end = input
					.end_as_inclusive()
					.ok_or_else(|| anyhow::anyhow!("Missing end in Range"))?;
				Bound::Included(Value::from_fb(end)?)
			}
			ValueBound::Exclusive => {
				let end = input
					.end_as_exclusive()
					.ok_or_else(|| anyhow::anyhow!("Missing end in Range"))?;
				Bound::Excluded(Value::from_fb(end)?)
			}
			_ => return Err(anyhow::anyhow!("Invalid end type in Range")),
		};

		Ok(Range {
			start,
			end,
		})
	}
}

impl ToFlatbuffers for Bound<Value> {
	type Output<'bldr> =
		(proto_fb::ValueBound, Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>);

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		Ok(match self {
			Bound::Included(value) => {
				let value = value.to_fb(builder)?.as_union_value();
				(proto_fb::ValueBound::Inclusive, Some(value))
			}
			Bound::Excluded(value) => {
				let value = value.to_fb(builder)?.as_union_value();
				(proto_fb::ValueBound::Exclusive, Some(value))
			}
			Bound::Unbounded => {
				let null_value = proto_fb::NullValue::create(builder, &proto_fb::NullValueArgs {});
				(proto_fb::ValueBound::Unbounded, Some(null_value.as_union_value()))
			}
		})
	}
}
