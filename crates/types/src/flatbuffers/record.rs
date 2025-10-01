use std::ops::Bound;

use surrealdb_protocol::fb::v1::{self as proto_fb, RecordIdKeyBound};

use super::{FromFlatbuffers, ToFlatbuffers};
use crate::{RecordId, RecordIdKey, RecordIdKeyRange};

impl ToFlatbuffers for RecordId {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::RecordId<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let table = builder.create_string(&self.table);
		let id = self.key.to_fb(builder)?;
		Ok(proto_fb::RecordId::create(
			builder,
			&proto_fb::RecordIdArgs {
				table: Some(table),
				id: Some(id),
			},
		))
	}
}

impl FromFlatbuffers for RecordId {
	type Input<'a> = proto_fb::RecordId<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let table = input.table().ok_or_else(|| anyhow::anyhow!("Missing table in RecordId"))?;
		let key = RecordIdKey::from_fb(
			input.id().ok_or_else(|| anyhow::anyhow!("Missing id in RecordId"))?,
		)?;
		Ok(RecordId {
			table: table.to_string(),
			key,
		})
	}
}

impl ToFlatbuffers for RecordIdKey {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::RecordIdKey<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		match self {
			RecordIdKey::Number(n) => {
				let id = n.to_fb(builder)?.as_union_value();
				Ok(proto_fb::RecordIdKey::create(
					builder,
					&proto_fb::RecordIdKeyArgs {
						id_type: proto_fb::RecordIdKeyType::Int64,
						id: Some(id),
					},
				))
			}
			RecordIdKey::String(s) => {
				let id = s.to_fb(builder)?.as_union_value();
				Ok(proto_fb::RecordIdKey::create(
					builder,
					&proto_fb::RecordIdKeyArgs {
						id_type: proto_fb::RecordIdKeyType::String,
						id: Some(id),
					},
				))
			}
			RecordIdKey::Uuid(uuid) => {
				let id = uuid.to_fb(builder)?.as_union_value();
				Ok(proto_fb::RecordIdKey::create(
					builder,
					&proto_fb::RecordIdKeyArgs {
						id_type: proto_fb::RecordIdKeyType::Uuid,
						id: Some(id),
					},
				))
			}
			RecordIdKey::Array(arr) => {
				let id = arr.to_fb(builder)?.as_union_value();
				Ok(proto_fb::RecordIdKey::create(
					builder,
					&proto_fb::RecordIdKeyArgs {
						id_type: proto_fb::RecordIdKeyType::Array,
						id: Some(id),
					},
				))
			}
			RecordIdKey::Range(range) => {
				let id = range.to_fb(builder)?.as_union_value();
				Ok(proto_fb::RecordIdKey::create(
					builder,
					&proto_fb::RecordIdKeyArgs {
						id_type: proto_fb::RecordIdKeyType::Range,
						id: Some(id),
					},
				))
			}
			_ => Err(anyhow::anyhow!(
				"Unsupported Id type for FlatBuffers serialization: {:?}",
				self
			)),
		}
	}
}

impl FromFlatbuffers for RecordIdKey {
	type Input<'a> = proto_fb::RecordIdKey<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.id_type() {
			proto_fb::RecordIdKeyType::Int64 => {
				let key_value =
					input.id_as_int_64().ok_or_else(|| anyhow::anyhow!("Expected Int64 Id"))?;
				Ok(RecordIdKey::Number(key_value.value()))
			}
			proto_fb::RecordIdKeyType::String => {
				let key_value =
					input.id_as_string().ok_or_else(|| anyhow::anyhow!("Expected String Id"))?;
				Ok(RecordIdKey::String(
					key_value
						.value()
						.ok_or_else(|| anyhow::anyhow!("Missing String value"))?
						.to_string(),
				))
			}
			proto_fb::RecordIdKeyType::Uuid => {
				let key_value =
					input.id_as_uuid().ok_or_else(|| anyhow::anyhow!("Expected Uuid Id"))?;
				let uuid = crate::Uuid::from_fb(key_value)?;
				Ok(RecordIdKey::Uuid(uuid))
			}
			proto_fb::RecordIdKeyType::Array => {
				let key_value =
					input.id_as_array().ok_or_else(|| anyhow::anyhow!("Expected Array Id"))?;
				let array = crate::Array::from_fb(key_value)?;
				Ok(RecordIdKey::Array(array))
			}
			proto_fb::RecordIdKeyType::Range => {
				let key_value =
					input.id_as_range().ok_or_else(|| anyhow::anyhow!("Expected Range Id"))?;
				let range = RecordIdKeyRange::from_fb(key_value)?;
				Ok(RecordIdKey::Range(Box::new(range)))
			}
			_ => Err(anyhow::anyhow!(
				"Unsupported RecordIdKey type for FlatBuffers deserialization: {:?}",
				input.id_type()
			)),
		}
	}
}

impl ToFlatbuffers for RecordIdKeyRange {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::RecordIdKeyRange<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let (start_type, start) = self.start.to_fb(builder)?;
		let (end_type, end) = self.end.to_fb(builder)?;
		Ok(proto_fb::RecordIdKeyRange::create(
			builder,
			&proto_fb::RecordIdKeyRangeArgs {
				start_type,
				start,
				end_type,
				end,
			},
		))
	}
}

impl FromFlatbuffers for RecordIdKeyRange {
	type Input<'bldr> = proto_fb::RecordIdKeyRange<'bldr>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let start = match input.start_type() {
			RecordIdKeyBound::Unbounded => {
				input
					.start_as_unbounded()
					.ok_or_else(|| anyhow::anyhow!("Missing start in IdRange"))?;
				Bound::Unbounded
			}
			RecordIdKeyBound::Inclusive => {
				let start = input
					.start_as_inclusive()
					.ok_or_else(|| anyhow::anyhow!("Missing start in IdRange"))?;
				Bound::Included(RecordIdKey::from_fb(start)?)
			}
			RecordIdKeyBound::Exclusive => {
				let start = input
					.start_as_exclusive()
					.ok_or_else(|| anyhow::anyhow!("Missing start in IdRange"))?;
				Bound::Excluded(RecordIdKey::from_fb(start)?)
			}
			_ => return Err(anyhow::anyhow!("Invalid start type in IdRange")),
		};

		let end = match input.end_type() {
			RecordIdKeyBound::Unbounded => {
				input
					.end_as_unbounded()
					.ok_or_else(|| anyhow::anyhow!("Missing end in IdRange"))?;
				Bound::Unbounded
			}
			RecordIdKeyBound::Inclusive => {
				let end = input
					.end_as_inclusive()
					.ok_or_else(|| anyhow::anyhow!("Missing end in IdRange"))?;
				Bound::Included(RecordIdKey::from_fb(end)?)
			}
			RecordIdKeyBound::Exclusive => {
				let end = input
					.end_as_exclusive()
					.ok_or_else(|| anyhow::anyhow!("Missing end in IdRange"))?;
				Bound::Excluded(RecordIdKey::from_fb(end)?)
			}
			_ => return Err(anyhow::anyhow!("Invalid end type in IdRange")),
		};

		Ok(RecordIdKeyRange {
			start,
			end,
		})
	}
}

impl ToFlatbuffers for Bound<RecordIdKey> {
	type Output<'bldr> =
		(proto_fb::RecordIdKeyBound, Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>);

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		Ok(match self {
			Bound::Included(id) => {
				let id_value = id.to_fb(builder)?.as_union_value();
				(proto_fb::RecordIdKeyBound::Inclusive, Some(id_value))
			}
			Bound::Excluded(id) => {
				let id_value = id.to_fb(builder)?.as_union_value();
				(proto_fb::RecordIdKeyBound::Exclusive, Some(id_value))
			}
			Bound::Unbounded => {
				let null_value = proto_fb::NullValue::create(builder, &proto_fb::NullValueArgs {});
				(proto_fb::RecordIdKeyBound::Unbounded, Some(null_value.as_union_value()))
			}
		})
	}
}
