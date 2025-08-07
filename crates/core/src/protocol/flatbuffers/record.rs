use crate::expr::{Id, IdRange, Table, Thing};
use crate::protocol::{FromFlatbuffers, ToFlatbuffers};
use std::ops::Bound;

use surrealdb_protocol::fb::v1::{self as proto_fb, RecordIdKeyBound};

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

impl ToFlatbuffers for Thing {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::RecordId<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let table = builder.create_string(&self.tb);
		let id = self.id.to_fb(builder)?;
		Ok(proto_fb::RecordId::create(
			builder,
			&proto_fb::RecordIdArgs {
				table: Some(table),
				id: Some(id),
			},
		))
	}
}

impl FromFlatbuffers for Thing {
	type Input<'a> = proto_fb::RecordId<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let table = input.table().ok_or_else(|| anyhow::anyhow!("Missing table in RecordId"))?;
		let id = Id::from_fb(input.id().ok_or_else(|| anyhow::anyhow!("Missing id in RecordId"))?)?;
		Ok(Thing {
			tb: table.to_string(),
			id,
		})
	}
}

impl ToFlatbuffers for Id {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::RecordIdKey<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		match self {
			Id::Number(n) => {
				let id = n.to_fb(builder)?.as_union_value();
				Ok(proto_fb::RecordIdKey::create(
					builder,
					&proto_fb::RecordIdKeyArgs {
						id_type: proto_fb::RecordIdKeyType::Int64,
						id: Some(id),
					},
				))
			}
			Id::String(s) => {
				let id = s.to_fb(builder)?.as_union_value();
				Ok(proto_fb::RecordIdKey::create(
					builder,
					&proto_fb::RecordIdKeyArgs {
						id_type: proto_fb::RecordIdKeyType::String,
						id: Some(id),
					},
				))
			}
			Id::Uuid(uuid) => {
				let id = uuid.to_fb(builder)?.as_union_value();
				Ok(proto_fb::RecordIdKey::create(
					builder,
					&proto_fb::RecordIdKeyArgs {
						id_type: proto_fb::RecordIdKeyType::Uuid,
						id: Some(id),
					},
				))
			}
			Id::Array(arr) => {
				let id = arr.to_fb(builder)?.as_union_value();
				Ok(proto_fb::RecordIdKey::create(
					builder,
					&proto_fb::RecordIdKeyArgs {
						id_type: proto_fb::RecordIdKeyType::Array,
						id: Some(id),
					},
				))
			}
			Id::Range(range) => {
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

impl FromFlatbuffers for Id {
	type Input<'a> = proto_fb::RecordIdKey<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.id_type() {
			proto_fb::RecordIdKeyType::Int64 => {
				let id_value =
					input.id_as_int_64().ok_or_else(|| anyhow::anyhow!("Expected Int64 Id"))?;
				Ok(Id::Number(id_value.value()))
			}
			proto_fb::RecordIdKeyType::String => {
				let id_value =
					input.id_as_string().ok_or_else(|| anyhow::anyhow!("Expected String Id"))?;
				Ok(Id::String(
					id_value
						.value()
						.ok_or_else(|| anyhow::anyhow!("Missing String value"))?
						.to_string(),
				))
			}
			proto_fb::RecordIdKeyType::Uuid => {
				let id_value =
					input.id_as_uuid().ok_or_else(|| anyhow::anyhow!("Expected Uuid Id"))?;
				let uuid = crate::expr::Uuid::from_fb(id_value)?;
				Ok(Id::Uuid(uuid))
			}
			proto_fb::RecordIdKeyType::Array => {
				let id_value =
					input.id_as_array().ok_or_else(|| anyhow::anyhow!("Expected Array Id"))?;
				let array = crate::expr::Array::from_fb(id_value)?;
				Ok(Id::Array(array))
			}
			proto_fb::RecordIdKeyType::Range => {
				let id_value =
					input.id_as_range().ok_or_else(|| anyhow::anyhow!("Expected Range Id"))?;
				let range = IdRange::from_fb(id_value)?;
				Ok(Id::Range(Box::new(range)))
			}
			_ => Err(anyhow::anyhow!(
				"Unsupported Id type for FlatBuffers deserialization: {:?}",
				input.id_type()
			)),
		}
	}
}

impl ToFlatbuffers for IdRange {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::RecordIdKeyRange<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let (start_type, start) = self.beg.to_fb(builder)?;
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

impl FromFlatbuffers for IdRange {
	type Input<'bldr> = proto_fb::RecordIdKeyRange<'bldr>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let beg = match input.start_type() {
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
				Bound::Included(Id::from_fb(start)?)
			}
			RecordIdKeyBound::Exclusive => {
				let start = input
					.start_as_exclusive()
					.ok_or_else(|| anyhow::anyhow!("Missing start in IdRange"))?;
				Bound::Excluded(Id::from_fb(start)?)
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
				Bound::Included(Id::from_fb(end)?)
			}
			RecordIdKeyBound::Exclusive => {
				let end = input
					.end_as_exclusive()
					.ok_or_else(|| anyhow::anyhow!("Missing end in IdRange"))?;
				Bound::Excluded(Id::from_fb(end)?)
			}
			_ => return Err(anyhow::anyhow!("Invalid end type in IdRange")),
		};

		Ok(IdRange {
			beg,
			end,
		})
	}
}

impl ToFlatbuffers for Bound<Id> {
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
			Bound::Unbounded => (proto_fb::RecordIdKeyBound::Unbounded, None),
		})
	}
}
