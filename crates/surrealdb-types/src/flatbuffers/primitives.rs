use anyhow::Context;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use surrealdb_protocol::fb::v1 as proto_fb;

use super::{FromFlatbuffers, ToFlatbuffers};
use crate::{Bytes, Duration, Uuid};

impl ToFlatbuffers for bool {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::BoolValue<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		Ok(proto_fb::BoolValue::create(
			builder,
			&proto_fb::BoolValueArgs {
				value: *self,
			},
		))
	}
}

impl FromFlatbuffers for bool {
	type Input<'a> = proto_fb::BoolValue<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		Ok(input.value())
	}
}

impl ToFlatbuffers for i64 {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Int64Value<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		Ok(proto_fb::Int64Value::create(
			builder,
			&proto_fb::Int64ValueArgs {
				value: *self,
			},
		))
	}
}

impl FromFlatbuffers for i64 {
	type Input<'a> = proto_fb::Int64Value<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		Ok(input.value())
	}
}

impl ToFlatbuffers for u64 {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::UInt64Value<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		Ok(proto_fb::UInt64Value::create(
			builder,
			&proto_fb::UInt64ValueArgs {
				value: *self,
			},
		))
	}
}

impl FromFlatbuffers for u64 {
	type Input<'a> = proto_fb::UInt64Value<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		Ok(input.value())
	}
}

impl ToFlatbuffers for f64 {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Float64Value<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		Ok(proto_fb::Float64Value::create(
			builder,
			&proto_fb::Float64ValueArgs {
				value: *self,
			},
		))
	}
}

impl FromFlatbuffers for f64 {
	type Input<'a> = proto_fb::Float64Value<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		Ok(input.value())
	}
}

impl ToFlatbuffers for String {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::StringValue<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let value = builder.create_string(self);
		Ok(proto_fb::StringValue::create(
			builder,
			&proto_fb::StringValueArgs {
				value: Some(value),
			},
		))
	}
}

impl FromFlatbuffers for String {
	type Input<'a> = proto_fb::StringValue<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let Some(value) = input.value() else {
			return Err(anyhow::anyhow!("Missing string value"));
		};
		Ok(value.to_string())
	}
}

impl ToFlatbuffers for Decimal {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Decimal<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let value = builder.create_string(&self.to_string());
		Ok(proto_fb::Decimal::create(
			builder,
			&proto_fb::DecimalArgs {
				value: Some(value),
			},
		))
	}
}

impl FromFlatbuffers for Decimal {
	type Input<'a> = proto_fb::Decimal<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let Some(value_str) = input.value() else {
			return Err(anyhow::anyhow!("Missing decimal string"));
		};
		value_str.parse::<Decimal>().context("Failed to parse decimal")
	}
}

impl ToFlatbuffers for std::time::Duration {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Duration<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		Ok(proto_fb::Duration::create(
			builder,
			&proto_fb::DurationArgs {
				seconds: self.as_secs(),
				nanos: self.subsec_nanos(),
			},
		))
	}
}

impl FromFlatbuffers for std::time::Duration {
	type Input<'a> = proto_fb::Duration<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let seconds = input.seconds();
		let nanos = input.nanos();
		Ok(std::time::Duration::new(seconds, nanos))
	}
}

impl ToFlatbuffers for Duration {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Duration<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		self.0.to_fb(builder)
	}
}

impl FromFlatbuffers for Duration {
	type Input<'a> = proto_fb::Duration<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let duration = std::time::Duration::from_fb(input)?;
		Ok(Duration(duration))
	}
}

impl ToFlatbuffers for DateTime<Utc> {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Timestamp<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		Ok(proto_fb::Timestamp::create(
			builder,
			&proto_fb::TimestampArgs {
				seconds: self.timestamp(),
				nanos: self.timestamp_subsec_nanos(),
			},
		))
	}
}

impl FromFlatbuffers for DateTime<Utc> {
	type Input<'a> = proto_fb::Timestamp<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let seconds = input.seconds();
		let nanos = input.nanos();
		DateTime::<Utc>::from_timestamp(seconds, nanos)
			.ok_or_else(|| anyhow::anyhow!("Invalid timestamp format"))
	}
}

impl ToFlatbuffers for Uuid {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Uuid<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let bytes = builder.create_vector(self.0.as_bytes());
		Ok(proto_fb::Uuid::create(
			builder,
			&proto_fb::UuidArgs {
				bytes: Some(bytes),
			},
		))
	}
}

impl FromFlatbuffers for Uuid {
	type Input<'a> = proto_fb::Uuid<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let bytes_vector = input.bytes().ok_or_else(|| anyhow::anyhow!("Missing bytes in Uuid"))?;
		uuid::Uuid::from_slice(bytes_vector.bytes())
			.map(Uuid)
			.map_err(|_| anyhow::anyhow!("Invalid UUID format"))
	}
}

impl ToFlatbuffers for Bytes {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Bytes<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let data = builder.create_vector(&self.0);
		Ok(proto_fb::Bytes::create(
			builder,
			&proto_fb::BytesArgs {
				value: Some(data),
			},
		))
	}
}

impl FromFlatbuffers for Bytes {
	type Input<'a> = proto_fb::Bytes<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let data = input.value().ok_or_else(|| anyhow::anyhow!("Missing value in Bytes"))?;
		Ok(Bytes(data.bytes().to_vec()))
	}
}
