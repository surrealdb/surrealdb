use std::collections::BTreeMap;

use anyhow::Context;
use surrealdb_protocol::fb::v1 as proto_fb;

use crate::expr::Kind;
use crate::protocol::{FromFlatbuffers, ToFlatbuffers};
use crate::val::{Array, Object, Value};

impl ToFlatbuffers for BTreeMap<String, Kind> {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::LiteralObject<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let entries: Vec<_> = self
			.iter()
			.map(|(key, kind)| -> anyhow::Result<_> {
				let key_offset = builder.create_string(key);
				let kind_offset = kind.to_fb(builder)?;
				Ok(proto_fb::ObjectField::create(
					builder,
					&proto_fb::ObjectFieldArgs {
						key: Some(key_offset),
						kind: Some(kind_offset),
					},
				))
			})
			.collect::<anyhow::Result<Vec<_>>>()?;

		let entries_vector = builder.create_vector(&entries);
		Ok(proto_fb::LiteralObject::create(
			builder,
			&proto_fb::LiteralObjectArgs {
				fields: Some(entries_vector),
			},
		))
	}
}

impl FromFlatbuffers for BTreeMap<String, Kind> {
	type Input<'a> = proto_fb::LiteralObject<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut map = BTreeMap::new();
		if let Some(entries) = input.fields() {
			for entry in entries {
				let Some(key) = entry.key() else {
					return Err(anyhow::anyhow!("Missing object field key"));
				};
				let Some(kind) = entry.kind() else {
					return Err(anyhow::anyhow!("Missing object field kind"));
				};
				map.insert(key.to_string(), Kind::from_fb(kind)?);
			}
		}
		Ok(map)
	}
}

impl ToFlatbuffers for Object {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Object<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let mut entries = Vec::with_capacity(self.0.len());
		for (key, value) in &self.0 {
			let key_fb = builder.create_string(key);
			let value_fb = value.to_fb(builder)?;

			let object_item = proto_fb::KeyValue::create(
				builder,
				&proto_fb::KeyValueArgs {
					key: Some(key_fb),
					value: Some(value_fb),
				},
			);

			entries.push(object_item);
		}
		let entries_vector = builder.create_vector(&entries);
		Ok(proto_fb::Object::create(
			builder,
			&proto_fb::ObjectArgs {
				items: Some(entries_vector),
			},
		))
	}
}

impl FromFlatbuffers for Object {
	type Input<'a> = proto_fb::Object<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut map = BTreeMap::new();
		let items = input.items().ok_or_else(|| anyhow::anyhow!("Missing items in Object"))?;
		if items.is_empty() {
			return Ok(Object(map));
		}
		for entry in items {
			let key = entry.key().context("Missing key in Object entry")?.to_string();
			let value = entry.value().context("Missing value in Object entry")?;
			map.insert(key, Value::from_fb(value)?);
		}
		Ok(Object(map))
	}
}

impl ToFlatbuffers for Array {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Array<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let mut values = Vec::with_capacity(self.0.len());
		for value in &self.0 {
			values.push(value.to_fb(builder)?);
		}
		let values_vector = builder.create_vector(&values);
		Ok(proto_fb::Array::create(
			builder,
			&proto_fb::ArrayArgs {
				values: Some(values_vector),
			},
		))
	}
}

impl FromFlatbuffers for Array {
	type Input<'a> = proto_fb::Array<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut vec = Vec::new();
		let values = input.values().context("Values is not set")?;
		for value in values {
			vec.push(Value::from_fb(value)?);
		}
		Ok(Array(vec))
	}
}
