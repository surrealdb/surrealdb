use std::ops::Bound;

use anyhow::Context;
use surrealdb_protocol::fb::v1 as proto_fb;

use super::{FromFlatbuffers, ToFlatbuffers};
use crate::{Kind, Value};

/// Encode a slice of `(&str, Kind)` pairs into a FlatBuffers `ArgumentList`.
pub fn encode_argument_list(args: &[(&str, Kind)]) -> anyhow::Result<Vec<u8>> {
	let mut fbb = flatbuffers::FlatBufferBuilder::new();
	let offsets: Vec<_> = args
		.iter()
		.map(|(name, kind)| -> anyhow::Result<_> {
			let key_offset = fbb.create_string(name);
			let kind_offset = kind.to_fb(&mut fbb)?;
			Ok(proto_fb::Argument::create(
				&mut fbb,
				&proto_fb::ArgumentArgs {
					key: Some(key_offset),
					value: Some(kind_offset),
				},
			))
		})
		.collect::<anyhow::Result<Vec<_>>>()?;
	let vec = fbb.create_vector(&offsets);
	let root = proto_fb::ArgumentList::create(
		&mut fbb,
		&proto_fb::ArgumentListArgs {
			arguments: Some(vec),
		},
	);
	fbb.finish(root, None);
	Ok(fbb.finished_data().to_vec())
}

/// Decode a FlatBuffers `ArgumentList` into a `Vec<(String, Kind)>`.
pub fn decode_argument_list(bytes: &[u8]) -> anyhow::Result<Vec<(String, Kind)>> {
	let root = flatbuffers::root::<proto_fb::ArgumentList>(bytes)
		.context("Failed to decode ArgumentList")?;
	let arguments = root.arguments().context("Missing arguments in ArgumentList")?;
	arguments
		.iter()
		.map(|arg| {
			let key = arg.key().context("Missing key in Argument")?.to_string();
			let kind_fb = arg.value().context("Missing value in Argument")?;
			let kind = Kind::from_fb(kind_fb)?;
			Ok((key, kind))
		})
		.collect()
}

/// Encode a slice of Values into a FlatBuffers `ValueList`.
pub fn encode_value_list(values: &[Value]) -> anyhow::Result<Vec<u8>> {
	let mut fbb = flatbuffers::FlatBufferBuilder::new();
	let offsets: Vec<_> =
		values.iter().map(|v| v.to_fb(&mut fbb)).collect::<anyhow::Result<Vec<_>>>()?;
	let vec = fbb.create_vector(&offsets);
	let root = proto_fb::ValueList::create(
		&mut fbb,
		&proto_fb::ValueListArgs {
			values: Some(vec),
		},
	);
	fbb.finish(root, None);
	Ok(fbb.finished_data().to_vec())
}

/// Decode a FlatBuffers `ValueList` into a `Vec<Value>`.
pub fn decode_value_list(bytes: &[u8]) -> anyhow::Result<Vec<Value>> {
	let root =
		flatbuffers::root::<proto_fb::ValueList>(bytes).context("Failed to decode ValueList")?;
	let values = root.values().context("Missing values in ValueList")?;
	values.iter().map(Value::from_fb).collect()
}

/// Encode a slice of Kinds into a FlatBuffers `KindList`.
pub fn encode_kind_list(kinds: &[Kind]) -> anyhow::Result<Vec<u8>> {
	let mut fbb = flatbuffers::FlatBufferBuilder::new();
	let offsets: Vec<_> =
		kinds.iter().map(|k| k.to_fb(&mut fbb)).collect::<anyhow::Result<Vec<_>>>()?;
	let vec = fbb.create_vector(&offsets);
	let root = proto_fb::KindList::create(
		&mut fbb,
		&proto_fb::KindListArgs {
			kinds: Some(vec),
		},
	);
	fbb.finish(root, None);
	Ok(fbb.finished_data().to_vec())
}

/// Decode a FlatBuffers `KindList` into a `Vec<Kind>`.
pub fn decode_kind_list(bytes: &[u8]) -> anyhow::Result<Vec<Kind>> {
	let root =
		flatbuffers::root::<proto_fb::KindList>(bytes).context("Failed to decode KindList")?;
	let kinds = root.kinds().context("Missing kinds in KindList")?;
	kinds.iter().map(Kind::from_fb).collect()
}

/// Encode a slice of `(String, Value)` pairs into a FlatBuffers `StringKeyValueList`.
pub fn encode_string_key_values(entries: &[(String, Value)]) -> anyhow::Result<Vec<u8>> {
	let mut fbb = flatbuffers::FlatBufferBuilder::new();
	let offsets: Vec<_> = entries
		.iter()
		.map(|(key, value)| -> anyhow::Result<_> {
			let key_offset = fbb.create_string(key);
			let value_offset = value.to_fb(&mut fbb)?;
			Ok(proto_fb::StringKeyValue::create(
				&mut fbb,
				&proto_fb::StringKeyValueArgs {
					key: Some(key_offset),
					value: Some(value_offset),
				},
			))
		})
		.collect::<anyhow::Result<Vec<_>>>()?;
	let vec = fbb.create_vector(&offsets);
	let root = proto_fb::StringKeyValueList::create(
		&mut fbb,
		&proto_fb::StringKeyValueListArgs {
			entries: Some(vec),
		},
	);
	fbb.finish(root, None);
	Ok(fbb.finished_data().to_vec())
}

/// Decode a FlatBuffers `StringKeyValueList` into a `Vec<(String, Value)>`.
pub fn decode_string_key_values(bytes: &[u8]) -> anyhow::Result<Vec<(String, Value)>> {
	let root = flatbuffers::root::<proto_fb::StringKeyValueList>(bytes)
		.context("Failed to decode StringKeyValueList")?;
	let entries = root.entries().context("Missing entries in StringKeyValueList")?;
	entries
		.iter()
		.map(|entry| {
			let key = entry.key().context("Missing key in StringKeyValue")?.to_string();
			let value_fb = entry.value().context("Missing value in StringKeyValue")?;
			let value = Value::from_fb(value_fb)?;
			Ok((key, value))
		})
		.collect()
}

/// Encode a slice of `Option<Value>` into a FlatBuffers `OptionalValueList`.
pub fn encode_optional_values(values: &[Option<Value>]) -> anyhow::Result<Vec<u8>> {
	let mut fbb = flatbuffers::FlatBufferBuilder::new();
	let offsets: Vec<_> = values
		.iter()
		.map(|opt| -> anyhow::Result<_> {
			match opt {
				Some(value) => {
					let value_offset = value.to_fb(&mut fbb)?;
					Ok(proto_fb::OptionalValue::create(
						&mut fbb,
						&proto_fb::OptionalValueArgs {
							present: true,
							value: Some(value_offset),
						},
					))
				}
				None => Ok(proto_fb::OptionalValue::create(
					&mut fbb,
					&proto_fb::OptionalValueArgs {
						present: false,
						value: None,
					},
				)),
			}
		})
		.collect::<anyhow::Result<Vec<_>>>()?;
	let vec = fbb.create_vector(&offsets);
	let root = proto_fb::OptionalValueList::create(
		&mut fbb,
		&proto_fb::OptionalValueListArgs {
			values: Some(vec),
		},
	);
	fbb.finish(root, None);
	Ok(fbb.finished_data().to_vec())
}

/// Decode a FlatBuffers `OptionalValueList` into a `Vec<Option<Value>>`.
pub fn decode_optional_values(bytes: &[u8]) -> anyhow::Result<Vec<Option<Value>>> {
	let root = flatbuffers::root::<proto_fb::OptionalValueList>(bytes)
		.context("Failed to decode OptionalValueList")?;
	let values = root.values().context("Missing values in OptionalValueList")?;
	values
		.iter()
		.map(|opt| {
			if opt.present() {
				let value_fb =
					opt.value().context("OptionalValue marked present but missing value")?;
				Ok(Some(Value::from_fb(value_fb)?))
			} else {
				Ok(None)
			}
		})
		.collect()
}

fn bound_to_tag(bound: &Bound<String>) -> (proto_fb::StringBoundTag, Option<&str>) {
	match bound {
		Bound::Unbounded => (proto_fb::StringBoundTag::Unbounded, None),
		Bound::Included(s) => (proto_fb::StringBoundTag::Included, Some(s.as_str())),
		Bound::Excluded(s) => (proto_fb::StringBoundTag::Excluded, Some(s.as_str())),
	}
}

fn tag_to_bound(
	tag: proto_fb::StringBoundTag,
	value: Option<&str>,
) -> anyhow::Result<Bound<String>> {
	match tag {
		proto_fb::StringBoundTag::Unbounded => Ok(Bound::Unbounded),
		proto_fb::StringBoundTag::Included => {
			let s = value.context("Included bound missing value")?;
			Ok(Bound::Included(s.to_string()))
		}
		proto_fb::StringBoundTag::Excluded => {
			let s = value.context("Excluded bound missing value")?;
			Ok(Bound::Excluded(s.to_string()))
		}
		_ => anyhow::bail!("Unknown StringBoundTag: {:?}", tag),
	}
}

/// Encode a pair of string bounds into a FlatBuffers `StringRange`.
pub fn encode_string_range(start: &Bound<String>, end: &Bound<String>) -> anyhow::Result<Vec<u8>> {
	let mut fbb = flatbuffers::FlatBufferBuilder::new();
	let (start_tag, start_val) = bound_to_tag(start);
	let (end_tag, end_val) = bound_to_tag(end);
	let start_offset = start_val.map(|s| fbb.create_string(s));
	let end_offset = end_val.map(|s| fbb.create_string(s));
	let root = proto_fb::StringRange::create(
		&mut fbb,
		&proto_fb::StringRangeArgs {
			start_tag,
			start_value: start_offset,
			end_tag,
			end_value: end_offset,
		},
	);
	fbb.finish(root, None);
	Ok(fbb.finished_data().to_vec())
}

/// Decode a FlatBuffers `StringRange` into a pair of bounds.
pub fn decode_string_range(bytes: &[u8]) -> anyhow::Result<(Bound<String>, Bound<String>)> {
	let root = flatbuffers::root::<proto_fb::StringRange>(bytes)
		.context("Failed to decode StringRange")?;
	let start = tag_to_bound(root.start_tag(), root.start_value())?;
	let end = tag_to_bound(root.end_tag(), root.end_value())?;
	Ok((start, end))
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use rstest::rstest;

	use super::*;
	use crate::{Kind, Number, Value};

	#[test]
	fn test_value_list_roundtrip() {
		let values =
			vec![Value::String("hello".into()), Value::Number(Number::Int(42)), Value::Bool(true)];
		let bytes = encode_value_list(&values).unwrap();
		let decoded = decode_value_list(&bytes).unwrap();
		assert_eq!(values, decoded);
	}

	#[test]
	fn test_value_list_empty() {
		let values: Vec<Value> = vec![];
		let bytes = encode_value_list(&values).unwrap();
		let decoded = decode_value_list(&bytes).unwrap();
		assert_eq!(values, decoded);
	}

	#[test]
	fn test_kind_list_roundtrip() {
		let kinds = vec![Kind::String, Kind::Int, Kind::Bool];
		let bytes = encode_kind_list(&kinds).unwrap();
		let decoded = decode_kind_list(&bytes).unwrap();
		assert_eq!(kinds, decoded);
	}

	#[test]
	fn test_kind_list_empty() {
		let kinds: Vec<Kind> = vec![];
		let bytes = encode_kind_list(&kinds).unwrap();
		let decoded = decode_kind_list(&bytes).unwrap();
		assert_eq!(kinds, decoded);
	}

	#[test]
	fn test_argument_list_roundtrip() {
		let args: Vec<(&str, Kind)> =
			vec![("name", Kind::String), ("age", Kind::Int), ("active", Kind::Bool)];
		let bytes = encode_argument_list(&args).unwrap();
		let decoded = decode_argument_list(&bytes).unwrap();
		let expected: Vec<(String, Kind)> =
			args.into_iter().map(|(n, k)| (n.to_string(), k)).collect();
		assert_eq!(expected, decoded);
	}

	#[test]
	fn test_argument_list_empty() {
		let args: Vec<(&str, Kind)> = vec![];
		let bytes = encode_argument_list(&args).unwrap();
		let decoded = decode_argument_list(&bytes).unwrap();
		assert!(decoded.is_empty());
	}

	#[test]
	fn test_string_key_values_roundtrip() {
		let entries = vec![
			("key1".to_string(), Value::String("val1".into())),
			("key2".to_string(), Value::Number(Number::Int(99))),
		];
		let bytes = encode_string_key_values(&entries).unwrap();
		let decoded = decode_string_key_values(&bytes).unwrap();
		assert_eq!(entries, decoded);
	}

	#[test]
	fn test_string_key_values_empty() {
		let entries: Vec<(String, Value)> = vec![];
		let bytes = encode_string_key_values(&entries).unwrap();
		let decoded = decode_string_key_values(&bytes).unwrap();
		assert_eq!(entries, decoded);
	}

	#[test]
	fn test_optional_values_roundtrip() {
		let values = vec![
			Some(Value::String("present".into())),
			None,
			Some(Value::Number(Number::Int(7))),
			None,
		];
		let bytes = encode_optional_values(&values).unwrap();
		let decoded = decode_optional_values(&bytes).unwrap();
		assert_eq!(values, decoded);
	}

	#[test]
	fn test_optional_values_all_none() {
		let values: Vec<Option<Value>> = vec![None, None, None];
		let bytes = encode_optional_values(&values).unwrap();
		let decoded = decode_optional_values(&bytes).unwrap();
		assert_eq!(values, decoded);
	}

	#[rstest]
	#[case::unbounded_unbounded(Bound::Unbounded, Bound::Unbounded)]
	#[case::included_excluded(
		Bound::Included("a".to_string()),
		Bound::Excluded("z".to_string())
	)]
	#[case::included_unbounded(
		Bound::Included("start".to_string()),
		Bound::Unbounded
	)]
	#[case::unbounded_excluded(
		Bound::Unbounded,
		Bound::Excluded("end".to_string())
	)]
	#[case::included_included(
		Bound::Included("a".to_string()),
		Bound::Included("z".to_string())
	)]
	fn test_string_range_roundtrip(#[case] start: Bound<String>, #[case] end: Bound<String>) {
		let bytes = encode_string_range(&start, &end).unwrap();
		let (decoded_start, decoded_end) = decode_string_range(&bytes).unwrap();
		assert_eq!(start, decoded_start);
		assert_eq!(end, decoded_end);
	}
}
