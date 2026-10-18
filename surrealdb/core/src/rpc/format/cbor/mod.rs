mod convert;

/// A decoded CBOR item as a public value.
pub(crate) fn to_value(item: ciborium::Value) -> anyhow::Result<Value> {
	convert::to_value(item).map_err(|e| anyhow::anyhow!(e))
}

use surrealdb_types::Value;

pub fn encode(v: Value) -> anyhow::Result<Vec<u8>> {
	// Convert public value to internal value for encoding
	let encoding = convert::from_value(v).map_err(|e| anyhow::anyhow!(e))?;
	let mut res = Vec::new();
	// `Vec<u8>` never returns an I/O error from `Write`, so the `Io` variant
	// of `ciborium::ser::Error` is unreachable here. The `Value` variant is
	// only constructed by ciborium's tag serializer, which our `CborValue`
	// representation never trips. Propagate as an error rather than panic
	// so any future ciborium change cannot crash the RPC layer.
	ciborium::into_writer(&encoding, &mut res)
		.map_err(|e| anyhow::anyhow!("failed to encode CBOR: {e}"))?;
	Ok(res)
}

pub fn decode(bytes: &[u8], recursion_limit: usize) -> anyhow::Result<Value> {
	let encoding = ciborium::de::from_reader_with_recursion_limit(bytes, recursion_limit)
		.map_err(|e| anyhow::anyhow!(e.to_string()))?;
	convert::to_value(encoding).map_err(|e| anyhow::anyhow!(e))
}

/// The `id` of a request envelope whose decoding failed.
///
/// A client matches a response to its request by this `id`, so a failure that
/// answers without it is never delivered to the call that caused it. The
/// envelope's other entries are skipped structurally, without being decoded,
/// so neither a parameter that cannot be converted to a [`Value`] nor one
/// nested deeper than `recursion_limit` hides the `id`; only the `id` value
/// itself is decoded, under that limit. `None` when the bytes are not a CBOR
/// map, are malformed before the `id`, or carry no usable `id`.
pub fn request_id(bytes: &[u8], recursion_limit: usize) -> Option<Value> {
	let mut scan = Scan {
		bytes,
		pos: 0,
	};
	let (major, _, entries) = scan.head()?;
	if major != 5 {
		return None;
	}
	let mut remaining = entries;
	loop {
		match remaining {
			Some(0) => return None,
			Some(n) => remaining = Some(n - 1),
			None if scan.peek()? == 0xff => return None,
			None => {}
		}
		let is_id = scan.key_is("id")?;
		scan.skip_item()?;
		let start = scan.pos;
		scan.skip_item()?;
		if is_id {
			let id: ciborium::Value = ciborium::de::from_reader_with_recursion_limit(
				&bytes[start..scan.pos],
				recursion_limit,
			)
			.ok()?;
			return match convert::to_value(id).ok()? {
				id @ (Value::Null
				| Value::Uuid(_)
				| Value::Number(_)
				| Value::String(_)
				| Value::Datetime(_)) => Some(id),
				_ => None,
			};
		}
	}
}

/// A cursor over CBOR bytes that walks item boundaries without building values.
struct Scan<'a> {
	bytes: &'a [u8],
	pos: usize,
}

impl Scan<'_> {
	fn peek(&self) -> Option<u8> {
		self.bytes.get(self.pos).copied()
	}

	fn take(&mut self, len: usize) -> Option<&[u8]> {
		let end = self.pos.checked_add(len)?;
		let out = self.bytes.get(self.pos..end)?;
		self.pos = end;
		Some(out)
	}

	/// An item's major type, additional info, and argument; the argument is
	/// `None` for the indefinite-length marker.
	fn head(&mut self) -> Option<(u8, u8, Option<u64>)> {
		let first = self.take(1)?[0];
		let (major, info) = (first >> 5, first & 31);
		let arg = match info {
			0..=23 => Some(u64::from(info)),
			24 => Some(u64::from(self.take(1)?[0])),
			25 => Some(u64::from(u16::from_be_bytes(self.take(2)?.try_into().ok()?))),
			26 => Some(u64::from(u32::from_be_bytes(self.take(4)?.try_into().ok()?))),
			27 => Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?)),
			31 => None,
			_ => return None,
		};
		Some((major, info, arg))
	}

	/// Whether the next item is the definite-length text `name`, leaving the
	/// cursor on that item either way.
	fn key_is(&mut self, name: &str) -> Option<bool> {
		let start = self.pos;
		let (major, _, len) = self.head()?;
		let is = major == 3
			&& len == Some(name.len() as u64)
			&& self.bytes.get(self.pos..self.pos + name.len())? == name.as_bytes();
		self.pos = start;
		Some(is)
	}

	/// Step over one complete item, however deeply it nests.
	///
	/// Iterative, with an explicit stack of how many children each open
	/// container still expects (`None` while it is indefinite-length), so the
	/// depth is bounded by the input length rather than the call stack.
	fn skip_item(&mut self) -> Option<()> {
		let mut stack: Vec<Option<u64>> = vec![Some(1)];
		fn complete(stack: &mut [Option<u64>]) {
			if let Some(Some(n)) = stack.last_mut() {
				*n -= 1;
			}
		}
		while let Some(top) = stack.last() {
			if *top == Some(0) {
				stack.pop();
				complete(&mut stack);
				continue;
			}
			if self.peek()? == 0xff {
				self.pos += 1;
				if stack.pop()?.is_some() {
					return None;
				}
				complete(&mut stack);
				continue;
			}
			let (major, info, arg) = self.head()?;
			match major {
				0 | 1 | 7 => {
					if info == 31 {
						return None;
					}
					complete(&mut stack);
				}
				2 | 3 => match arg {
					Some(len) => {
						self.take(usize::try_from(len).ok()?)?;
						complete(&mut stack);
					}
					None => stack.push(None),
				},
				4 => stack.push(arg),
				5 => stack.push(match arg {
					Some(n) => Some(n.checked_mul(2)?),
					None => None,
				}),
				_ => stack.push(Some(1)),
			}
		}
		Some(())
	}
}

#[cfg(test)]
mod tests {
	use ciborium::Value as Cbor;

	use super::*;

	fn encode_cbor(value: &Cbor) -> Vec<u8> {
		let mut bytes = Vec::new();
		ciborium::into_writer(value, &mut bytes).unwrap();
		bytes
	}

	fn text(value: &str) -> Cbor {
		Cbor::Text(value.to_owned())
	}

	#[test]
	fn the_id_survives_a_parameter_that_cannot_be_converted() {
		// A record id that is not one: tag 8 over text that does not parse.
		let bad_param = Cbor::Tag(8, Box::new(text("abc")));
		let envelope = Cbor::Map(vec![
			(text("id"), text("7")),
			(text("method"), text("query")),
			(
				text("params"),
				Cbor::Array(vec![text("RETURN $id"), Cbor::Map(vec![(text("id"), bad_param)])]),
			),
		]);
		let bytes = encode_cbor(&envelope);
		assert!(
			decode(&bytes, 64).is_err(),
			"the envelope must not decode, or this proves nothing"
		);
		assert_eq!(request_id(&bytes, 64), Some(Value::String("7".to_owned())));
	}

	#[test]
	fn the_id_survives_parameters_nested_past_the_recursion_limit() {
		// `params` first, then `id`: a map `{params: [[[...]]], id: "7"}` whose
		// nesting is far beyond the limit, built by hand since the encoder
		// would recurse too.
		let mut bytes = vec![0xa2, 0x66];
		bytes.extend_from_slice(b"params");
		bytes.extend(std::iter::repeat_n(0x81, 100_000));
		bytes.push(0x80);
		bytes.extend_from_slice(&[0x62, b'i', b'd', 0x61, b'7']);
		assert_eq!(request_id(&bytes, 256), Some(Value::String("7".to_owned())));
	}

	#[test]
	fn the_id_is_found_past_indefinite_length_entries() {
		// {_ "params": [_ h'0102' (_ "a" "b")], "id": "x"}
		let mut bytes = vec![0xbf, 0x66];
		bytes.extend_from_slice(b"params");
		bytes.extend_from_slice(&[0x9f, 0x42, 1, 2, 0x7f, 0x61, b'a', 0x61, b'b', 0xff, 0xff]);
		bytes.extend_from_slice(&[0x62, b'i', b'd', 0x61, b'x', 0xff]);
		assert_eq!(request_id(&bytes, 256), Some(Value::String("x".to_owned())));
	}

	#[test]
	fn no_id_is_recovered_from_something_that_is_not_a_request() {
		assert_eq!(request_id(&encode_cbor(&text("id")), 64), None);
		assert_eq!(
			request_id(&encode_cbor(&Cbor::Map(vec![(text("method"), text("ping"))])), 64),
			None
		);
		assert_eq!(
			request_id(&encode_cbor(&Cbor::Map(vec![(text("id"), Cbor::Array(vec![]))])), 64),
			None
		);
		assert_eq!(request_id(b"\xff\xff", 64), None);
	}
}
