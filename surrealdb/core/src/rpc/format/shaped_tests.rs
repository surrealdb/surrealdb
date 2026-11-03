//! Shaped responses held to CBOR: every response, `query_stream` frame and live notification
//! written shaped reads back ([`decode_shaped`]) to what the same answer written in CBOR decodes
//! to.

use surrealdb_rpc::{DbResponse, DbResult, OutgoingFrame, RowFormat, StreamFrames};
use surrealdb_types::shaped::{BlockReader, ShapeScope, ValueShaper};
use surrealdb_types::{Notification, Value};

use super::encoded::tests::{QUERIES, datastore, encoded, items, session};
use super::encoded::{decode_shaped, encode_response, encode_results, encode_shaped_notification};

/// `value` as CBOR bytes: compared as bytes, so a NaN equals itself.
pub(crate) fn canonical(value: Value) -> Vec<u8> {
	super::cbor::encode(value).expect("encodes")
}

fn cbor_value(bytes: &[u8]) -> Value {
	super::cbor::decode(bytes, 1024).expect("CBOR decodes")
}

/// Every query's buffered response, shaped, reads back to what it decodes to in CBOR: bare (as
/// `/sql` answers) and in the RPC envelope.
#[tokio::test]
async fn a_shaped_response_decodes_as_its_cbor_response_decodes() {
	let ds = datastore().await;
	for sql in QUERIES {
		let (cbor, _) = encoded(&ds, sql, RowFormat::Cbor).await;
		let (shaped, frames) = encoded(&ds, sql, RowFormat::Shaped).await;
		if *sql == "SELECT * FROM t" {
			assert!(frames > 0, "`{sql}` streams shaped frames");
		}
		let (mut a, mut b) = (Vec::new(), Vec::new());
		encode_results(RowFormat::Cbor, &cbor, &mut a).expect("encodes");
		encode_results(RowFormat::Shaped, &shaped, &mut b).expect("encodes");
		let expected = canonical(cbor_value(&a));
		let actual = canonical(decode_shaped(&b, &mut BlockReader::new()).expect("reads"));
		assert_eq!(actual, expected, "`{sql}`");

		let id = Some(Value::from_t(7i64));
		let session_id = Some(uuid::Uuid::nil());
		let a = encode_response(
			RowFormat::Cbor,
			DbResponse::success(id.clone(), session_id, DbResult::Encoded(cbor)),
		)
		.expect("encodes");
		let b = encode_response(
			RowFormat::Shaped,
			DbResponse::success(id, session_id, DbResult::Encoded(shaped)),
		)
		.expect("encodes");
		let expected = canonical(cbor_value(&a));
		let actual = canonical(decode_shaped(&b, &mut BlockReader::new()).expect("reads"));
		assert_eq!(actual, expected, "enveloped `{sql}`");
	}
}

/// A response that holds no rows is the CBOR response, byte for byte.
#[tokio::test]
async fn a_shaped_response_without_rows_is_its_cbor_response() {
	let ds = datastore().await;
	for sql in ["RETURN 1 + 2", "INFO FOR DB", "THROW 'boom'", "SELECT * FROM ONLY t:1"] {
		let (cbor, _) = encoded(&ds, sql, RowFormat::Cbor).await;
		let (shaped, _) = encoded(&ds, sql, RowFormat::Shaped).await;
		let (mut a, mut b) = (Vec::new(), Vec::new());
		encode_results(RowFormat::Cbor, &cbor, &mut a).expect("encodes");
		encode_results(RowFormat::Shaped, &shaped, &mut b).expect("encodes");
		assert_eq!(a, b, "`{sql}`");
	}
}

/// A `query_stream`'s frames, shaped, read back in order through one reader to the frames the
/// stream sends in CBOR; each shape is announced once for the whole stream.
#[tokio::test]
async fn shaped_stream_frames_decode_as_cbor_frames_decode() {
	let ds = datastore().await;
	let id = Some(Value::from_t(7i64));
	let session_id = Some(uuid::Uuid::nil());
	let frames_of = |format: RowFormat, items: Vec<surrealdb_rpc::QueryStreamItem>| {
		let mut frames = StreamFrames::keeping_encoded_rows();
		for item in items {
			frames.absorb(item);
		}
		let mut scope = ShapeScope::new();
		let mut shaper = ValueShaper::new();
		std::iter::from_fn(|| frames.pop_outgoing())
			.map(|frame| {
				if format == RowFormat::Shaped {
					super::encoded::shaped_outgoing(frame, &mut shaper).expect("shapes")
				} else {
					frame
				}
			})
			.map(|frame| match frame {
				OutgoingFrame::Frame(frame) => encode_response(
					format,
					DbResponse::success(
						id.clone(),
						session_id,
						DbResult::Other(frame.into_value()),
					),
				)
				.expect("encodes"),
				OutgoingFrame::Rows {
					index,
					rows,
					trace,
				} => super::encoded::encode_stream_rows(
					format,
					id.clone(),
					session_id,
					index,
					&rows,
					trace,
					&mut scope,
				)
				.expect("encodes"),
			})
			.collect::<Vec<_>>()
	};
	for sql in QUERIES {
		let cbor = frames_of(RowFormat::Cbor, items(&ds, sql, Some(RowFormat::Cbor)).await);
		for source in [Some(RowFormat::Shaped), None] {
			let shaped = frames_of(RowFormat::Shaped, items(&ds, sql, source).await);
			assert_eq!(shaped.len(), cbor.len(), "`{sql}` frame count");
			let mut reader = BlockReader::new();
			let mut announced = 0;
			for s in &shaped {
				let item: ciborium::Value = ciborium::de::from_reader(s.as_slice()).expect("CBOR");
				let frame = if let ciborium::Value::Map(entries) = &item {
					entries.iter().find(|(k, _)| k.as_text() == Some("result")).map(|(_, v)| v)
				} else {
					None
				};
				if let Some(ciborium::Value::Map(frame)) = frame
					&& frame
						.iter()
						.any(|(k, v)| k.as_text() == Some("stream") && v.as_text() == Some("rows"))
				{
					let values = frame.iter().find(|(k, _)| k.as_text() == Some("values"));
					assert!(
						matches!(
							values,
							Some((_, ciborium::Value::Tag(surrealdb_types::shaped::CBOR_TAG, _)))
						),
						"`{sql}`: every rows frame of a shaped stream is a block"
					);
				}
			}
			for (s, c) in shaped.iter().zip(&cbor) {
				announced += count_shape_messages(s);
				let actual = canonical(decode_shaped(s, &mut reader).expect("reads"));
				assert_eq!(actual, canonical(cbor_value(c)), "`{sql}`");
			}
			if *sql == "SELECT * FROM t ORDER BY n" {
				assert!(announced >= 1, "a shape is announced");
			}
		}
	}
}

/// The SHAPE messages in the blocks of a CBOR response.
pub(crate) fn count_shape_messages(bytes: &[u8]) -> usize {
	fn walk(item: &ciborium::Value) -> usize {
		if let ciborium::Value::Tag(tag, inner) = item
			&& *tag == surrealdb_types::shaped::CBOR_TAG
		{
			let ciborium::Value::Bytes(contents) = &**inner else {
				return 0;
			};
			let mut at = 1;
			let mut n = 0;
			while at + 5 <= contents.len() {
				let len =
					u32::from_le_bytes(contents[at + 1..at + 5].try_into().expect("4")) as usize;
				n += usize::from(contents[at] == surrealdb_types::shaped::MESSAGE_SHAPE);
				at += 5 + len;
			}
			n
		} else if let ciborium::Value::Map(entries) = item {
			entries.iter().map(|(_, v)| walk(v)).sum()
		} else if let ciborium::Value::Array(items) = item {
			items.iter().map(walk).sum()
		} else {
			0
		}
	}
	let item: ciborium::Value = ciborium::de::from_reader(bytes).expect("CBOR");
	walk(&item)
}

fn notification(action: surrealdb_types::Action, result: Value) -> Notification {
	Notification::new(
		uuid::Uuid::from_u128(42).into(),
		None,
		action,
		Value::RecordId(surrealdb_types::RecordId::new("person", 1i64)),
		result,
	)
}

/// Live notifications of one subscription, shaped, read back in order to what they decode to in
/// CBOR: records, a record that changes shape, DIFF patches, deletions and the final `KILLED`.
/// The record's shape is announced once and reused.
#[tokio::test]
async fn shaped_notifications_decode_as_cbor_notifications_decode() {
	use surrealdb_types::Action;
	let ds = datastore().await;
	let record = async |n: i64| {
		let rows =
			ds.execute(&format!("SELECT * FROM ONLY t:{n}"), &session(), None).await.expect("runs");
		rows.into_iter().next().expect("a result").result.expect("a record")
	};
	let mut notes = Vec::new();
	for n in [1, 2, 3, 1] {
		notes.push(notification(Action::Create, record(n).await));
	}
	let diff = ds
		.execute(
			"RETURN [{ op: 'replace', path: '/n', value: 2 }, { op: 'remove', path: '/s' }]",
			&session(),
			None,
		)
		.await
		.expect("runs")
		.remove(0)
		.result
		.expect("patches");
	notes.push(notification(Action::Update, diff));
	notes.push(notification(Action::Delete, record(2).await));
	notes.push(notification(Action::Killed, Value::None));
	let mut shaper = ValueShaper::new();
	let mut scope = ShapeScope::new();
	let mut reader = BlockReader::new();
	let mut announced = Vec::new();
	for note in notes {
		let cbor = super::cbor::encode(surrealdb_types::SurrealValue::into_value(
			DbResponse::success(None, None, DbResult::Live(note.clone())),
		))
		.expect("encodes");
		let shaped =
			encode_shaped_notification(None, None, note, &mut shaper, &mut scope).expect("encodes");
		announced.push(count_shape_messages(&shaped));
		let actual = canonical(decode_shaped(&shaped, &mut reader).expect("reads"));
		let Value::Object(expected) = cbor_value(&cbor) else {
			panic!("a notification response is an object");
		};
		// A shaped notification's `result` is the block of its one row.
		let mut expected = expected.into_inner();
		if let Some(Value::Object(result)) = expected.remove("result") {
			let mut result = result.into_inner();
			let row = result.remove("result").unwrap_or(Value::None);
			result.insert("result".to_owned(), Value::Array(vec![row].into()));
			expected.insert("result".to_owned(), Value::Object(result.into()));
		}
		assert_eq!(actual, canonical(Value::Object(expected.into())));
	}
	assert_eq!(announced[0], 1, "the first notification announces its shape");
	assert_eq!(announced[3], 0, "a record of a shape already announced announces nothing");
}
