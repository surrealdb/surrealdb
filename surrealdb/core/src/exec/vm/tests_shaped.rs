//! Shaped rows from the VM, held to CBOR.
//!
//! A packed row is written by its generation's copy plan, every other row through the frame's
//! shaper; either way a statement's rows, written as blocks in one shape scope and read back,
//! are what its CBOR frames decode to.
//! Field permissions: a field never visible to the caller is named nowhere in the stream, a field
//! behind a row-dependent predicate is a column that may be absent, and a row it is hidden in holds
//! it absent, as a row that does not hold the field, its slot zero.

use std::collections::BTreeMap;

use surrealdb_rpc::{EncodedRows, QueryStreamItem, RowFormat};
use surrealdb_types::Value;
use surrealdb_types::shaped::{
	BlockReader, MAY_BE_ABSENT, STATE_ABSENT, STATE_VALUE, ShapeScope, block_contents, write_block,
};

use super::row_frame::{RowFrame, observed};
use super::tests_packed_reads::{LONG, dataset, layouts_of, rows};
use super::tests_packed_transcode::{GUARDED, generated_db};
use crate::catalog::StoredRecord;
use crate::dbs::Session;
use crate::exec::operators::scan::pipeline::decode_record_value;
use crate::exec::operators::test_util::TestDb;
use crate::kvs::QueryRequest;
use crate::rpc::format::encoded::decode_dynamic;
use crate::rpc::format::shaped_tests::canonical;

/// What one statement answered when streamed: its row frames, its single value, or its error.
#[derive(Debug, Default)]
struct Answer {
	frames: Vec<EncodedRows>,
	rows: Vec<Value>,
	value: Option<Value>,
	error: Option<String>,
}

/// Every statement of `sql` as `session`, streamed with rows in `format`.
async fn stream(db: &TestDb, session: &Session, sql: &str, format: RowFormat) -> Vec<Answer> {
	let (items, received) = async_channel::unbounded();
	let job = db
		.datastore()
		.run_streaming(QueryRequest::new(sql, session).with_row_format(Some(format)), items)
		.expect("the query starts");
	let mut answers: BTreeMap<usize, Answer> = BTreeMap::new();
	let drain = async {
		while let Ok(item) = received.recv().await {
			match item {
				QueryStreamItem::Frame {
					index,
					rows,
				} => {
					assert_eq!(rows.format(), format, "frames in the stream's format");
					answers.entry(index).or_default().frames.push(rows);
				}
				QueryStreamItem::Rows {
					index,
					values,
				} => answers.entry(index).or_default().rows.extend(values),
				QueryStreamItem::Value {
					index,
					value,
				} => answers.entry(index).or_default().value = Some(value),
				QueryStreamItem::Finished {
					index,
					error,
					..
				} => answers.entry(index).or_default().error = error.map(|e| e.to_string()),
				QueryStreamItem::Trace {
					..
				} => {}
			}
		}
	};
	let (ran, ()) = futures::join!(job.run, drain);
	ran.expect("the query runs");
	answers.into_values().collect()
}

/// A statement's value, its frames decoded through [`EncodedRows::to_values`].
fn value_of(answer: &Answer) -> Value {
	if let Some(value) = &answer.value {
		return value.clone();
	}
	let mut rows = Vec::new();
	for frame in &answer.frames {
		rows.extend(frame.to_values().expect("frames decode"));
	}
	rows.extend(answer.rows.iter().cloned());
	Value::Array(rows.into())
}

/// A statement's shaped frames written as blocks in one scope, and read back by one reader.
fn blocks_of(answer: &Answer) -> (Vec<Vec<u8>>, Value) {
	let mut scope = ShapeScope::new();
	let mut reader = BlockReader::new();
	let mut blocks = Vec::new();
	let mut rows = Vec::new();
	for frame in &answer.frames {
		let mut block = Vec::new();
		write_block(&mut block, &mut scope, frame.runs()).expect("frames");
		let contents = block_contents(&block).expect("a block");
		rows.extend(reader.read_block(contents, &mut decode_dynamic).expect("reads"));
		blocks.push(block);
	}
	(blocks, Value::Array(rows.into()))
}

/// `sql` as `session` answers the same shaped as in CBOR, statement by statement, through
/// `to_values` and through blocks. Answers the packed rows the shaped frames ran a copy plan for.
///
/// The count is the frame's thread-local counter, read before and after the statement: every
/// test here runs on a current-thread runtime, so the statement runs on this thread, and no other
/// test shares the thread.
async fn assert_shaped_as_cbor(db: &TestDb, session: &Session, sql: &str) -> usize {
	let cbor = stream(db, session, sql, RowFormat::Cbor).await;
	let before = observed::packed_visits();
	let shaped = stream(db, session, sql, RowFormat::Shaped).await;
	let visits = observed::packed_visits() - before;
	assert_eq!(shaped.len(), cbor.len(), "{sql}: statements");
	for (s, c) in shaped.iter().zip(&cbor) {
		assert_eq!(s.error, c.error, "{sql}");
		if c.error.is_some() {
			continue;
		}
		let expected = canonical(value_of(c));
		assert_eq!(canonical(value_of(s)), expected, "{sql}: rows decoded");
		if s.value.is_none() {
			assert!(s.rows.is_empty(), "{sql}: a shaped statement's rows are all in frames");
			assert_eq!(canonical(blocks_of(s).1), expected, "{sql}: rows read from blocks");
		}
	}
	visits
}

const STATEMENTS: &[&str] = &[
	"SELECT * FROM {t}",
	"SELECT * FROM {t} LIMIT 3",
	"SELECT * FROM {t} START 2 LIMIT 5",
	"SELECT * FROM {t} ORDER BY id DESC LIMIT 4",
	"SELECT * OMIT id FROM {t}",
	"SELECT id FROM {t}",
	"SELECT VALUE id FROM {t}",
	"SELECT * FROM ONLY {t} LIMIT 1",
	"SELECT count() FROM {t} GROUP ALL",
	"SELECT * FROM {t}; RETURN 1; SELECT * FROM {t} LIMIT 1",
];

/// Packed tables of every field kind, and generated ones, answer shaped as in CBOR, and a
/// `SELECT *` over them runs the copy plan.
#[tokio::test(flavor = "current_thread")]
async fn packed_rows_answer_shaped_as_in_cbor() {
	let owner = Session::owner().with_ns("test").with_db("test");
	let db = TestDb::new(&dataset()).await;
	for table in ["p", LONG] {
		for sql in STATEMENTS {
			assert_shaped_as_cbor(&db, &owner, &sql.replace("{t}", table)).await;
		}
		let visits = assert_shaped_as_cbor(&db, &owner, &format!("SELECT * FROM {table}")).await;
		assert!(visits > 0, "SELECT * FROM {table} runs the copy plan");
	}
	let db = generated_db(99, 60).await;
	for sql in STATEMENTS {
		assert_shaped_as_cbor(&db, &owner, &sql.replace("{t}", "g")).await;
	}
	assert_shaped_as_cbor(&db, &owner, "SELECT * FROM g WHERE i > 0").await;
	assert_shaped_as_cbor(&db, &owner, "SELECT * FROM g ORDER BY i").await;
}

/// Computed fields and field permissions, as the owner and as a record user, answer shaped as in
/// CBOR; so do projections.
#[tokio::test(flavor = "current_thread")]
async fn guarded_rows_answer_shaped_as_in_cbor() {
	let db = TestDb::new_with_auth(GUARDED).await;
	let owner = Session::owner().with_ns("test").with_db("test");
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "alice"));
	let alice = Session::for_record("test", "test", "user", rid);
	for session in [&owner, &alice] {
		for sql in STATEMENTS {
			assert_shaped_as_cbor(&db, session, &sql.replace("{t}", "q")).await;
		}
		for sql in [
			"SELECT a, c, mine FROM q",
			"SELECT a AS x, s FROM q WHERE a > 10",
			"SELECT *, a * 3 AS triple FROM q LIMIT 6",
			"SELECT * FROM q:3..9",
		] {
			assert_shaped_as_cbor(&db, session, sql).await;
		}
	}
}

const OWNED: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	CREATE user:alice; CREATE user:bob;
	DEFINE TABLE d SCHEMAFULL PERMISSIONS FULL;
	DEFINE FIELD owner ON d TYPE record<user>;
	DEFINE FIELD n ON d TYPE int;
	DEFINE FIELD mine ON d TYPE option<int> PERMISSIONS FOR select WHERE owner = $auth;
	DEFINE FIELD never_seen_xyz ON d TYPE string PERMISSIONS FOR select NONE;
	CREATE d:1 CONTENT { owner: user:alice, n: 1, mine: 101, never_seen_xyz: 's' };
	CREATE d:2 CONTENT { owner: user:bob, n: 2, mine: 202, never_seen_xyz: 's' };
	CREATE d:3 CONTENT { owner: user:alice, n: 3, never_seen_xyz: 's' };
";

fn user(name: &str) -> Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", name));
	Session::for_record("test", "test", "user", rid)
}

/// Row `at` of `frame`, its shape, its presence state for the shape's column `name`, and that
/// column's fixed slot.
fn column_in_row<'f>(frame: &'f EncodedRows, at: usize, name: &str) -> (&'f [u8], u8, &'f [u8]) {
	let start = if at == 0 {
		0
	} else {
		frame.ends()[at - 1] as usize
	};
	let row = &frame.bytes()[start..frame.ends()[at] as usize];
	let shape = &frame.shapes()[frame.row_shapes()[at] as usize];
	let column = shape.columns().iter().find(|c| c.name == name).expect("the column is named");
	assert_ne!(column.flags & MAY_BE_ABSENT, 0, "{name} may be absent");
	let state = (row[(column.presence / 4) as usize] >> (2 * (column.presence % 4))) & 0b11;
	let width = column.kind.fixed_width().expect("a fixed column") as usize;
	let slot = shape.presence_len() + column.slot as usize;
	(row, state, &row[slot..slot + width])
}

/// A field behind a row-dependent predicate is in every shape as may-be-absent; each record user
/// reads their own values, and a row whose value is hidden holds it absent with its slot zero,
/// the same bytes as a row that does not hold the field. A field no record user may select is named
/// nowhere in their streams.
#[tokio::test(flavor = "current_thread")]
async fn a_row_dependent_field_is_absent_and_zero_where_it_is_hidden() {
	let db = TestDb::new_with_auth(OWNED).await;
	let owner = Session::owner().with_ns("test").with_db("test");
	for (who, own, other) in [("alice", 0, 1), ("bob", 1, 0)] {
		let session = user(who);
		assert_shaped_as_cbor(&db, &session, "SELECT * FROM d").await;
		let answers = stream(&db, &session, "SELECT * FROM d", RowFormat::Shaped).await;
		let frames = &answers[0].frames;
		let frame = frames.iter().find(|f| f.len() >= 2).expect("one frame of the rows");
		let (_, state, slot) = column_in_row(frame, own, "mine");
		assert_eq!(state, STATE_VALUE, "{who} reads their own value");
		assert_ne!(slot, [0u8; 8], "{who}'s value is written");
		let (_, state, slot) = column_in_row(frame, other, "mine");
		assert_eq!(state, STATE_ABSENT, "{who} does not read the other's value");
		assert_eq!(slot, [0u8; 8], "a hidden slot is zero");
		let (blocks, _) = blocks_of(&answers[0]);
		for block in &blocks {
			assert!(
				!block.windows(14).any(|w| w == b"never_seen_xyz"),
				"{who}: a field never visible is named nowhere"
			);
		}
	}
	// Hidden from alice by its owner, then not held at all (`NONE` unsets it): the same bytes.
	let alice = user("alice");
	let hidden = stream(&db, &alice, "SELECT * FROM d:2", RowFormat::Shaped).await;
	db.run_as(&owner, "UPDATE d:2 SET mine = NONE").await;
	let none = stream(&db, &alice, "SELECT * FROM d:2", RowFormat::Shaped).await;
	// A statement kept for its record's table shapes the row when it runs, naming only the
	// fields the row holds; either way the two rows are written alike.
	let written = |answer: &Answer| {
		let frame = &answer.frames[0];
		let shape = &frame.shapes()[frame.row_shapes()[0] as usize];
		let names: Vec<String> = shape.columns().iter().map(|c| c.name.clone()).collect();
		let row = if shape.columns().iter().any(|c| c.name == "mine") {
			column_in_row(frame, 0, "mine").0.to_vec()
		} else {
			frame.bytes()[..frame.ends()[0] as usize].to_vec()
		};
		(names, row)
	};
	assert_eq!(
		written(&hidden[0]),
		written(&none[0]),
		"absent by permission is a field the row does not hold"
	);
}

/// A packed row whose absent fixed column holds stray bytes is written with that column absent and
/// its slot zero: the copy plan's runs never carry a slot the row leaves empty.
#[tokio::test(flavor = "current_thread")]
async fn a_stale_slot_never_reaches_the_wire() {
	let db = TestDb::new(&dataset()).await;
	let rows = rows(&db, "p").await;
	let layouts = layouts_of(&db, "p", &rows).await;
	let mut dirtied = 0;
	for (key, value) in rows.iter().filter(|(_, value)| StoredRecord::is_packed_bytes(value)) {
		let record = StoredRecord::parse(value).expect("parses").expect("v1");
		let generation = record.generation().expect("a generation").expect("named");
		let descriptor = layouts.admit_packed(generation).expect("admitted");
		let stamp = value[2..].iter().position(|byte| byte & 0x80 == 0).expect("a stamp");
		let body = 2 + stamp + 1;
		for column in &descriptor.columns {
			let (
				crate::catalog::Presence::Optional {
					bit,
				},
				crate::catalog::Location::Fixed(offset),
			) = (column.presence, column.location)
			else {
				continue;
			};
			if value[body + (bit / 8) as usize] & (1 << (bit % 8)) != 0 {
				continue;
			}
			let mut dirty = value.clone();
			for byte in &mut dirty[body + offset as usize..][..1] {
				*byte ^= 0x5A;
			}
			let write = |bytes: &[u8]| {
				let mut frame = RowFrame::with_format(RowFormat::Shaped);
				let before = observed::packed_visits();
				frame.push_stored(key, bytes, Some(&layouts)).expect("writes");
				assert!(observed::packed_visits() > before, "the copy plan ran");
				frame.data().to_vec()
			};
			assert_eq!(write(&dirty), write(value), "the stray byte is not written");
			dirtied += 1;
		}
	}
	assert!(dirtied > 0, "a packed row holds an absent fixed column");
}

/// Every packed row, written by its copy plan, decodes to what the whole decode of it encodes to
/// as a value; every bit flip and truncation of it is written or refused, never a panic.
#[tokio::test(flavor = "current_thread")]
async fn a_copy_plan_writes_what_the_whole_decode_reads() {
	let db = TestDb::new(&dataset()).await;
	let rows = rows(&db, "p").await;
	let layouts = layouts_of(&db, "p", &rows).await;
	for (key, value) in &rows {
		let decoded = decode_record_value(key, value, Some(&layouts)).expect("decodes");
		let mut by_plan = RowFrame::with_format(RowFormat::Shaped);
		by_plan.push_stored(key, value, Some(&layouts)).expect("writes");
		let mut by_value = RowFrame::with_format(RowFormat::Shaped);
		by_value.push_value(&decoded).expect("writes");
		let a = by_plan.take_encoded().to_values().expect("decodes");
		let b = by_value.take_encoded().to_values().expect("decodes");
		assert_eq!(canonical(Value::Array(a.into())), canonical(Value::Array(b.into())));
		let mut frame = RowFrame::with_format(RowFormat::Shaped);
		for at in 0..value.len() {
			for bit in 0..8 {
				let mut mutant = value.clone();
				mutant[at] ^= 1 << bit;
				let _ = frame.push_stored(key, &mutant, Some(&layouts));
			}
			let _ = frame.push_stored(key, &value[..at], Some(&layouts));
		}
		// Whatever was written reads back or is refused.
		let _ = frame.take_encoded().to_values();
	}
}

/// A table whose rows span two generations (a field defined after some were written, before a
/// migration) answers shaped as in CBOR, its rows in the shape of each generation.
#[tokio::test(flavor = "current_thread")]
async fn a_shape_changes_mid_stream() {
	let db = TestDb::new(
		"
		DEFINE TABLE m SCHEMAFULL;
		DEFINE FIELD a ON m TYPE int;
		FOR $i IN 1..=5 { CREATE type::record('m', $i) SET a = $i };
		DEFINE FIELD b ON m TYPE option<string>;
		FOR $i IN 6..=9 { CREATE type::record('m', $i) SET a = $i, b = 'x' };
	",
	)
	.await;
	let owner = Session::owner().with_ns("test").with_db("test");
	let visits = assert_shaped_as_cbor(&db, &owner, "SELECT * FROM m").await;
	assert!(visits > 0, "packed rows run their generation's copy plan");
	let answers = stream(&db, &owner, "SELECT * FROM m", RowFormat::Shaped).await;
	let shapes: usize = answers[0].frames.iter().map(|f| f.shapes().len()).max().unwrap_or(0);
	let (blocks, _) = blocks_of(&answers[0]);
	let announced: usize =
		blocks.iter().map(|b| crate::rpc::format::shaped_tests::count_shape_messages(b)).sum();
	assert!(shapes >= 2 || announced >= 2, "rows of two generations in two shapes");
}

/// Fields declared as arrays of one plain kind are written natively by the copy plan, and answer as
/// in CBOR; so do rows whose array the plan cannot write natively.
#[tokio::test(flavor = "current_thread")]
async fn declared_arrays_are_written_natively() {
	let db = TestDb::new(
		"
		DEFINE TABLE n SCHEMAFULL;
		DEFINE FIELD tags ON n TYPE array<string>;
		DEFINE FIELD nums ON n TYPE array<int>;
		DEFINE FIELD flags ON n TYPE option<array<bool>>;
		DEFINE FIELD ratios ON n TYPE array<float>;
		DEFINE FIELD decs ON n TYPE array<decimal>;
		DEFINE FIELD ids ON n TYPE array<uuid>;
		DEFINE FIELD whens ON n TYPE array<datetime>;
		DEFINE FIELD spans ON n TYPE array<duration>;
		DEFINE FIELD blobs ON n TYPE array<bytes>;
		FOR $i IN 1..=20 {
			CREATE type::record('n', $i) SET tags = ['a', 'é', <string> $i], nums = [$i, -$i],
				flags = IF $i % 2 = 0 THEN [true, false] ELSE NONE END, ratios = [$i / 3f],
				decs = [1.5dec, <decimal> $i], ids = [u'018f9b8e-0000-7000-8000-000000000000'],
				whens = [d'2024-01-01T00:00:00Z' + duration::from_secs($i), d'1900-06-15T12:00:00Z'],
				spans = [1h30m, duration::from_nanos($i)], blobs = [<bytes> 'ab', <bytes> ''];
		};
		CREATE n:empty SET tags = [], nums = [], ratios = [], decs = [], ids = [], whens = [],
			spans = [], blobs = [];
	",
	)
	.await;
	let owner = Session::owner().with_ns("test").with_db("test");
	let whole = observed::whole_decodes();
	let visits = assert_shaped_as_cbor(&db, &owner, "SELECT * FROM n").await;
	assert_eq!(visits, 21, "every row runs the copy plan");
	assert_eq!(observed::whole_decodes(), whole, "no row falls back to a whole decode");
	let answers = stream(&db, &owner, "SELECT * FROM n", RowFormat::Shaped).await;
	let frame = &answers[0].frames[0];
	let shape = &frame.shapes()[frame.row_shapes()[0] as usize];
	for name in ["tags", "nums", "flags", "ratios", "decs", "ids", "whens", "spans", "blobs"] {
		let column = shape.columns().iter().find(|c| c.name == name).expect("a column");
		assert_eq!(column.kind, surrealdb_types::shaped::Kind::Array, "{name} is native");
	}
}

/// Record ids holding the bytes a record key escapes (`\x00` and `\x01`), and unicode, in tables
/// with long unicode names, answer shaped as in CBOR: a packed row's id written by its copy plan,
/// and a document's id with its table, each unescaped and without the key's terminator.
#[tokio::test(flavor = "current_thread")]
async fn ids_with_escaped_key_bytes_answer_as_in_cbor() {
	let packed = format!("{}é", "tâble_with_a_long_name_".repeat(4));
	let documents = format!("{}ü", "dôcuments_with_a_long_name_".repeat(4));
	let db = TestDb::new(&format!(
		"DEFINE TABLE ⟨{packed}⟩ SCHEMAFULL; DEFINE FIELD n ON ⟨{packed}⟩ TYPE int;"
	))
	.await;
	let owner = Session::owner().with_ns("test").with_db("test");
	let ids = ["a\u{0}b", "\u{1}", "é\u{1}\u{0}x", "plain", "😀\u{0}", "\u{0}\u{0}\u{1}\u{1}"];
	let mut vars = surrealdb_types::Variables::new();
	vars.insert("packed", packed.clone());
	vars.insert("documents", documents.clone());
	vars.insert(
		"ids",
		surrealdb_types::Value::Array(
			ids.iter()
				.map(|id| surrealdb_types::Value::String((*id).to_owned()))
				.collect::<Vec<_>>()
				.into(),
		),
	);
	let sql = "FOR $id IN $ids {
		CREATE type::record($packed, $id) SET n = 1;
		CREATE type::record($documents, $id) SET n = 2, s = $id;
	}";
	for response in db.datastore().execute(sql, &owner, Some(vars)).await.expect("runs") {
		response.result.expect("a record is created");
	}
	let visits = assert_shaped_as_cbor(&db, &owner, &format!("SELECT * FROM ⟨{packed}⟩")).await;
	assert!(visits > 0, "the packed rows run their copy plan");
	assert_shaped_as_cbor(&db, &owner, &format!("SELECT * FROM ⟨{documents}⟩")).await;
	assert_shaped_as_cbor(&db, &owner, &format!("SELECT id FROM ⟨{documents}⟩")).await;
	let answers =
		stream(&db, &owner, &format!("SELECT * FROM ⟨{packed}⟩"), RowFormat::Shaped).await;
	assert_eq!(value_of(&answers[0]), blocks_of(&answers[0]).1);
}
