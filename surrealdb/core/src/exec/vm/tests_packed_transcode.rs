//! Packed rows transcoded to the wire by visiting them.
//!
//! [`RowFrame::push_stored`] writes a packed row from its bytes ([`PackedRow::visit`]), with its
//! `id` from the key, without checking it whole or decoding it. These tests hold it to
//! [`RowFrame::push_value`] of the whole decode, byte for byte in CBOR and in JSON, over a dataset
//! of every field kind and over generated records; hold every bit flip and truncation of the rows
//! to that decode wherever the decode accepts the bytes; show that the whole-row check is not
//! made; and hold a streamed `SELECT *` over frames to the same statement over values.
//!
//! [`PackedRow::visit`]: crate::catalog::packed::PackedRow::visit

use std::collections::BTreeMap;

use surrealdb_rpc::{QueryStreamItem, RowFormat};
use surrealdb_types::Variables;

use super::row_frame::{RowFrame, observed};
use super::tests_packed_reads::{LONG, dataset, layouts_of, rows};
use crate::catalog::{StoredRecord, TableLayouts};
use crate::dbs::Session;
use crate::exec::operators::scan::pipeline::decode_record_value;
use crate::exec::operators::test_util::TestDb;
use crate::key::KVKeyDecode as _;
use crate::key::schema::RecordKey;
use crate::kvs::QueryRequest;

const FORMATS: [RowFormat; 2] = [RowFormat::Cbor, RowFormat::Json];

/// The bytes `push_value` writes for the whole decode of `value` in `format`.
fn expected(format: RowFormat, decoded: &crate::val::Value) -> Vec<u8> {
	let mut frame = RowFrame::with_format(format);
	frame.push_value(decoded).expect("a decoded row encodes");
	frame.row(0).to_vec()
}

/// The bytes `push_stored` writes for `value` in `format`, and whether it visited the row.
fn transcoded(
	format: RowFormat,
	key: &[u8],
	value: &[u8],
	layouts: &TableLayouts,
) -> anyhow::Result<(Vec<u8>, bool)> {
	let mut frame = RowFrame::with_format(format);
	let before = observed::packed_visits();
	frame.push_stored(key, value, Some(layouts))?;
	Ok((frame.row(0).to_vec(), observed::packed_visits() > before))
}

/// Holds every row of `rows` to the whole decode, byte for byte in both formats, and every packed
/// one to the visit without a whole decode. Answers how many rows were packed.
fn assert_transcodes(table: &str, rows: &[(Vec<u8>, Vec<u8>)], layouts: &TableLayouts) -> usize {
	let mut packed = 0;
	for (key, value) in rows {
		let what = format!("{table} {:?}", RecordKey::decode_key(key).map(|k| k.id));
		let decoded = decode_record_value(key, value, Some(layouts)).expect("a row decodes");
		let is_packed = StoredRecord::is_packed_bytes(value);
		packed += usize::from(is_packed);
		for format in FORMATS {
			let whole = observed::whole_decodes();
			let (bytes, visited) = transcoded(format, key, value, layouts).expect("transcodes");
			assert_eq!(
				String::from_utf8_lossy(&bytes),
				String::from_utf8_lossy(&expected(format, &decoded)),
				"{what} {format:?}"
			);
			if is_packed {
				assert!(visited, "{what}: a packed row is visited");
				assert_eq!(observed::whole_decodes(), whole, "{what}: and not decoded whole");
			}
		}
	}
	packed
}

#[tokio::test]
async fn a_packed_row_transcodes_as_its_whole_decode_encodes() {
	let db = TestDb::new(&dataset()).await;
	let mut packed = 0;
	for table in ["p", LONG] {
		let rows = rows(&db, table).await;
		let layouts = layouts_of(&db, table, &rows).await;
		packed += assert_transcodes(table, &rows, &layouts);
	}
	assert!(packed > 10, "{packed} packed rows");
}

// Generated records

/// A xorshift64* generator: the same seed gives the same records on every run.
struct Rng(u64);

impl Rng {
	fn next(&mut self) -> u64 {
		let mut x = self.0;
		x ^= x >> 12;
		x ^= x << 25;
		x ^= x >> 27;
		self.0 = x;
		x.wrapping_mul(0x2545_F491_4F6C_DD1D)
	}

	fn below(&mut self, n: u64) -> u64 {
		self.next() % n
	}

	fn one_in(&mut self, n: u64) -> bool {
		self.below(n) == 0
	}

	fn pick<'a>(&mut self, of: &[&'a str]) -> &'a str {
		of[self.below(of.len() as u64) as usize]
	}
}

const SCHEMA: &str = "
	DEFINE TABLE h SCHEMAFULL;
	DEFINE FIELD n ON h TYPE option<int>;
	DEFINE TABLE g SCHEMAFULL;
	DEFINE FIELD anyv ON g TYPE any;
	DEFINE FIELD arr ON g TYPE array<int>;
	DEFINE FIELD b ON g TYPE bool;
	DEFINE FIELD by ON g TYPE option<bytes>;
	DEFINE FIELD dec ON g TYPE option<decimal>;
	DEFINE FIELD du ON g TYPE option<duration>;
	DEFINE FIELD f ON g TYPE option<float>;
	DEFINE FIELD flex ON g TYPE option<object> FLEXIBLE;
	DEFINE FIELD i ON g TYPE int;
	DEFINE FIELD link ON g TYPE option<record<g | h>>;
	DEFINE FIELD lit ON g TYPE 'x' | 'y' | 'z';
	DEFINE FIELD nl ON g TYPE int | null;
	DEFINE FIELD obj ON g TYPE option<object>;
	DEFINE FIELD obj.a ON g TYPE int;
	DEFINE FIELD obj.b ON g TYPE option<string>;
	DEFINE FIELD obj.c ON g TYPE option<object>;
	DEFINE FIELD obj.c.d ON g TYPE option<float>;
	DEFINE FIELD onl ON g TYPE option<string | null>;
	DEFINE FIELD pt ON g TYPE option<geometry<point>>;
	DEFINE FIELD s ON g TYPE option<string>;
	DEFINE FIELD t ON g TYPE option<datetime>;
	DEFINE FIELD u ON g TYPE option<uuid>;
	DEFINE FIELD z ON g TYPE option<int>;
";

/// A string with quotes, escapes, control bytes and characters of every UTF-8 width.
fn gen_string(rng: &mut Rng) -> String {
	let parts =
		["a", "Z", "'", "\"", "\\", "\0", "\u{1}", "\n", "é", "€", "😀", " ", "⟨", "⟩", "id"];
	let long = rng.one_in(8);
	let len = rng.below(if long {
		300
	} else {
		12
	});
	(0..len).map(|_| rng.pick(&parts)).collect()
}

/// A SurrealQL literal of any shape, nested at most `depth` more levels.
fn gen_any(rng: &mut Rng, depth: u32, vars: &mut Variables, n: &mut usize) -> String {
	match rng.below(if depth == 0 {
		6
	} else {
		9
	}) {
		0 => format!("{}", rng.next() as i64),
		1 => rng.pick(&["1.5f", "-0.0f", "1e300f", "0.1f", "math::inf", "-math::inf"]).to_owned(),
		2 => format!("{}dec", rng.below(100_000) as f64 / 100.0),
		3 => {
			*n += 1;
			vars.insert(format!("v{n}"), gen_string(rng));
			format!("$v{n}")
		}
		4 => rng.pick(&["NULL", "true", "false", "d'2020-01-01T00:00:00Z'", "1h2m3s"]).to_owned(),
		5 => rng
			.pick(&[
				"g:1",
				"h:k",
				"g:u'0189f5c4-0000-7000-8000-000000000001'",
				"u'0189f5c4-0000-7000-8000-000000000001'",
			])
			.to_owned(),
		6 | 7 => {
			let wide = rng.one_in(4);
			let len = rng.below(if wide {
				40
			} else {
				5
			});
			let items: Vec<String> = (0..len).map(|_| gen_any(rng, depth - 1, vars, n)).collect();
			format!("[{}]", items.join(", "))
		}
		_ => gen_object(rng, depth - 1, vars, n),
	}
}

/// An object literal of random keys, `id` among them.
fn gen_object(rng: &mut Rng, depth: u32, vars: &mut Variables, n: &mut usize) -> String {
	let keys = ["a", "aa", "b", "id", "k", "z", "zz", "é", "with space", "flex"];
	let many = rng.one_in(6);
	let len = rng.below(if many {
		10
	} else {
		4
	});
	let mut entries = BTreeMap::new();
	for _ in 0..len {
		let key = rng.pick(&keys);
		entries.insert(key, gen_any(rng, depth, vars, n));
	}
	let entries: Vec<String> = entries.into_iter().map(|(k, v)| format!("`{k}`: {v}")).collect();
	format!("{{ {} }}", entries.join(", "))
}

/// Statements that create `count` generated records of `g`, and the variables they read.
fn generated(seed: u64, count: usize) -> (String, Variables) {
	let mut rng = Rng(seed | 1);
	let mut vars = Variables::new();
	let mut n = 0usize;
	let mut sql = String::from(SCHEMA);
	for record in 0..count {
		let mut fields = vec![
			format!(
				"i: {}",
				match rng.below(4) {
					0 => i64::MIN + 1,
					1 => i64::MAX,
					_ => rng.next() as i64 >> rng.below(60),
				}
			),
			format!("b: {}", rng.one_in(2)),
			format!("lit: '{}'", rng.pick(&["x", "y", "z"])),
			format!(
				"nl: {}",
				if rng.one_in(3) {
					"NULL".to_owned()
				} else {
					(rng.next() as i32).to_string()
				}
			),
		];
		let wide = rng.one_in(5);
		let len = rng.below(if wide {
			40
		} else {
			4
		});
		let arr: Vec<String> = (0..len).map(|_| (rng.next() as i16).to_string()).collect();
		fields.push(format!("arr: [{}]", arr.join(", ")));
		if !rng.one_in(4) {
			fields.push(format!("anyv: {}", gen_any(&mut rng, 3, &mut vars, &mut n)));
		}
		let optional: [(&str, &[&str]); 9] = [
			("f", &["1.5f", "-0.0f", "1e300f", "0.1f", "math::inf", "-math::inf", "3f"]),
			("dec", &["0dec", "2.25dec", "-1.000001dec", "79228162514264337593543950335dec"]),
			("du", &["0ns", "1ns", "90s", "1h", "600y", "52w"]),
			(
				"t",
				&[
					"d'2024-01-02T03:04:05.006Z'",
					"d'1970-01-01T00:00:00Z'",
					"d'2600-01-01T00:00:00Z'",
					"d'1500-06-01T12:00:00Z'",
				],
			),
			(
				"u",
				&[
					"u'00000000-0000-0000-0000-000000000000'",
					"u'ffffffff-ffff-ffff-ffff-ffffffffffff'",
				],
			),
			("by", &["<bytes>'ab'", "<bytes>''", "<bytes>'a longer byte string than most'"]),
			(
				"link",
				&[
					"g:1",
					"g:-7",
					"h:k",
					"h:⟨a b⟩",
					"h:⟨k'ey⟩",
					"g:u'0189f5c4-0000-7000-8000-000000000001'",
					"g:[1, 'a']",
					"h:{ a: 1 }",
				],
			),
			("pt", &["(1.0, 2.0)", "(-0.5, 1e10)"]),
			("z", &["0", "-1", "9223372036854775807"]),
		];
		for (name, choices) in optional {
			if rng.one_in(2) {
				fields.push(format!("{name}: {}", rng.pick(choices)));
			}
		}
		if rng.one_in(2) {
			n += 1;
			vars.insert(format!("v{n}"), gen_string(&mut rng));
			fields.push(format!("s: $v{n}"));
		}
		match rng.below(3) {
			0 => {}
			1 => fields.push("onl: NULL".to_owned()),
			_ => {
				n += 1;
				vars.insert(format!("v{n}"), gen_string(&mut rng));
				fields.push(format!("onl: $v{n}"));
			}
		}
		if rng.one_in(2) {
			let mut obj = vec![format!("a: {}", rng.next() as i32)];
			if rng.one_in(2) {
				n += 1;
				vars.insert(format!("v{n}"), gen_string(&mut rng));
				obj.push(format!("b: $v{n}"));
			}
			match rng.below(3) {
				0 => {}
				1 => obj.push("c: {}".to_owned()),
				_ => obj.push(format!("c: {{ d: {} }}", rng.pick(&["1.5f", "-2f", "0.1f"]))),
			}
			fields.push(format!("obj: {{ {} }}", obj.join(", ")));
		}
		if rng.one_in(2) {
			fields.push(format!("flex: {}", gen_object(&mut rng, 2, &mut vars, &mut n)));
		}
		// Ids of every kind the key holds in place, and one it does not.
		let id = match rng.below(5) {
			0 | 1 => format!("{record}"),
			4 => format!("u'0189f5c4-0000-7000-8000-{record:012x}'"),
			2 => {
				n += 1;
				vars.insert(format!("v{n}"), format!("k{record}{}", gen_string(&mut rng)));
				format!("$v{n}")
			}
			_ => format!("[{record}, 'k']"),
		};
		sql.push_str(&format!(
			"CREATE type::record('g', {id}) CONTENT {{ {} }};\n",
			fields.join(", ")
		));
	}
	(sql, vars)
}

/// A datastore holding the generated records of `seed`.
pub(super) async fn generated_db(seed: u64, count: usize) -> TestDb {
	let (sql, vars) = generated(seed, count);
	let db = TestDb::new("").await;
	let session = Session::owner().with_ns("test").with_db("test");
	for response in db.datastore().execute(&sql, &session, Some(vars)).await.expect("runs") {
		response.result.expect("a generated record is created");
	}
	db
}

#[tokio::test]
async fn a_generated_packed_row_transcodes_as_its_whole_decode_encodes() {
	let (mut rows_seen, mut packed) = (0, 0);
	for seed in [1, 2, 3, 5, 8, 13, 21, 0xDEAD_BEEF] {
		let db = generated_db(seed, 120).await;
		let rows = rows(&db, "g").await;
		let layouts = layouts_of(&db, "g", &rows).await;
		rows_seen += rows.len();
		packed += assert_transcodes("g", &rows, &layouts);
	}
	assert!(rows_seen >= 900, "{rows_seen} rows");
	assert!(packed * 10 >= rows_seen * 9, "{packed} of {rows_seen} rows packed");
}

// Corruption

/// Every bit flip and every truncation of every packed row: none panics; a mutant the whole decode
/// accepts transcodes to what its decode encodes, without a whole decode; a truncation fails.
/// Answers how many flips the decode refuses and the transcode writes, by the decode's refusal.
fn mutate_all(
	rows: &[(Vec<u8>, Vec<u8>)],
	layouts: &TableLayouts,
	accepted: &mut BTreeMap<String, usize>,
) -> usize {
	let mut mutants = 0;
	for (key, value) in rows.iter().filter(|(_, value)| StoredRecord::is_packed_bytes(value)) {
		let mut check = |mutant: &[u8], truncated: bool| {
			mutants += 1;
			let decoded = decode_record_value(key, mutant, Some(layouts));
			for format in FORMATS {
				let whole = observed::whole_decodes();
				let written = transcoded(format, key, mutant, layouts);
				// A decoded value `push_value` cannot encode (a regex a flip made) fails both ways.
				let encoded = decoded.as_ref().ok().map(|decoded| {
					let mut frame = RowFrame::with_format(format);
					frame.push_value(decoded).map(|()| frame.row(0).to_vec())
				});
				match (&decoded, written) {
					(Ok(_), Ok((bytes, visited))) => {
						let Some(Ok(encoded)) = encoded else {
							panic!("a row whose decode does not encode transcodes: {mutant:02x?}")
						};
						assert_eq!(bytes, encoded, "{format:?} {mutant:02x?}");
						if visited {
							assert_eq!(observed::whole_decodes(), whole, "visited, not decoded");
						}
					}
					(Ok(_), Err(error)) => {
						assert!(
							matches!(encoded, Some(Err(_))),
							"a row that decodes does not transcode: {error:#}\n{mutant:02x?}"
						);
					}
					(Err(error), Ok(_)) if format == RowFormat::Cbor => {
						assert!(!truncated, "a truncation transcodes: {mutant:02x?}");
						let name = format!("{error:#}");
						let name = name.rsplit(": ").next().unwrap_or_default();
						let name = name.split(" for column").next().unwrap_or_default().to_owned();
						*accepted.entry(name).or_default() += 1;
					}
					(Err(_), _) => {}
				}
			}
		};
		for at in 0..value.len() {
			for bit in 0..8 {
				let mut mutant = value.clone();
				mutant[at] ^= 1 << bit;
				check(&mutant, false);
			}
			check(&value[..at], true);
		}
	}
	mutants
}

#[tokio::test]
async fn a_corrupt_packed_row_never_panics_and_transcodes_as_it_decodes_where_it_decodes() {
	let mut accepted = BTreeMap::new();
	let mut mutants = 0;
	let db = TestDb::new(&dataset()).await;
	let p = rows(&db, "p").await;
	let layouts = layouts_of(&db, "p", &p).await;
	mutants += mutate_all(&p, &layouts, &mut accepted);
	let db = generated_db(77, 40).await;
	let g = rows(&db, "g").await;
	let layouts = layouts_of(&db, "g", &g).await;
	mutants += mutate_all(&g, &layouts, &mut accepted);
	assert!(mutants > 50_000, "{mutants} mutants");
	eprintln!(
		"{mutants} mutants; flips the decode refuses and the transcode writes, by refusal: {accepted:#?}"
	);
}

// The whole-row check is not made

/// A packed row whose absent fixed column holds a stray byte is refused by the whole decode, which
/// checks that a column the row leaves empty holds nothing; the transcode reads only the columns
/// the record holds, so it writes the row as the clean bytes write it.
#[tokio::test]
async fn a_packed_row_is_not_checked_whole_when_it_is_transcoded() {
	let db = TestDb::new(&dataset()).await;
	let rows = rows(&db, "p").await;
	let layouts = layouts_of(&db, "p", &rows).await;
	let mut dirtied = 0;
	for (key, value) in rows.iter().filter(|(_, value)| StoredRecord::is_packed_bytes(value)) {
		let record = StoredRecord::parse(value).expect("parses").expect("v1");
		let generation = record.generation().expect("a generation").expect("named");
		let descriptor = layouts.admit_packed(generation).expect("admitted");
		// The marker, the header byte, then the stamp in LEB128.
		let stamp = value[2..].iter().position(|byte| byte & 0x80 == 0).expect("a stamp");
		let body = 2 + stamp + 1;
		// An optional fixed column whose presence bit is clear: absent from the record.
		let absent =
			descriptor.columns.iter().find_map(|column| match (column.presence, column.location) {
				(
					crate::catalog::Presence::Optional {
						bit,
					},
					crate::catalog::Location::Fixed(offset),
				) if value[body + (bit / 8) as usize] & (1 << (bit % 8)) == 0 => Some(offset as usize),
				_ => None,
			});
		let Some(offset) = absent else {
			continue;
		};
		let mut dirty = value.clone();
		dirty[body + offset] ^= 0x5A;
		assert!(decode_record_value(key, &dirty, Some(&layouts)).is_err(), "the decode checks it");
		for format in FORMATS {
			let whole = observed::whole_decodes();
			let (clean, _) = transcoded(format, key, value, &layouts).expect("transcodes");
			let (written, visited) = transcoded(format, key, &dirty, &layouts).expect("transcodes");
			assert!(visited, "visited");
			assert_eq!(observed::whole_decodes(), whole, "not decoded whole");
			assert_eq!(written, clean, "{format:?}");
		}
		dirtied += 1;
	}
	assert!(dirtied > 0, "a packed row holds an absent fixed column");
}

// Streamed statements

/// What one statement answered: its rows as values, and as the JSON texts a frame joins with `,`.
type Answer = Result<(Vec<surrealdb_types::Value>, String), String>;

/// The rows `sql` answers as `session`, per statement, streamed with rows encoded in `format`, or
/// as values when `None`; a statement that fails answers its error. Rows that arrive as values are
/// also rendered as JSON, and rows that arrive in a JSON frame are kept as its text alone.
async fn streamed(
	db: &TestDb,
	session: &Session,
	sql: &str,
	format: Option<RowFormat>,
) -> Vec<Answer> {
	let (items, received) = async_channel::unbounded();
	let job = db
		.datastore()
		.run_streaming(QueryRequest::new(sql, session).with_row_format(format), items)
		.expect("the query starts");
	let mut answers: BTreeMap<usize, Answer> = BTreeMap::new();
	let join = |text: &mut String, more: &str| {
		if !text.is_empty() && !more.is_empty() {
			text.push(',');
		}
		text.push_str(more);
	};
	let drain = async {
		while let Ok(item) = received.recv().await {
			match item {
				QueryStreamItem::Rows {
					index,
					values,
				} => {
					if let Ok((rows, text)) = answers.entry(index).or_insert(Ok(Default::default()))
					{
						for value in values {
							let json = crate::rpc::format::json::encode_str(value.clone())
								.expect("a value renders as JSON");
							join(text, &json);
							rows.push(value);
						}
					}
				}
				QueryStreamItem::Frame {
					index,
					rows: frame,
				} => {
					assert_eq!(Some(frame.format()), format, "frames only when asked for");
					if let Ok((rows, text)) = answers.entry(index).or_insert(Ok(Default::default()))
					{
						match frame.format() {
							RowFormat::Cbor | RowFormat::Cbor2 | RowFormat::Shaped => {
								rows.extend(frame.to_values().expect("decodes"))
							}
							RowFormat::Json => {
								join(text, std::str::from_utf8(frame.bytes()).expect("UTF-8"))
							}
						}
					}
				}
				QueryStreamItem::Value {
					index,
					value,
				} => {
					answers.insert(index, Ok((vec![value], String::new())));
				}
				QueryStreamItem::Finished {
					index,
					error,
					..
				} => {
					let entry = answers.entry(index).or_insert(Ok(Default::default()));
					if let Some(error) = error {
						*entry = Err(error.to_string());
					}
				}
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

/// `sql` as `session` answers the same over frames as over values: the same rows over CBOR frames,
/// and their JSON over JSON frames. Answers the packed rows the frames visited, and those the frame
/// runs checked or decoded whole for their bodies.
async fn frames_against_values(db: &TestDb, session: &Session, sql: &str) -> (usize, usize) {
	let values = streamed(db, session, sql, None).await;
	let (mut visits, mut read) = (0, 0);
	for format in FORMATS {
		let before = observed::packed_visits();
		super::row_packed::observed::reset();
		let framed = streamed(db, session, sql, Some(format)).await;
		visits += observed::packed_visits() - before;
		read += super::row_packed::observed::whole() + super::row_packed::observed::in_place();
		assert_eq!(framed.len(), values.len(), "{sql}: statements");
		for (framed, values) in framed.iter().zip(&values) {
			match (framed, values, format) {
				(Ok((rows, _)), Ok((expected, _)), RowFormat::Cbor) => {
					assert_eq!(rows, expected, "{sql} over CBOR frames")
				}
				(Ok((single, text)), Ok((expected, json)), RowFormat::Json) => {
					if text.is_empty() && !single.is_empty() {
						assert_eq!(single, expected, "{sql}: a single value");
					} else {
						assert_eq!(text, json, "{sql} over JSON frames");
					}
				}
				(framed, values, _) => {
					assert_eq!(framed.is_err(), values.is_err(), "{sql}: {framed:?} {values:?}")
				}
			}
		}
	}
	(visits, read)
}

/// [`frames_against_values`], answering the packed rows the frames visited.
async fn assert_frames_answer_as_values(db: &TestDb, session: &Session, sql: &str) -> usize {
	frames_against_values(db, session, sql).await.0
}

pub(super) const GUARDED: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	CREATE user:alice;
	DEFINE TABLE q SCHEMAFULL PERMISSIONS FULL;
	DEFINE FIELD a ON q TYPE int;
	DEFINE FIELD c ON q COMPUTED a * 2;
	DEFINE FIELD owner ON q TYPE option<record<user>>;
	DEFINE FIELD s ON q TYPE option<string>;
	DEFINE FIELD secret ON q TYPE string PERMISSIONS FOR select NONE;
	DEFINE FIELD mine ON q TYPE option<int> PERMISSIONS FOR select WHERE owner = $auth;
	FOR $i IN 1..=40 {
		CREATE type::record('q', $i) CONTENT {
			a: $i, secret: 'hidden', s: IF $i % 3 = 0 THEN NONE ELSE 'v' END,
			owner: IF $i % 2 = 0 THEN user:alice ELSE NONE END, mine: $i
		};
	};
";

#[tokio::test]
async fn a_streamed_select_of_packed_rows_answers_over_frames_as_over_values() {
	let db = TestDb::new_with_auth(GUARDED).await;
	let rows = rows(&db, "q").await;
	assert!(rows.iter().all(|(_, value)| StoredRecord::is_packed_bytes(value)), "q is packed");
	let owner = Session::owner().with_ns("test").with_db("test");
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "alice"));
	let alice = Session::for_record("test", "test", "user", rid);
	let statements = [
		"SELECT * FROM q",
		"SELECT * FROM q LIMIT 3",
		"SELECT * FROM q START 5 LIMIT 7",
		"SELECT * FROM q WHERE a > 30",
		"SELECT * FROM q ORDER BY a DESC LIMIT 4",
		"SELECT * FROM q:3..9",
		"SELECT * FROM q:5",
		"SELECT * FROM q; SELECT a FROM q LIMIT 2; SELECT * FROM q LIMIT 1",
	];
	// A computed field and field permissions make the body build the row, which is then encoded
	// as a value; the answer is the same either way.
	for sql in statements {
		assert_frames_answer_as_values(&db, &owner, sql).await;
		assert_frames_answer_as_values(&db, &alice, sql).await;
	}
	// A table of plain fields emits its rows untouched from a body that runs in place, so a frame
	// visits them. A paged body runs on rows the host decodes, so it answers the same from values.
	let db = TestDb::new(&dataset()).await;
	for sql in ["SELECT * FROM p", &format!("SELECT * FROM {LONG}")] {
		let (visits, read) = frames_against_values(&db, &owner, sql).await;
		assert!(visits > 0, "{sql} visits");
		assert_eq!(read, 0, "{sql}: no packed row is checked or decoded whole for the body");
	}
	assert_frames_answer_as_values(&db, &owner, "SELECT * FROM p LIMIT 2").await;
	let db = generated_db(99, 60).await;
	for sql in ["SELECT * FROM g", "SELECT * FROM g WHERE i > 0", "SELECT * FROM g ORDER BY i"] {
		assert_frames_answer_as_values(&db, &owner, sql).await;
	}
	assert!(assert_frames_answer_as_values(&db, &owner, "SELECT * FROM g").await > 0);
}

// Wide objects

/// Tables whose objects hold more entries than a one-byte CBOR map head counts: 30 optional root
/// fields (the count crossing 23 from row to row), a nested node of 25 children, a flexible node
/// whose columns and remainder pass 23 together, and a table of 300 fields (a two-byte head).
fn wide(rows: usize) -> String {
	let mut rng = Rng(0x5EED);
	let mut sql = String::from("DEFINE TABLE wa SCHEMAFULL; DEFINE TABLE wb SCHEMAFULL;");
	for f in 0..30 {
		sql.push_str(&format!("DEFINE FIELD f{f:02} ON wa TYPE option<int>;"));
	}
	sql.push_str("DEFINE FIELD n ON wa TYPE option<object>;");
	for c in 0..25 {
		sql.push_str(&format!("DEFINE FIELD n.c{c:02} ON wa TYPE int;"));
	}
	sql.push_str("DEFINE FIELD fx ON wa TYPE option<object> FLEXIBLE;");
	for a in 0..10 {
		sql.push_str(&format!("DEFINE FIELD fx.a{a:02} ON wa TYPE option<int>;"));
	}
	for f in 0..300 {
		sql.push_str(&format!("DEFINE FIELD g{f:03} ON wb TYPE option<int>;"));
	}
	for row in 0..rows {
		let mut fields = Vec::new();
		for f in 0..30 {
			if !rng.one_in(5) {
				fields.push(format!("f{f:02}: {}", rng.next() as i32));
			}
		}
		if rng.one_in(2) {
			let children: Vec<String> = (0..25).map(|c| format!("c{c:02}: {c}")).collect();
			fields.push(format!("n: {{ {} }}", children.join(", ")));
		}
		if rng.one_in(2) {
			let mut entries: Vec<String> =
				(0..10).filter(|_| !rng.one_in(3)).map(|a| format!("a{a:02}: {a}")).collect();
			let extra = rng.below(24);
			entries.extend((0..extra).map(|r| format!("r{r:02}: 'x'")));
			fields.push(format!("fx: {{ {} }}", entries.join(", ")));
		}
		sql.push_str(&format!("CREATE wa:{row} CONTENT {{ {} }};", fields.join(", ")));
		let many: Vec<String> =
			(0..300).filter(|_| !rng.one_in(10)).map(|f| format!("g{f:03}: {f}")).collect();
		sql.push_str(&format!("CREATE wb:{row} CONTENT {{ {} }};", many.join(", ")));
	}
	sql
}

#[tokio::test]
async fn a_packed_row_of_wide_objects_transcodes_as_its_whole_decode_encodes() {
	let db = TestDb::new(&wide(40)).await;
	let owner = Session::owner().with_ns("test").with_db("test");
	for table in ["wa", "wb"] {
		let rows = rows(&db, table).await;
		let layouts = layouts_of(&db, table, &rows).await;
		assert!(assert_transcodes(table, &rows, &layouts) >= 30, "{table} is packed");
		// The record maps' CBOR heads: a byte of count after the head, or two.
		let heads: std::collections::BTreeSet<u8> = rows
			.iter()
			.map(|(key, value)| {
				transcoded(RowFormat::Cbor, key, value, &layouts).expect("transcodes")
			})
			.map(|(bytes, _)| bytes[0])
			.collect();
		match table {
			"wa" => assert!(
				heads.contains(&0xb8) && heads.iter().any(|&head| head <= 0xb7),
				"wa's maps fall either side of 23 entries: {heads:02x?}"
			),
			_ => assert!(heads.contains(&0xb9), "wb's maps take a two-byte count: {heads:02x?}"),
		}
		let sql = format!("SELECT * FROM {table}");
		let (visits, read) = frames_against_values(&db, &owner, &sql).await;
		assert!(visits > 0, "{sql} visits");
		assert_eq!(read, 0, "{sql}: no packed row is checked or decoded whole");
	}
}

// Migrations

/// Packed rows behind a pending migration step are brought through it before they are written,
/// and the rows written after it are visited: a frame answers as the values do.
#[tokio::test]
async fn a_packed_row_behind_a_migration_is_migrated_before_it_is_written() {
	let db = TestDb::new(
		"
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD name ON person TYPE string;
		DEFINE FIELD age ON person TYPE string;
		CREATE person:1 SET name = 'Ann', age = '41';
		CREATE person:2 SET name = 'Bob', age = '37';
		CREATE person:3 SET name = 'Cy', age = 'old';
		ALTER FIELD age ON person TYPE int MIGRATE EXISTING ROWS AS age_to_int USING <int>$value FALLBACK 0;
		CREATE person:4 SET name = 'Di', age = 29;
		CREATE person:5 SET name = 'Ed', age = 52;
		",
	)
	.await;
	let owner = Session::owner().with_ns("test").with_db("test");
	for sql in ["SELECT * FROM person", "SELECT * FROM person WHERE age > 30"] {
		assert_frames_answer_as_values(&db, &owner, sql).await;
	}
	let answers = streamed(&db, &owner, "SELECT VALUE age FROM person", None).await;
	assert_eq!(
		format!("{answers:?}").matches("Int(").count(),
		5,
		"every age migrated: {answers:?}"
	);
}

// Paged statements

/// A packed table and its schemaless twin of the same rows.
fn twins(rows: usize) -> String {
	format!(
		"
		DEFINE TABLE pk SCHEMAFULL;
		DEFINE FIELD score ON pk TYPE int; DEFINE FIELD cat ON pk TYPE int;
		DEFINE FIELD name ON pk TYPE string; DEFINE FIELD note ON pk TYPE option<string>;
		DEFINE TABLE sk SCHEMALESS;
		FOR $i IN 0..{rows} {{
			LET $row = {{ score: ($i * 7919) % 1000, cat: $i % 13, name: 'name ' + <string> $i,
				note: IF $i % 2 = 0 THEN 'even' ELSE NONE END }};
			CREATE type::record('pk', $i) CONTENT $row;
			CREATE type::record('sk', $i) CONTENT $row;
		}};
		"
	)
}

/// The paged statements over table `t` and the statement whose rows each is a slice of, with the
/// slice: `(paged, whole, start, limit)`.
fn paged(t: &str) -> Vec<(String, String, usize, usize)> {
	let all = format!("SELECT * FROM {t}");
	let by_score = format!("SELECT * FROM {t} ORDER BY score DESC, id");
	let filtered = format!("SELECT * FROM {t} WHERE cat = 3");
	let ranged = format!("SELECT * FROM {t}:5..400");
	vec![
		(format!("{all} LIMIT 1000"), all.clone(), 0, 1000),
		(format!("{all} START 10 LIMIT 5"), all.clone(), 10, 5),
		(format!("{all} START 2990 LIMIT 50"), all.clone(), 2990, 50),
		(format!("{all} START 5000"), all.clone(), 5000, usize::MAX),
		(format!("{all} LIMIT 0"), all, 0, 0),
		(format!("{by_score} LIMIT 7"), by_score.clone(), 0, 7),
		(format!("{by_score} START 100 LIMIT 30"), by_score, 100, 30),
		(format!("{filtered} LIMIT 20"), filtered.clone(), 0, 20),
		(format!("{filtered} START 4 LIMIT 3"), filtered, 4, 3),
		(format!("{ranged} LIMIT 3"), ranged.clone(), 0, 3),
		(format!("{ranged} START 390"), ranged, 390, usize::MAX),
	]
}

/// Every paged statement over each twin answers over frames as over values, and as the slice of
/// its unpaged statement's answer: run serially and with the morsel knobs that split a scan.
#[tokio::test]
async fn a_paged_select_answers_its_slice_over_frames_and_values() {
	let owner = Session::owner().with_ns("test").with_db("test");
	let morsels = surrealdb_cnf::ConfigMap::empty()
		.with_key_value("morsel_split_rows", "1")
		.with_key_value("morsel_split_pass_ppm", "1000000")
		.with_key_value("morsel_target_bytes", "4000")
		.with_key_value("morsel_oltp_reserve", "0");
	for db in
		[TestDb::new(&twins(3000)).await, TestDb::new_with_config(&twins(3000), morsels).await]
	{
		for t in ["pk", "sk"] {
			for (sql, whole, start, limit) in paged(t) {
				frames_against_values(&db, &owner, &sql).await;
				let rows = |answers: Vec<Answer>| match answers.into_iter().next() {
					Some(Ok((rows, _))) => rows,
					other => panic!("{sql}: {other:?}"),
				};
				let whole = rows(streamed(&db, &owner, &whole, None).await);
				let slice: Vec<_> = whole.into_iter().skip(start).take(limit).collect();
				assert_eq!(rows(streamed(&db, &owner, &sql, None).await), slice, "{sql}");
				assert_eq!(
					rows(streamed(&db, &owner, &sql, Some(RowFormat::Cbor)).await),
					slice,
					"{sql} over CBOR frames"
				);
			}
		}
	}
}

/// A plain paged `SELECT *` runs its body in place: over frames, every row it emits is written
/// from its stored bytes, a packed row visited and a schemaless one transcoded in place, and no row
/// is decoded whole.
#[tokio::test]
async fn a_paged_select_writes_its_rows_without_decoding_them() {
	let db = TestDb::new(&twins(3000)).await;
	let owner = Session::owner().with_ns("test").with_db("test");
	for format in FORMATS {
		let (visits, whole) = (observed::packed_visits(), observed::whole_decodes());
		super::row_packed::observed::reset();
		streamed(&db, &owner, "SELECT * FROM pk LIMIT 1000", Some(format)).await;
		assert_eq!(
			observed::packed_visits() - visits,
			1000,
			"{format:?}: every packed row visited"
		);
		assert_eq!(
			super::row_packed::observed::whole() + super::row_packed::observed::in_place(),
			0,
			"{format:?}: no packed row checked or decoded for the body"
		);
		streamed(&db, &owner, "SELECT * FROM sk LIMIT 1000", Some(format)).await;
		assert_eq!(observed::whole_decodes(), whole, "{format:?}: no record decoded whole");
	}
	for sql in ["SELECT * FROM pk LIMIT 1000", "SELECT * FROM pk START 7 LIMIT 3"] {
		super::row_packed::observed::reset();
		streamed(&db, &owner, sql, Some(RowFormat::Cbor)).await;
		let scans = super::row_packed::observed::pipelines();
		assert!(scans.iter().any(|shape| shape.scan && shape.in_place), "{sql}: {scans:?}");
	}
}

/// Paged rows behind a pending migration step are brought through it before they are written.
#[tokio::test]
async fn a_paged_select_of_rows_behind_a_migration_migrates_them() {
	let db = TestDb::new(
		"
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD name ON person TYPE string;
		DEFINE FIELD age ON person TYPE string;
		FOR $i IN 0..40 { CREATE type::record('person', $i) SET name = 'p' + <string> $i, age = <string> $i; };
		ALTER FIELD age ON person TYPE int MIGRATE EXISTING ROWS AS age_to_int USING <int>$value FALLBACK 0;
		FOR $i IN 40..60 { CREATE type::record('person', $i) SET name = 'p' + <string> $i, age = $i; };
		",
	)
	.await;
	let owner = Session::owner().with_ns("test").with_db("test");
	for sql in [
		"SELECT * FROM person LIMIT 10",
		"SELECT * FROM person START 35 LIMIT 10",
		"SELECT * FROM person WHERE age > 20 LIMIT 5",
	] {
		frames_against_values(&db, &owner, sql).await;
		let answers = streamed(&db, &owner, sql, Some(RowFormat::Cbor)).await;
		assert!(!format!("{answers:?}").contains("age\": String"), "{sql}: migrated: {answers:?}");
	}
}

// Escapes

/// A packed row of many datetimes and durations that live in the trailer transcodes byte for byte
/// as its whole decode encodes, in CBOR and JSON.
#[tokio::test]
async fn a_packed_row_of_many_escapes_transcodes_as_its_whole_decode_encodes() {
	let mut sql = String::from("DEFINE TABLE esc SCHEMAFULL;");
	for i in 0..60 {
		let kind = if i % 2 == 0 {
			"datetime"
		} else {
			"duration"
		};
		sql.push_str(&format!("DEFINE FIELD e{i:02} ON esc TYPE option<{kind}>;"));
	}
	for row in 0..20 {
		let fields: Vec<String> = (0..60)
			.filter(|i| (i + row) % 7 != 0)
			.map(|i| match i % 2 {
				0 if (i + row) % 3 == 0 => format!("e{i:02}: d'1500-01-01T00:00:0{}Z'", i % 10),
				0 => format!("e{i:02}: d'2600-01-01T00:00:0{}.123Z'", i % 10),
				_ => format!("e{i:02}: {}y", 600 + i),
			})
			.collect();
		sql.push_str(&format!("CREATE esc:{row} CONTENT {{ {} }};", fields.join(", ")));
	}
	let db = TestDb::new(&sql).await;
	let rows = rows(&db, "esc").await;
	let layouts = layouts_of(&db, "esc", &rows).await;
	assert_eq!(assert_transcodes("esc", &rows, &layouts), 20, "every row is packed");
}
