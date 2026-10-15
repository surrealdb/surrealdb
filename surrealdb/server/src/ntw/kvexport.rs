//! `POST /kvexport`: the open datastore's keyspace, streamed from one snapshot.
//!
//! This process is the source. It serves its keyspace and does not pull another.
//! The target writes what it receives into its own datastore under the rules of
//! [`surrealdb_core::kvs::copy`].
//!
//! # Enabling
//!
//! The route is off unless the operator enables the experimental `kvexport`
//! capability (`--allow-experimental kvexport`). It also has an HTTP route target
//! of its own, so `--deny-http kvexport` turns it off without touching `/export`.
//! The caller must be the root user with the owner role.
//!
//! SECURITY: the frames are the stored bytes, so they carry everything a SurrealQL
//! export redacts: password hashes, each user's token-signing key and the keys of
//! access methods. A reader of them can mint tokens for this instance that outlive
//! a password change. That is why the route is opt-in, refuses every role below
//! owner, reads no request body until the caller is authorised, and never logs a
//! request or response body.
//!
//! # Request
//!
//! A JSON object of at most [`MAX_REQUEST_BYTES`]. Every field is optional, and
//! an empty body asks for the whole keyspace:
//!
//! - `after`: the hex of a key. The stream starts at the first key strictly after it. A caller
//!   resuming a copy passes the last key it wrote in key order.
//! - `latest_only`: `true` to copy a datastore that keeps version history. The stream carries each
//!   key's latest value only, so without it such a datastore is refused with `409` rather than
//!   copied without its history.
//!
//! # Response
//!
//! A [`CONTENT_TYPE`] body, streamed and never compressed:
//!
//! 1. The four bytes `SKVX` and a format byte, `1`.
//! 2. A `u32` length and that many bytes of JSON: the source's
//!    [`KeyspaceVersion`](surrealdb_core::kvs::copy::KeyspaceVersion), read in the same snapshot as
//!    the frames. A target checks it before writing anything.
//! 3. Frames, each the byte `1`, a `u32` key length, the key, a `u32` value length and the value.
//! 4. The byte `0`, then the 32-byte BLAKE3 digest of every byte before it.
//!
//! Lengths are little-endian. A body that stops before the digest, or whose
//! digest does not match, was cut short.
//!
//! # Order and scope
//!
//! Every frame is read in one transaction, so the stream is one snapshot of the
//! keyspace, whatever the source's own background work commits while it is
//! served. The source's storage keeps every version that snapshot can read for
//! as long as the stream runs. Frames come in key order, with one exception: the version key comes
//! last, after every other frame, and the bootstrap sentinel never comes at all
//! (see [`surrealdb_core::kvs::copy::KeyRole`]). A target writes the frames in the
//! order they arrive and the version key only once the digest has verified, so a
//! copy cut short is refused by the target's version gate rather than served.
//!
//! The stream covers [`surrealdb_core::kvs::copy::keyspace`], from `0x00` up to but
//! excluding `0xff`. A composer keeps state of its own at or above `0xff`, which
//! this route never serves.
//!
//! A caller that loses the connection resumes with `after`. The resumed stream is
//! a new snapshot, so a copy assembled from more than one response is not one cut
//! of the source; a caller that needs one starts again from an empty target.

use std::ops::ControlFlow;

use anyhow::{Result, anyhow};
use axum::Router;
use axum::body::Body;
use axum::extract::Extension;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::{BufMut, Bytes, BytesMut};
use serde::Deserialize;
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::{ExperimentalTarget, RouteTarget};
use surrealdb_core::iam::Role;
use surrealdb_core::idx::planner::ScanDirection;
use surrealdb_core::kvs::copy::{self, KeyRole, KeyspaceVersion};
use surrealdb_core::kvs::{Datastore, LockType, Transaction, TransactionType};

use super::AppState;
use super::error::ResponseError;
use crate::ntw::error::Error as NetError;

/// The media type of a response. The compression layer leaves it alone: the
/// stored bytes are mostly compact already, and deflating them would cost the
/// source more CPU than the transfer saves.
pub(crate) const CONTENT_TYPE: &str = "application/vnd.surrealdb.kvexport";

/// The bytes that open a response.
pub(crate) const MAGIC: &[u8; 4] = b"SKVX";

/// The format byte that follows [`MAGIC`].
pub(crate) const FORMAT: u8 = 1;

/// Opens a frame.
pub(crate) const TAG_FRAME: u8 = 1;

/// Ends the frames; the digest follows.
pub(crate) const TAG_END: u8 = 0;

/// The length of the digest that closes a response.
pub(crate) const DIGEST_LEN: usize = 32;

/// The most request bytes the route reads, and only once the caller is
/// authorised. Sized for the hex of a cursor key of up to 64 MiB.
const MAX_REQUEST_BYTES: usize = 128 * 1024 * 1024 + 1024;

/// How many bytes of frames are buffered before they are handed to the
/// connection. A frame is never split, so a chunk holding one large value is
/// that value's size.
const CHUNK_BYTES: usize = 1024 * 1024;

/// Rows one cursor call may visit. The visitor ends a call early once a chunk
/// is full, and the cursor keeps its position between calls. A backend without
/// a cursor of its own reads a page of this many rows before visiting any of
/// them, so this also bounds the rows such a backend holds at once.
const SCAN_ROWS: u32 = 128;

/// Chunks the connection may lag behind the scan. Together with
/// [`CHUNK_BYTES`] this bounds what a response holds in memory.
const CHANNEL_DEPTH: usize = 2;

/// Body of `POST /kvexport`.
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportRequest {
	#[serde(default)]
	after: Option<String>,
	#[serde(default)]
	latest_only: bool,
}

pub fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/kvexport", post(handler))
}

/// Who may read the stored bytes: the root user with the owner role.
fn authorise(session: &Session) -> Result<(), StatusCode> {
	if session.au.is_anon() {
		Err(StatusCode::UNAUTHORIZED)
	} else if session.au.is_root() && session.au.has_role(Role::Owner) {
		Ok(())
	} else {
		Err(StatusCode::FORBIDDEN)
	}
}

/// Whether this process serves the route at all.
fn enabled(datastore: &Datastore) -> bool {
	datastore.get_capabilities().allows_experimental(&ExperimentalTarget::KvExport)
		&& datastore.allows_http_route(&RouteTarget::KvExport)
}

async fn handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	body: Body,
) -> Result<Response, ResponseError> {
	let db = &state.datastore;
	if !enabled(db) {
		warn!(
			"Capabilities denied HTTP route request attempt, target: '{}'",
			&RouteTarget::KvExport
		);
		return Err(NetError::ForbiddenRoute(RouteTarget::KvExport.to_string()).into());
	}
	if let Err(status) = authorise(&session) {
		return Ok(status.into_response());
	}
	let Ok(body) = axum::body::to_bytes(body, MAX_REQUEST_BYTES).await else {
		return Ok((StatusCode::PAYLOAD_TOO_LARGE, "the request body is too large").into_response());
	};
	let request = if body.is_empty() {
		ExportRequest::default()
	} else {
		match serde_json::from_slice::<ExportRequest>(&body) {
			Ok(request) => request,
			Err(err) => return Ok((StatusCode::BAD_REQUEST, err.to_string()).into_response()),
		}
	};
	let after = match request.after.as_deref().map(decode_hex).transpose() {
		Ok(after) => after,
		Err(err) => return Ok((StatusCode::BAD_REQUEST, err.to_string()).into_response()),
	};
	match open(db, after, request.latest_only).await {
		Ok(Opened::Stream(body)) => {
			Ok(([(axum::http::header::CONTENT_TYPE, CONTENT_TYPE)], body).into_response())
		}
		Ok(Opened::KeepsHistory) => Ok((
			StatusCode::CONFLICT,
			"this datastore keeps version history, which the stream does not carry; \
			 send `latest_only: true` to copy each key's latest value",
		)
			.into_response()),
		Err(err) => {
			warn!(error = %err, "kvexport could not start");
			Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response())
		}
	}
}

/// What a request opens.
pub(crate) enum Opened {
	/// The response body, streaming.
	Stream(Body),
	/// The datastore keeps history and the caller did not accept losing it.
	KeepsHistory,
}

/// Opens the snapshot the response is read from and starts streaming it.
pub(crate) async fn open(
	datastore: &Datastore,
	after: Option<Vec<u8>>,
	latest_only: bool,
) -> Result<Opened> {
	if !latest_only && copy::keeps_history(datastore).await? {
		return Ok(Opened::KeepsHistory);
	}
	let tx = datastore.transaction(TransactionType::Read, LockType::Optimistic).await?;
	let version = match KeyspaceVersion::read(&tx).await {
		Ok(version) => version,
		Err(err) => {
			let _ = tx.cancel().await;
			return Err(err);
		}
	};
	let (sender, receiver) = surrealdb::channel::bounded::<Result<Bytes>>(CHANNEL_DEPTH);
	tokio::spawn(async move {
		let mut out = Writer::new(sender);
		let streamed = stream(&tx, after, &version, &mut out).await;
		if let Err(err) = tx.cancel().await {
			warn!(error = %err, "kvexport could not close its transaction");
		}
		if let Err(err) = streamed {
			// The connection ends without the digest, which is how the caller
			// learns the stream was cut short.
			if !out.sender.is_closed() {
				warn!(error = %err, "kvexport stream failed");
				let _ = out.sender.send(Err(err)).await;
			}
		}
	});
	Ok(Opened::Stream(Body::from_stream(receiver)))
}

/// Writes the whole response for the snapshot `tx` reads.
async fn stream(
	tx: &Transaction,
	after: Option<Vec<u8>>,
	version: &KeyspaceVersion,
	out: &mut Writer,
) -> Result<()> {
	out.header(version)?;
	out.flush().await?;
	let mut range = copy::keyspace();
	let scanned = match after {
		// Nothing orders after a key at or past the end of the copied range.
		Some(key) if key >= range.end => false,
		Some(mut key) => {
			// The smallest key that orders after `key`
			key.push(0x00);
			range.start = key;
			true
		}
		None => true,
	};
	if scanned {
		let mut cursor = tx.open_vals_cursor(range, ScanDirection::Forward, 0, None).await?;
		loop {
			let mut visited = 0u32;
			let mut full = false;
			let mut failed = None;
			let mut visit =
				|key: &[u8], value: &[u8]| -> surrealdb_core::kvs::Result<ControlFlow<()>> {
					visited += 1;
					if copy::key_role(key) != KeyRole::Data {
						return Ok(ControlFlow::Continue(()));
					}
					// A row the visitor breaks on is consumed, so a frame is always
					// written before the visitor stops.
					if let Err(err) = out.frame(key, value) {
						failed = Some(err);
						return Ok(ControlFlow::Break(()));
					}
					if out.is_full() {
						full = true;
						return Ok(ControlFlow::Break(()));
					}
					Ok(ControlFlow::Continue(()))
				};
			cursor.for_each(SCAN_ROWS, &mut visit).await?;
			if let Some(err) = failed {
				return Err(err);
			}
			if full {
				out.flush().await?;
			} else if visited == 0 {
				break;
			}
		}
	}
	if let Some(value) = tx.get_raw(&copy::VERSION_KEY.to_vec(), None).await? {
		out.frame(copy::VERSION_KEY, &value)?;
	}
	out.finish().await
}

/// The response as it is encoded: a buffer that is hashed and handed to the
/// connection one chunk at a time.
struct Writer {
	sender: surrealdb::channel::Sender<Result<Bytes>>,
	buffer: BytesMut,
	hasher: blake3::Hasher,
}

impl Writer {
	fn new(sender: surrealdb::channel::Sender<Result<Bytes>>) -> Self {
		Self {
			sender,
			buffer: BytesMut::new(),
			hasher: blake3::Hasher::new(),
		}
	}

	/// Appends the magic, the format byte and the keyspace description.
	fn header(&mut self, version: &KeyspaceVersion) -> Result<()> {
		let json = serde_json::to_vec(version)?;
		self.buffer.put_slice(MAGIC);
		self.buffer.put_u8(FORMAT);
		self.buffer.put_u32_le(u32::try_from(json.len())?);
		self.buffer.put_slice(&json);
		Ok(())
	}

	/// Appends one frame.
	///
	/// # Errors
	///
	/// Fails when the key or the value is longer than a `u32` length can state.
	/// Truncating the length would hand the caller a frame that decodes into the
	/// wrong bytes.
	fn frame(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
		let key_len = u32::try_from(key.len())?;
		let value_len = u32::try_from(value.len())?;
		self.buffer.reserve(9 + key.len() + value.len());
		self.buffer.put_u8(TAG_FRAME);
		self.buffer.put_u32_le(key_len);
		self.buffer.put_slice(key);
		self.buffer.put_u32_le(value_len);
		self.buffer.put_slice(value);
		Ok(())
	}

	fn is_full(&self) -> bool {
		self.buffer.len() >= CHUNK_BYTES
	}

	/// Hashes the buffered bytes and hands them to the connection.
	async fn flush(&mut self) -> Result<()> {
		if self.buffer.is_empty() {
			return Ok(());
		}
		let chunk = self.buffer.split().freeze();
		self.hasher.update(&chunk);
		self.send(chunk).await
	}

	/// Ends the frames and sends the digest of everything before it.
	async fn finish(&mut self) -> Result<()> {
		self.buffer.put_u8(TAG_END);
		self.flush().await?;
		let digest: [u8; DIGEST_LEN] = *self.hasher.finalize().as_bytes();
		self.send(Bytes::copy_from_slice(&digest)).await
	}

	async fn send(&self, chunk: Bytes) -> Result<()> {
		self.sender.send(Ok(chunk)).await.map_err(|_| anyhow!("the client closed the connection"))
	}
}

fn decode_hex(text: &str) -> Result<Vec<u8>> {
	if !text.len().is_multiple_of(2) {
		anyhow::bail!("hex length is odd");
	}
	let bytes = text.as_bytes();
	let mut out = Vec::with_capacity(bytes.len() / 2);
	for pair in bytes.chunks_exact(2) {
		out.push((hex_value(pair[0])? << 4) | hex_value(pair[1])?);
	}
	Ok(out)
}

fn hex_value(byte: u8) -> Result<u8> {
	match byte {
		b'0'..=b'9' => Ok(byte - b'0'),
		b'a'..=b'f' => Ok(byte - b'a' + 10),
		b'A'..=b'F' => Ok(byte - b'A' + 10),
		_ => anyhow::bail!("not a hex digit"),
	}
}

/// A reader for the response format, for the tests in this crate that play the
/// part of a target. It accepts a body only if every byte of it is accounted
/// for: the digest matches everything before it, the header and every frame are
/// whole, the end tag is present, and nothing follows it.
#[cfg(test)]
pub(crate) mod wire {
	use anyhow::{Result, bail, ensure};
	use surrealdb_core::kvs::copy::KeyspaceVersion;

	use super::{DIGEST_LEN, FORMAT, MAGIC, TAG_END, TAG_FRAME};

	/// A whole response, taken apart and checked against its digest.
	pub(crate) struct Response {
		pub(crate) version: KeyspaceVersion,
		pub(crate) frames: Vec<(Vec<u8>, Vec<u8>)>,
	}

	pub(crate) fn decode(bytes: &[u8]) -> Result<Response> {
		ensure!(bytes.len() > DIGEST_LEN, "the body is shorter than a digest");
		let (body, digest) = bytes.split_at(bytes.len() - DIGEST_LEN);
		ensure!(blake3::hash(body).as_bytes() == digest, "the digest does not match the body");
		let Some(rest) = body.strip_prefix(MAGIC.as_slice()) else {
			bail!("the body does not open with the magic bytes");
		};
		let Some((&format, rest)) = rest.split_first() else {
			bail!("the format byte is missing");
		};
		ensure!(format == FORMAT, "unknown format {format}");
		let (header, mut rest) = take(rest)?;
		let version = serde_json::from_slice(header)?;
		let mut frames = Vec::new();
		loop {
			let Some((&tag, next)) = rest.split_first() else {
				bail!("the frames end without an end tag");
			};
			match tag {
				TAG_END => {
					ensure!(next.is_empty(), "bytes follow the end tag");
					break;
				}
				TAG_FRAME => {
					let (key, next) = take(next)?;
					let (value, next) = take(next)?;
					frames.push((key.to_vec(), value.to_vec()));
					rest = next;
				}
				other => bail!("unknown tag {other}"),
			}
		}
		Ok(Response {
			version,
			frames,
		})
	}

	fn take(input: &[u8]) -> Result<(&[u8], &[u8])> {
		let Some((len, rest)) = input.split_first_chunk::<4>() else {
			bail!("a length is truncated");
		};
		let len = u32::from_le_bytes(*len) as usize;
		ensure!(rest.len() >= len, "a field is truncated");
		Ok(rest.split_at(len))
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::sync::atomic::{AtomicBool, Ordering};

	use axum::http::Request;
	use futures::StreamExt;
	use surrealdb_core::dbs::Session;
	use surrealdb_core::dbs::capabilities::{Capabilities, Targets};
	use surrealdb_core::kvs::Transactor;
	use surrealdb_core::kvs::copy::VERSION_KEY;
	use tower::ServiceExt;

	use super::*;
	use crate::ntw::Readiness;
	use crate::ntw::client_ip::ClientIp;

	/// Runs a request against `ds` the way the handler does once the caller is
	/// authorised, and collects the body.
	async fn request(
		ds: &Datastore,
		after: Option<Vec<u8>>,
		latest_only: bool,
	) -> Result<Option<Vec<u8>>> {
		let Opened::Stream(body) = open(ds, after, latest_only).await? else {
			return Ok(None);
		};
		Ok(Some(collect(body).await?))
	}

	async fn collect(body: Body) -> Result<Vec<u8>> {
		let mut stream = body.into_data_stream();
		let mut bytes = Vec::new();
		while let Some(chunk) = stream.next().await {
			bytes.extend_from_slice(&chunk.map_err(|err| anyhow!("{err}"))?);
		}
		Ok(bytes)
	}

	async fn pull(ds: &Datastore, after: Option<Vec<u8>>) -> Result<wire::Response> {
		let body = request(ds, after, false).await?.expect("a memory store keeps no history");
		wire::decode(&body)
	}

	/// Writes raw keys and values in one transaction.
	async fn write(ds: &Datastore, rows: &[(&[u8], Vec<u8>)]) -> Result<()> {
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		set_all(&tx, rows.iter().map(|(key, value)| (key.to_vec(), value.clone()))).await?;
		tx.commit().await?;
		Ok(())
	}

	/// Writes raw keys and values in `tx`, outside any key type.
	async fn set_all(
		tx: &Transaction,
		rows: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
	) -> Result<()> {
		let raw: &Transactor = tx;
		for (key, value) in rows {
			raw.set(key, value).await?;
		}
		Ok(())
	}

	/// A memory datastore holding real catalog and record keys, a value larger
	/// than one chunk, and the bootstrap sentinel a store can carry as residue.
	async fn seeded() -> Result<Datastore> {
		let ds = Datastore::new("memory").await?;
		ds.check_version().await?;
		let session = Session::owner().with_ns("t").with_db("t");
		let mut sql = String::from("DEFINE NAMESPACE t; DEFINE DATABASE t; DEFINE TABLE item;");
		for n in 0..40 {
			sql.push_str(&format!("CREATE item:{n} SET n = {n}, text = 'row {n}';"));
		}
		sql.push_str(&format!("CREATE item:big SET text = '{}';", "x".repeat(CHUNK_BYTES + 10)));
		for result in ds.execute(&sql, &session, None).await? {
			result.result.map_err(|err| anyhow!("seeding failed: {err}"))?;
		}
		// The sentinel a later release leaves as residue, holding the major version
		// as `!v` does. This release never writes it.
		write(&ds, &[(copy::BOOTSTRAP_KEY, vec![0, 3])]).await?;
		Ok(ds)
	}

	/// Every key and value the store holds below `0xff`, read in one scan.
	async fn whole_keyspace(ds: &Datastore) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let all = tx.scan(copy::keyspace(), u32::MAX, 0, None).await;
		tx.cancel().await?;
		all
	}

	/// What a copy must carry: every key but the sentinel, with the version key
	/// moved to the end.
	fn copied(mut keyspace: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<(Vec<u8>, Vec<u8>)> {
		let at = keyspace.iter().position(|(key, _)| key == VERSION_KEY);
		let version = at.map(|at| keyspace.remove(at));
		keyspace.retain(|(key, _)| key != copy::BOOTSTRAP_KEY);
		keyspace.extend(version);
		keyspace
	}

	#[tokio::test]
	async fn a_stream_carries_the_keyspace_with_the_version_key_last() -> Result<()> {
		let ds = seeded().await?;
		let keyspace = whole_keyspace(&ds).await?;
		assert!(keyspace.len() > 40, "the seed did not write a keyspace worth streaming");
		assert!(keyspace.iter().any(|(key, _)| key == copy::BOOTSTRAP_KEY));
		assert!(keyspace.iter().any(|(_, value)| value.len() > CHUNK_BYTES));
		let outside_the_root: Vec<&[u8]> = keyspace
			.iter()
			.map(|(key, _)| key.as_slice())
			.take_while(|key| !key.starts_with(b"/"))
			.collect();
		assert!(
			outside_the_root.contains(&copy::BOOTSTRAP_KEY)
				&& outside_the_root.contains(&VERSION_KEY),
			"the sentinel and the version key open the keyspace, so a stream in key order would \
			 send them first"
		);

		let response = pull(&ds, None).await?;
		assert_eq!(response.frames, copied(keyspace));
		assert_eq!(response.frames.last().map(|(key, _)| key.as_slice()), Some(VERSION_KEY));
		assert_eq!(
			response.frames.iter().filter(|(key, _)| key == VERSION_KEY).count(),
			1,
			"the version key was sent more than once"
		);
		assert!(response.frames.iter().all(|(key, _)| key != copy::BOOTSTRAP_KEY));
		response.version.check()?;
		Ok(())
	}

	#[tokio::test]
	async fn a_resumed_stream_carries_what_follows_the_cursor_and_the_version_key() -> Result<()> {
		let ds = seeded().await?;
		let expected = copied(whole_keyspace(&ds).await?);
		let (ordered, version) = expected.split_at(expected.len() - 1);
		for cut in [0, 1, ordered.len() / 2, ordered.len() - 1] {
			let after = ordered[cut].0.clone();
			let response = pull(&ds, Some(after)).await?;
			let mut resumed = ordered[cut + 1..].to_vec();
			resumed.extend_from_slice(version);
			assert_eq!(response.frames, resumed, "resumed after frame {cut}");
		}
		Ok(())
	}

	/// A cursor names a position, not a stored key: one that no row carries still
	/// resumes at the next key in order, and the version key is not skipped when
	/// the cursor stands on or before it.
	#[tokio::test]
	async fn a_cursor_between_keys_resumes_at_the_next_key() -> Result<()> {
		let ds = seeded().await?;
		let expected = copied(whole_keyspace(&ds).await?);
		let (ordered, version) = expected.split_at(expected.len() - 1);
		let mut after = ordered[3].0.clone();
		after.extend_from_slice(&[0x00; 7]);
		let mut resumed = ordered[4..].to_vec();
		resumed.extend_from_slice(version);
		assert_eq!(pull(&ds, Some(after)).await?.frames, resumed);
		for after in [VERSION_KEY.to_vec(), copy::BOOTSTRAP_KEY.to_vec(), vec![0x00]] {
			let response = pull(&ds, Some(after.clone())).await?;
			assert_eq!(
				response.frames.last().map(|(key, _)| key.as_slice()),
				Some(VERSION_KEY),
				"after {after:?}"
			);
		}
		Ok(())
	}

	#[tokio::test]
	async fn a_cursor_past_the_copied_range_yields_only_the_version_key() -> Result<()> {
		let ds = seeded().await?;
		for after in [vec![0xff], vec![0xff, 0x01]] {
			let response = pull(&ds, Some(after)).await?;
			let keys: Vec<&[u8]> = response.frames.iter().map(|(key, _)| key.as_slice()).collect();
			assert_eq!(keys, [VERSION_KEY]);
		}
		Ok(())
	}

	/// State a composer keeps at or above `0xff` is stored but never served, so it
	/// cannot be exported into a second copy.
	#[tokio::test]
	async fn keys_at_or_above_the_ceiling_are_stored_and_never_served() -> Result<()> {
		let ds = seeded().await?;
		let expected = copied(whole_keyspace(&ds).await?);
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let state = b"state".to_vec();
		set_all(
			&tx,
			[&b"\xffmigrate/done"[..], &[0xff][..], &[0xff, 0xff][..]]
				.map(|key| (key.to_vec(), state.clone())),
		)
		.await?;
		tx.commit().await?;
		let response = pull(&ds, None).await?;
		assert_eq!(response.frames, expected);
		Ok(())
	}

	/// The stream reads one snapshot: a write committed after it opened is not in
	/// it, wherever in key order the write lands, and neither is an overwrite of a
	/// value the stream has yet to reach. The source holds more than the response
	/// buffers, so the stream is still being read when the writes commit.
	#[tokio::test]
	async fn a_write_after_the_stream_opens_is_not_in_it() -> Result<()> {
		let ds = seeded().await?;
		let big = vec![b'y'; CHUNK_BYTES + 10];
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		set_all(&tx, (0..6).map(|n| (format!("/zz-big-{n}").into_bytes(), big.clone()))).await?;
		tx.commit().await?;
		let expected = copied(whole_keyspace(&ds).await?);

		let Opened::Stream(body) = open(&ds, None, false).await? else {
			panic!("a memory store keeps no history");
		};
		write(
			&ds,
			&[
				(b"/zz-late", b"late".to_vec()),
				(b"/zz-big-5", b"overwritten".to_vec()),
				(VERSION_KEY, vec![0, 9]),
			],
		)
		.await?;
		assert_eq!(wire::decode(&collect(body).await?)?.frames, expected);
		Ok(())
	}

	/// A keyspace of many more rows than one cursor call visits, in values small
	/// enough that no chunk fills until the end, is carried whole and in order,
	/// from the start and from a cursor inside it.
	#[tokio::test]
	async fn a_keyspace_larger_than_a_scan_page_is_carried_whole() -> Result<()> {
		let ds = seeded().await?;
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let rows = (0..4 * SCAN_ROWS as usize + 17)
			.map(|n| (format!("/zz-row-{n:05}").into_bytes(), format!("value {n}").into_bytes()));
		set_all(&tx, rows).await?;
		tx.commit().await?;
		let expected = copied(whole_keyspace(&ds).await?);
		assert!(expected.len() > 4 * SCAN_ROWS as usize);
		assert_eq!(pull(&ds, None).await?.frames, expected);
		let (ordered, version) = expected.split_at(expected.len() - 1);
		for cut in [SCAN_ROWS as usize - 1, SCAN_ROWS as usize, 2 * SCAN_ROWS as usize + 1] {
			let mut resumed = ordered[cut + 1..].to_vec();
			resumed.extend_from_slice(version);
			let response = pull(&ds, Some(ordered[cut].0.clone())).await?;
			assert_eq!(response.frames, resumed, "resumed after frame {cut}");
		}
		Ok(())
	}

	/// A response is cut into chunks of about [`CHUNK_BYTES`], so what it holds
	/// in memory is bounded by the chunk size and not by the keyspace.
	#[tokio::test]
	async fn a_response_is_cut_into_bounded_chunks() -> Result<()> {
		let ds = seeded().await?;
		let big = vec![b'y'; CHUNK_BYTES + 10];
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		set_all(&tx, (0..12).map(|n| (format!("/zz-big-{n:02}").into_bytes(), big.clone())))
			.await?;
		tx.commit().await?;
		let Opened::Stream(body) = open(&ds, None, false).await? else {
			panic!("a memory store keeps no history");
		};
		let mut stream = body.into_data_stream();
		let mut chunks = 0;
		let mut bytes = Vec::new();
		while let Some(chunk) = stream.next().await {
			let chunk = chunk.map_err(|err| anyhow!("{err}"))?;
			assert!(
				chunk.len() < CHUNK_BYTES + big.len() + 64,
				"a chunk held {} bytes",
				chunk.len()
			);
			bytes.extend_from_slice(&chunk);
			chunks += 1;
		}
		assert!(chunks > 12, "the response came in {chunks} chunks");
		assert_eq!(wire::decode(&bytes)?.frames.len(), copied(whole_keyspace(&ds).await?).len());
		Ok(())
	}

	/// A reader that stops reading ends the scan and releases its transaction.
	#[tokio::test]
	async fn a_dropped_stream_releases_its_transaction() -> Result<()> {
		let ds = seeded().await?;
		let Opened::Stream(body) = open(&ds, None, false).await? else {
			panic!("a memory store keeps no history");
		};
		let mut stream = body.into_data_stream();
		assert!(stream.next().await.is_some());
		drop(stream);
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		// The datastore still answers, and serves the whole keyspace again
		assert_eq!(pull(&ds, None).await?.frames, copied(whole_keyspace(&ds).await?));
		Ok(())
	}

	#[tokio::test]
	async fn a_body_cut_short_or_changed_does_not_verify() -> Result<()> {
		let ds = seeded().await?;
		let body = request(&ds, None, false).await?.unwrap();
		wire::decode(&body)?;
		assert!(wire::decode(&body[..body.len() - 1]).is_err(), "a truncated body verified");
		assert!(
			wire::decode(&body[..body.len() - DIGEST_LEN - 1]).is_err(),
			"a body without its end tag verified"
		);
		let mut changed = body;
		changed[MAGIC.len() + 10] ^= 0x01;
		assert!(wire::decode(&changed).is_err(), "a changed byte verified");
		Ok(())
	}

	/// A reader of the format accepts a body only when all of it is there and
	/// unchanged. Every proper prefix of a response is refused, and so is every
	/// response with a byte changed, among them the end tag the digest follows.
	#[tokio::test]
	async fn a_truncated_or_changed_body_is_not_accepted() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		ds.check_version().await?;
		write(&ds, &[(b"/a", b"one".to_vec()), (b"/b", vec![0xff, 0x00, 0xff])]).await?;
		let body = request(&ds, None, false).await?.expect("a memory store keeps no history");
		assert!(wire::decode(&body)?.frames.len() >= 3);
		for len in 0..body.len() {
			assert!(wire::decode(&body[..len]).is_err(), "a body cut to {len} bytes verified");
		}
		for at in 0..body.len() {
			let mut changed = body.clone();
			changed[at] ^= 0x01;
			assert!(wire::decode(&changed).is_err(), "a body with byte {at} changed verified");
		}
		let mut extended = body;
		extended.push(0);
		assert!(wire::decode(&extended).is_err(), "a body with a byte added verified");
		Ok(())
	}

	/// The digest is the BLAKE3 of the framed bytes exactly as sent, so the byte
	/// that ends the frames is covered and a frame boundary cannot be moved.
	#[tokio::test]
	async fn the_digest_covers_the_framed_bytes() -> Result<()> {
		let ds = seeded().await?;
		let body = request(&ds, None, false).await?.expect("a memory store keeps no history");
		let (framed, digest) = body.split_at(body.len() - DIGEST_LEN);
		assert_eq!(blake3::hash(framed).as_bytes(), digest);
		assert_eq!(framed.last(), Some(&TAG_END), "the end tag is the last byte hashed");
		assert!(framed.starts_with(MAGIC));
		assert_eq!(framed[MAGIC.len()], FORMAT);
		// The same bytes with the end tag altered are a different digest
		let mut moved = framed.to_vec();
		let tag = moved.len() - 1;
		moved[tag] = TAG_FRAME;
		assert_ne!(blake3::hash(&moved).as_bytes(), digest);
		Ok(())
	}

	/// The header describes the source in the snapshot the frames come from, so a
	/// target can refuse a source it could not start from before writing a key.
	///
	/// This release applies no data migrations and keeps no ledger, so it has no
	/// row for any ledger entry and [`KeyspaceVersion::check`] judges the major
	/// version alone. The ledger a later release wrote is named in the header for
	/// the target that owns migrations to judge.
	#[tokio::test]
	async fn the_header_names_a_migration_this_build_lacks() -> Result<()> {
		let ds = seeded().await?;
		pull(&ds, None).await?.version.check()?;
		let mut ledger_key = b"/!mg".to_vec();
		ledger_key.extend_from_slice(&u32::MAX.to_be_bytes());
		write(&ds, &[(&ledger_key, Vec::new())]).await?;
		let response = pull(&ds, None).await?;
		assert_eq!(response.version.migrations, [u32::MAX]);
		response.version.check()?;
		// A ledger entry is a key like any other, so it is also carried
		assert!(response.frames.iter().any(|(key, _)| *key == ledger_key));
		Ok(())
	}

	/// The header is the JSON of the keyspace description, length-prefixed right
	/// after the format byte, with the first frame behind it.
	#[tokio::test]
	async fn the_header_is_length_prefixed_json() -> Result<()> {
		let ds = seeded().await?;
		let body = request(&ds, None, false).await?.expect("a memory store keeps no history");
		let described = wire::decode(&body)?.version;
		let rest = &body[MAGIC.len() + 1..];
		let len = u32::from_le_bytes(rest[..4].try_into()?) as usize;
		assert_eq!(&rest[4..4 + len], serde_json::to_vec(&described)?.as_slice());
		assert_eq!(rest[4 + len], TAG_FRAME, "the first frame follows the header");
		Ok(())
	}

	#[tokio::test]
	async fn an_empty_store_streams_a_header_and_no_frames() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let response = pull(&ds, None).await?;
		assert!(response.frames.is_empty());
		assert_eq!(response.version, KeyspaceVersion::default());
		Ok(())
	}

	#[test]
	fn hex_cursors_decode_in_either_case_and_bad_ones_are_refused() {
		assert_eq!(decode_hex("00ff2Ab1").unwrap(), [0x00, 0xff, 0x2a, 0xb1]);
		assert_eq!(decode_hex("").unwrap(), Vec::<u8>::new());
		assert!(decode_hex("abc").is_err());
		assert!(decode_hex("zz").is_err());
		assert!(decode_hex("0g").is_err());
	}

	#[test]
	fn only_the_root_owner_may_read_the_stored_bytes() {
		assert_eq!(authorise(&Session::owner()), Ok(()));
		assert_eq!(authorise(&Session::editor()), Err(StatusCode::FORBIDDEN));
		assert_eq!(authorise(&Session::viewer()), Err(StatusCode::FORBIDDEN));
		assert_eq!(authorise(&Session::default()), Err(StatusCode::UNAUTHORIZED));
	}

	async fn datastore_with(caps: Capabilities) -> Result<Arc<Datastore>> {
		let ds = Datastore::builder().with_capabilities(caps).build_with_path("memory").await?;
		ds.check_version().await?;
		Ok(Arc::new(ds))
	}

	fn kvexport_allowed() -> Capabilities {
		Capabilities::default()
			.with_experimental(Targets::Some([ExperimentalTarget::KvExport].into()))
	}

	/// Whether a datastore opened with `caps` serves the route.
	async fn enabled_with(caps: Capabilities) -> Result<bool> {
		let ds = datastore_with(caps).await?;
		Ok(enabled(&ds))
	}

	#[tokio::test]
	async fn the_route_is_off_unless_its_capability_is_enabled() -> Result<()> {
		assert!(
			!enabled_with(Capabilities::default()).await?,
			"the route answered with the default capabilities"
		);
		assert!(enabled_with(kvexport_allowed()).await?);
		let denied =
			kvexport_allowed().without_http_routes(Targets::Some([RouteTarget::KvExport].into()));
		assert!(
			!enabled_with(denied).await?,
			"denying the route target did not turn the route off"
		);
		Ok(())
	}

	/// Nothing but the capability for this route turns it on, and nothing but its
	/// own route target turns it off.
	#[tokio::test]
	async fn no_other_capability_turns_the_route_on_or_off() -> Result<()> {
		assert!(!enabled_with(Capabilities::all()).await?, "`all` enabled the route");
		let other = Capabilities::default()
			.with_experimental(Targets::Some([ExperimentalTarget::Files].into()));
		assert!(!enabled_with(other).await?, "another experimental target enabled it");
		let denied_experimental = kvexport_allowed()
			.without_experimental(Targets::Some([ExperimentalTarget::KvExport].into()));
		assert!(!enabled_with(denied_experimental).await?, "denying the capability left it on");
		// Denying the SurrealQL export does not deny this route
		let export_denied =
			kvexport_allowed().without_http_routes(Targets::Some([RouteTarget::Export].into()));
		assert!(enabled_with(export_denied).await?);
		Ok(())
	}

	/// Answers a request as the route does once the layers that authenticate it
	/// have named its session.
	async fn respond(ds: &Arc<Datastore>, session: &Session, body: Body) -> Response {
		let state = AppState {
			client_ip: ClientIp::None,
			datastore: Arc::clone(ds),
			metrics_observer: None,
			readiness: Readiness {
				ready: Arc::new(AtomicBool::new(true)),
				max_heartbeat_age: None,
			},
		};
		router::<()>()
			.layer(Extension(state))
			.layer(Extension(session.clone()))
			.oneshot(Request::post("/kvexport").body(body).unwrap())
			.await
			.unwrap()
	}

	/// A body that reports when it is read.
	fn watched_body(read: &Arc<AtomicBool>) -> Body {
		let read = Arc::clone(read);
		Body::from_stream(futures::stream::once(async move {
			read.store(true, Ordering::SeqCst);
			Ok::<_, std::io::Error>(Bytes::from_static(b"{}"))
		}))
	}

	/// The request body is read only for a caller whose request may be served.
	#[tokio::test]
	async fn the_body_is_not_read_before_the_caller_is_authorised() -> Result<()> {
		let enabled_ds = datastore_with(kvexport_allowed()).await?;
		let disabled_ds = datastore_with(Capabilities::default()).await?;
		for (ds, session, status) in [
			(&enabled_ds, Session::default(), StatusCode::UNAUTHORIZED),
			(&enabled_ds, Session::viewer(), StatusCode::FORBIDDEN),
			(&enabled_ds, Session::editor(), StatusCode::FORBIDDEN),
			(&disabled_ds, Session::owner(), StatusCode::FORBIDDEN),
			(&disabled_ds, Session::default(), StatusCode::FORBIDDEN),
		] {
			let read = Arc::new(AtomicBool::new(false));
			let response = respond(ds, &session, watched_body(&read)).await;
			assert_eq!(response.status(), status);
			assert!(!read.load(Ordering::SeqCst), "the body was read for a refused caller");
		}
		let read = Arc::new(AtomicBool::new(false));
		let response = respond(&enabled_ds, &Session::owner(), watched_body(&read)).await;
		assert_eq!(response.status(), StatusCode::OK);
		assert!(read.load(Ordering::SeqCst));
		Ok(())
	}

	/// A body over the route's own limit is refused, and a cursor longer than the
	/// 2 MiB default body limit is not.
	#[tokio::test]
	async fn the_request_is_bounded_by_the_route_and_not_by_the_default_limit() -> Result<()> {
		let ds = datastore_with(kvexport_allowed()).await?;
		let session = Session::owner();
		let long = format!(r#"{{"after":"{}"}}"#, "ab".repeat(3 * 1024 * 1024));
		assert!(long.len() > 2 * 1024 * 1024, "the request is under the default body limit");
		assert!(long.len() < MAX_REQUEST_BYTES);
		let response = respond(&ds, &session, Body::from(long)).await;
		assert_eq!(response.status(), StatusCode::OK);

		// One chunk, shared, sent until the limit is passed
		let chunk = Bytes::from(vec![b' '; 1024 * 1024]);
		let chunks = MAX_REQUEST_BYTES / chunk.len() + 1;
		let body = Body::from_stream(futures::stream::iter(
			(0..chunks).map(move |_| Ok::<_, std::io::Error>(chunk.clone())),
		));
		let response = respond(&ds, &session, body).await;
		assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
		Ok(())
	}

	#[tokio::test]
	async fn a_request_that_cannot_name_a_cursor_is_refused_whole() -> Result<()> {
		let ds = datastore_with(kvexport_allowed()).await?;
		let session = Session::owner();
		for body in [
			r#"{"after":"abc"}"#,
			r#"{"after":"zz"}"#,
			r#"{"after":7}"#,
			r#"{"latest_only":"yes"}"#,
			r#"{"limit_keys":10}"#,
			r#"["after"]"#,
			"not json",
		] {
			let response = respond(&ds, &session, Body::from(body)).await;
			assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{body}");
		}
		for body in ["", "{}", r#"{"after":null}"#, r#"{"after":"","latest_only":false}"#] {
			let response = respond(&ds, &session, Body::from(body)).await;
			assert_eq!(response.status(), StatusCode::OK, "{body}");
		}
		Ok(())
	}

	/// A datastore that keeps every version of a key, which is what a copy of its
	/// latest values drops. The memory backend of this release can be opened as
	/// one.
	async fn versioned() -> Result<Arc<Datastore>> {
		let ds = Datastore::builder()
			.with_capabilities(kvexport_allowed())
			.build_with_path("memory?versioned=true")
			.await?;
		ds.check_version().await?;
		Ok(Arc::new(ds))
	}

	#[tokio::test]
	async fn a_store_that_keeps_history_is_refused_unless_the_caller_accepts_its_loss() -> Result<()>
	{
		let ds = versioned().await?;
		assert!(copy::keeps_history(&ds).await?);
		assert!(matches!(open(&ds, None, false).await?, Opened::KeepsHistory));
		let body = request(&ds, None, true).await?.expect("latest_only was sent");
		let response = wire::decode(&body)?;
		assert_eq!(response.frames.last().map(|(key, _)| key.as_slice()), Some(VERSION_KEY));
		response.version.check()?;
		Ok(())
	}

	/// With `latest_only` the stream is what a store that keeps no history would
	/// serve for the same latest values: one frame per live key, holding its
	/// newest value, and none for a key that was written and then deleted.
	#[tokio::test]
	async fn latest_only_carries_each_keys_newest_value_and_nothing_older() -> Result<()> {
		let ds = versioned().await?;
		write(&ds, &[(b"/zz-kept", b"first".to_vec()), (b"/zz-gone", b"first".to_vec())]).await?;
		write(&ds, &[(b"/zz-kept", b"second".to_vec())]).await?;
		let pause = std::time::Duration::from_millis(20);
		tokio::time::sleep(pause).await;
		let before = web_time::SystemTime::now().duration_since(web_time::UNIX_EPOCH)?;
		tokio::time::sleep(pause).await;
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let raw: &Transactor = &tx;
		raw.del(b"/zz-gone".to_vec()).await?;
		raw.set(b"/zz-kept".to_vec(), b"third".to_vec()).await?;
		tx.commit().await?;

		// The history is there for the stream to drop: at a moment before the
		// last write, the deleted key is still stored, and so is the older value.
		// A version of the memory backend is the nanosecond of its commit.
		let version = u64::try_from(before.as_nanos())?;
		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let raw: &Transactor = &tx;
		let past_gone = raw.get(b"/zz-gone".to_vec(), Some(version)).await;
		let past_kept = raw.get(b"/zz-kept".to_vec(), Some(version)).await;
		tx.cancel().await?;
		assert_eq!(past_gone?.as_deref(), Some(b"first".as_slice()));
		assert_eq!(past_kept?.as_deref(), Some(b"second".as_slice()));

		let body = request(&ds, None, true).await?.expect("latest_only was sent");
		let response = wire::decode(&body)?;
		let kept: Vec<&[u8]> = response
			.frames
			.iter()
			.filter(|(key, _)| key == b"/zz-kept")
			.map(|(_, value)| value.as_slice())
			.collect();
		assert_eq!(kept, [b"third".as_slice()]);
		assert!(response.frames.iter().all(|(key, _)| key != b"/zz-gone"));
		assert_eq!(response.frames, copied(whole_keyspace(&ds).await?));
		let mut ordered: Vec<&[u8]> =
			response.frames.iter().map(|(key, _)| key.as_slice()).collect();
		ordered.pop();
		assert!(ordered.windows(2).all(|pair| pair[0] < pair[1]), "a key was sent twice");
		Ok(())
	}

	#[tokio::test]
	async fn the_route_answers_a_store_that_keeps_history_with_a_conflict_unless_latest_only()
	-> Result<()> {
		let ds = versioned().await?;
		let session = Session::owner();
		for body in ["", "{}", r#"{"latest_only":false}"#, r#"{"after":"00"}"#] {
			let response = respond(&ds, &session, Body::from(body)).await;
			assert_eq!(response.status(), StatusCode::CONFLICT, "{body}");
			let text = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
			assert!(String::from_utf8_lossy(&text).contains("latest_only"), "{body}");
		}
		let latest = Body::from(r#"{"latest_only":true}"#);
		let response = respond(&ds, &session, latest).await;
		assert_eq!(response.status(), StatusCode::OK);
		assert_eq!(response.headers()[axum::http::header::CONTENT_TYPE], CONTENT_TYPE);
		let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
		let response = wire::decode(&body)?;
		assert_eq!(response.frames.last().map(|(key, _)| key.as_slice()), Some(VERSION_KEY));
		Ok(())
	}

	/// Frames a target writes in the order they arrive, a transaction of two at a
	/// time.
	async fn write_in_order(target: &Datastore, frames: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
		for batch in frames.chunks(2) {
			let tx = target.transaction(TransactionType::Write, LockType::Optimistic).await?;
			set_all(&tx, batch.iter().cloned()).await?;
			tx.commit().await?;
		}
		Ok(())
	}

	/// The order the stream sends is what lets a target gate its own start: a
	/// keyspace written in that order is an existing store, not a creation. The
	/// version key arrives last, with the copy whole, so the version gate finds a
	/// store that already held data, and the bootstrap sentinel the source carried
	/// was never moved.
	#[tokio::test]
	async fn a_keyspace_copied_in_stream_order_is_an_existing_store() -> Result<()> {
		let source = seeded().await?;
		let response = pull(&source, None).await?;
		response.version.check()?;
		let target = Datastore::new("memory").await?;
		write_in_order(&target, &response.frames).await?;

		let (version, is_new) = target.check_version().await?;
		assert!(version.is_latest());
		assert!(!is_new, "the version gate treated the copied store as one it created");
		let mut expected = whole_keyspace(&source).await?;
		expected.retain(|(key, _)| key != copy::BOOTSTRAP_KEY);
		assert_eq!(whole_keyspace(&target).await?, expected);
		Ok(())
	}

	/// A copy that stops before the version key is refused, not served: the copied
	/// keys without `!v` read as a store from before versioning, and the version
	/// gate refuses that rather than start with keys missing.
	#[tokio::test]
	async fn a_copy_cut_short_before_the_version_key_is_refused() -> Result<()> {
		let source = seeded().await?;
		let response = pull(&source, None).await?;
		let (version, before) = response.frames.split_last().expect("a stream carries frames");
		assert_eq!(version.0, VERSION_KEY);
		assert!(before.len() > 2, "the copy wrote too little to test anything");
		let target = Datastore::new("memory").await?;
		write_in_order(&target, before).await?;

		let err = target.check_version().await.unwrap_err();
		assert!(
			err.to_string().contains("out-of-date"),
			"a partial copy was not refused as a store from before versioning: {err}"
		);
		Ok(())
	}
}
