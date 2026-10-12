//! The catalog entry envelope and the dispatch between its two grammars.
//!
//! ```text
//! entry  = FE, header, floor, body
//! header = one byte, every bit reserved and zero
//! floor  = canonical unsigned LEB128 of `Release::to_u32()`
//! body   = exactly one MAP item: a struct's map, or an enum's one-entry map
//! ```
//!
//! The positions of the marker, the header byte and the floor are fixed. A reader checks them in
//! this order, so a later release's header flag is reported as the release needed and never as a
//! reserved bit:
//!
//! 1. the marker (`Marker`) and a header byte (`NoHeader`);
//! 2. the floor (`Floor`, `NotARelease`);
//! 3. the floor against this build (`NeedsRelease`);
//! 4. the header bits (`ReservedBits`);
//! 5. the body.
//!
//! `0xFE` never starts a `revision` value: a `revision` header is a `u16` varint whose first byte
//! is a literal `0..=250` or the width marker `251`. Every other first byte is therefore read as
//! `revision`, exactly as 3.x wrote it.
//!
//! A body is one item and runs to the end of the entry. The floor is the oldest release able to
//! read the entry, computed by the encoder from the critical fields it writes and everything its
//! values use. Never hand-written.

use std::io::ErrorKind;

use revision::{DeserializeRevisioned, SerializeRevisioned, SkipRevisioned};
use surrealdb_expr::val::stored::schema::{Floor, SchemaError, StoredType};
use surrealdb_expr::val::stored::{Item, varint, with_encoder};
use surrealdb_kvs::{EncodeContext, Encoded, FeatureLevel, Release};

use super::error::EntryError;
use crate::Error;

/// The first byte of an entry in the stored grammar.
pub const CATALOG_MARKER: u8 = 0xFE;

/// What the encoder reports for a computed floor above the first release on a type that is
/// `floor_fixed`.
const FLOOR_FIXED: &str = "a floor_fixed type above its first release";

/// The marker, header byte and floor of an entry, and the body after them.
struct Parts<'a> {
	floor: Release,
	header: u8,
	body: &'a [u8],
}

/// Splits an entry into its parts, checking the marker, that a header byte is present, and that
/// the floor names a release. The floor is not compared with this build.
fn split(bytes: &[u8]) -> Result<Parts<'_>, EntryError> {
	let (&marker, rest) = bytes.split_first().ok_or(EntryError::NoHeader)?;
	if marker != CATALOG_MARKER {
		return Err(EntryError::Marker(marker));
	}
	let (&header, rest) = rest.split_first().ok_or(EntryError::NoHeader)?;
	let (floor, used) = varint::read_u64(rest).map_err(EntryError::Floor)?;
	let floor = u32::try_from(floor)
		.map(Release::from_u32)
		.ok()
		.filter(|release| *release >= Release::FIRST_STORED)
		.ok_or(EntryError::NotARelease(floor))?;
	// `used` counts bytes of `rest`, so the body starts within it.
	let body = rest.get(used..).ok_or(EntryError::NoHeader)?;
	Ok(Parts {
		floor,
		header,
		body,
	})
}

/// The floor of an entry and its body, without decoding the body.
///
/// Refuses, in order: a missing marker or header byte, a malformed floor, a floor newer than this
/// build (`ty` names the entry's type in the refusal), and a header with any bit set.
pub fn parse_header<'a>(
	ty: &'static str,
	bytes: &'a [u8],
) -> Result<(Release, &'a [u8]), EntryError> {
	let parts = split(bytes)?;
	if parts.floor > Release::CURRENT {
		return Err(EntryError::NeedsRelease {
			ty,
			needed: parts.floor,
			have: Release::CURRENT,
		});
	}
	if parts.header != 0 {
		return Err(EntryError::ReservedBits(parts.header));
	}
	Ok((parts.floor, parts.body))
}

/// The release `value` needs in the stored grammar: its type's own release, the newest critical
/// field or variant it writes, and everything its values use.
///
/// A stored text that does not parse has no known release, so the entry then also needs whatever
/// the entry it replaces needed, and a text that no longer parses stays writable.
fn release_of<T: StoredType>(value: &T, prior: Option<&[u8]>) -> Result<Release, EntryError> {
	let ty = T::SCHEMA.name;
	let mut acc = Floor::first();
	acc.raise(T::SCHEMA.since);
	value.floor(&mut acc).map_err(|error| EntryError::body(ty, error))?;
	let mut release = acc.release;
	if acc.unparsable_text
		&& let Some(parts) = prior.and_then(|bytes| split(bytes).ok())
	{
		release = release.max(parts.floor);
	}
	if T::SCHEMA.floor_fixed && release > Release::FIRST_STORED {
		return Err(EntryError::body(
			ty,
			SchemaError::Unencodable {
				ty,
				what: FLOOR_FIXED,
			},
		));
	}
	Ok(release)
}

/// The stored-grammar entry for `value`, written for a store at feature level `level`.
///
/// The unknown ignorable fields of `prior`, when it is an entry this build can read, are carried
/// over. The entry's floor never exceeds `level`.
fn write_stored<T: StoredType>(
	value: &T,
	prior: Option<&[u8]>,
	level: Release,
) -> Result<Encoded, EntryError> {
	let ty = T::SCHEMA.name;
	let floor = release_of(value, prior)?;
	if floor > level {
		return Err(EntryError::AboveLevel {
			ty,
			needed: floor,
			level: FeatureLevel::At(level),
		});
	}
	let prior_body =
		prior.and_then(|bytes| parse_header(ty, bytes).ok()).map(|(_, body)| Item::new(body));
	let bytes = with_encoder(|enc| {
		value.encode_item(enc, prior_body)?;
		enc.put_varint_u64(u64::from(floor.to_u32()));
		// The header: every bit is reserved and zero.
		enc.put_u8(0);
		enc.put_u8(CATALOG_MARKER);
		Ok(enc.as_bytes().to_vec())
	})
	.map_err(|error: SchemaError| EntryError::body(ty, error))?;
	Ok(Encoded {
		bytes,
		floor: Some(floor),
	})
}

/// The error a catalog write returns for `error`.
///
/// A closure is refused with the error its `revision` encoding raises, so a write storing one fails
/// alike whichever encoding it would be written in, and alike for a record and a catalog entry.
#[cold]
pub fn write_error(error: EntryError) -> anyhow::Error {
	match error {
		EntryError::Body {
			error: SchemaError::Grammar(surrealdb_expr::val::stored::Error::Closure),
			..
		} => anyhow::Error::new(revision::Error::Conversion(
			"Closures cannot be stored on disk".to_owned(),
		)),
		error => Error::Entry(error).into(),
	}
}

/// `value`'s bytes at `ctx`, for a type that also has a `revision` form.
///
/// At the Legacy level the bytes are `revision`, which every 3.x reader reads. That is refused,
/// as [`EntryError::AboveLevel`], for a value `revision` cannot hold: one whose floor is above the
/// first release, or one with a non-default `Unrevisioned` field, which `revision` would drop.
/// Above Legacy the bytes are the stored grammar, carrying over `ctx.prior`'s unknown ignorable
/// fields, and are refused when the floor exceeds the level.
pub fn encode_entry<T: StoredType + SerializeRevisioned>(
	value: &T,
	ctx: &EncodeContext<'_>,
) -> Result<Encoded, EntryError> {
	let ty = T::SCHEMA.name;
	match ctx.level {
		FeatureLevel::Legacy => {
			if let Some(needed) = value.unrevisioned_floor() {
				return Err(EntryError::AboveLevel {
					ty,
					needed,
					level: ctx.level,
				});
			}
			let needed = release_of(value, ctx.prior)?;
			if !ctx.level.admits(needed) {
				return Err(EntryError::AboveLevel {
					ty,
					needed,
					level: ctx.level,
				});
			}
			let bytes = revision::to_vec(value).map_err(|_| EntryError::RevisionWrite {
				ty,
			})?;
			Ok(Encoded {
				bytes,
				floor: None,
			})
		}
		FeatureLevel::At(level) => write_stored(value, ctx.prior, level),
	}
}

/// `value`'s bytes at `ctx`, for a type with no `revision` form: the stored grammar only, refused
/// at the Legacy level.
pub fn encode_stored<T: StoredType>(
	value: &T,
	ctx: &EncodeContext<'_>,
) -> Result<Encoded, EntryError> {
	match ctx.level {
		FeatureLevel::Legacy => Err(EntryError::AboveLevel {
			ty: T::SCHEMA.name,
			needed: release_of(value, ctx.prior)?,
			level: ctx.level,
		}),
		FeatureLevel::At(level) => write_stored(value, ctx.prior, level),
	}
}

/// The canonical stored-grammar bytes of `value`: at this build's release, replacing nothing.
///
/// Two values are equal exactly when their canonical bytes are, so these serve compare-and-set.
pub fn canonical<T: StoredType>(value: &T) -> Result<Vec<u8>, EntryError> {
	write_stored(value, None, Release::CURRENT).map(|encoded| encoded.bytes)
}

/// Decodes an entry in the stored grammar.
///
/// The body is one item, so it runs to the end of `bytes`.
pub fn decode_entry<T: StoredType>(bytes: &[u8]) -> Result<T, EntryError> {
	let ty = T::SCHEMA.name;
	let (_, body) = parse_header(ty, bytes)?;
	T::decode_item(Item::new(body), 0).map_err(|error| EntryError::body(ty, error))
}

/// Decodes an entry in either grammar.
///
/// An entry that starts with the marker is the stored grammar. Anything else is the `revision`
/// encoding, as 3.x wrote it. That form is first walked with [`revision::skip_slice`], which
/// proves every length prefix against the bytes present before anything is allocated, and must be
/// consumed whole: bytes beyond the value are [`EntryError::RevisionTrailing`].
pub fn decode_entry_or_revision<T>(bytes: &[u8]) -> anyhow::Result<T>
where
	T: StoredType + DeserializeRevisioned + SkipRevisioned,
{
	if bytes.first() == Some(&CATALOG_MARKER) {
		return decode_entry::<T>(bytes).map_err(|error| Error::Entry(error).into());
	}
	let used = revision::skip_slice::<T>(bytes)?;
	let extra = bytes.len().saturating_sub(used);
	if extra != 0 {
		return Err(Error::Entry(EntryError::RevisionTrailing {
			ty: T::SCHEMA.name,
			extra,
		})
		.into());
	}
	Ok(revision::from_slice(bytes)?)
}

/// Reads one `revision` value from the front of `bytes`: the value, and what follows it.
///
/// The extent is proved with [`revision::skip_slice`] before the value is read.
pub(crate) fn read_revision_prefix<T>(bytes: &[u8]) -> Result<(T, &[u8]), revision::Error>
where
	T: DeserializeRevisioned + SkipRevisioned,
{
	let used = revision::skip_slice::<T>(bytes)?;
	let (head, rest) = bytes
		.split_at_checked(used)
		.ok_or_else(|| revision::Error::Io(ErrorKind::UnexpectedEof.into()))?;
	Ok((revision::from_slice(head)?, rest))
}
