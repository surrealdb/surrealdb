use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str::FromStr;
use std::{ops, str};

use anyhow::{Result, anyhow};
use chrono::offset::LocalResult;
use chrono::{DateTime, Datelike, SecondsFormat, TimeZone, Timelike, Utc};
use common::fmt::QuoteStr;
use revision::revisioned;
use storekey::{BorrowDecode, Encode};
use surrealdb_kvs::timestamp::TimeStampImpl;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::Error;
use crate::syn;
use crate::val::{Duration, IndexFormat, IndexIdFormat, TrySub};

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Datetime(pub DateTime<Utc>);

impl Datetime {
	pub fn now() -> Datetime {
		Datetime(Utc::now())
	}
}

impl Datetime {
	pub const MIN_UTC: Self = Datetime(DateTime::<Utc>::MIN_UTC);
	pub const MAX_UTC: Self = Datetime(DateTime::<Utc>::MAX_UTC);
}

impl From<DateTime<Utc>> for Datetime {
	fn from(v: DateTime<Utc>) -> Self {
		Self(v)
	}
}

impl From<Datetime> for DateTime<Utc> {
	fn from(x: Datetime) -> Self {
		x.0
	}
}

impl From<surrealdb_types::Datetime> for Datetime {
	fn from(v: surrealdb_types::Datetime) -> Self {
		Self(v.into_inner())
	}
}

impl From<Datetime> for surrealdb_types::Datetime {
	fn from(x: Datetime) -> Self {
		surrealdb_types::Datetime::from(x.0)
	}
}

impl FromStr for Datetime {
	type Err = ();
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match syn::datetime(s) {
			Ok(v) => Ok(v.into()),
			_ => Err(()),
		}
	}
}

impl TryFrom<(i64, u32)> for Datetime {
	type Error = ();
	fn try_from(v: (i64, u32)) -> Result<Self, Self::Error> {
		match Utc.timestamp_opt(v.0, v.1) {
			LocalResult::Single(v) => Ok(Self(v)),
			_ => Err(()),
		}
	}
}

impl Deref for Datetime {
	type Target = DateTime<Utc>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Datetime {
	/// Convert the datetime to the version a versioned read takes, through the
	/// datastore's version implementation (`Transactable::version_impl`).
	pub fn to_version_stamp(self, ts_impl: &dyn TimeStampImpl) -> Result<u64> {
		let ts = ts_impl
			.create_from_datetime(self.0)
			.ok_or_else(|| anyhow!(Error::TimestampOverflow(self.to_string())))?;
		Ok(ts.as_versionstamp() as u64)
	}

	/// Convert to nanosecond timestamp.
	pub fn to_i64(self) -> Option<i64> {
		self.0.timestamp_nanos_opt()
	}

	/// Convert to second timestamp.
	pub fn to_secs(self) -> i64 {
		self.0.timestamp()
	}
}

/// The number of bytes in the `YYYY-MM-DDTHH:MM:SS` prefix of a datetime's key
/// encoding.
pub const KEY_SECOND_PREFIX_LEN: usize = 19;

impl Datetime {
	/// Whether the key encoding of this datetime opens with the fixed-width
	/// `YYYY-MM-DDTHH:MM:SS` text that [`Datetime::key_second_prefix`] returns.
	///
	/// A datetime is encoded in a key as its RFC 3339 text. Only the years
	/// 0000 to 9999 render as four digits; every other year renders with a sign
	/// and a different width, so its text neither has the fixed prefix nor
	/// orders consistently with the other years. A leap second (a nanosecond
	/// field of one billion or more) renders as second 60, which no whole-second
	/// floor maps onto.
	pub fn in_key_range(&self) -> bool {
		(0..=9999).contains(&self.0.year()) && !self.is_leap_second()
	}

	/// Whether this datetime is a leap second: its nanosecond field is one billion
	/// or more, so that it renders as second 60.
	pub fn is_leap_second(&self) -> bool {
		self.0.nanosecond() >= 1_000_000_000
	}

	/// The `YYYY-MM-DDTHH:MM:SS` text, in UTC, that every key encoding of a
	/// datetime within the same whole second opens with.
	///
	/// The text is fixed-width ASCII, so the byte order of these prefixes is the
	/// chronological order of the whole seconds. The bytes that follow it in a
	/// full encoding are the sub-second digits and the `Z`, and those do not sort
	/// chronologically: `.500Z` sorts before `Z`. A prefix therefore delimits a
	/// whole second of keys and nothing finer.
	///
	/// Returns `None` unless [`Datetime::in_key_range`].
	pub fn key_second_prefix(&self) -> Option<[u8; KEY_SECOND_PREFIX_LEN]> {
		if !self.in_key_range() {
			return None;
		}
		let t = &self.0;
		let mut out = *b"0000-00-00T00:00:00";
		put_digits(&mut out[0..4], t.year() as u32);
		put_digits(&mut out[5..7], t.month());
		put_digits(&mut out[8..10], t.day());
		put_digits(&mut out[11..13], t.hour());
		put_digits(&mut out[14..16], t.minute());
		put_digits(&mut out[17..19], t.second());
		Some(out)
	}

	/// This datetime with the sub-second part removed.
	///
	/// For a datetime that is [`Datetime::in_key_range`], two datetimes have the
	/// same floor exactly when they have the same
	/// [`Datetime::key_second_prefix`], and floors compare in the order of their
	/// prefixes.
	pub fn floor_second(&self) -> Datetime {
		match self.0.with_nanosecond(0) {
			Some(floored) => Datetime(floored),
			// Zero is a valid nanosecond field for every datetime, so this arm
			// is unreachable; the datetime is returned as it was.
			None => *self,
		}
	}
}

/// Writes `value` into `out` as zero-padded decimal digits.
fn put_digits(out: &mut [u8], mut value: u32) {
	for slot in out.iter_mut().rev() {
		*slot = b'0' + (value % 10) as u8;
		value /= 10;
	}
}

impl Display for Datetime {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		self.0.to_rfc3339_opts(SecondsFormat::AutoSi, true).fmt(f)
	}
}

impl ToSql for Datetime {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "d{}", QuoteStr(&self.to_string()))
	}
}

impl ops::Sub<Self> for Datetime {
	type Output = Duration;
	fn sub(self, other: Self) -> Duration {
		match (self.0 - other.0).to_std() {
			Ok(d) => Duration::from(d),
			Err(_) => Duration::default(),
		}
	}
}

impl TrySub for Datetime {
	type Output = Duration;
	fn try_sub(self, other: Self) -> Result<Duration> {
		(self.0 - other.0)
			.to_std()
			.map_err(|_| Error::ArithmeticNegativeOverflow(format!("{self} - {other}")))
			.map_err(anyhow::Error::new)
			.map(Duration::from)
	}
}

/// A `Datetime` in any key outside an index — a record key whose compound id
/// holds one, and the edges and references that name such a record — is its
/// RFC 3339 text in UTC with `SecondsFormat::AutoSi`, as a terminated string.
///
/// That text is part of the key of every such record, so it is fixed: a record
/// stays addressable by its id for as long as it exists. It does not collate
/// chronologically: `...:00Z` sorts after `...:00.5Z`, and signed years sort
/// apart from four-digit ones.
impl Encode<()> for Datetime {
	fn encode<W: std::io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> std::result::Result<(), storekey::EncodeError> {
		let encode = self.to_rfc3339_opts(SecondsFormat::AutoSi, true);
		Encode::<()>::encode(&encode, w)
	}
}

/// Reads RFC 3339 text. Signed years (`-0100`, `+10000`), which strict
/// RFC 3339 rejects, are read with the SurrealQL datetime parser.
impl<'de> BorrowDecode<'de, ()> for Datetime {
	fn borrow_decode(
		r: &mut storekey::BorrowReader<'de>,
	) -> std::result::Result<Self, storekey::DecodeError> {
		let s = r.read_str_cow()?;
		parse_key_text(s.as_ref())
	}
}

fn parse_key_text(s: &str) -> std::result::Result<Datetime, storekey::DecodeError> {
	match DateTime::parse_from_rfc3339(s) {
		Ok(x) => Ok(Datetime(x.to_utc())),
		Err(_) => Datetime::from_str(s).map_err(|_| storekey::DecodeError::InvalidFormat),
	}
}

/// Under [`IndexIdFormat`] a `Datetime` is its record-key text: its spelling in
/// a record id inside any index key, and among the indexed values of an index
/// stamped below `DATETIME_ORDER_FORMAT_VERSION`.
impl Encode<IndexIdFormat> for Datetime {
	fn encode<W: std::io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> std::result::Result<(), storekey::EncodeError> {
		Encode::<()>::encode(self, w)
	}
}

impl<'de> BorrowDecode<'de, IndexIdFormat> for Datetime {
	fn borrow_decode(
		r: &mut storekey::BorrowReader<'de>,
	) -> std::result::Result<Self, storekey::DecodeError> {
		BorrowDecode::<'de, ()>::borrow_decode(r)
	}
}

/// Under [`IndexFormat`] a `Datetime` is its whole seconds since the Unix epoch
/// (`i64`) followed by its subsecond nanoseconds (`u32`), each in storekey's
/// fixed-width order-preserving integer form: 12 bytes, written without an
/// intermediate allocation. It is the spelling among the indexed values of an
/// index stamped at `DATETIME_ORDER_FORMAT_VERSION` or later. One index holds
/// one spelling: every key of it is built for its stamp, where the b-tree index
/// keyspace chooses between this format and [`IndexIdFormat`].
///
/// Byte order is chronological order for every instant `DateTime<Utc>` can
/// hold, years before 0000 and after 9999 included, and one instant written at
/// any precision or offset is one key, so equality seeks, range scans and
/// index-ordered iteration agree with comparing the values. A leap second
/// carries its nanoseconds past 999,999,999 within the second it extends, which
/// is also where `DateTime` orders it.
///
/// The first byte is always `0x7F` (before the epoch) or `0x80` (from it on),
/// because `DateTime<Utc>` spans fewer than 2^43 seconds either side of it.
impl Encode<IndexFormat> for Datetime {
	fn encode<W: std::io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> std::result::Result<(), storekey::EncodeError> {
		w.write_i64(self.0.timestamp())?;
		w.write_u32(self.0.timestamp_subsec_nanos())
	}
}

/// Reads the 12-byte form, and RFC 3339 text too. The two are told apart by the
/// first byte: the integer form starts with `0x7F` or `0x80`, text with a digit
/// or a sign.
///
/// Both are accepted so that one decoder reads the keys of an index of either
/// stamp: a key type decodes under the format it is pinned to, which for index
/// entries and unique slots is this one, whichever spelling wrote them. No query
/// reads indexed values back out of a key — a read takes the record from the
/// entry's value — so this matters to decoding a key as a whole, such as the
/// keyspace's reverse decoder does.
impl<'de> BorrowDecode<'de, IndexFormat> for Datetime {
	fn borrow_decode(
		r: &mut storekey::BorrowReader<'de>,
	) -> std::result::Result<Self, storekey::DecodeError> {
		let [first] = r.read_array::<1>()?;
		if first == 0x7F || first == 0x80 {
			let rest = r.read_array::<7>()?;
			let mut secs = [first; 8];
			secs[1..].copy_from_slice(&rest);
			let secs = i64::from_be_bytes(secs) ^ i64::MIN;
			let nanos = r.read_u32()?;
			return DateTime::<Utc>::from_timestamp(secs, nanos)
				.map(Datetime)
				.ok_or(storekey::DecodeError::InvalidFormat);
		}
		let rest = r.read_str_cow()?;
		let mut text = String::with_capacity(rest.len() + 1);
		text.push(char::from(first));
		text.push_str(rest.as_ref());
		parse_key_text(&text)
	}
}

#[cfg(test)]
mod tests {
	use rstest::rstest;

	use super::*;
	use crate::types::PublicDatetime;
	use crate::val::{Array, RecordIdKey, Value};

	#[rstest]
	#[case("2021-01-01T00:00:00Z", Datetime(DateTime::<Utc>::from_timestamp(1_609_459_200, 0).unwrap()), PublicDatetime::from_timestamp(1_609_459_200, 0).unwrap())]
	fn test_from_str(
		#[case] input: &str,
		#[case] expected: Datetime,
		#[case] expected_public: PublicDatetime,
	) {
		let internal_actual = Datetime::from_str(input).unwrap();
		let public_actual = PublicDatetime::from_str(input).unwrap();

		assert_eq!(internal_actual.timestamp(), expected.timestamp());

		assert_eq!(internal_actual, expected);
		assert_eq!(public_actual, expected_public);

		assert_eq!(internal_actual.to_string(), public_actual.to_string());
	}

	fn ymd(year: i32, month: u32, day: u32) -> Datetime {
		Datetime(
			chrono::NaiveDate::from_ymd_opt(year, month, day)
				.unwrap()
				.and_hms_opt(0, 0, 0)
				.unwrap()
				.and_utc(),
		)
	}

	fn at(secs: i64, nanos: u32) -> Datetime {
		Datetime(DateTime::<Utc>::from_timestamp(secs, nanos).unwrap())
	}

	fn index_key<E: Encode<IndexFormat>>(v: &E) -> Vec<u8> {
		storekey::encode_vec_format::<IndexFormat, _>(v).unwrap()
	}

	/// Instants in strictly increasing order, spanning sub-second precisions,
	/// the epoch, a leap second and years that need a sign in RFC 3339.
	fn ascending() -> Vec<Datetime> {
		// 2026-08-28T09:00:00Z
		let s = 1_787_907_600;
		// 2016-12-31T23:59:59Z, the second a leap second extends.
		let leap = 1_483_228_799;
		vec![
			Datetime::MIN_UTC,
			ymd(-200, 1, 1),
			ymd(-100, 1, 1),
			ymd(-1, 12, 31),
			ymd(0, 1, 1),
			at(-1, 999_999_999),
			at(0, 0),
			at(0, 1),
			at(leap, 999_999_999),
			at(leap, 1_500_000_000),
			at(leap + 1, 0),
			at(s, 0),
			at(s, 1),
			at(s, 100_000_000),
			at(s, 152_000_000),
			at(s, 152_000_001),
			at(s, 152_300_000),
			at(s, 611_000_000),
			at(s + 1, 0),
			at(s + 1, 70_000_000),
			ymd(9999, 12, 31),
			ymd(10000, 1, 1),
			Datetime::MAX_UTC,
		]
	}

	#[test]
	fn index_format_collates_chronologically() {
		let values = ascending();
		for pair in values.windows(2) {
			assert!(pair[0] < pair[1], "fixture out of order: {} !< {}", pair[0], pair[1]);
			let (a, b) = (Value::Datetime(pair[0]), Value::Datetime(pair[1]));
			assert!(index_key(&a) < index_key(&b), "index key of {a:?} does not sort before {b:?}");
		}
	}

	/// The text spelling orders neither sub-seconds nor signed years by time.
	#[test]
	fn text_spelling_is_not_chronological() {
		let text = |d: Datetime| storekey::encode_vec(&d).unwrap();
		let whole = at(1_787_907_600, 0);
		let fraction = at(1_787_907_600, 152_000_000);
		assert!(text(whole) > text(fraction));
		assert!(text(ymd(-100, 1, 1)) < text(ymd(-200, 1, 1)));
		assert!(text(ymd(10000, 1, 1)) < text(ymd(0, 1, 1)));
	}

	/// Under `IndexIdFormat` an indexed value is spelled as under `IndexFormat`
	/// but for a Datetime, which is its record-key text wherever it is nested.
	#[test]
	fn text_format_spells_only_a_datetime_differently() {
		let text_key = |v: &Value| storekey::encode_vec_format::<IndexIdFormat, _>(v).unwrap();
		let dt = at(1_787_907_600, 152_000_000);
		let text = storekey::encode_vec(&dt).unwrap();
		for v in [
			Value::Datetime(dt),
			Value::Array(Array::from(vec![Value::from(1), Value::Datetime(dt)])),
		] {
			let key = text_key(&v);
			assert!(key.windows(text.len()).any(|w| w == text), "{v:?} in {key:?}");
			assert_ne!(key, index_key(&v), "{v:?}");
		}
		for v in [
			Value::from(1),
			Value::Number(crate::val::Number::Decimal(1.into())),
			Value::from("a"),
			Value::Array(Array::from(vec![Value::from(1.5f64), Value::from("x")])),
		] {
			assert_eq!(text_key(&v), index_key(&v), "{v:?}");
		}
		// Numbers of every kind still share key bytes.
		assert_eq!(
			text_key(&Value::from(1)),
			text_key(&Value::Number(crate::val::Number::Decimal(1.into())))
		);
	}

	#[test]
	fn index_format_is_twelve_bytes_and_leads_with_0x7f_or_0x80() {
		for dt in ascending() {
			let key = index_key(&dt);
			assert_eq!(key.len(), 12, "{dt}");
			let expected = if dt.timestamp() < 0 {
				0x7F
			} else {
				0x80
			};
			assert_eq!(key[0], expected, "{dt}");
		}
	}

	#[test]
	fn index_format_equates_precisions_and_offsets_of_one_instant() {
		let key = index_key(&Datetime::from_str("2026-08-28T09:00:00.1Z").unwrap());
		for text in ["2026-08-28T09:00:00.100000000Z", "2026-08-28T11:00:00.1+02:00"] {
			assert_eq!(index_key(&Datetime::from_str(text).unwrap()), key, "{text}");
		}
	}

	#[test]
	fn both_formats_round_trip() {
		for dt in ascending() {
			let back: Datetime =
				storekey::decode_borrow_format::<IndexFormat, _>(&index_key(&dt)).unwrap();
			assert_eq!(back, dt, "index format");
			let v = Value::Datetime(dt);
			let back: Value =
				storekey::decode_borrow_format::<IndexFormat, _>(&index_key(&v)).unwrap();
			assert_eq!(back, v, "index format, as a value");
			let back: Datetime = storekey::decode_borrow(&storekey::encode_vec(&dt).unwrap())
				.unwrap_or_else(|e| panic!("record-key format of {dt}: {e:?}"));
			assert_eq!(back, dt, "record-key format");
		}
	}

	/// The keys of an index stamped below `DATETIME_ORDER_FORMAT_VERSION` hold
	/// RFC 3339 text; they decode under `IndexFormat` too, alone and inside a
	/// list of indexed values.
	#[test]
	fn index_format_decodes_text_entries() {
		for dt in ascending() {
			for format in [SecondsFormat::AutoSi, SecondsFormat::Nanos] {
				let text = dt.to_rfc3339_opts(format, true);
				let legacy = storekey::encode_vec_format::<IndexFormat, _>(&text).unwrap();
				let back: Datetime =
					storekey::decode_borrow_format::<IndexFormat, _>(&legacy).unwrap();
				assert_eq!(back, dt, "{text}");
			}
			let legacy = storekey::encode_vec(&Array::from(vec![
				Value::from("a"),
				Value::Datetime(dt),
				Value::from(1),
			]))
			.unwrap();
			let back: Array = storekey::decode_borrow_format::<IndexFormat, _>(&legacy).unwrap();
			assert_eq!(back.0[1], Value::Datetime(dt), "{dt} inside a list");
			assert_eq!(back.0[2], Value::from(1), "{dt}: the value after it");
		}
	}

	/// A record key spells a Datetime in a compound id as `AutoSi` RFC 3339
	/// text.
	#[rstest]
	#[case("2026-08-28T09:00:00Z")]
	#[case("2026-08-28T09:00:00.152Z")]
	#[case("2026-08-28T09:00:00.152300Z")]
	#[case("2026-08-28T09:00:00.152300001Z")]
	fn record_key_format_is_autosi_text(#[case] text: &str) {
		let dt = Datetime::from_str(text).unwrap();
		let mut expected = text.as_bytes().to_vec();
		expected.push(0);
		assert_eq!(storekey::encode_vec(&dt).unwrap(), expected);
		let id = RecordIdKey::Array(Array::from(vec![Value::from("a"), Value::Datetime(dt)]));
		let key = storekey::encode_vec(&id).unwrap();
		assert!(key.windows(expected.len()).any(|w| w == expected), "{text} in {key:?}");
	}

	/// A record id inside an index key — the id ending an index entry, or a
	/// record link among the indexed values — spells a nested Datetime as text
	/// under either index spelling, and decodes back.
	#[rstest]
	#[case("2026-08-28T09:00:00Z")]
	#[case("2026-08-28T09:00:00.152Z")]
	fn record_id_in_an_index_key_keeps_the_text(#[case] text: &str) {
		let dt = Datetime::from_str(text).unwrap();
		let mut expected = text.as_bytes().to_vec();
		expected.push(0);
		let id = RecordIdKey::Array(Array::from(vec![
			Value::from("a"),
			Value::Datetime(dt),
			Value::from(1),
		]));
		let link = Value::RecordId(crate::val::RecordId::new("t".into(), id.clone()));
		let text_key = |v: &Value| storekey::encode_vec_format::<IndexIdFormat, _>(v).unwrap();
		assert_eq!(index_key(&link), text_key(&link), "the id is spelled alike under both");
		for key in [index_key(&id), index_key(&link)] {
			assert!(key.windows(expected.len()).any(|w| w == expected), "{text} in {key:?}");
		}
		let back: RecordIdKey =
			storekey::decode_borrow_format::<IndexFormat, _>(&index_key(&id)).unwrap();
		assert_eq!(back, id);
		let back: Value =
			storekey::decode_borrow_format::<IndexFormat, _>(&index_key(&link)).unwrap();
		assert_eq!(back, link);
		// A number in the id is still spelled as an indexed number: no kind.
		let dec = RecordIdKey::Array(Array::from(vec![
			Value::from("a"),
			Value::Datetime(dt),
			Value::Number(crate::val::Number::Decimal(1.into())),
		]));
		assert_eq!(index_key(&dec), index_key(&id));
	}
}
