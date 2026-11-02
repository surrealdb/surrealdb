#![deny(clippy::wildcard_enum_match_arm)]

use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use revision::revisioned;
use storekey::{BorrowDecode, Encode};
use surrealdb_types::{SqlFormat, ToSql};

use crate::val::{Datetime, IndexFormat, IndexIdFormat};

const MICROS_PER_DAY: i128 = 86_400_000_000;
const MICROS_PER_MONTH: i128 = 30 * MICROS_PER_DAY;

type EncodeResult = std::result::Result<(), storekey::EncodeError>;
type DecodeResult<T> = std::result::Result<T, storekey::DecodeError>;

fn flip_i32(v: i32) -> [u8; 4] {
	(v as u32 ^ 0x8000_0000).to_be_bytes()
}

fn unflip_i32(b: [u8; 4]) -> i32 {
	(u32::from_be_bytes(b) ^ 0x8000_0000) as i32
}

fn flip_i64(v: i64) -> [u8; 8] {
	(v as u64 ^ (1 << 63)).to_be_bytes()
}

fn unflip_i64(b: [u8; 8]) -> i64 {
	(u64::from_be_bytes(b) ^ (1 << 63)) as i64
}

fn flip_i128(v: i128) -> [u8; 16] {
	(v as u128 ^ (1 << 127)).to_be_bytes()
}

fn unflip_i128(b: [u8; 16]) -> i128 {
	(u128::from_be_bytes(b) ^ (1 << 127)) as i128
}

/// A calendar date as a count of days since 1970-01-01 in the proleptic Gregorian calendar.
///
/// The count is limited to the range of [`chrono::NaiveDate`]. Construct one through
/// [`Date::from_days`] to enforce that.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Date(pub i32);

impl Date {
	/// Creates a date from a count of days since 1970-01-01, or `None` if it is out of range.
	pub fn from_days(days: i32) -> Option<Self> {
		surrealdb_types::Date::from_days(days).map(Self::from)
	}

	/// Whether the day count is within the supported range.
	pub fn is_valid(&self) -> bool {
		Self::from_days(self.0).is_some()
	}

	/// The number of days since 1970-01-01.
	pub fn days(&self) -> i32 {
		self.0
	}

	/// The calendar date.
	pub fn naive(&self) -> NaiveDate {
		surrealdb_types::Date::from(*self).into_inner()
	}
}

impl Date {
	/// `infinity`, the greatest date.
	pub fn infinity() -> Self {
		surrealdb_types::Date::infinity().into()
	}

	/// `-infinity`, the least date.
	pub fn neg_infinity() -> Self {
		surrealdb_types::Date::neg_infinity().into()
	}

	/// Whether this date is `infinity` or `-infinity`.
	pub fn is_infinite(&self) -> bool {
		surrealdb_types::Date::from(*self).is_infinite()
	}

	/// Midnight of this date as a timestamp. An infinite date is the infinite timestamp of the
	/// same sign, since midnight of the last day is not the greatest timestamp.
	pub fn at_midnight(&self) -> Timestamp {
		if *self == Self::infinity() {
			Timestamp::infinity()
		} else if *self == Self::neg_infinity() {
			Timestamp::neg_infinity()
		} else {
			Timestamp::from(self.naive().and_time(NaiveTime::MIN))
		}
	}
}

impl From<surrealdb_types::Date> for Date {
	fn from(v: surrealdb_types::Date) -> Self {
		Self(v.days())
	}
}

impl From<Date> for surrealdb_types::Date {
	fn from(v: Date) -> Self {
		surrealdb_types::Date::from_days(v.0).unwrap_or_else(surrealdb_types::Date::min_value)
	}
}

impl From<NaiveDate> for Date {
	fn from(v: NaiveDate) -> Self {
		surrealdb_types::Date::from(v).into()
	}
}

impl FromStr for Date {
	type Err = anyhow::Error;
	fn from_str(s: &str) -> Result<Self> {
		surrealdb_types::Date::from_str(s).map(Self::from)
	}
}

impl Display for Date {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		surrealdb_types::Date::from(*self).fmt(f)
	}
}

impl ToSql for Date {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		surrealdb_types::Date::from(*self).fmt_sql(f, fmt)
	}
}

impl Encode<()> for Date {
	fn encode<W: std::io::Write>(&self, w: &mut storekey::Writer<W>) -> EncodeResult {
		w.write_array(flip_i32(self.0))
	}
}

impl<'de> BorrowDecode<'de, ()> for Date {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> DecodeResult<Self> {
		Self::from_days(unflip_i32(r.read_array::<4>()?))
			.ok_or(storekey::DecodeError::InvalidFormat)
	}
}

impl Encode<IndexFormat> for Date {
	fn encode<W: std::io::Write>(&self, w: &mut storekey::Writer<W>) -> EncodeResult {
		Encode::<()>::encode(self, w)
	}
}

impl<'de> BorrowDecode<'de, IndexFormat> for Date {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> DecodeResult<Self> {
		BorrowDecode::<'de, ()>::borrow_decode(r)
	}
}

impl Encode<IndexIdFormat> for Date {
	fn encode<W: std::io::Write>(&self, w: &mut storekey::Writer<W>) -> EncodeResult {
		Encode::<()>::encode(self, w)
	}
}

impl<'de> BorrowDecode<'de, IndexIdFormat> for Date {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> DecodeResult<Self> {
		BorrowDecode::<'de, ()>::borrow_decode(r)
	}
}

/// A time of day as nanoseconds since midnight, from `00:00:00` to `24:00:00` inclusive.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Time(pub u64);

impl Time {
	/// Nanoseconds in a day, the value of `24:00:00`.
	pub const NANOS_PER_DAY: u64 = 86_400_000_000_000;

	/// Creates a time from nanoseconds since midnight, or `None` if it is past `24:00:00`.
	pub fn from_nanos(nanos: u64) -> Option<Self> {
		(nanos <= Self::NANOS_PER_DAY).then_some(Self(nanos))
	}

	/// Whether the count is within `00:00:00` to `24:00:00`.
	pub fn is_valid(&self) -> bool {
		self.0 <= Self::NANOS_PER_DAY
	}

	/// The number of nanoseconds since midnight.
	pub fn nanos(&self) -> u64 {
		self.0
	}

	/// The time of day, or `None` for `24:00:00`.
	pub fn naive(&self) -> Option<NaiveTime> {
		surrealdb_types::Time::from(*self).into_inner()
	}
}

impl From<surrealdb_types::Time> for Time {
	fn from(v: surrealdb_types::Time) -> Self {
		Self(v.nanos())
	}
}

impl From<Time> for surrealdb_types::Time {
	fn from(v: Time) -> Self {
		surrealdb_types::Time::from_nanos(v.0).unwrap_or(surrealdb_types::Time::MAX)
	}
}

impl From<NaiveTime> for Time {
	fn from(v: NaiveTime) -> Self {
		surrealdb_types::Time::from(v).into()
	}
}

impl FromStr for Time {
	type Err = anyhow::Error;
	fn from_str(s: &str) -> Result<Self> {
		surrealdb_types::Time::from_str(s).map(Self::from)
	}
}

impl Display for Time {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		surrealdb_types::Time::from(*self).fmt(f)
	}
}

impl ToSql for Time {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		surrealdb_types::Time::from(*self).fmt_sql(f, fmt)
	}
}

impl Encode<()> for Time {
	fn encode<W: std::io::Write>(&self, w: &mut storekey::Writer<W>) -> EncodeResult {
		w.write_array(self.0.to_be_bytes())
	}
}

impl<'de> BorrowDecode<'de, ()> for Time {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> DecodeResult<Self> {
		Self::from_nanos(u64::from_be_bytes(r.read_array::<8>()?))
			.ok_or(storekey::DecodeError::InvalidFormat)
	}
}

impl Encode<IndexFormat> for Time {
	fn encode<W: std::io::Write>(&self, w: &mut storekey::Writer<W>) -> EncodeResult {
		Encode::<()>::encode(self, w)
	}
}

impl<'de> BorrowDecode<'de, IndexFormat> for Time {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> DecodeResult<Self> {
		BorrowDecode::<'de, ()>::borrow_decode(r)
	}
}

impl Encode<IndexIdFormat> for Time {
	fn encode<W: std::io::Write>(&self, w: &mut storekey::Writer<W>) -> EncodeResult {
		Encode::<()>::encode(self, w)
	}
}

impl<'de> BorrowDecode<'de, IndexIdFormat> for Time {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> DecodeResult<Self> {
		BorrowDecode::<'de, ()>::borrow_decode(r)
	}
}

/// A wall-clock date and time without a time zone, as seconds and nanoseconds since
/// 1970-01-01T00:00:00.
///
/// The pair is limited to the range of [`chrono::NaiveDateTime`]; a leap second carries
/// nanoseconds of one billion or more. Construct one through [`Timestamp::from_parts`] to
/// enforce that. The derived order is chronological.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Timestamp {
	pub secs: i64,
	pub nanos: u32,
}

impl Timestamp {
	/// Creates a timestamp, or `None` if the pair is out of range.
	pub fn from_parts(secs: i64, nanos: u32) -> Option<Self> {
		surrealdb_types::Timestamp::from_parts(secs, nanos).map(Self::from)
	}

	/// Whether the pair is within the supported range.
	pub fn is_valid(&self) -> bool {
		Self::from_parts(self.secs, self.nanos).is_some()
	}

	/// `infinity`, the greatest timestamp.
	pub fn infinity() -> Self {
		surrealdb_types::Timestamp::infinity().into()
	}

	/// `-infinity`, the least timestamp.
	pub fn neg_infinity() -> Self {
		surrealdb_types::Timestamp::neg_infinity().into()
	}

	/// Whether this timestamp is `infinity` or `-infinity`.
	pub fn is_infinite(&self) -> bool {
		surrealdb_types::Timestamp::from(*self).is_infinite()
	}

	/// The wall-clock date and time.
	pub fn naive(&self) -> NaiveDateTime {
		surrealdb_types::Timestamp::from(*self).into_inner()
	}
}

impl Timestamp {
	/// The date part.
	pub fn date(&self) -> Date {
		Date::from(self.naive().date())
	}

	/// The time-of-day part.
	pub fn time(&self) -> Time {
		Time::from(self.naive().time())
	}
}

impl From<Timestamp> for Datetime {
	/// The wall time read as UTC.
	fn from(v: Timestamp) -> Self {
		Datetime(v.naive().and_utc())
	}
}

impl From<Datetime> for Timestamp {
	/// The UTC wall time.
	fn from(v: Datetime) -> Self {
		Timestamp::from(v.0.naive_utc())
	}
}

impl From<Date> for Datetime {
	/// Midnight UTC of the date.
	fn from(v: Date) -> Self {
		Datetime::from(v.at_midnight())
	}
}

impl Datetime {
	/// The UTC calendar date.
	pub fn date(&self) -> Date {
		Date::from(self.0.date_naive())
	}

	/// The UTC time of day.
	pub fn time(&self) -> Time {
		Time::from(self.0.time())
	}

	/// The UTC calendar date when the instant is exactly 00:00:00 UTC.
	pub fn exact_date(&self) -> Option<Date> {
		(self.0.time() == NaiveTime::MIN).then(|| self.date())
	}
}

impl From<surrealdb_types::Timestamp> for Timestamp {
	fn from(v: surrealdb_types::Timestamp) -> Self {
		Self {
			secs: v.seconds(),
			nanos: v.nanos(),
		}
	}
}

impl From<Timestamp> for surrealdb_types::Timestamp {
	fn from(v: Timestamp) -> Self {
		surrealdb_types::Timestamp::from_parts(v.secs, v.nanos)
			.unwrap_or_else(|| surrealdb_types::Timestamp::from(NaiveDateTime::MIN))
	}
}

impl From<NaiveDateTime> for Timestamp {
	fn from(v: NaiveDateTime) -> Self {
		surrealdb_types::Timestamp::from(v).into()
	}
}

impl FromStr for Timestamp {
	type Err = anyhow::Error;
	fn from_str(s: &str) -> Result<Self> {
		surrealdb_types::Timestamp::from_str(s).map(Self::from)
	}
}

impl Display for Timestamp {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		surrealdb_types::Timestamp::from(*self).fmt(f)
	}
}

impl ToSql for Timestamp {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		surrealdb_types::Timestamp::from(*self).fmt_sql(f, fmt)
	}
}

impl Encode<()> for Timestamp {
	fn encode<W: std::io::Write>(&self, w: &mut storekey::Writer<W>) -> EncodeResult {
		w.write_array(flip_i64(self.secs))?;
		w.write_array(self.nanos.to_be_bytes())
	}
}

impl<'de> BorrowDecode<'de, ()> for Timestamp {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> DecodeResult<Self> {
		let secs = unflip_i64(r.read_array::<8>()?);
		let nanos = u32::from_be_bytes(r.read_array::<4>()?);
		Self::from_parts(secs, nanos).ok_or(storekey::DecodeError::InvalidFormat)
	}
}

impl Encode<IndexFormat> for Timestamp {
	fn encode<W: std::io::Write>(&self, w: &mut storekey::Writer<W>) -> EncodeResult {
		Encode::<()>::encode(self, w)
	}
}

impl<'de> BorrowDecode<'de, IndexFormat> for Timestamp {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> DecodeResult<Self> {
		BorrowDecode::<'de, ()>::borrow_decode(r)
	}
}

impl Encode<IndexIdFormat> for Timestamp {
	fn encode<W: std::io::Write>(&self, w: &mut storekey::Writer<W>) -> EncodeResult {
		Encode::<()>::encode(self, w)
	}
}

impl<'de> BorrowDecode<'de, IndexIdFormat> for Timestamp {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> DecodeResult<Self> {
		BorrowDecode::<'de, ()>::borrow_decode(r)
	}
}

/// A calendar-aware interval of months, days, and microseconds.
///
/// Equality, ordering, and hashing use the normalised total in which a month is 30 days and a
/// day is 24 hours, so `1 month` equals `30 days`. [`Interval::identical`] compares the
/// components as written.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Default)]
pub struct Interval {
	pub months: i32,
	pub days: i32,
	pub micros: i64,
}

impl Interval {
	/// Creates an interval from its components.
	pub const fn new(months: i32, days: i32, micros: i64) -> Self {
		Self {
			months,
			days,
			micros,
		}
	}

	/// The total in microseconds in which a month is 30 days and a day is 24 hours. Every
	/// component is widened to `i128` before it is multiplied.
	pub fn normalised_micros(&self) -> i128 {
		i128::from(self.months) * MICROS_PER_MONTH
			+ i128::from(self.days) * MICROS_PER_DAY
			+ i128::from(self.micros)
	}

	/// Whether both intervals carry the same components, which is stricter than equality.
	pub fn identical(&self, other: &Self) -> bool {
		self.months == other.months && self.days == other.days && self.micros == other.micros
	}

	/// An interval with the given normalised total, spelled in the fewest larger units that
	/// hold it, or `None` if no component split fits.
	pub fn from_normalised_micros(total: i128) -> Option<Self> {
		let clamp = |v: i128| v.clamp(i128::from(i32::MIN), i128::from(i32::MAX));
		for months in [0, clamp(total / MICROS_PER_MONTH)] {
			let rest = total - months * MICROS_PER_MONTH;
			for days in [0, clamp(rest / MICROS_PER_DAY)] {
				let micros = rest - days * MICROS_PER_DAY;
				if let Ok(micros) = i64::try_from(micros) {
					return Some(Self::new(months as i32, days as i32, micros));
				}
			}
		}
		None
	}
}

impl PartialEq for Interval {
	fn eq(&self, other: &Self) -> bool {
		self.normalised_micros() == other.normalised_micros()
	}
}

impl Eq for Interval {}

impl PartialOrd for Interval {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Interval {
	fn cmp(&self, other: &Self) -> Ordering {
		self.normalised_micros().cmp(&other.normalised_micros())
	}
}

impl Hash for Interval {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.normalised_micros().hash(state);
	}
}

impl From<surrealdb_types::Interval> for Interval {
	fn from(v: surrealdb_types::Interval) -> Self {
		Self::new(v.months(), v.days(), v.micros())
	}
}

impl From<Interval> for surrealdb_types::Interval {
	fn from(v: Interval) -> Self {
		surrealdb_types::Interval::new(v.months, v.days, v.micros)
	}
}

impl FromStr for Interval {
	type Err = anyhow::Error;
	fn from_str(s: &str) -> Result<Self> {
		surrealdb_types::Interval::from_str(s).map(Self::from)
	}
}

impl Display for Interval {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		surrealdb_types::Interval::from(*self).fmt(f)
	}
}

impl ToSql for Interval {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		surrealdb_types::Interval::from(*self).fmt_sql(f, fmt)
	}
}

/// Under the key and index formats an interval is its normalised total as a sign-flipped
/// big-endian `i128` (16 bytes), so `1 mon` and `30 days` are one key. The components cannot be
/// recovered from the key, so a decoded interval is the total in its fewest larger units.
impl Encode<()> for Interval {
	fn encode<W: std::io::Write>(&self, w: &mut storekey::Writer<W>) -> EncodeResult {
		w.write_array(flip_i128(self.normalised_micros()))
	}
}

impl<'de> BorrowDecode<'de, ()> for Interval {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> DecodeResult<Self> {
		Self::from_normalised_micros(unflip_i128(r.read_array::<16>()?))
			.ok_or(storekey::DecodeError::InvalidFormat)
	}
}

impl Encode<IndexFormat> for Interval {
	fn encode<W: std::io::Write>(&self, w: &mut storekey::Writer<W>) -> EncodeResult {
		Encode::<()>::encode(self, w)
	}
}

impl<'de> BorrowDecode<'de, IndexFormat> for Interval {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> DecodeResult<Self> {
		BorrowDecode::<'de, ()>::borrow_decode(r)
	}
}

/// Under [`IndexIdFormat`], the spelling of a record-id component, the normalised total is
/// followed by the exact components (4, 4, and 8 bytes, each sign-flipped), so a record id
/// stays addressable by the interval as written.
impl Encode<IndexIdFormat> for Interval {
	fn encode<W: std::io::Write>(&self, w: &mut storekey::Writer<W>) -> EncodeResult {
		Encode::<()>::encode(self, w)?;
		w.write_array(flip_i32(self.months))?;
		w.write_array(flip_i32(self.days))?;
		w.write_array(flip_i64(self.micros))
	}
}

impl<'de> BorrowDecode<'de, IndexIdFormat> for Interval {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> DecodeResult<Self> {
		let total = unflip_i128(r.read_array::<16>()?);
		let months = unflip_i32(r.read_array::<4>()?);
		let days = unflip_i32(r.read_array::<4>()?);
		let micros = unflip_i64(r.read_array::<8>()?);
		let v = Self::new(months, days, micros);
		if v.normalised_micros() != total {
			return Err(storekey::DecodeError::InvalidFormat);
		}
		Ok(v)
	}
}

#[cfg(feature = "arbitrary")]
mod arb {
	use arbitrary::{Arbitrary, Unstructured};

	use super::*;

	impl<'a> Arbitrary<'a> for Date {
		fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
			Ok(Self::from(u.arbitrary::<surrealdb_types::Date>()?))
		}
	}

	impl<'a> Arbitrary<'a> for Time {
		fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
			Ok(Self::from(u.arbitrary::<surrealdb_types::Time>()?))
		}
	}

	impl<'a> Arbitrary<'a> for Timestamp {
		fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
			Ok(Self::from(u.arbitrary::<surrealdb_types::Timestamp>()?))
		}
	}

	impl<'a> Arbitrary<'a> for Interval {
		fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
			Ok(Self::new(u.arbitrary()?, u.arbitrary()?, u.arbitrary()?))
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn key<E: Encode<()>>(v: &E) -> Vec<u8> {
		storekey::encode_vec_format::<(), _>(v).unwrap()
	}

	fn index_key<E: Encode<IndexFormat>>(v: &E) -> Vec<u8> {
		storekey::encode_vec_format::<IndexFormat, _>(v).unwrap()
	}

	fn id_key<E: Encode<IndexIdFormat>>(v: &E) -> Vec<u8> {
		storekey::encode_vec_format::<IndexIdFormat, _>(v).unwrap()
	}

	#[test]
	fn date_keys_are_chronological() {
		let days =
			[Date::from(NaiveDate::MIN), Date(-1), Date(0), Date(1), Date::from(NaiveDate::MAX)];
		for pair in days.windows(2) {
			assert!(key(&pair[0]) < key(&pair[1]));
			assert!(index_key(&pair[0]) < index_key(&pair[1]));
			assert!(id_key(&pair[0]) < id_key(&pair[1]));
		}
		for d in days {
			assert_eq!(storekey::decode_borrow_format::<(), Date>(&key(&d)).unwrap(), d);
		}
	}

	#[test]
	fn time_keys_are_chronological() {
		let times = [Time(0), Time(1), Time(86_399_999_999_999), Time(Time::NANOS_PER_DAY)];
		for pair in times.windows(2) {
			assert!(index_key(&pair[0]) < index_key(&pair[1]));
		}
		for t in times {
			assert_eq!(
				storekey::decode_borrow_format::<IndexFormat, Time>(&index_key(&t)).unwrap(),
				t
			);
		}
	}

	#[test]
	fn timestamp_keys_are_chronological() {
		let min = Timestamp::from(NaiveDateTime::MIN);
		let max = Timestamp::from(NaiveDateTime::MAX);
		let stamps = [
			min,
			Timestamp::from_parts(-1, 999_999_999).unwrap(),
			Timestamp::from_parts(0, 0).unwrap(),
			Timestamp::from_parts(0, 1).unwrap(),
			Timestamp::from_parts(1, 0).unwrap(),
			max,
		];
		for pair in stamps.windows(2) {
			assert!(pair[0] < pair[1]);
			assert!(index_key(&pair[0]) < index_key(&pair[1]));
			assert!(id_key(&pair[0]) < id_key(&pair[1]));
		}
		for t in stamps {
			assert_eq!(storekey::decode_borrow_format::<(), Timestamp>(&key(&t)).unwrap(), t);
		}
	}

	#[test]
	fn interval_keys_use_the_normalised_total() {
		let month = Interval::new(1, 0, 0);
		let days = Interval::new(0, 30, 0);
		assert_eq!(index_key(&month), index_key(&days));
		assert_eq!(index_key(&month).len(), 16);
		assert_ne!(id_key(&month), id_key(&days));
		assert_eq!(id_key(&month).len(), 32);
		let ascending = [
			Interval::new(i32::MIN, i32::MIN, i64::MIN),
			Interval::new(0, 0, -1),
			Interval::new(0, 0, 0),
			Interval::new(0, 0, 1),
			Interval::new(0, 1, 0),
			Interval::new(1, 0, 0),
			Interval::new(i32::MAX, i32::MAX, i64::MAX),
		];
		for pair in ascending.windows(2) {
			assert!(pair[0] < pair[1]);
			assert!(index_key(&pair[0]) < index_key(&pair[1]));
		}
		for v in ascending {
			let back =
				storekey::decode_borrow_format::<IndexFormat, Interval>(&index_key(&v)).unwrap();
			assert_eq!(back, v);
			let exact =
				storekey::decode_borrow_format::<IndexIdFormat, Interval>(&id_key(&v)).unwrap();
			assert!(exact.identical(&v));
		}
	}
}
