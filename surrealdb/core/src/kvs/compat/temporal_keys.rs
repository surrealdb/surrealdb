//! Frozen bytes for the `storekey` encodings of `date`, `time`, `timestamp` and `interval`.
//!
//! An index entry encodes its values under `IndexFormat`, a record-id component under
//! `IndexIdFormat`, and any other key under `()`. The four kinds are chronological in every
//! format. An interval is its sign-flipped 16-byte normalised total under `()` and `IndexFormat`,
//! so intervals equal by that total share a key, and under `IndexIdFormat` the exact triple
//! follows the total, so a record id stays addressable by the interval as written. The bytes below
//! are permanent: each case is written as exactly these bytes and decodes from them.
//!
//! Never edit the frozen bytes and never regenerate them to make a failing test pass.

use chrono::{NaiveDate, NaiveDateTime};
use storekey::{BorrowDecode, Encode};

use crate::val::{Date, IndexFormat, IndexIdFormat, Interval, Time, Timestamp};

#[derive(Clone, Copy)]
enum Case {
	Date(Date),
	Time(Time),
	Timestamp(Timestamp),
	Interval(Interval),
}

fn encoded<F, E: Encode<F>>(value: &E) -> Vec<u8> {
	storekey::encode_vec_format::<F, _>(value).expect("encodes")
}

impl Case {
	/// The encodings under `()`, `IndexFormat` and `IndexIdFormat`.
	fn keys(&self) -> [Vec<u8>; 3] {
		match self {
			Case::Date(v) => {
				[encoded::<(), _>(v), encoded::<IndexFormat, _>(v), encoded::<IndexIdFormat, _>(v)]
			}
			Case::Time(v) => {
				[encoded::<(), _>(v), encoded::<IndexFormat, _>(v), encoded::<IndexIdFormat, _>(v)]
			}
			Case::Timestamp(v) => {
				[encoded::<(), _>(v), encoded::<IndexFormat, _>(v), encoded::<IndexIdFormat, _>(v)]
			}
			Case::Interval(v) => {
				[encoded::<(), _>(v), encoded::<IndexFormat, _>(v), encoded::<IndexIdFormat, _>(v)]
			}
		}
	}

	/// Whether `keys` decode to this case under each format. An interval decodes from the
	/// normalised total of `()` and `IndexFormat` to an interval equal to it, and from
	/// `IndexIdFormat` to exactly the triple written.
	fn decodes_from(&self, keys: [&[u8]; 3]) -> bool {
		fn back<F, T: for<'de> BorrowDecode<'de, F>>(key: &[u8]) -> Option<T> {
			storekey::decode_borrow_format::<F, T>(key).ok()
		}
		match self {
			Case::Date(v) => {
				back::<(), Date>(keys[0]) == Some(*v)
					&& back::<IndexFormat, Date>(keys[1]) == Some(*v)
					&& back::<IndexIdFormat, Date>(keys[2]) == Some(*v)
			}
			Case::Time(v) => {
				back::<(), Time>(keys[0]) == Some(*v)
					&& back::<IndexFormat, Time>(keys[1]) == Some(*v)
					&& back::<IndexIdFormat, Time>(keys[2]) == Some(*v)
			}
			Case::Timestamp(v) => {
				back::<(), Timestamp>(keys[0]) == Some(*v)
					&& back::<IndexFormat, Timestamp>(keys[1]) == Some(*v)
					&& back::<IndexIdFormat, Timestamp>(keys[2]) == Some(*v)
			}
			Case::Interval(v) => {
				back::<(), Interval>(keys[0]) == Some(*v)
					&& back::<IndexFormat, Interval>(keys[1]) == Some(*v)
					&& back::<IndexIdFormat, Interval>(keys[2]).is_some_and(|i| i.identical(v))
			}
		}
	}
}

fn cases() -> Vec<(&'static str, Case)> {
	let micros_per_hour = 3_600_000_000i64;
	vec![
		("date_epoch", Case::Date(Date(0))),
		("date_before_epoch", Case::Date(Date(-1))),
		("date_2026_10_28", Case::Date(Date::from(NaiveDate::from_ymd_opt(2026, 10, 28).unwrap()))),
		("date_min", Case::Date(Date::from(NaiveDate::MIN))),
		("date_max", Case::Date(Date::from(NaiveDate::MAX))),
		("time_midnight", Case::Time(Time(0))),
		("time_noon", Case::Time(Time(43_200_000_000_000))),
		("time_last_nanosecond", Case::Time(Time(86_399_999_999_999))),
		("time_end_of_day", Case::Time(Time(Time::NANOS_PER_DAY))),
		("timestamp_epoch", Case::Timestamp(Timestamp::from_parts(0, 0).unwrap())),
		(
			"timestamp_before_epoch",
			Case::Timestamp(Timestamp::from_parts(-1, 999_999_999).unwrap()),
		),
		(
			"timestamp_fraction",
			Case::Timestamp(Timestamp::from_parts(1_700_000_000, 123_456_789).unwrap()),
		),
		("timestamp_min", Case::Timestamp(Timestamp::from(NaiveDateTime::MIN))),
		("timestamp_max", Case::Timestamp(Timestamp::from(NaiveDateTime::MAX))),
		("interval_zero", Case::Interval(Interval::new(0, 0, 0))),
		("interval_negative_microsecond", Case::Interval(Interval::new(0, 0, -1))),
		("interval_one_month", Case::Interval(Interval::new(1, 0, 0))),
		("interval_thirty_days", Case::Interval(Interval::new(0, 30, 0))),
		("interval_720_hours", Case::Interval(Interval::new(0, 0, 720 * micros_per_hour))),
		("interval_mixed", Case::Interval(Interval::new(-1, 30, -4_000_001))),
		("interval_min", Case::Interval(Interval::new(i32::MIN, i32::MIN, i64::MIN))),
		("interval_max", Case::Interval(Interval::new(i32::MAX, i32::MAX, i64::MAX))),
	]
}

/// FROZEN. For each case, its key under `()`, `IndexFormat` and `IndexIdFormat`.
#[rustfmt::skip]
const KEYS: &[(&str, [&[u8]; 3])] = &[
	("date_epoch", [
		&[128, 0, 0, 0],
		&[128, 0, 0, 0],
		&[128, 0, 0, 0],
	]),
	("date_before_epoch", [
		&[127, 255, 255, 255],
		&[127, 255, 255, 255],
		&[127, 255, 255, 255],
	]),
	("date_2026_10_28", [
		&[128, 0, 81, 18],
		&[128, 0, 81, 18],
		&[128, 0, 81, 18],
	]),
	("date_min", [
		&[122, 64, 14, 116],
		&[122, 64, 14, 116],
		&[122, 64, 14, 116],
	]),
	("date_max", [
		&[133, 169, 252, 60],
		&[133, 169, 252, 60],
		&[133, 169, 252, 60],
	]),
	("time_midnight", [
		&[0, 0, 0, 0, 0, 0, 0, 0],
		&[0, 0, 0, 0, 0, 0, 0, 0],
		&[0, 0, 0, 0, 0, 0, 0, 0],
	]),
	("time_noon", [
		&[0, 0, 39, 74, 72, 167, 128, 0],
		&[0, 0, 39, 74, 72, 167, 128, 0],
		&[0, 0, 39, 74, 72, 167, 128, 0],
	]),
	("time_last_nanosecond", [
		&[0, 0, 78, 148, 145, 78, 255, 255],
		&[0, 0, 78, 148, 145, 78, 255, 255],
		&[0, 0, 78, 148, 145, 78, 255, 255],
	]),
	("time_end_of_day", [
		&[0, 0, 78, 148, 145, 79, 0, 0],
		&[0, 0, 78, 148, 145, 79, 0, 0],
		&[0, 0, 78, 148, 145, 79, 0, 0],
	]),
	("timestamp_epoch", [
		&[128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
		&[128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
		&[128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
	]),
	("timestamp_before_epoch", [
		&[127, 255, 255, 255, 255, 255, 255, 255, 59, 154, 201, 255],
		&[127, 255, 255, 255, 255, 255, 255, 255, 59, 154, 201, 255],
		&[127, 255, 255, 255, 255, 255, 255, 255, 59, 154, 201, 255],
	]),
	("timestamp_fraction", [
		&[128, 0, 0, 0, 101, 83, 241, 0, 7, 91, 205, 21],
		&[128, 0, 0, 0, 101, 83, 241, 0, 7, 91, 205, 21],
		&[128, 0, 0, 0, 101, 83, 241, 0, 7, 91, 205, 21],
	]),
	("timestamp_min", [
		&[127, 255, 248, 107, 115, 13, 238, 0, 0, 0, 0, 0],
		&[127, 255, 248, 107, 115, 13, 238, 0, 0, 0, 0, 0],
		&[127, 255, 248, 107, 115, 13, 238, 0, 0, 0, 0, 0],
	]),
	("timestamp_max", [
		&[128, 0, 7, 119, 154, 10, 107, 127, 59, 154, 201, 255],
		&[128, 0, 7, 119, 154, 10, 107, 127, 59, 154, 201, 255],
		&[128, 0, 7, 119, 154, 10, 107, 127, 59, 154, 201, 255],
	]),
	("interval_zero", [
		&[128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
		&[128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
		&[128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 128, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0],
	]),
	("interval_negative_microsecond", [
		&[127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255],
		&[127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255],
		&[127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 128, 0, 0, 0, 128, 0, 0, 0, 127, 255, 255, 255, 255, 255, 255, 255],
	]),
	("interval_one_month", [
		&[128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 91, 127, 61, 64, 0],
		&[128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 91, 127, 61, 64, 0],
		&[128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 91, 127, 61, 64, 0, 128, 0, 0, 1, 128, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0],
	]),
	("interval_thirty_days", [
		&[128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 91, 127, 61, 64, 0],
		&[128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 91, 127, 61, 64, 0],
		&[128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 91, 127, 61, 64, 0, 128, 0, 0, 0, 128, 0, 0, 30, 128, 0, 0, 0, 0, 0, 0, 0],
	]),
	("interval_720_hours", [
		&[128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 91, 127, 61, 64, 0],
		&[128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 91, 127, 61, 64, 0],
		&[128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 91, 127, 61, 64, 0, 128, 0, 0, 0, 128, 0, 0, 0, 128, 0, 2, 91, 127, 61, 64, 0],
	]),
	("interval_mixed", [
		&[127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 194, 246, 255],
		&[127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 194, 246, 255],
		&[127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 194, 246, 255, 127, 255, 255, 255, 128, 0, 0, 30, 127, 255, 255, 255, 255, 194, 246, 255],
	]),
	("interval_min", [
		&[127, 255, 255, 255, 255, 255, 254, 199, 177, 117, 176, 0, 0, 0, 0, 0],
		&[127, 255, 255, 255, 255, 255, 254, 199, 177, 117, 176, 0, 0, 0, 0, 0],
		&[127, 255, 255, 255, 255, 255, 254, 199, 177, 117, 176, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
	]),
	("interval_max", [
		&[128, 0, 0, 0, 0, 0, 1, 56, 78, 138, 77, 144, 98, 235, 95, 255],
		&[128, 0, 0, 0, 0, 0, 1, 56, 78, 138, 77, 144, 98, 235, 95, 255],
		&[128, 0, 0, 0, 0, 0, 1, 56, 78, 138, 77, 144, 98, 235, 95, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255],
	]),
];

fn frozen(name: &str) -> &'static [&'static [u8]; 3] {
	KEYS.iter()
		.find(|(n, _)| *n == name)
		.map(|(_, keys)| keys)
		.unwrap_or_else(|| panic!("no frozen bytes for {name}"))
}

#[test]
fn every_case_has_frozen_bytes() {
	let names: Vec<&str> = cases().iter().map(|(name, _)| *name).collect();
	let frozen: Vec<&str> = KEYS.iter().map(|(name, _)| *name).collect();
	assert_eq!(names, frozen);
}

#[test]
fn a_value_is_written_as_its_frozen_bytes() {
	for (name, case) in cases() {
		let keys = case.keys();
		for (written, frozen) in keys.iter().zip(frozen(name)) {
			assert_eq!(written, frozen, "{name} is written differently");
		}
	}
}

#[test]
fn frozen_bytes_decode() {
	for (name, case) in cases() {
		let keys = frozen(name);
		assert!(
			case.decodes_from([keys[0], keys[1], keys[2]]),
			"BACKWARDS COMPATIBILITY BROKEN: {name} decodes"
		);
	}
}

#[test]
fn equal_intervals_share_an_index_key_and_not_a_record_id_key() {
	let month = frozen("interval_one_month");
	for other in ["interval_thirty_days", "interval_720_hours"] {
		let other = frozen(other);
		assert_eq!(month[0], other[0]);
		assert_eq!(month[1], other[1]);
		assert_ne!(month[2], other[2]);
		assert_eq!(&month[2][..16], &other[2][..16]);
	}
}

#[test]
#[ignore = "prints the frozen fixtures; run by hand when a case is added"]
fn generate() {
	println!("const KEYS: &[(&str, [&[u8]; 3])] = &[");
	for (name, case) in cases() {
		println!("\t(\"{name}\", [");
		for key in case.keys() {
			let bytes: Vec<String> = key.iter().map(|b| b.to_string()).collect();
			println!("\t\t&[{}],", bytes.join(", "));
		}
		println!("\t]),");
	}
	println!("];");
}
