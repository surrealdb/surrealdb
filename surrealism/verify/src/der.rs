//! A minimal DER reader for the CMS and RFC 3161 structures of a timestamp.
//!
//! It reads definite-length, minimally encoded TLVs with single-byte tags and
//! hands out slices of the input. That subset is all DER allows for the
//! structures read here; anything outside it is an error, never a guess.
//! Certificates are parsed by `x509-parser`, not by this module.

pub(crate) const INTEGER: u8 = 0x02;
pub(crate) const OCTET_STRING: u8 = 0x04;
pub(crate) const NULL: u8 = 0x05;
pub(crate) const OID: u8 = 0x06;
pub(crate) const UTF8_STRING: u8 = 0x0c;
pub(crate) const GENERALIZED_TIME: u8 = 0x18;
pub(crate) const SEQUENCE: u8 = 0x30;
pub(crate) const SET: u8 = 0x31;
/// `[0]`, constructed.
pub(crate) const CONTEXT_0: u8 = 0xa0;
/// `[1]`, constructed.
pub(crate) const CONTEXT_1: u8 = 0xa1;
/// `[0]`, primitive.
pub(crate) const CONTEXT_0_PRIMITIVE: u8 = 0x80;

/// The longest length field accepted: four bytes, so values up to 4 GiB.
const MAX_LENGTH_BYTES: usize = 4;

/// One DER element.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Tlv<'a> {
	pub tag: u8,
	/// The contents, without tag and length.
	pub value: &'a [u8],
	/// The whole element, tag and length included.
	pub raw: &'a [u8],
}

/// Reads consecutive DER elements from a byte slice.
pub(crate) struct Reader<'a> {
	data: &'a [u8],
}

impl<'a> Reader<'a> {
	pub fn new(data: &'a [u8]) -> Self {
		Self {
			data,
		}
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	pub fn peek_tag(&self) -> Option<u8> {
		self.data.first().copied()
	}

	/// Reads the next element of any tag.
	pub fn read(&mut self) -> Result<Tlv<'a>, String> {
		let data = self.data;
		let (&tag, rest) = data.split_first().ok_or("unexpected end of DER data")?;
		if tag & 0x1f == 0x1f {
			return Err("multi-byte DER tags are not supported".into());
		}
		let (&first, mut rest) = rest.split_first().ok_or("truncated DER length")?;
		let len = if first < 0x80 {
			usize::from(first)
		} else {
			let count = usize::from(first & 0x7f);
			if count == 0 {
				return Err("indefinite DER length".into());
			}
			if count > MAX_LENGTH_BYTES || count > rest.len() {
				return Err("oversized or truncated DER length".into());
			}
			let (bytes, after) = rest.split_at(count);
			rest = after;
			if bytes[0] == 0 {
				return Err("non-minimal DER length".into());
			}
			let len = bytes.iter().fold(0usize, |acc, &b| (acc << 8) | usize::from(b));
			if len < 0x80 {
				return Err("non-minimal DER length".into());
			}
			len
		};
		if len > rest.len() {
			return Err("truncated DER value".into());
		}
		let header = data.len() - rest.len();
		let raw = &data[..header + len];
		self.data = &data[header + len..];
		Ok(Tlv {
			tag,
			value: &raw[header..],
			raw,
		})
	}

	/// Reads the next element and requires `tag`; returns its contents.
	pub fn expect(&mut self, tag: u8) -> Result<&'a [u8], String> {
		Ok(self.expect_tlv(tag)?.value)
	}

	/// Reads the next element and requires `tag`.
	pub fn expect_tlv(&mut self, tag: u8) -> Result<Tlv<'a>, String> {
		let tlv = self.read()?;
		if tlv.tag != tag {
			return Err(format!("expected DER tag {tag:#04x}, found {:#04x}", tlv.tag));
		}
		Ok(tlv)
	}

	/// Reads the next element if it carries `tag`.
	pub fn optional(&mut self, tag: u8) -> Result<Option<Tlv<'a>>, String> {
		if self.peek_tag() == Some(tag) {
			self.read().map(Some)
		} else {
			Ok(None)
		}
	}

	/// Requires that nothing is left.
	pub fn finish(&self) -> Result<(), String> {
		if self.data.is_empty() {
			Ok(())
		} else {
			Err("trailing DER data".into())
		}
	}
}

/// Parses `data` as exactly one element with `tag`; returns its contents.
pub(crate) fn single(data: &[u8], tag: u8) -> Result<&[u8], String> {
	let mut reader = Reader::new(data);
	let value = reader.expect(tag)?;
	reader.finish()?;
	Ok(value)
}

/// Reads an `AlgorithmIdentifier` and returns its OID contents. Parameters
/// must be absent or `NULL`, which holds for every hash and ECDSA algorithm.
pub(crate) fn algorithm(reader: &mut Reader<'_>) -> Result<Vec<u8>, String> {
	let mut alg = Reader::new(reader.expect(SEQUENCE)?);
	let oid = alg.expect(OID)?.to_vec();
	if !alg.is_empty() {
		alg.expect(NULL)?;
	}
	alg.finish()?;
	Ok(oid)
}

/// A stamped instant as the whole seconds that enclose it.
///
/// `earliest` is the instant truncated to the second and `latest` the instant
/// rounded up; they are equal when the time has no fraction. A check that the
/// instant lies inside a window compares `earliest` with the window's start and
/// `latest` with its end, so a fraction never moves the instant into a window
/// it lies outside.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Stamp {
	pub earliest: i64,
	pub latest: i64,
}

/// Parses a DER `GeneralizedTime` (`YYYYMMDDHHMMSS[.f+]Z`), as seconds since
/// the Unix epoch.
pub(crate) fn generalized_time(value: &[u8]) -> Result<Stamp, String> {
	let text = std::str::from_utf8(value).map_err(|_| "GeneralizedTime is not ASCII")?;
	let body = text.strip_suffix('Z').ok_or("GeneralizedTime must end in Z")?;
	let (whole, fraction) = match body.split_once('.') {
		Some((whole, fraction)) => (whole, Some(fraction)),
		None => (body, None),
	};
	if let Some(fraction) = fraction {
		// DER: at least one digit, no trailing zero.
		if fraction.is_empty()
			|| fraction.ends_with('0')
			|| !fraction.bytes().all(|b| b.is_ascii_digit())
		{
			return Err("malformed GeneralizedTime fraction".into());
		}
	}
	if whole.len() != 14 || !whole.bytes().all(|b| b.is_ascii_digit()) {
		return Err("malformed GeneralizedTime".into());
	}
	let field = |range: std::ops::Range<usize>| -> i64 {
		whole[range].bytes().fold(0, |acc, b| acc * 10 + i64::from(b - b'0'))
	};
	let (year, month, day) = (field(0..4), field(4..6), field(6..8));
	let (hour, minute, second) = (field(8..10), field(10..12), field(12..14));
	if !(1..=12).contains(&month)
		|| day < 1
		|| day > days_in_month(year, month)
		|| hour > 23
		|| minute > 59
		|| second > 59
	{
		return Err("GeneralizedTime out of range".into());
	}
	let earliest = days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second;
	Ok(Stamp {
		earliest,
		latest: earliest + i64::from(fraction.is_some()),
	})
}

fn days_in_month(year: i64, month: i64) -> i64 {
	match month {
		2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
		2 => 28,
		4 | 6 | 9 | 11 => 30,
		_ => 31,
	}
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's
/// `days_from_civil`).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
	let y = if month <= 2 {
		year - 1
	} else {
		year
	};
	let era = y.div_euclid(400);
	let yoe = y - era * 400;
	let mp = (month + 9) % 12;
	let doy = (153 * mp + 2) / 5 + day - 1;
	let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
	era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn reads_short_and_long_lengths() {
		let mut long = vec![OCTET_STRING, 0x81, 0x80];
		long.extend([7u8; 0x80]);
		let tlv = Reader::new(&long).read().unwrap();
		assert_eq!(tlv.value.len(), 0x80);
		assert_eq!(tlv.raw.len(), long.len());

		let short = [SEQUENCE, 0x02, NULL, 0x00];
		let mut reader = Reader::new(&short);
		let tlv = reader.expect_tlv(SEQUENCE).unwrap();
		assert_eq!(tlv.value, &[NULL, 0x00]);
		assert!(reader.finish().is_ok());
	}

	#[test]
	fn refuses_non_der_lengths() {
		// Indefinite.
		assert!(Reader::new(&[SEQUENCE, 0x80, 0, 0]).read().is_err());
		// Long form for a short length.
		assert!(Reader::new(&[OCTET_STRING, 0x81, 0x01, 0]).read().is_err());
		// Leading zero in the length.
		assert!(Reader::new(&[OCTET_STRING, 0x82, 0x00, 0x80]).read().is_err());
		// Truncated value.
		assert!(Reader::new(&[OCTET_STRING, 0x05, 0, 0]).read().is_err());
		// Multi-byte tag.
		assert!(Reader::new(&[0x1f, 0x81, 0x00, 0x00]).read().is_err());
	}

	#[test]
	fn parses_generalized_time() {
		let at = |earliest, latest| Stamp {
			earliest,
			latest,
		};
		assert_eq!(generalized_time(b"19700101000000Z").unwrap(), at(0, 0));
		assert_eq!(generalized_time(b"20250612120220Z").unwrap(), at(1_749_729_740, 1_749_729_740));
		assert_eq!(
			generalized_time(b"20250612120220.123Z").unwrap(),
			at(1_749_729_740, 1_749_729_741)
		);
		assert_eq!(generalized_time(b"20240229235959Z").unwrap(), at(1_709_251_199, 1_709_251_199));
		assert!(generalized_time(b"20250612120220.120Z").is_err());
		assert!(generalized_time(b"20250612120220").is_err());
		assert!(generalized_time(b"20230229000000Z").is_err());
		assert!(generalized_time(b"2025061212022Z").is_err());
	}
}
