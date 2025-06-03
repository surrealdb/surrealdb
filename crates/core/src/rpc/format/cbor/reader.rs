use super::{err::Error, major::Major, simple::Simple, tags::Tag};
use half::f16;

pub struct Reader<'a>(&'a [u8], usize);

impl<'a> Reader<'a> {
	pub fn new(buf: &'a [u8]) -> Reader<'a> {
		Self(buf, 0)
	}

	pub fn peek(&mut self) -> Result<u8, Error> {
		let Some(x) = self.0.get(self.1) else {
			return Err(Error::OutOfBounds {
				byte: self.1,
				len: self.0.len(),
			});
		};

		Ok(*x)
	}

	pub fn pop_peek(&mut self) -> Result<(), Error> {
		if self.0.get(self.1).is_none() {
			return Err(Error::OutOfBounds {
				byte: self.1,
				len: self.0.len(),
			});
		}
		self.1 += 1;
		Ok(())
	}

	pub fn read_u8(&mut self) -> Result<u8, Error> {
		let Some(x) = self.0.get(self.1) else {
			return Err(Error::OutOfBounds {
				byte: self.1,
				len: self.0.len(),
			});
		};
		self.1 += 1;
		Ok(*x)
	}

	pub fn read_u16(&mut self) -> Result<u16, Error> {
		let bytes = self.0.get(self.1..self.1 + 2).ok_or(Error::OutOfBounds {
			byte: self.1 + 1,
			len: self.0.len(),
		})?;
		self.1 += 2;
		Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
	}

	pub fn read_u32(&mut self) -> Result<u32, Error> {
		let bytes = self.0.get(self.1..self.1 + 4).ok_or(Error::OutOfBounds {
			byte: self.1 + 3,
			len: self.0.len(),
		})?;
		self.1 += 4;
		Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
	}

	pub fn read_u64(&mut self) -> Result<u64, Error> {
		let bytes = self.0.get(self.1..self.1 + 8).ok_or(Error::OutOfBounds {
			byte: self.1 + 7,
			len: self.0.len(),
		})?;
		self.1 += 8;
		Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
	}

	pub fn read_f16(&mut self) -> Result<f16, Error> {
		Ok(f16::from_bits(self.read_u16()?))
	}

	pub fn read_f32(&mut self) -> Result<f32, Error> {
		Ok(f32::from_bits(self.read_u32()?))
	}

	pub fn read_f64(&mut self) -> Result<f64, Error> {
		Ok(f64::from_bits(self.read_u64()?))
	}

	pub fn read_bytes(&mut self, len: usize) -> Result<&[u8], Error> {
		let bytes = self.0.get(self.1..self.1 + len).ok_or(Error::OutOfBounds {
			byte: self.1 + len - 1,
			len: self.0.len(),
		})?;
		self.1 += len;
		Ok(bytes)
	}

	pub fn read_bytes_infinite(&mut self, for_major: impl Into<u8>) -> Result<Vec<u8>, Error> {
		let mut bytes: Vec<u8> = Vec::new();
		let for_major: u8 = for_major.into();

		loop {
			let (major, len) = self.read_major_raw()?;

			// CBOR simple break
			if major == 7 && len == 31 {
				break;
			}

			if major != for_major {
				return Err(Error::InvalidChunkMajor {
					found: major,
					expected: for_major,
				});
			}

			if len == 31 {
				return Err(Error::UnexpectedInfiniteValue);
			}

			let chunk_len = self.read_major_length(len)?;
			let slice = self.read_bytes(chunk_len)?;
			bytes.extend_from_slice(slice);
		}

		Ok(bytes)
	}

	pub fn read_major(&mut self) -> Result<Major, Error> {
		let (major, len) = self.read_major_raw()?;
		match major {
			0 => Ok(Major::Positive(self.read_major_length(len)? as i64)),
			1 => Ok(Major::Negative(-(self.read_major_length(len)? as i64))),
			2 => Ok(Major::Bytes(len)),
			3 => Ok(Major::Text(len)),
			4 => Ok(Major::Array(len)),
			5 => Ok(Major::Map(len)),
			6 => Ok(Major::Tagged(Tag::try_from(self.read_major_length(len)? as u64)?)),
			7 => Ok(Major::Simple(Simple::try_from(len)?)),
			major => Err(Error::InvalidMajor(major)),
		}
	}

	pub fn read_major_raw(&mut self) -> Result<(u8, u8), Error> {
		let byte = self.read_u8()?;
		let major = byte >> 5;
		let len = byte & 0x1f;
		Ok((major, len))
	}

	pub fn read_major_length(&mut self, len: u8) -> Result<usize, Error> {
		match len {
			len if len <= 23 => Ok(len as usize),
			24 => Ok(self.read_u8()? as usize),
			25 => Ok(self.read_u16()? as usize),
			26 => Ok(self.read_u32()? as usize),
			27 => Ok(self.read_u64()? as usize),
			len => Err(Error::InvalidMajorLength(len)),
		}
	}
}
