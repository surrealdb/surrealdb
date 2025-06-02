use half::f16;

use super::{simple::Simple, tags::Tag};

pub struct Writer {
	buf: Vec<u8>,
	pos: usize,
}

impl Writer {
	pub fn new(initial_capacity: usize) -> Self {
		Self {
			buf: Vec::with_capacity(initial_capacity),
			pos: 0,
		}
	}

	#[inline]
	fn claim(&mut self, len: usize) -> usize {
		let pos = self.pos;
		self.pos += len;
		if self.buf.len() < self.pos {
			self.buf.resize(self.pos.next_power_of_two(), 0);
		}
		pos
	}

	pub fn write_u8(&mut self, value: u8) {
		let pos = self.claim(1);
		self.buf[pos] = value;
	}

	pub fn write_u16(&mut self, value: u16) {
		let pos = self.claim(2);
		self.buf[pos..pos + 2].copy_from_slice(&value.to_be_bytes());
	}

	pub fn write_u32(&mut self, value: u32) {
		let pos = self.claim(4);
		self.buf[pos..pos + 4].copy_from_slice(&value.to_be_bytes());
	}

	pub fn write_u64(&mut self, value: u64) {
		let pos = self.claim(8);
		self.buf[pos..pos + 8].copy_from_slice(&value.to_be_bytes());
	}

	pub fn write_f16(&mut self, value: f16) {
		self.write_u16(value.to_bits());
	}

	pub fn write_f32(&mut self, value: f32) {
		self.write_u32(value.to_bits());
	}

	pub fn write_f64(&mut self, value: f64) {
		self.write_u64(value.to_bits());
	}

	pub fn write_bytes(&mut self, data: &[u8]) {
		let pos = self.claim(data.len());
		self.buf[pos..pos + data.len()].copy_from_slice(data);
	}

	pub fn write_major(&mut self, major: u8, len: u64) {
		let base = major << 5;
		match len {
			0..=23 => self.write_u8(base + len as u8),
			0..=0xFF => {
				self.write_u8(base + 24);
				self.write_u8(len as u8);
			}
			0x100..=0xFFFF => {
				self.write_u8(base + 25);
				self.write_u16(len as u16);
			}
			0x1_0000..=0xFFFF_FFFF => {
				self.write_u8(base + 26);
				self.write_u32(len as u32);
			}
			_ => {
				self.write_u8(base + 27);
				self.write_u64(len);
			}
		}
	}

	pub fn write_tag(&mut self, tag: Tag) {
		self.write_major(6, tag.into());
	}

	pub fn write_simple(&mut self, tag: Simple) {
		match tag {
			Simple::F16 => self.write_u8(0xF9),
			Simple::F32 => self.write_u8(0xFA),
			Simple::F64 => self.write_u8(0xFB),
			_ => self.write_major(7, tag.into()),
		}
	}

	pub fn buffer(&self) -> &[u8] {
		&self.buf[..self.pos]
	}

	pub fn into_inner(self) -> Vec<u8> {
		self.buf[..self.pos].to_vec()
	}
}

impl Default for Writer {
	fn default() -> Self {
		Self::new(256)
	}
}
