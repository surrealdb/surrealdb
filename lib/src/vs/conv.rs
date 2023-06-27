// u64_to_versionstamp converts a u64 to a 10-byte versionstamp
// assuming big-endian and the the last two bytes are zero.
pub fn u64_to_versionstamp(v: u64) -> [u8; 10] {
	let mut buf = [0; 10];
	buf[0] = (v >> 56) as u8;
	buf[1] = (v >> 48) as u8;
	buf[2] = (v >> 40) as u8;
	buf[3] = (v >> 32) as u8;
	buf[4] = (v >> 24) as u8;
	buf[5] = (v >> 16) as u8;
	buf[6] = (v >> 8) as u8;
	buf[7] = v as u8;
	buf
}

#[allow(unused)]
// u64_u16_to_versionstamp converts a u64 and a u16 to a 10-byte versionstamp
// assuming big-endian.
pub fn u64_u16_to_versionstamp(v: u64, v2: u16) -> [u8; 10] {
	let mut buf = [0; 10];
	buf[0] = (v >> 56) as u8;
	buf[1] = (v >> 48) as u8;
	buf[2] = (v >> 40) as u8;
	buf[3] = (v >> 32) as u8;
	buf[4] = (v >> 24) as u8;
	buf[5] = (v >> 16) as u8;
	buf[6] = (v >> 8) as u8;
	buf[7] = v as u8;
	buf[8] = (v2 >> 8) as u8;
	buf[9] = v2 as u8;
	buf
}

#[allow(unused)]
// u64_u16_to_versionstamp converts a u64 and a u16 to a 10-byte versionstamp
// assuming big-endian.
pub fn u16_u64_to_versionstamp(v: u16, v2: u64) -> [u8; 10] {
	let mut buf = [0; 10];
	buf[0] = (v >> 8) as u8;
	buf[1] = v as u8;
	buf[2] = (v2 >> 56) as u8;
	buf[3] = (v2 >> 48) as u8;
	buf[4] = (v2 >> 40) as u8;
	buf[5] = (v2 >> 32) as u8;
	buf[6] = (v2 >> 24) as u8;
	buf[7] = (v2 >> 16) as u8;
	buf[8] = (v2 >> 8) as u8;
	buf[9] = v2 as u8;
	buf
}

// u128_to_versionstamp converts a u128 to a 10-byte versionstamp
// assuming big-endian.
#[allow(unused)]
pub fn u128_to_versionstamp(v: u128) -> [u8; 10] {
	let mut buf = [0; 10];
	buf[0] = (v >> 72) as u8;
	buf[1] = (v >> 64) as u8;
	buf[2] = (v >> 56) as u8;
	buf[3] = (v >> 48) as u8;
	buf[4] = (v >> 40) as u8;
	buf[5] = (v >> 32) as u8;
	buf[6] = (v >> 24) as u8;
	buf[7] = (v >> 16) as u8;
	buf[8] = (v >> 8) as u8;
	buf[9] = v as u8;
	buf
}

// to_u128_be converts a 10-byte versionstamp to a u128 assuming big-endian.
// This is handy for human comparing versionstamps.
#[allow(unused)]
pub fn to_u128_be(vs: [u8; 10]) -> u128 {
	let mut buf = [0; 16];
	let mut i = 0;
	while i < 10 {
		buf[i + 6] = vs[i];
		i += 1;
	}
	u128::from_be_bytes(buf)
}

// to_u64_be converts a 10-byte versionstamp to a u64 assuming big-endian.
// Only the first 8 bytes are used.
#[allow(unused)]
pub fn to_u64_be(vs: [u8; 10]) -> u64 {
	let mut buf = [0; 8];
	let mut i = 0;
	while i < 8 {
		buf[i] = vs[i];
		i += 1;
	}
	u64::from_be_bytes(buf)
}

// to_u128_le converts a 10-byte versionstamp to a u128 assuming little-endian.
// This is handy for producing human-readable versions of versionstamps.
#[allow(unused)]
pub fn to_u128_le(vs: [u8; 10]) -> u128 {
	let mut buf = [0; 16];
	let mut i = 0;
	while i < 10 {
		buf[i] = vs[i];
		i += 1;
	}
	u128::from_be_bytes(buf)
}
