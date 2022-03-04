use super::*;
use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use serde::{Deserialize, Serialize};

// Default base key
pub const BASE: &str = "surreal";
// Ignore specifies an ignored field
pub const IGNORE: &str = "\x00";
// Prefix is the lowest char found in a key
pub const PREFIX: &str = "\x01";
// Suffix is the highest char found in a key
pub const SUFFIX: &str = "\x7f";

/// KV              {$kv}
/// NS              {$kv}!ns{$ns}
///
/// Namespace       {$kv}*{$ns}
/// NT              {$kv}*{$ns}!tk{$tk}
/// NU              {$kv}*{$ns}!us{$us}
/// DB              {$kv}*{$ns}!db{$db}
///
/// Database        {$kv}*{$ns}*{$db}
/// DT              {$kv}*{$ns}*{$db}!tk{$tk}
/// DU              {$kv}*{$ns}*{$db}!us{$us}
/// SC              {$kv}*{$ns}*{$db}!sc{$sc}
/// ST              {$kv}*{$ns}*{$db}!st{$sc}!tk{$tk}
///
/// TB              {$kv}*{$ns}*{$db}!tb{$tb}
///
/// Table           {$kv}*{$ns}*{$db}*{$tb}
/// FT              {$kv}*{$ns}*{$db}*{$tb}!ft{$ft}
/// FD              {$kv}*{$ns}*{$db}*{$tb}!fd{$fd}
/// EV              {$kv}*{$ns}*{$db}*{$tb}!ev{$ev}
/// IX              {$kv}*{$ns}*{$db}*{$tb}!ix{$ix}
/// LV              {$kv}*{$ns}*{$db}*{$tb}!lv{$lv}
///
/// Thing           {$kv}*{$ns}*{$db}*{$tb}*{$id}
///
/// Patch           {$kv}*{$ns}*{$db}*{$tb}~{$id}{$at}
///
/// Index           {$kv}*{$ns}*{$db}*{$tb}¤{$ix}{$fd}
/// Point           {$kv}*{$ns}*{$db}*{$tb}¤{$ix}{$fd}{$id}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Key {
	Ns(ns::Ns), // Namespace definition key
	Nt(nt::Nt), // Namespace token definition key
	Nu(nl::Nl), // Namespace login definition key
	Db(db::Db), // Database definition key
	Dt(dt::Dt), // Database token definition key
	Du(dl::Dl), // Database login definition key
	Sc(sc::Sc), // Scope definition key
	St(st::St), // Scope token definition key
	Tb(tb::Tb), // Table definition key
	Ft(ft::Ft), // Foreign table definition key
	Ev(ev::Ev), // Event definition key
	Fd(fd::Fd), // Field definition key
	Ix(ix::Ix), // Index definition key
	Lv(lv::Lv), // Live definition key
	Namespace,  // Namespace resource data key
	Database,   // Database resource data key
	Table,      // Table resource data key
	Thing,      // Thing resource data key
	Index,      // Index resource data key
	Point,      // Index resource data key
	Patch,      // Patch resource data key
	Edge,       // Edge resource data key
}

impl From<Key> for Vec<u8> {
	fn from(val: Key) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Key {
	fn from(val: Vec<u8>) -> Self {
		Key::decode(&val).unwrap()
	}
}

impl Key {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Key, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Key::Tb(tb::new("test", "test", "test"));
		let enc = Key::encode(&val).unwrap();
		let dec = Key::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
	#[test]
	fn sort() {
		use super::*;
		let less = Key::Tb(tb::new("test", "test", ""));
		let item = Key::Tb(tb::new("test", "test", "item"));
		let more = Key::Tb(tb::new("test", "test", "test"));
		assert!(less.encode().unwrap() < item.encode().unwrap());
		assert!(item.encode().unwrap() < more.encode().unwrap());
	}
}
