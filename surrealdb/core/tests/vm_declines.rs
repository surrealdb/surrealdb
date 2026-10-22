#![recursion_limit = "256"]
#![deny(clippy::wildcard_enum_match_arm)]
//! The VM runs every statement the fuzzer's generator writes: no statement of a
//! generated script is answered with the engine's decline ("The engine cannot
//! run this statement"), which a statement the language accepts must never
//! meet.
//!
//! The scripts are those `fuzz_vm_executor` generates
//! (`fuzz/fuzz_targets/vm_script.rs`), each from bytes drawn from a seeded
//! generator, so a run is reproducible: script `n` of a run is the same script
//! every time. `VM_DECLINE_SCRIPTS` sets how many scripts run (300 by
//! default), `VM_DECLINE_SEED` the first script's number, and
//! `VM_DECLINE_THREADS` how many threads run them.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

#[path = "../../../fuzz/fuzz_targets/vm_script.rs"]
#[allow(dead_code)]
mod vm_script;

/// The error a generated script's draw ends with: the generator never runs out
/// of bytes here, so no draw fails.
#[derive(Debug)]
struct Exhausted;

type Result<T> = std::result::Result<T, Exhausted>;

/// The draws of `arbitrary`'s `Unstructured` the generator makes, over a byte
/// string: each takes the bytes it needs from the front, and reads zeroes once
/// they run out.
struct Unstructured<'a> {
	data: &'a [u8],
}

/// An integer the generator draws in a range.
trait Draw: Copy {
	fn to_i128(self) -> i128;
	fn from_i128(value: i128) -> Self;
}

macro_rules! draw {
	($($t:ty),*) => {$(
		impl Draw for $t {
			fn to_i128(self) -> i128 {
				self as i128
			}
			fn from_i128(value: i128) -> Self {
				value as $t
			}
		}
	)*};
}

draw!(u32, u64, usize, i32, i64);

/// A value the generator draws whole.
trait Arbitrary: Sized {
	fn draw(u: &mut Unstructured<'_>) -> Self;
}

impl Arbitrary for bool {
	fn draw(u: &mut Unstructured<'_>) -> Self {
		u.word() & 1 == 1
	}
}

impl<'a> Unstructured<'a> {
	fn new(data: &'a [u8]) -> Self {
		Self {
			data,
		}
	}

	fn word(&mut self) -> u64 {
		let mut bytes = [0u8; 8];
		let n = self.data.len().min(8);
		bytes[..n].copy_from_slice(&self.data[..n]);
		self.data = &self.data[n..];
		u64::from_le_bytes(bytes)
	}

	fn int_in_range<T: Draw>(&mut self, range: std::ops::RangeInclusive<T>) -> Result<T> {
		let (lo, hi) = (range.start().to_i128(), range.end().to_i128());
		let span = (hi - lo + 1) as u128;
		Ok(T::from_i128(lo + (self.word() as u128 % span) as i128))
	}

	fn choose_index(&mut self, len: usize) -> Result<usize> {
		if len == 0 {
			return Err(Exhausted);
		}
		Ok((self.word() % len as u64) as usize)
	}

	fn ratio(&mut self, numerator: u32, denominator: u32) -> Result<bool> {
		Ok(self.word() % u64::from(denominator) < u64::from(numerator))
	}

	fn arbitrary<T: Arbitrary>(&mut self) -> Result<T> {
		Ok(T::draw(self))
	}
}

/// The bytes script `n` is generated from.
fn bytes(n: u64) -> Vec<u8> {
	let mut x = n.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
	(0..2048)
		.map(|_| {
			x ^= x << 13;
			x ^= x >> 7;
			x ^= x << 17;
			(x >> 24) as u8
		})
		.collect()
}

fn env(name: &str, default: usize) -> usize {
	std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// What the engine answers a statement it declines to run with.
const DECLINE: &str = "The engine cannot run this statement";

#[test]
fn the_vm_declines_no_generated_statement() {
	let scripts = env("VM_DECLINE_SCRIPTS", 300);
	let seed = env("VM_DECLINE_SEED", 0);
	let threads = env("VM_DECLINE_THREADS", 4).max(1);
	let next = AtomicUsize::new(seed);
	let declined: Mutex<Vec<String>> = Mutex::new(Vec::new());
	let ran = AtomicUsize::new(0);
	let statements = AtomicUsize::new(0);
	std::thread::scope(|scope| {
		for _ in 0..threads {
			scope.spawn(|| {
				loop {
					let n = next.fetch_add(1, Ordering::Relaxed);
					if n >= seed + scripts {
						break;
					}
					let data = bytes(n as u64);
					let Ok((setup, script, who)) = vm_script::generate(&data) else {
						continue;
					};
					if std::env::var_os("VM_DECLINE_PRINT").is_some() {
						eprintln!("--- script {n} as {who:?}\n{script}");
					}
					let Some(results) = vm_script::run(&setup, &script, who) else {
						continue;
					};
					ran.fetch_add(1, Ordering::Relaxed);
					statements.fetch_add(results.len(), Ordering::Relaxed);
					for (i, result) in results.iter().enumerate() {
						if result.contains("Internal database error") {
							eprintln!(
								"internal error: script {n} as {who:?}, statement {i}: {result}\n{script}"
							);
						}
						if result.contains(DECLINE) {
							declined.lock().unwrap().push(format!(
								"script {n} as {who:?}, statement {i}: {result}\n{script}"
							));
						}
					}
				}
			});
		}
	});
	let declined = declined.into_inner().unwrap();
	let (ran, statements) = (ran.into_inner(), statements.into_inner());
	eprintln!("{ran} of {scripts} scripts ran, {statements} statements");
	assert!(ran * 2 >= scripts, "only {ran} of {scripts} scripts ran");
	assert!(declined.is_empty(), "{} declines:\n\n{}", declined.len(), declined.join("\n\n"));
}
