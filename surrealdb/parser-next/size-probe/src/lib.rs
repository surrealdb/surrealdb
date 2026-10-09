//! A wasm entry point around one parser; see `Cargo.toml`.
//!
//! `probe_alloc` hands the host a buffer for the source and `probe_parse`
//! parses it and returns a count derived from the result, so the optimiser
//! keeps the whole parser reachable.

/// Returns a buffer of `len` bytes the host writes the source into.
#[unsafe(no_mangle)]
pub extern "C" fn probe_alloc(len: usize) -> *mut u8 {
	let mut v = Vec::<u8>::with_capacity(len);
	let p = v.as_mut_ptr();
	core::mem::forget(v);
	p
}

#[cfg_attr(not(feature = "next"), allow(dead_code))]
fn source<'a>(ptr: *const u8, len: usize) -> Option<&'a str> {
	// SAFETY: the host passes a buffer from `probe_alloc` holding `len`
	// initialised bytes, which stays alive for the call.
	let bytes = unsafe { core::slice::from_raw_parts(ptr, len) };
	core::str::from_utf8(bytes).ok()
}

/// Parses the source and returns the number of top-level statements, or
/// `u32::MAX` on a parse error.
#[cfg(feature = "next")]
#[unsafe(no_mangle)]
pub extern "C" fn probe_parse(ptr: *const u8, len: usize) -> u32 {
	let Some(src) = source(ptr, len) else {
		return u32::MAX;
	};
	let (ast, diags) = surrealdb_parser_next::parse(src);
	if diags.is_empty() {
		ast.stmts.len() as u32
	} else {
		u32::MAX - diags[0].start
	}
}

/// Streams the source in 4 KiB chunks and returns the number of statements,
/// or `u32::MAX` on a stream error, so the streaming layer is measured too.
#[cfg(feature = "next")]
#[unsafe(no_mangle)]
pub extern "C" fn probe_stream(ptr: *const u8, len: usize) -> u32 {
	use surrealdb_parser_next::{Limits, Step, StreamParser};
	// SAFETY: as in `source`.
	let bytes = unsafe { core::slice::from_raw_parts(ptr, len) };
	let mut sp = StreamParser::new(Limits::default(), 1 << 26);
	let mut n = 0u32;
	let mut chunks = bytes.chunks(4096);
	loop {
		match chunks.next() {
			Some(c) => sp.feed(c),
			None => sp.finish(),
		}
		loop {
			match sp.next_statement() {
				Ok(Step::Statement(_)) => n += 1,
				Ok(Step::NeedMore) => break,
				Ok(Step::End) => return n,
				Err(_) => return u32::MAX,
			}
		}
	}
}
