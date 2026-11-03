//! The exact bytes a bucket URL's signature covers.
//!
//! Two rules govern everything here, and both exist because a signature that
//! covers one spelling of a request while the store resolves another grants
//! access to an object nobody authorised:
//!
//! 1. **Sign the form that is resolved.** The path is normalised into an [`ObjectKey`] and
//!    re-encoded from that, never taken from the wire as-is. Axum percent-decodes path captures, so
//!    `a%2Fb` and `a/b` arrive identically; encoding from the resolved key makes both sides agree
//!    by construction. [`ObjectKey::new`] is what guarantees one key has one spelling, so this
//!    encoding is injective on every value it is handed.
//! 2. **Cover every query parameter.** Sorted, so a proxy that reorders them does not break a valid
//!    URL, and complete, so no parameter can be added, edited or dropped.

use std::fmt::Write as _;

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};

use super::{PresignOp, PresignParams, ScopeLevel};
use crate::buc::store::ObjectKey;

/// Identifies the layout of the signed blob.
///
/// A change to any rule in this module must change this string. Two builds that
/// disagree about the bytes would otherwise reject each other's URLs with a
/// signature mismatch, which reads as forgery rather than as a version skew.
const VERSION: &str = "SDBURL2";

/// Everything outside the RFC 3986 unreserved set is percent-encoded, `/`
/// included, so a segment can never introduce a path separator.
///
/// Spelled out rather than taken from one of the crate's presets. This set is
/// part of a signature: a preset that widened or narrowed in a later release
/// would silently change the bytes being signed and invalidate every URL
/// already issued. `NON_ALPHANUMERIC` is the crate's "encode everything"
/// starting point and the four removals below are the rest of the unreserved
/// set, so the definition is complete on its face.
const UNRESERVED: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'.').remove(b'_').remove(b'~');

/// Percent-encode one path segment.
fn encode_segment(out: &mut String, value: &str) {
	out.extend(utf8_percent_encode(value, UNRESERVED));
}

/// The request path for an object, encoded segment by segment: the path the
/// bucket route serves it at.
///
/// ```text
/// /bucket/{bucket}/{key}                   a bucket on the root
/// /ns/{ns}/bucket/{bucket}/{key}           a bucket on a namespace
/// /ns/{ns}/db/{db}/bucket/{bucket}/{key}   a bucket on a database
/// ```
///
/// The literal segments name the level, so buckets at two levels never share
/// a path.
///
/// `key` is already normalised, so its `/` separators are structural and are
/// the only ones emitted literally. The empty-segment filter is therefore a
/// belt-and-braces guard rather than a normalisation step: [`ObjectKey::new`]
/// has already collapsed them, and it must stay that way, because two keys
/// that differ only in separators would otherwise sign the same blob.
pub(crate) fn path(level: &ScopeLevel<'_>, bucket: &str, key: &ObjectKey) -> String {
	let mut out = String::new();
	match *level {
		ScopeLevel::Root => {}
		ScopeLevel::Namespace {
			name,
			..
		} => {
			out.push_str("/ns/");
			encode_segment(&mut out, name);
		}
		ScopeLevel::Database {
			ns_name,
			db_name,
			..
		} => {
			out.push_str("/ns/");
			encode_segment(&mut out, ns_name);
			out.push_str("/db/");
			encode_segment(&mut out, db_name);
		}
	}
	out.push_str("/bucket/");
	encode_segment(&mut out, bucket);
	for segment in key.as_str().split('/').filter(|s| !s.is_empty()) {
		out.push('/');
		encode_segment(&mut out, segment);
	}
	out
}

/// The query string, without `sig` and without a leading `?`.
///
/// Emitted in sorted key order. A parameter that does not apply is absent
/// rather than empty, so removing one changes the blob and breaks the
/// signature.
///
/// Assembled directly rather than through a form serializer. The schema is
/// four known keys whose values are integers, `false`, or a percent-encoded
/// media type, and `application/x-www-form-urlencoded` is a different
/// encoding from the one used for the path above — it spells a space `+`. Two encodings in
/// one canonical form is how a verifier ends up disagreeing with a signer.
pub(crate) fn query(params: &PresignParams) -> String {
	// Sorted by key: ct, exp, max, ow.
	let mut out = String::new();
	if let Some(content_type) = &params.content_type {
		// The one value that is caller text rather than a number. Encoded as a
		// path segment is, so it is unreserved-or-escaped and cannot introduce
		// a separator of its own.
		out.push_str("ct=");
		encode_segment(&mut out, content_type);
		out.push('&');
	}
	let _ = write!(out, "exp={}", params.exp);
	if let Some(max) = params.max {
		let _ = write!(out, "&max={max}");
	}
	if !params.overwrite {
		out.push_str("&ow=false");
	}
	out
}

/// The blob the signature is computed over.
pub(crate) fn blob(op: PresignOp, path: &str, query: &str) -> String {
	format!("{VERSION}\n{}\n{path}\n{query}", op.method())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};

	fn db<'a>(ns_name: &'a str, db_name: &'a str) -> ScopeLevel<'a> {
		ScopeLevel::Database {
			ns_id: NamespaceId(1),
			db_id: DatabaseId(1),
			ns_name,
			db_name,
		}
	}

	fn params() -> PresignParams {
		PresignParams {
			exp: 1756637700,
			content_type: None,
			max: Some(5242880),
			overwrite: true,
		}
	}

	#[test]
	fn a_segment_never_emits_a_separator() {
		// The property the whole scheme rests on: an object key containing a
		// slash, or a bucket name containing one, cannot restructure the path.
		let key = ObjectKey::new("/u/user:tobie/avatar.png");
		assert_eq!(
			path(&db("test", "app"), "images", &key),
			"/ns/test/db/app/bucket/images/u/user%3Atobie/avatar.png"
		);

		let odd = ObjectKey::new("/a%2Fb");
		assert_eq!(
			path(&db("test", "app"), "images", &odd),
			"/ns/test/db/app/bucket/images/a%252Fb"
		);
	}

	#[test]
	fn the_path_names_the_level() {
		let key = ObjectKey::new("/x");
		assert_eq!(path(&ScopeLevel::Root, "images", &key), "/bucket/images/x");
		let ns = ScopeLevel::Namespace {
			id: NamespaceId(1),
			name: "test",
		};
		assert_eq!(path(&ns, "images", &key), "/ns/test/bucket/images/x");
		assert_eq!(path(&db("test", "app"), "images", &key), "/ns/test/db/app/bucket/images/x");
	}

	#[test]
	fn an_awkward_name_is_encoded() {
		let key = ObjectKey::new("/x");
		assert_eq!(path(&db("n s", "d/b"), "b?u&c", &key), "/ns/n%20s/db/d%2Fb/bucket/b%3Fu%26c/x");
	}

	#[test]
	fn the_encoding_is_injective_on_normalised_keys() {
		// Two keys that encode to one path would let a URL authorised for one
		// redeem against the other, so the only keys that may share a path are
		// the ones `ObjectKey` has already proven equal.
		let spellings = ["//x//y/", "/x/y", "x/y", "/x/y/"];
		for s in spellings {
			assert_eq!(ObjectKey::new(s).as_str(), "/x/y", "{s}");
			assert_eq!(
				path(&db("a", "b"), "c", &ObjectKey::new(s)),
				path(&db("a", "b"), "c", &ObjectKey::new("/x/y"))
			);
		}

		// Distinct keys must stay distinct.
		assert_ne!(
			path(&db("a", "b"), "c", &ObjectKey::new("/x/y")),
			path(&db("a", "b"), "c", &ObjectKey::new("/x/y2"))
		);
	}

	#[test]
	fn query_is_sorted_and_omits_defaults() {
		assert_eq!(query(&params()), "exp=1756637700&max=5242880");

		let mut p = params();
		p.overwrite = false;
		assert_eq!(query(&p), "exp=1756637700&max=5242880&ow=false");

		let mut p = params();
		p.max = None;
		assert_eq!(query(&p), "exp=1756637700");

		// A content type sorts first, and every byte outside the unreserved
		// set is escaped, so `/`, `;` and a space cannot restructure the query.
		let mut p = params();
		p.content_type = Some("text/plain; charset=utf-8".to_owned());
		assert_eq!(query(&p), "ct=text%2Fplain%3B%20charset%3Dutf-8&exp=1756637700&max=5242880");
	}

	#[test]
	fn the_blob_names_its_own_version() {
		let q = query(&params());
		let b = blob(PresignOp::Put, "/bucket/c/d", &q);
		assert_eq!(b, "SDBURL2\nPUT\n/bucket/c/d\nexp=1756637700&max=5242880");
	}

	#[test]
	fn the_method_is_covered() {
		let q = query(&params());
		assert_ne!(blob(PresignOp::Get, "/p", &q), blob(PresignOp::Put, "/p", &q));
	}
}
