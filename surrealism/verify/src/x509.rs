//! Certificate parsing and path validation against an anchor list.
//!
//! An anchor list is trusted as a whole: the statement that carries it is the
//! installer's decision. Path validation therefore only has to show that a
//! certificate was issued by an anchor, that every issuer on the path is a CA,
//! and that every certificate on the path was valid at the attested time.

use x509_parser::certificate::X509Certificate;
use x509_parser::prelude::FromDer;

use crate::der::Stamp;
use crate::keys::PublicKey;

/// Extensions this crate understands, so may be critical: key usage, subject
/// alternative name, basic constraints and extended key usage.
const KNOWN_CRITICAL: &[&[u8]] =
	&[&[0x55, 0x1d, 0x0f], &[0x55, 0x1d, 0x11], &[0x55, 0x1d, 0x13], &[0x55, 0x1d, 0x25]];

/// Parses one DER certificate. Refuses trailing bytes, duplicate extensions
/// and critical extensions this crate does not understand.
pub(crate) fn parse(der: &[u8]) -> Result<X509Certificate<'_>, String> {
	let (rest, cert) =
		X509Certificate::from_der(der).map_err(|e| format!("malformed certificate: {e}"))?;
	if !rest.is_empty() {
		return Err("trailing bytes after certificate".into());
	}
	cert.extensions_map().map_err(|_| "certificate has duplicate extensions")?;
	if let Some(ext) =
		cert.extensions().iter().find(|e| e.critical && !KNOWN_CRITICAL.contains(&e.oid.as_bytes()))
	{
		return Err(format!("certificate has unsupported critical extension {}", ext.oid));
	}
	Ok(cert)
}

/// Parses an anchor list.
pub(crate) fn parse_all(ders: &[Vec<u8>]) -> Result<Vec<X509Certificate<'_>>, String> {
	ders.iter().map(|der| parse(der)).collect()
}

/// Whether `time` lies inside the certificate's validity, both ends included.
pub(crate) fn valid_at(cert: &X509Certificate<'_>, time: Stamp) -> bool {
	let validity = cert.validity();
	validity.not_before.timestamp() <= time.earliest
		&& time.latest <= validity.not_after.timestamp()
}

pub(crate) fn public_key(cert: &X509Certificate<'_>) -> Result<PublicKey, String> {
	PublicKey::from_spki(cert.public_key())
}

/// Whether `issuer` names and signed `child`.
fn issued_by(child: &X509Certificate<'_>, issuer: &X509Certificate<'_>) -> bool {
	let algorithm = child.signature_algorithm.algorithm.as_bytes();
	if child.issuer().as_raw() != issuer.subject().as_raw()
		|| child.tbs_certificate.signature.algorithm.as_bytes() != algorithm
	{
		return false;
	}
	let Ok(key) = public_key(issuer) else {
		return false;
	};
	key.verify_ecdsa(algorithm, child.tbs_certificate.as_ref(), &child.signature_value.data)
}

/// Whether the certificate may issue certificates.
fn is_ca(cert: &X509Certificate<'_>) -> bool {
	let ca = matches!(cert.basic_constraints(), Ok(Some(bc)) if bc.value.ca);
	let may_sign = match cert.key_usage() {
		Ok(Some(ku)) => ku.value.key_cert_sign(),
		Ok(None) => true,
		Err(_) => false,
	};
	ca && may_sign
}

/// Validates the path from `start` up through `anchors` at `time`.
///
/// With `start_is_anchor` false, `start` must be issued by an anchor. With it
/// true, `start` is itself trusted and the walk only checks the issuers it can
/// find. Either way every certificate reached, `start` included, must be valid
/// at `time`, and every issuer must be a CA. Where several anchors name and
/// signed the current certificate, such as a re-issued intermediate, the walk
/// takes one that is a CA valid at `time`. It ends at a self-issued anchor or
/// at an anchor whose issuer is not in the list.
pub(crate) fn validate_path(
	start: &X509Certificate<'_>,
	start_is_anchor: bool,
	anchors: &[X509Certificate<'_>],
	time: Stamp,
) -> Result<(), String> {
	if !valid_at(start, time) {
		return Err("certificate is not valid at the signing time".into());
	}
	let mut current = start;
	let mut first = true;
	// A path through distinct anchors has at most `anchors.len()` steps; a walk
	// that has not ended by then is going round a cycle of anchors.
	for _ in 0..=anchors.len() {
		let mut issuers = anchors.iter().filter(|a| issued_by(current, a)).peekable();
		let Some(&fallback) = issuers.peek() else {
			return if first && !start_is_anchor {
				Err("certificate is not issued by any anchor certificate".into())
			} else {
				Ok(())
			};
		};
		let Some(issuer) = issuers.find(|a| is_ca(a) && valid_at(a, time)) else {
			return Err(if is_ca(fallback) {
				"an issuing certificate is not valid at the signing time".into()
			} else {
				"an issuing certificate is not a certificate authority".into()
			});
		};
		if issuer.subject().as_raw() == issuer.issuer().as_raw() {
			return Ok(());
		}
		current = issuer;
		first = false;
	}
	Err("anchor certificates form a cycle".into())
}
