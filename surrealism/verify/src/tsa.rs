//! RFC 3161 timestamps: the trusted signing time of a Sigstore bundle.
//!
//! A Rekor v2 entry carries no integrated time, so the time a signature
//! existed comes from a timestamp authority. The timestamp is a CMS
//! `SignedData` over a `TSTInfo` whose message imprint is the hash of the
//! bundle's signature bytes. Only the `tsa` anchor decides which key may sign
//! it: certificates embedded in the token are ignored.
//!
//! The signing certificate is the `tsa` certificate the `SignerInfo` names and
//! whose key verifies the signature. The RFC 5816 `signingCertificateV2`
//! attribute, which repeats that choice by hash, is not consulted: it adds
//! nothing while every certificate in the anchor has its own key. The
//! `accuracy` field is ignored as well; the stamped time is taken as exact.

use x509_parser::certificate::X509Certificate;
use x509_parser::extensions::ParsedExtension;

use crate::der::{self, Reader, Stamp};
use crate::keys::{Hash, oid};
use crate::x509;

/// Verifies `token`, a DER `TimeStampResp` or bare `TimeStampToken`, as a
/// timestamp over `signature` by a certificate in `anchors`. Returns the
/// stamped time.
pub(crate) fn verify(
	token: &[u8],
	signature: &[u8],
	anchors: &[X509Certificate<'_>],
) -> Result<Stamp, String> {
	let signed_data = signed_data(token)?;
	let mut sd = Reader::new(signed_data);
	sd.expect(der::INTEGER)?;
	sd.expect(der::SET)?;
	// encapContentInfo: the TSTInfo.
	let mut encap = Reader::new(sd.expect(der::SEQUENCE)?);
	if encap.expect(der::OID)? != oid::TST_INFO {
		return Err("timestamp content is not a TSTInfo".into());
	}
	let tst_info = der::single(encap.expect(der::CONTEXT_0)?, der::OCTET_STRING)?;
	encap.finish()?;
	// Embedded certificates and CRLs carry no trust and are skipped.
	sd.optional(der::CONTEXT_0)?;
	sd.optional(der::CONTEXT_1)?;
	let mut signer_infos = Reader::new(sd.expect(der::SET)?);
	let signer_info = signer_infos.expect(der::SEQUENCE)?;
	if !signer_infos.is_empty() {
		return Err("timestamp has more than one signer".into());
	}
	sd.finish()?;

	let (time, imprint_hash, imprint) = parse_tst_info(tst_info)?;
	if imprint_hash.digest(signature) != imprint {
		return Err("timestamp is over different data".into());
	}

	let tsa_cert = verify_signer_info(signer_info, tst_info, anchors)?;
	x509::validate_path(tsa_cert, true, anchors, time)
		.map_err(|e| format!("timestamp authority certificate: {e}"))?;
	Ok(time)
}

/// Unwraps a `TimeStampResp` or a bare `ContentInfo` down to the contents of
/// its `SignedData`.
fn signed_data(token: &[u8]) -> Result<&[u8], String> {
	let mut outer = Reader::new(der::single(token, der::SEQUENCE)?);
	// A `TimeStampResp` starts with `PKIStatusInfo`, a SEQUENCE; a
	// `ContentInfo` starts with an OID.
	let content_info = if outer.peek_tag() == Some(der::SEQUENCE) {
		let mut status = Reader::new(outer.expect(der::SEQUENCE)?);
		// granted (0) or grantedWithMods (1)
		match status.expect(der::INTEGER)? {
			[0] | [1] => {}
			_ => return Err("timestamp request was not granted".into()),
		}
		let content_info = outer.expect(der::SEQUENCE)?;
		outer.finish()?;
		content_info
	} else {
		der::single(token, der::SEQUENCE)?
	};
	let mut ci = Reader::new(content_info);
	if ci.expect(der::OID)? != oid::SIGNED_DATA {
		return Err("timestamp is not CMS SignedData".into());
	}
	let signed_data = der::single(ci.expect(der::CONTEXT_0)?, der::SEQUENCE)?;
	ci.finish()?;
	Ok(signed_data)
}

/// Returns the stamped time and the message imprint.
fn parse_tst_info(tst_info: &[u8]) -> Result<(Stamp, Hash, &[u8]), String> {
	let mut tst = Reader::new(der::single(tst_info, der::SEQUENCE)?);
	if tst.expect(der::INTEGER)? != [1] {
		return Err("unsupported TSTInfo version".into());
	}
	tst.expect(der::OID)?;
	let mut imprint = Reader::new(tst.expect(der::SEQUENCE)?);
	let hash = Hash::from_digest_oid(&der::algorithm(&mut imprint)?)
		.ok_or("unsupported message imprint hash")?;
	let hashed = imprint.expect(der::OCTET_STRING)?;
	imprint.finish()?;
	tst.expect(der::INTEGER)?;
	let time = der::generalized_time(tst.expect(der::GENERALIZED_TIME)?)?;
	// accuracy, ordering, nonce, tsa and extensions do not affect the result.
	Ok((time, hash, hashed))
}

/// Checks the `SignerInfo` against the TSTInfo and the anchors; returns the
/// anchor certificate that signed.
fn verify_signer_info<'a, 'b>(
	signer_info: &[u8],
	tst_info: &[u8],
	anchors: &'a [X509Certificate<'b>],
) -> Result<&'a X509Certificate<'b>, String> {
	let mut si = Reader::new(signer_info);
	let version = si.expect(der::INTEGER)?;
	let sid = si.read()?;
	let cert = match (version, sid.tag) {
		([1], der::SEQUENCE) => {
			// issuerAndSerialNumber
			let mut ias = Reader::new(sid.value);
			let issuer = ias.expect_tlv(der::SEQUENCE)?;
			let serial = ias.expect(der::INTEGER)?;
			ias.finish()?;
			anchors.iter().find(|c| c.issuer().as_raw() == issuer.raw && c.raw_serial() == serial)
		}
		// subjectKeyIdentifier
		([3], der::CONTEXT_0_PRIMITIVE) => anchors.iter().find(|c| {
			c.iter_extensions().any(|e| {
				matches!(e.parsed_extension(), ParsedExtension::SubjectKeyIdentifier(id) if id.0 == sid.value)
			})
		}),
		_ => return Err("unsupported timestamp signer identifier".into()),
	}
	.ok_or("timestamp is not signed by a `tsa` anchor certificate")?;
	if !matches!(cert.extended_key_usage(), Ok(Some(eku)) if eku.critical && eku.value.time_stamping)
	{
		return Err("signing certificate is not a timestamping certificate".into());
	}

	let digest_hash = Hash::from_digest_oid(&der::algorithm(&mut si)?)
		.ok_or("unsupported timestamp digest algorithm")?;
	let signed_attrs = si.expect_tlv(der::CONTEXT_0)?;
	let signature_algorithm = der::algorithm(&mut si)?;
	let signature = si.expect(der::OCTET_STRING)?;
	si.optional(der::CONTEXT_1)?;
	si.finish()?;

	// The signed attributes bind the TSTInfo: content type and digest.
	let mut content_type = None;
	let mut message_digest = None;
	let mut attrs = Reader::new(signed_attrs.value);
	while !attrs.is_empty() {
		let mut attr = Reader::new(attrs.expect(der::SEQUENCE)?);
		let attr_oid = attr.expect(der::OID)?;
		let values = attr.expect(der::SET)?;
		attr.finish()?;
		let slot = match attr_oid {
			oid::CONTENT_TYPE => &mut content_type,
			oid::MESSAGE_DIGEST => &mut message_digest,
			_ => continue,
		};
		if slot.replace(values).is_some() {
			return Err("duplicate signed attribute".into());
		}
	}
	if content_type.map(|v| der::single(v, der::OID)).transpose()? != Some(oid::TST_INFO) {
		return Err("signed content type is not TSTInfo".into());
	}
	let stated = message_digest.map(|v| der::single(v, der::OCTET_STRING)).transpose()?;
	if stated != Some(digest_hash.digest(tst_info).as_slice()) {
		return Err("signed digest does not match the TSTInfo".into());
	}

	// The signature covers the attributes re-tagged as a SET (RFC 5652 5.4).
	let mut to_sign = signed_attrs.raw.to_vec();
	to_sign[0] = der::SET;
	let hash = match Hash::from_ecdsa_oid(&signature_algorithm) {
		Some(hash) if hash == digest_hash => hash,
		Some(_) => return Err("signature and digest algorithms disagree".into()),
		None if signature_algorithm == oid::EC_PUBLIC_KEY => digest_hash,
		None => return Err("unsupported timestamp signature algorithm".into()),
	};
	let key = x509::public_key(cert)?;
	if !key.verify_ecdsa_prehash(&hash.digest(&to_sign), signature) {
		return Err("timestamp signature does not verify".into());
	}
	Ok(cert)
}
