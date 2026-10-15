#!/usr/bin/env python3
"""Generates the `private/` and `ssh/` fixtures. Run once; the output is committed.

`private/` is a complete private Sigstore: a P-384 certificate authority, an
RFC 3161 timestamp authority (responses made by `openssl ts`), and a Rekor v2
log with an Ed25519 or ECDSA P-256 key. Each case directory holds the bundle
over `MANIFEST` and the signer a client would record for it.

`ssh/` holds SSH signatures over `MANIFEST` made by `ssh-keygen -Y sign`.

Requires Python 3.11+ with `cryptography`, `openssl` (3.x) and `ssh-keygen`
(OpenSSH 8.9+). Private keys live only in a temporary directory. Re-running
replaces every fixture with new keys and new times, so the tests that pin
values from the fixtures must be updated with them.
"""

import base64
import datetime
import hashlib
import json
import os
import subprocess
import tempfile
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519, utils
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID, ObjectIdentifier

HERE = Path(__file__).resolve().parent
MANIFEST = HERE / "MANIFEST"
NOW = datetime.datetime.now(datetime.UTC).replace(microsecond=0)
ORIGIN = "log.surrealism.test"
ISSUER = "https://token.actions.githubusercontent.com"
IDENTITY = "https://github.com/acme/color/.github/workflows/release.yml@refs/tags/v1.0.0"
REPOSITORY_ID = "812345678"
OWNER_ID = "98765432"


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def der_utf8(text: str) -> bytes:
    data = text.encode()
    assert len(data) < 128
    return bytes([0x0C, len(data)]) + data


def name(cn: str) -> x509.Name:
    return x509.Name(
        [
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "surrealism.test"),
            x509.NameAttribute(NameOID.COMMON_NAME, cn),
        ]
    )


def der(cert: x509.Certificate) -> bytes:
    return cert.public_bytes(serialization.Encoding.DER)


def spki(key) -> bytes:
    return key.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    )


def ca_cert(subject, key, issuer, issuer_key, path_length, eku=None):
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(NOW - datetime.timedelta(days=1))
        .not_valid_after(NOW + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=path_length), critical=True)
        .add_extension(
            x509.KeyUsage(False, False, False, False, False, True, True, False, False),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
    )
    if eku:
        builder = builder.add_extension(x509.ExtendedKeyUsage(eku), critical=False)
    return builder.sign(issuer_key, hashes.SHA384())


def leaf_cert(key, issuer, issuer_key, not_before, san, ids):
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([]))
        .issuer_name(issuer.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_before + datetime.timedelta(minutes=10))
        .add_extension(
            x509.KeyUsage(True, False, False, False, False, False, False, False, False),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CODE_SIGNING]), critical=False)
        .add_extension(x509.SubjectAlternativeName([san]), critical=True)
        .add_extension(
            x509.UnrecognizedExtension(
                ObjectIdentifier("1.3.6.1.4.1.57264.1.8"), der_utf8(ISSUER)
            ),
            critical=False,
        )
    )
    if ids:
        for oid, value in (("1.3.6.1.4.1.57264.1.15", REPOSITORY_ID), ("1.3.6.1.4.1.57264.1.17", OWNER_ID)):
            builder = builder.add_extension(
                x509.UnrecognizedExtension(ObjectIdentifier(oid), der_utf8(value)), critical=False
            )
    return builder.sign(issuer_key, hashes.SHA384())


def tsa_certs():
    root_key = ec.generate_private_key(ec.SECP384R1())
    root = ca_cert(name("test-tsa-root"), root_key, name("test-tsa-root"), root_key, 0)
    key = ec.generate_private_key(ec.SECP256R1())
    leaf = (
        x509.CertificateBuilder()
        .subject_name(name("test-tsa"))
        .issuer_name(root.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(NOW - datetime.timedelta(days=1))
        .not_valid_after(NOW + datetime.timedelta(days=3650))
        .add_extension(
            x509.KeyUsage(True, False, False, False, False, False, False, False, False),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.TIME_STAMPING]), critical=True)
        .sign(root_key, hashes.SHA384())
    )
    return key, leaf, root


TSA_CONFIG = """
[ tsa ]
default_tsa = tsa_config
[ tsa_config ]
serial = {serial}
crypto_device = builtin
default_policy = 1.3.6.1.4.1.57264.2
digests = sha256, sha384, sha512
accuracy = secs:1
ordering = no
tsa_name = no
ess_cert_id_chain = no
ess_cert_id_alg = sha256
signer_digest = sha256
"""


def timestamp(tmp: Path, signature: bytes, tsa_key, tsa_leaf, tsa_root) -> bytes:
    (tmp / "tsa.key").write_bytes(
        tsa_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    (tmp / "tsa.pem").write_bytes(tsa_leaf.public_bytes(serialization.Encoding.PEM))
    (tmp / "tsa-root.pem").write_bytes(tsa_root.public_bytes(serialization.Encoding.PEM))
    (tmp / "serial").write_text("01\n")
    (tmp / "tsa.cnf").write_text(TSA_CONFIG.format(serial=tmp / "serial"))
    imprint = hashlib.sha256(signature).hexdigest()
    run = lambda *args: subprocess.run(["openssl", "ts", *args], check=True, cwd=tmp, capture_output=True)
    run("-query", "-digest", imprint, "-sha256", "-cert", "-out", "req.tsq")
    run(
        "-reply", "-config", "tsa.cnf", "-section", "tsa_config", "-queryfile", "req.tsq",
        "-signer", "tsa.pem", "-inkey", "tsa.key", "-chain", "tsa-root.pem", "-out", "resp.tsr",
    )
    return (tmp / "resp.tsr").read_bytes()


def leaf_hash(data: bytes) -> bytes:
    return hashlib.sha256(b"\x00" + data).digest()


def node_hash(left: bytes, right: bytes) -> bytes:
    return hashlib.sha256(b"\x01" + left + right).digest()


def tree_root(leaves):
    if len(leaves) == 1:
        return leaves[0]
    k = 1 << ((len(leaves) - 1).bit_length() - 1)
    return node_hash(tree_root(leaves[:k]), tree_root(leaves[k:]))


def audit_path(index, leaves):
    if len(leaves) <= 1:
        return []
    k = 1 << ((len(leaves) - 1).bit_length() - 1)
    if index < k:
        return audit_path(index, leaves[:k]) + [tree_root(leaves[k:])]
    return audit_path(index - k, leaves[k:]) + [tree_root(leaves[:k])]


def checkpoint(log_key, size: int, root: bytes) -> str:
    body = f"{ORIGIN}\n{size}\n{b64(root)}\n"
    if isinstance(log_key, ed25519.Ed25519PrivateKey):
        raw = log_key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        hint = hashlib.sha256(ORIGIN.encode() + b"\n\x01" + raw).digest()[:4]
        sig = log_key.sign(body.encode())
    else:
        hint = hashlib.sha256(spki(log_key)).digest()[:4]
        sig = log_key.sign(body.encode(), ec.ECDSA(hashes.SHA256()))
    return body + "\n" + f"— {ORIGIN} {b64(hint + sig)}\n"


def bundle(digest, ca, log_key, tsa, tmp, not_before, san, ids):
    inter, inter_key = ca
    leaf_key = ec.generate_private_key(ec.SECP256R1())
    leaf = leaf_cert(leaf_key, inter, inter_key, not_before, san, ids)
    signature = leaf_key.sign(digest, ec.ECDSA(utils.Prehashed(hashes.SHA256())))
    tst = timestamp(tmp, signature, *tsa)

    body = json.dumps(
        {
            "apiVersion": "0.0.2",
            "kind": "hashedrekord",
            "spec": {
                "hashedRekordV002": {
                    "data": {"algorithm": "SHA2_256", "digest": b64(digest)},
                    "signature": {
                        "content": b64(signature),
                        "verifier": {
                            "keyDetails": "PKIX_ECDSA_P256_SHA_256",
                            "x509Certificate": {"rawBytes": b64(der(leaf))},
                        },
                    },
                }
            },
        },
        separators=(",", ":"),
    ).encode()
    # Our entry at index 4 of a 7-leaf tree, so the proof mixes left and right
    # siblings and an incomplete right subtree.
    leaves = [leaf_hash(f"other entry {i}".encode()) for i in range(7)]
    leaves[4] = leaf_hash(body)
    root = tree_root(leaves)
    if isinstance(log_key, ed25519.Ed25519PrivateKey):
        raw = log_key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        log_id = hashlib.sha256(ORIGIN.encode() + b"\n\x01" + raw).digest()
    else:
        log_id = hashlib.sha256(spki(log_key)).digest()
    return {
        "mediaType": "application/vnd.dev.sigstore.bundle.v0.3+json",
        "verificationMaterial": {
            "certificate": {"rawBytes": b64(der(leaf))},
            "tlogEntries": [
                {
                    "logIndex": "4",
                    "logId": {"keyId": b64(log_id)},
                    "kindVersion": {"kind": "hashedrekord", "version": "0.0.2"},
                    "inclusionProof": {
                        "logIndex": "4",
                        "rootHash": b64(root),
                        "treeSize": "7",
                        "hashes": [b64(h) for h in audit_path(4, leaves)],
                        "checkpoint": {"envelope": checkpoint(log_key, 7, root)},
                    },
                    "canonicalizedBody": b64(body),
                }
            ],
            "timestampVerificationData": {"rfc3161Timestamps": [{"signedTimestamp": b64(tst)}]},
        },
        "messageSignature": {
            "messageDigest": {"algorithm": "SHA2_256", "digest": b64(digest)},
            "signature": b64(signature),
        },
    }


def write_case(case, bundle_json, ca_chain, log_key, tsa, identity, ids):
    out = HERE / "private" / case
    out.mkdir(parents=True, exist_ok=True)
    (out / "bundle.sigstore.json").write_text(json.dumps(bundle_json, indent=2) + "\n")
    signer = {
        "ca": [b64(der(c)) for c in ca_chain],
        "log": b64(spki(log_key)),
        "tsa": [b64(der(tsa[1])), b64(der(tsa[2]))],
        "issuer": ISSUER,
        "identity": identity,
        "repository_id": REPOSITORY_ID if ids else None,
        "owner_id": OWNER_ID if ids else None,
    }
    (out / "signer.json").write_text(json.dumps(signer, indent=2) + "\n")


def sigstore_fixtures(tmp: Path):
    digest = hashlib.sha256(MANIFEST.read_bytes()).digest()
    root_key = ec.generate_private_key(ec.SECP384R1())
    root = ca_cert(name("test-fulcio-root"), root_key, name("test-fulcio-root"), root_key, 1)
    inter_key = ec.generate_private_key(ec.SECP384R1())
    inter = ca_cert(
        name("test-fulcio-intermediate"), inter_key, root.subject, root_key, 0,
        eku=[ExtendedKeyUsageOID.CODE_SIGNING],
    )
    ca = (inter, inter_key)
    tsa = tsa_certs()
    ed_log = ed25519.Ed25519PrivateKey.generate()
    ec_log = ec.generate_private_key(ec.SECP256R1())
    uri = x509.UniformResourceIdentifier(IDENTITY)
    email = "release@acme.test"
    fresh = NOW - datetime.timedelta(minutes=1)
    expired = NOW - datetime.timedelta(hours=2)

    for case, log_key, not_before, san, identity, ids in [
        ("ed25519-log", ed_log, fresh, uri, IDENTITY, True),
        ("ecdsa-log", ec_log, fresh, uri, IDENTITY, True),
        ("email-identity", ed_log, fresh, x509.RFC822Name(email), email, False),
        ("expired", ed_log, expired, uri, IDENTITY, True),
    ]:
        b = bundle(digest, ca, log_key, tsa, tmp, not_before, san, ids)
        write_case(case, b, [inter, root], log_key, tsa, identity, ids)


def ssh_fixtures(tmp: Path):
    out = HERE / "ssh"
    out.mkdir(exist_ok=True)

    def keygen(kind, *args):
        key = tmp / f"id_{kind}"
        subprocess.run(
            ["ssh-keygen", "-q", "-N", "", "-C", "release@acme", "-f", key, *args], check=True
        )
        (out / f"id_{kind}.pub").write_bytes(key.with_suffix(".pub").read_bytes())
        return key

    def sign(key, target, namespace="surrealism", hashalg="sha256"):
        subprocess.run(
            ["ssh-keygen", "-q", "-Y", "sign", "-n", namespace, "-O", f"hashalg={hashalg}", "-f", key, MANIFEST],
            check=True,
            capture_output=True,
        )
        os.replace(f"{MANIFEST}.sig", out / target)

    ed = keygen("ed25519", "-t", "ed25519")
    ecdsa = keygen("ecdsa", "-t", "ecdsa", "-b", "256")
    keygen("other", "-t", "ed25519")
    sign(ed, "ed25519.sig")
    sign(ecdsa, "ecdsa.sig")
    sign(ed, "ed25519-git-namespace.sig", namespace="git")
    sign(ed, "ed25519-sha512.sig", hashalg="sha512")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp:
        sigstore_fixtures(Path(tmp))
        ssh_fixtures(Path(tmp))
