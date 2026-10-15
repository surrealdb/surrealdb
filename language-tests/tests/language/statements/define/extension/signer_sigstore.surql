/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = """A Sigstore SIGNER records the anchor (the certificate authority chain, the log key and the timestamp authority chain) and the identity rule (issuer, identity and the numeric repository and owner IDs). It is checked for shape only; whether an archive satisfies it is decided when archives are verified. It renders back as written, and INFO STRUCTURE shows it with the capabilities granted."""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ "ext::color": "DEFINE EXTENSION ext::color FROM silo::acme::color<1.0.0> DIGEST 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d' SIGNER { sigstore: { ca: ['Zm9vYmFy', 'YmF6cXV4'], log: 'bG9na2V5', tsa: ['dHNhY2VydA=='], issuer: 'https://token.actions.githubusercontent.com', identity: 'https://github.com/acme/color/.github/workflows/release.yml@refs/tags/v1.0.0', repository_id: '812345678', owner_id: '98765432' } } CAPABILITIES { net: ['api.color.io:443'], functions: ['http::get'] } PERMISSIONS FULL" }"""

[[test.results]]
value = "[{ capabilities: { functions: ['http::get'], memory: NONE, net: ['api.color.io:443'], queries: false, scripting: false, strict_timeout: true, timeout: NONE, write: false }, digest: 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d', executable: { major: 1, minor: 0, organisation: 'acme', package: 'color', patch: 0, type: 'silo' }, name: 'color', permissions: true, signer: { sigstore: { ca: ['Zm9vYmFy', 'YmF6cXV4'], identity: 'https://github.com/acme/color/.github/workflows/release.yml@refs/tags/v1.0.0', issuer: 'https://token.actions.githubusercontent.com', log: 'bG9na2V5', owner_id: '98765432', repository_id: '812345678', tsa: ['dHNhY2VydA=='] } } }]"

*/

DEFINE EXTENSION ext::color FROM silo::acme::color::<1.0.0>
    DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d"
    SIGNER {
        sigstore: {
            ca: ["Zm9vYmFy", "YmF6cXV4"],
            log: "bG9na2V5",
            tsa: ["dHNhY2VydA=="],
            issuer: "https://token.actions.githubusercontent.com",
            identity: "https://github.com/acme/color/.github/workflows/release.yml@refs/tags/v1.0.0",
            repository_id: "812345678",
            owner_id: "98765432"
        }
    }
    CAPABILITIES { net: ["api.color.io:443"], functions: ["http::get"] };
(INFO FOR DB).extensions;
(INFO FOR DB STRUCTURE).extensions;
