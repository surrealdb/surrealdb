/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """A key SIGNER records an SSH public key line; the key is the whole anchor, so there is no identity rule. Without CAPABILITIES the definition grants nothing, which renders as `CAPABILITIES {}`."""

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ "ext::geo": "DEFINE EXTENSION ext::geo FROM f\"modules:/geo.surli\" DIGEST 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d' SIGNER { key: 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIK0w release@acme' } CAPABILITIES {} PERMISSIONS FULL" }'''

[[test.results]]
value = "[{ capabilities: { functions: false, memory: NONE, net: false, queries: false, scripting: false, strict_timeout: true, timeout: NONE, write: false }, digest: 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d', executable: { bucket: 'modules', key: '/geo.surli', type: 'surrealism' }, name: 'geo', permissions: true, signer: { key: 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIK0w release@acme' } }]"

*/

DEFINE EXTENSION ext::geo FROM f"modules:/geo.surli" DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d" SIGNER { key: "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIK0w release@acme" };
(INFO FOR DB).extensions;
(INFO FOR DB STRUCTURE).extensions;
