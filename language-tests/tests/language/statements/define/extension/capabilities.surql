/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """CAPABILITIES is one object listing everything an extension may do. Fields left out keep their defaults, which grant nothing and keep timeouts enforced; only the fields that differ from the default render back, and INFO STRUCTURE shows every field."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ "ext::all": "DEFINE EXTENSION ext::all FROM f\"modules:/all.surli\" DIGEST 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d' UNSIGNED CAPABILITIES { net: true, functions: ['http::*', 'fn::lookup'], queries: true, scripting: true, write: true, memory: 67108864, timeout: 5s, strict_timeout: false } PERMISSIONS FULL", "ext::none": "DEFINE EXTENSION ext::none FROM f\"modules:/none.surli\" DIGEST 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d' UNSIGNED CAPABILITIES {} PERMISSIONS FULL" }'''

[[test.results]]
value = "[{ capabilities: { functions: ['http::*', 'fn::lookup'], memory: 67108864, net: true, queries: true, scripting: true, strict_timeout: false, timeout: 5s, write: true }, digest: 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d', executable: { bucket: 'modules', key: '/all.surli', type: 'surrealism' }, name: 'all', permissions: true, unsigned: true }, { capabilities: { functions: false, memory: NONE, net: false, queries: false, scripting: false, strict_timeout: true, timeout: NONE, write: false }, digest: 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d', executable: { bucket: 'modules', key: '/none.surli', type: 'surrealism' }, name: 'none', permissions: true, unsigned: true }]"

*/

DEFINE EXTENSION ext::all FROM f"modules:/all.surli" DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d" UNSIGNED
    CAPABILITIES { net: true, functions: ["http::*", "fn::lookup"], queries: true, scripting: true, write: true, memory: 67108864, timeout: 5s, strict_timeout: false };
DEFINE EXTENSION ext::none FROM f"modules:/none.surli" DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d" UNSIGNED
    CAPABILITIES { net: false, functions: [], queries: false };
(INFO FOR DB).extensions;
(INFO FOR DB STRUCTURE).extensions;
