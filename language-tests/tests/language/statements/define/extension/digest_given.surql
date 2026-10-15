/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = """A DIGEST given in the statement is recorded as written and the archive is not fetched, so the definition succeeds even though no bucket holds it. This is how an export, which always writes DIGEST, restores without its archives."""

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ "ext::color": "DEFINE EXTENSION ext::color FROM f\"nowhere:/color.surli\" DIGEST 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d' UNSIGNED CAPABILITIES {} PERMISSIONS FULL" }'''

*/

DEFINE EXTENSION ext::color FROM f"nowhere:/color.surli" DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d" UNSIGNED;
(INFO FOR DB).extensions;
