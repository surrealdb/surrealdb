/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = """The trailing clauses of an extension definition are unordered, and render back in one canonical order."""

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ "ext::color": "DEFINE EXTENSION ext::color FROM f\"modules:/color.surli\" DIGEST 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d' UNSIGNED CAPABILITIES { write: true } COMMENT 'c' PERMISSIONS NONE" }'''

*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" PERMISSIONS NONE CAPABILITIES { write: true } COMMENT "c" UNSIGNED DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d";
(INFO FOR DB).extensions;
