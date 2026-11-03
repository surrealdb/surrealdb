/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """BINDINGS is an object of expressions, kept as written: a subquery stays parenthesised so the definition parses back, and INFO STRUCTURE shows each expression as text, never its value. With a DIGEST and no archive to fetch, the definition is stored unchecked, so the names are checked against the package when the extension reads them."""

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ "ext::mailer": "DEFINE EXTENSION ext::mailer FROM f\"modules:/mailer.surli\" DIGEST 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d' UNSIGNED CAPABILITIES {} BINDINGS { api_key: (SELECT VALUE key FROM ONLY vault:mailgun), author: 'noreply@acme.com', \"odd-name\": $auth.id } PERMISSIONS FULL" }'''

[[test.results]]
value = """[{ bindings: { api_key: '(SELECT VALUE key FROM ONLY vault:mailgun)', author: "'noreply@acme.com'", "odd-name": '$auth.id' }, capabilities: { functions: false, memory: NONE, net: false, queries: false, scripting: false, strict_timeout: true, timeout: NONE, write: false }, digest: 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d', executable: { bucket: 'modules', key: '/mailer.surli', type: 'surrealism' }, name: 'mailer', permissions: true, unsigned: true }]"""
*/

DEFINE EXTENSION ext::mailer FROM f"modules:/mailer.surli" DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d" UNSIGNED
    BINDINGS { api_key: (SELECT VALUE key FROM ONLY vault:mailgun), author: 'noreply@acme.com', `odd-name`: $auth.id };
(INFO FOR DB).extensions;
(INFO FOR DB STRUCTURE).extensions;
