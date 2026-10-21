/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An IP address written as its text is read back unchanged. The SDK stores a
Rust `IpAddr` as this text through serde and through SurrealValue alike, which
`ip_addresses_roundtrip_as_strings` in surrealdb-types checks.
"""
issue = 3301

[[test.results]]
value = "[{ id: peer:1, ip: '127.0.0.1', v6: '::1' }]"

[[test.results]]
value = "[['127.0.0.1', '::1']]"

[[test.results]]
value = "[peer:1]"
*/
CREATE peer:1 SET ip = '127.0.0.1', v6 = '::1';
SELECT VALUE [ip, v6] FROM peer:1;
SELECT VALUE id FROM peer WHERE ip = '127.0.0.1';
