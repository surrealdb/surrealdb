/**
[env]
namespace = "ns1"
database = "db1"
auth = { namespace = "ns1", level = "editor" }
imports = ["language/statements/use/auth_scope_setup.surql"]

[test]
reason = "Namespace-level users cannot switch to or access data in another namespace"

[[test.results]]
value = "[{ id: data:1, value: 'ns1 secret' }]"

[[test.results]]
error = "You don't have permission to change to the ns2 namespace"

[[test.results]]
value = "[{ id: data:1, value: 'ns1 secret' }]"

*/

-- NS1 editor can read own namespace
SELECT * FROM data;

-- Cannot switch namespace (Option C: USE handler auth check)
USE NS ns2 DB db1;

-- Verify still in ns1
SELECT * FROM data;
