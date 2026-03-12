/**
[env]
namespace = "ns1"
database = "db1"
auth = { namespace = "ns1", level = "editor" }
imports = ["language/statements/use/auth_scope_setup.surql"]

[test]
reason = "Namespace-level users cannot USE a namespace outside their auth scope"

[[test.results]]
value = "[{ id: data:1, value: 'ns1 secret' }]"

[[test.results]]
error = "You don't have permission to change to the ns2 namespace"

[[test.results]]
value = "[{ id: data:1, value: 'ns1 secret' }]"

*/

-- NS1 editor can read their own data
SELECT * FROM data;

-- NS1 editor cannot switch to ns2
USE NS ns2;

-- Confirm we're still in ns1 after the failed USE
SELECT * FROM data;
