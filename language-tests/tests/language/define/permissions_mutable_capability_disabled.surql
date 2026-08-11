/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = []

[test]
reason = """
	With the `mutable_permissions` capability off, a create/update/delete
	PERMISSIONS clause that modifies data (directly or through a function call)
	is refused at definition time, pointing the operator at DEFINE EVENT. A
	SELECT PERMISSIONS clause is always refused regardless of the capability.
	Read-only guards define fine.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define table `t1`: a create/update/delete PERMISSIONS clause modifies data, which requires the `mutable_permissions` capability to be enabled; otherwise move the side effect to a DEFINE EVENT"

[[test.results]]
error = "Cannot define table `t2`: a create/update/delete PERMISSIONS clause modifies data, which requires the `mutable_permissions` capability to be enabled; otherwise move the side effect to a DEFINE EVENT"

[[test.results]]
error = "The PERMISSIONS clause for table `t3` calls `fn::w`, which modifies data, but a PERMISSIONS clause must not modify data"

[[test.results]]
value = "NONE"
*/

DEFINE TABLE audit SCHEMALESS;
DEFINE FUNCTION fn::w() { CREATE audit; RETURN true; };

-- create-perm reaching a write through a function call: refused, capability off.
DEFINE TABLE t1 PERMISSIONS FOR create WHERE fn::w();

-- update-perm with a direct write: refused, capability off.
DEFINE TABLE t2 PERMISSIONS FOR update WHERE (CREATE audit) OR true;

-- select-perm reaching a write: always refused, capability-independent.
DEFINE TABLE t3 PERMISSIONS FOR select WHERE fn::w();

-- read-only create-perm: fine.
DEFINE TABLE t4 PERMISSIONS FOR create WHERE true;
