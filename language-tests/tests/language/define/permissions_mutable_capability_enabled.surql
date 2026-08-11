/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["mutable_permissions"]

[test]
reason = """
	With the `mutable_permissions` capability on, a create/update/delete
	PERMISSIONS clause that modifies data defines successfully. A SELECT
	PERMISSIONS clause is still refused — the capability never opens up the read
	path, where a mutation could never work and the escalation is sharpest.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The PERMISSIONS clause for table `t3` calls `fn::w`, which modifies data, but a PERMISSIONS clause must not modify data"

[[test.results]]
error = "Found a non-read-only expression in the PERMISSIONS clause for table `t4`, but a PERMISSIONS clause must not modify data"
*/

DEFINE TABLE audit SCHEMALESS;
DEFINE FUNCTION fn::w() { CREATE audit; RETURN true; };

-- create/update write clauses define successfully with the capability on.
DEFINE TABLE t1 PERMISSIONS FOR create WHERE fn::w();
DEFINE TABLE t2 PERMISSIONS FOR update WHERE (CREATE audit) OR true;

-- SELECT clauses stay refused regardless of the capability, via function call
-- or directly.
DEFINE TABLE t3 PERMISSIONS FOR select WHERE fn::w();
DEFINE TABLE t4 PERMISSIONS FOR select WHERE (CREATE audit) OR true;
