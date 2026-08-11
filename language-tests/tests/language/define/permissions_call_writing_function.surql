/**
[env]
namespace = true
database = true

[test]
reason = """
	A permission guard is evaluated on reads under a frame that refuses
	writes, so a guard that calls a function whose stored body writes can
	never succeed. The definition-time check resolves the call graph and
	refuses such a guard, naming the function; a read-only guard defines
	fine, and from then on protects its function from being redefined to
	write.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The PERMISSIONS clause for table `doc` calls `fn::audit`, which modifies data, but a PERMISSIONS clause must not modify data"

[[test.results]]
error = "The PERMISSIONS clause for param `p` calls `fn::audit`, which modifies data, but a PERMISSIONS clause must not modify data"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define function `fn::check`: it modifies data, but it must remain read-only because it is used by PERMISSIONS FOR select ON TABLE doc"
*/

DEFINE TABLE log SCHEMALESS;
DEFINE FUNCTION fn::audit() { CREATE log; RETURN true; };
DEFINE FUNCTION fn::check() { RETURN true; };

-- A guard calling a writing function is refused at definition, naming it.
DEFINE TABLE doc PERMISSIONS FOR select WHERE fn::audit();
DEFINE PARAM $p VALUE 1 PERMISSIONS WHERE fn::audit();

-- A read-only guard defines fine...
DEFINE TABLE doc PERMISSIONS FOR select WHERE fn::check();

-- ...and now protects its function from being redefined to write.
DEFINE FUNCTION OVERWRITE fn::check() { CREATE log; RETURN true; };
