/**
[env]
namespace = false
database = false
clean = true
auth = { level = "owner" }

[test]
reason = """
	A write whose table is only known when it runs still needs the session to
	select a namespace and a database: without one it fails with the error
	naming what is missing, as a write to a named table does.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "Specify a namespace to use"

[[test.results]]
value = "{ database: NONE, namespace: 'test' }"

[[test.results]]
error = "Specify a database to use"

*/
LET $t = type::table("person");
CREATE $t;
USE NS test;
CREATE $t;
