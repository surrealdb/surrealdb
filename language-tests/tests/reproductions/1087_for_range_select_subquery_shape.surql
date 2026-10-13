/**
[env]
namespace = true
database = true

[env.capabilities]
allow-eval-query = ["system"]

[test]
reason = """
A SELECT used directly as a FOR range keeps its array shape. Zero matching rows
iterate zero times and one matching row iterates once; only `FROM ONLY`
collapses a SELECT to a scalar.

A block reduced to a single LET statement is a non-query expression: its
value is the LET's own single NONE row unwrapped, not an array containing it.
"""
issue = 1087

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: somebody:one, name: 'one' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[seen:one]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

DEFINE TABLE nobody SCHEMALESS;
DEFINE TABLE somebody SCHEMALESS;
DEFINE TABLE seen SCHEMALESS;

CREATE somebody:one SET name = 'one';

// A VALUE projection over a table scan is an array, empty table included.
SELECT VALUE name FROM nobody;

// An empty range iterates zero times rather than erroring on NONE.
FOR $n IN (SELECT VALUE name FROM nobody) { CREATE type::record('seen', $n) };

// A single-row range iterates once over that row, not over the bare element.
FOR $n IN (SELECT VALUE name FROM somebody) { CREATE type::record('seen', $n) };

SELECT VALUE id FROM seen;

// A block containing only a LET is still a scalar-shaped expression.
RETURN eval::surql("{ LET $x = 1 }");
RETURN IF true { LET $x = 1 };
