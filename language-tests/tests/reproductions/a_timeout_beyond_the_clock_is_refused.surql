/**
[env]
namespace = true
database = true

[test]
reason = """
A `TIMEOUT` whose duration the clock cannot reach names no deadline: adding it
to the current instant overflows rather than naming a distant moment. The
clause is refused with an error instead of computing a deadline from it, and the refusal is the clause's, so the statement writes nothing.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:1 }]"

[[test.results]]
error = "Invalid timeout: 9460800000000000000 seconds"

[[test.results]]
error = "Invalid timeout: 9460800000000000000 seconds"

[[test.results]]
error = "Invalid timeout: 9460800000000000000 seconds"

[[test.results]]
value = "[{ id: test:1 }]"
*/

DEFINE TABLE test SCHEMALESS;
CREATE test:1;

SELECT * FROM test TIMEOUT 9460800000000000000s;
UPDATE test TIMEOUT 9460800000000000000s;
DELETE test TIMEOUT 9460800000000000000s;

-- The refusal is the clause's, so the statement wrote nothing.
SELECT * FROM test;
