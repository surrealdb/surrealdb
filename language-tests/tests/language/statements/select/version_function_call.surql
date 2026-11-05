/**
[env]
backend = ["mem", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = """
A user-defined function called where a statement reads at a VERSION stamp runs
as of the stamp: the definition the stamp holds, and its body's reads made at
the stamp, both when the statement itself is versioned and when the call sits
in a subquery of it. An unversioned call still runs the current definition
over the current data. The current definition is called and its unit kept
first, so a kept unit must not stand for the stamp.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, n: 2 }]"

[[test.results]]
value = "'new 2'"

[[test.results]]
value = "['old 1']"

[[test.results]]
value = "['old 1']"

[[test.results]]
value = "'new 2'"

*/

DEFINE FUNCTION fn::v() { LET $n = (SELECT VALUE n FROM ONLY t:1); RETURN 'old ' + <string> $n; };
CREATE t:1 SET n = 1;
SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;
DEFINE FUNCTION OVERWRITE fn::v() { LET $n = (SELECT VALUE n FROM ONLY t:1); RETURN 'new ' + <string> $n; };
UPDATE t:1 SET n = 2;
RETURN fn::v();
SELECT VALUE fn::v() FROM t VERSION $t1;
SELECT VALUE (SELECT VALUE fn::v() FROM ONLY t:1) FROM t VERSION $t1;
RETURN fn::v();
