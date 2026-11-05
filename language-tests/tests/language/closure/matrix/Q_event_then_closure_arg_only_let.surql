/**
[test]
reason = "The body is written in `DEFINE EVENT ev ON src WHEN true THEN { CREATE log SET v = [1, 10].map(|$e| $e + $m) }`. A `LET` of the statement that defines or runs a stored clause is not visible in the clause's own frame, so the clause reads the name as unbound."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event ev: Cannot perform addition with 'int' and 'none'"

[[test.results]]
error = "The table 'log' does not exist"
*/
LET $m = 8;
DEFINE EVENT ev ON src WHEN true THEN { CREATE log SET v = [1, 10].map(|$e| $e + $m) };
CREATE src:1;
SELECT VALUE v FROM log;
