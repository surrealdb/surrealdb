/**
[test]
reason = "The body is written in `DEFINE EVENT ev ON src WHEN true THEN { CREATE log SET v = $m }`. A `LET` of the statement that defines or runs a stored clause is not visible in the clause's own frame, so the clause reads the name as unbound."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1 }]"

[[test.results]]
value = "[NONE]"
*/
LET $m = 8;
DEFINE EVENT ev ON src WHEN true THEN { CREATE log SET v = $m };
CREATE src:1;
SELECT VALUE v FROM log;
