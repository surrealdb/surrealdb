/**
[test]
reason = "A subquery in SET is evaluated per subject and sees every write the statement has already made: earlier subjects on the same table, and what their events wrote elsewhere"

[[test.results]]
value = "[{ id: person:1, n: 0 }, { id: person:2, n: 0 }, { id: person:3, n: 0 }]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[0, 1, 2]"

*/
CREATE person:1, person:2, person:3 SET n = 0;
UPDATE person SET n = 1 + count((SELECT * FROM person WHERE n > 0)) RETURN VALUE n;
DEFINE TABLE log;
DEFINE EVENT log_update ON person WHEN $event = "UPDATE" THEN { CREATE log };
UPDATE person SET m = count((SELECT * FROM log)) RETURN VALUE m;
