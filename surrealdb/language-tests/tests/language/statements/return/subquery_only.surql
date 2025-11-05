/**
[test]

[[test.results]]
value = "[{ id: person:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ id: person:jaime, name: 'Jaime' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Tobie' }]"

[[test.results]]
value = "['Jaime', 'Tobie']"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
value = "[{ name: 'Tobie' }]"

[[test.results]]
value = "['Tobie']"

[[test.results]]
value = "{ name: 'Tobie' }"

[[test.results]]
value = "'Tobie'"

[[test.results]]
value = "[{ name: 'Tobie' }]"

[[test.results]]
value = "['Tobie']"

[[test.results]]
value = "{ name: 'Tobie' }"

[[test.results]]
value = "'Tobie'"

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Tobie' }]"

[[test.results]]
value = "['Jaime', 'Tobie']"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
value = "[{ name: 'Tobie' }]"

[[test.results]]
value = "['Tobie']"

[[test.results]]
value = "{ name: 'Tobie' }"

[[test.results]]
value = "'Tobie'"

[[test.results]]
value = "[{ name: 'Tobie' }]"

[[test.results]]
value = "['Tobie']"

[[test.results]]
value = "{ name: 'Tobie' }"

[[test.results]]
value = "'Tobie'"

*/

CREATE person:tobie SET name = 'Tobie';
CREATE person:jaime SET name = 'Jaime';
LET $single = person:tobie;
--
SELECT name FROM person;
SELECT VALUE name FROM person;
SELECT name FROM ONLY person;
SELECT VALUE name FROM ONLY person;
SELECT name FROM person:tobie;
SELECT VALUE name FROM person:tobie;
SELECT name FROM ONLY person:tobie;
SELECT VALUE name FROM ONLY person:tobie;
SELECT name FROM $single;
SELECT VALUE name FROM $single;
SELECT name FROM ONLY $single;
SELECT VALUE name FROM ONLY $single;
--
RETURN SELECT name FROM person;
RETURN SELECT VALUE name FROM person;
RETURN SELECT name FROM ONLY person;
RETURN SELECT VALUE name FROM ONLY person;
RETURN SELECT name FROM person:tobie;
RETURN SELECT VALUE name FROM person:tobie;
RETURN SELECT name FROM ONLY person:tobie;
RETURN SELECT VALUE name FROM ONLY person:tobie;
RETURN SELECT name FROM $single;
RETURN SELECT VALUE name FROM $single;
RETURN SELECT name FROM ONLY $single;
RETURN SELECT VALUE name FROM ONLY $single;
