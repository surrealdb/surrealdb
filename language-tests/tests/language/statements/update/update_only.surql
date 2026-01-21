/**
[test]

[[test.results]]
value = "[{ id: test:a, name: 'A' }]"

[[test.results]]
value = "{ id: test:a, name: 'A' }"

*/


CREATE test:a SET name = 'A';
UPDATE ONLY test:a SET name = 'A';
-- UPDATE ONLY test:b SET name = 'B' RETURN NULL;
-- UPDATE ONLY test:c SET name = 'C' RETURN DIFF;
-- UPDATE ONLY test:d SET name = 'D' RETURN AFTER;
-- UPDATE ONLY test:e SET name = 'E' RETURN BEFORE;
-- UPDATE ONLY test:f SET name = 'F' RETURN name;
-- UPDATE ONLY test:g SET name = 'G' RETURN name AS alias, id AS the_id;

