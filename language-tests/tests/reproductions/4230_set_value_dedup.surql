/**
[test]
reason = "SET += with element VALUE alteration should deduplicate after normalization"
issue = 4230

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{'test1',}"

[[test.results]]
value = "{'test1', 'test2'}"

[[test.results]]
value = "{'test1', 'test2'}"

[[test.results]]
value = "{'test1', 'test2'}"

[[test.results]]
value = "{'test1', 'test2', 'test3'}"
*/

DEFINE TABLE test SCHEMAFULL TYPE NORMAL;
DEFINE FIELD test ON test TYPE set<string>;
DEFINE FIELD OVERWRITE test.* ON test TYPE string
    VALUE string::trim($value);

CREATE ONLY test:test SET test += ' test1 ' RETURN VALUE test;
UPDATE ONLY test:test SET test += ' test2 ' RETURN VALUE test;
UPDATE ONLY test:test SET test += ' test1 ' RETURN VALUE test;
UPDATE ONLY test:test SET test += ' test2 ' RETURN VALUE test;
UPDATE ONLY test:test SET test += ' test3 ' RETURN VALUE test;
