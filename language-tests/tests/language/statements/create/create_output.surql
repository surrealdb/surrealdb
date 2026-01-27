/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
value = "[NULL]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { id: test:c, name: 'C' } }]]"

[[test.results]]
value = "[{ id: test:d, name: 'D' }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ name: 'F' }]"

[[test.results]]
value = "[{ alias: 'G', the_id: test:g }]"

*/

CREATE test:a SET name = 'A' RETURN NONE;
CREATE test:b SET name = 'B' RETURN NULL;
CREATE test:c SET name = 'C' RETURN DIFF;
CREATE test:d SET name = 'D' RETURN AFTER;
CREATE test:e SET name = 'E' RETURN BEFORE;
CREATE test:f SET name = 'F' RETURN name;
CREATE test:g SET name = 'G' RETURN name AS alias, id AS the_id;
