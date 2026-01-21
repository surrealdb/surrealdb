/**
[test]

[[test.results]]
value = "[{ id: test:a, name: 'A' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[NULL]"

[[test.results]]
value = '''[[{ op: 'change', path: '/name', value: '@@ -1,1 +1,1 @@\n-C\n+D\n' }]]'''

[[test.results]]
value = "[{ id: test:a, name: 'E' }]"

[[test.results]]
value = "[{ id: test:a, name: 'E' }]"

[[test.results]]
value = "[{ name: 'G' }]"

[[test.results]]
value = "[{ alias: 'H', the_id: test:a }]"

*/


CREATE test:a SET name = 'A';
UPDATE test:a SET name = 'B' RETURN NONE;
UPDATE test:a SET name = 'C' RETURN NULL;
UPDATE test:a SET name = 'D' RETURN DIFF;
UPDATE test:a SET name = 'E' RETURN AFTER;
UPDATE test:a SET name = 'F' RETURN BEFORE;
UPDATE test:a SET name = 'G' RETURN name;
UPDATE test:a SET name = 'H' RETURN name AS alias, id AS the_id;
