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


UPSERT test:a SET name = 'A';
UPSERT test:a SET name = 'B' RETURN NONE;
UPSERT test:a SET name = 'C' RETURN NULL;
UPSERT test:a SET name = 'D' RETURN DIFF;
UPSERT test:a SET name = 'E' RETURN AFTER;
UPSERT test:a SET name = 'F' RETURN BEFORE;
UPSERT test:a SET name = 'G' RETURN name;
UPSERT test:a SET name = 'H' RETURN name AS alias, id AS the_id;
