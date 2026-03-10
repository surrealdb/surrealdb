/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:1, type: 'make' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:2, type: 'bake' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:3, type: 'cake' }]"

*/

DEFINE TABLE test;
DEFINE FIELD type ON test TYPE 'make';
CREATE test:1 CONTENT { type: 'make' };

DEFINE FIELD OVERWRITE type ON test TYPE 'make' | 'bake';
DEFINE TABLE OVERWRITE test;
CREATE test:2 CONTENT { type: 'bake' };

DEFINE FIELD OVERWRITE type ON test TYPE 'make' | 'bake' | 'cake';
DEFINE TABLE OVERWRITE test;
CREATE test:3 CONTENT { type: 'cake' };