/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: thing:2gmswqs3pk5j8a5s5tql }]"
skip-record-id-key = true

[[test.results]]
match = '''string::matches($error, /Found record: `thing:.*` which is not a relation, but expected a NORMAL/)'''
error = true
*/

DEFINE TABLE thing TYPE NORMAL;
CREATE thing;
RELATE foo:one->thing->foo:two;
