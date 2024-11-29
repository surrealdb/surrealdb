/**
[test]
wip = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: thing:2gmswqs3pk5j8a5s5tql }]"
skip-record-id-key = true

[[test.results]]
error = "Found record: `thing:n4sx19mpsru6exg6egjk` which is not a relation, but expected a  NORMAL"

*/

DEFINE TABLE thing TYPE NORMAL;
CREATE thing;
RELATE foo:one->thing->foo:two;
