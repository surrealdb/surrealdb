/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:raphael }, { id: person:tobie }]"

[[test.results]]
value = "[{ id: likes:gcnv21jr14dsvx8k2ghr, in: person:raphael, out: person:tobie }]"
skip-record-id-key = true

[[test.results]]
error = "Found record: `likes:1` which is a relation, but expected a RELATION"

*/

DEFINE TABLE likes TYPE RELATION;
CREATE person:raphael, person:tobie;
RELATE person:raphael->likes->person:tobie;
CREATE likes:1;
