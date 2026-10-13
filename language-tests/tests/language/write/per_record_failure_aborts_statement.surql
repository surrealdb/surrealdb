/**
[env]
namespace = true
database = true

[test]
reason = """
A per-record hard failure aborts the whole mutation and rolls it back: a record
written before the failing one does not survive. This is the error contract the
streaming write path's stage outcome models, which is why it has no
partial-success state.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, n: 1 }]"

[[test.results]]
value = "[{ id: person:2, n: 2 }]"

[[test.results]]
value = "[{ id: person:3, n: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found 2 for field `n`, with record `person:2`, but field must conform to: $value < 2"

[[test.results]]
value = "[{ id: person:1, n: 1, touched: NONE }, { id: person:2, n: 2, touched: NONE }, { id: person:3, n: 3, touched: NONE }]"
*/

DEFINE FIELD n ON person TYPE int;
CREATE person:1 SET n = 1;
CREATE person:2 SET n = 2;
CREATE person:3 SET n = 3;
DEFINE FIELD OVERWRITE n ON person TYPE int ASSERT $value < 2;
UPDATE person SET touched = true;
SELECT id, n, touched FROM person ORDER BY id;
