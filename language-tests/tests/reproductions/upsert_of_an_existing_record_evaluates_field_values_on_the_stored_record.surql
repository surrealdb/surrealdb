/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	An `UPSERT` that names a stored record derives the table's `VALUE` clauses
	over the stored record, as an `UPDATE` does: a clause that reads a field the
	`SET` or `MERGE` does not write reads the stored field rather than `NONE`.
	The record is read before an order is chosen, so a stored record never
	takes the create order, which would derive the clauses over the record the
	statement would create. A `CONTENT` clause supplies the whole record and
	derives over what it supplies.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: h:1, n: 1, twice: 2, up: 5 }]"

[[test.results]]
value = "[{ id: h:1, n: 1, twice: 2, up: 1 }]"

[[test.results]]
value = "[{ id: h:1, n: 1, twice: 2, up: 2 }]"

[[test.results]]
value = "[{ id: h:1, n: 1, twice: 2, up: 3 }]"

[[test.results]]
value = "[{ id: h:1, n: 4, twice: 8, up: 3 }]"

[[test.results]]
value = "[{ id: h:1, n: 6, twice: 12, up: 7 }]"
*/

DEFINE TABLE h;
DEFINE FIELD n ON h TYPE option<int>;
DEFINE FIELD up ON h TYPE option<int>;
DEFINE FIELD twice ON h VALUE n * 2;
CREATE h:1 SET n = 1, up = 5;
UPDATE h:1 SET up = 1;
UPSERT h:1 SET up = 2;
UPSERT h:1 MERGE { up: 3 };
UPSERT h:1 CONTENT { n: 4, up: 3 };
UPSERT h:1 SET n = 6, up = 7;
