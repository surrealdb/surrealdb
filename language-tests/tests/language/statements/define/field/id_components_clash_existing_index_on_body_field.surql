/**
[env]
namespace = true
database = true

[test]
reason = """
	An index over `city` reads the stored field of that name. Naming `city` in
	the record id would make the index read the component instead, which the
	index entries already written do not hold, so the statement is refused
	until the index is removed.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot name `city` in the record id of `reading`: the index `by_city` already reads a stored `city` field. Remove it, then define it again after changing the record id"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE TABLE reading;
DEFINE INDEX by_city ON reading FIELDS city;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

REMOVE INDEX by_city ON reading;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
