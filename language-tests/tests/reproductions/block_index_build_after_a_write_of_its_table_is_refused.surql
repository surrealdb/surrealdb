/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A `DEFINE INDEX` without `CONCURRENTLY` after a write of its table in the
	same block or loop is refused: its build reads the table as last
	committed, so it would leave out the rows the statement wrote before the
	definition, and a unique index would raise no error over them. The refused
	statement keeps nothing. Once the writes are committed, the same
	definition builds over them, and a unique index refuses their duplicates.
"""

[[test.results]]
error = "Cannot define index `by_k` on `item`: this transaction has written records of `item`, which a build without CONCURRENTLY cannot see. Define the index CONCURRENTLY, or commit those writes first."

[[test.results]]
error = "Cannot define index `u` on `dup`: this transaction has written records of `dup`, which a build without CONCURRENTLY cannot see. Define the index CONCURRENTLY, or commit those writes first."

[[test.results]]
error = "Cannot define index `u` on `looped`: this transaction has written records of `looped`, which a build without CONCURRENTLY cannot see. Define the index CONCURRENTLY, or commit those writes first."

[[test.results]]
error = "The table 'item' does not exist"

[[test.results]]
error = "The table 'dup' does not exist"

[[test.results]]
error = "The table 'looped' does not exist"

[[test.results]]
value = "[{ id: item:2, k: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:1, item:2]"

[[test.results]]
value = "[{ id: dup:2, k: 1 }]"

[[test.results]]
error = "Database index `u` already contains 1, with record `dup:1`"
*/

{ CREATE item:1 SET k = 1; DEFINE INDEX by_k ON item FIELDS k; CREATE item:2 SET k = 1; RETURN SELECT VALUE id FROM item WHERE k = 1 ORDER BY id };
{ CREATE dup:1 SET k = 1; CREATE dup:2 SET k = 1; DEFINE INDEX u ON dup FIELDS k UNIQUE; CREATE dup:3 };
FOR $i IN 0..4 { CREATE type::record('looped', $i) SET k = $i % 2; IF $i = 1 { DEFINE INDEX u ON looped FIELDS k UNIQUE } };
SELECT VALUE id FROM item;
SELECT VALUE id FROM dup;
SELECT VALUE id FROM looped;
{ CREATE item:1 SET k = 1; CREATE item:2 SET k = 1 };
DEFINE INDEX by_k ON item FIELDS k;
SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 1 ORDER BY id;
{ CREATE dup:1 SET k = 1; CREATE dup:2 SET k = 1 };
DEFINE INDEX u ON dup FIELDS k UNIQUE;
