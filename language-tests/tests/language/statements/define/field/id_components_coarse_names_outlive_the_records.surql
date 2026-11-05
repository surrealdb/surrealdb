/**
[env]
namespace = true
database = true

[test]
reason = """
	A record id that names a datetime component holds its keys in the text of
	the datetime, which orders by the whole second alone: `00:00:00.500Z`
	sorts before `00:00:00Z`. That is why a scan of such a table is not taken
	to deliver `ORDER BY id`, and the judgement reads the structure the
	catalog holds now, while the keys it judges were written earlier and are
	rewritten by no change of the names.

	So a statement leaving the table with no structure is refused while the
	keys it already holds are stored in an order other than their record ids':
	by REMOVE FIELD id, by an ALTER that retypes `id` to an unnamed kind, and
	by a DEFINE FIELD OVERWRITE that does the same. Dropping the names changes
	the answer to no query over the records that exist.

	The refusal is on the records, not on the structure. A coarse structure
	whose records happen to order exactly — one reading per second — keeps its
	freedom to drop the names, as does a structure whose every component
	orders exactly and a table holding nothing at all. A statement that keeps
	a structure scans for conformance instead, and is not refused for order.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ at: d'2024-01-01T00:00:00Z', id: coarse:[d'2024-01-01T00:00:00Z'] }]"

[[test.results]]
value = "[{ at: d'2024-01-01T00:00:00.500Z', id: coarse:[d'2024-01-01T00:00:00.500Z'] }]"

[[test.results]]
error = "Cannot remove the names from the record id of `coarse`: it holds coarse:[d'2024-01-01T00:00:00.500Z'] and coarse:[d'2024-01-01T00:00:00Z'], whose keys are stored in the opposite order to their record ids, which only the names account for. Delete those records, or copy them into a table with the record id you want, first"

[[test.results]]
error = "Cannot remove the names from the record id of `coarse`: it holds coarse:[d'2024-01-01T00:00:00.500Z'] and coarse:[d'2024-01-01T00:00:00Z'], whose keys are stored in the opposite order to their record ids, which only the names account for. Delete those records, or copy them into a table with the record id you want, first"

[[test.results]]
error = "Cannot remove the names from the record id of `coarse`: it holds coarse:[d'2024-01-01T00:00:00.500Z'] and coarse:[d'2024-01-01T00:00:00Z'], whose keys are stored in the opposite order to their record ids, which only the names account for. Delete those records, or copy them into a table with the record id you want, first"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[coarse:[d'2024-01-01T00:00:00Z'], coarse:[d'2024-01-01T00:00:00.500Z']]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ at: d'2024-01-02T00:00:00Z', id: apart:[d'2024-01-02T00:00:00Z'] }]"

[[test.results]]
value = "[{ at: d'2024-01-01T00:00:00Z', id: apart:[d'2024-01-01T00:00:00Z'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[apart:[d'2024-01-01T00:00:00Z'], apart:[d'2024-01-02T00:00:00Z']]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: exact:['b'], s: 'b' }]"

[[test.results]]
value = "[{ id: exact:['a'], s: 'a' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[exact:['a'], exact:['b']]"
*/

-- Two records inside one second: their keys are stored in the opposite order.
DEFINE FIELD id ON coarse TYPE [at: datetime];
CREATE coarse SET at = d'2024-01-01T00:00:00Z';
CREATE coarse SET at = d'2024-01-01T00:00:00.500Z';

REMOVE FIELD id ON coarse;
ALTER FIELD id ON coarse TYPE [datetime];
DEFINE FIELD OVERWRITE id ON coarse TYPE [datetime];
-- Not a loss of the structure: the records are held to the new one instead.
ALTER FIELD id ON coarse TYPE [when: datetime];
SELECT VALUE id FROM coarse ORDER BY id;

-- With the records gone there is no key the judgement could be wrong about.
DELETE coarse;
REMOVE FIELD id ON coarse;

-- A coarse structure whose records fall in different seconds orders exactly
-- as it stands, so its names are free to go.
DEFINE FIELD id ON apart TYPE [at: datetime];
CREATE apart SET at = d'2024-01-02T00:00:00Z';
CREATE apart SET at = d'2024-01-01T00:00:00Z';
REMOVE FIELD id ON apart;
SELECT VALUE id FROM apart ORDER BY id;

-- A structure that orders exactly keeps its order without its names.
DEFINE FIELD id ON exact TYPE [s: string];
CREATE exact SET s = 'b';
CREATE exact SET s = 'a';
REMOVE FIELD id ON exact;
SELECT VALUE id FROM exact ORDER BY id;
