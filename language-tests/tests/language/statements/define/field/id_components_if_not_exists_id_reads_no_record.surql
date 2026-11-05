/**
[env]
namespace = true
database = true

[test]
reason = """
	`DEFINE FIELD IF NOT EXISTS id` that meets an existing `id` leaves the
	stored definition alone, so it changes no record id structure and reads no
	record: the check that refuses leaving a table with no structure under the
	keys only that structure orders does not run for it, even where the stored
	structure is coarse and the records are out of their record ids' order.

	The statement is a no-op whatever its TYPE would have said, so a kind that
	names nothing is not read as dropping the names. A plain `DEFINE FIELD`
	that meets the same `id` replaces nothing either: it is refused as a
	duplicate, before any record is read.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ at: d'2024-01-01T00:00:00Z', id: r:[d'2024-01-01T00:00:00Z'] }]"

[[test.results]]
value = "[{ at: d'2024-01-01T00:00:00.500Z', id: r:[d'2024-01-01T00:00:00.500Z'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[r:[d'2024-01-01T00:00:00Z'], r:[d'2024-01-01T00:00:00.500Z']]"

[[test.results]]
error = "The field 'id' already exists"

[[test.results]]
error = "Cannot remove the names from the record id of `r`: it holds r:[d'2024-01-01T00:00:00.500Z'] and r:[d'2024-01-01T00:00:00Z'], whose keys are stored in the opposite order to their record ids, which only the names account for. Delete those records, or copy them into a table with the record id you want, first"
*/

DEFINE FIELD id ON r TYPE [at: datetime];
CREATE r SET at = d'2024-01-01T00:00:00Z';
CREATE r SET at = d'2024-01-01T00:00:00.500Z';

-- The `id` is there, so this returns without reading a record.
DEFINE FIELD IF NOT EXISTS id ON r TYPE [datetime];
-- The structure is untouched, so the names are still read and the sort kept.
SELECT VALUE id FROM r ORDER BY id;
-- A plain DEFINE cannot replace the `id` either: the duplicate is reported.
DEFINE FIELD id ON r TYPE [datetime];
-- An OVERWRITE of the same statement does drop the names, and is refused.
DEFINE FIELD OVERWRITE id ON r TYPE [datetime];
