/**
[env]
namespace = true
database = true

[test]
reason = """
	A component name is a field the record id derives, with no definition of its
	own, so it can be neither removed nor altered: the way to rename or drop it
	is to change the TYPE of `id`, and REMOVE FIELD IF EXISTS refuses it in the
	same way rather than doing nothing. Removing the `id` field frees the names.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "`city` is part of the record id of `reading`; change the TYPE of `id` to rename or remove it"

[[test.results]]
error = "`time` is part of the record id of `reading`; change the TYPE of `id` to rename or remove it"

[[test.results]]
error = "`city` is part of the record id of `reading`; change the TYPE of `id` to rename or remove it"

[[test.results]]
error = "`city` is part of the record id of `reading`; change the TYPE of `id` to rename or remove it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

REMOVE FIELD city ON reading;
REMOVE FIELD time ON reading;
ALTER FIELD city ON reading TYPE string;
-- The field exists, so IF EXISTS does not make the removal a no-op.
REMOVE FIELD IF EXISTS city ON reading;

-- Nothing reads the names, so the id can go; the names are fields again.
REMOVE FIELD id ON reading;
DEFINE FIELD city ON reading TYPE string;
