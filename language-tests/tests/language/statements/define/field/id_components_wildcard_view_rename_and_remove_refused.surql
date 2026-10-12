/**
[env]
namespace = true
database = true

[test]
reason = """
	Once a view that selects every field holds rows written under named
	components, renaming a component, moving it or removing the names would
	leave those rows holding the old names while new rows hold the new ones.
	ALTER FIELD, DEFINE FIELD OVERWRITE and REMOVE FIELD of the `id` are refused
	while the view holds a row, on the statement's own route and inside a
	transaction alike. Defining the same names again, with the same structure
	or another comment, changes no name and is accepted.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot change the record id of `reading`: the view `v` selects every field of `reading` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"

[[test.results]]
error = "Cannot change the record id of `reading`: the view `v` selects every field of `reading` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"

[[test.results]]
error = "Cannot change the record id of `reading`: the view `v` selects every field of `reading` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"

[[test.results]]
error = "Cannot change the record id of `reading`: the view `v` selects every field of `reading` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: 'London', id: v:['London', d'2024-01-01T00:00:00Z'], temp: 10, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `reading`: the view `v` selects every field of `reading` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "true"
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE TABLE v AS SELECT * FROM reading;
CREATE reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 10 RETURN NONE;

-- A rename, a move, a removal of the names and of the field.
ALTER FIELD id ON reading TYPE [place: string, time: datetime];
DEFINE FIELD OVERWRITE id ON reading TYPE [time: datetime, city: string];
ALTER FIELD id ON reading TYPE [string, datetime];
REMOVE FIELD id ON reading;

-- The same names again.
ALTER FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE FIELD OVERWRITE id ON reading TYPE [city: string, time: datetime] COMMENT 'kept';
ALTER FIELD id ON reading COMMENT 'still kept';
SELECT * FROM v;

-- Inside a transaction, the refusal fails the transaction.
BEGIN;
REMOVE FIELD id ON reading;
COMMIT;
(INFO FOR TABLE reading).fields.id != NONE;
