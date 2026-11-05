/**
[env]
imports = ["upgrade/define/3.00.00_id_components_under_wildcard_view_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
A database an older release wrote can hold a view that selects every field of a
table whose record ids are arrays: the realistic starting point for naming the
components. The view's rows were written by that release and hold no component
fields, so naming the components must be refused while the view holds them, and
the remedy, removing the view, naming the components and defining the view again
in one transaction, must leave every row carrying the components. The view
definition this checkout judges is the one the older release stored, so this
also proves its field list is still read as selecting every field.

The syntax is this checkout's, so the body runs only where the upgrade target is
this checkout: the seed axis, each old release straight to HEAD, and the final
adjacent hop. The import writes plain arrays and a view, which every release
writes, so `introduced` stays 3.0.0."""

[[test.results]]
error = "Cannot change the record id of `reading`: the view `v` selects every field of `reading` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"

[[test.results]]
value = "[10, 20]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: 'London', temp: 10, time: d'2025-01-01T10:00:00Z' }, { city: 'Paris', temp: 20, time: d'2025-01-01T10:00:00Z' }]"

[[test.results]]
value = "[20]"

*/
-- Naming the components is refused while the view holds the rows the old binary wrote.
DEFINE FIELD OVERWRITE id ON reading TYPE [city: string, time: datetime];
-- The view is unchanged.
SELECT VALUE temp FROM v ORDER BY temp;
(INFO FOR TABLE reading).fields.id;
-- The remedy, in one transaction: the view is filled again from the source.
BEGIN;
REMOVE TABLE v;
DEFINE FIELD OVERWRITE id ON reading TYPE [city: string, time: datetime];
DEFINE TABLE v AS SELECT * FROM reading;
COMMIT;
SELECT city, temp, time FROM v ORDER BY temp;
SELECT VALUE temp FROM v WHERE city = 'Paris';
