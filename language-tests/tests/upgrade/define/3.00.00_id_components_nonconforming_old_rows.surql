/**
[env]
imports = ["upgrade/define/3.00.00_id_components_nonconforming_old_rows_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
Naming the components of a record id is refused when a record the table already
holds does not fit the structure, and an older release wrote those records with
nothing to stop it writing one that does not: a string where a datetime is
needed, or a `city` field that disagrees with the `city` its key holds. The
statement has to read the older writer's keys and bodies, refuse the record that
fails with a message that names the record and the remedy, and define nothing,
so the table reads exactly as it did. A scan that passed such a record would
leave a structure over keys it cannot order.

The syntax is this checkout's. No released binary parses a named record id, so
the body runs only where the upgrade target is this checkout: the seed axis,
each old release straight to HEAD, and the final adjacent hop. The
release-to-release hops of the pair axis and the cumulative walk cannot reach
the construct until a release ships the syntax."""

[[test.results]]
error = "Cannot give `reading` the record id [city: string, time: datetime]: the existing record reading:['London', 'not-a-date'] does not match it (`time` is not a datetime). Copy the records into a table with the new record id instead"

[[test.results]]
value = "{  }"

[[test.results]]
value = "[{ id: reading:['London', d'2025-01-01T10:00:00Z'], temp: 1 }, { id: reading:['London', 'not-a-date'], temp: 2 }]"

[[test.results]]
error = "Cannot give `forecast` the record id [city: string, time: datetime]: the record forecast:['London', d'2025-01-01T10:00:00Z'] stores a `city` field that differs from its record id. Remove it first, for example with UPDATE forecast UNSET city"

[[test.results]]
value = "{  }"

[[test.results]]
value = "[{ city: 'Paris', id: forecast:['London', d'2025-01-01T10:00:00Z'], temp: 3 }]"

*/
-- A string stands where the record id needs a datetime.
DEFINE FIELD OVERWRITE id ON reading TYPE [city: string, time: datetime];
-- The refused statement defined nothing.
RETURN (INFO FOR TABLE reading).fields;
-- The records are as the old binary wrote them: no component is derived without a definition.
SELECT * FROM reading ORDER BY temp;

-- A body field that disagrees with the key is not a component, and is not shadowed silently.
DEFINE FIELD OVERWRITE id ON forecast TYPE [city: string, time: datetime];
RETURN (INFO FOR TABLE forecast).fields;
-- The body field is still the old binary's.
SELECT * FROM forecast;
