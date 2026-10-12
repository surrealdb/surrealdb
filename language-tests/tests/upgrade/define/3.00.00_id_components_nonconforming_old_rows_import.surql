/**
[test]
reason = """
Setup for 3.00.00_id_components_nonconforming_old_rows.surql: the old binary
writes two tables whose record ids are plain arrays and has nothing to stop it
writing a record the current binary's named structure cannot admit. `reading`
holds one record whose second element is a string where a datetime is needed,
and `forecast` holds one record whose body stores a `city` that differs from the
`city` its key holds. Each table has exactly one such record, so which record
the refusal names does not depend on the order the scan meets them in."""
run = false
*/
DEFINE TABLE reading SCHEMALESS;
-- One record that fits [city: string, time: datetime], and one with a string in the datetime position.
CREATE reading:['London', d'2025-01-01T10:00:00Z'] SET temp = 1;
CREATE reading:['London', 'not-a-date'] SET temp = 2;

DEFINE TABLE forecast SCHEMALESS;
-- The key says London and the body says Paris.
CREATE forecast:['London', d'2025-01-01T10:00:00Z'] SET city = 'Paris', temp = 3;
