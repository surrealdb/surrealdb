/**
[test]
reason = """
Setup for 3.00.00_id_components_under_wildcard_view.surql: the old binary writes
a table whose record ids are plain arrays of a city and a datetime, and a view
that selects every field of it, which it fills with rows that hold no `city` or
`time` field."""
run = false
*/
DEFINE TABLE reading SCHEMALESS;
CREATE reading:['London', d'2025-01-01T10:00:00Z'] SET temp = 10;
CREATE reading:['Paris', d'2025-01-01T10:00:00Z'] SET temp = 20;
DEFINE TABLE v AS SELECT * FROM reading;
