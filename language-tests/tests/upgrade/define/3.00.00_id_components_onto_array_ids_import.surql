/**
[test]
reason = """
Setup for 3.00.00_id_components_onto_array_ids.surql and its _current sibling:
the old binary writes the rows of a table that has no definition for `id`, each
under a record id that is an array of a city and a datetime, so the current
binary has to name the components of keys it did not write.

Two of the keys carry a sub-second part. A datetime in a record key is its
RFC 3339 text, so `...:00.500Z` sorts before the whole second `...:00Z` it falls
in. `reading:['Paris', d'2025-01-01T10:00:00Z']` also stores a `city` field equal
to its key; the other rows store none."""
run = false
*/
DEFINE TABLE reading SCHEMALESS;
-- London: a whole second, a fraction of that same second, the next second, and an hour on.
CREATE reading:['London', d'2025-01-01T10:00:00Z'] SET temp = 10;
CREATE reading:['London', d'2025-01-01T10:00:00.500Z'] SET temp = 11;
CREATE reading:['London', d'2025-01-01T10:00:01Z'] SET temp = 12;
CREATE reading:['London', d'2025-01-01T11:00:00Z'] SET temp = 13;
-- Paris: the first row stores a copy of its key's city, the second stores none.
CREATE reading:['Paris', d'2025-01-01T10:00:00Z'] SET city = 'Paris', temp = 20;
CREATE reading:['Paris', d'2025-01-01T10:00:00.250Z'] SET temp = 21;
