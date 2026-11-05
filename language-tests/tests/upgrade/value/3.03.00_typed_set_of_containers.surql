/**
[env]
imports = ["upgrade/value/3.03.00_typed_set_of_containers_import.surql"]

[test]
upgrade = true
reason = """
A field typed as a set of arrays carries an implicit nested field, `s[*]`, and
a 3.3 binary stores such a set as a set holding copies of the set it was given,
which its own decoder refuses. The current checkout must read every such record,
delete one, and rewrite one through the documented repair into the set it was
given. A record this checkout wrote is already well-formed, and the repair
leaves it unchanged, so every assertion holds whichever binary wrote the import.
"""
introduced = "3.3.0"
# A 3.3 reader refuses the records a 3.3 writer stored.
version = ">=3.4.0-a"

[[test.results]]
value = "[true, true]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{[1, 2], [3]}]"
*/

-- Every record is readable, and its field still holds a set.
SELECT VALUE type::is_set(s) FROM tsc ORDER BY id;
-- A record is deletable.
DELETE tsc:2;
-- The repair for a set stored one level too deep.
UPDATE tsc SET s = s.first() WHERE type::is_set(s.first()) RETURN NONE;
-- The repaired record holds the set it was given.
SELECT VALUE s FROM tsc ORDER BY id;
