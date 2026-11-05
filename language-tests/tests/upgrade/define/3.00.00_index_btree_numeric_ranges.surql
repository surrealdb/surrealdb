/**
[env]
imports = ["upgrade/define/3.00.00_index_btree_numeric_ranges_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
reason = """
A range read over a B-tree or UNIQUE index computes its key bounds from the
predicate's values, and finds an entry only when those bounds sort around the
key bytes the writer chose for the entry's value. Ints, floats and decimals of
one value share key bytes and sort numerically, so a read written in one kind
finds entries written in another, and a bound between two numbers admits
exactly the numbers between them. The same reads scanned backward are in the
_backward sibling. An index that cannot be read still returns these rows
through a table scan; the plans are asserted in the _current sibling.

Numeric expectations compare by value, not kind, so ties between `1`, `1f` and
`1dec` are equal in whatever order they arrive. Equality reads return ids,
which tell the kinds apart."""

# Non-unique: each open bound.
[[test.results]]
value = "[0.25, 1, 1, 1, 1.5, 2, 9007199254740992, 9007199254740993, 100000000000000000000dec]"

[[test.results]]
value = "[0, 0, 0, 0.25, 1, 1, 1, 1.5, 2, 9007199254740992, 9007199254740993, 100000000000000000000dec]"

[[test.results]]
value = "[-9007199254740993, -1000.5, -2.5, -1]"

[[test.results]]
value = "[-9007199254740993, -1000.5, -2.5, -1, 0, 0, 0]"

# Non-unique: a bounded range.
[[test.results]]
value = "[0, 0, 0, 0.25, 1, 1, 1]"

# Non-unique: equality across kinds and past float precision.
[[test.results]]
value = "[num:5, num:6, num:7]"

[[test.results]]
value = "[num:9, num:10, num:11]"

[[test.results]]
value = "[num:15]"

[[test.results]]
value = "[num:1]"

# Non-unique: counts answered by the index keys.
[[test.results]]
value = "[{ count: 12 }]"

[[test.results]]
value = "[{ count: 7 }]"

# Unique: each open bound.
[[test.results]]
value = "[0.25, 1, 1.5, 2, 9007199254740992, 9007199254740993, 100000000000000000000dec]"

[[test.results]]
value = "[0, 0.25, 1, 1.5, 2, 9007199254740992, 9007199254740993, 100000000000000000000dec]"

[[test.results]]
value = "[-9007199254740993, -1000.5, -2.5, -1]"

[[test.results]]
value = "[-9007199254740993, -1000.5, -2.5, -1, 0]"

# Unique: a bounded range.
[[test.results]]
value = "[0, 0.25, 1]"

# Unique: equality across kinds and past float precision.
[[test.results]]
value = "[numu:5]"

[[test.results]]
value = "[numu:11]"

[[test.results]]
value = "[numu:14]"

[[test.results]]
value = "[numu:15]"

# Unique: a count answered by the index keys.
[[test.results]]
value = "[{ count: 8 }]"

# Unique: a value of another kind still collides with the old entry.
[[test.results]]
error = "Database index `numu_n` already contains 1, with record `numu:11`"

[[test.results]]
error = "Database index `numu_n` already contains 0f, with record `numu:5`"

[[test.results]]
value = "[{ id: numu:20, n: 0.5dec }]"

[[test.results]]
value = "[0.25, 0.5, 1]"

*/
-- Non-unique: each open bound.
SELECT VALUE n FROM num WHERE n > 0 ORDER BY n;
SELECT VALUE n FROM num WHERE n >= 0 ORDER BY n;
SELECT VALUE n FROM num WHERE n < 0 ORDER BY n;
SELECT VALUE n FROM num WHERE n <= 0 ORDER BY n;
-- Non-unique: a bounded range.
SELECT VALUE n FROM num WHERE n > -1 AND n < 1.5 ORDER BY n;
-- Non-unique: `-0f`, `0` and `0dec` are one value; so are `1`, `1f` and
-- `1dec`; 2^53 + 1 is not 2^53.
SELECT VALUE id FROM num WHERE n = 0 ORDER BY id;
SELECT VALUE id FROM num WHERE n = 1dec ORDER BY id;
SELECT VALUE id FROM num WHERE n = 9007199254740993;
SELECT VALUE id FROM num WHERE n = -9007199254740993;
-- Non-unique: counts answered by the index keys.
SELECT count() FROM num WHERE n >= 0 GROUP ALL;
SELECT count() FROM num WHERE n > -1 AND n < 1.5 GROUP ALL;

-- Unique: each open bound.
SELECT VALUE n FROM numu WHERE n > 0 ORDER BY n;
SELECT VALUE n FROM numu WHERE n >= 0 ORDER BY n;
SELECT VALUE n FROM numu WHERE n < 0 ORDER BY n;
SELECT VALUE n FROM numu WHERE n <= 0 ORDER BY n;
-- Unique: a bounded range.
SELECT VALUE n FROM numu WHERE n > -1 AND n < 1.5 ORDER BY n;
-- Unique: equality across kinds and past float precision.
SELECT VALUE id FROM numu WHERE n = 0;
SELECT VALUE id FROM numu WHERE n = 1f;
SELECT VALUE id FROM numu WHERE n = 9007199254740992;
SELECT VALUE id FROM numu WHERE n = 9007199254740993;
-- Unique: a count answered by the index keys.
SELECT count() FROM numu WHERE n >= 0 GROUP ALL;

-- Unique: an int collides with the decimal the old binary wrote, and a float
-- zero with its negative zero, since each pair shares key bytes.
CREATE numu:21 SET n = 1;
CREATE numu:22 SET n = 0f;
-- A new value lands between the old entries and is read in order with them.
CREATE numu:20 SET n = 0.5dec;
SELECT VALUE n FROM numu WHERE n > 0 AND n <= 1 ORDER BY n;
