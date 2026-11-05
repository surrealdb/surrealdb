/**
[env]
namespace = true
database = true
auth = { level = "owner" }
imports = ["reproductions/a_range_over_a_composite_unique_index_prefix_bounds_the_whole_prefix_import.surql"]

[test]
reason = """
A range on the leading column of a composite index admits or excludes every
entry whose leading value is the bound, whatever follows it. A UNIQUE index ends
its keys at the last column, with no record id after them, and a NONE tuple sits
under the same leading value as a plain entry; `g > 'y'` must still skip
('y', 5), ('y', 6) and ('y', NONE), and `g <= 'y'` must still reach them. The
planner takes the key range as the whole answer to the comparison and keeps no
filter over it, so a range that bounds only the bare value ['y'] returns rows
the predicate refuses and loses rows it admits. Each statement runs over the
UNIQUE index (`tu`) and over the plain one (`tc`), which must agree, reading
forwards and backwards, and counting, and a range literal under `IN` is the
same access as the comparisons that spell it out.
"""

# tu: UNIQUE (g, a), ascending
[[test.results]]
value = "[tu:5]"

[[test.results]]
value = "[tu:2, tu:3, tu:4, tu:5]"

[[test.results]]
value = "[tu:1]"

[[test.results]]
value = "[tu:1, tu:2, tu:3, tu:4]"

[[test.results]]
value = "[tu:2, tu:3, tu:4]"

[[test.results]]
value = "[tu:2, tu:3, tu:4]"

[[test.results]]
value = "[tu:2, tu:3, tu:4]"

# tu: descending
[[test.results]]
value = "['z']"

[[test.results]]
value = "['z', 'y', 'y', 'y']"

[[test.results]]
value = "['x']"

[[test.results]]
value = "['y', 'y', 'y', 'x']"

# tu: counts
[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 4 }]"

# tc: plain (g, a), ascending
[[test.results]]
value = "[tc:5]"

[[test.results]]
value = "[tc:2, tc:3, tc:4, tc:5]"

[[test.results]]
value = "[tc:1]"

[[test.results]]
value = "[tc:1, tc:2, tc:3, tc:4]"

[[test.results]]
value = "[tc:2, tc:3, tc:4]"

[[test.results]]
value = "[tc:2, tc:3, tc:4]"

[[test.results]]
value = "[tc:2, tc:3, tc:4]"

# tc: descending
[[test.results]]
value = "['z']"

[[test.results]]
value = "['z', 'y', 'y', 'y']"

[[test.results]]
value = "['x']"

[[test.results]]
value = "['y', 'y', 'y', 'x']"

# tc: counts
[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 4 }]"

*/

SELECT VALUE id FROM tu WHERE g > 'y' ORDER BY id;
SELECT VALUE id FROM tu WHERE g >= 'y' ORDER BY id;
SELECT VALUE id FROM tu WHERE g < 'y' ORDER BY id;
SELECT VALUE id FROM tu WHERE g <= 'y' ORDER BY id;
SELECT VALUE id FROM tu WHERE g > 'x' AND g < 'z' ORDER BY id;
SELECT VALUE id FROM tu WHERE g >= 'y' AND g <= 'y' ORDER BY id;
SELECT VALUE id FROM tu WHERE g IN 'x'>..='y' ORDER BY id;

SELECT VALUE g FROM tu WHERE g > 'y' ORDER BY g DESC;
SELECT VALUE g FROM tu WHERE g >= 'y' ORDER BY g DESC;
SELECT VALUE g FROM tu WHERE g < 'y' ORDER BY g DESC;
SELECT VALUE g FROM tu WHERE g <= 'y' ORDER BY g DESC;

SELECT count() FROM tu WHERE g > 'y' GROUP ALL;
SELECT count() FROM tu WHERE g >= 'y' GROUP ALL;
SELECT count() FROM tu WHERE g < 'y' GROUP ALL;
SELECT count() FROM tu WHERE g <= 'y' GROUP ALL;

SELECT VALUE id FROM tc WHERE g > 'y' ORDER BY id;
SELECT VALUE id FROM tc WHERE g >= 'y' ORDER BY id;
SELECT VALUE id FROM tc WHERE g < 'y' ORDER BY id;
SELECT VALUE id FROM tc WHERE g <= 'y' ORDER BY id;
SELECT VALUE id FROM tc WHERE g > 'x' AND g < 'z' ORDER BY id;
SELECT VALUE id FROM tc WHERE g >= 'y' AND g <= 'y' ORDER BY id;
SELECT VALUE id FROM tc WHERE g IN 'x'>..='y' ORDER BY id;

SELECT VALUE g FROM tc WHERE g > 'y' ORDER BY g DESC;
SELECT VALUE g FROM tc WHERE g >= 'y' ORDER BY g DESC;
SELECT VALUE g FROM tc WHERE g < 'y' ORDER BY g DESC;
SELECT VALUE g FROM tc WHERE g <= 'y' ORDER BY g DESC;

SELECT count() FROM tc WHERE g > 'y' GROUP ALL;
SELECT count() FROM tc WHERE g >= 'y' GROUP ALL;
SELECT count() FROM tc WHERE g < 'y' GROUP ALL;
SELECT count() FROM tc WHERE g <= 'y' GROUP ALL;
