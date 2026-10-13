/**
[env]
namespace = true
database = true

[test]
reason = """
Tie order is input order at every input size. Small inputs are ordered
correctly by any sort, so this drives enough rows through the sorts to reach
their partitioning and parallel paths, where an unstable sort reorders equal
keys. Each assertion rebuilds the expected output from the unsorted scan --
concatenating the tie groups in key order -- so it states the contract rather
than a captured permutation.

The paths covered are the full in-memory sort (no LIMIT), the top-k heap
(LIMIT, and START paging into a tie group, where an arbitrary tie order can
otherwise repeat or drop rows between pages) and the disk-backed external sort
(TEMPFILES, when a temporary directory is configured).
"""
issue = 472

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"
*/

{
	FOR $i IN 0..2500 {
		CREATE type::record("g", $i) SET grp = $i % 5, all_equal = 1;
	};
	RETURN "OK";
};

LET $scan = SELECT id, grp FROM g;
-- The tie groups, concatenated in ascending key order, each group in scan order
LET $expected = array::flatten(array::map([0, 1, 2, 3, 4], |$grp| ($scan[WHERE grp = $grp]).id));

-- Full in-memory sort
RETURN (SELECT VALUE id FROM g ORDER BY grp) == $expected;
-- A single tie group covering every row keeps scan order in both directions
RETURN (SELECT VALUE id FROM g ORDER BY all_equal) == ($scan.id);
RETURN (SELECT VALUE id FROM g ORDER BY all_equal DESC) == ($scan.id);

-- Top-k heap: LIMIT, and START paging into a tie group
RETURN (SELECT VALUE id FROM g ORDER BY grp LIMIT 30) == $expected[0..30];
RETURN (SELECT VALUE id FROM g ORDER BY grp START 10 LIMIT 20) == $expected[10..30];

-- Disk-backed external sort
RETURN (SELECT VALUE id FROM g ORDER BY grp TEMPFILES) == $expected;
