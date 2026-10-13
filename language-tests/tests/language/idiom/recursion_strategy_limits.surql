/**
[test]
reason = "Parity (#471, gap H5): the collect / path / shortest / repeat recursion strategies must raise the idiom recursion limit error when an unbounded recursion exhausts the system `idiom_recursion_limit`, matching the default strategy and the legacy compute() engine. The new streaming executor previously returned a silently-truncated result for these four strategies (only `default` raised the error). The other side of the contract is pinned too: an explicit user bound equal to the system limit (`{1..256}`) must stop silently, not error."
issue = 471

[[test.results]]
value = "NONE"

[[test.results]]
error = "Exceeded the idiom recursion limit of 256."

[[test.results]]
error = "Exceeded the idiom recursion limit of 256."

[[test.results]]
error = "Exceeded the idiom recursion limit of 256."

[[test.results]]
error = "Exceeded the idiom recursion limit of 256."

[[test.results]]
value = "256"

[[test.results]]
value = "1"

[[test.results]]
value = "256"

[[test.results]]
value = "a:1"

[env]
timeout = 15000
*/

-- A non-cyclic chain longer than idiom_recursion_limit (256): a:1 -> a:2 -> ... -> a:301.
FOR $i IN 1..=300 {
	UPSERT type::record('a', $i) SET link = type::record('a', $i + 1);
};

-- collect: unbounded recursion, the frontier is still advancing at the limit.
a:1.{1..+collect}(.link);

-- path: unbounded recursion, paths are still active at the limit.
a:1.{1..+path}(.link);

-- shortest: the target is never on the chain, so the search exhausts the limit.
a:1.{1..+shortest=a:999}(.link);

-- repeat (`@`): unbounded destructure recursion exhausts the limit.
a:1.{1..}.{ id, link: link.@ };

-- The silent-stop side: an explicit user bound equal to the system limit (256)
-- must NOT error, for every strategy.

-- collect: 256 nodes collected (a:2 .. a:257), no error.
count(a:1.{1..256+collect}(.link));

-- path: the single chain path is completed at the bound, no error.
count(a:1.{1..256+path}(.link));

-- shortest: target reachable at exactly the bound; path of 256 nodes, no error.
count(a:1.{1..256+shortest=a:257}(.link));

-- repeat (`@`): bounded destructure recursion; returns the nested tree, no error.
a:1.{1..256}.{ id, link: link.@ }.id;
