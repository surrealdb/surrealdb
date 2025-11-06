/**
[test]

[[test.results]]
value = "{2, 4, 6, 8, 10}"

[[test.results]]
value = "{1, 4, 9, 16, 25}"

[[test.results]]
value = "{2, 3, 4, 5, 6}"

[[test.results]]
value = "{,}"

[[test.results]]
value = "{4, 5, 6}"

*/

-- Map doubling numbers
RETURN set::map(type::set([1, 2, 3, 4, 5]), |$v| $v * 2);

-- Map squaring numbers
RETURN set::map(type::set([1, 2, 3, 4, 5]), |$v| $v * $v);

-- Map incrementing numbers
RETURN set::map(type::set([1, 2, 3, 4, 5]), |$v| $v + 1);

-- Map empty set
RETURN set::map(type::set([]), |$v| $v * 2);

-- Map strings to their lengths
RETURN set::map(type::set(['apple', 'banana', 'kiwi']), |$v| string::len($v));

