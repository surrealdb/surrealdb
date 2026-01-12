/**
[test]

[[test.results]]
value = "{2, 4}"

[[test.results]]
value = "{4, 5}"

[[test.results]]
value = "{2,}"

[[test.results]]
value = "{,}"

[[test.results]]
value = "{'banana',}"

*/

-- Filter even numbers
RETURN set::filter(type::set([1, 2, 3, 4, 5]), |$v| $v % 2 == 0);

-- Filter numbers greater than 3
RETURN set::filter(type::set([1, 2, 3, 4, 5]), |$v| $v > 3);

-- Filter specific value
RETURN set::filter(type::set([1, 2, 3, 4, 5]), 2);

-- Filter all (none match)
RETURN set::filter(type::set([1, 2, 3]), |$v| $v > 10);

-- Filter strings by length
RETURN set::filter(type::set(['apple', 'banana', 'kiwi']), |$v| string::len($v) > 5);

