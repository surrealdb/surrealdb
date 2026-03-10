/**
[test]
reason = "Test set::find function for finding first matching element"

[[test.results]]
value = "2"

[[test.results]]
value = "4"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'banana'"

*/

-- Find first even number
RETURN set::find(type::set([1, 2, 3, 4, 5]), |$v| $v % 2 == 0);

-- Find first number greater than 3
RETURN set::find(type::set([1, 2, 3, 4, 5]), |$v| $v > 3);

-- Find specific value
RETURN set::find(type::set([1, 2, 3, 4, 5]), 3);

-- Find non-existent value
RETURN set::find(type::set([1, 2, 3]), |$v| $v > 10);

-- Find string by length
RETURN set::find(type::set(['apple', 'banana', 'kiwi']), |$v| string::len($v) > 5);

