/**
[test]
wip = true
reason = "Test set::map function for transforming set elements"
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

