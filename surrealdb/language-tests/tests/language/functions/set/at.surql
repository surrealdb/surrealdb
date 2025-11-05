/**
[test]
reason = "Test set::at function for accessing elements by position"

[[test.results]]
value = "1"

[[test.results]]
value = "2"

[[test.results]]
value = "3"

[[test.results]]
value = "3"

[[test.results]]
value = "2"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

-- Access first element
RETURN set::at(type::set([1, 2, 3]), 0);

-- Access second element
RETURN set::at(type::set([1, 2, 3]), 1);

-- Access third element
RETURN set::at(type::set([1, 2, 3]), 2);

-- Access last element with -1
RETURN set::at(type::set([1, 2, 3]), -1);

-- Access second-to-last element with -2
RETURN set::at(type::set([1, 2, 3]), -2);

-- Access first element with -3
RETURN set::at(type::set([1, 2, 3]), -3);

-- Out of bounds positive index
RETURN set::at(type::set([1, 2, 3]), 10);

-- Out of bounds negative index
RETURN set::at(type::set([1, 2, 3]), -10);

-- Empty set
RETURN set::at(type::set([]), 0);
