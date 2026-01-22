/**
[test]
reason = "Test set ordering and comparison behavior"

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

-- Sets with same elements in different order are equal
RETURN {3, 1, 2} == {1, 2, 3};

-- Sets maintain sorted order based on Value::Ord
RETURN {3, 1, 2} == {1, 2, 3};

-- Set is less than object (per Value ordering)
RETURN {1, 2, 3} < { foo: 'bar' };

-- Set is greater than array (per Value ordering)
RETURN {1, 2, 3} > [1, 2, 3];

-- Comparing sets by value
RETURN {1, 2} < {1, 2, 3};

-- Equal size sets compare by elements
RETURN {1, 3} > {1, 2};

