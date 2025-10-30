/**
[test]
wip = true
reason = "Test set::reduce function for reducing a set to a single value"

*/

-- Reduce to sum all numbers (uses first element as initial)
RETURN set::reduce(type::set([1, 2, 3, 4, 5]), |$acc, $v| $acc + $v);

-- Reduce to multiply all numbers
RETURN set::reduce(type::set([1, 2, 3, 4, 5]), |$acc, $v| $acc * $v);

-- Reduce empty set
RETURN set::reduce(type::set([]), |$acc, $v| $acc + $v);

-- Reduce single element (returns the element)
RETURN set::reduce(type::set([5]), |$acc, $v| $acc + $v);

-- Reduce to find max
RETURN set::reduce(type::set([1, 5, 3, 2, 4]), |$acc, $v| IF $v > $acc THEN $v ELSE $acc END);

