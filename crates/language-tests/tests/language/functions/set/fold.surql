/**
[test]

[[test.results]]
value = "15"

[[test.results]]
value = "120"

[[test.results]]
value = "10"

[[test.results]]
value = "'applebananakirwi'"

[[test.results]]
value = "[1, 2, 3, 4, 5]"

*/

-- Fold to sum all numbers
RETURN set::fold(type::set([1, 2, 3, 4, 5]), 0, |$acc, $v| $acc + $v);

-- Fold to multiply all numbers
RETURN set::fold(type::set([1, 2, 3, 4, 5]), 1, |$acc, $v| $acc * $v);

-- Fold with initial value
RETURN set::fold(type::set([1, 2, 3]), 4, |$acc, $v| $acc + $v);

-- Fold strings
RETURN set::fold(type::set(['apple', 'banana', 'kirwi']), '', |$acc, $v| $acc + $v);

-- Fold to build array
RETURN set::fold(type::set([1, 2, 3, 4, 5]), [], |$acc, $v| array::append($acc, $v));

