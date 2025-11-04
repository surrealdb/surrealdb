/**
[test]

[[test.results]]
value = "[1, 2, 3, 4, 5]"

[[test.results]]
value = "1"

[[test.results]]
value = "[1, 2, 3]"

*/
-- Return a full array
RETURN [1,2,3,4,5];
-- Return the first ("zeroeth") item
RETURN [1,2,3,4,5][0];
-- Return indexes 0 to 2 of an array
RETURN [1,2,3,4,5][0..=2];
