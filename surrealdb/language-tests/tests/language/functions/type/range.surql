/**
[test]

[[test.results]]
value = ".."

[[test.results]]
value = "1..2"

[[test.results]]
value = "1..2"

*/
RETURN type::range(..);
RETURN type::range(1..2);
RETURN type::range([1, 2]);
