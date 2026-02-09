/**
[test]

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
error = "Expected `array<int,3>` but found a collection of length `2`"

*/

<array> {1,2};
<array> {1,2,2};
<array<int,3>> {1,2,2}