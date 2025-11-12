/**
[test]

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

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

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/

-- Regular values in arrays and sets
[1,2] ANYINSIDE [1,2,3]; -- true
{1,2} ANYINSIDE {1,2,3}; -- true
[1,2,4] ANYINSIDE [1,2,3]; -- true
{1,2,4} ANYINSIDE {1,2,3}; -- true
[1,2] ANYINSIDE [3,4]; -- false;
{1,2} ANYINSIDE {3,4}; -- false;

-- Array values in sets and vice versa
[1,2] ANYINSIDE {1,2}; -- true
{1,2} ANYINSIDE [1,2]; -- true

-- Strings in arrays and sets
["in", "out"] ANYINSIDE "inout"; -- true
{"in", "out"} ANYINSIDE "inout"; -- true
["in", "outt"] ANYINSIDE "inout"; -- true
{"in", "outt"} ANYINSIDE "inout"; -- true

-- Strings in strings
"in" ANYINSIDE "inout"; -- true
"inn" ANYINSIDE "inout"; -- false