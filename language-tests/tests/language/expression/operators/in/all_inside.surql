/**
[test]

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

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
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/

-- Regular values in arrays and sets
[1,2] ALLINSIDE [1,2,3]; -- true
{1,2} ALLINSIDE {1,2,3}; -- true
[1,2,4] ALLINSIDE [1,2]; -- false
{1,2,4} ALLINSIDE {1,2}; -- false
[1,2] ANYINSIDE [3,4]; -- false;
{1,2} ANYINSIDE {3,4}; -- false;

-- Array values in sets and vice versa
[1,2] ALLINSIDE {1,2}; -- true
{1,2} ALLINSIDE [1,2]; -- true

-- Strings in arrays and sets
["in", "out"] ALLINSIDE "inout"; -- true
{"in", "out"} ALLINSIDE "inout"; -- true
["in", "outt"] ALLINSIDE "inout"; -- false
{"in", "outt"} ALLINSIDE "inout"; -- false

-- Strings in strings
"in" ALLINSIDE "inout"; -- true
"inn" ALLINSIDE "inout"; -- false