/**
[test]

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "true"
*/
[1,2] ALLINSIDE [1,2,3]; -- true
[1,2,4] ALLINSIDE [1,2]; -- false
"in" ALLINSIDE "inout"; -- true
"inn" ALLINSIDE "inout"; -- false
["in", "out"] ALLINSIDE "inout"; -- true
["in", "outt"] ALLINSIDE "inout"; -- false
[1,2] ANYINSIDE [1,2,3]; -- true
[1,2,4] ANYINSIDE [1,2,3]; -- true
"in" ANYINSIDE "inout"; -- true
"inn" ANYINSIDE "inout"; -- false
["in", "out"] ANYINSIDE "inout"; -- true
["in", "outt"] ANYINSIDE "inout"; -- true