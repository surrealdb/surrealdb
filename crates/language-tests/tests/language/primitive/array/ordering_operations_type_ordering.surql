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
value = "true"

*/

[NONE] < [NULL];
[NULL] < [true];
[true] < [1];
[1] < [1d];
[1d] < [u"FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"];
[u"FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"] < [[]];
[[]] < [{}];
[{}] < [(1,2)];
[(1,2)] < [a:b];
[a:b] < [type::table("a")];
[/a/] < [1..2];
[1..2] < [|$a| {}];
