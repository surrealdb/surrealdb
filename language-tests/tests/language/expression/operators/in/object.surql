/**
[test]

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

'a' IN { a: 1 };
'b' IN { a: 1 };
'1' IN { a: 1 };
'1' IN { 1: 1 };
1 IN { 1: 1 };
