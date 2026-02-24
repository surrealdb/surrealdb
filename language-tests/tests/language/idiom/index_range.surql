/**
[test]
reason = "Test indexing arrays with range expressions"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3, 4]"

[[test.results]]
value = "[2, 3, 4]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[1, 2, 3]"
*/

[1,2,3,4][1..2];
[1,2,3,4][1..=2];
[1,2,3,4][1>..=2];
[1,2,3,4][1>..];
[1,2,3,4][1..];
[1,2,3,4][..2];
[1,2,3,4][..=2];
