/**
[test]

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
value = "0"

[[test.results]]
value = "1"

[[test.results]]
value = "0"

[[test.results]]
value = "1"

[[test.results]]
value = "2"

[[test.results]]
value = "0"

[[test.results]]
value = "2"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

*/
count();
count(true);
count(false);
count(15 > 10);
count(15 < 10);
count([1]);
count([1,2]);
count(0..2);
count(|test:0..2|);
count({a: 1, b: 2});
count(1d);
