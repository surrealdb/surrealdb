/**
[test]

# count results

[[test.results]]
value = "0"

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
value = "0"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

# count_all results

[[test.results]]
value = "0"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
value = "2"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

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


count_value();
count_all(true);
count_all(false);
count_all(15 > 10);
count_all(15 < 10);
count_all([1]);
count_all([1,2]);
count_all(0..2);
count_all(|test:0..2|);
count_all({a: 1, b: 2});
count_all(1d);
