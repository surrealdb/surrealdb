/**
[test]

# count results

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
value = "0"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

# count_value results

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
count_value(true);
count_value(false);
count_value(15 > 10);
count_value(15 < 10);
count_value([1]);
count_value([1,2]);
count_value(0..2);
count_value(|test:0..2|);
count_value({a: 1, b: 2});
count_value(1d);
