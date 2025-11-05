/**
[test]

[[test.results]]
value = "1"

[[test.results]]
value = "0"

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
value = "2"

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
value = "0"

[[test.results]]
value = "NONE"

[[test.results]]
value = "0"

[[test.results]]
value = "NONE"

[[test.results]]
value = "0"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

*/
count();
count(NONE);
count(NULL);
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
count("");
count("test");
count(b"");
count(<bytes>"test");

count($undefined);

LET $exists = NONE;
count($exists);

LET $exists = [];
count($exists);

LET $exists = [1,2,3];
count($exists);
