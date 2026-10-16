/**
[env]
imports = ["bench/util/references-100000.surql"]

[bench]
reason = "Read a field over an array of record links for every user of 20000"
run = true
rebuild = false
warmup = "2s"
sample-size = 20
measurement-time = "30s"

[test]
run = false
*/

SELECT friends.name FROM user;
