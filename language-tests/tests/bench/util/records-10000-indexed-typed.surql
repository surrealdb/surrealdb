/**
[env]
imports = ["./records-10000-indexed.surql"]

[test]
run = false
reason = "The indexed records dataset with `number` declared an int, so its index holds one entry per record and a count over it is answered from the index keys alone."

[[test.results]]
value = "NONE"
*/

DEFINE FIELD number ON record TYPE int;
