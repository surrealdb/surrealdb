/**
[env]
imports = ["bench/util/records-100000-schemafull.surql"]

[bench]
reason = "Update every one of 100,000 records of a SCHEMAFULL table with a deterministic row-local computation that costs more than the write: the case where the compute half can show"
run = true
rebuild = true
rebuild-file-backend = true
warmup = "2s"
sample-size = 10
measurement-time = "30s"

[test]
run = false
*/

UPDATE record SET h = crypto::sha256(name + <string> n), slug = string::slug(name), w = string::words(name) RETURN NONE;
