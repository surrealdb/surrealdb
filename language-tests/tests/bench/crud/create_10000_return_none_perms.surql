/**
[env]
imports = ["bench/util/perms-where-empty.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bench" }

[bench]
reason = "Create a batch of 10000 records as a record user whose select permission is a predicate over a field of the record, returning nothing. The write cannot show such a predicate to be free of raises and effects, so the select gate runs: each created record is judged against the predicate."
run = true
rebuild = true
rebuild-file-backend = true
warmup = "2s"
sample-size = 10
measurement-time = "20s"

[test]
run = false
*/

CREATE |record:10000| SET
	name = rand::string(20),
	age = rand::int(1, 100),
	score = rand::float(0.0, 100.0),
	active = rand::bool(),
	tags = [rand::enum('alpha', 'beta', 'gamma'), rand::enum('delta', 'epsilon')]
	RETURN NONE;
