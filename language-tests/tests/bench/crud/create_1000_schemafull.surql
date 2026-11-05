/**
[env]
imports = ["bench/util/schemafull-record-table.surql"]

[bench]
reason = "Create a batch of 1000 records into a schema-rich table (typed fields, VALUE/ASSERT/DEFAULT clauses, field permissions, an event, and a composite index)"
run = true
rebuild = true
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

CREATE |record:1000| SET
	text = rand::string(50),
	name = rand::string(20),
	age = rand::int(1, 100),
	city = rand::enum('London', 'Paris', 'Berlin', 'Tokyo', 'New York'),
	score = rand::float(0.0, 100.0),
	active = rand::bool(),
	created_at = rand::time(),
	status = rand::enum('draft', 'published', 'archived'),
	tags = [rand::enum('alpha', 'beta', 'gamma')],
	geography = { code: rand::string(3), text: rand::string(15) };
