/**
[test]
run = false
reason = "Import only: 20k `source` rows that crud/insert_20000 re-inserts into `record` with one INSERT statement"

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE source SCHEMALESS;
	DEFINE TABLE record SCHEMALESS;

	CREATE |source:20000|
		SET text = rand::string(50),
			name = rand::string(20),
			age = rand::int(1, 100),
			city = rand::enum('London', 'Paris', 'Berlin', 'Tokyo', 'New York', 'Sydney', 'Madrid', 'Rome', 'Beijing'),
			score = rand::float(0.0, 100.0),
			number = rand::int(1, 5000),
			active = rand::bool(),
			created_at = rand::time(),
			status = rand::enum('draft', 'published', 'archived'),
			tags = [
				rand::enum('alpha', 'beta', 'gamma', 'delta', 'epsilon'),
				rand::enum('theta', 'iota', 'kappa', 'lambda', 'mu')
			]
	RETURN NONE;

	RETURN 'OK';
};
