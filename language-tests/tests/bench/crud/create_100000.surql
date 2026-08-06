/**
[bench]
reason = "Create a batch of 100,000 records with realistic field data (the mock-target memory-scaling case)"
run = true
rebuild = true
warmup = "2s"
sample-size = 10
measurement-time = "30s"

[test]
run = false
*/

CREATE |record:100000| SET
	text = rand::string(50),
	name = rand::string(20),
	age = rand::int(1, 100),
	city = rand::enum('London', 'Paris', 'Berlin', 'Tokyo', 'New York', 'Sydney', 'Madrid', 'Rome', 'Beijing'),
	score = rand::float(0.0, 100.0),
	number = rand::int(1, 5000),
	integer = rand::int(),
	active = rand::bool(),
	created_at = rand::time(),
	http_status = rand::enum(200, 201, 400, 404, 500),
	request_id = rand::uuid(),
	status = rand::enum('draft', 'published', 'archived'),
	description = rand::string(100, 400),
	tier = rand::enum(0, 1, 2, 3),
	words = array::join(array::map(array::range(0, 24), |$i| rand::enum('hello', 'world', 'foo', 'bar', 'test', 'search', 'data', 'query', 'index', 'document', 'database', 'performance')), ' '),
	tags = [
		rand::enum('alpha', 'beta', 'gamma', 'delta', 'epsilon'),
		rand::enum('theta', 'iota', 'kappa', 'lambda', 'mu'),
		rand::enum('nu', 'xi', 'omicron', 'pi', 'rho')
	],
	geography = {
		code: rand::string(3),
		text: rand::string(15),
		location: rand::enum('Africa', 'Antarctica', 'Asia', 'Europe', 'North America', 'South America', 'Oceania'),
		description: rand::string(80)
	}
;
