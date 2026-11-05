/**
# `records-10000-heavy-schema` without the events and the views: twelve typed fields, four
# indexes (one unique) and a change feed. No stage of a write requests a unit of another
# table, which the stages after a `WHERE` that no record satisfies need not be bound for.
# `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE record SCHEMALESS CHANGEFEED 1h;
	DEFINE FIELD text ON record TYPE string;
	DEFINE FIELD name ON record TYPE string ASSERT string::len($value) > 0;
	DEFINE FIELD age ON record TYPE int ASSERT $value >= 0;
	DEFINE FIELD city ON record TYPE string;
	DEFINE FIELD score ON record TYPE float VALUE $value ?? 0.0;
	DEFINE FIELD number ON record TYPE int;
	DEFINE FIELD active ON record TYPE bool VALUE $value ?? false;
	DEFINE FIELD status ON record TYPE string ASSERT $value INSIDE ['draft', 'published', 'archived'];
	DEFINE FIELD tier ON record TYPE int;
	DEFINE FIELD http_status ON record TYPE int;
	DEFINE FIELD tags ON record TYPE array<string>;
	DEFINE FIELD description ON record TYPE string VALUE string::trim($value);
	DEFINE INDEX record_age ON record FIELDS age;
	DEFINE INDEX record_city ON record FIELDS city;
	DEFINE INDEX record_status_tier ON record FIELDS status, tier;
	DEFINE INDEX record_request ON record FIELDS request_id UNIQUE;
	CREATE |record:1..=10000|
		SET text = rand::string(50),
			name = rand::string(20),
			age = rand::int(1, 100),
			city = rand::enum('London', 'Paris', 'Berlin', 'Tokyo', 'New York', 'Sydney', 'Madrid', 'Rome', 'Beijing'),
			score = rand::float(0.0, 100.0),
			number = rand::int(1, 5000),
			active = rand::bool(),
			http_status = rand::enum(200, 201, 400, 404, 500),
			request_id = rand::uuid(),
			status = rand::enum('draft', 'published', 'archived'),
			description = rand::string(100, 400),
			tier = rand::enum(0, 1, 2, 3),
			tags = [
				rand::enum('alpha', 'beta', 'gamma', 'delta', 'epsilon'),
				rand::enum('theta', 'iota', 'kappa', 'lambda', 'mu'),
				rand::enum('nu', 'xi', 'omicron', 'pi', 'rho')
			]
		RETURN NONE;

	RETURN 'OK';
}
