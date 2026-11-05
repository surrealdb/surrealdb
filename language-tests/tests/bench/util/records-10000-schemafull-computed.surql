/**
[test]
run = false

[[test.results]]
value = "'OK'"
*/

-- `record` of 10000 rows in a SCHEMAFULL table, written as packed rows, with one `COMPUTED` field that no query reads. Every stored field is declared before the first row and has no clause, and the table has no permissions. The computed field makes a read of the table enforced, so a scan decodes its rows and the pre-decode filter judges them before it does. `note` is NONE on half the rows, `extra` holds an undeclared key on a third, and `far` is a datetime past the 8-byte slot on 1%.
{
	DEFINE TABLE record SCHEMAFULL;
	DEFINE FIELD text ON record TYPE string;
	DEFINE FIELD name ON record TYPE string;
	DEFINE FIELD words ON record TYPE string;
	DEFINE FIELD description ON record TYPE string;
	DEFINE FIELD age ON record TYPE int;
	DEFINE FIELD number ON record TYPE int;
	DEFINE FIELD integer ON record TYPE int;
	DEFINE FIELD tier ON record TYPE int;
	DEFINE FIELD http_status ON record TYPE int;
	DEFINE FIELD city ON record TYPE string;
	DEFINE FIELD status ON record TYPE string;
	DEFINE FIELD score ON record TYPE float;
	DEFINE FIELD active ON record TYPE bool;
	DEFINE FIELD created_at ON record TYPE datetime;
	DEFINE FIELD request_id ON record TYPE uuid;
	DEFINE FIELD tags ON record TYPE array<string>;
	DEFINE FIELD geography ON record TYPE object;
	DEFINE FIELD geography.code ON record TYPE string;
	DEFINE FIELD geography.text ON record TYPE string;
	DEFINE FIELD geography.location ON record TYPE string;
	DEFINE FIELD geography.description ON record TYPE string;
	DEFINE FIELD geography.region ON record TYPE object;
	DEFINE FIELD geography.region.name ON record TYPE string;
	DEFINE FIELD geography.region.details ON record TYPE object;
	DEFINE FIELD geography.region.details.continent ON record TYPE string;
	DEFINE FIELD geography.region.details.timezone ON record TYPE string;
	DEFINE FIELD note ON record TYPE option<string>;
	DEFINE FIELD extra ON record TYPE object FLEXIBLE;
	DEFINE FIELD far ON record TYPE option<datetime>;
	DEFINE FIELD c ON record TYPE int COMPUTED age + 1;

	CREATE |record:10000|
		SET text = rand::string(50),
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
				description: rand::string(80),
				region: {
					name: rand::string(10),
					details: {
						continent: rand::enum('Africa', 'Antarctica', 'Asia', 'Europe', 'North America', 'South America', 'Oceania'),
						timezone: rand::enum('UTC', 'GMT', 'PST', 'EST', 'CET', 'JST')
					}
				}
			},
			note = IF rand::bool() { rand::string(8) } ELSE { NONE },
			extra = rand::enum({}, {}, { x: rand::int(1, 100) }),
			far = IF rand::int(0, 99) = 0 { d'9999-01-01T00:00:00Z' } ELSE { NONE }
		RETURN NONE;

	RETURN 'OK';
}
