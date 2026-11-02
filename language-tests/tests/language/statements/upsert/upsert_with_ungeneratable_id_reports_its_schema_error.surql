/**
[env]
namespace = true
database = true

[test]
requires = ["scripting"]
reason = """
An UPSERT on a table whose create attempt fails before a record id exists — an
id the `id` field's declared kind rejects — has no record to fall back to
updating. It fails with the schema error that caused it, rolling back the
create attempt's save point, rather than reaching the update retry and
reporting that a document id was expected. The transaction carries on cleanly
when the error is caught: a later write in it commits.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `id` of `person:not_an_int`: Expected `int` but found `'not_an_int'`"

[[test.results]]
value = "['y']"

[[test.results]]
value = "[{ id: person:1, name: 'y' }]"
*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD id ON person TYPE int;
DEFINE FIELD name ON person TYPE string;
UPSERT person SET id = 'not_an_int', name = 'x';

function() {
	try {
		await surrealdb.query(`UPSERT person SET id = 'not_an_int', name = 'x'`);
	} catch (e) {
	}
	return await surrealdb.query(`CREATE person:1 SET name = 'y' RETURN VALUE name`);
};
SELECT * FROM person;
