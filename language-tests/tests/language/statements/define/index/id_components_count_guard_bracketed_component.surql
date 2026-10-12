/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A COUNT index's guard may read a component of the record id through the
	record with a string literal in brackets (`$this['city']`), which reads
	the element of the id as `$this.city` does. `@['city']` indexes the record
	id rather than reading the record, and reads NONE in the guard as in a
	query. A count the index answers equals the count WITH NOINDEX gives, at
	write time and over records that existed when the index was built.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: 'f', id: reading:['f', 1], n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: 'f', id: reading:['f', 2], n: 2 }]"

[[test.results]]
value = "[{ city: 'g', id: reading:['g', 3], n: 3 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

*/

DEFINE FIELD id ON reading TYPE [city: string, n: int];
CREATE reading SET city = 'f', n = 1;
DEFINE INDEX by_this ON reading COUNT WHERE $this['city'] = 'f';
DEFINE INDEX by_doc ON reading COUNT WHERE @['city'] = 'g';
CREATE reading SET city = 'f', n = 2;
CREATE reading SET city = 'g', n = 3;
SELECT count() FROM reading WHERE $this['city'] = 'f' GROUP ALL;
SELECT count() FROM reading WITH NOINDEX WHERE $this['city'] = 'f' GROUP ALL;
SELECT count() FROM reading WHERE @['city'] = 'g' GROUP ALL;
SELECT count() FROM reading WITH NOINDEX WHERE @['city'] = 'g' GROUP ALL;
DELETE reading:['f', 1];
SELECT count() FROM reading WHERE $this['city'] = 'f' GROUP ALL;
SELECT count() FROM reading WITH NOINDEX WHERE $this['city'] = 'f' GROUP ALL;
