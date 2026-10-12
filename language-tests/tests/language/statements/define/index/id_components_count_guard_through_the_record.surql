/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A COUNT index's guard may read a component of the record id through the
	record (`$this.city`, `@.city`) as well as bare. The guard is evaluated
	over an image that holds no component fields, so each of these reads the
	element of the id, at write time and when the index is built over
	existing records. A count the index answers equals the count WITH
	NOINDEX gives.
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
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Aggregate [ctx: Db] [mode: GROUP ALL]
        Filter [ctx: Db] [predicate: $this.city = 'f']
            ProjectLayer [ctx: Db] [columns: city = id[0] : string @key]
                TableScan [ctx: Db] [table: reading, direction: Forward]
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

*/

DEFINE FIELD id ON reading TYPE [city: string, n: int];
CREATE reading SET city = 'f', n = 1;
DEFINE INDEX by_this ON reading COUNT WHERE $this.city = 'f';
DEFINE INDEX by_doc ON reading COUNT WHERE @.city = 'g';
CREATE reading SET city = 'f', n = 2;
CREATE reading SET city = 'g', n = 3;
SELECT count() FROM reading WHERE $this.city = 'f' GROUP ALL;
SELECT count() FROM reading WITH NOINDEX WHERE $this.city = 'f' GROUP ALL;
SELECT count() FROM reading WHERE @.city = 'g' GROUP ALL;
SELECT count() FROM reading WITH NOINDEX WHERE @.city = 'g' GROUP ALL;
EXPLAIN SELECT count() FROM reading WHERE $this.city = 'f' GROUP ALL;
DELETE reading:['f', 1];
SELECT count() FROM reading WHERE $this.city = 'f' GROUP ALL;
SELECT count() FROM reading WITH NOINDEX WHERE $this.city = 'f' GROUP ALL;
