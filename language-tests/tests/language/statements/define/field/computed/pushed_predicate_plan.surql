/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = """
	A computed field is a column set on the row above the scan, so a
	conjunct that reads it is judged above the layer. The other conjuncts
	of the predicate read only stored fields and move beneath the layer: the
	scan applies them per row before a row is decoded into the layer, and an
	index over one of them drives the scan. A conjunct reading the row through
	`$this` may reach the computed column and stays above; a predicate on the
	computed column alone leaves the scan bare.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: total = 4]
        ProjectLayer [ctx: Db] [columns: total = price * 2 : int @computed]
            TableScan [ctx: Db] [table: items, direction: Forward, predicate: note = 'a', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: total = 4]
        ProjectLayer [ctx: Db] [columns: total = price * 2 : int @computed]
            IndexScan [ctx: Db] [index: idx_price, access: = 2, direction: Forward, predicate: price = 2]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: total = 4]
        ProjectLayer [ctx: Db] [columns: total = price * 2 : int @computed]
            TableScan [ctx: Db] [table: items, direction: Forward]
''''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: $this.total = 4]
        ProjectLayer [ctx: Db] [columns: total = price * 2 : int @computed]
            TableScan [ctx: Db] [table: items, direction: Forward, predicate: note = 'a', pre_decode_filter: yes]
"'''
*/

DEFINE TABLE items SCHEMAFULL;
DEFINE FIELD price ON items TYPE int;
DEFINE FIELD note ON items TYPE string;
DEFINE FIELD total ON items TYPE int COMPUTED price * 2;
DEFINE INDEX idx_price ON items FIELDS price;
EXPLAIN SELECT VALUE id FROM items WHERE total = 4 AND note = 'a';
EXPLAIN SELECT VALUE id FROM items WHERE total = 4 AND price = 2;
EXPLAIN SELECT VALUE id FROM items WHERE total = 4;
EXPLAIN SELECT VALUE id FROM items WHERE $this.total = 4 AND note = 'a';
