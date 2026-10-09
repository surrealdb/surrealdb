/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A projected field the ORDER BY does not name is computed after the sort and
its LIMIT: the plan projects above the bounded sort, so a graph traversal or
a subquery in the projection runs for the rows the limit keeps and for no
other. A field the ORDER BY names by its alias is computed once, beneath the
sort, as its key, and the projection reads that value rather than computing
it again.
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
value = '''
'SelectProject [ctx: Db] [projections: name, friends]
    Limit [ctx: Db] [limit: 3]
        Sort [ctx: Db] [sort_keys: age ASC, keep: 3]
            TableScan [ctx: Db] [table: person, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = "[{ friends: [], name: 'p8' }, { friends: ['p6'], name: 'p5' }, { friends: ['p3'], name: 'p2' }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: name, next]
    Limit [ctx: Db] [limit: 2]
        Sort [ctx: Db] [sort_keys: age DESC, keep: 2]
            TableScan [ctx: Db] [table: person, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = "[{ name: 'p3', next: [] }, { name: 'p6', next: ['p3'] }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: name, degree]
    Limit [ctx: Db] [limit: 2]
        Sort [ctx: Db] [sort_keys: count(->knows) DESC, name ASC, keep: 2]
            TableScan [ctx: Db] [table: person, direction: Forward]
''''

[[test.results]]
value = "[{ degree: 1, name: 'p1' }, { degree: 1, name: 'p2' }]"

*/

DEFINE TABLE person SCHEMALESS;
DEFINE TABLE knows TYPE RELATION;
FOR $i IN 1..=8 { CREATE type::record('person', $i) SET age = 20 + ($i * 7) % 11, name = 'p' + <string> $i; };
FOR $i IN 1..=7 { LET $a = type::record('person', $i); LET $b = type::record('person', $i + 1); RELATE $a->knows->$b; };

EXPLAIN SELECT name, ->knows->person.name AS friends FROM person ORDER BY age LIMIT 3;
SELECT name, ->knows->person.name AS friends FROM person ORDER BY age LIMIT 3;

EXPLAIN SELECT name, (SELECT VALUE name FROM person WHERE age > $parent.age ORDER BY age LIMIT 1) AS next FROM person ORDER BY age DESC LIMIT 2;
SELECT name, (SELECT VALUE name FROM person WHERE age > $parent.age ORDER BY age LIMIT 1) AS next FROM person ORDER BY age DESC LIMIT 2;

EXPLAIN SELECT name, count(->knows) AS degree FROM person ORDER BY degree DESC, name LIMIT 2;
SELECT name, count(->knows) AS degree FROM person ORDER BY degree DESC, name LIMIT 2;
