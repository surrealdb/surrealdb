/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A filter's conjuncts are ordered by cost class, field comparisons before
builtin calls, where neither can raise or has an effect: then the order decides
only how much work a rejected row costs. A conjunct that can raise (a cast, a
call that rejects its argument's kind, a user-defined function, a subquery)
keeps its place and nothing moves across it, so the statement raises for the
same rows, and returns the same rows, as written.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1, n: 1, name: 'one' }]"

[[test.results]]
value = "[{ id: item:2, n: 2, name: 2 }]"

[[test.results]]
value = "[{ id: item:3, n: 3, name: 'three' }]"

[[test.results]]
value = "[{ id: item:4, n: 4, name: 'four' }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: n > 2 AND type::is_string(name)]
        TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = "[{ id: item:3 }, { id: item:4 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: n > 2 AND type::is_string(name)]
        TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: string::len(name) > 3 AND n > 2]
        TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
error = "Incorrect arguments for function string::len(). Argument 1 was the wrong type. Expected `string` but found `2`"

[[test.results]]
value = "[{ id: item:3 }, { id: item:4 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: type::is_string(name) AND <int> n > 0 AND n > 2]
        TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = "[{ id: item:3 }, { id: item:4 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: fn::big(n) AND n < 4 AND type::is_string(name)]
        TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = "[{ id: item:3 }]"

[[test.results]]
value = "[{ id: item:3 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: n > 2]
        RecordIdScan [ctx: Db] [record_id: item:3]
''''

[[test.results]]
value = "[{ id: item:3 }]"

[[test.results]]
value = "[]"

*/

DEFINE FUNCTION fn::big($n: any) { IF $n > 9 { THROW 'too big' }; RETURN $n > 2 };
CREATE item:1 SET n = 1, name = 'one';
CREATE item:2 SET n = 2, name = 2;
CREATE item:3 SET n = 3, name = 'three';
CREATE item:4 SET n = 4, name = 'four';

-- Both total: the comparison moves ahead of the type test.
EXPLAIN SELECT id FROM item WHERE type::is_string(name) AND n > 2;
SELECT id FROM item WHERE type::is_string(name) AND n > 2;

-- Already cheapest first: unchanged.
EXPLAIN SELECT id FROM item WHERE n > 2 AND type::is_string(name);

-- A call that rejects a number holds its place: item:2 raises, as written.
EXPLAIN SELECT id FROM item WHERE string::len(name) > 3 AND n > 2;
SELECT id FROM item WHERE string::len(name) > 3 AND n > 2;

-- Written the other way, item:2 is rejected before the call reaches it.
SELECT id FROM item WHERE n > 2 AND string::len(name) > 3;

-- Nothing crosses the call: the type test before it and the comparison
-- after it keep their sides.
EXPLAIN SELECT id FROM item WHERE type::is_string(name) AND <int> n > 0 AND n > 2;
SELECT id FROM item WHERE type::is_string(name) AND <int> n > 0 AND n > 2;

-- A user-defined function and a subquery hold their places; the conjuncts
-- after each are ordered among themselves.
EXPLAIN SELECT id FROM item WHERE fn::big(n) AND type::is_string(name) AND n < 4;
SELECT id FROM item WHERE fn::big(n) AND type::is_string(name) AND n < 4;
SELECT id FROM item WHERE (SELECT VALUE n FROM ONLY $parent.id) > 1 AND type::is_string(name) AND n < 4;

-- A record lookup answers `id = r`: the conjunct is dropped, the rest kept.
EXPLAIN SELECT id FROM item WHERE id = item:3 AND n > 2;
SELECT id FROM item WHERE id = item:3 AND n > 2;
SELECT id FROM item WHERE id = item:3 AND n > 3;
