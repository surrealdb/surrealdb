/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Wire-fast regex match for `WHERE field = /pattern/`. The pre-compiled `regex::Regex` is pre-encoded as `LiteralWire::Regex`; eval peeks the field's wire, borrows the inner UTF-8 as `&str` via `from_utf8_unchecked` (sound by the rev-2 Strand pre-validation invariant), and runs `regex.is_match(s)`. No `Strand` decode in the hot loop."

[[test.results]]
value = "{ id: pfregex:1, name: 'Alice' }"

[[test.results]]
value = "{ id: pfregex:2, name: 'Adam' }"

[[test.results]]
value = "{ id: pfregex:3, name: 'Bob' }"

# WHERE name = /^A/ matches Alice and Adam
[[test.results]]
value = "[{ id: pfregex:1 }, { id: pfregex:2 }]"

# WHERE name != /^A/ matches Bob
[[test.results]]
value = "[{ id: pfregex:3 }]"

# WHERE name = /Alice/ matches just Alice
[[test.results]]
value = "[{ id: pfregex:1 }]"

# EXPLAIN: the regex-literal predicate uses the wire-fast path
[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: pfregex, direction: Forward, predicate: name = /^A/, pre_decode_filter: yes]
"'''

*/

CREATE ONLY pfregex:1 SET name = 'Alice';
CREATE ONLY pfregex:2 SET name = 'Adam';
CREATE ONLY pfregex:3 SET name = 'Bob';

SELECT id FROM pfregex WHERE name = /^A/;
SELECT id FROM pfregex WHERE name != /^A/;
SELECT id FROM pfregex WHERE name = /Alice/;

EXPLAIN SELECT id FROM pfregex WITH NOINDEX WHERE name = /^A/;
