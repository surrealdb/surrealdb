/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
Plan matrix for the containment and membership operators: which of them reach
an index, how, and which cannot. A prefix EXPLAIN is rejected by the per-row
engine, so the compute-only strategy is dropped; the two streaming strategies
plan identically by contract and both are pinned so the file fails if that
stops being true.

Each shape is asserted three ways -- the plan, the rows, and the same rows
under WITH NOINDEX, which forces a table scan and is therefore the oracle for
what the plan should have produced. Each distinct access shape over a fanning
column (a column carrying `*`, or a compound index with such a column) also
carries a count()/GROUP ALL pair, because count is served by a separate
index-only path that can disagree with both. Operand-swapped mirrors and
ORDER BY variants reach the same access shape as the case they mirror -- an
operand swap plans identically to it, an ORDER BY adds an ordering node above
the same access -- so they rely on that shape's pair rather than repeating it;
the comment on each says which case it mirrors.

A record reaches the statement above the scan exactly once, however many index
entries it owns, and the `Distinct` operator is what enforces that -- so its
presence and its absence are both load-bearing here. A scan whose key range
pins every fanning column of its index to one value, whether by an equality
prefix or by a single-value containment extending that prefix, reaches each
record through a single entry and carries no `Distinct` (B1, C1, C4, F4). A
scan that leaves a fanning column open reaches a record once per element of
that column and is wrapped (F1, F2, F5, F6): the `IndexScan` below the wrap
reports entries in its `{rows: N}` and the `Distinct` above it reports records,
which is how the two counts are told apart. A union needs no wrap of its own --
UnionIndexScan merges its branches on record id, so a record sitting in several
branches is emitted once (A4, B7, B10, F3).

Unordered results are compared as sets where the plans emit in different
orders: a UnionIndexScan emits branch-major, a table scan emits in record-id
order, and no ORDER BY was asked for. The ORDER BY id cases (B8, C3) pin the
ordered form, where the comparison against the oracle is literal.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: code] {rows: 2}
    Filter [ctx: Db] [predicate: code INSIDE ['a', 'c']] {rows: 2}
        UnionIndexScan [ctx: Db] [table: sc, branches: 2] {rows: 2}
            IndexScan [ctx: Db] [index: idx_sc_code, access: = 'a', direction: Forward] {rows: 1}
            IndexScan [ctx: Db] [index: idx_sc_code, access: = 'c', direction: Forward] {rows: 1}

Total rows: 2"'''

[[test.results]]
value = "['a', 'c']"

[[test.results]]
value = "['a', 'c']"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: code] {rows: 2}
    Filter [ctx: Db] [predicate: code INSIDE ['a', 'c']] {rows: 2}
        UnionIndexScan [ctx: Db] [table: sc, branches: 2] {rows: 2}
            IndexScan [ctx: Db] [index: idx_sc_code, access: = 'a', direction: Forward] {rows: 1}
            IndexScan [ctx: Db] [index: idx_sc_code, access: = 'c', direction: Forward] {rows: 1}

Total rows: 2"'''

[[test.results]]
value = "['a', 'c']"

[[test.results]]
value = "['a', 'c']"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: code] {rows: 1}
    IndexScan [ctx: Db] [index: idx_sc_code, access: = 'a', direction: Forward, predicate: code INSIDE ['a']] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "['a']"

[[test.results]]
value = "['a']"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: code] {rows: 2}
    Filter [ctx: Db] [predicate: code INSIDE ['a', 'a', 'b']] {rows: 2}
        UnionIndexScan [ctx: Db] [table: sc, branches: 3] {rows: 2}
            IndexScan [ctx: Db] [index: idx_sc_code, access: = 'a', direction: Forward] {rows: 1}
            IndexScan [ctx: Db] [index: idx_sc_code, access: = 'a', direction: Forward] {rows: 1}
            IndexScan [ctx: Db] [index: idx_sc_code, access: = 'b', direction: Forward] {rows: 1}

Total rows: 2"'''

[[test.results]]
value = "['a', 'b']"

[[test.results]]
value = "['a', 'b']"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: code]
    EmptyScan [ctx: Rt]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: code] {rows: 2}
    TableScan [ctx: Db] [table: sc, direction: Forward, predicate: code NOTINSIDE ['a', 'c'], pre_decode_filter: yes] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "['b', 'd']"

[[test.results]]
value = "['b', 'd']"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: code] {rows: 2}
    TableScan [ctx: Db] [table: sc, direction: Forward, predicate: code NOTINSIDE ['a', 'c'], pre_decode_filter: yes] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "['b', 'd']"

[[test.results]]
value = "['b', 'd']"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: code]
    TableScan [ctx: Db] [table: sc, direction: Forward, predicate: code NOTINSIDE [], pre_decode_filter: yes]
''''

[[test.results]]
value = "['a', 'b', 'c', 'd']"

[[test.results]]
value = "['a', 'b', 'c', 'd']"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: code] {rows: 1}
    TableScan [ctx: Db] [table: sc, direction: Forward, predicate: 'a' INSIDE code, pre_decode_filter: yes] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "['a']"

[[test.results]]
value = "['a']"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n] {rows: 3}
    Filter [ctx: Db] [predicate: n INSIDE [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]] {rows: 3}
        UnionIndexScan [ctx: Db] [table: sc, branches: 32] {rows: 3}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 1, direction: Forward] {rows: 1}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 2, direction: Forward] {rows: 1}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 3, direction: Forward] {rows: 1}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 4, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 5, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 6, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 7, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 8, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 9, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 10, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 11, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 12, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 13, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 14, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 15, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 16, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 17, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 18, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 19, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 20, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 21, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 22, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 23, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 24, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 25, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 26, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 27, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 28, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 29, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 30, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 31, direction: Forward] {rows: 0}
            IndexScan [ctx: Db] [index: idx_sc_n, access: = 32, direction: Forward] {rows: 0}

Total rows: 3''''

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n] {rows: 3}
    TableScan [ctx: Db] [table: sc, direction: Forward, predicate: n INSIDE [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33], pre_decode_filter: yes] {rows: 3}

Total rows: 3''''

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 3}
    IndexScan [ctx: Db] [index: idx_bag_tags, access: = 'red', direction: Forward, predicate: tags CONTAINS 'red'] {rows: 3}

Total rows: 3"'''

[[test.results]]
value = "[bag:a, bag:c, bag:e]"

[[test.results]]
value = "[bag:a, bag:c, bag:e]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 3}
    IndexScan [ctx: Db] [index: idx_bag_tags, access: = 'red', direction: Forward, predicate: 'red' INSIDE tags] {rows: 3}

Total rows: 3"'''

[[test.results]]
value = "[bag:a, bag:c, bag:e]"

[[test.results]]
value = "[bag:a, bag:c, bag:e]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 2}
    TableScan [ctx: Db] [table: bag, direction: Forward, predicate: tags CONTAINSNOT 'red', pre_decode_filter: yes] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[bag:b, bag:d]"

[[test.results]]
value = "[bag:b, bag:d]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 2}
    TableScan [ctx: Db] [table: bag, direction: Forward, predicate: 'red' NOTINSIDE tags, pre_decode_filter: yes] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[bag:b, bag:d]"

[[test.results]]
value = "[bag:b, bag:d]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 1}
    Filter [ctx: Db] [predicate: tags CONTAINSALL ['red', 'green']] {rows: 1}
        UnionIndexScan [ctx: Db] [table: bag, branches: 2] {rows: 4}
            IndexScan [ctx: Db] [index: idx_bag_tags, access: = 'red', direction: Forward] {rows: 3}
            IndexScan [ctx: Db] [index: idx_bag_tags, access: = 'green', direction: Forward] {rows: 2}

Total rows: 1"'''

[[test.results]]
value = "[bag:a]"

[[test.results]]
value = "[bag:a]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 1}
    Filter [ctx: Db] [predicate: ['red', 'green'] ALLINSIDE tags] {rows: 1}
        UnionIndexScan [ctx: Db] [table: bag, branches: 2] {rows: 4}
            IndexScan [ctx: Db] [index: idx_bag_tags, access: = 'red', direction: Forward] {rows: 3}
            IndexScan [ctx: Db] [index: idx_bag_tags, access: = 'green', direction: Forward] {rows: 2}

Total rows: 1"'''

[[test.results]]
value = "[bag:a]"

[[test.results]]
value = "[bag:a]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 4}
    UnionIndexScan [ctx: Db] [table: bag, branches: 2] {rows: 4}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 'red', direction: Forward] {rows: 3}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 'green', direction: Forward] {rows: 2}

Total rows: 4"'''

[[test.results]]
value = "[bag:a, bag:c, bag:e, bag:b]"

[[test.results]]
value = "[bag:a, bag:b, bag:c, bag:e]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 4}
    UnionIndexScan [ctx: Db] [table: bag, branches: 2, merge_by_id: Asc] {rows: 4}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 'red', direction: Forward] {rows: 3}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 'green', direction: Forward] {rows: 2}

Total rows: 4"'''

[[test.results]]
value = "[bag:a, bag:b, bag:c, bag:e]"

[[test.results]]
value = "[bag:a, bag:b, bag:c, bag:e]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 4}
    UnionIndexScan [ctx: Db] [table: bag, branches: 2] {rows: 4}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 'red', direction: Forward] {rows: 3}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 'green', direction: Forward] {rows: 2}

Total rows: 4"'''

[[test.results]]
value = "[bag:a, bag:c, bag:e, bag:b]"

[[test.results]]
value = "[bag:a, bag:b, bag:c, bag:e]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 4}
    UnionIndexScan [ctx: Db] [table: bag, branches: 3] {rows: 4}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 'red', direction: Forward] {rows: 3}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 'green', direction: Forward] {rows: 2}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 'blue', direction: Forward] {rows: 1}

Total rows: 4"'''

[[test.results]]
value = "[bag:a, bag:c, bag:e, bag:b]"

[[test.results]]
value = "[bag:a, bag:b, bag:c, bag:e]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 1}
    TableScan [ctx: Db] [table: bag, direction: Forward, predicate: tags CONTAINSNONE ['red', 'green'], pre_decode_filter: yes] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[bag:d]"

[[test.results]]
value = "[bag:d]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 1}
    TableScan [ctx: Db] [table: bag, direction: Forward, predicate: ['red', 'green'] NONEINSIDE tags, pre_decode_filter: yes] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[bag:d]"

[[test.results]]
value = "[bag:d]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: bag, direction: Forward, predicate: tags CONTAINSANY [], pre_decode_filter: yes]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: bag, direction: Forward, predicate: tags CONTAINSALL [], pre_decode_filter: yes]
''''

[[test.results]]
value = "[bag:a, bag:b, bag:c, bag:d, bag:e]"

[[test.results]]
value = "[bag:a, bag:b, bag:c, bag:d, bag:e]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 3}
    TableScan [ctx: Db] [table: bag, direction: Forward, predicate: tags ?= 'red', pre_decode_filter: no (unsupported predicate)] {rows: 3}

Total rows: 3"'''

[[test.results]]
value = "[bag:a, bag:c, bag:e]"

[[test.results]]
value = "[bag:a, bag:c, bag:e]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 2}
    TableScan [ctx: Db] [table: bag, direction: Forward, predicate: tags *= 'red', pre_decode_filter: no (unsupported predicate)] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[bag:d, bag:e]"

[[test.results]]
value = "[bag:d, bag:e]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 3}
    UnionIndexScan [ctx: Db] [table: bag, branches: 32] {rows: 3}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 'red', direction: Forward] {rows: 3}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't02', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't03', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't04', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't05', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't06', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't07', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't08', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't09', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't10', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't11', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't12', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't13', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't14', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't15', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't16', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't17', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't18', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't19', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't20', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't21', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't22', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't23', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't24', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't25', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't26', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't27', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't28', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't29', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't30', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't31', direction: Forward] {rows: 0}
        IndexScan [ctx: Db] [index: idx_bag_tags, access: = 't32', direction: Forward] {rows: 0}

Total rows: 3"'''

[[test.results]]
value = "[bag:a, bag:c, bag:e]"

[[test.results]]
value = "[bag:a, bag:c, bag:e]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 3}
    TableScan [ctx: Db] [table: bag, direction: Forward, predicate: tags CONTAINSANY ['red', 't02', 't03', 't04', 't05', 't06', 't07', 't08', 't09', 't10', 't11', 't12', 't13', 't14', 't15', 't16', 't17', 't18', 't19', 't20', 't21', 't22', 't23', 't24', 't25', 't26', 't27', 't28', 't29', 't30', 't31', 't32', 't33'], pre_decode_filter: yes] {rows: 3}

Total rows: 3"'''

[[test.results]]
value = "[bag:a, bag:c, bag:e]"

[[test.results]]
value = "[bag:a, bag:c, bag:e]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 2}
    IndexScan [ctx: Db] [index: idx_box, access: ['red'], direction: Forward, predicate: tags CONTAINS 'red'] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[box:a, box:c]"

[[test.results]]
value = "[box:a, box:c]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 3}
    UnionIndexScan [ctx: Db] [table: box, branches: 2] {rows: 3}
        IndexScan [ctx: Db] [index: idx_box, access: ['red'], direction: Forward] {rows: 2}
        IndexScan [ctx: Db] [index: idx_box, access: ['green'], direction: Forward] {rows: 2}

Total rows: 3"'''

[[test.results]]
value = "[box:a, box:c, box:b]"

[[test.results]]
value = "[box:a, box:b, box:c]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 3}
    SortByKey [ctx: Db] [sort_keys: id ASC] {rows: 3}
        UnionIndexScan [ctx: Db] [table: box, branches: 2] {rows: 3}
            IndexScan [ctx: Db] [index: idx_box, access: ['red'], direction: Forward] {rows: 2}
            IndexScan [ctx: Db] [index: idx_box, access: ['green'], direction: Forward] {rows: 2}

Total rows: 3"'''

[[test.results]]
value = "[box:a, box:b, box:c]"

[[test.results]]
value = "[box:a, box:b, box:c]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 2}
    IndexScan [ctx: Db] [index: idx_box, access: ['red', 'x'], direction: Forward, predicate: tags CONTAINS 'red' AND code = 'x'] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[box:a, box:c]"

[[test.results]]
value = "[box:a, box:c]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 2}
    Filter [ctx: Db] [predicate: code INSIDE ['x', 'y']] {rows: 2}
        IndexScan [ctx: Db] [index: idx_box, access: ['red'], direction: Forward] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[box:a, box:c]"

[[test.results]]
value = "[box:a, box:c]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id] {rows: 3}
    TableScan [ctx: Db] [table: kv, direction: Forward, predicate: v INSIDE [1, 2], pre_decode_filter: yes] {rows: 3}

Total rows: 3''''

[[test.results]]
value = "[kv:1, kv:2, kv:3]"

[[test.results]]
value = "[kv:1, kv:2, kv:3]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 3}
    Filter [ctx: Db] [predicate: k INSIDE ['a', 'b']] {rows: 3}
        UnionIndexScan [ctx: Db] [table: kv, branches: 2] {rows: 3}
            IndexScan [ctx: Db] [index: idx_kv, access: ['a'], direction: Forward] {rows: 2}
            IndexScan [ctx: Db] [index: idx_kv, access: ['b'], direction: Forward] {rows: 1}

Total rows: 3"'''

[[test.results]]
value = "[kv:1, kv:2, kv:3]"

[[test.results]]
value = "[kv:1, kv:2, kv:3]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 2}
    Filter [ctx: Db] [predicate: meta.kind INSIDE ['p', 'z']] {rows: 2}
        UnionIndexScan [ctx: Db] [table: kv, branches: 2] {rows: 2}
            IndexScan [ctx: Db] [index: idx_kv_meta, access: = 'p', direction: Forward] {rows: 2}
            IndexScan [ctx: Db] [index: idx_kv_meta, access: = 'z', direction: Forward] {rows: 0}

Total rows: 2"'''

[[test.results]]
value = "[kv:1, kv:3]"

[[test.results]]
value = "[kv:1, kv:3]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 2}
    UnionIndexScan [ctx: Db] [table: ord, branches: 2] {rows: 2}
        IndexScan [ctx: Db] [index: idx_ord_sku, access: = 's1', direction: Forward] {rows: 1}
        IndexScan [ctx: Db] [index: idx_ord_sku, access: = 's2', direction: Forward] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[ord:1, ord:2]"

[[test.results]]
value = "[ord:1, ord:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 2}
    UnionIndexScan [ctx: Db] [table: ord, branches: 2] {rows: 2}
        IndexScan [ctx: Db] [index: idx_ord_sku, access: = 's1', direction: Forward] {rows: 1}
        IndexScan [ctx: Db] [index: idx_ord_sku, access: = 's2', direction: Forward] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[ord:1, ord:2]"

[[test.results]]
value = "[ord:1, ord:2]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 0}
    TableScan [ctx: Db] [table: ord, direction: Forward, predicate: lines.*.sku INSIDE ['s1', 's2'], pre_decode_filter: no (unsupported predicate)] {rows: 0}

Total rows: 0"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 1}
    IndexScan [ctx: Db] [index: idx_nest, access: = ['red', 'green'], direction: Forward, predicate: tags CONTAINS ['red', 'green']] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[nest:f]"

[[test.results]]
value = "[nest:f]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 2}
    DistinctRecords [ctx: Db] {rows: 2}
        IndexScan [ctx: Db] [index: idx_tl, access: ['x'], direction: Forward, predicate: code INSIDE ['x']] {rows: 4}

Total rows: 2"'''

[[test.results]]
value = "[tl:1, tl:3]"

[[test.results]]
value = "[tl:1, tl:3]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 2}
    DistinctRecords [ctx: Db] {rows: 2}
        IndexScan [ctx: Db] [index: idx_tl, access: ['x'], direction: Forward, predicate: code = 'x'] {rows: 4}

Total rows: 2"'''

[[test.results]]
value = "[tl:1, tl:3]"

[[test.results]]
value = "[tl:1, tl:3]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 3}
    Filter [ctx: Db] [predicate: code INSIDE ['x', 'y']] {rows: 3}
        UnionIndexScan [ctx: Db] [table: tl, branches: 2] {rows: 3}
            DistinctRecords [ctx: Db] {rows: 2}
                IndexScan [ctx: Db] [index: idx_tl, access: ['x'], direction: Forward] {rows: 4}
            DistinctRecords [ctx: Db] {rows: 1}
                IndexScan [ctx: Db] [index: idx_tl, access: ['y'], direction: Forward] {rows: 1}

Total rows: 3"'''

[[test.results]]
value = "[tl:1, tl:3, tl:2]"

[[test.results]]
value = "[tl:1, tl:2, tl:3]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 2}
    IndexScan [ctx: Db] [index: idx_tl, access: ['x', 'red'], direction: Forward, predicate: code = 'x' AND tags CONTAINS 'red'] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[tl:1, tl:3]"

[[test.results]]
value = "[tl:1, tl:3]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 1}
    Filter [ctx: Db] [predicate: tags CONTAINSNOT 'yellow'] {rows: 1}
        DistinctRecords [ctx: Db] {rows: 2}
            IndexScan [ctx: Db] [index: idx_tl, access: ['x'], direction: Forward] {rows: 4}

Total rows: 1"'''

[[test.results]]
value = "[tl:1]"

[[test.results]]
value = "[tl:1]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id] {rows: 2}
    Filter [ctx: Db] [predicate: tags CONTAINSANY ['red', 'yellow']] {rows: 2}
        DistinctRecords [ctx: Db] {rows: 2}
            IndexScan [ctx: Db] [index: idx_tl, access: ['x'], direction: Forward] {rows: 4}

Total rows: 2"'''

[[test.results]]
value = "[tl:1, tl:3]"

[[test.results]]
value = "[tl:1, tl:3]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

*/

-- ===========================================================================
-- Section A -- scalar membership: IN / INSIDE / NOT IN / NOTINSIDE
-- ===========================================================================
-- A scalar column holds one value per record, so a record matches at most one
-- DISTINCT literal of an IN list, and branches for distinct literals are
-- record-disjoint -- the opposite of the array case in section B, where one
-- record can hold several of the literals at once. The list is expanded
-- without de-duplication, so a repeated literal still puts one record in two
-- branches (A4) and the union's merge on record id remains load-bearing.

{
	DEFINE TABLE sc;
	DEFINE INDEX idx_sc_code ON sc COLUMNS code;
	DEFINE INDEX idx_sc_n ON sc COLUMNS n;
	CREATE sc:a SET code = 'a', n = 1;
	CREATE sc:b SET code = 'b', n = 2;
	CREATE sc:c SET code = 'c', n = 3;
	CREATE sc:d SET code = 'd', n = 40;
	RETURN 'OK';
};

-- A1: a multi-element IN expands to one equality branch per literal, with the
-- membership test retained above the union as a residual Filter.
EXPLAIN ANALYZE SELECT VALUE code FROM sc WHERE code IN ['a', 'c'];
SELECT VALUE code FROM sc WHERE code IN ['a', 'c'];
SELECT VALUE code FROM sc WITH NOINDEX WHERE code IN ['a', 'c'];

-- A2: IN and INSIDE are one operator. The plan must match A1 exactly, down to
-- the name the predicate renders with.
EXPLAIN ANALYZE SELECT VALUE code FROM sc WHERE code INSIDE ['a', 'c'];
SELECT VALUE code FROM sc WHERE code INSIDE ['a', 'c'];
SELECT VALUE code FROM sc WITH NOINDEX WHERE code INSIDE ['a', 'c'];

-- A3: a single-element IN normalises to an equality seek -- one IndexScan, no
-- union. The `predicate:` attribute records that the scan still re-checks the
-- membership test per fetched row rather than treating the seek as proof.
EXPLAIN ANALYZE SELECT VALUE code FROM sc WHERE code IN ['a'];
SELECT VALUE code FROM sc WHERE code IN ['a'];
SELECT VALUE code FROM sc WITH NOINDEX WHERE code IN ['a'];

-- A4: repeated literals produce one branch each -- the list is not
-- de-duplicated before expansion. The matching record therefore sits in two
-- branches at once and must still be emitted once.
EXPLAIN ANALYZE SELECT VALUE code FROM sc WHERE code IN ['a', 'a', 'b'];
SELECT VALUE code FROM sc WHERE code IN ['a', 'a', 'b'];
SELECT VALUE code FROM sc WITH NOINDEX WHERE code IN ['a', 'a', 'b'];
SELECT count() FROM sc WHERE code IN ['a', 'a', 'b'] GROUP ALL;
SELECT count() FROM sc WITH NOINDEX WHERE code IN ['a', 'a', 'b'] GROUP ALL;

-- A5: `IN []` is always false. It must be answered without touching the index
-- and, above all, not dropped from the WHERE clause -- dropping it would
-- return the whole table.
EXPLAIN SELECT VALUE code FROM sc WHERE code IN [];
SELECT VALUE code FROM sc WHERE code IN [];
SELECT VALUE code FROM sc WITH NOINDEX WHERE code IN [];

-- A6: negated membership inverts the result set, so no index candidate may be
-- taken from it -- the index can enumerate the records that match a literal,
-- never the records that match none. Expect a table scan.
EXPLAIN ANALYZE SELECT VALUE code FROM sc WHERE code NOT IN ['a', 'c'];
SELECT VALUE code FROM sc WHERE code NOT IN ['a', 'c'];
SELECT VALUE code FROM sc WITH NOINDEX WHERE code NOT IN ['a', 'c'];

-- A7: NOTINSIDE is the word form of NOT IN and must plan identically to A6.
EXPLAIN ANALYZE SELECT VALUE code FROM sc WHERE code NOTINSIDE ['a', 'c'];
SELECT VALUE code FROM sc WHERE code NOTINSIDE ['a', 'c'];
SELECT VALUE code FROM sc WITH NOINDEX WHERE code NOTINSIDE ['a', 'c'];

-- A8: `NOT IN []` is always true -- the mirror of A5. Pinned so that a
-- short-circuit added for one cannot collapse the other the wrong way.
EXPLAIN SELECT VALUE code FROM sc WHERE code NOT IN [];
SELECT VALUE code FROM sc WHERE code NOT IN [];
SELECT VALUE code FROM sc WITH NOINDEX WHERE code NOT IN [];

-- A9: the reversed operand order against a column that does not fan out.
-- `v INSIDE field` asks whether `field` contains `v` -- element membership for
-- an array, substring containment for the string this column holds. Either
-- reading needs the column's contents, not its whole values, so the scalar
-- index over `code` cannot answer it and must be declined.
EXPLAIN ANALYZE SELECT VALUE code FROM sc WHERE 'a' INSIDE code;
SELECT VALUE code FROM sc WHERE 'a' INSIDE code;
SELECT VALUE code FROM sc WITH NOINDEX WHERE 'a' INSIDE code;

-- A10: the low side of the IN expansion threshold. 32 literals is the largest
-- list still expanded (MAX_IN_EXPANSION_SIZE), so this must show 32 branches.
EXPLAIN ANALYZE SELECT VALUE n FROM sc WHERE n IN [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32];
SELECT VALUE n FROM sc WHERE n IN [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32];
SELECT VALUE n FROM sc WITH NOINDEX WHERE n IN [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32];

-- A11: the high side. One literal more and the per-branch overhead is judged
-- to outweigh the seeks, so the plan falls back to one sequential pass. The
-- rows must not change across the threshold.
EXPLAIN ANALYZE SELECT VALUE n FROM sc WHERE n IN [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33];
SELECT VALUE n FROM sc WHERE n IN [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33];
SELECT VALUE n FROM sc WITH NOINDEX WHERE n IN [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33];

-- ===========================================================================
-- Section B -- array-element index: CONTAINS family and INSIDE family
-- ===========================================================================
-- `bag` is the dedupe fixture. bag:a and bag:c each hold two DISTINCT tags, so
-- a union over those tags places each of them in two branches at once; a union
-- that fails to dedupe emits them twice. bag:e holds the same tag twice inside
-- one array, which probes the other duplicate source: two index entries for
-- one record under a single branch. bag:d holds the empty array, which is what
-- makes the vacuous-truth cases (B14, B16) observable at all.

{
	DEFINE TABLE bag;
	DEFINE INDEX idx_bag_tags ON bag COLUMNS tags.*;
	CREATE bag:a SET tags = ['red', 'green'];
	CREATE bag:b SET tags = ['green'];
	CREATE bag:c SET tags = ['blue', 'red'];
	CREATE bag:d SET tags = [];
	CREATE bag:e SET tags = ['red', 'red'];
	RETURN 'OK';
};

-- B1: single-value containment against a fanning column is an equality seek on
-- the element index, which pins the only fanning column to one value -- so the
-- seek reaches each record through one entry and the plan carries no Distinct.
-- bag:e holds the same tag twice, so the seek's own {rows: N} is what proves
-- the entries collapse in the index rather than above it, and count() agrees.
EXPLAIN ANALYZE SELECT VALUE id FROM bag WHERE tags CONTAINS 'red';
SELECT VALUE id FROM bag WHERE tags CONTAINS 'red';
SELECT VALUE id FROM bag WITH NOINDEX WHERE tags CONTAINS 'red';
SELECT count() FROM bag WHERE tags CONTAINS 'red' GROUP ALL;
SELECT count() FROM bag WITH NOINDEX WHERE tags CONTAINS 'red' GROUP ALL;

-- B2: B1's question with the operands swapped. `v INSIDE field` must reach
-- the same access as `field CONTAINS v`; only the rendered predicate differs.
EXPLAIN ANALYZE SELECT VALUE id FROM bag WHERE 'red' INSIDE tags;
SELECT VALUE id FROM bag WHERE 'red' INSIDE tags;
SELECT VALUE id FROM bag WITH NOINDEX WHERE 'red' INSIDE tags;

-- B3: CONTAINSNOT is a negation and may not be served from the element index.
EXPLAIN ANALYZE SELECT VALUE id FROM bag WHERE tags CONTAINSNOT 'red';
SELECT VALUE id FROM bag WHERE tags CONTAINSNOT 'red';
SELECT VALUE id FROM bag WITH NOINDEX WHERE tags CONTAINSNOT 'red';
SELECT count() FROM bag WHERE tags CONTAINSNOT 'red' GROUP ALL;
SELECT count() FROM bag WITH NOINDEX WHERE tags CONTAINSNOT 'red' GROUP ALL;

-- B4: the swapped form of B3.
EXPLAIN ANALYZE SELECT VALUE id FROM bag WHERE 'red' NOTINSIDE tags;
SELECT VALUE id FROM bag WHERE 'red' NOTINSIDE tags;
SELECT VALUE id FROM bag WITH NOINDEX WHERE 'red' NOTINSIDE tags;

-- B5: CONTAINSALL expands to a union of per-element seeks, which over-fetches
-- -- the union yields records holding EITHER tag -- so the predicate must
-- survive as a residual Filter above it. A union with no Filter here would
-- return the wrong rows.
EXPLAIN ANALYZE SELECT VALUE id FROM bag WHERE tags CONTAINSALL ['red', 'green'];
SELECT VALUE id FROM bag WHERE tags CONTAINSALL ['red', 'green'];
SELECT VALUE id FROM bag WITH NOINDEX WHERE tags CONTAINSALL ['red', 'green'];
SELECT count() FROM bag WHERE tags CONTAINSALL ['red', 'green'] GROUP ALL;
SELECT count() FROM bag WITH NOINDEX WHERE tags CONTAINSALL ['red', 'green'] GROUP ALL;

-- B6: ALLINSIDE is B5's CONTAINSALL with the operands swapped.
EXPLAIN ANALYZE SELECT VALUE id FROM bag WHERE ['red', 'green'] ALLINSIDE tags;
SELECT VALUE id FROM bag WHERE ['red', 'green'] ALLINSIDE tags;
SELECT VALUE id FROM bag WITH NOINDEX WHERE ['red', 'green'] ALLINSIDE tags;

-- B7: CONTAINSANY is exactly the union, so the branches ARE the answer and no
-- residual Filter is needed -- which is what makes the union's own dedupe
-- load-bearing here rather than incidental. bag:a holds both tags and is
-- emitted once: UnionIndexScan merges its branches on record id, so no
-- Distinct is wrapped around it, and the branch row counts summing to more
-- than the union's output is that merge visible in the plan.
EXPLAIN ANALYZE SELECT VALUE id FROM bag WHERE tags CONTAINSANY ['red', 'green'];
SELECT VALUE id FROM bag WHERE tags CONTAINSANY ['red', 'green'];
SELECT VALUE id FROM bag WITH NOINDEX WHERE tags CONTAINSANY ['red', 'green'];
SELECT count() FROM bag WHERE tags CONTAINSANY ['red', 'green'] GROUP ALL;
SELECT count() FROM bag WITH NOINDEX WHERE tags CONTAINSANY ['red', 'green'] GROUP ALL;

-- B8: the same union under ORDER BY id. Equality sub-scans over a
-- single-column index arrive in record-id order, so the union switches to an
-- id merge and the sort is eliminated -- a different dedupe path from B7, and
-- the one shape where the row comparison against the oracle is literal rather
-- than set-wise.
EXPLAIN ANALYZE SELECT VALUE id FROM bag WHERE tags CONTAINSANY ['red', 'green'] ORDER BY id;
SELECT VALUE id FROM bag WHERE tags CONTAINSANY ['red', 'green'] ORDER BY id;
SELECT VALUE id FROM bag WITH NOINDEX WHERE tags CONTAINSANY ['red', 'green'] ORDER BY id;

-- B9: ANYINSIDE is B7's CONTAINSANY with the operands swapped.
EXPLAIN ANALYZE SELECT VALUE id FROM bag WHERE ['red', 'green'] ANYINSIDE tags;
SELECT VALUE id FROM bag WHERE ['red', 'green'] ANYINSIDE tags;
SELECT VALUE id FROM bag WITH NOINDEX WHERE ['red', 'green'] ANYINSIDE tags;

-- B10: three branches with two records overlapping two branches each (bag:a in
-- red+green, bag:c in red+blue). Two simultaneous overlaps is what separates a
-- real set union from a merge that only collapses adjacent duplicates.
EXPLAIN ANALYZE SELECT VALUE id FROM bag WHERE tags CONTAINSANY ['red', 'green', 'blue'];
SELECT VALUE id FROM bag WHERE tags CONTAINSANY ['red', 'green', 'blue'];
SELECT VALUE id FROM bag WITH NOINDEX WHERE tags CONTAINSANY ['red', 'green', 'blue'];
SELECT count() FROM bag WHERE tags CONTAINSANY ['red', 'green', 'blue'] GROUP ALL;
SELECT count() FROM bag WITH NOINDEX WHERE tags CONTAINSANY ['red', 'green', 'blue'] GROUP ALL;

-- B11: CONTAINSNONE negates the union and cannot be served by it.
EXPLAIN ANALYZE SELECT VALUE id FROM bag WHERE tags CONTAINSNONE ['red', 'green'];
SELECT VALUE id FROM bag WHERE tags CONTAINSNONE ['red', 'green'];
SELECT VALUE id FROM bag WITH NOINDEX WHERE tags CONTAINSNONE ['red', 'green'];
SELECT count() FROM bag WHERE tags CONTAINSNONE ['red', 'green'] GROUP ALL;
SELECT count() FROM bag WITH NOINDEX WHERE tags CONTAINSNONE ['red', 'green'] GROUP ALL;

-- B12: NONEINSIDE is B11's CONTAINSNONE with the operands swapped.
EXPLAIN ANALYZE SELECT VALUE id FROM bag WHERE ['red', 'green'] NONEINSIDE tags;
SELECT VALUE id FROM bag WHERE ['red', 'green'] NONEINSIDE tags;
SELECT VALUE id FROM bag WITH NOINDEX WHERE ['red', 'green'] NONEINSIDE tags;

-- B13: `CONTAINSANY []` is always false. The expansion guard skips empty
-- lists, so this must still be answered correctly rather than expanded into a
-- zero-branch union or dropped from the WHERE clause. Unlike `IN []` (A5) it
-- is not recognised as a constant, so it costs a full pass -- a missed
-- optimisation, not a wrong answer. The count pair pins `count: 0` rather than
-- an absent row: GROUP ALL over no matches still yields one grouped row.
EXPLAIN SELECT VALUE id FROM bag WHERE tags CONTAINSANY [];
SELECT VALUE id FROM bag WHERE tags CONTAINSANY [];
SELECT VALUE id FROM bag WITH NOINDEX WHERE tags CONTAINSANY [];
SELECT count() FROM bag WHERE tags CONTAINSANY [] GROUP ALL;
SELECT count() FROM bag WITH NOINDEX WHERE tags CONTAINSANY [] GROUP ALL;

-- B14: `CONTAINSALL []` is vacuously true -- every record, bag:d's empty array
-- included. The mirror of B13, and the count pair pins the whole table.
EXPLAIN SELECT VALUE id FROM bag WHERE tags CONTAINSALL [];
SELECT VALUE id FROM bag WHERE tags CONTAINSALL [];
SELECT VALUE id FROM bag WITH NOINDEX WHERE tags CONTAINSALL [];
SELECT count() FROM bag WHERE tags CONTAINSALL [] GROUP ALL;
SELECT count() FROM bag WITH NOINDEX WHERE tags CONTAINSALL [] GROUP ALL;

-- B15: `?=` (AnyEqual) asks whether any element equals the value, which is the
-- same question as B1's CONTAINS. The analyzer does not recognise it, so it
-- does not reach the element index; the rows must nonetheless match B1.
EXPLAIN ANALYZE SELECT VALUE id FROM bag WHERE tags ?= 'red';
SELECT VALUE id FROM bag WHERE tags ?= 'red';
SELECT VALUE id FROM bag WITH NOINDEX WHERE tags ?= 'red';
SELECT count() FROM bag WHERE tags ?= 'red' GROUP ALL;
SELECT count() FROM bag WITH NOINDEX WHERE tags ?= 'red' GROUP ALL;

-- B16: `*=` (AllEqual) asks whether EVERY element equals the value, which is
-- not a union of seeks under any reading, so a table scan is the right plan.
-- bag:d matches vacuously, which is the case an index-based rewrite would
-- most easily lose.
EXPLAIN ANALYZE SELECT VALUE id FROM bag WHERE tags *= 'red';
SELECT VALUE id FROM bag WHERE tags *= 'red';
SELECT VALUE id FROM bag WITH NOINDEX WHERE tags *= 'red';
SELECT count() FROM bag WHERE tags *= 'red' GROUP ALL;
SELECT count() FROM bag WITH NOINDEX WHERE tags *= 'red' GROUP ALL;

-- B17: the containment expansion shares MAX_IN_EXPANSION_SIZE with the scalar
-- expansion of A10/A11 but reaches it through a separate guard, so both sides
-- of the threshold are pinned here too. 32 elements: still a union, and the
-- count pair pins that a 32-branch union dedupes as the 2- and 3-branch ones
-- of B7 and B10 do.
EXPLAIN ANALYZE SELECT VALUE id FROM bag WHERE tags CONTAINSANY ['red','t02','t03','t04','t05','t06','t07','t08','t09','t10','t11','t12','t13','t14','t15','t16','t17','t18','t19','t20','t21','t22','t23','t24','t25','t26','t27','t28','t29','t30','t31','t32'];
SELECT VALUE id FROM bag WHERE tags CONTAINSANY ['red','t02','t03','t04','t05','t06','t07','t08','t09','t10','t11','t12','t13','t14','t15','t16','t17','t18','t19','t20','t21','t22','t23','t24','t25','t26','t27','t28','t29','t30','t31','t32'];
SELECT VALUE id FROM bag WITH NOINDEX WHERE tags CONTAINSANY ['red','t02','t03','t04','t05','t06','t07','t08','t09','t10','t11','t12','t13','t14','t15','t16','t17','t18','t19','t20','t21','t22','t23','t24','t25','t26','t27','t28','t29','t30','t31','t32'];
SELECT count() FROM bag WHERE tags CONTAINSANY ['red','t02','t03','t04','t05','t06','t07','t08','t09','t10','t11','t12','t13','t14','t15','t16','t17','t18','t19','t20','t21','t22','t23','t24','t25','t26','t27','t28','t29','t30','t31','t32'] GROUP ALL;
SELECT count() FROM bag WITH NOINDEX WHERE tags CONTAINSANY ['red','t02','t03','t04','t05','t06','t07','t08','t09','t10','t11','t12','t13','t14','t15','t16','t17','t18','t19','t20','t21','t22','t23','t24','t25','t26','t27','t28','t29','t30','t31','t32'] GROUP ALL;

-- B18: 33 elements -- the expansion is skipped and the whole predicate is
-- carried by one sequential pass. Rows and count are both unchanged across the
-- threshold, so neither answer may depend on which side of it a list falls.
EXPLAIN ANALYZE SELECT VALUE id FROM bag WHERE tags CONTAINSANY ['red','t02','t03','t04','t05','t06','t07','t08','t09','t10','t11','t12','t13','t14','t15','t16','t17','t18','t19','t20','t21','t22','t23','t24','t25','t26','t27','t28','t29','t30','t31','t32','t33'];
SELECT VALUE id FROM bag WHERE tags CONTAINSANY ['red','t02','t03','t04','t05','t06','t07','t08','t09','t10','t11','t12','t13','t14','t15','t16','t17','t18','t19','t20','t21','t22','t23','t24','t25','t26','t27','t28','t29','t30','t31','t32','t33'];
SELECT VALUE id FROM bag WITH NOINDEX WHERE tags CONTAINSANY ['red','t02','t03','t04','t05','t06','t07','t08','t09','t10','t11','t12','t13','t14','t15','t16','t17','t18','t19','t20','t21','t22','t23','t24','t25','t26','t27','t28','t29','t30','t31','t32','t33'];
SELECT count() FROM bag WHERE tags CONTAINSANY ['red','t02','t03','t04','t05','t06','t07','t08','t09','t10','t11','t12','t13','t14','t15','t16','t17','t18','t19','t20','t21','t22','t23','t24','t25','t26','t27','t28','t29','t30','t31','t32','t33'] GROUP ALL;
SELECT count() FROM bag WITH NOINDEX WHERE tags CONTAINSANY ['red','t02','t03','t04','t05','t06','t07','t08','t09','t10','t11','t12','t13','t14','t15','t16','t17','t18','t19','t20','t21','t22','t23','t24','t25','t26','t27','t28','t29','t30','t31','t32','t33'] GROUP ALL;

-- ===========================================================================
-- Section C -- containment against a compound index whose LEADING column fans
-- ===========================================================================
-- A compound key cannot be point-seeked on its leading column alone, so a
-- containment match becomes a prefix scan rather than an equality scan. The
-- leading column is the only fanning one here, which is what lets a second
-- equality conjunct extend the prefix (C4).

{
	DEFINE TABLE box;
	DEFINE INDEX idx_box ON box COLUMNS tags.*, code;
	CREATE box:a SET tags = ['red', 'green'], code = 'x';
	CREATE box:b SET tags = ['green'], code = 'y';
	CREATE box:c SET tags = ['blue', 'red'], code = 'x';
	RETURN 'OK';
};

-- C1: a prefix scan on the leading fanning column, not an equality seek. The
-- containment pins that column to one element and `code` does not fan, so the
-- prefix covers one key per record and the plan carries no Distinct.
EXPLAIN ANALYZE SELECT VALUE id FROM box WHERE tags CONTAINS 'red';
SELECT VALUE id FROM box WHERE tags CONTAINS 'red';
SELECT VALUE id FROM box WITH NOINDEX WHERE tags CONTAINS 'red';
SELECT count() FROM box WHERE tags CONTAINS 'red' GROUP ALL;
SELECT count() FROM box WITH NOINDEX WHERE tags CONTAINS 'red' GROUP ALL;

-- C2: the union branches are compound prefix scans rather than equality scans.
-- box:a sits in two branches and is emitted once, so the union's merge on
-- record id holds over prefix scans as it does over equality scans, and no
-- Distinct is wrapped around either.
-- The recorded rows are the oracle's set in a different order, not a different
-- answer: the union emits branch-major and the oracle's scan emits by record
-- id, and no ORDER BY was asked for. C3 pins the ordered form.
EXPLAIN ANALYZE SELECT VALUE id FROM box WHERE tags CONTAINSANY ['red', 'green'];
SELECT VALUE id FROM box WHERE tags CONTAINSANY ['red', 'green'];
SELECT VALUE id FROM box WITH NOINDEX WHERE tags CONTAINSANY ['red', 'green'];
SELECT count() FROM box WHERE tags CONTAINSANY ['red', 'green'] GROUP ALL;
SELECT count() FROM box WITH NOINDEX WHERE tags CONTAINSANY ['red', 'green'] GROUP ALL;

-- C3: the counterpart of B8 on a compound index. A compound prefix scan is
-- ordered by the trailing columns, not by record id, so the union cannot merge
-- by id and an explicit sort stays in the plan. Pinned because the wrong
-- choice here -- claiming an id merge over prefix scans -- would emit rows out
-- of order while still looking like a valid plan.
EXPLAIN ANALYZE SELECT VALUE id FROM box WHERE tags CONTAINSANY ['red', 'green'] ORDER BY id;
SELECT VALUE id FROM box WHERE tags CONTAINSANY ['red', 'green'] ORDER BY id;
SELECT VALUE id FROM box WITH NOINDEX WHERE tags CONTAINSANY ['red', 'green'] ORDER BY id;

-- C4: a containment match on the fanning leading column pins it to a single
-- element, which leaves one key per record, so the trailing equality can
-- extend the prefix to both columns and no residual Filter is needed.
EXPLAIN ANALYZE SELECT VALUE id FROM box WHERE tags CONTAINS 'red' AND code = 'x';
SELECT VALUE id FROM box WHERE tags CONTAINS 'red' AND code = 'x';
SELECT VALUE id FROM box WITH NOINDEX WHERE tags CONTAINS 'red' AND code = 'x';
SELECT count() FROM box WHERE tags CONTAINS 'red' AND code = 'x' GROUP ALL;
SELECT count() FROM box WITH NOINDEX WHERE tags CONTAINS 'red' AND code = 'x' GROUP ALL;

-- C5: membership on the TRAILING column alongside containment on the leading
-- one. The containment pins the prefix and the IN list is left as a residual
-- rather than being expanded into one prefix per literal -- a narrower scan is
-- available in principle, so this records the plan actually chosen.
EXPLAIN ANALYZE SELECT VALUE id FROM box WHERE code IN ['x', 'y'] AND tags CONTAINS 'red';
SELECT VALUE id FROM box WHERE code IN ['x', 'y'] AND tags CONTAINS 'red';
SELECT VALUE id FROM box WITH NOINDEX WHERE code IN ['x', 'y'] AND tags CONTAINS 'red';
SELECT count() FROM box WHERE code IN ['x', 'y'] AND tags CONTAINS 'red' GROUP ALL;
SELECT count() FROM box WITH NOINDEX WHERE code IN ['x', 'y'] AND tags CONTAINS 'red' GROUP ALL;

-- ===========================================================================
-- Section D -- membership against a non-leading column, and nested paths
-- ===========================================================================
-- The IN expansion requires the column to be the index's FIRST column: a
-- compound key cannot be seeked on its second component.

{
	DEFINE TABLE kv;
	DEFINE INDEX idx_kv ON kv COLUMNS k, v;
	DEFINE INDEX idx_kv_meta ON kv COLUMNS meta.kind;
	CREATE kv:1 SET k = 'a', v = 1, meta = { kind: 'p' };
	CREATE kv:2 SET k = 'a', v = 2, meta = { kind: 'q' };
	CREATE kv:3 SET k = 'b', v = 1, meta = { kind: 'p' };
	RETURN 'OK';
};

-- D1: membership on the SECOND column of the only matching index. No seek is
-- possible, so this must be a table scan -- not a union of prefix scans on
-- `k`, which would be a different question entirely.
EXPLAIN ANALYZE SELECT VALUE id FROM kv WHERE v IN [1, 2];
SELECT VALUE id FROM kv WHERE v IN [1, 2];
SELECT VALUE id FROM kv WITH NOINDEX WHERE v IN [1, 2];

-- D2: membership on the leading column of the same index expands into prefix
-- scans, with the trailing column left for ordering and further narrowing.
EXPLAIN ANALYZE SELECT VALUE id FROM kv WHERE k IN ['a', 'b'];
SELECT VALUE id FROM kv WHERE k IN ['a', 'b'];
SELECT VALUE id FROM kv WITH NOINDEX WHERE k IN ['a', 'b'];

-- D3: membership on a nested object path. The index column is a dotted path
-- with no `*`, so this is ordinary scalar membership -- nesting alone must not
-- push the predicate onto the element-containment path.
EXPLAIN ANALYZE SELECT VALUE id FROM kv WHERE meta.kind IN ['p', 'z'];
SELECT VALUE id FROM kv WHERE meta.kind IN ['p', 'z'];
SELECT VALUE id FROM kv WITH NOINDEX WHERE meta.kind IN ['p', 'z'];

-- ===========================================================================
-- Section E -- array-of-object element paths, and array-valued elements
-- ===========================================================================
-- The fanning part sits in the middle of the path rather than at the end, so
-- this exercises the path matcher as well as the access choice. ord:1 holds
-- two DISTINCT skus and is the dedupe row for this shape. nest:f's array holds
-- an array as one of its elements, which is what makes E4 answerable.

{
	DEFINE TABLE ord;
	DEFINE INDEX idx_ord_sku ON ord COLUMNS lines.*.sku;
	DEFINE TABLE nest;
	DEFINE INDEX idx_nest ON nest COLUMNS tags.*;
	CREATE ord:1 SET lines = [{ sku: 's1', qty: 1 }, { sku: 's2', qty: 2 }];
	CREATE ord:2 SET lines = [{ sku: 's2', qty: 3 }];
	CREATE ord:3 SET lines = [{ sku: 's3', qty: 4 }];
	CREATE nest:a SET tags = ['red', 'green'];
	CREATE nest:f SET tags = [['red', 'green'], 'x'];
	RETURN 'OK';
};

-- E1: a union over a nested element path; ord:1 sits in both branches and must
-- be emitted once, with count() agreeing with the oracle.
EXPLAIN ANALYZE SELECT VALUE id FROM ord WHERE lines.*.sku CONTAINSANY ['s1', 's2'];
SELECT VALUE id FROM ord WHERE lines.*.sku CONTAINSANY ['s1', 's2'];
SELECT VALUE id FROM ord WITH NOINDEX WHERE lines.*.sku CONTAINSANY ['s1', 's2'];
SELECT count() FROM ord WHERE lines.*.sku CONTAINSANY ['s1', 's2'] GROUP ALL;
SELECT count() FROM ord WITH NOINDEX WHERE lines.*.sku CONTAINSANY ['s1', 's2'] GROUP ALL;

-- E2: E1's union over a nested element path, with the operands swapped.
EXPLAIN ANALYZE SELECT VALUE id FROM ord WHERE ['s1', 's2'] ANYINSIDE lines.*.sku;
SELECT VALUE id FROM ord WHERE ['s1', 's2'] ANYINSIDE lines.*.sku;
SELECT VALUE id FROM ord WITH NOINDEX WHERE ['s1', 's2'] ANYINSIDE lines.*.sku;

-- E3: membership written against the flattened path itself. `lines.*.sku`
-- evaluates to the whole array of skus, so `IN` compares that array against
-- each literal instead of asking about elements -- a different question from
-- E1, and one no record answers. Pinned because the element index must not be
-- used to answer it: doing so would return ord:1 and ord:2. The count pair
-- holds the index-only count path to the same answer, which is the half that
-- can reach the index without the rows changing.
EXPLAIN ANALYZE SELECT VALUE id FROM ord WHERE lines.*.sku IN ['s1', 's2'];
SELECT VALUE id FROM ord WHERE lines.*.sku IN ['s1', 's2'];
SELECT VALUE id FROM ord WITH NOINDEX WHERE lines.*.sku IN ['s1', 's2'];
SELECT count() FROM ord WHERE lines.*.sku IN ['s1', 's2'] GROUP ALL;
SELECT count() FROM ord WITH NOINDEX WHERE lines.*.sku IN ['s1', 's2'] GROUP ALL;

-- E4: CONTAINS with an array on the right is still element equality, not a
-- subset test: it matches nest:f, whose array holds that array as one element,
-- and not nest:a, whose elements are the two strings. The seek is an equality
-- on an array-valued index entry, and the count pair pins that the two-element
-- key is visited once rather than once per element.
EXPLAIN ANALYZE SELECT VALUE id FROM nest WHERE tags CONTAINS ['red', 'green'];
SELECT VALUE id FROM nest WHERE tags CONTAINS ['red', 'green'];
SELECT VALUE id FROM nest WITH NOINDEX WHERE tags CONTAINS ['red', 'green'];
SELECT count() FROM nest WHERE tags CONTAINS ['red', 'green'] GROUP ALL;
SELECT count() FROM nest WITH NOINDEX WHERE tags CONTAINS ['red', 'green'] GROUP ALL;

-- ===========================================================================
-- Section F -- compound index whose TRAILING column fans out
-- ===========================================================================
-- `idx_tl` leads with a scalar column and trails with a fanning one, so each
-- record contributes one key per element of `tags`. A key range that pins the
-- leading column alone therefore reaches a record once per element, and the
-- scan is wrapped in Distinct so each record reaches the statement above it
-- once. Pinning the trailing column as well -- which a single-value
-- containment does -- leaves one key per record and needs no wrap. tl:1 and
-- tl:3 share a `code`, so the wrap has to collapse each record's own entries
-- rather than collapse the whole scan to a single row.

{
	DEFINE TABLE tl;
	DEFINE INDEX idx_tl ON tl COLUMNS code, tags.*;
	CREATE tl:1 SET code = 'x', tags = ['red', 'green'];
	CREATE tl:2 SET code = 'y', tags = ['blue'];
	CREATE tl:3 SET code = 'x', tags = ['red', 'yellow'];
	RETURN 'OK';
};

-- F1: a single-element IN normalises to one prefix seek (as in A3), and the
-- seek walks every key of every matching record. The Distinct directly above
-- the scan is what makes each record arrive once: the scan's own {rows: N}
-- counts the keys it walked, the Distinct's counts the records it passed on,
-- and count() takes the second of those -- it agrees with the oracle rather
-- than with the entry total.
EXPLAIN ANALYZE SELECT VALUE id FROM tl WHERE code IN ['x'];
SELECT VALUE id FROM tl WHERE code IN ['x'];
SELECT VALUE id FROM tl WITH NOINDEX WHERE code IN ['x'];
SELECT count() FROM tl WHERE code IN ['x'] GROUP ALL;
SELECT count() FROM tl WITH NOINDEX WHERE code IN ['x'] GROUP ALL;

-- F2: the same seek reached through equality rather than membership, which
-- identifies the key range -- and not the IN normalisation -- as what decides
-- the wrap.
EXPLAIN ANALYZE SELECT VALUE id FROM tl WHERE code = 'x';
SELECT VALUE id FROM tl WHERE code = 'x';
SELECT VALUE id FROM tl WITH NOINDEX WHERE code = 'x';
SELECT count() FROM tl WHERE code = 'x' GROUP ALL;
SELECT count() FROM tl WITH NOINDEX WHERE code = 'x' GROUP ALL;

-- F3: a multi-element IN over the same index. UnionIndexScan merges its
-- branches on record id, so it collapses each record's per-element keys itself
-- and neither the branches nor the union carry a Distinct -- the same reason
-- the array unions of section B carry none. The recorded rows are the
-- oracle's set emitted branch-major, as in C2.
EXPLAIN ANALYZE SELECT VALUE id FROM tl WHERE code IN ['x', 'y'];
SELECT VALUE id FROM tl WHERE code IN ['x', 'y'];
SELECT VALUE id FROM tl WITH NOINDEX WHERE code IN ['x', 'y'];
SELECT count() FROM tl WHERE code IN ['x', 'y'] GROUP ALL;
SELECT count() FROM tl WITH NOINDEX WHERE code IN ['x', 'y'] GROUP ALL;

-- F4: a single-value containment on the trailing column extends the prefix to
-- both columns, which pins the fanning column to one element and leaves one
-- key per record -- so this is an exact two-column seek with NO Distinct. The
-- contrast against F2, the same leading value on the same index, is the
-- observable difference between an exact seek and a seek-then-collapse. The
-- containment is still re-checked per record as a residual Filter rather than
-- being treated as proven by the key range.
EXPLAIN ANALYZE SELECT VALUE id FROM tl WHERE code = 'x' AND tags CONTAINS 'red';
SELECT VALUE id FROM tl WHERE code = 'x' AND tags CONTAINS 'red';
SELECT VALUE id FROM tl WITH NOINDEX WHERE code = 'x' AND tags CONTAINS 'red';
SELECT count() FROM tl WHERE code = 'x' AND tags CONTAINS 'red' GROUP ALL;
SELECT count() FROM tl WITH NOINDEX WHERE code = 'x' AND tags CONTAINS 'red' GROUP ALL;

-- F5: a negated containment cannot narrow a key range, so the leading column
-- is pinned alone and the trailing one stays open. This pins the stacking
-- order: the Distinct sits directly above the IndexScan and the residual
-- Filter above the Distinct, so the negation is tested once per record. Tested
-- once per key instead it would reject tl:3 on its 'yellow' key and admit it
-- on its 'red' one.
EXPLAIN ANALYZE SELECT VALUE id FROM tl WHERE code = 'x' AND tags CONTAINSNOT 'yellow';
SELECT VALUE id FROM tl WHERE code = 'x' AND tags CONTAINSNOT 'yellow';
SELECT VALUE id FROM tl WITH NOINDEX WHERE code = 'x' AND tags CONTAINSNOT 'yellow';
SELECT count() FROM tl WHERE code = 'x' AND tags CONTAINSNOT 'yellow' GROUP ALL;
SELECT count() FROM tl WITH NOINDEX WHERE code = 'x' AND tags CONTAINSNOT 'yellow' GROUP ALL;

-- F6: only a single-value containment extends the prefix (F4). A multi-value
-- one leaves the trailing column open, so the seek stays on the leading column
-- with the wrap above it and the containment as a residual -- the prefix is
-- not extended once per literal, and no union appears.
EXPLAIN ANALYZE SELECT VALUE id FROM tl WHERE code = 'x' AND tags CONTAINSANY ['red', 'yellow'];
SELECT VALUE id FROM tl WHERE code = 'x' AND tags CONTAINSANY ['red', 'yellow'];
SELECT VALUE id FROM tl WITH NOINDEX WHERE code = 'x' AND tags CONTAINSANY ['red', 'yellow'];
SELECT count() FROM tl WHERE code = 'x' AND tags CONTAINSANY ['red', 'yellow'] GROUP ALL;
SELECT count() FROM tl WITH NOINDEX WHERE code = 'x' AND tags CONTAINSANY ['red', 'yellow'] GROUP ALL;
