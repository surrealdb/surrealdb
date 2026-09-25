/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
Plan and row coverage for the BOOLEAN STRUCTURE of a WHERE clause: the index set and the leaf operators are held fixed and only the shape of the AND/OR/NOT tree varies. One conjunct; two conjuncts on one index; two conjuncts on two indexes; a top-level OR of two indexed equalities; an OR with an unindexed branch; an OR with an always-empty branch; AND nested in OR and OR nested in AND, including an OR whose member is unindexed; NOT and ! around an indexed leaf; double negation in both spellings; AND NOT; a non-boolean conjunct taken for its truthiness; true/false literals ANDed and ORed in; bitmap intersection, union and subtraction and what blocks them; and each of those shapes again over a fan-out array column in both index spellings, including whole-array equality against a scalar and against the array's own value. Every indexed query is paired with the same query under WITH NOINDEX, which forces a table scan and is therefore the row oracle, and every shape whose candidate set can fan out also pins count() ... GROUP ALL on both sides, because count has its own index-only path.

planner-strategy is narrowed because a prefix EXPLAIN errors under compute-only. Both streaming strategies are pinned rather than one: they plan identically by contract, so recording both states that intent and costs nothing.

ONE SHAPE RETURNS A WRONG ANSWER AND IS PINNED AS WRONG. `plain = ['red', 'blue'] AND b = true` -- whole-array equality against the array's own value, on a column indexed as a bare `FIELDS plain`, fused with an indexed scalar -- builds a BitmapAnd over idx_b and idx_plain and returns [], losing bs:1, while the WITH NOINDEX oracle returns it. idx_plain holds one entry per element, so the whole-array key the seek asks for is not in it; fusing the leaf into a bitmap intersection neither hides nor repairs the loss, which is what this file adds. Correct answer: [{ id: bs:1 }]. Correct behaviour is either to decline the leaf on this index shape -- which is exactly what the `tags[*]` spelling does with the identical comparison, answering it by scan and returning bs:1, pinned here as the contrast -- or to hold the key the seek asks for. The leaf on its own, and its count() form, belong to the fan-out key model and are pinned once, in array_fanout_oracle.surql.

Every other shape agrees with the oracle. Two of them emit the same rows in a different order from the oracle because the query has no ORDER BY and the index scan emits in key order, which is not a defect. Five further defects are plan-level only — the rows stay correct, so nothing but the plan assertion would notice them — and are likewise recorded as they currently behave, not as they should:

1. NOT and ! are the same operator but do not plan the same. Around a standalone leaf the keyword renders `not(...)` and gives up the pre-decode filter (`pre_decode_filter: no (unsupported predicate)`), while the symbol renders `! a = 'x'` and keeps it (`pre_decode_filter: yes`). Correct behaviour is one plan for both spellings, with the pre-decode filter applied.

2. A double negation is not folded back to its positive leaf. Both `NOT (NOT (a = 'x'))` and `!(!(a = 'x'))` scan the table, and the keyword form also loses the pre-decode filter. Correct behaviour is to fold to `a = 'x'` and seek idx_a.

3. An index declared on a bare array column is unreachable by every element test. idx_plain holds one entry per element of `plain`, yet `plain CONTAINS 'red'` scans the table in every boolean position, while the same shapes over `tags`, indexed as `tags[*]`, fuse and seek correctly. Correct behaviour is for CONTAINS to use the per-element entries under both index spellings. This is the same index shape as the wrong answer above, seen from the opposite side: the element test that the entries would serve is declined, and the whole-array test that they cannot serve is accepted.

4. The index-only count fast path covers AND and AND-NOT fusion but not union. A count over a fused BitmapOr, and a count over a UnionIndexScan of two branches of one index, both fall back to Aggregate over a resolved candidate set even though the union is exact and its cardinality is available. The counts are correct; the fast path is simply not taken.

5. An OR of two conjunctions does not fuse per branch. `(a = 'x' AND b = true) OR (a = 'y' AND b = false)` unions idx_a on both branches and keeps both `b` conjuncts as one residual Filter, instead of intersecting each branch and unioning the two results.
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
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_a, access: = 'x', direction: Forward, predicate: a = 'x']
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:6 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_n, access: >2 <8, direction: Forward, predicate: n > 2 AND n < 8]
''''

[[test.results]]
value = "[{ id: bs:4 }, { id: bs:2 }, { id: bs:5 }]"

[[test.results]]
value = "[{ id: bs:2 }, { id: bs:4 }, { id: bs:5 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: a = 'x' AND b = true]
        BitmapResolve [ctx: Db] [table: bs]
            BitmapAnd [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_a, access: = 'x']
                BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:6 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: bs, condition: a = 'x' AND b = true]
    BitmapAnd [ctx: Db]
        BitmapIndexScan [ctx: Db] [index: idx_a, access: = 'x']
        BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: a = 'x' OR b = false]
        BitmapResolve [ctx: Db] [table: bs]
            BitmapOr [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_a, access: = 'x']
                BitmapIndexScan [ctx: Db] [index: idx_b, access: = false]
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:4 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:4 }, { id: bs:6 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    Filter [ctx: Db] [predicate: a = 'x' OR b = false]
        BitmapResolve [ctx: Db] [table: bs]
            BitmapOr [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_a, access: = 'x']
                BitmapIndexScan [ctx: Db] [index: idx_b, access: = false]
"'''

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: bs, direction: Forward, predicate: a = 'x' OR u = 'r', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:4 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:4 }, { id: bs:6 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: a = 'x' OR a = 'absent']
        UnionIndexScan [ctx: Db] [table: bs, branches: 2]
            IndexScan [ctx: Db] [index: idx_a, access: = 'x', direction: Forward]
            IndexScan [ctx: Db] [index: idx_a, access: = 'absent', direction: Forward]
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:6 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: a = 'x' AND b = true OR n = 9]
        BitmapResolve [ctx: Db] [table: bs]
            BitmapOr [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_a, access: = 'x']
                BitmapIndexScan [ctx: Db] [index: idx_n, access: = 9]
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:3 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:3 }, { id: bs:6 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: a = 'x' AND b = true OR a = 'y' AND b = false]
        UnionIndexScan [ctx: Db] [table: bs, branches: 2]
            IndexScan [ctx: Db] [index: idx_a, access: = 'x', direction: Forward]
            IndexScan [ctx: Db] [index: idx_a, access: = 'y', direction: Forward]
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:6 }, { id: bs:4 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:4 }, { id: bs:6 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: b = true AND a = 'x' OR a = 'y']
        BitmapResolve [ctx: Db] [table: bs]
            BitmapAnd [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]
                BitmapOr [ctx: Db]
                    BitmapIndexScan [ctx: Db] [index: idx_a, access: = 'x']
                    BitmapIndexScan [ctx: Db] [index: idx_a, access: = 'y']
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:3 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:3 }, { id: bs:6 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: a = 'x' OR u = 'r']
        IndexScan [ctx: Db] [index: idx_b, access: = true, direction: Forward]
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:6 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: bs, direction: Forward, predicate: not(...), pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = "[{ id: bs:3 }, { id: bs:4 }, { id: bs:5 }]"

[[test.results]]
value = "[{ id: bs:3 }, { id: bs:4 }, { id: bs:5 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: bs, direction: Forward, predicate: ! a = 'x', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: bs:3 }, { id: bs:4 }, { id: bs:5 }]"

[[test.results]]
value = "[{ id: bs:3 }, { id: bs:4 }, { id: bs:5 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: a = 'x' AND not(...)]
        BitmapResolve [ctx: Db] [table: bs]
            BitmapAndNot [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_a, access: = 'x']
                BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]
"'''

[[test.results]]
value = "[{ id: bs:2 }]"

[[test.results]]
value = "[{ id: bs:2 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: bs, condition: a = 'x' AND not(...)]
    BitmapAndNot [ctx: Db]
        BitmapIndexScan [ctx: Db] [index: idx_a, access: = 'x']
        BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]
"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: a = 'x' AND ! b = true]
        BitmapResolve [ctx: Db] [table: bs]
            BitmapAndNot [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_a, access: = 'x']
                BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]
"'''

[[test.results]]
value = "[{ id: bs:2 }]"

[[test.results]]
value = "[{ id: bs:2 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: bs, direction: Forward, predicate: not(...), pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:6 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: bs, direction: Forward, predicate: ! ! a = 'x', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:6 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: u]
        IndexScan [ctx: Db] [index: idx_a, access: = 'x', direction: Forward]
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:6 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_a, access: = 'x', direction: Forward, predicate: a = 'x']
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:6 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Rt] [projections: id]
    EmptyScan [ctx: Rt]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: bs, direction: Forward, predicate: true, pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:3 }, { id: bs:4 }, { id: bs:5 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:3 }, { id: bs:4 }, { id: bs:5 }, { id: bs:6 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: a = 'x' AND b = true AND u = 'p']
        BitmapResolve [ctx: Db] [table: bs]
            BitmapAnd [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_a, access: = 'x']
                BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:6 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    Filter [ctx: Db] [predicate: a = 'x' AND b = true AND u = 'p']
        BitmapResolve [ctx: Db] [table: bs]
            BitmapAnd [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_a, access: = 'x']
                BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_tags, access: = 'red', direction: Forward, predicate: tags CONTAINS 'red']
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:3 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:3 }, { id: bs:6 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: bs, condition: tags CONTAINS 'red']
"'''

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: tags CONTAINS 'red' AND b = true]
        BitmapResolve [ctx: Db] [table: bs]
            BitmapAnd [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]
                BitmapIndexScan [ctx: Db] [index: idx_tags, access: = 'red']
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:3 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:3 }, { id: bs:6 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: bs, condition: tags CONTAINS 'red' AND b = true]
    BitmapAnd [ctx: Db]
        BitmapIndexScan [ctx: Db] [index: idx_tags, access: = 'red']
        BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]
"'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: tags CONTAINS 'red' OR b = false]
        BitmapResolve [ctx: Db] [table: bs]
            BitmapOr [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_tags, access: = 'red']
                BitmapIndexScan [ctx: Db] [index: idx_b, access: = false]
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:3 }, { id: bs:4 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:3 }, { id: bs:4 }, { id: bs:6 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    Filter [ctx: Db] [predicate: tags CONTAINS 'red' OR b = false]
        BitmapResolve [ctx: Db] [table: bs]
            BitmapOr [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_tags, access: = 'red']
                BitmapIndexScan [ctx: Db] [index: idx_b, access: = false]
"'''

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: tags CONTAINS 'red' OR tags CONTAINS 'blue']
        UnionIndexScan [ctx: Db] [table: bs, branches: 2]
            IndexScan [ctx: Db] [index: idx_tags, access: = 'red', direction: Forward]
            IndexScan [ctx: Db] [index: idx_tags, access: = 'blue', direction: Forward]
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:3 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:3 }, { id: bs:6 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    Filter [ctx: Db] [predicate: tags CONTAINS 'red' OR tags CONTAINS 'blue']
        UnionIndexScan [ctx: Db] [table: bs, branches: 2]
            IndexScan [ctx: Db] [index: idx_tags, access: = 'red', direction: Forward]
            IndexScan [ctx: Db] [index: idx_tags, access: = 'blue', direction: Forward]
"'''

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: bs, direction: Forward, predicate: not(...), pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = "[{ id: bs:4 }, { id: bs:5 }]"

[[test.results]]
value = "[{ id: bs:4 }, { id: bs:5 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    TableScan [ctx: Db] [table: bs, direction: Forward, predicate: not(...), pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: b = true AND not(...)]
        BitmapResolve [ctx: Db] [table: bs]
            BitmapAndNot [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]
                BitmapIndexScan [ctx: Db] [index: idx_tags, access: = 'red']
"'''

[[test.results]]
value = "[{ id: bs:5 }]"

[[test.results]]
value = "[{ id: bs:5 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: bs, condition: b = true AND not(...)]
    BitmapAndNot [ctx: Db]
        BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]
        BitmapIndexScan [ctx: Db] [index: idx_tags, access: = 'red']
"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: bs, direction: Forward, predicate: plain CONTAINS 'red', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:3 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:3 }, { id: bs:6 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    TableScan [ctx: Db] [table: bs, direction: Forward, predicate: plain CONTAINS 'red', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: plain CONTAINS 'red']
        IndexScan [ctx: Db] [index: idx_b, access: = true, direction: Forward]
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:3 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:3 }, { id: bs:6 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    Filter [ctx: Db] [predicate: plain CONTAINS 'red']
        IndexScan [ctx: Db] [index: idx_b, access: = true, direction: Forward]
"'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: bs, direction: Forward, predicate: plain CONTAINS 'red' OR b = false, pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:3 }, { id: bs:4 }, { id: bs:6 }]"

[[test.results]]
value = "[{ id: bs:1 }, { id: bs:2 }, { id: bs:3 }, { id: bs:4 }, { id: bs:6 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    TableScan [ctx: Db] [table: bs, direction: Forward, predicate: plain CONTAINS 'red' OR b = false, pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_plain, access: = 'red', direction: Forward, predicate: plain = 'red']
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: plain = 'red' AND b = true]
        BitmapResolve [ctx: Db] [table: bs]
            BitmapAnd [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]
                BitmapIndexScan [ctx: Db] [index: idx_plain, access: = 'red']
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    Filter [ctx: Db] [predicate: plain = 'red' AND b = true]
        BitmapResolve [ctx: Db] [table: bs]
            BitmapAnd [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]
                BitmapIndexScan [ctx: Db] [index: idx_plain, access: = 'red']
"'''

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: plain = ['red', 'blue'] AND b = true]
        BitmapResolve [ctx: Db] [table: bs]
            BitmapAnd [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_b, access: = true]
                BitmapIndexScan [ctx: Db] [index: idx_plain, access: = ['red', 'blue']]
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: bs:1 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: bs, direction: Forward, predicate: tags = ['red', 'blue'], pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: bs:1 }]"

[[test.results]]
value = "[{ id: bs:1 }]"
*/

-- Fixture. The index set and the leaf operators are held fixed across the whole
-- file; only the boolean structure of the WHERE clause varies. Declared scalar
-- kinds are what let a negated bitmap branch be exact, so they are part of the
-- fixture rather than decoration.
DEFINE FIELD a ON bs TYPE string;
DEFINE FIELD b ON bs TYPE bool;
DEFINE FIELD n ON bs TYPE int;
DEFINE FIELD tags ON bs TYPE array<string>;
DEFINE FIELD plain ON bs TYPE array<string>;
DEFINE FIELD u ON bs TYPE string;
DEFINE INDEX idx_a ON bs FIELDS a;
DEFINE INDEX idx_b ON bs FIELDS b;
DEFINE INDEX idx_n ON bs FIELDS n;
DEFINE INDEX idx_tags ON bs FIELDS tags[*];
DEFINE INDEX idx_plain ON bs FIELDS plain;
-- `u` is deliberately unindexed: it is the only way to build a branch no index
-- can serve. bs:1 and bs:3 carry multi-element tags with distinct members, so a
-- fan-out defect shows up as a duplicated row or an inflated count rather than
-- hiding behind a single-element array; bs:4 carries the empty array, which
-- produces no index entry at all.
CREATE bs:1 SET a = 'x', b = true,  n = 1, u = 'p', tags = ['red', 'blue'], plain = ['red', 'blue'] RETURN NONE;
CREATE bs:2 SET a = 'x', b = false, n = 5, u = 'q', tags = ['red'], plain = ['red'] RETURN NONE;
CREATE bs:3 SET a = 'y', b = true,  n = 9, u = 'p', tags = ['blue', 'green', 'red'], plain = ['blue', 'green', 'red'] RETURN NONE;
CREATE bs:4 SET a = 'y', b = false, n = 3, u = 'r', tags = [], plain = [] RETURN NONE;
CREATE bs:5 SET a = 'z', b = true,  n = 7, u = 'p', tags = ['green'], plain = ['green'] RETURN NONE;
-- bs:6 repeats an element. A fan-out index holds one entry per occurrence, so
-- a candidate set that is not deduped by record counts this row twice.
CREATE bs:6 SET a = 'x', b = true,  n = 11, u = 'p', tags = ['red', 'red', 'blue'], plain = ['red', 'red', 'blue'] RETURN NONE;

-- One conjunct, one index. The baseline every other shape is measured against.
EXPLAIN SELECT id FROM bs WHERE a = 'x';
SELECT id FROM bs WHERE a = 'x';
SELECT id FROM bs WITH NOINDEX WHERE a = 'x';

-- Two conjuncts, one index. A two-sided range on the same column must collapse
-- into a single bounded access, not into two scans or a scan plus a residual.
EXPLAIN SELECT id FROM bs WHERE n > 2 AND n < 8;
SELECT id FROM bs WHERE n > 2 AND n < 8;
SELECT id FROM bs WITH NOINDEX WHERE n > 2 AND n < 8;

-- Two conjuncts, two indexes: the AND fuses into a bitmap intersection. Both
-- branches are exactly index-covered, so the count variant is answered from
-- cardinalities without fetching a record.
EXPLAIN SELECT id FROM bs WHERE a = 'x' AND b = true;
SELECT id FROM bs WHERE a = 'x' AND b = true;
SELECT id FROM bs WITH NOINDEX WHERE a = 'x' AND b = true;
EXPLAIN SELECT count() FROM bs WHERE a = 'x' AND b = true GROUP ALL;
SELECT count() FROM bs WHERE a = 'x' AND b = true GROUP ALL;
SELECT count() FROM bs WITH NOINDEX WHERE a = 'x' AND b = true GROUP ALL;

-- Top-level OR of two indexed equalities: a bitmap union. bs:2 satisfies both
-- branches, so the row must appear once and the count must not double it. The
-- count variant records a defect: an exact BitmapOr does not reach the
-- index-only count path the way BitmapAnd does, so the plan resolves candidates
-- and aggregates over them. The count is right; the fast path is not taken.
EXPLAIN SELECT id FROM bs WHERE a = 'x' OR b = false;
SELECT id FROM bs WHERE a = 'x' OR b = false;
SELECT id FROM bs WITH NOINDEX WHERE a = 'x' OR b = false;
EXPLAIN SELECT count() FROM bs WHERE a = 'x' OR b = false GROUP ALL;
SELECT count() FROM bs WHERE a = 'x' OR b = false GROUP ALL;
SELECT count() FROM bs WITH NOINDEX WHERE a = 'x' OR b = false GROUP ALL;

-- An OR is only as indexable as its weakest branch. With `u` unindexed no
-- union can be built, and using idx_a for the left branch alone would lose the
-- rows that only satisfy the right one: the whole disjunction must fall back to
-- a scan.
EXPLAIN SELECT id FROM bs WHERE a = 'x' OR u = 'r';
SELECT id FROM bs WHERE a = 'x' OR u = 'r';
SELECT id FROM bs WITH NOINDEX WHERE a = 'x' OR u = 'r';

-- An OR branch that matches nothing is still a branch: it must contribute an
-- empty candidate set rather than being dropped or widening the result.
EXPLAIN SELECT id FROM bs WHERE a = 'x' OR a = 'absent';
SELECT id FROM bs WHERE a = 'x' OR a = 'absent';
SELECT id FROM bs WITH NOINDEX WHERE a = 'x' OR a = 'absent';

-- AND nested inside OR: the conjunction is one union member, the bare equality
-- the other.
EXPLAIN SELECT id FROM bs WHERE (a = 'x' AND b = true) OR n = 9;
SELECT id FROM bs WHERE (a = 'x' AND b = true) OR n = 9;
SELECT id FROM bs WITH NOINDEX WHERE (a = 'x' AND b = true) OR n = 9;

-- Both union members are conjunctions. bs:1 and bs:4 each satisfy exactly one
-- side, so a member that leaked its partner's rows would be visible here. The
-- recorded plan is a defect: idx_a serves both branches and both `b` conjuncts
-- collapse into one residual Filter, rather than each branch intersecting its
-- own pair of indexes and the two results being unioned.
EXPLAIN SELECT id FROM bs WHERE (a = 'x' AND b = true) OR (a = 'y' AND b = false);
SELECT id FROM bs WHERE (a = 'x' AND b = true) OR (a = 'y' AND b = false);
SELECT id FROM bs WITH NOINDEX WHERE (a = 'x' AND b = true) OR (a = 'y' AND b = false);

-- OR nested inside AND: the disjunction is indexable, so it can serve as a
-- union under the intersection.
EXPLAIN SELECT id FROM bs WHERE b = true AND (a = 'x' OR a = 'y');
SELECT id FROM bs WHERE b = true AND (a = 'x' OR a = 'y');
SELECT id FROM bs WITH NOINDEX WHERE b = true AND (a = 'x' OR a = 'y');

-- OR nested inside AND with an unindexed OR member. The nested disjunction is
-- unusable, but the sibling conjunct is not: the index may narrow on `b` alone
-- and keep the disjunction as a residual. What must not happen is the nested OR
-- being treated as indexable.
EXPLAIN SELECT id FROM bs WHERE b = true AND (a = 'x' OR u = 'r');
SELECT id FROM bs WHERE b = true AND (a = 'x' OR u = 'r');
SELECT id FROM bs WITH NOINDEX WHERE b = true AND (a = 'x' OR u = 'r');

-- NOT around the only leaf. An index seek on the inner equality would answer
-- the complement of the question, so no candidate may be produced from it, and
-- there is no positive anchor for a subtraction to start from.
EXPLAIN SELECT id FROM bs WHERE NOT (a = 'x');
SELECT id FROM bs WHERE NOT (a = 'x');
SELECT id FROM bs WITH NOINDEX WHERE NOT (a = 'x');

-- `!` is the other spelling of the same operator and must plan identically. It
-- does not, and the divergence is recorded: the keyword renders `not(...)`
-- and reports `pre_decode_filter: no (unsupported predicate)`, while `!` renders
-- `! a = 'x'` and keeps the pre-decode filter. Both return the same rows, so
-- only the plan assertion sees it.
EXPLAIN SELECT id FROM bs WHERE !(a = 'x');
SELECT id FROM bs WHERE !(a = 'x');
SELECT id FROM bs WITH NOINDEX WHERE !(a = 'x');

-- AND NOT: a positive anchor plus a negated indexed leaf becomes a bitmap
-- subtraction, and the count variant has its own subtraction path.
EXPLAIN SELECT id FROM bs WHERE a = 'x' AND NOT (b = true);
SELECT id FROM bs WHERE a = 'x' AND NOT (b = true);
SELECT id FROM bs WITH NOINDEX WHERE a = 'x' AND NOT (b = true);
EXPLAIN SELECT count() FROM bs WHERE a = 'x' AND NOT (b = true) GROUP ALL;
SELECT count() FROM bs WHERE a = 'x' AND NOT (b = true) GROUP ALL;
SELECT count() FROM bs WITH NOINDEX WHERE a = 'x' AND NOT (b = true) GROUP ALL;
EXPLAIN SELECT id FROM bs WHERE a = 'x' AND !(b = true);
SELECT id FROM bs WHERE a = 'x' AND !(b = true);
SELECT id FROM bs WITH NOINDEX WHERE a = 'x' AND !(b = true);

-- Double negation is the positive leaf again, and neither spelling is folded
-- back to it: both scan the table instead of seeking idx_a, and the keyword form
-- additionally loses the pre-decode filter. Recorded as the defect it is — the
-- correct plan is the `a = 'x'` index seek — because the rows stay correct and
-- nothing else would notice.
EXPLAIN SELECT id FROM bs WHERE NOT (NOT (a = 'x'));
SELECT id FROM bs WHERE NOT (NOT (a = 'x'));
SELECT id FROM bs WITH NOINDEX WHERE NOT (NOT (a = 'x'));
EXPLAIN SELECT id FROM bs WHERE !(!(a = 'x'));
SELECT id FROM bs WHERE !(!(a = 'x'));
SELECT id FROM bs WITH NOINDEX WHERE !(!(a = 'x'));

-- A conjunct that is not a comparison at all — a bare field read taken for its
-- truthiness. It is not indexable, so it must survive as a residual while the
-- sibling equality still drives the scan.
EXPLAIN SELECT id FROM bs WHERE a = 'x' AND u;
SELECT id FROM bs WHERE a = 'x' AND u;
SELECT id FROM bs WITH NOINDEX WHERE a = 'x' AND u;

-- Boolean literals ANDed and ORed in. `AND true` must not disturb the plan,
-- `AND false` makes the whole predicate unsatisfiable, and `OR true` makes the
-- index useless because every row qualifies.
EXPLAIN SELECT id FROM bs WHERE a = 'x' AND true;
SELECT id FROM bs WHERE a = 'x' AND true;
SELECT id FROM bs WITH NOINDEX WHERE a = 'x' AND true;
EXPLAIN SELECT id FROM bs WHERE a = 'x' AND false;
SELECT id FROM bs WHERE a = 'x' AND false;
SELECT id FROM bs WITH NOINDEX WHERE a = 'x' AND false;
EXPLAIN SELECT id FROM bs WHERE a = 'x' OR true;
SELECT id FROM bs WHERE a = 'x' OR true;
SELECT id FROM bs WITH NOINDEX WHERE a = 'x' OR true;

-- What blocks fusion: a third conjunct on an unindexed field. The two indexed
-- conjuncts can still fuse, but the fused set is no longer exact, so the count
-- fast path has to give way to a resolve-and-filter plan.
EXPLAIN SELECT id FROM bs WHERE a = 'x' AND b = true AND u = 'p';
SELECT id FROM bs WHERE a = 'x' AND b = true AND u = 'p';
SELECT id FROM bs WITH NOINDEX WHERE a = 'x' AND b = true AND u = 'p';
EXPLAIN SELECT count() FROM bs WHERE a = 'x' AND b = true AND u = 'p' GROUP ALL;
SELECT count() FROM bs WHERE a = 'x' AND b = true AND u = 'p' GROUP ALL;
SELECT count() FROM bs WITH NOINDEX WHERE a = 'x' AND b = true AND u = 'p' GROUP ALL;

-- The same boolean shapes over a fan-out column. `idx_tags` is declared on
-- `tags[*]`, so it holds one entry per element and CONTAINS is an indexable
-- leaf: the shapes below put a fan-out branch in each boolean position. bs:1
-- and bs:3 match both OR branches through several entries each, so a union that
-- deduped by entry rather than by record would emit a row twice and inflate the
-- count.
EXPLAIN SELECT id FROM bs WHERE tags CONTAINS 'red';
SELECT id FROM bs WHERE tags CONTAINS 'red';
SELECT id FROM bs WITH NOINDEX WHERE tags CONTAINS 'red';
EXPLAIN SELECT count() FROM bs WHERE tags CONTAINS 'red' GROUP ALL;
SELECT count() FROM bs WHERE tags CONTAINS 'red' GROUP ALL;
SELECT count() FROM bs WITH NOINDEX WHERE tags CONTAINS 'red' GROUP ALL;

-- Fan-out branch intersected with a scalar branch.
EXPLAIN SELECT id FROM bs WHERE tags CONTAINS 'red' AND b = true;
SELECT id FROM bs WHERE tags CONTAINS 'red' AND b = true;
SELECT id FROM bs WITH NOINDEX WHERE tags CONTAINS 'red' AND b = true;
EXPLAIN SELECT count() FROM bs WHERE tags CONTAINS 'red' AND b = true GROUP ALL;
SELECT count() FROM bs WHERE tags CONTAINS 'red' AND b = true GROUP ALL;
SELECT count() FROM bs WITH NOINDEX WHERE tags CONTAINS 'red' AND b = true GROUP ALL;

-- Fan-out branch unioned with a scalar branch.
EXPLAIN SELECT id FROM bs WHERE tags CONTAINS 'red' OR b = false;
SELECT id FROM bs WHERE tags CONTAINS 'red' OR b = false;
SELECT id FROM bs WITH NOINDEX WHERE tags CONTAINS 'red' OR b = false;
EXPLAIN SELECT count() FROM bs WHERE tags CONTAINS 'red' OR b = false GROUP ALL;
SELECT count() FROM bs WHERE tags CONTAINS 'red' OR b = false GROUP ALL;
SELECT count() FROM bs WITH NOINDEX WHERE tags CONTAINS 'red' OR b = false GROUP ALL;

-- Two branches of the same fan-out index. bs:1 and bs:3 satisfy both branches,
-- so this is the shape where per-entry dedupe emits a row twice.
EXPLAIN SELECT id FROM bs WHERE tags CONTAINS 'red' OR tags CONTAINS 'blue';
SELECT id FROM bs WHERE tags CONTAINS 'red' OR tags CONTAINS 'blue';
SELECT id FROM bs WITH NOINDEX WHERE tags CONTAINS 'red' OR tags CONTAINS 'blue';
EXPLAIN SELECT count() FROM bs WHERE tags CONTAINS 'red' OR tags CONTAINS 'blue' GROUP ALL;
SELECT count() FROM bs WHERE tags CONTAINS 'red' OR tags CONTAINS 'blue' GROUP ALL;
SELECT count() FROM bs WITH NOINDEX WHERE tags CONTAINS 'red' OR tags CONTAINS 'blue' GROUP ALL;

-- NOT around the only fan-out leaf: the negation of an element test is not the
-- complement of any key range, so no candidate may come from idx_tags. bs:4's
-- empty array has no index entry at all and must still be returned.
EXPLAIN SELECT id FROM bs WHERE NOT (tags CONTAINS 'red');
SELECT id FROM bs WHERE NOT (tags CONTAINS 'red');
SELECT id FROM bs WITH NOINDEX WHERE NOT (tags CONTAINS 'red');
EXPLAIN SELECT count() FROM bs WHERE NOT (tags CONTAINS 'red') GROUP ALL;
SELECT count() FROM bs WHERE NOT (tags CONTAINS 'red') GROUP ALL;
SELECT count() FROM bs WITH NOINDEX WHERE NOT (tags CONTAINS 'red') GROUP ALL;

-- AND NOT over a fan-out column. The subtracted bitmap is per-element, so
-- subtracting it must remove every record holding at least one matching
-- element — bs:1 and bs:3 both go, leaving only bs:5.
EXPLAIN SELECT id FROM bs WHERE b = true AND NOT (tags CONTAINS 'red');
SELECT id FROM bs WHERE b = true AND NOT (tags CONTAINS 'red');
SELECT id FROM bs WITH NOINDEX WHERE b = true AND NOT (tags CONTAINS 'red');
EXPLAIN SELECT count() FROM bs WHERE b = true AND NOT (tags CONTAINS 'red') GROUP ALL;
SELECT count() FROM bs WHERE b = true AND NOT (tags CONTAINS 'red') GROUP ALL;
SELECT count() FROM bs WITH NOINDEX WHERE b = true AND NOT (tags CONTAINS 'red') GROUP ALL;

-- The same fan-out shapes against `plain`, whose index is declared on the bare
-- column rather than on `plain[*]`. The index still holds one entry per element,
-- but no element test reaches it: CONTAINS is not an indexable leaf on this
-- index shape in any boolean position, so every shape below degrades to a table
-- scan while the identical shapes over `tags` fuse and seek. That contrast is
-- what makes this a defect rather than a limitation, and it is recorded as it
-- behaves: boolean structure cannot rescue an unindexable leaf.
EXPLAIN SELECT id FROM bs WHERE plain CONTAINS 'red';
SELECT id FROM bs WHERE plain CONTAINS 'red';
SELECT id FROM bs WITH NOINDEX WHERE plain CONTAINS 'red';
EXPLAIN SELECT count() FROM bs WHERE plain CONTAINS 'red' GROUP ALL;
SELECT count() FROM bs WHERE plain CONTAINS 'red' GROUP ALL;
SELECT count() FROM bs WITH NOINDEX WHERE plain CONTAINS 'red' GROUP ALL;
EXPLAIN SELECT id FROM bs WHERE plain CONTAINS 'red' AND b = true;
SELECT id FROM bs WHERE plain CONTAINS 'red' AND b = true;
SELECT id FROM bs WITH NOINDEX WHERE plain CONTAINS 'red' AND b = true;
EXPLAIN SELECT count() FROM bs WHERE plain CONTAINS 'red' AND b = true GROUP ALL;
SELECT count() FROM bs WHERE plain CONTAINS 'red' AND b = true GROUP ALL;
SELECT count() FROM bs WITH NOINDEX WHERE plain CONTAINS 'red' AND b = true GROUP ALL;
EXPLAIN SELECT id FROM bs WHERE plain CONTAINS 'red' OR b = false;
SELECT id FROM bs WHERE plain CONTAINS 'red' OR b = false;
SELECT id FROM bs WITH NOINDEX WHERE plain CONTAINS 'red' OR b = false;
EXPLAIN SELECT count() FROM bs WHERE plain CONTAINS 'red' OR b = false GROUP ALL;
SELECT count() FROM bs WHERE plain CONTAINS 'red' OR b = false GROUP ALL;
SELECT count() FROM bs WITH NOINDEX WHERE plain CONTAINS 'red' OR b = false GROUP ALL;

-- Whole-array equality is the one leaf shape that does reach a bare-column
-- index over an array, and it has two halves that behave differently. Against a
-- scalar the comparison is false for every array value: the scan narrows to the
-- element key and the re-checked `predicate:` then rejects the row, so index and
-- oracle agree on the empty answer. The shape is recorded so that a change
-- letting the index answer it without the re-check would be caught as the wrong
-- answer it would be.
--
-- Against the array's own value the answer is WRONG, and the last shape below
-- is that wrong answer fused into a bitmap intersection: `plain = ['red',
-- 'blue'] AND b = true` builds a BitmapAnd over idx_b and idx_plain and still
-- returns [], because the seek asks for a key the per-element fan-out never
-- wrote. Correct answer: [{ id: bs:1 }], which the oracle returns. The leaf on
-- its own, and its count() form, are pinned in array_fanout_oracle.surql, which
-- owns the fan-out key model; what this file adds is that no boolean position
-- around the leaf repairs the loss.
EXPLAIN SELECT id FROM bs WHERE plain = 'red';
SELECT id FROM bs WHERE plain = 'red';
SELECT id FROM bs WITH NOINDEX WHERE plain = 'red';
EXPLAIN SELECT id FROM bs WHERE plain = 'red' AND b = true;
SELECT id FROM bs WHERE plain = 'red' AND b = true;
SELECT id FROM bs WITH NOINDEX WHERE plain = 'red' AND b = true;
EXPLAIN SELECT count() FROM bs WHERE plain = 'red' AND b = true GROUP ALL;
SELECT count() FROM bs WHERE plain = 'red' AND b = true GROUP ALL;
SELECT count() FROM bs WITH NOINDEX WHERE plain = 'red' AND b = true GROUP ALL;
EXPLAIN SELECT id FROM bs WHERE plain = ['red', 'blue'] AND b = true;
SELECT id FROM bs WHERE plain = ['red', 'blue'] AND b = true;
SELECT id FROM bs WITH NOINDEX WHERE plain = ['red', 'blue'] AND b = true;

-- The identical comparison over `tags`, indexed as `tags[*]`, is declined by
-- the planner and answered by a scan. That is the correct outcome, and it is
-- the contrast that makes the bare-column result above a defect rather than a
-- property of indexing arrays.
EXPLAIN SELECT id FROM bs WHERE tags = ['red', 'blue'];
SELECT id FROM bs WHERE tags = ['red', 'blue'];
SELECT id FROM bs WITH NOINDEX WHERE tags = ['red', 'blue'];