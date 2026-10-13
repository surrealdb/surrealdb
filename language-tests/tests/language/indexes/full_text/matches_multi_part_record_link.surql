/**
[env]
namespace = true
database = true

[test]
issue = 475
reason = """
Multi-part MATCHES (`t.name @@ 'x'`) semantics, identical across the legacy
and streaming executors (this file deliberately runs under every planner
strategy as a differential proof). A MATCHES traversing a record link to a
full-text index on the target table evaluates per row by analyzing the
idiom's value with the remote index's analyzer (the value path). A MATCHES
registered from the WHERE condition evaluates; one that is not (projection
or function-argument position, a different expression than the WHERE's, an
unresolvable idiom, or no WHERE at all) errors on rows of full table
sources with 'no suitable index'; rows of record-id and array sources
evaluate to false because they never get a query executor. `ts[*].name`
resolves through the auto-defined `ts[*]` child field while `ts.name` does
not; `record<a | b>` uses the last resolvable target before the first
unresolvable one; search::score/highlight on a remote match ref yield NONE.
Before #475 the legacy value path compared the query against its own terms
(always true) and the streaming planner rejected multi-part MATCHES with
PlannerUnimplemented.
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
value = "[{ id: t:1, name: 'Hello World' }]"

[[test.results]]
value = "[{ id: t:2, name: 'Foo Bar' }]"

[[test.results]]
value = "[{ id: i:A, t: t:1 }]"

[[test.results]]
value = "[{ id: i:B, t: t:2 }]"

[[test.results]]
value = "[{ id: i:A, t: t:1 }]"

[[test.results]]
error = "There was no suitable index supporting the expression: t.name @@ 'hello'"

[[test.results]]
value = "[{ id: i:A, m: true }]"

[[test.results]]
error = "There was no suitable index supporting the expression: t.name @@ 'foo'"

[[test.results]]
error = "There was no suitable index supporting the expression: t.name @@ 'hello'"

[[test.results]]
value = "[{ id: j:A, t: t:1 }]"

[[test.results]]
error = "There was no suitable index supporting the expression: t.name @@ 'hello'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:1, name: 'Hello World' }]"

[[test.results]]
value = "[{ id: k:A, u: u:1 }]"

[[test.results]]
error = "There was no suitable index supporting the expression: u.name @@ 'hello'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: { b: 'Hello World' }, id: s:1 }]"

[[test.results]]
value = "[{ a: { b: 'Foo Bar' }, id: s:2 }]"

[[test.results]]
value = "[{ a: { b: 'Hello World' }, id: s:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: m:1, ts: [t:1, t:2] }]"

[[test.results]]
value = "[{ id: m:2, ts: [t:2] }]"

[[test.results]]
error = "There was no suitable index supporting the expression: ts.name @@ 'hello'"

[[test.results]]
value = "[{ id: m:1, ts: [t:1, t:2] }]"

[[test.results]]
value = "[{ id: i:A, sc: NONE }]"

[[test.results]]
value = "[{ hl: NONE, id: i:A }]"

[[test.results]]
error = "There was no suitable index supporting the expression: name @@ 'hello'"

[[test.results]]
value = "[{ id: p:1, x: 'hello' }]"

[[test.results]]
error = "There was no suitable index supporting the expression: x @@ 'hello'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t2:1, name: 'Hello Planet' }]"

[[test.results]]
value = "[{ id: t2:2, name: 'Nothing Here' }]"

[[test.results]]
value = "[{ id: w:A, r: t:1 }]"

[[test.results]]
value = "[{ id: w:B, r: t2:1 }]"

[[test.results]]
value = "[{ id: w:C, r: t2:2 }]"

[[test.results]]
value = "[{ id: w:A, r: t:1 }, { id: w:B, r: t2:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:2, name: 'Nothing Here' }]"

[[test.results]]
value = "[{ id: w2:A, r: t:1 }]"

[[test.results]]
value = "[{ id: w2:B, r: u:1 }]"

[[test.results]]
value = "[{ id: w2:C, r: u:2 }]"

[[test.results]]
value = "[{ id: w2:A, r: t:1 }, { id: w2:B, r: u:1 }]"

[[test.results]]
value = "[{ id: i:A, t: t:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: i:A, t: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ i: i:A, id: c:A }]"

[[test.results]]
error = "There was no suitable index supporting the expression: i.t.name @@ 'hello'"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:1, m: false }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:1, name: 'Hello World' }]"

[[test.results]]
value = "[{ id: i:A, t: t:1 }]"

[[test.results]]
error = "Couldn't coerce value for field `t` of `i:C`: Expected `record<t>` but found `NONE`"

[[test.results]]
value = "[{ id: i:A, t: t:1 }]"
*/

DEFINE ANALYZER az TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft_name ON t COLUMNS name FULLTEXT ANALYZER az BM25 HIGHLIGHTS;
DEFINE FIELD name ON t TYPE string;
DEFINE FIELD t ON i TYPE record<t>;
CREATE t:1 SET name = 'Hello World';
CREATE t:2 SET name = 'Foo Bar';
CREATE i:A SET t = t:1;
CREATE i:B SET t = t:2;
-- 1. WHERE position (baseline)
SELECT * FROM i WHERE t.name @@ 'hello';
-- 2. projection position, no WHERE
SELECT id, t.name @@ 'hello' AS m FROM i;
-- 3. projection position, same expression in WHERE
SELECT id, t.name @@ 'hello' AS m FROM i WHERE t.name @@ 'hello';
-- 4. projection position, different MATCHES in WHERE
SELECT id, t.name @@ 'foo' AS m FROM i WHERE t.name @@ 'hello';
-- 5. fn-arg position
SELECT * FROM i WHERE array::any([t.name @@ 'hello']);
-- 6. untyped link field
CREATE j:A SET t = t:1;
SELECT * FROM j WHERE t.name @@ 'hello';
-- 7. typed link, remote table without FT index
DEFINE FIELD u ON k TYPE record<u>;
CREATE u:1 SET name = 'Hello World';
CREATE k:A SET u = u:1;
SELECT * FROM k WHERE u.name @@ 'hello';
-- 8. same-table multi-part FT column
DEFINE INDEX ft_ab ON s COLUMNS a.b FULLTEXT ANALYZER az BM25;
CREATE s:1 SET a = { b: 'Hello World' };
CREATE s:2 SET a = { b: 'Foo Bar' };
SELECT * FROM s WHERE a.b @@ 'hello';
-- 9. array-of-records link field
DEFINE FIELD ts ON m TYPE array<record<t>>;
CREATE m:1 SET ts = [t:1, t:2];
CREATE m:2 SET ts = [t:2];
SELECT * FROM m WHERE ts.name @@ 'hello';
SELECT * FROM m WHERE ts[*].name @@ 'hello';
-- 10. match ref + score on remote index
SELECT id, search::score(1) AS sc FROM i WHERE t.name @1@ 'hello';
-- 11. match ref + highlight on remote index
SELECT id, search::highlight('<b>', '</b>', 1) AS hl FROM i WHERE t.name @1@ 'hello';
-- 12. single-part projection with index, no WHERE (context)
SELECT id, name @@ 'hello' AS m FROM t;
-- 13. single-part WHERE without any FT index (context)
CREATE p:1 SET x = 'hello';
SELECT * FROM p WHERE x @@ 'hello';
-- 14. multi-target record<t | t2>: FT on both targets
DEFINE ANALYZER az2 TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft_name2 ON t2 COLUMNS name FULLTEXT ANALYZER az2 BM25;
DEFINE FIELD name ON t2 TYPE string;
DEFINE FIELD r ON w TYPE record<t | t2>;
CREATE t2:1 SET name = 'Hello Planet';
CREATE t2:2 SET name = 'Nothing Here';
CREATE w:A SET r = t:1;
CREATE w:B SET r = t2:1;
CREATE w:C SET r = t2:2;
SELECT * FROM w WHERE r.name @@ 'hello';
-- 15. multi-target where only ONE target has FT index
DEFINE FIELD r ON w2 TYPE record<t | u>;
CREATE u:2 SET name = 'Nothing Here';
CREATE w2:A SET r = t:1;
CREATE w2:B SET r = u:1;
CREATE w2:C SET r = u:2;
SELECT * FROM w2 WHERE r.name @@ 'hello';
-- 16. @OR@ / @AND@ on remote (multi-term)
SELECT * FROM i WHERE t.name @OR@ 'hello nosuchterm';
SELECT * FROM i WHERE t.name @AND@ 'hello nosuchterm';
SELECT * FROM i WHERE t.name @AND@ 'hello world';
-- 17. two-level link chain i.t.name via chain table
DEFINE FIELD i ON c TYPE record<i>;
CREATE c:A SET i = i:A;
SELECT * FROM c WHERE i.t.name @@ 'hello';
-- 18. record-id source: cond and projection positions
SELECT * FROM t:1 WHERE name @@ 'hello';
SELECT id, name @@ 'hello' AS m FROM t:1;
-- 19. array source with cond MATCHES
SELECT * FROM [{ name: 'hello' }, { name: 'world' }] WHERE name @@ 'hello';
-- 20. WITH NOINDEX: single-part and multi-part
SELECT * FROM t WITH NOINDEX WHERE name @@ 'hello';
SELECT * FROM i WITH NOINDEX WHERE t.name @@ 'hello';
-- 21. remote link is NONE on some rows
CREATE i:C SET t = NONE;
SELECT * FROM i WHERE t.name @@ 'hello';
