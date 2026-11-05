/**
[env]
namespace = true
database = true

[test]
reason = """
A creating statement has no pre-mutation record, so there is nothing for a
`COMPUTED` field to be derived from while its data clause runs: reading one there
yields `NONE`, and it yields `NONE` uniformly whether the record id was named by
the statement, generated for it, or reached through `UPSERT`'s create attempt.

The value is still derived for the record the statement returns — only the data
clause's view of it is empty. The contrast with the updating shapes, where the
same read yields the pre-mutation value, is the point: which shape ran decides
what the data clause can see, and nothing else does.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, dbl: 10, id: b:named }]"

[[test.results]]
value = "[{ a: 5, dbl: 10, id: b:upserted }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[NONE, NONE, NONE]"

[[test.results]]
value = "[{ a: 5, copied: NONE, dbl: 10 }]"

[[test.results]]
value = "[{ a: 5, copied: NONE, dbl: 10 }]"

[[test.results]]
value = "[{ a: 5, copied: 10, dbl: 10, id: b:upserted }]"
*/

DEFINE TABLE b SCHEMALESS;
DEFINE FIELD a   ON b TYPE number;
DEFINE FIELD dbl ON b COMPUTED a * 2;

-- A named record id, a generated one, and UPSERT's create attempt all agree.
CREATE b:named SET a = 5, copied = dbl;
UPSERT b:upserted SET a = 5, copied = dbl;
CREATE b SET a = 5, copied = dbl RETURN NONE;

-- Nothing was stored for `copied`, and `dbl` is still derived on read.
SELECT VALUE copied FROM b;
SELECT a, copied, dbl FROM b:named;
SELECT a, copied, dbl FROM b:upserted;

-- The same statement against an existing record does see the pre-image value.
UPSERT b:upserted SET copied = dbl;
