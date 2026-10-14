/**
[env]
imports = ["upgrade/define/3.00.00_clause_effects_stored_import.surql"]

[test]
upgrade = true
reason = "A schema clause an older binary stored with an effect the clause must not have is kept, and the effect raises the schema-clause error where it would be performed: a COMPUTED field calling a function redefined to write after the field, and an ASSERT clause calling a function redefined to advance a sequence. Nothing the refused clause would have done is left behind, and the rows already written stay readable."

[[test.results]]
value = "true"

[[test.results]]
error = "The COMPUTED clause must not have effects: `CREATE` writes data"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
error = "The ASSERT clause must not have effects: `sequence::nextval` advances state outside the transaction"

[[test.results]]
value = "[q:1]"

*/
-- The definition is kept as it was stored.
RETURN string::contains((INFO FOR TABLE t).fields.c, 'COMPUTED fn::log_write()');
-- Reading the computed field reaches the write inside the function.
SELECT c FROM t:1;
-- The refused write left nothing behind.
SELECT count() FROM log GROUP ALL;
-- A row the ASSERT clause would judge is refused where the sequence advances.
CREATE q:2;
-- The rows already written stay readable.
SELECT VALUE id FROM q;
