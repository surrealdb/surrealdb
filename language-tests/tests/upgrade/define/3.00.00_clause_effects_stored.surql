/**
[env]
imports = ["upgrade/define/3.00.00_clause_effects_stored_import.surql"]

[test]
upgrade = true
reason = "A schema clause an older binary stored with an effect the clause must not have is kept, and the rows already written under it stay readable: a COMPUTED field calling a function redefined to write after the field, and an ASSERT clause calling a function redefined to advance a sequence. Where the effect raises the schema-clause error is asserted on the current checkout by 3.00.00_clause_effects_stored_current.surql."

[[test.results]]
value = "true"

[[test.results]]
value = "[q:1]"

*/
-- The definition is kept as it was stored.
RETURN string::contains((INFO FOR TABLE t).fields.c, 'COMPUTED fn::log_write()');
-- The rows already written stay readable.
SELECT VALUE id FROM q;
