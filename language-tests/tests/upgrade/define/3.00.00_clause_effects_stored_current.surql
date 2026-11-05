/**
[env]
imports = ["upgrade/define/3.00.00_clause_effects_stored_import.surql"]

[test]
upgrade = true
version = ">=3.4.0-a"
reason = "A schema clause an older binary stored with an effect the clause must not have raises the schema-clause error where the effect would be performed: a COMPUTED field calling a function redefined to write after the field, and an ASSERT clause calling a function redefined to advance a sequence. Nothing the refused clause would have done is left behind. No released binary refuses these effects, so the case is gated to the current checkout; the data assertions are in the ungated 3.00.00_clause_effects_stored.surql."

[[test.results]]
error = "The COMPUTED clause must not have effects: `CREATE` writes data"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
error = "The ASSERT clause must not have effects: `sequence::nextval` advances state outside the transaction"

[[test.results]]
value = "[q:1]"

*/
-- Reading the computed field reaches the write inside the function.
SELECT c FROM t:1;
-- The refused write left nothing behind.
SELECT count() FROM log GROUP ALL;
-- A row the ASSERT clause would judge is refused where the sequence advances.
CREATE q:2;
-- The refused row was not written.
SELECT VALUE id FROM q;
