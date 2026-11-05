/**
[env]
imports = ["upgrade/define/3.00.00_index_btree_decimal_spelling_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
An index an older binary built keeps the spelling it was written with, and this
checkout must keep maintaining it that way without a rebuild: rewriting or
deleting a record whose decimal is stored as 2.50 removes the entry at the key
the old binary wrote it under. A delete that looked for the respelled key would
miss it and leave an entry that a read of the computed spelling still finds, for
a record that no longer holds the value. No repair is declared, so the reads are
of an index nobody rebuilt.

The upgrade target is bounded to the current checkout, as its siblings are."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 3.4dec, id: dec_idx:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[dec_idx:4]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[dec_idx:1]"

[[test.results]]
value = "[true, true, true]"

*/
LET $computed = 1.25dec * 2;
UPDATE dec_idx:1 SET c = 3.4dec;
DELETE dec_idx:2;
DELETE dec_uniq:1;
-- No entry is left at either spelling of the value the records no longer hold.
SELECT VALUE id FROM dec_idx WITH INDEX dec_idx_c WHERE c = $computed;
SELECT VALUE id FROM dec_idx WITH INDEX dec_idx_c WHERE c >= 2dec AND c <= 2.5dec ORDER BY id;
SELECT VALUE id FROM dec_uniq WITH INDEX dec_uniq_c WHERE c = $computed;
SELECT VALUE id FROM dec_uniq WITH INDEX dec_uniq_c WHERE c >= 2dec AND c <= 2.5dec;
-- The record that holds the value now is found, and the values are decimals.
SELECT VALUE id FROM dec_idx WITH INDEX dec_idx_c WHERE c = 3.4dec;
SELECT VALUE type::is_decimal(c) FROM dec_idx ORDER BY id;
