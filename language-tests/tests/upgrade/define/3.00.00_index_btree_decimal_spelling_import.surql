/**
[test]
reason = """
Setup for 3.00.00_index_btree_decimal_spelling.surql and its _write and _current
siblings: the old binary indexes decimals that are equal and spelled differently.
`1.25dec * 2` is stored as 2.50, where the literal `2.5dec` is 2.5, and the old
binary writes each at its own key. The UNIQUE index holds only the computed
spelling of the pair, so a rebuild over it can succeed."""
run = false
*/
DEFINE TABLE dec_idx SCHEMALESS;
DEFINE INDEX dec_idx_c ON dec_idx FIELDS c;
CREATE dec_idx:1 SET c = 1.25dec * 2;
CREATE dec_idx:2 SET c = 2.5dec;
CREATE dec_idx:3 SET c = 2.6dec;
CREATE dec_idx:4 SET c = 2.4dec;

DEFINE TABLE dec_uniq SCHEMALESS;
DEFINE INDEX dec_uniq_c ON dec_uniq FIELDS c UNIQUE;
CREATE dec_uniq:1 SET c = 1.25dec * 2;
CREATE dec_uniq:2 SET c = 3dec;
