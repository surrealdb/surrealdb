/**
[test]
reason = """
Setup for 3.00.00_index_btree_identity_entry_ids.surql: the old binary indexes
two records whose ids differ only in a nested number's kind, under one value.
It spells an entry's record id without the number's kind, so the two share one
entry, which names the record written last."""
run = false
*/
DEFINE TABLE ent SCHEMALESS;
DEFINE INDEX ent_body ON ent FIELDS body;
CREATE ent:[1] SET body = 'hello';
CREATE ent:[1dec] SET body = 'hello';
CREATE ent:[2] SET body = 'other';
