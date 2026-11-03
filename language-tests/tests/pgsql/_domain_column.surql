/**
[test]
run = false
reason = "Setup only: a domain and a table with a column of it, read by staged_drop_column_frees_domain and staged_refused_drop_keeps_domain"
*/

DEFINE TYPE pos AS i32 CONSTRAINT pos_check ASSERT $value > 0 SQL TYPE POSTGRESQL 'integer';
DEFINE TABLE held SCHEMAFULL;
DEFINE FIELD id ON held TYPE int SQL TYPE POSTGRESQL 'integer';
DEFINE FIELD x ON held TYPE pos;
