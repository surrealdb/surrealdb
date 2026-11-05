/**
[test]
reason = "Chain setup for table_schemafull.surql: defines the SCHEMAFULL table and its typed fields on the first walked version. SCHEMAFULL tables and field-level TYPE/ASSERT/DEFAULT exist in every 3.x release, so this runs at the head of the chain."
run = false
*/
DEFINE TABLE chain_table_schemafull SCHEMAFULL;
DEFINE FIELD name ON chain_table_schemafull TYPE string ASSERT $value != NONE;
DEFINE FIELD age ON chain_table_schemafull TYPE int ASSERT $value >= 0 AND $value < 200;
DEFINE FIELD email ON chain_table_schemafull TYPE string ASSERT string::is_email($value);
DEFINE FIELD tags ON chain_table_schemafull TYPE array<string> DEFAULT [];
DEFINE FIELD hop ON chain_table_schemafull TYPE int;
DEFINE FIELD wrote_at ON chain_table_schemafull TYPE string;
