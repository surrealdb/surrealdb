/**
[test]
reason = "Setup for 3.00.00_clause_effects_stored.surql and 3.00.00_clause_effects_stored_current.surql: the old binary stores a COMPUTED field whose stored function writes and an ASSERT clause whose stored function advances a sequence. Each function only reads when its field is defined and is redefined afterwards, so every binary in the matrix, and the current one, admits the definitions."
run = false
*/
DEFINE TABLE log SCHEMALESS;
DEFINE FUNCTION fn::log_write() { RETURN 1; };
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD c ON t COMPUTED fn::log_write();
CREATE t:1;
DEFINE FUNCTION OVERWRITE fn::log_write() { CREATE log:computed; RETURN 1; };
DEFINE SEQUENCE s;
DEFINE TABLE q SCHEMALESS;
DEFINE FUNCTION fn::next() { RETURN 0; };
DEFINE FIELD n ON q DEFAULT 0 ASSERT fn::next() >= 0;
CREATE q:1;
DEFINE FUNCTION OVERWRITE fn::next() { RETURN sequence::nextval('s'); };
