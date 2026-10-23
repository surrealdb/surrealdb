/**
[test]
reason = "Setup for id_key_join_literals.pgsql: one row to join, and a table whose integer keys 0, 1, 3 and -3 sit beside string keys that spell a number or a boolean."
run = false
*/

DEFINE TABLE a SCHEMALESS;
CREATE a:1 SET label = 'only' RETURN NONE;

DEFINE TABLE emp SCHEMALESS;
CREATE emp:0 SET name = 'int zero' RETURN NONE;
CREATE emp:1 SET name = 'int one' RETURN NONE;
CREATE emp:3 SET name = 'int three' RETURN NONE;
CREATE emp:-3 SET name = 'int minus three' RETURN NONE;
CREATE emp:`3` SET name = 'text three' RETURN NONE;
CREATE emp:`1.5` SET name = 'text 1.5' RETURN NONE;
CREATE emp:`true` SET name = 'text true' RETURN NONE;
