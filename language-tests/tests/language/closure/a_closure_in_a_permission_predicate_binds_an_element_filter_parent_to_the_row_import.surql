/**
[test]
reason = "Setup for a_closure_in_a_permission_predicate_binds_an_element_filter_parent_to_the_row: a record user and two tables, each with a select permission predicate holding a closure over an element filter reading `$parent` or its `LET` twin, and two rows. Runs as owner."
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNUP (CREATE user) SIGNIN (SELECT * FROM user) DURATION FOR SESSION 1h;
CREATE user:test;
DEFINE TABLE pt_parent PERMISSIONS FOR select WHERE (|$parent| [5, 10][WHERE $this > $parent.lim])({ lim: 6 }) = [10];
DEFINE TABLE pt_parent_cf PERMISSIONS FOR select WHERE { LET $parent = { lim: 6 }; [5, 10][WHERE $this > $parent.lim] } = [10];
CREATE pt_parent:1 SET lim = 1;
CREATE pt_parent:2 SET lim = 8;
CREATE pt_parent_cf:1 SET lim = 1;
CREATE pt_parent_cf:2 SET lim = 8;
