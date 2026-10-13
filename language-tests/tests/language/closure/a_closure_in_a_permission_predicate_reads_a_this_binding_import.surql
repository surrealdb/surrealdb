/**
[test]
reason = "Setup for a_closure_in_a_permission_predicate_reads_a_this_binding: a record user and two tables, each with a select permission predicate holding a closure or its closure-free twin, and two rows. Runs as owner."
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNUP (CREATE user) SIGNIN (SELECT * FROM user) DURATION FOR SESSION 1h;
CREATE user:test;
DEFINE TABLE pt_this PERMISSIONS FOR select WHERE { LET $this = { lim: 3 }; (|$e| $this.lim)(0) } = 3;
DEFINE TABLE pt_this_cf PERMISSIONS FOR select WHERE { LET $this = { lim: 3 }; $this.lim } = 3;
CREATE pt_this:1 SET lim = 1;
CREATE pt_this:2 SET lim = 8;
CREATE pt_this_cf:1 SET lim = 1;
CREATE pt_this_cf:2 SET lim = 8;
