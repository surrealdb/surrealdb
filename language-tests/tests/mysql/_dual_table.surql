/**
[test]
run = false
reason = "Setup only: a table named dual, which from_dual.mysql reads through a quoted name"
*/

CREATE dual:1 SET label = 'real' RETURN NONE;
