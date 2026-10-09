/**
[test]
run = false
reason = "Setup only: indexes on every field the joins of native_joins_indexed.pgsql pair on, over the tables of _native.surql."
*/

DEFINE INDEX dept_did ON dept FIELDS did;
DEFINE INDEX proj_emp ON proj FIELDS emp_id;
DEFINE INDEX proj_pid ON proj FIELDS pid;
DEFINE INDEX tag_k ON tag FIELDS k;
DEFINE INDEX empty_did ON empty FIELDS did;
