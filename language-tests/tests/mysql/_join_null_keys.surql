/**
[test]
run = false
reason = "The tables behind the join_null_keys and join_null_keys_indexed tests: keys that are NULL, missing or partly NULL on both sides of a join, the right side once without an index and once with one on k"
*/

DEFINE TABLE joinnull_l SCHEMALESS;
DEFINE TABLE joinnull_r SCHEMALESS;

CREATE joinnull_l:1 SET k = 1, k2 = 'a', name = 'l1' RETURN NONE;
CREATE joinnull_l:2 SET k = 2, k2 = 'a', name = 'l2' RETURN NONE;
CREATE joinnull_l:3 SET k = NULL, k2 = 'a', name = 'lnull' RETURN NONE;
CREATE joinnull_l:4 SET k2 = 'a', name = 'lnone' RETURN NONE;
CREATE joinnull_l:5 SET k = 3, k2 = NULL, name = 'lpartnull' RETURN NONE;
CREATE joinnull_l:6 SET k = 3, name = 'lpartnone' RETURN NONE;

CREATE joinnull_r:1 SET k = 1, k2 = 'a', name = 'r1' RETURN NONE;
CREATE joinnull_r:2 SET k = NULL, k2 = 'a', name = 'rnull' RETURN NONE;
CREATE joinnull_r:3 SET k2 = 'a', name = 'rnone' RETURN NONE;
CREATE joinnull_r:4 SET k = NULL, k2 = 'a', name = 'rnull2' RETURN NONE;
CREATE joinnull_r:5 SET k = 3, k2 = NULL, name = 'rpartnull' RETURN NONE;
CREATE joinnull_r:6 SET k = 3, name = 'rpartnone' RETURN NONE;
CREATE joinnull_r:7 SET k = 4, k2 = 'z', name = 'runmatched' RETURN NONE;

DEFINE TABLE joinnull_ri SCHEMALESS;
DEFINE INDEX joinnull_ri_k ON joinnull_ri FIELDS k;

CREATE joinnull_ri:1 SET k = 1, k2 = 'a', name = 'r1' RETURN NONE;
CREATE joinnull_ri:2 SET k = NULL, k2 = 'a', name = 'rnull' RETURN NONE;
CREATE joinnull_ri:3 SET k2 = 'a', name = 'rnone' RETURN NONE;
CREATE joinnull_ri:4 SET k = NULL, k2 = 'a', name = 'rnull2' RETURN NONE;
CREATE joinnull_ri:5 SET k = 3, k2 = NULL, name = 'rpartnull' RETURN NONE;
CREATE joinnull_ri:6 SET k = 3, name = 'rpartnone' RETURN NONE;
CREATE joinnull_ri:7 SET k = 4, k2 = 'z', name = 'runmatched' RETURN NONE;
