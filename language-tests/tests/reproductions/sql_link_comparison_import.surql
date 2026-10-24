/**
[test]
run = false
*/

DEFINE TABLE link_cmp SCHEMALESS;
DEFINE FIELD dept ON link_cmp TYPE option<record<dept>>;
CREATE link_cmp:1 SET dept = dept:1;
CREATE link_cmp:2 SET dept = dept:2;
CREATE link_cmp:3;
