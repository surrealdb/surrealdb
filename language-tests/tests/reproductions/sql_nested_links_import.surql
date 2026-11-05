/**
[test]
run = false
*/
DEFINE TABLE nested_staff SCHEMALESS;
DEFINE FIELD dept ON nested_staff TYPE option<record<nested_dept>>;
CREATE nested_dept:1;
CREATE nested_dept:2;
CREATE nested_staff:1 SET dept = nested_dept:1;
CREATE nested_staff:2 SET dept = nested_dept:2;
CREATE nested_staff:3;
