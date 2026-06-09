/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: child:c1 }]"

[[test.results]]
value = "[{ child_ref: child:c1, id: parent:p1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[parent:p1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/
DEFINE FIELD child_ref ON parent TYPE record<child>;
CREATE child:c1;
CREATE parent:p1 SET child_ref = child:c1;
ALTER FIELD child_ref ON parent REFERENCE ON DELETE IGNORE;
RETURN child:c1<~(parent FIELD child_ref);
ALTER FIELD child_ref ON parent DROP REFERENCE;
RETURN child:c1<~(parent FIELD child_ref);
