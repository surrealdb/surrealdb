/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ test: 'DEFINE INDEX test ON test FIELDS a.b, a, b, c' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"

*/
LET $name = "test";
LET $table = "test";
LET $single_field = "a";
LET $multiple_fields = ["b", "c"];
LET $comment = "test comment";

DEFINE INDEX $name ON $table FIELDS a.b, type::field($single_field), type::fields($multiple_fields);
(INFO FOR TABLE $table).indexes;

REMOVE INDEX $name ON $table;
(INFO FOR TABLE $table).indexes;