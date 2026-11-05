/**
[test]
reason = "A COUNT index whose WHERE guard reads a field is an index that reads it: removing the field, or making it COMPUTED, is refused on an enforced table with rows until the index is removed, because the entries the index holds were judged on the values the removal clears. A table that is not enforced, or has no rows, takes the removal as it always did."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change field `a` of table `tb`: the index `cnt_five` reads its values, which the removal clears: remove the index first"

[[test.results]]
error = "Cannot change field `a` of table `tb`: the index `cnt_five` reads its values, which the removal clears: remove the index first"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = '5' RETURN NONE;
CREATE tb:2 SET a = '6' RETURN NONE;
DEFINE INDEX cnt_five ON tb COUNT WHERE a = '5';
-- Both removals clear the values the guard judged.
REMOVE FIELD a ON tb;
DEFINE FIELD OVERWRITE a ON tb COMPUTED 'x';
-- The definition and the count are as they were.
SELECT count() FROM tb WHERE a = '5' GROUP ALL;
-- The way the refusal asks for.
REMOVE INDEX cnt_five ON tb;
REMOVE FIELD a ON tb;
REMOVE TABLE tb;
