/**
[test]
reason = "A nested step visits every position its path selects: children of objects and of arrays of objects, absent children, and the elements of arrays and sets, with set elements merging. The elements of a field whose kind admits elements of any kind take a kind of their own, which a step migrates."

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ arr: [{ b: 2 }, { b: -1, c: 1 }], id: post:1, labels: {0, 1}, o: { b: 1 }, tags: [1, 0] }, { arr: [], id: post:2, o: { b: -1 } }, { id: post:3 }]"

*/
DEFINE TABLE post SCHEMALESS;
DEFINE FIELD o ON post TYPE option<object>;
DEFINE FIELD arr ON post TYPE option<array<object>>;
DEFINE FIELD tags ON post TYPE option<array>;
DEFINE FIELD labels ON post TYPE option<set>;
CREATE post:1 SET o = { b: '1' }, arr = [{ b: '2' }, { c: 1 }], tags = ['1', 'x'], labels = <set>['1', '01', 'z'] RETURN NONE;
CREATE post:2 SET o = {}, arr = [] RETURN NONE;
CREATE post:3 RETURN NONE;
ALTER FIELD tags.* ON post TYPE int MIGRATE EXISTING ROWS AS tags_int USING <int>$value FALLBACK 0;
DEFINE FIELD o.b ON post TYPE int MIGRATE EXISTING ROWS AS o_b_int USING <int>$value FALLBACK -1;
DEFINE FIELD arr.*.b ON post TYPE int MIGRATE EXISTING ROWS AS arr_b_int USING <int>$value FALLBACK -1;
ALTER FIELD labels.* ON post TYPE int MIGRATE EXISTING ROWS AS labels_int USING <int>$value FALLBACK 0;
SELECT * FROM post ORDER BY id;
