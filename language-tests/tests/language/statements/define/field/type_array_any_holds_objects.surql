/**
[test]
reason = "A field whose kind holds `any` where its interior is, as `array`, `array<any>`, `array<array<any>>`, `set<any>` and `option<array<any>>` do, admits whatever the values in that place hold on a SCHEMAFULL table, as `TYPE any` does; `array<object>` still refuses the keys no definition names"

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
value = "[{ anys: [{ b: { c: [2] } }], bag: {{ e: 'f' },}, grid: [[{ d: true }]], id: t:1, maybe: [{ g: [{ h: 1 }] }], plain: [{ a: 1 }] }]"

[[test.results]]
value = "[{ anys: [{ b: { c: [2] } }], bag: {{ e: 'f' },}, grid: [[{ d: true }]], id: t:1, maybe: [{ g: [{ h: 1 }] }], plain: [{ a: 1 }] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'objs[0].g', but no such field exists for table 'o'"

*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD plain ON t TYPE array;
DEFINE FIELD anys ON t TYPE array<any>;
DEFINE FIELD grid ON t TYPE array<array<any>>;
DEFINE FIELD bag ON t TYPE set<any>;
DEFINE FIELD maybe ON t TYPE option<array<any>>;
CREATE t:1 SET plain = [{ a: 1 }], anys = [{ b: { c: [2] } }], grid = [[{ d: true }]], bag = [{ e: 'f' }], maybe = [{ g: [{ h: 1 }] }];
SELECT * FROM t;
DEFINE TABLE o SCHEMAFULL;
DEFINE FIELD objs ON o TYPE array<object>;
CREATE o:1 SET objs = [{ g: 1 }];
