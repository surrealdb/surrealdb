/**
[test]
reason = "Tests `natural order` or the order that record-id's are naturally sorted in"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: test:-1000000 }, { id: test:-1 }, { id: test:1 }, { id: test:1000000 }, { id: test:⟨-100000000000000000000⟩ }, { id: test:⟨100000000000000000000⟩ }, { id: test:A }, { id: test:Z }, { id: test:a }, { id: test:z }, { id: test:u'00000000-0000-0000-0000-000000000000' }, { id: test:u'ffffffff-ffff-ffff-ffff-ffffffffffff' }, { id: test:[NONE] }, { id: test:[NULL] }, { id: test:[1, 2, 3] }, { id: test:['a'] }, { id: test:{ a: NONE } }, { id: test:{ a: NULL } }, { id: test:{ a: 1 } }, { id: test:{ a: 'a' } }, { id: test:{ z: NONE } }, { id: test:{ z: NULL } }, { id: test:{ z: 1 } }, { id: test:{ z: 'a' } }]"

*/

BEGIN;

create test:1;
create test:1000000;
create test:A;
create test:Z;
create test:a;
create test:z;
create test:-1;
create test:-1000000;
create test:-100000000000000000000;
create test:100000000000000000000;
create test:[1,2,3];
create test:["a"];
create test:[NULL];
create test:[NONE];
create test:{a: 1};
create test:{a: "a"};
create test:{a: NULL};
create test:{a: NONE};
create test:{z: 1};
create test:{z: "a"};
create test:{z: NULL};
create test:{z: NONE};
create test:u"00000000-0000-0000-0000-000000000000";
create test:u"ffffffff-ffff-ffff-ffff-ffffffffffff";

RETURN "OK";
COMMIT;

SELECT * FROM test;
