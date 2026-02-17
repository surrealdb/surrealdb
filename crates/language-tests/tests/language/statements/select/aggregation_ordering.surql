/**
[test]

[[test.results]]
value = "[{ a: 'a', b: 1, id: thing:1 } ]"

[[test.results]]
value = "[{ a: 'a', b: 1, id: thing:2 } ]"

[[test.results]]
value = "[{ a: 'a', b: 1, id: thing:3 } ]"

[[test.results]]
value = "[{ a: 'b', b: 1, id: thing:4 } ]"

[[test.results]]
value = "[{ a: 'b', b: 1, id: thing:5 } ]"

[[test.results]]
value = "[{ a: 'b', b: 1, id: thing:6 } ]"

[[test.results]]
value = "[{ a: 'b', b: 1, id: thing:7 } ]"

[[test.results]]
value = "[{ a: 'b', b: 1, id: thing:8 } ]"

[[test.results]]
value = "[{ a: 'c', b: 1, id: thing:9 } ]"

[[test.results]]
value = "[{ a: 'b', count: 5 }, { a: 'a', count: 3 },{ a: 'c', count: 1 }]"

[[test.results]]
value = "[{ a: 'b', count: 5 }, { a: 'a', count: 3 },{ a: 'c', count: 1 }]"

*/
CREATE thing:1 CONTENT {"a":"a","b":1};
CREATE thing:2 CONTENT {"a":"a","b":1};
CREATE thing:3 CONTENT {"a":"a","b":1};
CREATE thing:4 CONTENT {"a":"b","b":1};
CREATE thing:5 CONTENT {"a":"b","b":1};
CREATE thing:6 CONTENT {"a":"b","b":1};
CREATE thing:7 CONTENT {"a":"b","b":1};
CREATE thing:8 CONTENT {"a":"b","b":1};
CREATE thing:9 CONTENT {"a":"c","b":1};
SELECT * FROM (SELECT a,count() as count FROM thing GROUP BY a) ORDER BY count DESC;
SELECT a,count() as count FROM thing GROUP BY a ORDER BY count DESC;

