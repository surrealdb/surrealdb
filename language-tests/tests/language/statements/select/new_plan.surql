/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ name: 'A' }, { name: 'B' }, { name: 'C' }]"

[[test.results]]
value = "[{ name: 'A' }]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[{ name: 'B' }, { name: 'C' }]"

[[test.results]]
value = "[{ age: 2, id: person:b, knows: [[person:c]], name: 'B' }, { age: 2, id: person:c, knows: [], name: 'C' }]"

[[test.results]]
value = "{ age: person:b, id: person:a }"

*/

{
    CREATE person:a SET name = 'A', age = 1;
    CREATE person:b SET name = 'B', age = 2;
    CREATE person:c SET name = 'C', age = 2;
    
    RELATE person:a->knows->person:b;
    RELATE person:b->knows->person:c;
    RETURN "OK";
};

SELECT name FROM person;
SELECT name FROM person:a;
SELECT * FROM [1, 2, 3];
SELECT name FROM [{ name: 'A', age: 1 }, { name: 'B', age: 2 }, { name: 'C', age: 3 }] WHERE age > 1;

SELECT *, ->knows->person AS knows FROM person WHERE age > 1;

{ id: person:a, age: person:b }.*;
