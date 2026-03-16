/**[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ name: 'A' }, { name: 'B' }, { name: 'C' }]"

[[test.results]]
value = "['A', 'B', 'C']"

[[test.results]]
value = "{ name: 'A' }"

[[test.results]]
value = "'A'"

[[test.results]]
value = "[{ age: 1 }, { age: 2 }]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "['a', 'b', 'c']"

[[test.results]]
value = "[['A', 'B', 'C'], ['A', 'B', 'C'], ['A', 'B', 'C']]"


*/

{
    CREATE person:a SET name = 'A', age = 1;
    CREATE person:b SET name = 'B', age = 2;
    CREATE person:c SET name = 'C', age = 2;
    RETURN "OK";
};

// 1: Basic value select
SELECT name FROM person;
SELECT VALUE name FROM person;

// 2: VALUE with ONLY
SELECT name FROM ONLY person LIMIT 1;
SELECT VALUE name FROM ONLY person LIMIT 1;

// 3: VALUE in GROUP BY
SELECT age FROM person GROUP BY age;
SELECT VALUE age FROM person GROUP BY age;

// 4: VALUE with functions
SELECT VALUE string::lowercase(name) FROM person;

// 5: VALUE subquery
SELECT VALUE (SELECT VALUE name FROM person) FROM person;
