



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

EXPLAIN FORMAT JSON SELECT * FROM (SELECT name FROM person WHERE age > 1) WHERE name = 'A';