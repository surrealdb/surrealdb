/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 17, can_drive: false, id: person:a, name: 'A' }]"

[[test.results]]
value = "[{ age: 17, can_drive: false, id: person:a, name: 'A' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 17, can_drive: false, id: person:a, name: 'A' }]"


*/

{
    DEFINE TABLE person SCHEMALESS;
    DEFINE FIELD can_drive ON person TYPE bool COMPUTED age >= 18;
    CREATE person:a SET name = 'A', age = 17;
    CREATE person:b SET name = 'B', age = 18;
    CREATE person:c SET name = 'C', age = 19;
    
    RELATE person:a->knows->person:b;
    RELATE person:b->knows->person:c;

    CREATE test:a SET a = [2, 1], b = [3, 4];

    RETURN "OK";
};

type::field('bob');

SELECT * FROM person LIMIT 1;
SELECT * FROM type::table('person') LIMIT 1;

LET $param = |$n: string| type::table('perso' + $n);

SELECT * FROM $param('n') LIMIT 1;
