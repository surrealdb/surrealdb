/**
[test]

[[test.results]]
value = "'OK'"


[[test.results]]
value = "['C', 'B', 'A']"

[[test.results]]
value = "[NONE, NONE, NONE]"

[[test.results]]
value = "[2, 2, 1]"

*/

{
    CREATE person:a SET name = 'A', age = 1;
    CREATE person:b SET name = 'B', age = 2;
    CREATE person:c SET name = 'C', age = 2;
    RETURN "OK";
};

SELECT VALUE name FROM person ORDER BY name DESC;
SELECT VALUE name OMIT name FROM person ORDER BY name DESC;
SELECT VALUE age OMIT name FROM person ORDER BY name DESC;
