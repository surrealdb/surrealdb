/**

[test]

[test.results]
parsing-error = """
Missing group idiom `age` in statement selection
  --> [28:63]
   |
28 | ... GROUP BY age;
   |              ^^^
  --> [28:8]
   |
28 | SELECT array::first(name) AS name_by_age FROM person GROUP BY age;
   |        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Idiom missing here
"""


*/

{
    CREATE person:1 SET name = "John", age = 20;
    CREATE person:2 SET name = "Jane", age = 20;
    CREATE person:3 SET name = "Jim", age = 21;
    RETURN "OK";
};

SELECT array::first(name) AS name_by_age FROM person GROUP BY age;
