/**

[test]
wip = true
reason = "OMIT does not work as expected with group by. It applies to the records not the group by results."

# 0: Setup
[[test.results]]
value = "'OK'"

# 1, 2: Test OMIT with group by
[[test.results]]
value = "[{ age: 1, names: 'A' }, { age: 2, names: 'B, C' }]"
[[test.results]]
value = "[{ names: 'A' }, { names: 'B, C' }]"

# 3, 4: OMIT applies to SELECT, not the GROUP BY
[[test.results]]
value = "[{ age: 1, names: 'A' }, { age: 2, names: 'B, C' }]"
[[test.results]]
value = "[{ age: 1 }, { age: 2 }]"

*/

// 0: Setup
{
    CREATE person:a SET name = 'A', age = 1;
    CREATE person:b SET name = 'B', age = 2;
    CREATE person:c SET name = 'C', age = 2;
    RETURN "OK";
};

// 1, 2: Test OMIT with group by
SELECT age, array::join(name, ', ') as names FROM person GROUP BY age;
SELECT age, array::join(name, ', ') as names OMIT age FROM person GROUP BY age;

// 3, 4: OMIT applies to SELECT, not the GROUP BY
SELECT age, array::join(name, ', ') as names OMIT name FROM person GROUP BY age;
SELECT age, array::join(name, ', ') as names OMIT names FROM person GROUP BY age;
