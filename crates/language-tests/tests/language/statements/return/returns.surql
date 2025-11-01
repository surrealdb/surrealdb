/**

[test]

[[test.results]]
# Option should return NONE.
value = "NONE"

[[test.results]]
# RETURN should return a value.
value = "3"

[[test.results]]
# Expression should return its value.
value = "3"

[[test.results]]
# Block should return the last expression.
value = "7"

[[test.results]]
# Function definition returns NONE.
value = "NONE"

[[test.results]]
# Function should return the last expression.
value = "7"

[[test.results]]
# Transaction should return a value for each statement.
# BEGIN;
value = "NONE"

[[test.results]]
# DEFINE TABLE test;
value = "NONE"

[[test.results]]
# UPSERT test:name SET name = 'Test';
value = "[{ id: test:name, name: 'Test' }]"

[[test.results]]
# RETURN test:name;
value = "test:name"

[[test.results]]
# COMMIT;
value = "NONE"

[[test.results]]
# Block exits at the first RETURN statement.
value = "5"

*/

# Option should return NONE.
OPTION IMPORT = false;

# RETURN should return a value.
RETURN 1 + 2;

# Expression should return its value.
1 + 2;

# Block should return the last expression.
{
    1 + 2;
    3 + 4;
};

# Function should return the last expression.
DEFINE FUNCTION fn::test() {
    1 + 2;
    3 + 4;
};
fn::test();


# Transaction should return a value for each statement.
BEGIN;
DEFINE TABLE test;
UPSERT test:name SET name = 'Test';
RETURN test:name;
COMMIT;

# Block exits at the first RETURN statement.
{
    1 + 2;
    3 + 4;
    RETURN 5;

    6 + 7;
    RETURN 8;
}

