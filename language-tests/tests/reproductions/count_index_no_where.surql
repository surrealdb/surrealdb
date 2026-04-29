/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "CountScan should use DEFINE INDEX ... COUNT (unconditional) for O(delta) counting"

# Setup
[[test.results]]
value = "None"

# COUNT should return 0 on empty table
[[test.results]]
value = "[{ count: 0 }]"

# Insert 5 records
[[test.results]]
value = "None"

# COUNT should return 5
[[test.results]]
value = "[{ count: 5 }]"

# Delete 2 records
[[test.results]]
value = "None"

# COUNT should return 3 (delta tracking: +5 -2 = 3)
[[test.results]]
value = "[{ count: 3 }]"
*/

-- Define a table with an unconditional COUNT index
{
    DEFINE TABLE counter_test SCHEMAFULL;
    DEFINE FIELD val ON counter_test TYPE int;
    DEFINE INDEX idx_count ON counter_test COUNT;
    RETURN NONE;
};

-- COUNT should return 0 on empty table
SELECT count() FROM counter_test GROUP ALL;

-- Insert 5 records
{
    CREATE counter_test:1 SET val = 1;
    CREATE counter_test:2 SET val = 2;
    CREATE counter_test:3 SET val = 3;
    CREATE counter_test:4 SET val = 4;
    CREATE counter_test:5 SET val = 5;
    RETURN NONE;
};

-- COUNT should return 5
SELECT count() FROM counter_test GROUP ALL;

-- Delete 2 records
{
    DELETE counter_test:1;
    DELETE counter_test:3;
    RETURN NONE;
};

-- COUNT should return 3 (delta tracking: +5 -2 = 3)
SELECT count() FROM counter_test GROUP ALL;
