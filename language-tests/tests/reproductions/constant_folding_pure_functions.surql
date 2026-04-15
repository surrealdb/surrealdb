/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Constant folding should evaluate pure functions (math::floor, math::ceil, string::lowercase, type::int) at plan time, enabling index range access"

# Block: setup data
[[test.results]]
value = "'OK'"

# Test 1: literal bound baseline
[[test.results]]
value = "[{ id: item:3, score: 30 }, { id: item:4, score: 40 }, { id: item:5, score: 50 }, { id: item:6, score: 60 }, { id: item:7, score: 70 }]"

# Test 2: math::floor(20.5) folds to 20
[[test.results]]
value = "[{ id: item:3, score: 30 }, { id: item:4, score: 40 }, { id: item:5, score: 50 }, { id: item:6, score: 60 }, { id: item:7, score: 70 }]"

# Test 3: math::ceil(79.5) folds to 80
[[test.results]]
value = "[{ id: item:1, score: 10 }, { id: item:2, score: 20 }, { id: item:3, score: 30 }, { id: item:4, score: 40 }, { id: item:5, score: 50 }]"

# Test 4: both bounds folded
[[test.results]]
value = "[{ id: item:3, score: 30 }, { id: item:4, score: 40 }, { id: item:5, score: 50 }, { id: item:6, score: 60 }, { id: item:7, score: 70 }]"

# Test 5: type::int('25') folds to 25
[[test.results]]
value = "[{ id: item:3, score: 30 }, { id: item:4, score: 40 }, { id: item:5, score: 50 }]"

# Test 6: string::lowercase('CHERRY') folds to 'cherry'
[[test.results]]
value = "[{ id: item:4, name: 'date' }, { id: item:5, name: 'elderberry' }, { id: item:6, name: 'fig' }]"

# Test 7: nested folding
[[test.results]]
value = "[{ id: item:3, score: 30 }, { id: item:4, score: 40 }, { id: item:5, score: 50 }]"
*/

-- Setup: index on score (numeric) and name (string)
{
    DEFINE TABLE item SCHEMAFULL;
    DEFINE FIELD score ON item TYPE int;
    DEFINE FIELD name ON item TYPE string;
    DEFINE INDEX idx_score ON item FIELDS score;
    DEFINE INDEX idx_name ON item FIELDS name;

    CREATE item:1 SET score = 10, name = "apple";
    CREATE item:2 SET score = 20, name = "banana";
    CREATE item:3 SET score = 30, name = "cherry";
    CREATE item:4 SET score = 40, name = "date";
    CREATE item:5 SET score = 50, name = "elderberry";
    CREATE item:6 SET score = 60, name = "fig";
    CREATE item:7 SET score = 70, name = "grape";
    CREATE item:8 SET score = 80, name = "honeydew";
    CREATE item:9 SET score = 90, name = "indian_fig";
    CREATE item:10 SET score = 100, name = "jackfruit";
    RETURN "OK";
};

-- Test 1: literal bound (baseline)
SELECT id, score FROM item WHERE score > 20 ORDER BY score ASC LIMIT 5;

-- Test 2: math::floor — should fold to score > 20 and use index
SELECT id, score FROM item WHERE score > math::floor(20.5) ORDER BY score ASC LIMIT 5;

-- Test 3: math::ceil — should fold to score < 80 and use index
SELECT id, score FROM item WHERE score < math::ceil(79.5) ORDER BY score ASC LIMIT 5;

-- Test 4: both bounds folded
SELECT id, score FROM item
    WHERE score > math::floor(20.5) AND score < math::ceil(79.5)
    ORDER BY score ASC;

-- Test 5: type::int folding
SELECT id, score FROM item WHERE score > type::int("25") ORDER BY score ASC LIMIT 3;

-- Test 6: string::lowercase folding for string range
SELECT id, name FROM item WHERE name > string::lowercase("CHERRY") ORDER BY name ASC LIMIT 3;

-- Test 7: nested expression folding (math::floor + math::ceil → literal + literal → binary fold)
SELECT id, score FROM item
    WHERE score > math::floor(10.9) + math::ceil(9.1)
    ORDER BY score ASC LIMIT 3;
