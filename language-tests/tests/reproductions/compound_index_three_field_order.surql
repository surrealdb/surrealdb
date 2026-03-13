/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Compound index with 3 fields must support equality prefix + range + ORDER BY with LIMIT pushdown"

# Block: setup data
[[test.results]]
value = "'OK'"

# Test 1: 2-field equality prefix, ORDER BY third DESC LIMIT 3
[[test.results]]
value = "[{ category: 'electronics', id: product:1, region: 'US', score: 90 }, { category: 'electronics', id: product:2, region: 'US', score: 70 }, { category: 'electronics', id: product:3, region: 'US', score: 50 }]"

# Test 2: same but ASC
[[test.results]]
value = "[{ category: 'electronics', id: product:5, region: 'US', score: 10 }, { category: 'electronics', id: product:4, region: 'US', score: 30 }, { category: 'electronics', id: product:3, region: 'US', score: 50 }]"

# Test 3: 1-field equality, ORDER BY category, score DESC LIMIT 3
[[test.results]]
value = "[{ category: 'clothing', id: product:6, region: 'US', score: 80 }, { category: 'clothing', id: product:7, region: 'US', score: 40 }, { category: 'electronics', id: product:1, region: 'US', score: 90 }]"

# Test 4: full equality prefix + range score > 30, ASC LIMIT 3
[[test.results]]
value = "[{ category: 'electronics', id: product:3, region: 'US', score: 50 }, { category: 'electronics', id: product:2, region: 'US', score: 70 }, { category: 'electronics', id: product:1, region: 'US', score: 90 }]"
*/

-- Setup: 3-field compound index on (region, category, score)
{
    DEFINE TABLE product SCHEMAFULL;
    DEFINE FIELD region ON product TYPE string;
    DEFINE FIELD category ON product TYPE string;
    DEFINE FIELD score ON product TYPE int;
    DEFINE INDEX idx_region_cat_score ON product FIELDS region, category, score;

    -- Region US, category electronics
    CREATE product:1 SET region = "US", category = "electronics", score = 90;
    CREATE product:2 SET region = "US", category = "electronics", score = 70;
    CREATE product:3 SET region = "US", category = "electronics", score = 50;
    CREATE product:4 SET region = "US", category = "electronics", score = 30;
    CREATE product:5 SET region = "US", category = "electronics", score = 10;

    -- Region US, category clothing
    CREATE product:6 SET region = "US", category = "clothing", score = 80;
    CREATE product:7 SET region = "US", category = "clothing", score = 40;

    -- Region EU, category electronics
    CREATE product:8 SET region = "EU", category = "electronics", score = 85;
    CREATE product:9 SET region = "EU", category = "electronics", score = 60;

    -- Region EU, category food
    CREATE product:10 SET region = "EU", category = "food", score = 95;
    RETURN "OK";
};

-- Test 1: 2-field equality prefix, ORDER BY third field DESC LIMIT 3
SELECT id, region, category, score FROM product
    WHERE region = "US" AND category = "electronics"
    ORDER BY score DESC LIMIT 3;

-- Test 2: same but ASC
SELECT id, region, category, score FROM product
    WHERE region = "US" AND category = "electronics"
    ORDER BY score ASC LIMIT 3;

-- Test 3: 1-field equality prefix, retrieve across categories
SELECT id, region, category, score FROM product
    WHERE region = "US"
    ORDER BY category, score DESC LIMIT 3;

-- Test 4: full equality prefix + range on score
SELECT id, region, category, score FROM product
    WHERE region = "US" AND category = "electronics" AND score > 30
    ORDER BY score ASC LIMIT 3;
