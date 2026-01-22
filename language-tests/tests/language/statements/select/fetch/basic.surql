/**
[test]
reason = "Test basic FETCH clause operations with single and multiple fields"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ author: author:tobie, content: 'Hello World', id: post:1, title: 'First Post' }]"

[[test.results]]
value = "[{ content: 'Hello World', id: post:1, title: 'First Post', author: { id: author:tobie, name: 'Tobie', email: 'tobie@example.com', active: true } }]"

[[test.results]]
value = "[{ content: 'Hello World', id: post:1, title: 'First Post', author: { id: author:tobie, name: 'Tobie', email: 'tobie@example.com', active: true } }]"

[[test.results]]
value = "[{ author: author:tobie, content: 'More content', id: post:3, reviewer: { id: reviewer:alice, name: 'Alice', role: 'senior', years: 5 }, title: 'Third Post' }]"

[[test.results]]
value = "[{ author: { active: true, email: 'tobie@example.com', id: author:tobie, name: 'Tobie' }, content: 'More content', id: post:3, reviewer: { id: reviewer:alice, name: 'Alice', role: 'senior', years: 5 }, title: 'Third Post' }]"

*/

{
    -- Setup test data with related records
    CREATE author:tobie SET name = 'Tobie', email = 'tobie@example.com', active = true;
    CREATE author:jaime SET name = 'Jaime', email = 'jaime@example.com', active = true;
    CREATE reviewer:alice SET name = 'Alice', role = 'senior', years = 5;
    CREATE reviewer:bob SET name = 'Bob', role = 'junior', years = 2;

    -- Create posts with author references
    CREATE post:1 SET title = 'First Post', content = 'Hello World', author = author:tobie;
    CREATE post:2 SET title = 'Second Post', content = 'Another post', author = author:jaime;
    CREATE post:3 SET title = 'Third Post', content = 'More content', author = author:tobie, reviewer = reviewer:alice;
    CREATE post:4 SET title = 'Fourth Post', content = 'Final post', author = author:jaime, reviewer = reviewer:bob;
    RETURN "OK";
}; 

-- 1: Basic SELECT without FETCH (should show record IDs)
SELECT * FROM post:1;

-- 2: FETCH single field - replace author record ID with full record
SELECT * FROM post:1 FETCH author;

-- 3: FETCH multiple fields - reviewer doesn't exist for this post
SELECT * FROM post:1 FETCH author, reviewer;

-- 4: FETCH reviewer - reviewer exists for this post
SELECT * FROM post:3 FETCH reviewer;

-- 5: FETCH multiple fields - author and reviewer exist for this post
SELECT * FROM post:3 FETCH author, reviewer;
