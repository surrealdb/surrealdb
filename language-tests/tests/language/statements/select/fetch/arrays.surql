/**
[test]
reason = "Test FETCH clause with arrays of record IDs and nested array access"

# 0: Setup
[[test.results]]
value = "'OK'"

# 1: SELECT without FETCH (should show array of record IDs)
[[test.results]]
value = """[\
    { categories: [category:language, category:technology], id: post:p1, tags: [tag:rust, tag:database], title: 'Getting Started with Rust' },\
    { categories: [category:language], id: post:p2, tags: [tag:javascript, tag:web], title: 'JavaScript Fundamentals' },\
    { categories: [], id: post:p3, tags: [], title: 'Empty Arrays Test' }\
]"""

# 2: FETCH array field - replace tag IDs with full records
[[test.results]]
value = """[\
    { categories: [category:language, category:technology], id: post:p1, tags: [{ category: 'language', id: tag:rust, name: 'Rust', popularity: 95 }, { category: 'technology', id: tag:database, name: 'Database', popularity: 90 }], title: 'Getting Started with Rust' },\
    { categories: [category:language], id: post:p2, tags: [{ category: 'language', id: tag:javascript, name: 'JavaScript', popularity: 98 }, { category: 'technology', id: tag:web, name: 'Web', popularity: 92 }], title: 'JavaScript Fundamentals' },\
    { categories: [], id: post:p3, tags: [], title: 'Empty Arrays Test' }\
]"""

# 3: FETCH multiple array fields
[[test.results]]
value = """[\
    { categories: [{ description: 'Languages for coding', id: category:language, name: 'Programming Languages' }, { description: 'Tech topics', id: category:technology, name: 'Technology' }], id: post:p1, tags: [{ category: 'language', id: tag:rust, name: 'Rust', popularity: 95 }, { category: 'technology', id: tag:database, name: 'Database', popularity: 90 }], title: 'Getting Started with Rust' },\
    { categories: [{ description: 'Languages for coding', id: category:language, name: 'Programming Languages' }], id: post:p2, tags: [{ category: 'language', id: tag:javascript, name: 'JavaScript', popularity: 98 }, { category: 'technology', id: tag:web, name: 'Web', popularity: 92 }], title: 'JavaScript Fundamentals' },\
    { categories: [], id: post:p3, tags: [], title: 'Empty Arrays Test' }\
]"""

*/

{
    -- Setup tags and categories
    CREATE tag:rust SET name = 'Rust', category = 'language', popularity = 95;
    CREATE tag:javascript SET name = 'JavaScript', category = 'language', popularity = 98;
    CREATE tag:python SET name = 'Python', category = 'language', popularity = 97;
    CREATE tag:database SET name = 'Database', category = 'technology', popularity = 90;
    CREATE tag:web SET name = 'Web', category = 'technology', popularity = 92;

    CREATE category:language SET name = 'Programming Languages', description = 'Languages for coding';
    CREATE category:technology SET name = 'Technology', description = 'Tech topics';

    -- Create posts with arrays of tag references
    CREATE post:p1 SET 
        title = 'Getting Started with Rust',
        tags = [tag:rust, tag:database],
        categories = [category:language, category:technology];

    CREATE post:p2 SET 
        title = 'JavaScript Fundamentals',
        tags = [tag:javascript, tag:web],
        categories = [category:language];

    CREATE post:p3 SET
        title = 'Empty Arrays Test',
        tags = [],
        categories = [];
    
    RETURN "OK";
};

-- 1: SELECT without FETCH (should show array of record IDs)
SELECT * FROM post ORDER BY id;

-- 2: FETCH array field - replace tag IDs with full records
SELECT * FROM post ORDER BY id FETCH tags;

-- 3: FETCH multiple array fields
SELECT * FROM post ORDER BY id FETCH tags, categories;
